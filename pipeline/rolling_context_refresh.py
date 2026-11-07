"""Finish confirmed context work at a qualified publication checkpoint.

Only the append producer can record ``saved`` after its ACK and exact event
readback. An unknown ``intent`` is never upgraded here. Before superseding a
publishing generation, every recorded effect must still match its ACK evidence,
and there must be no source intent or inflight write. The caller's queue CAS
fences an active writer before that writer can save a new inflight operation.
"""

import copy
import hashlib
import re

from . import rolling_night_references as rolling
from .night_products import normalize_args, storage_key


def _affected(row, day):
    n = row.get("rolling_window")
    return (n in rolling.WINDOWS and day <= row["end_date"]
            <= rolling.source_range("sleep", day, n)[1])


def reconcile_saved_contexts(store, queue, *, reserve_writes=0):
    from .night_baseline_refresh import (
        ComponentBudgetDeferred,
        _decode,
        _snapshot,
        _source_maps,
        canonical,
        require,
    )

    result = {"completed": 0, "busy": 0, "restarted_rows": 0, "remaining": 0}
    chosen = []
    for identity, binding in sorted(queue.get("rolling_context_intents", {}).items()):
        if binding["state"] != "saved":
            continue
        rows = [row for row in queue["pending"].values() if _affected(row, binding["wake_date"])]
        if any(row.get("inflight") or any(
            item["state"] != "ready" for sources in _source_maps(row) for item in sources.values()
        ) for row in rows):
            result["busy"] += 1
            continue
        if len(chosen) < 2:
            chosen.append((identity, binding, rows))
    if not chosen:
        result["remaining"] = sum(b["state"] == "saved"
                                  for b in queue.get("rolling_context_intents", {}).values())
        return result, False
    # Reserve verification plus the single caller CAS/readback. Check the exact
    # resulting byte count below; never authorize a write from a smaller preimage.
    reads = sum(1 + sum(len(row.get(field, {})) for row in rows
                        for field in ("completed", "history")) for _, _, rows in chosen)
    store.require_capacity("get", reads + 1)
    store.require_capacity("put", 1)
    original = copy.deepcopy(queue)
    for identity, binding, rows in chosen:
        event = _snapshot(store, binding["key"], 16384)
        require(event is not None and event.revision == binding["saved_revision"]
                and event.content_type == "application/json" and event.encoding is None
                and hashlib.sha256(event.data).hexdigest() == binding["sha256"],
                "Confirmed context immutable event changed")
        value = _decode(event.data, 16384)
        require(value.get("schema_version") == 1 and value.get("source") == "user"
                and value.get("wake_date") == binding["wake_date"]
                and value.get("context_id") == identity,
                "Confirmed context binding changed")
        for row in rows:
            args = normalize_args(start_date=row["start_date"], end_date=row["end_date"],
                                  period="custom", channel=row["channel"])
            target = storage_key(args)
            require(set(row.get("completed", {})) <= {target},
                    "Recorded context generation product unverified")
            for field in ("completed", "history"):
                for key, pin in row.get(field, {}).items():
                    require(field == "completed" or re.fullmatch(
                        r"health/night-products/v1/history/(?:[a-f0-9]{64}/){4}(?:product|provenance)\.json", key
                    ), "Recorded context history namespace unverified")
                    require(isinstance(pin, dict) and set(pin) == {"revision", "sha256"}
                            and isinstance(pin["revision"], str)
                            and re.fullmatch(r'"[^"\\\x00-\x20\x7f]{1,1022}"', pin["revision"])
                            and isinstance(pin["sha256"], str)
                            and re.fullmatch(r"[a-f0-9]{64}", pin["sha256"]),
                            "Recorded context checkpoint unverified")
                    observed = _snapshot(store, key, 262144)
                    require(observed is not None and observed.revision == pin["revision"]
                            and observed.content_type == "application/json"
                            and observed.encoding in ({None} if field == "completed" else {None, "gzip"})
                            and hashlib.sha256(observed.data).hexdigest() == pin["sha256"],
                            "Recorded context checkpoint changed")
        # All effects across this context's affected generations are now freshly
        # verified. An active writer using the old queue revision must lose its
        # next CAS before issuing another external write; no unknown is adopted.
        for row in rows:
            row["sources"].update(copy.deepcopy(row.pop("following_sources", {})))
            row["phase"] = "dirty"
            row["completed"] = {}
            for field in ("source_proof_sha256", "validated_progress", "blocked"):
                row.pop(field, None)
            result["restarted_rows"] += 1
        rolling.invalidate(queue, "sleep", binding["wake_date"])
        del queue["rolling_context_intents"][identity]
        result["completed"] += 1
    result["remaining"] = sum(b["state"] == "saved"
                              for b in queue.get("rolling_context_intents", {}).values())
    if not store.store.can_write_group(attempts=1 + reserve_writes,
                                       byte_count=len(canonical(queue).encode())):
        queue.clear()
        queue.update(original)
        raise ComponentBudgetDeferred("Known context checkpoint deferred")
    return result, True
