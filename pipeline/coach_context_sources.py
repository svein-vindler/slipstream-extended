"""Bounded same-day stored sources for a new, immutable Coach snapshot.

No provider, listing, current clock, neighbouring-day search or writes live here.
The returned revisions are private selection proof, not fitness output.
"""

from __future__ import annotations

import gzip
import io
import json
import re
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from .granular import sha256
from .r2_store import R2ReadBoundaryError, missing_object
from .recorded_running_metrics import recorded_running_metrics
from .training_day import MAX_DAY_BYTES, day_key, decode_day

# The existing granular stored/decoded envelope and small check envelope used
# by the Worker readers. Training retains its canonical MAX_DAY_BYTES limit.
HEALTH_STORED_BYTES = 8 * 1024 * 1024
HEALTH_DECODED_BYTES = 32 * 1024 * 1024
CHECK_BYTES = 64 * 1024


def valid_source_proof(proof, day):
    """Recognize our bounded pointer proof without reading historical health."""
    if (not isinstance(proof, dict) or set(proof) != {"schema_version", "day", "sources"}
            or type(proof["schema_version"]) is not int or proof["schema_version"] != 1
            or proof["day"] != day or not isinstance(proof["sources"], dict)
            or set(proof["sources"]) != {"training_day", "sleep", "hrv", "sleep_check", "hrv_check"}):
        return False
    for binding in proof["sources"].values():
        if not isinstance(binding, dict):
            return False
        if binding == {"status": "absent"}:
            continue
        if (set(binding) != {"status", "sha256", "revision"} or binding["status"] != "present"
                or not isinstance(binding["sha256"], str) or not re.fullmatch(r"[a-f0-9]{64}", binding["sha256"])
                or binding["revision"] is not None and (not isinstance(binding["revision"], str)
                                                        or not 1 <= len(binding["revision"]) <= 1024)):
            return False
    return True


def _utc(value):
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0:
            return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except ValueError:
        pass
    return None


def _read(store, key, limit):
    try:
        selected = store.get_snapshot(key, max_bytes=limit)
    except ClientError as error:
        if not missing_object(error):
            raise
        return None, {"status": "absent"}
    # A plain KeyError is not evidence that a remote read returned 404.
    return selected.data, {"status": "present", "sha256": sha256(selected.data),
                           "revision": selected.revision}


def _json(body, limit):
    if body[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            body = stream.read(limit + 1)
    if len(body) > limit:
        raise R2ReadBoundaryError("oversize")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("Invalid stored context schema")
    return value


def assemble_stored_context(store, activity, decoded, endurance, *, cache=None):
    """Read at most five exact keys, only for a verified activity-local day.

    Missing objects are explicit input. Read/authorization/budget failures escape
    before canonical publication; they never replace a previous good selection.
    Cache belongs to this one bulk invocation, never a later historical refresh.
    """
    clock = recorded_running_metrics(activity, decoded, endurance)
    day = clock["local_date"]
    if clock["association"] != "matched" or day is None or clock["session_start_time_utc"] is None:
        return None, None
    if cache is not None and day in cache:
        return cache[day]
    snapshot = {"schema_version": 1, "snapshot_at": None}
    proof = {"schema_version": 1, "day": day, "sources": {}}
    clocks = []
    body, binding = _read(store, day_key(day), MAX_DAY_BYTES)
    proof["sources"]["training_day"] = binding
    if body is not None:
        # A decoded but invalid optional schema suppresses only this stream.
        # Unreadable/unknown JSON is not a known missing day and still stops.
        json.loads(body)
        try:
            training = decode_day(body, day)
        except ValueError:
            training = {"status": "invalid_schema"}
        snapshot["training_day"] = training
        checked = _utc(training.get("source_checked_at"))
        if checked:
            clocks.append(checked)
    for kind, root in (("sleep", "health/sleep/v1"), ("hrv", "health/hrv")):
        key = f"{root}/{day[:4]}/{day[5:7]}/{day}.json"
        body, binding = _read(store, key, HEALTH_STORED_BYTES)
        proof["sources"][kind] = binding
        receipt, receipt_binding = _read(store, f"refresh/checks/v1/health/{kind}/{day}.json", CHECK_BYTES)
        proof["sources"][f"{kind}_check"] = receipt_binding
        checked = None
        if receipt is not None:
            check = _json(receipt, CHECK_BYTES)
            if (body is not None and type(check.get("schema_version")) is int
                    and check["schema_version"] == 1 and check.get("kind") == "health-source-check"
                    and check.get("stream") == kind and check.get("scope") == f"health/{kind}/{day}"
                    and check.get("status") == "ready" and check.get("date") == day
                    and check.get("source_key") == key and check.get("source_sha256") == binding["sha256"]):
                checked = _utc(check.get("checked_at"))
        if body is not None:
            data = _json(body, HEALTH_DECODED_BYTES)
            if (type(data.get("schema_version")) is not int or data["schema_version"] != 1
                    or data.get("source") != "garmin" or data.get("date") != day
                    or not isinstance(data.get("summary"), dict)):
                checked = None
            snapshot[kind] = {"data": data,
                              "source_revision": binding["sha256"], "source_checked_at": checked}
            if checked:
                clocks.append(checked)
    snapshot["snapshot_at"] = max(clocks) if clocks else None
    result = snapshot, proof
    if cache is not None:
        cache[day] = result
    return result
