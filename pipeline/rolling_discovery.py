"""Bounded discovery of stored sleep endpoints inside admitted source horizons.

Ranges describe dates whose endpoint eligibility is still unknown. They are
not publication jobs and do not establish eligibility without a fresh exact
sleep object observation. The caller checkpoints all mutations with queue CAS.
"""

from __future__ import annotations

import calendar
import hashlib
import re
from datetime import date, timedelta

MAX_MONTHS = 4
MAX_ENDPOINTS = 7


def _ranges(queue):
    return queue.get("rolling_discovery", [])


def validate_discovery(queue):
    from .rolling_night_references import merge

    ranges = _ranges(queue)
    if not isinstance(ranges, list) or len(ranges) > 512:
        raise ValueError("Discovery range capacity")
    for item in ranges:
        if (not isinstance(item, list) or len(item) != 2
                or any(not isinstance(day, str) or date.fromisoformat(day).isoformat() != day
                       for day in item) or item[0] > item[1]):
            raise ValueError("Unverified endpoint discovery range")
    if merge(ranges) != ranges:
        raise ValueError("Noncanonical endpoint discovery ranges")


def schedule_discovery(queue, kind, day):
    from .rolling_night_references import merge, source_range

    if kind not in {"sleep", "hrv", "signals"}:
        raise ValueError("Only admitted night source horizons may be discovered")
    queue["rolling_discovery"] = merge([*_ranges(queue), list(source_range(kind, day, 90))])
    validate_discovery(queue)


def discovery_status(queue, *, today=None, sources=None):
    from .rolling_night_references import intersect, merge, source_range

    ranges = list(_ranges(queue))
    if today is not None:
        ranges = intersect(ranges, date.min.isoformat(), today)
    if sources is not None:
        ranges = merge([target for kind, day in sources
                        for target in intersect(ranges, *source_range(kind, day, 90))])
    return {
        "kind": "endpoint_discovery_scan_candidates",
        "eligibility": "unknown_until_verified_stored_sleep",
        "count": sum((date.fromisoformat(b) - date.fromisoformat(a)).days + 1 for a, b in ranges),
        "ranges": ranges,
    }


def _subtract(ranges, first, last):
    from .rolling_night_references import subtract

    return subtract(ranges, first, last)


def _held_horizons(queue):
    from .night_baseline_refresh import _source
    from .rolling_night_references import merge, source_range

    result = [list(source_range("sleep", binding["wake_date"], 90))
              for binding in queue.get("rolling_context_intents", {}).values()]
    for row in queue["pending"].values():
        for entries in (row["sources"], row.get("following_sources", {})):
            result.extend(list(source_range(*_source(key), 90)) for key, binding in entries.items()
                          if binding["state"] == "intent")
    return merge(result)


def drain_discovery(store, queue, *, today, max_endpoints=MAX_ENDPOINTS,
                    snapshot_cache=None, allowed_sources=None):
    """Observe at most four sleep months and seven positive objects, no writes.

    Complete month inventories prove missing candidate dates without daily GETs.
    Future dates are discarded; later source admission schedules them again.
    Unknown canonical/context intents retain their complete dependent horizons.
    A source collision or unverified response aborts before queue mutation.
    """
    from .night_baseline_refresh import _decode, _snapshot, _validate_source, require
    from .rolling_night_references import intersect, merge, register, source_range

    require(type(max_endpoints) is int and 1 <= max_endpoints <= MAX_ENDPOINTS,
            "Choose1-7 discovery endpoints")
    require(date.fromisoformat(today).isoformat() == today, "Exact discovery day required")
    validate_discovery(queue)
    original = list(_ranges(queue))
    result = {"registered_endpoints": 0, "sleep_objects_observed": 0,
              "months_observed": 0, "unavailable_sleep_endpoints": 0, "held_ranges": []}
    if allowed_sources == [] or not original:
        return {**result, "changed": False, "remaining": discovery_status(queue)}
    remaining = intersect(original, date.min.isoformat(), today)
    candidates = list(remaining)
    if allowed_sources is not None:
        candidates = merge([target for kind, day in allowed_sources
                            for target in intersect(candidates, *source_range(kind, day, 90))])
    for first, last in _held_horizons(queue):
        result["held_ranges"].extend(intersect(candidates, first, last))
        candidates = _subtract(candidates, first, last)
    days, snapshots = [], {}
    inventories = {}
    stop = False
    for first, last in candidates:
        cursor = date.fromisoformat(first)
        while cursor.isoformat() <= last and not stop:
            month = cursor.strftime("%Y/%m/")
            if month not in inventories and len(inventories) >= MAX_MONTHS:
                stop = True
                break
            prefix = "health/sleep/v1/" + month
            if month not in inventories:
                inventories[month] = store.list_revisions_bounded(prefix, max_keys=63)
            inventory = inventories[month]
            choices = {}
            for key, binding in inventory.items():
                match = re.fullmatch(re.escape(prefix) + r"(\d{4}-\d{2}-\d{2})\.json(\.gz)?", key)
                require(match is not None, "Discovery sleep namespace unverified")
                day, compressed = match.groups()
                require(date.fromisoformat(day).isoformat() == day and day[:7] == month[:4] + "-" + month[5:7],
                        "Discovery sleep calendar unverified")
                require(isinstance(binding, dict) and isinstance(binding.get("revision"), str)
                        and 2 < len(binding["revision"]) <= 1024
                        and binding["revision"].startswith('"') and binding["revision"].endswith('"')
                        and type(binding.get("size")) is int and 0 <= binding["size"] <= 524288,
                        "Discovery sleep revision unverified")
                if day not in choices or not compressed:
                    choices[day] = (key, binding)
            month_last = cursor.replace(day=calendar.monthrange(cursor.year, cursor.month)[1])
            end = min(date.fromisoformat(last), month_last)
            scanned_first = cursor.isoformat()
            scanned_last = None
            while cursor <= end:
                day = cursor.isoformat()
                chosen = choices.get(day)
                if chosen is not None:
                    if result["sleep_objects_observed"] >= max_endpoints:
                        stop = True
                        break
                    key, binding = chosen
                    snapshot = _snapshot(store, key, 524288)
                    require(snapshot is not None and snapshot.revision == binding["revision"]
                            and len(snapshot.data) == binding["size"],
                            "Discovery sleep changed after inventory")
                    value = _decode(snapshot.data, 2097152)
                    require(value.get("schema_version") == 1 and value.get("source") == "garmin"
                            and value.get("date") == day, "Discovery sleep source unverified")
                    result["sleep_objects_observed"] += 1
                    if _validate_source("sleep", day, value):
                        days.append(day)
                        snapshots[key] = snapshot
                    else:
                        result["unavailable_sleep_endpoints"] += 1
                scanned_last = day
                cursor += timedelta(days=1)
            if scanned_last is not None:
                remaining = _subtract(remaining, scanned_first, scanned_last)
            if stop:
                break
    # All listed positive snapshots have now passed exact fresh revision/size
    # and the canonical source validator; caller CAS makes this progress durable.
    if days:
        register(queue, days)
    queue["rolling_discovery"] = remaining
    if snapshot_cache is not None:
        used = sum(len(snapshot.data) for snapshot in snapshot_cache.values())
        for key, snapshot in snapshots.items():
            if key in snapshot_cache:
                used -= len(snapshot_cache[key].data)
                snapshot_cache.pop(key)
            if len(snapshot_cache) < 512 and used + len(snapshot.data) <= 16777216:
                snapshot_cache[key] = snapshot
                used += len(snapshot.data)
    result.update(registered_endpoints=len(days), months_observed=len(inventories),
                  changed=remaining != original or bool(days), remaining=discovery_status(queue))
    result["sleep_proofs"] = [{"key": key, "revision": value.revision,
                               "sha256": hashlib.sha256(value.data).hexdigest()}
                              for key, value in snapshots.items()]
    return result
