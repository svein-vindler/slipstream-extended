"""Compact endpoint registry and resumable rolling reference ranges.

The existing durable queue owns these additive fields. Ranges are inclusive;
only the existing consumer materializes bounded per-channel publication rows.
"""

import re
from datetime import date, timedelta

WINDOWS = (28, 30, 90)
CHANNELS = ("heart_rate", "hrv")


def merge(ranges):
    result = []
    for first, last in sorted(ranges):
        if result and (date.fromisoformat(first) - date.fromisoformat(result[-1][1])).days <= 1:
            result[-1][1] = max(last, result[-1][1])
        else:
            result.append([first, last])
    return result


def intersect(ranges, first, last):
    return [[max(a, first), min(b, last)] for a, b in ranges if a <= last and b >= first]


def subtract(ranges, first, last):
    """Subtract an inclusive calendar interval without expanding its dates."""
    result = []
    for a, b in ranges:
        if b < first or a > last:
            result.append([a, b])
        else:
            if a < first:
                result.append([a, (date.fromisoformat(first) - timedelta(days=1)).isoformat()])
            if b > last:
                result.append([(date.fromisoformat(last) + timedelta(days=1)).isoformat(), b])
    return result


def remove_day(ranges, day):
    return subtract(ranges, day, day)


def state(queue):
    return queue.setdefault("rolling", {"eligible": [], "dirty": {
        str(n): {channel: [] for channel in CHANNELS} for n in WINDOWS
    }})


def validate(queue):
    from .rolling_discovery import validate_discovery

    validate_discovery(queue)
    intents = queue.get("rolling_context_intents", {})
    if not isinstance(intents, dict) or len(intents) > 128:
        raise ValueError("Unverified context intents")
    for identity, binding in intents.items():
        if (not isinstance(identity, str) or not re.fullmatch(r"nctx-[a-f0-9]{24}", identity)
                or not isinstance(binding, dict)
                or set(binding) != ({"wake_date", "key", "sha256", "state", "saved_revision"}
                                    if binding.get("state") == "saved"
                                    else {"wake_date", "key", "sha256", "state"})
                or binding["state"] not in {"intent", "saved"}
                or not isinstance(binding["sha256"], str)
                or not re.fullmatch(r"[a-f0-9]{64}", binding["sha256"])):
            raise ValueError("Unverified context intent")
        if binding["state"] == "saved" and (
            not isinstance(binding["saved_revision"], str)
            or not re.fullmatch(r'"[^"\\\x00-\x20\x7f]{1,1022}"', binding["saved_revision"])
        ):
            raise ValueError("Unverified confirmed context revision")
        day = binding["wake_date"]
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError("Unverified context calendar")
        prefix = f"health/night-context/v1/{day[:4]}/{day[5:7]}/{day}/"
        if (not isinstance(binding["key"], str) or not binding["key"].startswith(prefix)
                or not re.fullmatch(r"\d{8}T\d{9}-" + identity + r"\.json", binding["key"][len(prefix):])):
            raise ValueError("Unverified context intent key")
    if "rolling" not in queue:
        return
    value = queue["rolling"]
    if (not isinstance(value, dict) or set(value) != {"eligible", "dirty"}
            or not isinstance(value["dirty"], dict)
            or set(value["dirty"]) != {str(n) for n in WINDOWS}):
        raise ValueError("Unverified rolling registry")
    collections = [value["eligible"]]
    for channels in value["dirty"].values():
        if not isinstance(channels, dict) or set(channels) != set(CHANNELS):
            raise ValueError("Unverified rolling channels")
        collections.extend(channels.values())
    for ranges in collections:
        if not isinstance(ranges, list) or len(ranges) > 512:
            raise ValueError("Rolling range capacity")
        for item in ranges:
            if (not isinstance(item, list) or len(item) != 2
                    or any(not isinstance(day, str) or date.fromisoformat(day).isoformat() != day
                           for day in item) or item[0] > item[1]):
                raise ValueError("Unverified rolling range")
        if merge(ranges) != ranges:
            raise ValueError("Noncanonical rolling ranges")
    for channels in value["dirty"].values():
        for ranges in channels.values():
            if any(intersect(value["eligible"], a, b) != [[a, b]] for a, b in ranges):
                raise ValueError("Rolling endpoint lacks eligibility")


def register(queue, days):
    value = state(queue)
    for day in days:
        if intersect(value["eligible"], day, day):
            continue
        value["eligible"] = merge([*value["eligible"], [day, day]])
        for channels in value["dirty"].values():
            for channel in CHANNELS:
                channels[channel] = merge([*channels[channel], [day, day]])


def source_range(kind, day, window):
    source = date.fromisoformat(day)
    margin = 4 if kind == "signals" else 0
    return ((source - timedelta(days=margin)).isoformat(),
            (source + timedelta(days=window - 1 + margin)).isoformat())


def invalidate(queue, kind, day):
    from .rolling_discovery import schedule_discovery

    schedule_discovery(queue, kind, day)
    if "rolling" not in queue:
        return
    value = state(queue)
    for n in WINDOWS:
        first, last = source_range(kind, day, n)
        targets = intersect(value["eligible"], first, last)
        for channel in CHANNELS:
            value["dirty"][str(n)][channel] = merge([
                *value["dirty"][str(n)][channel], *targets,
            ])
    # Safe known work can be rebuilt; publication/unknown rows retain ownership.
    for row in queue["pending"].values():
        if "rolling_window" not in row:
            continue
        first, last = source_range(kind, day, row["rolling_window"])
        if first <= row["end_date"] <= last and row["phase"] == "dirty" and not row.get("inflight"):
            row.pop("blocked", None)
            row.pop("validated_progress", None)
            row["completed"] = {}
            channels = value["dirty"][str(row["rolling_window"])]
            channels[row["channel"]] = remove_day(channels[row["channel"]], row["end_date"])


def row_identity(name, row):
    n, channel = row.get("rolling_window"), row.get("channel")
    return (type(n) is int and n in WINDOWS and channel in CHANNELS
            and name == f"rolling/{n}/{row.get('end_date')}/{channel}"
            and date.fromisoformat(row["start_date"]) + timedelta(days=n - 1)
            == date.fromisoformat(row["end_date"]))


def status(queue, *, today=None, sources=None):
    """Count compact rest plus durable rows, optionally intersecting a source cohort.

    Sources are (canonical kind, source date) pairs; no historical enumeration.
    Counts are channel endpoint counts, not expected successful products.
    """
    value = queue.get("rolling")
    result = {}
    for n in WINDOWS:
        result[str(n)] = {}
        for channel in CHANNELS:
            ranges = list(value["dirty"][str(n)][channel]) if value else []
            ranges.extend([row["end_date"], row["end_date"]]
                          for row in queue["pending"].values()
                          if row.get("rolling_window") == n and row.get("channel") == channel)
            ranges = merge(ranges)
            if today:
                ranges = intersect(ranges, date.min.isoformat(), today)
            if sources is not None:
                ranges = merge([target for kind, day in sources
                                for target in intersect(ranges, *source_range(kind, day, n))])
            result[str(n)][channel] = {
                "count": sum((date.fromisoformat(b) - date.fromisoformat(a)).days + 1
                             for a, b in ranges),
                "ranges": ranges,
            }
    return result


def materialize(queue, *, today, limit, max_pending, allowed_sources=None, allowed_endpoints=None):
    """Move at most limit ranges' first endpoints into the durable pending map."""
    if "rolling" not in queue:
        return False
    changed = False
    value = state(queue)
    intents = [(key, binding) for row in queue["pending"].values()
               for entries in (row["sources"], row.get("following_sources", {}))
               for key, binding in entries.items() if binding.get("state") == "intent"]
    held_sources = [({"sleep/v1": "sleep", "hrv": "hrv", "signals/v1": "signals"}[
        key.removeprefix("health/").rsplit("/", 3)[0]], key.rsplit("/", 1)[-1][:-5])
        for key, _ in intents]
    held_sources.extend(("sleep", row["wake_date"])
                        for row in queue.get("rolling_context_intents", {}).values())
    groups = [(n, channel) for n in WINDOWS for channel in CHANNELS]
    # One blocked channel must not fill all publication slots before another
    # channel gets a checkpoint. Prefer cohorts with less retained held work.
    groups.sort(key=lambda group: sum(
        row.get("rolling_window") == group[0] and row.get("channel") == group[1]
        and bool(row.get("blocked") or row.get("inflight") or row["phase"] == "publishing")
        for row in queue["pending"].values()
    ))
    for n, channel in groups:
        ranges = value["dirty"][str(n)][channel]
        candidates = intersect(ranges, date.min.isoformat(), today)
        if allowed_endpoints is not None:
            candidates = merge([target for day in allowed_endpoints
                                for target in intersect(candidates, day, day)])
        if allowed_sources is not None:
            candidates = merge([target for kind, day in allowed_sources
                                for target in intersect(candidates, *source_range(kind, day, n))])
        for kind, day in held_sources:
            a, b = source_range(kind, day, n)
            candidates = subtract(candidates, a, b)
        for row in queue["pending"].values():
            if row.get("rolling_window") == n and row.get("channel") == channel:
                candidates = remove_day(candidates, row["end_date"])
        if candidates and limit and len(queue["pending"]) < max_pending:
            first = candidates[0][0]
            name = f"rolling/{n}/{first}/{channel}"
            queue["pending"][name] = {
                "rolling_window": n, "channel": channel,
                "start_date": (date.fromisoformat(first) - timedelta(days=n - 1)).isoformat(),
                "end_date": first, "phase": "dirty", "sources": {}, "completed": {},
            }
            ranges[:] = remove_day(ranges, first)
            limit -= 1
            changed = True
    return changed
