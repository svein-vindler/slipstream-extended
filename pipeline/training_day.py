"""Canonical daily Readiness/Recovery snapshots. No provider/storage side effects."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .garmin_sdk import CURRENT_SDK_VERSION, STORED_SDK_VERSIONS
from .sources.training_readiness import strict_json

MAX_DAY_BYTES = 16 * 1024
MAX_DEVICES = 4
MAX_OBSERVATIONS = 64
DAY_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
ID_RE = re.compile(r"[1-9][0-9]{0,19}")
TIME_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})?")
ATTEMPT_STATUSES = frozenset({"available", "partial", "no_observation", "auth_required", "source_error",
    "timeout", "oversize", "schema_unverified", "wrong_date"})


def valid_day(day):
    return isinstance(day, str) and DAY_RE.fullmatch(day) is not None and date.fromisoformat(day).isoformat() == day


def day_key(day):
    if not valid_day(day):
        raise ValueError("Invalid training date")
    return f"health/training-context/v1/{day[:4]}/{day[5:7]}/{day}.json"


def configuration(env, *, now=None):
    """Resolve before new I/O. Missing flag is off; invalid config never enables."""
    flag = env.get("TRAINING_READINESS_ENABLED", "false")
    if flag not in {"true", "false"}:
        return {"status": "configuration_required"}
    if flag == "false":
        return {"status": "disabled"}
    zone = env.get("HEALTH_TIMEZONE", "")
    device = env.get("TRAINING_READINESS_DEVICE_ID") or None
    if not isinstance(zone, str) or not zone or len(zone) > 128 or (device is not None and (not isinstance(device, str) or not ID_RE.fullmatch(device))):
        return {"status": "configuration_required"}
    try:
        tz = ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError):
        return {"status": "configuration_required"}
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        return {"status": "configuration_required"}
    return {"status": "enabled", "timezone": zone, "device_id": device,
            "date": now.astimezone(tz).date().isoformat()}


def _time(value):
    if not isinstance(value, str) or not TIME_RE.fullmatch(value):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _clock(row, zone):
    local = _time(row.get("timestampLocal"))
    utc = _time(row.get("timestamp"))
    if local is not None and local.tzinfo is not None:
        local = None
    if utc is not None and utc.tzinfo is None:
        utc = None  # Offsetless timestamp is not documented UTC.
    tz = ZoneInfo(zone)
    if utc is not None:
        resolved = utc.astimezone(timezone.utc)
        if local is not None and resolved.astimezone(tz).replace(tzinfo=None) != local:
            return local.isoformat(), None, "timezone_conflict"
        return local.isoformat() if local else None, resolved.isoformat(), "garmin_offset"
    if local is None:
        return None, None, "unavailable"
    # Reject both DST holes and ambiguous repeated wall clocks. Round trips
    # verify each candidate; a normal wall clock has only one unique instant.
    candidates = set()
    for fold in (0, 1):
        candidate = local.replace(tzinfo=tz, fold=fold).astimezone(timezone.utc)
        if candidate.astimezone(tz).replace(tzinfo=None) == local:
            candidates.add(candidate.isoformat())
    if len(candidates) != 1:
        return local.isoformat(), None, "unavailable"
    return local.isoformat(), candidates.pop(), "configured_timezone"


def _field(value, low, high, unit, checked, local, utc):
    status = "no_observation" if value is None else "available" if type(value) is int and low <= value <= high else "invalid"
    return {"value": value if status == "available" else None, "status": status, "unit": unit,
            "source_checked_at": checked if status == "available" else None,
            "observation_local": local, "observation_at": utc, "retained_previous": False}


def _observation(row, checked, zone):
    local, utc, source = _clock(row, zone)
    readiness = _field(row.get("score"), 1, 100, "score_1_100", checked, local, utc)
    recovery = _field(row.get("recoveryTime"), 0, 2**31 - 1, "minutes", checked, local, utc)
    readiness["time_source"] = recovery["time_source"] = source
    recovery["reached_zero"] = True if row.get("recoveryTimeChangePhrase") == "REACHED_ZERO" else None
    if recovery["status"] == "available" and recovery["value"] > 0 and recovery["reached_zero"]:
        recovery["status"] = "conflicting"
    return {"calendar_date": row["calendarDate"], "observation_local": local,
            "observation_at": utc, "time_source": source,
            "input_context": "AFTER_WAKEUP_RESET" if row.get("inputContext") == "AFTER_WAKEUP_RESET" else None,
            "readiness": readiness, "recovery": recovery}


def _choose(rows):
    if len(rows) == 1:
        return rows[0]
    if not rows or any(row["observation_at"] is None for row in rows):
        return None
    ordered = sorted(rows, key=lambda row: datetime.fromisoformat(row["observation_at"]))
    if ordered[-1]["observation_at"] == ordered[-2]["observation_at"]:
        return None
    return ordered[-1]


def normalize(payload, config, checked):
    rows = [payload] if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or len(rows) > MAX_OBSERVATIONS:
        raise ValueError("Unverified training schema")
    groups = {}
    partial = False
    wrong_date = False
    for row in rows:
        if not isinstance(row, dict):
            partial = True
            continue
        raw_id = row.get("deviceId")
        device = str(raw_id) if type(raw_id) in {int, str} else ""
        if not ID_RE.fullmatch(device):
            partial = True
            continue
        if row.get("calendarDate") != config["date"]:
            partial = True
            wrong_date = True
            continue
        obs = _observation(row, checked, config["timezone"])
        if obs["observation_local"] and obs["observation_local"][:10] != config["date"]:
            partial = True
            wrong_date = True
            continue
        if obs["observation_local"] is None and obs["observation_at"] is not None and datetime.fromisoformat(obs["observation_at"]).astimezone(ZoneInfo(config["timezone"])).date().isoformat() != config["date"]:
            obs["observation_at"] = None
            obs["time_source"] = "timezone_conflict"
            for field in ("readiness", "recovery"):
                obs[field]["observation_at"] = None
                obs[field]["time_source"] = "timezone_conflict"
        if obs["time_source"] == "timezone_conflict":
            partial = True
        if any(obs[field]["status"] in {"invalid", "no_observation"} for field in ("readiness", "recovery")):
            partial = True
        group = groups.setdefault(device, [])
        if obs not in group:
            group.append(obs)
    if len(groups) > MAX_DEVICES:
        raise ValueError("Too many training devices")
    if rows and not groups:
        raise ValueError("Wrong training date" if wrong_date else "Unverified training schema")
    devices = []
    for device, group in sorted(groups.items()):
        morning = [row for row in group if row["input_context"] == "AFTER_WAKEUP_RESET"]
        devices.append({"device_id": device, "latest": _choose(group),
            "latest_status": "available" if _choose(group) else "ambiguous",
            "morning": morning[0] if len(morning) == 1 else None,
            "morning_status": "available" if len(morning) == 1 else "ambiguous" if morning else "no_observation"})
    return devices, "partial" if partial else "available" if devices else "no_observation"


def encode_day(day):
    body = json.dumps(day, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(body) > MAX_DAY_BYTES:
        raise ValueError("Training day exceeds storage limit")
    return body


def decode_day(body, day):
    """Validate the complete normalizer-owned shape before preserving prior data."""
    if len(body) > MAX_DAY_BYTES:
        raise ValueError("Training day exceeds storage limit")
    value = strict_json(body)
    if (not isinstance(value, dict) or type(value.get("schema_version")) is not int or value.get("schema_version") != 1
            or type(value.get("normalizer_revision")) is not int or value.get("normalizer_revision") != 1 or value.get("kind") != "training-day"
            or value.get("source") != "garmin-connect" or value.get("sdk_version") not in STORED_SDK_VERSIONS
            or value.get("calendar_date") != day or value.get("requested_date") != day
            or not isinstance(value.get("devices"), list) or len(value["devices"]) > MAX_DEVICES
            or not isinstance(value.get("last_attempt"), dict)
            or value["last_attempt"].get("status") not in ATTEMPT_STATUSES):
        raise ValueError("Unverified training day")
    def clock_valid(clock, *, local=False, nullable=True):
        if clock is None:
            return nullable
        parsed = _time(clock)
        return parsed is not None and (parsed.tzinfo is None if local else
               parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0)

    if (not clock_valid(value.get("source_checked_at"))
            or not clock_valid(value["last_attempt"].get("attempted_at"), nullable=False)
            or not isinstance(value.get("source_revision"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", value["source_revision"])
            or not isinstance(value.get("date_resolution"), dict)
            or value["date_resolution"].get("basis") != "configured_timezone"
            or not isinstance(value.get("selection"), dict)
            or value["selection"].get("status") not in {"available", "ambiguous", "no_observation", "selected_device_unavailable", "unknown"}):
        raise ValueError("Unverified training provenance")
    try:
        ZoneInfo(value["date_resolution"]["timezone"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Unverified training timezone") from error
    seen = set()
    for device in value["devices"]:
        if not isinstance(device, dict) or not isinstance(device.get("device_id"), str) or not ID_RE.fullmatch(device["device_id"]) or device["device_id"] in seen:
            raise ValueError("Unverified training device")
        seen.add(device["device_id"])
        if any(device.get(f"{slot}_status") not in {"available", "ambiguous", "no_observation"} for slot in ("morning", "latest")):
            raise ValueError("Unverified training selection")
        for slot in ("morning", "latest"):
            obs = device.get(slot)
            if obs is None:
                continue
            if (not isinstance(obs, dict) or obs.get("calendar_date") != day
                    or not clock_valid(obs.get("observation_at"))
                    or not clock_valid(obs.get("observation_local"), local=True)
                    or obs.get("time_source") not in {"garmin_offset", "configured_timezone", "unavailable", "timezone_conflict"}
                    or obs.get("input_context") not in {None, "AFTER_WAKEUP_RESET"}):
                raise ValueError("Unverified training observation")
            if obs.get("observation_local") is not None and obs["observation_local"][:10] != day:
                raise ValueError("Unverified local training date")
            for field, unit, low, high in (("readiness", "score_1_100", 1, 100), ("recovery", "minutes", 0, 2**31-1)):
                metric = obs.get(field)
                if not isinstance(metric, dict) or metric.get("unit") != unit or metric.get("status") not in {"available", "conflicting", "invalid", "no_observation"}:
                    raise ValueError("Unverified training field")
                if metric["status"] in {"available", "conflicting"} and (type(metric.get("value")) is not int or not low <= metric["value"] <= high):
                    raise ValueError("Unverified training value")
                if (type(metric.get("retained_previous")) is not bool
                        or not clock_valid(metric.get("observation_at"))
                        or not clock_valid(metric.get("observation_local"), local=True)
                        or not clock_valid(metric.get("source_checked_at"), nullable=metric["status"] not in {"available", "conflicting"})
                        or metric.get("time_source") not in {"garmin_offset", "configured_timezone", "unavailable", "timezone_conflict"}
                        or (metric["status"] in {"invalid", "no_observation"} and
                            (metric.get("value") is not None or metric.get("source_checked_at") is not None))):
                    raise ValueError("Unverified training field provenance")
                if field == "recovery" and ((metric.get("reached_zero") is not None and type(metric["reached_zero"]) is not bool) or
                        (metric["status"] == "conflicting") != (metric["value"] is not None and metric["value"] > 0 and metric["reached_zero"] is True)):
                    raise ValueError("Unverified recovery conflict")
                if field == "readiness" and metric["status"] == "conflicting":
                    raise ValueError("Unverified readiness conflict")
                if metric.get("observation_local") is not None and metric["observation_local"][:10] != day:
                    raise ValueError("Unverified local training field date")
    selected = value["selection"].get("device_id")
    if (selected is not None and (not isinstance(selected, str) or not ID_RE.fullmatch(selected))) or (value["selection"]["status"] == "available" and selected not in seen):
        raise ValueError("Unverified selected device")
    return value


def merge_day(previous, devices, status, config, checked):
    day = copy.deepcopy(previous) if previous else {
        "schema_version": 1, "kind": "training-day", "normalizer_revision": 1,
        "source": "garmin-connect", "sdk_version": CURRENT_SDK_VERSION, "requested_date": config["date"],
        "calendar_date": config["date"], "source_checked_at": None, "devices": [],
    }
    day["date_resolution"] = {"timezone": config["timezone"], "basis": "configured_timezone"}
    day["last_attempt"] = {"status": status, "attempted_at": checked}
    old = {item["device_id"]: item for item in day["devices"]}
    incoming = {item["device_id"]: item for item in devices}
    if len(old.keys() | incoming.keys()) > MAX_DEVICES:
        raise ValueError("Too many retained training devices")
    merged = []
    for device_id in sorted(old.keys() | incoming.keys()):
        prev = old.get(device_id, {})
        new = incoming.get(device_id)
        item = copy.deepcopy(new or prev)
        for slot in ("morning", "latest"):
            before = prev.get(slot)
            after = item.get(slot)
            if before and (not after or (before["observation_at"] and after["observation_at"]
                    and datetime.fromisoformat(after["observation_at"]) < datetime.fromisoformat(before["observation_at"]))):
                after = item[slot] = copy.deepcopy(before)
                for field in ("readiness", "recovery"):
                    after[field]["retained_previous"] = True
            elif before and after:
                for field in ("readiness", "recovery"):
                    if after[field]["status"] not in {"available", "conflicting"} and before[field]["status"] in {"available", "conflicting"}:
                        after[field] = copy.deepcopy(before[field])
                        after[field]["retained_previous"] = True
            if new is None and after:
                for field in ("readiness", "recovery"):
                    after[field]["retained_previous"] = True
        merged.append(item)
    day["devices"] = merged
    # A new SDK may update only one field or device. Keep retained data's
    # provenance instead of attributing the entire object to the latest SDK.
    versions = {previous["sdk_version"] if metric["retained_previous"] and previous else CURRENT_SDK_VERSION
                for item in merged for slot in ("morning", "latest") if item.get(slot)
                for metric in (item[slot]["readiness"], item[slot]["recovery"])}
    if versions:
        day["sdk_version"] = "mixed" if len(versions) > 1 or "mixed" in versions else next(iter(versions))
    elif previous and status not in {"available", "no_observation"}:
        day["sdk_version"] = previous["sdk_version"]
    else:
        day["sdk_version"] = CURRENT_SDK_VERSION
    if status in {"available", "no_observation"}:
        day["source_checked_at"] = checked
    selected = config.get("device_id")
    if selected:
        if selected in incoming:
            selection_status = "available"
        elif status in {"available", "no_observation"}:
            selection_status = "selected_device_unavailable"
        else:
            # Failure/partial is not evidence of absence. Retained observations
            # identify the configured device, independently of the last attempt.
            selection_status = "available" if selected in old else "unknown"
    elif len(merged) == 1:
        selected, selection_status = merged[0]["device_id"], "available"
    else:
        selection_status = "ambiguous" if merged else "no_observation"
    day["selection"] = {"status": selection_status, "device_id": selected}
    # Hash values/time provenance independently of checks/attempt timestamps.
    revision_data = copy.deepcopy(merged)
    for item in revision_data:
        for slot in ("morning", "latest"):
            if item.get(slot):
                for field in ("readiness", "recovery"):
                    item[slot][field].pop("source_checked_at", None)
                    item[slot][field].pop("retained_previous", None)
    day["source_revision"] = hashlib.sha256(json.dumps(revision_data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return day
