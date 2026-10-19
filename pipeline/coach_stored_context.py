"""Pure, neutral projection of explicitly supplied stored training context.

This module neither fetches sources nor chooses a history window. Its clock is
the caller's explicit snapshot time, never the current time.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SLEEP_METRICS = {
    "sleep_seconds": "s", "deep_sleep_seconds": "s", "light_sleep_seconds": "s",
    "rem_sleep_seconds": "s", "awake_sleep_seconds": "s", "unmeasurable_sleep_seconds": "s",
    "nap_seconds": "s", "sleep_score": "score_0_100", "average_spo2_percent": "%",
    "lowest_spo2_percent": "%", "average_respiration_brpm": "breaths_per_minute",
    "lowest_respiration_brpm": "breaths_per_minute", "highest_respiration_brpm": "breaths_per_minute",
    "average_sleep_stress": "score_0_100",
}
HRV_METRICS = {
    "last_night_avg_ms": ("lastNightAvg", "lastNightAverage", "lastNightAvgMs"),
    "weekly_avg_ms": ("weeklyAvg", "weeklyAverage", "weeklyAvgMs"),
    "last_night_5_min_high_ms": ("lastNight5MinHigh", "lastNightFiveMinHigh", "lastNight5MinHighMs"),
    "baseline_low_ms": ("baselineBalancedLower", "baselineLow", "baselineLower"),
    "baseline_high_ms": ("baselineBalancedUpper", "baselineHigh", "baselineUpper"),
}


def _utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0 else None


def _health_time(value: Any) -> datetime | None:
    """These stored fields are explicitly GMT; numeric clocks can be milliseconds."""
    if type(value) in (int, float):
        try:
            if not math.isfinite(value):
                return None
            return datetime.fromtimestamp(value / 1000 if value > 10_000_000_000 else value, timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None
    if isinstance(value, str) and len(value) <= 40:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def _garmin_local(value: Any, utc: datetime | None) -> datetime | None:
    if utc is None:
        return None
    try:
        if isinstance(value, str) and len(value) <= 40:
            wall = datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            parsed = _health_time(value)
            wall = parsed.replace(tzinfo=None) if parsed else None
        if wall is None:
            return None
        minutes = (wall - utc.replace(tzinfo=None)).total_seconds() / 60
        offset = round(minutes)
        if abs(offset) > 14 * 60 or abs(minutes - offset) > 0.5:
            return None
        return wall.replace(tzinfo=timezone(timedelta(minutes=offset)))
    except (ValueError, TypeError, OverflowError):
        return None


def _recorded_metric(value: Any, unit: str) -> dict[str, Any]:
    try:
        valid = (type(value) in (int, float) and math.isfinite(value) and value >= 0
                 and (unit not in {"score_0_100", "%"} or value <= 100))
    except OverflowError:
        valid = False
    return {"status": "missing" if value is None else "available" if valid else "invalid",
            "value": value if valid else None, "unit": unit}


def _health_context(kind: str, source: dict[str, Any] | None, reference: datetime | None,
                    activity_day: str | None, configured_timezone: Any = None) -> dict[str, Any]:
    if source is None:
        names = SLEEP_METRICS if kind == "sleep" else {name: "ms" for name in HRV_METRICS}
        return {
            "status": "not_collected", "source": "garmin", "source_revision": None,
            "source_checked_at": None, "wake_date": activity_day, "night_of": None,
            "local_time_source": "unavailable", "observation_start_utc": None, "observation_end_utc": None,
            "metrics": {name: {"value": None, "status": "unavailable", "unit": unit}
                        for name, unit in names.items()},
        }
    data = source.get("data") if isinstance(source, dict) else None
    if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
            or data.get("schema_version") != 1 or data.get("source") != "garmin"
            or not isinstance(data.get("date"), str) or not isinstance(data.get("summary"), dict)):
        invalid = _health_context(kind, None, reference, activity_day)
        invalid["status"] = (source["status"] if isinstance(source, dict)
                             and source.get("status") in {"source_error", "oversize", "not_collected"}
                             else "invalid_schema")
        return invalid
    summary = data["summary"]
    names = SLEEP_METRICS if kind == "sleep" else {name: "ms" for name in HRV_METRICS}
    values = {name: summary.get(name) if kind == "sleep" else next(
        (summary[key] for key in HRV_METRICS[name] if summary.get(key) is not None), None,
    ) for name in names}
    metrics = {name: _recorded_metric(value, names[name]) for name, value in values.items()}
    start, end = (_health_time(data.get(field)) for field in ("sleep_start_gmt", "sleep_end_gmt"))
    local_start = _garmin_local(data.get("sleep_start_garmin_local"), start)
    local_end = _garmin_local(data.get("sleep_end_garmin_local"), end)
    local_source = "garmin_local" if local_start is not None and local_end is not None else "unavailable"
    if (data.get("sleep_start_garmin_local") is None and data.get("sleep_end_garmin_local") is None
            and start is not None and end is not None and isinstance(configured_timezone, str)
            and 0 < len(configured_timezone) <= 128):
        try:
            zone = ZoneInfo(configured_timezone)
            local_start, local_end = start.astimezone(zone), end.astimezone(zone)
            local_source = "configured_timezone"
        except (ZoneInfoNotFoundError, ValueError):
            pass
    status = ("time_unknown" if activity_day is None
              else "wrong_date" if data.get("date") != activity_day
              else "time_unknown" if reference is None or start is None or end is None or end <= start
              else "local_time_unknown" if local_start is None or local_end is None
              else "wrong_date" if local_end.date().isoformat() != activity_day
              else "after_activity" if end > reference else "available")
    if status != "available":
        metrics = {name: {**metric, "status": "unavailable", "value": None}
                   for name, metric in metrics.items()}
    return {
        "status": status, "source": "garmin",
        "source_revision": source.get("source_revision"),
        "source_checked_at": source.get("source_checked_at"),
        "wake_date": data["date"], "night_of": local_start.date().isoformat() if local_start else None,
        "sleep_start_local": local_start.isoformat() if local_start else None,
        "sleep_end_local": local_end.isoformat() if local_end else None,
        "local_time_source": local_source,
        "observation_start_utc": _iso(start),
        "observation_end_utc": _iso(end),
        "metrics": metrics,
    }


def _empty_training(status: str) -> dict[str, Any]:
    return {
        "status": status, "selection_status": "unknown", "selected_slot": None,
        "temporal_status": "selection_unavailable",
        **{name: {"value": None, "status": "unavailable", "unit": unit,
                  "observation_at": None, "source_checked_at": None, "retained_previous": False}
           for name, unit in (("readiness", "score_1_100"), ("recovery", "minutes"))},
    }


def stored_context_snapshot(
    snapshot: dict[str, Any], activity_clock: dict[str, Any],
) -> dict[str, Any]:
    """Project the existing canonical day without exposing a private device ID."""
    reference = _utc(activity_clock.get("session_start_time_utc"))
    result = {
        "schema_version": 1,
        "snapshot_at": snapshot["snapshot_at"],
        "reference_time_utc": activity_clock["session_start_time_utc"],
        "semantics": "retrospective_pre_activity",
        **{name: _health_context(name, snapshot.get(name), reference, activity_clock.get("local_date"),
                                snapshot.get("timezone"))
           for name in ("sleep", "hrv")},
    }
    day = snapshot.get("training_day")
    if day is None:
        result["training_day"] = _empty_training("not_collected")
        return result
    if isinstance(day, dict) and day.get("status") in {"source_error", "oversize", "not_collected"}:
        result["training_day"] = _empty_training(day["status"])
        return result
    # Backup imports the data-only projection contract without the sync parser's
    # optional provider dependencies. Decode only when building actual context.
    from .training_day import decode_day

    try:
        day = decode_day(json.dumps(day, allow_nan=False).encode(), day["calendar_date"])
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
        result["training_day"] = _empty_training("invalid_schema")
        return result
    selected = (next((row for row in day["devices"]
                      if row["device_id"] == day["selection"]["device_id"]), None)
                if day["selection"]["status"] == "available" else None)
    observation = selected["latest"] if selected else None
    selected_slot = "latest" if observation else None
    eligible = []
    if selected is not None and reference is not None:
        for slot in ("latest", "morning"):
            candidate = selected[slot]
            observed = _utc(candidate["observation_at"]) if candidate else None
            if selected[f"{slot}_status"] == "available" and observed is not None and observed <= reference:
                eligible.append((slot, candidate, observed))
    if eligible:
        selected_slot, observation, _ = max(eligible, key=lambda item: item[2])
    metrics = {
        name: {field: observation[name][field] for field in (
            "value", "status", "unit", "observation_at", "source_checked_at",
            "retained_previous",
        )}
        for name in ("readiness", "recovery")
    } if observation else {name: {
        "value": None, "status": "unavailable", "unit": unit,
        "observation_at": None, "source_checked_at": None, "retained_previous": False,
    } for name, unit in (("readiness", "score_1_100"), ("recovery", "minutes"))}
    observed = _utc(observation["observation_at"]) if observation else None
    temporal_status = ("selection_unavailable" if observation is None
                       else "time_unknown" if reference is None or observed is None
                       else "before_activity" if observed <= reference else "after_activity")
    if temporal_status != "before_activity":
        metrics = {name: {**metric, "value": None, "status": "unavailable"}
                   for name, metric in metrics.items()}
    else:
        for name, metric in metrics.items():
            measured = _utc(metric["observation_at"])
            if measured is None or measured > reference:
                metrics[name] = {**metric, "value": None, "status": "unavailable"}
    return {
        **result,
        "training_day": {
            "status": "stored",
            "source": day["source"], "sdk_version": day["sdk_version"],
            "normalizer_revision": day["normalizer_revision"],
            "source_revision": day["source_revision"],
            "date": day["calendar_date"], "timezone": day["date_resolution"]["timezone"],
            "selection_status": day["selection"]["status"],
            "temporal_status": temporal_status,
            "selected_slot": selected_slot,
            "input_context": observation["input_context"] if observation else None,
            "last_attempt": {field: day["last_attempt"][field] for field in ("status", "attempted_at")},
            "source_checked_at": day["source_checked_at"],
            "time_provenance": {name: {
                "observation_local": observation[name]["observation_local"] if observation else None,
                "time_source": observation[name]["time_source"] if observation else "unavailable",
            } for name in ("readiness", "recovery")},
            **metrics,
        },
    }
