"""Shared per-day health import and successful source-check receipts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from .granular import gzip_json, json_bytes, normalize_hrv, sha256
from .health_detail import normalize_body_composition, normalize_sleep_detail
from .health_history_index import _number, _timestamp_seconds, sync_dates

ROOTS = {"sleep": "health/sleep/v1", "hrv": "health/hrv",
         "body_composition": "health/body-composition/v1"}
NIGHT_STREAMS = ("sleep", "hrv")


class HealthNotReady(ValueError):
    def __init__(self, status: str = "garmin_not_ready"):
        super().__init__(status)
        self.status = status


def _validate_body_dates(item: dict[str, Any], day: str) -> None:
    for key in ("calendarDate", "date", "summaryDate"):
        value = item.get(key)
        if value is None:
            continue
        if key == "date" and _number(value) is not None:
            # Garmin dayview rows also carry an epoch-valued `date`. It is a
            # timestamp, not an ISO calendar declaration; never substitute its
            # UTC date for the dayview's Garmin-local calendar date.
            timestamp = _timestamp_seconds(value)
            try:
                if timestamp is None or timestamp <= 0:
                    raise ValueError("Invalid epoch date")
                datetime.fromtimestamp(timestamp, timezone.utc)
            except (ValueError, OverflowError, OSError) as exc:
                raise HealthNotReady("invalid_response") from exc
        elif str(value)[:10] != day:
            raise HealthNotReady("wrong_date")


def _body_day(day: str, raw: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete individual dayview before replacing a good day."""
    rows = raw.get("dateWeightList")
    if not isinstance(rows, list):
        raise HealthNotReady("invalid_response")
    if not rows:
        raise HealthNotReady()
    items = []
    for row in rows:
        if not isinstance(row, dict):
            raise HealthNotReady("invalid_response")
        _validate_body_dates(row, day)
        if "allWeightMetrics" in row:
            metrics = row["allWeightMetrics"]
            if not isinstance(metrics, list) or not metrics:
                raise HealthNotReady("invalid_response")
            if any(not isinstance(item, dict) for item in metrics):
                raise HealthNotReady("invalid_response")
            for item in metrics:
                _validate_body_dates(item, day)
                items.append({"calendarDate": day, **item})
        elif "latestWeight" in row or "totalAverage" in row:
            raise HealthNotReady("invalid_response")
        else:
            items.append({"calendarDate": day, **row})
    if any(not any((weight := _number(item.get(key))) is not None and weight > 0
                   for key in ("weight", "value")) for item in items):
        # An aggregate max/min/latest value is not an individual dayview sample.
        raise HealthNotReady()
    try:
        payload = normalize_body_composition(day, {"dateWeightList": items})
    except ValueError as exc:
        raise HealthNotReady() from exc
    if len(payload["measurements"]) != len(items) or any(
        item["is_daily_average"]
        or (weight := _number(item["weight_kg"])) is None or weight <= 0
        or not any((timestamp := _timestamp_seconds(item[key])) is not None and timestamp > 0
                   for key in ("timestamp_gmt", "timestamp_local"))
        for item in payload["measurements"]
    ):
        raise HealthNotReady()
    for item in payload["measurements"]:
        for key in ("timestamp_gmt", "timestamp_local"):
            value = item[key]
            if value is None or value == "":
                continue
            timestamp = _timestamp_seconds(value)
            if timestamp is None or timestamp <= 0:
                raise HealthNotReady("invalid_response")
            if isinstance(value, str) and _number(value) is None and "T" not in value and " " not in value:
                raise HealthNotReady("invalid_response")
            try:
                instant = datetime.fromtimestamp(timestamp, timezone.utc)
                local_day = (datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
                             if isinstance(value, str) and _number(value) is None else instant.date().isoformat())
            except (ValueError, OverflowError, OSError) as exc:
                raise HealthNotReady("invalid_response") from exc
            if key == "timestamp_local" and local_day != day:
                raise HealthNotReady("wrong_date")
    # Provider ordering is not a source edit. Preserve each measurement's fields
    # and clocks, including duplicates, while producing a stable day representation.
    payload["measurements"].sort(key=json_bytes)
    return payload


def normalize_day(stream: str, day: str, raw: Any) -> dict[str, Any]:
    if stream not in ROOTS or date.fromisoformat(day).isoformat() != day:
        raise ValueError("Invalid health stream or calendar date")
    if not isinstance(raw, dict):
        raise HealthNotReady("invalid_response")
    if stream == "body_composition":
        if raw.get("calendarDate") is not None and str(raw["calendarDate"])[:10] != day:
            raise HealthNotReady("wrong_date")
        return _body_day(day, raw)
    dto = raw.get("dailySleepDTO" if stream == "sleep" else "hrvSummary", {})
    if isinstance(dto, dict) and dto.get("calendarDate") and dto["calendarDate"] != day:
        raise HealthNotReady("wrong_date")
    try:
        payload = normalize_sleep_detail(day, raw) if stream == "sleep" else normalize_hrv(day, raw)
    except ValueError as exc:
        raise HealthNotReady() from exc
    if stream == "sleep":
        duration = _number(payload["summary"].get("sleep_seconds"))
        start = _timestamp_seconds(payload.get("sleep_start_gmt"))
        end = _timestamp_seconds(payload.get("sleep_end_gmt"))
        valid_stage = start is not None and end is not None and any(
            item.get("stage") is not None
            and (stage_start := _timestamp_seconds(item.get("start_gmt"))) is not None
            and (stage_end := _timestamp_seconds(item.get("end_gmt"))) is not None
            and start <= stage_start < stage_end <= end
            for item in payload["stages"]
        )
        usable = (duration is not None and duration > 0 and payload.get("confirmed") is not False
                  and start is not None and end is not None and end > start and valid_stage)
    else:
        readings = payload["readings"]
        usable = bool(readings) and all(
            (timestamp := _timestamp_seconds(item.get("timestamp"))) is not None and timestamp > 0
            and (value := _number(item.get("hrv_ms"))) is not None and value > 0
            for item in readings
        )
    if not usable:
        raise HealthNotReady()
    return payload


def store_day(store, stream: str, day: str, raw: Any) -> dict[str, Any]:
    payload = normalize_day(stream, day, raw)
    data = gzip_json(payload)
    key = f"{ROOTS[stream]}/{day[:4]}/{day[5:7]}/{day}.json"
    write = getattr(store, "put_if_changed", None)
    if callable(write):
        written = bool(write(key, data, "application/json", encoding="gzip"))
    else:
        store.put(key, data, "application/json", encoding="gzip")
        written = True
    return {"date": day, "source_key": key, "source_sha256": sha256(data),
            "checked_at": datetime.now(timezone.utc).isoformat(), "written": written}


def finalize_days(store, stream: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Repair indexes even on reuse; checkpoint only after storage/index success."""
    index = {"months_written": [], "months_unchanged": []}
    if stream not in ROOTS:
        raise ValueError("Invalid health stream")
    if records and stream in NIGHT_STREAMS and hasattr(store, "list_object_revisions"):
        index = sync_dates(store, stream, (item["date"] for item in records))
    elif records and stream == "body_composition" and hasattr(store, "list_object_revisions"):
        from .weight_index import sync_dates as sync_weight_dates
        index = sync_weight_dates(store, (item["date"] for item in records))
    for record in records:
        day = record["date"]
        store.put(f"refresh/checks/v1/health/{stream}/{day}.json", json_bytes({
            "schema_version": 1, "kind": "health-source-check", "stream": stream,
            "scope": f"health/{stream}/{day}", "status": "ready",
            **{key: record[key] for key in ("date", "checked_at", "source_key", "source_sha256")},
        }), "application/json")
    return index


def publish_completed_nights(store, records: dict[str, list[dict[str, Any]]]) -> int:
    """Existing Worker night freshness advances only for two checked components."""
    sleep = {item["date"]: item for item in records.get("sleep", [])}
    hrv = {item["date"]: item for item in records.get("hrv", [])}
    for day in sorted(sleep.keys() & hrv.keys()):
        checked_at = min(sleep[day]["checked_at"], hrv[day]["checked_at"])
        store.put(f"refresh/checks/v1/night/{day}.json", json_bytes({
            "schema_version": 1, "kind": "latest-night", "wake_date": day,
            "scope": f"night/{day}", "status": "stored", "source_checked": True,
            "sleep_status": "stored", "hrv_status": "stored", "checked_at": checked_at,
        }), "application/json")
    return len(sleep.keys() & hrv.keys())
