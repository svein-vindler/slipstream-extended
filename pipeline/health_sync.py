"""Shared per-day sleep/HRV import and successful source-check receipts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from .granular import gzip_json, json_bytes, normalize_hrv, sha256
from .health_detail import normalize_sleep_detail
from .health_history_index import _number, _timestamp_seconds, sync_dates

ROOTS = {"sleep": "health/sleep/v1", "hrv": "health/hrv"}


class HealthNotReady(ValueError):
    def __init__(self, status: str = "garmin_not_ready"):
        super().__init__(status)
        self.status = status


def normalize_day(stream: str, day: str, raw: Any) -> dict[str, Any]:
    if stream not in ROOTS or date.fromisoformat(day).isoformat() != day:
        raise ValueError("Invalid health stream or calendar date")
    if not isinstance(raw, dict):
        raise HealthNotReady("invalid_response")
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
    if records and hasattr(store, "list_object_revisions"):
        index = sync_dates(store, stream, (item["date"] for item in records))
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
