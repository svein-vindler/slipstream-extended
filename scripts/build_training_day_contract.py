"""Synthetic Python day contract for isolated Workers runtime tests; zero I/O clients."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.training_day import decode_day, encode_day, merge_day, normalize  # noqa: E402


def synthetic_day(day="2026-10-08", score=80, devices=1, distinct=False):
    config = {"status": "enabled", "date": day, "timezone": "Europe/Oslo", "device_id": None}
    local = datetime.fromisoformat(f"{day}T07:30:00")
    utc = local.replace(tzinfo=ZoneInfo(config["timezone"])).astimezone(timezone.utc).isoformat()
    rows = [{"deviceId": 900000 + index, "calendarDate": day, "score": score,
        "timestamp": utc, "timestampLocal": local.isoformat(),
        "inputContext": "AFTER_WAKEUP_RESET", "recoveryTime": 60,
        "recoveryTimeChangePhrase": "REACHED_ZERO", "userProfilePK": 123456,
        "feedbackLong": "synthetic text must never leave normalization"} for index in range(devices)]
    if distinct:
        later = local.replace(hour=18)
        later_utc = later.replace(tzinfo=ZoneInfo(config["timezone"])).astimezone(timezone.utc).isoformat()
        rows += [{**row, "score": 85, "timestamp": later_utc, "timestampLocal": later.isoformat(),
            "inputContext": "AFTER_ACTIVITY", "recoveryTime": 90, "recoveryTimeChangePhrase": None} for row in list(rows)]
    checked = f"{day}T{'19' if distinct else '07'}:00:00+00:00"
    normalized, status = normalize(rows, config, checked)
    value = merge_day(None, normalized, status, config, checked)
    encode_day(value)
    return value


def configured_day(status, retained):
    config = {"status": "enabled", "date": "2026-10-08", "timezone": "Europe/Oslo", "device_id": "900000"}
    previous = synthetic_day() if retained else None
    incoming = synthetic_day(devices=2)["devices"][1:] if status in {"partial", "available"} else []
    value = merge_day(previous, incoming, status, config, "2026-10-08T08:00:00+00:00")
    encode_day(value)
    return value


def field_cases():
    """Invented independent fields, round-tripped through the actual day codec."""
    config = {"status": "enabled", "date": "2026-10-08", "timezone": "Europe/Oslo", "device_id": None}
    row = {"deviceId": 900000, "calendarDate": config["date"], "score": 80,
        "recoveryTime": 60, "timestamp": "2026-10-08T05:30:00Z",
        "timestampLocal": "2026-10-08T07:30:00", "inputContext": "AFTER_WAKEUP_RESET"}
    def build(rows, *, previous=None, failure=None, later=False, device=None):
        cfg = {**config, "device_id": device}
        checked = f"2026-10-08T{'08' if later else '07'}:00:00+00:00"
        devices, status = ([], failure) if failure else normalize(rows, cfg, checked)
        return decode_day(encode_day(merge_day(previous, devices, status, cfg, checked)), cfg["date"])
    readiness = {key: value for key, value in row.items() if key != "recoveryTime"}
    recovery = {key: value for key, value in row.items() if key != "score"}
    both = build([row])
    updated = {**row, "timestamp": "2026-10-08T06:30:00Z", "timestampLocal": "2026-10-08T08:30:00"}
    complementary = [readiness, {**recovery, "deviceId": 900001}]
    return {"both": both, "readiness_only": build([readiness]), "recovery_only": build([recovery]),
        "recovery_zero": build([{**recovery, "recoveryTime": 0}]), "empty": build([]),
        "empty_retained": build([], previous=both, later=True),
        "invalid_readiness": build([{**row, "score": 0}]),
        "invalid_recovery": build([{**row, "recoveryTime": -1}]),
        "readiness_retained": build([{**updated, "score": None, "recoveryTime": 30}], previous=both, later=True),
        "recovery_retained": build([{**updated, "score": 90, "recoveryTime": None}], previous=both, later=True),
        "source_error_retained": build([], previous=both, failure="source_error", later=True),
        "multiple_complementary": build(complementary),
        "selected_readiness_only": build(complementary, device="900000"),
        "selected_recovery_only": build(complementary, device="900001")}


if __name__ == "__main__":
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    dates = [f"2026-10-{index:02}" for index in range(1, 32)]
    output.write_text(json.dumps({"single": synthetic_day(), "multiple": synthetic_day(devices=4), "field_cases": field_cases(),
        "days": {day: {"single": synthetic_day(day), "multiple": synthetic_day(day, devices=4),
            "dual": synthetic_day(day, distinct=True), "multiple_dual": synthetic_day(day, devices=4, distinct=True)} for day in dates},
        "configured": {f"{status}-{'retained' if retained else 'empty'}": configured_day(status, retained)
            for status in ("auth_required", "timeout", "source_error", "schema_unverified", "oversize", "wrong_date", "partial", "available", "no_observation")
            for retained in (False, True)}}, indent=2) + "\n", encoding="utf-8")
