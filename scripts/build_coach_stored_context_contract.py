"""Synthetic stored context through the frozen real FIT/export/Coach producers."""
from __future__ import annotations

import base64
import io
import json
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

# An explicit frozen tree for local development; ordinary CI uses its own source.
PRODUCER = Path(sys.argv[1]).resolve() if len(sys.argv) > 2 else Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PRODUCER))

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular import sha256  # noqa: E402
from pipeline.granular_export import export_activity  # noqa: E402
from pipeline.health_sync import normalize_day  # noqa: E402
from pipeline.training_day import day_key, merge_day, normalize  # noqa: E402
from scripts import build_running_segments_contract as running  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402

DAY = "2026-10-19"
CHECKED = "2026-10-19T08:00:00Z"
running.START = datetime(2026, 10, 19, 7, tzinfo=timezone.utc)
DTO = {**running.DTO, "startTimeGMT": "2026-10-19 07:00:00", "startTimeLocal": "2026-10-19 09:00:00"}
ACTIVITY = {**running.ACTIVITY, "date": DAY}


def build_case(mode="complete"):
    store = CountingStore()
    store.objects = {
        "summary/activities.csv": (
            b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
            b"garmin-42,2026-10-19 07:00:00,Synthetic run,Run,40,2026-10-19\n"),
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-context", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode(),
    }
    config = {"status": "enabled", "timezone": "Europe/Oslo", "device_id": None, "date": DAY}
    row = {
        "deviceId": "90000042", "calendarDate": DAY,
        "timestamp": "2026-10-19T06:00:00Z", "timestampLocal": "2026-10-19T08:00:00",
        "inputContext": "AFTER_WAKEUP_RESET", "score": 80, "recoveryTime": 0,
    }
    if mode == "after":
        row.update(timestamp="2026-10-19T08:00:00Z", timestampLocal="2026-10-19T10:00:00")
    if mode in {"time_unknown", "retained_unknown"}:
        row.update(timestamp=None, timestampLocal=None)
    if mode == "zero":
        row["score"] = 1
    if mode == "invalid_measurement":
        row["score"] = 0
    rows = [row]
    if mode == "ambiguous":
        rows = [{**row, "recoveryTime": None}, {**row, "deviceId": "90000043", "score": None, "recoveryTime": 30}]
    if mode == "morning":
        rows.append({**row, "timestamp": "2026-10-19T08:00:00Z", "timestampLocal": "2026-10-19T10:00:00",
                     "inputContext": None, "score": 30, "recoveryTime": 50})
    devices, status = normalize(rows, config, CHECKED)
    day = merge_day(None, devices, status, config, CHECKED)
    if mode in {"retained", "retained_unknown"}:
        day = merge_day(day, [], "auth_required", config, "2026-10-19T09:00:00Z")
    if mode == "source_error":
        day = merge_day(None, [], "source_error", config, CHECKED)
    if mode != "sparse":
        store.objects[day_key(DAY)] = json.dumps(day).encode()
    payloads = {
        "sleep": {"dailySleepDTO": {
            "calendarDate": DAY, "sleepTimeSeconds": 25200, "sleepScore": 82,
            "sleepStartTimestampGMT": "2026-10-18T22:00:00Z", "sleepEndTimestampGMT": "2026-10-19T05:00:00Z",
            "sleepStartTimestampLocal": "2026-10-19T00:00:00", "sleepEndTimestampLocal": "2026-10-19T07:00:00",
            "sleepWindowConfirmed": True,
        }, "sleepLevels": [{"startGMT": "2026-10-18T22:00:00Z", "endGMT": "2026-10-19T05:00:00Z", "activityLevel": 1}]},
        "hrv": {
            "sleepStartTimestampGMT": "2026-10-18T22:00:00Z", "sleepEndTimestampGMT": "2026-10-19T05:00:00Z",
            "sleepStartTimestampLocal": "2026-10-19T00:00:00", "sleepEndTimestampLocal": "2026-10-19T07:00:00",
            "hrvSummary": {"calendarDate": DAY, "lastNightAvg": 50, "weeklyAvg": 48},
            "hrvReadings": [{"readingTimeGMT": "2026-10-18T23:00:00Z", "hrvValue": 40,
                             "providerOnly": "synthetic-private-raw"}],
        },
    }
    if mode == "after":
        payloads["sleep"]["dailySleepDTO"].update(sleepEndTimestampGMT="2026-10-19T08:00:00Z",
                                                  sleepEndTimestampLocal="2026-10-19T10:00:00")
        payloads["hrv"].update(sleepEndTimestampGMT="2026-10-19T08:00:00Z", sleepEndTimestampLocal="2026-10-19T10:00:00")
    if mode in {"garmin_residual", "partial_local"}:
        payloads["sleep"]["dailySleepDTO"].update(sleepStartTimestampGMT="2026-10-18T22:00:20Z",
                                                  sleepEndTimestampGMT="2026-10-19T05:00:20Z")
        payloads["hrv"].update(sleepStartTimestampGMT="2026-10-18T22:00:20Z", sleepEndTimestampGMT="2026-10-19T05:00:20Z")
        payloads["sleep"]["sleepLevels"] = [{"startGMT": "2026-10-18T22:00:20Z",
                                               "endGMT": "2026-10-19T05:00:20Z", "activityLevel": 1}]
        if mode == "partial_local":
            payloads["sleep"]["dailySleepDTO"]["sleepEndTimestampLocal"] = None
            payloads["hrv"]["sleepEndTimestampLocal"] = None
    if mode == "zero":
        payloads["sleep"]["dailySleepDTO"]["sleepScore"] = 0
        payloads["hrv"]["hrvSummary"].update(lastNightAvg=0, weeklyAvg=0)
    if mode == "invalid_measurement":
        payloads["sleep"]["dailySleepDTO"]["sleepScore"] = 101
    for kind, root in (("sleep", "health/sleep/v1"), ("hrv", "health/hrv")):
        if mode == "sparse" or mode == "missing_hrv" and kind == "hrv":
            continue
        key = f"{root}/2026/10/{DAY}.json"
        normalized = normalize_day(kind, DAY, payloads[kind])
        if mode == "time_unknown":
            for name in ("sleep_start_gmt", "sleep_end_gmt", "sleep_start_garmin_local", "sleep_end_garmin_local"):
                normalized[name] = None
        if mode == "source_error":
            # An invalid optional stored schema is not missing data. The real
            # assembler/projection suppresses only this stream, not the workout.
            normalized["schema_version"] = 2
        body = json.dumps(normalized).encode()
        store.objects[key] = body
        store.objects[f"refresh/checks/v1/health/{kind}/{DAY}.json"] = json.dumps({
            "schema_version": 1, "kind": "health-source-check", "stream": kind,
            "scope": f"health/{kind}/{DAY}", "status": "ready", "date": DAY,
            "source_key": key, "source_sha256": sha256(body), "checked_at": CHECKED,
        }).encode()
    with TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
        export_activity(DTO, running.SyntheticGarmin(False), store, Path(temporary))
        result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    return {"analysis_id": result["processed_sources"]["42"],
            "objects_base64": {key: base64.b64encode(body).decode()
                               for key, body in sorted(store.objects.items())
                               if key.endswith((".json", ".csv"))}}


if __name__ == "__main__":
    target = Path(sys.argv[2] if len(sys.argv) > 2 else sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    cases = {name: build_case(name) for name in (
        "sparse", "missing_hrv", "after", "time_unknown", "ambiguous", "morning", "retained", "retained_unknown",
        "source_error", "garmin_residual", "partial_local", "zero", "invalid_measurement",
    )}
    target.write_text(json.dumps({**build_case(), "cases": cases}, separators=(",", ":")), encoding="utf-8")
