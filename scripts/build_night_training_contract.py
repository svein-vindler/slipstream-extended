"""Actual public offline CLI products for synthetic authenticated training-reader tests."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEADERS = [
    "Activity ID",
    "Activity Date",
    "Activity Type",
    "Elapsed Time",
    "Moving Time",
    "Average Heart Rate",
    "Source",
    "Activity Name",
]


def night(day, start, end, points, zone=None):
    return {
        "date": day,
        "wake_date": day,
        "method_version": "night-signals-v1",
        "resolution": "native",
        "status": "available",
        "sleep_start_utc": start,
        "sleep_end_utc": end,
        "actual_night_timezone": zone,
        "view": {"context_status": "not_stored"},
        "user_context": None,
        "channels": {
            "hrv": {
                "status": "available",
                "unit": "ms",
                "source_revisions": [{"date": day, "revision": "synthetic-v1"}],
                "points": [
                    {"timestamp_utc": stamp, "value": value, "time_basis": "utc"}
                    for stamp, value in points
                ],
            }
        },
    }


def build():
    initial = [
        night(
            "2026-10-25",
            "2026-10-24T21:58:00Z",
            "2026-10-25T06:48:00Z",
            [("2026-10-25T00:30:00Z", 40), ("2026-10-25T01:30:00Z", 80)],
            "Europe/Oslo",
        ),
        night(
            "2026-10-26",
            "2026-10-25T14:30:00Z",
            "2026-10-25T22:00:00Z",
            [("2026-10-25T15:00:00Z", 70)],
            "Asia/Tokyo",
        ),
    ]
    initial[0]["view"] = {"context_status": "available"}
    initial[0]["user_context"] = {
        "wake_date": "2026-10-25",
        "source": "user",
        "context_id": "synthetic-context-v1",
        "alcohol": False,
        "sleep_quality": 4,
        "declared_fields": ["alcohol", "sleep_quality"],
        "note": "PRIVATE_NOTE_NEVER_PROJECT",
    }
    rows = [
        [
            "garmin-1",
            "2026-10-24 20:00:00",
            "Run",
            3600,
            3000,
            140,
            "garmin",
            "PRIVATE_ACTIVITY_NAME",
        ],
        ["garmin-2", "2026-10-24 23:30:00", "Walk", 1800, 1800, 90, "garmin", ""],
        ["garmin-3", "2026-10-25T23:00:00+09:00", "Ride", 900, 800, 130, "garmin", ""],
        ["garmin-4", "2026-10-24 19:00:00", "Run", "", 1800, "", "garmin", ""],
    ]
    first = date(2026, 8, 1)
    annual = [
        night(
            (first + timedelta(days=index)).isoformat(),
            (first + timedelta(days=index)).isoformat() + "T00:00:00Z",
            (first + timedelta(days=index)).isoformat() + "T01:00:00Z",
            [((first + timedelta(days=index)).isoformat() + "T00:30:00Z", 50 + index)],
        )
        for index in range(40)
    ]
    floats = night(
        "2026-10-25",
        "2026-10-25T00:00:00Z",
        "2026-10-25T01:00:00Z",
        [("2026-10-25T00:10:00Z", 0.0000001), ("2026-10-25T00:20:00Z", 1.2345675)],
    )
    floats["channels"]["body_battery"] = {
        "status": "available",
        "unit": "points",
        "source_revisions": [],
        "points": [{"timestamp_utc": "2026-10-25T00:30:00Z", "value": -0.0, "time_basis": "utc"}],
    }
    cases = [
        ("basic", initial, rows, "2026-10-25", "2026-10-26"),
        ("missing_activities", initial, None, "2026-10-25", "2026-10-26"),
        ("empty_activities", initial, [], "2026-10-25", "2026-10-26"),
        ("pages", annual, [], "2026-08-01", "2026-09-09"),
        ("floats", [floats], [], "2026-10-25", "2026-10-25"),
    ]
    for label, filename in [
        ("source_ambiguous", "night-training-source-quality.json"),
        ("source_empty", "night-training-source-empty.json"),
    ]:
        captured = json.loads((ROOT / "scripts/fixtures" / filename).read_text(encoding="utf-8"))
        cases.append((label, [captured], [], "2026-10-25", "2026-10-25"))
    association = [
        night("2026-10-25", "2026-10-24T22:00:00Z", "2026-10-25T06:00:00Z", []),
        night("2026-10-26", "2026-10-25T22:00:00Z", "2026-10-26T06:00:00Z", []),
    ]
    before = [["garmin-before", "2026-10-24 20:00:00", "Run", 3600, 3600, 140, "garmin", ""]]
    overlap = [["garmin-overlap", "2026-10-25 05:30:00", "Run", 3600, 3600, 140, "garmin", ""]]
    cases += [
        ("association_full", association, before, "2026-10-25", "2026-10-26"),
        ("association_narrow", association, before, "2026-10-26", "2026-10-26"),
        ("association_overlap", association, overlap, "2026-10-26", "2026-10-26"),
    ]
    unavailable = [dict(association[0], status="read_error"), association[1]]
    cases.append(("unavailable_window_narrow", unavailable, before, "2026-10-26", "2026-10-26"))
    overlapping_windows = [
        night("2026-10-25", "2026-10-25T21:30:00Z", "2026-10-26T05:00:00Z", []),
        association[1],
    ]
    cases.append(
        ("overlapping_windows_narrow", overlapping_windows, [], "2026-10-26", "2026-10-26")
    )
    dense_start = date(2026, 10, 1)
    dense_nights = []
    dense_rows = []
    for offset in range(8):
        day = (dense_start + timedelta(days=offset)).isoformat()
        previous = (dense_start + timedelta(days=offset - 1)).isoformat()
        dense_nights.append(
            night(day, day + "T00:00:00Z", day + "T01:00:00Z", [(day + "T00:30:00Z", 50)])
        )
        for index in range(100):
            dense_rows.append(
                [
                    (f"garmin-dense-{offset}-{index}").ljust(64, "a"),
                    previous + " 20:00:00",
                    "SyntheticType".ljust(64, "x"),
                    3600,
                    3500,
                    140,
                    "synthetic".ljust(32, "x"),
                    "",
                ]
            )
    cases.append(("dense", dense_nights, dense_rows, "2026-10-01", "2026-10-08"))
    output = {}
    with tempfile.TemporaryDirectory(prefix="night-training-contract-") as directory:
        root = Path(directory)
        for label, nights, activities, start, end in cases:
            exported = root / f"{label}.json"
            if label in {"source_ambiguous", "source_empty"}:
                filename = (
                    "night-training-source-quality.json"
                    if label == "source_ambiguous"
                    else "night-training-source-empty.json"
                )
                exported.write_bytes((ROOT / "scripts/fixtures" / filename).read_bytes())
            else:
                exported.write_text(json.dumps({"nights": nights}), encoding="utf-8")
            command = [
                sys.executable,
                "-m",
                "pipeline.night_training",
                "build",
                "--input",
                str(exported),
                "--start-date",
                start,
                "--end-date",
                end,
            ]
            if activities is not None:
                stored = root / f"{label}.csv"
                with stored.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(HEADERS)
                    writer.writerows(activities)
                command += ["--activities", str(stored)]
            result = subprocess.run(
                command,
                cwd=ROOT,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1"},
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError("Synthetic public training CLI fixture failed")
            output[label] = json.loads(result.stdout)
            if label in {"source_ambiguous", "source_empty"}:
                output[label]["source_export"] = nights[0]
                output[label]["source_export_sha256"] = hashlib.sha256(
                    exported.read_bytes()
                ).hexdigest()
            output[label]["product_json"] = json.dumps(
                output[label]["product"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
    return output


if __name__ == "__main__":
    destination = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else ROOT / "worker/test-runtime/generated/night-training-contract.json"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(build(), sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
