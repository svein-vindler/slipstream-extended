"""Generate actual offline CLI products for authenticated baseline-reader tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def exported_night(day: str, value: int, count: int, zone: str | None = "Europe/Oslo") -> dict:
    return {
        "date": day,
        "wake_date": day,
        "method_version": "night-signals-v1",
        "resolution": "native",
        "status": "available",
        "sleep_start_utc": f"{day}T00:00:00Z",
        "sleep_end_utc": f"{day}T01:00:00Z",
        "actual_night_timezone": zone,
        "user_context": None,
        "channels": {
            "hrv": {
                "status": "available",
                "unit": "ms",
                "source_revisions": [{"date": day, "revision": "synthetic-v1"}],
                "points": [
                    {"timestamp_utc": f"{day}T00:{i:02}:00Z", "value": value, "time_basis": "utc"}
                    for i in range(count)
                ],
            }
        },
    }


def build() -> dict:
    current = [
        exported_night(f"2026-10-{22 + i}", value, 1 + 2 * i)
        for i, value in enumerate([40, 60, 80, 100])
    ]
    for night in current:
        day = night["wake_date"]
        night["user_context"] = {
            "schema_version": 1,
            "source": "garmin" if day == "2026-10-24" else "user",
            "wake_date": day,
            "context_id": f"context-{day}",
            "illness": False,
            "travel": False,
            "note": "PRIVATE_NOTE_NOT_IN_PRODUCT",
        }
    previous = [
        exported_night(f"2026-10-{15 + i}", value, 1 + 2 * i)
        for i, value in enumerate([20, 30, 40, 50])
    ]
    fold = exported_night("2026-10-25", 40, 1)
    fold["sleep_end_utc"] = "2026-10-25T03:00:00Z"
    fold["channels"]["hrv"]["points"] = [
        {"timestamp_utc": "2026-10-25T00:30:00Z", "value": 40, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T01:30:00Z", "value": 80, "time_basis": "utc"},
    ]
    regular = exported_night("2026-10-26", 100, 1)
    regular["sleep_end_utc"] = "2026-10-26T03:00:00Z"
    regular["channels"]["hrv"]["points"][0]["timestamp_utc"] = "2026-10-26T01:30:00Z"
    clock = [exported_night("2026-10-24", 10, 1, zone=None), fold, regular]
    cases = [
        {"label": "current", "exports": current + previous, "end": "2026-10-25", "extra": []},
        {"label": "previous", "exports": current + previous, "end": "2026-10-18", "extra": []},
        {
            "label": "filtered",
            "exports": current,
            "end": "2026-10-25",
            "extra": [
                "--context-filter",
                '{"illness":false,"travel":false}',
                "--exclude-date",
                "2026-10-25",
            ],
        },
        {
            "label": "clock",
            "exports": clock,
            "end": "2026-10-26",
            "extra": ["--alignment", "clock"],
        },
        {
            "label": "wake",
            "exports": current,
            "end": "2026-10-25",
            "extra": ["--alignment", "wake"],
        },
    ]
    cases.append(
        {
            "label": "annual",
            "exports": [
                exported_night((date(2025, 10, 26) + timedelta(days=offset)).isoformat(), 50, 1)
                for offset in range(365)
            ],
            "end": "2026-10-25",
            "extra": ["--period", "365"],
        }
    )
    products = []
    with tempfile.TemporaryDirectory(
        prefix="night-products-", dir=os.environ.get("TMPDIR") or os.environ.get("TEMP")
    ) as folder:
        source = Path(folder) / "source.json"
        for case in cases:
            source.write_text(json.dumps({"nights": case["exports"]}), encoding="utf-8")
            command = [
                sys.executable,
                "-m",
                "pipeline.night_products",
                "baseline",
                "--input",
                str(source),
                "--end-date",
                case["end"],
                "--period",
                "7",
                "--alignment",
                "sleep_start",
                "--bin-minutes",
                "15",
                "--channel",
                "hrv",
                *case["extra"],
            ]
            result = subprocess.run(
                command,
                cwd=ROOT,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError("Synthetic baseline CLI failed")
            products.append({"label": case["label"], **json.loads(result.stdout)})
    return {
        "products": products,
        "expected": {
            "current": {"p10": 46, "p25": 55, "p50": 70, "p75": 85, "p90": 94},
            "previous": {"p10": 23, "p25": 27.5, "p50": 35, "p75": 42.5, "p90": 47},
            "clock": {"p10": 64, "p25": 70, "p50": 80, "p75": 90, "p90": 96},
        },
    }


def main() -> None:
    output = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else ROOT / "worker/test-runtime/generated/night-products-contract.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(build(), separators=(",", ":"), allow_nan=False) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
