"""Merge daily Garmin health summaries into data/health_daily.csv."""

from __future__ import annotations

import csv
from pathlib import Path

HEALTH_HEADERS = [
    "Date", "Sleep Seconds", "Deep Sleep Seconds", "Light Sleep Seconds",
    "REM Sleep Seconds", "Awake Sleep Seconds", "Sleep Score",
    "HRV Weekly Average", "HRV Last Night Average", "HRV Status",
    "Resting Heart Rate", "Minimum Heart Rate", "Maximum Heart Rate",
    "Average Heart Rate", "Body Battery Highest", "Body Battery Lowest",
    "Body Battery Charged", "Body Battery Drained", "Average Stress",
    "Maximum Stress", "Stress Duration Seconds", "Steps",
    "Average Respiration", "Lowest Respiration", "Highest Respiration",
    "Weight KG", "Source",
]


def _existing_rows(csv_path: Path) -> dict[str, dict[str, str]]:
    if not csv_path.exists():
        return {}
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return {
            row["Date"]: {header: row.get(header, "") for header in HEALTH_HEADERS}
            for row in csv.DictReader(handle)
            if row.get("Date")
        }


def write_health_dataset(
    rows: list[dict[str, object]], data_dir: str, *, preserve_existing: bool = True
) -> dict[str, object]:
    """Write one row per date, retaining old values after partial responses."""
    data = Path(data_dir)
    data.mkdir(parents=True, exist_ok=True)
    csv_path = data / "health_daily.csv"
    existing = _existing_rows(csv_path) if preserve_existing else {}
    merged = {day: dict(row) for day, row in existing.items()}

    for incoming in rows:
        day = str(incoming.get("Date") or "")
        if not day:
            continue
        current = merged.get(day, {header: "" for header in HEALTH_HEADERS})
        for header in HEALTH_HEADERS:
            value = incoming.get(header, "")
            if value is not None and value != "":
                current[header] = str(value)
        current["Date"] = day
        current["Source"] = str(incoming.get("Source") or current.get("Source") or "garmin")
        merged[day] = current

    ordered = [merged[day] for day in sorted(merged, reverse=True)]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEALTH_HEADERS)
        writer.writeheader()
        writer.writerows(ordered)

    return {
        "health_days_fetched": len(rows),
        "health_days_written": len(ordered),
        "health_csv": str(csv_path),
    }
