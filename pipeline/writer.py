"""Serialize activities into the data/activities.csv the MCP worker reads."""

from __future__ import annotations

import csv
from pathlib import Path

from .schema import Activity

_HEADERS = [
    "Activity ID", "Activity Date", "Activity Name", "Activity Type", "Raw Sport",
    "Distance", "Filename", "Moving Time", "Elapsed Time", "Max Heart Rate",
    "Average Heart Rate", "Elevation Gain", "Average Speed", "Average Watts",
    "Calories", "Source",
]


def _num(v, ndigits):
    if v is None:
        return ""
    r = round(float(v), ndigits)
    return int(r) if r == int(r) else r


def _avg_speed_ms(a: Activity) -> float | None:
    if a.distance_km and a.moving_s:
        return (a.distance_km * 1000.0) / a.moving_s
    return None


def _row(a: Activity) -> list:
    return [
        f"{a.source}-{a.source_id}" if a.source_id else "",
        a.start.strftime("%Y-%m-%d %H:%M:%S") if a.start else "",
        a.name or "",
        (a.sport or "other").title(),
        a.raw_sport or "",
        _num(a.distance_km, 3),
        a.track_file or "",
        a.moving_s if a.moving_s is not None else "",
        a.elapsed_s if a.elapsed_s is not None else "",
        _num(a.max_hr, 0),
        _num(a.avg_hr, 1),
        _num(a.elevation_gain_m, 1),
        _num(_avg_speed_ms(a), 3),
        _num(a.avg_watts, 1),
        _num(a.calories, 0),
        a.source,
    ]


def _existing_rows(csv_path: Path) -> dict[str, list[str]]:
    if not csv_path.exists():
        return {}

    with csv_path.open(newline="", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        return {
            row["Activity ID"]: [row.get(header, "") for header in _HEADERS]
            for row in rows
            if row.get("Activity ID")
        }


def write_dataset(
    activities: list[Activity], data_dir: str, *, preserve_existing: bool = False
) -> dict:
    data = Path(data_dir)
    data.mkdir(parents=True, exist_ok=True)
    (data / "tracks").mkdir(exist_ok=True)

    csv_path = data / "activities.csv"
    existing = _existing_rows(csv_path) if preserve_existing else {}
    fetched = [_row(a) for a in activities if a.start is not None]

    # Keep the historical dataset, while letting a newly fetched activity
    # replace the older copy with the same source/id. If track downloads are
    # disabled for routine refreshes, retain any existing private GPX filename.
    merged = dict(existing)
    for row in fetched:
        activity_id = str(row[0])
        if activity_id in existing and not row[6]:
            row[6] = existing[activity_id][6]
        merged[activity_id] = row

    rows = sorted(merged.values(), key=lambda row: row[1], reverse=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(_HEADERS)
        w.writerows(rows)

    return {
        "activities_fetched": len(fetched),
        "activities_preserved": max(0, len(rows) - len(fetched)),
        "activities_written": len(rows),
        "csv": str(csv_path),
    }
