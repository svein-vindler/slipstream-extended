"""Serialize activities into the data/activities.csv the MCP worker reads."""

from __future__ import annotations

import csv
from pathlib import Path

from .schema import Activity, activity_local_date, activity_metric, activity_text

_HEADERS = [
    "Activity ID", "Activity Date", "Activity Name", "Activity Type", "Raw Sport",
    "Distance", "Filename", "Moving Time", "Elapsed Time", "Max Heart Rate",
    "Average Heart Rate", "Elevation Gain", "Average Speed", "Average Watts",
    "Calories", "Source", "Activity Local Date",
]


def _num(v, ndigits):
    v = activity_metric(v)
    if v is None:
        return ""
    r = round(float(v), ndigits)
    return int(r) if r == int(r) else r


def _avg_speed_ms(a: Activity) -> float | None:
    distance, moving = activity_metric(a.distance_km), activity_metric(a.moving_s)
    if distance is not None and moving is not None and moving > 0:
        return (distance * 1000.0) / moving
    return None


def _row(a: Activity) -> list:
    return [
        f"{a.source}-{a.source_id}" if a.source_id else "",
        a.start.strftime("%Y-%m-%d %H:%M:%S") if a.start else "",
        activity_text(a.name) or "",
        (activity_text(a.sport) or "other").title(),
        activity_text(a.raw_sport) or "",
        _num(a.distance_km, 3),
        activity_text(a.track_file) or "",
        _num(a.moving_s, 0),
        _num(a.elapsed_s, 0),
        _num(a.max_hr, 0),
        _num(a.avg_hr, 1),
        _num(a.elevation_gain_m, 1),
        _num(_avg_speed_ms(a), 3),
        _num(a.avg_watts, 1),
        _num(a.calories, 0),
        a.source,
        activity_local_date(a.local_start_date) or "",
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


def _stored_metric(value):
    try:
        return activity_metric(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def write_dataset(
    activities: list[Activity], data_dir: str, *, preserve_existing: bool = False
) -> dict:
    data = Path(data_dir)
    data.mkdir(parents=True, exist_ok=True)
    (data / "tracks").mkdir(exist_ok=True)

    csv_path = data / "activities.csv"
    existing = _existing_rows(csv_path) if preserve_existing else {}
    fetched = [_row(a) for a in activities if a.start is not None]

    # Missing fields in a legitimate partial response are not deletions.
    # Retained values remain previous observations, not fresh measurements.
    merged = dict(existing)
    for activity, row in zip((a for a in activities if a.start is not None), fetched, strict=True):
        activity_id = str(row[0])
        if activity_id in merged:
            old = list(merged[activity_id])
            old[16] = activity_local_date(old[16]) or ""
            for index in (5, 7, 8, 9, 10, 11, 12, 13, 14):
                if _stored_metric(old[index]) is None:
                    old[index] = ""
            for index, value in enumerate(row):
                if value == "":
                    row[index] = old[index]
            if row[3] == "Other" and not activity_text(activity.raw_sport):
                row[3] = old[3]
            if activity.moving_s_is_elapsed_fallback and old[7] != "":
                row[7] = old[7]
            # Speed derives from the merged distance and moving time. Retaining
            # an old derived value after either input changes would be stale.
            distance, moving = _stored_metric(row[5]), _stored_metric(row[7])
            if distance is None or moving is None or moving == 0:
                row[12] = ""
            elif (distance, moving) == (_stored_metric(old[5]), _stored_metric(old[7])) and old[12] != "":
                row[12] = old[12]
            else:
                row[12] = _num(distance * 1000 / moving, 3)
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
