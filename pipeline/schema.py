"""Common activity model + sport-name normalization."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from math import isfinite

# Map Garmin's many sport keys onto a tidy family for display/filtering.
_SPORT_FAMILY = {
    "running": "run", "trail_running": "run", "treadmill_running": "run",
    "track_running": "run", "virtual_run": "run", "indoor_running": "run",
    "cycling": "ride", "road_biking": "ride", "mountain_biking": "ride",
    "gravel_cycling": "ride", "indoor_cycling": "ride", "virtual_ride": "ride",
    "cyclocross": "ride", "emountain_biking": "ride", "commuting": "ride",
    "lap_swimming": "swim", "open_water_swimming": "swim", "swimming": "swim",
    "walking": "walk", "casual_walking": "walk", "speed_walking": "walk",
    "hiking": "hike",
    "rowing": "row", "indoor_rowing": "row",
    "strength_training": "strength",
    "yoga": "yoga", "pilates": "yoga", "breathwork": "yoga", "meditation": "yoga",
    "indoor_cardio": "workout", "cardio": "workout", "hiit": "workout",
    "elliptical": "workout", "stair_climbing": "workout",
    "bouldering": "climb", "rock_climbing": "climb", "indoor_climbing": "climb",
}
_KNOWN = {"run", "ride", "swim", "walk", "hike", "row", "strength", "workout", "yoga", "climb"}


def canonical_sport(raw_type: str | None) -> str:
    if not raw_type:
        return "other"
    key = str(raw_type).strip().lower().replace(" ", "_").replace("-", "_")
    if key in _SPORT_FAMILY:
        return _SPORT_FAMILY[key]
    return key if key in _KNOWN else "other"


def to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def activity_metric(value) -> float | None:
    """Summary metrics accept finite nonnegative numbers, including zero."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if isfinite(number) and number >= 0 else None


def activity_text(value) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


_LOCAL_CLOCK = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[ T](?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})"
    r"(?::(?P<second>[0-9]{2})(?:\.[0-9]{1,9})?)?(?:Z|[+-](?P<zone_hour>[0-9]{2}):(?P<zone_minute>[0-9]{2}))?)?"
)


def activity_local_date(value) -> str | None:
    """Validate the provider's local calendar, without converting it to UTC."""
    if not isinstance(value, str) or len(value) > 64:
        return None
    match = _LOCAL_CLOCK.fullmatch(value)
    if not match or any(int(match[field] or 0) > ceiling for field, ceiling in (
        ("hour", 23), ("minute", 59), ("second", 59), ("zone_hour", 23), ("zone_minute", 59),
    )):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def activity_calendar_years(day: str) -> list[str]:
    """UTC/local clocks can differ by one calendar day, including travel."""
    parsed = date.fromisoformat(day)
    years = []
    for offset in (0, 1, -1):
        try:
            year = f"{(parsed + timedelta(days=offset)).year:04d}"
        except OverflowError:
            continue
        if year not in years:
            years.append(year)
    return years


@dataclass
class Activity:
    source: str
    source_id: str
    start: datetime | None
    sport: str
    raw_sport: str | None = None
    name: str | None = None
    distance_km: float | None = None
    moving_s: int | None = None
    elapsed_s: int | None = None
    elevation_gain_m: float | None = None
    avg_hr: float | None = None
    max_hr: float | None = None
    avg_watts: float | None = None
    calories: float | None = None
    track_file: str | None = None
    local_start_date: str | None = None
    local_start_time: str | None = None
    moving_s_is_elapsed_fallback: bool = False

    def __post_init__(self):
        self.start = to_utc(self.start)
        if not self.sport:
            self.sport = canonical_sport(self.raw_sport)
