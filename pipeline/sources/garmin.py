"""Garmin Connect source adapter (via the python-garminconnect library).

Resumes from a serialized session token (GARMINTOKENS) so no password is ever
stored. Mint the token once with scripts/garmin_login.py.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ..schema import Activity, activity_metric, activity_text, canonical_sport
from .garmin_readonly import ReadOnlyGarmin


def _login():
    from garminconnect import Garmin

    tokens = os.environ.get("GARMINTOKENS")
    token_file = os.environ.get("GARMINTOKENS_FILE")
    if not tokens and token_file:
        tokens = Path(token_file).read_text(encoding="utf-8").strip()
    if tokens:
        g = Garmin()
        read_only = ReadOnlyGarmin(g)  # Install the policy before any API request.
        g.login(tokens)  # >512-char arg is treated as token data, not a path
        return read_only
    raise RuntimeError(
        "Garmin: no session token. Run scripts/garmin_login.py once to mint "
        "GARMINTOKENS, or set GARMINTOKENS_FILE to a local token file "
        "(handles MFA; no password stored)."
    )


def _parse_start(raw: dict) -> datetime | None:
    s = raw.get("startTimeGMT")  # "2024-08-18 13:00:00" in UTC
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _download_track(g, activity_id, data_dir: str) -> str | None:
    rel = f"tracks/garmin-{activity_id}.gpx"
    if Path(data_dir, rel).exists():
        return rel  # already have it - refreshes stay incremental
    try:
        from garminconnect import Garmin
        data = g.download_activity(activity_id, dl_fmt=Garmin.ActivityDownloadFormat.GPX)
        Path(data_dir, rel).write_bytes(data)
        return rel
    except Exception:
        return None


def fetch(
    days_back: int = 30,
    download_tracks: bool = False,
    data_dir: str = "data",
    *,
    client=None,
    end_date: date | None = None,
) -> list[Activity]:
    g = client or _login()
    requested_end = end_date or date.today()
    start = (requested_end - timedelta(days=days_back)).isoformat()
    raw_list = g.get_activities_by_date(start, requested_end.isoformat())

    out: list[Activity] = []
    for r in raw_list:
        aid = r.get("activityId")
        activity_type = r.get("activityType")
        type_key = activity_text(activity_type.get("typeKey")) if isinstance(activity_type, dict) else None
        dist_m = activity_metric(r.get("distance"))
        elapsed = activity_metric(r.get("duration"))
        moving = activity_metric(r.get("movingDuration"))
        moving_fallback = moving is None and elapsed is not None
        if moving is None:
            moving = elapsed
        power = activity_metric(r.get("avgPower"))
        if power is None:
            power = activity_metric(r.get("averagePower"))
        out.append(Activity(
            source="garmin",
            source_id=str(aid),
            start=_parse_start(r),
            sport=canonical_sport(type_key),
            raw_sport=type_key,
            name=activity_text(r.get("activityName")),
            distance_km=(dist_m / 1000.0) if dist_m is not None else None,
            moving_s=int(moving) if moving is not None else None,
            moving_s_is_elapsed_fallback=moving_fallback,
            elapsed_s=int(elapsed) if elapsed is not None else None,
            elevation_gain_m=activity_metric(r.get("elevationGain")),
            avg_hr=activity_metric(r.get("averageHR")),
            max_hr=activity_metric(r.get("maxHR")),
            avg_watts=power,
            calories=activity_metric(r.get("calories")),
            track_file=_download_track(g, aid, data_dir) if download_tracks else None,
            local_start_date=(
                str(r.get("startTimeLocal"))[:10]
                if isinstance(r.get("startTimeLocal"), str)
                and len(r["startTimeLocal"]) >= 10 else None
            ),
            local_start_time=(
                r.get("startTimeLocal")
                if isinstance(r.get("startTimeLocal"), str) else None
            ),
        ))
    return out
