"""Orchestrator: pull Garmin activities -> write data/.

Runs on a schedule and on-demand (GitHub Actions or locally):

  python -m pipeline.fetch --days 30
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta

from .health_writer import write_health_dataset
from .output_privacy import SafeArgumentParser, log_report, safe_cli
from .writer import write_dataset


def run(
    days_back: int = 30,
    data_dir: str = "data",
    download_tracks: bool = False,
    *,
    skip_activities: bool = False,
    health_days: int = 14,
    health_start: date | None = None,
    health_end: date | None = None,
    client=None,
) -> dict:
    from .sources import garmin, garmin_health

    summary: dict = {}
    if not skip_activities:
        activities = garmin.fetch(
            days_back=days_back, download_tracks=download_tracks, data_dir=data_dir, client=client
        )
        print(f"[garmin] fetched {len(activities)} activities", file=sys.stderr)
        summary.update(write_dataset(activities, data_dir, preserve_existing=True))

    end = health_end or date.today()
    start = health_start or (end - timedelta(days=max(1, health_days) - 1))
    health = garmin_health.fetch(start, end, client=client)
    print(f"[garmin-health] fetched {len(health)} populated days", file=sys.stderr)
    summary.update(write_health_dataset(health, data_dir, preserve_existing=True))
    log_report("fetch", summary)
    return summary


@safe_cli
def main():
    ap = SafeArgumentParser(description="Fetch Garmin activities into data/.")
    ap.add_argument("--days", type=int, default=int(os.environ.get("DAYS_BACK", "30")))
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "data"))
    ap.add_argument("--skip-activities", action="store_true")
    ap.add_argument("--health-days", type=int, default=int(os.environ.get("HEALTH_DAYS_BACK", "14")))
    ap.add_argument("--health-start", type=date.fromisoformat)
    ap.add_argument("--health-end", type=date.fromisoformat)
    ap.add_argument(
        "--download-tracks",
        action="store_true",
        help="Download missing GPX tracks (off by default for faster, safer refreshes).",
    )
    args = ap.parse_args()
    summary = run(
        days_back=args.days,
        data_dir=args.data_dir,
        download_tracks=args.download_tracks,
        skip_activities=args.skip_activities,
        health_days=args.health_days,
        health_start=args.health_start,
        health_end=args.health_end,
    )
    if not args.skip_activities and summary.get("activities_fetched") == 0:
        print("No activities fetched - check your Garmin token / date range.", file=sys.stderr)


if __name__ == "__main__":
    main()
