"""Recheck recent measured dates without scanning or mutating backfill plans."""
from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import date, timedelta
from pathlib import Path

from .activity_backfill import is_job_stopping_error
from .health_detail_backfill import history_dates
from .health_sync import HealthNotReady, finalize_days, store_day
from .r2_store import R2Store
from .sources.garmin import _login
from .summary_restore import decode_summary

MAX_DAYS = 14
STREAM = "body_composition"


def run(*, max_days: int = 3, health_csv: bytes | None = None, store=None, garmin=None,
        today: date | None = None) -> dict:
    if not isinstance(max_days, int) or isinstance(max_days, bool) or not 1 <= max_days <= MAX_DAYS:
        raise ValueError("Choose 1-14 recent measurement dates")
    store = store or R2Store()
    if health_csv is None:
        health_csv = store.get("summary/health_daily.csv")
    decoded = decode_summary(health_csv, "Date,")
    header = next(csv.reader(io.StringIO(decoded.decode("utf-8"))))
    if "Weight KG" not in header:
        raise ValueError("Health summary must include Weight KG")
    # Garmin's current local date may be one day ahead of the runner's clock.
    # Select the source date verbatim, as with targeted night imports.
    end = ((today or date.today()) + timedelta(days=1)).isoformat()
    # Retain the existing overlap in measurement dates, rather than assuming
    # every user weighs in daily. Explicit history work keeps its old entrypoints.
    selected = [day for day in history_dates(decoded, "Weight KG") if day <= end][:max_days]
    records = []
    not_ready = failures = 0
    if selected:
        garmin = garmin or _login()
    for day in selected:
        try:
            raw = garmin.get_daily_weigh_ins(day)
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError("Garmin service or authentication error") from exc
            failures += 1
            continue
        try:
            records.append(store_day(store, STREAM, day, raw))
        except HealthNotReady:
            not_ready += 1
    # No body index exists; canonical reads keep every actual measurement.
    # Storage/budget failures propagate and leave failed scope checks unchanged.
    finalize_days(store, STREAM, records)
    report = {"schema_version": 1, "kind": "recent-body-sync",
              "status": ("idle" if not selected else "complete" if len(records) == len(selected) else "partial"),
              "days_checked": len(selected), "days_ready": len(records), "days_not_ready": not_ready,
              "source_errors": failures, "objects_written": sum(item["written"] for item in records),
              "objects_unchanged": sum(not item["written"] for item in records),
              "checkpoints_written": len(records)}
    print(json.dumps(report))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile recent individual weigh-in dayviews")
    parser.add_argument("--max-days", type=int, default=3)
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    try:
        run(max_days=args.max_days, health_csv=args.summary.read_bytes() if args.summary else None)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
