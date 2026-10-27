"""Reconcile a bounded recent overlap without touching historical backfill plans."""

from __future__ import annotations

from datetime import date, timedelta

from .activity_backfill import is_job_stopping_error
from .health_sync import (
    NIGHT_STREAMS,
    HealthNotReady,
    finalize_days,
    publish_completed_nights,
    store_day,
)
from .output_privacy import SafeArgumentParser, log_report, safe_cli
from .r2_store import R2Store
from .sources.garmin import _login

MAX_DAYS = 14


def run(*, days: int = 3, streams=NIGHT_STREAMS, store=None, garmin=None,
        today: date | None = None) -> dict:
    selected = tuple(dict.fromkeys(streams))
    if (not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= MAX_DAYS
            or not selected or any(stream not in NIGHT_STREAMS for stream in selected)):
        raise ValueError("Choose sleep/HRV and a 1-14 day overlap")
    store = store or R2Store()
    garmin = garmin or _login()
    from .sources.provider_capabilities import capabilities_for
    supported = capabilities_for(garmin)
    end = today or date.today()
    results = {}
    all_records = {}
    validation_rejections = []
    for stream in selected:
        if stream == "sleep" and not supported["sleep_stages"]:
            results[stream] = {"status": "unsupported", "days_checked": 0, "days_ready": 0,
                               "days_not_ready": 0, "source_errors": 0, "objects_written": 0,
                               "objects_unchanged": 0, "checkpoints_written": 0,
                               "months_written": 0, "months_unchanged": 0}
            all_records[stream] = []
            continue
        records = []
        failures = 0
        not_ready = 0
        for offset in range(days):
            day = (end - timedelta(days=offset)).isoformat()
            try:
                raw = (garmin.get_sleep_data(day) if stream == "sleep"
                       else garmin.get_hrv_data(day))
            except Exception as exc:
                if is_job_stopping_error(exc):
                    raise RuntimeError("Garmin service or authentication error") from exc
                failures += 1
                continue
            try:
                records.append(store_day(store, stream, day, raw))
            except HealthNotReady as exc:
                not_ready += 1
                if exc.status in ("invalid_response", "wrong_date", "garmin_not_ready"):
                    validation_rejections.append({"stream": stream, "date": day, "category": exc.status})
                if exc.status != "garmin_not_ready":
                    failures += 1
        # Storage/auth/budget/index failures propagate; failed scopes keep their
        # previous successful check. Other successfully checked dates may advance.
        index = finalize_days(store, stream, records)
        all_records[stream] = records
        results[stream] = {
            "days_checked": days, "days_ready": len(records), "days_not_ready": not_ready,
            "source_errors": failures, "objects_written": sum(item["written"] for item in records),
            "objects_unchanged": sum(not item["written"] for item in records),
            "checkpoints_written": len(records), "months_written": len(index["months_written"]),
            "months_unchanged": len(index["months_unchanged"]),
        }
    report = {"schema_version": 1, "kind": "recent-health-sync",
              "status": "complete" if all(item["days_ready"] == days for item in results.values()) else "partial",
              "night_checkpoints_written": publish_completed_nights(store, all_records),
              "streams": results, "health_validation_rejections": validation_rejections}
    log_report("recent_health", report)
    return report


@safe_cli
def main() -> None:
    parser = SafeArgumentParser(description="Reconcile recent sleep and HRV incrementally")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--stream", action="append", choices=NIGHT_STREAMS)
    args = parser.parse_args()
    try:
        run(days=args.days, streams=args.stream or NIGHT_STREAMS)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
