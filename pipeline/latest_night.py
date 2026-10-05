"""Fetch sleep and HRV for exactly one Garmin wake-date, preserving local clocks."""

from __future__ import annotations

import argparse

from .activity_backfill import is_job_stopping_error
from .freshness import publish, receipt, recent_day
from .health_sync import HealthNotReady, finalize_days, store_day
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login


def run(*, run_id: str, wake_date: str, request_id: str | None = None,
        store=None, garmin=None) -> dict:
    recent_day(wake_date)
    store = store or R2Store()
    scope = f"night/{wake_date}"
    receipt(store, run_id, request_id, scope)
    garmin = garmin or _login()
    report = {"schema_version": 1, "kind": "latest-night", "wake_date": wake_date,
              "status": "pending", "sleep_status": "not_checked", "hrv_status": "not_checked"}
    source_checked = True
    for stream in ("sleep", "hrv"):
        try:
            raw = (garmin.get_sleep_data(wake_date) if stream == "sleep"
                   else garmin.get_hrv_data(wake_date))
            try:
                record = store_day(store, stream, wake_date, raw)
            except HealthNotReady as exc:
                report[f"{stream}_status"] = exc.status
                if exc.status != "garmin_not_ready":
                    source_checked = False
                continue
            finalize_days(store, stream, [record])
            report[f"{stream}_status"] = "stored"
            report[f"{stream}_reused"] = not record["written"]
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError("Garmin service or authentication error") from exc
            # Partial/failed calls do not advance a successful scope checkpoint.
            source_checked = False
            report[f"{stream}_status"] = "import_error"
    report["status"] = "stored" if all(report[f"{s}_status"] == "stored"
                                          for s in ("sleep", "hrv")) else "pending"
    return publish(store, run_id, scope, report, source_checked=source_checked)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import one recent Garmin night")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--wake-date", required=True)
    parser.add_argument("--request-id")
    args = parser.parse_args()
    run(run_id=args.run_id, wake_date=args.wake_date, request_id=args.request_id)


if __name__ == "__main__":
    main()
