"""Fetch sleep and HRV for exactly one Garmin wake-date, preserving local clocks."""

from __future__ import annotations

import argparse

from .activity_backfill import is_job_stopping_error
from .freshness import publish, receipt, recent_day
from .granular import gzip_json, normalize_hrv
from .health_detail import normalize_sleep_detail
from .health_history_index import sync_dates
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
            if not isinstance(raw, dict):
                raise ValueError("Invalid provider response")
            dto = raw.get("dailySleepDTO", {}) if stream == "sleep" else raw.get("hrvSummary", {})
            provider_date = dto.get("calendarDate") if isinstance(dto, dict) else None
            if provider_date and provider_date != wake_date:
                source_checked = False
                report[f"{stream}_status"] = "wrong_date"
                continue
            try:
                payload = (normalize_sleep_detail(wake_date, raw) if stream == "sleep"
                           else normalize_hrv(wake_date, raw))
            except ValueError:
                report[f"{stream}_status"] = "garmin_not_ready"
                continue
            duration = payload.get("summary", {}).get("sleep_seconds")
            has_data = (isinstance(duration, (int, float)) and not isinstance(duration, bool) and duration > 0 if stream == "sleep"
                        else payload.get("reading_count", 0) > 0)
            if not has_data:
                report[f"{stream}_status"] = "garmin_not_ready"
                continue
            root = "health/sleep/v1" if stream == "sleep" else "health/hrv"
            key = f"{root}/{wake_date[:4]}/{wake_date[5:7]}/{wake_date}.json"
            store.put(key, gzip_json(payload), "application/json", encoding="gzip")
            report[f"{stream}_status"] = "stored"
            if hasattr(store, "list_object_revisions"):
                sync_dates(store, stream, [wake_date])
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
