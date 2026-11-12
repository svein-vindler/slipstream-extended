"""Fetch recent night sources and refresh references, preserving local clocks."""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

from .activity_backfill import is_job_stopping_error
from .freshness import publish, receipt, recent_day
from .health_sync import HealthNotReady, finalize_days, store_day
from .output_privacy import SafeArgumentParser, safe_cli
from .r2_store import R2BudgetError, R2Store, missing_object
from .sources.garmin import _login


def _checked_heart_rate(store, day, wake_date, instant, calendar):
    from .granular import sha256
    from .signal_series import MAX_STORED_BYTES, signal_key

    try:
        proof = json.loads(store.get_bounded(
            f"refresh/checks/v1/health/signals/{day}.json", 65536))
    except ClientError as exc:
        if not missing_object(exc):
            raise
        return None
    if (not isinstance(proof, dict) or proof.get("schema_version") != 1
            or proof.get("kind") != "health-source-check" or proof.get("status") != "ready"
            or proof.get("stream") != "signals" or proof.get("date") != day
            or proof.get("scope") != f"health/signals/{day}"
            or proof.get("source_key") != signal_key(day)
            or not isinstance(proof.get("source_sha256"), str)
            or re.fullmatch("[a-f0-9]{64}", proof["source_sha256"]) is None):
        raise ValueError("Stored HR source check is invalid")
    checked = datetime.fromisoformat(proof["checked_at"].replace("Z", "+00:00"))
    if checked.tzinfo is None or checked.utcoffset() is None or checked > instant:
        raise ValueError("Stored HR source-check time is invalid")
    if checked.astimezone(calendar).date().isoformat() < wake_date:
        return None
    try:
        snapshot = store.get_snapshot(signal_key(day), max_bytes=MAX_STORED_BYTES)
    except ClientError as exc:
        if not missing_object(exc):
            raise
        return None
    return proof if sha256(snapshot.data) == proof["source_sha256"] else None


def collect_heart_rate(store, garmin, wake_dates, *, timezone=None, reuse_checks=False,
                       now=None, login=None) -> dict:
    """Use the native reader's exact source calendars, including travel clocks."""
    from .native_night import NativeNightReader
    from .signal_series import DailySignalCollector

    reader = NativeNightReader(store, timezone=timezone)
    days = sorted({source_day for wake in wake_dates
                   for source_day in reader.night(wake)["channels"]["heart_rate"]["source_dates"]})
    if len(days) > 7:
        raise ValueError("Recent night HR source calendar exceeds the bounded overlap")
    collector = DailySignalCollector(store)
    failures = []
    reused = []
    instant = now or datetime.now(UTC)
    calendar = ZoneInfo(timezone or "UTC")
    for day in days:
        if reuse_checks:
            proof = _checked_heart_rate(store, day, max(wake_dates), instant, calendar)
            if proof is not None:
                reused.append(proof)
                continue
        try:
            garmin = garmin or (login or _login)()
            raw = garmin.get_heart_rates(day)
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError("Garmin service or authentication error") from exc
            failures.append({"date": day, "status": "import_error"})
            continue
        collector.collect(day, "heart_rate", raw)
    result = collector.publish(source_checks=True, refresh_references=False)
    result["source_records"].extend(reused)
    result["source_results"].extend({"date": row["date"], "status": "stored"} for row in reused)
    result["source_results"].extend(failures)
    result["status"] = ("stored" if days and all(
        row["status"] == "stored" for row in result["source_results"])
        else "import_error" if any(row["status"] in {"invalid_response", "import_error"}
                                  for row in result["source_results"])
        else "garmin_not_ready")
    return result


def refresh_references(store, wake_dates, *, rolling_windows=None, fresh_endpoint=None):
    """Finish selected rolling work and retain the existing bounded calendar tail."""
    from .night_baseline_refresh import refresh_pending, refresh_recent_night_references

    if fresh_endpoint is not None:
        from .fresh_night_references import refresh_current_night

        fresh = refresh_current_night(store, fresh_endpoint)
        if fresh["status"] != "not_run_not_current":
            return fresh
    cache = {}
    report = refresh_recent_night_references(store, wake_dates=wake_dates, snapshot_cache=cache,
                                            rolling_windows=rolling_windows)
    tail = refresh_pending(store, snapshot_cache=cache, rolling_windows=rolling_windows)
    report["calendar_refresh"] = tail
    for name in ("products_written", "products_unchanged", "periods_completed"):
        report[name] = report.get(name, 0) + tail.get(name, 0)
    for name in ("pending", "deferred_open", "closed_pending", "component_operations"):
        if name in tail:
            report[name] = tail[name]
    report["closed_remaining"] = tail.get("closed_pending")
    held = (report.get("held_periods", 0), tail.get("held_periods", 0))
    report["held_periods"] = None if None in held else max(held)
    report["blocked"] = sorted(set(report.get("blocked", []) + tail.get("blocked", [])))
    if tail.get("status") in {"held", "calendar_timezone_required"}:
        report["status"] = tail["status"]
    elif report.get("selected_remaining") == 0 and tail.get("closed_pending"):
        report["status"] = "bounded_pending"
    return report


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
    from .sources.provider_capabilities import capabilities_for
    supported = capabilities_for(garmin)
    for stream in ("sleep", "hrv"):
        if stream == "sleep" and not supported["sleep_stages"]:
            report["sleep_status"] = "unsupported"
            report["unsupported_features"] = ["sleep_stages"]
            source_checked = False
            continue
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
    report["heart_rate"] = (collect_heart_rate(
        store, garmin, [wake_date], timezone=os.environ.get("HEALTH_TIMEZONE"))
        if report["sleep_status"] == "stored" else {
            "status": "garmin_not_ready" if report["sleep_status"] == "garmin_not_ready" else "not_checked",
            "source_results": [], "source_records": []})
    report["heart_rate_status"] = report["heart_rate"]["status"]
    if report["heart_rate_status"] == "import_error":
        source_checked = False
        report["status"] = "partial"
    elif report["heart_rate_status"] != "stored":
        report["status"] = "pending"
    if not source_checked:
        report["baseline_refresh"] = {"status": "not_run_source_failure"}
    elif report["sleep_status"] == "stored":
        report["baseline_refresh"] = refresh_references(
            store, [wake_date], fresh_endpoint=(wake_date if all(report[f"{stream}_status"] == "stored"
            for stream in ("sleep", "hrv", "heart_rate")) else None))
    else:
        from .night_baseline_refresh import refresh_pending
        report["baseline_refresh"] = refresh_pending(store)
    return publish(store, run_id, scope, report, source_checked=source_checked)


@safe_cli
def main() -> None:
    parser = SafeArgumentParser(description="Import one recent Garmin night")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--wake-date", required=True)
    parser.add_argument("--request-id")
    args = parser.parse_args()
    run(run_id=args.run_id, wake_date=args.wake_date, request_id=args.request_id)


if __name__ == "__main__":
    main()
