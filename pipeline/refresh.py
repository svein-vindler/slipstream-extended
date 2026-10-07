"""One bounded refresh orchestration for chat dispatch, schedules and local use."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from . import (
    fetch,
    health_index_reconcile,
    latest_activity,
    latest_night,
    manual_activity_refresh,
    recent_body,
    recent_health,
    refresh_summaries,
    summary_export,
    summary_restore,
)
from .freshness import recent_day
from .granular import json_bytes
from .local_bootstrap import prepare_environment
from .r2_store import R2Store
from .sources.garmin import _login


@dataclass(frozen=True)
class RefreshRequest:
    mode: str = "general"
    scheduled: bool = False
    include_granular: bool = False
    run_id: str = "0"
    request_id: str | None = None
    wake_date: str | None = None
    activity_id: str | None = None
    expected_date: str | None = None
    new_activity_expected: bool = True
    repair_only: bool = False
    health_start: str | None = None
    health_end: str | None = None
    backfill_start_year: str | None = None
    backfill_end_year: str | None = None

    def validate(self):
        if self.mode not in {"general", "activity", "night"}:
            raise ValueError("Choose general, activity or night refresh")
        if not re.fullmatch(r"\d+", self.run_id):
            raise ValueError("run_id must contain digits only")
        if self.request_id and not re.fullmatch(r"[a-f0-9-]{36}", self.request_id):
            raise ValueError("Invalid request_id")
        if self.scheduled and (self.mode != "general" or self.include_granular or self.backfill):
            raise ValueError("Scheduled refresh must use the normal bounded general mode")
        if self.repair_only and self.mode != "activity":
            raise ValueError("R2-only repair requires activity mode")
        if self.mode != "general" and self.backfill:
            raise ValueError("Targeted refresh cannot include historical backfill")
        if self.mode == "night" and not self.wake_date:
            raise ValueError("Night mode requires wake_date")
        for value in (self.wake_date, self.expected_date, self.health_start, self.health_end):
            if value and (not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
                          or date.fromisoformat(value).isoformat() != value):
                raise ValueError("Dates must use YYYY-MM-DD")
        if self.activity_id and not re.fullmatch(r"\d{1,20}", self.activity_id):
            raise ValueError("activity_id must be a numeric Garmin ID")
        if self.mode == "night":
            recent_day(self.wake_date)
        if self.mode == "activity" and self.expected_date:
            recent_day(self.expected_date)
        if bool(self.health_start) != bool(self.health_end):
            raise ValueError("Both health_start and health_end are required")
        if bool(self.backfill_start_year) != bool(self.backfill_end_year):
            raise ValueError("Both backfill years are required")
        if self.health_start and self.backfill_start_year:
            raise ValueError("Choose date inputs or year inputs, not both")
        if self.health_start and self.health_start > self.health_end:
            raise ValueError("health_start cannot be after health_end")
        if self.backfill_start_year:
            for year in (self.backfill_start_year, self.backfill_end_year):
                if not re.fullmatch(r"\d{4}", year) or not 1 <= int(year) <= 9999:
                    raise ValueError("Backfill years must contain four valid digits")
            if self.backfill_start_year > self.backfill_end_year:
                raise ValueError("backfill_start_year cannot be after backfill_end_year")

    @property
    def backfill(self):
        return any((self.health_start, self.health_end, self.backfill_start_year, self.backfill_end_year))


class MeasuredGarmin:
    """Lazy shared login; count connectapi attempts, without logging arguments."""
    def __init__(self, login):
        self.login = login
        self.client = None
        self.calls = self.errors = 0
        self.available = False
        self.original = None
        self.elapsed_ms = 0.0
        self.login_ms = None

    def get(self):
        if self.client is None:
            started = time.perf_counter()
            try:
                self.client = self.login()
            finally:
                self.login_ms = (time.perf_counter() - started) * 1000
            original = getattr(self.client, "connectapi", None)
            self.available = callable(original)
            self.original = original
            if self.available:
                def counted(*args, **kwargs):
                    self.calls += 1
                    started = time.perf_counter()
                    try:
                        return original(*args, **kwargs)
                    except Exception:
                        self.errors += 1
                        raise
                    finally:
                        self.elapsed_ms += (time.perf_counter() - started) * 1000
                self.client.connectapi = counted
        return self.client


def _publish_diagnostics(report, *, store, run_id, original_failure):
    """One guarded PUT, with its own sanitized log-only measurement."""
    if run_id == "0":
        return
    started = time.perf_counter()
    before = dict(getattr(store, "operations", {}))
    publication = {"schema_version": 1, "kind": "refresh-diagnostics-publication", "status": "skipped"}
    try:
        if isinstance(original_failure, (KeyboardInterrupt, SystemExit)):
            publication["reason"] = "interrupted"
        elif store is None:
            publication["reason"] = "no_write_inventory"
            report["diagnostics_persistence_skipped"] = True
        else:
            store.put(f"refresh/diagnostics/v1/{run_id}.json", json_bytes(report), "application/json")
            publication["status"] = "stored"
    except Exception:
        publication["status"] = "failed"
        report["diagnostics_persistence_failed"] = True
        if original_failure is None:
            raise
    finally:
        publication.update(elapsed_ms=round((time.perf_counter() - started) * 1000),
            r2_sdk_operations={key: value - before.get(key, 0)
                               for key, value in getattr(store, "operations", {}).items()})
        # Its own PUT cannot time itself in the stored object without another PUT.
        print(json.dumps(publication))


def run(request: RefreshRequest, *, data_dir="data", store_factory=R2Store,
        login=_login, diagnostics_file: Path | None = None) -> dict:
    request.validate()  # Reject incompatible inputs before login or storage access.
    data = Path(data_dir)
    baseline = data.parent / ".granular/on-demand-baseline.json"
    provider = MeasuredGarmin(login)
    began = time.perf_counter()
    report = {"schema_version": 1, "kind": "refresh-diagnostics",
              "correlation_id": str(uuid.uuid4()), "mode": request.mode,
              "scheduled": request.scheduled, "status": "complete", "stages": [],
              "started_at": datetime.now(timezone.utc).isoformat(), "source_checks": []}
    last_store = publication_store = None

    def source_checks(result):
        if not isinstance(result, dict):
            return
        checked_at = datetime.now(timezone.utc).isoformat()
        def add(scope, checked, outcome="checked"):
            report["source_checks"].append({"scope": scope, "checked": checked,
                "checked_at": checked_at if checked else None, "outcome": outcome})
        kind = result.get("kind")
        if kind in {"latest-activity", "latest-night", "activity-repair"}:
            checked = result.get("source_checked") is True
            negative = result.get("status") in {"pending", "no_recent_activity", "no_new_activity",
                                                   "expected_activity_missing", "files_unavailable"}
            add(request.mode, checked, "checked" if checked and not negative else
                "not_ready" if checked else "not_checked" if request.repair_only else "failed")
        elif kind == "summary-window-refresh":
            for stream, window in result["windows"].items():
                checked = window["source_checked"]
                add(f"{stream}_summary", checked, "checked" if checked else "failed")
        elif kind == "recent-health-sync":
            for stream, value in result["streams"].items():
                checked = value["source_errors"] == 0
                add(stream, checked, "failed" if not checked else
                    "not_ready" if value["days_not_ready"] else "checked")
        elif kind == "recent-body-sync":
            checked = result["days_checked"] > 0 and result["source_errors"] == 0
            add("body", checked, "not_checked" if not result["days_checked"] else
                "failed" if not checked else "not_ready" if result["days_not_ready"] else "checked")

    def stage(name, operation, *, storage=True):
        nonlocal last_store, publication_store
        expects_source = name in {"refresh_summaries", "historical_health", "recent_health", "recent_body",
                                  "manual_activity", "latest_activity", "latest_night"} and not request.repair_only
        # Separate step budgets; one client/inventory and conservative bucket
        # accounting across this serial run. Custom non-R2 adapters retain
        # their factory semantics.
        store = None
        before = {}
        calls, errors = provider.calls, provider.errors
        source_ms = provider.elapsed_ms
        started = time.perf_counter()
        entry = {"stage": name, "status": "complete"}
        try:
            store = (last_store.new_stage() if isinstance(last_store, R2Store)
                     else store_factory()) if storage else None
            if store is not None:
                last_store = store
            before = dict(getattr(store, "operations", {}))
            result = operation(store)
            source_checks(result)
            if isinstance(result, dict) and result.get("status") in {
                    "partial", "pending", "idle", "files_unavailable", "coach_pending"}:
                entry["status"] = result["status"] if result["status"] in {"partial", "pending", "idle"} else "partial"
                if entry["status"] in {"partial", "pending"}:
                    report["status"] = "partial"
            return result
        except BaseException:
            entry["status"] = "failed"
            report["status"] = "failed"
            if expects_source:
                report["source_checks"].append({"scope": request.mode if request.mode != "general" else name,
                    "checked": False, "checked_at": None, "outcome": "failed"})
            raise
        finally:
            # Stages share guarded inventory, retaining their own allowances.
            # Use the last established budget without starting a new inventory.
            if getattr(store, "write_budget_initialized", False):
                publication_store = store
            entry.update(elapsed_ms=round((time.perf_counter() - started) * 1000),
                         garmin_connectapi_calls=provider.calls - calls if provider.available or not expects_source else None,
                         garmin_connectapi_errors=provider.errors - errors if provider.available or not expects_source else None,
                         garmin_fetch_ms=round(provider.elapsed_ms - source_ms) if provider.available or not expects_source else None,
                         r2_read_ms=round(sum(getattr(store, "timings_ms", {}).get(k, 0) for k in ("get", "head", "list")))
                             if hasattr(store, "timings_ms") else None if storage else 0,
                         r2_write_ms=round(store.timings_ms["put"]) if hasattr(store, "timings_ms") else None if storage else 0,
                         **{f"r2_{key}_ms": round(store.timings_ms[key]) if hasattr(store, "timings_ms")
                            and key in store.timings_ms else None if storage else 0
                            for key in ("get", "head", "list", "inventory")},
                         activity_file_import_ms=getattr(store, "component_timings_ms", {}).get("activity_file_import"),
                         coach_input_ms=getattr(store, "component_timings_ms", {}).get("coach_input"),
                         r2_sdk_operations={key: value - before.get(key, 0)
                                            for key, value in getattr(store, "operations", {}).items()})
            report["stages"].append(entry)

    try:
        if request.mode != "night" and not request.repair_only:
            stage("restore_summaries", lambda store: summary_restore.restore_summaries(str(data), store))
        if request.mode == "activity":
            stage("latest_activity", lambda store: latest_activity.run(
                run_id=request.run_id, data_dir=str(data), lookback_days=7,
                request_id=request.request_id, repair_only=request.repair_only,
                requested_activity_id=request.activity_id, expected_date=request.expected_date,
                new_activity_expected=request.new_activity_expected,
                store=store, garmin=None if request.repair_only else provider.get()))
        elif request.mode == "night":
            stage("latest_night", lambda store: latest_night.run(
                run_id=request.run_id, wake_date=request.wake_date,
                request_id=request.request_id, store=store, garmin=provider.get()))
        else:
            if request.include_granular:
                stage("activity_baseline", lambda _: manual_activity_refresh.snapshot(
                    data / "activities.csv", baseline), storage=False)
            if request.backfill:
                ranges = [(request.health_start, request.health_end)] if request.health_start else [
                    (f"{year:04}-01-01", f"{year:04}-12-31")
                    for year in range(int(request.backfill_end_year), int(request.backfill_start_year) - 1, -1)]
                for start, end in ranges:
                    stage("historical_health", lambda _, start=start, end=end: fetch.run(
                        skip_activities=True, data_dir=str(data), health_start=date.fromisoformat(start),
                        health_end=date.fromisoformat(end), client=provider.get()), storage=False)
                stage("export_summaries", lambda store: summary_export.run(str(data), store=store))
            else:
                stage("refresh_summaries", lambda store: refresh_summaries.run(
                    str(data), store=store, client=provider.get()))
            stage("recent_health", lambda store: recent_health.run(days=3, store=store, garmin=provider.get()))
            stage("recent_body", lambda store: recent_body.run(
                max_days=3, health_csv=(data / "health_daily.csv").read_bytes(), store=store, garmin=provider.get()))
            if request.include_granular:
                stage("manual_activity", lambda store: manual_activity_refresh.run(
                    baseline=baseline, summary=data / "activities.csv", run_id=request.run_id,
                    store=store, garmin=provider.get()))
    finally:
        try:
            # As with the old !cancelled() scheduled-only step, repair derived
            # indexes after ordinary source failure, but never after Ctrl+C.
            if request.scheduled and not isinstance(sys.exception(), (KeyboardInterrupt, SystemExit)):
                result = stage("health_indexes", lambda store: health_index_reconcile.run(store=store))
                if result["status"] != "complete":
                    report["status"] = "failed"
                    raise RuntimeError("Health index reconciliation is incomplete")
        finally:
            if provider.available:
                provider.client.connectapi = provider.original
            report.update(elapsed_ms=round((time.perf_counter() - began) * 1000),
                          garmin_connectapi_available=provider.available,
                          login_ms=round(provider.login_ms) if provider.login_ms is not None else None,
                          finished_at=datetime.now(timezone.utc).isoformat())
            # Never initialize an inventory solely to publish measurements.
            _publish_diagnostics(report, store=publication_store, run_id=request.run_id,
                                 original_failure=sys.exception())
            if diagnostics_file:
                diagnostics_file.parent.mkdir(parents=True, exist_ok=True)
                temporary = diagnostics_file.with_suffix(".tmp")
                temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
                temporary.replace(diagnostics_file)
            print(json.dumps(report))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", action="store_true", help="Read the established GitHub workflow inputs")
    parser.add_argument("--mode", choices=("general", "activity", "night"), default="general")
    parser.add_argument("--include-granular", action="store_true")
    parser.add_argument("--repair-only", action="store_true")
    parser.add_argument("--wake-date")
    parser.add_argument("--activity-id")
    parser.add_argument("--expected-date")
    parser.add_argument("--new-activity-expected", choices=("true", "false"), default="true")
    parser.add_argument("--run-id", default=str(time.time_ns()))
    parser.add_argument("--request-id")
    parser.add_argument("--health-start")
    parser.add_argument("--health-end")
    parser.add_argument("--backfill-start-year")
    parser.add_argument("--backfill-end-year")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--confirm-cloud-jobs-paused", action="store_true")
    parser.add_argument("--diagnostics-file", type=Path, default=Path(".granular/refresh-diagnostics.json"))
    args = parser.parse_args()
    try:
        if args.workflow:
            values = os.environ
            activity = values.get("ACTIVITY_ONLY") == "true"
            night = values.get("NIGHT_ONLY") == "true"
            if activity and night:
                raise ValueError("Choose one targeted mode")
            request = RefreshRequest(
                mode="activity" if activity else "night" if night else "general",
                scheduled=values.get("REFRESH_EVENT") == "schedule",
                include_granular=values.get("INCLUDE_GRANULAR") == "true",
                run_id=values["GITHUB_RUN_ID"], request_id=values.get("SYNC_REQUEST_ID") or None,
                wake_date=values.get("LATEST_WAKE_DATE") or None,
                repair_only=values.get("LATEST_R2_ONLY") == "true",
                activity_id=values.get("LATEST_ACTIVITY_ID") or None,
                expected_date=values.get("LATEST_EXPECTED_DATE") or None,
                new_activity_expected=values.get("LATEST_NEW_ACTIVITY_EXPECTED") != "false",
                health_start=values.get("HEALTH_START") or None, health_end=values.get("HEALTH_END") or None,
                backfill_start_year=values.get("BACKFILL_START_YEAR") or None,
                backfill_end_year=values.get("BACKFILL_END_YEAR") or None)
        else:
            if not args.confirm_cloud_jobs_paused:
                raise ValueError("Confirm cloud Garmin jobs are paused/idle before local refresh")
            request = RefreshRequest(**{name: getattr(args, name) for name in (
                "mode", "include_granular", "run_id", "request_id", "wake_date", "activity_id",
                "expected_date", "repair_only", "health_start", "health_end",
                "backfill_start_year", "backfill_end_year")},
                new_activity_expected=args.new_activity_expected == "true")
        request.validate()
        if not args.workflow:
            prepare_environment(args.env_file)
        run(request, data_dir=args.data_dir, diagnostics_file=args.diagnostics_file)
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
