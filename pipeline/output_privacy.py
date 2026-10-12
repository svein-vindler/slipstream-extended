"""Explicit projections for hosted logs; private reports and raised errors stay intact."""
import argparse
import functools
import json
import logging
import math
import sys
from contextlib import contextmanager

_EVENTS = frozenset({
    "manual_activity_refresh", "granular_export", "activity_refresh", "health_detail_backfill",
    "hrv_backfill", "coach_backfill", "activity_backfill_scheduler", "activity_backfill",
    "summary_export_manifest", "summary_restore", "fetch", "health_history_index",
    "health_index_reconcile", "recent_health", "recent_body", "refresh_summaries",
    "refresh-diagnostics", "refresh-diagnostics-publication",
})
_COUNTS = (
    "schema_version", "new_activity_count", "checked_activity_count", "remaining_candidate_count",
    "activities_checked", "attempted_this_run", "generated_this_run", "processed_source_count",
    "blocked_activity_count", "profile_count", "running_activities", "already_current_activities", "total", "complete", "blocked", "remaining",
    "total_days", "completed_days", "blocked_days", "remaining_days",
    "total_activities", "completed_activities", "remaining_activities", "blocked_activities_count",
    "activities_fetched", "activities_preserved", "activities_written", "health_days_fetched",
    "health_days_written", "days_checked", "days_ready", "days_not_ready", "source_errors",
    "objects_written", "objects_unchanged", "checkpoints_written", "night_checkpoints_written",
    "months_considered", "months_written", "months_unchanged", "invalid_days", "days",
    "activity_rows_fetched", "health_rows_fetched", "calendar_days",
    "elapsed_ms", "login_ms", "garmin_connectapi_calls", "garmin_connectapi_errors", "garmin_fetch_ms",
    "r2_read_ms", "r2_write_ms", "r2_get_ms", "r2_head_ms", "r2_list_ms", "r2_inventory_ms",
    "activity_file_import_ms", "coach_input_ms", "sdk_calls", "daily_summary_calls", "garmin_api_calls", "garmin_api_errors",
)
_OPERATIONS = ("get", "head", "put", "list_pages", "listed_objects", "downloaded_bytes", "uploaded_bytes")
_STATUSES = frozenset({
    "active", "complete", "complete_with_blocked", "partial", "pending", "idle", "failed",
    "error", "waiting_for_profile", "stored", "skipped", "unchanged", "refreshed", "blocked", "checked", "ready",
})
_STAGES = frozenset({
    "restore_summaries", "latest_activity", "latest_night", "activity_baseline", "historical_health",
    "export_summaries", "refresh_summaries", "recent_health", "recent_body", "manual_activity",
    "training_context", "health_indexes",
})
_STREAMS = ("sleep", "hrv", "body-composition", "body_composition", "activities", "health")

def _number(value, key):
    if value is None:
        return True
    if type(value) is int:
        return 0 <= value <= 2**53 - 1
    return key.endswith("_ms") and type(value) is float and 0 <= value <= 2**53 - 1 and math.isfinite(value)

def _counts(report):
    if not isinstance(report, dict):
        return {}
    result = {key: report[key] for key in _COUNTS if key in report and _number(report[key], key)}
    if "status" in report:
        result["status"] = report["status"] if isinstance(report["status"], str) and report["status"] in _STATUSES else "unknown"
    for key in ("scheduled", "source_checked", "checkpoint_written",
                "diagnostics_persistence_skipped", "diagnostics_persistence_failed"):
        if type(report.get(key)) is bool:
            result[key] = report[key]
    for key in ("r2_operations", "r2_sdk_operations", "health_diagnostics", "provider_diagnostics"):
        metrics = report.get(key)
        if isinstance(metrics, dict):
            allowed = _OPERATIONS if key.startswith("r2_") else _COUNTS
            result[key] = {name: metrics[name] for name in allowed if name in metrics and _number(metrics[name], name)}
    # Lengths are aggregates. Never copy list members, object keys, dates or free errors.
    for key in ("completed_this_run", "failed_this_run", "skipped_this_run", "last_run_ranges",
                "refreshed_existing_this_run", "activities", "hrv", "files"):
        if isinstance(report.get(key), list):
            result[key + "_count"] = len(report[key])
    for key in ("months_written", "months_unchanged"):
        if isinstance(report.get(key), list):
            result[key] = len(report[key])
    return result

def log_report(event, report):
    """One event per existing report print; unknown input is dropped, never stringified."""
    if event not in _EVENTS:
        raise ValueError("Unknown hosted log event")
    kinds = {"recent_health": "recent-health-sync", "recent_body": "recent-body-sync",
             "refresh_summaries": "summary-window-refresh", "health_index_reconcile": "health-index-maintenance"}
    result = {"kind": kinds.get(event, event), **_counts(report)}
    if isinstance(report, dict):
        for key in ("results", "streams", "windows"):
            parts = report.get(key)
            if isinstance(parts, dict):
                result[key] = {stream: _counts(parts[stream]) for stream in _STREAMS if stream in parts}
        for stream in ("sleep", "body_composition"):
            if isinstance(report.get(stream), dict):
                result[stream] = _counts(report[stream])
        if isinstance(report.get("stages"), list):
            result["stages"] = [
                {"stage": stage["stage"], **_counts(stage)}
                for stage in report["stages"] if isinstance(stage, dict)
                and isinstance(stage.get("stage"), str) and stage["stage"] in _STAGES
            ]
        if report.get("mode") in ("general", "activity", "night"):
            result["mode"] = report["mode"]
        if report.get("reason") in ("interrupted", "no_write_inventory"):
            result["reason"] = report["reason"]
    print(json.dumps(result))

class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # argparse errors can include raw argv, paths or provider validation text.
        super().error("Invalid arguments; check the command's --help and configured inputs.")

class _ProviderLogProjection(logging.Filter):
    """Pinned SDK loggers can log provider errors before returning/raising."""
    def filter(self, record):
        category = ("operation_failed" if record.levelno >= logging.ERROR else
                    "provider_warning" if record.levelno >= logging.WARNING else "provider_message")
        record.msg = json.dumps({"event": "garmin_provider_log", "category": category})
        record.args = ()
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return True


@contextmanager
def _private_provider_logs():
    # Scope the two known pinned SDK loggers to this CLI invocation. Leave
    # levels/handlers/propagation and all unrelated loggers untouched.
    loggers = [logging.getLogger(name) for name in ("garminconnect", "garminconnect.client")]
    projection = _ProviderLogProjection()
    for logger in loggers:
        logger.addFilter(projection)
    try:
        yield
    finally:
        for logger in loggers:
            logger.removeFilter(projection)


def safe_cli(main):
    """Sanitize only the CLI boundary; run() and its private consumers keep original errors."""
    @functools.wraps(main)
    def wrapped(*args, **kwargs):
        with _private_provider_logs():
            try:
                return main(*args, **kwargs)
            except SystemExit as exc:
                if exc.code is None or type(exc.code) is int:
                    raise
                print("Pipeline failed: operation_failed.", file=sys.stderr)
                raise SystemExit(1) from None
            except KeyboardInterrupt:
                print("Pipeline stopped: interrupted.", file=sys.stderr)
                raise SystemExit(130) from None
            except Exception:
                print("Pipeline failed: operation_failed.", file=sys.stderr)
                raise SystemExit(1) from None
    return wrapped
