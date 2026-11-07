"""Versioned, payload-free evidence shared by Actions receipts and offline audit."""

import re
from datetime import datetime, timedelta

SCHEMA_VERSION = 1
RECEIPT_KIND = "actions-run-receipt"
WORKFLOWS = {
    "refresh.yml": {"module": "pipeline.refresh", "cron": "0 */6 * * *"},
    "activity-refresh.yml": {"module": "pipeline.activity_refresh", "cron": "15 23 * * *"},
    "scheduled-activity-backfill.yml": {
        "module": "pipeline.activity_backfill_scheduler", "cron": "30 2,8,14,20 * * *",
    },
    "scheduled-health-detail-backfill.yml": {
        "module": "pipeline.health_detail_backfill", "cron": "15 1,7,13,19 * * *",
    },
    "scheduled-hrv-backfill.yml": {"module": "pipeline.hrv_backfill", "cron": "30 4,10,16,22 * * *"},
    "coach-input-backfill.yml": {"module": "pipeline.coach_backfill", "cron": "40 2,8,14,20 * * *"},
}
DATA_OUTCOMES = frozenset({"complete", "partial", "no_work", "not_ready", "error", "unknown", "unsupported"})
COUNTER_NAMES = frozenset({
    "activities_checked", "attempted_this_run", "generated_this_run", "processed_source_count",
    "blocked_activity_count", "profile_count", "running_activities", "already_current_activities",
    "remaining_activities", "total_activities", "complete_activities", "completed_activities",
    "total_days", "complete_days", "completed_days", "remaining_days", "blocked_after_three_failures",
    "completed_this_run_count", "failed_this_run_count", "skipped_this_run_count", "last_run_ranges_count",
    "refreshed_count", "unchanged_count", "baseline_count", "unsupported_count", "error_count", "unknown_count",
    "sleep_attempted_this_run", "body_composition_attempted_this_run",
})
RECEIPT_FIELDS = frozenset({
    "schema_version", "kind", "workflow", "run_id", "run_attempt", "head_sha", "event",
    "cron_expression", "scheduled_for", "occurrence_id", "trigger_provenance", "started_at",
    "finished_at", "native_exit", "command_outcome", "job_conclusion", "data_outcome", "counters",
    "context_status",
})


def utc_time(value):
    """Parse an explicit UTC instant; never infer a timezone from a naive time."""
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("Invalid UTC timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("Invalid UTC timestamp")
    return result


def validate_receipt(value):
    """Reject unknown fields/statuses; return the validated existing dictionary."""
    try:
        valid = (
            isinstance(value, dict) and set(value) == RECEIPT_FIELDS
            and type(value["schema_version"]) is int and value["schema_version"] == SCHEMA_VERSION
            and value["kind"] == RECEIPT_KIND and value["workflow"] in WORKFLOWS
            and value["event"] in {"schedule", "workflow_dispatch", "workflow_run", "unknown"}
            and value["trigger_provenance"] == "unavailable"
            and value["scheduled_for"] is None and value["occurrence_id"] is None
            and value["job_conclusion"] is None and value["data_outcome"] in DATA_OUTCOMES
            and value["context_status"] in {"valid", "missing", "invalid"}
            and value["command_outcome"] in {"success", "failure", "not_started", "interrupted"}
            and type(value["native_exit"]) is int and -(2**31) <= value["native_exit"] < 2**31
        )
        if not valid:
            raise ValueError("Invalid run receipt")
        if value["run_id"] is not None and not (
            isinstance(value["run_id"], str) and re.fullmatch(r"[1-9][0-9]{0,19}", value["run_id"])
        ):
            raise ValueError("Invalid run receipt")
        if value["run_attempt"] is not None and not (
            type(value["run_attempt"]) is int and 1 <= value["run_attempt"] <= 1000000
        ):
            raise ValueError("Invalid run receipt")
        if value["head_sha"] is not None and not (
            isinstance(value["head_sha"], str) and re.fullmatch(r"[0-9a-f]{40}", value["head_sha"])
        ):
            raise ValueError("Invalid run receipt")
        cron = value["cron_expression"]
        if cron is not None and not (value["event"] == "schedule" and cron == WORKFLOWS[value["workflow"]]["cron"]):
            raise ValueError("Invalid run receipt")
        if value["context_status"] == "valid" and (
            any(value[key] is None for key in ("run_id", "run_attempt", "head_sha"))
            or value["event"] == "unknown" or value["event"] == "schedule" and cron is None
        ):
            raise ValueError("Invalid run receipt")
        if utc_time(value["finished_at"]) < utc_time(value["started_at"]):
            raise ValueError("Invalid run receipt")
        if not isinstance(value["counters"], dict) or any(
            key not in COUNTER_NAMES or type(count) is not int or not 0 <= count <= 2**53 - 1
            for key, count in value["counters"].items()
        ):
            raise ValueError("Invalid run receipt")
        if (value["native_exit"] == 0) != (value["command_outcome"] == "success"):
            raise ValueError("Invalid run receipt")
    except (KeyError, TypeError, OverflowError, ValueError):
        raise ValueError("Invalid run receipt") from None
    return value
