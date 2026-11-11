"""Run one fixed hosted pipeline command and emit its privacy-safe Actions receipt."""

import importlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from .actions_receipt_schema import (
    COUNTER_NAMES,
    RECEIPT_KIND,
    SCHEMA_VERSION,
    WORKFLOWS,
    configured_schedules,
    utc_time,
    validate_receipt,
)
from .output_privacy import SafeArgumentParser, safe_cli


def _context(workflow):
    values = {
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "head_sha": os.environ.get("GITHUB_SHA"),
        "event": os.environ.get("GITHUB_EVENT_NAME"),
    }
    missing = any(value is None or value == "" for value in values.values())
    invalid = False
    for key in ("run_id", "run_attempt"):
        value = values[key]
        if value is None or value == "":
            values[key] = None
        elif not re.fullmatch(r"[1-9][0-9]{0,19}", value) or (key == "run_attempt" and int(value) > 1000000):
            values[key] = None
            invalid = True
    if values["run_attempt"] is not None:
        values["run_attempt"] = int(values["run_attempt"])
    sha = values["head_sha"]
    if not sha:
        values["head_sha"] = None
    elif not re.fullmatch(r"[0-9a-fA-F]{40}", sha):
        values["head_sha"] = None
        invalid = True
    if values["head_sha"] is not None:
        values["head_sha"] = values["head_sha"].lower()
    event = values["event"]
    if event not in {"schedule", "workflow_dispatch", "workflow_run"}:
        if event:
            invalid = True
        values["event"] = "unknown"
    cron = os.environ.get("ACTIONS_CRON_EXPRESSION")
    zone = next((item["timezone"] for item in configured_schedules(workflow)
                 if cron == item["cron_expression"]), None)
    values["cron_expression"] = cron if values["event"] == "schedule" and zone else None
    values["cron_timezone"] = zone if values["cron_expression"] is not None else None
    if values["event"] == "schedule" and not cron:
        missing = True
    if cron and (values["event"] != "schedule" or zone is None):
        invalid = True
    values["context_status"] = "invalid" if invalid else "missing" if missing else "valid"
    return values


def _project(workflow, report, started, finished):
    versions = {1, 2} if workflow == "scheduled-activity-backfill.yml" else {1}
    if not isinstance(report, dict) or type(report.get("schema_version")) is not int or report["schema_version"] not in versions:
        return "unknown", {}
    if report.get("status") == "unsupported":
        return "unsupported", {}
    if workflow == "activity-refresh.yml":
        count = report.get("activities_checked")
        statuses = report.get("by_status")
        if not _count(count) or not isinstance(statuses, dict):
            return "unknown", {}
        counters = {"activities_checked": count}
        allowed = {"refreshed", "unchanged", "baseline", "unsupported", "error", "unknown"}
        if not all(key in allowed and _count(value) for key, value in statuses.items()):
            return "unknown", counters
        counters.update({key + "_count": value for key, value in statuses.items()})
        if statuses.get("error", 0) > 0:
            return "partial", counters
        if sum(statuses.values()) != count or statuses.get("unknown", 0) > 0:
            return "unknown", counters
        if count > 0 and statuses.get("unsupported", 0) == count:
            return "unsupported", counters
        return "no_work" if count == 0 else "complete", counters
    if workflow == "refresh.yml":
        status = report.get("status")
        if not isinstance(status, str):
            return "unknown", {}
        return {"complete": "complete", "partial": "partial", "failed": "error"}.get(status, "unknown"), {}
    if workflow == "scheduled-activity-backfill.yml":
        counters = {}
        ranges = report.get("last_run_ranges")
        if _current(report.get("last_run_at"), started, finished) and isinstance(ranges, list) and all(
            isinstance(item, dict) and _count(item.get("attempted")) for item in ranges
        ):
            counters = {"attempted_this_run": sum(item["attempted"] for item in ranges),
                        "last_run_ranges_count": len(ranges)}
        status = report.get("status")
        if isinstance(status, str) and status in {"active", "complete_with_blocked"}:
            return "partial", counters
        if status == "complete":
            return "no_work" if counters.get("attempted_this_run") == 0 else "complete", counters
    if workflow == "scheduled-health-detail-backfill.yml":
        counters = {}
        outcomes = []
        for name in ("sleep", "body_composition"):
            stream = report.get(name)
            if not isinstance(stream, dict):
                outcomes.append("unknown")
                continue
            attempted = stream.get("attempted_this_run")
            if _count(attempted):
                counters[name + "_attempted_this_run"] = attempted
            status = stream.get("status")
            if status not in ("unsupported", "skipped") and (
                type(stream.get("schema_version")) is not int or stream["schema_version"] != 1
            ):
                outcomes.append("unknown")
            else:
                outcomes.append(_status_outcome(status))
        for outcome in ("error", "partial", "not_ready", "unknown"):
            if outcome in outcomes:
                return outcome, counters
        if outcomes == ["unsupported", "unsupported"]:
            return "unsupported", counters
        if "unsupported" in outcomes:
            return "partial", counters
        no_work = counters == {"sleep_attempted_this_run": 0, "body_composition_attempted_this_run": 0}
        return "no_work" if no_work else "complete", counters
    if workflow in {"scheduled-hrv-backfill.yml", "coach-input-backfill.yml"}:
        counters = _counters(report)
        if not _current(report.get("updated_at"), started, finished):
            counters = {key: value for key, value in counters.items() if "this_run" not in key}
        status = report.get("status")
        outcome = "not_ready" if status == "waiting_for_profile" and workflow == "coach-input-backfill.yml" else _status_outcome(status)
        if outcome == "complete" and status == "complete" and counters.get("attempted_this_run") == 0:
            outcome = "no_work"
        return ("unknown" if status == "skipped" else outcome), counters
    return "unknown", {}


def _reference_outcome(report, outcome):
    # These are observed child reports only; do not read or infer stored queue state.
    if not isinstance(report, dict):
        return outcome
    night = report.get("night_followup")
    if (isinstance(night, dict) and night.get("status") == "partial"
            and outcome in {"complete", "no_work", "not_ready"}):
        # The scheduled night check is part of this invocation even when the
        # primary historical/activity operation has no eligible work.
        outcome = "partial"
    baseline = report.get("baseline_refresh")
    if isinstance(baseline, dict):
        status = baseline.get("status")
        pending = any(_count(baseline.get(key)) and baseline[key] > 0 for key in ("pending", "held_periods"))
        if pending or isinstance(status, str) and status in {
            "bounded_pending", "held", "calendar_timezone_required", "not_run_source_failure",
        }:
            if outcome in {"complete", "no_work", "not_ready"}:
                outcome = "partial"
    for key in ("sleep", "body_composition"):
        child_outcome = _reference_outcome(report.get(key), outcome)
        if child_outcome == "partial" and outcome in {"complete", "no_work", "not_ready"}:
            outcome = "partial"
    return outcome


def _current(value, started, finished):
    try:
        return utc_time(started) <= utc_time(value) <= utc_time(finished)
    except (ValueError, TypeError):
        return False


def _counters(report):
    result = {key: value for key, value in report.items() if key in COUNTER_NAMES and _count(value)}
    for name in ("completed_this_run", "failed_this_run", "skipped_this_run"):
        if isinstance(report.get(name), list):
            result[name + "_count"] = len(report[name])
    return result


def _status_outcome(status):
    if not isinstance(status, str):
        return "unknown"
    return {"complete": "complete", "skipped": "complete", "active": "partial",
            "partial": "partial", "blocked": "partial", "complete_with_blocked": "partial",
            "pending": "not_ready", "failed": "error", "error": "error",
            "unsupported": "unsupported"}.get(status, "unknown")


def _count(value):
    return type(value) is int and 0 <= value <= 2**53 - 1


@safe_cli
def _invoke(module):
    return importlib.import_module(module).main()


def _notice():
    try:
        print("Actions receipt unavailable.", file=sys.stderr)
    except Exception:
        pass


def _emit(receipt, receipt_path):
    encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_text(encoded, encoding="utf-8")
    except Exception:
        _notice()
    try:
        print(encoded, end="")
    except Exception:
        _notice()
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with Path(summary).open("a", encoding="utf-8") as handle:
                handle.write(
                    f"Actions receipt: {receipt['workflow']} — command {receipt['command_outcome']}; "
                    f"data {receipt['data_outcome']}; native exit {receipt['native_exit']}.\n"
                    f"Run {receipt['run_id']}; attempt {receipt['run_attempt']}; head {receipt['head_sha']}.\n"
                    f"Event {receipt['event']}; configured cron {receipt['cron_expression']}; timezone {receipt['cron_timezone']}; context {receipt['context_status']}.\n"
                    f"Actual UTC start {receipt['started_at']}; finish {receipt['finished_at']}.\n"
                    "Scheduled occurrence unavailable; final job conclusion unavailable.\n"
                )
        except Exception:
            _notice()


def main(argv=None):
    """CLI entry: original command arguments follow a required ``--`` separator."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--workflow", choices=tuple(WORKFLOWS), required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    if arguments in (["--help"], ["-h"]):
        parser.parse_args(arguments)
    try:
        separator = arguments.index("--")
    except ValueError:
        parser.error("Command separator required")
    args = parser.parse_args(arguments[:separator])
    command_args = arguments[separator + 1:]
    started = datetime.now(timezone.utc).isoformat()
    report = None
    native_exit = 0
    previous_argv = sys.argv
    try:
        sys.argv = [WORKFLOWS[args.workflow]["module"], *command_args]
        report = _invoke(WORKFLOWS[args.workflow]["module"])
    except SystemExit as exc:
        native_exit = exc.code if type(exc.code) is int else 0 if exc.code is None else 1
    except KeyboardInterrupt:
        native_exit = 130
    finally:
        sys.argv = previous_argv
    finished = datetime.now(timezone.utc).isoformat()
    try:
        outcome, counters = _project(args.workflow, report, started, finished)
        outcome = _reference_outcome(report, outcome)
        if native_exit:
            outcome = "error"
        receipt = {
            "schema_version": SCHEMA_VERSION, "kind": RECEIPT_KIND,
            "workflow": args.workflow, **_context(args.workflow),
            "scheduled_for": None, "occurrence_id": None, "trigger_provenance": "unavailable",
            "started_at": started, "finished_at": finished, "native_exit": native_exit,
            "command_outcome": "interrupted" if native_exit == 130 else "failure" if native_exit else "success",
            "job_conclusion": None, "data_outcome": outcome, "counters": counters,
        }
        _emit(validate_receipt(receipt), args.receipt)
    except Exception:
        _notice()
    return native_exit


if __name__ == "__main__":
    raise SystemExit(main())
