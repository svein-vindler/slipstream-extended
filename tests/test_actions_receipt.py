"""Synthetic checks at the Actions command-entry and receipt filesystem boundary."""
import importlib
import json
import os
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def synthetic_environment(monkeypatch):
    for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_EVENT_NAME",
                 "ACTIONS_CRON_EXPRESSION", "GITHUB_STEP_SUMMARY"):
        monkeypatch.delenv(name, raising=False)
    def blocked(*args, **kwargs):
        raise AssertionError("External systems forbidden in synthetic receipt tests")
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _stub_data_command(monkeypatch, command, operation):
    # Private scheduled CLIs call run_command; their public counterparts call run.
    # Keep each repo's actual main/parser and fake only its outer data boundary.
    boundary = "run_command" if hasattr(command, "run_command") else "run"
    monkeypatch.setattr(command, boundary, operation)


def test_activity_command_emits_a_safe_complete_receipt(monkeypatch, tmp_path, capsys):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.activity_refresh")
    observed = []

    def data_command(*args, **kwargs):
        observed.append(kwargs)
        return {"schema_version": 1, "activities_checked": 2,
                "by_status": {"refreshed": 1, "unchanged": 1},
                "activities": [{"id": "private-id", "date": "private-date"}]}

    _stub_data_command(monkeypatch, command, data_command)
    for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "GITHUB_EVENT_NAME",
                 "ACTIONS_CRON_EXPRESSION", "GITHUB_STEP_SUMMARY"):
        monkeypatch.delenv(name, raising=False)
    receipt_path = tmp_path / "receipt.json"
    assert main(["--workflow", "activity-refresh.yml", "--receipt", str(receipt_path),
                 "--", "--lookback-days", "7", "--max-activities", "3"]) == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == "complete"
    assert receipt["native_exit"] == 0
    assert receipt["command_outcome"] == "success"
    assert receipt["context_status"] == "missing"
    assert receipt["counters"]["activities_checked"] == 2
    assert observed[0]["lookback_days"] == 7
    assert observed[0]["max_activities"] == 3
    assert len(observed) == 1
    output = capsys.readouterr()
    assert "private-id" not in receipt_path.read_text(encoding="utf-8") + output.out + output.err
    assert "private-date" not in output.out


def test_scheduler_partial_and_empty_attempts_remain_distinct(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.activity_backfill_scheduler")
    report = {"schema_version": 2, "status": "active", "last_run_ranges": [
        {"attempted": 3, "start_date": "private-date", "status": "active"}]}
    def operation(*args, **kwargs):
        assert kwargs["max_activities"] == 13
        report["last_run_at"] = datetime.now(timezone.utc).isoformat()
        return report
    _stub_data_command(monkeypatch, command, operation)
    receipt_path = tmp_path / "receipt.json"
    args = ["--workflow", "scheduled-activity-backfill.yml", "--receipt", str(receipt_path), "--", "--max-activities", "13"]
    assert main(args) == 0
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == "partial"
    assert receipt["counters"]["attempted_this_run"] == 3
    report.update(status="complete", last_run_ranges=[])
    assert main(args) == 0
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["data_outcome"] == "no_work"
    report.pop("last_run_ranges")
    assert main(args) == 0
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["data_outcome"] == "complete"


def test_health_stream_outcomes_are_aggregated_without_private_members(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.health_detail_backfill")
    report = {"schema_version": 1,
              "sleep": {"schema_version": 1, "status": "complete", "attempted_this_run": 1},
              "body_composition": {"schema_version": 1, "status": "active", "attempted_this_run": 2,
                                   "failed_this_run": [{"date": "private-date", "error": "private-error"}]}}
    observed = []
    def operation(*args, **kwargs):
        observed.append(kwargs)
        return report
    _stub_data_command(monkeypatch, command, operation)
    path = tmp_path / "health.json"
    args = ["--workflow", "scheduled-health-detail-backfill.yml", "--receipt", str(path), "--",
            "--max-sleep-days", "4", "--max-body-days", "5", "--refresh-recent-days", "1"]
    assert main(args) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == "partial"
    assert receipt["counters"]["sleep_attempted_this_run"] == 1
    assert receipt["counters"]["body_composition_attempted_this_run"] == 2
    assert observed[0]["max_sleep_days"] == 4
    assert observed[0]["max_body_days"] == 5
    assert observed[0]["refresh_recent_days"] == 1
    assert "private" not in path.read_text(encoding="utf-8")
    report["body_composition"]["status"] = "pending"
    assert main(args) == 0
    assert json.loads(path.read_text(encoding="utf-8"))["data_outcome"] == "not_ready"


def test_hrv_partial_and_coach_waiting_reports_use_actual_arguments(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main

    cases = [
        ("scheduled-hrv-backfill.yml", "pipeline.hrv_backfill",
         ["--max-days", "6", "--retry-failures"], {"max_days": 6, "retry_failures": True},
         {"schema_version": 1, "status": "active", "attempted_this_run": 2,
          "completed_this_run": [{"date": "private-date"}], "failed_this_run": [{"error": "private-error"}]}, "partial"),
        ("coach-input-backfill.yml", "pipeline.coach_backfill",
         ["--max-activities", "7"], {"max_activities": 7},
         {"schema_version": 1, "status": "waiting_for_profile", "attempted_this_run": 0}, "not_ready"),
    ]
    for workflow, module, arguments, expected, report, outcome in cases:
        command = importlib.import_module(module)
        observed = []
        def operation(*args, result=report, calls=observed, **kwargs):
            calls.append(kwargs)
            result["updated_at"] = datetime.now(timezone.utc).isoformat()
            return result
        _stub_data_command(monkeypatch, command, operation)
        path = tmp_path / workflow
        assert main(["--workflow", workflow, "--receipt", str(path), "--", *arguments]) == 0
        receipt = json.loads(path.read_text(encoding="utf-8"))
        assert receipt["data_outcome"] == outcome
        assert receipt["counters"]["attempted_this_run"] == report["attempted_this_run"]
        if workflow == "scheduled-hrv-backfill.yml":
            assert receipt["counters"]["completed_this_run_count"] == 1
            assert receipt["counters"]["failed_this_run_count"] == 1
        assert len(observed) == 1
        assert all(observed[0][key] == value for key, value in expected.items())
        assert "private" not in path.read_text(encoding="utf-8")


def test_refresh_report_partial_and_failed_preserve_native_success(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.refresh")
    report = {"schema_version": 1, "kind": "refresh-diagnostics", "status": "partial"}
    observed = []
    def operation(request, **kwargs):
        observed.append(request)
        return report
    monkeypatch.setattr(command, "run", operation)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    path = tmp_path / "refresh.json"
    args = ["--workflow", "refresh.yml", "--receipt", str(path), "--", "--workflow"]
    for status, expected in (("partial", "partial"), ("failed", "error")):
        report["status"] = status
        assert main(args) == 0
        receipt = json.loads(path.read_text(encoding="utf-8"))
        assert receipt["data_outcome"] == expected
        assert receipt["command_outcome"] == "success"
    assert len(observed) == 2
    assert observed[0].mode == "general"
    assert observed[0].run_id == "123"


def test_nested_reference_pending_is_visible_after_hrv_source_complete(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.hrv_backfill")
    report = {"schema_version": 1, "status": "complete", "attempted_this_run": 4,
              "updated_at": "2001-01-01T00:00:00+00:00",
              "baseline_refresh": {"status": "bounded_pending", "pending": 2}}
    _stub_data_command(monkeypatch, command, lambda *args, **kwargs: report)
    path = tmp_path / "hrv.json"
    assert main(["--workflow", "scheduled-hrv-backfill.yml", "--receipt", str(path), "--"]) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == "partial"
    assert "attempted_this_run" not in receipt["counters"]


def test_validated_context_and_summary_never_infer_a_scheduled_occurrence(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main
    from pipeline.actions_receipt_schema import validate_receipt

    command = importlib.import_module("pipeline.activity_refresh")
    _stub_data_command(monkeypatch, command, lambda *args, **kwargs: {
        "schema_version": 1, "activities_checked": 0, "by_status": {}})
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("ACTIONS_CRON_EXPRESSION", "15 23 * * *")
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    path = tmp_path / "receipt.json"
    args = ["--workflow", "activity-refresh.yml", "--receipt", str(path), "--"]
    assert main(args) == 0
    receipt = validate_receipt(json.loads(path.read_text(encoding="utf-8")))
    assert receipt["context_status"] == "valid"
    assert receipt["run_id"] == "123"
    assert receipt["run_attempt"] == 2
    assert receipt["head_sha"] == "a" * 40
    assert receipt["event"] == "schedule"
    assert receipt["cron_expression"] == "15 23 * * *"
    assert receipt["scheduled_for"] is None
    assert receipt["occurrence_id"] is None
    assert receipt["job_conclusion"] is None
    assert receipt["data_outcome"] == "no_work"
    summary_text = summary.read_text(encoding="utf-8")
    for value in ("123", "2", "a" * 40, "schedule", "15 23 * * *", receipt["started_at"], receipt["finished_at"]):
        assert value in summary_text
    monkeypatch.delenv("ACTIONS_CRON_EXPRESSION")
    assert main(args) == 0
    receipt = validate_receipt(json.loads(path.read_text(encoding="utf-8")))
    assert receipt["cron_expression"] is None
    assert receipt["context_status"] == "missing"


def test_malformed_context_is_dropped_while_original_job_runs(monkeypatch, tmp_path, capsys):
    from pipeline.actions_receipt import main
    from pipeline.actions_receipt_schema import validate_receipt

    command = importlib.import_module("pipeline.activity_refresh")
    calls = []
    def operation(*args, **kwargs):
        calls.append(1)
        return {"schema_version": 1, "activities_checked": 0, "by_status": {}}
    _stub_data_command(monkeypatch, command, operation)
    monkeypatch.setenv("GITHUB_RUN_ID", "0123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1000001")
    monkeypatch.setenv("GITHUB_SHA", "A" * 40)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "private-event")
    monkeypatch.setenv("ACTIONS_CRON_EXPRESSION", "private-cron")
    path = tmp_path / "receipt.json"
    assert main(["--workflow", "activity-refresh.yml", "--receipt", str(path), "--"]) == 0
    receipt = validate_receipt(json.loads(path.read_text(encoding="utf-8")))
    assert receipt["context_status"] == "invalid"
    assert receipt["run_id"] is None
    assert receipt["run_attempt"] is None
    assert receipt["head_sha"] == "a" * 40
    assert receipt["event"] == "unknown"
    assert receipt["cron_expression"] is None
    assert len(calls) == 1
    output = capsys.readouterr()
    assert "private" not in output.out + output.err + path.read_text(encoding="utf-8")


def test_unknown_report_shapes_still_emit_an_honest_receipt(monkeypatch, tmp_path):
    from pipeline.actions_receipt import main
    from pipeline.actions_receipt_schema import validate_receipt

    command = importlib.import_module("pipeline.activity_refresh")
    for index, report in enumerate((
        {"schema_version": 99, "activities_checked": 0, "by_status": {}},
        {"schema_version": True, "activities_checked": 0, "by_status": {}},
        {"schema_version": 1, "activities_checked": 0, "by_status": {"error": "private-error"}},
        {"schema_version": 1, "activities_checked": 2**54, "by_status": {}},
        {"schema_version": 1, "activities_checked": 0, "by_status": {"private-status": 0}},
        None,
    )):
        _stub_data_command(monkeypatch, command, lambda *args, result=report, **kwargs: result)
        path = tmp_path / f"unknown-{index}.json"
        assert main(["--workflow", "activity-refresh.yml", "--receipt", str(path), "--"]) == 0
        receipt = validate_receipt(json.loads(path.read_text(encoding="utf-8")))
        assert receipt["data_outcome"] == "unknown"
    refresh = importlib.import_module("pipeline.refresh")
    monkeypatch.setattr(refresh, "run", lambda *args, **kwargs: {"schema_version": 1, "status": ["complete"]})
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    path = tmp_path / "unknown-refresh.json"
    assert main(["--workflow", "refresh.yml", "--receipt", str(path), "--", "--workflow"]) == 0
    assert json.loads(path.read_text(encoding="utf-8"))["data_outcome"] == "unknown"


def test_secondary_write_failures_never_replace_native_exit_or_rerun(monkeypatch, tmp_path, capsys):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.activity_refresh")
    calls = []
    def operation(*args, **kwargs):
        calls.append(1)
        raise SystemExit(7)
    _stub_data_command(monkeypatch, command, operation)
    # Both explicit paths are directories, producing real filesystem write failures.
    receipt_directory = tmp_path / "private-receipt"
    receipt_directory.mkdir()
    summary_directory = tmp_path / "private-summary"
    summary_directory.mkdir()
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_directory))
    previous_argv = sys.argv
    assert main(["--workflow", "activity-refresh.yml", "--receipt", str(receipt_directory), "--"]) == 7
    assert sys.argv is previous_argv
    assert calls == [1]
    output = capsys.readouterr()
    receipt = json.loads(output.out)
    assert receipt["native_exit"] == 7
    assert receipt["command_outcome"] == "failure"
    assert receipt["data_outcome"] == "error"
    assert output.err == "Actions receipt unavailable.\nActions receipt unavailable.\n"
    assert "private" not in output.out + output.err


@pytest.mark.parametrize("mode,native,outcome", [
    ("complete", 0, "success"), ("partial", 0, "success"),
    ("failure", 1, "failure"), ("nonzero", 7, "failure"),
    ("interrupt", 130, "interrupted"),
    ("invalid", 2, "failure"),
])
def test_public_module_entry_preserves_process_exit_and_privacy(tmp_path, mode, native, outcome):
    # Only the explicitly agreed data-command boundary is fake. Real argparse,
    # existing safe_cli, module entry, JSON and filesystem execute in a child.
    driver = r'''
import importlib, runpy, socket, sys

def blocked(*args, **kwargs):
    raise AssertionError('External systems forbidden in synthetic receipt tests')
socket.create_connection = blocked
socket.socket.connect = blocked
command = importlib.import_module('pipeline.activity_refresh')
mode, path = sys.argv[1:]
calls = []
def operation(*args, **kwargs):
    calls.append(1)
    assert kwargs['lookback_days'] == 7
    assert kwargs['max_activities'] == 3
    if mode == 'failure': raise RuntimeError('PRIVATE_MARKER email@example.invalid private/key')
    if mode == 'nonzero': raise SystemExit(7)
    if mode == 'interrupt': raise KeyboardInterrupt()
    return {'schema_version': 1, 'activities_checked': 1,
            'by_status': {'error': 1} if mode == 'partial' else {'unchanged': 1},
            'activities': [{'id': 'PRIVATE_MARKER', 'date': 'private-date'}]}
setattr(command, "run_command" if hasattr(command, "run_command") else "run", operation)
sys.argv = ['pipeline.actions_receipt', '--workflow', 'activity-refresh.yml', '--receipt', path,
            '--', '--lookback-days', '7', '--max-activities', '3']
if mode == 'invalid': sys.argv.append('--PRIVATE_MARKER')
try:
    runpy.run_module('pipeline.actions_receipt', run_name='__main__')
finally:
    assert len(calls) == (0 if mode == 'invalid' else 1)
'''
    path = tmp_path / "receipt.json"
    result = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-c", driver, mode, str(path)],
        cwd=Path(__file__).parents[1], capture_output=True, text=True, timeout=20,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert result.returncode == native, result.stderr
    receipt = json.loads(path.read_text(encoding="utf-8"))
    stdout_receipts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{") and json.loads(line).get("kind") == "actions-run-receipt"]
    assert stdout_receipts == [receipt]
    assert receipt["native_exit"] == native
    assert receipt["command_outcome"] == outcome
    assert receipt["data_outcome"] == ("partial" if mode == "partial" else "error" if native else "complete")
    output = result.stdout + result.stderr + path.read_text(encoding="utf-8")
    assert "PRIVATE_MARKER" not in output
    assert "email@example.invalid" not in output
    assert "private/key" not in output
    assert "private-date" not in output
    assert "Traceback" not in output


@pytest.mark.parametrize("workflow,module,status,expected", [
    ("scheduled-activity-backfill.yml", "activity_backfill_scheduler", "complete_with_blocked", "partial"),
    ("scheduled-hrv-backfill.yml", "hrv_backfill", "complete_with_blocked", "partial"),
    ("scheduled-hrv-backfill.yml", "hrv_backfill", "complete", "no_work"),
    ("coach-input-backfill.yml", "coach_backfill", "active", "partial"),
    ("coach-input-backfill.yml", "coach_backfill", "complete_with_blocked", "partial"),
    ("coach-input-backfill.yml", "coach_backfill", "complete", "no_work"),
    ("coach-input-backfill.yml", "coach_backfill", "private-status", "unknown"),
    ("scheduled-activity-backfill.yml", "activity_backfill_scheduler", "unsupported", "unsupported"),
])
def test_known_report_statuses_preserve_data_meaning(monkeypatch, tmp_path, workflow, module, status, expected):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline." + module)
    def operation(*args, **kwargs):
        now = datetime.now(timezone.utc).isoformat()
        return {"schema_version": 2 if module == "activity_backfill_scheduler" else 1,
                "status": status, "updated_at": now, "last_run_at": now,
                "attempted_this_run": 0, "last_run_ranges": []}
    _stub_data_command(monkeypatch, command, operation)
    path = tmp_path / "receipt.json"
    assert main(["--workflow", workflow, "--receipt", str(path), "--"]) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == expected
    assert receipt["command_outcome"] == "success"


@pytest.mark.parametrize("module,workflow", [
    ("activity_backfill_scheduler", "scheduled-activity-backfill.yml"),
    ("hrv_backfill", "scheduled-hrv-backfill.yml"),
])
def test_passive_stored_plan_counters_are_not_claimed_for_this_run(monkeypatch, tmp_path, module, workflow):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline." + module)
    _stub_data_command(monkeypatch, command, lambda *args, **kwargs: {
        "schema_version": 2 if module == "activity_backfill_scheduler" else 1,
        "status": "complete", "updated_at": "2001-01-01T00:00:00+00:00",
        "last_run_at": "2001-01-01T00:00:00+00:00", "last_run_ranges": [],
        "attempted_this_run": 0, "completed_this_run": [{"date": "private-date"}],
    })
    path = tmp_path / "receipt.json"
    assert main(["--workflow", workflow, "--receipt", str(path), "--"]) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == "complete"
    assert "attempted_this_run" not in receipt["counters"]
    assert "completed_this_run_count" not in receipt["counters"]
    assert "last_run_ranges_count" not in receipt["counters"]


def test_public_module_help_and_fixed_workflow_allowlist(tmp_path):
    for arguments, expected in ((["--help"], 0), (["--workflow", "private-module", "--receipt", str(tmp_path / "receipt.json"), "--"], 2)):
        result = subprocess.run(
            [sys.executable, "-B", "-X", "utf8", "-m", "pipeline.actions_receipt", *arguments],
            cwd=Path(__file__).parents[1], capture_output=True, text=True, timeout=20,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        assert result.returncode == expected
        assert "private-module" not in result.stdout + result.stderr
        assert "Traceback" not in result.stderr


@pytest.mark.parametrize("timestamp", ["old", "missing", "fresh"])
def test_coach_per_run_counters_require_current_invocation_evidence(monkeypatch, tmp_path, timestamp):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.coach_backfill")
    observed = []
    def operation(*args, **kwargs):
        observed.append(kwargs)
        report = {
            "schema_version": 1, "status": "complete", "attempted_this_run": 3,
            "generated_this_run": 2, "completed_this_run": [{"id": "private-a"}, {"id": "private-b"}],
            "skipped_this_run": [{"id": "private-c"}], "processed_source_count": 17,
            "remaining_activities": 0, "profile_count": 1,
        }
        if timestamp == "old":
            report["updated_at"] = "2001-01-01T00:00:00+00:00"
        elif timestamp == "fresh":
            report["updated_at"] = datetime.now(timezone.utc).isoformat()
        return report
    monkeypatch.setattr(command, "run", operation)
    path = tmp_path / "coach.json"
    assert main(["--workflow", "coach-input-backfill.yml", "--receipt", str(path), "--",
                 "--max-activities", "5"]) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["native_exit"] == 0
    assert receipt["command_outcome"] == "success"
    assert receipt["data_outcome"] == "complete"
    expected = {"processed_source_count": 17, "remaining_activities": 0, "profile_count": 1}
    if timestamp == "fresh":
        expected.update(attempted_this_run=3, generated_this_run=2,
                        completed_this_run_count=2, skipped_this_run_count=1)
    assert receipt["counters"] == expected
    assert observed == [{"max_activities": 5}]
    assert "private-" not in path.read_text(encoding="utf-8")


@pytest.mark.parametrize("checked,statuses,expected", [
    (1, {"unsupported": 1}, "unsupported"),
    (2, {"unsupported": 1, "unchanged": 1}, "complete"),
    (2, {"unsupported": 1, "error": 1}, "partial"),
    (0, {}, "no_work"),
    (1, {"refreshed": 1}, "complete"),
])
def test_activity_all_unsupported_preserves_native_and_other_outcomes(monkeypatch, tmp_path, checked, statuses, expected):
    from pipeline.actions_receipt import main

    command = importlib.import_module("pipeline.activity_refresh")
    observed = []
    def operation(*args, **kwargs):
        observed.append(kwargs)
        return {"schema_version": 1, "activities_checked": checked, "by_status": statuses}
    _stub_data_command(monkeypatch, command, operation)
    path = tmp_path / "activity.json"
    assert main(["--workflow", "activity-refresh.yml", "--receipt", str(path), "--",
                 "--lookback-days", "4", "--max-activities", "9", "--activity-id", "synthetic", "--force"]) == 0
    receipt = json.loads(path.read_text(encoding="utf-8"))
    assert receipt["data_outcome"] == expected
    assert receipt["native_exit"] == 0
    assert receipt["command_outcome"] == "success"
    assert receipt["counters"]["activities_checked"] == checked
    assert len(observed) == 1
    assert observed[0]["lookback_days"] == 4
    assert observed[0]["max_activities"] == 9
    assert observed[0]["activity_id"] == "synthetic"
    assert observed[0]["force"] is True
