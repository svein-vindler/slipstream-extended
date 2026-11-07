"""Synthetic, pinned JSON at the public offline reconciliation CLI."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def pin(directory, name, value):
    data = json.dumps(value).encode()
    (directory / name).write_bytes(data)
    return {"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def manifest(directory, schedules=None, **changes):
    config = pin(directory, "config.json", {
        "workflow": "refresh.yml",
        "schedules": schedules or [{"cron_expression": "0 23 * * *", "timezone": "UTC"}],
    })
    value = {
        "schema_version": 1, "kind": "actions-reconcile-input",
        "day_start": "2026-10-24", "day_end": "2026-10-24",
        "as_of": "2026-10-26T12:00:00Z",
        "observation_start": "2026-10-24T00:00:00Z",
        "observation_end": "2026-10-26T12:00:00Z",
        "delay_window_minutes": 120, "workflow_configs": [config],
        "runs": [], "jobs": [], "receipts": [], "trigger_evidence": [], "pauses": [],
    }
    value.update(changes)
    (directory / "manifest.json").write_text(json.dumps(value), encoding="utf-8")
    return value


def run_cli(directory):
    return subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-m", "pipeline.actions_reconcile",
         "--input", str(directory / "manifest.json"), "--output", str(directory / "report.json")],
        cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=30,
    )


def report(directory):
    result = run_cli(directory)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == ""
    assert result.stderr == ""
    return json.loads((directory / "report.json").read_text(encoding="utf-8"))


def run_record(**changes):
    value = {
        "workflow": "refresh.yml", "run_id": "101", "run_attempt": 1,
        "head_sha": "a" * 40, "event": "schedule", "status": "completed",
        "conclusion": "success", "created_at": "2026-10-25T00:05:00Z",
        "run_started_at": "2026-10-25T00:06:00Z",
    }
    value.update(changes)
    return value


def test_standard_github_run_does_not_prove_a_cron_occurrence(tmp_path):
    runs = [pin(tmp_path, "run.json", run_record())]
    manifest(tmp_path, runs=runs)
    value = report(tmp_path)
    assert value["occurrences"] == [{
        "workflow": "refresh.yml", "scheduled_for": "2026-10-24T23:00:00Z",
        "status": "not_observed_after_window", "reason": "no_occurrence_proof",
        "coverage": "covered", "run_ids": [],
    }]
    assert value["unassigned_runs"] == [{
        "workflow": "refresh.yml", "run_id": "101", "run_attempt": 1,
        "head_sha": "a" * 40, "event": "schedule", "status": "completed",
        "conclusion": "success", "data_outcome": "unknown",
        "reason": "trigger_evidence_unavailable",
        "candidate_scheduled_for": ["2026-10-24T23:00:00Z"],
    }]
    assert value["limitations"] == [
        "supplied_observations_only", "absence_does_not_prove_github_dropped",
        "configured_schedule_is_not_trigger_evidence",
    ]


@pytest.mark.parametrize(("day", "cron", "zone", "times"), [
    ("2026-10-25", "30 2 * * *", "Europe/Oslo", ["2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z"]),
    ("2026-03-29", "30 2 * * *", "Europe/Oslo", []),
    ("2026-10-24", "0,30 0-1/1 * OCT SAT", "UTC", [
        "2026-10-24T00:00:00Z", "2026-10-24T00:30:00Z", "2026-10-24T01:00:00Z", "2026-10-24T01:30:00Z"]),
    ("2026-10-24", "0 0 24 * MON", "UTC", ["2026-10-24T00:00:00Z"]),
])
def test_expected_slots_follow_posix_cron_and_real_utc_dst_minutes(tmp_path, day, cron, zone, times):
    manifest(tmp_path, schedules=[{"cron_expression": cron, "timezone": zone}], day_start=day, day_end=day,
             observation_start=day + "T00:00:00Z")
    value = report(tmp_path)
    assert [item["scheduled_for"] for item in value["occurrences"]] == times


@pytest.mark.parametrize("schedule", [
    {"cron_expression": "@daily", "timezone": "UTC"},
    {"cron_expression": "0 0 * * *", "timezone": "not-a-zone"},
    {"cron_expression": "0 0 * * *"},
])
def test_unsupported_schedule_is_preserved_without_guessing(tmp_path, schedule):
    manifest(tmp_path, schedules=[schedule])
    value = report(tmp_path)
    assert value["occurrences"] == []
    assert value["unsupported_schedules"] == [{"workflow": "refresh.yml", "reason": "unsupported_schedule"}]


def test_spring_forward_cannot_claim_no_expected_runs_from_a_nonexistent_local_time(tmp_path):
    manifest(tmp_path, schedules=[{"cron_expression": "30 2 * * *", "timezone": "Europe/Oslo"}],
             day_start="2026-03-29", day_end="2026-03-29", observation_start="2026-03-29T00:00:00Z")
    value = report(tmp_path)
    assert value["expected_complete"] is False
    assert value["unsupported_schedules"] == [{
        "workflow": "refresh.yml", "reason": "dst_skipped_hour_unresolved",
    }]


def trigger(directory, run, when="2026-10-24T23:00:00Z", cron="0 23 * * *", name="trigger"):
    value = {key: run[key] for key in ("workflow", "run_id", "run_attempt", "head_sha", "event")}
    value.update(scheduled_for=when, cron_expression=cron,
                 provenance="github-trigger-metadata", source_field="scheduled_for")
    source = {**value, "kind": "github-trigger-metadata"}
    value["source_ref"] = pin(directory, name + "-source.json", source)
    return pin(directory, name + ".json", value)


def receipt(**changes):
    value = {
        "schema_version": 1, "kind": "actions-run-receipt", "workflow": "refresh.yml",
        "run_id": "101", "run_attempt": 1, "head_sha": "a" * 40, "event": "schedule",
        "cron_expression": "0 */6 * * *", "scheduled_for": None, "occurrence_id": None,
        "trigger_provenance": "unavailable", "started_at": "2026-10-25T00:06:00Z",
        "finished_at": "2026-10-25T00:07:00Z", "native_exit": 0,
        "command_outcome": "success", "job_conclusion": None, "data_outcome": "partial",
        "counters": {"refreshed_count": 1}, "context_status": "valid",
    }
    value.update(changes)
    return value


def test_actual_pinned_trigger_binds_cross_midnight_run_and_rerun_once(tmp_path):
    run = run_record()
    rerun = run_record(run_attempt=2)
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run), pin(tmp_path, "rerun.json", rerun)],
             trigger_evidence=[trigger(tmp_path, run)])
    value = report(tmp_path)
    assert value["occurrences"] == [{
        "workflow": "refresh.yml", "scheduled_for": "2026-10-24T23:00:00Z",
        "status": "observed", "reason": "explicit_trigger_evidence", "coverage": "covered", "run_ids": ["101"],
    }]
    assert [(item["run_id"], item["run_attempt"], item["data_outcome"]) for item in value["observed_runs"]] == [
        ("101", 1, "unknown"), ("101", 2, "unknown"),
    ]
    assert value["unassigned_runs"] == []


@pytest.mark.parametrize("event", ["workflow_dispatch", "workflow_run"])
def test_manual_and_downstream_runs_cannot_supply_cron_proof(tmp_path, event):
    run = run_record(event=event)
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)],
             trigger_evidence=[trigger(tmp_path, {**run, "event": "schedule"})])
    value = report(tmp_path)
    assert value["observed_runs"] == []
    assert value["occurrences"][0]["status"] == "not_observed_after_window"
    assert value["unassigned_runs"][0]["candidate_scheduled_for"] == []
    assert value["unassigned_runs"][0]["reason"] == "not_a_schedule_event"


def test_temporal_candidates_never_assign_multiple_possible_cron_slots(tmp_path):
    run = run_record(created_at="2026-10-24T23:30:00Z")
    manifest(tmp_path, schedules=[{"cron_expression": "0 22,23 * * *", "timezone": "UTC"}],
             runs=[pin(tmp_path, "run.json", run)])
    value = report(tmp_path)
    assert value["observed_runs"] == []
    assert value["unassigned_runs"][0]["reason"] == "multiple_candidate_occurrences"
    assert value["unassigned_runs"][0]["candidate_scheduled_for"] == ["2026-10-24T22:00:00Z", "2026-10-24T23:00:00Z"]


@pytest.mark.parametrize(("conclusion", "outcome", "reason"), [
    ("success", "partial", "receipt_data_outcome"),
    ("failure", "unknown", "status_conflict"),
])
def test_job_success_never_promotes_partial_receipt(tmp_path, conclusion, outcome, reason):
    run = run_record()
    job = {"workflow": "refresh.yml", "run_id": "101", "run_attempt": 1, "conclusion": conclusion}
    manifest(tmp_path, schedules=[{"cron_expression": "0 */6 * * *", "timezone": "UTC"}],
             delay_window_minutes=480, runs=[pin(tmp_path, "run.json", run)],
             jobs=[pin(tmp_path, "job.json", job)], receipts=[pin(tmp_path, "receipt.json", receipt())],
             trigger_evidence=[trigger(tmp_path, run, "2026-10-24T18:00:00Z", "0 */6 * * *")])
    value = report(tmp_path)
    assert value["observed_runs"][0]["data_outcome"] == outcome
    assert value["observed_runs"][0]["data_reason"] == reason


def test_pause_is_explicit_bounded_and_unobserved_window_remains_incomplete(tmp_path):
    pause = {"workflow": "refresh.yml", "paused_from": "2026-10-24T22:00:00Z",
             "paused_until": "2026-10-24T23:30:00Z", "provenance": "operator-record"}
    manifest(tmp_path, pauses=[pin(tmp_path, "pause.json", pause)],
             observation_start="2026-10-25T00:00:00Z")
    value = report(tmp_path)
    assert value["occurrences"][0]["status"] == "known_paused"
    assert value["occurrences"][0]["coverage"] == "incomplete"
    assert value["occurrences"][0]["reason"] == "explicit_pause_window"


def test_future_slot_is_not_yet_due_and_partial_coverage_is_explicit(tmp_path):
    manifest(tmp_path, as_of="2026-10-24T23:30:00Z", observation_end="2026-10-24T23:30:00Z")
    assert report(tmp_path)["occurrences"] == [{
        "workflow": "refresh.yml", "scheduled_for": "2026-10-24T23:00:00Z",
        "status": "not_yet_due", "reason": "delay_window_open", "coverage": "incomplete", "run_ids": [],
    }]


def test_two_different_run_ids_for_one_proven_slot_remain_ambiguous(tmp_path):
    first, second = run_record(), run_record(run_id="102")
    manifest(tmp_path, runs=[pin(tmp_path, "a.json", first), pin(tmp_path, "b.json", second)],
             trigger_evidence=[trigger(tmp_path, first), trigger(tmp_path, second, name="second")])
    value = report(tmp_path)
    assert value["occurrences"][0]["status"] == "ambiguous"
    assert value["occurrences"][0]["reason"] == "multiple_runs_for_occurrence"
    assert value["occurrences"][0]["run_ids"] == ["101", "102"]


def test_malformed_receipt_is_unknown_and_foreign_fields_never_leak(tmp_path):
    run = run_record(secret="PRIVATE_SENTINEL", object_key="PRIVATE_SENTINEL")
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)],
             receipts=[pin(tmp_path, "receipt.json", {"health": "PRIVATE_SENTINEL"})],
             trigger_evidence=[trigger(tmp_path, run)])
    value = report(tmp_path)
    assert value["observed_runs"][0]["data_outcome"] == "unknown"
    assert value["invalid_receipt_count"] == 1
    assert "PRIVATE_SENTINEL" not in (tmp_path / "report.json").read_text()
    assert str(tmp_path) not in (tmp_path / "report.json").read_text()


def test_closed_delay_with_incomplete_observations_is_ambiguous(tmp_path):
    manifest(tmp_path, observation_start="2026-10-25T00:00:00Z")
    assert report(tmp_path)["occurrences"] == [{
        "workflow": "refresh.yml", "scheduled_for": "2026-10-24T23:00:00Z",
        "status": "ambiguous", "reason": "observation_coverage_incomplete",
        "coverage": "incomplete", "run_ids": [],
    }]


def test_overlapping_schedules_do_not_claim_one_certain_github_trigger(tmp_path):
    manifest(tmp_path, schedules=[{"cron_expression": "0 23 * * *", "timezone": "UTC"},
                                  {"cron_expression": "0 23 24 * *", "timezone": "UTC"}])
    value = report(tmp_path)
    assert value["expected_complete"] is False
    assert value["unsupported_schedules"] == [{"workflow": "refresh.yml", "reason": "overlapping_schedules_ambiguous"}]


def test_failed_command_cannot_claim_complete_data(tmp_path):
    run = run_record(conclusion="failure")
    job = {"workflow": "refresh.yml", "run_id": "101", "run_attempt": 1, "conclusion": "failure"}
    failed = receipt(native_exit=1, command_outcome="failure", data_outcome="complete")
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)],
             jobs=[pin(tmp_path, "job.json", job)], receipts=[pin(tmp_path, "receipt.json", failed)],
             trigger_evidence=[trigger(tmp_path, run)])
    value = report(tmp_path)
    assert value["observed_runs"][0]["data_outcome"] == "unknown"
    assert value["observed_runs"][0]["data_reason"] == "status_conflict"


@pytest.mark.parametrize("conclusion", ["skipped", "neutral"])
def test_skipped_or_neutral_job_cannot_prove_a_successful_data_command(tmp_path, conclusion):
    run = run_record()
    job = {"workflow": "refresh.yml", "run_id": "101", "run_attempt": 1, "conclusion": conclusion}
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)], jobs=[pin(tmp_path, "job.json", job)],
             receipts=[pin(tmp_path, "receipt.json", receipt(data_outcome="complete"))],
             trigger_evidence=[trigger(tmp_path, run)])
    value = report(tmp_path)
    assert value["observed_runs"][0]["data_outcome"] == "unknown"
    assert value["observed_runs"][0]["data_reason"] == "status_conflict"


def test_conflicting_receipts_with_same_data_outcome_are_unknown(tmp_path):
    run = run_record()
    job = {"workflow": "refresh.yml", "run_id": "101", "run_attempt": 1, "conclusion": "success"}
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)], jobs=[pin(tmp_path, "job.json", job)],
             receipts=[pin(tmp_path, "receipt-a.json", receipt()),
                       pin(tmp_path, "receipt-b.json", receipt(counters={"refreshed_count": 2}))],
             trigger_evidence=[trigger(tmp_path, run)])
    value = report(tmp_path)
    assert value["observed_runs"][0]["data_outcome"] == "unknown"
    assert value["observed_runs"][0]["data_reason"] == "receipt_conflict"


@pytest.mark.parametrize(("finished_at", "available_as_of"), [
    ("2026-10-25T00:06:59Z", True),
    ("2026-10-25T00:07:00Z", True),
    ("2026-10-25T00:07:01Z", False),
])
@pytest.mark.parametrize("data_outcome", ["complete", "no_work"])
@pytest.mark.parametrize("has_trigger_evidence", [False, True])
def test_receipt_after_audit_as_of_cannot_prove_a_finished_data_outcome(
    tmp_path, finished_at, available_as_of, data_outcome, has_trigger_evidence,
):
    run = run_record()
    job = {"workflow": "refresh.yml", "run_id": "101", "run_attempt": 1, "conclusion": "success"}
    manifest(tmp_path, as_of="2026-10-25T00:07:00Z", observation_end="2026-10-25T00:07:00Z",
             runs=[pin(tmp_path, "run.json", run)], jobs=[pin(tmp_path, "job.json", job)],
             receipts=[pin(tmp_path, "receipt.json", receipt(
                 finished_at=finished_at, data_outcome=data_outcome))],
             trigger_evidence=[trigger(tmp_path, run)] if has_trigger_evidence else [])
    value = report(tmp_path)
    collection = "observed_runs" if has_trigger_evidence else "unassigned_runs"
    assert value[collection][0]["data_outcome"] == (data_outcome if available_as_of else "unknown")
    assert value["occurrences"][0]["status"] == ("observed" if has_trigger_evidence else "not_yet_due")
    if has_trigger_evidence:
        assert value["observed_runs"][0]["data_reason"] == (
            "receipt_data_outcome" if available_as_of else "receipt_after_as_of")
        assert value["observed_runs"][0]["counters"] == (
            {"refreshed_count": 1} if available_as_of else {})
    else:
        assert value["unassigned_runs"][0]["reason"] == "trigger_evidence_unavailable"


@pytest.mark.parametrize("case", [
    "pin_hash", "pin_bytes", "file_bound", "total_bound", "audit_bound", "delay_bound",
    "runs_bound", "jobs_bound", "receipts_bound", "workflows_bound", "pauses_bound", "triggers_bound",
    "source_mismatch", "future_run", "no_configuration", "output_is_input", "duplicate_json_keys", "missing_file",
    "occurrence_bound",
])
def test_invalid_or_unbounded_input_fails_closed_with_a_fixed_safe_message(tmp_path, case):
    value = manifest(tmp_path)
    if case == "pin_hash":
        value["workflow_configs"][0]["sha256"] = "0" * 64
    elif case == "pin_bytes":
        value["workflow_configs"][0]["bytes"] += 1
    elif case == "file_bound":
        value["workflow_configs"] = [pin(tmp_path, "large.json", {"secret": "PRIVATE_SENTINEL" * 80000})]
    elif case == "total_bound":
        ref = pin(tmp_path, "large.json", {"secret": "PRIVATE_SENTINEL" * 65000})
        value["receipts"] = [ref] * 10
    elif case == "audit_bound":
        value["day_end"] = "2026-11-24"
    elif case == "delay_bound":
        value["delay_window_minutes"] = 10081
    elif case == "occurrence_bound":
        value["day_end"] = "2026-10-30"
        value["workflow_configs"] = [pin(tmp_path, "config.json", {
            "workflow": "refresh.yml", "schedules": [{"cron_expression": "* * * * *", "timezone": "UTC"}],
        })]
    elif case.endswith("_bound"):
        key, count = {"runs_bound": ("runs", 513), "jobs_bound": ("jobs", 2049),
                      "receipts_bound": ("receipts", 1025), "workflows_bound": ("workflow_configs", 17),
                      "pauses_bound": ("pauses", 129), "triggers_bound": ("trigger_evidence", 513)}[case]
        value[key] = [{}] * count
    elif case == "source_mismatch":
        run = run_record()
        value["runs"] = [pin(tmp_path, "run.json", run)]
        value["trigger_evidence"] = [trigger(tmp_path, run)]
        source = json.loads((tmp_path / "trigger-source.json").read_text())
        source["run_id"] = "999"
        bad_source = pin(tmp_path, "trigger-source.json", source)
        proof = json.loads((tmp_path / "trigger.json").read_text())
        proof["source_ref"] = bad_source
        value["trigger_evidence"] = [pin(tmp_path, "trigger.json", proof)]
    elif case == "future_run":
        value["runs"] = [pin(tmp_path, "run.json", run_record(created_at="2026-10-27T00:00:00Z"))]
    elif case == "no_configuration":
        value["workflow_configs"] = []
    elif case == "output_is_input":
        (tmp_path / "report.json").write_text("PRIVATE_SENTINEL")
    elif case == "missing_file":
        value["workflow_configs"][0]["path"] = "PRIVATE_SENTINEL.json"
    (tmp_path / "manifest.json").write_text(json.dumps(value))
    if case == "duplicate_json_keys":
        (tmp_path / "manifest.json").write_text('{"PRIVATE_SENTINEL": 1, "PRIVATE_SENTINEL": 2}')
    result = run_cli(tmp_path)
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "actions_reconcile: invalid input or output\n"
    if case == "output_is_input":
        assert (tmp_path / "report.json").read_text() == "PRIVATE_SENTINEL"
    else:
        assert not (tmp_path / "report.json").exists()


@pytest.mark.parametrize("day,expected_utc", [
    ("2026-03-27", "2026-03-27T07:00:00Z"),
    ("2026-03-28", "2026-03-28T09:00:00Z"),
    ("2026-03-29", "2026-03-29T08:00:00Z"),
    ("2026-03-30", "2026-03-30T06:00:00Z"),
    ("2026-10-23", "2026-10-23T06:00:00Z"),
    ("2026-10-24", "2026-10-24T08:00:00Z"),
    ("2026-10-25", "2026-10-25T09:00:00Z"),
    ("2026-10-26", "2026-10-26T07:00:00Z"),
])
def test_native_morning_configs_keep_local_times_across_dst_in_real_offline_cli(tmp_path, day, expected_utc):
    configs = [pin(tmp_path, name + ".json", {
        "workflow": name,
        "schedules": [{"cron_expression": "0 8 * * 1-5", "timezone": "Europe/Oslo"},
                      {"cron_expression": "0 10 * * 0,6", "timezone": "Europe/Oslo"}],
    }) for name in ("refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml")]
    manifest(tmp_path, day_start=day, day_end=day, as_of=day + "T23:59:59Z",
             observation_start=day + "T00:00:00Z", observation_end=day + "T23:59:59Z",
             workflow_configs=configs)
    value = report(tmp_path)
    assert [(item["workflow"], item["scheduled_for"]) for item in value["occurrences"]] == [
        ("refresh.yml", expected_utc), ("scheduled-health-detail-backfill.yml", expected_utc),
        ("scheduled-hrv-backfill.yml", expected_utc),
    ]
    assert value["unsupported_schedules"] == []
    assert all(item["status"] == "not_observed_after_window" and item["reason"] == "no_occurrence_proof"
               for item in value["occurrences"])
    assert value["observed_runs"] == []


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("workflow,legacy_cron", [
    ("refresh.yml", "0 */6 * * *"),
    ("scheduled-health-detail-backfill.yml", "15 1,7,13,19 * * *"),
    ("scheduled-hrv-backfill.yml", "30 4,10,16,22 * * *"),
])
def test_legacy_and_morning_receipts_remain_readable_without_rewriting_facts(tmp_path, version, workflow, legacy_cron):
    run = run_record(workflow=workflow)
    value = receipt(workflow=workflow, schema_version=version, cron_expression=legacy_cron)
    if version == 2:
        value.update(cron_expression="0 10 * * 0,6", cron_timezone="Europe/Oslo")
    pinned_receipt = pin(tmp_path, "receipt.json", value)
    receipt_before = (tmp_path / "receipt.json").read_bytes()
    job = {"workflow": workflow, "run_id": "101", "run_attempt": 1, "conclusion": "success"}
    manifest(tmp_path, runs=[pin(tmp_path, "run.json", run)], receipts=[pinned_receipt],
             jobs=[pin(tmp_path, "job.json", job)])
    audit = report(tmp_path)
    assert audit["unassigned_runs"][0]["data_outcome"] == "partial"
    assert audit["unassigned_runs"][0]["run_id"] == "101"
    assert audit["observed_runs"] == []
    assert (tmp_path / "receipt.json").read_bytes() == receipt_before
