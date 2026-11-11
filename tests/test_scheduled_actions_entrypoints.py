"""Hosted entrypoints with real pipeline code and synthetic external services.

These tests qualify code *after* GitHub creates a run. They do not model or
prove delivery by GitHub's internal cron scheduler. The child uses actual
actions_receipt module execution, command argparse, pipeline operations and
R2Store; only service clients, login and clocks are replaced.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
PRIVATE_MARKER = "SYNTHETIC_PRIVATE_PROVIDER_TEXT"


def _synthetic_environment():
    values = dict(os.environ)
    for name in tuple(values):
        if name.startswith(("GARMIN", "GARMINTOKENS", "R2_", "CLOUDFLARE_", "AWS_", "BOTO_", "GITHUB_")):
            values.pop(name, None)
    for name in ("BOTO_CONFIG", "DOTENV_CONFIG_PATH", "DOTENV_CONFIG_ENCODING"):
        values.pop(name, None)
    values.update(PYTHONDONTWRITEBYTECODE="1", AWS_EC2_METADATA_DISABLED="true")
    return values


def _child_main(workflow, event, scenario, directory):
    # Direct pytest/CI is safe independently of the local qualification runner.
    # Remove selectors by name before importing a provider/storage SDK.
    clean = _synthetic_environment()
    os.environ.clear()
    os.environ.update(clean)
    import importlib
    import runpy
    import socket
    from contextlib import ExitStack
    from datetime import date, datetime, timezone
    from unittest.mock import patch

    import boto3
    from garminconnect import Garmin

    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected external network access")

    socket.socket.connect = forbidden
    socket.create_connection = forbidden
    directory = Path(directory)
    os.chdir(directory)
    # Each value is synthetic. No real secret or provider profile is read.
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": "synthetic",
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
        "R2_BUCKET": "synthetic-entrypoints",
        "GARMIN_PROVIDER": "legacy",
        "HEALTH_TIMEZONE": "Europe/Oslo",
        "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_EVENT_NAME": event,
        "REFRESH_EVENT": event,
        "GITHUB_STEP_SUMMARY": str(directory / "summary.md"),
    }.items():
        os.environ[name] = value
    for name in ("ACTIVITY_ONLY", "NIGHT_ONLY", "INCLUDE_GRANULAR",
                 "LATEST_R2_ONLY", "HEALTH_START", "HEALTH_END",
                 "BACKFILL_START_YEAR", "BACKFILL_END_YEAR", "SYNC_REQUEST_ID"):
        os.environ.pop(name, None)
    crons = {
        "refresh.yml": "0 8 * * 1-5",
        "scheduled-health-detail-backfill.yml": "0 8 * * 1-5",
        "scheduled-hrv-backfill.yml": "0 8 * * 1-5",
        "activity-refresh.yml": "15 23 * * *",
        "scheduled-activity-backfill.yml": "30 2,8,14,20 * * *",
        "coach-input-backfill.yml": "40 2,8,14,20 * * *",
    }
    if event == "schedule":
        os.environ["ACTIONS_CRON_EXPRESSION"] = crons[workflow]
    else:
        os.environ.pop("ACTIONS_CRON_EXPRESSION", None)

    from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

    class Source(SyntheticGarmin):
        def get_activities_by_date(self, *args, **kwargs):
            self.calls.append(("activities", "synthetic"))
            if scenario == "source_failure":
                raise RuntimeError(PRIVATE_MARKER + " email@example.invalid private/key")
            if scenario == "native_failure":
                raise SystemExit(7)
            return []

        def connectapi(self, *args, **kwargs):
            raise AssertionError("Unexpected SDK transport call")

        def get_daily_steps(self, first, last):
            return [{"calendarDate": first, "totalSteps": 100}]

        def get_sleep_daily(self, first, last):
            return []

        def get_hrv_data_range(self, first, last):
            return {"hrvSummaries": []}

        def get_body_battery(self, first, last):
            return []

        def get_weigh_ins(self, first, last):
            return {"dateWeightList": []}

        def get_stats(self, day):
            self.calls.append(("stats", day))
            if scenario == "summary_source_error" and day == "2026-09-30":
                raise ValueError(PRIVATE_MARKER + " email@example.invalid private/key")
            return {"calendarDate": day, "totalSteps": 100}

        def get_respiration_data(self, day):
            return {"calendarDate": day}

    client = HealthClient()
    source = Source()
    if scenario in {"night_pending", "night_source_error"}:
        if scenario == "night_pending":
            sleep = source._fetch("sleep", "2026-09-30")
            sleep["dailySleepDTO"]["sleepTimeSeconds"] = 0
            source.responses["sleep", "2026-09-30"] = sleep
            source.responses["hrv", "2026-09-30"] = {
                "hrvSummary": {"calendarDate": "2026-09-30"}, "hrvReadings": [],
            }
        else:
            for stream in ("sleep", "hrv"):
                source.responses[stream, "2026-09-30"] = ValueError(
                    PRIVATE_MARKER + " email@example.invalid private/key")
        source.calls.clear()
    logins = []

    def login(*args, **kwargs):
        logins.append(True)
        if scenario == "login_failure":
            raise RuntimeError(PRIVATE_MARKER + " email@example.invalid private/key")
        return source

    from pipeline.r2_store import R2Store

    store = R2Store(client=client, bucket="synthetic-entrypoints")
    summaries = {
        "activities.csv": b"Activity ID,Activity Name,Activity Type,Date\n",
        "health_daily.csv": b"Date,Sleep Seconds,Weight KG,HRV Last Night Average\n2026-09-30,25200,,50\n",
    }
    if workflow != "refresh.yml":
        for name, body in summaries.items():
            store.put("summary/" + name, body, "text/csv")
    if scenario != "no_profile":
        store.put("coach/profiles/v1/2026-01-01/synthetic.json", json.dumps({
            "profile_id": "synthetic", "effective_from": "2026-01-01",
            "zones": {"zone_basis": "lthr", "lthr_bpm": 170},
        }).encode(), "application/json")
    for key, plan in {
        "backfill/activities/plan-v2.json": {
            "schema_version": 2, "status": "complete", "ranges": [],
        },
        "backfill/hrv/plan.json": {
            "schema_version": 1, "status": "complete", "target_dates": ["2026-09-30"],
        },
        "backfill/sleep/v1/plan.json": {
            "schema_version": 1, "status": "complete", "target_dates": ["2026-09-30"],
        },
        "backfill/body-composition/v1/plan.json": {
            "schema_version": 1, "status": "complete", "target_dates": [],
        },
    }.items():
        store.put(key, json.dumps(plan).encode(), "application/json")

    instant = datetime(2026, 9, 30, 7, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    class Calendar(date):
        @classmethod
        def today(cls):
            return instant.date()

    command_modules = (
        "pipeline.activity_refresh", "pipeline.activity_backfill_scheduler",
        "pipeline.health_detail_backfill", "pipeline.hrv_backfill",
        "pipeline.coach_backfill", "pipeline.refresh",
        "pipeline.night_followup", "pipeline.night_baseline_refresh",
        "pipeline.health_sync", "pipeline.freshness", "pipeline.latest_night",
    )
    with ExitStack() as patches:
        # Login and S3 service construction are the external system boundaries.
        from pipeline.sources import garmin

        patches.enter_context(patch.object(garmin, "_login", login))
        patches.enter_context(patch.object(boto3, "client", lambda *args, **kwargs: client))
        patches.enter_context(patch.object(Garmin, "__init__", forbidden))
        for name in command_modules:
            importlib.import_module(name)
        refresh = importlib.import_module("pipeline.refresh")
        assert refresh.run.__kwdefaults__["login"] is login
        assert refresh.run.__kwdefaults__["store_factory"] is R2Store
        for name, module in tuple(sys.modules.items()):
            if not name.startswith("pipeline."):
                continue
            if hasattr(module, "_login"):
                patches.enter_context(patch.object(module, "_login", login))
            if getattr(module, "datetime", None) is datetime:
                patches.enter_context(patch.object(module, "datetime", Clock))
            if getattr(module, "date", None) is date:
                patches.enter_context(patch.object(module, "date", Calendar))
        patches.enter_context(patch("datetime.datetime", Clock))
        patches.enter_context(patch("time.sleep", lambda seconds: None))
        if workflow == "refresh.yml":
            # General refresh requires the real committed summary pair. Orphan
            # CSV objects are intentionally refused by summary restoration.
            from pipeline import summary_export

            seed = directory / "seed"
            seed.mkdir()
            for name, body in summaries.items():
                (seed / name).write_bytes(body)
            summary_export.run(str(seed), store=store)
        arguments = {
            "refresh.yml": ["--workflow", "--data-dir", str(directory / "data"),
                            "--diagnostics-file", str(directory / "diagnostics.json")],
            "activity-refresh.yml": ["--lookback-days", "7", "--max-activities", "3"],
            "scheduled-activity-backfill.yml": ["--max-activities", "3"],
            "scheduled-health-detail-backfill.yml": ["--max-sleep-days", "2", "--max-body-days", "2"],
            "scheduled-hrv-backfill.yml": ["--max-days", "2"],
            "coach-input-backfill.yml": ["--max-activities", "3"],
        }
        sys.argv = ["pipeline.actions_receipt", "--workflow", workflow,
                    "--receipt", str(directory / "receipt.json"), "--", *arguments[workflow]]
        try:
            runpy.run_module("pipeline.actions_receipt", run_name="__main__")
        finally:
            observation = {"logins": len(logins), "source_calls": source.calls,
                           "reads": client.reads, "writes": client.writes,
                           "objects": sorted(client.objects)}
            if (directory / "diagnostics.json").is_file():
                observation["diagnostics"] = json.loads((directory / "diagnostics.json").read_text())
            (directory / "observation.json").write_text(json.dumps(observation), encoding="utf-8")


def invoke(tmp_path, workflow, *, event="workflow_dispatch", scenario="no_work"):
    driver = "import runpy,sys; ns=runpy.run_path(sys.argv[1]); ns['_child_main'](*sys.argv[2:])"
    child_env = _synthetic_environment()
    child_env["PYTHONPATH"] = str(ROOT.resolve())
    result = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-c", driver, str(Path(__file__).resolve()),
         workflow, event, scenario, str(tmp_path)],
        cwd=ROOT, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=30,
        env=child_env,
    )
    assert (tmp_path / "receipt.json").is_file(), result.stderr
    receipt = json.loads((tmp_path / "receipt.json").read_text(encoding="utf-8"))
    observation = json.loads((tmp_path / "observation.json").read_text(encoding="utf-8"))
    assert receipt["native_exit"] == result.returncode
    assert receipt["event"] == event
    assert receipt["context_status"] == "valid"
    assert receipt["scheduled_for"] is None and receipt["occurrence_id"] is None
    output = result.stdout + result.stderr + json.dumps(receipt)
    assert PRIVATE_MARKER not in output
    assert "email@example.invalid" not in output and "private/key" not in output
    assert "Traceback" not in output
    return result, receipt, observation


def test_coach_actual_module_entry_reports_missing_profile_without_source_login(tmp_path):
    result, receipt, observation = invoke(tmp_path, "coach-input-backfill.yml", scenario="no_profile")
    assert result.returncode == 0
    assert receipt["command_outcome"] == "success" and receipt["data_outcome"] == "not_ready"
    assert receipt["counters"]["attempted_this_run"] == 0
    assert observation["logins"] == 0 and observation["source_calls"] == []


@pytest.mark.parametrize("event", ["schedule", "workflow_dispatch", "workflow_run"])
def test_coach_actual_module_entry_distinguishes_empty_work_from_not_ready(tmp_path, event):
    result, receipt, observation = invoke(tmp_path, "coach-input-backfill.yml", event=event)
    assert result.returncode == 0
    assert receipt["data_outcome"] == "no_work"
    assert receipt["counters"]["attempted_this_run"] == 0
    assert observation["logins"] == 0 and observation["source_calls"] == []
    assert receipt["cron_timezone"] == ("UTC" if event == "schedule" else None)


def test_activity_command_no_work_is_a_started_successful_job(tmp_path):
    result, receipt, observation = invoke(tmp_path, "activity-refresh.yml")
    assert result.returncode == 0 and receipt["data_outcome"] == "no_work"
    assert receipt["counters"] == {"activities_checked": 0}
    assert observation["logins"] == 1
    assert observation["source_calls"] == [["activities", "synthetic"]]


@pytest.mark.parametrize("scenario,native", [("source_failure", 1), ("native_failure", 7)])
def test_real_activity_entry_preserves_primary_failure_and_safe_receipt(tmp_path, scenario, native):
    result, receipt, observation = invoke(tmp_path, "activity-refresh.yml", scenario=scenario)
    assert result.returncode == native
    assert receipt["command_outcome"] == "failure" and receipt["data_outcome"] == "error"
    assert observation["logins"] == 1
    assert observation["source_calls"] == [["activities", "synthetic"]]


def test_scheduled_activity_keeps_night_source_error_visible_when_activity_work_is_empty(tmp_path):
    result, receipt, observation = invoke(
        tmp_path, "activity-refresh.yml", event="schedule", scenario="night_source_error")
    assert result.returncode == 0 and receipt["command_outcome"] == "success"
    assert receipt["data_outcome"] == "partial"
    assert {row[0] for row in observation["source_calls"]} == {"activities", "sleep", "hrv"}


@pytest.mark.parametrize("workflow", [
    "activity-refresh.yml", "scheduled-activity-backfill.yml",
    "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
])
def test_scheduled_command_does_not_claim_no_work_when_current_night_is_pending(tmp_path, workflow):
    result, receipt, observation = invoke(
        tmp_path, workflow, event="schedule", scenario="night_pending")
    assert result.returncode == 0 and receipt["command_outcome"] == "success"
    assert receipt["data_outcome"] == "partial"
    assert {"sleep", "hrv"}.issubset({row[0] for row in observation["source_calls"]})


@pytest.mark.parametrize("workflow,outcome", [
    ("scheduled-activity-backfill.yml", "complete"),
    ("scheduled-health-detail-backfill.yml", "no_work"),
    ("scheduled-hrv-backfill.yml", "complete"),
])
def test_manual_completed_historical_plans_do_not_login_or_reset_tickets(tmp_path, workflow, outcome):
    result, receipt, observation = invoke(tmp_path, workflow)
    assert result.returncode == 0 and receipt["command_outcome"] == "success"
    assert receipt["data_outcome"] == outcome
    assert observation["logins"] == 0 and observation["source_calls"] == []
    assert all(not key.startswith("health/") for key in observation["writes"])


@pytest.mark.parametrize("event", ["schedule", "workflow_dispatch"])
def test_refresh_real_workflow_entry_emits_safe_primary_login_failure(tmp_path, event):
    result, receipt, observation = invoke(tmp_path, "refresh.yml", event=event, scenario="login_failure")
    assert result.returncode == 1 and receipt["command_outcome"] == "failure"
    assert receipt["data_outcome"] == "error"
    assert observation["logins"] == 1 and observation["source_calls"] == []
    assert observation["diagnostics"]["stages"][0]["stage"] == "restore_summaries"
    assert observation["diagnostics"]["stages"][0]["status"] == "complete"


def test_refresh_actual_general_pipeline_reports_partial_without_failing_the_command(tmp_path):
    result, receipt, observation = invoke(
        tmp_path, "refresh.yml", event="schedule", scenario="summary_source_error")
    assert result.returncode == 0 and receipt["command_outcome"] == "success"
    assert receipt["data_outcome"] == "partial"
    assert observation["logins"] == 1
    assert [row["stage"] for row in observation["diagnostics"]["stages"]] == [
        "restore_summaries", "refresh_summaries", "recent_health", "recent_body", "health_indexes",
    ]


def test_documented_created_run_queue_retains_and_executes_all_four_real_entries(tmp_path):
    # The external GitHub queue contract is synthetic; the retained entries,
    # command parsing, receipts and downstream Python code actually execute.
    from test_scheduled_workflow_queue import CreatedRunQueue, concurrency

    queue = CreatedRunQueue()
    for workflow in (
        "refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
        "activity-refresh.yml",
    ):
        queue.arrive(workflow, concurrency(workflow))
    completed = []
    while queue.running is not None:
        workflow = queue.running
        directory = tmp_path / str(len(completed))
        directory.mkdir()
        event = "workflow_dispatch" if workflow == "activity-refresh.yml" else "schedule"
        scenario = ("summary_source_error" if workflow == "refresh.yml"
                    else "no_work" if event == "workflow_dispatch" else "night_pending")
        result, receipt, _ = invoke(directory, workflow, event=event, scenario=scenario)
        assert result.returncode == 0 and receipt["command_outcome"] == "success"
        assert receipt["data_outcome"] == ("no_work" if event == "workflow_dispatch" else "partial")
        completed.append(workflow)
        queue.finish_running()
    assert completed == [
        "refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
        "activity-refresh.yml",
    ]
    assert queue.cancelled == []
