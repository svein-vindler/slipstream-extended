"""Hosted output boundaries use synthetic values and no external services."""
import csv
import importlib
import json
import os
import socket
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import pytest

from pipeline import manual_activity_refresh as manual
from pipeline.sources import garmin_health

MARKER = "SYNTHETIC_PRIVATE token email@example.invalid activity GPS=1,2 health=77 2041-02-03 key/private\n\x1b[31m" + "x" * 600
ENTRYPOINTS = [
    ("activity_backfill", ["--start-date", "2026-01-01", "--end-date", "2026-01-02"]),
    ("activity_backfill_scheduler", []), ("activity_refresh", []),
    ("granular_export", []), ("health_detail_backfill", []), ("hrv_backfill", []),
    ("coach_backfill", []), ("health_history_index", []), ("health_history_index", ["--scheduled"]),
    ("manual_activity_refresh", ["process", "--baseline", "synthetic", "--summary", "synthetic", "--run-id", "123"]),
    ("manual_activity_refresh", ["snapshot", "--summary", "synthetic", "--output", "synthetic"]),
    ("refresh", ["--workflow"]), ("summary_export", []), ("summary_restore", []),
    ("refresh_summaries", []), ("recent_health", []), ("recent_body", []),
    ("latest_activity", ["--run-id", "123"]),
    ("latest_night", ["--run-id", "123", "--wake-date", "2026-01-01"]),
    ("fetch", []),
]

@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("External network forbidden in privacy tests")
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    for key in list(os.environ):
        if any(word in key.upper() for word in ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "ACCESS_KEY")):
            monkeypatch.delenv(key, raising=False)

def test_manual_report_remains_private_while_stdout_contains_only_counts(tmp_path, monkeypatch, capsys):
    baseline = tmp_path / "baseline.json"
    baseline.write_text('[]')
    summary = tmp_path / "activities.csv"
    with summary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Activity ID", "Activity Date", "Activity Name", "Activity Type"])
        writer.writerow(["garmin-123", "2041-02-03 10:00:00", MARKER, "Run"])
    class Store:
        objects = {}
        calls = []
        def list_keys(self, prefix=""):
            self.calls.append("list")
            return set()
        def get(self, key):
            self.calls.append("get")
            return self.objects[key]
        def put(self, key, data, *args, **kwargs):
            self.calls.append("put")
            self.objects[key] = data
    class Garmin:
        calls = 0
        def get_activity(self, value):
            self.calls += 1
            return {"activityId": value, "activityType": {"typeKey": "running"},
                    "startTimeLocal": "2041-02-03 10:00:00"}
    def fail(*args, **kwargs):
        raise ValueError(MARKER)
    monkeypatch.setattr(manual, "refresh_activity", fail)
    store, garmin = Store(), Garmin()
    report = manual.run(baseline=baseline, summary=summary, run_id="123",
                        store=store, garmin=garmin, today=date(2041, 2, 3))
    output = capsys.readouterr()
    print("PRIVACY_METRIC " + json.dumps({"case": "manual_report", "events": 1, "log_bytes": len((output.out + output.err).encode()), "response_bytes": len(json.dumps(report).encode()), "garmin": garmin.calls, "r2": len(store.calls)}))
    assert report["checked_activity_count"] == 1
    assert report["activities"][0]["name"] == MARKER
    assert report["activities"][0]["error"] == MARKER[:300]
    assert json.loads(store.objects["refresh/reports/123.json"]) == report
    assert garmin.calls == 1 and store.calls == ["list", "put"]
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert "2041-02-03" not in output.out + output.err
    assert json.loads(output.out)["checked_activity_count"] == 1

@pytest.mark.parametrize("module,args", ENTRYPOINTS)
@pytest.mark.parametrize("mode,exit_code", [("success", 0), ("failure", 1), ("invalid", 2)])
def test_actual_cli_stdout_stderr_and_exit(module, args, mode, exit_code):
    # Real main/argparse boundary in another interpreter; only its producers are faked.
    driver = """
import importlib, socket, sys
def blocked(*args, **kwargs): raise AssertionError('Network forbidden')
socket.create_connection = blocked
socket.socket.connect = blocked
__import__('os').environ['GITHUB_RUN_ID'] = '123'
m = importlib.import_module('pipeline.' + sys.argv[1])
mode, args = sys.argv[2], __import__('json').loads(sys.argv[3])
def operation(*args, **kwargs):
    if mode == 'failure':
        raise RuntimeError('SYNTHETIC_PRIVATE token email@example.invalid key/private\\n' + 'x' * 1000)
    return {"status": "complete"}
m.run = operation
if hasattr(m, 'snapshot'): m.snapshot = operation
if sys.argv[1] == 'health_history_index':
    import pipeline.health_index_reconcile
    pipeline.health_index_reconcile.run = operation
if hasattr(m, 'restore_summaries'): m.restore_summaries = operation
if hasattr(m, 'prepare_environment'): m.prepare_environment = lambda *args, **kwargs: None
sys.argv = ['synthetic'] + (['--unknown=SYNTHETIC_PRIVATE'] if mode == 'invalid' else args)
m.main()
"""
    started = time.perf_counter()
    result = subprocess.run([sys.executable, "-c", driver, module, mode, json.dumps(args)],
                            cwd=Path(__file__).parents[1], env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                            capture_output=True, text=True, timeout=30)
    print("PRIVACY_METRIC " + json.dumps({"case": module + ":" + ("scheduled" if "--scheduled" in args else "snapshot" if "snapshot" in args else "standard") + ":" + mode, "events": int(bool(result.stdout)) + int(bool(result.stderr)), "log_bytes": len((result.stdout + result.stderr).encode()), "elapsed_ms": round((time.perf_counter() - started) * 1000), "response_bytes": 0, "garmin": 0, "r2": 0}))
    assert result.returncode == exit_code, result.stderr
    assert "SYNTHETIC_PRIVATE" not in result.stdout + result.stderr
    assert "Traceback" not in result.stderr

@pytest.mark.parametrize("rate_limited", [False, True])
def test_health_provider_never_logs_label_or_dynamic_exception_name(monkeypatch, capsys, rate_limited):
    error = type("SYNTHETIC_PRIVATE", (RuntimeError,), {})
    attempts = []
    def fail():
        attempts.append(1)
        raise error(MARKER + ("429" if rate_limited else ""))
    monkeypatch.setattr(garmin_health.time, "sleep", lambda value: None)
    assert garmin_health._call(MARKER, fail, 0) is None
    assert len(attempts) == (4 if rate_limited else 1)
    output = capsys.readouterr()
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert len(output.err.splitlines()) == (4 if rate_limited else 1)

class MemoryStore:
    def __init__(self, objects=None):
        self.objects = objects or {}
        self.operations = {"get": 0, "put": 0, "head": 0, "list_pages": 0}
    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}
    def get(self, key):
        self.operations["get"] += 1
        return self.objects[key]
    def put(self, key, data, *args, **kwargs):
        self.operations["put"] += 1
        self.objects[key] = data

def test_granular_private_manifest_and_return_remain_readable(tmp_path, monkeypatch, capsys):
    from pipeline import granular_export
    class Garmin:
        def get_hrv_data(self, day):
            raise ValueError(MARKER)
    monkeypatch.setattr(granular_export, "_login", lambda: Garmin())
    result = granular_export.run(hrv_days=1, cardio_count=0, strength_count=0, output_dir=str(tmp_path))
    output = capsys.readouterr()
    assert result["hrv"][0]["error"] == MARKER
    assert json.loads((tmp_path / "pilot/manifest.json").read_text()) == result
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err

def test_activity_refresh_keeps_private_failure_result(monkeypatch, capsys):
    from pipeline import activity_refresh
    class Garmin:
        def get_activities_by_date(self, *args, **kwargs):
            return [{"activityId": "123", "activityName": MARKER, "activityType": {"typeKey": "running"}}]
    def fail(*args, **kwargs):
        raise ValueError(MARKER)
    monkeypatch.setattr(activity_refresh, "refresh_activity", fail)
    result = activity_refresh.run(store=MemoryStore(), garmin=Garmin(), request_pause=0)
    output = capsys.readouterr()
    assert result["activities"][0]["error"] == MARKER
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert json.loads(output.out)["activities_checked"] == 1

@pytest.mark.parametrize("module", ["hrv_backfill", "activity_backfill_scheduler"])
def test_completed_plan_does_not_echo_unknown_saved_private_fields(module, monkeypatch, capsys):
    imported = importlib.import_module("pipeline." + module)
    plan = {"schema_version": 1, "status": "complete", "target_dates": [],
            "last_error": MARKER, "unknown_provider_field": MARKER}
    store = MemoryStore({imported.PLAN_KEY: json.dumps(plan).encode()})
    if module == "hrv_backfill":
        monkeypatch.setattr(imported, "_refresh_plan_dates", lambda *args: False)
    result = imported.run(store=store)
    output = capsys.readouterr()
    assert result == plan
    assert json.loads(store.objects[imported.PLAN_KEY]) == plan
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err

def test_detail_stream_reports_are_not_changed_by_log_projection(monkeypatch, capsys):
    from pipeline import health_detail_backfill as detail
    plan = {"status": "active", "attempted_this_run": 1, "failed_this_run": [{"date": "2041-02-03", "error": MARKER}]}
    monkeypatch.setattr(detail, "run_stream", lambda *args, **kwargs: dict(plan))
    store = MemoryStore({"summary/health_daily.csv": b"Date,Weight KG\n"})
    result = detail.run(store=store)
    output = capsys.readouterr()
    assert result["sleep"] == result["body_composition"] == plan
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert "2041-02-03" not in output.out

def test_coach_report_keeps_source_fingerprint_only_in_private_result(monkeypatch, capsys):
    from pipeline import coach_backfill
    monkeypatch.setattr(coach_backfill, "_profiles", lambda *args, **kwargs: [])
    store = MemoryStore({"summary/activities.csv": b"Activity ID,Activity Type\n"})
    result = coach_backfill.run(store=store)
    output = capsys.readouterr()
    assert result["status"] == "waiting_for_profile"
    assert json.loads(store.objects[coach_backfill.PLAN_KEY]) == result
    assert result["source_fingerprint"] not in output.out
    assert "source_fingerprint" not in output.out

@pytest.mark.parametrize("module", ["health_history_index", "health_index_reconcile"])
def test_index_reports_preserve_private_months_but_log_counts(module, monkeypatch, capsys):
    imported = importlib.import_module("pipeline." + module)
    private = {"months_considered": 1, "months_written": [{"month": "2041-02", "private": MARKER}],
               "months_unchanged": ["2041-01"], "invalid_days": 0}
    monkeypatch.setattr(imported, "sync_stream", lambda *args, **kwargs: dict(private))
    result = imported.run(store=MemoryStore(), streams=["hrv"])
    output = capsys.readouterr()
    assert result["results"]["hrv"]["months_written"] == private["months_written"]
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert "2041-" not in output.out
    assert json.loads(output.out)["results"]["hrv"]["months_written"] == 1

@pytest.mark.parametrize("event", [
    "manual_activity_refresh", "granular_export", "activity_refresh", "health_detail_backfill",
    "hrv_backfill", "coach_backfill", "activity_backfill_scheduler", "activity_backfill",
    "summary_export_manifest", "summary_restore", "fetch", "health_history_index",
    "health_index_reconcile", "recent_health", "recent_body", "refresh_summaries",
    "refresh-diagnostics", "refresh-diagnostics-publication",
])
def test_shared_projection_rejects_unknown_nested_fields_and_non_aggregate_values(event, capsys):
    from pipeline.output_privacy import log_report
    report = {"status": MARKER, "elapsed_ms": 1, "activities_checked": 2.5,
              "days": True, "source_errors": float("nan"), "unknown": MARKER,
              "results": {"hrv": {"months_written": [{"month": "2041-02", "error": MARKER}]}, MARKER: MARKER},
              "stages": [{"stage": "recent_health", "status": "complete", "elapsed_ms": 2.5, "unknown": MARKER},
                         {"stage": MARKER, "error": MARKER}]}
    log_report(event, report)
    output = capsys.readouterr()
    assert "SYNTHETIC_PRIVATE" not in output.out + output.err
    assert "2041-" not in output.out
    value = json.loads(output.out)
    assert value["elapsed_ms"] == 1 and value["status"] == "unknown"
    assert value["stages"] == [{"stage": "recent_health", "status": "complete", "elapsed_ms": 2.5}]
    assert "days" not in value and "source_errors" not in value and "activities_checked" not in value

@pytest.mark.parametrize("operation", ["connectapi", "download"])
def test_pinned_sdk_logs_are_sanitized_inside_actual_cli_and_logger_state_is_restored(operation):
    driver = """
import json, logging, socket, sys
from types import SimpleNamespace
import garminconnect, requests
from pipeline import activity_refresh
def blocked(*args, **kwargs): raise AssertionError('External network forbidden')
socket.create_connection = blocked
socket.socket.connect = blocked
attempts = []
def fail(*args, **kwargs):
    attempts.append(1)
    logging.getLogger('garminconnect.client').warning('Token rejected: %s', 'SYNTHETIC_PRIVATE')
    raise requests.ReadTimeout('SYNTHETIC_PRIVATE token email@example.invalid key/private\\n' + 'x' * 1000)
api = garminconnect.Garmin()
api.client = SimpleNamespace(connectapi=fail, download=fail)
api.retry_attempts = 1
api.retry_min_wait = api.retry_max_wait = 0
names = ['garminconnect', 'garminconnect.client']
state = [(logging.getLogger(name).level, list(logging.getLogger(name).handlers), list(logging.getLogger(name).filters), logging.getLogger(name).propagate) for name in names]
def operation(*args, **kwargs):
    getattr(api, sys.argv[1])('/hrv-service/hrv/2041-02-03')
activity_refresh.run = operation
action = sys.argv[1]
sys.argv = [sys.argv[0], action]
def call(*args, **kwargs):
    getattr(api, action)('/hrv-service/hrv/2041-02-03')
activity_refresh.run = call
sys.argv = ['synthetic']
try:
    activity_refresh.main()
except BaseException as error:
    code = error.code if isinstance(error, SystemExit) else 1
    assert [(logging.getLogger(name).level, list(logging.getLogger(name).handlers), list(logging.getLogger(name).filters), logging.getLogger(name).propagate) for name in names] == state
    print(json.dumps({'attempts': len(attempts)}))
    sys.exit(code)
"""
    result = subprocess.run([sys.executable, "-c", driver, operation],
                            cwd=Path(__file__).parents[1], env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 1
    assert json.loads(result.stdout)["attempts"] == 2
    assert "SYNTHETIC_PRIVATE" not in result.stderr
    assert "2041-02-03" not in result.stderr
    assert "Traceback" not in result.stderr

def test_timeout_classification_remains_internal_and_unchanged():
    import requests

    from pipeline.activity_backfill import is_job_stopping_error
    assert is_job_stopping_error(requests.ReadTimeout(""))
    assert is_job_stopping_error(ConnectionError(""))
    assert not is_job_stopping_error(requests.ConnectionError(""))
    assert not is_job_stopping_error(ValueError("synthetic item unavailable"))

def test_large_invalid_saved_aggregate_is_dropped_without_changing_result(monkeypatch, capsys):
    from pipeline import hrv_backfill
    plan = {"schema_version": 1, "status": "complete", "target_dates": [], "elapsed_ms": 10**400}
    store = MemoryStore({hrv_backfill.PLAN_KEY: json.dumps(plan).encode()})
    monkeypatch.setattr(hrv_backfill, "_refresh_plan_dates", lambda *args: False)
    assert hrv_backfill.run(store=store) == plan
    output = capsys.readouterr()
    assert json.loads(output.out) == {"kind": "hrv_backfill", "schema_version": 1, "status": "complete"}
