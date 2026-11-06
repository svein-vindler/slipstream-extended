"""Daily command counts new source dates separately from stored source backlog."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import boto3
from history_storage_fixture import ACCOUNT, BUCKET, Server
from test_history_backfill import cli, native_runner
from test_history_daily_robustness import journal

from pipeline import history_backfill, history_collect
from pipeline.history_state import Job


def test_daily_cli_reuses_stored_source_without_consuming_new_date_target(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")
    job = Job.create(
        tmp_path / "job", "2026-04-27", "2026-04-29", ["hrv"], "legacy", "synthetic"
    )
    history_collect.collect(
        job, job.plan_sha, confirm=True, max_dates=1,
        date_runner=native_runner("normal"),
    )
    server = Server()
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    native_run = history_collect.subprocess.run
    source_dates = []

    def process(args, **kwargs):
        day = args[args.index("--date") + 1]
        source_dates.append(day)
        return native_run(
            [args[0], "-B", str(Path(__file__).with_name("history_native_fixture.py")),
             "normal", str(job.folder), args[args.index("--run") + 1], day],
            input=kwargs["input"], capture_output=True, check=False,
        )

    monkeypatch.setattr(history_collect.subprocess, "run", process)
    assert history_backfill.main([
        "--state", str(job.folder), "daily", "--plan-sha", job.plan_sha,
        "--confirm-cloud-jobs-paused", "--confirm-r2-write", "--max-dates", "2",
    ]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["counts"] == {"published": 3}
    assert source_dates == ["2026-04-28", "2026-04-27"]
    assert report["daily_new_dates"] == 2
    assert report["daily_stored_dates_reused"] == 1
    assert report["daily_publication_phases"] <= 14


def test_daily_waits_for_exact_rolling_expiry_then_finishes_the_same_selection(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")
    job = Job.create(
        tmp_path / "job", "2026-04-28", "2026-04-29", ["hrv"], "legacy", "synthetic"
    )
    events = []
    for index in range(30):
        events.extend([
            {"type": "date_reserved", "id": f"old:{index}",
             "UTC": "2026-11-05T13:00:00+00:00", "HTTP_reserved": 8, "reload_reserved": 1},
            {"type": "date_closed", "id": f"old:{index}",
             "UTC": "2026-11-05T13:00:00+00:00", "physicalHTTP": 8,
             "reload_intent": 1, "reload_ACK": 1},
        ])
    ledger = journal(tmp_path / "previous.jsonl", events)
    old_bytes = ledger.read_bytes()

    class Clock:
        value = 0.0

        def now(self):
            return datetime(2026, 11, 6, 12, 30, tzinfo=timezone.utc) + timedelta(seconds=self.value)

        def monotonic(self):
            return self.value

        def sleep(self, seconds):
            self.value += seconds

    clock = Clock()
    monkeypatch.setattr(history_collect, "time", clock)
    server = Server()
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    native_run = history_collect.subprocess.run
    starts = []

    def process(args, **kwargs):
        starts.append(clock.now())
        return native_run(
            [args[0], "-B", str(Path(__file__).with_name("history_native_fixture.py")),
             "normal", str(job.folder), args[args.index("--run") + 1],
             args[args.index("--date") + 1]],
            input=kwargs["input"], capture_output=True, check=False,
        )

    monkeypatch.setattr(history_collect.subprocess, "run", process)
    report = history_backfill.daily(job, SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True, max_dates=2, max_http=56,
        plan_sha=job.plan_sha, resume=False, archive_reload=True, ledger=[ledger],
        max_wait_seconds=3600,
    ), clock=clock)
    assert report["daily_status"] == "bounded_complete"
    assert report["counts"] == {"published": 2}
    assert report["daily_new_dates"] == 2
    assert starts[0] == datetime(2026, 11, 6, 13, 0, 0, 1, tzinfo=timezone.utc)
    assert report["daily_wait_seconds"] == 1800.000001
    assert ledger.read_bytes() == old_bytes


def test_native_daily_budget_pending_is_durable_without_credentials_or_source(tmp_path):
    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hrv"], "legacy", "synthetic"
    )
    stamp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    events = []
    for index in range(30):
        events.extend([
            {"type": "date_reserved", "id": f"old:{index}", "UTC": stamp,
             "HTTP_reserved": 8, "reload_reserved": 1},
            {"type": "date_closed", "id": f"old:{index}", "UTC": stamp,
             "physicalHTTP": 8, "reload_intent": 1, "reload_ACK": 1},
        ])
    ledger = journal(tmp_path / "previous.jsonl", events)
    old_bytes = ledger.read_bytes()
    argv = [
        "daily", "--plan-sha", job.plan_sha, "--confirm-cloud-jobs-paused",
        "--confirm-r2-write", "--archive-reload", "--max-dates", "1",
        "--max-wait-seconds", "0", "--ledger", str(ledger),
    ]
    first = cli(job.folder, *argv)
    assert first.returncode == 2, first.stderr
    result = json.loads(first.stdout)
    assert result["daily_status"] == "source_budget_pending"
    assert result["counts"] == {"pending": 1}
    assert result["daily_not_before"] == (
        datetime.fromisoformat(stamp) + timedelta(hours=24, microseconds=1)
    ).isoformat()
    assert result["daily_HTTP"] == result["daily_wait_seconds"] == 0
    repeat = cli(job.folder, *argv)
    assert repeat.returncode == 2, repeat.stderr
    assert json.loads(repeat.stdout)["daily_round"] == result["daily_round"]
    assert not (job.folder / "runs").exists()
    assert not job.status()["locked"]
    assert ledger.read_bytes() == old_bytes


def test_continue_publication_cli_completes_bound_old_references_without_source(monkeypatch, capsys):
    from daily_autonomy_fixture import daily_system

    from pipeline import history_publish
    from pipeline.history_state import digest

    system = daily_system(monkeypatch)
    job, files = system.job, system.files
    history_collect.collect(job, job.plan_sha, confirm=True, target_dates=["2025-12-28"])
    history_publish.publish(
        job, job.plan_sha, confirm=True, defer_references=True,
        finalize_dates=["2025-12-28"],
        execution_context={"counts": {"get": 1587, "list": 379, "put": 235},
                           "body_bytes": 4111887, "publication_get_limit": 2000},
    )
    receipts = {name: body for name, body in files.items() if name.endswith("/RECEIPT.json")}
    prior = next(body for body in receipts.values() if "baseline_refresh" in json.loads(body))
    source_calls = list(system.source_calls)
    args = ["--state", str(job.folder), "continue-publication", "--plan-sha", job.plan_sha,
            "--confirm-r2-write", "--prior-receipt-sha", digest(prior),
            "--independent-gets", "95"]
    for name in ["week/2025-W52", "week/2026-W01", "month/2025-12"]:
        args.extend(["--period", name])
    assert history_backfill.main(args) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["continuation_status"] == "bounded_complete"
    assert report["baseline_refresh"]["continuation_pending"] == []
    assert report["baseline_refresh"]["execution_totals"]["independent_gets"] == 95
    assert all(files[name] == body for name, body in receipts.items())
    assert system.source_calls == source_calls


def test_daily_restart_keeps_the_original_new_date_selection_and_source_counts(monkeypatch, capsys):
    from daily_autonomy_fixture import daily_system

    system = daily_system(monkeypatch)
    job = system.job
    native_process = history_collect.subprocess.run
    stopped = False

    def process(args, **kwargs):
        nonlocal stopped
        result = native_process(args, **kwargs)
        if not stopped:
            assert history_backfill.main(["--state", str(job.folder), "stop"]) == 0
            stopped = True
        return result

    monkeypatch.setattr(history_collect.subprocess, "run", process)
    args = SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True, max_dates=2, max_http=56,
        plan_sha=job.plan_sha, resume=False, archive_reload=False, ledger=[],
    )
    first = history_backfill.daily(job, args)
    assert first["daily_status"] == "source_stopped"
    assert first["daily_new_dates"] == 1 and first["daily_HTTP"] == 4
    resumed = history_backfill.daily(Job(job.folder), args)
    # This small two-date cohort cannot assemble the whole adjacent January
    # reference within one phase. The unchanged per-phase reserve stays pending.
    assert resumed["daily_status"] == "baselines_pending"
    assert resumed["baseline_refresh"]["cohort_closed_pending"] > 0
    assert resumed["daily_round"] == first["daily_round"]
    assert resumed["daily_new_dates"] == 2 and resumed["daily_HTTP"] == 8
    assert resumed["counts"] == {"published": 4, "pending": 58}
    assert {day for day, _, _ in system.source_calls} == {"2026-01-26", "2026-01-25"}
    assert len(system.native_results) == 2
    capsys.readouterr()
