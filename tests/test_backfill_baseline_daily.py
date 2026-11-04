"""Daily completion is observed through the ordinary CLI and real R2 SDK."""

import json

from history_storage_fixture import ACCOUNT, BUCKET
from test_history_baseline_integration import BaselineServer, sdk_store

from pipeline.history_backfill import main
from pipeline.history_state import Job


def configured_storage(monkeypatch, server):
    import boto3

    clients = [sdk_store(server).client for _ in range(5)]
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: clients.pop(0))
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
        "HEALTH_TIMEZONE": "Europe/Oslo",
    }.items():
        monkeypatch.setenv(name, value)


def published_job(tmp_path, report):
    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    (job.folder / "runs").mkdir()
    job.append({"type": "target", "date": "2026-04-29", "stream": "hr", "status": "published"})
    job.append({"type": "reference_refresh", "report": report})
    return job


def daily_args(job):
    return [
        "--state", str(job.folder), "daily", "--plan-sha", job.plan_sha,
        "--confirm-cloud-jobs-paused", "--confirm-r2-write",
    ]


def test_all_published_daily_checks_pending_baselines_without_source_credentials(
    monkeypatch, tmp_path, capsys
):
    job = published_job(tmp_path, {"status": "bounded_pending", "closed_pending": 1})
    server = BaselineServer()
    configured_storage(monkeypatch, server)
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    monkeypatch.delenv("GARMIN_PERSONAL_API_KEY", raising=False)
    assert main(daily_args(job)) == 0
    result = json.loads(capsys.readouterr().out)
    assert server.calls, "published raw must not skip known pending reference work"
    assert result["daily_status"] == "bounded_complete"
    assert result["baseline_refresh"]["closed_pending"] == 0
    assert result["daily_HTTP"] == 0


def queued_server(periods):
    from pipeline.night_baseline_refresh import QUEUE_KEY

    server = BaselineServer()
    queue = {
        "schema_version": 1,
        "kind": "night-baseline-refresh-queue",
        "pending": {
            name: {
                "start_date": first, "end_date": last,
                "phase": "dirty", "sources": {}, "completed": {},
            }
            for name, first, last in periods
        },
    }
    server.objects[QUEUE_KEY] = (json.dumps(queue).encode(), '"synthetic-queue"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}
    return server


def test_source_pause_does_not_block_stored_only_baseline_completion(
    monkeypatch, tmp_path, capsys
):
    job = published_job(tmp_path, {"status": "not_run"})
    job.pause("2099-01-01T00:00:00Z", "operator_quota")
    server = queued_server([("week/2026-W02", "2026-01-05", "2026-01-11")])
    configured_storage(monkeypatch, server)
    assert main(daily_args(job)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["daily_HTTP"] == 0
    assert result["baseline_refresh"]["periods_completed"] == 1
    assert result["pause"] == "operator_quota"
    assert result["counts"] == {"published": 1}


def test_daily_five_phase_limit_reports_remaining_closed_work(monkeypatch, tmp_path, capsys):
    from datetime import date, timedelta

    job = published_job(tmp_path, {"status": "not_run"})
    first = date(2026, 1, 5)
    periods = []
    for offset in range(11):
        start = first + timedelta(days=7 * offset)
        periods.append((
            f"week/2026-W{offset + 2:02d}", start.isoformat(),
            (start + timedelta(days=6)).isoformat(),
        ))
    server = queued_server(periods)
    configured_storage(monkeypatch, server)
    assert main(daily_args(job)) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["daily_status"] == "baselines_pending"
    assert result["daily_publication_phases"] == 5
    assert result["baseline_refresh"]["closed_pending"] == 1
    assert sum(r["periods_completed"] for r in result["daily_baseline_reports"]) == 10
    assert result["daily_HTTP"] == 0 and not result["locked"]
    totals = result["daily_baseline_reports"][-1]["execution_totals"]
    assert totals["counts"]["get"] <= 2256
    assert totals["counts"]["list"] + totals["counts"]["put"] <= 1000
    assert totals["body_bytes"] <= 10 * 1024 * 1024


def test_open_period_is_reported_without_forcing_a_reference(monkeypatch, tmp_path, capsys):
    job = published_job(tmp_path, {"status": "not_run"})
    server = queued_server([("month/2099-12", "2099-12-01", "2099-12-31")])
    configured_storage(monkeypatch, server)
    assert main(daily_args(job)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["daily_status"] == "bounded_complete"
    assert result["baseline_refresh"]["deferred_open"] == 1
    assert result["baseline_refresh"]["closed_pending"] == 0
    assert result["daily_publication_phases"] == 1 and server.puts == []
