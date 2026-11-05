"""Daily completion is observed through the ordinary CLI and real R2 SDK."""

import json

from history_storage_fixture import ACCOUNT, BUCKET
from test_history_baseline_integration import BaselineServer, sdk_store

from pipeline.history_backfill import main
from pipeline.history_state import Job


def configured_storage(monkeypatch, server):
    import boto3

    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
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
    from test_history_backfill import native_runner

    from pipeline.history_collect import collect
    from pipeline.history_publish import publish
    from pipeline.night_baseline_refresh import QUEUE_KEY

    day = "2026-04-29"
    job = Job.create(tmp_path / "job", day, day, ["hr"], "legacy", "synthetic")
    server = queued_server([("week/2026-W02", "2026-01-05", "2026-01-11")])
    unrelated = json.loads(server.objects[QUEUE_KEY][0])["pending"]["week/2026-W02"]
    configured_storage(monkeypatch, server)
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("normal"))
    publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
            finalize_dates=[day], defer_references=True)
    job.pause("2099-01-01T00:00:00Z", "operator_quota")
    source_dates = [frame for frame in job.events()[0] if frame["event"]["type"] == "date_closed"]
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    monkeypatch.delenv("GARMIN_PERSONAL_API_KEY", raising=False)
    assert main(daily_args(job)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["daily_HTTP"] == 0
    assert result["baseline_refresh"]["cohort_complete"] is True
    assert result["baseline_refresh"]["cohort_closed_pending"] == 0
    assert result["baseline_refresh"]["closed_pending"] == 1
    assert sum(row["periods_completed"] for row in result["daily_baseline_reports"]) == 4
    assert json.loads(server.objects[QUEUE_KEY][0])["pending"] == {"week/2026-W02": unrelated}
    assert [frame for frame in job.events()[0] if frame["event"]["type"] == "date_closed"] == source_dates
    assert server.puts.count("health/signals/v1/2026/04/2026-04-29.json") == 1
    assert result["pause"] == "operator_quota"
    assert result["counts"] == {"published": 1}


def test_daily_five_phase_limit_reports_remaining_closed_work(monkeypatch):
    from datetime import date, timedelta
    from types import SimpleNamespace

    from test_history_baseline_integration import memory_job

    from pipeline.history_backfill import daily
    from pipeline.history_publish import publish

    days = [(date(2025, 12, 28) + timedelta(days=offset)).isoformat() for offset in range(30)]
    job, _ = memory_job(monkeypatch, [{"date": day, "stream": "hrv"} for day in days])
    server = BaselineServer()
    configured_storage(monkeypatch, server)
    context = {"publication_get_limit": 2000}
    for offset in range(0, 30, 7):
        publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                target_dates=days[offset:offset + 7], finalize_dates=days,
                execution_context=context, defer_references=True)
    assert job.status()["counts"] == {"published": 30}
    # A lower configured write limit splits real product groups. A default
    # stored-only call must still stop at its five-phase allowance, even when
    # resuming the larger bound cohort, with known progress honestly pending.
    monkeypatch.setenv("R2_MAX_WRITES_PER_RUN", "5")
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    monkeypatch.delenv("GARMIN_PERSONAL_API_KEY", raising=False)
    result = daily(job, SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True,
        max_dates=7, max_http=56, plan_sha=job.plan_sha, resume=False,
        archive_reload=False, ledger=[],
    ))
    assert result["daily_status"] == "baselines_pending"
    assert result["daily_publication_phases"] == 5
    assert result["baseline_refresh"]["cohort_closed_pending"] > 0
    assert sum(row["products_written"] for row in result["daily_baseline_reports"]) > 0
    assert result["daily_HTTP"] == 0 and not result["locked"]
    assert result["counts"] == {"published": 30}
    assert all(server.puts.count(f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json") == 1 for day in days)
    totals = result["daily_baseline_reports"][-1]["execution_totals"]
    print(json.dumps({
        "default_cap_ledger": "stored_only_bound_cohort",
        "phases": result["daily_publication_phases"],
        "cohort_closed_pending": result["baseline_refresh"]["cohort_closed_pending"],
        "HTTP": result["daily_HTTP"], "totals": totals,
    }, sort_keys=True))
    assert totals["counts"]["get"] <= 2000
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
