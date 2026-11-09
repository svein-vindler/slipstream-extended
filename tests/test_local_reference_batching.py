"""Bounded local references through public planning/publisher and actual SDK wire."""

import json
import time
from collections import Counter

import boto3
import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_history_baseline_integration import sdk_store
from test_history_source_free_completion import source_date
from test_rolling_reference_sources import seeded

from pipeline import history_publish
from pipeline.history_backfill import main as daily_main
from pipeline.history_state import Job, Stop
from pipeline.rolling_reference_cli import create_plan, refresh_plan


def workload(folder, batch=None, shared=False):
    job = Job.create(folder, "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    server, _ = seeded("2026-10-25", 1, contexts=True)
    raw_responses = []
    send = server.send
    def measure_send(request):
        response = send(request)
        raw_responses.append(response.raw)
        return response
    server.send = measure_send
    root = sdk_store(server)
    created = create_plan(job, job.plan_sha, store=root, endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="Europe/Oslo")
    reports = []
    started = time.perf_counter()
    for _ in range(6):
        store = root.new_stage() if shared else sdk_store(server)
        kwargs = {} if batch is None else {"reference_max_periods": batch}
        result = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=store,
                              confirm=True, **kwargs)
        if not shared:
            store.client.close()
        reports.extend(result["reports"])
        for report in result["reports"]:
            if report.get("budget_deferred"):
                from pipeline.night_baseline_refresh import QUEUE_KEY

                pending = json.loads(server.objects[QUEUE_KEY][0])["pending"]
                assert all(pending[name]["phase"] != "publishing" and not pending[name].get("inflight")
                           for name in report["budget_deferred"] if name in pending)
        if result["status"] == "complete":
            break
    elapsed = time.perf_counter() - started
    root.client.close()
    products = {key: json.loads(body) for key, (body, _) in server.objects.items()
                if "/baseline/" in key}
    for value in products.values():
        value.pop("resources", None)
    counts = Counter("LIST" if query.get("list-type") == ["2"] else method
                     for method, _, query in server.calls)
    inventories = sum(query.get("list-type") == ["2"] and not query.get("prefix")
                      for _, _, query in server.calls)
    assert result["status"] == "complete"
    assert len(products) == 6
    assert not job.status()["locked"]
    assert all(report["component_operations"][kind] <= bound for report in reports
               for kind, bound in [("get", 400), ("list", 100), ("put", 100)])
    assert all(report["publication_body_bytes"] <= 2 * 1024 * 1024 for report in reports)
    assert all(not key.startswith(("health/sleep/", "health/hrv/", "health/signals/"))
               for key in server.puts)
    return products, {"physical": dict(counts), "inventories": inventories,
                      "phases": len(reports), "seconds": elapsed,
                      "snapshot_hits": sum(r["local_snapshot_hits"] for r in reports),
                      "max_completed": max(r["periods_completed"] for r in reports),
                      "attempted_write_bytes": result["execution_totals"]["body_bytes"],
                      "response_body_bytes": sum(raw.observed for raw in raw_responses),
                      "reports": reports}


def test_bounded_opt_in_keeps_products_and_fences_with_fewer_serial_inventories(tmp_path):
    before_products, before = workload(tmp_path / "before")
    after_products, after = workload(tmp_path / "after", batch=8, shared=True)
    assert after_products == before_products
    assert before["max_completed"] <= 2
    assert after["max_completed"] > 2
    assert after["snapshot_hits"] > 0
    assert after["inventories"] < before["inventories"]
    print(json.dumps({"local_reference_workload": {"before": before, "after": after}}))


@pytest.mark.parametrize("batch", [True, 0, 9, "8"])
def test_invalid_local_batch_stops_before_storage_or_job_admission(batch):
    with pytest.raises(Stop) as caught:
        history_publish.publish(None, "synthetic", confirm=True, reference_max_periods=batch)
    assert caught.value.category == "reference_batch_limit"


def test_new_serial_stages_can_finish_march31_calendar_without_refunding_totals():
    import hashlib

    from pipeline import night_baseline_refresh as refresh
    from pipeline.night_products import normalize_args, storage_key

    server, _ = seeded("2026-01-18", 1, contexts=True)
    sources = []
    for day in ["2026-01-18", "2026-02-18", "2026-03-18"]:
        addition, _ = seeded(day, 1, contexts=True)
        server.objects.update(addition.objects)
        server.metadata.update(addition.metadata)
        key = f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"
        body, revision = server.objects[key]
        sources.append({"key": key, "revision": revision, "sha256": hashlib.sha256(body).hexdigest()})
    store = sdk_store(server)
    refresh.enqueue_verified_sources(store, sources)
    reports = []
    for _ in range(3):
        reports.append(refresh.refresh_pending(store.new_stage(), today="2026-04-30", timezone="Europe/Oslo",
                       period_names={"month/2026-01", "month/2026-02", "month/2026-03"}, rolling_sources=[]))
    args = normalize_args(start_date="2026-03-01", end_date="2026-03-31", period="custom", channel="hrv")
    product = json.loads(server.objects[storage_key(args)][0])
    assert product["nights_requested"] == 31
    assert product["contributing_nights"] == 1
    assert sum(report["products_written"] for report in reports) == 6
    assert all(r["component_operations"][kind] <= bound for r in reports
               for kind, bound in [("get", 400), ("list", 100), ("put", 100)])
    print(json.dumps({"calendar31_serial_phases": [r["component_operations"] for r in reports]}))


def test_local_cli_batch_is_explicit_and_stays_inside_original_phase_budgets(tmp_path, monkeypatch, capsys):
    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET

    from pipeline.history_backfill import main

    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    server, _ = seeded("2026-10-25", 1, contexts=True)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="Europe/Oslo")
    original = boto3.client
    def factory(*args, **kwargs):
        client = original(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client
    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    assert main(["--state", str(job.folder), "rolling-refresh", "--plan-sha", job.plan_sha,
                 "--reference-plan-sha", created["plan_sha256"], "--confirm-r2-write",
                 "--reference-max-periods", "8"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "bounded_pending"
    assert sum("/baseline/" in key for key in server.puts) == 4
    assert result["execution_totals"]["counts"]["list"] <= 100


def test_new_stage_cannot_clear_an_unknown_transport_outcome():
    from pipeline import night_baseline_refresh as refresh

    server, _ = seeded("2026-10-25", 1)
    root = sdk_store(server)
    attempts = []
    def eof(request, **kwargs):
        attempts.append(request.method)
        raise EOFError("synthetic incomplete response")
    root.client._endpoint.http_session.send = eof
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(root.new_stage(), today="2026-10-25", timezone="Europe/Oslo")
    root.client._endpoint.http_session.send = server.send
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(root.new_stage(), today="2026-10-25", timezone="Europe/Oslo")
    assert attempts == ["GET"]
    assert server.calls == []
    assert server.puts == []


def prepared_daily(tmp_path, monkeypatch, *, fail_close=False):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2026-04-29"
    job = Job.create(tmp_path / "job", day, day, ["hr", "hrv"], "legacy", "synthetic")
    source_date(job, day)
    job.append({"type": "daily_round_started", "round": "b" * 32, "dates": [day],
                "new_dates": [], "stored_dates": [day], "finalize_dates": [day],
                "max_dates": 30, "max_http": 240})
    server, _ = seeded("2026-05-10", 1, contexts=True)
    root = sdk_store(server)
    history_publish.publish(job, job.plan_sha, confirm=True, store=root, target_dates=[day], finalize_dates=[day],
            execution_context={"round": "b" * 32}, defer_references=True)
    root.client.close()
    canonical_puts = [key for key in server.puts if key.startswith(("health/signals/", "health/hrv/"))]
    server.calls.clear()
    clients, closes = [], []
    original = boto3.client
    def factory(*args, **kwargs):
        client = original(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        close = client.close
        def owned_close():
            closes.append(client)
            close()
            if fail_close:
                raise OSError("synthetic command cleanup failure")
        client.close = owned_close
        clients.append(client)
        return client
    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    return job, server, clients, closes, canonical_puts


def invocation(job, batch=None):
    args = ["--state", str(job.folder), "daily", "--plan-sha", job.plan_sha,
            "--confirm-cloud-jobs-paused", "--confirm-r2-write", "--max-dates", "30", "--max-http", "240"]
    return args if batch is None else args + ["--reference-max-periods", str(batch)]


def test_explicit_daily_batch8_reaches_materialization_and_one_command_store(tmp_path, monkeypatch, capsys):
    job, server, clients, closes, canonical = prepared_daily(tmp_path, monkeypatch)
    code = daily_main(invocation(job, 8))
    output = json.loads(capsys.readouterr().out)
    reports = output["daily_baseline_reports"]
    assert code in {0, 2}
    assert max(row["periods_completed"] for row in reports) > 2
    assert len(clients) == len(closes) == 1
    assert sum(query.get("list-type") == ["2"] and not query.get("prefix")
               for _, _, query in server.calls) == 1
    assert all(row["publication_counts"][kind] <= bound for row in reports
               for kind, bound in [("get", 400), ("list", 100), ("put", 100)])
    assert all(row["publication_body_bytes"] <= 2 * 1024 * 1024 for row in reports)
    totals = reports[-1]["execution_totals"]
    assert totals["phases"] <= 14
    assert totals["counts"]["get"] <= 6000
    assert totals["counts"]["list"] + totals["counts"]["put"] <= 1000
    assert totals["body_bytes"] <= 10 * 1024 * 1024
    assert all(server.puts.count(key) == 1 for key in canonical)
    assert output["daily_HTTP"] == output["daily_new_dates"] == 0
    print(json.dumps({"ordinary_daily_selected8": {
        "phase_completed": [r["periods_completed"] for r in reports],
        "physical": dict(Counter("LIST" if q.get("list-type") == ["2"] else m for m, _, q in server.calls)),
        "clients": len(clients), "closes": len(closes), "source_calls": 0}}))


def test_unchanged_daily_caller_retains_two_row_default(tmp_path, monkeypatch, capsys):
    job, server, clients, closes, canonical = prepared_daily(tmp_path, monkeypatch)
    assert daily_main(invocation(job)) in {0, 2}
    output = json.loads(capsys.readouterr().out)
    assert max(row["periods_completed"] for row in output["daily_baseline_reports"]) <= 2
    assert output["daily_HTTP"] == 0


def test_opt_in_daily_cleanup_failure_cannot_finish_legacy_round(tmp_path, monkeypatch, capsys):
    job, server, clients, closes, canonical = prepared_daily(tmp_path, monkeypatch, fail_close=True)
    assert daily_main(invocation(job, 8)) == 3
    assert len(closes) == 1
    assert not any(frame["event"]["type"] == "daily_round_finished" for frame in job.events()[0])

def test_opt_in_daily_unknown_wire_survives_secondary_cleanup_and_stops(tmp_path, monkeypatch, capsys):
    job, server, clients, closes, canonical = prepared_daily(tmp_path, monkeypatch, fail_close=True)
    attempts = []
    def eof(request):
        attempts.append(request.method)
        raise EOFError("synthetic primary transport EOF")
    server.send = eof
    assert daily_main(invocation(job, 8)) == 2
    assert attempts == ["GET"]
    assert len(closes) == 1
    assert job.status()["locked"]
    assert not any(frame["event"]["type"] == "daily_round_finished" for frame in job.events()[0])


def test_positive_source_free_uses_same_explicit_choice_without_source_or_round_completion(tmp_path, monkeypatch, capsys):
    from test_history_source_free_completion import physical_storage, stopped_job

    job, proof_path, proof_sha = stopped_job(tmp_path)
    server, _ = seeded("2026-05-10", 1, contexts=True)
    clients = physical_storage(monkeypatch, server)
    assert daily_main(["--state", str(job.folder), "publish-source-free", "--plan-sha", job.plan_sha,
                 "--confirm-r2-write", "--source-proof", str(proof_path),
                 "--source-proof-sha", proof_sha, "--reference-max-periods", "8"]) in {0, 2}
    output = json.loads(capsys.readouterr().out)
    assert max(row["periods_completed"] for row in output["daily_baseline_reports"]) > 2
    assert output["daily_status"] == "source_stopped"
    assert output["source_free_source_native"] == 2
    assert len(clients) == 1
    assert not any(frame["event"]["type"] == "daily_round_finished" for frame in job.events()[0])