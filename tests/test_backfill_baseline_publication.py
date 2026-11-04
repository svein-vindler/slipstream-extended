import json

from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_publish
from pipeline.night_baseline_refresh import QUEUE_KEY


def test_same_publication_finishes_closed_references_only_after_exact_source_readback(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    report = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert report["counts"] == {"published": 1}
    assert report["baseline_refresh"]["periods_completed"] == 2
    assert report["baseline_refresh"]["local_snapshot_hits"] >= 1
    assert report["baseline_refresh"]["closed_pending"] == 2
    source = "health/signals/v1/2026/04/2026-04-29.json"
    first_product = next(i for i, (method, key, _) in enumerate(server.calls) if method == "PUT" and "/baseline/" in key)
    assert any(method == "GET" and key == source for method, key, _ in server.calls[:first_product])
    assert len(json.loads(server.objects[QUEUE_KEY][0])["pending"]) == 2



def test_daily_selected_period_waits_for_its_last_raw_then_reuses_both_postimages(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    days = ["2026-04-29", "2026-04-30"]
    job, _ = memory_job(monkeypatch, [{"date": day, "stream": "hrv"} for day in days])
    server = BaselineServer()
    context = {}
    first = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=days[:1],
        finalize_dates=days, execution_context=context,
    )
    assert first["baseline_refresh"]["periods_completed"] == 0
    assert not any("/baseline/" in name for name in server.puts)
    assert first["counts"] == {"published": 1, "ready": 1}
    second = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=days[1:],
        finalize_dates=days, execution_context=context,
    )
    assert second["baseline_refresh"]["periods_completed"] == 2
    assert second["baseline_refresh"]["closed_pending"] == 0
    assert second["baseline_refresh"]["local_snapshot_hits"] == 2
    assert context["counts"]["get"] > second["baseline_refresh"]["publication_counts"]["get"]


def test_baseline_denial_keeps_raw_verified_receipt_without_completion(monkeypatch):
    import pytest
    from botocore.awsrequest import AWSResponse
    from history_storage_fixture import Raw

    from pipeline.history_state import Stop

    class DeniedBaseline(BaselineServer):
        def send(self, request):
            if request.method == "PUT" and "/baseline/" in request.url:
                return AWSResponse(request.url, 403, {"content-length": "0"}, Raw(b""))
            return super().send(request)

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = DeniedBaseline()
    with pytest.raises(Stop):
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    receipt = next(json.loads(body) for name, body in files.items()
                   if name.endswith("RAW-DESTINATIONS-VERIFIED.json"))
    assert receipt["status"] == "raw_verified_baselines_pending"
    assert len(receipt["destinations"]) == 1
    assert receipt["destinations"][0]["sha256"]
    assert job.status()["locked"]
    assert not any(name.endswith("REMOTE-VERIFIED.json") and not name.endswith("RAW-DESTINATIONS-VERIFIED.json")
                   for name in files)


def test_fresh_plan_can_finish_stored_references_without_collecting_pending_sources(monkeypatch, tmp_path):
    import socket
    from datetime import date

    import pytest
    from test_night_baseline_corrections import calendar_store, queued_period

    from pipeline.history_state import Job, Stop

    def forbid_network(*args, **kwargs):
        raise AssertionError("No real network or Garmin allowed")

    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    for credential in ("GARMINTOKENS", "GARMIN_PERSONAL_API_KEY", "GARMIN_ACCOUNT"):
        monkeypatch.delenv(credential, raising=False)
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    initial = calendar_store(first=date(2026, 10, 5), days=7)
    queued_period(initial, "2026-10-05", "2026-10-11", name="week/2026-W41")
    server = BaselineServer()
    for name, snap in initial.objects.items():
        server.objects[name] = (snap.data, snap.revision)
        server.metadata[name] = {"content-type": snap.content_type}
        if snap.encoding:
            server.metadata[name]["content-encoding"] = snap.encoding
    job = Job.create(tmp_path / "job", "2026-01-26", "2026-01-26", ["hr", "hrv"], "legacy", "synthetic")
    assert not (job.folder / "runs").exists()
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["baseline_refresh"]["periods_completed"] == 1
    assert result["baseline_refresh"]["closed_pending"] == 0
    assert result["counts"] == {"pending": 2}
    assert not result["locked"]
    assert not any(event["event"]["type"] == "target" for event in job.events()[0])
    assert not any(name.startswith(("health/signals/", "health/hrv/")) for name in server.puts)
    # An existing owner still prevents any new operation before storage traffic.
    (job.folder / "OWNER.lock").write_text("{}", encoding="utf-8")
    before = list(server.calls)
    with pytest.raises(Stop):
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert server.calls == before and job.status()["locked"]
