"""Known complete local checkpoints through real publication and fake R2 wire."""

import json

import boto3
import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_publish
from pipeline.history_state import Stop, digest, encoded
from pipeline.night_baseline_refresh import QUEUE_KEY, BaselineRefreshStop


def test_raw_phase_reserves_reference_work_until_after_canonical_delivery(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    result = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    reference = result["baseline_refresh"]
    assert result["counts"] == {"published": 1}
    assert reference["status"] == "bounded_pending" and reference["closed_pending"] == 4
    assert reference["products_written"] == reference["periods_completed"] == 0
    assert reference["reference_work_deferred"] is True
    assert not any("/baseline/" in name for name in server.objects)


def test_ordinary_resume_reuses_exact_known_postimage_without_new_raw_writes(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    first = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    source_key = "health/signals/v1/2026/04/2026-04-29.json"
    original = server.objects[source_key]
    puts = server.puts.count(source_key)
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[]
    )["baseline_refresh"]
    assert report["local_snapshot_hits"] > 0
    assert report["products_written"] > 0
    assert server.objects[source_key] == original and server.puts.count(source_key) == puts == 1
    assert report["execution_totals"]["counts"]["get"] > first["baseline_refresh"]["execution_totals"]["counts"]["get"]


@pytest.mark.parametrize("when", ["before", "after_product"])
def test_resumed_local_snapshot_never_satisfies_either_fresh_source_fence(monkeypatch, when):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    source_key = "health/signals/v1/2026/04/2026-04-29.json"

    class RevisionChanged(BaselineServer):
        change_after_product = False

        def send(self, request):
            response = super().send(request)
            if self.change_after_product and request.method == "PUT" and "/baseline/" in request.url:
                self.objects[source_key] = (self.objects[source_key][0], '"changed-revision"')
            return response

    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = RevisionChanged()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    if when == "before":
        server.objects[source_key] = (server.objects[source_key][0], '"changed-revision"')
    else:
        server.change_after_product = True
    server.calls.clear()
    with pytest.raises(BaselineRefreshStop):
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[])
    source_reads = [c for c in server.calls if c[0] == "GET" and c[1] == source_key]
    assert len(source_reads) == (1 if when == "before" else 2)
    product_writes = [name for name in server.puts if "/baseline/" in name]
    assert bool(product_writes) is (when == "after_product")
    assert job.status()["locked"] and job.status()["counts"] == {"published": 1}


@pytest.mark.parametrize("limit", [2256, 2000])
def test_complete_checkpoint_cannot_refund_exhausted_actual_execution_budget(monkeypatch, limit):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    result = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        execution_context={"counts": {"get": limit - 8, "list": 0, "put": 0},
                           "body_bytes": 0, "publication_get_limit": limit},
    )
    assert result["baseline_refresh"]["execution_totals"]["counts"]["get"] == limit
    calls = len(server.calls)
    with pytest.raises(Stop) as failure:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[])
    assert failure.value.category == "storage_execution_budget"
    assert len(server.calls) == calls


@pytest.mark.parametrize("legacy", [False, True])
def test_only_exact_complete_local_bytes_are_admitted_with_legacy_compatibility(monkeypatch, legacy):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    receipt_name = next(name for name in files if name.endswith("/REMOTE-VERIFIED.json"))
    run_folder = receipt_name.rsplit("/", 1)[0]
    if legacy:
        for name in (receipt_name, run_folder + "/RECEIPT.json"):
            receipt = json.loads(files[name])
            for field in ("run", "plan_sha256", "target_sha256"):
                receipt.pop(field)
            receipt["destinations"][0].pop("snapshot")
            files[name] = (json.dumps(receipt) + "\n").encode()
    else:
        files[run_folder + "/source-postimage-000.bin"] = b"changed local bytes"
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[]
    )["baseline_refresh"]
    assert (report["local_snapshot_hits"] > 0) is legacy
    assert report["products_written"] > 0
    assert server.puts.count("health/signals/v1/2026/04/2026-04-29.json") == 1


def test_missing_completed_receipt_cannot_refund_budget_or_adopt_earlier_parent(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    receipt_name = next(name for name in files if name.endswith("/REMOTE-VERIFIED.json"))
    del files[receipt_name.rsplit("/", 1)[0] + "/RECEIPT.json"]
    calls = len(server.calls)
    with pytest.raises(Stop) as failure:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[])
    assert failure.value.category == "publication_checkpoint_unverified"
    assert len(server.calls) == calls


def test_missing_local_bytes_still_restricts_finalization_to_explicit_round_dates(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [
        {"date": "2026-04-29", "stream": "hr"},
        {"date": "2026-02-18", "stream": "hr"},
    ])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    body_name = next(name for name in files if name.endswith("/source-postimage-000.bin"))
    files[body_name] = b"unavailable exact local bytes"
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
        finalize_dates=["2026-04-29"],
    )["baseline_refresh"]
    products = [json.loads(body) for name, (body, _) in server.objects.items() if "/baseline/" in name]
    # The HR wall-clock margin also affects early May; February belongs to an older cohort.
    assert {value["args"]["start_date"] for value in products} == {"2026-04-01", "2026-05-01"}
    assert report["finalize_dates"] == ["2026-04-29"]


@pytest.mark.parametrize("day,open_periods", [("2026-04-15", 0), ("2026-11-01", 2)])
def test_explicit_round_completes_without_adopting_unrelated_pending_history(monkeypatch, day, open_periods):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [
        {"date": day, "stream": "hrv"},
        {"date": "2026-02-18", "stream": "hrv"},
    ])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
        finalize_dates=[day],
    )["baseline_refresh"]
    assert report["status"] == "bounded_pending" and report["closed_pending"] == 2
    assert report["cohort_complete"] is True
    assert report["cohort_closed_pending"] == report["cohort_held_periods"] == 0
    assert report["cohort_deferred_open"] == open_periods
    assert report["cohort_blocked"] == []
    pending = json.loads(server.objects[QUEUE_KEY][0])["pending"]
    assert "month/2026-02" in pending and "week/2026-W08" in pending
    assert not any(
        json.loads(body)["args"]["start_date"].startswith("2026-02")
        for name, (body, _) in server.objects.items() if "/baseline/" in name
    )
    # A completed explicit cohort starts a new ordinary execution even while
    # unrelated queued history remains; no old budget is falsely carried forever.
    next_report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[], finalize_dates=[day]
    )["baseline_refresh"]
    assert next_report["execution_totals"]["counts"]["get"] == 1
    # The ordinary unfiltered publication API still drains that older queue.
    unfiltered = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[]
    )["baseline_refresh"]
    assert unfiltered["closed_pending"] == 0 and unfiltered["periods_completed"] == 2
    assert "cohort_complete" not in unfiltered


def test_explicit_cohort_cannot_complete_while_raw_source_targets_are_deferred(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-15", "stream": "hrv"}])
    server = BaselineServer()
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server, max_writes_per_run=2),
        finalize_dates=["2026-04-15"], defer_references=True,
    )["baseline_refresh"]
    assert report["status"] == "complete" and report["closed_pending"] == 0
    assert report["cohort_complete"] is False and report["source_targets_deferred"] == 1
    assert job.status()["counts"] == {"ready": 1} and server.puts == []


@pytest.mark.parametrize("mutation", ["totals", "completion", "cohort", "missing_reference", "duplicate_reference"])
def test_completed_receipts_must_match_their_own_journal_reference_report(monkeypatch, mutation):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2026-04-29"
    job, files = memory_job(monkeypatch, [{"date": day, "stream": "hr"}])
    server = BaselineServer()
    first = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[day], execution_context={
            "counts": {"get": 1992, "list": 0, "put": 0},
            "body_bytes": 0, "publication_get_limit": 2000,
        },
    )
    assert first["baseline_refresh"]["execution_totals"]["counts"]["get"] == 2000
    receipt_name = next(name for name in files if name.endswith("/REMOTE-VERIFIED.json"))
    if mutation in {"totals", "completion", "cohort"}:
        for name in (receipt_name, receipt_name.rsplit("/", 1)[0] + "/RECEIPT.json"):
            receipt = json.loads(files[name])
            reference = receipt["baseline_refresh"]
            if mutation == "totals":
                reference["execution_totals"]["counts"] = {"get": 0, "list": 0, "put": 0}
                reference["execution_totals"]["body_bytes"] = 0
            elif mutation == "completion":
                reference.update(cohort_complete=True, status="complete", closed_pending=0)
            else:
                reference["finalize_dates"] = []
            files[name] = encoded(receipt)
    else:
        events = [frame["event"] for frame in job.events()[0]]
        reference = next(event for event in events if event["type"] == "reference_refresh")
        position = events.index(reference)
        if mutation == "missing_reference":
            del events[position]
        else:
            events.insert(position, reference)
        previous, body = None, b""
        for sequence, event in enumerate(events):
            line = encoded({"sequence": sequence, "previous": previous,
                            "plan": job.plan_sha, "event": event})
            body += line
            previous = digest(line)
        files[str(job.folder / "events.jsonl")] = body
    before = len(server.calls)
    error = None
    try:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                                target_dates=[], finalize_dates=[day])
    except Stop as failure:
        error = failure.category
    extra = server.calls[before:]
    print(json.dumps({"checkpoint_mutation": mutation, "new_wire": len(extra),
                      "new_GET": sum(call[0] == "GET" and not call[2] for call in extra)}))
    assert not extra
    assert error == "publication_checkpoint_unverified"
    assert not job.status()["locked"] and job.status()["active_run"] is None


@pytest.mark.parametrize("stage", ["checkpoint", "constructor", "guard_setup", "target_binding"])
@pytest.mark.parametrize("close_fails", [False, True])
def test_owned_publication_preflight_closes_client_without_creating_unknown_run(monkeypatch, stage, close_fails):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    if stage in {"checkpoint", "target_binding"}:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                                defer_references=True)
        if stage == "checkpoint":
            remote = next(name for name in files if name.endswith("/REMOTE-VERIFIED.json"))
            del files[remote.rsplit("/", 1)[0] + "/RECEIPT.json"]
        else:
            files[str(job.folder / "storage-target.json")] = encoded({"target_sha256": "b" * 64})
    client = sdk_store(server).client
    original_send = client._endpoint.http_session.send
    closes = []

    def close():
        closes.append(True)
        if close_fails:
            raise OSError("synthetic cleanup failure")

    monkeypatch.setattr(client, "close", close)
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    if stage == "constructor":
        monkeypatch.setenv("R2_MAX_BUCKET_BYTES", "0")
    if stage == "guard_setup":
        original_register = client.meta.events.register_first

        def reject_registration(*args, **kwargs):
            original_register(*args, **kwargs)
            raise RuntimeError("synthetic SDK registration failure")

        monkeypatch.setattr(client.meta.events, "register_first", reject_registration)
    journal = files[str(job.folder / "events.jsonl")]
    before = len(server.calls)
    expected = Stop if stage in {"checkpoint", "target_binding"} else ValueError if stage == "constructor" else RuntimeError
    with pytest.raises(expected) as failure:
        history_publish.publish(job, job.plan_sha, confirm=True, target_dates=[])
    if isinstance(failure.value, Stop):
        assert failure.value.category == ("publication_checkpoint_unverified" if stage == "checkpoint" else "storage_target_changed")
    assert closes == [True]
    assert len(server.calls) == before
    assert client._endpoint.http_session.send == original_send
    assert not job.status()["locked"] and job.status()["active_run"] is None
    assert files[str(job.folder / "events.jsonl")] == journal



def test_reference_only_progress_between_preflight_and_owner_rejects_stale_carry(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2026-04-29"
    job, files = memory_job(monkeypatch, [{"date": day, "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                            defer_references=True, finalize_dates=[day])
    write = history_publish.write
    raced = {}

    def race_at_owner(path, value):
        if str(path) == str(job.folder / "OWNER.lock") and not raced:
            raced["started"] = True
            # Another real serial publisher finishes before this caller acquires
            # its owner. Its reference-only progress leaves source rows unchanged.
            history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                                    target_dates=[], finalize_dates=[day])
            raced["wire_after_other_publisher"] = len(server.calls)
            raced["journal_after_other_publisher"] = files[str(job.folder / "events.jsonl")]
        return write(path, value)

    monkeypatch.setattr(history_publish, "write", race_at_owner)
    with pytest.raises(Stop) as failure:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                                target_dates=[], finalize_dates=[day])
    assert failure.value.category == "publication_progress_changed_before_owner"
    assert len(server.calls) == raced["wire_after_other_publisher"]
    assert files[str(job.folder / "events.jsonl")] == raced["journal_after_other_publisher"]
    assert not job.status()["locked"] and job.status()["active_run"] is None
