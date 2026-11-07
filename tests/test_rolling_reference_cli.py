"""Private job-bound stored-only plan and existing publication integration."""

import json
from datetime import date, timedelta

import pytest
from test_history_baseline_integration import sdk_store
from test_rolling_reference_sources import seeded

from pipeline.history_state import Job, Stop, digest, read


def job_at(tmp_path):
    return Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")


def test_private_plan_is_hash_bound_and_restores_cache_and_charged_reads(tmp_path):
    from pipeline.rolling_reference_cli import create_plan, load_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="Europe/Oslo")
    assert digest(read(created["path"])) == created["plan_sha256"]
    binding, cache, context, plan = load_plan(job, job.plan_sha, created["plan_sha256"],
                                               store=sdk_store(server))
    assert binding["job_plan_sha256"] == job.plan_sha
    assert binding["endpoints"] == ["2026-10-25"]
    assert context["counts"]["get"] == plan["reads"]["operations"]["get"] == 4
    assert context["independent_gets"] == 0
    assert len(cache) == 4 and cache["health/hrv/2026/10/2026-10-25.json"].revision
    assert not job.status()["locked"] and server.puts == []
    assert job.events()[0] == []
    assert all(call[1] not in {"", "inventory"} for call in server.calls)
    json.dumps(created["plan"], allow_nan=False)


def test_wrong_job_or_storage_binding_stops_without_remote_io(tmp_path):
    from pipeline.rolling_reference_cli import create_plan, load_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    with pytest.raises(Stop):
        create_plan(job, "0" * 64, store=sdk_store(server), endpoints=["2026-10-25"],
                    today="2026-10-25", timezone="UTC")
    assert server.calls == []
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="UTC")
    changed = sdk_store(server)
    changed.bucket = "changed-synthetic"
    before = len(server.calls)
    with pytest.raises(Stop):
        load_plan(job, job.plan_sha, created["plan_sha256"], store=changed)
    assert len(server.calls) == before


def test_reference_only_refresh_uses_existing_publisher_and_resumes_exact_plan(tmp_path):
    from pipeline.rolling_reference_cli import admit_reference_round, create_plan, refresh_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="Europe/Oslo")
    first = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server),
                         confirm=True, max_phases=1)
    assert first["status"] == "bounded_pending" and first["publication_phases"] == 1
    with pytest.raises(Stop) as caught:
        admit_reference_round(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert caught.value.category == "reference_round_existing_capacity_remains"
    second = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert second["status"] == "bounded_pending"
    third = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert third["status"] == "complete"
    assert third["execution_totals"]["phases"] == 3
    assert sum("/baseline/" in key for key in server.puts) == 6
    assert not job.status()["locked"]
    assert all(not key.startswith(("health/sleep/", "health/hrv/", "health/signals/")) for key in server.puts)
    last = job.events()[0][-1]["event"]
    assert last["status"] == "published"
    print({"read_plan": created["plan"]["reads"], "refresh_total": third["execution_totals"]})


def test_refresh_has_no_write_without_confirmation_or_eligible_endpoints(tmp_path):
    from pipeline.rolling_reference_cli import create_plan, refresh_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-26"],
                          today="2026-10-26", timezone="UTC")
    before = len(server.calls)
    with pytest.raises(Stop):
        refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server))
    empty = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert empty["status"] == "no_eligible_endpoints" and empty["publication_phases"] == 0
    assert len(server.calls) == before and server.puts == []


def test_repeated_read_plan_reuses_persisted_bytes_and_never_refunds_prior_reads(tmp_path):
    from pipeline.rolling_reference_cli import create_plan, load_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    old = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                      today="2026-10-25", timezone="UTC")
    new = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                      today="2026-10-25", timezone="UTC")
    assert new["plan"]["reads"]["operations"]["get"] == 0
    _binding, _cache, context, _plan = load_plan(job, job.plan_sha, old["plan_sha256"], store=sdk_store(server))
    assert context["counts"]["get"] == 4
    assert context["counts"]["list"] == (old["plan"]["reads"]["operations"]["list"]
                                         + new["plan"]["reads"]["operations"]["list"])


def test_corrupted_local_snapshot_hash_stops_before_remote_io(tmp_path):
    from pathlib import Path

    from pipeline.rolling_reference_cli import create_plan, load_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="UTC")
    index = json.loads(read(Path(created["path"]).parent / "CACHE.json"))
    (job.folder / "rolling-reference-plans" / "objects" / (index[0]["sha256"] + ".bin")).write_bytes(b"changed")
    before = len(server.calls)
    with pytest.raises(Stop) as caught:
        load_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server))
    assert caught.value.category == "rolling_reference_cache_bytes_changed"
    assert len(server.calls) == before


def test_unknown_read_outcome_retains_owner_and_blocks_automatic_retry(tmp_path):
    from pipeline.rolling_reference_cli import create_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    server.deny_lists = True
    with pytest.raises(Stop):
        create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                    today="2026-10-25", timezone="UTC")
    assert job.status()["locked"] and job.events()[0] == []
    before = len(server.calls)
    with pytest.raises(Stop) as caught:
        create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                    today="2026-10-25", timezone="UTC")
    assert caught.value.category == "unknown_owner_hold_no_takeover"
    assert len(server.calls) == before and server.puts == []


def test_normal_plan_factory_closes_owned_sdk_before_releasing_owner(tmp_path, monkeypatch):
    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET

    from pipeline.rolling_reference_cli import create_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    closed, options = [], []

    def factory(service, **kwargs):
        assert service == "s3"
        options.append(kwargs["config"])
        return store.client

    def close():
        assert job.status()["locked"]
        closed.append(True)

    monkeypatch.setattr(boto3, "client", factory)
    monkeypatch.setattr(store.client, "close", close)
    created = create_plan(job, job.plan_sha, endpoints=["2026-10-25"],
                          today="2026-10-25", timezone="UTC")
    assert created["plan"]["reads"]["operations"]["get"] == 4
    assert closed == [True] and not job.status()["locked"]
    assert options[0].retries == {"total_max_attempts": 1}
    assert (options[0].connect_timeout, options[0].read_timeout) == (5, 20)


def test_new_read_plan_cannot_open_extra_get_budget(tmp_path):
    from pipeline.rolling_reference_cli import create_plan

    job = job_at(tmp_path)
    server, _ = seeded("2026-10-25", 1)
    with pytest.raises(Stop) as caught:
        create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-10-25"],
                    today="2026-10-25", timezone="UTC", execution_context={"publication_get_limit": 0})
    assert caught.value.category == "storage_execution_budget"
    assert not any("list-type" not in call[2] for call in server.calls)
    assert job.status()["locked"] and server.puts == []


def test_explicit_reference_round_resumes_remaining_targets_without_refunding_usage(tmp_path, monkeypatch):
    from pipeline.history_publish import publish
    from pipeline.night_baseline_refresh import register_rolling_endpoints
    from pipeline.rolling_reference_cli import admit_reference_round, create_plan, refresh_plan

    job = job_at(tmp_path)
    server, sources = seeded("2026-08-04", 97, contexts=True)
    endpoints = [(date(2026, 11, 1) + timedelta(days=i)).isoformat() for i in range(7)]
    proofs = [{"key": key, "revision": snapshot.revision, "sha256": digest(snapshot.data)}
              for key, snapshot in sources.objects.items()
              if key.startswith("health/sleep/") and key.rsplit("/", 1)[-1][:10] in endpoints]
    register_rolling_endpoints(sdk_store(server), proofs, today=endpoints[-1])
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    monkeypatch.setattr("pipeline.night_baseline_refresh._today", lambda _tz: endpoints[-1])
    ordinary = {}
    for _ in range(14):
        first = publish(job, job.plan_sha, store=sdk_store(server), confirm=True,
                         target_dates=[], execution_context=ordinary)
    assert ordinary["phases"] == 14 and first["baseline_refresh"]["rolling_pending"] > 0
    assert "rolling_plan_sha256" not in first["baseline_refresh"]["execution_totals"]
    old_counts = dict(ordinary["counts"])
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=endpoints,
                          today=endpoints[-1], timezone="Europe/Oslo")
    assert created["plan"]["reads"]["operations"]["get"] == 385
    original_counts = {kind: old_counts[kind] + created["plan"]["reads"]["operations"][kind]
                       for kind in old_counts}
    before = len(server.calls)
    capped = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert capped["publication_phases"] == 0 and len(server.calls) == before
    admission = admit_reference_round(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert len(server.calls) == before
    round_sha = admission["reference_round_sha256"]
    repeated_admission = admit_reference_round(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True)
    assert repeated_admission["reference_round_sha256"] == round_sha
    with pytest.raises(Stop) as caught:
        refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True,
                     reference_round_sha="f" * 64)
    assert caught.value.category == "reference_round_binding_changed"
    assert len(server.calls) == before
    resumed = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True,
                            reference_round_sha=round_sha)
    assert resumed["execution_totals"]["phases"] == 1
    assert all(resumed["execution_totals"]["counts"][kind] >= value for kind, value in original_counts.items())
    replay = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True,
                           reference_round_sha=round_sha)
    assert replay["execution_totals"]["phases"] == 2
    assert replay["execution_totals"]["counts"]["put"] > resumed["execution_totals"]["counts"]["put"]
    # The90-day conservative LIST admission can complete one channel per phase.
    for _ in range(12):
        replay = refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True,
                               reference_round_sha=round_sha)
    assert replay["status"] == "complete"
    assert sum("/baseline/" in key for key in server.puts) == 42
    assert not job.status()["locked"]
    assert all(not key.startswith(("health/sleep/", "health/hrv/", "health/signals/")) for key in server.puts)
    print({"original_epoch": original_counts, "new_epoch_totals": replay["execution_totals"]})
    # A completed plan cannot permanently consume the next explicit endpoint's
    # allowance. The new epoch still requires a separate exact admission.
    next_plan = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=["2026-11-08"],
                            today="2026-11-08", timezone="Europe/Oslo")
    assert next_plan["plan"]["reads"]["operations"]["get"] == 4
    next_round = admit_reference_round(job, job.plan_sha, next_plan["plan_sha256"], store=sdk_store(server), confirm=True)
    next_result = refresh_plan(job, job.plan_sha, next_plan["plan_sha256"], store=sdk_store(server), confirm=True,
                                reference_round_sha=next_round["reference_round_sha256"])
    assert next_result["publication_phases"] == 1
    assert sum("/baseline/" in key for key in server.puts) == 44
    before = len(server.calls)
    with pytest.raises(Stop):
        refresh_plan(job, job.plan_sha, created["plan_sha256"], store=sdk_store(server), confirm=True,
                     reference_round_sha=round_sha)
    assert len(server.calls) == before and not job.status()["locked"]
