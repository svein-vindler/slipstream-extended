"""The ordinary daily worker drains a fixed cohort before the next source admission."""

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from daily_autonomy_fixture import daily_system, transport_counts

from pipeline.history_backfill import daily
from pipeline.history_collect import collect
from pipeline.history_publish import publish
from pipeline.history_state import Job, Stop, rolling_usage
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.night_products import normalize_args, storage_key


def test_ordinary_successor_preserves_a_lower_get_cap_and_independent_usage(monkeypatch):
    system = daily_system(monkeypatch)
    job = system.job
    day, round_id = "2026-01-26", "a" * 32
    collect(job, job.plan_sha, confirm=True, target_dates=[day], now=system.clock.now)
    job.append({"type": "daily_round_started", "round": round_id, "dates": [day],
                "new_dates": [], "stored_dates": [day], "finalize_dates": [day],
                "max_dates": 30, "max_http": 240})
    initial = publish(job, job.plan_sha, confirm=True, target_dates=[day], finalize_dates=[day],
                       defer_references=True, execution_context={"round": round_id,
                                                                  "publication_get_limit": 400,
                                                                  "independent_gets": 23})
    source_calls = list(system.source_calls)
    args = SimpleNamespace(confirm_cloud_jobs_paused=True, confirm_r2_write=True,
                           max_dates=30, max_http=240, plan_sha=job.plan_sha,
                           resume=False, archive_reload=True, ledger=[], max_wait_seconds=0)
    result = daily(Job(job.folder), args, clock=system.clock)
    totals = result["baseline_refresh"]["execution_totals"]
    assert result["daily_publication_phases"] == 1
    assert totals["publication_get_limit"] == 400 and totals["independent_gets"] == 23
    assert totals["counts"]["get"] > initial["baseline_refresh"]["execution_totals"]["counts"]["get"]
    assert totals["round_baseline"]["counts"] == initial["baseline_refresh"]["execution_totals"]["counts"]
    assert totals["counts"]["get"] - totals["round_baseline"]["counts"]["get"] <= 400
    assert system.source_calls == source_calls and not job.status()["locked"]


def test_ordinary_daily_drains_186_targets_then_admits_next_thirty_with_real_source_expiry(monkeypatch):
    system = daily_system(monkeypatch, start="2025-11-27")
    job, server, files = system.job, system.server, system.files
    collect(job, job.plan_sha, confirm=True, max_dates=1, target_dates=["2026-01-26"], now=system.clock.now)
    args = SimpleNamespace(confirm_cloud_jobs_paused=True, confirm_r2_write=True,
                           max_dates=30, max_http=240, plan_sha=job.plan_sha,
                           resume=False, archive_reload=True, ledger=[], max_wait_seconds=0)
    first = daily(job, args, clock=system.clock)
    assert first["daily_status"] == "baselines_pending"
    assert first["counts"] == {"published": 62, "pending": 60}
    first_source_calls = list(system.source_calls)
    assert len(first_source_calls) == 214
    source_round = first["daily_round"]
    # A terminal journal alone cannot authorize successor adoption when the
    # matching durable ACK/readback evidence changed or an owner is unknown.
    last_run = next(frame["event"]["run"] for frame in reversed(job.events()[0])
                    if frame["event"].get("type") == "end")
    remote_path = str(job.folder / "runs" / last_run / "REMOTE-VERIFIED.json")
    remote_body = files[remote_path]
    files[remote_path] = b"{}"
    before = len(server.calls)
    with pytest.raises(Stop) as held:
        daily(Job(job.folder), args, clock=system.clock)
    assert held.value.category == "publication_checkpoint_unverified"
    assert len(server.calls) == before and system.source_calls == first_source_calls
    files[remote_path] = remote_body
    owner_path = str(job.folder / "OWNER.lock")
    files[owner_path] = b'{"run":"unknown","phase":"publish"}'
    with pytest.raises(Stop) as held:
        daily(Job(job.folder), args, clock=system.clock)
    assert held.value.category == "unknown_owner_hold_no_takeover"
    assert owner_path in files and len(server.calls) == before
    del files[owner_path]  # Fixture removes its own unknown owner; production never does.
    reports, invocations = list(first["daily_baseline_reports"]), [first]
    for _ in range(20):
        if invocations[-1]["daily_status"] == "bounded_complete":
            break
        before = len(server.calls)
        following = daily(Job(job.folder), args, clock=system.clock)
        invocations.append(following)
        reports.extend(following["daily_baseline_reports"])
        assert following["daily_round"] == source_round
        assert following["daily_publication_phases"] <= 14
        assert system.source_calls == first_source_calls
        assert len(server.calls) > before
    assert invocations[-1]["daily_status"] == "bounded_complete"
    assert invocations[-1]["baseline_refresh"]["cohort_complete"] is True
    endpoints = [(datetime(2025, 12, 27) + timedelta(days=i)).date().isoformat() for i in range(31)]
    keys = {storage_key(normalize_args(end_date=end, period="custom" if n == 30 else str(n),
                                     start_date=(datetime.fromisoformat(end).date()
                                                 - timedelta(days=n - 1)).isoformat(), channel=channel))
            for end in endpoints for n in (28, 30, 90) for channel in ("heart_rate", "hrv")}
    assert len(keys) == 186 and keys <= server.objects.keys()
    assert all(server.puts.count(key) == 1 for key in keys)
    queue = json.loads(server.objects[QUEUE_KEY][0])
    assert queue["pending"] == [] or queue["pending"] == {}
    assert not any("rolling-reference-plans" in name for name in files)
    counts = Counter()
    previous = dict.fromkeys(("get", "list", "put"), 0)
    epoch_phases = {}
    for report in reports:
        for kind, value in report["publication_counts"].items():
            counts[kind] += value
        totals = report["execution_totals"]
        epoch = totals.get("reference_successor_sha256", "initial")
        assert totals["phases"] == epoch_phases.get(epoch, 0) + 1
        epoch_phases[epoch] = totals["phases"]
        assert all(totals["counts"][kind] >= previous[kind] for kind in previous)
        previous = totals["counts"]
        baseline = totals["round_baseline"]
        assert totals["phases"] <= 14
        assert totals["counts"]["get"] - baseline["counts"]["get"] <= 6000
        assert sum(totals["counts"][kind] - baseline["counts"][kind] for kind in ("list", "put")) <= 1000
        assert totals["body_bytes"] - baseline["body_bytes"] <= 10485760
        assert report["publication_counts"]["get"] <= 400
        assert report["publication_counts"]["list"] <= 100
        assert report["publication_counts"]["put"] <= 100
        assert report["publication_body_bytes"] <= 2097152
    assert dict(counts) == previous
    assert all(row.get("event", {}).get("type") != "reference_successor_admitted"
               for row in job.events()[0])
    assert not job.status()["locked"]
    # The next cohort is selected normally but cannot reuse yesterday's quota.
    blocked = daily(Job(job.folder), args, clock=system.clock)
    assert blocked["daily_status"] == "source_budget_pending"
    assert blocked["daily_round"] != source_round
    assert system.source_calls == first_source_calls
    before_events = list(job.events()[0])
    monkeypatch.setattr(system.clock, "now", lambda: datetime(2026, 2, 11, 12, 0, 1, tzinfo=timezone.utc))
    next_round = daily(Job(job.folder), args, clock=system.clock)
    assert next_round["daily_new_dates"] == 30
    assert next_round["counts"] == {"published": 122}
    assert len(system.source_calls) - len(first_source_calls) == 210
    assert sum(method == "POST" for _, method, _ in system.source_calls[len(first_source_calls):]) == 30
    assert rolling_usage(job, (), system.clock.now())["reloads"] == 30
    assert job.events()[0][:len(before_events)] == before_events
    assert system.native_results == [0] * 61
    print(json.dumps({"synthetic": True, "ordinary_invocations_to_drain": len(invocations),
                      "publication_phases": len(reports), "rolling_targets": len(keys),
                      "cumulative_first_cohort": previous, "physical_all": dict(transport_counts(server.calls)),
                      "source_first_HTTP": len(first_source_calls), "source_next_HTTP": 210,
                      "next_source_dates": next_round["daily_new_dates"]}, sort_keys=True))
