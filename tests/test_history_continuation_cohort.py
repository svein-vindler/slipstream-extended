"""A selected reference continuation cannot complete its original cohort."""

import json
from types import SimpleNamespace

import pytest
from daily_autonomy_fixture import daily_system

from pipeline import history_backfill, history_collect, history_publish
from pipeline.history_state import Job, Stop, digest
from pipeline.night_baseline_refresh import QUEUE_KEY


@pytest.mark.parametrize("restart", [False, True], ids=["same-process", "reloaded-job"])
def test_one_of_three_continuation_keeps_original_cohort_and_budget_for_daily(monkeypatch, restart):
    system = daily_system(monkeypatch)
    job, files = system.job, system.files
    old_day, new_day = "2025-12-28", "2026-01-26"
    history_collect.collect(job, job.plan_sha, confirm=True, target_dates=[old_day, new_day])
    # The fixture's unrelated dates already have terminal empty-source evidence.
    # Only the two positive dates participate in this publication scenario.
    for target in job.plan["targets"]:
        if target["date"] not in {old_day, new_day}:
            job.append({"type": "target", **target, "status": "empty_unresolved"})
    initial = history_publish.publish(
        job, job.plan_sha, confirm=True, target_dates=[old_day], defer_references=True,
        finalize_dates=[old_day],
        execution_context={"counts": {"get": 1200, "list": 200, "put": 200},
                           "body_bytes": 2000000, "publication_get_limit": 2000, "phases": 3},
    )["baseline_refresh"]
    old_receipts = {name: body for name, body in files.items() if name.endswith("/RECEIPT.json")}
    prior = next(body for body in old_receipts.values() if "baseline_refresh" in json.loads(body))
    pending = json.loads(system.server.objects[QUEUE_KEY][0])["pending"]
    assert set(pending) == {"week/2025-W52", "week/2026-W01", "month/2025-12"}
    source_before = list(system.source_calls)
    continuation = history_backfill.continue_publication(job, SimpleNamespace(
        continuation_sha=None, prior_receipt_sha=digest(prior), independent_gets=95,
        period=["week/2025-W52"], plan_sha=job.plan_sha, confirm_r2_write=True,
    ))
    assert continuation["continuation_status"] == "bounded_complete"
    assert system.source_calls == source_before
    after = continuation["baseline_refresh"]
    admission = json.loads(files[str(job.folder / (
        "publication-continuation-" + continuation["publication_continuation_sha256"] + ".json"
    ))])
    assert admission["finalize_dates"] == [old_day]
    assert admission["closure_day"] == initial["closure_day"]
    pending = json.loads(system.server.objects[QUEUE_KEY][0])["pending"]
    assert set(pending) == {"week/2026-W01", "month/2025-12"}
    if restart:
        job = Job(job.folder)
    args = SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True, max_dates=1, max_http=8,
        plan_sha=job.plan_sha, resume=False, archive_reload=False, ledger=[], max_wait_seconds=0,
    )
    try:
        daily = history_backfill.daily(job, args, clock=system.clock)
    except Stop as stopped:
        # A controlled hold is permitted, but must retain the two old periods'
        # original cohort and must never establish a fresh allowance.
        assert stopped.category in {
            "unfinished_publication_round_requires_continuation", "explicit_continuation_required",
        }
        assert job.status()["baseline_refresh"]["finalize_dates"] == [old_day]
    else:
        first = daily["daily_baseline_reports"][0]
        assert set(first["finalize_dates"]) == {old_day, new_day}
        assert first["execution_totals"]["round_baseline"] == {
            "counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0,
        }
        assert first["execution_totals"]["independent_gets"] == 95
        assert first["execution_totals"]["phases"] > after["execution_totals"]["phases"]
        assert first["execution_totals"]["counts"]["get"] > after["execution_totals"]["counts"]["get"]
    assert after["continuation_complete"] is True
    assert after["cohort_complete"] is False
    assert after["original_cohort_complete"] is False
    assert after["finalize_dates"] == [old_day]
    assert after["original_cohort_closure_day"] == initial["closure_day"]
    assert after["execution_totals"]["counts"]["get"] > initial["execution_totals"]["counts"]["get"]
    assert all(files[name] == body for name, body in old_receipts.items())
    assert system.source_calls == source_before
    print(json.dumps({"restart": restart, "initial": initial["execution_totals"],
                      "continued": after["execution_totals"]}))


def test_completed_calendar_continuation_retains_rolling_rest_before_new_source_round(monkeypatch):
    system = daily_system(monkeypatch)
    job, files = system.job, system.files
    old_day, unrelated_day, next_day = "2025-12-28", "2026-01-26", "2026-01-25"
    history_collect.collect(
        job, job.plan_sha, confirm=True, target_dates=[old_day, unrelated_day, next_day],
    )
    for target in job.plan["targets"]:
        if target["date"] not in {old_day, unrelated_day, next_day}:
            job.append({"type": "target", **target, "status": "empty_unresolved"})
    history_publish.publish(
        job, job.plan_sha, confirm=True, target_dates=[old_day, unrelated_day],
        defer_references=True, finalize_dates=[old_day],
        execution_context={"publication_get_limit": 2000},
    )
    old_receipts = {name: body for name, body in files.items() if name.endswith("/RECEIPT.json")}
    prior = next(body for body in old_receipts.values() if "baseline_refresh" in json.loads(body))
    source_before = list(system.source_calls)
    continuation = history_backfill.continue_publication(job, SimpleNamespace(
        continuation_sha=None, prior_receipt_sha=digest(prior), independent_gets=95,
        period=["week/2025-W52", "week/2026-W01", "month/2025-12"],
        plan_sha=job.plan_sha, confirm_r2_write=True,
    ))
    reference = continuation["baseline_refresh"]
    assert reference["continuation_complete"] is True
    assert reference["original_cohort_complete"] is False
    assert reference["cohort_complete"] is False
    assert reference["cohort_closed_pending"] > 0
    pending = json.loads(system.server.objects[QUEUE_KEY][0])["pending"]
    calendar_pending = {name for name, row in pending.items() if not row.get("rolling_window")}
    assert calendar_pending == {"week/2026-W04", "week/2026-W05", "month/2026-01"}
    assert reference["closed_pending"] >= 3
    # A new Job reconstructs completion and cumulative usage from durable bytes.
    job = Job(job.folder)
    daily = history_backfill.daily(job, SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True, max_dates=1, max_http=8,
        plan_sha=job.plan_sha, resume=False, archive_reload=False, ledger=[], max_wait_seconds=0,
    ), clock=system.clock)
    # The next already-stored date joins the original pending references; the
    # selected calendar continuation was not global completion or a source call.
    assert daily["daily_baseline_reports"][0]["finalize_dates"] == sorted([old_day, next_day])
    assert daily["daily_publication_phases"] <= 14
    assert daily["daily_status"] == "baselines_pending"
    assert all(files[name] == body for name, body in old_receipts.items())
    assert system.source_calls == source_before
    print(json.dumps({"completed_calendar": reference["execution_totals"],
                      "rolling_remaining": reference["cohort_rolling_remaining"]}))
