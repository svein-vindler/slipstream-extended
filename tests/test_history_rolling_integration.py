"""The actual history publisher drains later stored endpoints in the same cohort."""

import json

from test_history_baseline_integration import memory_job, sdk_store
from test_rolling_reference_sources import seeded

from pipeline.history_publish import publish
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.night_products import normalize_args, storage_key


def test_raw_staging_defers_rolling_then_same_cohort_discovers_later_stored_night(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server, _ = seeded("2026-05-10", 1)
    context = {}
    raw = publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                  execution_context=context, target_dates=["2026-04-29"],
                  finalize_dates=["2026-04-29"], defer_references=True)["baseline_refresh"]
    assert raw["products_written"] == 0
    queue = json.loads(server.objects[QUEUE_KEY][0])
    assert queue["rolling_discovery"]
    assert not queue.get("rolling", {}).get("eligible")
    reports = []
    for _ in range(12):
        result = publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                         execution_context=context, target_dates=[],
                         finalize_dates=["2026-04-29"])["baseline_refresh"]
        reports.append(result)
        if result["cohort_complete"]:
            break
    args = normalize_args(start_date="2026-04-13", end_date="2026-05-10",
                          period="28", channel="heart_rate")
    assert storage_key(args) in server.objects
    assert reports[-1]["cohort_complete"]
    assert reports[-1]["cohort_discovery_remaining"]["count"] == 0
    assert all(not r["cohort_complete"] for r in reports[:-1])
    assert all(r["execution_totals"]["phases"] <= 14 for r in reports)
    assert context["counts"]["get"] <= 6000
    assert context["counts"]["list"] + context["counts"]["put"] <= 1000
    assert context["body_bytes"] <= 10 * 1024 * 1024


def test_compact_remaining_cannot_be_hidden_by_empty_legacy_cohort(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server, _ = seeded("2026-05-10", 7)
    result = publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                     finalize_dates=["2026-04-29"])["baseline_refresh"]
    assert result["cohort_complete"] is False
    assert result["cohort_rolling_remaining"]["90"]["hrv"]["count"] == 7
    assert result["cohort_closed_pending"] >= 42
    assert result["cohort_discovery_remaining"]["count"] >= 0
