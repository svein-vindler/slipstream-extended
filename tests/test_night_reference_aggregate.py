"""Reference aggregation preserves known counts and exhausted-budget unknowns."""
from pipeline.latest_night import refresh_references
from pipeline.night_baseline_refresh import refresh_pending
from pipeline.r2_store import R2Store
from scripts.benchmark_health_reuse import HealthClient


def test_empty_reference_queue_reports_known_zero_counts(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    client = HealthClient()
    report = refresh_references(R2Store(client=client, bucket="synthetic"), [])
    assert report["status"] == "complete"
    assert report["held_periods"] == 0
    assert report["closed_remaining"] == 0
    assert report["calendar_refresh"]["held_periods"] == 0
    assert client.counts["get"] == 2
    assert client.writes == []


def test_exhausted_reference_reads_preserve_unknown_counts_before_queue_load(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    client = HealthClient()
    store = R2Store(client=client, bucket="synthetic")
    for _ in range(400):
        assert refresh_pending(store)["status"] == "complete"
    report = refresh_references(store, [])
    assert report["status"] == "bounded_pending"
    assert report["held_periods"] is None
    assert report["closed_remaining"] is None
    assert report["calendar_refresh"]["held_periods"] is None
    assert report["calendar_refresh"]["closed_pending"] is None
    assert report["calendar_refresh"]["component_operations"]["get"] == 400
    assert client.counts["get"] == 400
    assert client.writes == []
