"""Context selection through the public bulk/store boundary, with synthetic data."""
import gzip
import json

import pytest

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def context_store(*, legacy_clock=False):
    store = CountingStore()
    for key in list(store.objects):
        if key.startswith("activities/"):
            del store.objects[key]
    day = "2026-01-01" if legacy_clock else "2027-01-01"
    utc = "2026-01-02T00:00:00Z" if legacy_clock else "2026-12-31T23:30:00Z"
    prefix = f"activities/{day[:4]}/1"
    store.objects[f"{prefix}/activity.v1.json"] = json.dumps({
        "schema_version": 1, "source": "garmin-fit", "source_fit_sha256": "synthetic-fit",
        "activity": {"id": "1", "start_time_local": f"{day}T00:30:00", "start_time_gmt": utc},
        "messages": {},
    }).encode()
    store.objects[f"{prefix}/activity.endurance.v1.json"] = (
        b'{"available":true,"summary":{"distance_m":5000}}'
    )
    store.objects[f"{prefix}/activity.tcx"] = b"synthetic tcx"
    local = "" if legacy_clock else day
    store.objects["summary/activities.csv"] = (
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        f"garmin-1,{utc},Synthetic context,Run,1800,{local}\n"
    ).encode()
    year = day[:4]
    store.objects[f"activities/{year}/1/context/v1/2026-01-01T08-00-00.json"] = (
        b'{"context_id":"synthetic-old"}'
    )
    newer = f"activities/{int(year)-1}/1/context/v1/2026-01-02T08-00-00.json"
    store.objects[newer] = b'{"context_id":"synthetic-new"}'
    return store, prefix, newer


def selected(store, prefix):
    pointer = json.loads(store.objects[f"{prefix}/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    return pointer, analysis


def test_bulk_context_uses_basename_and_ignores_other_namespaces_and_children():
    store, prefix, newer = context_store()
    for year in (2026, 2027):
        store.objects[f"activities/{year}/1/context/v1/2020-01-01.json"] = b"{}"
    for key in (
        f"{prefix}/context/v1/nested/2099-01-01.json",
        f"{prefix}/context/v1/2099-01-01.txt",
        f"{prefix}/context/v1/2099-01-01.json.gz",
        f"{prefix}/context/v10/2099-01-01.json",
        "activities/2025/1/context/v1/2099-01-01.json",
        "activities/2027/10/context/v1/2099-01-01.json",
        "activities/2027/01/context/v1/2099-01-01.json",
    ):
        store.objects[key] = b'{"context_id":"synthetic-decoy"}'
    result = coach_backfill.run(store=store, max_activities=1)
    pointer, analysis = selected(store, prefix)
    assert result["status"] == "complete"
    assert pointer["input_context_key"] == newer
    assert analysis["user_context"]["context_id"] == "synthetic-new"
    before = store.objects.copy()
    store.reset_counts()
    repeated = coach_backfill.run(store=store, max_activities=1)
    assert repeated == result
    assert store.objects == before and store.counts["put"] == 0


@pytest.mark.parametrize("already_complete", [False, True])
def test_bulk_context_rejects_equal_highest_timestamp_for_selected_activity(already_complete):
    store, _, _ = context_store()
    if already_complete:
        coach_backfill.run(store=store, max_activities=1)
    for year in (2026, 2027):
        store.objects[f"activities/{year}/1/context/v1/2099-01-01.json"] = b"{}"
    before = store.objects.copy()
    store.reset_counts()
    with pytest.raises(ValueError, match="timestamp is ambiguous"):
        coach_backfill.run(store=store, max_activities=1)
    assert store.objects == before and store.counts["put"] == 0


def test_bulk_ignores_unrelated_ambiguity_but_fingerprints_its_full_keyset():
    store, prefix, newer = context_store()
    first = coach_backfill.run(store=store, max_activities=1)
    for year, identifier in ((2026, "2"), (2027, "2"), (2024, "1"), (2025, "1")):
        store.objects[f"activities/{year}/{identifier}/context/v1/2099-01-01.json"] = b"{}"
    before = store.objects.copy()
    store.reset_counts()
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["status"] == "complete"
    assert result["source_fingerprint"] != first["source_fingerprint"]
    assert selected(store, prefix)[0]["input_context_key"] == newer
    assert store.objects == before and store.counts["put"] == 0


def test_bulk_sees_new_context_in_next_invocation_and_preserves_history():
    store, prefix, _ = context_store()
    coach_backfill.run(store=store, max_activities=1)
    before = store.objects.copy()
    newest = f"{prefix}/context/v1/2027-01-02T08-00-00.json"
    store.objects[newest] = b'{"context_id":"synthetic-next-invocation"}'
    result = coach_backfill.run(store=store, max_activities=1)
    pointer, analysis = selected(store, prefix)
    assert result["generated_this_run"] == 1 and result["remaining_activities"] == 0
    assert pointer["input_context_key"] == newest
    assert analysis["user_context"]["context_id"] == "synthetic-next-invocation"
    assert all(store.objects[key] == body for key, body in before.items() if "/canonical/" in key)


@pytest.mark.parametrize("existing_selection", [False, True])
def test_bulk_context_follows_repaired_local_calendar(existing_selection):
    store, prefix, newer = context_store(legacy_clock=True)
    if existing_selection:
        coach_backfill.run_one(store=store, activity={
            "id": "1", "date": "2026-01-01", "name": "Synthetic context",
            "type": "Run", "moving_seconds": 1800.0,
        })
    result = coach_backfill.run(store=store, max_activities=1)
    pointer, analysis = selected(store, prefix)
    assert result["status"] == "complete" and result["blocked_activity_count"] == 0
    assert result["activity_clock_revisions"]["1"]["date"] == "2026-01-01"
    assert pointer["input_context_key"] == newer
    assert analysis["user_context"]["context_id"] == "synthetic-new"
