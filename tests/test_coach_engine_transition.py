"""C6 transition through actual bulk/targeted storage and selection seams."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from pipeline import coach_backfill
from pipeline.coach import ANALYZER_VERSION, LOGICAL_BOUT_ANALYZER_VERSION
from scripts.benchmark_coach_reuse import CountingStore


def legacy_store():
    fixture = json.loads((Path(__file__).parent / "fixtures/coach_legacy_transition.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(value) for key, value in fixture["objects_base64"].items()}
    store.reset_counts()
    return store


def selected(store, identifier):
    pointer = json.loads(store.objects[f"activities/2026/{identifier}/coach-input/v1/latest-ready.json"])
    return json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))


def test_ordinary_bulk_retains_verified_legacy_without_new_history_queue():
    store = legacy_store()
    before = store.objects.copy()
    first = coach_backfill.run(store=store, max_activities=3)
    assert first["generated_this_run"] == 0
    assert first["remaining_activities"] == 0
    assert store.counts["put"] == 0
    assert store.objects == before
    again = coach_backfill.run(store=store, max_activities=3)
    assert again["generated_this_run"] == 0
    assert again["remaining_activities"] == 0
    assert store.objects == before
    assert all(selected(store, str(n))["analyzer_version"] == "1.0.0" for n in (1, 2, 3))

def activity(identifier="3", day="2026-03-05"):
    return {"id": identifier, "date": day, "name": f"Synthetic {identifier}",
            "type": "Run", "moving_seconds": 120.0}


def test_targeted_upgrade_changes_exactly_one_selection_and_bulk_keeps_others():
    store = legacy_store()
    before = store.objects.copy()
    result = coach_backfill.run_one(store=store, activity=activity())
    assert set(result["processed_sources"]) == {"3"}
    assert selected(store, "3")["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    assert selected(store, "3")["sections"][0]["average_heart_rate_bpm"] == 150
    bulk = coach_backfill.run(store=store, max_activities=3)
    assert bulk["generated_this_run"] == bulk["remaining_activities"] == 0
    for key, value in before.items():
        if "/canonical/" in key or key.endswith("/latest-ready.json") and "/3/" not in key:
            assert store.objects[key] == value
    assert selected(store, "1")["analyzer_version"] == selected(store, "2")["analyzer_version"] == "1.0.0"


def test_genuine_changed_legacy_source_generates_new_engine_and_preserves_old_bytes():
    store = legacy_store()
    before = store.objects.copy()
    key = "activities/2026/3/activity.endurance.v1.json"
    store.objects[key] = b'{"available":true,"summary":{"distance_m":6000}}'
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == 1
    assert result["remaining_activities"] == 0
    assert selected(store, "3")["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    for key, value in before.items():
        if "/canonical/" in key:
            assert store.objects[key] == value


def test_unknown_legacy_selection_is_held_without_writes_or_regeneration_queue():
    store = legacy_store()
    key = "activities/2026/3/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[key])
    pointer["schema_version"] = 99
    store.objects[key] = json.dumps(pointer).encode()
    before = store.objects.copy()
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 1
    assert store.counts["put"] == 0
    assert store.objects == before


def test_unverified_legacy_calendar_does_not_start_historical_recalculation():
    store = legacy_store()
    csv = store.objects["summary/activities.csv"].decode()
    store.objects["summary/activities.csv"] = csv.replace("120,2026-03-05", "120,").replace(
        "garmin-3,2026-03-05T10:00:00Z", "garmin-3,2026-13-05T10:00:00Z",
    ).encode()
    before = store.objects.copy()
    with pytest.raises(ValueError):
        coach_backfill.run(store=store, max_activities=3)
    assert store.counts["put"] == 0
    assert store.objects == before


def test_inspection_budget_holds_remaining_history_instead_of_generation_queue():
    store = legacy_store()
    before = store.objects.copy()
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 2
    assert store.objects == before

def add_new(store):
    prefix = "activities/2026/4"
    source = json.loads(store.objects["activities/2026/3/activity.v1.json"])
    source["source_fit_sha256"] = "synthetic-fit-4"
    source["activity"] = {"id": "4", "start_time_local": "2026-10-14T10:00:00"}
    store.objects[prefix + "/activity.v1.json"] = json.dumps(source).encode()
    store.objects[prefix + "/activity.endurance.v1.json"] = store.objects["activities/2026/3/activity.endurance.v1.json"]
    store.objects[prefix + "/activity.tcx"] = b"synthetic tcx"
    store.objects["summary/activities.csv"] += b"garmin-4,2026-10-14T10:00:00Z,Synthetic 4,Run,120,2026-10-14\n"


def test_new_bulk_and_targeted_results_share_identity_without_upgrading_history():
    bulk_store = legacy_store()
    target_store = legacy_store()
    add_new(bulk_store)
    add_new(target_store)
    old = {key: value for key, value in bulk_store.objects.items() if "/canonical/" in key}
    result = coach_backfill.run(store=bulk_store, max_activities=1)
    targeted = coach_backfill.run_one(store=target_store, activity=activity("4", "2026-10-14"))
    assert result["generated_this_run"] == 1
    assert result["remaining_activities"] == 0
    assert result["completed_this_run"][0]["analysis_id"] == targeted["processed_sources"]["4"]
    assert selected(bulk_store, "4")["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    assert selected(bulk_store, "4")["sections"][0]["average_heart_rate_bpm"] == 150
    assert all(bulk_store.objects[key] == value for key, value in old.items())
    repeated = coach_backfill.run(store=bulk_store, max_activities=3)
    assert repeated["generated_this_run"] == repeated["remaining_activities"] == 0


def test_corrupt_canonical_or_unproved_input_signature_is_held():
    for corruption in ("canonical", "input_signature", "missing_canonical", "unknown_engine"):
        store = legacy_store()
        pointer_key = "activities/2026/3/coach-input/v1/latest-ready.json"
        pointer = json.loads(store.objects[pointer_key])
        key = pointer["analysis_key"]
        if corruption == "input_signature":
            pointer["input_signature"] = "0" * 64
            store.objects[pointer_key] = json.dumps(pointer).encode()
        elif corruption == "missing_canonical":
            del store.objects[key]
        else:
            canonical = json.loads(gzip.decompress(store.objects[key]))
            if corruption == "unknown_engine":
                canonical["analyzer_version"] = "unknown"
            else:
                canonical["sections"][0]["average_heart_rate_bpm"] = 99
            store.objects[key] = gzip.compress(json.dumps(canonical).encode())
        before = store.objects.copy()
        result = coach_backfill.run(store=store, max_activities=3)
        assert result["generated_this_run"] == result["remaining_activities"] == 0
        assert result["deferred_legacy_activity_count"] == 1
        assert store.objects == before


def test_cached_plan_notices_source_revision_change_without_manifest_replacement():
    store = legacy_store()
    add_new(store)
    coach_backfill.run(store=store, max_activities=3)
    old = {key: value for key, value in store.objects.items() if "/canonical/" in key}
    key = "activities/2026/4/activity.endurance.v1.json"
    store.objects[key] = b'{"available":true,"summary":{"distance_m":6500}}'
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == 1
    assert selected(store, "4")["summary"]["distance_m"] == 6500
    assert all(store.objects[key] == value for key, value in old.items())

def test_unknown_legacy_inspections_are_charged_to_the_activity_budget():
    store = legacy_store()
    for identifier in ("1", "2", "3"):
        pointer = json.loads(store.objects[f"activities/2026/{identifier}/coach-input/v1/latest-ready.json"])
        candidate = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
        candidate["analyzer_version"] = "unknown"
        store.objects[pointer["analysis_key"]] = gzip.compress(json.dumps(candidate).encode())
    before = store.objects.copy()
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["deferred_legacy_activity_count"] == 3
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert store.counts["get"] <= 5  # summary/profile/plan plus at most one pointer/canonical pair
    assert store.objects == before

def test_corrupt_engine_claim_and_bad_marker_cannot_authorize_history_upgrade():
    store = legacy_store()
    pointer_key = "activities/2026/3/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    key = pointer["analysis_key"]
    candidate = json.loads(gzip.decompress(store.objects[key]))
    candidate["analyzer_version"] = ANALYZER_VERSION  # ID still belongs to untouched 1.0.0 content
    store.objects[key] = gzip.compress(json.dumps(candidate).encode())
    pointer["schema_version"] = True
    store.objects[pointer_key] = json.dumps(pointer).encode()
    before = store.objects.copy()
    with pytest.raises(ValueError, match="identity conflict"):
        coach_backfill.run(store=store, max_activities=3)
    assert store.counts["put"] == 0
    assert store.objects == before

def test_priority_applies_before_legacy_inspections_without_expanding_scope():
    store = legacy_store()
    add_new(store)
    csv = store.objects["summary/activities.csv"].decode()
    store.objects["summary/activities.csv"] = csv.replace("2026-10-14", "2026-01-04").encode()
    source = json.loads(store.objects["activities/2026/4/activity.v1.json"])
    source["activity"]["start_time_local"] = "2026-01-04T10:00:00"
    store.objects["activities/2026/4/activity.v1.json"] = json.dumps(source).encode()
    before = store.objects.copy()
    result = coach_backfill.run(store=store, max_activities=1, priority_activity_ids={"4"})
    assert result["generated_this_run"] == 1 and result["remaining_activities"] == 0
    assert result["completed_this_run"][0]["activity_id"] == "4"
    assert result["deferred_legacy_activity_count"] == 3
    assert all(store.objects[key] == value for key, value in before.items() if "/canonical/" in key)

def test_unmarked_adoption_does_not_hide_pending_new_work_on_ordinary_continuation():
    store = legacy_store()
    lines = store.objects["summary/activities.csv"].decode().splitlines()
    store.objects["summary/activities.csv"] = ("\n".join(
        [lines[0], *[line for line in lines[1:] if line.startswith("garmin-3,")]],
    ) + "\n").encode()
    add_new(store)
    store.objects["summary/activities.csv"] = store.objects["summary/activities.csv"].replace(
        b"2026-10-14", b"2026-01-04",
    )
    pointer_key = "activities/2026/3/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    pointer.pop("selection_version")
    store.objects[pointer_key] = json.dumps(pointer).encode()
    original = {key: value for key, value in store.objects.items()
                if "/canonical/" in key or key.endswith("/latest-ready.json")}
    first = coach_backfill.run(store=store, max_activities=1)
    assert first["generated_this_run"] == 0
    assert first["remaining_activities"] == 1 and first["status"] == "active"
    assert "activities/2026/4/coach-input/v1/latest-ready.json" not in store.objects
    assert all(store.objects[key] == value for key, value in original.items())
    assert store.counts["get"] <= 5
    store.reset_counts()
    second = coach_backfill.run(store=store, max_activities=1)
    assert second["generated_this_run"] == 1
    assert second["remaining_activities"] == 0
    assert selected(store, "4")["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    assert selected(store, "4")["sections"][0]["average_heart_rate_bpm"] == 150
    assert all(store.objects[key] == value for key, value in original.items())


def calendar_store(context_year="2026"):
    store = legacy_store()
    for key in list(store.objects):
        if key.startswith("activities/"):
            del store.objects[key]
    source = json.loads(legacy_store().objects["activities/2026/1/activity.v1.json"])
    source["activity"] = {"id": "1", "start_time_local": "2027-01-01T00:30:00",
                          "start_time_gmt": "2026-12-31T23:30:00Z"}
    prefix = "activities/2027/1"
    store.objects[prefix + "/activity.v1.json"] = json.dumps(source).encode()
    store.objects[prefix + "/activity.endurance.v1.json"] = b'{"available":true,"summary":{"distance_m":5000}}'
    store.objects[prefix + "/activity.tcx"] = b"synthetic tcx"
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        b"garmin-1,2026-12-31T23:30:00Z,Synthetic 1,Run,120,2027-01-01\n"
    )
    del store.objects[coach_backfill.PLAN_KEY]
    old_key = f"activities/{context_year}/1/context/v1/2026-12-31T23-40-00.json"
    store.objects[old_key] = b'{"context_id":"synthetic-old","note":"synthetic old note"}'
    coach_backfill.run(store=store, max_activities=1)
    store.reset_counts()
    return store, old_key


@pytest.mark.parametrize("old_year", ["2026", "2027"])
def test_context_change_preserves_neighbor_year_and_same_year_contract(old_year):
    store, _ = calendar_store(old_year)
    before = store.objects.copy()
    store.objects["activities/2027/1/context/v1/2027-01-01T01-00-00.json"] = (
        b'{"context_id":"synthetic-new","note":"synthetic new note"}'
    )
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["generated_this_run"] == 1
    assert result["remaining_activities"] == result["blocked_activity_count"] == 0
    current = json.loads(gzip.decompress(store.objects[json.loads(
        store.objects["activities/2027/1/coach-input/v1/latest-ready.json"])["analysis_key"]]))
    assert current["user_context"]["context_id"] == "synthetic-new"
    assert current["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    assert all(store.objects[key] == value for key, value in before.items() if "/canonical/" in key)

@pytest.mark.parametrize("bad_key", [
    "activities/2026/2/context/v1/2026-12-31T23-40-00.json",
    "activities/2025/1/context/v1/2026-12-31T23-40-00.json",
    "activities/2028/1/context/v1/2026-12-31T23-40-00.json",
    "activities/2026/1/context/v1/nested/2026-12-31T23-40-00.json",
    "activities/2026/1/context/v1/../escape.json",
    "activities/2026/1/context/v1/2026-12-31T23-40-00.txt",
    "activities/2026/1/context/v1/2026-12-31T23-40-00\\escape.json",
])
def test_saved_context_key_cannot_widen_activity_year_or_basename_scope(bad_key):
    store, old_key = calendar_store()
    pointer_key = "activities/2027/1/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    pointer["input_context_key"] = bad_key
    store.objects[bad_key] = store.objects[old_key]
    store.objects[pointer_key] = json.dumps(pointer).encode()
    # Even a matching retained pointer revision cannot bypass the key scope.
    plan = json.loads(store.objects[coach_backfill.PLAN_KEY])
    plan["selection_revisions"]["1"] = hashlib.sha256(store.objects[pointer_key]).hexdigest()
    store.objects[coach_backfill.PLAN_KEY] = json.dumps(plan).encode()
    store.objects["activities/2027/1/context/v1/2027-01-01T01-00-00.json"] = (
        b'{"context_id":"synthetic-new","note":"synthetic new note"}'
    )
    before = store.objects.copy()
    store.reset_counts()
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 1
    assert store.counts["put"] == 0 and store.objects == before


def test_changed_or_missing_saved_context_revision_remains_unverified():
    for missing in (False, True):
        store, old_key = calendar_store()
        if missing:
            del store.objects[old_key]
        else:
            store.objects[old_key] = b'{"context_id":"synthetic-old","note":"changed old source"}'
        store.objects["activities/2027/1/context/v1/2027-01-01T01-00-00.json"] = (
            b'{"context_id":"synthetic-new","note":"synthetic new note"}'
        )
        before = store.objects.copy()
        store.reset_counts()
        result = coach_backfill.run(store=store, max_activities=1)
        assert result["generated_this_run"] == result["remaining_activities"] == 0
        assert result["deferred_legacy_activity_count"] == 1
        assert store.objects == before and store.counts["put"] == 0
