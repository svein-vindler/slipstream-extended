"""Retain real historic engines while publishing changed/new current input."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_recorded_running_metrics import ACTIVITY, PROFILE, decoded_source

from pipeline import coach_backfill
from pipeline.coach import ANALYZER_VERSION, STORED_CONTEXT_ANALYZER_VERSION
from pipeline.granular import gzip_json
from scripts.benchmark_coach_reuse import CountingStore


def historic_store():
    fixture = json.loads((Path(__file__).parent / "fixtures/coach_engine_101_transition.json").read_text())
    assert fixture["analyzer_version"] == "1.0.1"
    store = CountingStore()
    store.objects = {key: base64.b64decode(value) for key, value in fixture["objects_base64"].items()}
    store.reset_counts()
    return store


def selected(store, identifier):
    pointer = json.loads(store.objects[f"activities/2026/{identifier}/coach-input/v1/latest-ready.json"])
    return json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))


@pytest.mark.parametrize("cold", [False, True])
def test_genuine_101_history_stays_unchanged_without_generation_or_writes(cold, monkeypatch):
    store = historic_store()
    if cold:
        plan = json.loads(store.objects[coach_backfill.PLAN_KEY])
        plan.pop("bulk_reuse_proofs", None)
        store.objects[coach_backfill.PLAN_KEY] = json.dumps(plan).encode()
    before = store.objects.copy()

    def must_not_rebuild(**kwargs):
        pytest.fail("Unchanged genuine 1.0.1 history must never be rebuilt")

    monkeypatch.setattr(coach_backfill, "build_coach_input", must_not_rebuild)
    for _ in range(2):
        result = coach_backfill.run(store=store, max_activities=3)
        assert result["generated_this_run"] == result["remaining_activities"] == 0
        assert result["deferred_legacy_activity_count"] == 0
        assert result["already_current_activities"] == 3
        assert store.counts["put"] == 0 and store.objects == before
    assert all(selected(store, str(n))["analyzer_version"] == "1.0.1" for n in (1, 2, 3))


def test_101_checkpoint_then_explicit_same_input_110_selection_is_bulk_noop(monkeypatch):
    store = historic_store()
    original = store.objects.copy()
    target = {"id": "3", "date": "2026-03-05", "name": "Synthetic 3",
              "type": "Run", "moving_seconds": 120.0}
    coach_backfill.run_one(store=store, activity=target)
    assert selected(store, "3")["analyzer_version"] == ANALYZER_VERSION
    assert store.objects[coach_backfill.PLAN_KEY] == original[coach_backfill.PLAN_KEY]
    after_targeted = store.objects.copy()
    store.reset_counts()

    def must_not_rebuild(**kwargs):
        pytest.fail("Same-input targeted upgrade must not cause redundant bulk build")

    monkeypatch.setattr(coach_backfill, "build_coach_input", must_not_rebuild)
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 0
    assert store.counts["put"] == 0 and store.objects == after_targeted
    assert all(store.objects[key] == value for key, value in original.items() if "/canonical/" in key)


def add_recorded_activity(store):
    prefix = "activities/2026/42"
    store.objects[prefix + "/activity.v1.json"] = gzip_json(decoded_source())
    store.objects[prefix + "/activity.endurance.v1.json"] = gzip_json({
        "available": True, "activity": {"id": "42", "start_time": "2026-10-07T23:30:05Z"},
        "summary": {"distance_m": 5000},
    })
    store.objects[prefix + "/activity.tcx"] = b"synthetic tcx input"
    store.objects["coach/profiles/v1/2026-01-01/test.json"] = json.dumps(PROFILE).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        b"garmin-42,2026-10-07T23:30:00Z,Synthetic recorded run,Run,1800,2026-10-08\n"
    )


def test_new_bulk_and_targeted_share_recorded_identity_and_repeat_bytes():
    bulk, target = historic_store(), historic_store()
    add_recorded_activity(bulk)
    add_recorded_activity(target)
    first = coach_backfill.run(store=bulk, max_activities=1)
    one = coach_backfill.run_one(store=target, activity=ACTIVITY)
    assert first["completed_this_run"][0]["analysis_id"] == one["processed_sources"]["42"]
    assert selected(bulk, "42") == selected(target, "42")
    block = selected(bulk, "42")["recorded_running_metrics"]
    assert block["association"] == "matched" and block["average_power"]["value"] == 250
    before = bulk.objects.copy()
    bulk.reset_counts()
    again = coach_backfill.run(store=bulk, max_activities=1)
    assert again["generated_this_run"] == 1  # Saved complete checkpoint's original result.
    assert bulk.counts["put"] == 0 and bulk.objects == before
    # FIT/TCX are still read once; five bounded known-day context reads now also
    # attest absence. They do not contact a provider or scan health history.
    assert target.counts["get"] == 9  # Profile + FIT + endurance + TCX + five context keys.


def test_changed_recorded_input_gets_new_id_and_keeps_old_canonical_bytes():
    store = historic_store()
    add_recorded_activity(store)
    coach_backfill.run(store=store, max_activities=1)
    old = {key: value for key, value in store.objects.items() if "/canonical/" in key}
    old_id = selected(store, "42")["analysis_id"]
    store.objects["activities/2026/42/activity.v1.json"] = gzip_json(decoded_source(avg_power=275))
    result = coach_backfill.run(store=store, max_activities=1)
    assert result["generated_this_run"] == 1
    current = selected(store, "42")
    assert current["analysis_id"] != old_id
    assert current["recorded_running_metrics"]["average_power"]["value"] == 275
    assert all(store.objects[key] == value for key, value in old.items())


def test_saved_mixed_engine_proofs_are_warm_noop_without_rebuilding(monkeypatch):
    store = historic_store()
    old_summary = store.objects["summary/activities.csv"]
    old_profile = store.objects["coach/profiles/v1/2026-01-01/test.json"]
    old = {key: value for key, value in store.objects.items() if "/canonical/" in key}
    add_recorded_activity(store)
    # The new recorded activity must not change all historical profiles.
    store.objects["coach/profiles/v1/2026-01-01/test.json"] = old_profile
    new_row = store.objects["summary/activities.csv"].splitlines()[1] + b"\n"
    store.objects["summary/activities.csv"] = old_summary + new_row
    result = coach_backfill.run(store=store, max_activities=4)
    assert result["generated_this_run"] == 1 and result["already_current_activities"] == 3
    plan = json.loads(store.objects[coach_backfill.PLAN_KEY])
    assert {proof["analyzer_version"] for proof in plan["bulk_reuse_proofs"].values()} == {
        "1.0.1", STORED_CONTEXT_ANALYZER_VERSION,
    }
    before = store.objects.copy()
    store.reset_counts()

    def must_not_rebuild(**kwargs):
        pytest.fail("Persisted mixed-engine proofs must be a warm no-op")

    monkeypatch.setattr(coach_backfill, "build_coach_input", must_not_rebuild)
    assert coach_backfill.run(store=store, max_activities=4) == result
    assert store.counts["put"] == 0 and store.objects == before
    assert all(store.objects[key] == value for key, value in old.items())


@pytest.mark.parametrize("corruption", ["engine", "content", "marker"])
def test_corrupt_historical_101_is_held_not_repaired(corruption):
    store = historic_store()
    pointer_key = "activities/2026/3/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    if corruption == "marker":
        pointer["schema_version"] = True
        store.objects[pointer_key] = json.dumps(pointer).encode()
    else:
        canonical = selected(store, "3")
        canonical["analyzer_version" if corruption == "engine" else "summary"] = (
            "unknown" if corruption == "engine" else {"moving_seconds": 999})
        store.objects[pointer["analysis_key"]] = gzip_json(canonical)
    before = store.objects.copy()
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 1
    assert store.counts["put"] == 0 and store.objects == before
