"""Native-lap metadata through authenticated backup and ordinary stored reuse."""
import gzip
import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_coach_context_backup import document
from test_private_backup import PASSWORD, _write_document

from pipeline import private_backup as backup
from pipeline.coach import validate_profile
from pipeline.granular import gzip_json


def historical_analysis():
    path = Path(__file__).parents[1] / "worker/test-runtime/fixtures/coach17-intermediate-contract.json"
    return json.loads(path.read_text())["partial"]["analysis"]


def native_analysis():
    analysis = historical_analysis()
    analysis["analyzer_version"] = "1.8.0"
    analysis["native_lap_workout_binding"] = {
        "schema_version": 1, "method": "fit_lap_wkt_step_index",
        "measurement_source": "recorded_running_metrics.laps",
        "activity_id": "42", "source_fit_sha256": analysis["source"]["fit_sha256"],
        "status": "unavailable", "reason": "recorded_laps_unavailable", "items": [],
    }
    return analysis


def objects(analysis):
    profile = validate_profile({**analysis["profile"], "created_at": "2026-01-01T00:00:00.000Z"})
    return {
        f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json": gzip_json(analysis),
        "coach/profiles/v1/2026-01-01/synthetic-partial.json": backup._json_bytes(profile),
    }


def verify(tmp_path, analysis):
    stored = objects(analysis)
    return backup.verify_backup(_write_document(tmp_path, document(stored)), PASSWORD), stored


def test_native_unavailable_metadata_roundtrips_backup_without_rewriting_17(tmp_path):
    old = historical_analysis()
    before = deepcopy(old)
    verified, original = verify(tmp_path, old)
    assert verified.objects == original and old == before
    current = native_analysis()
    verified, original = verify(tmp_path, current)
    assert verified.objects == original
    assert current["segment_running_metrics"] == old["segment_running_metrics"]
    assert current["recorded_running_metrics"] == old["recorded_running_metrics"]


def fixture_analysis(name):
    return json.loads((Path(__file__).parent / "fixtures" / name).read_text())


def test_actual_native_links_roundtrip_backup_with_old_clock_gate_preserved(tmp_path):
    old = fixture_analysis("coach_engine_170_native_transition.json")
    current = fixture_analysis("coach_engine_180_native_binding.json")
    assert verify(tmp_path, old)[0].objects == objects(old)
    verified, original = verify(tmp_path, current)
    assert verified.objects == original
    assert current["native_lap_workout_binding"]["items"] == [
        {"lap": 1, "source_message_index": 7, "status": "bound", "reason": None, "workout_step_index": 3},
        {"lap": 2, "source_message_index": 9, "status": "bound", "reason": None, "workout_step_index": 3},
    ]
    assert current["segment_running_metrics"] == old["segment_running_metrics"]
    assert current["segment_running_metrics"]["executed_intervals"]["reason"] == "clock_basis_unverified"
    assert [lap["average_power"]["value"] for lap in current["recorded_running_metrics"]["laps"]["items"]] == [111, 222]


@pytest.mark.parametrize("damage", ["missing_block", "extra", "old_engine", "digest", "activity",
                                    "order", "duplicate_source", "boolean_source", "unsafe_source",
                                    "missing_plan", "duplicate_plan", "missing_execution", "duplicate_execution",
                                    "execution_step", "execution_clock", "execution_distance", "state", "unbound_step"])
def test_native_backup_rejects_tampered_references(tmp_path, damage):
    current = fixture_analysis("coach_engine_180_native_binding.json")
    block = current["native_lap_workout_binding"]
    item = block["items"][0]
    structure = current["workout_structure"]
    if damage == "missing_block":
        del current["native_lap_workout_binding"]
    elif damage == "extra":
        item["average_power"] = 111
    elif damage == "old_engine":
        current["analyzer_version"] = "1.7.0"
    elif damage == "digest":
        block["source_fit_sha256"] = "c" * 64
    elif damage == "activity":
        block["activity_id"] = "43"
    elif damage == "order":
        block["items"].reverse()
    elif damage == "duplicate_source":
        block["items"][1]["source_message_index"] = 7
    elif damage == "boolean_source":
        item["source_message_index"] = True
    elif damage == "unsafe_source":
        item["source_message_index"] = 2**53
    elif damage == "missing_plan":
        structure["planned_steps"] = []
    elif damage == "duplicate_plan":
        structure["planned_steps"] *= 2
    elif damage == "missing_execution":
        structure["executed_laps"] = structure["executed_laps"][1:]
    elif damage == "duplicate_execution":
        structure["executed_laps"].append(deepcopy(structure["executed_laps"][0]))
    elif damage == "execution_step":
        structure["executed_laps"][0]["workout_step_index"] = 4
    elif damage == "execution_clock":
        structure["executed_laps"][0]["start_time"] = "2026-01-01T00:00:00.001Z"
    elif damage == "execution_distance":
        structure["executed_laps"][0]["distance_m"] = 0
    elif damage == "state":
        block["status"] = "partial"
    else:
        item.update(status="unbound", reason="workout_step_unverified")
    with pytest.raises(backup.BackupError):
        verify(tmp_path, current)


@pytest.mark.parametrize("case", ["sparse", "unbound", "partial"])
def test_native_backup_preserves_real_sparse_and_unbound_outputs(tmp_path, case):
    from pipeline.coach import build_coach_input
    from scripts.build_native_lap_binding_contract import build_cases, native_source_case

    if case == "partial":
        source = native_source_case()
        del source["decoded_fit"]["messages"]["lap_mesgs"][1]["wkt_step_index"]
        current = build_coach_input(**source)
        assert current["native_lap_workout_binding"]["status"] == "partial"
        assert current["native_lap_workout_binding"]["reason"] == "some_laps_unbound"
    else:
        current = build_cases()[case]
    verified, original = verify(tmp_path, current)
    assert verified.objects == original
    if case == "sparse":
        first, second = current["recorded_running_metrics"]["laps"]["items"]
        assert first["average_power"]["value"] is None and first["distance_m"] is None
        assert second["average_power"]["value"] == 0
    if case == "unbound":
        assert current["native_lap_workout_binding"]["status"] == "unavailable"
        assert current["native_lap_workout_binding"]["reason"] == "no_verified_lap_workout_links"


def test_native_current_bulk_reuses_new_fingerprint_without_history_rewrite():
    from pipeline import coach_backfill
    from scripts.benchmark_coach_reuse import CountingStore

    store = CountingStore()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30,Synthetic run,Run,1800\n")
    activity = {"id": "1", "date": "2026-09-30", "name": "Synthetic run", "type": "Run", "moving_seconds": 1800.0}
    first = coach_backfill.run_one(activity=activity, store=store)
    pointer = json.loads(store.objects["activities/2026/1/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["analyzer_version"] == pointer["analyzer_version"] == "1.9.0"
    assert analysis["native_lap_workout_binding"]["reason"] == "source_unavailable"
    before = store.objects.copy()
    store.reset_counts()
    bulk = coach_backfill.run(store=store, max_activities=1)
    assert bulk["generated_this_run"] == bulk["remaining_activities"] == 0
    assert bulk.get("deferred_legacy_activity_count", 0) == 0
    # The first bulk inspection records its reuse proof in its control plan;
    # it must not rewrite the canonical analysis, selection, profile or sources.
    assert {key: value for key, value in store.objects.items() if key != coach_backfill.PLAN_KEY} == before
    assert coach_backfill.PLAN_KEY in store.objects and store.counts["put"] == 1
    checkpoint = store.objects.copy()
    store.reset_counts()
    retained = coach_backfill.run(store=store, max_activities=1)
    assert retained["generated_this_run"] == retained["remaining_activities"] == 0
    assert retained.get("deferred_legacy_activity_count", 0) == 0
    assert store.objects == checkpoint and store.counts["put"] == 0
    replay = coach_backfill.run_one(activity=activity, store=store)
    assert replay["reused_sources"] == first["processed_sources"]
    assert store.objects == checkpoint and store.counts["put"] == 0
