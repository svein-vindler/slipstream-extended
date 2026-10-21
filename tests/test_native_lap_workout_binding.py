"""Native metadata through complete Coach Input, with synthetic FIT messages."""
import json
from copy import deepcopy
from pathlib import Path

import pytest

from pipeline.coach import analysis_id, build_coach_input
from scripts.build_native_lap_binding_contract import build_cases, native_source_case


def test_native_laps_bind_explicit_step_even_when_derived_lap_clocks_are_unverified(record_property):
    source = native_source_case()
    before = deepcopy(source)
    analysis = build_coach_input(**source)
    record_property("actual_coach_json", json.dumps(analysis, sort_keys=True))
    assert analysis["segment_running_metrics"]["executed_intervals"]["reason"] == "clock_basis_unverified"
    assert analysis["segment_running_metrics"]["main_distance_halves"]["reason"] == "clock_basis_unverified"
    assert analysis["native_lap_workout_binding"] == {
        "schema_version": 1, "method": "fit_lap_wkt_step_index",
        "measurement_source": "recorded_running_metrics.laps", "activity_id": "42",
        "source_fit_sha256": "a" * 64, "status": "available", "reason": None,
        "items": [
            {"lap": 1, "source_message_index": 7, "status": "bound", "reason": None, "workout_step_index": 3},
            {"lap": 2, "source_message_index": 9, "status": "bound", "reason": None, "workout_step_index": 3},
        ],
    }
    assert analysis["analyzer_version"] == "1.8.0"
    assert [row["average_power"]["value"] for row in analysis["recorded_running_metrics"]["laps"]["items"]] == [111, 222]
    assert analysis["recorded_running_metrics"]["laps"]["items"][0]["average_ground_contact_balance"]["side_semantics"] == "unknown"
    assert source == before


def test_sparse_native_fields_keep_links_without_copying_values_or_filling_nulls():
    source = native_source_case()
    first, second = source["decoded_fit"]["messages"]["lap_mesgs"]
    first["avg_power"] = None
    first["avg_step_length"] = True
    first["total_distance"] = None
    second["avg_power"] = 0
    analysis = build_coach_input(**source)
    block = analysis["native_lap_workout_binding"]
    assert block["status"] == "available" and block["reason"] is None
    assert [row["workout_step_index"] for row in block["items"]] == [3, 3]
    assert all(set(row) == {"lap", "source_message_index", "status", "reason", "workout_step_index"}
               for row in block["items"])
    laps = analysis["recorded_running_metrics"]["laps"]["items"]
    assert laps[0]["distance_m"] is None
    assert laps[0]["average_power"]["status"] == "missing" and laps[0]["average_power"]["value"] is None
    assert laps[0]["average_step_length"]["status"] == "invalid"
    assert laps[1]["average_power"]["status"] == "available" and laps[1]["average_power"]["value"] == 0
    assert laps[1]["average_ground_contact_balance"]["value"] == 103
    assert laps[1]["average_ground_contact_balance"]["side_semantics"] == "unknown"


@pytest.mark.parametrize("field", ["message_index", "wkt_step_index"])
@pytest.mark.parametrize("raw", [None, True, -1, 3.0, "3", 2**53, [], {}])
def test_invalid_explicit_lap_indices_never_borrow_legacy_coercion(field, raw):
    source = native_source_case()
    source["decoded_fit"]["messages"]["lap_mesgs"][0][field] = raw
    block = build_coach_input(**source)["native_lap_workout_binding"]
    assert block["status"] == "partial" and block["reason"] == "some_laps_unbound"
    assert block["items"][0] == {
        "lap": 1, "source_message_index": None if field == "message_index" else 7,
        "status": "unbound", "reason": "lap_identity_unverified" if field == "message_index" else "workout_step_unverified",
        "workout_step_index": None,
    }
    assert block["items"][1] == {
        "lap": 2, "source_message_index": 9, "status": "bound", "reason": None, "workout_step_index": 3,
    }


def test_duplicate_raw_lap_message_indices_leave_all_corresponding_laps_unbound():
    source = native_source_case()
    source["decoded_fit"]["messages"]["lap_mesgs"][0]["message_index"] = 9
    block = build_coach_input(**source)["native_lap_workout_binding"]
    assert block["status"] == "unavailable" and block["reason"] == "no_verified_lap_workout_links"
    assert [row["lap"] for row in block["items"]] == [1, 2]
    assert all(row["source_message_index"] is None and row["workout_step_index"] is None
               and row["status"] == "unbound" and row["reason"] == "lap_identity_unverified"
               for row in block["items"])


@pytest.mark.parametrize("raw", [None, False, -1, 3.0, "3", 2**53, [], {}])
def test_invalid_explicit_plan_index_never_gains_identity_from_legacy_projection(raw):
    source = native_source_case()
    source["decoded_fit"]["messages"]["workout_step_mesgs"][0]["message_index"] = raw
    block = build_coach_input(**source)["native_lap_workout_binding"]
    assert block["status"] == "unavailable" and block["reason"] == "no_verified_lap_workout_links"
    assert [row["source_message_index"] for row in block["items"]] == [7, 9]
    assert all(row["status"] == "unbound" and row["reason"] == "workout_step_unverified"
               and row["workout_step_index"] is None for row in block["items"])


@pytest.mark.parametrize("mode", ["missing_lap_index", "missing_lap_step", "missing_plan_index", "duplicate_plan_index",
                                 "ordinal_fallback_plan", "legacy_plan_collision", "coerced_execution_distance"])
def test_missing_ambiguous_or_inconsistent_links_stay_explicitly_unbound(mode):
    source = native_source_case()
    messages = source["decoded_fit"]["messages"]
    reason, status, unbound_count = "workout_step_unverified", "unavailable", 2
    if mode == "missing_lap_index":
        del messages["lap_mesgs"][0]["message_index"]
        reason, status, unbound_count = "lap_identity_unverified", "partial", 1
    elif mode == "missing_lap_step":
        del messages["lap_mesgs"][0]["wkt_step_index"]
        status, unbound_count = "partial", 1
    elif mode == "missing_plan_index":
        del messages["workout_step_mesgs"][0]["message_index"]
    elif mode == "duplicate_plan_index":
        messages["workout_step_mesgs"].append({"message_index": 3, "intensity": "active"})
    elif mode == "ordinal_fallback_plan":
        messages["workout_step_mesgs"] = [
            {"message_index": 0}, {"message_index": 1}, {"message_index": 2}, {"intensity": "active"}]
    elif mode == "legacy_plan_collision":
        messages["workout_step_mesgs"].append({"message_index": 3.5, "intensity": "active"})
        reason = "executed_lap_mismatch"
    else:
        messages["lap_mesgs"][0]["total_distance"] = "400"
        reason, status, unbound_count = "executed_lap_mismatch", "partial", 1
    block = build_coach_input(**source)["native_lap_workout_binding"]
    assert block["status"] == status
    assert block["reason"] == ("some_laps_unbound" if status == "partial" else "no_verified_lap_workout_links")
    assert [row["lap"] for row in block["items"]] == [1, 2]
    assert all(row["status"] == "unbound" and row["reason"] == reason
               and row["workout_step_index"] is None for row in block["items"][:unbound_count])
    if unbound_count == 1:
        assert block["items"][1]["status"] == "bound" and block["items"][1]["workout_step_index"] == 3


def test_explicit_zero_and_largest_safe_lap_index_remain_valid():
    source = native_source_case()
    messages = source["decoded_fit"]["messages"]
    for row, index in zip(messages["lap_mesgs"], [0, 2**53 - 1], strict=True):
        row["message_index"], row["wkt_step_index"] = index, 0
    messages["workout_step_mesgs"][0]["message_index"] = 0
    block = build_coach_input(**source)["native_lap_workout_binding"]
    assert block["status"] == "available"
    assert [row["source_message_index"] for row in block["items"]] == [0, 2**53 - 1]
    assert [row["workout_step_index"] for row in block["items"]] == [0, 0]


@pytest.mark.parametrize("mode", ["decode_error", "missing_digest", "malformed_digest", "wrong_date"])
def test_unverified_source_leaves_only_native_link_metadata_unavailable(mode):
    source = native_source_case()
    if mode == "decode_error":
        source["decoded_fit"]["decode_errors"] = ["synthetic_failure"]
    elif mode == "missing_digest":
        del source["decoded_fit"]["source_fit_sha256"]
    elif mode == "malformed_digest":
        source["decoded_fit"]["source_fit_sha256"] = "synthetic-not-a-digest"
    else:
        source["activity"]["date"] = "2026-01-02"
    analysis = build_coach_input(**source)
    assert analysis["analyzer_version"] == "1.8.0"
    assert analysis["native_lap_workout_binding"] == {
        "schema_version": 1, "method": "fit_lap_wkt_step_index",
        "measurement_source": "recorded_running_metrics.laps", "activity_id": "42",
        "source_fit_sha256": None, "status": "unavailable", "reason": "source_unavailable", "items": [],
    }
    assert "segment_running_metrics" in analysis and "recorded_running_metrics" in analysis


@pytest.mark.parametrize("mode", ["no_laps", "invalid_lap", "unknown_summary_end"])
def test_unavailable_native_laps_keep_verified_source_without_inventing_links(mode):
    source = native_source_case()
    messages = source["decoded_fit"]["messages"]
    if mode == "no_laps":
        messages["lap_mesgs"] = []
    elif mode == "invalid_lap":
        del messages["lap_mesgs"][0]["start_time"]
    else:
        del messages["session_mesgs"][0]["timestamp"]
    analysis = build_coach_input(**source)
    assert analysis["native_lap_workout_binding"] == {
        "schema_version": 1, "method": "fit_lap_wkt_step_index",
        "measurement_source": "recorded_running_metrics.laps", "activity_id": "42",
        "source_fit_sha256": "a" * 64, "status": "unavailable", "reason": "recorded_laps_unavailable", "items": [],
    }


def test_genuine17_fields_and_seed_stay_unchanged_while_new_identity_binds_metadata():
    before = json.loads((Path(__file__).parent / "fixtures/coach_engine_170_native_lap_transition.json").read_text())
    after = build_coach_input(**native_source_case())
    assert before["analyzer_version"] == "1.7.0" and "native_lap_workout_binding" not in before
    assert {key: value for key, value in after.items()
            if key not in ("analysis_id", "analyzer_version", "native_lap_workout_binding")} == {
                key: value for key, value in before.items() if key not in ("analysis_id", "analyzer_version")}
    seed = {
        "activity_id": before["activity_id"], "fit_sha256": before["source"]["fit_sha256"],
        "tcx_sha256": before["source"]["tcx_sha256"], "profile_id": before["profile"]["profile_id"],
        "context_revision": None, "training_effect": before["training_effect"],
        "gct_sides": before["ground_contact_sides"], "segment_metrics": before["segment_running_metrics"],
    }
    assert analysis_id(**seed) == before["analysis_id"]
    assert analysis_id(**seed, native_lap_workout_binding=after["native_lap_workout_binding"]) == after["analysis_id"]
    assert after["analysis_id"] != before["analysis_id"]
    assert build_coach_input(**native_source_case()) == after
    changed = native_source_case()
    changed["decoded_fit"]["messages"]["lap_mesgs"][0]["message_index"] = 8
    newer = build_coach_input(**changed)
    assert newer["analysis_id"] != after["analysis_id"]
    assert newer["recorded_running_metrics"] == after["recorded_running_metrics"]
    assert newer["segment_running_metrics"] == after["segment_running_metrics"]
    assert newer["workout_structure"] == after["workout_structure"]


def test_exported_contract_cases_are_actual_sparse_complete_and_unbound_producer_outputs():
    cases = build_cases()
    assert set(cases) == {"complete", "sparse", "unbound"}
    assert all(analysis["analyzer_version"] == "1.8.0" for analysis in cases.values())
    assert cases["complete"]["native_lap_workout_binding"]["status"] == "available"
    assert cases["sparse"]["native_lap_workout_binding"]["status"] == "available"
    assert cases["sparse"]["recorded_running_metrics"]["laps"]["items"][0]["average_power"]["value"] is None
    assert cases["sparse"]["recorded_running_metrics"]["laps"]["items"][1]["average_power"]["value"] == 0
    assert cases["unbound"]["native_lap_workout_binding"]["status"] == "unavailable"
    assert cases["unbound"]["native_lap_workout_binding"]["reason"] == "no_verified_lap_workout_links"
