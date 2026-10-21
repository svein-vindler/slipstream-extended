"""Verified in-frame means through the real Coach producer; synthetic sources only."""
import base64
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from pipeline.coach import build_coach_input
from scripts.build_running_segments_contract import partial_source_case


def test_terminal_pair_excludes_values_but_exposes_in_frame_boundary_time(record_property):
    source = partial_source_case()
    before = deepcopy(source)
    analysis = build_coach_input(**source)
    block = analysis["segment_running_metrics"]
    whole = block["whole_distance_halves"]
    assert whole["status"] == "available"
    assert [row["average_power"]["value"] for row in whole["items"]] == [166.6667, 357.1429]
    assert analysis["analyzer_version"] == "1.8.0" and block["schema_version"] == 2
    assert block["session_end_time_utc"] == "2026-01-01T00:00:08.500Z"
    assert block["source_window"] == {
        "disposition": "verified_terminal_exclusion", "original_record_count": 6,
        "boundary_excluded_record_count": 1, "boundary_excluded_pair_count": 1,
    }
    first, second = [row["average_power"]["coverage"] for row in whole["items"]]
    assert first == {"valid_seconds": 4.5, "eligible_seconds": 4.5, "excluded_gap_seconds": 0,
                     "boundary_excluded_seconds": 0, "contributing_intervals": 3, "status": "complete"}
    assert second == {"valid_seconds": 3.5, "eligible_seconds": 3.5, "excluded_gap_seconds": 0,
                      "boundary_excluded_seconds": 0.5, "contributing_intervals": 2, "status": "partial"}
    assert block["main_distance_halves"]["items"] == whole["items"]
    assert block["executed_intervals"]["reason"] == "no_verified_structured_workout"
    assert whole["items"][1]["average_ground_contact_balance"]["value"] == 101
    assert analysis["recorded_running_metrics"]["distance_halves"]["items"][1]["average_power"]["value"] != 357.1429
    assert source == before
    record_property("actual_partial_analysis", json.dumps(analysis, sort_keys=True))


def test_actual_partial_output_roundtrips_strict_encrypted_backup(tmp_path):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.coach import validate_profile
    from pipeline.granular import gzip_json

    analysis = build_coach_input(**partial_source_case())
    profile = validate_profile({**analysis["profile"], "created_at": "2026-01-01T00:00:00.000Z"})
    objects = {
        f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json": gzip_json(analysis),
        "coach/profiles/v1/2026-01-01/synthetic-partial.json": private_backup._json_bytes(profile),
    }
    archive = _write_document(tmp_path, document(objects))
    assert private_backup.verify_backup(archive, PASSWORD).objects == objects


@pytest.mark.parametrize("damage", ["leading", "interior", "two_outside", "missing_time", "missing_distance",
                                    "negative_distance", "decreasing_distance", "duplicate_time", "one_record"])
def test_terminal_exception_never_hides_invalid_original_geometry(damage):
    source = partial_source_case()
    records = source["decoded_fit"]["messages"]["record_mesgs"]
    if damage == "leading":
        records[0]["timestamp"] = "2025-12-31T23:59:59Z"
    elif damage == "interior":
        records[2]["timestamp"] = "2026-01-01T00:00:10Z"
    elif damage == "two_outside":
        source["decoded_fit"]["messages"]["session_mesgs"][0]["total_elapsed_time"] = 7.5
    elif damage == "missing_time":
        del records[-1]["timestamp"]
    elif damage == "missing_distance":
        del records[-1]["distance"]
    elif damage == "negative_distance":
        records[-1]["distance"] = -1
    elif damage == "decreasing_distance":
        records[-1]["distance"] = 700
    elif damage == "duplicate_time":
        records[-1]["timestamp"] = records[-2]["timestamp"]
    else:
        source["decoded_fit"]["messages"]["record_mesgs"] = records[:1]
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["source_window"]["disposition"] == "unverified"
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        assert block[name]["reason"] == "record_geometry_unverified" and block[name]["items"] == []


def test_fully_in_frame_record_pairs_keep_complete_values_and_zero_boundary():
    source = partial_source_case()
    source["decoded_fit"]["messages"]["session_mesgs"][0]["total_elapsed_time"] = 9
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["source_window"] == {
        "disposition": "verified_in_frame", "original_record_count": 6,
        "boundary_excluded_record_count": 0, "boundary_excluded_pair_count": 0,
    }
    halves = block["whole_distance_halves"]["items"]
    assert [row["average_power"]["value"] for row in halves] == [166.6667, 388.8889]
    assert all(row["average_power"]["coverage"]["boundary_excluded_seconds"] == 0
               and row["average_power"]["coverage"]["status"] == "complete" for row in halves)


def test_a_boundary_pair_alone_never_fills_values_from_legacy_means():
    source = partial_source_case()
    rows = source["decoded_fit"]["messages"]["record_mesgs"]
    source["decoded_fit"]["messages"]["record_mesgs"] = [rows[0], rows[-1]]
    analysis = build_coach_input(**source)
    halves = analysis["segment_running_metrics"]["whole_distance_halves"]["items"]
    assert [row["average_power"]["coverage"]["boundary_excluded_seconds"] for row in halves] == [4.5, 4]
    assert all(row["average_power"]["status"] == "missing" and row["average_power"]["value"] is None
               and row["average_power"]["coverage"]["eligible_seconds"] == 0
               and row["average_power"]["coverage"]["status"] == "none" for row in halves)
    assert all(row["average_power"]["value"] == 100
               for row in analysis["recorded_running_metrics"]["distance_halves"]["items"])


def test_genuine16_remains_readable_bulk_unqueued_and_targeted18_versioned(tmp_path, record_property):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import coach_backfill, private_backup
    from scripts.benchmark_coach_reuse import CountingStore
    from scripts.build_gct_sides_contract import selected
    from scripts.build_running_segments_contract import SEGMENT_ACTIVITY

    fixture = json.loads((Path(__file__).parent / "fixtures/coach_engine_160_partial_transition.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in fixture["objects_base64"].items()}
    old_pointer, old = selected(store)
    before = dict(store.objects)
    assert old["analyzer_version"] == "1.6.0" and old["segment_running_metrics"]["schema_version"] == 1
    assert hashlib.sha256(before[old_pointer["analysis_key"]]).hexdigest() == fixture["canonical_sha256"]
    objects, document = archive_document(store)
    assert private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD).objects == objects
    store.reset_counts()
    bulk = coach_backfill.run(store=store)
    assert bulk["generated_this_run"] == bulk["remaining_activities"] == 0
    assert selected(store)[1] == old
    record_property("genuine16_bulk_engine_only_counts", json.dumps(store.counts))
    store.reset_counts()
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    pointer, current = selected(store)
    assert current["analyzer_version"] == "1.8.0"
    assert pointer["analysis_key"] != old_pointer["analysis_key"]
    assert all(store.objects[key] == body for key, body in before.items()
               if "/canonical/" in key or "/context/" in key or key.startswith("coach/profiles/"))
    record_property("genuine16_targeted18_counts", json.dumps(store.counts))
    store.reset_counts()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["blocked_activities"] == []
    assert selected(store)[1] == current
    record_property("current18_cold_bulk_counts", json.dumps(store.counts))


@pytest.mark.parametrize("path,value", [
    (("schema_version",), True),
    (("source_window", "disposition"), "verified_in_frame"),
    (("source_window", "original_record_count"), None),
    (("source_window", "original_record_count"), 2),
    pytest.param(("source_window", "original_record_count"), 4, id="excluded_pair_not_usable"),
    (("source_window", "boundary_excluded_record_count"), True),
    (("source_window", "boundary_excluded_pair_count"), 2),
    (("source_window", "unexpected"), "synthetic"),
    (("whole_distance_halves", "items", 1, "average_power", "coverage", "boundary_excluded_seconds"), -0.5),
    (("whole_distance_halves", "items", 1, "average_power", "coverage", "boundary_excluded_seconds"), True),
    (("whole_distance_halves", "items", 1, "average_power", "coverage", "status"), "complete"),
    (("whole_distance_halves", "items", 1, "average_power", "coverage", "boundary_excluded_seconds"), 0),
])
def test_strict_encrypted_backup_rejects_forged_partial_metadata(tmp_path, path, value):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.backup_source import BackupError
    from pipeline.coach import validate_profile
    from pipeline.granular import gzip_json

    analysis = build_coach_input(**partial_source_case())
    target = analysis["segment_running_metrics"]
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    profile = validate_profile({**analysis["profile"], "created_at": "2026-01-01T00:00:00.000Z"})
    objects = {
        f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json": gzip_json(analysis),
        "coach/profiles/v1/2026-01-01/synthetic-partial.json": private_backup._json_bytes(profile),
    }
    with pytest.raises(BackupError, match="invalid_segment_running_metrics"):
        private_backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


@pytest.mark.parametrize("group", ["whole_distance_halves", "main_distance_halves"])
def test_joint_half_boundary_budget_is_checked_in_real_backup(tmp_path, group):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.backup_source import BackupError
    from pipeline.coach import validate_profile
    from pipeline.granular import gzip_json

    analysis = build_coach_input(**partial_source_case())
    row = analysis["segment_running_metrics"][group]["items"][1]
    for field in ("average_power", "average_step_length", "average_ground_contact_time",
                  "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance"):
        row[field]["coverage"]["boundary_excluded_seconds"] = 1
    profile = validate_profile({**analysis["profile"], "created_at": "2026-01-01T00:00:00.000Z"})
    objects = {
        f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json": gzip_json(analysis),
        "coach/profiles/v1/2026-01-01/synthetic-partial.json": private_backup._json_bytes(profile),
    }
    with pytest.raises(BackupError, match="invalid_segment_running_metrics"):
        private_backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


def planned_source():
    source = partial_source_case()
    messages = source["decoded_fit"]["messages"]
    messages["workout_step_mesgs"] = [
        {"message_index": 0, "intensity": "warmup"}, {"message_index": 1, "intensity": "active"},
        {"message_index": 2, "intensity": "cooldown"}]
    messages["lap_mesgs"] = [
        {"start_time": f"2026-01-01T00:00:{start:02d}Z", "timestamp": f"2026-01-01T00:00:{start+2:02d}Z",
         "total_elapsed_time": 2, "total_distance": 200, "wkt_step_index": step}
        for start, step in ((0, 0), (2, 1), (4, 1), (6, 2))]
    return source


def test_early_verified_intervals_and_main_keep_complete_coverage_despite_terminal_sentinels():
    source = planned_source()
    records = source["decoded_fit"]["messages"]["record_mesgs"]
    records[-2]["power"], records[-1]["power"] = 999999, 9999999
    block = build_coach_input(**source)["segment_running_metrics"]
    interval = block["executed_intervals"]["items"][0]
    assert interval["source_laps"] == [2, 3] and interval["average_power"]["value"] == 250
    assert interval["average_power"]["coverage"] == {
        "valid_seconds": 4, "eligible_seconds": 4, "excluded_gap_seconds": 0,
        "boundary_excluded_seconds": 0, "contributing_intervals": 2, "status": "complete"}
    main = block["main_distance_halves"]["items"]
    assert [row["average_power"]["value"] for row in main] == [200, 300]
    assert all(row["average_power"]["coverage"]["status"] == "complete"
               and row["average_power"]["coverage"]["boundary_excluded_seconds"] == 0 for row in main)
    assert block["whole_distance_halves"]["items"][1]["average_power"]["value"] == 357.1429


def test_outside_endpoint_never_establishes_a_lap_boundary():
    source = planned_source()
    laps = source["decoded_fit"]["messages"]["lap_mesgs"]
    laps[1]["total_elapsed_time"] = 6.5
    laps[1]["total_distance"] = 650
    source["decoded_fit"]["messages"]["lap_mesgs"] = laps[:2]
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["whole_distance_halves"]["status"] == "available"
    assert block["executed_intervals"]["reason"] == block["main_distance_halves"]["reason"] == "main_distance_alignment_unverified"


@pytest.mark.parametrize("overlap", [0.25, 1.25])
def test_subsecond_and_larger_actual_lap_overlap_remain_rejected(overlap):
    source = planned_source()
    source["decoded_fit"]["messages"]["lap_mesgs"][1]["total_elapsed_time"] += overlap
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["whole_distance_halves"]["status"] == "available"
    assert block["executed_intervals"]["reason"] == block["main_distance_halves"]["reason"] == "clock_basis_unverified"


def test_nominal_whole_distance_mismatch_does_not_taint_verified_main():
    source = planned_source()
    source["endurance"]["summary"]["distance_m"] = 1000
    for half in source["endurance"]["distance_halves"].values():
        half["distance_m"] = 500
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["whole_distance_halves"]["reason"] == "distance_alignment_unverified"
    assert block["executed_intervals"]["items"][0]["average_power"]["value"] == 250
    assert [row["average_power"]["value"] for row in block["main_distance_halves"]["items"]] == [200, 300]


def test_one_original_pair_across_active_laps_still_counts_once():
    source = planned_source()
    del source["decoded_fit"]["messages"]["record_mesgs"][2]
    block = build_coach_input(**source)["segment_running_metrics"]
    metric = block["executed_intervals"]["items"][0]["average_power"]
    assert metric["value"] == 200 and metric["coverage"]["contributing_intervals"] == 1
    assert metric["coverage"]["valid_seconds"] == 4 and metric["coverage"]["boundary_excluded_seconds"] == 0


def test_partial_normal_bulk_targeted_and_retry_share_immutable_content(monkeypatch, record_property):
    from pipeline import coach_backfill
    from scripts.benchmark_coach_reuse import CountingStore
    from scripts.build_gct_sides_contract import selected
    from scripts.build_running_segments_contract import partial_store_case

    store, activity = partial_store_case()
    bulk = CountingStore()
    bulk.objects = dict(store.objects)
    store.reset_counts()
    coach_backfill.run_one(activity=activity, store=store)
    pointer, analysis = selected(store)
    body = store.objects[pointer["analysis_key"]]
    assert analysis["analyzer_version"] == "1.8.0"
    assert analysis["segment_running_metrics"]["whole_distance_halves"]["items"][1]["average_power"]["value"] == 357.1429
    record_property("partial_targeted_counts", json.dumps(store.counts))
    bulk.reset_counts()
    coach_backfill.run(store=bulk)
    assert selected(bulk)[0]["analysis_key"] == pointer["analysis_key"]
    assert bulk.objects[pointer["analysis_key"]] == body
    record_property("partial_bulk_counts", json.dumps(bulk.counts))
    store.reset_counts()
    coach_backfill.run_one(activity=activity, store=store)
    assert store.objects[pointer["analysis_key"]] == body and store.counts["put"] == 0
    record_property("partial_repeat_counts", json.dumps(store.counts))
    retry, retry_activity = partial_store_case()
    put = retry.put

    def interrupted(key, *args, **kwargs):
        if key.endswith("/latest-ready.json"):
            raise RuntimeError("synthetic pointer interruption")
        return put(key, *args, **kwargs)

    monkeypatch.setattr(retry, "put", interrupted)
    with pytest.raises(RuntimeError, match="synthetic pointer interruption"):
        coach_backfill.run_one(activity=retry_activity, store=retry)
    canonicals = {key: value for key, value in retry.objects.items() if "/canonical/" in key}
    assert len(canonicals) == 1
    monkeypatch.setattr(retry, "put", put)
    retry.reset_counts()
    coach_backfill.run_one(activity=retry_activity, store=retry)
    assert all(retry.objects[key] == value for key, value in canonicals.items())
    assert selected(retry)[0]["analysis_key"] in canonicals and retry.counts["put"] == 1
    record_property("partial_pointer_retry_counts", json.dumps(retry.counts))


@pytest.mark.parametrize("raw,status", [(None, "missing"), (True, "invalid"), (0, "available"),
                                      (-1, "available"), (float("nan"), "invalid"), (float("inf"), "invalid")])
def test_partial_fields_preserve_missing_invalid_and_zero_without_boundary_fill(raw, status):
    source = partial_source_case()
    for row in source["decoded_fit"]["messages"]["record_mesgs"]:
        row["power"] = raw
    source["decoded_fit"]["messages"]["record_mesgs"][-1]["power"] = 9999999
    block = build_coach_input(**source)["segment_running_metrics"]
    for index, row in enumerate(block["whole_distance_halves"]["items"]):
        metric = row["average_power"]
        assert metric["status"] == status and metric["value"] == (raw if status == "available" else None)
        assert metric["coverage"]["eligible_seconds"] == (4.5 if index == 0 else 3.5)
        assert metric["coverage"]["valid_seconds"] == (
            (4.5 if index == 0 else 3.5) if status == "available" else 0)
        assert metric["coverage"]["boundary_excluded_seconds"] == (0 if index == 0 else 0.5)
        assert metric["coverage"]["status"] == (
            ("complete" if index == 0 else "partial") if status == "available" else "none")
        assert row["average_step_length"]["value"] == 1000 and row["average_step_length"]["unit"] == "mm"
        assert row["average_ground_contact_balance"]["value"] == 101
        assert row["average_ground_contact_balance"]["side_semantics"] == "unknown"


def test_partial_original_gap_and_boundary_time_are_never_double_counted():
    source = partial_source_case()
    messages = source["decoded_fit"]["messages"]
    rows = messages["record_mesgs"]
    messages["record_mesgs"] = [rows[0], rows[-2], rows[-1]]
    messages["record_mesgs"][1]["timestamp"] = "2026-01-01T00:00:40Z"
    messages["record_mesgs"][2]["timestamp"] = "2026-01-01T00:00:42Z"
    messages["session_mesgs"][0]["total_elapsed_time"] = 41
    block = build_coach_input(**source)["segment_running_metrics"]
    halves = block["whole_distance_halves"]["items"]
    assert [row["average_power"]["coverage"]["excluded_gap_seconds"] for row in halves] == [22.5, 17.5]
    assert [row["average_power"]["coverage"]["boundary_excluded_seconds"] for row in halves] == [0, 1]
    assert all(row["average_power"]["value"] is None
               and row["average_power"]["coverage"]["eligible_seconds"] == 0 for row in halves)


def test_terminal_exclusion_uses_no_seconds_tolerance_and_stationary_pairs_add_no_time():
    source = partial_source_case()
    records = source["decoded_fit"]["messages"]["record_mesgs"]
    records[-1]["timestamp"] = "2026-01-01T01:00:00Z"
    records[-1]["distance"] = 800
    source["endurance"]["summary"]["distance_m"] = 800
    for half in source["endurance"]["distance_halves"].values():
        half["distance_m"] = 400
    block = build_coach_input(**source)["segment_running_metrics"]
    assert block["source_window"]["disposition"] == "verified_terminal_exclusion"
    halves = block["whole_distance_halves"]["items"]
    assert [row["average_power"]["value"] for row in halves] == [150, 350]
    assert all(row["average_power"]["coverage"]["status"] == "complete"
               and row["average_power"]["coverage"]["boundary_excluded_seconds"] == 0
               and row["average_power"]["coverage"]["excluded_gap_seconds"] == 0 for row in halves)
