"""New segment behavior through real SDK sources and Coach publication."""
import base64
import gzip
import json
from pathlib import Path

import pytest

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore
from scripts.build_gct_sides_contract import selected
from scripts.build_running_segments_contract import SEGMENT_ACTIVITY, structured_source_store


def test_real_source_segments_keep_main_recovery_and_raw_scalar_policy(tmp_path, record_property):
    store = structured_source_store(tmp_path)
    store.reset_counts()
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    assert analysis["analyzer_version"] == "1.9.1"
    assert block["schema_version"] == 2 and block["activity_id"] == "42"
    assert block["source_fit_sha256"] == analysis["source"]["fit_sha256"]
    assert block["source_tcx_sha256"] == analysis["source"]["tcx_sha256"]
    intervals = block["executed_intervals"]["items"]
    assert [row["source_laps"] for row in intervals] == [[2, 3], [5]]
    assert [row["average_power"]["value"] for row in intervals] == [150, 400]
    main = next(row for row in analysis["sections"] if row["section"] == "main")
    assert main["elapsed_seconds"] == 40 and main["distance_m"] == 2000
    whole = block["whole_distance_halves"]["items"]
    halves = block["main_distance_halves"]["items"]
    assert [row["average_power"]["value"] for row in whole] == [400, 433.3333]
    expected = {
        "average_power": [150, 200],
        "average_step_length": [1050, 1300],
        "average_ground_contact_time": [210, 250],
        "average_vertical_oscillation": [70, 110],
        "average_vertical_ratio": [7, 11],
        "average_ground_contact_balance": [102, 106],
    }
    for name, values in expected.items():
        assert [row[name]["value"] for row in halves] == values
    cover = halves[1]["average_step_length"]["coverage"]
    assert cover["valid_seconds"] == 10 and cover["eligible_seconds"] == 20
    assert cover["status"] == "partial"
    for row in [*intervals, *whole, *halves]:
        raw = row["average_ground_contact_balance"]
        assert raw["side_semantics"] == "unknown"
        assert "left_percent" not in raw and "right_percent" not in raw
    assert analysis["recorded_running_metrics"]["distance_halves"]["items"][0][
        "average_ground_contact_balance"]["value"] is None
    record_property("targeted_segment_counts", json.dumps(store.counts))


def test_bulk_recognizes_targeted16_without_historical_regeneration(tmp_path, record_property):
    store = structured_source_store(tmp_path)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    pointer, _ = selected(store)
    canonicals = {key: body for key, body in store.objects.items() if "/canonical/" in key}
    store.reset_counts()
    plan = coach_backfill.run(store=store)
    assert plan["generated_this_run"] == plan["remaining_activities"] == 0
    assert plan["blocked_activities"] == []
    assert selected(store)[0]["analysis_key"] == pointer["analysis_key"]
    assert {key: body for key, body in store.objects.items() if "/canonical/" in key} == canonicals
    record_property("cold_bulk_after_targeted_counts", json.dumps(store.counts))


def test_overlapping_source_elapsed_windows_do_not_hide_registered_laps(tmp_path):
    store = structured_source_store(tmp_path, elapsed_shift=0.125)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    assert analysis["recorded_running_metrics"]["laps"]["status"] == "available"
    assert analysis["recorded_running_metrics"]["laps"]["items"][1]["average_power"]["value"] == 777
    assert analysis["workout_structure"]["executed_laps"][1]["elapsed_seconds"] == 10.125
    for name in ("executed_intervals", "main_distance_halves"):
        group = analysis["segment_running_metrics"][name]
        assert group["status"] == "unavailable" and group["reason"] == "clock_basis_unverified"
        assert group["items"] == []


def test_tcx_whole_alignment_gate_is_separate_from_valid_fit_segments(tmp_path):
    store = structured_source_store(tmp_path, tcx_distance_scale=1.1)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    assert block["whole_distance_halves"]["status"] == "unavailable"
    assert block["whole_distance_halves"]["reason"] == "distance_alignment_unverified"
    assert block["executed_intervals"]["status"] == "available"
    assert [row["average_power"]["value"] for row in block["main_distance_halves"]["items"]] == [150, 200]


def test_raw_balance_half_does_not_claim_coverage_before_first_record(tmp_path):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["messages"]["record_mesgs"] = decoded["messages"]["record_mesgs"][1:]
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    before = analysis["recorded_running_metrics"]["distance_halves"]["items"][0]
    assert before["average_power"]["coverage"]["status"] == "partial"
    raw = analysis["segment_running_metrics"]["whole_distance_halves"]["items"][0][
        "average_ground_contact_balance"]
    assert raw["value"] == 102
    assert raw["coverage"]["status"] == "partial"
    assert raw["coverage"]["valid_seconds"] == raw["coverage"]["eligible_seconds"] == 20


def test_official_start_plus_elapsed_window_is_not_summary_timestamp(tmp_path):
    store = structured_source_store(tmp_path, summary_timestamp_shift=-1)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    assert block["session_end_time_utc"] == "2026-10-07T23:31:00.000Z"
    assert block["session_summary_timestamp_utc"] == "2026-10-07T23:30:59.000Z"
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        assert block[name]["status"] == "available"
        assert block[name]["clock_basis"] == "fit_start_time_plus_total_elapsed_time"
    assert [row["average_power"]["value"] for row in block["main_distance_halves"]["items"]] == [150, 200]
    assert analysis["recorded_running_metrics"]["measured_at_utc"] == "2026-10-07T23:30:59.000Z"
    assert analysis["recorded_running_metrics"]["distance_halves"]["status"] == "unavailable"


def test_one_original_record_pair_counts_once_across_split_active_laps(tmp_path):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    del decoded["messages"]["record_mesgs"][2]
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    metric = analysis["segment_running_metrics"]["executed_intervals"]["items"][0]["average_power"]
    assert metric["value"] == 100
    assert metric["coverage"]["valid_seconds"] == metric["coverage"]["eligible_seconds"] == 20
    assert metric["coverage"]["contributing_intervals"] == 1


def test_genuine16_is_verified_by_existing_encrypted_backup_path(tmp_path):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    objects, document = archive_document(store)
    verified = private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD)
    assert verified.objects == objects


def test_encrypted16_backup_rejects_duplicate_source_lap_identity(tmp_path):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.backup_source import BackupError
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    pointer, analysis = selected(store)
    analysis["workout_structure"]["executed_laps"].append(
        dict(analysis["workout_structure"]["executed_laps"][0]))
    store.objects[pointer["analysis_key"]] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    _, document = archive_document(store)
    with pytest.raises(BackupError, match="invalid_segment_running_metrics"):
        private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD)


@pytest.mark.parametrize("offset,available", [(0.0004, True), (0.0006, False)])
def test_whole_halves_keep_existing_millimetre_partition_tolerance(tmp_path, offset, available):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.endurance.v1.json"
    endurance = json.loads(gzip.decompress(store.objects[key]))
    for half in ("first", "second"):
        endurance["distance_halves"][half]["distance_m"] = 1500 + offset
    store.objects[key] = gzip.compress(json.dumps(endurance).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    old = analysis["recorded_running_metrics"]["distance_halves"]
    new = analysis["segment_running_metrics"]["whole_distance_halves"]
    assert (new["status"] == "available") is available
    if available:
        assert old["reason"] is new["reason"] is None
        assert [row["distance_m"] for row in new["items"]] == [1500, 1500]
    else:
        assert old["reason"] == new["reason"] == "invalid_distance_partition"
        assert new["items"] == []
    assert analysis["segment_running_metrics"]["main_distance_halves"]["status"] == "available"


@pytest.mark.parametrize("mode,reason", [
    ("local_distance", "main_distance_alignment_unverified"),
    ("boolean_step", "step_binding_unverified"),
    ("string_step", "step_binding_unverified"),
    ("duplicate_step", "step_binding_unverified"),
    ("missing_step", "step_binding_unverified"),
    ("missing_elapsed", "clock_basis_unverified"),
])
def test_unverified_lap_binding_never_borrows_filtered_or_total_geometry(tmp_path, mode, reason):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    laps, steps = decoded["messages"]["lap_mesgs"], decoded["messages"]["workout_step_mesgs"]
    if mode == "local_distance":
        laps[1]["total_distance"], laps[2]["total_distance"] = 100, 900
    elif mode == "boolean_step":
        laps[1]["wkt_step_index"] = True
    elif mode == "string_step":
        laps[1]["wkt_step_index"] = "1"
    elif mode == "duplicate_step":
        steps.append(dict(steps[1]))
    elif mode == "missing_step":
        del laps[1]["wkt_step_index"]
    else:
        del laps[1]["total_elapsed_time"]
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    assert block["whole_distance_halves"]["status"] == "available"
    for name in ("executed_intervals", "main_distance_halves"):
        assert block[name]["status"] == "unavailable" and block[name]["reason"] == reason
        assert block[name]["items"] == []


@pytest.mark.parametrize("raw,status", [(0, "available"), (101, "available"), (None, "missing"),
                                       (True, "invalid"), (float("nan"), "invalid"),
                                       (float("inf"), "invalid")])
def test_interval_raw_scalar_preserves_zero_101_and_missing_invalid_distinction(tmp_path, raw, status):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["messages"]["record_mesgs"][4]["stance_time_balance"] = raw
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    metric = analysis["segment_running_metrics"]["executed_intervals"]["items"][1][
        "average_ground_contact_balance"]
    assert metric["status"] == status
    assert metric["value"] == (raw if status == "available" else None)
    assert metric["side_semantics"] == "unknown"
    assert metric["coverage"]["eligible_seconds"] == 10
    assert metric["coverage"]["valid_seconds"] == (10 if status == "available" else 0)


def test_original_over_30_second_gap_is_excluded_before_half_clipping(tmp_path):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    del decoded["messages"]["record_mesgs"][1:4]
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    metric = block["whole_distance_halves"]["items"][0]["average_ground_contact_balance"]
    assert metric["value"] is None and metric["status"] == "missing"
    assert metric["coverage"] == {"valid_seconds": 0, "eligible_seconds": 0, "excluded_gap_seconds": 30,
                                  "boundary_excluded_seconds": 0,
                                  "contributing_intervals": 0, "status": "none"}
    assert block["main_distance_halves"]["reason"] == "main_distance_alignment_unverified"


@pytest.mark.parametrize("path,value", [
    (("schema_version",), True), (("activity_id",), "999"),
    (("source_fit_sha256",), "f" * 64), (("source_tcx_sha256",), "f" * 64),
    (("session_end_time_utc",), "2026-10-07T23:29:00.000Z"),
    (("executed_intervals", "clock_basis"), "summary_timestamp_is_end"),
    (("executed_intervals", "items"), []),
    (("executed_intervals", "items", 0, "source_laps"), [2, 4]),
    (("executed_intervals", "items", 0, "workout_step_index"), 2),
    (("executed_intervals", "items", 0, "average_power", "private_extra"), "synthetic"),
    (("executed_intervals", "items", 0, "average_power", "value"), True),
    (("executed_intervals", "items", 0, "average_power", "coverage", "valid_seconds"), 21),
    (("executed_intervals", "items", 0, "average_power", "coverage", "contributing_intervals"), True),
    (("executed_intervals", "items", 0, "average_ground_contact_balance", "side_semantics"), "left"),
    (("whole_distance_halves", "items", 0, "distance_m"), 1400),
    (("main_distance_halves", "balance_aggregation"), "left_complement"),
])
def test_encrypted16_backup_rejects_unbound_segment_contract(tmp_path, path, value):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.backup_source import BackupError
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    pointer, analysis = selected(store)
    parent = analysis["segment_running_metrics"]
    for part in path[:-1]:
        parent = parent[part]
    parent[path[-1]] = value
    store.objects[pointer["analysis_key"]] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    _, document = archive_document(store)
    with pytest.raises(BackupError, match="invalid_segment_running_metrics"):
        private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD)


@pytest.mark.parametrize("options", [{"summary_timestamp_shift": -1}, {"tcx_distance_scale": 1.1}])
def test_encrypted16_backup_preserves_independent_clock_and_unavailable_whole(tmp_path, options):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path, **options)
    declare(store)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    objects, document = archive_document(store)
    assert private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD).objects == objects


def test_genuine15_history_stays_readable_and_is_not_bulk_queued_by_engine_only(tmp_path, record_property):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from scripts.build_gct_sides_contract import ACTIVITY

    fixture = json.loads((Path(__file__).parent / "fixtures/coach_engine_150_segment_transition.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in fixture["left"]["objects_base64"].items()}
    old_pointer, old = selected(store)
    before = dict(store.objects)
    assert old["analyzer_version"] == "1.5.0" and "segment_running_metrics" not in old
    objects, document = archive_document(store)
    assert private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD).objects == objects
    store.reset_counts()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert selected(store)[1] == old
    record_property("genuine15_engine_only_bulk_counts", json.dumps(store.counts))
    store.reset_counts()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    new_pointer, new = selected(store)
    assert new["analyzer_version"] == "1.9.1"
    assert new_pointer["analysis_key"] != old_pointer["analysis_key"]
    original_fields = {key: new[key] for key in old if key not in ("analysis_id", "analyzer_version")}
    current_gct = original_fields["ground_contact_sides"]
    assert current_gct["schema_version"] == 2
    assert set(current_gct) == set(old["ground_contact_sides"]) | {"record_aggregates"}
    assert set(current_gct["record_aggregates"]) == {
        "executed_intervals", "whole_distance_halves", "main_distance_halves",
    }
    original_fields["ground_contact_sides"] = {
        **{key: value for key, value in current_gct.items() if key not in ("schema_version", "record_aggregates")},
        "schema_version": 1,
    }
    assert original_fields == {key: value for key, value in old.items()
                               if key not in ("analysis_id", "analyzer_version")}
    assert all(store.objects[key] == body for key, body in before.items() if "/canonical/" in key
               or key.startswith("coach/profiles/") or "/context/" in key)
    record_property("genuine15_targeted_upgrade_counts", json.dumps(store.counts))


def test_same_real_segment_sources_publish_same_bulk_and_targeted_identity(tmp_path, record_property):
    targeted = structured_source_store(tmp_path)
    bulk = CountingStore()
    bulk.objects = dict(targeted.objects)
    targeted.reset_counts()
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=targeted)
    first_pointer, first = selected(targeted)
    record_property("new16_targeted_counts", json.dumps(targeted.counts))
    bulk.reset_counts()
    coach_backfill.run(store=bulk)
    pointer, analysis = selected(bulk)
    assert pointer["analysis_key"] == first_pointer["analysis_key"]
    assert analysis == first and bulk.objects[pointer["analysis_key"]] == targeted.objects[pointer["analysis_key"]]
    record_property("new16_bulk_counts", json.dumps(bulk.counts))
    targeted.reset_counts()
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=targeted)
    assert selected(targeted)[0]["analysis_key"] == first_pointer["analysis_key"]
    assert targeted.counts["put"] == 0
    record_property("new16_targeted_repeat_counts", json.dumps(targeted.counts))


def test_real_multigroup_publication_retry_keeps_committed_canonical_bytes(tmp_path, monkeypatch, record_property):
    store = structured_source_store(tmp_path)
    put = store.put

    def interrupted(key, *args, **kwargs):
        if key.endswith("/latest-ready.json"):
            raise RuntimeError("synthetic interruption before pointer")
        return put(key, *args, **kwargs)

    monkeypatch.setattr(store, "put", interrupted)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    canonical = {key: body for key, body in store.objects.items() if "/canonical/" in key}
    assert len(canonical) == 1
    monkeypatch.setattr(store, "put", put)
    store.reset_counts()
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    assert all(store.objects[key] == body for key, body in canonical.items())
    assert selected(store)[0]["analysis_key"] in canonical
    assert store.counts["put"] == 1
    record_property("real16_interrupted_pointer_retry_counts", json.dumps(store.counts))


@pytest.mark.parametrize("mode", ["complete", "source_error", "missing_elapsed"])
def test_continuous_main_basis_remains_consistent_when_source_is_unavailable(tmp_path, mode):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path)
    declare(store)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["messages"]["workout_step_mesgs"] = []
    decoded["messages"]["lap_mesgs"] = []
    if mode == "source_error":
        decoded["decode_errors"] = ["synthetic_decode_error"]
    elif mode == "missing_elapsed":
        del decoded["messages"]["session_mesgs"][0]["total_elapsed_time"]
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["segment_running_metrics"]
    assert analysis["workout_structure"]["has_structured_workout"] is False
    assert block["main_distance_halves"]["distance_basis"] == "tcx_endurance_distance_continuous_main"
    assert block["executed_intervals"]["status"] == "unavailable"
    if mode == "complete":
        assert block["main_distance_halves"]["items"] == block["whole_distance_halves"]["items"]
        assert block["main_distance_halves"]["status"] == "available"
    else:
        assert block["main_distance_halves"]["items"] == []
    objects, document = archive_document(store)
    assert private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD).objects == objects


@pytest.mark.parametrize("gzip_os", [None, 3], ids=["native-gzip", "linux312-gzip-os"])
def test_same_genuine_source_workload_keeps_storage_operation_budget(tmp_path, record_property, gzip_os):
    from scripts.build_gct_sides_contract import ACTIVITY, declare, source_store

    fixture = json.loads((Path(__file__).parent / "fixtures/coach_engine_150_segment_transition.json").read_text())
    before = {key: base64.b64decode(body) for key, body in fixture["left"]["objects_base64"].items()}
    store = source_store(tmp_path)
    declare(store)
    if gzip_os is not None:
        # Reproduce Python3.12/zlib's OS header without changing source content.
        for key, body in store.objects.items():
            if body.startswith(b"\x1f\x8b"):
                changed = body[:9] + bytes([gzip_os]) + body[10:]
                assert gzip.decompress(changed) == gzip.decompress(body)
                store.objects[key] = changed
    for key, body in store.objects.items():
        if body.startswith(b"\x1f\x8b"):
            assert gzip.decompress(before[key]) == gzip.decompress(body)
        else:
            assert before[key] == body
        # Replay the genuine source bytes after verifying the current source.
        # Gzip OS headers differ across runtimes; the frozen read-byte baseline
        # and all historical canonical bytes must stay unchanged.
        store.objects[key] = before[key]
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    assert store.counts == fixture["left"]["counts"]
    pointer, _ = selected(store)
    old_canonical = next(body for key, body in before.items() if "/canonical/" in key)
    record_property("same_source_before15_counts", json.dumps(fixture["left"]["counts"]))
    record_property("same_source_after16_counts", json.dumps(store.counts))
    record_property("same_source_canonical_gzip_bytes", json.dumps({
        "before15": len(old_canonical), "after16": len(store.objects[pointer["analysis_key"]]),
    }))


def test_stationary_original_interval_is_not_counted_or_filled_from_lap_mean(tmp_path):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    for row in decoded["messages"]["record_mesgs"][2:]:
        row["distance"] -= 500
    decoded["messages"]["lap_mesgs"][1]["total_distance"] = 0
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    interval = analysis["segment_running_metrics"]["executed_intervals"]["items"][0]
    assert interval["source_laps"] == [2, 3] and interval["distance_m"] == 500
    assert interval["average_power"]["value"] == 200
    assert interval["average_power"]["coverage"] == {
        "valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 0, "boundary_excluded_seconds": 0,
        "contributing_intervals": 1, "status": "complete",
    }


@pytest.mark.parametrize("second,expected", [(1e308, 1e308), (-1e308, 0)])
def test_new_segment_mean_keeps_finite_extreme_scalar_values(tmp_path, second, expected):
    store = structured_source_store(tmp_path)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    records = decoded["messages"]["record_mesgs"]
    records[1]["stance_time_balance"], records[2]["stance_time_balance"] = 1e308, second
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    _, analysis = selected(store)
    metric = analysis["segment_running_metrics"]["executed_intervals"]["items"][0][
        "average_ground_contact_balance"]
    assert metric["value"] == expected and metric["coverage"]["status"] == "complete"
    json.dumps(analysis, allow_nan=False)


@pytest.mark.parametrize("group,seconds", [("whole_distance_halves", 31), ("main_distance_halves", 21)])
def test_encrypted16_backup_rejects_half_coverage_exceeding_its_total_frame(tmp_path, group, seconds):
    from test_gct_sides import archive_document
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup
    from pipeline.backup_source import BackupError
    from scripts.build_gct_sides_contract import declare

    store = structured_source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    pointer, analysis = selected(store)
    for row in analysis["segment_running_metrics"][group]["items"]:
        for name in ("average_power", "average_step_length", "average_ground_contact_time",
                     "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance"):
            row[name]["coverage"].update(valid_seconds=seconds, eligible_seconds=seconds, status="complete")
    store.objects[pointer["analysis_key"]] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    _, document = archive_document(store)
    with pytest.raises(BackupError, match="invalid_segment_running_metrics"):
        private_backup.verify_backup(_write_document(tmp_path, document), PASSWORD)
