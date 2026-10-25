"""Recovery cadence from the agreed synthetic stored-source to published Coach seam."""
import base64
import copy
import gzip
import hashlib
import json
from datetime import datetime
from pathlib import Path

from pipeline import coach_backfill
from pipeline.granular import gzip_json, json_bytes
from scripts.benchmark_coach_reuse import CountingStore


def test_stored_records_supply_recovery_cadence_bound_to_verified_logical_bout(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json"
    fixture_bytes = fixture.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == (
        "5efb4b85fd19f4d80293176b4c0e53ef0ec300c45c8bb25606865afc493083d3"
    )
    encoded = json.loads(fixture_bytes)
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    original_objects = dict(store.objects)
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    original_decoded = copy.deepcopy(decoded)
    records = decoded["messages"]["record_mesgs"]
    assert len(records) == 7 and decoded["decode_errors"] == []
    origin = datetime.fromisoformat(records[0]["timestamp"])
    assert [(datetime.fromisoformat(row["timestamp"]) - origin).total_seconds()
            for row in records] == [0, 10, 20, 30, 40, 50, 60]
    assert [row["distance"] for row in records] == [0, 500, 1000, 1500, 2000, 2500, 3000]
    for row, base in zip(records, (80, 81, 82, 83, 84, 85, 86), strict=True):
        assert "cadence" not in row and "fractional_cadence" not in row
        row.update(cadence=base, fractional_cadence=0)
    # This is a declared decoded-input variant, not a new SDK export/FIT claim.
    for name, rows in decoded["messages"].items():
        if name != "record_mesgs":
            assert rows == original_decoded["messages"][name]
    assert {name: value for name, value in decoded.items() if name != "messages"} == {
        name: value for name, value in original_decoded.items() if name != "messages"
    }
    for row, original in zip(records, original_decoded["messages"]["record_mesgs"], strict=True):
        assert {name: value for name, value in row.items()
                if name not in ("cadence", "fractional_cadence")} == original
    store.objects[key] = gzip_json(decoded)
    assert {name: body for name, body in store.objects.items() if name != key} == {
        name: body for name, body in original_objects.items() if name != key
    }
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run",
        "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    counts = dict(store.counts)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    after = {name: store.objects.get(name) for name in before}
    evidence = tmp_path / "recovery-cadence-tracer"
    evidence.mkdir()
    (evidence / "decoded-input.actual.json").write_bytes(json_bytes(decoded))
    (evidence / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    prior_comparison = copy.deepcopy({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")
    })
    # Keep the genuine reference hash; the raw-balance addition is an explicit
    # comparison-only projection, never a relabelled stored/current analysis.
    for group in ("kilometer_splits", "distance_halves"):
        assert prior_comparison["recorded_running_metrics"][group]["balance_aggregation"] == "raw_scalar_time_weighted_mean_side_unknown"
        prior_comparison["recorded_running_metrics"][group]["balance_aggregation"] = "not_computed_without_verified_side_semantics"
        for row in prior_comparison["recorded_running_metrics"][group]["items"]:
            metric = row["average_ground_contact_balance"]
            metric.update(status="unavailable", value=None)
            metric["coverage"].update(valid_seconds=0.0, contributing_intervals=0, status="none")
    old_blocks = json_bytes(prior_comparison)
    cadence_controls = copy.deepcopy(analysis["recorded_cadence"])
    cadence_controls["record_aggregates"] = {
        name: analysis["recorded_cadence"]["record_aggregates"][name]
        for name in ("whole_distance_halves", "main_distance_halves", "executed_intervals")
    }
    cadence_control_bytes = json_bytes(cadence_controls)
    (evidence / "preserved-blocks.comparison.json").write_bytes(old_blocks)
    (evidence / "cadence-controls.actual.json").write_bytes(cadence_control_bytes)
    for label, objects in (("before", before), ("after", after)):
        (evidence / f"sources.{label}.json").write_text(json.dumps({
            name: base64.b64encode(body).decode("ascii") if body is not None else None
            for name, body in objects.items()
        }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "counts": counts, "result": result, "sources_preserved": before == after,
        "fixture_bytes_preserved": fixture.read_bytes() == fixture_bytes,
        "variant_fields": ["record_mesgs.cadence", "record_mesgs.fractional_cadence"],
    }, sort_keys=True), encoding="utf-8")
    assert before == after and fixture.read_bytes() == fixture_bytes
    # Full published blocks are pinned to the accepted work-GREEN result.
    assert len(old_blocks) == 26520
    assert hashlib.sha256(old_blocks).hexdigest() == (
        "0903d502bb3df6193ccf5357b5ec08a6dc33e04602b15eafc40905a6cfbc91e9"
    )
    assert len(cadence_control_bytes) == 8388
    assert hashlib.sha256(cadence_control_bytes).hexdigest() == (
        "fe22ac4689ad7ba018a8935d6aebbf8903b917eec26390216bd0e3f5617c46ff"
    )
    native = analysis["recorded_running_metrics"]
    assert native["schema_version"] == 2 and native["association"] == "matched"
    assert [row["average_power"]["value"] for row in native["laps"]["items"]] == [777] * 6
    old_main = analysis["segment_running_metrics"]["main_distance_halves"]
    assert old_main["status"] == "available" and old_main["reason"] is None
    assert old_main["clock_basis"] == "fit_start_time_plus_total_elapsed_time"
    assert old_main["distance_basis"] == "fit_executed_main_distance"
    assert [row["segment"] for row in old_main["items"]] == ["first", "second"]
    assert [row["average_power"]["value"] for row in old_main["items"]] == [150, 200]
    coverage = {
        "valid_seconds": 20, "eligible_seconds": 20, "excluded_gap_seconds": 0,
        "contributing_intervals": 2, "status": "complete", "boundary_excluded_seconds": 0,
    }
    for row in old_main["items"]:
        assert row["average_power"]["coverage"] == coverage
    cadence = analysis["recorded_cadence"]
    assert cadence["schema_version"] == 2 and cadence["association"] == "matched"
    assert cadence["session"]["average"]["value"] is None
    assert all(row["average"]["value"] is None for row in cadence["laps"]["items"])
    whole = cadence["record_aggregates"]["whole_distance_halves"]
    assert whole["status"] == "available"
    assert [row["average_cadence"]["value"] for row in whole["items"]] == [162, 168]

    # Existing main cadence remains a control for the new verified-work scope.
    main = cadence["record_aggregates"]["main_distance_halves"]
    assert main["status"] == "available" and main["reason"] is None
    assert main["clock_basis"] == old_main["clock_basis"]
    assert main["distance_basis"] == old_main["distance_basis"]
    assert main["source_tcx_sha256"] == analysis["source"]["tcx_sha256"]
    assert [row["segment"] for row in main["items"]] == ["first", "second"]
    for row, expected in zip(main["items"], (163, 167), strict=True):
        metric = row["average_cadence"]
        assert metric["status"] == "available"
        assert type(metric["value"]) in (int, float) and metric["value"] == expected
        assert metric["unit"] == "steps_per_minute"
        assert metric["coverage"] == coverage

    old_work = analysis["segment_running_metrics"]["executed_intervals"]
    assert old_work["status"] == "available" and old_work["reason"] is None
    assert old_work["clock_basis"] == "fit_start_time_plus_total_elapsed_time"
    assert old_work["distance_basis"] == "fit_executed_active_distance"
    assert [(row["interval"], row["source_laps"], row["workout_step_index"])
            for row in old_work["items"]] == [(1, [2, 3], 1), (2, [5], 1)]
    assert [row["average_power"]["value"] for row in old_work["items"]] == [150, 400]
    assert [(
        (datetime.fromisoformat(row["start_time_utc"].replace("Z", "+00:00")) - origin).total_seconds(),
        (datetime.fromisoformat(row["end_time_utc"].replace("Z", "+00:00")) - origin).total_seconds(),
    ) for row in old_work["items"]] == [(10, 30), (40, 50)]
    work_coverages = [
        {"valid_seconds": 20, "eligible_seconds": 20, "excluded_gap_seconds": 0,
         "contributing_intervals": 2, "status": "complete", "boundary_excluded_seconds": 0},
        {"valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 0,
         "contributing_intervals": 1, "status": "complete", "boundary_excluded_seconds": 0},
    ]
    for row, expected_coverage in zip(old_work["items"], work_coverages, strict=True):
        assert row["average_power"]["coverage"] == expected_coverage

    # Existing work cadence is also a control before the new recovery assertion.
    work = cadence["record_aggregates"]["executed_intervals"]
    assert work["status"] == "available" and work["reason"] is None
    assert work["clock_basis"] == old_work["clock_basis"]
    assert work["distance_basis"] == old_work["distance_basis"]
    assert work["source_tcx_sha256"] == analysis["source"]["tcx_sha256"]
    assert len(work["items"]) == 2
    for row, old, expected, expected_coverage in zip(
        work["items"], old_work["items"], (163, 168), work_coverages, strict=True,
    ):
        for field in ("interval", "source_laps", "workout_step_index", "start_time_utc", "end_time_utc"):
            assert row[field] == old[field]
        metric = row["average_cadence"]
        assert metric["status"] == "available"
        assert type(metric["value"]) in (int, float) and metric["value"] == expected
        assert metric["unit"] == "steps_per_minute"
        assert metric["coverage"] == expected_coverage

    logical = analysis["logical_bouts"]
    assert logical["schema_version"] == 2 and logical["status"] == "available"
    assert logical["method"] == "verified_native_source_order"
    assert logical["source_coverage"] == {
        "status": "complete", "reason": None, "raw_lap_count": 6,
        "native_lap_count": 6, "binding_count": 6, "execution_count": 6,
    }
    assert logical["unassigned_laps"] == []
    recovery_sources = [row for row in logical["items"] if row["role"] == "recovery"]
    assert len(recovery_sources) == 1
    source = recovery_sources[0]
    assert (source["bout"], source["source_laps"], source["source_message_indices"],
            source["workout_step_index"], source["source_intensity"]) == (
        3, [4], [3], 2, "recovery",
    )
    assert source["observed_step_occurrence"] == 1
    assert source["planned_repeat_iteration"] is None
    assert {name: source["timing"][name] for name in ("status", "basis", "reason")} == {
        "status": "available", "basis": "fit_start_time_plus_total_elapsed_time",
        "reason": None,
    }
    # Equivalent ISO UTC precision is not a different verified elapsed window.
    assert [(datetime.fromisoformat(source["timing"][field].replace("Z", "+00:00"))
             - origin).total_seconds() for field in ("start_time_utc", "end_time_utc")] == [30, 40]
    assert native["laps"]["items"][3]["distance_m"] == 500

    # First new-contract lookup follows all source and existing-scope controls.
    recovery = cadence["record_aggregates"]["recovery_bouts"]
    assert recovery["status"] == "available" and recovery["reason"] is None
    for field in (
        "method", "clock_basis", "interval_policy", "distance_allocation",
        "source_tcx_sha256", "source_fields", "input_unit", "unit", "conversion",
        "valid_record_precision", "session_start_time_utc", "session_end_time_utc",
        "session_summary_timestamp_utc", "source_window",
    ):
        assert recovery[field] == work[field]
    assert isinstance(recovery["distance_basis"], str) and recovery["distance_basis"]
    assert recovery["source_tcx_sha256"] == analysis["source"]["tcx_sha256"]
    assert len(recovery["items"]) == 1
    row = recovery["items"][0]
    assert (row["bout"], row["source_laps"], row["workout_step_index"]) == (3, [4], 2)
    assert row["bout"] == source["bout"] and row["source_laps"] == source["source_laps"]
    assert row["workout_step_index"] == source["workout_step_index"]
    for field in ("start_time_utc", "end_time_utc"):
        assert datetime.fromisoformat(row[field].replace("Z", "+00:00")) == datetime.fromisoformat(
            source["timing"][field].replace("Z", "+00:00"))
    assert row["distance_m"] == 500
    metric = row["average_cadence"]
    assert metric["status"] == "available"
    assert type(metric["value"]) in (int, float) and metric["value"] == 166
    assert metric["unit"] == "steps_per_minute"
    assert metric["source_fields"] == ["cadence", "fractional_cadence"]
    assert metric["coverage"] == {
        "valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 0,
        "contributing_intervals": 1, "status": "complete", "boundary_excluded_seconds": 0,
    }
