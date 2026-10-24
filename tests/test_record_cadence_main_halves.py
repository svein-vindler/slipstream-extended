"""Main-half cadence from an explicit variant of genuine synthetic stored sources."""
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


def test_stored_records_supply_main_half_cadence_without_borrowing_whole_or_native(tmp_path):
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
    evidence = tmp_path / "main-cadence-tracer"
    evidence.mkdir()
    (evidence / "decoded-input.actual.json").write_bytes(json_bytes(decoded))
    (evidence / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "preserved-blocks.actual.json").write_bytes(json_bytes({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")
    }))
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

    # First new-contract assertion follows actual source/main-window/whole controls.
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
