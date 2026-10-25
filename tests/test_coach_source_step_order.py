"""Source-ordered step groups are distinct from elapsed timing and aggregates."""
import base64
import gzip
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_source_order_groups_keep_complete_native_refs_without_claiming_elapsed_timing(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "logical_bouts_source_order.json")
                         .read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {
        key: (value if isinstance(value, str) else json.dumps(value, sort_keys=True)).encode("utf-8")
        for key, value in fixture["objects"].items()
    }
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=dict(fixture["activity"]), store=store)
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    preserved = all(store.objects.get(key) == body for key, body in before.items())
    (tmp_path / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (tmp_path / "canonical.actual.json.gz").write_bytes(canonical)
    (tmp_path / "objects.actual.json").write_text(json.dumps({
        key: base64.b64encode(body).decode("ascii") for key, body in store.objects.items()
    }, sort_keys=True), encoding="utf-8")
    (tmp_path / "resources.actual.json").write_text(json.dumps({
        "result": result, "counts": store.counts, "source_bytes_preserved": preserved,
        "analyzer_version": analysis["analyzer_version"],
        "logical_schema_version": analysis["logical_bouts"]["schema_version"],
    }, sort_keys=True), encoding="utf-8")

    # These input/native guards must pass before the new source-order contract.
    assert preserved and result["blocked_activities"] == []
    assert not any("/coach-input/" in key for key in before)
    raw = fixture["objects"]["activities/2026/42/activity.v1.json"]["messages"]
    assert len(raw["lap_mesgs"]) == 4
    assert raw["lap_mesgs"][0]["timestamp"] == "2026-01-01T00:00:10Z"
    assert raw["lap_mesgs"][1]["start_time"] == "2026-01-01T00:00:12Z"
    binding = analysis["native_lap_workout_binding"]
    assert binding["status"] == "available" and binding["reason"] is None
    assert [(row["lap"], row["source_message_index"], row["workout_step_index"], row["status"])
            for row in binding["items"]] == [
        (1, 10, 3, "bound"), (2, 14, 3, "bound"), (3, 21, 8, "bound"), (4, 30, 3, "bound"),
    ]
    assert [row["lap"] for row in analysis["workout_structure"]["executed_laps"]] == [1, 2, 3, 4]
    native = analysis["recorded_running_metrics"]["laps"]["items"]
    assert [row["lap"] for row in native] == [1, 2, 3, 4]
    expected_values = {
        "average_power": [111, 222, 333, 444], "average_step_length": [1000, 1100, 1200, 1300],
        "average_ground_contact_time": [200, 210, 220, 230], "average_vertical_oscillation": [10, 11, 12, 13],
        "average_vertical_ratio": [5, 6, 7, 8], "average_ground_contact_balance": [101, 103, 105, 107],
    }
    units = {"average_power": "W", "average_step_length": "mm", "average_ground_contact_time": "ms",
             "average_vertical_oscillation": "mm", "average_vertical_ratio": "%", "average_ground_contact_balance": "%"}
    for name, values in expected_values.items():
        assert [row[name]["value"] for row in native] == values
        assert [row[name]["status"] for row in native] == ["available"] * 4
        assert [row[name]["unit"] for row in native] == [units[name]] * 4
    assert [row["average_ground_contact_balance"]["side_semantics"] for row in native] == ["unknown"] * 4
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        recorded = analysis["segment_running_metrics"][name]
        assert recorded["status"] == "unavailable" and recorded["reason"] == "clock_basis_unverified"
        assert recorded["items"] == []

    block = analysis["logical_bouts"]
    assert block["grouping_basis"] == "verified_native_source_order"
    assert block["schema_version"] == 2 and analysis["analyzer_version"] == "1.10.0"
    assert block["status"] == "available" and block["reason"] is None
    assert block["unassigned_laps"] == []
    assert block["occurrence_basis"] == "derived_verified_source_order_per_step"
    assert [(row["role"], row["source_intensity"], row["source_laps"], row["source_message_indices"], row["workout_step_index"],
             row["observed_step_occurrence"], row["planned_repeat_iteration"]) for row in block["items"]] == [
        ("work", "active", [1, 2], [10, 14], 3, 1, None),
        ("recovery", "recovery", [3], [21], 8, 1, None),
        ("work", "active", [4], [30], 3, 2, None),
    ]
    for row in block["items"]:
        timing = row["timing"]
        assert timing["status"] == "unavailable" and timing["reason"] == "clock_basis_unverified"
        assert timing["basis"] == "fit_start_time_plus_total_elapsed_time"
        assert timing.get("start_time_utc") is None and timing.get("end_time_utc") is None
        assert row.get("start_time_utc") is None and row.get("end_time_utc") is None
        assert "duration_seconds" not in row and "distance_m" not in row
        aggregate = row["record_aggregate"]
        assert aggregate["status"] == "unavailable" and aggregate["reason"] == "clock_basis_unverified"
        assert aggregate["interval"] is None and "value" not in aggregate
