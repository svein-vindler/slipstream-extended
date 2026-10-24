"""Source-order boundaries through normal publication from stored synthetic inputs."""
import gzip
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_unbound_lap_breaks_same_step_groups_without_dropping_native_measurements(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "logical_bouts_source_order.json")
                         .read_text(encoding="utf-8"))
    # Inputs only: no planned step99 exists. The later two laps are verified step3.
    # All four raw rows and their native metrics remain present, in source order.
    raw = fixture["objects"]["activities/2026/42/activity.v1.json"]["messages"]["lap_mesgs"]
    raw[1]["wkt_step_index"] = 99
    raw[2]["wkt_step_index"] = 3
    store = CountingStore()
    store.objects = {
        key: (value if isinstance(value, str) else json.dumps(value, sort_keys=True)).encode("utf-8")
        for key, value in fixture["objects"].items()
    }
    original = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=dict(fixture["activity"]), store=store)
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    preserved = all(store.objects.get(key) == body for key, body in original.items())
    (tmp_path / "unbound-analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (tmp_path / "unbound-canonical.actual.json.gz").write_bytes(canonical)
    (tmp_path / "unbound-resources.actual.json").write_text(json.dumps({
        "result": result, "counts": store.counts, "source_bytes_preserved": preserved,
        "analyzer_version": analysis["analyzer_version"], "provider_calls": 0,
    }, sort_keys=True), encoding="utf-8")

    assert preserved and result["blocked_activities"] == []
    assert result["processed_sources"]["42"] == analysis["analysis_id"]
    assert pointer["analyzer_version"] == analysis["analyzer_version"]
    assert analysis["analyzer_version"] == "1.9.1"
    # This source-order fixture already uses four LISTs (actual214c); it is
    # distinct from the earlier schema1 stored fixture's two LISTs.
    assert store.counts["get"] <= 9 and store.counts["list"] <= 4
    assert store.counts["put"] == 2
    binding = analysis["native_lap_workout_binding"]
    assert binding["status"] == "partial" and binding["reason"] == "some_laps_unbound"
    assert [(row["lap"], row["source_message_index"], row["status"], row["reason"])
            for row in binding["items"]] == [
        (1, 10, "bound", None), (2, 14, "unbound", "workout_step_unverified"),
        (3, 21, "bound", None), (4, 30, "bound", None),
    ]
    assert [row["lap"] for row in analysis["workout_structure"]["executed_laps"]] == [1, 2, 3, 4]
    native = analysis["recorded_running_metrics"]["laps"]["items"]
    assert [row["lap"] for row in native] == [1, 2, 3, 4]
    for name, values in {
        "average_power": [111, 222, 333, 444], "average_step_length": [1000, 1100, 1200, 1300],
        "average_ground_contact_time": [200, 210, 220, 230], "average_vertical_oscillation": [10, 11, 12, 13],
        "average_vertical_ratio": [5, 6, 7, 8], "average_ground_contact_balance": [101, 103, 105, 107],
    }.items():
        assert [row[name]["value"] for row in native] == values
        assert [row[name]["status"] for row in native] == ["available"] * 4

    block = analysis["logical_bouts"]
    assert block["schema_version"] == 2 and block["grouping_basis"] == "verified_native_source_order"
    assert block["status"] == "partial" and block["reason"] == "some_laps_unassigned"
    assert block["source_coverage"] == {
        "status": "complete", "reason": None, "raw_lap_count": 4,
        "native_lap_count": 4, "binding_count": 4, "execution_count": 4,
    }
    assert block["unassigned_laps"] == [{"lap": 2, "reason": "native_binding_unverified"}]
    assert [(row["role"], row["source_intensity"], row["source_laps"], row["source_message_indices"],
             row["workout_step_index"], row["observed_step_occurrence"], row["planned_repeat_iteration"])
            for row in block["items"]] == [
        ("work", "active", [1], [10], 3, 1, None),
        ("work", "active", [3, 4], [21, 30], 3, 2, None),
    ]
    for row in block["items"]:
        assert row["timing"] == {
            "status": "unavailable", "basis": "fit_start_time_plus_total_elapsed_time",
            "reason": "clock_basis_unverified", "start_time_utc": None, "end_time_utc": None,
        }
        assert row["record_aggregate"] == {
            "status": "unavailable", "interval": None, "reason": "clock_basis_unverified",
        }
        assert "duration_seconds" not in row and "distance_m" not in row
        assert row.get("start_time_utc") is None and row.get("end_time_utc") is None
