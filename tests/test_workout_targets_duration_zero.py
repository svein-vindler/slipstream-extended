"""Typed observed zero differs from absent typed time with the same raw parent."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_stored_targets_preserve_typed_zero_and_do_not_infer_missing_time_from_parent_zero(tmp_path):
    fixture_bytes = (Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json").read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == "5efb4b85fd19f4d80293176b4c0e53ef0ec300c45c8bb25606865afc493083d3"
    original_sources = {key: base64.b64decode(body) for key, body in json.loads(fixture_bytes).items()}
    decoded_key = "activities/2026/42/activity.v1.json"
    unchanged_metrics = None
    for label, typed_time, expected_status, expected_value, expected_reason in (
        ("typed-zero", True, "available", 0, None),
        ("missing-typed-time", False, "unavailable", None, "active_duration_missing_or_invalid"),
    ):
        # Declared decoded-input variants, not a claim of new SDK/watch delivery.
        # Only the active step's duration fields differ from the original export.
        decoded = json.loads(gzip.decompress(original_sources[decoded_key]))
        step = decoded["messages"]["workout_step_mesgs"][1]
        assert step["message_index"] == 1 and step["intensity"] == "active"
        step.update(duration_type="time", duration_value=0)
        step.pop("duration_time", None)
        if typed_time:
            step["duration_time"] = 0
        store = CountingStore()
        store.objects = dict(original_sources)
        store.objects[decoded_key] = gzip.compress(json.dumps(decoded, sort_keys=True).encode(), mtime=0)
        before = dict(store.objects)
        store.reset_counts()
        result = coach_backfill.run_one(activity={
            "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
        }, store=store)
        pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
        canonical = store.objects[pointer["analysis_key"]]
        analysis = json.loads(gzip.decompress(canonical))
        preserved = all(store.objects.get(key) == body for key, body in before.items())
        (tmp_path / f"{label}.decoded.actual.json").write_text(json.dumps(decoded, sort_keys=True), encoding="utf-8")
        (tmp_path / f"{label}.analysis.actual.json").write_bytes(gzip.decompress(canonical))
        (tmp_path / f"{label}.resources.actual.json").write_text(json.dumps({
            "result": result, "counts": store.counts, "source_bytes_preserved": preserved,
            "variant": label, "provider_calls": 0,
        }, sort_keys=True), encoding="utf-8")

        assert preserved and result["blocked_activities"] == []
        assert all(store.objects[key] == body for key, body in original_sources.items() if key != decoded_key)
        assert store.counts["put"] == 2
        assert decoded["source_fit_sha256"] == "3144e4ad6c8c6c291e91d47dd521867805e05c1bc9ceb0a1e5b91b827e10a91f"
        planned = analysis["workout_structure"]["planned_steps"]
        assert [row["step_index"] for row in planned] == [0, 1, 2, 3]
        assert planned[1]["duration_type"] == "time" and planned[1]["duration_value"] == 0
        assert step["duration_value"] == 0 and ("duration_time" in step) is typed_time
        native = analysis["recorded_running_metrics"]["laps"]["items"]
        assert [row["lap"] for row in native] == [1, 2, 3, 4, 5, 6]
        for name, value, status, unit in (
            ("average_power", 777, "available", "W"),
            ("average_step_length", None, "missing", "mm"),
            ("average_ground_contact_time", None, "missing", "ms"),
            ("average_vertical_oscillation", None, "missing", "mm"),
            ("average_vertical_ratio", None, "missing", "%"),
            ("average_ground_contact_balance", None, "missing", "%"),
        ):
            assert [row[name]["value"] for row in native] == [value] * 6
            assert [row[name]["status"] for row in native] == [status] * 6
            assert [row[name]["unit"] for row in native] == [unit] * 6
        metrics = {name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")}
        if unchanged_metrics is None:
            unchanged_metrics = metrics
        else:
            assert metrics == unchanged_metrics
        assert [row["average_power"]["value"] for row in
                analysis["segment_running_metrics"]["executed_intervals"]["items"]] == [150, 400]

        targets = analysis["workout_targets"]
        assert targets["schema_version"] == 1 and targets["status"] == "available" and targets["reason"] is None
        assert targets["method"] == "fit_active_typed_workout_fields" and targets["activity_id"] == "42"
        assert targets["source_fit_sha256"] == decoded["source_fit_sha256"]
        assert [row["step_index"] for row in targets["items"]] == [0, 1, 2, 3]
        assert targets["items"][1]["duration"] == {
            "status": expected_status, "type": "time", "value": expected_value, "unit": "s",
            "source_field": "duration_time", "reason": expected_reason,
        }
