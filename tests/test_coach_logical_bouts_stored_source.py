"""Already stored real synthetic export through the ordinary Coach producer.

The frozen source objects are the unchanged output of the real FIT/TCX export
tracer. This independent stored-source seam does not replace that export test.
Only the external object store is fake; no producer/decoder success is stubbed.
"""
import base64
import gzip
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_stored_fit_sources_produce_logical_bouts_without_changing_sources(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(value) for key, value in encoded.items()}
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    counts = dict(store.counts)
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    (tmp_path / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (tmp_path / "canonical.actual.json.gz").write_bytes(canonical)
    (tmp_path / "objects.actual.json").write_text(json.dumps({
        key: base64.b64encode(body).decode("ascii") for key, body in store.objects.items()
    }, sort_keys=True), encoding="utf-8")
    (tmp_path / "resources.actual.json").write_text(json.dumps({
        "counts": counts, "result": result, "analyzer_version": analysis["analyzer_version"],
        "source_bytes_preserved": all(store.objects[key] == body for key, body in before.items()),
    }, sort_keys=True), encoding="utf-8")
    assert all(store.objects[key] == body for key, body in before.items())
    assert result["blocked_activities"] == []
    block = analysis["logical_bouts"]
    assert analysis["analyzer_version"] == "1.10.0"
    assert block["status"] == "available" and block["reason"] is None
    assert block["occurrence_basis"] == "derived_verified_source_order_per_step"
    assert [(item["role"], item["source_laps"], item["source_message_indices"],
             item["workout_step_index"], item["observed_step_occurrence"], item["planned_repeat_iteration"])
            for item in block["items"]] == [
        ("warmup", [1], [0], 0, 1, None),
        ("work", [2, 3], [1, 2], 1, 1, None),
        ("recovery", [4], [3], 2, 1, None),
        ("work", [5], [4], 1, 2, None),
        ("cooldown", [6], [5], 3, 1, None),
    ]
    assert [item["record_aggregate"]["interval"] for item in block["items"]] == [None, 1, None, 2, None]
    assert [row["average_power"]["value"] for row in analysis["recorded_running_metrics"]["laps"]["items"]] == [777] * 6
    assert [row["average_power"]["value"] for row in analysis["segment_running_metrics"]["executed_intervals"]["items"]] == [150, 400]
