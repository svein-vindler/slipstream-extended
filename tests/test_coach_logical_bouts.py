"""Logical bout references through real synthetic FIT export and Coach publication."""
import base64
import gzip
import hashlib
import json
from time import perf_counter

from pipeline import coach_backfill
from scripts.build_gct_sides_contract import selected
from scripts.build_running_segments_contract import SEGMENT_ACTIVITY, structured_source_store


def test_real_fit_groups_split_work_but_separates_the_same_step_after_recovery(tmp_path, record_property):
    store = structured_source_store(tmp_path)
    source_before = dict(store.objects)
    store.reset_counts()
    started = perf_counter()
    result = coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
    elapsed_ms = (perf_counter() - started) * 1000
    counts = dict(store.counts)
    pointer, analysis = selected(store)
    canonical = store.objects[pointer["analysis_key"]]
    decoded = gzip.decompress(canonical)
    source_after = {name: store.objects.get(name) for name in source_before}

    # Preserve actual output before the new-contract assertion, including on RED.
    # These are the existing synthetic source bytes, never relabelled history.
    evidence = tmp_path / "logical-bouts-tracer"
    evidence.mkdir()
    (evidence / "analysis.actual.json").write_bytes(decoded)
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "pointer.actual.json").write_text(json.dumps(pointer, sort_keys=True), encoding="utf-8")
    for label, objects in (("before", source_before), ("after", source_after)):
        encoded = {name: base64.b64encode(body).decode("ascii") if body is not None else None
                   for name, body in objects.items()}
        (evidence / f"sources.{label}.json").write_text(json.dumps(encoded, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "analyzer_version": analysis.get("analyzer_version"), "analysis_id": analysis.get("analysis_id"),
        "result": result, "counts": counts, "publication_elapsed_ms": elapsed_ms,
        "canonical_gzip_bytes": len(canonical), "analysis_json_bytes": len(decoded),
        "canonical_sha256": hashlib.sha256(canonical).hexdigest(),
        "analysis_json_sha256": hashlib.sha256(decoded).hexdigest(),
        "sources_preserved": source_after == source_before,
        "unmeasured": ["head", "uploaded_bytes", "peak_memory", "queue_time", "account_cost"],
    }, sort_keys=True), encoding="utf-8")
    record_property("logical_bouts_evidence_dir", str(evidence))
    record_property("logical_bouts_storage_counts", json.dumps(counts, sort_keys=True))
    assert source_after == source_before
    assert result["blocked_activities"] == []

    block = analysis["logical_bouts"]
    assert analysis["analyzer_version"] == "1.10.0"
    assert block["schema_version"] == 2
    assert block["method"] == "verified_native_source_order"
    assert block["activity_id"] == "42"
    assert block["source_fit_sha256"] == analysis["source"]["fit_sha256"]
    assert block["status"] == "available" and block["reason"] is None
    assert block["unassigned_laps"] == []
    assert block["occurrence_basis"] == "derived_verified_source_order_per_step"
    items = block["items"]
    assert [(item["bout"], item["role"], item["source_laps"], item["source_message_indices"],
             item["workout_step_index"], item["observed_step_occurrence"], item["planned_repeat_iteration"])
            for item in items] == [
        (1, "warmup", [1], [0], 0, 1, None),
        (2, "work", [2, 3], [1, 2], 1, 1, None),
        (3, "recovery", [4], [3], 2, 1, None),
        (4, "work", [5], [4], 1, 2, None),
        (5, "cooldown", [6], [5], 3, 1, None),
    ]
    assert [(item["record_aggregate"]["status"], item["record_aggregate"]["interval"]) for item in items] == [
        ("unavailable", None), ("available", 1), ("unavailable", None), ("available", 2), ("unavailable", None),
    ]
    for item in (items[0], items[2], items[4]):
        assert isinstance(item["record_aggregate"]["reason"], str) and item["record_aggregate"]["reason"]
    for item in (items[1], items[3]):
        assert item["record_aggregate"]["reason"] is None

    intervals = analysis["segment_running_metrics"]["executed_intervals"]["items"]
    assert [row["source_laps"] for row in intervals] == [[2, 3], [5]]
    assert [row["average_power"]["value"] for row in intervals] == [150, 400]
    assert [row["average_power"]["unit"] for row in intervals] == ["W", "W"]
    assert [(row["average_power"]["coverage"]["valid_seconds"],
             row["average_power"]["coverage"]["eligible_seconds"],
             row["average_power"]["coverage"]["status"]) for row in intervals] == [
        (20, 20, "complete"), (10, 10, "complete"),
    ]
    for item, interval in ((items[1], intervals[0]), (items[3], intervals[1])):
        assert item["timing"]["status"] == "available" and item["timing"]["reason"] is None
        assert item["timing"]["start_time_utc"] == interval["start_time_utc"]
        assert item["timing"]["end_time_utc"] == interval["end_time_utc"]
    native = analysis["recorded_running_metrics"]["laps"]["items"]
    assert [row["lap"] for row in native] == [1, 2, 3, 4, 5, 6]
    assert [row["average_power"]["value"] for row in native] == [777, 777, 777, 777, 777, 777]
    assert [row["average_power"]["status"] for row in native] == ["available"] * 6
