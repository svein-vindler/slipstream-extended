"""Genuine synthetic 1.8 history through ordinary stored-source Coach APIs."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore

POINTER = "activities/2026/42/coach-input/v1/latest-ready.json"
ACTIVITY = {"id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0}


def _history_store():
    # Exact actual 1.8 export/publication snapshot, never relabelled as 1.9.
    fixture = Path(__file__).parent / "fixtures" / "logical_bouts_180_stored_history.json"
    encoded = json.loads(fixture.read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    store.reset_counts()
    return store


def test_normal_targeted_19_preserves_genuine18_bytes_and_reuses_the_new_version(tmp_path):
    store = _history_store()
    original = dict(store.objects)
    old_pointer = json.loads(original[POINTER])
    old_key = old_pointer["analysis_key"]
    old_canonical = original[old_key]
    old_analysis = json.loads(gzip.decompress(old_canonical))
    assert old_analysis["analyzer_version"] == "1.8.0"
    assert "logical_bouts" not in old_analysis

    # Default invocation is explicit normal generation, with no force flag.
    generated = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    first_counts = dict(store.counts)
    current_pointer = json.loads(store.objects[POINTER])
    current_key = current_pointer["analysis_key"]
    current_analysis = json.loads(gzip.decompress(store.objects[current_key]))
    preserved = all(store.objects.get(key) == body for key, body in original.items() if key != POINTER)
    (tmp_path / "targeted-upgrade.actual.json").write_text(json.dumps({
        "result": generated, "counts": first_counts, "old_key": old_key, "current_key": current_key,
        "current_version": current_analysis["analyzer_version"], "original_nonpointer_bytes_preserved": preserved,
        "old_canonical_sha256": hashlib.sha256(store.objects[old_key]).hexdigest(),
        "current_canonical_sha256": hashlib.sha256(store.objects[current_key]).hexdigest(),
    }, sort_keys=True), encoding="utf-8")
    assert generated["blocked_activities"] == []
    assert current_analysis["analyzer_version"] == "1.10.0"
    assert current_key != old_key and current_analysis["analysis_id"] != old_analysis["analysis_id"]
    assert current_pointer["analyzer_version"] == "1.10.0"
    assert current_analysis["logical_bouts"]["status"] == "available"
    assert generated["processed_sources"]["42"] == current_analysis["analysis_id"]
    assert store.objects[old_key] == old_canonical and preserved
    assert {key for key in store.objects if "/canonical/" in key} == {old_key, current_key}

    after_upgrade = dict(store.objects)
    store.reset_counts()
    repeated = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    (tmp_path / "targeted-repeat.actual.json").write_text(json.dumps({
        "result": repeated, "counts": store.counts, "all_object_bytes_preserved": store.objects == after_upgrade,
    }, sort_keys=True), encoding="utf-8")
    assert repeated["blocked_activities"] == []
    assert repeated["processed_sources"]["42"] == current_analysis["analysis_id"]
    assert repeated["reused_sources"]["42"] == current_analysis["analysis_id"]
    assert store.counts["put"] == 0
    assert store.objects == after_upgrade


def test_bulk_keeps_genuine18_current_without_engine_only_regeneration(tmp_path):
    store = _history_store()
    original = dict(store.objects)
    old_pointer = json.loads(original[POINTER])
    old_analysis = json.loads(gzip.decompress(original[old_pointer["analysis_key"]]))
    assert old_analysis["analyzer_version"] == "1.8.0"
    canonicals = {key: body for key, body in original.items() if "/canonical/" in key}

    plan = coach_backfill.run(store=store)
    preserved = all(store.objects.get(key) == body for key, body in original.items())
    (tmp_path / "bulk-retention.actual.json").write_text(json.dumps({
        "status": plan["status"], "generated_this_run": plan["generated_this_run"],
        "remaining_activities": plan["remaining_activities"], "blocked_activities": plan["blocked_activities"],
        "counts": store.counts, "all_original_bytes_preserved": preserved,
        "pointer_version": json.loads(store.objects[POINTER])["analyzer_version"],
    }, sort_keys=True), encoding="utf-8")
    assert plan["status"] == "complete"
    assert plan["generated_this_run"] == 0 and plan["remaining_activities"] == 0
    assert plan["blocked_activities"] == []
    assert store.objects[POINTER] == original[POINTER]
    assert {key: body for key, body in store.objects.items() if "/canonical/" in key} == canonicals
    assert preserved
