"""Explicit current generation from actual schema1 history, then ordinary replay."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_targeted_upgrades_genuine_schema1_preserves_history_and_replays_without_writes(tmp_path):
    fixture_path = Path(__file__).parent / "fixtures" / "coach_engine_190_logical_schema1.json"
    fixture_bytes = fixture_path.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == "9022abff3d5fae700ca46402625a95a71a44a5faf254ed327d9d367b3d4607b6"
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in json.loads(fixture_bytes).items()}
    original = dict(store.objects)
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    old_pointer = json.loads(original[pointer_key])
    old_key = old_pointer["analysis_key"]
    old_canonical = original[old_key]
    old_analysis = json.loads(gzip.decompress(old_canonical))
    assert old_analysis["analysis_id"] == "9487effdc2d0e426b5321387"
    assert hashlib.sha256(old_canonical).hexdigest() == "d8cdcfc3ec996573c40248292f18fc329a3194ec6911a10b43c283adceed40be"
    assert old_analysis["analyzer_version"] == "1.9.0"
    assert old_analysis["logical_bouts"]["schema_version"] == 1
    assert old_analysis["logical_bouts"]["method"] == "verified_native_lap_contiguity"
    activity = {"id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0}

    # Normal explicit targeted generation, with no force flag or altered input.
    store.reset_counts()
    generated = coach_backfill.run_one(activity=dict(activity), store=store)
    first_counts = dict(store.counts)
    current_pointer = json.loads(store.objects[pointer_key])
    current_key = current_pointer["analysis_key"]
    current_canonical = store.objects[current_key]
    current_analysis = json.loads(gzip.decompress(current_canonical))
    preserved = all(store.objects.get(key) == body for key, body in original.items() if key != pointer_key)
    (tmp_path / "targeted-current.actual.json.gz").write_bytes(current_canonical)
    (tmp_path / "targeted-upgrade.actual.json").write_text(json.dumps({
        "result": generated, "counts": first_counts, "old_analysis_id": old_analysis["analysis_id"],
        "current_analysis_id": current_analysis["analysis_id"], "current_key": current_key,
        "original_nonselector_bytes_preserved": preserved, "provider_calls": 0,
        "old_canonical_sha256": hashlib.sha256(store.objects[old_key]).hexdigest(),
        "current_canonical_sha256": hashlib.sha256(current_canonical).hexdigest(),
    }, sort_keys=True), encoding="utf-8")

    assert generated["blocked_activities"] == [] and generated["reused_sources"] == {}
    assert current_analysis["analyzer_version"] == current_pointer["analyzer_version"] == "1.10.0"
    assert current_analysis["logical_bouts"]["schema_version"] == 2
    assert current_analysis["logical_bouts"]["method"] == "verified_native_source_order"
    assert current_key != old_key and current_analysis["analysis_id"] != old_analysis["analysis_id"]
    assert generated["processed_sources"] == {"42": current_analysis["analysis_id"]}
    assert current_pointer["selection_version"] == 1
    assert current_pointer["input_signature"] != old_pointer["input_signature"]
    assert preserved and store.objects[old_key] == old_canonical
    assert {key for key in store.objects if "/canonical/" in key} == {old_key, current_key}
    assert set(store.objects) - set(original) == {current_key}
    assert first_counts["put"] == 2

    after_upgrade = dict(store.objects)
    store.reset_counts()
    repeated = coach_backfill.run_one(activity=dict(activity), store=store)
    (tmp_path / "targeted-replay.actual.json").write_text(json.dumps({
        "result": repeated, "counts": store.counts, "all_object_bytes_preserved": store.objects == after_upgrade,
        "current_key": json.loads(store.objects[pointer_key])["analysis_key"], "provider_calls": 0,
    }, sort_keys=True), encoding="utf-8")
    assert repeated["blocked_activities"] == []
    assert repeated["processed_sources"] == repeated["reused_sources"] == {"42": current_analysis["analysis_id"]}
    assert store.counts["put"] == 0
    assert store.objects == after_upgrade
