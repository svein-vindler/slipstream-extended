"""Retain actual schema1 history through ordinary bulk under the current builder."""
import base64
import gzip
import hashlib
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_bulk_retains_genuine_schema1_current_without_schema_only_regeneration(tmp_path):
    # Exact earlier producer output and source proof, never relabelled or reseeded.
    fixture_path = Path(__file__).parent / "fixtures" / "coach_engine_190_logical_schema1.json"
    fixture_bytes = fixture_path.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == "9022abff3d5fae700ca46402625a95a71a44a5faf254ed327d9d367b3d4607b6"
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in json.loads(fixture_bytes).items()}
    original = dict(store.objects)
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    checkpoint_key = "backfill/coach-input/v1/plan.json"
    old_pointer = json.loads(original[pointer_key])
    old_key = old_pointer["analysis_key"]
    old_canonical = original[old_key]
    old_analysis = json.loads(gzip.decompress(old_canonical))
    assert old_analysis["analysis_id"] == "9487effdc2d0e426b5321387"
    assert hashlib.sha256(old_canonical).hexdigest() == "d8cdcfc3ec996573c40248292f18fc329a3194ec6911a10b43c283adceed40be"
    assert old_analysis["analyzer_version"] == old_pointer["analyzer_version"] == "1.9.0"
    assert old_pointer["selection_version"] == 1
    assert old_pointer["input_signature"] == "90b353502648d83aa48e89cc3e0a3a1dcae3ad0ecd136eb78852039235970099"
    old_logical = old_analysis["logical_bouts"]
    assert old_logical["schema_version"] == 1
    assert old_logical["method"] == "verified_native_lap_contiguity"
    assert old_logical["occurrence_basis"] == "derived_verified_bout_order_per_step"
    assert old_logical["items"][1]["source_laps"] == [2, 3]
    assert old_logical["items"][1]["start_time_utc"] == "2026-10-07T23:30:10.000Z"
    assert old_logical["items"][1]["end_time_utc"] == "2026-10-07T23:30:30.000Z"
    assert checkpoint_key not in original
    old_canonicals = {key: body for key, body in original.items() if "/canonical/" in key}

    store.reset_counts()
    plan = coach_backfill.run(store=store)
    preserved = all(store.objects.get(key) == body for key, body in original.items())
    current_pointer = json.loads(store.objects[pointer_key])
    new_keys = sorted(set(store.objects) - set(original))
    (tmp_path / "schema1-bulk-retention.actual.json").write_text(json.dumps({
        "plan": plan, "counts": store.counts, "all_original_bytes_preserved": preserved,
        "new_keys": new_keys, "old_analysis_id": old_analysis["analysis_id"],
        "old_canonical_sha256": hashlib.sha256(old_canonical).hexdigest(),
        "current_analysis_key": current_pointer["analysis_key"], "provider_calls": 0,
    }, sort_keys=True), encoding="utf-8")

    assert plan["status"] == "complete" and plan["blocked_activities"] == []
    assert plan["generated_this_run"] == 0 and plan["completed_this_run"] == []
    assert plan["remaining_activities"] == 0 and plan["already_current_activities"] == 1
    assert plan["deferred_legacy_activity_count"] == 0
    assert preserved and store.objects[pointer_key] == original[pointer_key]
    assert {key: body for key, body in store.objects.items() if "/canonical/" in key} == old_canonicals
    assert current_pointer["analysis_key"] == old_key
    # An ordinary first bulk inspection may persist proof in its one checkpoint;
    # it must not spend writes on a replacement canonical or current selector.
    assert new_keys == [checkpoint_key] and store.counts["put"] == 1
    assert json.loads(store.objects[checkpoint_key]) == plan
    proof = plan["bulk_reuse_proofs"]["42"]
    assert proof["analysis_key"] == old_key and proof["analyzer_version"] == "1.9.0"
    assert proof["input_signature"] == old_pointer["input_signature"]
    assert proof.get("logical_bouts_schema_version", 1) == 1
