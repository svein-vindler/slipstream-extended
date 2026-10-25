"""Real stored Coach output through the authenticated offline backup reader."""
import base64
import gzip
import hashlib
import json
import re
from pathlib import Path

import pytest

from pipeline import coach_backfill
from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits
from pipeline.coach import validate_profile
from scripts.benchmark_coach_reuse import CountingStore

PASSWORD = b"synthetic-backup-password-only"
POINTER = "activities/2026/42/coach-input/v1/latest-ready.json"


def _archive_input(objects, analysis_key):
    # Same outer authenticated-document recipe as existing context backup tests.
    # It does not call the strict validator or simulate a successful analysis.
    analysis = json.loads(gzip.decompress(objects[analysis_key]))
    profile_body = next(body for key, body in objects.items() if key.startswith("coach/profiles/"))
    profile = validate_profile({**json.loads(profile_body), "created_at": "2026-01-01T00:00:00Z"})
    profile_key = f"coach/profiles/v1/{profile['effective_from']}/{profile['profile_id']}.json"
    archived = {analysis_key: objects[analysis_key], profile_key: json.dumps(profile).encode()}
    context = analysis.get("user_context")
    context_key = None
    if context is not None:
        stamp = re.sub(r"[-:.Z]", "", context["created_at"])
        context_key = f"activities/2026/42/context/v1/{stamp}-{context['context_id']}.json"
        archived[context_key] = json.dumps(context).encode()
    entries = []
    for key, body in sorted(archived.items()):
        decoded = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
        digest = hashlib.sha256(body).hexdigest()
        entries.append({"key": key, "type": "analysis" if key == analysis_key else "context" if key == context_key else "profile",
                        "revision": digest, "size": len(body), "decoded_size": len(decoded),
                        "sha256": digest, "data": base64.b64encode(body).decode("ascii")})
    document = {
        "format": "slipstream-user-backup", "version": 1,
        "created_at": "2026-10-23T09:00:00Z", "scope": [{"year": "2026", "activity_id": "42"}],
        "complete": True, "objects": entries,
        "stored_bytes": sum(row["size"] for row in entries),
        "decoded_bytes": sum(row["decoded_size"] for row in entries),
        "references": [{
            "analysis": analysis_key, "profile": profile_key, "context": context_key,
            "external_sources": ["activities/2026/42/" + name for name in
                                 ("activity.v1.json", "activity.endurance.v1.json", "activity.tcx")],
            "source_hashes": {name: analysis["source"][name] for name in ("fit_sha256", "tcx_sha256")},
            "ready": False, "reason": "external_sources_not_backed_up",
        }],
    }
    return document, archived


def test_authenticated_backup_reads_real19_and_preserves_genuine18_history(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" /
                          "logical_bouts_180_stored_history.json").read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    before = dict(store.objects)
    old_key = json.loads(before[POINTER])["analysis_key"]
    old_analysis = json.loads(gzip.decompress(before[old_key]))
    assert old_analysis["analyzer_version"] == "1.8.0" and "logical_bouts" not in old_analysis
    old_document, old_objects = _archive_input(before, old_key)
    old_path = tmp_path / "genuine18.slbk"
    old_path.write_bytes(backup._seal(old_document, PASSWORD, Limits()))
    assert backup.verify_backup(old_path, PASSWORD).objects == old_objects

    generated = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    current_key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[current_key]))
    assert generated["blocked_activities"] == []
    assert analysis["analyzer_version"] == "1.10.0"
    assert analysis["logical_bouts"]["status"] == "available"
    assert all(store.objects[key] == body for key, body in before.items() if key != POINTER)
    document, archived = _archive_input(store.objects, current_key)
    archive_path = tmp_path / "real19.slbk"
    archive_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    # The seam under test is the public authenticated reader, not _analysis_shape.
    verified = backup.verify_backup(archive_path, PASSWORD)
    assert verified.objects == archived
    assert json.loads(gzip.decompress(verified.objects[current_key])) == analysis
    assert verified.summary()["ready_analyses"] == 0
    assert store.objects[old_key] == before[old_key]


def test_authenticated_backup_rejects_non_scalar_bout_role_as_invalid_input(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" /
                          "logical_bouts_stored_source.json").read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    assert analysis["logical_bouts"]["items"][0]["source_intensity"] == "warmup"
    analysis["logical_bouts"]["items"][0]["source_intensity"] = []
    tampered = dict(store.objects)
    tampered[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(tampered, key)
    archive_path = tmp_path / "invalid-role.slbk"
    archive_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_logical_bouts"):
        backup.verify_backup(archive_path, PASSWORD)


def test_authenticated_backup_does_not_accept_unaccounted_native_laps(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" /
                          "logical_bouts_stored_source.json").read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    assert len(analysis["recorded_running_metrics"]["laps"]["items"]) == 6
    assert analysis["native_lap_workout_binding"]["status"] == "available"
    # An unavailable group is not permission to silently drop every source lap.
    analysis["logical_bouts"].update(items=[], unassigned_laps=[], status="unavailable", reason="no_verified_bouts")
    tampered = dict(store.objects)
    tampered[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(tampered, key)
    archive_path = tmp_path / "unaccounted-laps.slbk"
    archive_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_logical_bouts"):
        backup.verify_backup(archive_path, PASSWORD)


def test_authenticated_backup_rejects_group_end_without_elapsed_source_proof(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" /
                          "logical_bouts_stored_source.json").read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    last = analysis["logical_bouts"]["items"][-1]
    assert last["role"] == "cooldown" and last["source_laps"] == [6]
    assert analysis["logical_bouts"]["schema_version"] == 2
    assert last["timing"]["status"] == "available" and last["timing"]["reason"] is None
    assert last["timing"]["basis"] == "fit_start_time_plus_total_elapsed_time"
    assert last["timing"]["end_time_utc"] == "2026-10-07T23:31:00.000Z"
    assert last["record_aggregate"]["status"] == "unavailable"
    last["timing"]["end_time_utc"] = "2026-10-07T23:32:00.000Z"
    tampered = dict(store.objects)
    tampered[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(tampered, key)
    archive_path = tmp_path / "forged-end.slbk"
    archive_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_logical_bouts"):
        backup.verify_backup(archive_path, PASSWORD)


def test_authenticated_backup_keeps_elapsed_end_distinct_from_summary_timestamp(tmp_path):
    # Synthetic stored-source variant, not a claim of a new physical FIT decode.
    encoded = json.loads((Path(__file__).parent / "fixtures" /
                          "logical_bouts_stored_source.json").read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    source_key = "activities/2026/42/activity.v1.json"
    body = store.objects[source_key]
    decoded = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
    decoded["messages"]["session_mesgs"][0]["timestamp"] = "2026-10-07T23:31:05Z"
    decoded["messages"]["lap_mesgs"][-1]["timestamp"] = "2026-10-07T23:31:05Z"
    store.objects[source_key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    before = dict(store.objects)
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    assert analysis["recorded_running_metrics"]["laps"]["items"][-1]["end_time_utc"] == "2026-10-07T23:31:05.000Z"
    last = analysis["logical_bouts"]["items"][-1]
    assert analysis["logical_bouts"]["schema_version"] == 2
    assert last["timing"]["status"] == "available" and last["timing"]["reason"] is None
    assert last["timing"]["basis"] == "fit_start_time_plus_total_elapsed_time"
    assert last["timing"]["end_time_utc"] == "2026-10-07T23:31:00.000Z"
    document, archived = _archive_input(store.objects, key)
    archive_path = tmp_path / "distinct-elapsed-end.slbk"
    archive_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    assert backup.verify_backup(archive_path, PASSWORD).objects == archived
    assert all(store.objects[source] == original for source, original in before.items())
