"""Real authenticated backup verification of immutable stored Coach context."""

import base64
import copy
import gzip
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_coach_context_wiring import ACTIVITY, CONTEXT_KEYS, source_store
from test_coach_stored_context_snapshot import health_snapshot
from test_private_backup import PASSWORD, _write_document

from pipeline import coach_backfill
from pipeline import private_backup as backup
from pipeline.backup_source import BackupError
from pipeline.coach import build_coach_input, validate_profile
from pipeline.health_sync import normalize_day


def context_objects(*, stored_snapshot=None):
    client = source_store()
    if stored_snapshot is not None:
        for kind in ("sleep", "hrv"):
            body = gzip.compress(backup._json_bytes(stored_snapshot[kind]["data"]), mtime=0)
            client.objects[CONTEXT_KEYS[kind]] = body
            receipt = json.loads(client.objects[CONTEXT_KEYS[f"{kind}_check"]])
            receipt["source_sha256"] = hashlib.sha256(body).hexdigest()
            client.objects[CONTEXT_KEYS[f"{kind}_check"]] = json.dumps(receipt).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    objects = {}
    for key, body in client.objects.items():
        if "/canonical/" in key:
            objects[key] = body
        elif key.startswith("coach/profiles/"):
            profile = validate_profile({**json.loads(body), "created_at": "2026-01-01T00:00:00Z"})
            objects[f"coach/profiles/v1/{profile['effective_from']}/{profile['profile_id']}.json"] = backup._json_bytes(profile)
    return objects


def document(objects):
    """Authenticated archive fixture, not the atomic archive publisher."""
    entries = []
    analysis_key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[analysis_key]))
    profile_key = next(key for key in objects if key.startswith("coach/profiles/"))
    year, activity_id = analysis_key.split("/")[1:3]
    for key, body in sorted(objects.items()):
        decoded = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
        digest = hashlib.sha256(body).hexdigest()
        entries.append({"key": key, "type": "profile" if key == profile_key else "analysis",
                        "revision": digest, "size": len(body), "decoded_size": len(decoded),
                        "sha256": digest, "data": base64.b64encode(body).decode("ascii")})
    return {
        "format": "slipstream-user-backup", "version": 1,
        "created_at": "2026-10-19T10:00:00.000Z",
        "scope": [{"year": year, "activity_id": activity_id}], "complete": True,
        "objects": entries, "stored_bytes": sum(entry["size"] for entry in entries),
        "decoded_bytes": sum(entry["decoded_size"] for entry in entries),
        "references": [{
            "analysis": analysis_key, "profile": profile_key, "context": None,
            "external_sources": [f"activities/{year}/{activity_id}/" + name for name in
                                 ("activity.v1.json", "activity.endurance.v1.json", "activity.tcx")],
            "source_hashes": {name: analysis["source"][name] for name in ("fit_sha256", "tcx_sha256")},
            "ready": False, "reason": "external_sources_not_backed_up",
        }],
    }


def test_verify_backup_preserves_real_wired_current_and_v2_running_fields_exactly(tmp_path):
    objects = context_objects()
    analysis_key = next(key for key in objects if "/canonical/" in key)
    original = json.loads(gzip.decompress(objects[analysis_key]))
    assert original["analyzer_version"] == "1.8.0"
    assert original["recorded_running_metrics"]["schema_version"] == 2
    assert original["stored_context_snapshot"]["training_day"]["recovery"]["value"] == 0
    verified = backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
    assert verified.objects == objects
    restored = json.loads(gzip.decompress(verified.objects[analysis_key]))
    assert restored == original
    assert verified.summary()["ready_analyses"] == 0


def mutate_analysis(objects, changes):
    result = copy.deepcopy(objects)
    key = next(key for key in result if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(result[key]))
    for path, replacement in changes:
        target = analysis["stored_context_snapshot"]
        for name in path[:-1]:
            target = target[name]
        target[path[-1]] = replacement
    result[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    return result


@pytest.mark.parametrize("changes", [
    [(('private_feedback',), 'synthetic private extra')],
    [(('schema_version',), True)],
    [(('semantics',), 'known_before_activity')],
    [(('reference_time_utc',), '2026-10-19T06:00:00.000Z')],
    [(('snapshot_at',), '2026-10-19T08:00:00+01:00')],
    [(('snapshot_at',), '2026-10-19T07:59:59Z')],
    [(('sleep', 'metrics', 'sleep_score', 'value'), 101)],
    [(('sleep', 'metrics', 'average_spo2_percent'), {'status': 'available', 'value': 200, 'unit': '%'})],
    [(('sleep', 'metrics', 'sleep_seconds', 'unit'), 'minutes')],
    [(('sleep', 'metrics', 'sleep_seconds', 'value'), True)],
    [(('sleep', 'metrics', 'sleep_seconds', 'value'), 10**400)],
    [(('sleep', 'raw_readings'), [1, 2, 3])],
    [(('sleep', 'night_of'), '2026-10-18')],
    [(('sleep', 'source_revision'), 'unverified-source')],
    [(('sleep', 'observation_end_utc'), '2026-10-19T08:00:00.000Z'),
     (('sleep', 'sleep_end_local'), '2026-10-19T10:00:00+02:00')],
    [(('hrv', 'metrics', 'weekly_avg_ms', 'value'), -1)],
    [(('hrv', 'metrics', 'last_night_avg_ms', 'status'), 'estimated')],
    [(('training_day', 'last_attempt', 'feedback'), 'synthetic private extra')],
    [(('training_day', 'readiness', 'value'), 101)],
    [(('training_day', 'recovery', 'value'), -1)],
    [(('training_day', 'recovery', 'value'), 2**31)],
    [(('training_day', 'recovery', 'value'), 0.0)],
    [(('training_day', 'readiness', 'observation_at'), '2026-10-19T08:00:00Z')],
    [(('training_day', 'readiness', 'unit'), '%')],
    [(('training_day', 'readiness', 'retained_previous'), 'yes')],
    [(('training_day', 'date'), '2026-10-18')],
    [(('training_day', 'timezone'), '../Europe/Oslo')],
    [(('training_day', 'temporal_status'), 'selection_unavailable')],
    [(('training_day', 'time_provenance', 'readiness', 'observation_local'), '2026-10-20T08:00:00')],
    [(('training_day', 'time_provenance', 'readiness', 'observation_local'), '2026-10-19T08:00:00Z')],
], ids=[
    'private-extra', 'bool-schema', 'false-preknowledge', 'wrong-session-clock', 'non-utc-snapshot',
    'snapshot-older-than-check', 'score-over-100', 'percent-over-100', 'wrong-health-unit',
    'bool-health-value', 'unbounded-health-int', 'raw-readings', 'wrong-local-night',
    'unverified-source', 'future-night-attributed-as-pre-activity', 'negative-hrv', 'unknown-value-status',
    'private-attempt-extra', 'readiness-over-100', 'negative-recovery', 'recovery-over-existing-domain',
    'noninteger-recovery', 'future-readiness-attributed-as-pre-activity', 'wrong-training-unit',
    'nonboolean-retention', 'wrong-training-day', 'invalid-timezone-identifier', 'inconsistent-selection',
    'wrong-local-observation-day', 'nonlocal-observation-clock',
])
def test_verify_backup_rejects_context_contract_corruption(tmp_path, changes):
    objects = mutate_analysis(context_objects(), changes)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


@pytest.mark.parametrize("snapshot_at", [None, '2026-10-19T10:00:00Z'])
def test_explicit_unknown_or_later_snapshot_does_not_claim_pre_activity_knowledge(tmp_path, snapshot_at):
    objects = mutate_analysis(context_objects(), [(('snapshot_at',), snapshot_at)])
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_known_missing_context_retains_v2_running_fields_without_fabricated_clock(tmp_path):
    client = source_store(context=False)
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    objects = context_objects()
    old_key = next(key for key in objects if '/canonical/' in key)
    del objects[old_key]
    objects.update({key: body for key, body in client.objects.items() if '/canonical/' in key})
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


@pytest.mark.parametrize('engine,fixture,prior_rejection', [
    ('1.0.0', 'coach_legacy_transition.json', 'invalid_analysis_source_hash'),
    ('1.0.1', 'coach_engine_101_transition.json', 'invalid_analysis_source_hash'),
    ('1.1.0', 'coach_engine_110_transition.json', None),
])
def test_genuine_legacy_fixture_acceptance_is_not_relaxed(tmp_path, engine, fixture, prior_rejection):
    saved = json.loads((Path(__file__).parent / 'fixtures' / fixture).read_text())
    for key, encoded in saved['objects_base64'].items():
        if '/canonical/' not in key:
            continue
        body = base64.b64decode(encoded)
        analysis = json.loads(gzip.decompress(body))
        if analysis['analyzer_version'] == engine and analysis['user_context'] is None:
            break
    else:
        pytest.fail('Expected genuine legacy fixture analysis')
    profile = validate_profile({**analysis['profile'], 'created_at': '2026-01-01T00:00:00Z'})
    objects = {key: body,
               f"coach/profiles/v1/{profile['effective_from']}/{profile['profile_id']}.json": backup._json_bytes(profile)}
    archive = _write_document(tmp_path, document(objects))
    if prior_rejection is not None:
        # These transition inputs were never valid backup inputs: old synthetic
        # non-hashes are not grounds to weaken import. Versioned HR coverage is
        # now accepted, but the original malformed synthetic hash remains invalid.
        with pytest.raises(BackupError, match=prior_rejection):
            backup.verify_backup(archive, PASSWORD)
    else:
        assert backup.verify_backup(archive, PASSWORD).objects == objects


@pytest.mark.parametrize('engine', ['1.0.0', '1.0.1'])
def test_allowed_legacy_backup_schema_without_new_extensions_is_still_accepted(tmp_path, engine):
    # A versioned format fixture, not a claim that today's builder ran old code.
    objects = context_objects()
    key = next(key for key in objects if '/canonical/' in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    analysis['analyzer_version'] = engine
    del analysis['stored_context_snapshot']
    del analysis['recorded_running_metrics']
    del analysis['training_effect']
    del analysis['ground_contact_sides']
    del analysis['segment_running_metrics']
    del analysis['native_lap_workout_binding']
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_current_public_builder_remains_backup_compatible_without_context_injection(tmp_path):
    client = source_store()
    decoded = client.read('activities/2026/42/activity.v1.json')
    endurance = client.read('activities/2026/42/activity.endurance.v1.json')
    profile = validate_profile({**json.loads(next(body for key, body in client.objects.items()
                                                if key.startswith('coach/profiles/'))),
                                'created_at': '2026-01-01T00:00:00Z'})
    analysis = build_coach_input(activity=ACTIVITY, decoded_fit=decoded, endurance=endurance,
                                profile=profile, tcx_sha256=hashlib.sha256(
                                    client.objects['activities/2026/42/activity.tcx']).hexdigest())
    assert analysis['analyzer_version'] == '1.8.0'
    assert 'stored_context_snapshot' not in analysis
    objects = {
        f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json": gzip.compress(backup._json_bytes(analysis), mtime=0),
        f"coach/profiles/v1/{profile['effective_from']}/{profile['profile_id']}.json": backup._json_bytes(profile),
    }
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_backup_verification_does_not_require_an_installed_timezone_database(tmp_path, monkeypatch):
    import zoneinfo
    from zoneinfo import _common

    objects = context_objects()
    archive = _write_document(tmp_path, document(objects))
    original_path = zoneinfo.TZPATH

    def missing_database(_key):
        raise zoneinfo.ZoneInfoNotFoundError("synthetic unavailable timezone database")

    monkeypatch.setattr(_common, "load_tzdata", missing_database)
    try:
        zoneinfo.reset_tzpath(())
        zoneinfo.ZoneInfo.clear_cache()
        with pytest.raises(zoneinfo.ZoneInfoNotFoundError):
            zoneinfo.ZoneInfo("Europe/Oslo")  # Prove the outer environment condition.
        verified = backup.verify_backup(archive, PASSWORD)
        assert verified.objects == objects
    finally:
        zoneinfo.reset_tzpath(original_path)
        zoneinfo.ZoneInfo.clear_cache()


def test_verify_backup_accepts_real_garmin_local_clock_with_retained_rest_seconds(tmp_path):
    snapshot = health_snapshot(end_local="2026-10-19T07:00:01")
    objects = context_objects(stored_snapshot=snapshot)
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    for kind in ("sleep", "hrv"):
        assert analysis["stored_context_snapshot"][kind]["status"] == "available"
        assert analysis["stored_context_snapshot"][kind]["sleep_end_local"] == "2026-10-19T07:00:01+02:00"
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_verify_backup_preserves_known_garmin_clock_when_local_pair_is_incomplete(tmp_path):
    snapshot = health_snapshot(end_local=None)
    clocks = {
        "sleepStartTimestampGMT": "2026-10-18T22:00:20Z",
        "sleepEndTimestampGMT": "2026-10-19T05:00:00Z",
        "sleepStartTimestampLocal": "2026-10-19T00:00:00",
        "sleepEndTimestampLocal": None,
    }
    snapshot["sleep"]["data"] = normalize_day("sleep", "2026-10-19", {
        "dailySleepDTO": {
            **clocks, "calendarDate": "2026-10-19", "sleepTimeSeconds": 25180,
            "sleepWindowConfirmed": True, "sleepScore": 82,
        },
        "sleepLevels": [{"startGMT": clocks["sleepStartTimestampGMT"],
                         "endGMT": clocks["sleepEndTimestampGMT"], "activityLevel": 1}],
    })
    snapshot["hrv"]["data"] = normalize_day("hrv", "2026-10-19", {
        **clocks,
        "hrvSummary": {"calendarDate": "2026-10-19", "lastNightAvg": 50, "weeklyAvg": 48},
        "hrvReadings": [{"readingTimeGMT": "2026-10-19T03:00:00Z", "hrvValue": 50}],
    })
    objects = context_objects(stored_snapshot=snapshot)
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    assert analysis["analyzer_version"] == "1.8.0"
    assert analysis["summary"]["distance_m"] == 5000
    for kind in ("sleep", "hrv"):
        context = analysis["stored_context_snapshot"][kind]
        assert context["status"] == "local_time_unknown"
        assert context["local_time_source"] == "unavailable"
        assert context["observation_start_utc"] == "2026-10-18T22:00:20.000Z"
        assert context["sleep_start_local"] == "2026-10-19T00:00:00+02:00"
        assert context["sleep_end_local"] is None
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_backup_only_dependency_boundary_can_import_and_verify_real_archive(tmp_path):
    objects = context_objects()
    archive = _write_document(tmp_path, document(objects))
    script = """
import importlib.abc
import sys
from pathlib import Path

class BackupOnlyDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'requests', 'urllib3', 'garminconnect', 'garmin_fit_sdk', 'boto3', 'botocore',
        }:
            raise ModuleNotFoundError('undeclared backup-only dependency: ' + fullname)

sys.meta_path.insert(0, BackupOnlyDependencies())
sys.path.insert(0, sys.argv[1])
from pipeline.private_backup import verify_backup
verified = verify_backup(Path(sys.argv[2]), b'synthetic-test-password-only')
assert len(verified.objects) == 2
assert verified.summary()['ready_analyses'] == 0
assert 'pipeline.training_day' not in sys.modules
print('backup-only authenticated verification: ok')
"""
    environment = {name: value for name, value in os.environ.items()
                   if name.upper() in {"SYSTEMROOT", "TEMP", "TMP"}}
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", script, str(Path(__file__).resolve().parents[1]), str(archive)],
        capture_output=True, text=True, timeout=30, env=environment, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "backup-only authenticated verification: ok"


@pytest.mark.parametrize("changes", [
    [(('sleep', 'local_time_source'), 'configured_timezone')],
    [(('sleep', 'sleep_end_local'), '2026-10-19T07:00:01+02:00:01')],
], ids=['configured-zone-retains-exact-clock', 'garmin-offset-must-be-rounded-minutes'])
def test_rest_second_support_does_not_weaken_other_clock_attribution(tmp_path, changes):
    objects = context_objects(stored_snapshot=health_snapshot(end_local="2026-10-19T07:00:01"))
    damaged = mutate_analysis(objects, changes)
    with pytest.raises(BackupError, match='invalid_stored_context_clock'):
        backup.verify_backup(_write_document(tmp_path, document(damaged)), PASSWORD)
