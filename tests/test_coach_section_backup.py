"""Actual structured FIT producer history through strict authenticated backup."""

import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_context_backup import document
from test_private_backup import PASSWORD, _write_document

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError
from pipeline.coach import validate_profile
from scripts.build_coach_training_effect_contract import build_case


def historic_structured_objects():
    # Fresh real baseline1.3 producer output, not current bytes relabelled old.
    fixture = json.loads((Path(__file__).parent / "fixtures/coach_engine_130_structured_transition.json").read_text())
    return fixture_objects(fixture)


def fixture_objects(fixture):
    objects = {}
    for key, value in fixture["objects_base64"].items():
        body = base64.b64decode(value)
        if "/canonical/" in key:
            objects[key] = body
        elif key.startswith("coach/profiles/"):
            profile = validate_profile({**json.loads(body), "created_at": "2026-01-01T00:00:00Z"})
            objects[f"coach/profiles/v1/{profile['effective_from']}/{profile['profile_id']}.json"] = backup._json_bytes(profile)
    return objects


def current_structured_analysis(**kwargs):
    fixture = build_case(**kwargs)
    key = next(key for key in fixture["objects_base64"] if "/canonical/" in key)
    return json.loads(gzip.decompress(base64.b64decode(fixture["objects_base64"][key])))


def test_genuine_structured13_backup_keeps_recorded_hr_coverage_and_original_bytes(tmp_path):
    objects = historic_structured_objects()
    analysis = json.loads(gzip.decompress(next(body for key, body in objects.items() if "/canonical/" in key)))
    assert analysis["analyzer_version"] == "1.3.0"
    assert analysis["workout_structure"]["has_structured_workout"] is True
    assert analysis["sections"][0]["heart_rate_coverage"] == {
        "basis": "lap_elapsed", "covered_seconds": 40, "contributing_laps": 1, "status": "complete",
    }
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


def test_structured14_backup_rejects_missing_mandatory_hr_coverage(tmp_path):
    objects = fixture_objects(build_case())
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    assert analysis["analyzer_version"] == "1.8.0"
    assert analysis["workout_structure"]["has_structured_workout"] is True
    section = analysis["sections"][0]
    assert section["measurement_coverage"]["elapsed_seconds"]["status"] == "complete"
    del section["heart_rate_coverage"]
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    # document recalculates digest/sizes from these exact mutated object bytes;
    # only the missing producer field, not archive authentication, is at fault.
    with pytest.raises(BackupError, match="invalid_format_fields"):
        backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


def test_legacy13_backup_still_accepts_optional_hr_coverage_absence(tmp_path):
    # A legacy format-compatibility case based on genuine1.3 bytes, not a claim
    # that the historical producer omitted its field or any history was rewritten.
    objects = historic_structured_objects()
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    assert analysis["analyzer_version"] == "1.3.0"
    del analysis["sections"][0]["heart_rate_coverage"]
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


@pytest.mark.parametrize("damage", ["section_extra", "coverage_extra", "negative", "huge", "false_complete"])
def test_structured_coverage_acceptance_still_rejects_unknown_or_invalid_values(tmp_path, damage):
    objects = historic_structured_objects()
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    section = analysis["sections"][0]
    if damage == "section_extra":
        section["provider_only"] = "synthetic-extra"
    elif damage == "coverage_extra":
        section["heart_rate_coverage"]["device_id"] = "synthetic-extra"
    elif damage == "false_complete":
        section["heart_rate_coverage"].update(covered_seconds=0, contributing_laps=0)
    else:
        section["heart_rate_coverage"]["covered_seconds"] = -1 if damage == "negative" else 1 << 4096
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


def test_structured_laps_without_recorded_totals_are_unknown_not_measured_zero():
    analysis = current_structured_analysis(laps=[(None, None, 150)])
    section = analysis["sections"][0]
    assert analysis["summary"]["distance_m"] == 2000  # Other real TCX summary remains readable.
    assert section["elapsed_seconds"] is None
    assert section["distance_m"] is None
    assert section["measurement_coverage"] == {
        "elapsed_seconds": {"status": "unknown", "observed_laps": 0, "total_laps": 1},
        "distance_m": {"status": "unknown", "observed_laps": 0, "total_laps": 1},
    }


def test_new_structured14_unknown_totals_and_effect_survive_strict_backup(tmp_path):
    objects = fixture_objects(build_case(laps=[(None, None, 150)]))
    analysis = json.loads(gzip.decompress(next(body for key, body in objects.items() if "/canonical/" in key)))
    assert analysis["analyzer_version"] == "1.8.0"
    assert analysis["training_effect"]["aerobic"]["value"] == 3.4
    assert analysis["sections"][0]["elapsed_seconds"] is None
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


@pytest.mark.parametrize(("laps", "elapsed", "distance", "elapsed_status", "distance_status", "counts"), [
    ([(None, 500, 150), (40, 1000, 150)], None, 1500, "partial", "complete", (1, 2)),
    ([(40, None, 150), (40, 0, 150)], 80, None, "complete", "partial", (2, 1)),
    ([(0, 0, 0)], 0, 0, "complete", "complete", (1, 1)),
])
def test_section_totals_preserve_partial_information_and_real_zero(
        tmp_path, laps, elapsed, distance, elapsed_status, distance_status, counts):
    objects = fixture_objects(build_case(laps=laps))
    analysis = json.loads(gzip.decompress(next(body for key, body in objects.items() if "/canonical/" in key)))
    section = analysis["sections"][0]
    assert section["elapsed_seconds"] == elapsed
    assert section["distance_m"] == distance
    assert section["measurement_coverage"] == {
        "elapsed_seconds": {"status": elapsed_status, "observed_laps": counts[0], "total_laps": len(laps)},
        "distance_m": {"status": distance_status, "observed_laps": counts[1], "total_laps": len(laps)},
    }
    assert backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD).objects == objects


@pytest.mark.parametrize("damage", ["unknown_to_zero", "false_complete", "extra", "invalid_effect", "unbound_effect_clock"])
def test_new14_strict_backup_does_not_weaken_unknown_or_effect_validation(tmp_path, damage):
    objects = fixture_objects(build_case(laps=[(None, None, 150)]))
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    section = analysis["sections"][0]
    if damage == "unknown_to_zero":
        section["elapsed_seconds"] = 0
    elif damage == "false_complete":
        section["measurement_coverage"]["elapsed_seconds"]["observed_laps"] = 1
    elif damage == "extra":
        section["measurement_coverage"]["elapsed_seconds"]["device_id"] = "synthetic-extra"
    elif damage == "invalid_effect":
        analysis["training_effect"]["aerobic"]["value"] = 5.1
    else:
        analysis["training_effect"]["session_start_time_utc"] = "2026-10-07T22:30:00.000Z"
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
