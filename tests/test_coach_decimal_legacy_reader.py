"""Exact synthetic old-producer bytes through authenticated reader validation only."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from test_coach_context_backup import document
from test_coach_section_backup import fixture_objects
from test_private_backup import PASSWORD, _write_document

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError

FIXTURE = Path(__file__).parent / "fixtures/coach_engine_190_decimal_hr_rounding.json"
FIXTURE_SHA256 = "bb809f95245e6ea03774c0b71048afaed8a015f69f3eeaa4e50f54f4c0dae14b"


def objects_and_analysis():
    body = FIXTURE.read_bytes()
    assert hashlib.sha256(body).hexdigest() == FIXTURE_SHA256
    fixture = json.loads(body)
    assert fixture["provenance"]["network_attempts"] == 0
    assert fixture["provenance"]["source_sha256"]["pipeline/coach.py"] == (
        "083e4788fada0c9b576e4accc529c9f4977eaf8305921981547b0ade99f75aef")
    objects = fixture_objects(fixture)
    key = next(key for key in objects if "/canonical/" in key)
    return objects, key, json.loads(gzip.decompress(objects[key]))


def test_genuine_decimal_190_reader_retains_its_partial_label_id_and_bytes(tmp_path):
    objects, key, analysis = objects_and_analysis()
    assert analysis["analyzer_version"] == "1.9.0"
    assert analysis["analysis_id"] == "96bb429f0335621d02449bd1"
    assert analysis["sections"][0]["heart_rate_coverage"] == {
        "basis": "lap_elapsed", "covered_seconds": 3.3, "contributing_laps": 2, "status": "partial",
    }
    # This independently tests the authenticated reader. Archive publication is
    # separately blocked; _write_document never substitutes for create_backup.
    verified = backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
    assert verified.objects == objects
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == FIXTURE_SHA256


@pytest.mark.parametrize("damage", ["version", "pulse", "count", "mean", "distance", "duplicate", "integer_time", "extra"])
def test_equal_published_times_do_not_authorize_an_unproven_legacy_variant(tmp_path, damage):
    objects, key, analysis = objects_and_analysis()
    section = analysis["sections"][0]
    if damage == "version":  # Deliberate forgery; the genuine fixture remains unchanged.
        analysis["analyzer_version"] = "1.9.1"
    elif damage == "pulse":
        analysis["workout_structure"]["executed_laps"][1]["average_heart_rate_bpm"] = None
    elif damage == "count":
        section["heart_rate_coverage"]["contributing_laps"] = 1
    elif damage == "mean":
        section["average_heart_rate_bpm"] = 160
    elif damage == "distance":
        section["distance_m"] = 299
    elif damage == "duplicate":
        analysis["sections"].append(dict(section))
    elif damage == "integer_time":
        analysis["workout_structure"]["executed_laps"][0]["elapsed_seconds"] = 1
        analysis["workout_structure"]["executed_laps"][1]["elapsed_seconds"] = 2.3
    else:
        section["heart_rate_coverage"]["extra"] = "synthetic-extra"
    objects[key] = gzip.compress(backup._json_bytes(analysis), mtime=0)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
