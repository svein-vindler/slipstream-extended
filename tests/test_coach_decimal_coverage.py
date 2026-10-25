"""Real synthetic FIT -> Coach -> archive creation/reader, without live services."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_section_backup import fixture_objects
from test_private_backup import PASSWORD, MemorySource

from pipeline import private_backup as backup
from pipeline.backup_source import Scope
from scripts.build_coach_training_effect_contract import build_case


@pytest.mark.parametrize(("heart_rates", "coverage", "seconds", "count", "average"), [
    ((100, 200), "complete", 30.3, 2, 166.67),
    ((100, None), "partial", 10.1, 1, 100),
    ((None, None), "none", 0, 0, None),
    ((0, 0), "complete", 30.3, 2, 0),
])
def test_decimal_fit_coverage_and_created_backup_preserve_exact_bytes(
        tmp_path, heart_rates, coverage, seconds, count, average):
    fixture = build_case(laps=[(10.1, 100, heart_rates[0]), (20.2, 200, heart_rates[1])])
    objects = fixture_objects(fixture)
    key = next(key for key in objects if "/canonical/" in key)
    analysis = json.loads(gzip.decompress(objects[key]))
    assert analysis["analyzer_version"] == "1.10.0"
    section = analysis["sections"][0]
    assert section["elapsed_seconds"] == 30.3
    assert section["average_heart_rate_bpm"] == average
    assert section["heart_rate_coverage"] == {
        "basis": "lap_elapsed", "covered_seconds": seconds, "contributing_laps": count, "status": coverage,
    }
    destination = tmp_path / "decimal-created.slbk"
    backup.create_backup(MemorySource(objects), Scope((("2026", "42"),)), destination, PASSWORD)
    assert backup.verify_backup(destination, PASSWORD).objects == objects


def test_genuine_190_history_keeps_its_original_version_id_and_canonical_bytes(tmp_path):
    fixture_path = Path(__file__).parent / "fixtures/coach_engine_190_logical_schema1.json"
    encoded = json.loads(fixture_path.read_text(encoding="utf-8"))
    stored = {key: base64.b64decode(body) for key, body in encoded.items()}
    pointer = json.loads(stored["activities/2026/42/coach-input/v1/latest-ready.json"])
    key = pointer["analysis_key"]
    canonical = stored[key]
    analysis = json.loads(gzip.decompress(canonical))
    assert analysis["analyzer_version"] == "1.9.0"
    assert analysis["analysis_id"] == "9487effdc2d0e426b5321387"
    fixture = {"objects_base64": {name: base64.b64encode(body).decode("ascii")
                                  for name, body in stored.items()}}
    objects = fixture_objects(fixture)
    destination = tmp_path / "genuine-history-created.slbk"
    backup.create_backup(MemorySource(objects), Scope((("2026", "42"),)), destination, PASSWORD)
    verified = backup.verify_backup(destination, PASSWORD)
    assert verified.objects == objects and verified.objects[key] == canonical
