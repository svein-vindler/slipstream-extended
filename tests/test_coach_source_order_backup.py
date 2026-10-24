"""Genuine producer schema2 output at the authenticated backup seam."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, POINTER, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits


def test_authenticated_backup_keeps_source_groups_without_fabricated_timing(tmp_path):
    # Frozen actual run_one output, not a rewritten schema1 snapshot.
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_source_order.json")
                         .read_text(encoding="utf-8"))
    objects = {key: base64.b64decode(body) for key, body in fixture.items()}
    before = dict(objects)
    key = json.loads(objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    block = analysis["logical_bouts"]
    assert analysis["analyzer_version"] == "1.9.0" and block["schema_version"] == 2
    assert block["grouping_basis"] == "verified_native_source_order"
    assert [(row["role"], row["source_laps"]) for row in block["items"]] == [
        ("work", [1, 2]), ("recovery", [3]), ("work", [4]),
    ]
    assert all(row["timing"]["status"] == "unavailable" for row in block["items"])
    assert "recorded_cadence" not in analysis  # Genuine pre-cadence generation remains distinct.
    document, archived = _archive_input(objects, key)
    archive = tmp_path / "actual-source-order.slbk"
    archive.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(archive, PASSWORD)
    assert verified.objects == archived
    assert json.loads(gzip.decompress(verified.objects[key])) == analysis
    assert objects == before


def test_authenticated_backup_keeps_actual_decoded_cadence_and_source_scope(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_cadence.json")
                         .read_text(encoding="utf-8"))
    objects = {key: base64.b64decode(body) for key, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    cadence = analysis["recorded_cadence"]
    assert cadence["association"] == "matched"
    assert cadence["source_fit_sha256"] == analysis["source"]["fit_sha256"]
    assert [(scope["average"]["value"], scope["maximum"]["value"])
            for scope in (cadence["session"], cadence["laps"]["items"][0])] == [(161, 164), (161, 164)]
    assert cadence["session"]["average"]["unit"] == "steps_per_minute"
    document, archived = _archive_input(objects, key)
    archive = tmp_path / "actual-decoded-cadence.slbk"
    archive.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(archive, PASSWORD)
    assert verified.objects == archived
    assert json.loads(gzip.decompress(verified.objects[key])) == analysis


@pytest.mark.parametrize(("case", "error"), [("timing", "invalid_logical_bouts"), ("unit", "invalid_recorded_cadence")])
def test_authenticated_backup_rejects_unsupported_new_claims(tmp_path, case, error):
    name = "coach_engine_190_source_order.json" if case == "timing" else "coach_engine_190_cadence.json"
    fixture = json.loads((Path(__file__).parent / "fixtures" / name).read_text(encoding="utf-8"))
    encoded = fixture if case == "timing" else fixture["objects"]
    objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    key = json.loads(objects[POINTER])["analysis_key"] if case == "timing" else fixture["analysis_key"]
    document, archived = _archive_input(objects, key)
    valid = tmp_path / "positive.slbk"
    valid.write_bytes(backup._seal(document, PASSWORD, Limits()))
    assert backup.verify_backup(valid, PASSWORD).objects == archived
    analysis = json.loads(gzip.decompress(objects[key]))
    if case == "timing":
        group = analysis["logical_bouts"]["items"][0]
        assert group["timing"]["status"] == "unavailable"
        group["timing"].update(status="available", reason=None,
                               start_time_utc="2026-01-01T00:00:00.000Z", end_time_utc="2026-01-01T00:00:20.000Z")
    else:
        analysis["recorded_cadence"]["session"]["average"]["unit"] = "rpm"
    objects[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(objects, key)
    corrupt = tmp_path / "unsupported-claim.slbk"
    corrupt.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match=error):
        backup.verify_backup(corrupt, PASSWORD)


def test_authenticated_backup_keeps_actual_derived_sides_and_declaration(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_derived_sides.json")
                         .read_text(encoding="utf-8"))
    objects = {key: base64.b64decode(body) for key, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    sides = analysis["ground_contact_sides"]
    assert sides["schema_version"] == 2 and sides["declaration"]["raw_side"] == "left"
    assert [row["left_percent"] for row in sides["record_aggregates"]["executed_intervals"]["items"]] == [48.5, 52]
    document, archived = _archive_input(objects, key)
    archive = tmp_path / "actual-derived-sides.slbk"
    archive.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(archive, PASSWORD)
    assert verified.objects == archived
    assert json.loads(gzip.decompress(verified.objects[key])) == analysis


def test_authenticated_backup_keeps_actual_record_cadence_halves(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence.json")
                         .read_text(encoding="utf-8"))
    objects = {key: base64.b64decode(body) for key, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    block = analysis["recorded_cadence"]
    assert block["schema_version"] == 2 and block["session"]["average"]["value"] == 180
    assert [row["average_cadence"]["value"] for row in block["record_aggregates"]["whole_distance_halves"]["items"]] == [161, 164]
    document, archived = _archive_input(objects, key)
    archive = tmp_path / "actual-record-cadence.slbk"
    archive.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(archive, PASSWORD)
    assert verified.objects == archived
    assert json.loads(gzip.decompress(verified.objects[key])) == analysis
