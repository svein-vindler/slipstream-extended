"""Actual source-verified recovery cadence survives authenticated backup unchanged."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits


def test_authenticated_backup_preserves_genuine_verified_recovery_cadence(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_recovery.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    raw = gzip.decompress(objects[key])
    analysis = json.loads(raw)
    group = analysis["recorded_cadence"]["record_aggregates"]["recovery_bouts"]
    assert group["status"] == "available" and len(group["items"]) == 1
    row = group["items"][0]
    assert (row["bout"], row["source_laps"], row["workout_step_index"]) == (3, [4], 2)
    assert row["average_cadence"]["value"] == 166
    assert row["average_cadence"]["coverage"]["valid_seconds"] == 10
    assert [item["average_cadence"]["value"] for item in analysis["recorded_cadence"]["record_aggregates"]["executed_intervals"]["items"]] == [163, 168]
    document, archived = _archive_input(objects, key)
    target = tmp_path / "actual-recovery-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[key]) == raw


@pytest.mark.parametrize("mutation", ("wrong_source_lap", "wrong_elapsed_end", "false_elapsed_coverage"))
def test_authenticated_backup_rejects_recovery_scope_or_elapsed_coverage_corruption(tmp_path, mutation):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_recovery.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    row = analysis["recorded_cadence"]["record_aggregates"]["recovery_bouts"]["items"][0]
    if mutation == "wrong_source_lap":
        row["source_laps"] = [5]
    elif mutation == "wrong_elapsed_end":
        row["end_time_utc"] = "2026-10-07T23:30:50.000Z"
    else:
        row["average_cadence"]["coverage"].update(valid_seconds=11, eligible_seconds=11)
    objects[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(objects, key)
    target = tmp_path / "false-recovery-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_recorded_cadence"):
        backup.verify_backup(target, PASSWORD)
