"""Actual TCX-partitioned record cadence survives authenticated backup unchanged."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits


def test_authenticated_backup_preserves_genuine_kilometer_cadence(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_km.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    raw = gzip.decompress(objects[key])
    analysis = json.loads(raw)
    rows = analysis["recorded_cadence"]["record_aggregates"]["kilometer_splits"]["items"]
    assert [(row["split"], row["start_distance_m"], row["end_distance_m"]) for row in rows] == [
        (1, 0, 1000), (2, 1000, 2000), (3, 2000, 3000),
    ]
    assert [row["average_cadence"]["value"] for row in rows] == [161, 165, 169]
    assert [row["average_cadence"]["coverage"]["valid_seconds"] for row in rows] == [20, 20, 20]
    assert len(analysis["recorded_running_metrics"]["laps"]["items"]) == 6
    assert analysis["recorded_cadence"]["record_aggregates"]["recovery_bouts"]["items"][0]["average_cadence"]["value"] == 166
    document, archived = _archive_input(objects, key)
    target = tmp_path / "actual-km-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[key]) == raw


@pytest.mark.parametrize("mutation", ("native_lap_geometry", "wrong_split", "false_eligible_coverage"))
def test_authenticated_backup_rejects_kilometer_partition_or_coverage_corruption(tmp_path, mutation):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_km.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    row = analysis["recorded_cadence"]["record_aggregates"]["kilometer_splits"]["items"][0]
    if mutation == "native_lap_geometry":
        row.update(end_distance_m=500, distance_m=500)
    elif mutation == "wrong_split":
        row["split"] = 2
    else:
        row["average_cadence"]["coverage"].update(valid_seconds=21, eligible_seconds=21)
    objects[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(objects, key)
    target = tmp_path / "false-km-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_recorded_cadence"):
        backup.verify_backup(target, PASSWORD)
