"""Actual verified work cadence remains readable without changing old measurements."""
import base64
import gzip
import json
from pathlib import Path

from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import Limits


def test_authenticated_backup_preserves_genuine_verified_work_cadence(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_work.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    raw = gzip.decompress(objects[key])
    analysis = json.loads(raw)
    group = analysis["recorded_cadence"]["record_aggregates"]["executed_intervals"]
    assert [row["average_cadence"]["value"] for row in group["items"]] == [163, 168]
    assert [row["source_laps"] for row in group["items"]] == [[2, 3], [5]]
    assert [row["average_power"]["value"] for row in analysis["segment_running_metrics"]["executed_intervals"]["items"]] == [150, 400]
    document, archived = _archive_input(objects, key)
    target = tmp_path / "actual-work-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[key]) == raw
