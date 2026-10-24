"""The actual stored main-half cadence output remains readable as a backup."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits


def test_authenticated_backup_preserves_genuine_record_main_half_cadence(tmp_path):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_main.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    raw = gzip.decompress(objects[key])
    analysis = json.loads(raw)
    main = analysis["recorded_cadence"]["record_aggregates"]["main_distance_halves"]
    assert [row["average_cadence"]["value"] for row in main["items"]] == [163, 167]
    assert main["source_laps"] == [2, 3, 4, 5]
    assert [row["average_power"]["value"] for row in analysis["segment_running_metrics"]["main_distance_halves"]["items"]] == [150, 200]
    document, archived = _archive_input(objects, key)
    target = tmp_path / "actual-main-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[key]) == raw


@pytest.mark.parametrize("mutation", ("drop_recovery", "wrong_elapsed_end", "false_coverage"))
def test_authenticated_backup_rejects_main_cadence_scope_or_coverage_corruption(tmp_path, mutation):
    fixture = json.loads((Path(__file__).parent / "fixtures" / "coach_engine_190_record_cadence_main.json").read_bytes())
    objects = {name: base64.b64decode(body) for name, body in fixture["objects"].items()}
    key = fixture["analysis_key"]
    analysis = json.loads(gzip.decompress(objects[key]))
    main = analysis["recorded_cadence"]["record_aggregates"]["main_distance_halves"]
    if mutation == "drop_recovery":
        main["source_laps"] = [2, 3, 5]
    elif mutation == "wrong_elapsed_end":
        main["source_windows"][0]["end_time_utc"] = main["session_end_time_utc"]
    else:
        main["items"][0]["average_cadence"]["coverage"]["eligible_seconds"] = 21
    objects[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(objects, key)
    target = tmp_path / "false-main-cadence.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_recorded_cadence"):
        backup.verify_backup(target, PASSWORD)
