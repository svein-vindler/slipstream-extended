"""Actual Encoder-produced typed targets survive the authenticated reader."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits
from scripts.benchmark_coach_reuse import CountingStore


def test_authenticated_backup_preserves_genuine_typed_targets_without_rescaling(tmp_path):
    raw = (Path(__file__).parent / "fixtures" / "coach_engine_190_targets.json").read_bytes()
    analysis = json.loads(raw)
    assert analysis["analysis_id"] == "7686b0dd2c760d5a2c5f7678"
    assert analysis["workout_targets"]["items"][0]["target"]["range"]["low"] == 3
    assert analysis["workout_targets"]["items"][0]["target"]["raw_value"] == 0
    assert [row["duration"]["value"] for row in analysis["workout_targets"]["items"]] == [60, 400]
    key = "activities/2026/42/coach-input/v1/canonical/7686b0dd2c760d5a2c5f7678.json"
    store = CountingStore()
    # Profile is the same original synthetic profile used by the actual tracer.
    objects = {name: body for name, body in store.objects.items() if name.startswith("coach/")}
    objects[key] = gzip.compress(raw, mtime=0)
    document, archived = _archive_input(objects, key)
    target = tmp_path / "actual-typed-targets.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    (tmp_path / "source-analysis.sha256").write_text(hashlib.sha256(raw).hexdigest(), encoding="utf-8")
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[key]) == raw
    assert json.loads(gzip.decompress(verified.objects[key]))["workout_targets"] == analysis["workout_targets"]


@pytest.mark.parametrize("mutation", ("speed_unit", "duration_unit", "duplicate_step", "boolean_raw"))
def test_authenticated_backup_rejects_target_unit_identity_and_zero_sentinel_corruption(tmp_path, mutation):
    raw = (Path(__file__).parent / "fixtures" / "coach_engine_190_targets.json").read_bytes()
    analysis = json.loads(raw)
    first, second = analysis["workout_targets"]["items"]
    if mutation == "speed_unit":
        first["target"]["range"]["unit"] = "rpm"
    elif mutation == "duration_unit":
        first["duration"]["unit"] = "m"
    elif mutation == "duplicate_step":
        second["step_index"] = 0
    else:
        first["target"]["raw_value"] = False
    key = "activities/2026/42/coach-input/v1/canonical/7686b0dd2c760d5a2c5f7678.json"
    objects = {name: body for name, body in CountingStore().objects.items() if name.startswith("coach/")}
    objects[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(objects, key)
    target = tmp_path / "tampered-targets.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_workout_targets"):
        backup.verify_backup(target, PASSWORD)
