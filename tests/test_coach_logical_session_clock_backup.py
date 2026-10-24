"""Complete elapsed-frame admission at the authenticated backup seam."""
import base64
import gzip
import json
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, POINTER, _archive_input

from pipeline import coach_backfill
from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits
from scripts.benchmark_coach_reuse import CountingStore


@pytest.mark.parametrize("missing_session_elapsed", [False, True])
def test_backup_requires_global_elapsed_frame_before_admitting_local_group_timing(tmp_path, missing_session_elapsed):
    encoded = json.loads((Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json")
                         .read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    source_key = "activities/2026/42/activity.v1.json"
    body = store.objects[source_key]
    decoded = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
    assert decoded["messages"]["lap_mesgs"][0]["total_elapsed_time"] == 10
    if missing_session_elapsed:
        del decoded["messages"]["session_mesgs"][0]["total_elapsed_time"]
        store.objects[source_key] = json.dumps(decoded, sort_keys=True).encode()
    before = dict(store.objects)
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects[POINTER])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    assert analysis["logical_bouts"]["schema_version"] == 2
    assert analysis["logical_bouts"]["status"] == "available"
    first = analysis["logical_bouts"]["items"][0]
    assert first["source_laps"] == [1]
    assert first["timing"]["status"] == ("unavailable" if missing_session_elapsed else "available")
    assert analysis["segment_running_metrics"]["session_end_time_utc"] == (
        None if missing_session_elapsed else "2026-10-07T23:31:00.000Z")
    (tmp_path / "actual-analysis.json").write_bytes(gzip.decompress(store.objects[key]))
    document, archived = _archive_input(store.objects, key)
    valid = tmp_path / "genuine.slbk"
    valid.write_bytes(backup._seal(document, PASSWORD, Limits()))
    assert backup.verify_backup(valid, PASSWORD).objects == archived
    assert all(store.objects[source] == original for source, original in before.items())
    if not missing_session_elapsed:
        return
    # Exact finding: local elapsed exists, but does not prove the session frame.
    first["timing"].update(status="available", reason=None,
                           start_time_utc="2026-10-07T23:30:00.000Z", end_time_utc="2026-10-07T23:30:10.000Z")
    forged = dict(store.objects)
    forged[key] = gzip.compress(json.dumps(analysis).encode(), mtime=0)
    document, _ = _archive_input(forged, key)
    invalid = tmp_path / "unsupported-local-timing.slbk"
    invalid.write_bytes(backup._seal(document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_logical_bouts"):
        backup.verify_backup(invalid, PASSWORD)
