"""Real terminal-excluded cadence through stored publication and authenticated backup."""
import base64
import copy
import gzip
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest
from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import coach_backfill
from pipeline import private_backup as backup
from pipeline.backup_source import BackupError, Limits
from pipeline.granular import gzip_json, json_bytes
from scripts.benchmark_coach_reuse import CountingStore


def test_authenticated_backup_rejects_counting_the_excluded_terminal_pair(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json"
    fixture_bytes = fixture.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == (
        "5efb4b85fd19f4d80293176b4c0e53ef0ec300c45c8bb25606865afc493083d3"
    )
    store = CountingStore()
    store.objects = {
        key: base64.b64decode(body) for key, body in json.loads(fixture_bytes).items()
    }
    original_objects = dict(store.objects)
    decoded_key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[decoded_key]))
    original = copy.deepcopy(decoded)
    original_records = original["messages"]["record_mesgs"]
    assert len(original_records) == 7 and decoded["decode_errors"] == []
    kept = (0, 1, 6)
    records = [copy.deepcopy(original_records[index]) for index in kept]
    origin = datetime.fromisoformat(records[0]["timestamp"])
    assert [(datetime.fromisoformat(row["timestamp"]) - origin).total_seconds()
            for row in records] == [0, 10, 60]
    assert [row["distance"] for row in records] == [0, 500, 3000]
    for row, base in zip(records, (80, 81, 86), strict=True):
        assert "cadence" not in row and "fractional_cadence" not in row
        row.update(cadence=base, fractional_cadence=0)
    decoded["messages"]["record_mesgs"] = records
    sessions = decoded["messages"]["session_mesgs"]
    assert len(sessions) == 1
    sessions[0]["total_elapsed_time"] = 20
    # Declared decoded-input variant only; raw FIT and native lap facts stay intact.
    for name, rows in decoded["messages"].items():
        if name not in ("record_mesgs", "session_mesgs"):
            assert rows == original["messages"][name]
    assert {name: value for name, value in decoded.items() if name != "messages"} == {
        name: value for name, value in original.items() if name != "messages"
    }
    assert {name: value for name, value in sessions[0].items() if name != "total_elapsed_time"} == {
        name: value for name, value in original["messages"]["session_mesgs"][0].items()
        if name != "total_elapsed_time"
    }
    assert sessions[0]["timestamp"] == original["messages"]["session_mesgs"][0]["timestamp"]
    for row, index in zip(records, kept, strict=True):
        assert {name: value for name, value in row.items()
                if name not in ("cadence", "fractional_cadence")} == original_records[index]
    store.objects[decoded_key] = gzip_json(decoded)
    assert {key: body for key, body in store.objects.items() if key != decoded_key} == {
        key: body for key, body in original_objects.items() if key != decoded_key
    }

    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run",
        "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    counts = dict(store.counts)
    after = {key: store.objects.get(key) for key in before}
    evidence = tmp_path / "terminal-contribution-tracer"
    evidence.mkdir()
    (evidence / "decoded-input.actual.json").write_bytes(json_bytes(decoded))
    for label, objects in (("before", before), ("after", after)):
        (evidence / f"sources.{label}.json").write_text(json.dumps({
            key: base64.b64encode(body).decode("ascii") if body is not None else None
            for key, body in objects.items()
        }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_bytes(json_bytes({
        "counts": counts, "result": result, "sources_preserved": before == after,
        "fixture_bytes_preserved": fixture.read_bytes() == fixture_bytes,
        "variant": {
            "retained_record_indices": [0, 1, 6], "cadence": [80, 81, 86],
            "fractional_cadence": 0, "session.total_elapsed_time": 20,
            "session.summary_timestamp_changed": False,
        },
    }))
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    pointer_bytes = store.objects.get(pointer_key)
    if pointer_bytes is not None:
        (evidence / "pointer.actual.json").write_bytes(pointer_bytes)
    assert result["blocked_activities"] == []
    assert pointer_bytes is not None
    analysis_key = json.loads(pointer_bytes)["analysis_key"]
    canonical = store.objects[analysis_key]
    raw = gzip.decompress(canonical)
    analysis = json.loads(raw)
    (evidence / "analysis.actual.json").write_bytes(raw)
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "preserved-blocks.actual.json").write_bytes(json_bytes({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")
    }))
    assert before == after and fixture.read_bytes() == fixture_bytes

    whole = analysis["recorded_cadence"]["record_aggregates"]["whole_distance_halves"]
    assert whole["status"] == "available" and whole["reason"] is None
    assert whole["clock_basis"] == "fit_start_time_plus_total_elapsed_time"
    assert whole["source_window"] == {
        "disposition": "verified_terminal_exclusion", "original_record_count": 3,
        "boundary_excluded_record_count": 1, "boundary_excluded_pair_count": 1,
    }
    assert whole["source_window"] == analysis["segment_running_metrics"]["source_window"]
    assert (datetime.fromisoformat(whole["session_end_time_utc"].replace("Z", "+00:00"))
            - origin).total_seconds() == 20
    assert (datetime.fromisoformat(whole["session_summary_timestamp_utc"].replace("Z", "+00:00"))
            - origin).total_seconds() == 60
    assert [row["segment"] for row in whole["items"]] == ["first", "second"]
    metric = whole["items"][0]["average_cadence"]
    assert metric["status"] == "available" and metric["value"] == 160
    assert metric["unit"] == "steps_per_minute"
    assert metric["coverage"] == {
        "valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 0,
        "contributing_intervals": 1, "status": "partial", "boundary_excluded_seconds": 10,
    }

    # Genuine baseline must pass the public authenticated reader before corruption.
    published_objects = dict(store.objects)
    document, archived = _archive_input(published_objects, analysis_key)
    genuine_path = evidence / "genuine-terminal.slbk"
    genuine_path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(genuine_path, PASSWORD)
    (evidence / "baseline-backup.actual.json").write_bytes(json_bytes({
        "verified": True, "all_archived_bytes_preserved": verified.objects == archived,
        "canonical_bytes_preserved": verified.objects.get(analysis_key) == canonical,
    }))
    assert verified.objects == archived
    assert verified.objects[analysis_key] == canonical
    assert gzip.decompress(verified.objects[analysis_key]) == raw
    assert store.objects == published_objects

    corrupted = copy.deepcopy(analysis)
    corrupted["recorded_cadence"]["record_aggregates"]["whole_distance_halves"]["items"][0][
        "average_cadence"
    ]["coverage"]["contributing_intervals"] = 2
    restored = copy.deepcopy(corrupted)
    restored["recorded_cadence"]["record_aggregates"]["whole_distance_halves"]["items"][0][
        "average_cadence"
    ]["coverage"]["contributing_intervals"] = 1
    assert json_bytes(restored) == json_bytes(analysis)
    (evidence / "single-field-mutation.actual.json").write_bytes(json_bytes({
        "path": "recorded_cadence.record_aggregates.whole_distance_halves.items[0].average_cadence.coverage.contributing_intervals",
        "before": 1, "after": 2, "all_other_fields_unchanged": True,
    }))
    (evidence / "corrupted-analysis.actual.json").write_bytes(json_bytes(corrupted))
    tampered_objects = dict(published_objects)
    tampered_objects[analysis_key] = gzip_json(corrupted)
    assert {key: body for key, body in tampered_objects.items() if key != analysis_key} == {
        key: body for key, body in published_objects.items() if key != analysis_key
    }
    bad_document, _ = _archive_input(tampered_objects, analysis_key)
    bad_path = evidence / "false-terminal-contributions.slbk"
    bad_path.write_bytes(backup._seal(bad_document, PASSWORD, Limits()))
    with pytest.raises(BackupError, match="invalid_recorded_cadence"):
        backup.verify_backup(bad_path, PASSWORD)
    assert store.objects == published_objects
