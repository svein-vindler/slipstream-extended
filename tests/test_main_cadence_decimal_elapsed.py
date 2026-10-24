"""Real stored producer/backup preserve exact decimal-millisecond main windows."""
import base64
import copy
import gzip
import hashlib
import json
from pathlib import Path

from test_coach_logical_bouts_backup import PASSWORD, _archive_input

from pipeline import coach_backfill
from pipeline import private_backup as backup
from pipeline.backup_source import Limits
from pipeline.granular import gzip_json
from scripts.benchmark_coach_reuse import CountingStore


def test_stored_main_cadence_preserves_decimal_elapsed_through_authenticated_backup(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json"
    fixture_bytes = fixture.read_bytes()
    assert hashlib.sha256(fixture_bytes).hexdigest() == "5efb4b85fd19f4d80293176b4c0e53ef0ec300c45c8bb25606865afc493083d3"
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in json.loads(fixture_bytes).items()}
    key = "activities/2026/42/activity.v1.json"
    original_objects = dict(store.objects)
    decoded = json.loads(gzip.decompress(store.objects[key]))
    original = copy.deepcopy(decoded)
    for row, base in zip(decoded["messages"]["record_mesgs"], (80, 81, 82, 83, 84, 85, 86), strict=True):
        row.update(cadence=base, fractional_cadence=0)
    # Explicit decoded-source variant, not a new SDK/FIT export. At the stored
    # 500m per10s geometry, the first1.001s covers50.05m, not a stretched500m lap.
    lap = decoded["messages"]["lap_mesgs"][1]
    lap.update(total_elapsed_time=1.001, total_distance=50.05)
    for name, rows in decoded["messages"].items():
        if name not in ("record_mesgs", "lap_mesgs"):
            assert rows == original["messages"][name]
    for index, (row, old) in enumerate(zip(decoded["messages"]["lap_mesgs"], original["messages"]["lap_mesgs"], strict=True)):
        assert {name: value for name, value in row.items() if index != 1 or name not in ("total_elapsed_time", "total_distance")} == {
            name: value for name, value in old.items() if index != 1 or name not in ("total_elapsed_time", "total_distance")
        }
    store.objects[key] = gzip_json(decoded)
    assert {name: body for name, body in store.objects.items() if name != key} == {
        name: body for name, body in original_objects.items() if name != key
    }
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    counts = dict(store.counts)
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    raw = gzip.decompress(canonical)
    analysis = json.loads(raw)
    evidence = tmp_path / "main-decimal-elapsed-tracer"
    evidence.mkdir()
    (evidence / "analysis.actual.json").write_bytes(raw)
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "sources.before.json").write_text(json.dumps({
        name: base64.b64encode(body).decode("ascii") for name, body in before.items()
    }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "counts": counts, "result": result, "sources_preserved": all(store.objects[name] == body for name, body in before.items()),
        "variant_fields": ["record_mesgs.cadence", "record_mesgs.fractional_cadence", "lap_mesgs[1].total_elapsed_time", "lap_mesgs[1].total_distance"],
    }, sort_keys=True), encoding="utf-8")
    assert result["blocked_activities"] == []
    assert all(store.objects[name] == body for name, body in before.items())
    assert fixture.read_bytes() == fixture_bytes
    assert analysis["recorded_running_metrics"]["association"] == "matched"
    assert [row["average_power"]["value"] for row in analysis["recorded_running_metrics"]["laps"]["items"]] == [777] * 6
    main = analysis["recorded_cadence"]["record_aggregates"]["main_distance_halves"]
    assert main["status"] == "available" and main["distance_basis"] == "fit_executed_main_distance"
    assert main["source_laps"] == [2, 3, 4, 5]
    assert main["source_windows"][0]["lap"] == 2
    assert main["source_windows"][0]["distance_m"] == 50.05
    assert main["source_windows"][0]["end_time_utc"] == "2026-10-07T23:30:11.001Z"
    assert analysis["workout_structure"]["executed_laps"][1]["elapsed_seconds"] == 1.001
    objects = {name: store.objects[name] for name in before}
    objects[pointer["analysis_key"]] = canonical
    document, archived = _archive_input(objects, pointer["analysis_key"])
    target = tmp_path / "main-decimal-elapsed.slbk"
    target.write_bytes(backup._seal(document, PASSWORD, Limits()))
    verified = backup.verify_backup(target, PASSWORD)
    assert verified.objects == archived
    assert gzip.decompress(verified.objects[pointer["analysis_key"]]) == raw
