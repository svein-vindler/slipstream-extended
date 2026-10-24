"""Declared sides on existing eligible record means, not new contact channels."""
import base64
import gzip
import json
from pathlib import Path

from pipeline import coach_backfill
from scripts.benchmark_coach_reuse import CountingStore


def test_published_declared_sides_reference_existing_interval_means_without_changing_raw_metrics(tmp_path):
    encoded = json.loads((Path(__file__).parent / "fixtures" / "logical_bouts_stored_source.json")
                         .read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in encoded.items()}
    source_key = "activities/2026/42/activity.v1.json"
    source = store.objects[source_key]
    decoded = json.loads(gzip.decompress(source) if source[:2] == b"\x1f\x8b" else source)
    # Synthetic normalized-source variant. No claim of a new physical FIT decode.
    for row, value in zip(decoded["messages"]["record_mesgs"], (50, 48, 49, 50, 52, 50, 0), strict=True):
        row["stance_time_balance"] = value
    store.objects[source_key] = json.dumps(decoded, sort_keys=True).encode()
    context = {
        "schema_version": 2, "activity_id": "42", "context_id": "ctx-derived-sides",
        "activity_date": "2026-10-08", "date_source": "garmin_local", "rpe": 7,
        "conditions": "Synthetic dry", "note": "Synthetic preserved note", "workout_correction": None,
        "source": "user", "created_at": "2026-10-08T12:00:00.000Z",
        "gct_balance_side": {"source_fit_sha256": decoded["source_fit_sha256"], "raw_side": "left",
                             "source": "user_activity_declaration"},
    }
    store.objects["activities/2026/42/context/v1/20261008T120000000-ctx-derived-sides.json"] = json.dumps(context).encode()
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity={
        "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
    }, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    (tmp_path / "analysis.actual.json").write_bytes(gzip.decompress(store.objects[key]))
    (tmp_path / "preserved-blocks.actual.json").write_text(json.dumps({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics", "user_context")
    }, sort_keys=True), encoding="utf-8")
    (tmp_path / "resources.actual.json").write_text(json.dumps(store.counts, sort_keys=True), encoding="utf-8")
    assert all(store.objects[key] == body for key, body in before.items())
    assert analysis["user_context"] == context
    raw = analysis["segment_running_metrics"]["executed_intervals"]
    assert raw["status"] == "available"
    assert [row["source_laps"] for row in raw["items"]] == [[2, 3], [5]]
    assert [row["average_ground_contact_balance"]["value"] for row in raw["items"]] == [48.5, 52]
    assert all(row["average_ground_contact_balance"]["side_semantics"] == "unknown" for row in raw["items"])
    sides = analysis["ground_contact_sides"]
    assert sides["status"] == "available"
    projected = sides["record_aggregates"]["executed_intervals"]
    assert sides["schema_version"] == 2 and projected["status"] == "available"
    assert [(row["interval"], row["left_percent"], row["right_percent"]) for row in projected["items"]] == [
        (1, 48.5, 51.5), (2, 52, 48),
    ]
    for row, original in zip(projected["items"], raw["items"], strict=True):
        assert row["status"] == "available" and row["both_sides_derived"] is True
        assert row["raw_side"] == "left" and row["calculated_side"] == "right"
        assert row["method"] == "user_declared_record_mean_and_complement"
        assert row["coverage"] == original["average_ground_contact_balance"]["coverage"]
