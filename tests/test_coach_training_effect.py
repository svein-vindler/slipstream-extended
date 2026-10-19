"""Recorded FIT estimates through actual Coach/canonical storage seams."""

import base64
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from pipeline import coach_backfill
from pipeline.granular import gzip_json
from scripts.benchmark_coach_reuse import CountingStore
from scripts.build_coach_training_effect_contract import ACTIVITY, source_store
from scripts.build_running_segments_contract import build_case, synthetic_tcx


def selected(store):
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    return pointer, json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))


def test_actual_fit_training_effect_reaches_versioned_coach_without_dto_estimates():
    produced = build_case()
    objects = {key: base64.b64decode(value) for key, value in produced["objects_base64"].items()}
    prefix = "activities/2026/42/coach-input/v1/"
    pointer = json.loads(objects[prefix + "latest-ready.json"])
    coach = json.loads(gzip.decompress(objects[pointer["analysis_key"]]))
    assert coach["analyzer_version"] == "1.5.0"
    assert pointer["analyzer_version"] == "1.5.0"
    assert coach["summary"]["distance_m"] == 2000
    assert coach["training_effect"] == {
        "schema_version": 1, "source": "fit_session", "method": "fit_recorded_estimate",
        "scale": "0_to_5", "activity_id": "42", "association": "matched",
        "local_start_time": "2026-10-08T01:30:00", "local_date": "2026-10-08",
        "session_start_time_utc": "2026-10-07T23:30:00.000Z",
        "measured_at_utc": "2026-10-07T23:30:40.000Z",
        "source_fit_sha256": coach["source"]["fit_sha256"],
        "aerobic": {"status": "available", "value": 3.2, "source_field": "total_training_effect"},
        "anaerobic": {"status": "available", "value": 0, "source_field": "total_anaerobic_training_effect"},
    }


def test_cold_bulk_verifies_new_effect_seed_without_regenerating():
    store = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, coach = selected(store)
    canonical = store.objects[pointer["analysis_key"]]
    store.reset_counts()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["already_current_activities"] == 1
    assert result["deferred_legacy_activity_count"] == 0
    assert selected(store)[1]["analysis_id"] == coach["analysis_id"]
    assert store.objects[pointer["analysis_key"]] == canonical


def test_unchanged_repeat_and_force_keep_same_id_bytes_and_no_writes():
    store = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, first = selected(store)
    before = dict(store.objects)
    store.reset_counts()
    for force in (False, True, False):
        result = coach_backfill.run_one(activity=ACTIVITY, store=store, force=force)
        assert result["processed_sources"]["42"] == first["analysis_id"]
    assert store.objects == before
    assert store.counts["put"] == 0
    assert selected(store)[0] == pointer


def test_changed_real_fit_estimate_creates_new_canonical_without_overwriting_history():
    store = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    old_pointer, first = selected(store)
    original = store.objects[old_pointer["analysis_key"]]
    changed = source_store(aerobic=4.1)
    for name in ("activity.v1.json", "activity.endurance.v1.json", "activity.tcx"):
        key = "activities/2026/42/" + name
        store.objects[key] = changed.objects[key]
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    new_pointer, second = selected(store)
    assert second["training_effect"]["aerobic"]["value"] == 4.1
    assert second["analysis_id"] != first["analysis_id"]
    assert new_pointer["analysis_key"] != old_pointer["analysis_key"]
    assert store.objects[old_pointer["analysis_key"]] == original


@pytest.mark.parametrize(("value", "status", "expected"), [
    (None, "missing", None), (True, "invalid", None), ("3.2", "invalid", None),
    (-0.1, "invalid", None), (5.1, "invalid", None), (3.14, "invalid", None),
    (1 << 4096, "invalid", None), (0, "available", 0), (5.0, "available", 5.0),
])
def test_stored_decoded_estimate_keeps_sparse_invalid_and_true_zero_explicit(value, status, expected):
    store = source_store()
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["messages"]["session_mesgs"][0]["total_training_effect"] = value
    store.objects[key] = gzip_json(decoded)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    _, analysis = selected(store)
    assert analysis["summary"]["distance_m"] == 2000
    assert analysis["training_effect"]["aerobic"] == {
        "status": status, "value": expected, "source_field": "total_training_effect",
    }
    # No activity-list DTO fallback: that outer source deliberately reports4.8/4.9.
    assert analysis["training_effect"]["anaerobic"]["value"] == 0
    assert analysis["training_effect"]["source"] == "fit_session"


@pytest.mark.parametrize("damage,reason", [
    ("source", "invalid_schema"), ("decode", "decode_errors"), ("id", "id_mismatch"),
    ("local_date", "time_mismatch"), ("session_start", "time_mismatch"),
    ("missing_session", "sessions_missing"), ("two_sessions", "sessions_ambiguous"),
])
def test_unassociated_stored_fit_never_claims_available_estimates(damage, reason):
    store = source_store()
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    session = decoded["messages"]["session_mesgs"][0]
    if damage == "source":
        decoded["source"] = "unverified-third-party"
    elif damage == "decode":
        decoded["decode_errors"] = ["synthetic-decode-error"]
    elif damage == "id":
        decoded["activity"]["id"] = "43"
    elif damage == "local_date":
        decoded["activity"]["start_time_local"] = "2026-10-09T01:30:00"
    elif damage == "session_start":
        session["start_time"] = "2026-10-07T22:30:00.000Z"
    else:
        decoded["messages"]["session_mesgs"] = [] if damage == "missing_session" else [session, session]
    store.objects[key] = gzip_json(decoded)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    _, analysis = selected(store)
    assert analysis["summary"]["distance_m"] == 2000
    block = analysis["training_effect"]
    assert block["association"] == reason
    assert block["session_start_time_utc"] is block["measured_at_utc"] is None
    assert [block[name]["status"] for name in ("aerobic", "anaerobic")] == ["unavailable", "unavailable"]
    assert [block[name]["value"] for name in ("aerobic", "anaerobic")] == [None, None]


def test_missing_activity_clock_emits_current_without_context_and_remains_cold_verifiable():
    store = source_store()
    key = "activities/2026/42/activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["activity"]["start_time_gmt"] = None
    store.objects[key] = gzip_json(decoded)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, first = selected(store)
    assert first["analyzer_version"] == "1.5.0"
    assert "stored_context_snapshot" not in first
    assert "input_source_revisions" not in pointer
    assert first["training_effect"]["association"] == "time_mismatch"
    original = dict(store.objects)
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["already_current_activities"] == 1
    assert store.objects[pointer["analysis_key"]] == original[pointer["analysis_key"]]


@pytest.mark.parametrize(("fixture", "engine"), [
    ("coach_engine_120_transition.json", "1.2.0"),
    ("coach_engine_130_structured_transition.json", "1.3.0"),
])
def test_genuine_old_canonical_is_retained_by_bulk_without_engine_only_history_queue(fixture, engine):
    saved = json.loads((Path(__file__).parent / "fixtures" / fixture).read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(value) for key, value in saved["objects_base64"].items()}
    # Exact synthetic TCX transport attested by both historical pointers. Do not
    # recompress it on this host: gzip's OS header differs across Python versions.
    tcx_transport = base64.b64decode(
        "H4sIAAAAAAAC/62TUUvDQAzHv8roq4xLr92EkR2oe1DQl7X4Hrsgh9u19DLBb286p7Lpuj7sJReS3D+/QIJl"
        "Sz748HrHQbhdkNALRXZ4U4l/9+I5/vgfo6KpW5kny23oviQOH1bOgp2OUxjDdWmzWQYzgCvoLBrN4iM1o0"
        "KoldJveJ6crFaxshZad2UFV3VYRZerxp8gLnwUChU/sQJH7d+1OgqijlW97Z+m9kHUV5E+2l3+WP0f6XvWaZ"
        "YkfNtsHD7TessuzbTwy0VzWGAOGM7ypP08ExhKlF+KyPYTpTAYaXIppLwfyQ5Hmg5DMvt9MrrOar8P4tfd3Yk"
        "5cUyfs0TboGUDAAA=")
    assert gzip.decompress(tcx_transport) == synthetic_tcx()
    store.objects["activities/2026/42/activity.tcx"] = tcx_transport
    pointer, analysis = selected(store)
    for key, digest in pointer["source_revisions"].items():
        assert hashlib.sha256(store.objects[key]).hexdigest() == digest
    original = store.objects[pointer["analysis_key"]]
    assert analysis["analyzer_version"] == engine
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["already_current_activities"] == 1
    assert selected(store)[1]["analyzer_version"] == engine
    assert store.objects[pointer["analysis_key"]] == original
