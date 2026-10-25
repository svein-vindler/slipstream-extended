"""Current Coach measurements through the synthetic stored FIT/TCX interface."""
import gzip
import hashlib
import json

import pytest

from pipeline.coach import build_coach_input, validate_profile
from scripts.build_gct_sides_contract import source_store
from scripts.build_running_segments_contract import ACTIVITY


def stored_analysis(tmp_path):
    store = source_store(tmp_path)
    before = dict(store.objects)
    prefix = "activities/2026/42/"
    decoded = json.loads(gzip.decompress(store.objects[prefix + "activity.v1.json"]))
    endurance = json.loads(gzip.decompress(store.objects[prefix + "activity.endurance.v1.json"]))
    profile = json.loads(store.objects["coach/profiles/v1/2026-01-01/synthetic.json"])
    result = build_coach_input(activity=ACTIVITY, decoded_fit=decoded, endurance=endurance,
                               profile=profile, tcx_sha256=hashlib.sha256(store.objects[prefix + "activity.tcx"]).hexdigest())
    assert store.objects == before
    return result, profile


def stored_recovery_analysis(tmp_path):
    from pipeline import coach_backfill
    from scripts.build_running_segments_contract import SEGMENT_ACTIVITY, structured_source_store

    store = structured_source_store(tmp_path, balance_values=[50, 48, 49, 48.24, 52, 50, 0])
    result = coach_backfill.run_one(activity=dict(SEGMENT_ACTIVITY), store=store)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    profile = json.loads(store.objects["coach/profiles/v1/2026-01-01/synthetic.json"])
    return analysis, profile


def test_kilometer_balance_keeps_the_recorded_scalar_without_a_side_declaration(tmp_path):
    result, _ = stored_analysis(tmp_path)
    group = result["recorded_running_metrics"]["kilometer_splits"]
    assert group["status"] == "available"
    assert [row["split"] for row in group["items"]] == [1, 2]
    # Every original synthetic FIT record explicitly contains 49%; no lap mean
    # or left/right convention is used as a substitute for these samples.
    for row in group["items"]:
        metric = row["average_ground_contact_balance"]
        assert metric["status"] == "available"
        assert metric["value"] == 49
        assert metric["unit"] == "%" and metric["side_semantics"] == "unknown"
        assert metric["coverage"]["status"] == "complete"
        assert "left_percent" not in metric and "right_percent" not in metric


@pytest.mark.parametrize("factory", [stored_analysis, stored_recovery_analysis], ids=["km", "recovery"])
def test_current_raw_balance_analysis_survives_authenticated_backup_verification(tmp_path, factory):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline.private_backup import verify_backup

    analysis, profile = factory(tmp_path)
    assert analysis["analyzer_version"] == "1.10.0"
    normalized = validate_profile({**profile, "created_at": "2026-10-25T00:00:00.000Z"})
    analysis_key = f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json"
    profile_key = f"coach/profiles/v1/{normalized['effective_from']}/{normalized['profile_id']}.json"
    objects = {analysis_key: gzip.compress(json.dumps(analysis).encode(), mtime=0),
               profile_key: json.dumps(normalized).encode()}
    # The established authenticated archive fixture avoids the separate atomic
    # publisher/link seam. Verification still authenticates and strictly reads it.
    verified = verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
    assert verified.objects == objects
    assert json.loads(gzip.decompress(verified.objects[analysis_key])) == analysis


def test_current_coach_publication_is_reused_without_another_source_or_analysis_write(tmp_path):
    from pipeline import coach_backfill

    store = source_store(tmp_path)
    sources = dict(store.objects)
    first = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    assert first["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["analyzer_version"] == "1.10.0"
    assert pointer["analyzer_version"] == "1.10.0"
    assert all(store.objects[key] == body for key, body in sources.items())
    published = dict(store.objects)
    store.reset_counts()
    second = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    assert second["blocked_activities"] == []
    assert second["reused_sources"].get("42") == analysis["analysis_id"]
    assert store.counts["put"] == 0
    assert store.objects == published
    plan = coach_backfill.run(store=store)
    assert plan["blocked_activities"] == []
    assert plan["generated_this_run"] == 0
    assert plan["status"] == "complete"
    assert all(store.objects[key] == body for key, body in published.items())


def test_context_only_changes_reuse_current_immutable_analysis_in_targeted_and_bulk_paths(tmp_path):
    from pipeline import coach_backfill
    from scripts.build_gct_sides_contract import declare

    store = source_store(tmp_path)
    first = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    assert first["blocked_activities"] == []
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    pointer_bytes = store.objects[pointer_key]
    pointer = json.loads(pointer_bytes)
    analysis_bytes = store.objects[pointer["analysis_key"]]
    identifier = first["processed_sources"]["42"]
    context = declare(store, "right")
    store.reset_counts()
    second = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    assert second["reused_sources"].get("42") == identifier
    assert store.counts["put"] == 0
    assert store.objects[pointer_key] == pointer_bytes
    assert store.objects[pointer["analysis_key"]] == analysis_bytes
    bulk = coach_backfill.run(store=store)
    assert bulk["blocked_activities"] == [] and bulk["generated_this_run"] == 0
    assert store.objects[pointer["analysis_key"]] == analysis_bytes
    assert context["gct_balance_side"]["raw_side"] == "right"


def test_targeted_reuse_rejects_a_repointed_frozen_context_binding(tmp_path):
    from pipeline import coach_backfill
    from scripts.build_gct_sides_contract import declare

    store = source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    pointer["input_context_key"] = "activities/2026/43/context/v1/forged.json"
    store.objects[pointer_key] = json.dumps(pointer).encode()
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    assert result["blocked_activities"] == [{"activity_id": "42", "reason": "historical_analysis_unverified"}]
    assert result["processed_sources"] == {}
    assert store.objects == before and store.counts["put"] == 0


@pytest.mark.parametrize("path", ["targeted", "bulk-cold", "bulk-warm"])
@pytest.mark.parametrize("binding", ["null-binding", "null-all", "null-all-engine-downgrade", "same-activity-repoint"])
@pytest.mark.parametrize("original", ["changed", "deleted"])
def test_reuse_rejects_detached_frozen_context_without_analysis_or_pointer_writes(tmp_path, path, binding, original):
    from pipeline import coach_backfill
    from scripts.build_gct_sides_contract import CONTEXT_KEY, declare

    store = source_store(tmp_path)
    context = declare(store)
    coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
    if path == "bulk-warm":
        assert coach_backfill.run(store=store)["blocked_activities"] == []
    pointer_key = "activities/2026/42/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.objects[pointer_key])
    if binding == "same-activity-repoint":
        next_key = "activities/2026/42/context/v1/20261008T130000000-ctx-next.json"
        store.objects[next_key] = json.dumps({**context, "context_id": "ctx-next", "note": "Synthetic later note",
                                               "created_at": "2026-10-08T13:00:00.000Z"}).encode()
        pointer["input_context_key"] = next_key
        pointer["input_context_revision"] = store.list_object_revisions("activities/")[next_key]
    else:
        pointer["input_context_key"] = pointer["input_context_revision"] = None
        if binding in ("null-all", "null-all-engine-downgrade"):
            pointer["context_id"] = None
        if binding == "null-all-engine-downgrade":
            pointer["analyzer_version"] = "1.9.1"
    store.objects[pointer_key] = json.dumps(pointer).encode()
    if original == "deleted":
        del store.objects[CONTEXT_KEY]
    else:
        store.objects[CONTEXT_KEY] = json.dumps({**context, "note": "Synthetic corrupted frozen object"}).encode()
    before = dict(store.objects)
    store.reset_counts()
    if path == "targeted":
        result = coach_backfill.run_one(activity=dict(ACTIVITY), store=store)
        assert result["blocked_activities"] == [{"activity_id": "42", "reason": "historical_analysis_unverified"}]
        assert result["processed_sources"] == {} and store.counts["put"] == 0
    else:
        result = coach_backfill.run(store=store)
        assert result["blocked_activity_count"] == 1 and result["generated_this_run"] == 0
    assert all(store.objects[key] == body for key, body in before.items() if key != coach_backfill.PLAN_KEY)
    assert set(store.objects) - set(before) <= {coach_backfill.PLAN_KEY}


def test_verified_recovery_records_keep_raw_balance_and_contact_time(tmp_path):
    from pipeline import coach_backfill
    from scripts.build_running_segments_contract import SEGMENT_ACTIVITY, structured_source_store

    store = structured_source_store(tmp_path, balance_values=[50, 48, 49, 48.24, 52, 50, 0])
    sources = dict(store.objects)
    result = coach_backfill.run_one(activity=dict(SEGMENT_ACTIVITY), store=store)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    recovery = analysis["recovery_running_metrics"]
    assert recovery["status"] == "available" and recovery["reason"] is None
    assert len(recovery["items"]) == 1
    row = recovery["items"][0]
    assert row["bout"] == 3 and row["source_laps"] == [4]
    assert row["average_ground_contact_time"]["value"] == 240
    assert row["average_ground_contact_balance"] == {
        "status": "available", "value": 48.24, "unit": "%", "source_field": "stance_time_balance",
        "side_semantics": "unknown", "coverage": {"valid_seconds": 10, "eligible_seconds": 10,
            "excluded_gap_seconds": 0, "contributing_intervals": 1, "status": "complete", "boundary_excluded_seconds": 0},
    }
    assert all(store.objects[key] == value for key, value in sources.items())


@pytest.mark.parametrize("corruption", ["foreign-laps", "wrong-unit", "false-coverage", "inferred-side",
                                        "extra-side", "boolean-window"])
def test_authenticated_backup_rejects_corrupt_recovery_measurements(tmp_path, corruption):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline.backup_source import BackupError
    from pipeline.private_backup import verify_backup

    analysis, profile = stored_recovery_analysis(tmp_path)
    block = analysis["recovery_running_metrics"]
    row = block["items"][0]
    if corruption == "foreign-laps":
        row["source_laps"] = [3]
    elif corruption == "wrong-unit":
        row["average_ground_contact_time"]["unit"] = "s"
    elif corruption == "false-coverage":
        row["average_ground_contact_time"]["coverage"]["valid_seconds"] = 20
    elif corruption == "inferred-side":
        row["average_ground_contact_balance"]["side_semantics"] = "left"
    elif corruption == "extra-side":
        row["average_ground_contact_balance"]["left_percent"] = 48.24
    else:
        block["source_window"]["boundary_excluded_record_count"] = False
    normalized = validate_profile({**profile, "created_at": "2026-10-25T00:00:00.000Z"})
    objects = {f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json":
               gzip.compress(json.dumps(analysis).encode(), mtime=0),
               f"coach/profiles/v1/{normalized['effective_from']}/{normalized['profile_id']}.json":
               json.dumps(normalized).encode()}
    with pytest.raises(BackupError):
        verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)


@pytest.mark.parametrize("corruption", ["historical-engine", "inferred-side", "extra-side", "false-coverage", "wrong-unit"])
def test_authenticated_backup_rejects_forged_raw_balance_semantics(tmp_path, corruption):
    from test_coach_context_backup import document
    from test_private_backup import PASSWORD, _write_document

    from pipeline.backup_source import BackupError
    from pipeline.private_backup import verify_backup

    analysis, profile = stored_analysis(tmp_path)
    metric = analysis["recorded_running_metrics"]["kilometer_splits"]["items"][0]["average_ground_contact_balance"]
    if corruption == "historical-engine":
        analysis["analyzer_version"] = "1.9.1"
    elif corruption == "inferred-side":
        metric["side_semantics"] = "left"
    elif corruption == "extra-side":
        metric["left_percent"] = 49
    elif corruption == "false-coverage":
        metric["coverage"]["valid_seconds"] = 10000
    else:
        metric["unit"] = "ratio"
    normalized = validate_profile({**profile, "created_at": "2026-10-25T00:00:00.000Z"})
    objects = {f"activities/2026/42/coach-input/v1/canonical/{analysis['analysis_id']}.json":
               gzip.compress(json.dumps(analysis).encode(), mtime=0),
               f"coach/profiles/v1/{normalized['effective_from']}/{normalized['profile_id']}.json":
               json.dumps(normalized).encode()}
    with pytest.raises(BackupError):
        verify_backup(_write_document(tmp_path, document(objects)), PASSWORD)
