"""Version-safe Coach Input through real bulk/targeted and storage boundaries."""

import gzip
import hashlib
import json
import threading

import pytest
from botocore.exceptions import ClientError

from pipeline import coach_backfill
from pipeline.r2_store import R2Store
from scripts.build_coach_versions_contract import ACTIVITY, PREFIX, MemoryS3


@pytest.mark.parametrize("field,value", [("moving_seconds", 1900.0), ("name", "Changed synthetic run")])
def test_bulk_metadata_change_creates_new_version_and_preserves_old_bytes(field, value):
    client = MemoryS3()
    first = coach_backfill.run(store=client.store())["completed_this_run"][0]
    old_bytes = client.objects[first["key"]]
    client.set_activity({**ACTIVITY, field: value})
    second = coach_backfill.run(store=client.store())["completed_this_run"][0]
    assert second["analysis_id"] != first["analysis_id"]
    assert client.objects[first["key"]] == old_bytes
    assert client.read(first["key"])["summary"]["moving_seconds"] == 1800.0

@pytest.mark.parametrize("mode", ["bulk", "targeted"])
def test_concurrent_different_content_cannot_claim_the_same_analysis_identity(monkeypatch, mode):
    client = MemoryS3()
    original = client.put_object
    competitor = b'{"synthetic":"different analysis occupying the same key"}'
    occupied = []

    def competing_write(**kwargs):
        if "/canonical/" in kwargs["Key"]:
            occupied.append(kwargs["Key"])
            client.objects[kwargs["Key"]] = competitor
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", competing_write)
    with pytest.raises(ValueError, match="analysis identity conflict"):
        if mode == "bulk":
            coach_backfill.run(store=client.store())
        else:
            coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    assert client.objects[occupied[0]] == competitor
    assert coach_backfill.PLAN_KEY not in client.objects
    assert f"{PREFIX}/coach-input/v1/latest-ready.json" not in client.objects

@pytest.mark.parametrize("targeted_first", [True, False])
@pytest.mark.parametrize("moving_seconds", [1800, 1800.0])
def test_bulk_and_targeted_publish_the_same_version_for_the_same_input(targeted_first, moving_seconds):
    client = MemoryS3()
    activity = {**ACTIVITY, "moving_seconds": moving_seconds}
    if targeted_first:
        targeted = coach_backfill.run_one(activity=activity, store=client.store())
        bulk = coach_backfill.run(store=client.store())
    else:
        bulk = coach_backfill.run(store=client.store())
        targeted = coach_backfill.run_one(activity=activity, store=client.store())
    assert bulk["status"] == "complete" and bulk["blocked_activity_count"] == 0
    if targeted_first:
        assert bulk["generated_this_run"] == 0 and bulk["already_current_activities"] == 1
    pointer = client.read(f"{PREFIX}/coach-input/v1/latest-ready.json")
    canonical = client.read(pointer["analysis_key"])
    assert targeted["processed_sources"]["1"] == canonical["analysis_id"]
    assert len([key for key in client.objects if "/canonical/" in key]) == 1
    assert client.puts.count(pointer["analysis_key"]) == 1

@pytest.mark.parametrize("mode", ["bulk", "targeted"])
def test_unchanged_input_reuses_identity_without_rewriting_canonical_bytes(mode):
    client = MemoryS3()
    if mode == "bulk":
        first = coach_backfill.run(store=client.store())["completed_this_run"][0]
        def repeat():
            return coach_backfill.run(store=client.store())["completed_this_run"][0]["analysis_id"]
        identifier = first["analysis_id"]
        key = first["key"]
    else:
        first = coach_backfill.run_one(activity=ACTIVITY, store=client.store())
        identifier = first["processed_sources"]["1"]
        key = f"{PREFIX}/coach-input/v1/canonical/{identifier}.json"
        def repeat():
            return coach_backfill.run_one(activity=ACTIVITY, store=client.store())["processed_sources"]["1"]
    old_bytes = client.objects[key]
    assert repeat() == repeat() == identifier
    assert client.objects[key] == old_bytes
    assert client.puts.count(key) == 1


@pytest.mark.parametrize("changed", ["endurance", "fit", "tcx", "context"])
def test_changed_relevant_source_produces_new_version_in_both_generators(changed):
    client = MemoryS3()
    old = coach_backfill.run(store=client.store())["completed_this_run"][0]
    old_bytes = client.objects[old["key"]]
    if changed == "endurance":
        client.objects[f"{PREFIX}/activity.endurance.v1.json"] = b'{"available":true,"summary":{"distance_m":6000}}'
    elif changed == "fit":
        client.objects[f"{PREFIX}/activity.v1.json"] = b'{"source_fit_sha256":"new-synthetic-fit","messages":{}}'
    elif changed == "tcx":
        client.objects[f"{PREFIX}/activity.tcx"] = b"new synthetic tcx"
    else:
        client.objects[f"{PREFIX}/context/v1/2026-09-30T12-00-00.json"] = b'{"context_id":"synthetic-context","note":"synthetic note"}'
    # Canonical source publication updates its manifest in the normal import flow.
    client.objects[f"{PREFIX}/source-manifest.v1.json"] = b'{"activity_fingerprint":"synthetic-new-source"}'
    new = coach_backfill.run(store=client.store())["completed_this_run"][0]
    targeted = coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    assert new["analysis_id"] != old["analysis_id"]
    assert targeted["processed_sources"]["1"] == new["analysis_id"]
    assert client.objects[old["key"]] == old_bytes
    assert client.read(old["key"])["analysis_id"] == old["analysis_id"]


@pytest.mark.parametrize("mode", ["bulk", "targeted"])
@pytest.mark.parametrize("failure", ["before_checkpoint", "committed_canonical_reply_lost"])
def test_retry_finishes_publication_without_rewriting_committed_analysis(monkeypatch, mode, failure):
    client = MemoryS3()
    original = client.put_object
    checkpoint = coach_backfill.PLAN_KEY if mode == "bulk" else f"{PREFIX}/coach-input/v1/latest-ready.json"
    failed = False

    def interrupted(**kwargs):
        nonlocal failed
        if not failed and failure == "before_checkpoint" and kwargs["Key"] == checkpoint:
            failed = True
            raise TimeoutError("synthetic interruption before checkpoint")
        result = original(**kwargs)
        if not failed and failure == "committed_canonical_reply_lost" and "/canonical/" in kwargs["Key"]:
            failed = True
            raise TimeoutError("synthetic lost canonical reply")
        return result

    monkeypatch.setattr(client, "put_object", interrupted)
    generate = (lambda: coach_backfill.run(store=client.store())) if mode == "bulk" else (
        lambda: coach_backfill.run_one(activity=ACTIVITY, store=client.store()))
    with pytest.raises(TimeoutError):
        generate()
    key = next(key for key in client.objects if "/canonical/" in key)
    old_bytes = client.objects[key]
    assert checkpoint not in client.objects
    generate()
    assert checkpoint in client.objects
    assert client.objects[key] == old_bytes
    assert client.puts.count(key) == 1


@pytest.mark.parametrize("mode", ["bulk", "targeted"])
def test_existing_identity_with_different_content_fails_closed(monkeypatch, mode):
    client = MemoryS3()
    first = coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    key = f"{PREFIX}/coach-input/v1/canonical/{first['processed_sources']['1']}.json"
    competitor = b'{"analysis_id":"synthetic-conflicting-content"}'
    client.objects[key] = competitor
    pointer = client.objects[f"{PREFIX}/coach-input/v1/latest-ready.json"]
    with pytest.raises(ValueError, match="analysis identity conflict"):
        if mode == "bulk":
            coach_backfill.run(store=client.store())
        else:
            coach_backfill.run_one(activity=ACTIVITY, store=client.store(), force=True)
    assert client.objects[key] == competitor
    assert client.objects[f"{PREFIX}/coach-input/v1/latest-ready.json"] == pointer
    assert coach_backfill.PLAN_KEY not in client.objects


@pytest.mark.parametrize("code", ["AccessDenied", "InternalError", "ConditionalRequestConflict"])
def test_storage_failure_never_falls_back_to_unconditional_publication(monkeypatch, code):
    client = MemoryS3()
    original = client.put_object

    def rejected(**kwargs):
        if "/canonical/" in kwargs["Key"]:
            raise ClientError({"Error": {"Code": code}}, "PutObject")
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", rejected)
    with pytest.raises(ClientError):
        coach_backfill.run(store=client.store())
    assert not any("/canonical/" in key for key in client.objects)
    assert coach_backfill.PLAN_KEY not in client.objects


@pytest.mark.parametrize("modes", [("bulk", "bulk"), ("targeted", "targeted"), ("bulk", "targeted")])
def test_simultaneous_equal_input_publishes_one_immutable_analysis(monkeypatch, modes):
    from concurrent.futures import ThreadPoolExecutor

    client = MemoryS3()
    barrier = threading.Barrier(2)
    original = client.put_object

    def simultaneous(**kwargs):
        if "/canonical/" in kwargs["Key"]:
            barrier.wait(timeout=5)
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", simultaneous)

    def generate(mode):
        if mode == "bulk":
            return coach_backfill.run(store=client.store())["completed_this_run"][0]["analysis_id"]
        return coach_backfill.run_one(activity=ACTIVITY, store=client.store())["processed_sources"]["1"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(generate, modes))
    assert results[0] == results[1]
    keys = [key for key in client.objects if "/canonical/" in key]
    assert len(keys) == 1
    assert client.read(keys[0])["analysis_id"] == results[0]
    assert client.puts.count(keys[0]) == 1


def test_rejected_create_only_attempt_keeps_existing_write_budgets():
    from pipeline.r2_store import R2BudgetError

    client = MemoryS3()
    store = R2Store(client=client, bucket="synthetic-bucket", max_writes_per_run=1)
    assert not store.put_if_absent(f"{PREFIX}/activity.tcx", b"different bytes", "text/plain")
    assert client.objects[f"{PREFIX}/activity.tcx"] == b"synthetic tcx"
    with pytest.raises(R2BudgetError, match="write limit"):
        store.put_if_absent("synthetic-new-key", b"new", "text/plain")
    assert store.operations["put"] == 1

def test_legacy_analysis_profile_and_context_bytes_remain_readable_after_new_publication():
    from pipeline.coach import analysis_id, build_coach_input

    client = MemoryS3()
    profile_key = "coach/profiles/v1/2026-01-01/test.json"
    profile_bytes = client.objects[profile_key]
    context_key = f"{PREFIX}/context/v1/2026-09-30T12-00-00.json"
    context_bytes = b'{"context_id":"synthetic-legacy-context","rpe":6}'
    client.objects[context_key] = context_bytes
    # Explicit synthetic 1.2-format/seed projection from the current builder,
    # not genuine historical producer evidence. Genuine legacy bytes have their
    # own transition fixtures and retention tests.
    legacy = build_coach_input(
        activity=ACTIVITY, decoded_fit=client.read(f"{PREFIX}/activity.v1.json"),
        endurance=client.read(f"{PREFIX}/activity.endurance.v1.json"),
        profile=client.read(profile_key), tcx_sha256=hashlib.sha256(b"synthetic tcx").hexdigest(),
        context=client.read(context_key),
    )
    for field in ("training_effect", "ground_contact_sides", "segment_running_metrics",
                  "native_lap_workout_binding", "logical_bouts", "recorded_cadence", "workout_targets"):
        del legacy[field]
    legacy["analyzer_version"] = "1.2.0"
    legacy["analysis_id"] = analysis_id(
        activity_id=ACTIVITY["id"], fit_sha256=legacy["source"]["fit_sha256"],
        tcx_sha256=legacy["source"]["tcx_sha256"], profile_id=legacy["profile"]["profile_id"],
        context_revision=legacy["user_context"]["context_id"],
    )
    old_key = f"{PREFIX}/coach-input/v1/canonical/{legacy['analysis_id']}.json"
    old_bytes = gzip.compress(json.dumps(legacy, indent=2).encode(), mtime=1234)
    client.objects[old_key] = old_bytes
    client.objects[f"{PREFIX}/coach-input/v1/latest-ready.json"] = json.dumps({
        "schema_version": 1, "analysis_key": old_key,
    }).encode()
    publication = coach_backfill.run(store=client.store())
    assert publication["blocked_activities"] == []
    new = publication["completed_this_run"][0]
    targeted = coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    assert new["key"] != old_key
    assert targeted["processed_sources"]["1"] == new["analysis_id"]
    assert client.read(old_key) == legacy
    assert client.objects[old_key] == old_bytes
    assert client.objects[profile_key] == profile_bytes
    assert client.objects[context_key] == context_bytes


def test_identical_existing_analysis_with_older_encoding_is_never_rewritten():
    client = MemoryS3()
    generated = coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    key = f"{PREFIX}/coach-input/v1/canonical/{generated['processed_sources']['1']}.json"
    original_analysis = client.read(key)
    # Same JSON meaning, older whitespace/key order/gzip metadata.
    existing = gzip.compress(json.dumps(original_analysis, indent=2, sort_keys=True).encode(), mtime=4321)
    client.objects[key] = existing
    coach_backfill.run_one(activity=ACTIVITY, store=client.store(), force=True)
    coach_backfill.run(store=client.store())
    assert client.objects[key] == existing
    assert client.puts.count(key) == 1


def test_simultaneous_distinct_inputs_keep_both_versions_and_a_coherent_pointer(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    client = MemoryS3()
    barrier = threading.Barrier(2)
    original = client.put_object

    def simultaneous(**kwargs):
        if "/canonical/" in kwargs["Key"]:
            barrier.wait(timeout=5)
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", simultaneous)
    activities = [ACTIVITY, {**ACTIVITY, "moving_seconds": 1900.0}]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda activity: coach_backfill.run_one(
            activity=activity, store=client.store())["processed_sources"]["1"], activities))
    assert results[0] != results[1]
    for identifier, moving_seconds in zip(results, (1800.0, 1900.0), strict=True):
        key = f"{PREFIX}/coach-input/v1/canonical/{identifier}.json"
        assert client.read(key)["summary"]["moving_seconds"] == moving_seconds
        assert client.puts.count(key) == 1
    pointer = client.read(f"{PREFIX}/coach-input/v1/latest-ready.json")
    assert client.read(pointer["analysis_key"])["analysis_id"] in results


def test_existing_identity_with_different_json_types_is_a_conflict():
    client = MemoryS3()
    activity = {**ACTIVITY, "moving_seconds": 0.0}
    result = coach_backfill.run_one(activity=activity, store=client.store())
    key = f"{PREFIX}/coach-input/v1/canonical/{result['processed_sources']['1']}.json"
    different = client.read(key)
    different["summary"]["moving_seconds"] = False
    occupied_bytes = json.dumps(different).encode()
    client.objects[key] = occupied_bytes
    with pytest.raises(ValueError, match="analysis identity conflict"):
        coach_backfill.run_one(activity=activity, store=client.store(), force=True)
    assert client.objects[key] == occupied_bytes


def test_bulk_reserves_plan_write_with_shared_selection_checkpoint_budget():
    client = MemoryS3()
    for activity_id in ('2', '3'):
        for name in ('activity.v1.json', 'activity.endurance.v1.json', 'activity.tcx'):
            client.objects[f'activities/2026/{activity_id}/{name}'] = client.objects[f'{PREFIX}/{name}']
    client.objects['summary/activities.csv'] = (
        b'Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n'
        b'garmin-1,2026-09-30,Synthetic one,Run,1800\n'
        b'garmin-2,2026-09-30,Synthetic two,Run,1800\n'
        b'garmin-3,2026-09-30,Synthetic three,Run,1800\n'
    )
    store = R2Store(client=client, bucket='synthetic-bucket', max_writes_per_run=5)
    first = coach_backfill.run(max_activities=3, store=store)
    assert first['status'] == 'active'
    assert first['generated_this_run'] == 2
    assert first['remaining_activities'] == 1
    assert store.operations['put'] == 5
    second = coach_backfill.run(max_activities=3, store=R2Store(
        client=client, bucket='synthetic-bucket', max_writes_per_run=5))
    assert second['status'] == 'complete'
    assert second['generated_this_run'] == 1
    assert len([key for key in client.objects if '/canonical/' in key]) == 3


@pytest.mark.parametrize('targeted_first', [True, False])
def test_equal_bulk_targeted_input_keeps_current_selection_bytes(targeted_first):
    client = MemoryS3()
    def targeted():
        return coach_backfill.run_one(activity={**ACTIVITY, 'moving_seconds': 1800}, store=client.store())

    def bulk():
        return coach_backfill.run(store=client.store())
    if targeted_first:
        targeted()
    else:
        bulk()
    pointer_key = f'{PREFIX}/coach-input/v1/latest-ready.json'
    first_bytes = client.objects[pointer_key]
    if targeted_first:
        bulk()
    else:
        targeted()
    assert client.objects[pointer_key] == first_bytes



@pytest.mark.parametrize("mode", ["bulk", "targeted"])
@pytest.mark.parametrize("checkpoint", ["before_pointer", "pointer_reply_lost", "canonical_reply_lost"])
def test_selection_checkpoint_retry_retains_both_canonical_versions(monkeypatch, mode, checkpoint):
    client = MemoryS3()
    pointer_key = f"{PREFIX}/coach-input/v1/latest-ready.json"
    activity = ACTIVITY.copy()

    def generate():
        if mode == "bulk":
            return coach_backfill.run(store=client.store())
        return coach_backfill.run_one(activity=activity, store=client.store())

    generate()
    old_key = client.read(pointer_key)["analysis_key"]
    old_bytes = client.objects[old_key]
    activity["moving_seconds"] = 1900.0
    client.set_activity(activity)
    original = client.put_object
    failed = False

    def interrupted(**kwargs):
        nonlocal failed
        is_pointer = kwargs["Key"] == pointer_key
        is_canonical = "/canonical/" in kwargs["Key"]
        if not failed and checkpoint == "before_pointer" and is_pointer:
            failed = True
            raise TimeoutError("synthetic selection interruption")
        result = original(**kwargs)
        if not failed and ((checkpoint == "pointer_reply_lost" and is_pointer)
                           or (checkpoint == "canonical_reply_lost" and is_canonical)):
            failed = True
            raise TimeoutError("synthetic checkpoint reply lost")
        return result

    monkeypatch.setattr(client, "put_object", interrupted)
    with pytest.raises(TimeoutError):
        generate()
    expected = 1900.0 if checkpoint == "pointer_reply_lost" else 1800.0
    assert client.read(client.read(pointer_key)["analysis_key"])["summary"]["moving_seconds"] == expected
    versions = {key: body for key, body in client.objects.items() if "/canonical/" in key}
    assert len(versions) == 2
    generate()
    assert client.read(client.read(pointer_key)["analysis_key"])["summary"]["moving_seconds"] == 1900.0
    assert client.objects[old_key] == old_bytes
    assert all(client.objects[key] == body and client.puts.count(key) == 1 for key, body in versions.items())


def test_bulk_plan_reply_lost_is_a_safe_committed_checkpoint(monkeypatch):
    client = MemoryS3()
    original = client.put_object
    failed = False

    def committed_then_timeout(**kwargs):
        nonlocal failed
        result = original(**kwargs)
        if not failed and kwargs["Key"] == coach_backfill.PLAN_KEY:
            failed = True
            raise TimeoutError("synthetic plan reply lost")
        return result

    monkeypatch.setattr(client, "put_object", committed_then_timeout)
    with pytest.raises(TimeoutError):
        coach_backfill.run(store=client.store())
    pointer = client.read(f"{PREFIX}/coach-input/v1/latest-ready.json")
    assert client.read(pointer["analysis_key"])["summary"]["moving_seconds"] == 1800.0
    store = client.store()
    result = coach_backfill.run(store=store)
    assert result["status"] == "complete"
    assert store.operations["put"] == 0


def test_byte_budget_failure_cannot_advance_selection_without_its_checkpoint():
    from pipeline.r2_store import R2BudgetError

    client = MemoryS3()
    coach_backfill.run(store=client.store())
    pointer_key = f"{PREFIX}/coach-input/v1/latest-ready.json"
    old_pointer = client.objects[pointer_key]
    old_plan = client.objects[coach_backfill.PLAN_KEY]
    client.set_activity({**ACTIVITY, "moving_seconds": 1900.0})
    # A budget small enough for one compressed canonical result, not its pointer/plan.
    store = R2Store(client=client, bucket="synthetic-bucket", max_write_bytes_per_run=900)
    with pytest.raises(R2BudgetError, match="byte limit"):
        coach_backfill.run(store=store)
    assert client.objects[pointer_key] == old_pointer
    assert client.objects[coach_backfill.PLAN_KEY] == old_plan
    old_versions = {key: body for key, body in client.objects.items() if "/canonical/" in key}
    coach_backfill.run(store=client.store())
    assert client.read(client.read(pointer_key)["analysis_key"])["summary"]["moving_seconds"] == 1900.0
    assert all(client.objects[key] == body for key, body in old_versions.items())


@pytest.mark.parametrize('mode', ['bulk', 'targeted'])
@pytest.mark.parametrize('field', ['schema_version', 'selection_version'])
def test_writer_repairs_boolean_selection_markers_without_claiming_them_as_valid(mode, field):
    client = MemoryS3()
    pointer_key = f'{PREFIX}/coach-input/v1/latest-ready.json'

    def generate():
        if mode == 'bulk':
            return coach_backfill.run(store=client.store())
        return coach_backfill.run_one(activity=ACTIVITY, store=client.store())

    generate()
    original = {key: body for key, body in client.objects.items() if '/canonical/' in key}
    pointer = client.read(pointer_key)
    pointer[field] = True
    client.objects[pointer_key] = json.dumps(pointer).encode()
    generate()
    assert type(client.read(pointer_key)[field]) is int
    assert client.read(pointer_key)[field] == 1
    assert all(client.objects[key] == body for key, body in original.items())


def test_legacy_cached_plan_reconciles_a_new_targeted_selection_without_sweeping_history():
    client = MemoryS3()
    first = coach_backfill.run(store=client.store())
    old = client.read(coach_backfill.PLAN_KEY)
    old.pop('selection_revisions')
    client.objects[coach_backfill.PLAN_KEY] = json.dumps(old).encode()
    coach_backfill.run_one(activity={**ACTIVITY, 'moving_seconds': 1900.0}, store=client.store())
    pointer_key = f'{PREFIX}/coach-input/v1/latest-ready.json'
    assert client.read(client.read(pointer_key)['analysis_key'])['summary']['moving_seconds'] == 1900.0
    # Bulk's original input fingerprint matches the legacy completed plan again.
    result = coach_backfill.run(store=client.store())
    assert result['completed_this_run'][0]['analysis_id'] == first['completed_this_run'][0]['analysis_id']
    assert client.read(client.read(pointer_key)['analysis_key'])['summary']['moving_seconds'] == 1800.0
    assert len([key for key in client.objects if '/canonical/' in key]) == 2


def test_matching_legacy_selection_is_only_inspected_and_never_regenerated(monkeypatch):
    client = MemoryS3()
    coach_backfill.run(store=client.store())
    pointer_key = f'{PREFIX}/coach-input/v1/latest-ready.json'
    pointer = client.read(pointer_key)
    pointer.pop('selection_version')
    legacy_bytes = json.dumps(pointer).encode()
    client.objects[pointer_key] = legacy_bytes
    plan = client.read(coach_backfill.PLAN_KEY)
    plan.pop('selection_revisions')
    client.objects[coach_backfill.PLAN_KEY] = json.dumps(plan).encode()
    canonical_bytes = client.objects[pointer['analysis_key']]
    original = client.get_object

    def unchanged_sources_must_not_be_downloaded(**kwargs):
        if kwargs['Key'] in {f'{PREFIX}/activity.v1.json', f'{PREFIX}/activity.endurance.v1.json', f'{PREFIX}/activity.tcx'}:
            raise AssertionError('Matching legacy input must not regenerate or download historical sources')
        return original(**kwargs)

    monkeypatch.setattr(client, 'get_object', unchanged_sources_must_not_be_downloaded)
    first = coach_backfill.run(store=client.store())
    assert first['generated_this_run'] == 0
    assert first['selection_inspections_this_run'] == 1
    assert client.objects[pointer_key] == legacy_bytes
    assert client.objects[pointer['analysis_key']] == canonical_bytes
    store = client.store()
    second = coach_backfill.run(store=store)
    assert second == first
    assert store.operations['put'] == 0
