"""Stored FIT + known-day context through real Coach/storage publication."""

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError
from garmin_fit_sdk import Encoder
from test_coach_stored_context_snapshot import health_snapshot

from pipeline import coach_backfill
from pipeline.granular import decode_fit, gzip_json, normalize_endurance_session
from pipeline.r2_store import R2BudgetError
from scripts.build_coach_versions_contract import MemoryS3

DAY = "2026-10-19"
PREFIX = "activities/2026/42"
ACTIVITY = {"id": "42", "date": DAY, "name": "Synthetic context run",
            "type": "Run", "moving_seconds": 1800.0}
CONTEXT_KEYS = {
    "training_day": "health/training-context/v1/2026/10/2026-10-19.json",
    "sleep": "health/sleep/v1/2026/10/2026-10-19.json",
    "hrv": "health/hrv/2026/10/2026-10-19.json",
    "sleep_check": "refresh/checks/v1/health/sleep/2026-10-19.json",
    "hrv_check": "refresh/checks/v1/health/hrv/2026-10-19.json",
}


class StoredSources(MemoryS3):
    """Only the outer S3 transport is substituted; retain real R2 budgets."""

    def __init__(self):
        super().__init__()
        self.gets = []
        self.failures = {}

    def get_object(self, **kwargs):
        self.gets.append(kwargs["Key"])
        if kwargs["Key"] in self.failures:
            raise self.failures[kwargs["Key"]]
        response = super().get_object(**kwargs)
        response["ETag"] = '"' + hashlib.md5(
            self.objects[kwargs["Key"]], usedforsecurity=False).hexdigest() + '"'
        return response


def source_store(*, context=True, activity_id="42"):
    client = StoredSources()
    client.objects = {key: body for key, body in client.objects.items()
                      if not key.startswith("activities/")}
    client.objects["summary/activities.csv"] = (
        "Activity ID,Activity Date,Activity Local Date,Activity Name,Activity Type,Moving Time\n"
        f"garmin-{activity_id},2026-10-19 07:00:00,2026-10-19,Synthetic context run,Run,1800\n").encode()
    start = datetime(2026, 10, 19, 7, tzinfo=timezone.utc)
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "start_time": start,
                        "timestamp": start + timedelta(minutes=30), "sport": "running"})
    dto = {"activityId": int(activity_id), "activityName": ACTIVITY["name"],
           "activityType": {"typeKey": "running"},
           "startTimeGMT": "2026-10-19 07:00:00", "startTimeLocal": "2026-10-19 09:00:00"}
    tcx = (b'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
           b'<Id>2026-10-19T07:00:00Z</Id><Lap StartTime="2026-10-19T07:00:00Z">'
           b'<TotalTimeSeconds>1800</TotalTimeSeconds><DistanceMeters>5000</DistanceMeters>'
           b'<Track><Trackpoint><Time>2026-10-19T07:00:00Z</Time><DistanceMeters>0</DistanceMeters>'
           b'</Trackpoint><Trackpoint><Time>2026-10-19T07:30:00Z</Time><DistanceMeters>5000</DistanceMeters>'
           b'</Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>')
    prefix = f"activities/2026/{activity_id}"
    client.objects.update({
        f"{prefix}/activity.v1.json": gzip_json(decode_fit(bytes(encoder.close()), activity=dto)),
        f"{prefix}/activity.endurance.v1.json": gzip_json(normalize_endurance_session(dto, tcx)),
        f"{prefix}/activity.tcx": tcx,
    })
    if context:
        snapshot = health_snapshot()
        client.objects[CONTEXT_KEYS["training_day"]] = json.dumps(snapshot["training_day"]).encode()
        for kind in ("sleep", "hrv"):
            body = gzip_json(snapshot[kind]["data"])
            client.objects[CONTEXT_KEYS[kind]] = body
            client.objects[CONTEXT_KEYS[f"{kind}_check"]] = json.dumps({
                "schema_version": 1, "kind": "health-source-check", "stream": kind,
                "scope": f"health/{kind}/{DAY}", "status": "ready", "date": DAY,
                "source_key": CONTEXT_KEYS[kind], "source_sha256": hashlib.sha256(body).hexdigest(),
                "checked_at": "2026-10-19T08:00:00Z",
            }).encode()
    return client


def selected(client):
    pointer = client.read(f"{PREFIX}/coach-input/v1/latest-ready.json")
    return pointer, client.read(pointer["analysis_key"])


def test_targeted_reads_exact_day_sources_into_real_append_only_coach():
    client = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    pointer, result = selected(client)
    assert result["analyzer_version"] == "1.5.0"
    context = result["stored_context_snapshot"]
    assert context["snapshot_at"] == "2026-10-19T08:00:00.000Z"
    assert context["reference_time_utc"] == "2026-10-19T07:00:00.000Z"
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] == 25200
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] == 50
    assert context["training_day"]["readiness"]["value"] == 80
    assert context["training_day"]["recovery"]["value"] == 0
    assert context["sleep"]["source_checked_at"] == "2026-10-19T08:00:00.000Z"
    assert pointer["analyzer_version"] == "1.5.0"
    assert {key for key in client.gets if key.startswith(("health/", "refresh/checks/"))} == set(CONTEXT_KEYS.values())
    assert sum("/canonical/" in key for key in client.puts) == 1


def test_repeat_and_forced_recovery_keep_identity_bytes_and_do_not_rewrite():
    client = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    pointer, result = selected(client)
    body = client.objects[pointer["analysis_key"]]
    writes = list(client.puts)
    for force in (False, True, False):
        again = coach_backfill.run_one(activity=ACTIVITY, store=client.store(), force=force)
        assert again["processed_sources"]["42"] == result["analysis_id"]
    assert client.puts == writes
    assert client.objects[pointer["analysis_key"]] == body


def test_targeted_verified_absence_then_present_creates_a_new_immutable_snapshot():
    client = source_store(context=False)
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    old_pointer, old = selected(client)
    old_bytes = client.objects[old_pointer["analysis_key"]]
    assert old["stored_context_snapshot"]["snapshot_at"] is None
    assert old["stored_context_snapshot"]["sleep"]["status"] == "not_collected"
    present = source_store()
    client.objects.update({key: body for key, body in present.objects.items() if key in CONTEXT_KEYS.values()})
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    _, current = selected(client)
    assert current["analysis_id"] != old["analysis_id"]
    assert current["stored_context_snapshot"]["sleep"]["status"] == "available"
    assert client.objects[old_pointer["analysis_key"]] == old_bytes


@pytest.mark.parametrize("failure", [
    ClientError({"Error": {"Code": "403"}}, "GetObject"),
    TimeoutError("synthetic storage timeout"), R2BudgetError("synthetic budget stop"),
    KeyError("unverified missing response"),
])
def test_failed_source_read_is_not_absence_and_preserves_previous_selection(failure):
    client = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    preserved = dict(client.objects)
    writes = list(client.puts)
    client.failures[CONTEXT_KEYS["sleep"]] = failure
    with pytest.raises(type(failure)):
        coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    assert client.objects == preserved
    assert client.puts == writes


def test_mismatched_or_missing_receipt_cannot_claim_checked_time():
    client = source_store()
    del client.objects[CONTEXT_KEYS["training_day"]]
    del client.objects[CONTEXT_KEYS["hrv_check"]]
    receipt = json.loads(client.objects[CONTEXT_KEYS["sleep_check"]])
    receipt["source_sha256"] = "0" * 64
    receipt["checked_at"] = "2099-01-01T00:00:00Z"
    client.objects[CONTEXT_KEYS["sleep_check"]] = json.dumps(receipt).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    _, current = selected(client)
    context = current["stored_context_snapshot"]
    assert context["snapshot_at"] is None
    for kind in ("sleep", "hrv"):
        assert context[kind]["status"] == "available"
        assert context[kind]["source_checked_at"] is None


def test_cold_bulk_retains_existing_snapshot_without_reading_new_health():
    client = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    pointer, result = selected(client)
    preserved = dict(client.objects)
    writes = list(client.puts)
    client.gets.clear()
    # A later matching receipt is not a global historical regeneration trigger.
    receipt = json.loads(client.objects[CONTEXT_KEYS["sleep_check"]])
    receipt["checked_at"] = "2026-10-20T08:00:00Z"
    client.objects[CONTEXT_KEYS["sleep_check"]] = json.dumps(receipt).encode()
    bulk = coach_backfill.run(store=client.store())
    assert bulk["generated_this_run"] == 0
    assert bulk["already_current_activities"] == 1
    assert bulk["blocked_activity_count"] == 0
    assert client.objects[pointer["analysis_key"]] == preserved[pointer["analysis_key"]]
    assert selected(client)[1]["analysis_id"] == result["analysis_id"]
    assert not any(key.startswith(("health/", "refresh/checks/")) for key in client.gets)
    assert not any("/canonical/" in key for key in client.puts[len(writes):])


def test_legitimate_new_bulk_uses_same_context_and_identity_as_targeted():
    bulk_client = source_store()
    targeted_client = source_store()
    bulk = coach_backfill.run(store=bulk_client.store())
    coach_backfill.run_one(activity=ACTIVITY, store=targeted_client.store())
    bulk_pointer, bulk_coach = selected(bulk_client)
    target_pointer, target_coach = selected(targeted_client)
    assert bulk_coach["analyzer_version"] == "1.5.0"
    assert bulk_coach == target_coach
    assert bulk_pointer == target_pointer
    assert bulk["bulk_reuse_proofs"]["42"]["analyzer_version"] == "1.5.0"
    assert bulk_client.objects[bulk_pointer["analysis_key"]] == targeted_client.objects[target_pointer["analysis_key"]]


def test_two_new_same_day_activities_share_five_reads_only_for_this_bulk_invocation(record_property):
    client = source_store()
    second = source_store(activity_id="43")
    client.objects.update({key: body for key, body in second.objects.items() if key.startswith("activities/")})
    client.objects["summary/activities.csv"] += second.objects["summary/activities.csv"].split(b"\n", 1)[1]
    store = client.store()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == 2
    for key in CONTEXT_KEYS.values():
        assert client.gets.count(key) == 1
    assert {name: store.operations[name] for name in ("get", "head", "list_pages", "put")} == {
        "get": 13, "head": 0, "list_pages": 4, "put": 5,
    }
    record_property("synthetic_two_new_activities_r2_operations", json.dumps(store.operations, sort_keys=True))
    client.gets.clear()
    repeat_store = client.store()
    repeat = coach_backfill.run(store=repeat_store)
    assert repeat["completed_this_run"] == result["completed_this_run"]  # saved complete-plan result
    assert repeat_store.operations["put"] == 0
    assert not any(key.startswith(("health/", "refresh/checks/")) for key in client.gets)
    record_property("synthetic_saved_complete_plan_r2_operations", json.dumps(repeat_store.operations, sort_keys=True))


@pytest.mark.parametrize("damage", ["snapshot", "missing_source_proof", "invalid_source_proof"])
def test_cold_identity_or_source_proof_damage_is_held_without_repair_or_history_queue(damage):
    client = source_store()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    pointer, result = selected(client)
    if damage == "snapshot":
        result["stored_context_snapshot"]["training_day"]["readiness"]["value"] = 81
        client.objects[pointer["analysis_key"]] = gzip_json(result)
    else:
        if damage == "missing_source_proof":
            del pointer["input_source_revisions"]
        else:
            pointer["input_source_revisions"]["sources"]["sleep"] = {"status": "absent", "sha256": "0" * 64}
        client.objects[f"{PREFIX}/coach-input/v1/latest-ready.json"] = json.dumps(pointer).encode()
    before = dict(client.objects)
    client.gets.clear()
    result = coach_backfill.run(store=client.store())
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert result["deferred_legacy_activity_count"] == 1
    assert all(client.objects[key] == body for key, body in before.items())
    assert not any(key.startswith(("health/", "refresh/checks/")) for key in client.gets)


def test_targeted_new_snapshot_reconciles_old_bulk_checkpoint_without_new_health_reads():
    client = source_store()
    coach_backfill.run(store=client.store())
    old_pointer, _ = selected(client)
    old_bytes = client.objects[old_pointer["analysis_key"]]
    receipt = json.loads(client.objects[CONTEXT_KEYS["sleep_check"]])
    receipt["checked_at"] = "2026-10-20T08:00:00Z"
    client.objects[CONTEXT_KEYS["sleep_check"]] = json.dumps(receipt).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    new_pointer, new = selected(client)
    assert new_pointer["analysis_key"] != old_pointer["analysis_key"]
    client.gets.clear()
    bulk = coach_backfill.run(store=client.store())
    assert bulk["generated_this_run"] == bulk["remaining_activities"] == 0
    assert bulk["already_current_activities"] == 1
    assert bulk["deferred_legacy_activity_count"] == 0
    assert client.objects[old_pointer["analysis_key"]] == old_bytes
    assert selected(client)[1]["analysis_id"] == new["analysis_id"]
    assert not any(key.startswith(("health/", "refresh/checks/")) for key in client.gets)


def test_wrong_day_payload_cannot_borrow_a_matching_hash_receipts_clock():
    client = source_store()
    del client.objects[CONTEXT_KEYS["training_day"]]
    del client.objects[CONTEXT_KEYS["hrv"]]
    del client.objects[CONTEXT_KEYS["hrv_check"]]
    sleep = client.read(CONTEXT_KEYS["sleep"])
    sleep["date"] = "2026-10-18"
    body = gzip_json(sleep)
    client.objects[CONTEXT_KEYS["sleep"]] = body
    receipt = json.loads(client.objects[CONTEXT_KEYS["sleep_check"]])
    receipt["source_sha256"] = hashlib.sha256(body).hexdigest()
    client.objects[CONTEXT_KEYS["sleep_check"]] = json.dumps(receipt).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    context = selected(client)[1]["stored_context_snapshot"]
    assert context["sleep"]["status"] == "wrong_date"
    assert context["sleep"]["source_checked_at"] is None
    assert context["snapshot_at"] is None


def test_parseable_invalid_optional_training_preserves_sleep_hrv_and_coach_summary():
    client = source_store()
    training = json.loads(client.objects[CONTEXT_KEYS["training_day"]])
    training["schema_version"] = True
    training["source_checked_at"] = "2099-01-01T00:00:00Z"
    client.objects[CONTEXT_KEYS["training_day"]] = json.dumps(training).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=client.store())
    _, result = selected(client)
    context = result["stored_context_snapshot"]
    assert result["summary"]["distance_m"] == 5000
    assert context["training_day"]["status"] == "invalid_schema"
    assert context["training_day"]["readiness"]["value"] is None
    assert context["sleep"]["status"] == context["hrv"]["status"] == "available"
    assert context["snapshot_at"] == "2026-10-19T08:00:00.000Z"
