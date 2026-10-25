"""Complete profile inventories belong to one actual bulk invocation."""
import gzip
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from time import perf_counter

import pytest
from botocore.exceptions import ClientError
from test_coach_engine_transition import add_new, legacy_store

from pipeline import coach_backfill
from pipeline.r2_store import R2BudgetError, R2Store
from scripts.build_coach_versions_contract import MemoryS3

PROFILE = "coach/profiles/v1/2026-01-01/test.json"


class ObservedS3(MemoryS3):
    """Existing synthetic S3 boundary, with fixture-compatible opaque revisions."""
    def __init__(self, objects=None, page_size=1000):
        super().__init__()
        if objects is not None:
            self.objects = objects.copy()
        self.page_size = page_size
        self.trace = []
        self.after_profiles = None
        self.after_profile_get = None

    def get_object(self, **kwargs):
        result = super().get_object(**kwargs)
        key = kwargs["Key"]
        self.trace.append({"op": "GET", "key": key, "bytes": len(self.objects[key])})
        if key.startswith(coach_backfill.PROFILE_PREFIX) and self.after_profile_get:
            self.after_profile_get()
        return result

    def put_object(self, **kwargs):
        result = super().put_object(**kwargs)
        result["ETag"] = hashlib.sha256(kwargs["Body"]).hexdigest()
        self.trace.append({"op": "PUT", "key": kwargs["Key"], "bytes": len(kwargs["Body"])})
        return result

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, *, Bucket, Prefix=""):
                with client.lock:
                    entries = [{"Key": key, "Size": len(body), "ETag": hashlib.sha256(body).hexdigest()}
                               for key, body in client.objects.items() if key.startswith(Prefix)]
                for start in range(0, max(1, len(entries)), client.page_size):
                    page = {"Contents": entries[start:start + client.page_size]}
                    client.trace.append({"op": "LIST", "prefix": Prefix, "objects": page["Contents"]})
                    yield page
                if Prefix == coach_backfill.PROFILE_PREFIX and client.after_profiles:
                    client.after_profiles()

        return Paginator()


def selected(client, identifier="1"):
    key = f"activities/2026/{identifier}/coach-input/v1/latest-ready.json"
    pointer = json.loads(client.objects[key])
    body = client.objects[pointer["analysis_key"]]
    return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)


def measured(client, label, record_property, generated=1):
    before = client.objects.copy()
    client.trace.clear()
    root = R2Store(client=client, bucket="synthetic-bucket")
    stage = root.new_stage()
    started = perf_counter()
    result = coach_backfill.run(store=stage, max_activities=3)
    elapsed = (perf_counter() - started) * 1000
    counts = {op: sum(e["op"] == op for e in client.trace) for op in ("GET", "HEAD", "LIST", "PUT")}
    profiles = [e for e in client.trace if e["op"] == "LIST" and e["prefix"] == coach_backfill.PROFILE_PREFIX]
    record_property(label, json.dumps({"result": result, "trace": client.trace, "counts": counts,
        "get_bytes": sum(e["bytes"] for e in client.trace if e["op"] == "GET"),
        "put_bytes": sum(e["bytes"] for e in client.trace if e["op"] == "PUT"), "local_ms": elapsed,
        "object_digests": {key: hashlib.sha256(body).hexdigest() for key, body in client.objects.items()}}, sort_keys=True))
    assert result["generated_this_run"] == generated
    assert result["remaining_activities"] == 0
    assert all(client.objects[key] == value for key, value in before.items() if "/canonical/" in key)
    assert root._bucket_budget is stage._bucket_budget
    assert root._bucket_budget.writes == counts["PUT"]
    assert sum(e["op"] == "LIST" and e["prefix"] == "" for e in client.trace) == int(bool(counts["PUT"]))
    assert len(profiles) == 1
    return result


@pytest.fixture
def frozen_clock(monkeypatch):
    class Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(coach_backfill, "datetime", Fixed)


def test_bulk_noop_lists_profiles_once_and_keeps_all_stored_bytes(monkeypatch):
    store = legacy_store()
    before = store.objects.copy()
    prefixes = []
    original = store.list_keys

    def observed(prefix=""):
        prefixes.append(prefix)
        return original(prefix)

    monkeypatch.setattr(store, "list_keys", observed)
    result = coach_backfill.run(store=store, max_activities=3)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert store.objects == before
    assert store.counts["put"] == 0
    assert prefixes.count(coach_backfill.PROFILE_PREFIX) == 1


@pytest.mark.parametrize("group", ["noop", "new_changed", "invalidations", "99_to_100"])
def test_actual_bulk_workload_groups(group, frozen_clock, record_property):
    if group == "noop":
        client = ObservedS3(legacy_store().objects)
        original = client.objects.copy()
        for label in ("ordinary_noop_first", "ordinary_noop_repeat_fresh_root"):
            measured(client, label, record_property, generated=0)
        assert client.objects == original
    elif group == "new_changed":
        client = ObservedS3(legacy_store().objects)
        add_new(client)
        measured(client, "genuinely_new_activity4", record_property)
        old = selected(client, "4")["analysis_id"]
        client.objects["activities/2026/4/activity.endurance.v1.json"] = b'{"available":true,"summary":{"distance_m":6500}}'
        measured(client, "genuinely_changed_activity4", record_property)
        assert selected(client, "4")["analysis_id"] != old
    elif group == "invalidations":
        initial = ObservedS3()
        first = measured(initial, "prepare_current_single_activity", record_property)
        for change in ("profile", "context", "source"):
            client = ObservedS3(initial.objects)
            if change == "profile":
                value = json.loads(client.objects[PROFILE])
                value["zones"][0]["max_bpm"] = 160
                client.objects[PROFILE] = json.dumps(value).encode()
            elif change == "context":
                client.objects["activities/2026/1/context/v1/2026-09-30T12-00-00.json"] = b'{"context_id":"synthetic-context-new","note":"synthetic changed context"}'
            else:
                client.objects["activities/2026/1/activity.endurance.v1.json"] = b'{"available":true,"summary":{"distance_m":6000}}'
                client.objects["activities/2026/1/source-manifest.v1.json"] = b'{"schema_version":1,"synthetic_revision":"changed"}'
            result = measured(client, change + "_invalidation", record_property,
                              generated=0 if change == "context" else 1)
            assert result["source_fingerprint"] != first["source_fingerprint"]
            if change == "context":
                assert selected(client) == selected(initial)
    else:
        client = ObservedS3()
        profile = json.loads(client.objects[PROFILE])
        for i in range(98):
            client.objects[f"coach/profiles/v1/2027-01-01/future-{i:03}.json"] = json.dumps({
                **profile, "profile_id": f"synthetic-future-{i:03}", "effective_from": "2027-01-01"}).encode()
        assert measured(client, "99_profiles_invocation", record_property)["profile_count"] == 99
        client.objects["coach/profiles/v1/2026-09-01/new-effective.json"] = json.dumps({
            **profile, "profile_id": "synthetic-effective-new", "effective_from": "2026-09-01"}).encode()
        assert measured(client, "next_invocation_100_profiles", record_property)["profile_count"] == 100
        assert selected(client)["profile"]["profile_id"] == "synthetic-effective-new"


def test_complete_paginated_profiles_and_full_write_inventory():
    client = ObservedS3(page_size=1)
    profile = json.loads(client.objects[PROFILE])
    client.objects["coach/profiles/v1/2026-09-01/new.json"] = json.dumps({
        **profile, "profile_id": "paginated-new", "effective_from": "2026-09-01"}).encode()
    result = coach_backfill.run(store=client.store())
    pages = [e for e in client.trace if e["op"] == "LIST"]
    assert sum(e["prefix"] == coach_backfill.PROFILE_PREFIX for e in pages) == 2
    assert result["profile_count"] == 2 and selected(client)["profile"]["profile_id"] == "paginated-new"
    inventory = [item["Key"] for page in pages if page["prefix"] == "" for item in page["objects"]]
    assert set(inventory) >= {PROFILE, "summary/activities.csv", "activities/2026/1/activity.tcx"}


@pytest.mark.parametrize("body", [b"{broken-json", b"null", b"[]", b"0"])
def test_invalid_profile_body_key_remains_in_fingerprint_without_dropping_valid_profile(body):
    client = ObservedS3()
    first = coach_backfill.run(store=client.store())
    client.objects["coach/profiles/v1/2026-09-01/invalid.json"] = body
    second = coach_backfill.run(store=client.store())
    assert second["source_fingerprint"] != first["source_fingerprint"]
    assert second["profile_count"] == 1
    assert selected(client)["profile"]["profile_id"] == "synthetic-profile"


def test_append_between_profile_loading_and_fingerprint_waits_until_next_invocation():
    client = ObservedS3()
    profile = json.loads(client.objects[PROFILE])
    key = "coach/profiles/v1/2026-09-01/new-effective.json"
    def append():
        client.after_profile_get = None
        client.objects[key] = json.dumps({**profile, "profile_id": "next-profile", "effective_from": "2026-09-01"}).encode()
    client.after_profile_get = append
    first = coach_backfill.run(store=client.store())
    first_profile = selected(client)["profile"]["profile_id"]
    reference = coach_backfill.run(store=ObservedS3().store())
    second = coach_backfill.run(store=client.store())
    assert first["profile_count"] == 1 and first_profile == "synthetic-profile"
    assert first["source_fingerprint"] == reference["source_fingerprint"]
    assert second["profile_count"] == 2 and selected(client)["profile"]["profile_id"] == "next-profile"
    assert first["source_fingerprint"] != second["source_fingerprint"]


def test_concurrent_invocations_keep_distinct_profile_snapshots():
    clients = [ObservedS3(), ObservedS3()]
    barrier = threading.Barrier(2)
    for i, client in enumerate(clients):
        profile = json.loads(client.objects[PROFILE])
        profile["profile_id"] = f"isolated-{i}"
        client.objects[PROFILE] = json.dumps(profile).encode()
        client.after_profiles = lambda: barrier.wait(timeout=5)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda client: coach_backfill.run(store=client.store()), clients))
    assert [selected(client)["profile"]["profile_id"] for client in clients] == ["isolated-0", "isolated-1"]
    assert results[0]["source_fingerprint"] != results[1]["source_fingerprint"]
    assert all(sum(e["op"] == "LIST" and e["prefix"] == coach_backfill.PROFILE_PREFIX for e in c.trace) == 1
               for c in clients)


def test_missing_profile_after_listing_propagates_storage_failure_before_writes():
    client = ObservedS3()
    def remove():
        client.after_profiles = None
        del client.objects[PROFILE]
    client.after_profiles = remove
    with pytest.raises(ClientError):
        coach_backfill.run(store=client.store())
    assert not client.puts
    assert not any(e["op"] == "LIST" and e["prefix"] == "" for e in client.trace)


def test_deleted_profile_is_absent_in_the_next_invocation():
    client = ObservedS3()
    coach_backfill.run(store=client.store())
    before = {k:v for k,v in client.objects.items() if "/canonical/" in k}
    del client.objects[PROFILE]
    client.trace.clear()
    result = coach_backfill.run(store=client.store())
    assert result["status"] == "waiting_for_profile" and result["profile_count"] == 0
    assert all(client.objects[k] == v for k,v in before.items())
    assert sum(e["op"] == "LIST" and e["prefix"] == coach_backfill.PROFILE_PREFIX for e in client.trace) == 1


@pytest.mark.parametrize("limits", [
    {"max_write_bytes_per_run": 1},
    {"max_bucket_bytes": 1}, {"max_bucket_objects": 1},
])
def test_snapshot_does_not_bypass_existing_write_guards(limits):
    client = ObservedS3()
    before = client.objects.copy()
    with pytest.raises((R2BudgetError, ValueError)):
        coach_backfill.run(store=R2Store(client=client, bucket="synthetic-bucket", **limits))
    assert client.objects == before
    assert not client.puts


def test_one_write_allowance_remains_reserved_for_checkpoint_only():
    client = ObservedS3()
    root = R2Store(client=client, bucket="synthetic-bucket", max_writes_per_run=1)
    result = coach_backfill.run(store=root.new_stage())
    assert result["status"] == "active"
    assert result["generated_this_run"] == 0 and result["remaining_activities"] == 1
    assert client.puts == [coach_backfill.PLAN_KEY]
    assert root._bucket_budget.writes == 1
