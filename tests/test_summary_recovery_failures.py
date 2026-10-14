"""Committed summary recovery under interruption, conflicts and unverified input."""
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from test_summary_publication import ConditionalClient, seed_legacy, source

from pipeline import summary_export, summary_restore
from pipeline.r2_store import R2BudgetError, R2ReadBoundaryError, R2Store
from pipeline.summary_publication import SummaryConflict, SummaryUnverified, prepare_normal_base


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 13, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(summary_export, "datetime", Clock)


@pytest.mark.parametrize("baseline,cut", [(b, i) for b, count in [("empty", 6), ("marked", 6), ("legacy", 11)]
                                         for i in range(1, count + 1)])
@pytest.mark.parametrize("after", [False, True])
def test_every_publication_put_cut_keeps_a_verifiable_commit_and_retry(baseline, cut, after, tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket='synthetic')
    old_pair = {name: (tmp_path / name).read_bytes() for name in ("activities.csv", "health_daily.csv")}
    if baseline == "marked":
        summary_export.run(str(tmp_path), store=factory())
    elif baseline == "legacy":
        seed_legacy(tmp_path, R2Store(client=client, bucket="synthetic"))
    history = {k: v["Body"] for k, v in client.objects.items() if k != "summary/manifest.json"}
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
    (tmp_path / "health_daily.csv").write_bytes(b"Date,Weight KG\n2026-10-13,70\n")
    new_pair = {name: (tmp_path / name).read_bytes() for name in old_pair}
    original, calls = client.put_object, []
    def interrupt(**kwargs):
        calls.append(kwargs["Key"])
        if len(calls) == cut and not after:
            raise RuntimeError("synthetic PUT cut")
        result = original(**kwargs)
        if len(calls) == cut and after:
            raise RuntimeError("synthetic PUT cut")
        return result
    monkeypatch.setattr(client, "put_object", interrupt)
    with pytest.raises(RuntimeError, match="synthetic PUT cut"):
        summary_export.run(str(tmp_path), store=factory())
    assert len(calls) == cut
    monkeypatch.setattr(client, "put_object", original)
    if "summary/manifest.json" in client.objects:
        reader = tmp_path / "cut-reader"
        summary_restore.restore_summaries(str(reader), factory())
        recovered = {name: (reader / name).read_bytes() for name in old_pair}
        assert recovered == old_pair or recovered == new_pair
    summary_export.run(str(tmp_path), store=factory())
    reader = tmp_path / "retry-reader"
    summary_restore.restore_summaries(str(reader), factory())
    assert {name: (reader / name).read_bytes() for name in old_pair} == new_pair
    assert all(client.objects[k]["Body"] == raw for k, raw in history.items()
               if "/generations/" in k or "/snapshots/" in k)


@pytest.mark.parametrize("defect", ["none", "scope", "flow", "root", "hash", "snapshot"])
def test_normal_publication_never_turns_missing_or_misbound_proof_into_full_export(defect, tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    base = summary_restore.restore_with_base(str(tmp_path), store, flow_id="one").base
    proof = {"none": None, "scope": replace(base, storage_scope=(0, "other")),
             "flow": replace(base, flow_id="other"), "root": replace(base, local_root="elsewhere"),
             "hash": replace(base, dataset_hashes=("0"*64, "0"*64)),
             "snapshot": replace(base, manifest_sha256="0"*64)}[defect]
    old, puts = client.objects["summary/manifest.json"]["Body"], client.counts["put"]
    with pytest.raises(SummaryUnverified):
        summary_export.publish_derived(str(tmp_path), store=store, base=proof, flow_id="one")
    assert client.objects["summary/manifest.json"]["Body"] == old and client.counts["put"] == puts


def test_supplied_prefetch_proof_rejects_half_local_replacement(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    base = summary_restore.restore_with_base(str(tmp_path), store, flow_id="one").base
    (tmp_path / "health_daily.csv").write_bytes(b"Date,Weight KG\n2026-10-13,70\n")
    with pytest.raises(SummaryUnverified):
        prepare_normal_base(str(tmp_path), store=store, flow_id="one", base=base)


def test_half_restore_never_returns_a_publication_proof(tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    original = Path.replace
    def fail_health(path, target):
        if str(target).endswith("health_daily.csv"):
            raise OSError("synthetic local cut")
        return original(path, target)
    monkeypatch.setattr(Path, "replace", fail_health)
    with pytest.raises(OSError, match="synthetic local cut"):
        summary_restore.restore_with_base(str(tmp_path / "cold"), store, flow_id="one")
    assert not list((tmp_path / "cold").glob("*.tmp"))


def test_unknown_legacy_upgrade_ack_stops_before_new_compatibility_copies(tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    seed_legacy(tmp_path, R2Store(client=client, bucket="synthetic"))
    store = R2Store(client=client, bucket="synthetic")
    base = summary_restore.restore_with_base(str(tmp_path), store, flow_id="one").base
    before = client.objects["summary/activities.csv"]["Body"]
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,B\n")
    original = client.put_object
    def unknown_ack(**kwargs):
        result = original(**kwargs)
        return {} if kwargs["Key"] == "summary/manifest.json" else result
    monkeypatch.setattr(client, "put_object", unknown_ack)
    with pytest.raises(SummaryUnverified):
        summary_export.publish_derived(str(tmp_path), store=store, base=base, flow_id="one")
    assert client.objects["summary/activities.csv"]["Body"] == before
    assert json.loads(client.objects["summary/manifest.json"]["Body"])["publication_version"] == 2
    with pytest.raises(SummaryConflict):
        summary_export.publish_derived(str(tmp_path), store=store, base=base, flow_id="one")


@pytest.mark.parametrize("code", ["AccessDenied", "SlowDown", "NoSuchBucket", "409"])
def test_provider_errors_never_become_absence_or_conditional_retry(code, tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code}}, "GetObject")
    monkeypatch.setattr(client, "get_object", fail)
    with pytest.raises(ClientError):
        summary_export.run(str(tmp_path), store=store)
    assert client.counts["put"] == 0


@pytest.mark.parametrize("code", ["AccessDenied", "409", "SlowDown"])
def test_unknown_conditional_put_failures_propagate_and_consume_the_attempt(code, tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic", max_writes_per_run=1)
    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code}}, "PutObject")
    monkeypatch.setattr(client, "put_object", fail)
    with pytest.raises(ClientError):
        summary_export.run(str(tmp_path), store=store)
    assert store.remaining_write_attempts == 0
    with pytest.raises(R2BudgetError):
        summary_export.run(str(tmp_path), store=store)


@pytest.mark.parametrize("orphan", ["summary/activities.csv", "summary/snapshots/2026/10/13/activities.csv",
                                    "summary/unrecognized", "summary/generations/v2/2026/02/30/"+"0"*64+"/activities.csv"])
def test_orphan_or_unknown_initial_namespace_is_never_empty(orphan, tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    client.objects[orphan] = {"Body": b"synthetic", "ContentType": "text/csv"}
    with pytest.raises(SummaryUnverified):
        summary_export.run(str(tmp_path), store=R2Store(client=client, bucket="synthetic"))
    assert client.counts["put"] == 0


def test_truncated_initial_namespace_stops_without_inventory_or_writes(tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    monkeypatch.setattr(client, "list_objects_v2", lambda **kw: {"Contents": [], "IsTruncated": True})
    store = R2Store(client=client, bucket="synthetic")
    with pytest.raises(R2ReadBoundaryError):
        summary_export.run(str(tmp_path), store=store)
    assert client.counts["put"] == 0 and store._initial_inventory is None


@pytest.mark.parametrize("limit", ["writes", "bytes", "objects", "bucket_bytes"])
def test_guard_stop_preserves_last_committed_pair(limit, tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    def factory(**kw):
        return R2Store(client=client, bucket='synthetic', **kw)
    summary_export.run(str(tmp_path), store=factory())
    old = {name: (tmp_path / name).read_bytes() for name in ("activities.csv", "health_daily.csv")}
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,B\n")
    limits = {"writes": {"max_writes_per_run": 1}, "bytes": {"max_write_bytes_per_run": 1},
              "objects": {"max_bucket_objects": len(client.objects)},
              "bucket_bytes": {"max_bucket_bytes": sum(len(v["Body"]) for v in client.objects.values())}}
    with pytest.raises(R2BudgetError):
        summary_export.run(str(tmp_path), store=factory(**limits[limit]))
    reader = tmp_path / "reader"
    summary_restore.restore_summaries(str(reader), factory())
    assert {name: (reader / name).read_bytes() for name in old} == old



@pytest.mark.parametrize("initial", [False, True])
def test_conditional_root_race_preserves_the_winner_and_does_not_retry_blindly(initial, tmp_path, monkeypatch):
    source(tmp_path)
    other = tmp_path / "other"
    source(other)
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket='synthetic')
    if not initial:
        summary_export.run(str(tmp_path), store=factory())
        (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Candidate A\n")
        (other / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Winner B\n")
    original = client.put_object
    root_requests = []
    competing = False
    def race(**kwargs):
        nonlocal competing
        if kwargs["Key"] == "summary/manifest.json" and not competing:
            competing = True
            summary_export.run(str(other), store=factory())
            root_requests.append(kwargs)
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", race)
    with pytest.raises(SummaryConflict):
        summary_export.run(str(tmp_path), store=factory())
    assert len(root_requests) == 1
    target = tmp_path / "winner-reader"
    summary_restore.restore_summaries(str(target), factory())
    assert (target / "activities.csv").read_bytes() == (other / "activities.csv").read_bytes()


def test_initial_archive_create_collision_with_foreign_bytes_never_overwrites_or_commits(tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    original = client.put_object
    collided = []
    def race(**kwargs):
        if not collided and kwargs["Key"].endswith("/activities.csv"):
            collided.append(kwargs["Key"])
            client.objects[kwargs["Key"]] = {"Body": b"synthetic foreign bytes", "ContentType": "text/csv"}
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", race)
    with pytest.raises(SummaryUnverified):
        summary_export.run(str(tmp_path), store=R2Store(client=client, bucket="synthetic"))
    assert client.objects[collided[0]]["Body"] == b"synthetic foreign bytes"
    assert "summary/manifest.json" not in client.objects


def test_initial_archive_create_collision_with_equal_bytes_can_resume_create_only(tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    original = client.put_object
    collided = []
    def race(**kwargs):
        if not collided and kwargs["Key"].endswith("/activities.csv"):
            collided.append(kwargs["Key"])
            client.objects[kwargs["Key"]] = kwargs.copy()
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", race)
    summary_export.run(str(tmp_path), store=R2Store(client=client, bucket="synthetic"))
    assert len(collided) == 1
    summary_restore.restore_summaries(str(tmp_path / "reader"), R2Store(client=client, bucket="synthetic"))
    assert (tmp_path / "reader/activities.csv").read_bytes() == (tmp_path / "activities.csv").read_bytes()


@pytest.mark.parametrize("defect", ["body", "key", "day", "gid", "many"])
def test_initial_staging_must_match_the_exact_incoming_pair_and_bounded_shape(defect, tmp_path, monkeypatch):
    source(tmp_path)
    client = ConditionalClient()
    original = client.put_object
    def lost(**kwargs):
        original(**kwargs)
        raise TimeoutError("synthetic lost ack")
    monkeypatch.setattr(client, "put_object", lost)
    def factory():
        return R2Store(client=client, bucket='synthetic')
    with pytest.raises(TimeoutError):
        summary_export.run(str(tmp_path), store=factory())
    monkeypatch.setattr(client, "put_object", original)
    key = next(iter(client.objects))
    if defect == "body":
        client.objects[key]["Body"] = b"different"
    elif defect in {"key", "day", "gid"}:
        alternative = (key + ".unknown" if defect == "key" else key.replace("2026/10/13", "2026/02/30")
                       if defect == "day" else key.replace(key.split("/")[-2], "0" * 64))
        client.objects[alternative] = client.objects.pop(key)
    else:
        for index in range(8):
            client.objects[f"summary/unknown{index}"] = {"Body": b"synthetic", "ContentType": "text/csv"}
    puts = client.counts["put"]
    with pytest.raises((SummaryUnverified, R2ReadBoundaryError)):
        summary_export.run(str(tmp_path), store=factory())
    assert client.counts["put"] == puts


@pytest.mark.parametrize("field,value", [
    ("schema_version", True), ("schema_version", 2), ("publication_version", True),
    ("publication_version", 3), ("snapshot_date", "2026-02-30"), ("snapshot_date", None),
    ("generated_at", "2026-10-13T12:00:00"), ("generated_at", "unknown"),
    ("generation_id", "0"*64), ("files", []), ("files", [None, None]),
    ("files", [{"name": "activities.csv"}, {"name": "activities.csv"}]),
])
def test_unverified_marked_schema_or_provenance_never_replaces_local_files(field, value, tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    root = client.objects["summary/manifest.json"]
    manifest = json.loads(root["Body"])
    manifest[field] = value
    root["Body"] = json.dumps(manifest).encode()
    before = (tmp_path / "activities.csv").read_bytes()
    puts = client.counts["put"]
    with pytest.raises(SummaryUnverified):
        summary_restore.restore_summaries(str(tmp_path), store)
    assert (tmp_path / "activities.csv").read_bytes() == before
    assert client.counts["put"] == puts


@pytest.mark.parametrize("field,value", [("name", "unknown.csv"), ("sha256", "0"*64),
    ("current_key", "foreign/activities.csv"), ("snapshot_key", "foreign/activities.csv"),
    ("rows", True), ("rows", 999), ("uncompressed_bytes", -1), ("compressed_bytes", 999)])
def test_unverified_child_binding_or_counts_never_replaces_local_files(field, value, tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    root = client.objects["summary/manifest.json"]
    manifest = json.loads(root["Body"])
    manifest["files"][0][field] = value
    root["Body"] = json.dumps(manifest).encode()
    before = (tmp_path / "activities.csv").read_bytes()
    puts = client.counts["put"]
    with pytest.raises(SummaryUnverified):
        summary_restore.restore_summaries(str(tmp_path), store)
    assert (tmp_path / "activities.csv").read_bytes() == before
    assert client.counts["put"] == puts
