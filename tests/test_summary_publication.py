"""Actual summary interfaces across a conditional synthetic S3 boundary."""
import hashlib
import json
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from pipeline import summary_export, summary_restore
from pipeline.r2_store import R2Store
from scripts.benchmark_summary_reuse import SummaryClient


class ConditionalClient(SummaryClient):
    def get_object(self, **kwargs):
        result = super().get_object(**kwargs)
        obj = self.objects[kwargs["Key"]]
        return {**result, "ETag": self._etag(obj["Body"]),
                "ContentLength": len(obj["Body"]), "ContentType": obj.get("ContentType"),
                "ContentEncoding": obj.get("ContentEncoding")}

    @staticmethod
    def _etag(raw):
        return '"' + hashlib.md5(raw, usedforsecurity=False).hexdigest() + '"'

    def put_object(self, **kwargs):
        old = self.objects.get(kwargs["Key"])
        if (kwargs.get("IfNoneMatch") == "*" and old is not None
                or kwargs.get("IfMatch") is not None and
                (old is None or kwargs["IfMatch"] != self._etag(old["Body"]))):
            self.counts["put"] += 1
            self.counts["upload_bytes"] += len(kwargs["Body"])
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        super().put_object(**kwargs)
        return {"ETag": self._etag(kwargs["Body"])}

    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
        self.counts["list"] += 1
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys[:MaxKeys]],
                "IsTruncated": len(keys) > MaxKeys}


@pytest.fixture(autouse=True)
def fixed_export_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 13, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(summary_export, "datetime", Clock)


def source(root):
    root.mkdir(exist_ok=True)
    (root / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic A\n")
    (root / "health_daily.csv").write_bytes(b"Date,Weight KG\n")


def test_explicit_first_export_commits_a_marked_complete_pair(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    manifest = summary_export.run(str(tmp_path), store=store)
    assert manifest.get("publication_version") == 2
    target = tmp_path / "new-runner"
    summary_restore.restore_summaries(str(target), store)
    assert (target / "activities.csv").read_bytes() == (tmp_path / "activities.csv").read_bytes()
    assert (target / "health_daily.csv").read_bytes() == b"Date,Weight KG\n"


def test_explicit_repeat_preserves_commit_and_attempts_no_writes(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    first = summary_export.run(str(tmp_path), store=store)
    old = {key: value["Body"] for key, value in client.objects.items()}
    before_puts = client.counts["put"]
    repeated = summary_export.run(str(tmp_path), store=store)
    assert repeated == first
    assert client.counts["put"] == before_puts
    assert old == {key: value["Body"] for key, value in client.objects.items()}


def test_changed_explicit_export_creates_new_version_and_preserves_old_bytes(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    first = summary_export.run(str(tmp_path), store=store)
    historical = {item["snapshot_key"]: store.get(item["snapshot_key"]) for item in first["files"]}
    changed = b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n"
    (tmp_path / "activities.csv").write_bytes(changed)
    second = summary_export.run(str(tmp_path), store=store)
    assert second["generation_id"] != first["generation_id"]
    summary_restore.restore_summaries(str(tmp_path / "new-runner"), store)
    assert (tmp_path / "new-runner/activities.csv").read_bytes() == changed
    assert all(store.get(key) == raw for key, raw in historical.items())


def test_cold_restore_uses_committed_generation_after_current_changed_before_commit(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    first = summary_export.run(str(tmp_path), store=store)
    before = (tmp_path / "activities.csv").read_bytes()
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
    original = client.put_object

    def interrupt(**kwargs):
        if kwargs["Key"] == "summary/manifest.json":
            raise RuntimeError("synthetic final commit interruption")
        return original(**kwargs)

    client.put_object = interrupt
    with pytest.raises(RuntimeError, match="synthetic final commit interruption"):
        summary_export.run(str(tmp_path), store=store)
    client.put_object = original
    restored = summary_restore.restore_summaries(str(tmp_path / "cold-runner"), store)
    assert (tmp_path / "cold-runner/activities.csv").read_bytes() == before
    assert restored["generated_at"] == first["generated_at"]


def seed_legacy(root, store):
    objects, manifest = summary_export.build_summary_exports(
        str(root), generated_at=datetime(2026, 10, 12, 23, 55, tzinfo=timezone.utc))
    for item in objects:
        store.put(item["key"], item["data"], item["content_type"], encoding=item["encoding"])
    return manifest


def test_existing_legacy_pair_remains_readable_without_writes(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    legacy = seed_legacy(tmp_path, store)
    puts = client.counts["put"]
    restored = summary_restore.restore_summaries(str(tmp_path / "cold-runner"), store)
    assert restored["generated_at"] == legacy["generated_at"]
    assert (tmp_path / "cold-runner/activities.csv").read_bytes() == (tmp_path / "activities.csv").read_bytes()
    assert client.counts["put"] == puts


def test_legacy_current_mismatch_recovers_only_the_exact_manifest_snapshot_pair(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    legacy = seed_legacy(tmp_path, store)
    store.put("summary/activities.csv", b"Activity ID,Activity Name\ngarmin-1,Uncommitted B\n", "text/csv")
    puts = client.counts["put"]
    restored = summary_restore.restore_summaries(str(tmp_path / "cold-runner"), store)
    assert restored["generated_at"] == legacy["generated_at"]
    assert (tmp_path / "cold-runner/activities.csv").read_bytes() == (tmp_path / "activities.csv").read_bytes()
    assert client.counts["put"] == puts


def test_legacy_old_day_is_sealed_before_new_day_commit_can_fail(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    legacy = seed_legacy(tmp_path, store)
    old_manifest_bytes = store.get("summary/manifest.json")
    old_pair = {item["name"]: store.get(item["current_key"]) for item in legacy["files"]}
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
    original = client.put_object

    def interrupt_new_day(**kwargs):
        if kwargs["Key"] == "summary/manifest.json" and json.loads(kwargs["Body"])["snapshot_date"] != legacy["snapshot_date"]:
            raise RuntimeError("synthetic new commit interrupted")
        return original(**kwargs)

    client.put_object = interrupt_new_day
    with pytest.raises(RuntimeError, match="synthetic new commit interrupted"):
        summary_export.run(str(tmp_path), store=store)
    client.put_object = original
    committed = json.loads(store.get("summary/manifest.json"))
    assert committed.get("publication_version") == 2
    assert committed["snapshot_date"] == legacy["snapshot_date"]
    assert committed["generated_at"] == legacy["generated_at"]
    assert all(store.get(item["snapshot_key"]) == old_pair[item["name"]] for item in committed["files"])
    assert any(value["Body"] == old_manifest_bytes for key, value in client.objects.items() if "/origins/" in key)
    summary_restore.restore_summaries(str(tmp_path / "cold-runner"), store)
    assert b"Synthetic A" in (tmp_path / "cold-runner/activities.csv").read_bytes()


def test_derived_A_cannot_publish_after_B_commits_before_publication(tmp_path):
    from pipeline.summary_publication import SummaryConflict
    original = tmp_path / "explicit-source"
    source(original)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(original), store=store)
    normal = tmp_path / "normal-runner"
    restored = summary_restore.restore_with_base(str(normal), store, flow_id="normal-flow")
    (normal / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Derived C from A\n")
    (original / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Committed B\n")
    committed_B = summary_export.run(str(original), store=store)
    puts = client.counts["put"]
    with pytest.raises(SummaryConflict):
        summary_export.publish_derived(str(normal), store=store, base=restored.base, flow_id="normal-flow")
    assert client.counts["put"] == puts
    assert json.loads(store.get("summary/manifest.json")) == committed_B
    summary_restore.restore_summaries(str(tmp_path / "check"), store)
    assert b"Committed B" in (tmp_path / "check/activities.csv").read_bytes()


def test_direct_normal_base_rejects_local_divergence_before_source(tmp_path):
    from pipeline.summary_publication import SummaryUnverified, prepare_normal_base
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    changed = b"Activity ID,Activity Name\ngarmin-1,Unverified local\n"
    (tmp_path / "activities.csv").write_bytes(changed)
    before = client.counts["put"]
    with pytest.raises(SummaryUnverified):
        prepare_normal_base(str(tmp_path), store=store, flow_id="normal")
    assert client.counts["put"] == before
    assert (tmp_path / "activities.csv").read_bytes() == changed


def test_explicit_initial_retry_completes_matching_stage_across_day_without_overwrite(tmp_path, monkeypatch):
    from pipeline.summary_publication import export_full
    source(tmp_path)
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket='synthetic')
    original = client.put_object
    def lost_ack(**kwargs):
        original(**kwargs)
        raise RuntimeError("synthetic lost first archive acknowledgement")
    monkeypatch.setattr(client, "put_object", lost_ack)
    with pytest.raises(RuntimeError, match="synthetic lost"):
        summary_export.run(str(tmp_path), store=factory())
    archived = {k: v["Body"] for k, v in client.objects.items()}
    assert len(archived) == 1
    monkeypatch.setattr(client, "put_object", original)
    manifest = export_full(str(tmp_path), store=factory(),
                           generated_at=datetime(2026, 10, 14, 0, 5, tzinfo=timezone.utc))
    assert manifest["snapshot_date"] == "2026-10-14"
    assert all(client.objects[k]["Body"] == raw for k, raw in archived.items())
    summary_restore.restore_summaries(str(tmp_path / "reader"), factory())
    assert (tmp_path / "reader/activities.csv").read_bytes() == (tmp_path / "activities.csv").read_bytes()


def test_duplicate_manifest_fields_are_unverified_and_leave_local_and_remote_untouched(tmp_path):
    from pipeline.summary_publication import SummaryUnverified
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(tmp_path), store=store)
    raw = client.objects["summary/manifest.json"]["Body"]
    client.objects["summary/manifest.json"]["Body"] = raw.replace(b'{', b'{"schema_version":99,', 1)
    before = client.counts["put"]
    target = tmp_path / "reader"
    source(target)
    old = (target / "activities.csv").read_bytes()
    with pytest.raises(SummaryUnverified):
        summary_restore.restore_summaries(str(target), store)
    assert (target / "activities.csv").read_bytes() == old
    assert client.counts["put"] == before


def test_same_day_return_to_plain_legacy_pair_reuses_its_original_encoded_bytes(tmp_path):
    source(tmp_path)
    client = ConditionalClient()
    store = R2Store(client=client, bucket="synthetic")
    originals = {name: (tmp_path / name).read_bytes() for name in ("activities.csv", "health_daily.csv")}
    objects, legacy = summary_export.build_summary_exports(str(tmp_path),
                     generated_at=datetime(2026, 10, 13, 8, tzinfo=timezone.utc))
    for entry in legacy["files"]:
        entry["compressed_bytes"] = len(originals[entry["name"]])
    for item in objects:
        data = json.dumps(legacy).encode() if item["key"] == "summary/manifest.json" else originals[item["key"].split("/")[-1]]
        store.put(item["key"], data, item["content_type"])
    first = summary_export.run(str(tmp_path), store=store)
    archived = {entry["snapshot_key"]: store.get(entry["snapshot_key"]) for entry in first["files"]}
    (tmp_path / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
    summary_export.run(str(tmp_path), store=store)
    (tmp_path / "activities.csv").write_bytes(originals["activities.csv"])
    returned = summary_export.run(str(tmp_path), store=store)
    assert returned["generation_id"] == first["generation_id"]
    assert all(store.get(key) == raw for key, raw in archived.items())
    summary_restore.restore_summaries(str(tmp_path / "reader"), store)
    assert (tmp_path / "reader/activities.csv").read_bytes() == originals["activities.csv"]
