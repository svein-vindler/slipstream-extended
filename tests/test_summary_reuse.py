"""Real R2Store behavior through a counted, synthetic S3 client."""
import json
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from pipeline import summary_export
from pipeline.r2_store import R2BudgetError, R2Store
from scripts.benchmark_summary_reuse import SummaryClient


@pytest.fixture
def source(tmp_path):
    (tmp_path / "activities.csv").write_text("Activity ID,Activity Name\ngarmin-1,Synthetic run\n", encoding="utf-8")
    (tmp_path / "health_daily.csv").write_text("Date,Source\n2026-09-30,garmin\n", encoding="utf-8")
    return str(tmp_path)


def store(client, **kwargs):
    return R2Store(client=client, bucket="synthetic-bucket", **kwargs)


def reset(client):
    client.counts = dict.fromkeys(client.counts, 0)


def test_repeat_preserves_manifest_time_and_skips_all_writes_and_inventory(source):
    client = SummaryClient()
    first = summary_export.run(source, store=store(client))
    before = {key: value["Body"] for key, value in client.objects.items()}
    reset(client)
    second = summary_export.run(source, store=store(client))
    assert second == first
    assert before == {key: value["Body"] for key, value in client.objects.items()}
    assert client.counts == {"get": 1, "head": 5, "list": 0, "put": 0, "upload_bytes": 0}


def test_changed_source_replaces_only_its_current_snapshot_and_manifest(source):
    client = SummaryClient()
    before = summary_export.run(source, store=store(client))
    health = client.objects["summary/health_daily.csv"].copy()
    with open(source + "/activities.csv", "a", encoding="utf-8") as handle:
        handle.write("garmin-2,Changed synthetic run\n")
    reset(client)
    after = summary_export.run(source, store=store(client))
    assert client.counts["put"] == 3
    assert client.objects["summary/health_daily.csv"] == health
    assert before["files"][0]["sha256"] != after["files"][0]["sha256"]


def test_new_day_retains_prior_snapshot_and_writes_only_two_snapshots_and_manifest(source, monkeypatch):
    client = SummaryClient()
    original = summary_export.build_summary_exports

    def at(day):
        monkeypatch.setattr(summary_export, "build_summary_exports", lambda directory: original(
            directory, generated_at=datetime(2026, 9, day, tzinfo=timezone.utc)))

    at(29)
    summary_export.run(source, store=store(client))
    old_snapshot = {key: value.copy() for key, value in client.objects.items() if "/snapshots/" in key}
    at(30)
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 3
    assert all(client.objects[key] == value for key, value in old_snapshot.items())


@pytest.mark.parametrize("missing", ["summary/activities.csv", "snapshot", "summary/manifest.json"])
def test_missing_object_is_repaired_even_when_manifest_claims_unchanged(source, missing):
    client = SummaryClient()
    manifest = summary_export.run(source, store=store(client))
    key = manifest["files"][0]["snapshot_key"] if missing == "snapshot" else missing
    del client.objects[key]
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 1
    assert key in client.objects


def test_failed_partial_export_keeps_manifest_and_retry_skips_completed_writes(source, monkeypatch):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    old_manifest = client.objects["summary/manifest.json"].copy()
    with open(source + "/activities.csv", "a", encoding="utf-8") as handle:
        handle.write("garmin-2,Changed synthetic run\n")
    original = client.put_object
    calls = []

    def fail_second(**kwargs):
        calls.append(kwargs["Key"])
        if len(calls) == 2:
            raise RuntimeError("synthetic interruption")
        original(**kwargs)

    monkeypatch.setattr(client, "put_object", fail_second)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        summary_export.run(source, store=store(client))
    assert client.objects["summary/manifest.json"] == old_manifest
    monkeypatch.setattr(client, "put_object", original)
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 2


@pytest.mark.parametrize("code", ["AccessDenied", "SlowDown", "NoSuchBucket"])
@pytest.mark.parametrize("operation", ["get_object", "head_object"])
def test_storage_failures_are_not_treated_as_missing_and_do_not_write(source, monkeypatch, code, operation):
    client = SummaryClient()

    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code}}, operation)

    monkeypatch.setattr(client, operation, fail)
    with pytest.raises(ClientError):
        summary_export.run(source, store=store(client))
    assert client.counts["put"] == 0


@pytest.mark.parametrize("field,value", [
    ("ContentType", "text/plain"), ("ContentEncoding", None),
    ("ContentLength", 999), ("ETag", '"multipart-2"'), ("ETag", '"unknown"'),
    ("ETag", None),
    ("ServerSideEncryption", "unknown"),
])
def test_metadata_and_unknown_revisions_write_conservatively(source, monkeypatch, field, value):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    original = client.head_object

    def different(**kwargs):
        previous = original(**kwargs)
        if kwargs["Key"] == "summary/activities.csv":
            previous[field] = value
        return previous

    monkeypatch.setattr(client, "head_object", different)
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 1


def test_changed_write_still_obeys_budget_but_unchanged_does_not_consume_it(source):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    limited = store(client, max_writes_per_run=1)
    summary_export.run(source, store=limited)
    assert limited._writes == 0 and limited._initial_inventory is None
    with open(source + "/activities.csv", "a", encoding="utf-8") as handle:
        handle.write("garmin-2,Changed synthetic run\n")
    with pytest.raises(R2BudgetError):
        summary_export.run(source, store=limited)
    assert limited._writes == 1


@pytest.mark.parametrize("content", [b"bad json", b"[]", b'{"schema_version":1}'])
def test_invalid_manifest_is_rebuilt_after_valid_files(source, content):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    client.objects["summary/manifest.json"]["Body"] = content
    reset(client)
    result = summary_export.run(source, store=store(client))
    assert client.counts["put"] == 1
    assert json.loads(client.objects["summary/manifest.json"]["Body"]) == result
