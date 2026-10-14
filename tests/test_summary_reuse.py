"""Real R2Store behavior through a counted, synthetic S3 client."""
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from pipeline import summary_export
from pipeline.r2_store import R2BudgetError, R2ReadBoundaryError, R2Store
from pipeline.summary_publication import SummaryUnverified
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
    assert {k: client.counts[k] for k in ("get", "head", "list", "put", "upload_bytes")} == {"get": 7, "head": 0, "list": 0, "put": 0, "upload_bytes": 0}


def test_changed_source_seals_a_new_pair_and_changes_only_affected_current_copy(source):
    client = SummaryClient()
    before = summary_export.run(source, store=store(client))
    health = client.objects["summary/health_daily.csv"].copy()
    with open(source + "/activities.csv", "a", encoding="utf-8") as handle:
        handle.write("garmin-2,Changed synthetic run\n")
    reset(client)
    after = summary_export.run(source, store=store(client))
    assert client.counts["put"] == 5
    assert client.objects["summary/health_daily.csv"] == health
    assert before["files"][0]["sha256"] != after["files"][0]["sha256"]


def test_new_day_retains_prior_generation_and_writes_only_archive_and_commit(source, monkeypatch):
    client = SummaryClient()
    def at(day):
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, day, tzinfo=timezone.utc)
        monkeypatch.setattr(summary_export, "datetime", Clock)
    at(29)
    first = summary_export.run(source, store=store(client))
    old_snapshot = {key: value.copy() for key, value in client.objects.items() if "/generations/" in key}
    at(30)
    reset(client)
    second = summary_export.run(source, store=store(client))
    assert first["snapshot_date"] == "2026-09-29" and second["snapshot_date"] == "2026-09-30"
    assert client.counts["put"] == 4
    assert all(client.objects[key] == value for key, value in old_snapshot.items())


@pytest.mark.parametrize("missing", ["summary/activities.csv", "snapshot", "summary/manifest.json"])
def test_missing_object_is_repaired_even_when_manifest_claims_unchanged(source, missing):
    client = SummaryClient()
    manifest = summary_export.run(source, store=store(client))
    key = manifest["files"][0]["snapshot_key"] if missing == "snapshot" else missing
    del client.objects[key]
    reset(client)
    if missing == "summary/activities.csv":
        summary_export.run(source, store=store(client))
        assert client.counts["put"] == 1 and key in client.objects
    else:
        with pytest.raises(SummaryUnverified):
            summary_export.run(source, store=store(client))
        assert client.counts["put"] == 0 and key not in client.objects


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
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", fail_second)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        summary_export.run(source, store=store(client))
    assert client.objects["summary/manifest.json"] == old_manifest
    monkeypatch.setattr(client, "put_object", original)
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 4


@pytest.mark.parametrize("code", ["AccessDenied", "SlowDown", "NoSuchBucket"])
@pytest.mark.parametrize("operation", ["get_object", "list_objects_v2"])
def test_storage_failures_are_not_treated_as_missing_and_do_not_write(source, monkeypatch, code, operation):
    client = SummaryClient()

    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code}}, operation)

    monkeypatch.setattr(client, operation, fail)
    with pytest.raises(ClientError):
        summary_export.run(source, store=store(client))
    assert client.counts["put"] == 0


@pytest.mark.parametrize("field,value", [("ContentType", "text/plain"), ("ContentEncoding", None)])
def test_current_serving_metadata_is_repaired_conditionally(source, monkeypatch, field, value):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    client.objects["summary/activities.csv"][field] = value
    reset(client)
    summary_export.run(source, store=store(client))
    assert client.counts["put"] == 1
    assert client.objects["summary/activities.csv"]["ContentType"] == "text/csv; charset=utf-8"
    assert client.objects["summary/activities.csv"]["ContentEncoding"] == "gzip"


@pytest.mark.parametrize("revision", [None, 'W/"weak"', "unquoted"])
def test_missing_or_invalid_current_revision_cannot_authorize_metadata_replacement(source, monkeypatch, revision):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    original = client.get_object
    def different(**kwargs):
        result = original(**kwargs)
        if kwargs["Key"] == "summary/activities.csv":
            result.update(ETag=revision, ContentType="text/plain")
        return result
    monkeypatch.setattr(client, "get_object", different)
    reset(client)
    with pytest.raises(SummaryUnverified):
        summary_export.run(source, store=store(client))
    assert client.counts["put"] == 0


def test_inconsistent_get_body_length_cannot_authorize_mutation(source, monkeypatch):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    original = client.get_object
    def different(**kwargs):
        return {**original(**kwargs), "ContentLength": 999999}
    monkeypatch.setattr(client, "get_object", different)
    reset(client)
    with pytest.raises(R2ReadBoundaryError):
        summary_export.run(source, store=store(client))
    assert client.counts["put"] == 0


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
def test_invalid_manifest_is_not_rebuilt_or_treated_as_absence(source, content):
    client = SummaryClient()
    summary_export.run(source, store=store(client))
    client.objects["summary/manifest.json"]["Body"] = content
    reset(client)
    with pytest.raises(SummaryUnverified):
        summary_export.run(source, store=store(client))
    assert client.counts["put"] == 0
    assert client.objects["summary/manifest.json"]["Body"] == content
