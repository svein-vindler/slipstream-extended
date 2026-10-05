"""R2 history repair, scope/receipt recovery and paginated request regressions."""
import gzip
import json
from datetime import datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from pipeline.granular import gzip_json
from pipeline.health_history_index import SOURCE_PREFIXES, index_key, sync_stream
from pipeline.health_index_reconcile import INTERVAL, checkpoint_key, recent_month_dates, run
from pipeline.r2_store import R2BudgetError, R2Store
from scripts.benchmark_health_indexes import NOW, PaginatedHealthClient, measure, seed


def doc(client, key):
    raw = client.objects[key]["Body"]
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def source(stream, month="2026-10"):
    return f"{SOURCE_PREFIXES[stream]}{month[:4]}/{month[5:7]}/{month}-01.json"


@pytest.fixture
def client():
    result = PaginatedHealthClient()
    seed(result, ["2020-02", "2026-09", "2026-10"], days=1)
    return result


def refresh(client, **kwargs):
    return run(store=R2Store(client=client, bucket="synthetic-indexes"), now=kwargs.pop("now", NOW), **kwargs)


def test_recent_reuses_all_bytes_and_avoids_old_history_and_checkpoint_writes(client):
    before = {k: v["Body"] for k, v in client.objects.items()}
    report = refresh(client)
    assert report["status"] == "complete"
    assert all(r["scope"] == "recent" and r["months_considered"] == 2 for r in report["results"].values())
    assert report["r2_operations"] == {"get": 6, "head": 0, "list_pages": 4, "listed_objects": 4, "put": 0}
    assert client.prefixes == [f"{root}{month}/" for root in SOURCE_PREFIXES.values() for month in ("2026/09", "2026/10")]
    assert all("2020" not in key for key in client.reads)
    assert not client.writes and before == {k: v["Body"] for k, v in client.objects.items()}


@pytest.mark.parametrize("defect", ["missing", "corrupt", "list", "wrong_stream", "wrong_scope", "wrong_builder",
                                   "wrong_policy", "wrong_status", "naive", "future", "expired"])
def test_bad_full_receipt_forces_only_affected_stream_full(client, defect):
    key = checkpoint_key("hrv")
    receipt = doc(client, key)
    if defect == "missing":
        del client.objects[key]
    else:
        changes = {"wrong_stream": {"stream": "sleep"}, "wrong_scope": {"scope": "recent"},
                   "wrong_builder": {"builder_revision": -1}, "wrong_policy": {"policy_revision": -1},
                   "wrong_status": {"status": "partial"}, "naive": {"checked_at": "2026-10-05T00:00:00"},
                   "future": {"checked_at": (NOW + timedelta(seconds=1)).isoformat()},
                   "expired": {"checked_at": (NOW - INTERVAL).isoformat()}}
        receipt.update(changes.get(defect, {}))
        client.objects[key]["Body"] = b"{" if defect == "corrupt" else json.dumps([] if defect == "list" else receipt).encode()
    report = refresh(client)
    assert report["results"]["hrv"]["scope"] == "full"
    assert report["results"]["hrv"]["checkpoint_written"]
    assert report["results"]["sleep"]["scope"] == "recent"
    assert not any(k.startswith(SOURCE_PREFIXES["hrv"]) for k in client.reads)


@pytest.mark.parametrize("defect", ["missing", "corrupt_json", "corrupt_gzip", "row"])
def test_recent_repairs_missing_or_corrupt_index_from_only_affected_month(client, defect):
    key = index_key("sleep", "2026-10")
    if defect == "missing":
        del client.objects[key]
    elif defect == "row":
        value = doc(client, key)
        value["days"][0] = {"date": "2026-10-01"}
        client.objects[key]["Body"] = gzip_json(value)
    else:
        client.objects[key]["Body"] = b"{" if defect == "corrupt_json" else b"\x1f\x8b"
    before = client.objects[index_key("sleep", "2020-02")]["Body"]
    report = refresh(client)
    assert report["status"] == "complete"
    assert doc(client, key)["days"][0]["status"] == "available"
    assert [k for k in client.reads if k.startswith(SOURCE_PREFIXES["sleep"])] == [source("sleep")]
    assert client.writes == [key]
    assert client.objects[index_key("sleep", "2020-02")]["Body"] == before


def test_old_source_edit_waits_for_weekly_repair_and_reads_only_changed_day(client):
    key = source("hrv", "2020-02")
    value = doc(client, key)
    value["summary"]["lastNightAvg"] = 70
    client.objects[key]["Body"] = gzip_json(value)
    old_index = client.objects[index_key("hrv", "2020-02")]["Body"]
    refresh(client)
    assert client.objects[index_key("hrv", "2020-02")]["Body"] == old_index
    client.reset()
    report = refresh(client, now=NOW + INTERVAL)
    assert report["results"]["hrv"]["scope"] == "full"
    assert [k for k in client.reads if k.startswith(tuple(SOURCE_PREFIXES.values()))] == [key]
    assert doc(client, index_key("hrv", "2020-02"))["days"][0]["garmin"]["last_night_avg_ms"] == 70


def test_full_repair_clears_orphan_index_when_entire_source_month_is_removed(client):
    del client.objects[source("sleep", "2020-02")]
    report = refresh(client, now=NOW + INTERVAL)
    assert report["status"] == "complete"
    assert doc(client, index_key("sleep", "2020-02"))["days"] == []
    assert not any(k.startswith(tuple(SOURCE_PREFIXES.values())) for k in client.reads)
    assert source("hrv", "2020-02") in client.objects


def test_invalid_source_keeps_full_repair_due_and_retries_body_on_repeat(client):
    key = source("hrv", "2020-02")
    original = client.objects[key]["Body"]
    client.objects[key]["Body"] = b"\x1f\x8b"
    receipt = client.objects[checkpoint_key("hrv")]["Body"]
    report = refresh(client, now=NOW + INTERVAL)
    assert report["status"] == "partial" and report["results"]["hrv"]["invalid_days"] == 1
    assert client.objects[checkpoint_key("hrv")]["Body"] == receipt
    client.reset()
    again = refresh(client, now=NOW + INTERVAL)
    assert again["results"]["hrv"]["scope"] == "full" and key in client.reads
    client.objects[key]["Body"] = original
    repaired = refresh(client, now=NOW + INTERVAL)
    assert repaired["status"] == "complete"
    assert doc(client, index_key("hrv", "2020-02"))["days"][0]["status"] == "available"


def test_failed_index_write_never_advances_receipt_and_repeat_repairs(client, monkeypatch):
    key = index_key("hrv", "2020-02")
    del client.objects[key]
    receipt = client.objects[checkpoint_key("hrv")]["Body"]
    original = client.put_object
    def fail(**kwargs):
        if kwargs["Key"] == key:
            raise RuntimeError("synthetic interruption")
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", fail)
    with pytest.raises(RuntimeError):
        refresh(client, now=NOW + INTERVAL)
    assert client.objects[checkpoint_key("hrv")]["Body"] == receipt
    monkeypatch.setattr(client, "put_object", original)
    assert refresh(client, now=NOW + INTERVAL)["status"] == "complete"


def test_write_guard_applies_to_weekly_receipts(client):
    store = R2Store(client=client, bucket="synthetic-indexes", max_writes_per_run=1)
    with pytest.raises(R2BudgetError):
        run(store=store, now=NOW + INTERVAL)
    assert doc(client, checkpoint_key("hrv"))["checked_at"] == (NOW + INTERVAL).isoformat()
    assert doc(client, checkpoint_key("sleep"))["checked_at"] == NOW.isoformat()


def test_recent_source_edit_rebuilds_only_changed_day_and_preserves_other_month(client):
    key = source("sleep")
    value = doc(client, key)
    value["summary"]["sleep_seconds"] = 26000
    client.objects[key]["Body"] = gzip_json(value)
    historical = client.objects[index_key("sleep", "2020-02")]["Body"]
    assert refresh(client)["status"] == "complete"
    assert [k for k in client.reads if k.startswith(tuple(SOURCE_PREFIXES.values()))] == [key]
    assert client.writes == [index_key("sleep", "2026-10")]
    assert client.objects[index_key("sleep", "2020-02")]["Body"] == historical


def test_interrupted_receipt_write_retries_full_without_rebuilding_unchanged_indexes(client, monkeypatch):
    key = checkpoint_key("hrv")
    original = client.put_object
    receipt = client.objects[key]["Body"]
    def fail(**kwargs):
        if kwargs["Key"] == key:
            raise RuntimeError("synthetic receipt interruption")
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", fail)
    with pytest.raises(RuntimeError):
        refresh(client, now=NOW + INTERVAL)
    assert client.objects[key]["Body"] == receipt
    monkeypatch.setattr(client, "put_object", original)
    client.reset()
    report = refresh(client, now=NOW + INTERVAL)
    assert report["results"]["hrv"]["scope"] == "full"
    assert not any(k.startswith(tuple(SOURCE_PREFIXES.values())) for k in client.reads)


def test_full_list_failure_leaves_stream_due(client, monkeypatch):
    receipt = client.objects[checkpoint_key("hrv")]["Body"]
    original = client.get_paginator
    class BrokenPaginator:
        def paginate(self, **kwargs):
            raise ClientError({"Error": {"Code": "ServiceUnavailable"}}, "ListObjectsV2")
    monkeypatch.setattr(client, "get_paginator", lambda name: BrokenPaginator())
    with pytest.raises(ClientError):
        refresh(client, now=NOW + INTERVAL)
    assert client.objects[checkpoint_key("hrv")]["Body"] == receipt
    monkeypatch.setattr(client, "get_paginator", original)
    assert refresh(client, now=NOW + INTERVAL)["results"]["hrv"]["scope"] == "full"


def test_storage_permission_failure_is_not_treated_as_missing_receipt(client, monkeypatch):
    def denied(**kwargs):
        raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
    monkeypatch.setattr(client, "get_object", denied)
    with pytest.raises(ClientError):
        refresh(client)
    assert not client.writes


@pytest.mark.parametrize("now,expected", [
    ("2026-01-01T00:00:00+00:00", ["2025-12-01", "2026-01-01"]),
    ("2026-12-31T23:00:00+00:00", ["2026-11-01", "2026-12-01", "2027-01-01"]),
    ("2026-03-01T00:00:00+00:00", ["2026-02-01", "2026-03-01"]),
])
def test_calendar_month_scope_handles_new_year_and_local_date_ahead_of_utc(now, expected):
    assert recent_month_dates(datetime.fromisoformat(now)) == expected


def test_full_manual_build_is_immediate_and_does_not_change_scheduling_receipts(client):
    receipt = client.objects[checkpoint_key("hrv")]["Body"]
    report = sync_stream("hrv", store=R2Store(client=client, bucket="synthetic-indexes"))
    assert report["months_considered"] == 3
    assert client.objects[checkpoint_key("hrv")]["Body"] == receipt


def test_real_store_counts_list_pages_across_large_inventory_and_recent_scope():
    client = PaginatedHealthClient()
    months = [f"{year}-{month:02}" for year in range(2017, 2027) for month in range(1, 13)]
    seed(client, months)
    full, recent = measure(client, full=True), measure(client)
    assert full["r2_operations"]["list_pages"] == 10
    assert full["r2_operations"]["get"] == 240
    assert recent["r2_operations"]["list_pages"] == 4
    assert recent["r2_operations"]["get"] == 6
    assert recent["r2_operations"]["put"] == 0
