"""Provider-to-S3-to-index regressions with real R2Store write guards."""
import gzip
import json
from datetime import date, datetime

import pytest
from botocore.exceptions import ClientError

from pipeline.granular import gzip_json
from pipeline.health_detail_backfill import run as backfill_run
from pipeline.health_history_index import sync_dates
from pipeline.health_sync import HealthNotReady, finalize_days, normalize_day, store_day
from pipeline.latest_night import run as night_run
from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.recent_health import run
from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

DAY = "2026-09-30"
TODAY = date.fromisoformat(DAY)


def document(client, key):
    data = client.objects[key]["Body"]
    return json.loads(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)


def canonical(stream, day=DAY):
    prefix = "health/sleep/v1" if stream == "sleep" else "health/hrv"
    return f"{prefix}/{day[:4]}/{day[5:7]}/{day}.json"


def check(stream, day=DAY):
    return f"refresh/checks/v1/health/{stream}/{day}.json"


@pytest.fixture
def environment():
    client, garmin = HealthClient(), SyntheticGarmin()

    def refresh(**kwargs):
        return run(store=R2Store(client=client, bucket="synthetic-health"),
                   garmin=garmin, today=TODAY, **kwargs)

    return client, garmin, refresh


def test_repeat_checks_overlap_but_skips_canonical_and_index_writes(environment):
    client, garmin, refresh = environment
    first = refresh()
    before = {key: value["Body"] for key, value in client.objects.items() if key.startswith("health/")}
    previous_check = document(client, check("sleep"))
    client.reset()
    garmin.calls = []
    second = refresh()
    assert first["status"] == second["status"] == "complete"
    assert len(garmin.calls) == 6  # Reconciliation remains a real source check.
    assert all(value["objects_written"] == 0 and value["objects_unchanged"] == 3
               and value["months_written"] == 0 for value in second["streams"].values())
    assert before == {key: value["Body"] for key, value in client.objects.items() if key.startswith("health/")}
    assert len(client.writes) == 9 and all(key.startswith("refresh/checks/") for key in client.writes)
    assert all(not key.startswith(("health/hrv/", "health/sleep/v1/")) for key in client.reads)
    assert client.prefixes == ["health/sleep/v1/2026/09/", "", "health/hrv/2026/09/"]
    current = document(client, check("sleep"))
    assert datetime.fromisoformat(current["checked_at"]) > datetime.fromisoformat(previous_check["checked_at"])
    assert current["source_sha256"] == previous_check["source_sha256"]


@pytest.mark.parametrize("stream", ["sleep", "hrv"])
def test_source_edit_reads_only_changed_day_and_keeps_other_cached_month_days(environment, stream):
    client, garmin, refresh = environment
    refresh()
    old_other = client.objects[canonical("hrv" if stream == "sleep" else "sleep")]["Body"]
    raw = garmin._fetch(stream, DAY)
    if stream == "sleep":
        raw["dailySleepDTO"]["sleepTimeSeconds"] = 26000
    else:
        raw["hrvReadings"][0]["hrvValue"] = 70
    garmin.responses[stream, DAY] = raw
    client.reset()
    result = refresh()
    canonical_reads = [key for key in client.reads if key.startswith(("health/sleep/v1/", "health/hrv/"))]
    assert canonical_reads == [canonical(stream)]
    assert result["streams"][stream]["objects_written"] == result["streams"][stream]["months_written"] == 1
    assert len(document(client, f"health/indexes/{stream}/v1/2026/09.json")["days"]) == 3
    assert client.objects[canonical("hrv" if stream == "sleep" else "sleep")]["Body"] == old_other


def test_overlap_crosses_new_year_without_reading_or_modifying_old_history(environment):
    client, garmin, _ = environment
    client.put_object(Key="health/hrv/2020/01/2020-01-01.json", Body=b"synthetic old day", ContentType="application/json")
    client.put_object(Key="backfill/sleep/v1/plan.json", Body=b"synthetic old plan", ContentType="application/json")
    client.reset()
    result = run(days=3, store=R2Store(client=client, bucket="synthetic-health"),
                 garmin=garmin, today=date(2026, 1, 1))
    assert result["status"] == "complete"
    assert client.objects["backfill/sleep/v1/plan.json"]["Body"] == b"synthetic old plan"
    assert "health/hrv/2020/01/2020-01-01.json" not in client.reads
    assert {prefix for prefix in client.prefixes if prefix} == {
        f"{root}/{month}/" for root in ("health/sleep/v1", "health/hrv") for month in ("2025/12", "2026/01")}
    assert document(client, canonical("sleep", "2026-01-01"))["sleep_start_garmin_local"] == "2026-01-01T00:00:00"
    assert document(client, check("hrv", "2025-12-31"))["date"] == "2025-12-31"


@pytest.mark.parametrize("stream,raw", [
    ("sleep", {}), ("sleep", {"dailySleepDTO": {"sleepTimeSeconds": 0}}),
    ("sleep", {"dailySleepDTO": {"calendarDate": "2026-09-29", "sleepTimeSeconds": 25200}}),
    ("hrv", {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []}),
    ("hrv", {"hrvReadings": [{"hrvValue": 50}]}),
    ("hrv", {"hrvReadings": [{"timestamp": "invalid", "hrvValue": 50}]}),
    ("hrv", {"hrvReadings": [{"timestamp": 1, "hrvValue": float("nan")}]}),
    ("hrv", {"hrvSummary": {"calendarDate": "2026-09-29"}, "hrvReadings": [{"timestamp": 1, "hrvValue": 50}]}),
    ("hrv", []),
])
def test_partial_or_wrong_date_preserves_previous_data_and_successful_check(environment, stream, raw):
    client, garmin, refresh = environment
    refresh(days=1)
    before = {key: client.objects[key].copy() for key in (canonical(stream), check(stream), f"refresh/checks/v1/night/{DAY}.json")}
    garmin.responses[stream, DAY] = raw
    client.reset()
    result = refresh(days=1)
    assert result["status"] == "partial" and result["streams"][stream]["days_not_ready"] == 1
    assert all(client.objects[key] == value for key, value in before.items())
    assert canonical(stream) not in client.writes and check(stream) not in client.writes


def test_provider_failure_keeps_only_failed_scope_checkpoint_unchanged(environment):
    client, garmin, refresh = environment
    refresh(days=1)
    before = client.objects[check("sleep")].copy()
    garmin.responses["sleep", DAY] = ValueError("synthetic provider failure")
    result = refresh(days=1)
    assert result["status"] == "partial" and result["streams"]["sleep"]["source_errors"] == 1
    assert client.objects[check("sleep")] == before


@pytest.mark.parametrize("defect", ["missing_window", "reversed_window", "missing_stages",
                                   "invalid_stage", "stage_outside_window", "unconfirmed"])
def test_incomplete_positive_sleep_preserves_complete_night(environment, defect):
    client, garmin, refresh = environment
    refresh(days=1)
    keys = (canonical("sleep"), check("sleep"), f"refresh/checks/v1/night/{DAY}.json")
    before = {key: client.objects[key].copy() for key in keys}
    raw = garmin.get_sleep_data(DAY)
    if defect == "missing_window":
        del raw["dailySleepDTO"]["sleepEndTimestampGMT"]
    elif defect == "reversed_window":
        raw["dailySleepDTO"]["sleepEndTimestampGMT"] = f"{DAY}T00:00:00Z"
    elif defect == "missing_stages":
        raw["sleepLevels"] = []
    elif defect == "invalid_stage":
        raw["sleepLevels"][0]["endGMT"] = "invalid"
    elif defect == "stage_outside_window":
        raw["sleepLevels"][0]["endGMT"] = f"{DAY}T08:00:00Z"
    else:
        raw["dailySleepDTO"]["sleepWindowConfirmed"] = False
    garmin.responses["sleep", DAY] = raw
    result = refresh(days=1)
    assert result["status"] == "partial" and result["streams"]["sleep"]["days_not_ready"] == 1
    assert all(client.objects[key] == value for key, value in before.items())


def test_service_failure_does_not_advance_checkpoints_after_partial_batch(environment):
    client, garmin, refresh = environment
    refresh()
    before = {key: value.copy() for key, value in client.objects.items() if key.startswith("refresh/checks/")}
    garmin.responses["sleep", "2026-09-29"] = TimeoutError("synthetic timeout")
    with pytest.raises(RuntimeError, match="Garmin service"):
        refresh()
    assert all(client.objects[key] == value for key, value in before.items())


@pytest.mark.parametrize("error", ["403", "SlowDown"])
def test_storage_failures_are_not_mistaken_for_unchanged_or_missing(environment, monkeypatch, error):
    client, _, refresh = environment
    refresh(days=1)
    before = client.objects[check("sleep")].copy()

    def failing_head(**kwargs):
        raise ClientError({"Error": {"Code": error}}, "HeadObject")

    monkeypatch.setattr(client, "head_object", failing_head)
    with pytest.raises(ClientError):
        refresh(days=1)
    assert client.objects[check("sleep")] == before


def test_missing_index_is_repaired_even_when_canonical_data_is_unchanged(environment):
    client, _, refresh = environment
    refresh()
    del client.objects["health/indexes/sleep/v1/2026/09.json"]
    client.reset()
    result = refresh()
    assert result["streams"]["sleep"]["objects_written"] == 0
    assert result["streams"]["sleep"]["months_written"] == 1
    assert len(document(client, "health/indexes/sleep/v1/2026/09.json")["days"]) == 3


def test_incomplete_index_repairs_only_missing_day_using_cached_valid_days(environment):
    client, _, refresh = environment
    refresh()
    key = "health/indexes/sleep/v1/2026/09.json"
    current = document(client, key)
    missing = current["days"].pop()
    client.put_object(Key=key, Body=gzip_json(current), ContentType="application/json", ContentEncoding="gzip")
    client.reset()
    refresh()
    assert [key for key in client.reads if key.startswith("health/sleep/v1/")] == [canonical("sleep", missing["date"])]
    assert len(document(client, key)["days"]) == 3


def test_index_failure_keeps_checkpoint_and_retry_repairs_without_canonical_upload(environment, monkeypatch):
    client, garmin, refresh = environment
    refresh()
    before = client.objects[check("sleep")].copy()
    raw = garmin.get_sleep_data(DAY)
    raw["dailySleepDTO"]["sleepTimeSeconds"] = 26000
    garmin.responses["sleep", DAY] = raw
    original = client.put_object

    def fail_index(**kwargs):
        if kwargs["Key"].startswith("health/indexes/"):
            raise RuntimeError("synthetic interrupted index write")
        return original(**kwargs)

    monkeypatch.setattr(client, "put_object", fail_index)
    with pytest.raises(RuntimeError, match="interrupted index"):
        refresh()
    assert client.objects[check("sleep")] == before
    monkeypatch.setattr(client, "put_object", original)
    client.reset()
    result = refresh()
    assert result["streams"]["sleep"]["objects_written"] == 0
    assert result["streams"]["sleep"]["months_written"] == 1
    assert document(client, check("sleep"))["source_sha256"] != json.loads(before["Body"])["source_sha256"]


def test_index_storage_error_is_not_caught_as_missing(environment, monkeypatch):
    client, _, refresh = environment
    refresh()
    before = client.objects[check("sleep")].copy()
    original = client.get_object

    def fail_index(**kwargs):
        if kwargs["Key"].startswith("health/indexes/"):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        return original(**kwargs)

    monkeypatch.setattr(client, "get_object", fail_index)
    with pytest.raises(ClientError):
        refresh()
    assert client.objects[check("sleep")] == before


@pytest.mark.parametrize("limit", [1, 2])
def test_day_and_checkpoint_writes_obey_existing_budget(limit):
    client, garmin = HealthClient(), SyntheticGarmin()
    store = R2Store(client=client, bucket="synthetic-health", max_writes_per_run=limit)
    record = store_day(store, "sleep", DAY, garmin.get_sleep_data(DAY))
    with pytest.raises(R2BudgetError):
        finalize_days(store, "sleep", [record])
    assert check("sleep") not in client.objects


@pytest.mark.parametrize("days,streams", [(0, ("sleep",)), (15, ("hrv",)), (True, ("sleep",)), (1.5, ("hrv",)), (3, ()), (3, ("invalid",))])
def test_invalid_window_or_stream_never_logs_into_provider(days, streams, monkeypatch):
    monkeypatch.setattr("pipeline.recent_health._login", lambda: pytest.fail("Invalid input must precede login"))
    with pytest.raises(ValueError):
        run(days=days, streams=streams)


def test_targeted_night_uses_same_reuse_and_preserves_local_clocks(environment, monkeypatch):
    client, garmin, _ = environment

    class FixedDate(date):
        @classmethod
        def today(cls):
            return TODAY

    monkeypatch.setattr("pipeline.freshness.date", FixedDate)
    raw = garmin.get_sleep_data(DAY)
    raw["dailySleepDTO"]["sleepStartTimestampLocal"] = "2026-09-29T23:30:00"
    garmin.responses["sleep", DAY] = raw
    for run_id in ("1", "2"):
        result = night_run(run_id=run_id, wake_date=DAY,
                           store=R2Store(client=client, bucket="synthetic-health"), garmin=garmin)
    assert result["sleep_reused"] is result["hrv_reused"] is True
    assert result["status"] == "stored"
    assert document(client, canonical("sleep"))["sleep_start_garmin_local"] == "2026-09-29T23:30:00"


def test_body_only_backfill_does_not_read_or_change_sleep_plan(environment):
    client, garmin, _ = environment
    client.put_object(Key="summary/health_daily.csv", Body=b"Date,Sleep Seconds,Weight KG\n2026-09-30,25200,\n", ContentType="text/csv")
    client.put_object(Key="backfill/sleep/v1/plan.json", Body=b"untouched synthetic sleep plan", ContentType="application/json")
    result = backfill_run(skip_sleep=True, store=R2Store(client=client, bucket="synthetic-health"), garmin=garmin)
    assert result["sleep"]["status"] == "skipped"
    assert client.objects["backfill/sleep/v1/plan.json"]["Body"] == b"untouched synthetic sleep plan"
    assert "backfill/sleep/v1/plan.json" not in client.reads and not garmin.calls


def test_empty_selected_index_scope_performs_no_inventory_reads(environment):
    client, _, _ = environment
    sync_dates(R2Store(client=client, bucket="synthetic-health"), "hrv", [])
    assert client.counts["get"] == client.counts["list"] == client.counts["put"] == 0


def test_invalid_date_is_never_substituted_with_utc():
    with pytest.raises(ValueError):
        normalize_day("sleep", "2026-02-30", {})
    with pytest.raises(HealthNotReady, match="wrong_date"):
        normalize_day("sleep", "2026-01-01", {"dailySleepDTO": {"calendarDate": "2025-12-31", "sleepTimeSeconds": 25200}})


def test_complete_night_checkpoint_uses_older_component_check_time(environment):
    client, _, refresh = environment
    result = refresh(days=1)
    night = document(client, f"refresh/checks/v1/night/{DAY}.json")
    assert result["night_checkpoints_written"] == 1
    assert night["scope"] == f"night/{DAY}" and night["source_checked"] is True
    assert night["checked_at"] == min(document(client, check(stream))["checked_at"] for stream in ("sleep", "hrv"))


def test_single_stream_cannot_advance_combined_night_check(environment):
    client, _, refresh = environment
    refresh(days=1)
    key = f"refresh/checks/v1/night/{DAY}.json"
    before = client.objects[key].copy()
    result = refresh(days=1, streams=("sleep",))
    assert result["night_checkpoints_written"] == 0 and client.objects[key] == before


def test_corrupt_index_row_is_repaired_without_rereading_valid_days(environment):
    client, _, refresh = environment
    refresh()
    key = "health/indexes/hrv/v1/2026/09.json"
    current = document(client, key)
    current["days"][-1] = {"date": DAY}
    client.put_object(Key=key, Body=gzip_json(current), ContentType="application/json", ContentEncoding="gzip")
    client.reset()
    refresh()
    assert [key for key in client.reads if key.startswith("health/hrv/")] == [canonical("hrv")]
    assert document(client, key)["days"][-1]["status"] == "available"


def test_removed_day_is_removed_from_only_its_index_without_body_reads(environment):
    client, _, refresh = environment
    refresh()
    del client.objects[canonical("hrv")]
    client.reset()
    result = sync_dates(R2Store(client=client, bucket="synthetic-health"), "hrv", [DAY])
    assert result["months_written"][0]["days"] == 2
    assert not any(key.startswith("health/hrv/") for key in client.reads)
    assert client.writes == ["health/indexes/hrv/v1/2026/09.json"]


def test_canonical_key_change_does_not_reuse_summary_from_old_alias(environment):
    client, _, refresh = environment
    refresh()
    key = canonical("hrv")
    client.objects[key + ".gz"] = client.objects.pop(key)
    sync_dates(R2Store(client=client, bucket="synthetic-health"), "hrv", [DAY])
    client.objects[key] = client.objects[key + ".gz"].copy()
    client.reset()
    sync_dates(R2Store(client=client, bucket="synthetic-health"), "hrv", [DAY])
    assert [key for key in client.reads if key.startswith("health/hrv/")] == [canonical("hrv")]
    assert len(document(client, "health/indexes/hrv/v1/2026/09.json")["days"]) == 3
