"""Synthetic HRV ingestion contracts through the actual historical caller."""

import gzip
import json
from datetime import date

import pytest

from pipeline import hrv_backfill
from pipeline.health_history_index import summarize_hrv_payload
from pipeline.health_sync import HealthNotReady, store_day
from pipeline.latest_night import run as run_night
from pipeline.r2_store import R2BudgetError
from pipeline.sources.garmin_policy import GarminReadOnlyViolation

DAY = "2020-01-02"


class MemoryStore:
    def __init__(self, objects=None):
        self.objects = {
            "summary/health_daily.csv": b"Date,HRV Last Night Average\n2020-01-02,42\n",
            **(objects or {}),
        }
        self.calls = []

    def get(self, key):
        self.calls.append(("GET", key))
        return self.objects[key]

    def list_keys(self, prefix):
        self.calls.append(("LIST", prefix))
        return {key for key in self.objects if key.startswith(prefix)}

    def put(self, key, data, content_type, **kwargs):
        self.calls.append(("PUT", key))
        self.objects[key] = data


class GarminReplies:
    def __init__(self, *replies):
        self.replies = iter(replies)
        self.calls = []

    def get_hrv_data(self, day):
        self.calls.append(day)
        result = next(self.replies)
        if isinstance(result, Exception):
            raise result
        return result


def test_unknown_hrv_schema_is_retryable_not_a_permanent_unavailable_object():
    store = MemoryStore()
    provider = GarminReplies({"unrecognized_synthetic_schema": True})
    report = hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert hrv_backfill.hrv_key(DAY) not in store.objects
    assert report["complete_days"] == 0 and report["remaining_days"] == 1
    assert report["failures"][DAY]["attempts"] == 1
    assert provider.calls == [DAY]
    assert json.loads(store.objects[hrv_backfill.PLAN_KEY])["status"] == "active"


def detailed():
    return {"hrvSummary": {"calendarDate": DAY, "lastNightAvg": 42},
            "hrvReadings": [{"readingTimeGMT": DAY + "T01:00:00Z", "hrvValue": 42}]}


@pytest.mark.parametrize("raw", [
    None, [], {}, {"hrvSummary": None}, {"hrvSummary": {}},
    {"hrvReadings": []}, {"hrvReadings": None}, {"hrvReadings": {}},
    {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": None},
    {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": [None]},
    {"hrvReadings": [{"hrvValue": 42}]},
    {"hrvReadings": [{"timestamp": "invalid", "hrvValue": 42}]},
    {"hrvReadings": [{"timestamp": 1, "hrvValue": float("nan")}]},
    {"hrvReadings": [{"timestamp": 1, "hrvValue": True}]},
    {"hrvReadings": detailed()["hrvReadings"] + [{"timestamp": 1}]},
    {"hrvSummary": {"calendarDate": "2019-01-02"}, "hrvReadings": []},
    {"calendarDate": "2019-01-02", **detailed()},
    {"hrvSummary": {"calendarDate": DAY, "lastNightAvg": "invalid"}, "hrvReadings": []},
], ids=["null", "list", "empty_object", "null_summary", "empty_summary", "undated_empty",
        "null_readings", "object_readings", "partial_empty", "null_item", "no_time",
        "invalid_time", "nan", "bool", "mixed_partial", "wrong_summary_date",
        "wrong_top_date", "invalid_summary"])
def test_invalid_or_partial_hrv_never_creates_a_success_checkpoint(raw):
    store = MemoryStore()
    report = hrv_backfill.run(max_days=1, store=store, garmin=GarminReplies(raw))
    assert hrv_backfill.hrv_key(DAY) not in store.objects
    assert report["complete_days"] == 0 and report["remaining_days"] == 1
    assert report["failures"][DAY]["attempts"] == 1


def test_verified_empty_is_stored_once_and_remains_a_passive_checkpoint():
    store = MemoryStore()
    provider = GarminReplies({"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []})
    first = hrv_backfill.run(max_days=1, store=store, garmin=provider)
    body = store.objects[hrv_backfill.hrv_key(DAY)]
    payload = json.loads(gzip.decompress(body))
    assert first["status"] == "complete" and payload["available"] is False
    assert payload["unavailable_reason"] == "garmin_returned_no_detailed_readings"
    assert summarize_hrv_payload(DAY, payload)["status"] == "no_data"
    hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert provider.calls == [DAY] and store.objects[hrv_backfill.hrv_key(DAY)] == body


@pytest.mark.parametrize("has_readings", [False, True])
def test_recognized_summary_only_keeps_real_data_without_claiming_detailed_readings(has_readings):
    raw = {"hrvSummary": {"calendarDate": DAY, "lastNightAvg": 42}}
    if has_readings:
        raw["hrvReadings"] = []
    store = MemoryStore()
    report = hrv_backfill.run(max_days=1, store=store, garmin=GarminReplies(raw))
    payload = json.loads(gzip.decompress(store.objects[hrv_backfill.hrv_key(DAY)]))
    row = summarize_hrv_payload(DAY, payload)
    assert report["status"] == "complete" and payload["reading_count"] == 0
    assert row["status"] == "available" and row["garmin"]["last_night_avg_ms"] == 42
    assert row["detailed_readings_available"] is False


def test_unknown_then_valid_retries_the_same_date_and_clears_failure():
    store = MemoryStore()
    provider = GarminReplies({"unknown": True}, detailed())
    first = hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert first["failures"][DAY]["attempts"] == 1
    second = hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert second["status"] == "complete" and second["failures"] == {}
    body = store.objects[hrv_backfill.hrv_key(DAY)]
    hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert provider.calls == [DAY, DAY] and store.objects[hrv_backfill.hrv_key(DAY)] == body


def test_three_failures_preserve_the_existing_explicit_retry_budget():
    store = MemoryStore()
    provider = GarminReplies(*([{"unknown": True}] * 3), detailed())
    for _ in range(3):
        report = hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert report["status"] == "complete_with_blocked"
    assert report["blocked_after_three_failures"] == 1
    hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert provider.calls == [DAY] * 3 and hrv_backfill.hrv_key(DAY) not in store.objects
    retried = hrv_backfill.run(max_days=1, retry_failures=True, store=store, garmin=provider)
    assert retried["status"] == "complete" and provider.calls == [DAY] * 4


@pytest.mark.parametrize("raw,status", [({"unknown": True}, "invalid_response"),
                                       ({"calendarDate": "2019-01-02", **detailed()}, "wrong_date"),
                                       ({"hrvReadings": detailed()["hrvReadings"] + [None]}, "invalid_response")])
def test_recent_caller_rejects_unverified_hrv_and_preserves_good_canonical_bytes(raw, status):
    store = MemoryStore()
    store_day(store, "hrv", DAY, detailed())
    body = store.objects[hrv_backfill.hrv_key(DAY)]
    store.calls.clear()
    with pytest.raises(HealthNotReady) as caught:
        store_day(store, "hrv", DAY, raw)
    assert caught.value.status == status
    assert store.objects[hrv_backfill.hrv_key(DAY)] == body
    assert not any(method == "PUT" for method, _ in store.calls)


class ProviderError(RuntimeError):
    def __init__(self, status):
        super().__init__("synthetic provider failure")
        self.response = {"status": status}


@pytest.mark.parametrize("failure", [ProviderError(401), ProviderError(403), ProviderError(429),
                                     ProviderError(503), ConnectionError("synthetic transport"),
                                     TimeoutError("synthetic timeout"),
                                     GarminReadOnlyViolation("forbidden synthetic operation")])
def test_auth_transport_and_guard_failures_keep_checkpoint_and_do_not_create_absence(failure):
    before = json.dumps(hrv_backfill.new_plan(MemoryStore().objects["summary/health_daily.csv"])).encode()
    store = MemoryStore({hrv_backfill.PLAN_KEY: before})
    provider = GarminReplies(failure)
    with pytest.raises(RuntimeError, match="stopping this HRV batch") as caught:
        hrv_backfill.run(max_days=1, store=store, garmin=provider)
    assert caught.value.__cause__ is failure
    assert store.objects[hrv_backfill.PLAN_KEY] == before
    assert hrv_backfill.hrv_key(DAY) not in store.objects
    assert not any(method == "PUT" for method, _ in store.calls)


def test_storage_budget_stop_remains_a_stop_not_a_date_failure():
    class BudgetStore(MemoryStore):
        def put(self, *args, **kwargs):
            raise R2BudgetError("synthetic write budget")
    store = BudgetStore()
    with pytest.raises(R2BudgetError):
        hrv_backfill.run(max_days=1, store=store, garmin=GarminReplies(detailed()))
    assert hrv_backfill.PLAN_KEY not in store.objects


def test_existing_canonical_and_legacy_unavailable_markers_are_not_revalidated():
    for old in (b"synthetic good old canonical bytes", b"synthetic legacy unavailable marker"):
        store = MemoryStore({hrv_backfill.hrv_key(DAY): old})
        provider = GarminReplies()
        result = hrv_backfill.run(max_days=1, store=store, garmin=provider)
        assert result["complete_days"] == 1 and provider.calls == []
        assert store.objects[hrv_backfill.hrv_key(DAY)] == old


@pytest.mark.parametrize("raw,status,checked", [
    ({"unknown": True}, "invalid_response", False),
    ({"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []}, "garmin_not_ready", True),
    ({"hrvSummary": {"calendarDate": "2019-01-02"}, "hrvReadings": []}, "wrong_date", False),
    ({"hrvReadings": detailed()["hrvReadings"] + [None]}, "invalid_response", False),
])
def test_actual_targeted_night_distinguishes_confirmed_empty_from_unverified_source(
    monkeypatch, raw, status, checked,
):
    class Clock(date):
        @classmethod
        def today(cls):
            return cls(2020, 1, 2)
    monkeypatch.setattr("pipeline.freshness.date", Clock)
    class NightGarmin(GarminReplies):
        def get_sleep_data(self, day):
            return {"dailySleepDTO": {"calendarDate": day, "sleepTimeSeconds": 25200,
                    "sleepStartTimestampGMT": "2020-01-01T22:00:00Z",
                    "sleepEndTimestampGMT": "2020-01-02T05:00:00Z", "sleepWindowConfirmed": True},
                    "sleepLevels": [{"startGMT": "2020-01-01T22:00:00Z",
                                     "endGMT": "2020-01-02T05:00:00Z", "activityLevel": 1}]}
    check_key = f"refresh/checks/v1/health/hrv/{DAY}.json"
    old = b"synthetic earlier successful HRV source check"
    store = MemoryStore({hrv_backfill.hrv_key(DAY): b"synthetic good old HRV", check_key: old})
    report = run_night(run_id="9", wake_date=DAY, store=store, garmin=NightGarmin(raw))
    assert report["hrv_status"] == status and report["source_checked"] is checked
    assert report["status"] == "pending"
    assert store.objects[hrv_backfill.hrv_key(DAY)] == b"synthetic good old HRV"
    assert store.objects[check_key] == old
