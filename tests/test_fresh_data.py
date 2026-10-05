"""Synthetic provider/R2 fixtures for bounded request pipelines."""
import gzip
import json
from datetime import date

import pytest
from test_latest_activity import FakeGarmin, FakeStore, _activity, _health_file

from pipeline.coach_backfill import run_one
from pipeline.freshness import activity_scope, recent_day
from pipeline.latest_activity import run as activity_run
from pipeline.latest_night import run
from pipeline.r2_store import R2BudgetError

DAY = "2026-09-30"
REQUEST_ID = "12345678-1234-4234-8234-123456789012"


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 30)
    monkeypatch.setattr("pipeline.freshness.date", FixedDate)


class NightGarmin:
    def __init__(self, *, sleep=None, hrv=None):
        self.calls = []
        self.sleep = sleep if sleep is not None else {
            "dailySleepDTO": {"calendarDate": DAY, "sleepTimeSeconds": 25200,
                              "sleepStartTimestampGMT": "2026-09-29T21:00:00Z",
                              "sleepEndTimestampGMT": "2026-09-30T04:00:00Z",
                              "sleepStartTimestampLocal": "2026-09-29T23:00:00",
                              "sleepEndTimestampLocal": "2026-09-30T06:00:00",
                              "sleepWindowConfirmed": True},
            "sleepLevels": [{"startGMT": "2026-09-29T21:00:00Z",
                             "endGMT": "2026-09-30T04:00:00Z", "activityLevel": 1}],
        }
        self.hrv = hrv if hrv is not None else {
            "hrvSummary": {"calendarDate": DAY, "lastNightAvg": 51},
            "hrvReadings": [{"readingTimeGMT": "2026-09-30T01:00:00Z", "hrvValue": 51}],
        }

    def get_sleep_data(self, day):
        self.calls.append(("sleep", day))
        if isinstance(self.sleep, Exception):
            raise self.sleep
        return self.sleep

    def get_hrv_data(self, day):
        self.calls.append(("hrv", day))
        if isinstance(self.hrv, Exception):
            raise self.hrv
        return self.hrv


def test_night_contacts_only_two_endpoints_and_preserves_local_clocks():
    store, garmin = FakeStore(), NightGarmin()
    result = run(run_id="9", wake_date=DAY, request_id=REQUEST_ID, store=store, garmin=garmin)
    assert garmin.calls == [("sleep", DAY), ("hrv", DAY)]
    assert result["status"] == "stored" and result["source_checked"] is True
    sleep = json.loads(gzip.decompress(store.get(f"health/sleep/v1/2026/09/{DAY}.json")))
    assert sleep["sleep_start_garmin_local"] == "2026-09-29T23:00:00"
    assert json.loads(store.get(f"refresh/requests/{REQUEST_ID}.json"))["run_id"] == 9
    assert "refresh/checks/v1/night/2026-09-30.json" in store.objects


@pytest.mark.parametrize("sleep", [{}, {"dailySleepDTO": {"calendarDate": DAY, "sleepTimeSeconds": 0}}])
def test_empty_night_is_a_successful_negative_check_without_overwriting_history(sleep):
    store = FakeStore()
    key = f"health/sleep/v1/2026/09/{DAY}.json"
    store.objects[key] = b"existing synthetic revision"
    result = run(run_id="10", wake_date=DAY, store=store, garmin=NightGarmin(sleep=sleep, hrv={}))
    assert result["sleep_status"] == result["hrv_status"] == "garmin_not_ready"
    assert result["source_checked"] is True
    assert store.objects[key] == b"existing synthetic revision"


@pytest.mark.parametrize("raw,status", [
    ({"dailySleepDTO": {"calendarDate": "2026-09-29", "sleepTimeSeconds": 25200}}, "wrong_date"),
    (ValueError("provider failure"), "import_error"),
])
def test_failed_partial_source_does_not_advance_checkpoint(raw, status):
    store = FakeStore()
    key = f"refresh/checks/v1/night/{DAY}.json"
    store.objects[key] = b"previous checkpoint"
    result = run(run_id="11", wake_date=DAY, store=store, garmin=NightGarmin(sleep=raw))
    assert result["sleep_status"] == status
    assert result["source_checked"] is False
    assert store.objects[key] == b"previous checkpoint"


def test_night_propagates_write_budget_failure():
    class BudgetStore(FakeStore):
        def put(self, *args, **kwargs):
            raise R2BudgetError("synthetic write limit")
    with pytest.raises(R2BudgetError):
        run(run_id="12", wake_date=DAY, store=BudgetStore(), garmin=NightGarmin())


@pytest.mark.parametrize("day", ["2026-02-30", "2026-09-01", "2026-10-02", "20260930"])
def test_request_window_rejects_invalid_old_or_future_dates(day):
    with pytest.raises(ValueError):
        recent_day(day)


def test_ambiguous_activities_are_reported_before_downloading(monkeypatch, tmp_path):
    garmin = FakeGarmin()
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("1", 9), _activity("2", 10)])
    result = activity_run(run_id="13", expected_date="2026-09-29",
                          store=FakeStore(), garmin=garmin, data_dir=str(tmp_path))
    assert result["status"] == "ambiguous_activity"
    assert result["candidate_ids"] == ["garmin-1", "garmin-2"]
    assert garmin.checked == []


def test_unknown_local_date_is_never_inferred_from_utc(monkeypatch, tmp_path):
    activity = _activity("1", 9)
    activity.local_start_date = None
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch", lambda **kwargs: [activity])
    result = activity_run(run_id="14", store=FakeStore(), garmin=FakeGarmin(), data_dir=str(tmp_path))
    assert result["status"] == "activity_date_unknown" and result["files_ready"] is False


def test_wrong_detail_id_cannot_be_published_as_selected_activity(monkeypatch, tmp_path):
    _health_file(tmp_path)
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch", lambda **kwargs: [_activity("1", 9)])
    garmin = FakeGarmin()
    garmin.get_activity = lambda activity_id: {"activityId": "2"}
    result = activity_run(run_id="15", store=FakeStore(), garmin=garmin, data_dir=str(tmp_path))
    assert result["status"] == "files_unavailable" and result["source_checked"] is False


def test_single_coach_scope_never_lists_history_or_changes_backfill_plan():
    class PrefixStore(FakeStore):
        def list_keys(self, prefix=""):
            assert prefix in {"activities/2026/1/", "coach/profiles/v1/"}
            return super().list_keys(prefix)
    store = PrefixStore()
    store.objects["backfill/coach-input/v1/plan.json"] = b"existing plan"
    result = run_one(activity={"id": "1", "date": DAY}, store=store)
    assert result["blocked_activities"][0]["reason"] == "no_effective_profile"
    assert store.objects["backfill/coach-input/v1/plan.json"] == b"existing plan"


def test_r2_only_repair_uses_canonical_id_without_a_summary_or_garmin(monkeypatch):
    store = FakeStore()
    store.objects["activities/2026/1/activity.v1.json"] = json.dumps({
        "activity": {"id": "1", "type": "running", "start_time_local": f"{DAY} 10:00:00"},
    }).encode()
    monkeypatch.setattr("pipeline.latest_activity.garmin_source._login",
                        lambda: pytest.fail("R2 repair cannot login to Garmin"))
    result = activity_run(run_id="16", expected_date=DAY, requested_activity_id="1",
                          repair_only=True, store=store)
    assert result["coach_status"] == "no_effective_profile"
    assert result["source_checked"] is False
    assert not any(key.startswith("refresh/checks/") for key in store.objects)
    assert activity_scope("1", DAY, True) == f"activity/{DAY}/1/new"


def test_single_coach_preserves_versions_and_updates_only_the_derived_pointer():
    import hashlib

    class RevisionStore(FakeStore):
        def list_object_revisions(self, prefix):
            assert prefix == "activities/2026/1/"
            return {key: hashlib.md5(value, usedforsecurity=False).hexdigest()
                    for key, value in self.objects.items() if key.startswith(prefix)}

    store = RevisionStore()
    prefix = "activities/2026/1"
    store.objects[f"{prefix}/activity.v1.json"] = json.dumps({
        "activity": {"id": "1"}, "source_fit_sha256": "synthetic-fit",
    }).encode()
    store.objects[f"{prefix}/activity.endurance.v1.json"] = json.dumps({
        "available": True, "activity": {"id": "1"}, "summary": {"distance_m": 5000},
    }).encode()
    store.objects[f"{prefix}/activity.tcx"] = b"synthetic tcx hash input"
    store.objects["coach/profiles/v1/2026-01-01/test.json"] = json.dumps({
        "profile_id": "synthetic-profile", "effective_from": "2026-01-01",
        "zones": [{"label": "test", "min_bpm": 100, "max_bpm": 150}],
    }).encode()
    first = run_one(activity={"id": "1", "date": DAY, "name": "Before", "moving_seconds": 1800}, store=store)
    pointer_key = f"{prefix}/coach-input/v1/latest-ready.json"
    pointer = json.loads(store.get(pointer_key))
    original_key = pointer["analysis_key"]
    original = store.get(original_key)
    assert pointer["source_revisions"][f"{prefix}/activity.tcx"]
    second = run_one(activity={"id": "1", "date": DAY, "name": "After"}, store=store)
    assert first["processed_sources"] != second["processed_sources"]
    assert store.get(original_key) == original
    assert json.loads(store.get(pointer_key))["analysis_key"] != original_key
    repaired = json.loads(gzip.decompress(store.get(json.loads(store.get(pointer_key))["analysis_key"])))
    assert repaired["summary"]["moving_seconds"] == 1800
    keys_before = set(store.objects)
    run_one(activity={"id": "1", "date": DAY, "name": "After"}, store=store)
    assert set(store.objects) == keys_before
