from datetime import date, datetime, timezone

import pytest

from pipeline.latest_activity import run
from pipeline.schema import Activity


@pytest.fixture(autouse=True)
def fixed_recent_window(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 30)
    monkeypatch.setattr("pipeline.freshness.date", FixedDate)


class FakeStore:
    def __init__(self):
        self.objects = {}

    def get(self, key):
        return self.objects[key]

    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}

    def put(self, key, data, content_type, *, encoding=None):
        self.objects[key] = data


class FakeGarmin:
    def __init__(self):
        self.checked = []

    def get_activity(self, activity_id):
        self.checked.append(activity_id)
        # Garmin's detail endpoint sometimes omits activityType.
        return {"activityId": activity_id}


def _activity(activity_id, hour, sport="run", raw_sport="running"):
    return Activity(
        source="garmin", source_id=activity_id,
        start=datetime(2026, 9, 29, hour, tzinfo=timezone.utc),
        local_start_date="2026-09-29", local_start_time=f"2026-09-29 {hour:02}:00:00",
        sport=sport, raw_sport=raw_sport, name=f"Workout {activity_id}",
    )


def _health_file(tmp_path):
    (tmp_path / "health_daily.csv").write_text("Date,Source\n", encoding="utf-8")


def test_latest_imports_only_newest_supported_activity_and_its_coach(monkeypatch, tmp_path):
    _health_file(tmp_path)
    store = FakeStore()
    garmin = FakeGarmin()
    fetched = [_activity("1", 9), _activity("2", 10),
               _activity("3", 11, "other", "other")]
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: fetched)

    def refresh(activity, *, existing_keys, force, **kwargs):
        assert force is False
        assert activity["activityId"] == "2"
        assert activity["activityType"] == {"typeKey": "running"}
        for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                     "activity.endurance.v1.json"):
            existing_keys.add(f"activities/2026/2/{name}")
        return {"status": "refreshed"}

    def coach(*, activity, **kwargs):
        assert activity["id"] == "2"
        store.put("activities/2026/2/coach-input/v1/canonical/result.json",
                  b"{}", "application/json")
        return {"processed_sources": {"2": "signature"},
                "skipped_this_run": [], "blocked_activities": []}

    monkeypatch.setattr("pipeline.latest_activity.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.latest_activity.run_coach_one", coach)
    result = run(run_id="123", data_dir=str(tmp_path),
                 store=store, garmin=garmin)

    assert garmin.checked == ["2"]
    assert result["activity_id"] == "garmin-2"
    assert result["status"] == "ready"
    assert result["files_ready"] is True
    assert result["coach_status"] == "ready"
    assert "summary/activities.csv" in store.objects
    assert "refresh/reports/123.json" in store.objects


def test_latest_reports_no_recent_supported_activity(monkeypatch, tmp_path):
    store = FakeStore()
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("3", 11, "other", "other")])
    result = run(run_id="124", data_dir=str(tmp_path),
                 store=store, garmin=FakeGarmin())
    assert result["status"] == "no_recent_activity"
    assert result["activity_id"] is None
    assert "summary/activities.csv" not in store.objects


def test_latest_does_not_mistake_yesterdays_complete_run_for_new_one(monkeypatch, tmp_path):
    (tmp_path / "activities.csv").write_text(
        "Activity ID,Activity Date,Activity Name,Activity Type\n"
        "garmin-2,2026-09-29 10:00:00,Old run,Run\n",
        encoding="utf-8",
    )
    store = FakeStore()
    for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                 "activity.endurance.v1.json"):
        store.put(f"activities/2026/2/{name}", b"old", "application/octet-stream")
    store.put("activities/2026/2/coach-input/v1/canonical/old.json",
              b"{}", "application/json")
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("2", 10)])
    garmin = FakeGarmin()
    result = run(run_id="128", data_dir=str(tmp_path),
                 store=store, garmin=garmin, new_activity_expected=True)
    assert result["status"] == "no_new_activity"
    assert result["activity_id"] == "garmin-2"
    assert garmin.checked == []
    assert result["summary_updated"] is False


def test_latest_expected_local_date_rejects_older_workout(monkeypatch, tmp_path):
    store = FakeStore()
    activity = _activity("2", 10)
    activity.local_start_date = "2026-09-28"
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [activity])
    garmin = FakeGarmin()
    result = run(run_id="129", data_dir=str(tmp_path),
                 expected_date="2026-09-29", store=store, garmin=garmin)
    assert result["status"] == "expected_activity_missing"
    assert result["activity_date"] is None
    assert garmin.checked == []


def test_latest_expected_local_date_accepts_same_night_across_utc_date(monkeypatch, tmp_path):
    _health_file(tmp_path)
    activity = _activity("2", 23)
    activity.local_start_date = "2026-09-30"
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [activity])

    def refresh(details, *, existing_keys, **kwargs):
        for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                     "activity.endurance.v1.json"):
            existing_keys.add(f"activities/2026/2/{name}")
        return {"status": "refreshed"}

    monkeypatch.setattr("pipeline.latest_activity.refresh_activity", refresh)
    store = FakeStore()

    def coach(**kwargs):
        store.put("activities/2026/2/coach-input/v1/canonical/new.json",
                  b"{}", "application/json")
        return {"processed_sources": {"2": "signature"},
                "skipped_this_run": [], "blocked_activities": []}

    monkeypatch.setattr("pipeline.latest_activity.run_coach_one", coach)
    result = run(run_id="130", data_dir=str(tmp_path),
                 expected_date="2026-09-30", store=store, garmin=FakeGarmin())
    assert result["status"] == "ready"
    assert result["activity_date"] == "2026-09-30"


def test_latest_reports_missing_files_without_claiming_coach(monkeypatch, tmp_path):
    _health_file(tmp_path)
    store = FakeStore()
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("2", 10)])
    monkeypatch.setattr("pipeline.latest_activity.refresh_activity",
                        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("no FIT")))
    monkeypatch.setattr("pipeline.latest_activity.run_coach_one",
                        lambda **kwargs: (_ for _ in ()).throw(AssertionError("coach must wait")))
    result = run(run_id="125", data_dir=str(tmp_path),
                 store=store, garmin=FakeGarmin())
    assert result["status"] == "files_unavailable"
    assert result["files_ready"] is False
    assert result["coach_status"] == "waiting_for_files"
    assert result["summary_updated"] is True


def test_latest_can_target_an_exact_recent_activity(monkeypatch, tmp_path):
    _health_file(tmp_path)
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("1", 9), _activity("2", 10)])
    monkeypatch.setattr("pipeline.latest_activity.refresh_activity",
                        lambda *args, **kwargs: {"status": "unsupported"})
    garmin = FakeGarmin()
    result = run(run_id="126", data_dir=str(tmp_path),
                 requested_activity_id="1", store=FakeStore(), garmin=garmin)
    assert garmin.checked == ["1"]
    assert result["activity_id"] == "garmin-1"
    assert result["status"] == "files_unavailable"


def test_latest_keeps_files_ready_when_coach_generation_fails(monkeypatch, tmp_path):
    _health_file(tmp_path)
    store = FakeStore()
    monkeypatch.setattr("pipeline.latest_activity.garmin_source.fetch",
                        lambda **kwargs: [_activity("2", 10)])

    def refresh(activity, *, existing_keys, **kwargs):
        for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                     "activity.endurance.v1.json"):
            existing_keys.add(f"activities/2026/2/{name}")
        return {"status": "refreshed"}

    monkeypatch.setattr("pipeline.latest_activity.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.latest_activity.run_coach_one",
                        lambda **kwargs: (_ for _ in ()).throw(ValueError("bad profile")))
    result = run(run_id="127", data_dir=str(tmp_path),
                 store=store, garmin=FakeGarmin())
    assert result["status"] == "coach_pending"
    assert result["files_ready"] is True
    assert result["coach_status"] == "error"
