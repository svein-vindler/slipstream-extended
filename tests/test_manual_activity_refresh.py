import csv
from datetime import date
from pathlib import Path

from pipeline.granular import activity_type
from pipeline.manual_activity_refresh import _complete_activity_details, run, snapshot


def _summary(path: Path, rows: list[tuple[str, str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("Activity ID", "Activity Date", "Activity Name", "Activity Type"))
        for activity_id, day, kind in rows:
            writer.writerow((f"garmin-{activity_id}", f"{day} 10:00:00", "Test workout", kind))


class FakeStore:
    def __init__(self, keys=()):
        self.objects = {key: b"" for key in keys}

    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}

    def put(self, key, data, content_type, *, encoding=None):
        self.objects[key] = data


class FakeGarmin:
    def __init__(self):
        self.checked = []

    def get_activity(self, activity_id):
        self.checked.append(activity_id)
        return {
            "activityId": activity_id,
            "activityType": {"typeKey": "running"},
            "startTimeLocal": "2026-09-28 10:00:00",
        }


def test_on_demand_prioritizes_new_run_and_verifies_its_coach(monkeypatch, tmp_path):
    summary = tmp_path / "activities.csv"
    baseline = tmp_path / "baseline.json"
    _summary(summary, [("1", "2026-09-27", "Run")])
    snapshot(summary, baseline)
    _summary(summary, [
        ("2", "2026-09-28", "Run"),
        ("1", "2026-09-27", "Run"),
    ])
    store = FakeStore({
        "activities/2026/1/activity.fit",
        "activities/2026/1/activity.v1.json",
        "activities/2026/1/activity.tcx",
        "activities/2026/1/activity.endurance.v1.json",
    })
    garmin = FakeGarmin()

    def refresh(activity, *, existing_keys, **kwargs):
        if activity["activityId"] == "1":
            return {"status": "unchanged"}
        for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                     "activity.endurance.v1.json"):
            existing_keys.add(f"activities/2026/2/{name}")
        return {"status": "refreshed"}

    def coach(*, priority_activity_ids, **kwargs):
        assert priority_activity_ids == {"1", "2"}
        store.put(
            "activities/2026/2/coach-input/v1/canonical/new.json",
            b"{}", "application/json",
        )
        return {"processed_sources": {"2": "signature"},
                "blocked_activities": [], "skipped_this_run": []}

    monkeypatch.setattr("pipeline.manual_activity_refresh.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.manual_activity_refresh.run_coach_backfill", coach)
    result = run(
        baseline=baseline,
        summary=summary,
        run_id="12345",
        store=store,
        garmin=garmin,
        today=date(2026, 9, 28),
    )

    assert garmin.checked == ["2", "1"]
    assert result["new_activity_count"] == 1
    assert result["activities"][0]["files_ready"] is True
    assert result["activities"][0]["coach_status"] == "ready"
    assert "refresh/reports/12345.json" in store.objects


def test_on_demand_reports_no_new_workout_without_claiming_coach(monkeypatch, tmp_path):
    summary = tmp_path / "activities.csv"
    baseline = tmp_path / "baseline.json"
    _summary(summary, [("1", "2026-09-28", "Run")])
    snapshot(summary, baseline)
    store = FakeStore({
        "activities/2026/1/activity.fit",
        "activities/2026/1/activity.v1.json",
        "activities/2026/1/activity.tcx",
        "activities/2026/1/activity.endurance.v1.json",
    })
    garmin = FakeGarmin()
    monkeypatch.setattr(
        "pipeline.manual_activity_refresh.refresh_activity",
        lambda *args, **kwargs: {"status": "unchanged"},
    )
    monkeypatch.setattr(
        "pipeline.manual_activity_refresh.run_coach_backfill",
        lambda **kwargs: {"processed_sources": {}, "blocked_activities": [],
                          "skipped_this_run": []},
    )
    result = run(
        baseline=baseline,
        summary=summary,
        store=store,
        garmin=garmin,
        today=date(2026, 9, 28),
    )
    assert result["new_activity_count"] == 0
    assert garmin.checked == ["1"]
    assert result["activities"][0]["coach_status"] == "pending"


def test_snapshot_rejects_missing_activity_id(tmp_path):
    summary = tmp_path / "activities.csv"
    summary.write_text("Date,Name\n2026-09-28,Run\n", encoding="utf-8")
    try:
        snapshot(summary, tmp_path / "baseline.json")
    except ValueError as exc:
        assert "Activity ID" in str(exc)
    else:
        raise AssertionError("snapshot should reject invalid summaries")


def test_on_demand_uses_summary_type_when_detail_omits_it(monkeypatch, tmp_path):
    summary = tmp_path / "activities.csv"
    baseline = tmp_path / "baseline.json"
    _summary(summary, [("2", "2026-09-28", "Run")])
    baseline.write_text("[]", encoding="utf-8")
    store = FakeStore()

    class GarminWithoutSummaryFields:
        def get_activity(self, activity_id):
            return {"activityId": activity_id}

    def refresh(activity, *, existing_keys, **kwargs):
        assert activity["activityType"] == {"typeKey": "Run"}
        assert activity["startTimeLocal"].startswith("2026-09-28")
        for key in (
            "activity.fit", "activity.v1.json", "activity.tcx",
            "activity.endurance.v1.json",
        ):
            existing_keys.add(f"activities/2026/2/{key}")
        return {"status": "refreshed"}

    def coach(*, priority_activity_ids, **kwargs):
        assert priority_activity_ids == {"2"}
        store.put(
            "activities/2026/2/coach-input/v1/canonical/test.json",
            b"{}", "application/json",
        )
        return {"processed_sources": {"2": "signature"},
                "blocked_activities": [], "skipped_this_run": []}

    monkeypatch.setattr("pipeline.manual_activity_refresh.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.manual_activity_refresh.run_coach_backfill", coach)
    result = run(
        baseline=baseline,
        summary=summary,
        store=store,
        garmin=GarminWithoutSummaryFields(),
        today=date(2026, 9, 28),
    )

    assert result["activities"][0]["files_ready"] is True
    assert result["activities"][0]["coach_status"] == "ready"


def test_detail_type_fallback_preserves_explicit_supported_type():
    row = {
        "Activity ID": "garmin-123",
        "Activity Date": "2026-09-28 10:00:00",
        "Activity Type": "Run",
    }
    detail = {
        "activityId": 123,
        "activityType": {"typeKey": "cycling"},
        "startTimeLocal": "2026-09-28 10:05:00",
    }
    complete = _complete_activity_details(detail, row)
    assert activity_type(complete) == "cycling"
    assert complete["startTimeLocal"] == "2026-09-28 10:05:00"


def test_detail_type_fallback_recognizes_dto_and_unknown_detail_type():
    row = {
        "Activity ID": "garmin-123",
        "Activity Date": "2026-09-28 10:00:00",
        "Activity Type": "Run",
    }
    assert activity_type({"activityTypeDTO": {"typeKey": "running"}}) == "running"
    complete = _complete_activity_details(
        {"activityId": 123, "activityType": {"typeKey": "other"}}, row
    )
    assert activity_type(complete) == "run"
