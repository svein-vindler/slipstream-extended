import csv
import json
from datetime import date
from pathlib import Path

import pytest

from pipeline import coach_backfill
from pipeline.granular import activity_type
from pipeline.manual_activity_refresh import (
    _coach_metadata,
    _complete_activity_details,
    run,
    snapshot,
)
from scripts.benchmark_coach_reuse import CountingStore


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

    def get(self, key):
        return self.objects[key]

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
            store.objects["activities/2026/1/activity.v1.json"] = b'{"activity":{"id":"1","start_time_local":"2026-09-28 10:00:00"}}'
            return {"status": "unchanged"}
        for name in ("activity.fit", "activity.v1.json", "activity.tcx",
                     "activity.endurance.v1.json"):
            existing_keys.add(f"activities/2026/2/{name}")
        return {"status": "refreshed"}

    coached = []

    def coach(*, activity, **kwargs):
        coached.append(activity["id"])
        if activity["id"] == "1":
            return {"processed_sources": {}, "blocked_activities": [], "skipped_this_run": []}
        store.put(
            "activities/2026/2/coach-input/v1/canonical/new.json",
            b"{}", "application/json",
        )
        return {"processed_sources": {"2": "signature"},
                "blocked_activities": [], "skipped_this_run": []}

    monkeypatch.setattr("pipeline.manual_activity_refresh.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.manual_activity_refresh.run_coach_one", coach)
    result = run(
        baseline=baseline,
        summary=summary,
        run_id="12345",
        store=store,
        garmin=garmin,
        today=date(2026, 9, 28),
    )

    assert garmin.checked == ["2", "1"]
    assert coached == ["2", "1"]
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
    store.objects["activities/2026/1/activity.v1.json"] = b'{"activity":{"id":"1","start_time_local":"2026-09-28 10:00:00"}}'
    monkeypatch.setattr(
        "pipeline.manual_activity_refresh.refresh_activity",
        lambda *args, **kwargs: {"status": "unchanged"},
    )
    monkeypatch.setattr(
        "pipeline.manual_activity_refresh.run_coach_one",
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
        assert activity["startTimeGMT"].startswith("2026-09-28")
        for key in (
            "activity.fit", "activity.v1.json", "activity.tcx",
            "activity.endurance.v1.json",
        ):
            existing_keys.add(f"activities/2026/2/{key}")
        store.put("activities/2026/2/activity.v1.json", json.dumps({
            "activity": {"id": "2", "start_time_local": "2026-09-28 11:00:00"},
        }).encode(), "application/json")
        return {"status": "refreshed"}

    def coach(*, activity, **kwargs):
        assert activity["id"] == "2"
        assert activity["date"] == "2026-09-28"
        store.put(
            "activities/2026/2/coach-input/v1/canonical/test.json",
            b"{}", "application/json",
        )
        return {"processed_sources": {"2": "signature"},
                "blocked_activities": [], "skipped_this_run": []}

    monkeypatch.setattr("pipeline.manual_activity_refresh.refresh_activity", refresh)
    monkeypatch.setattr("pipeline.manual_activity_refresh.run_coach_one", coach)
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


@pytest.fixture
def incremental(tmp_path, monkeypatch):
    summary, baseline = tmp_path / "activities.csv", tmp_path / "baseline.json"
    _summary(summary, [("1", "2026-09-30", "Run")])
    snapshot(summary, baseline)

    class ObservedStore(CountingStore):
        def get(self, key):
            self.reads.append(key)
            return super().get(key)

    store = ObservedStore()
    store.reads = []
    store.objects["activities/2026/1/activity.fit"] = b"synthetic original fit"
    store.objects["backfill/coach-input/v1/plan.json"] = b"unchanged historical plan"
    garmin = FakeGarmin()
    detail = {"activityId": "1", "activityType": {"typeKey": "running"},
              "activityName": "Test workout", "startTimeLocal": "2026-09-30 10:00:00"}

    def get_activity(activity_id):
        garmin.checked.append(activity_id)
        return detail.copy()

    garmin.get_activity = get_activity
    calls = []
    original = coach_backfill.build_coach_input

    def build(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(coach_backfill, "build_coach_input", build)

    def unexpected_download(*args, **kwargs):
        pytest.fail("Unchanged complete activity must not download FIT/TCX again")

    monkeypatch.setattr("pipeline.activity_refresh.export_activity", unexpected_download)
    return {"baseline": baseline, "summary": summary, "store": store,
            "garmin": garmin, "today": date(2026, 9, 30)}, detail, calls


def test_general_refresh_reuses_analysis_without_downloads_or_historical_coach_work(incremental):
    arguments, _, builds = incremental
    first = run(**arguments)
    store = arguments["store"]
    store.reset_counts()
    store.reads = []
    second = run(**arguments)
    assert first["activities"][0]["coach_status"] == "ready"
    assert second["activities"][0]["file_status"] == "unchanged"
    assert second["activities"][0]["coach_status"] == "ready"
    assert second["activities"][0]["coach_reused"] is True
    assert len(builds) == 1 and store.counts["put"] == 0
    assert "activities/2026/1/activity.tcx" not in store.reads
    assert "summary/activities.csv" not in store.reads
    assert "backfill/coach-input/v1/plan.json" not in store.reads
    assert store.objects["backfill/coach-input/v1/plan.json"] == b"unchanged historical plan"
    assert arguments["garmin"].checked == ["1", "1"]  # Source observation still happens.


def test_changed_source_downloads_only_selected_activity_and_preserves_old_analysis(incremental, monkeypatch):
    arguments, detail, builds = incremental
    run(**arguments)
    store = arguments["store"]
    pointer = "activities/2026/1/coach-input/v1/latest-ready.json"
    old_key = json.loads(store.objects[pointer])["analysis_key"]
    old_analysis = store.objects[old_key]
    detail["activityName"] = "Edited synthetic name"
    downloads = []

    def export(activity, garmin, target, output, **kwargs):
        assert activity["activityId"] == "1" and kwargs["force"] is True
        downloads.append(activity["activityId"])
        target.put("activities/2026/1/activity.v1.json", json.dumps({
            "source_fit_sha256": "changed-synthetic-fit", "activity": {"id": "1"},
        }).encode(), "application/json")
        return {"files": []}

    monkeypatch.setattr("pipeline.activity_refresh.export_activity", export)
    result = run(**arguments)
    assert result["activities"][0]["file_status"] == "refreshed"
    assert result["activities"][0]["coach_status"] == "ready"
    assert result["activities"][0]["coach_reused"] is False
    assert downloads == ["1"] and len(builds) == 2
    assert json.loads(store.objects[pointer])["analysis_key"] != old_key
    assert store.objects[old_key] == old_analysis


@pytest.mark.parametrize("changed", ["profile", "context"])
def test_profile_or_context_edit_rebuilds_coach_without_source_downloads(incremental, changed):
    arguments, _, builds = incremental
    run(**arguments)
    store = arguments["store"]
    if changed == "profile":
        key = "coach/profiles/v1/2026-01-01/test.json"
        value = json.loads(store.objects[key])
        value["zones"][0]["max_bpm"] = 160
        store.objects[key] = json.dumps(value).encode()
    else:
        store.objects["activities/2026/1/context/v1/2026-09-30T12-00-00.json"] = b'{"context_id":"synthetic-context","rpe":5}'
    result = run(**arguments)
    assert result["activities"][0]["file_status"] == "unchanged"
    assert result["activities"][0]["coach_reused"] is False
    assert len(builds) == 2


def test_unknown_local_date_is_explicit_and_never_taken_from_utc_summary(incremental):
    arguments, detail, builds = incremental
    del detail["startTimeLocal"]
    result = run(**arguments)
    assert result["activities"][0]["files_ready"] is True
    assert result["activities"][0]["coach_status"] == "activity_date_unknown"
    assert builds == []


def test_source_local_day_controls_analysis_across_new_year():
    row = {"Activity ID": "garmin-1", "Activity Date": "2025-12-31 23:30:00", "Moving Time": "1800"}
    activity = {"activityId": "1", "activityName": "Synthetic run", "activityType": "running",
                "startTimeLocal": "2026-01-01 00:30:00"}
    metadata = _coach_metadata(activity, row, FakeStore())
    assert metadata["date"] == "2026-01-01"
    assert metadata["moving_seconds"] == 1800 and isinstance(metadata["moving_seconds"], int)


@pytest.mark.parametrize("canonical_id,expected", [("1", "2026-09-30"), ("2", None)])
def test_canonical_local_date_fallback_requires_matching_id(canonical_id, expected):
    store = FakeStore()
    store.objects["activities/2026/1/activity.v1.json"] = json.dumps({
        "activity": {"id": canonical_id, "start_time_local": "2026-09-30 01:00:00"},
    }).encode()
    metadata = _coach_metadata({"activityId": "1", "startTimeGMT": "2026-09-29 23:00:00"}, {}, store)
    assert (metadata["date"] if metadata else None) == expected


def test_analyzer_error_preserves_successful_file_status(incremental, monkeypatch):
    arguments, _, _ = incremental

    def failure(**kwargs):
        raise ValueError("synthetic analyzer failure")

    monkeypatch.setattr(coach_backfill, "build_coach_input", failure)
    result = run(**arguments)
    assert result["activities"][0]["files_ready"] is True
    assert result["activities"][0]["coach_status"] == "error"


def test_nested_garmin_dates_preserve_local_year_and_explicit_timestamps():
    row = {"Activity ID": "garmin-123", "Activity Date": "2025-12-31 23:30:00",
           "Activity Type": "Run"}
    details = {"activityId": 123, "activityTypeDTO": {"typeKey": "running"},
               "summaryDTO": {"startTimeLocal": "2026-01-01 00:30:00",
                              "startTimeGMT": "2025-12-31 23:30:00"}}
    complete = _complete_activity_details(details, row)
    assert complete["startTimeLocal"] == "2026-01-01 00:30:00"
    assert _coach_metadata(complete, row, FakeStore())["date"] == "2026-01-01"
    assert "startTimeLocal" not in details  # Do not mutate the source response.
    details["startTimeLocal"] = "2026-01-02 00:30:00"
    assert _complete_activity_details(details, row)["startTimeLocal"] == details["startTimeLocal"]


@pytest.mark.parametrize("summary_dto", [None, [], "invalid"])
def test_malformed_nested_summary_never_invents_a_local_date(summary_dto):
    complete = _complete_activity_details(
        {"activityId": 123, "summaryDTO": summary_dto},
        {"Activity ID": "garmin-123", "Activity Date": "2026-09-30 10:00:00"},
    )
    assert not complete.get("startTimeLocal")
    assert complete["startTimeGMT"] == "2026-09-30 10:00:00"


def test_general_refresh_nested_source_dates_are_ready_and_reused(incremental):
    arguments, detail, builds = incremental
    local = detail.pop("startTimeLocal")
    detail["summaryDTO"] = {"startTimeLocal": local, "startTimeGMT": "2026-09-30 08:00:00"}
    first = run(**arguments)
    second = run(**arguments)
    assert first["activities"][0]["coach_status"] == "ready"
    assert second["activities"][0]["file_status"] == "unchanged"
    assert second["activities"][0]["coach_reused"] is True
    assert len(builds) == 1


def test_general_refresh_repairs_missing_canonical_local_metadata_without_download(incremental):
    import gzip

    arguments, _, builds = incremental
    store = arguments["store"]
    key = "activities/2026/1/activity.v1.json"
    canonical = json.loads(store.objects[key])
    canonical["activity"] = {"id": "1"}
    store.objects[key] = json.dumps(canonical).encode()
    first = run(**arguments)
    assert first["activities"][0]["file_status"] == "baseline"
    assert first["activities"][0]["coach_status"] == "ready"
    repaired = json.loads(gzip.decompress(store.objects[key]))
    assert repaired["activity"]["start_time_local"] == "2026-09-30 10:00:00"
    store.reset_counts()
    second = run(**arguments)
    assert second["activities"][0]["coach_reused"] is True
    assert store.counts["put"] == 0 and len(builds) == 1
