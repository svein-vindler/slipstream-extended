import csv
import gzip
import hashlib
import io
import json
import os
import sys
import types
import zipfile
from datetime import date, datetime, timezone

from pipeline.activity_backfill import (
    is_job_stopping_error,
    progress_key,
    select_backfill_batch,
    validate_backfill_request,
)
from pipeline.activity_backfill import (
    run as run_activity_backfill,
)
from pipeline.activity_backfill_scheduler import (
    PLAN_KEY as ACTIVITY_PLAN_KEY,
)
from pipeline.activity_backfill_scheduler import (
    history_bounds,
    year_ranges,
)
from pipeline.activity_backfill_scheduler import (
    run as run_scheduled_activity_backfill,
)
from pipeline.activity_refresh import (
    activity_fingerprint,
    manifest_key,
    refresh_activity,
)
from pipeline.activity_refresh import run as run_activity_refresh
from pipeline.coach import build_coach_input, select_profile
from pipeline.coach_backfill import PLAN_KEY as COACH_PLAN_KEY
from pipeline.coach_backfill import run as run_coach_backfill
from pipeline.granular import (
    activity_prefix,
    decode_fit,
    extract_fit,
    gzip_bytes,
    gzip_json,
    is_endurance_activity,
    json_bytes,
    normalize_endurance_session,
    normalize_hrv,
    normalize_strength_session,
    select_activity_sample,
)
from pipeline.granular_export import (
    activity_artifact_keys,
    export_activity,
    validate_granular_request,
)
from pipeline.granular_export import run as run_granular_export
from pipeline.health_detail import normalize_body_composition, normalize_sleep_detail
from pipeline.health_detail_backfill import STREAMS as HEALTH_DETAIL_STREAMS
from pipeline.health_detail_backfill import history_dates as health_detail_dates
from pipeline.health_detail_backfill import run as run_health_detail_backfill
from pipeline.health_history_index import (
    build_month_index,
    summarize_hrv_payload,
)
from pipeline.health_history_index import (
    index_key as health_history_index_key,
)
from pipeline.health_history_index import (
    sync_stream as sync_health_history_stream,
)
from pipeline.health_writer import write_health_dataset
from pipeline.hrv_backfill import (
    PLAN_KEY as HRV_PLAN_KEY,
)
from pipeline.hrv_backfill import (
    history_bounds as hrv_history_bounds,
)
from pipeline.hrv_backfill import (
    history_dates as hrv_history_dates,
)
from pipeline.hrv_backfill import (
    hrv_key,
    select_hrv_batch,
)
from pipeline.hrv_backfill import run as run_hrv_backfill
from pipeline.local_bootstrap import (
    all_phases_terminal,
    compact_result,
    load_env_file,
    phase_is_terminal,
    run_cycle,
    run_local_bootstrap,
)
from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.schema import Activity, canonical_sport, to_utc
from pipeline.sources.garmin_health import _heart_values, _sleep_fields, _weight_kg, _weight_rows
from pipeline.summary_export import build_summary_exports
from pipeline.summary_restore import decode_summary, restore_summaries
from pipeline.writer import _avg_speed_ms, _num, write_dataset

UTC = timezone.utc


class FakeR2Client:
    def __init__(self, pages=None):
        self.pages = pages or [{"Contents": []}]
        self.puts = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        pages = self.pages

        class Paginator:
            def paginate(self, **kwargs):
                assert kwargs["Bucket"] == "test-bucket"
                return pages

        return Paginator()

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def test_canonical_sport_maps_families():
    assert canonical_sport("running") == "run"
    assert canonical_sport("cycling") == "ride"
    assert canonical_sport("lap_swimming") == "swim"
    assert canonical_sport("yoga") == "yoga"
    assert canonical_sport("HIIT") == "workout"
    assert canonical_sport("some_unknown_thing") == "other"
    assert canonical_sport(None) == "other"


def test_r2_store_inventory_and_per_run_write_limit():
    client = FakeR2Client([
        {"Contents": [{"Size": 10}, {"Size": 20}]},
        {"Contents": [{"Size": 30}]},
    ])
    store = R2Store(
        client=client,
        bucket="test-bucket",
        max_bucket_bytes=1_000,
        max_bucket_objects=10,
        max_writes_per_run=2,
        max_write_bytes_per_run=100,
    )

    assert store.inventory() == {"objects": 3, "bytes": 60}
    store.put("one", b"123", "text/plain")
    store.put("two", b"456", "text/plain")
    try:
        store.put("three", b"789", "text/plain")
        raise AssertionError("Expected the per-run R2 write limit to stop the upload")
    except R2BudgetError as exc:
        assert "write limit" in str(exc)
    assert len(client.puts) == 2


def test_r2_store_stops_before_storage_limit():
    client = FakeR2Client([{"Contents": [{"Size": 95}]}])
    store = R2Store(
        client=client,
        bucket="test-bucket",
        max_bucket_bytes=100,
        max_bucket_objects=10,
        max_writes_per_run=10,
        max_write_bytes_per_run=100,
    )

    try:
        store.put("too-large", b"123456", "text/plain")
        raise AssertionError("Expected the R2 storage limit to stop the upload")
    except R2BudgetError as exc:
        assert "storage safety limit" in str(exc)
    assert client.puts == []


def test_r2_store_lists_paginated_keys_with_prefix():
    client = FakeR2Client([
        {"Contents": [{"Key": "activities/2025/1/activity.fit", "Size": 10}]},
        {"Contents": [{"Key": "activities/2025/2/activity.fit", "Size": 20}]},
    ])
    store = R2Store(
        client=client,
        bucket="test-bucket",
        max_bucket_bytes=1_000,
        max_bucket_objects=10,
        max_writes_per_run=10,
        max_write_bytes_per_run=100,
    )

    assert store.list_keys("activities/") == {
        "activities/2025/1/activity.fit",
        "activities/2025/2/activity.fit",
    }


def test_r2_store_lists_object_revisions_without_getting_bodies():
    client = FakeR2Client([
        {
            "Contents": [
                {"Key": "activities/1", "ETag": '"revision-1"', "Size": 10},
                {"Key": "activities/2", "ETag": '"revision-2"', "Size": 20},
            ]
        }
    ])
    store = R2Store(client=client, bucket="test-bucket")

    assert store.list_object_revisions("activities/") == {
        "activities/1": "revision-1",
        "activities/2": "revision-2",
    }


def test_granular_request_limits_protect_free_tier():
    validate_granular_request(60, 3660, 25, 25)
    for values, expected in (
        ((61, 180, 2, 2), "hrv_days"),
        ((7, 3661, 2, 2), "activity_days"),
        ((7, 180, 26, 25), "combined"),
    ):
        try:
            validate_granular_request(*values)
            raise AssertionError("Expected granular request limit to fail")
        except ValueError as exc:
            assert expected in str(exc)


def test_health_only_granular_refresh_skips_activity_discovery(tmp_path, monkeypatch):
    class FakeGarmin:
        def get_activities_by_date(self, *_args):
            raise AssertionError("activity discovery should be skipped")

    monkeypatch.setattr("pipeline.granular_export._login", lambda: FakeGarmin())
    result = run_granular_export(
        hrv_days=0,
        activity_days=0,
        cardio_count=0,
        strength_count=0,
        output_dir=str(tmp_path),
    )

    assert result["activities"] == []


def test_activity_backfill_bounds_and_resumable_selection():
    validate_backfill_request("2024-01-01", "2024-12-31", 25)
    for values, expected in (
        (("2024-12-31", "2024-01-01", 20), "after"),
        (("2023-01-01", "2024-01-02", 20), "366"),
        (("2024-01-01", "2024-12-31", 51), "between"),
    ):
        try:
            validate_backfill_request(*values)
            raise AssertionError("Expected activity backfill validation to fail")
        except ValueError as exc:
            assert expected in str(exc)

    activities = [
        {
            "activityId": 1,
            "startTimeLocal": "2024-01-01 08:00:00",
            "activityType": {"typeKey": "running"},
        },
        {
            "activityId": 2,
            "startTimeLocal": "2024-01-02 08:00:00",
            "activityType": {"typeKey": "strength_training"},
        },
        {
            "activityId": 3,
            "startTimeLocal": "2024-01-03 08:00:00",
            "activityType": {"typeKey": "yoga"},
        },
        {
            "activityId": 4,
            "startTimeLocal": "2024-01-04 08:00:00",
            "activityType": {"typeKey": "cycling"},
        },
        {
            "activityId": 5,
            "startTimeLocal": "2024-01-05 08:00:00",
            "activityType": {"typeKey": "cross_country_skiing"},
        },
    ]
    existing = set(activity_artifact_keys(activities[0]))
    failures = {"4": {"attempts": 3}}

    selected = select_backfill_batch(
        activities, existing, failures, limit=2
    )
    assert [row["activityId"] for row in selected] == [2, 5]
    retried = select_backfill_batch(
        activities, existing, failures, limit=2, retry_failures=True
    )
    assert [row["activityId"] for row in retried] == [2, 4]


def test_activity_backfill_stops_batch_for_service_and_auth_errors():
    assert is_job_stopping_error(TimeoutError("request timed out"))
    assert is_job_stopping_error(RuntimeError("429 Too Many Requests"))
    assert is_job_stopping_error(RuntimeError("Garmin rate limit reached"))
    assert is_job_stopping_error(RuntimeError("401 Unauthorized"))
    assert not is_job_stopping_error(ValueError("FIT file is unavailable"))


def test_activity_export_reuses_existing_fit_when_json_is_missing(
    tmp_path, monkeypatch
):
    activity = {
        "activityId": 123,
        "startTimeLocal": "2024-01-02 08:00:00",
        "activityType": {"typeKey": "strength_training"},
    }
    fit_key, json_key = activity_artifact_keys(activity)

    class FakeStore:
        def __init__(self):
            self.puts = []

        def get(self, key):
            assert key == fit_key
            return b"existing-fit"

        def put(self, key, data, content_type, *, encoding=None):
            self.puts.append((key, data, content_type, encoding))

    class FakeGarmin:
        def download_activity(self, *args, **kwargs):
            raise AssertionError("Existing FIT data should be reused from R2")

        def get_activity_exercise_sets(self, activity_id):
            assert activity_id == 123
            return {"exerciseSets": []}

    monkeypatch.setattr(
        "pipeline.granular_export.decode_fit",
        lambda *args, **kwargs: {
            "message_counts": {"session_mesgs": 1},
            "normalized_strength_sets": [],
        },
    )
    store = FakeStore()
    result = export_activity(
        activity,
        FakeGarmin(),
        store,
        tmp_path,
        existing_keys={fit_key},
    )

    assert result["status"] == "updated"
    assert [item[0] for item in store.puts] == [json_key]
    assert store.puts[0][3] == "gzip"


def test_activity_backfill_writes_resumable_progress_when_range_is_complete(
    tmp_path,
):
    activity = {
        "activityId": 123,
        "startTimeLocal": "2024-01-02 08:00:00",
        "activityType": {"typeKey": "running"},
    }
    existing = set(activity_artifact_keys(activity))

    class FakeStore:
        def __init__(self):
            self.puts = []

        def list_keys(self, prefix):
            return existing if prefix == "activities/" else set()

        def put(self, key, data, content_type, *, encoding=None):
            self.puts.append((key, data, content_type, encoding))

    class FakeGarmin:
        def get_activities_by_date(self, start_date, end_date, sortorder=None):
            assert (start_date, end_date, sortorder) == (
                "2024-01-01",
                "2024-12-31",
                "desc",
            )
            return [activity]

    store = FakeStore()
    manifest = run_activity_backfill(
        start_date="2024-01-01",
        end_date="2024-12-31",
        max_activities=20,
        output_dir=str(tmp_path),
        garmin=FakeGarmin(),
        store=store,
    )

    assert manifest["complete_activities"] == 1
    assert manifest["remaining_activities"] == 0
    assert manifest["attempted_this_run"] == 0
    assert [item[0] for item in store.puts] == [
        progress_key("2024-01-01", "2024-12-31")
    ]


def test_scheduled_backfill_builds_stable_year_ranges_from_r2_summary():
    csv_data = gzip.compress(
        b"Activity ID,Activity Date\n"
        b"garmin-new,2026-09-20 10:00:00\n"
        b"garmin-old,2016-09-15 08:00:00\n"
    )
    start, end = history_bounds(csv_data)

    assert (start.isoformat(), end.isoformat()) == ("2016-09-15", "2026-09-20")
    ranges = year_ranges(start, end)
    assert ranges[0] == {
        "start_date": "2026-01-01",
        "end_date": "2026-09-20",
        "status": "pending",
    }
    assert ranges[-1] == {
        "start_date": "2016-09-15",
        "end_date": "2016-12-31",
        "status": "pending",
    }
    assert len(ranges) == 11


def test_scheduled_backfill_starts_with_newest_unfinished_range(monkeypatch):
    range_key = progress_key("2016-09-15", "2016-12-31")
    summary = (
        b"Activity ID,Activity Date\n"
        b"garmin-new,2017-06-01 10:00:00\n"
        b"garmin-old,2016-09-15 08:00:00\n"
    )
    progress = {
        "remaining_activities": 43,
        "complete_activities": 5,
        "blocked_after_three_failures": 0,
    }

    class FakeStore:
        def __init__(self):
            self.puts = []

        def list_keys(self, prefix):
            assert prefix == "backfill/activities/"
            return {range_key}

        def get(self, key):
            if key == "summary/activities.csv":
                return summary
            if key == range_key:
                return json.dumps(progress).encode()
            raise AssertionError(f"Unexpected R2 key: {key}")

        def put(self, key, data, content_type, *, encoding=None):
            self.puts.append((key, json.loads(data), content_type, encoding))

    calls = []
    monkeypatch.setattr(
        "pipeline.activity_backfill_scheduler.run_backfill",
        lambda **kwargs: calls.append(kwargs) or {
            "complete_activities": 30,
            "remaining_activities": 18,
            "blocked_after_three_failures": 0,
        },
    )
    store = FakeStore()
    garmin = object()
    plan = run_scheduled_activity_backfill(
        max_activities=50, store=store, garmin=garmin
    )

    assert calls == [{
        "start_date": "2017-01-01",
        "end_date": "2017-06-01",
        "max_activities": 50,
        "garmin": garmin,
        "store": store,
    }]
    assert plan["history_end"] == "2017-06-01"
    assert plan["last_range"] == {
        "start_date": "2017-01-01",
        "end_date": "2017-06-01",
    }
    assert [item[0] for item in store.puts] == [
        ACTIVITY_PLAN_KEY
    ]


def test_scheduled_backfill_uses_leftover_budget_on_the_next_year(monkeypatch):
    summary = (
        b"Activity ID,Activity Date\n"
        b"garmin-new,2017-06-01 10:00:00\n"
        b"garmin-old,2016-09-15 08:00:00\n"
    )

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/activities.csv": summary}

        def list_keys(self, prefix):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    store = FakeStore()
    calls = []

    def fake_run_backfill(**kwargs):
        calls.append(kwargs)
        if kwargs["start_date"] == "2017-01-01":
            result = {
                "complete_activities": 2,
                "remaining_activities": 0,
                "blocked_after_three_failures": 0,
                "attempted_this_run": 2,
            }
        else:
            result = {
                "complete_activities": 48,
                "remaining_activities": 10,
                "blocked_after_three_failures": 0,
                "attempted_this_run": 48,
            }
        key = progress_key(kwargs["start_date"], kwargs["end_date"])
        store.objects[key] = json.dumps(result).encode()
        return result

    monkeypatch.setattr(
        "pipeline.activity_backfill_scheduler.run_backfill",
        fake_run_backfill,
    )
    plan = run_scheduled_activity_backfill(
        max_activities=50, store=store, garmin=object()
    )

    assert [call["max_activities"] for call in calls] == [50, 48]
    assert [call["start_date"] for call in calls] == [
        "2017-01-01", "2016-09-15",
    ]
    assert plan["last_run_ranges"] == [
        {
            "start_date": "2017-01-01",
            "end_date": "2017-06-01",
            "attempted": 2,
            "status": "complete",
        },
        {
            "start_date": "2016-09-15",
            "end_date": "2016-12-31",
            "attempted": 48,
            "status": "active",
        },
    ]


def test_completed_scheduled_plan_does_not_write_or_contact_garmin():
    plan = {
        "schema_version": 1,
        "kind": "activity-backfill-plan",
        "status": "complete",
        "ranges": [],
    }

    class FakeStore:
        def list_keys(self, prefix):
            return {ACTIVITY_PLAN_KEY}

        def get(self, key):
            return json.dumps(plan).encode()

        def put(self, *args, **kwargs):
            raise AssertionError("A completed plan must not write to R2")

    result = run_scheduled_activity_backfill(store=FakeStore())
    assert result == plan


def test_blocked_scheduled_plan_rechecks_progress_and_can_become_complete(
    monkeypatch,
):
    range_key = progress_key("2021-01-01", "2021-12-31")
    plan = {
        "schema_version": 2,
        "kind": "activity-backfill-plan",
        "status": "complete_with_blocked",
        "ranges": [{
            "start_date": "2021-01-01",
            "end_date": "2021-12-31",
            "status": "blocked",
            "remaining_activities": 1,
            "blocked_activities": 1,
        }],
    }
    progress = {
        "complete_activities": 187,
        "remaining_activities": 0,
        "blocked_after_three_failures": 0,
    }

    class FakeStore:
        def __init__(self):
            self.objects = {
                ACTIVITY_PLAN_KEY: json.dumps(plan).encode(),
                range_key: json.dumps(progress).encode(),
            }

        def list_keys(self, prefix):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    monkeypatch.setattr(
        "pipeline.activity_backfill_scheduler._login",
        lambda: (_ for _ in ()).throw(AssertionError("Garmin must not be contacted")),
    )

    result = run_scheduled_activity_backfill(store=FakeStore())

    assert result["status"] == "complete"
    assert result["ranges"][0]["status"] == "complete"
    assert result["ranges"][0]["remaining_activities"] == 0
    assert result["ranges"][0]["blocked_activities"] == 0


def test_num_rounds_and_blanks():
    assert _num(6.289410156, 3) == 6.289
    assert _num(160.0, 0) == 160          # integer, no trailing .0
    assert _num(None, 3) == ""


def test_avg_speed():
    a = Activity(source="garmin", source_id="1", start=datetime(2026, 6, 1, tzinfo=UTC),
                 sport="run", distance_km=10, moving_s=3000)
    assert round(_avg_speed_ms(a), 2) == 3.33  # 10000 m / 3000 s


def test_naive_datetime_is_treated_as_utc():
    assert to_utc(datetime(2026, 6, 1, 6, 30)).tzinfo == UTC


def test_write_dataset_roundtrip(tmp_path):
    acts = [
        Activity(source="garmin", source_id="1", start=datetime(2026, 6, 1, 6, 30, tzinfo=UTC),
                 sport="run", raw_sport="running", name="Morning Run",
                 distance_km=10.123456, moving_s=3000, avg_hr=160, calories=600),
        Activity(source="garmin", source_id="2", start=datetime(2026, 5, 20, 8, 0, tzinfo=UTC),
                 sport="ride", raw_sport="cycling", name="Long Ride",
                 distance_km=40, moving_s=7200, avg_hr=140, calories=1200),
    ]
    summary = write_dataset(acts, str(tmp_path))
    assert summary["activities_written"] == 2

    rows = list(csv.DictReader(open(tmp_path / "activities.csv")))
    assert len(rows) == 2
    assert rows[0]["Activity Name"] == "Morning Run"   # newest first
    assert rows[0]["Activity Type"] == "Run"           # sport, title-cased
    assert rows[0]["Distance"] == "10.123"             # rounded to 3 dp
    assert rows[0]["Source"] == "garmin"


def test_incremental_write_preserves_history_and_updates_overlap(tmp_path):
    write_dataset([
        Activity(source="garmin", source_id="old", start=datetime(2020, 1, 1, tzinfo=UTC),
                 sport="run", name="Historical Run", distance_km=5),
        Activity(source="garmin", source_id="recent", start=datetime(2026, 6, 1, tzinfo=UTC),
                 sport="run", name="Original Name", distance_km=10,
                 track_file="tracks/garmin-recent.gpx"),
    ], str(tmp_path))

    summary = write_dataset([
        Activity(source="garmin", source_id="recent", start=datetime(2026, 6, 1, tzinfo=UTC),
                 sport="run", name="Updated Name", distance_km=11),
        Activity(source="garmin", source_id="new", start=datetime(2026, 6, 2, tzinfo=UTC),
                 sport="ride", name="New Ride", distance_km=20),
    ], str(tmp_path), preserve_existing=True)

    rows = list(csv.DictReader(open(tmp_path / "activities.csv")))
    by_id = {row["Activity ID"]: row for row in rows}
    assert summary["activities_fetched"] == 2
    assert summary["activities_written"] == 3
    assert by_id["garmin-old"]["Activity Name"] == "Historical Run"
    assert by_id["garmin-recent"]["Activity Name"] == "Updated Name"
    assert by_id["garmin-recent"]["Filename"] == "tracks/garmin-recent.gpx"
    assert rows[0]["Activity ID"] == "garmin-new"


def test_health_extractors_reduce_raw_data_to_daily_summaries():
    assert _heart_values({"heartRateValues": [[1, 50], [2, -1], [3, 70]]}) == [50, 70]
    assert _weight_kg(81234) == 81.234
    assert _weight_rows({"dailyWeightSummaries": [{
        "summaryDate": "2026-06-01",
        "latestWeight": {"calendarDate": "2026-06-01", "weight": 81234.0},
    }]}) == [("2026-06-01", 81.234)]
    assert _sleep_fields({
        "calendarDate": "2026-06-01",
        "sleepTimeSeconds": 25200,
        "deepSleepSeconds": 3600,
        "sleepScores": {"overall": {"value": 82}},
    }) == {
        "Sleep Seconds": 25200,
        "Deep Sleep Seconds": 3600,
        "Light Sleep Seconds": None,
        "REM Sleep Seconds": None,
        "Awake Sleep Seconds": None,
        "Sleep Score": 82,
    }


def test_health_writer_merges_by_date_and_keeps_old_nonblank_values(tmp_path):
    write_health_dataset([{
        "Date": "2020-01-01", "Sleep Score": 80, "Steps": 8000, "Source": "garmin",
    }], str(tmp_path))
    summary = write_health_dataset([
        {"Date": "2020-01-01", "Sleep Score": "", "Steps": 9000, "Source": "garmin"},
        {"Date": "2020-01-02", "Resting Heart Rate": 50, "Source": "garmin"},
    ], str(tmp_path))

    rows = list(csv.DictReader(open(tmp_path / "health_daily.csv")))
    by_date = {row["Date"]: row for row in rows}
    assert summary["health_days_written"] == 2
    assert by_date["2020-01-01"]["Sleep Score"] == "80"
    assert by_date["2020-01-01"]["Steps"] == "9000"
    assert rows[0]["Date"] == "2020-01-02"


def test_extract_fit_accepts_original_zip():
    fit = b"\x0e\x10\x00\x00\x00\x00\x00\x00.FITpayload"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("123_ACTIVITY.fit", fit)
    assert extract_fit(buffer.getvalue()) == fit


def test_normalize_and_compress_hrv_readings():
    payload = normalize_hrv("2026-09-20", {
        "sleepStartTimestampGMT": "2026-09-19T22:00:00Z",
        "sleepEndTimestampGMT": "2026-09-20T06:00:00Z",
        "sleepStartTimestampLocal": "2026-09-20T00:00:00.0",
        "sleepEndTimestampLocal": "2026-09-20T08:00:00.0",
        "hrvSummary": {"lastNightAvg": 42},
        "hrvReadings": [
            {"readingTimeGMT": "2026-09-20T01:00:00Z", "hrvValue": 39},
            {"readingTimeGMT": "2026-09-20T01:01:00Z", "hrvValue": 44},
        ],
    })
    assert payload["reading_count"] == 2
    assert payload["sleep_start_garmin_local"] == "2026-09-20T00:00:00.0"
    assert payload["sleep_end_garmin_local"] == "2026-09-20T08:00:00.0"
    assert payload["readings"][0]["hrv_ms"] == 39
    assert json.loads(gzip.decompress(gzip_json(payload)))["summary"]["lastNightAvg"] == 42


def test_json_serialization_replaces_nonfinite_floats_with_null():
    encoded = json_bytes({
        "nan": float("nan"),
        "positive_infinity": float("inf"),
        "nested": [1, float("-inf")],
    })
    assert b"NaN" not in encoded
    assert b"Infinity" not in encoded
    assert json.loads(encoded) == {
        "nan": None,
        "positive_infinity": None,
        "nested": [1, None],
    }


def test_selects_cardio_and_strength_sample_without_duplicates():
    activities = [
        {"activityId": 1, "activityType": {"typeKey": "running"}},
        {"activityId": 2, "activityType": {"typeKey": "strength_training"}},
        {"activityId": 3, "activityType": {"typeKey": "cycling"}},
        {"activityId": 4, "activityType": {"typeKey": "strength_training"}},
        {"activityId": 5, "activityType": {"typeKey": "yoga"}},
    ]
    selected = select_activity_sample(activities)
    assert [row["activityId"] for row in selected] == [1, 3, 2, 4]


def test_endurance_activity_types_include_ski_and_other_common_sports():
    included = (
        "running", "trail_running", "cycling", "lap_swimming", "indoor_rowing",
        "cross_country_skiing", "resort_skiing", "backcountry_skiing",
        "elliptical", "stair_climbing", "snow_shoe", "kayaking", "triathlon",
    )
    assert all(is_endurance_activity(kind) for kind in included)
    assert not is_endurance_activity("strength_training")


def test_gzip_bytes_roundtrip_is_deterministic():
    tcx = b"<TrainingCenterDatabase><Activity /></TrainingCenterDatabase>"
    assert gzip.decompress(gzip_bytes(tcx)) == tcx
    assert gzip_bytes(tcx) == gzip_bytes(tcx)


SAMPLE_TCX = b'''<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
 xmlns:ns3="http://www.garmin.com/xmlschemas/ActivityExtension/v2">
  <Activities><Activity Sport="Running"><Id>2026-09-21T10:00:00Z</Id>
    <Lap StartTime="2026-09-21T10:00:00Z">
      <TotalTimeSeconds>600</TotalTimeSeconds><DistanceMeters>2000</DistanceMeters>
      <Calories>150</Calories><AverageHeartRateBpm><Value>150</Value></AverageHeartRateBpm>
      <MaximumHeartRateBpm><Value>170</Value></MaximumHeartRateBpm>
      <Intensity>Active</Intensity><TriggerMethod>Distance</TriggerMethod>
      <Track>
        <Trackpoint><Time>2026-09-21T10:00:00Z</Time><Position><LatitudeDegrees>60</LatitudeDegrees><LongitudeDegrees>10</LongitudeDegrees></Position><AltitudeMeters>100</AltitudeMeters><DistanceMeters>0</DistanceMeters><HeartRateBpm><Value>140</Value></HeartRateBpm><Extensions><ns3:TPX><ns3:Speed>3.2</ns3:Speed></ns3:TPX></Extensions></Trackpoint>
        <Trackpoint><Time>2026-09-21T10:05:00Z</Time><Position><LatitudeDegrees>60.01</LatitudeDegrees><LongitudeDegrees>10.01</LongitudeDegrees></Position><AltitudeMeters>110</AltitudeMeters><DistanceMeters>1000</DistanceMeters><HeartRateBpm><Value>150</Value></HeartRateBpm></Trackpoint>
        <Trackpoint><Time>2026-09-21T10:10:00Z</Time><Position><LatitudeDegrees>60.02</LatitudeDegrees><LongitudeDegrees>10.02</LongitudeDegrees></Position><AltitudeMeters>105</AltitudeMeters><DistanceMeters>2000</DistanceMeters><HeartRateBpm><Value>160</Value></HeartRateBpm></Trackpoint>
      </Track>
    </Lap>
  </Activity></Activities>
</TrainingCenterDatabase>'''

EMPTY_TCX = b'''<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
  <Activities><Activity Sport="Other"><Id>2021-01-02T10:00:00Z</Id>
    <Lap StartTime="2021-01-02T10:00:00Z">
      <TotalTimeSeconds>2.8</TotalTimeSeconds><DistanceMeters>0</DistanceMeters>
      <Calories>0</Calories><Intensity>Active</Intensity><TriggerMethod>Manual</TriggerMethod>
      <Track />
    </Lap>
  </Activity></Activities>
</TrainingCenterDatabase>'''


def test_normalize_tcx_builds_gps_free_endurance_analysis():
    activity = {
        "activityId": 24444691902,
        "activityName": "Easy run",
        "activityType": {"typeKey": "running"},
    }
    result = normalize_endurance_session(activity, SAMPLE_TCX)

    assert result["activity"]["id"] == "24444691902"
    assert result["summary"]["distance_m"] == 2000
    assert result["summary"]["duration_seconds"] == 600
    assert result["summary"]["maximum_heart_rate_bpm"] == 160
    assert len(result["kilometer_splits"]) == 2
    assert result["kilometer_splits"][0]["duration_seconds"] == 300
    assert result["distance_halves"]["first"]["average_heart_rate_bpm"] == 145
    assert result["distance_halves"]["second"]["average_heart_rate_bpm"] == 155
    assert result["summary"]["aerobic_decoupling_percent"] > 0
    encoded = json.dumps(result)
    assert "Latitude" not in encoded
    assert "Longitude" not in encoded


def test_normalize_tcx_represents_empty_activity_without_blocking_backfill():
    activity = {
        "activityId": 7035306992,
        "activityName": "Snowboard",
        "activityType": {"typeKey": "snowboarding"},
    }

    result = normalize_endurance_session(activity, EMPTY_TCX)

    assert result["available"] is False
    assert result["reason"] == "no_timed_trackpoints"
    assert result["summary"]["duration_seconds"] == 2.8
    assert result["summary"]["distance_m"] == 0
    assert result["summary"]["trackpoint_count"] == 0
    assert result["sampled_trackpoints"] == []


def test_activity_export_normalizes_existing_tcx_without_redownloading(
    tmp_path,
):
    activity = {
        "activityId": 24444691902,
        "activityName": "Easy run",
        "startTimeLocal": "2026-09-21 12:00:00",
        "activityType": {"typeKey": "running"},
    }
    fit_key, json_key, tcx_key, endurance_key = activity_artifact_keys(activity)

    class FakeStore:
        def __init__(self):
            self.puts = []

        def get(self, key):
            assert key == tcx_key
            return gzip_bytes(SAMPLE_TCX)

        def put(self, key, data, content_type, *, encoding=None):
            self.puts.append((key, data, content_type, encoding))

    class FakeGarmin:
        def download_activity(self, *args, **kwargs):
            raise AssertionError("Existing TCX should be normalized without Garmin download")

    store = FakeStore()
    result = export_activity(
        activity,
        FakeGarmin(),
        store,
        tmp_path,
        existing_keys={fit_key, json_key, tcx_key},
    )

    assert result["status"] == "updated"
    assert result["endurance_trackpoints"] == 3
    assert [item[0] for item in store.puts] == [endurance_key]
    assert json.loads(gzip.decompress(store.puts[0][1]))["summary"]["distance_m"] == 2000


def test_summary_exports_include_current_files_daily_snapshots_and_manifest(tmp_path):
    activities = b"Activity ID,Activity Date\ngarmin-1,2026-09-20\n"
    health = b"Date,Steps\n2026-09-20,12345\n"
    (tmp_path / "activities.csv").write_bytes(activities)
    (tmp_path / "health_daily.csv").write_bytes(health)

    objects, manifest = build_summary_exports(
        str(tmp_path),
        snapshot_day=datetime(2026, 9, 21, tzinfo=UTC).date(),
        generated_at=datetime(2026, 9, 21, 8, 30, tzinfo=UTC),
    )
    by_key = {item["key"]: item for item in objects}
    assert set(by_key) == {
        "summary/activities.csv",
        "summary/health_daily.csv",
        "summary/snapshots/2026/09/21/activities.csv",
        "summary/snapshots/2026/09/21/health_daily.csv",
        "summary/manifest.json",
    }
    assert gzip.decompress(by_key["summary/activities.csv"]["data"]) == activities
    assert by_key["summary/activities.csv"]["encoding"] == "gzip"
    assert manifest["files"][0]["rows"] == 1
    assert json.loads(by_key["summary/manifest.json"]["data"])["snapshot_date"] == "2026-09-21"


def test_restore_summaries_validates_and_restores_both_files(tmp_path):
    activities = b"Activity ID,Activity Date\ngarmin-1,2026-09-20\n"
    health = b"Date,Steps\n2026-09-20,12345\n"
    source = tmp_path / "source"
    source.mkdir()
    (source / "activities.csv").write_bytes(activities)
    (source / "health_daily.csv").write_bytes(health)
    objects, _ = build_summary_exports(
        str(source),
        generated_at=datetime(2026, 9, 21, 8, 30, tzinfo=UTC),
    )
    remote = {item["key"]: item["data"] for item in objects}

    class FakeStore:
        def get(self, key):
            return remote[key]

    destination = tmp_path / "restored"
    result = restore_summaries(str(destination), FakeStore())

    assert (destination / "activities.csv").read_bytes() == activities
    assert (destination / "health_daily.csv").read_bytes() == health
    assert result["files"] == [
        {"name": "activities.csv", "rows": 1},
        {"name": "health_daily.csv", "rows": 1},
    ]


def test_restore_rejects_checksum_mismatch_before_replacing_files(tmp_path):
    destination = tmp_path / "data"
    destination.mkdir()
    existing = b"Activity ID,Activity Date\nold,2020-01-01\n"
    (destination / "activities.csv").write_bytes(existing)

    manifest = {
        "schema_version": 1,
        "files": [
            {
                "name": "activities.csv",
                "current_key": "summary/activities.csv",
                "sha256": "wrong",
            },
            {
                "name": "health_daily.csv",
                "current_key": "summary/health_daily.csv",
                "sha256": "also-wrong",
            },
        ],
    }
    remote = {
        "summary/manifest.json": json.dumps(manifest).encode(),
        "summary/activities.csv": gzip.compress(
            b"Activity ID,Activity Date\nnew,2026-09-20\n"
        ),
    }

    class FakeStore:
        def get(self, key):
            return remote[key]

    try:
        restore_summaries(str(destination), FakeStore())
        raise AssertionError("Expected checksum validation to fail")
    except ValueError as exc:
        assert "checksum mismatch" in str(exc)

    assert (destination / "activities.csv").read_bytes() == existing


def test_decode_summary_accepts_plain_and_gzipped_csv():
    raw = b"Date,Steps\n2026-09-20,12345\n"
    assert decode_summary(raw, "Date,") == raw
    assert decode_summary(gzip.compress(raw), "Date,") == raw


def test_activity_prefix_uses_year_and_id():
    activity = {"activityId": 123, "startTimeLocal": "2026-09-20 10:00:00"}
    assert activity_prefix(activity) == "activities/2026/123"


def test_decode_fit_validates_through_decoder_instance(monkeypatch):
    calls = []

    class FakeStream:
        @staticmethod
        def from_byte_array(data):
            calls.append(("stream", bytes(data)))
            return object()

    class FakeDecoder:
        def __init__(self, stream):
            calls.append(("decoder", stream))

        def is_fit(self):
            calls.append(("is_fit", None))
            return True

        def read(self):
            calls.append(("read", None))
            return {"set_mesgs": [{"repetitions": 10}]}, []

    monkeypatch.setitem(
        sys.modules,
        "garmin_fit_sdk",
        types.SimpleNamespace(Decoder=FakeDecoder, Stream=FakeStream),
    )
    result = decode_fit(
        b"fit-bytes",
        activity={"activityId": 123, "activityType": {"typeKey": "strength_training"}},
    )
    assert [name for name, _ in calls] == ["stream", "decoder", "is_fit", "read"]
    assert result["normalized_strength_sets"] == [{"repetitions": 10}]


def test_normalize_strength_session_pairs_active_sets_with_following_rest():
    result = normalize_strength_session("123", {"exerciseSets": [
        {
            "exercises": [{"category": "PULL_UP", "probability": 100.0}],
            "duration": 30.173,
            "repetitionCount": 8,
            "weight": 0.0,
            "setType": "ACTIVE",
            "startTime": "2026-09-20T10:33:45.0",
            "messageIndex": 0,
            "avgConcentricMeanVelocity": None,
        },
        {"duration": 69.024, "setType": "REST", "messageIndex": 1},
        {
            "exercises": [{"category": "SQUAT", "name": "Back Squat", "probability": 95}],
            "duration": 66.317,
            "repetitionCount": 12,
            "weight": 60000.0,
            "setType": "ACTIVE",
            "messageIndex": 2,
            "avgConcentricMeanVelocity": 0.42,
            "tal_grit": float("nan"),
        },
    ]})

    assert len(result["sets"]) == 2
    assert result["sets"][0]["exercise"]["category"] == "PULL_UP"
    assert result["sets"][0]["rest_after_seconds"] == 69.024
    assert result["sets"][1]["performance_metrics"] == {
        "avgConcentricMeanVelocity": 0.42,
        "tal_grit": None,
    }
    assert result["summary"]["total_repetitions"] == 20
    assert result["summary"]["external_volume_kg"] == 720.0
    assert result["summary"]["by_exercise"]["Back Squat"] == {
        "sets": 1, "repetitions": 12, "external_volume_kg": 720.0,
    }
    assert result["summary"]["by_category"]["SQUAT"] == {
        "sets": 1, "repetitions": 12, "external_volume_kg": 720.0,
    }


def test_strength_summary_separates_specific_exercises_within_one_category():
    result = normalize_strength_session("24431147581", {"exerciseSets": [
        {
            "exercises": [{"category": "PULL_UP", "name": None, "probability": 100}],
            "repetitionCount": 8,
            "weight": 0,
            "setType": "ACTIVE",
            "messageIndex": 0,
        },
        {"duration": 60, "setType": "REST", "messageIndex": 1},
        {
            "exercises": [{
                "category": "PULL_UP", "name": "LAT_PULLDOWN", "probability": 100,
            }],
            "repetitionCount": 10,
            "weight": 60000,
            "setType": "ACTIVE",
            "messageIndex": 2,
        },
    ]})

    summary = result["summary"]
    assert summary["by_exercise"] == {
        "PULL_UP": {"sets": 1, "repetitions": 8, "external_volume_kg": 0.0},
        "LAT_PULLDOWN": {"sets": 1, "repetitions": 10, "external_volume_kg": 600.0},
    }
    assert summary["by_category"] == {
        "PULL_UP": {"sets": 2, "repetitions": 18, "external_volume_kg": 600.0},
    }


def test_hrv_history_bounds_accept_gzipped_health_summary():
    raw = (
        b"Date,HRV Last Night Average,Steps\n"
        b"2026-09-21,42,10000\n"
        b"2017-05-04,35,8000\n"
        b"2020-01-01,,9000\n"
        b"invalid,50,0\n"
    )
    assert hrv_history_bounds(gzip.compress(raw)) == (
        date(2017, 5, 4),
        date(2026, 9, 21),
    )
    assert hrv_history_dates(raw) == ["2026-09-21", "2017-05-04"]


def test_hrv_batch_is_newest_first_and_skips_existing_and_blocked_dates():
    existing = {hrv_key("2026-09-20")}
    failures = {"2026-09-19": {"attempts": 3}}
    assert select_hrv_batch(
        ["2026-09-21", "2026-09-20", "2026-09-19", "2026-09-18", "2026-09-17"],
        existing,
        failures,
        limit=2,
    ) == ["2026-09-21", "2026-09-18"]


def test_hrv_backfill_stores_newest_curves_and_progress():
    health = (
        b"Date,HRV Last Night Average,Steps\n"
        b"2026-09-19,38,1\n2026-09-20,40,2\n2026-09-21,42,3\n"
    )

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/health_daily.csv": gzip.compress(health)}
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append((key, content_type, encoding))

    class FakeGarmin:
        def __init__(self):
            self.calls = []

        def get_hrv_data(self, day):
            self.calls.append(day)
            return {"hrvReadings": [{"readingTimeGMT": day, "hrvValue": 42}]}

    store = FakeStore()
    garmin = FakeGarmin()
    result = run_hrv_backfill(max_days=2, store=store, garmin=garmin)

    assert garmin.calls == ["2026-09-21", "2026-09-20"]
    assert result["complete_days"] == 2
    assert result["remaining_days"] == 1
    curve = json.loads(gzip.decompress(store.objects[hrv_key("2026-09-21")]))
    assert curve["available"] is True
    assert curve["reading_count"] == 1
    assert hrv_key("2026-09-20") in store.objects
    assert store.puts[-1][0] == HRV_PLAN_KEY


def test_hrv_backfill_stores_unavailable_marker_for_valid_empty_response():
    health = b"Date,HRV Last Night Average\n2026-09-20,40\n"

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/health_daily.csv": health}

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    class FakeGarmin:
        def get_hrv_data(self, day):
            return {"hrvSummary": {"calendarDate": day}, "hrvReadings": []}

    store = FakeStore()
    result = run_hrv_backfill(max_days=1, store=store, garmin=FakeGarmin())

    assert result["status"] == "complete"
    assert result["complete_days"] == 1
    assert result["failed_this_run"] == []
    marker = json.loads(gzip.decompress(store.objects[hrv_key("2026-09-20")]))
    assert marker["available"] is False
    assert marker["reading_count"] == 0
    assert marker["unavailable_reason"] == "garmin_returned_no_detailed_readings"


def test_completed_hrv_plan_is_passive():
    plan = {
        "status": "complete",
        "history_start": "2026-09-20",
        "history_end": "2026-09-21",
        "target_dates": ["2026-09-21", "2026-09-20"],
    }
    health = (
        b"Date,HRV Last Night Average\n"
        b"2026-09-21,42\n2026-09-20,40\n"
    )

    class FakeStore:
        def __init__(self):
            self.gets = []
            self.puts = []

        def list_keys(self, prefix=""):
            assert prefix == "backfill/hrv/"
            return {HRV_PLAN_KEY}

        def get(self, key):
            self.gets.append(key)
            if key == HRV_PLAN_KEY:
                return json.dumps(plan).encode()
            return health

        def put(self, *args, **kwargs):
            self.puts.append((args, kwargs))

    store = FakeStore()
    result = run_hrv_backfill(max_days=50, store=store)
    assert result == plan
    assert store.gets == [HRV_PLAN_KEY, "summary/health_daily.csv"]
    assert store.puts == []


def test_hrv_backfill_prioritizes_a_new_date_while_history_is_active():
    plan = {
        "status": "active",
        "history_start": "2026-09-19",
        "history_end": "2026-09-20",
        "target_dates": ["2026-09-20", "2026-09-19"],
        "failures": {},
    }
    health = (
        b"Date,HRV Last Night Average\n"
        b"2026-09-21,42\n2026-09-20,40\n2026-09-19,38\n"
    )

    class FakeStore:
        def __init__(self):
            self.objects = {
                HRV_PLAN_KEY: json.dumps(plan).encode(),
                "summary/health_daily.csv": health,
                hrv_key("2026-09-20"): b"existing",
            }

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    class FakeGarmin:
        def __init__(self):
            self.calls = []

        def get_hrv_data(self, day):
            self.calls.append(day)
            return {"hrvReadings": [{"readingTimeGMT": day, "hrvValue": 42}]}

    store = FakeStore()
    garmin = FakeGarmin()
    result = run_hrv_backfill(max_days=1, store=store, garmin=garmin)

    assert garmin.calls == ["2026-09-21"]
    assert result["target_dates"][0] == "2026-09-21"
    assert result["history_end"] == "2026-09-21"


def test_normalize_sleep_detail_keeps_stages_and_score_components():
    result = normalize_sleep_detail("2026-09-21", {
        "dailySleepDTO": {
            "sleepTimeSeconds": 25200,
            "deepSleepSeconds": 4200,
            "lightSleepSeconds": 15000,
            "remSleepSeconds": 6000,
            "sleepStartTimestampGMT": 1789941600000,
            "sleepEndTimestampGMT": 1789966800000,
            "sleepStartTimestampLocal": 1789948800000,
            "sleepEndTimestampLocal": 1789974000000,
            "sleepWindowConfirmed": 1,
            "sleepScores": {
                "overall": {"value": 84, "qualifierKey": "GOOD"},
                "duration": {"value": 90, "qualifierKey": "EXCELLENT"},
            },
        },
        "sleepLevels": [
            {"startGMT": 1, "endGMT": 2, "activityLevel": "deep"},
            {"startGMT": 2, "endGMT": 3, "activityLevel": "light"},
        ],
    })

    assert result["summary"]["sleep_score"] == 84
    assert result["sleep_start_garmin_local"] == 1789948800000
    assert result["sleep_end_garmin_local"] == 1789974000000
    assert result["confirmed"] is True
    assert result["stage_count"] == 2
    assert result["score_breakdown"]["duration"] == {
        "value": 90,
        "qualifier": "EXCELLENT",
    }


def test_normalize_body_composition_preserves_each_weigh_in_and_units():
    result = normalize_body_composition("2026-09-21", {
        "dateWeightList": [
            {
                "calendarDate": "2026-09-21",
                "allWeightMetrics": [
                    {
                        "weight": 81234,
                        "bmi": 24.3,
                        "bodyFat": 18.5,
                        "muscleMass": 60123,
                        "boneMass": 3210,
                        "samplePk": 42,
                    },
                    {
                        "weight": 81100,
                        "timestampGMT": 1789966800000,
                    },
                ],
            },
        ],
    })

    assert result["measurement_count"] == 2
    assert result["measurements"][0]["weight_kg"] == 81.234
    assert result["measurements"][0]["muscle_mass_kg"] == 60.123
    assert result["measurements"][0]["bone_mass_kg"] == 3.21
    assert result["measurements"][0]["measurement_id"] == 42
    assert result["measurements"][0]["is_daily_average"] is False

    average = normalize_body_composition("2026-09-21", {
        "totalAverage": {"weight": 81000, "bodyFat": 18.2},
    })
    assert average["measurements"][0]["weight_kg"] == 81
    assert average["measurements"][0]["is_daily_average"] is True


def test_health_detail_dates_only_include_populated_summary_days():
    summary = (
        b"Date,Sleep Seconds,Weight KG\n"
        b"2026-09-19,25000,\n"
        b"2026-09-20,,80.1\n"
        b"2026-09-21,26000,80.0\n"
    )
    assert health_detail_dates(summary, "Sleep Seconds") == [
        "2026-09-21", "2026-09-19",
    ]
    assert health_detail_dates(summary, "Weight KG") == [
        "2026-09-21", "2026-09-20",
    ]


def test_health_detail_backfill_writes_bounded_sleep_and_body_batches():
    summary = (
        b"Date,Sleep Seconds,Weight KG\n"
        b"2026-09-20,25000,\n"
        b"2026-09-21,26000,80.0\n"
    )

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/health_daily.csv": summary}
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append((key, content_type, encoding))

    class FakeGarmin:
        def __init__(self):
            self.sleep_calls = []
            self.body_calls = []

        def get_sleep_data(self, day):
            self.sleep_calls.append(day)
            return {"dailySleepDTO": {"sleepTimeSeconds": 26000}}

        def get_body_composition(self, start, end=None):
            self.body_calls.append((start, end))
            return {
                "dateWeightList": [
                    {"calendarDate": start, "weight": 80000},
                ]
            }

    store = FakeStore()
    garmin = FakeGarmin()
    result = run_health_detail_backfill(
        max_sleep_days=1,
        max_body_days=1,
        store=store,
        garmin=garmin,
    )

    assert garmin.sleep_calls == ["2026-09-21"]
    assert garmin.body_calls == [("2026-09-21", "2026-09-21")]
    assert result["sleep"]["remaining_days"] == 1
    assert result["body_composition"]["remaining_days"] == 0
    sleep_key = HEALTH_DETAIL_STREAMS["sleep"].object_key("2026-09-21")
    body_key = HEALTH_DETAIL_STREAMS["body_composition"].object_key("2026-09-21")
    assert json.loads(gzip.decompress(store.objects[sleep_key]))["stage_count"] == 0
    assert json.loads(gzip.decompress(store.objects[body_key]))["measurement_count"] == 1


def test_body_composition_backfill_fetches_sparse_dates_in_one_range():
    summary = (
        b"Date,Sleep Seconds,Weight KG\n"
        b"2026-09-01,,80.2\n"
        b"2026-09-10,,80.1\n"
        b"2026-09-21,,80.0\n"
    )

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/health_daily.csv": summary}

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    class FakeGarmin:
        def __init__(self):
            self.calls = []

        def get_body_composition(self, start, end):
            self.calls.append((start, end))
            return {
                "dateWeightList": [
                    {"calendarDate": day, "weight": 80000}
                    for day in ("2026-09-01", "2026-09-10", "2026-09-21")
                ]
            }

    store = FakeStore()
    garmin = FakeGarmin()
    result = run_health_detail_backfill(
        max_sleep_days=1,
        max_body_days=3,
        store=store,
        garmin=garmin,
    )

    assert garmin.calls == [("2026-09-01", "2026-09-21")]
    assert result["body_composition"]["complete_days"] == 3
    assert result["body_composition"]["remaining_days"] == 0


def test_completed_health_detail_backfills_do_not_contact_garmin_or_write():
    summary = b"Date,Sleep Seconds,Weight KG\n2026-09-21,26000,80.0\n"
    plans = {}
    for stream in HEALTH_DETAIL_STREAMS.values():
        plans[stream.plan_key] = json.dumps({
            "status": "complete",
            "target_dates": ["2026-09-21"],
            "failures": {},
            "attempted_this_run": 95,
            "completed_this_run": [{"date": "2026-09-21"}],
        }).encode()

    class FakeStore:
        def __init__(self):
            self.objects = {"summary/health_daily.csv": summary, **plans}
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, *args, **kwargs):
            self.puts.append((args, kwargs))

    class NoGarmin:
        def __getattr__(self, name):
            raise AssertionError(f"Garmin must not be contacted through {name}")

    store = FakeStore()
    result = run_health_detail_backfill(store=store, garmin=NoGarmin())

    assert result["sleep"]["status"] == "complete"
    assert result["body_composition"]["status"] == "complete"
    assert result["sleep"]["attempted_this_run"] == 0
    assert result["body_composition"]["attempted_this_run"] == 0
    assert result["body_composition"]["completed_this_run"] == []
    assert store.puts == []


def test_completed_health_details_can_refresh_recent_existing_dates():
    summary = b"Date,Sleep Seconds,Weight KG\n2026-09-21,26000,80.0\n"
    plans = {}
    objects = {"summary/health_daily.csv": summary}
    for stream in HEALTH_DETAIL_STREAMS.values():
        plans[stream.plan_key] = json.dumps({
            "status": "complete",
            "target_dates": ["2026-09-21"],
            "failures": {},
        }).encode()
        objects[stream.object_key("2026-09-21")] = gzip_json({"old": True})
    objects.update(plans)

    class FakeStore:
        def __init__(self):
            self.objects = objects
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append(key)

    class FakeGarmin:
        def __init__(self):
            self.sleep_calls = []
            self.body_calls = []

        def get_sleep_data(self, day):
            self.sleep_calls.append(day)
            return {"dailySleepDTO": {"sleepTimeSeconds": 27000}}

        def get_body_composition(self, start, end):
            self.body_calls.append((start, end))
            return {"dateWeightList": [{"calendarDate": start, "weight": 79500}]}

    store = FakeStore()
    garmin = FakeGarmin()
    result = run_health_detail_backfill(
        max_sleep_days=3,
        max_body_days=3,
        refresh_recent_days=3,
        store=store,
        garmin=garmin,
    )

    assert garmin.sleep_calls == ["2026-09-21"]
    assert garmin.body_calls == [("2026-09-21", "2026-09-21")]
    assert result["sleep"]["refreshed_existing_this_run"] == 1
    assert result["body_composition"]["refreshed_existing_this_run"] == 1
    assert result["sleep"]["status"] == "complete"
    assert result["body_composition"]["status"] == "complete"


def test_activity_fingerprint_changes_when_garmin_metadata_changes():
    activity = {
        "activityId": 123,
        "activityName": "Easy run",
        "activityType": {"typeKey": "running"},
        "distance": 5000,
    }
    edited = {**activity, "activityName": "Easy run - edited"}
    assert activity_fingerprint(activity) == activity_fingerprint(dict(activity))
    assert activity_fingerprint(activity) != activity_fingerprint(edited)


def test_activity_refresh_baselines_existing_artifacts_without_downloads(tmp_path):
    activity = {
        "activityId": 123,
        "activityType": {"typeKey": "running"},
        "startTimeGMT": "2026-09-21 08:00:00",
    }

    class FakeStore:
        def __init__(self):
            self.objects = {key: b"existing" for key in activity_artifact_keys(activity)}
            self.puts = []

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append(key)

    class NoDownloads:
        def download_activity(self, *args, **kwargs):
            raise AssertionError("existing artifacts must not be downloaded")

    store = FakeStore()
    existing = set(store.objects)
    result = refresh_activity(
        activity,
        garmin=NoDownloads(),
        store=store,
        existing_keys=existing,
        output=tmp_path,
    )

    assert result["status"] == "baseline"
    assert store.puts == [manifest_key(activity)]


def test_activity_refresh_replaces_artifacts_after_source_change(monkeypatch, tmp_path):
    original = {
        "activityId": 123,
        "activityName": "Run",
        "activityType": {"typeKey": "running"},
        "startTimeGMT": "2026-09-21 08:00:00",
    }
    changed = {**original, "activityName": "Renamed run"}
    key = manifest_key(changed)
    previous = {
        "activity_fingerprint": activity_fingerprint(original),
        "exercise_sets_fingerprint": None,
    }

    class FakeStore:
        def __init__(self):
            self.objects = {
                **{item: b"old" for item in activity_artifact_keys(changed)},
                key: json.dumps(previous).encode(),
            }

        def get(self, name):
            return self.objects[name]

        def put(self, name, data, content_type, *, encoding=None):
            self.objects[name] = data

    calls = []

    def fake_export(activity, garmin, store, output, **kwargs):
        calls.append(kwargs)
        return {"files": [{"key": "replacement"}]}

    monkeypatch.setattr("pipeline.activity_refresh.export_activity", fake_export)
    store = FakeStore()
    result = refresh_activity(
        changed,
        garmin=object(),
        store=store,
        existing_keys=set(store.objects),
        output=tmp_path,
    )

    assert result == {"id": "123", "status": "refreshed", "reason": "source_changed"}
    assert calls[0]["force"] is True


def test_forced_activity_refresh_requires_an_explicit_id():
    try:
        run_activity_refresh(force=True, store=object(), garmin=object())
    except ValueError as exc:
        assert str(exc) == "force requires an explicit activity_id"
    else:
        raise AssertionError("force without activity_id should fail")


def _coach_profile(profile_id="hr-test", effective_from="2016-09-15"):
    return {
        "schema_version": 1,
        "profile_id": profile_id,
        "name": "Example running profile",
        "sport": "running",
        "effective_from": effective_from,
        "default_time_basis": "elapsed",
        "zones": [
            {"label": "Z1", "min_bpm": 105, "max_bpm": 132},
            {"label": "Z2", "min_bpm": 133, "max_bpm": 153},
            {"label": "Z3", "min_bpm": 154, "max_bpm": 164},
            {"label": "Z4", "min_bpm": 165, "max_bpm": 175},
            {"label": "Z5", "min_bpm": 176, "max_bpm": 188},
        ],
        "references": {
            "lt1_min_bpm": 148, "lt1_max_bpm": 152,
            "lt2_min_bpm": 166, "lt2_max_bpm": 169,
            "interval_thresholds_bpm": [165, 168, 170, 172, 175],
        },
        "created_at": "2026-09-22T10:00:00Z",
        "source": "user",
    }


def test_coach_profile_selection_preserves_historical_effective_dates():
    old = _coach_profile("old", "2016-09-15")
    new = _coach_profile("new", "2026-09-01")
    assert select_profile([old, new], "2026-08-31")["profile_id"] == "old"
    assert select_profile([old, new], "2026-09-01")["profile_id"] == "new"


def test_coach_input_uses_elapsed_hr_zones_and_keeps_plan_separate_from_execution():
    decoded = {
        "source_fit_sha256": "fit-hash",
        "messages": {
            "workout_mesgs": [{"wkt_name": "6 x 3 min"}],
            "workout_step_mesgs": [
                {"message_index": 0, "wkt_step_name": "Warm up", "intensity": "warmup"},
                {"message_index": 1, "wkt_step_name": "Run", "intensity": "active",
                 "duration_type": "time", "duration_value": 180000},
            ],
            "lap_mesgs": [
                {"wkt_step_index": 0, "total_elapsed_time": 600, "avg_heart_rate": 140},
                {"wkt_step_index": 1, "total_elapsed_time": 180, "avg_heart_rate": 166,
                 "max_heart_rate": 171, "total_distance": 800},
            ],
        },
    }
    endurance = {
        "activity": {"id": "123", "name": "Intervals", "type": "running"},
        "summary": {
            "duration_seconds": 780, "distance_m": 3000,
            "average_speed_mps": 3.85, "average_heart_rate_bpm": 155,
            "maximum_heart_rate_bpm": 171, "trackpoint_count": 781,
            "sampled_trackpoint_count": 79, "aerobic_decoupling_percent": 1.2,
        },
        "heart_rate_seconds_by_bpm": {"132": 60, "150": 300, "166": 300, "176": 120},
        "distance_halves": {
            "first": {"average_heart_rate_bpm": 153},
            "second": {"average_heart_rate_bpm": 157},
        },
        "kilometer_splits": [{"split": 1}],
    }
    result = build_coach_input(
        activity={"id": "123", "date": "2026-09-21", "name": "Intervals",
                  "type": "Run", "moving_seconds": 770},
        decoded_fit=decoded, endurance=endurance, profile=_coach_profile(),
        tcx_sha256="tcx-hash", context={"context_id": "ctx-1", "rpe": 7},
    )
    zones = {row["label"]: row["seconds"] for row in result["heart_rate"]["zones_total"]["zones"]}
    assert zones == {"Z1": 60.0, "Z2": 300.0, "Z3": 0.0, "Z4": 300.0, "Z5": 120.0}
    assert result["heart_rate"]["seconds_at_or_above_bpm"]["170"] == 120.0
    assert result["heart_rate"]["first_to_second_distance_half_drift_bpm"] == 4.0
    assert result["workout_structure"]["planned_steps"][1]["name"] == "Run"
    assert result["workout_structure"]["executed_laps"][1]["distance_m"] == 800
    assert [section["section"] for section in result["sections"]] == ["warmup", "main"]
    assert result["user_context"]["rpe"] == 7


def test_coach_backfill_uses_only_r2_and_becomes_passive_when_complete():
    summary = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-123,2026-09-21 08:00:00,Easy run,Run,1800\n"
    )
    prefix = "activities/2026/123"
    objects = {
        "summary/activities.csv": summary,
        "coach/profiles/v1/2016-09-15/hr-test.json": json.dumps(_coach_profile()).encode(),
        f"{prefix}/activity.v1.json": gzip.compress(json.dumps({
            "source_fit_sha256": "fit", "messages": {},
        }).encode()),
        f"{prefix}/activity.endurance.v1.json": gzip.compress(json.dumps({
            "activity": {"id": "123"},
            "summary": {"duration_seconds": 1800, "distance_m": 6000},
            "heart_rate_seconds_by_bpm": {"150": 1800},
            "kilometer_splits": [],
        }).encode()),
        f"{prefix}/activity.tcx": gzip.compress(b"<TrainingCenterDatabase/>"),
    }

    class FakeStore:
        def __init__(self):
            self.objects = dict(objects)
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append(key)

    store = FakeStore()
    first = run_coach_backfill(max_activities=25, store=store)
    assert first["status"] == "complete"
    assert first["attempted_this_run"] == 1
    coach_key = first["completed_this_run"][0]["key"]
    analysis = json.loads(gzip.decompress(store.objects[coach_key]))
    assert analysis["profile"]["profile_id"] == "hr-test"
    assert analysis["heart_rate"]["zones_total"]["zones"][1]["seconds"] == 1800.0

    store.puts.clear()
    second = run_coach_backfill(max_activities=25, store=store)
    assert second == first
    assert store.puts == []
    assert COACH_PLAN_KEY in store.objects


def test_coach_backfill_skips_explicitly_unavailable_endurance_data():
    summary = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-123,2026-09-21 08:00:00,Empty run,Run,0\n"
    )
    prefix = "activities/2026/123"
    objects = {
        "summary/activities.csv": summary,
        "coach/profiles/v1/2016-09-15/hr-test.json": json.dumps(
            _coach_profile()
        ).encode(),
        f"{prefix}/activity.v1.json": gzip.compress(json.dumps({
            "source_fit_sha256": "fit", "messages": {},
        }).encode()),
        f"{prefix}/activity.endurance.v1.json": gzip.compress(json.dumps({
            "available": False,
            "reason": "no_timed_trackpoints",
            "activity": {"id": "123"},
            "summary": {"trackpoint_count": 0},
        }).encode()),
        f"{prefix}/activity.tcx": gzip.compress(b"<TrainingCenterDatabase/>"),
    }

    class FakeStore:
        def __init__(self):
            self.objects = dict(objects)

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data

    store = FakeStore()
    result = run_coach_backfill(max_activities=25, store=store)

    assert result["status"] == "complete"
    assert result["attempted_this_run"] == 1
    assert result["generated_this_run"] == 0
    assert result["skipped_this_run"] == [{
        "activity_id": "123",
        "reason": "no_timed_trackpoints",
    }]
    assert result["blocked_activity_count"] == 0
    assert not any(
        key.startswith(f"{prefix}/coach-input/") for key in store.objects
    )


def test_coach_backfill_reads_only_new_activity_sources_after_initial_index():
    def activity_objects(activity_id: str):
        prefix = f"activities/2026/{activity_id}"
        return {
            f"{prefix}/activity.v1.json": gzip.compress(
                json.dumps({
                    "source_fit_sha256": f"fit-{activity_id}",
                    "messages": {},
                }).encode()
            ),
            f"{prefix}/activity.endurance.v1.json": gzip.compress(
                json.dumps({
                    "activity": {"id": activity_id},
                    "summary": {"duration_seconds": 1800, "distance_m": 6000},
                    "heart_rate_seconds_by_bpm": {"150": 1800},
                    "kilometer_splits": [],
                }).encode()
            ),
            f"{prefix}/activity.tcx": gzip.compress(b"<TrainingCenterDatabase/>"),
        }

    class TrackingStore:
        def __init__(self):
            self.objects = {
                "summary/activities.csv": (
                    b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
                    b"garmin-123,2026-09-20 08:00:00,First run,Run,1800\n"
                ),
                "coach/profiles/v1/2016-09-15/hr-test.json": json.dumps(
                    _coach_profile()
                ).encode(),
                **activity_objects("123"),
            }
            self.gets = []
            self.puts = []

        def list_keys(self, prefix=""):
            return {key for key in self.objects if key.startswith(prefix)}

        def list_object_revisions(self, prefix=""):
            return {
                key: hashlib.sha256(value).hexdigest()
                for key, value in self.objects.items()
                if key.startswith(prefix)
            }

        def get(self, key):
            self.gets.append(key)
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.puts.append(key)

    store = TrackingStore()
    first = run_coach_backfill(max_activities=50, store=store)
    assert first["processed_source_count"] == 1

    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-124,2026-09-21 08:00:00,New run,Run,1800\n"
        b"garmin-123,2026-09-20 08:00:00,First run,Run,1800\n"
    )
    store.objects.update(activity_objects("124"))
    store.gets.clear()
    store.puts.clear()

    second = run_coach_backfill(max_activities=50, store=store)

    assert second["attempted_this_run"] == 1
    assert second["processed_source_count"] == 2
    assert all("activities/2026/123/activity" not in key for key in store.gets)
    assert {
        "activities/2026/124/activity.v1.json",
        "activities/2026/124/activity.endurance.v1.json",
        "activities/2026/124/activity.tcx",
    }.issubset(store.gets)

    store.objects["activities/2026/123/source-manifest.v1.json"] = json.dumps(
        {"activity_id": "123", "activity_fingerprint": "changed"}
    ).encode()
    store.gets.clear()
    store.puts.clear()

    changed = run_coach_backfill(max_activities=50, store=store)

    assert changed["attempted_this_run"] == 1
    assert any("activities/2026/123/activity" in key for key in store.gets)
    assert all("activities/2026/124/activity" not in key for key in store.gets)


def test_local_bootstrap_terminal_status_handles_health_and_waiting_profile():
    assert phase_is_terminal("activity", {"status": "complete"})
    assert phase_is_terminal("coach", {"status": "waiting_for_profile"})
    assert phase_is_terminal(
        "health",
        {
            "sleep": {"status": "complete"},
            "body_composition": {"status": "complete_with_blocked"},
        },
    )
    assert not phase_is_terminal(
        "health",
        {
            "sleep": {"status": "active"},
            "body_composition": {"status": "complete"},
        },
    )


def test_local_bootstrap_compact_result_uses_full_blocked_activity_count():
    result = compact_result(
        "coach",
        {
            "status": "active",
            "blocked_activity_count": 449,
            "blocked_activities": [{"activity_id": str(index)} for index in range(100)],
        },
    )

    assert result["blocked_activities"] == 449


def test_local_bootstrap_cycle_reuses_garmin_and_separates_r2_budgets(monkeypatch):
    stores = []
    garmin = object()

    def store_factory():
        store = object()
        stores.append(store)
        return store

    monkeypatch.setattr(
        "pipeline.local_bootstrap.run_activity_backfill",
        lambda **kwargs: {"status": "complete", "store": kwargs["store"]},
    )
    monkeypatch.setattr(
        "pipeline.local_bootstrap.run_hrv_backfill",
        lambda **kwargs: {"status": "complete", "store": kwargs["store"]},
    )
    monkeypatch.setattr(
        "pipeline.local_bootstrap.run_health_detail_backfill",
        lambda **kwargs: {
            "sleep": {"status": "complete"},
            "body_composition": {"status": "complete"},
            "store": kwargs["store"],
        },
    )
    monkeypatch.setattr(
        "pipeline.local_bootstrap.run_coach_backfill",
        lambda **kwargs: {"status": "complete", "store": kwargs["store"]},
    )

    result = run_cycle(
        phases=("activity", "hrv", "health", "coach"),
        garmin=garmin,
        store_factory=store_factory,
    )

    assert len(stores) == 4
    assert len({id(store) for store in stores}) == 4
    assert result["activity"]["store"] is stores[0]
    assert result["coach"]["store"] is stores[3]
    assert result["activity"]["duration_seconds"] >= 0
    assert result["health"]["duration_seconds"] >= 0
    assert result["coach"]["duration_seconds"] >= 0


def test_local_bootstrap_stops_without_sleep_when_all_phases_complete(tmp_path):
    sleeps = []

    def cycle_runner(**kwargs):
        return {
            "activity": {"status": "complete"},
            "coach": {"status": "complete_with_blocked"},
        }

    result = run_local_bootstrap(
        phases=("activity", "coach"),
        max_hours=1,
        status_file=tmp_path / "status.json",
        lock_file=tmp_path / "lock",
        garmin=object(),
        cycle_runner=cycle_runner,
        monotonic=lambda: 0,
        sleep=sleeps.append,
    )

    assert result["status"] == "complete"
    assert result["cycles"] == 1
    assert sleeps == []
    assert all_phases_terminal(
        ("activity", "coach"),
        {
            "activity": {"status": "complete"},
            "coach": {"status": "complete_with_blocked"},
        },
    )
    assert json.loads((tmp_path / "status.json").read_text())["status"] == "complete"


def test_local_bootstrap_loads_env_without_overriding_existing(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FIRST=from-file\nSECOND='quoted value'\n", encoding="utf-8"
    )
    monkeypatch.setenv("FIRST", "existing")
    monkeypatch.delenv("SECOND", raising=False)

    load_env_file(env_file)

    assert os.environ["FIRST"] == "existing"
    assert os.environ["SECOND"] == "quoted value"


def test_local_bootstrap_records_safe_stop_before_reraising(tmp_path):
    def broken_cycle(**kwargs):
        raise RuntimeError("Garmin rate limit")

    try:
        run_local_bootstrap(
            phases=("activity",),
            max_hours=1,
            status_file=tmp_path / "status.json",
            lock_file=tmp_path / "lock",
            garmin=object(),
            cycle_runner=broken_cycle,
            monotonic=lambda: 0,
        )
    except RuntimeError as exc:
        assert str(exc) == "Garmin rate limit"
    else:
        raise AssertionError("the original stopping error must be reraised")

    status = json.loads((tmp_path / "status.json").read_text())
    assert status["status"] == "stopped_error"
    assert status["cycles"] == 1
    assert status["error"]["type"] == "RuntimeError"


def test_health_history_hrv_summary_derives_distribution_and_slope():
    result = summarize_hrv_payload("2026-09-20", {
        "sleep_start_gmt": "2026-09-19T22:00:00Z",
        "sleep_end_gmt": "2026-09-20T06:00:00Z",
        "sleep_start_garmin_local": "2026-09-20T00:00:00.0",
        "sleep_end_garmin_local": "2026-09-20T08:00:00.0",
        "summary": {
            "lastNightAvg": 42,
            "lastNight5MinHigh": 61,
            "weeklyAvg": 44,
            "status": "BALANCED",
        },
        "readings": [
            {"timestamp": "2026-09-20T00:00:00Z", "hrv_ms": 30},
            {"timestamp": "2026-09-20T01:00:00Z", "hrv_ms": 40},
            {"timestamp": "2026-09-20T02:00:00Z", "hrv_ms": 50},
            {"timestamp": "2026-09-20T03:00:00Z", "hrv_ms": 60},
        ],
    })

    assert result["status"] == "available"
    assert result["sleep_start_garmin_local"] == "2026-09-20T00:00:00.0"
    assert result["sleep_end_garmin_local"] == "2026-09-20T08:00:00.0"
    assert result["detailed_readings_available"] is True
    assert result["garmin"] == {
        "last_night_avg_ms": 42,
        "last_night_5_min_high_ms": 61,
        "weekly_avg_ms": 44,
        "status": "BALANCED",
        "baseline_low_ms": None,
        "baseline_high_ms": None,
    }
    assert result["derived"]["median_ms"] == 45
    assert result["derived"]["second_minus_first_ms"] == 20
    assert result["derived"]["slope_ms_per_hour"] == 10


def test_health_history_hrv_summary_is_available_without_detailed_readings():
    result = summarize_hrv_payload("2026-03-23", {
        "available": False,
        "unavailable_reason": "garmin_returned_no_detailed_readings",
        "summary": {
            "lastNightAvg": 39,
            "lastNight5MinHigh": 66,
            "weeklyAvg": 40,
            "status": "BALANCED",
        },
        "readings": [],
    })

    assert result["status"] == "available"
    assert result["detailed_readings_available"] is False
    assert result["garmin"]["last_night_avg_ms"] == 39
    assert result["garmin"]["last_night_5_min_high_ms"] == 66
    assert result["derived"]["valid_reading_count"] == 0
    assert result["derived"]["mean_ms"] is None


def test_health_history_hrv_without_summary_or_readings_is_no_data():
    result = summarize_hrv_payload("2026-03-22", {
        "available": False,
        "summary": {},
        "readings": [],
    })

    assert result == {"date": "2026-03-22", "status": "no_data"}


def test_health_history_month_index_is_incremental_and_gzip_compressed():
    day_key = "health/hrv/2026/09/2026-09-20.json"

    class FakeStore:
        def __init__(self):
            self.objects = {
                day_key: gzip_json({
                    "date": "2026-09-20",
                    "sleep_start_garmin_local": "2026-09-20T00:00:00.0",
                    "summary": {"lastNightAvg": 42},
                    "readings": [{"timestamp": 1, "hrv_ms": 42}],
                }),
            }
            self.revisions = {day_key: "source-v1"}
            self.puts = []

        def list_object_revisions(self, prefix):
            return {
                key: revision for key, revision in self.revisions.items()
                if key.startswith(prefix)
            }

        def get(self, key):
            return self.objects[key]

        def put(self, key, data, content_type, *, encoding=None):
            self.objects[key] = data
            self.revisions[key] = f"written-{len(self.puts) + 1}"
            self.puts.append((key, content_type, encoding))

    store = FakeStore()
    first = sync_health_history_stream("hrv", store=store)
    key = health_history_index_key("hrv", "2026-09")
    payload = json.loads(gzip.decompress(store.objects[key]))

    assert first["months_written"][0]["days"] == 1
    assert store.puts == [(key, "application/json", "gzip")]
    assert payload["builder_revision"] == 2
    assert payload["days"][0]["garmin"]["last_night_avg_ms"] == 42
    assert payload["days"][0]["sleep_start_garmin_local"] == "2026-09-20T00:00:00.0"

    second = sync_health_history_stream("hrv", store=store)
    assert second["months_written"] == []
    assert second["months_unchanged"] == ["2026-09"]
    assert len(store.puts) == 1


def test_health_history_sleep_index_does_not_copy_stage_timeline():
    class Store:
        def get(self, key):
            return gzip_json({
                "date": "2026-09-20",
                "sleep_start_garmin_local": 1789948800000,
                "summary": {"sleep_seconds": 28800, "sleep_score": 85},
                "score_breakdown": {
                    "duration": {"value": 90, "qualifier": "EXCELLENT"},
                },
                "stages": [{"start_gmt": 1, "end_gmt": 2, "stage": "deep"}],
                "stage_count": 1,
            })

    payload = build_month_index(
        "sleep",
        "2026-09",
        {"2026-09-20": ("sleep.json", "v1")},
        Store(),
    )

    assert payload["days"][0]["stage_count"] == 1
    assert payload["days"][0]["sleep_start_garmin_local"] == 1789948800000
    assert "stages" not in payload["days"][0]
