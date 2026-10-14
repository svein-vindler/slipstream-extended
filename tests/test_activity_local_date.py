"""Garmin calendar context must join producer storage and coach profile dates."""
import csv
import gzip
import hashlib
import json
from datetime import date

import pytest

from pipeline.coach_backfill import run, run_one
from pipeline.granular import activity_prefix
from pipeline.latest_activity import run as latest_activity
from pipeline.manual_activity_refresh import (
    _coach_metadata,
    _complete_activity_details,
    _missing_artifacts,
    _recent,
)
from pipeline.schema import activity_local_date
from pipeline.sources.garmin import fetch
from pipeline.writer import write_dataset
from scripts.benchmark_coach_reuse import CountingStore


def _calendar_fixture(store, key):
    decoded = json.loads(store.objects[key])
    decoded.update(schema_version=1, source="garmin-fit")
    return decoded


def test_bulk_coach_uses_producer_local_year_and_new_year_profile(tmp_path):
    raw = {"activityId": 1, "startTimeGMT": "2026-12-31 23:30:00",
           "startTimeLocal": "2027-01-01 00:30:00", "activityType": {"typeKey": "running"}}

    class Provider:
        def get_activities_by_date(self, *_args):
            return [raw]

    write_dataset(fetch(client=Provider(), end_date=date(2027, 1, 1)), str(tmp_path))
    prefix = activity_prefix(raw)
    assert prefix == "activities/2027/1"
    store = CountingStore()
    store.objects = {key.replace("activities/2026/1/", prefix + "/"): value
                     for key, value in store.objects.items()}
    decoded = _calendar_fixture(store, prefix + "/activity.v1.json")
    decoded["activity"].update({"start_time_gmt": raw["startTimeGMT"], "start_time_local": raw["startTimeLocal"]})
    store.objects[prefix + "/activity.v1.json"] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (tmp_path / "activities.csv").read_bytes()
    store.objects["coach/profiles/v1/2027-01-01/new-year.json"] = json.dumps({
        "profile_id": "new-year-profile", "effective_from": "2027-01-01",
        "zones": [{"label": "test", "min_bpm": 120, "max_bpm": 160}],
    }).encode()

    result = run(max_activities=1, store=store)
    assert result["generated_this_run"] == 1
    pointer = json.loads(store.objects[prefix + "/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["activity"]["date"] == "2027-01-01"
    assert analysis["profile"]["profile_id"] == "new-year-profile"


@pytest.mark.parametrize("local", [None, "invalid", "2027-02-30 00:30:00", {"date": "2027-01-01"}])
def test_unknown_local_clock_has_no_local_claim_and_consistent_utc_storage_fallback(tmp_path, local):
    raw = {"activityId": 1, "startTimeGMT": "2026-12-31 23:30:00",
           "startTimeLocal": local, "activityType": {"typeKey": "running"}}

    class Provider:
        def get_activities_by_date(self, *_args):
            return [raw]

    write_dataset(fetch(client=Provider(), end_date=date(2027, 1, 1)), str(tmp_path))
    with (tmp_path / "activities.csv").open(newline="", encoding="utf-8") as stream:
        row = next(csv.DictReader(stream))
    assert row["Activity Local Date"] == ""
    assert row["Activity Date"] == "2026-12-31 23:30:00"
    assert activity_prefix(raw) == "activities/2026/1"


def test_legacy_bulk_resolves_local_year_only_for_bounded_pending_work():
    store = CountingStore()
    store.objects = {key.replace("activities/2026/1/", "activities/2027/1/"): value
                     for key, value in store.objects.items()}
    key = "activities/2027/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["activity"].update(start_time_gmt="2026-12-31 23:30:00", start_time_local="2027-01-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-12-31 23:30:00,Synthetic run,Run,1800\n")
    store.objects["coach/profiles/v1/2027-01-01/new-year.json"] = json.dumps({
        "profile_id": "new-year-profile", "effective_from": "2027-01-01",
        "zones": [{"label": "test", "min_bpm": 120, "max_bpm": 160}],
    }).encode()
    result = run(max_activities=1, store=store)
    assert result["generated_this_run"] == 1
    pointer = json.loads(store.objects["activities/2027/1/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["activity"]["date"] == "2027-01-01"
    assert analysis["profile"]["profile_id"] == "new-year-profile"
    store.reset_counts()
    assert run(max_activities=1, store=store) == result
    assert store.counts["put"] == 0
    assert store.counts["download_bytes"] < 20_000


def test_targeted_coach_preserves_latest_context_from_old_utc_year():
    store = CountingStore()
    store.objects = {key.replace("activities/2026/1/", "activities/2027/1/"): value
                     for key, value in store.objects.items()}
    store.objects["activities/2026/1/context/v1/20270102T120000-ctx-old.json"] = json.dumps({
        "activity_id": "1", "context_id": "ctx-old", "rpe": 7,
    }).encode()
    metadata = {"id": "1", "date": "2027-01-01", "name": "Synthetic run", "moving_seconds": 1800}
    result = run_one(activity=metadata, store=store)
    assert result["processed_sources"]
    pointer = json.loads(store.objects["activities/2027/1/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["user_context"]["context_id"] == "ctx-old"


@pytest.mark.parametrize("utc,local,day", [
    ("2026-10-25 00:30:00", "2026-10-25 02:30:00", "2026-10-25"),
    ("2026-10-25 01:30:00", "2026-10-25 02:30:00", "2026-10-25"),
    ("2026-03-29 01:30:00", "2026-03-29 03:30:00", "2026-03-29"),
    ("2027-01-01 07:30:00", "2026-12-31 23:30:00-08:00", "2026-12-31"),
])
def test_travel_and_dst_keep_device_calendar_not_configured_home_calendar(tmp_path, utc, local, day):
    raw = {"activityId": 1, "startTimeGMT": utc, "startTimeLocal": local}

    class Provider:
        def get_activities_by_date(self, *_args):
            return [raw]

    activity = fetch(client=Provider(), end_date=date.fromisoformat(day))[0]
    assert activity.local_start_date == day
    assert activity.start.strftime("%Y-%m-%d %H:%M:%S") == utc
    write_dataset([activity], str(tmp_path))
    write_dataset(fetch(client=type("Sparse", (), {"get_activities_by_date": lambda *_: [
        {"activityId": 1, "startTimeGMT": utc} ]})(), end_date=date.fromisoformat(day)),
        str(tmp_path), preserve_existing=True)
    row = next(csv.DictReader((tmp_path / "activities.csv").read_text().splitlines()))
    assert row["Activity Local Date"] == day
    assert activity_prefix(raw).split("/")[1] == day[:4]


@pytest.mark.parametrize("local", ["2027-01-01 12:30:00+00:60", "2027-01-01 24:00:00",
                                        "2027-01-01 12:60:00", "2027-01-01 12:30:60"])
def test_malformed_clock_is_unknown(local):
    assert activity_local_date(local) is None


def test_manual_reader_uses_local_calendar_for_recent_and_artifact_checks():
    row = {"Activity ID": "garmin-1", "Activity Date": "2026-12-31 23:30:00",
           "Activity Local Date": "2027-01-01", "Activity Type": "Run"}
    assert _recent(row, date(2027, 1, 3))
    assert not _missing_artifacts(row, {f"activities/2027/1/{name}" for name in
                                      ("activity.fit", "activity.v1.json", "activity.tcx", "activity.endurance.v1.json")})


def test_manual_coach_does_not_accept_malformed_local_clock_as_valid_calendar():
    store = CountingStore()
    raw = {"activityId": 1, "startTimeGMT": "2026-09-30 10:00:00", "startTimeLocal": "2026-09-30 24:00:00"}
    assert _coach_metadata(raw, {}, store) is None


def test_legacy_bulk_uses_verified_local_namespace_when_an_old_unknown_clock_namespace_also_exists():
    store = CountingStore()
    for key, body in list(store.objects.items()):
        if key.startswith("activities/2026/1/"):
            store.objects[key.replace("/2026/", "/2027/")] = body
    key = "activities/2027/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["activity"].update(start_time_gmt="2026-12-31 23:30:00", start_time_local="2027-01-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-12-31 23:30:00,Synthetic run,Run,1800\n")
    result = run(max_activities=1, store=store)
    assert result["completed_this_run"][0]["key"].startswith("activities/2027/1/")


def test_legacy_bulk_rejects_a_canonical_clock_for_a_different_utc_instant():
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["activity"].update(start_time_gmt="2026-10-01 23:30:00", start_time_local="2026-10-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    with pytest.raises(ValueError, match="UTC"):
        run(max_activities=1, store=store)
    assert store.counts["put"] == 0


def test_legacy_clock_revision_change_invalidates_only_its_selected_cache():
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local="2026-09-30 23:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    first = run(max_activities=1, store=store)
    old_key = first["completed_this_run"][0]["key"]
    old_body = store.objects[old_key]
    decoded["activity"]["start_time_local"] = "2026-10-01 00:30:00"
    store.objects[key] = json.dumps(decoded).encode()
    second = run(max_activities=1, store=store)
    assert second["generated_this_run"] == 1
    pointer = json.loads(store.objects["activities/2026/1/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["activity"]["date"] == "2026-10-01"
    assert store.objects[old_key] == old_body


def test_legacy_clock_downloads_are_bounded_to_pending_batch():
    class TracedStore(CountingStore):
        def get(self, key):
            self.reads.append(key)
            return super().get(key)

    store = TracedStore()
    store.reads = []
    template = dict(store.objects)
    for number in range(1, 4):
        for key, body in template.items():
            if key.startswith("activities/2026/1/"):
                destination = key.replace("/1/", f"/{number}/")
                if destination.endswith("activity.v1.json"):
                    decoded = json.loads(body)
                    decoded.update(schema_version=1, source="garmin-fit")
                    decoded["activity"] = {"id": str(number), "start_time_gmt": "2026-09-30 23:30:00",
                                           "start_time_local": "2026-10-01 00:30:00"}
                    body = json.dumps(decoded).encode()
                store.objects[destination] = body
    store.objects["summary/activities.csv"] = (
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n" + "".join(
            f"garmin-{number},2026-09-30 23:30:00,Synthetic run,Run,1800\n" for number in range(1, 4))).encode()
    first = run(max_activities=1, store=store)
    assert first["generated_this_run"] == 1
    assert first["remaining_activities"] == 2
    assert [key for key in store.reads if key.endswith("/activity.v1.json")] == ["activities/2026/3/activity.v1.json"]
    store.reads.clear()
    second = run(max_activities=1, store=store)
    assert second["generated_this_run"] == 1
    assert second["remaining_activities"] == 1
    assert [key for key in store.reads if key.endswith("/activity.v1.json")] == ["activities/2026/2/activity.v1.json"]


def test_sparse_manual_details_keep_verified_summary_calendar_without_inventing_a_time():
    row = {"Activity ID": "garmin-1", "Activity Date": "2026-12-31 23:30:00", "Activity Local Date": "2027-01-01"}
    complete = _complete_activity_details({"activityId": 1}, row)
    assert complete["startTimeLocal"] == "2027-01-01"
    assert complete["startTimeGMT"] == "2026-12-31 23:30:00"
    assert activity_prefix(complete) == "activities/2027/1"


def test_conflicting_manual_local_calendar_fails_before_artifact_writes():
    row = {"Activity ID": "garmin-1", "Activity Date": "2026-12-31 23:30:00", "Activity Local Date": "2027-01-01"}
    with pytest.raises(ValueError, match="local"):
        _complete_activity_details({"activityId": 1, "startTimeLocal": "2026-12-31 12:00:00"}, row)


def test_r2_only_repair_rejects_a_malformed_local_clock_even_when_the_date_prefix_matches(monkeypatch):
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = json.loads(store.objects[key])
    decoded["activity"]["start_time_local"] = "2026-10-13 24:00:00"
    store.objects[key] = json.dumps(decoded).encode()
    monkeypatch.setattr("pipeline.latest_activity.recent_day", lambda day: date.fromisoformat(day))
    calls = []
    monkeypatch.setattr("pipeline.latest_activity.run_coach_one", lambda **kwargs:
                        calls.append(kwargs) or {"blocked_activities": [], "processed_sources": {}})
    with pytest.raises(ValueError, match="local date"):
        latest_activity(run_id="1", store=store, expected_date="2026-10-13", requested_activity_id="1", repair_only=True)
    assert calls == []


@pytest.mark.parametrize("version,source", [(2, "garmin-fit"), (True, "garmin-fit"), (1, "other-source"), (1, None), (None, "garmin-fit")])
def test_unknown_canonical_schema_or_source_cannot_establish_local_calendar_authority(version, source):
    store = CountingStore()
    store.objects = {key.replace("activities/2026/1/", "activities/2027/1/"): value
                     for key, value in store.objects.items()}
    key = "activities/2027/1/activity.v1.json"
    decoded = json.loads(store.objects[key])
    decoded.update(schema_version=version, source=source)
    decoded["activity"].update(start_time_gmt="2026-12-31 23:30:00", start_time_local="2027-01-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-12-31 23:30:00,Synthetic run,Run,1800\n")
    with pytest.raises(ValueError, match="schema|source"):
        run(max_activities=1, store=store)
    assert store.counts["put"] == 0


def test_legacy_missing_source_in_utc_namespace_is_traceable_fallback_without_local_authority():
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = json.loads(store.objects[key])
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local="2026-10-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    result = run(max_activities=1, store=store)
    assert result["activity_clock_revisions"]["1"]["source"] == "utc_fallback"
    assert result["activity_clock_revisions"]["1"]["date"] == "2026-09-30"


def test_numeric_schema_one_matches_reader_json_number_semantics():
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["schema_version"] = 1.0
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local="2026-10-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    result = run(max_activities=1, store=store)
    assert result["activity_clock_revisions"]["1"]["source"] == "garmin_local"
    assert result["activity_clock_revisions"]["1"]["date"] == "2026-10-01"


def test_unmarked_cached_local_claim_cannot_bypass_canonical_contract_validation():
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local="2026-10-01 00:30:00")
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    run(max_activities=1, store=store)
    decoded["schema_version"] = 2
    store.objects[key] = json.dumps(decoded).encode()
    revision = hashlib.sha256(store.objects[key]).hexdigest()
    plan_key = "backfill/coach-input/v1/plan.json"
    plan = json.loads(store.objects[plan_key])
    clock = plan["activity_clock_revisions"]["1"]
    clock.pop("calendar_contract_version", None)
    clock["source_revision"] = revision
    clock["candidate_revisions"][key] = revision
    store.objects[plan_key] = json.dumps(plan).encode()
    before = dict(store.objects)
    store.reset_counts()
    with pytest.raises(ValueError, match="schema|source"):
        run(max_activities=1, store=store)
    assert store.counts["put"] == 0
    assert store.objects == before


@pytest.mark.parametrize("version,source", [(2, "garmin-fit"), (True, "garmin-fit"), (1, "other-source")])
@pytest.mark.parametrize("local", [None, "", "2026-09-30 24:00:00"])
def test_unsupported_contract_cannot_escape_through_unknown_local_fallback(version, source, local):
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = _calendar_fixture(store, key)
    decoded.update(schema_version=version, source=source)
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local=local)
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    store.objects["activities/2026/1/context/v1/20260930-ctx-old.json"] = b'{"activity_id":"1","context_id":"ctx-old"}'
    store.objects["activities/2026/1/coach-input/v1/canonical/old.json"] = b'{"historical":"synthetic unchanged bytes"}'
    before = dict(store.objects)
    with pytest.raises(ValueError, match="schema|source"):
        run(max_activities=1, store=store)
    assert store.counts["put"] == 0
    assert store.objects == before


@pytest.mark.parametrize("declared", [False, True])
@pytest.mark.parametrize("local", [None, "", "2026-09-30 24:00:00"])
def test_supported_or_undeclared_legacy_without_local_keeps_visible_utc_fallback(declared, local):
    store = CountingStore()
    key = "activities/2026/1/activity.v1.json"
    decoded = json.loads(store.objects[key])
    if declared:
        decoded.update(schema_version=1, source="garmin-fit")
    decoded["activity"].update(start_time_gmt="2026-09-30 23:30:00", start_time_local=local)
    store.objects[key] = json.dumps(decoded).encode()
    store.objects["summary/activities.csv"] = (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
        b"garmin-1,2026-09-30 23:30:00,Synthetic run,Run,1800\n")
    result = run(max_activities=1, store=store)
    assert result["generated_this_run"] == 1
    assert result["activity_clock_revisions"]["1"]["source"] == "utc_fallback"
    assert result["activity_clock_revisions"]["1"]["date"] == "2026-09-30"
