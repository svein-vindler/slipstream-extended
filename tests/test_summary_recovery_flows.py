"""Normal pipeline flows against synthetic external Garmin and conditional S3."""
import json
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError
from test_summary_publication import ConditionalClient, source

from pipeline import refresh, refresh_summaries, summary_export, summary_restore
from pipeline.r2_store import R2ReadBoundaryError, R2Store
from pipeline.summary_publication import SummaryConflict, SummaryUnverified
from scripts.benchmark_summary_windows import SummaryGarmin


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 13, 12, tzinfo=timezone.utc)


class Garmin(SummaryGarmin):
    def __init__(self, hook=None):
        super().__init__()
        self.hook = hook

    def connectapi(self, path, params=None):
        if self.hook is not None:
            hook, self.hook = self.hook, None
            hook()
        return super().connectapi(path, params)

    def get_hrv_data(self, day):
        return self.connectapi("hrv-day", {"day": day})


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    monkeypatch.setenv("TRAINING_CONTEXT_ENABLED", "false")
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    data, other = tmp_path / "data", tmp_path / "other"
    source(data)
    source(other)
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket='synthetic')
    summary_export.run(str(data), store=factory())
    def competing_commit():
        (other / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
        summary_export.run(str(other), store=factory())
    return data, other, client, factory, competing_commit


def test_direct_summary_carries_its_prefetch_base_and_rejects_competing_commit(scenario):
    data, other, client, factory, commit = scenario
    sdk = Garmin(commit)
    with pytest.raises(SummaryConflict):
        refresh_summaries.run(str(data), store=factory(), client=sdk, request_pause=0)
    reader = other / "verify"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (other / "activities.csv").read_bytes()
    assert not any(k.startswith("refresh/checks/") for k in client.objects)



@pytest.mark.parametrize("route", ["historical", "activity", "direct_activity", "general_login_race"])
def test_all_normal_callers_keep_the_captured_base_across_source_stages(scenario, tmp_path, route):
    from pipeline import latest_activity
    data, other, client, factory, commit = scenario
    sdk = Garmin(commit)
    if route == "direct_activity":
        def operation():
            return latest_activity.run(run_id='0', data_dir=str(data), store=factory(), garmin=sdk)
    else:
        request = (refresh.RefreshRequest(health_start="2026-10-12", health_end="2026-10-13")
                   if route == "historical" else refresh.RefreshRequest(mode="activity")
                   if route == "activity" else refresh.RefreshRequest())
        def login():
            if route == "general_login_race":
                sdk.hook = None
                commit()
            return sdk
        def operation():
            return refresh.run(request, data_dir=str(tmp_path / route), store_factory=factory, login=login)
    with pytest.raises(SummaryConflict):
        operation()
    target = other / "verify"
    summary_restore.restore_summaries(str(target), factory())
    assert (target / "activities.csv").read_bytes() == (other / "activities.csv").read_bytes()
    assert not any(k.startswith("refresh/checks/") for k in client.objects)


def test_direct_summary_keeps_its_existing_verified_empty_bootstrap(tmp_path, monkeypatch):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    client, sdk = ConditionalClient(), Garmin()
    store = R2Store(client=client, bucket="synthetic")
    result = refresh_summaries.run(str(tmp_path / "empty"), store=store, client=sdk, request_pause=0)
    assert result["status"] == "complete"
    assert json.loads(client.objects["summary/manifest.json"]["Body"])["publication_version"] == 2
    reader = tmp_path / "reader"
    summary_restore.restore_summaries(str(reader), store)
    assert b"garmin-" in (reader / "activities.csv").read_bytes()


@pytest.mark.parametrize("scheduled", [False, True])
def test_shared_general_bootstraps_verified_empty_summaries(tmp_path, monkeypatch, record_property, scheduled):
    import csv

    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    monkeypatch.setenv("TRAINING_CONTEXT_ENABLED", "false")
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    client, sdk = ConditionalClient(), Garmin()
    unrelated = b'{"synthetic":"preserve"}'
    client.objects["coach/profiles/synthetic.json"] = {"Body": unrelated}
    logins = []

    def login():
        logins.append(True)
        return sdk

    def factory():
        return R2Store(client=client, bucket="synthetic")

    target = tmp_path / "first-normal"
    try:
        report = refresh.run(refresh.RefreshRequest(scheduled=scheduled), data_dir=str(target),
                             store_factory=factory, login=login)
        assert report["status"] != "failed"
        assert len(logins) == 1
        manifest = json.loads(client.objects["summary/manifest.json"]["Body"])
        assert manifest["publication_version"] == 2
        reader = tmp_path / "common-reader"
        summary_restore.restore_summaries(str(reader), factory())
        with (reader / "health_daily.csv").open(newline="", encoding="utf-8") as handle:
            rows = {row["Date"]: row for row in csv.DictReader(handle)}
        assert rows["2026-10-13"]["Steps"] == "1000"
        assert rows["2026-10-13"]["Resting Heart Rate"] == "50"
        assert b"Synthetic run" in (reader / "activities.csv").read_bytes()
        assert (reader / "activities.csv").read_bytes() == (target / "activities.csv").read_bytes()
        assert (reader / "health_daily.csv").read_bytes() == (target / "health_daily.csv").read_bytes()
        assert client.objects["coach/profiles/synthetic.json"]["Body"] == unrelated
    finally:
        record_property("c1_resources", json.dumps({"sdk_calls": len(sdk.calls), "logins": len(logins),
                                                   "r2": client.counts}, sort_keys=True))


def test_shared_initial_general_with_granular_produces_readable_summaries(tmp_path, monkeypatch, record_property):
    import gzip
    from copy import deepcopy
    from datetime import date, timedelta

    from garmin_fit_sdk import Encoder

    from pipeline import (
        activity_refresh,
        freshness,
        health_sync,
        manual_activity_refresh,
        recent_body,
        recent_health,
        summary_publication,
    )
    from scripts.benchmark_body_reuse import SyntheticBodyGarmin
    from scripts.benchmark_health_reuse import SyntheticGarmin as SyntheticNightGarmin

    day = "2026-10-14"
    PROFILE_KEY = "coach/profiles/v1/2026-01-01/synthetic.json"
    PROFILE = {
        "profile_id": "synthetic-components", "name": "Synthetic test zones",
        "effective_from": "2026-01-01",
        "zones": [{"label": "Low", "min_bpm": 0, "max_bpm": 130},
                  {"label": "High", "min_bpm": 131, "max_bpm": 200}],
        "references": {"interval_thresholds_bpm": [140]},
    }
    activity = {
        "activityId": 101, "activityName": "Synthetic component run",
        "activityType": {"typeKey": "running"},
        "startTimeGMT": day + " 10:00:00", "startTimeLocal": day + " 12:00:00",
        "distance": 200, "duration": 60, "movingDuration": 50,
        "averageHR": 140, "maxHR": 160,
    }
    # Invented GPS-free bytes, decoded by the real FIT/TCX/Coach pipeline.
    tcx = b'''<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2">
<Activities><Activity Sport="Running"><Id>2026-10-14T10:00:00Z</Id>
<Lap StartTime="2026-10-14T10:00:00Z"><TotalTimeSeconds>60</TotalTimeSeconds>
<DistanceMeters>200</DistanceMeters><Intensity>Active</Intensity><Track>
<Trackpoint><Time>2026-10-14T10:00:00Z</Time><DistanceMeters>0</DistanceMeters><HeartRateBpm><Value>120</Value></HeartRateBpm></Trackpoint>
<Trackpoint><Time>2026-10-14T10:00:30Z</Time><DistanceMeters>100</DistanceMeters><HeartRateBpm><Value>140</Value></HeartRateBpm></Trackpoint>
<Trackpoint><Time>2026-10-14T10:01:00Z</Time><DistanceMeters>200</DistanceMeters><HeartRateBpm><Value>160</Value></HeartRateBpm></Trackpoint>
</Track></Lap></Activity></Activities></TrainingCenterDatabase>'''

    class DayClock(date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 14)

    class Instant(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 10, 14, 12, tzinfo=timezone.utc)
            return value if tz else value.replace(tzinfo=None)

    def synthetic_fit():
        encoder = Encoder()
        start = datetime(2026, 10, 14, 10, tzinfo=timezone.utc)
        encoder.write_mesg({"mesg_num": 0, "type": "activity", "manufacturer": "development",
                            "time_created": start})
        for seconds, distance, heart_rate in [(0, 0, 120), (30, 100, 140), (60, 200, 160)]:
            encoder.write_mesg({"mesg_num": 20, "timestamp": start + timedelta(seconds=seconds),
                                "distance": distance, "heart_rate": heart_rate})
        encoder.write_mesg({"mesg_num": 18, "timestamp": start + timedelta(seconds=60),
                            "start_time": start, "sport": "running", "total_elapsed_time": 60,
                            "total_timer_time": 50, "total_distance": 200, "avg_heart_rate": 140,
                            "max_heart_rate": 160})
        return bytes(encoder.close())

    class SyntheticGeneralGarmin(SummaryGarmin):
        def __init__(self, fit):
            super().__init__()
            self.fit = fit
            self.night = SyntheticNightGarmin()
            self.body = SyntheticBodyGarmin()

        def get_activities_by_date(self, start, end):
            self.calls.append(["activity_list", start, end])
            return [deepcopy(activity)] if start <= day <= end else []

        def get_activity(self, activity_id):
            assert activity_id == "101"
            self.calls.append(["activity_details", activity_id])
            return deepcopy(activity)

        def download_activity(self, activity_id, *, dl_fmt):
            assert str(activity_id) == "101"
            kind = dl_fmt.name
            assert kind in {"ORIGINAL", "TCX"}
            self.calls.append(["download", kind])
            return self.fit if kind == "ORIGINAL" else tcx

        def get_sleep_data(self, requested_day):
            return self.night.get_sleep_data(requested_day)

        def get_hrv_data(self, requested_day):
            return self.night.get_hrv_data(requested_day)

        def get_weigh_ins(self, first, last):
            return self.body.get_daily_weigh_ins(day) if first <= day <= last else {"dateWeightList": []}

        def get_daily_weigh_ins(self, requested_day):
            return self.body.get_daily_weigh_ins(requested_day)

    def decode(raw):
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRAINING_READINESS_ENABLED", "false")
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    for module in (activity_refresh, freshness, health_sync, manual_activity_refresh, refresh,
                   refresh_summaries, summary_export, summary_publication):
        monkeypatch.setattr(module, "datetime", Instant)
    for module in (manual_activity_refresh, recent_body, recent_health):
        monkeypatch.setattr(module, "date", DayClock)
    client, sdk = ConditionalClient(), SyntheticGeneralGarmin(synthetic_fit())
    profile = json.dumps(PROFILE).encode()
    client.objects[PROFILE_KEY] = {"Body": profile, "ContentType": "application/json"}
    target = tmp_path / "first-normal"
    logins = []

    def login():
        logins.append(True)
        return sdk

    def factory():
        return R2Store(client=client, bucket="synthetic")

    try:
        diagnostic = refresh.run(refresh.RefreshRequest(include_granular=True), data_dir=str(target),
                                 store_factory=factory, login=login)
        assert diagnostic["status"] == "complete"
        assert len(logins) == 1
        assert json.loads((tmp_path / ".granular/on-demand-baseline.json").read_text()) == []
        manual = json.loads(client.objects["refresh/reports/0.json"]["Body"])
        assert manual["new_activity_count"] == manual["checked_activity_count"] == 1
        assert manual["activities"][0]["activity_id"] == "garmin-101"
        assert manual["activities"][0]["files_ready"] is True
        assert manual["activities"][0]["coach_status"] == "ready"
        canonical = [key for key in client.objects if "/canonical/" in key]
        assert len(canonical) == 1
        analysis = decode(client.objects[canonical[0]]["Body"])
        assert analysis["summary"]["moving_seconds"] == 50.0
        assert analysis["summary"]["distance_m"] == 200.0
        reader = tmp_path / "common-reader"
        summary_restore.restore_summaries(str(reader), factory())
        assert b"garmin-101" in (reader / "activities.csv").read_bytes()
        assert b"Synthetic component run" in (reader / "activities.csv").read_bytes()
        assert (reader / "health_daily.csv").read_bytes() == (target / "health_daily.csv").read_bytes()
        assert client.objects[PROFILE_KEY]["Body"] == profile
        original_analysis = client.objects[canonical[0]]["Body"]
        record_property("c1_f1_first_resources", json.dumps({"logins": len(logins), "sdk_calls": len(sdk.calls),
            "night_calls": len(sdk.night.calls), "body_calls": len(sdk.body.calls), "r2": client.counts}, sort_keys=True))
        client.counts = dict.fromkeys(client.counts, 0)
        logins.clear()
        sdk.calls.clear()
        sdk.night.calls.clear()
        sdk.body.calls.clear()
        repeated = refresh.run(refresh.RefreshRequest(include_granular=True), data_dir=str(target),
                               store_factory=factory, login=login)
        assert repeated["status"] == "complete"
        assert len(logins) == 1
        assert json.loads((tmp_path / ".granular/on-demand-baseline.json").read_text()) == ["garmin-101"]
        manual = json.loads(client.objects["refresh/reports/0.json"]["Body"])
        assert manual["new_activity_count"] == 0
        assert manual["checked_activity_count"] == 1
        assert manual["activities"][0]["files_ready"] is True
        assert manual["activities"][0]["coach_status"] == "ready"
        assert not any(call[0] == "download" for call in sdk.calls)
        assert client.objects[canonical[0]]["Body"] == original_analysis
        assert client.objects[PROFILE_KEY]["Body"] == profile
    finally:
        record_property("c1_f1_resources", json.dumps({"logins": len(logins), "sdk_calls": len(sdk.calls),
            "night_calls": len(sdk.night.calls), "body_calls": len(sdk.body.calls), "r2": client.counts}, sort_keys=True))


def test_initial_scheduled_general_still_rejects_granular_before_storage(tmp_path):
    stores, logins = [], []

    def factory():
        stores.append(True)
        return R2Store(client=ConditionalClient(), bucket="synthetic")

    def login():
        logins.append(True)
        return Garmin()

    with pytest.raises(ValueError, match="Scheduled refresh"):
        refresh.run(refresh.RefreshRequest(scheduled=True, include_granular=True), data_dir=str(tmp_path),
                    store_factory=factory, login=login)
    assert stores == logins == []


@pytest.mark.parametrize("cut", [None, "archive", "commit"])
def test_fresh_normal_runner_recovers_committed_pair_after_export_interruption(scenario, tmp_path, monkeypatch, cut):
    import csv
    data, _, client, factory, _ = scenario
    if cut:
        (data / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic changed\n")
        original = client.put_object
        def interrupt(**kwargs):
            key = kwargs["Key"]
            if ((cut == "archive" and key.startswith("summary/generations/") and key.endswith("/activities.csv"))
                    or cut == "commit" and key == "summary/manifest.json"):
                raise RuntimeError("Synthetic publication interrupted")
            return original(**kwargs)
        monkeypatch.setattr(client, "put_object", interrupt)
        with pytest.raises(RuntimeError, match="Synthetic publication interrupted"):
            summary_export.run(str(data), store=factory())
        monkeypatch.setattr(client, "put_object", original)
    sdk = Garmin()
    target = tmp_path / "cold-normal"
    result = refresh.run(refresh.RefreshRequest(), data_dir=str(target), store_factory=factory, login=lambda: sdk)
    assert result["status"] != "failed"
    assert result["stages"][1]["stage"] == "refresh_summaries"
    with (target / "activities.csv").open(encoding="utf-8", newline="") as handle:
        assert any(row["Activity ID"] == "garmin-1" for row in csv.DictReader(handle))
    summary_restore.restore_summaries(str(tmp_path / "verified"), factory())
    assert (tmp_path / "verified/activities.csv").read_bytes() == (target / "activities.csv").read_bytes()



@pytest.mark.parametrize("route", ["general", "scheduled", "historical", "activity", "direct_activity"])
def test_positive_normal_routes_publish_a_pair_readable_by_the_common_reader(scenario, tmp_path, route):
    from pipeline import latest_activity
    data, _, client, factory, _ = scenario
    sdk = Garmin()
    if route == "direct_activity":
        result = latest_activity.run(run_id="0", data_dir=str(data), store=factory(), garmin=sdk)
        assert result["summary_updated"] is True
        target = data
    else:
        target = tmp_path / route
        request = (refresh.RefreshRequest(scheduled=True) if route == "scheduled" else
                   refresh.RefreshRequest(health_start="2026-10-12", health_end="2026-10-13")
                   if route == "historical" else refresh.RefreshRequest(mode="activity")
                   if route == "activity" else refresh.RefreshRequest())
        result = refresh.run(request, data_dir=str(target), store_factory=factory, login=lambda: sdk)
        assert result["status"] != "failed"
    reader = tmp_path / "common-reader"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (target / "activities.csv").read_bytes()
    assert (reader / "health_daily.csv").read_bytes() == (target / "health_daily.csv").read_bytes()


@pytest.mark.parametrize("route", ["shared", "direct_activity"])
def test_partial_target_or_direct_activity_is_not_an_implicit_full_export(tmp_path, route):
    from pipeline import latest_activity
    client, sdk = ConditionalClient(), Garmin()
    if route == "shared":
        client.objects["summary/activities.csv"] = {"Body": b"Activity ID,Activity Name\ngarmin-1,Preserve\n"}
    before = {key: value["Body"] for key, value in client.objects.items()}
    def factory():
        return R2Store(client=client, bucket="synthetic")
    with pytest.raises(SummaryUnverified):
        if route == "shared":
            refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path), store_factory=factory, login=lambda: sdk)
        else:
            latest_activity.run(run_id="0", data_dir=str(tmp_path), store=factory(), garmin=sdk)
    assert sdk.calls == []
    assert {key: value["Body"] for key, value in client.objects.items()} == before
    assert client.counts["put"] == 0


@pytest.mark.parametrize("operation", ["get_object", "list_objects_v2"])
@pytest.mark.parametrize("code", ["AccessDenied", "SlowDown", "NoSuchBucket", "transport"])
def test_shared_bootstrap_storage_failure_is_not_absence(tmp_path, monkeypatch, operation, code, record_property):
    client, sdk, logins = ConditionalClient(), Garmin(), []
    failure = (TimeoutError("synthetic transport") if code == "transport" else
               ClientError({"Error": {"Code": code}}, operation))

    def fail(**kwargs):
        client.counts["get" if operation == "get_object" else "list"] += 1
        raise failure

    monkeypatch.setattr(client, operation, fail)

    def login():
        logins.append(True)
        return sdk

    with pytest.raises(type(failure)) as caught:
        refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path / "normal"),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login)
    assert caught.value is failure
    assert logins == sdk.calls == []
    assert client.objects == {}
    assert client.counts["put"] == 0
    record_property("c1_resources", json.dumps({"sdk_calls": 0, "logins": 0, "r2": client.counts}, sort_keys=True))


@pytest.mark.parametrize("response", [
    {"Contents": []},
    {"Contents": [], "IsTruncated": True},
    {"Contents": {}, "IsTruncated": False},
])
def test_shared_bootstrap_requires_a_verified_complete_absence_listing(tmp_path, monkeypatch, response):
    client, sdk, logins = ConditionalClient(), Garmin(), []

    def listing(**kwargs):
        client.counts["list"] += 1
        return response

    def login():
        logins.append(True)
        return sdk

    monkeypatch.setattr(client, "list_objects_v2", listing)
    with pytest.raises(R2ReadBoundaryError) as caught:
        refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path / "normal"),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login)
    assert caught.value.status == "schema_unverified"
    assert logins == sdk.calls == []
    assert client.objects == {}
    assert client.counts["put"] == 0


@pytest.mark.parametrize("key", ["summary/snapshots/2026/10/12/activities.csv", "summary/unknown-partial"])
def test_shared_bootstrap_does_not_replace_orphan_summary_history(tmp_path, key):
    client, sdk = ConditionalClient(), Garmin()
    previous = b"synthetic old publication bytes"
    client.objects[key] = {"Body": previous}
    with pytest.raises(SummaryUnverified):
        refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path / "normal"),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=lambda: sdk)
    assert sdk.calls == []
    assert {name: obj["Body"] for name, obj in client.objects.items()} == {key: previous}
    assert client.counts["put"] == 0


@pytest.mark.parametrize("corruption", ["json", "schema", "hash"])
def test_shared_bootstrap_does_not_replace_an_unverified_manifest(tmp_path, corruption):
    client, sdk, logins = ConditionalClient(), Garmin(), []
    seed = tmp_path / "seed"
    source(seed)
    store = R2Store(client=client, bucket="synthetic")
    summary_export.run(str(seed), store=store)
    manifest = json.loads(client.objects["summary/manifest.json"]["Body"])
    if corruption == "schema":
        manifest["schema_version"] = 999
    elif corruption == "hash":
        manifest["files"][0]["sha256"] = "0" * 64
    client.objects["summary/manifest.json"]["Body"] = (
        b"synthetic invalid JSON" if corruption == "json" else json.dumps(manifest).encode())
    before = {key: obj["Body"] for key, obj in client.objects.items()}
    client.counts = {key: 0 for key in client.counts}
    target = tmp_path / "normal"
    target.mkdir()
    local = target / "health_daily.csv"
    local.write_bytes(b"synthetic local bytes to preserve")

    def login():
        logins.append(True)
        return sdk

    with pytest.raises(SummaryUnverified):
        refresh.run(refresh.RefreshRequest(), data_dir=str(target),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login)
    assert logins == sdk.calls == []
    assert client.counts["put"] == 0
    assert {key: obj["Body"] for key, obj in client.objects.items()} == before
    assert local.read_bytes() == b"synthetic local bytes to preserve"


@pytest.mark.parametrize("names", [("activities.csv",), ("health_daily.csv",), ("activities.csv", "health_daily.csv")])
def test_shared_bootstrap_does_not_adopt_local_seed_or_partial_files(tmp_path, names):
    client, sdk, logins = ConditionalClient(), Garmin(), []
    target = tmp_path / "normal"
    target.mkdir()
    for name in names:
        (target / name).write_bytes(b"synthetic local data")

    def login():
        logins.append(True)
        return sdk

    with pytest.raises(SummaryUnverified):
        refresh.run(refresh.RefreshRequest(), data_dir=str(target),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login)
    assert logins == sdk.calls == []
    assert client.objects == {}
    assert client.counts["put"] == 0
    assert all((target / name).read_bytes() == b"synthetic local data" for name in names)


@pytest.mark.parametrize("mode", ["activity", "backfill", "repair"])
def test_shared_bootstrap_does_not_extend_to_targeted_backfill_or_repair(tmp_path, mode):
    client, sdk, logins = ConditionalClient(), Garmin(), []
    today = datetime.now(timezone.utc).date().isoformat()
    request = (refresh.RefreshRequest(health_start=today, health_end=today) if mode == "backfill" else
               refresh.RefreshRequest(mode="activity", repair_only=mode == "repair",
                                      activity_id="1", expected_date=today))

    def login():
        logins.append(True)
        return sdk

    with pytest.raises(ClientError if mode == "repair" else SummaryUnverified):
        refresh.run(request, data_dir=str(tmp_path / "normal"),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login)
    assert logins == sdk.calls == []
    assert not any(key.startswith("summary/") for key in client.objects)


def test_shared_initial_publication_conflict_preserves_the_winner(tmp_path, monkeypatch, record_property):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    monkeypatch.setenv("TRAINING_CONTEXT_ENABLED", "false")
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    client = ConditionalClient()
    other = tmp_path / "other"
    source(other)
    winning = {}
    after_commit = {}

    def factory():
        return R2Store(client=client, bucket="synthetic")

    def competing_commit():
        summary_export.run(str(other), store=factory())
        winning.update({key: obj["Body"] for key, obj in client.objects.items() if key.startswith("summary/")})
        after_commit.update(client.counts)

    sdk = Garmin(competing_commit)
    with pytest.raises(SummaryConflict):
        refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path / "loser"),
                    store_factory=factory, login=lambda: sdk)
    assert winning
    assert sdk.calls
    assert client.counts["put"] == after_commit["put"]
    assert {key: obj["Body"] for key, obj in client.objects.items() if key.startswith("summary/")} == winning
    assert not any(key.startswith("refresh/checks/") for key in client.objects)
    reader = tmp_path / "winner-reader"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == b"Activity ID,Activity Name\ngarmin-1,Synthetic A\n"
    record_property("c1_resources", json.dumps({"sdk_calls": len(sdk.calls), "r2_including_competitor": client.counts,
                                               "after_competing_commit": after_commit}, sort_keys=True))


@pytest.mark.parametrize("cut", range(1, 5))
@pytest.mark.parametrize("after", [False, True])
def test_each_source_receipt_cut_retains_the_commit_and_next_cold_normal_run(scenario, tmp_path, monkeypatch, cut, after):
    data, _, client, factory, _ = scenario
    sdk, original, writes = Garmin(), client.put_object, []
    def interrupt(**kwargs):
        checkpoint = kwargs["Key"].startswith("refresh/checks/v1/summary/")
        if checkpoint:
            writes.append(kwargs["Key"])
        if checkpoint and len(writes) == cut and not after:
            raise TimeoutError("synthetic source receipt cut")
        result = original(**kwargs)
        if checkpoint and len(writes) == cut and after:
            raise TimeoutError("synthetic source receipt cut")
        return result
    monkeypatch.setattr(client, "put_object", interrupt)
    with pytest.raises(TimeoutError, match="synthetic source receipt cut"):
        refresh_summaries.run(str(data), store=factory(), client=sdk, request_pause=0)
    assert len(writes) == cut
    monkeypatch.setattr(client, "put_object", original)
    reader = tmp_path / "committed-before-retry"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (data / "activities.csv").read_bytes()
    cold = tmp_path / "cold-retry"
    result = refresh.run(refresh.RefreshRequest(), data_dir=str(cold), store_factory=factory, login=lambda: Garmin())
    assert result["status"] != "failed"
    summary_restore.restore_summaries(str(tmp_path / "final-reader"), factory())
    assert (tmp_path / "final-reader/activities.csv").read_bytes() == (cold / "activities.csv").read_bytes()
