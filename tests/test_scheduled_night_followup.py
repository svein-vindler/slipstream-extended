"""Scheduled command -> guarded storage: current nights need no summary seed."""

import copy
import gzip
import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from pipeline import health_detail_backfill
from pipeline.r2_store import R2Store, missing_object
from scripts.benchmark_health_reuse import HealthClient
from scripts.build_health_contract import FIXTURE, FixtureGarmin


def test_scheduled_health_command_imports_current_night_without_summary_seed(scheduled_job):
    job = scheduled_job()
    job.invoke()
    sleep = json.loads(gzip.decompress(job.store.get(
        "health/sleep/v1/2026/09/2026-09-30.json",
    )))
    hrv = json.loads(gzip.decompress(job.store.get("health/hrv/2026/09/2026-09-30.json")))
    assert sleep["summary"]["sleep_seconds"] == 25200
    assert hrv["reading_count"] == 2


def test_scheduled_health_command_does_not_refetch_a_complete_current_night(scheduled_job):
    job = scheduled_job()
    job.invoke()
    job.garmin.calls.clear()

    job.invoke()

    assert job.garmin.calls == [], "An already complete current night must not cause extra Garmin reads"


def test_later_scheduled_health_command_retries_pending_night_and_preserves_history(scheduled_job):
    job = scheduled_job()
    summary = job.store.get("summary/health_daily.csv")
    old_key = "health/sleep/v1/2025/01/2025-01-01.json"
    old_bytes = b"synthetic preserved historical bytes"
    job.store.put(old_key, old_bytes, "application/json")
    job.clock.current = datetime(2026, 9, 30, 1, tzinfo=timezone.utc)
    job.garmin.responses["sleep", "2026-09-30"]["dailySleepDTO"]["sleepTimeSeconds"] = 0
    job.garmin.responses["hrv", "2026-09-30"]["hrvReadings"] = []
    job.invoke()
    job.garmin.responses["sleep", "2026-09-30"] = copy.deepcopy(job.case["sleep"])
    job.garmin.responses["hrv", "2026-09-30"] = copy.deepcopy(job.case["hrv"])
    job.clock.current = datetime(2026, 9, 30, 7, tzinfo=timezone.utc)

    job.invoke()

    sleep = json.loads(gzip.decompress(job.store.get(
        "health/sleep/v1/2026/09/2026-09-30.json",
    )))
    assert sleep["summary"]["sleep_seconds"] == 25200
    assert job.store.get(old_key) == old_bytes
    assert job.store.get("summary/health_daily.csv") == summary


@pytest.mark.parametrize("module_name", [
    "hrv_backfill", "activity_backfill_scheduler", "activity_refresh",
])
def test_later_scheduled_garmin_jobs_also_check_the_current_night(monkeypatch, module_name):
    from pipeline.health_sync import store_day

    module = importlib.import_module(f"pipeline.{module_name}")
    cases = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]
    current = copy.deepcopy(cases[0])
    old = next(case for case in cases if case["id"] == "new-year")
    client = HealthClient()
    store = R2Store(client=client, bucket="synthetic-scheduled-night")
    store.put(
        "summary/health_daily.csv",
        b"Date,Sleep Seconds,Weight KG,HRV Last Night Average\n2026-01-01,25200,,60\n",
        "text/csv",
    )
    store_day(store, "hrv", old["date"], old["hrv"])
    store.put("backfill/hrv/plan.json", json.dumps({
        "schema_version": 1, "status": "complete", "target_dates": ["2026-01-01"],
    }).encode(), "application/json")
    store.put("backfill/activities/plan-v2.json", json.dumps({
        "schema_version": 2, "status": "complete", "ranges": [],
    }).encode(), "application/json")

    class Garmin(FixtureGarmin):
        def get_activities_by_date(self, *args, **kwargs):
            return []

    garmin = Garmin([current])

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2026, 9, 30, 7, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    monkeypatch.setattr(sys, "argv", [module_name])
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr("pipeline.health_sync.datetime", Clock)
    monkeypatch.setattr("pipeline.night_followup.datetime", Clock)
    monkeypatch.setattr(module, "R2Store", lambda: store)
    monkeypatch.setattr(module, "_login", lambda: garmin)

    module.main()

    try:
        data = store.get("health/sleep/v1/2026/09/2026-09-30.json")
        sleep = json.loads(gzip.decompress(data))
    except ClientError as exc:
        if not missing_object(exc):
            raise
        sleep = {}
    assert sleep.get("summary", {}).get("sleep_seconds") == 25200, (
        f"The scheduled {module_name} command must also follow up a missing current night"
    )


@pytest.fixture
def scheduled_job(monkeypatch):
    def prepare(*, when=None, case_id="ordinary", max_writes=None):
        module = health_detail_backfill
        cases = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]
        case = copy.deepcopy(next(item for item in cases if item["id"] == case_id))
        client = HealthClient()
        store = R2Store(
            client=client, bucket="synthetic-scheduled-night",
            max_writes_per_run=max_writes,
        )
        store.put(
            "summary/health_daily.csv",
            b"Date,Sleep Seconds,Weight KG,HRV Last Night Average\n",
            "text/csv",
        )
        garmin = FixtureGarmin([case])
        logins = []

        class Clock(datetime):
            current = when or datetime(2026, 9, 30, 7, tzinfo=timezone.utc)

            @classmethod
            def now(cls, tz=None):
                return cls.current.astimezone(tz) if tz else cls.current.replace(tzinfo=None)

        def login():
            logins.append(True)
            return garmin

        monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
        monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
        monkeypatch.setattr(sys, "argv", ["health_detail_backfill"])
        monkeypatch.setattr(module, "datetime", Clock)
        monkeypatch.setattr("pipeline.health_sync.datetime", Clock)
        monkeypatch.setattr("pipeline.night_followup.datetime", Clock)
        monkeypatch.setattr(module, "R2Store", lambda: store)
        monkeypatch.setattr(module, "_login", login)
        return SimpleNamespace(
            store=store, client=client, garmin=garmin, clock=Clock,
            invoke=module.main, case=case, logins=logins,
        )
    return prepare


@pytest.mark.parametrize("case_id,early,late", [
    ("ordinary", "2026-09-29T22:30:00+00:00", "2026-09-30T07:00:00+00:00"),
    ("dst-fallback", "2026-10-24T22:30:00+00:00", "2026-10-25T07:00:00+00:00"),
    ("new-year", "2025-12-31T23:30:00+00:00", "2026-01-01T07:00:00+00:00"),
])
def test_scheduled_retry_uses_local_wake_date_across_midnight_dst_and_new_year(
    scheduled_job, case_id, early, late,
):
    job = scheduled_job(when=datetime.fromisoformat(early), case_id=case_id)
    day = job.case["date"]
    job.garmin.responses["sleep", day]["dailySleepDTO"]["sleepTimeSeconds"] = 0
    job.garmin.responses["hrv", day]["hrvReadings"] = []
    job.invoke()
    assert job.garmin.calls == [["sleep", day], ["hrv", day]]

    job.clock.current = datetime.fromisoformat(late)
    job.garmin.responses["sleep", day] = copy.deepcopy(job.case["sleep"])
    job.garmin.responses["hrv", day] = copy.deepcopy(job.case["hrv"])
    job.invoke()

    body = job.store.get(f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json")
    sleep = json.loads(gzip.decompress(body))
    assert sleep["summary"]["sleep_seconds"] == job.case["expected"]["sleep_seconds"]


@pytest.mark.parametrize("event", ["workflow_dispatch", "workflow_run", ""])
def test_non_scheduled_historical_command_does_not_add_a_current_night_fetch(
    scheduled_job, monkeypatch, event,
):
    job = scheduled_job()
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    job.invoke()
    assert job.logins == []
    assert job.garmin.calls == []


def test_stored_read_error_does_not_become_absence_or_start_provider_fetch(
    scheduled_job, monkeypatch,
):
    job = scheduled_job()
    original = job.client.get_object

    def denied(**kwargs):
        if kwargs["Key"].startswith("refresh/checks/v1/health/"):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        return original(**kwargs)

    monkeypatch.setattr(job.client, "get_object", denied)
    with pytest.raises(SystemExit) as failure:
        job.invoke()
    assert failure.value.code == 1
    assert job.logins == []
    assert job.garmin.calls == []


def test_night_followup_uses_the_same_r2_write_budget_as_the_existing_job(scheduled_job):
    job = scheduled_job(max_writes=1)
    with pytest.raises(SystemExit) as failure:
        job.invoke()
    assert failure.value.code == 1
    assert job.client.counts["put"] == 1


def test_rejected_source_responses_preserve_good_canonical_bytes_and_checkpoints(
    scheduled_job, capsys,
):
    job = scheduled_job()
    job.invoke()
    day = job.case["date"]
    sleep_key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
    hrv_key = f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"
    hrv_check = f"refresh/checks/v1/health/hrv/{day}.json"
    proof = json.loads(job.store.get(hrv_check))
    proof["source_sha256"] = "0" * 64
    job.store.put(hrv_check, json.dumps(proof).encode(), "application/json")
    keys = [sleep_key, hrv_key, hrv_check, f"refresh/checks/v1/night/{day}.json"]
    before = {key: job.store.get(key) for key in keys}
    job.garmin.responses["sleep", day]["dailySleepDTO"]["calendarDate"] = "2026-09-29"
    job.garmin.responses["hrv", day] = {"unexpected": "PRIVATE_SYNTHETIC_MARKER"}
    capsys.readouterr()

    job.invoke()

    assert {key: job.store.get(key) for key in keys} == before
    assert "PRIVATE_SYNTHETIC_MARKER" not in capsys.readouterr().out


def test_later_scheduled_job_repairs_interrupted_combined_receipt_without_refetch(
    scheduled_job, monkeypatch,
):
    job = scheduled_job()
    day = job.case["date"]
    night_key = f"refresh/checks/v1/night/{day}.json"
    original = job.client.put_object

    def interrupted(**kwargs):
        if kwargs["Key"] == night_key:
            raise RuntimeError("Synthetic interrupted final night receipt")
        return original(**kwargs)

    monkeypatch.setattr(job.client, "put_object", interrupted)
    with pytest.raises(SystemExit):
        job.invoke()
    checks = {stream: job.store.get(f"refresh/checks/v1/health/{stream}/{day}.json")
              for stream in ("sleep", "hrv")}
    monkeypatch.setattr(job.client, "put_object", original)
    job.garmin.calls.clear()
    job.clock.current = datetime(2026, 9, 30, 8, tzinfo=timezone.utc)

    job.invoke()

    try:
        proof = json.loads(job.store.get(night_key))
    except ClientError as exc:
        if not missing_object(exc):
            raise
        proof = {}
    assert proof.get("source_checked") is True, (
        "A later scheduled job must finish an interrupted combined night receipt"
    )
    assert proof["checked_at"] == "2026-09-30T07:00:00+00:00"
    assert job.garmin.calls == []
    for stream, data in checks.items():
        assert job.store.get(f"refresh/checks/v1/health/{stream}/{day}.json") == data


def test_scheduled_job_rechecks_a_legitimate_pending_targeted_night(
    scheduled_job, monkeypatch,
):
    from pipeline import latest_night

    job = scheduled_job()
    day = job.case["date"]
    job.invoke()

    class Calendar:
        @staticmethod
        def fromisoformat(value):
            return datetime.fromisoformat(value).date()

        @staticmethod
        def today():
            return datetime(2026, 9, 30).date()

    monkeypatch.setattr("pipeline.freshness.date", Calendar)
    monkeypatch.setattr("pipeline.freshness.datetime", job.clock)
    job.clock.current = datetime(2026, 9, 30, 7, 30, tzinfo=timezone.utc)
    job.garmin.responses["sleep", day]["dailySleepDTO"]["sleepTimeSeconds"] = 0
    job.garmin.responses["hrv", day]["hrvReadings"] = []
    latest_night.run(run_id="456", wake_date=day, store=job.store, garmin=job.garmin)
    assert json.loads(job.store.get(f"refresh/checks/v1/night/{day}.json"))["status"] == "pending"
    job.garmin.responses["sleep", day] = copy.deepcopy(job.case["sleep"])
    job.garmin.responses["hrv", day] = copy.deepcopy(job.case["hrv"])
    job.garmin.calls.clear()
    job.clock.current = datetime(2026, 9, 30, 8, tzinfo=timezone.utc)

    try:
        job.invoke()
    except SystemExit:
        resumed = False
    else:
        resumed = True

    assert resumed, "A valid pending night must permit later ordinary scheduled reconciliation"
    assert job.garmin.calls == [["sleep", day], ["hrv", day]]
    proof = json.loads(job.store.get(f"refresh/checks/v1/night/{day}.json"))
    assert proof["status"] == "stored"
    assert proof["checked_at"] == "2026-09-30T08:00:00+00:00"


def test_scheduled_followup_validates_new_receipts_with_an_advancing_clock(scheduled_job, monkeypatch):
    from pipeline import fresh_night_references

    job = scheduled_job()
    start = job.clock.current
    checked = start + timedelta(seconds=1)
    day = job.case["date"]
    original_sleep = job.garmin.get_sleep_data

    def later_sleep(requested_day):
        job.clock.current = checked
        return original_sleep(requested_day)

    monkeypatch.setattr(job.garmin, "get_sleep_data", later_sleep)
    real_refresh = fresh_night_references.refresh_current_night
    observed = []

    def refresh(store, wake_date, **kwargs):
        observed.append((wake_date, kwargs["now"]))
        return real_refresh(store, wake_date, **kwargs)

    monkeypatch.setattr(fresh_night_references, "refresh_current_night", refresh)
    job.invoke()

    assert observed == [(day, checked)], "Use the original wake date and a fresh validation time"
    key = f"refresh/checks/v1/health/sleep/{day}.json"
    proof = json.loads(job.store.get(key))
    assert proof["checked_at"] == checked.isoformat()
    # Explicit clocks remain authoritative: no max(now, real_clock) tolerance.
    with pytest.raises(ValueError, match="Stored night source-check time is invalid"):
        real_refresh(job.store, day, now=start)
    proof["checked_at"] = (checked + timedelta(seconds=1)).isoformat()
    job.store.put(key, json.dumps(proof).encode(), "application/json")
    with pytest.raises(ValueError, match="Stored night source-check time is invalid"):
        real_refresh(job.store, day, now=checked)
