"""Normal night entry points, real R2 SDK, synthetic HTTP and Garmin reads."""

import copy
import gzip
import importlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit

import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError
from history_storage_fixture import BUCKET, Raw
from test_history_baseline_integration import sdk_store
from test_rolling_reference_sources import RevisionServer

from pipeline import health_detail_backfill, latest_night, night_followup, refresh
from pipeline.night_products import normalize_args, storage_key
from pipeline.signal_series import signal_key
from scripts.benchmark_summary_windows import SummaryGarmin
from scripts.build_health_contract import FIXTURE, FixtureGarmin

DAY = "2026-09-30"


class NightServer(RevisionServer):
    def send(self, request):
        # The fixture also serves ordinary source/index/freshness SDK operations,
        # outside the reference component's streaming observer.
        old = request.stream_output
        try:
            request.stream_output = True
            key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
            if request.method == "GET" and key == getattr(self, "denied_key", None):
                if self.denial == "unknown":
                    raise TimeoutError("Synthetic unknown GET outcome")
                body = b"<Error><Code>AccessDenied</Code></Error>"
                return AWSResponse(request.url, 403, {"content-length": str(len(body))}, Raw(body))
            return super().send(request)
        finally:
            request.stream_output = old


class Garmin(FixtureGarmin):
    def __init__(self, case_id="ordinary"):
        cases = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]
        self.case = next(item for item in cases if item["id"] == case_id)
        super().__init__([self.case])
        self.hr = {}

    def get_heart_rates(self, day):
        self.calls.append(["heart_rate", day])
        return copy.deepcopy(self.hr.get(day, {
            "calendarDate": day,
            "heartRateValues": [[int(datetime.fromisoformat(day).replace(
                tzinfo=timezone.utc).timestamp() * 1000), 55]],
        }))

    def get_activities_by_date(self, *args, **kwargs):
        return []


def clock(monkeypatch, day=DAY):
    class Calendar(date):
        @classmethod
        def today(cls):
            return cls.fromisoformat(day)

    monkeypatch.setattr("pipeline.freshness.date", Calendar)
    class Instant(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = cls.fromisoformat(day + "T07:00:00+00:00")
            return instant.astimezone(tz) if tz else instant.replace(tzinfo=None)

    for module in ("latest_night", "health_sync", "signal_series", "freshness", "night_baseline_refresh"):
        monkeypatch.setattr(f"pipeline.{module}.datetime", Instant)
    for module in ("refresh", "refresh_summaries", "summary_publication"):
        monkeypatch.setattr(f"pipeline.{module}.datetime", Instant)
    monkeypatch.setattr("pipeline.recent_health.date", Calendar)
    monkeypatch.setattr("pipeline.recent_body.date", Calendar)
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")


def test_targeted_recent_night_retains_standard_hr_and_bound_source_checks(monkeypatch):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)

    result = latest_night.run(run_id="10", wake_date=DAY, store=store, garmin=garmin)

    assert signal_key(DAY) in server.objects, "Targeted night discarded the HR source curve"
    payload = json.loads(gzip.decompress(store.get(signal_key(DAY))))
    assert payload["channels"]["heart_rate"]["readings"][0]["value"] == 55
    proof = json.loads(store.get(f"refresh/checks/v1/health/signals/{DAY}.json"))
    assert proof["source_key"] == signal_key(DAY) and proof["status"] == "ready"
    assert result["heart_rate_status"] == "stored"
    assert not any("reload" in call[0].lower() for call in garmin.calls)
    print(json.dumps({"synthetic_night_ledger": "targeted", "garmin_calls": len(garmin.calls),
                      "r2": store.operations, "component": result["baseline_refresh"]["component_operations"]}))


def test_targeted_recent_night_publishes_both_channels_all_three_windows(monkeypatch):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)

    result = latest_night.run(run_id="11", wake_date=DAY, store=store, garmin=garmin)

    for width, first in ((28, "2026-09-03"), (30, "2026-09-01"), (90, "2026-07-03")):
        for channel in ("heart_rate", "hrv"):
            key = storage_key(normalize_args(start_date=first, end_date=DAY,
                                             period="custom", channel=channel))
            assert key in server.objects, f"Missing {width}-day {channel} product"
            product = json.loads(store.get(key))
            assert product["nights_requested"] == width
            assert product["contributing_nights"] == 1
    assert result["baseline_refresh"]["selected_remaining"] == 0


def test_normal_scheduled_command_fills_hr_then_reuses_a_qualified_complete_night(monkeypatch):
    clock(monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    store.put("summary/health_daily.csv", b"Date,Sleep Seconds,Weight KG\n", "text/csv")
    options = {"store_factory": lambda: store, "login": lambda: garmin,
               "now": datetime(2026, 9, 30, 7, tzinfo=timezone.utc)}

    result = night_followup.run_command(health_detail_backfill.run, **options)

    assert signal_key(DAY) in server.objects, "Ordinary scheduled night follow-up omitted HR"
    assert result["sleep"]["status"] == "complete"
    garmin.calls.clear()
    night_followup.run_command(health_detail_backfill.run, **options)
    assert garmin.calls == [], "A fully qualified same-night source was fetched twice"


@pytest.mark.parametrize("bad", [{}, {"calendarDate": "2026-09-29", "heartRateValues": []}])
def test_malformed_recent_hr_never_becomes_not_ready_or_canonical_data(monkeypatch, bad):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    garmin.hr = {"2026-09-29": bad, DAY: bad}
    store = sdk_store(server)

    result = latest_night.run(run_id="12", wake_date=DAY, store=store, garmin=garmin)

    assert result["heart_rate_status"] == "import_error"
    assert result["source_checked"] is False
    assert signal_key(DAY) not in server.objects
    assert not any(key.startswith("refresh/checks/v1/health/signals/") for key in server.objects)


class GeneralGarmin(SummaryGarmin):
    def connectapi(self, path, params=None):
        if path == self.garmin_connect_activities:
            self.calls.append((path, params))
            return []
        return super().connectapi(path, params)

    def get_sleep_data(self, day):
        before = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        return {"dailySleepDTO": {"calendarDate": day, "sleepTimeSeconds": 25200,
            "sleepStartTimestampGMT": before + "T21:30:00Z",
            "sleepEndTimestampGMT": day + "T04:30:00Z"},
            "sleepLevels": [{"startGMT": before + "T21:30:00Z",
                             "endGMT": day + "T04:30:00Z", "activityLevel": 1}]}

    def get_hrv_data(self, day):
        return {"hrvSummary": {"calendarDate": day, "lastNightAvg": 44},
                "hrvReadings": [{"readingTimeGMT": day + "T01:00:00Z", "hrvValue": 44}]}

    def get_heart_rates(self, day):
        self.calls.append(("heart", {"day": day}))
        return {"calendarDate": day, "heartRateValues": [[int(datetime.fromisoformat(day).replace(
            tzinfo=timezone.utc).timestamp() * 1000), 55]]}


def test_general_refresh_without_activity_changes_reuses_hr_and_generates_night_windows(
    monkeypatch, tmp_path,
):
    clock(monkeypatch)
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda _: None)
    server, garmin = NightServer(), GeneralGarmin()

    report = refresh.run(refresh.RefreshRequest(run_id="20"), data_dir=str(tmp_path),
                         store_factory=lambda: sdk_store(server), login=lambda: garmin)

    heart_days = [params["day"] for path, params in garmin.calls if path == "heart"]
    assert len(heart_days) == len(set(heart_days)), "General recent-night work fetched HR twice"
    assert not any(key.startswith("activities/") for key in server.objects)
    for first in ("2026-09-03", "2026-09-01", "2026-07-03"):
        for channel in ("heart_rate", "hrv"):
            key = storage_key(normalize_args(start_date=first, end_date=DAY,
                                            period="custom", channel=channel))
            assert key in server.objects, "No activity changes prevented the latest rolling reference"
    recent = next(row for row in report["stages"] if row["stage"] == "recent_health")
    assert recent["baseline_refresh"]["selected_remaining"] > 0
    assert report["status"] == "partial"
    garmin.calls.clear()

    resumed = refresh.run(refresh.RefreshRequest(run_id="21"), data_dir=str(tmp_path),
                          store_factory=lambda: sdk_store(server), login=lambda: garmin)

    recent = next(row for row in resumed["stages"] if row["stage"] == "recent_health")
    assert recent["baseline_refresh"]["selected_remaining"] == 0
    for offset in range(3):
        end = date.fromisoformat(DAY) - timedelta(days=offset)
        for width in (28, 30, 90):
            for channel in ("heart_rate", "hrv"):
                key = storage_key(normalize_args(
                    start_date=(end - timedelta(days=width - 1)).isoformat(),
                    end_date=end.isoformat(), period="custom", channel=channel))
                assert key in server.objects
    print(json.dumps({"synthetic_night_ledger": "general_second_cycle", "selected_remaining": 0,
        "stage_r2": recent["r2_sdk_operations"],
        "component": recent["baseline_refresh"]["component_operations"]}))


@pytest.mark.parametrize("case_id", ["travel", "dst-fallback", "new-year"])
def test_recent_night_keeps_garmin_wake_calendar_and_context(monkeypatch, case_id):
    from pipeline.native_night import NativeNightReader

    server, garmin = NightServer(), Garmin(case_id)
    day = garmin.case["date"]
    clock(monkeypatch, day)
    store = sdk_store(server)

    result = latest_night.run(run_id="30", wake_date=day, store=store, garmin=garmin)

    night = NativeNightReader(store, timezone="Europe/Oslo").night(day)
    assert night["wake_date"] == day
    assert night["night_of"] == garmin.case["expected"]["night_of"]
    hr_days = {value for kind, value in garmin.calls if kind == "heart_rate"}
    assert hr_days == set(night["channels"]["heart_rate"]["source_dates"])
    assert result["baseline_refresh"]["selected_remaining"] == 0


def test_genuine_pending_hr_retries_only_hr_in_a_later_normal_scheduled_job(monkeypatch):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    for day in ("2026-09-29", DAY):
        garmin.hr[day] = {"calendarDate": day, "heartRateValues": []}
    first = latest_night.run(run_id="31", wake_date=DAY, store=store, garmin=garmin)
    assert first["status"] == "pending" and first["source_checked"] is True
    assert first["heart_rate_status"] == "garmin_not_ready"
    garmin.hr.clear()
    garmin.calls.clear()
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    store.put("summary/health_daily.csv", b"Date,Sleep Seconds,Weight KG\n", "text/csv")

    report = night_followup.run_command(health_detail_backfill.run,
        store_factory=lambda: store, login=lambda: garmin,
        now=datetime(2026, 9, 30, 7, tzinfo=timezone.utc))

    assert garmin.calls == [["heart_rate", "2026-09-29"], ["heart_rate", DAY]]
    assert report["night_followup"]["heart_rate"]["status"] == "stored"


@pytest.mark.parametrize("denial,error", [("denied", ClientError), ("unknown", TimeoutError)])
def test_hr_source_check_read_failure_stops_scheduled_job_without_refetch(monkeypatch, denial, error):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    latest_night.run(run_id="32", wake_date=DAY, store=store, garmin=garmin)
    store.put("summary/health_daily.csv", b"Date,Sleep Seconds,Weight KG\n", "text/csv")
    before = copy.deepcopy(server.objects)
    server.denied_key = f"refresh/checks/v1/health/signals/{DAY}.json"
    server.denial = denial
    garmin.calls.clear()
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")

    with pytest.raises(error):
        night_followup.run_command(health_detail_backfill.run,
            store_factory=lambda: store, login=lambda: garmin,
            now=datetime(2026, 9, 30, 7, tzinfo=timezone.utc))

    assert garmin.calls == []
    assert server.objects == before


@pytest.mark.parametrize("module_name", [
    "health_detail_backfill", "hrv_backfill", "activity_backfill_scheduler", "activity_refresh",
])
def test_normal_scheduled_cli_imports_hr_and_all_six_latest_products(monkeypatch, module_name):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    store.put("summary/health_daily.csv",
              b"Date,Sleep Seconds,Weight KG,HRV Last Night Average\n2026-09-30,,,50\n", "text/csv")
    store.put("backfill/activities/plan-v2.json", json.dumps({
        "schema_version": 2, "status": "complete", "ranges": [],
    }).encode(), "application/json")
    module = importlib.import_module("pipeline." + module_name)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setattr(sys, "argv", [module_name])
    monkeypatch.setattr(module, "R2Store", lambda: store)
    monkeypatch.setattr(module, "_login", lambda: garmin)
    monkeypatch.setattr(module, "datetime", latest_night.datetime)

    module.main()

    assert f"refresh/checks/v1/health/signals/{DAY}.json" in server.objects
    for first in ("2026-09-03", "2026-09-01", "2026-07-03"):
        for channel in ("heart_rate", "hrv"):
            assert storage_key(normalize_args(start_date=first, end_date=DAY,
                period="custom", channel=channel)) in server.objects


def test_normal_source_arrival_automatically_improves_existing_partial_rolling_products(
    monkeypatch, tmp_path,
):
    clock(monkeypatch)
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda _: None)

    class ArrivingGarmin(GeneralGarmin):
        ready = False

        def get_heart_rates(self, day):
            if day == "2026-09-29" and not self.ready:
                self.calls.append(("heart", {"day": day}))
                return {"calendarDate": day, "heartRateValues": []}
            return super().get_heart_rates(day)

        def get_hrv_data(self, day):
            if day == "2026-09-29" and not self.ready:
                return {"hrvSummary": {"calendarDate": day}, "hrvReadings": []}
            return super().get_hrv_data(day)

    server, garmin = NightServer(), ArrivingGarmin()
    options = {"data_dir": str(tmp_path), "store_factory": lambda: sdk_store(server),
               "login": lambda: garmin}
    first = refresh.run(refresh.RefreshRequest(run_id="40"), **options)
    assert first["status"] == "partial"
    previous = {}
    for start in ("2026-09-03", "2026-09-01", "2026-07-03"):
        for channel in ("heart_rate", "hrv"):
            key = storage_key(normalize_args(start_date=start, end_date=DAY,
                period="custom", channel=channel))
            body = server.objects[key][0]
            assert json.loads(body)["contributing_nights"] == 2
            previous[key] = body
    unrelated = {key: body for key, (body, _) in server.objects.items()
        if "/baseline/" in key and json.loads(body)["args"]["end_date"] < "2026-09-26"}
    assert unrelated, "Ordinary calendar work must provide an unrelated stored-product control"
    server.puts.clear()
    garmin.ready = True

    second = refresh.run(refresh.RefreshRequest(run_id="41"), **options)

    history = [body for key, (body, _) in server.objects.items()
               if "/history/" in key and key.endswith("/product.json")]
    for key, old in previous.items():
        assert json.loads(server.objects[key][0])["contributing_nights"] == 3
        assert server.objects[key][0] != old and old in history
    assert not set(unrelated).intersection(server.puts)
    assert all(server.objects[key][0] == body for key, body in unrelated.items())
    recent = next(row for row in second["stages"] if row["stage"] == "recent_health")
    print(json.dumps({"synthetic_night_ledger": "automatic_partial_improvement",
        "products_improved": len(previous), "contribution_before": 2, "contribution_after": 3,
        "unrelated_products_unchanged": len(unrelated), "stage_r2": recent["r2_sdk_operations"]}))
