"""Night HR collection prepares missing daily channels before strict reading."""

import gzip
import json
from datetime import UTC, datetime

import pytest
from test_history_baseline_integration import sdk_store
from test_recent_night_production import Garmin, NightServer, clock

from pipeline.health_sync import store_day
from pipeline.latest_night import collect_heart_rate
from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.r2_store import R2ReadBoundaryError
from pipeline.signal_series import prepare_signal_day, signal_key

DAY = "2026-09-30"


def test_partial_daily_source_can_gain_hr_before_strict_night_read(monkeypatch):
    clock(monkeypatch)
    store, garmin = sdk_store(NightServer()), Garmin()
    store_day(store, "sleep", DAY, garmin.get_sleep_data(DAY))
    partial = prepare_signal_day(DAY, {"body_battery": {
        "calendarDate": DAY, "bodyBatteryValuesArray": [[1790726400000, 0]],
    }})
    store.put(signal_key(DAY), json.dumps(partial).encode(), "application/json")
    # The normal reader must remain strict until the absent HR is collected.
    with pytest.raises(NativeInputError):
        NativeNightReader(store, timezone="Europe/Oslo").night(DAY)

    result = collect_heart_rate(store, garmin, [DAY], timezone="Europe/Oslo")

    assert result["status"] == "stored"
    assert [row["date"] for row in result["source_records"]] == ["2026-09-29", DAY]
    night = NativeNightReader(store, timezone="Europe/Oslo").night(DAY)
    assert night["channels"]["heart_rate"]["source_dates"] == ["2026-09-29", DAY]
    assert night["channels"]["heart_rate"]["valid_points"] > 0
    canonical = json.loads(gzip.decompress(store.get(signal_key(DAY))))
    assert canonical["channels"]["body_battery"] == partial["channels"]["body_battery"]
    print(json.dumps({"synthetic_hr_prerequisite": "partial_and_missing",
                      "source_days": 2, "r2": store.operations}))


def test_acknowledged_daily_hr_is_reused_without_provider_login(monkeypatch):
    clock(monkeypatch)
    store, garmin = sdk_store(NightServer()), Garmin()
    store_day(store, "sleep", DAY, garmin.get_sleep_data(DAY))
    first = collect_heart_rate(store, garmin, [DAY], timezone="Europe/Oslo")
    garmin.calls.clear()

    def no_login():
        pytest.fail("Acknowledged HR must not request provider credentials")

    resumed = collect_heart_rate(store, None, [DAY], timezone="Europe/Oslo",
                                 reuse_checks=True, now=datetime(2026, 9, 30, 7, tzinfo=UTC),
                                 login=no_login)

    assert resumed["status"] == "stored"
    assert [{field: row[field] for field in ("date", "source_key", "source_sha256", "checked_at")}
            for row in resumed["source_records"]] == first["source_records"]
    assert resumed["objects_written"] == 0
    assert garmin.calls == []
    assert NativeNightReader(store, timezone="Europe/Oslo").night(DAY)["channels"][
        "heart_rate"]["valid_points"] > 0


@pytest.mark.parametrize("failure", [PermissionError("unauthorized"), TimeoutError("synthetic")])
def test_provider_auth_and_transport_failures_stop_hr_collection(monkeypatch, failure):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    store_day(store, "sleep", DAY, garmin.get_sleep_data(DAY))

    class FailedGarmin:
        def get_heart_rates(self, day):
            raise failure

    with pytest.raises(RuntimeError, match="Garmin service or authentication error"):
        collect_heart_rate(store, FailedGarmin(), [DAY], timezone="Europe/Oslo")
    assert not any(key.startswith("health/signals/") for key in server.objects)
    assert not any(key.startswith("refresh/checks/v1/health/signals/") for key in server.objects)


def test_unknown_stored_signal_schema_cannot_be_replaced_or_acknowledged(monkeypatch):
    clock(monkeypatch)
    server, garmin = NightServer(), Garmin()
    store = sdk_store(server)
    store_day(store, "sleep", DAY, garmin.get_sleep_data(DAY))
    day = "2026-09-29"
    invalid = json.dumps({"schema_version": 2, "date": day, "channels": {}}).encode()
    store.put(signal_key(day), invalid, "application/json")

    with pytest.raises(R2ReadBoundaryError):
        collect_heart_rate(store, garmin, [DAY], timezone="Europe/Oslo")
    assert store.get(signal_key(day)) == invalid
    assert not any(key.startswith("refresh/checks/v1/health/signals/") for key in server.objects)


@pytest.mark.parametrize("case_id, expected_dates, night_of", [
    ("travel", ["2026-06-11", "2026-06-12"], "2026-06-12"),
    ("dst-fallback", ["2026-10-24", "2026-10-25"], "2026-10-24"),
    ("new-year", ["2025-12-31", "2026-01-01"], "2025-12-31"),
])
def test_sleep_context_calendar_matches_strict_reader_after_hr_collection(
        monkeypatch, case_id, expected_dates, night_of):
    garmin = Garmin(case_id)
    day = garmin.case["date"]
    clock(monkeypatch, day)
    store = sdk_store(NightServer())
    store_day(store, "sleep", day, garmin.get_sleep_data(day))

    result = collect_heart_rate(store, garmin, [day], timezone="Europe/Oslo")

    assert result["status"] == "stored"
    assert [row["date"] for row in result["source_records"]] == expected_dates
    night = NativeNightReader(store, timezone="Europe/Oslo").night(day)
    assert night["channels"]["heart_rate"]["source_dates"] == expected_dates
    assert night["night_of"] == night_of
    print(json.dumps({"synthetic_hr_prerequisite": case_id,
                      "source_days": len(expected_dates), "r2": store.operations}))
