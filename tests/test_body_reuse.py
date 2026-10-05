"""Real R2Store regressions for bounded individual weigh-in refresh."""
import gzip
import json
from datetime import date, datetime

import pytest
from botocore.exceptions import ClientError

from pipeline.health_sync import normalize_day
from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.recent_body import run
from pipeline.recent_health import run as night_refresh
from scripts.benchmark_body_reuse import PREFIX, SUMMARY, TODAY, HealthClient, SyntheticBodyGarmin

DAY = TODAY.isoformat()
DAYS = [DAY, "2026-09-22", "2026-09-10"]


def canonical(day=DAY):
    return f"{PREFIX}{day[:4]}/{day[5:7]}/{day}.json"


def check(day=DAY):
    return f"refresh/checks/v1/health/body_composition/{day}.json"


def document(client, key):
    data = client.objects[key]["Body"]
    return json.loads(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)


@pytest.fixture
def environment():
    client, garmin = HealthClient(), SyntheticBodyGarmin()

    def refresh(**kwargs):
        return run(store=R2Store(client=client, bucket="synthetic-body"), garmin=garmin,
                   health_csv=SUMMARY, today=TODAY, **kwargs)

    return client, garmin, refresh


def test_repeat_keeps_all_measurements_and_reuses_canonical_without_plan_reads(environment):
    client, garmin, refresh = environment
    refresh()
    before = {key: value.copy() for key, value in client.objects.items() if key.startswith(PREFIX)}
    previous = document(client, check())
    client.reset()
    garmin.calls = []
    result = refresh()
    assert garmin.calls == DAYS  # Three measured dates, not three calendar days.
    assert result["status"] == "complete" and result["objects_written"] == 0 and result["objects_unchanged"] == 3
    assert client.reads == [] and client.prefixes == [""]  # Required bucket-budget inventory only.
    assert client.writes == [check(day) for day in DAYS]
    assert all(client.objects[key] == value for key, value in before.items())
    current = document(client, check())
    assert current["source_sha256"] == previous["source_sha256"]
    assert datetime.fromisoformat(current["checked_at"]) > datetime.fromisoformat(previous["checked_at"])
    payload = document(client, canonical())
    assert payload["measurement_count"] == 3
    assert {item["timestamp_local"] for item in payload["measurements"]} == {f"{DAY}T{hour:02}:00:00" for hour in (7, 14, 21)}
    assert {item["measurement_id"] for item in payload["measurements"]} == {1, 2, 3}
    assert all(not item["is_daily_average"] for item in payload["measurements"])


def test_provider_order_alone_is_not_a_change_and_duplicates_are_preserved(environment):
    client, garmin, refresh = environment
    raw = garmin.get_daily_weigh_ins(DAY)
    raw["dateWeightList"].append(raw["dateWeightList"][0].copy())
    garmin.responses[DAY] = raw
    refresh()
    before = client.objects[canonical()].copy()
    raw["dateWeightList"].reverse()
    result = refresh()
    assert result["objects_written"] == 0 and client.objects[canonical()] == before
    assert document(client, canonical())["measurement_count"] == 4


@pytest.mark.parametrize("edit", ["added", "corrected", "removed"])
def test_source_edit_replaces_only_affected_day_preserving_actual_fields(environment, edit):
    client, garmin, refresh = environment
    refresh()
    raw = garmin.get_daily_weigh_ins(DAY)
    if edit == "added":
        raw["dateWeightList"].append({**raw["dateWeightList"][-1], "samplePk": 4, "weight": 80500})
    elif edit == "corrected":
        raw["dateWeightList"][0]["bodyFat"] = 17.1
    else:
        raw["dateWeightList"].pop()
    garmin.responses[DAY] = raw
    other = client.objects[canonical(DAYS[1])].copy()
    client.reset()
    result = refresh()
    assert result["objects_written"] == 1 and result["objects_unchanged"] == 2
    assert [key for key in client.writes if key.startswith(PREFIX)] == [canonical()]
    payload = document(client, canonical())
    assert payload["measurement_count"] == len(raw["dateWeightList"])
    assert all(item["muscle_mass_kg"] == 60.1 and item["source_type"] == "MANUAL" for item in payload["measurements"])
    assert client.objects[canonical(DAYS[1])] == other


@pytest.mark.parametrize("defect", ["empty", "average", "missing_list", "list_wrong_type", "bad_row",
    "wrong_root_date", "wrong_row_date", "mixed_dates", "empty_row", "latest_only", "empty_nested",
    "bad_nested", "wrong_nested_date", "missing_weight", "negative_weight", "nan_weight", "boolean_weight",
    "missing_time", "bad_gmt", "bad_local", "wrong_local_date", "date_only", "huge_time", "partial_rows", "aggregate_only"])
def test_partial_invalid_or_wrong_date_preserves_previous_data_and_success_check(environment, defect):
    client, garmin, refresh = environment
    refresh()
    before = {key: client.objects[key].copy() for key in (canonical(), check())}
    raw = garmin.get_daily_weigh_ins(DAY)
    row = raw["dateWeightList"][0]
    if defect == "empty":
        raw["dateWeightList"] = []
    elif defect == "average":
        raw["dateWeightList"] = [{"calendarDate": DAY, "weight": 80000, "isDailyAverage": True}]
    elif defect == "missing_list":
        del raw["dateWeightList"]
    elif defect == "list_wrong_type":
        raw["dateWeightList"] = {}
    elif defect == "bad_row":
        raw["dateWeightList"].append(None)
    elif defect == "wrong_root_date":
        raw["calendarDate"] = "2026-09-29"
    elif defect in {"wrong_row_date", "mixed_dates"}:
        row["calendarDate"] = "2026-09-29"
        if defect == "wrong_row_date":
            raw["dateWeightList"] = [row]
    elif defect == "empty_row":
        raw["dateWeightList"] = [{}]
    elif defect == "latest_only":
        raw["dateWeightList"] = [{"calendarDate": DAY, "latestWeight": row}]
    elif defect == "empty_nested":
        raw["dateWeightList"] = [{"calendarDate": DAY, "allWeightMetrics": []}]
    elif defect == "bad_nested":
        raw["dateWeightList"] = [{"calendarDate": DAY, "allWeightMetrics": [row, None]}]
    elif defect == "wrong_nested_date":
        raw["dateWeightList"] = [{"calendarDate": DAY, "allWeightMetrics": [{**row, "calendarDate": "2026-09-29"}]}]
    elif defect in {"missing_weight", "negative_weight", "nan_weight", "boolean_weight"}:
        row["weight"] = {"missing_weight": None, "negative_weight": -1, "nan_weight": float("nan"), "boolean_weight": True}[defect]
    elif defect == "missing_time":
        del row["timestampGMT"], row["timestampLocal"]
    elif defect == "bad_gmt":
        row["timestampGMT"] = "invalid"
    elif defect == "bad_local":
        row["timestampLocal"] = "invalid"
    elif defect == "wrong_local_date":
        row["timestampLocal"] = "2026-09-29T23:30:00"
    elif defect == "date_only":
        row["timestampGMT"] = DAY
    elif defect == "huge_time":
        row["timestampGMT"] = 1e99
    elif defect == "aggregate_only":
        del row["weight"]
        row["maxWeight"] = 80000
    else:
        raw["dateWeightList"].append({"calendarDate": DAY, "timestampGMT": f"{DAY}T21:30:00Z"})
    garmin.responses[DAY] = raw
    client.reset()
    result = refresh()
    assert result["status"] == "partial" and result["days_not_ready"] == 1
    assert all(client.objects[key] == value for key, value in before.items())
    assert canonical() not in client.writes and check() not in client.writes


def test_nested_full_dayview_preserves_all_measurements(environment):
    client, garmin, refresh = environment
    raw = garmin.get_daily_weigh_ins(DAY)
    garmin.responses[DAY] = {"dateWeightList": [{"calendarDate": DAY, "allWeightMetrics": raw["dateWeightList"]}]}
    result = refresh()
    assert result["status"] == "complete" and document(client, canonical())["measurement_count"] == 3


def test_local_date_does_not_come_from_utc_at_travel_midnight():
    raw = {"dateWeightList": [{"calendarDate": "2026-01-01", "weight": 80000,
                               "timestampGMT": "2025-12-31T23:30:00Z", "timestampLocal": "2026-01-01T01:30:00"}]}
    result = normalize_day("body_composition", "2026-01-01", raw)
    assert result["date"] == "2026-01-01" and result["measurements"][0]["timestamp_local"] == "2026-01-01T01:30:00"


def test_service_failure_stops_batch_without_advancing_its_receipts(environment):
    client, garmin, refresh = environment
    refresh()
    before = {check(day): client.objects[check(day)].copy() for day in DAYS}
    garmin.responses[DAYS[1]] = TimeoutError("synthetic service failure")
    with pytest.raises(RuntimeError, match="Garmin service"):
        refresh()
    assert all(client.objects[key] == value for key, value in before.items())


def test_day_source_failure_preserves_only_failed_scope_and_never_logs_private_error(environment, capsys):
    client, garmin, refresh = environment
    refresh()
    before = client.objects[check()].copy()
    capsys.readouterr()
    garmin.responses[DAY] = ValueError("PRIVATE_SOURCE_DETAIL")
    result = refresh()
    assert result["source_errors"] == 1 and result["status"] == "partial"
    assert client.objects[check()] == before
    assert "PRIVATE_SOURCE_DETAIL" not in capsys.readouterr().out


@pytest.mark.parametrize("error", ["403", "SlowDown"])
def test_storage_head_failures_propagate_and_preserve_success_checks(environment, monkeypatch, error):
    client, _, refresh = environment
    refresh()
    before = client.objects[check()].copy()
    def fail(**kwargs):
        raise ClientError({"Error": {"Code": error}}, "HeadObject")
    monkeypatch.setattr(client, "head_object", fail)
    with pytest.raises(ClientError):
        refresh()
    assert client.objects[check()] == before


def test_missing_canonical_is_repaired_despite_existing_successful_receipt(environment):
    client, _, refresh = environment
    refresh()
    del client.objects[canonical()]
    client.reset()
    result = refresh()
    assert result["objects_written"] == 1 and canonical() in client.objects


def test_checkpoint_failure_retry_reuses_completed_canonical_write(environment, monkeypatch):
    client, garmin, refresh = environment
    refresh()
    before = client.objects[check()].copy()
    raw = garmin.get_daily_weigh_ins(DAY)
    raw["dateWeightList"][0]["weight"] = 79000
    garmin.responses[DAY] = raw
    original = client.put_object
    def fail(**kwargs):
        if kwargs["Key"] == check():
            raise RuntimeError("synthetic interrupted checkpoint")
        return original(**kwargs)
    monkeypatch.setattr(client, "put_object", fail)
    with pytest.raises(RuntimeError, match="interrupted checkpoint"):
        refresh()
    assert client.objects[check()] == before
    monkeypatch.setattr(client, "put_object", original)
    client.reset()
    result = refresh()
    assert result["objects_written"] == 0 and result["checkpoints_written"] == 3
    assert document(client, check())["source_sha256"] != json.loads(before["Body"])["source_sha256"]


def test_canonical_and_check_writes_keep_existing_budget_guards():
    client, garmin = HealthClient(), SyntheticBodyGarmin()
    store = R2Store(client=client, bucket="synthetic-body", max_writes_per_run=1)
    with pytest.raises(R2BudgetError):
        run(max_days=1, health_csv=SUMMARY, store=store, garmin=garmin, today=TODAY)
    assert canonical() in client.objects and check() not in client.objects


def test_history_plan_and_unselected_old_day_are_never_read_or_changed(environment):
    client, _, refresh = environment
    for key in ("backfill/body-composition/v1/plan.json", canonical("2020-01-01"), f"refresh/checks/v1/night/{DAY}.json"):
        client.put_object(Key=key, Body=b"synthetic unchanged history", ContentType="application/json")
    before = {key: value.copy() for key, value in client.objects.items()}
    client.reset()
    refresh()
    assert all(client.objects[key] == value for key, value in before.items())
    assert client.reads == [] and client.prefixes == [""]


@pytest.mark.parametrize("limit", [0, 15, True, 1.5])
def test_invalid_limit_never_creates_store_or_logs_into_provider(limit, monkeypatch):
    monkeypatch.setattr("pipeline.recent_body.R2Store", lambda: pytest.fail("No store for invalid arguments"))
    with pytest.raises(ValueError):
        run(max_days=limit)


@pytest.mark.parametrize("summary", [b"wrong", b"Date,Steps\n2026-09-30,1\n", b"\x1f\x8bbad"])
def test_invalid_summary_does_not_log_in_or_advance_any_check(summary, monkeypatch):
    monkeypatch.setattr("pipeline.recent_body._login", lambda: pytest.fail("Invalid summary must precede login"))
    client = HealthClient()
    with pytest.raises(ValueError):
        run(health_csv=summary, store=R2Store(client=client, bucket="synthetic-body"))
    assert not client.writes


def test_no_weight_dates_are_idle_without_login_or_checkpoint_claim(monkeypatch):
    monkeypatch.setattr("pipeline.recent_body._login", lambda: pytest.fail("Empty dates must not log in"))
    client = HealthClient()
    result = run(health_csv=b"Date,Weight KG\n2026-09-30,\n", store=R2Store(client=client, bucket="synthetic-body"))
    assert result["status"] == "idle" and result["days_checked"] == result["checkpoints_written"] == 0
    assert client.prefixes == client.writes == client.reads == []


def test_summary_r2_fallback_and_gzip_preserve_selected_measurement_dates():
    client, garmin = HealthClient(), SyntheticBodyGarmin()
    client.put_object(Key="summary/health_daily.csv", Body=gzip.compress(SUMMARY), ContentType="text/csv")
    client.reset()
    result = run(store=R2Store(client=client, bucket="synthetic-body"), garmin=garmin, today=TODAY)
    assert result["status"] == "complete" and garmin.calls == DAYS
    assert client.reads == ["summary/health_daily.csv"]


def test_future_dates_are_not_selected_and_month_boundaries_keep_exact_source_days():
    summary = b"Date,Weight KG\n2026-01-03,80\n2026-01-01,80\n2025-12-31,80\n2025-12-22,80\n"
    client, garmin = HealthClient(), SyntheticBodyGarmin()
    run(health_csv=summary, store=R2Store(client=client, bucket="synthetic-body"), garmin=garmin, today=date(2026, 1, 1))
    assert garmin.calls == ["2026-01-01", "2025-12-31", "2025-12-22"]
    assert all(canonical(day) in client.objects for day in garmin.calls)


def test_garmin_local_day_ahead_of_utc_runner_is_kept_verbatim():
    client, garmin = HealthClient(), SyntheticBodyGarmin()
    run(health_csv=b"Date,Weight KG\n2026-01-01,80\n", store=R2Store(client=client, bucket="synthetic-body"),
        garmin=garmin, today=date(2025, 12, 31))
    assert garmin.calls == ["2026-01-01"] and canonical("2026-01-01") in client.objects


def test_fall_back_repeated_local_time_keeps_both_measurements():
    day = "2026-10-25"
    raw = {"dateWeightList": [{"calendarDate": day, "samplePk": index + 1, "weight": 80000 + index * 100,
                               "timestampGMT": f"{day}T0{index}:30:00Z", "timestampLocal": f"{day}T02:30:00"}
                              for index in range(2)]}
    result = normalize_day("body_composition", day, raw)
    assert result["measurement_count"] == 2
    assert {item["measurement_id"] for item in result["measurements"]} == {1, 2}
    assert {item["timestamp_gmt"] for item in result["measurements"]} == {f"{day}T00:30:00Z", f"{day}T01:30:00Z"}


def test_body_stream_cannot_accidentally_use_sleep_hrv_entrypoint():
    with pytest.raises(ValueError):
        night_refresh(streams=("body_composition",))
