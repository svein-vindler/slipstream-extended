"""Ordinary synthetic source collection retains nightly signal source curves."""
import gzip
import json
from datetime import timedelta

import pytest

from pipeline.r2_store import R2Store
from pipeline.refresh_summaries import run
from scripts.benchmark_summary_windows import NOW, HealthClient, SummaryGarmin

DAY = "2026-09-30"
KEY = "health/signals/v1/2026/09/2026-09-30.json"


class SignalGarmin(SummaryGarmin):
    def get_heart_rates(self, day):
        super().get_heart_rates(day)
        if day == DAY and hasattr(self, "heart_reply"):
            return self.heart_reply
        return {"calendarDate": day, "heartRateValues": [
            [1790726400000, 80], [1790726520000, 48],
        ]} if day == DAY else {"calendarDate": day, "heartRateValues": []}

    def get_body_battery(self, first, last):
        super().get_body_battery(first, last)
        if hasattr(self, "battery_reply"):
            return self.battery_reply
        return [{"date": DAY, "bodyBatteryValuesArray": [[1790726400000, 65], [1790726520000, 66]]}]


def read(client, key=KEY):
    data = client.objects[key]["Body"]
    return json.loads(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)


def test_ordinary_refresh_retains_existing_hr_and_battery_responses_without_more_calls(tmp_path):
    storage, source = HealthClient(), SignalGarmin()
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
                 client=source, now=NOW, request_pause=0)
    assert KEY in storage.objects, "Ordinary refresh discarded timed HR and Body Battery"
    payload = read(storage)
    assert payload["date"] == DAY
    assert payload["schema_version"] == 1 and payload["source"] == "garmin"
    hr = payload["channels"]["heart_rate"]
    battery = payload["channels"]["body_battery"]
    assert hr["status"] == battery["status"] == "available"
    assert hr["unit"] == "bpm" and battery["unit"] == "points"
    assert [(r["timestamp"], r["value"]) for r in hr["readings"]] == [
        (1790726400000, 80), (1790726520000, 48),
    ]
    assert hr["readings"][0]["time_basis"] == "utc"
    assert hr["readings"][0]["timestamp_unit"] == "epoch_milliseconds"
    assert [r["value"] for r in battery["readings"]] == [65, 66]
    assert result["health_diagnostics"]["sdk_calls"] == 61
    assert result["provider_diagnostics"]["garmin_api_calls"] == 66

@pytest.mark.parametrize("replacement", [
    {"calendarDate": DAY, "heartRateValues": []},
    {"calendarDate": DAY, "heartRateValues": [[1790726400000, "invalid"]]},
    {"calendarDate": DAY, "heartRateValues": [[1790726400000, 70], [True, 50]]},
    {"calendarDate": "2026-09-29", "heartRateValues": [[1790726400000, 70]]},
    {},
])
def test_refresh_preserves_prior_good_channel_on_empty_or_invalid_source(tmp_path, replacement):
    storage, source = HealthClient(), SignalGarmin()
    first = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
                client=source, now=NOW, request_pause=0)
    before = storage.objects[KEY]["Body"]
    source.heart_reply = replacement
    source.battery_reply = []
    storage.reset()
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
                 client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert storage.objects[KEY]["Body"] == before, "Unavailable reply replaced an available curve"
    assert KEY not in storage.writes
    assert first["signals"]["objects_written"] == 14
    assert result["signals"]["objects_written"] == 0
    assert result["signals"]["objects_unchanged"] == 3

@pytest.mark.parametrize("field,timestamp,basis,unit", [
    ("readingTimeGMT", "2020-01-02T01:00:00", "utc", "iso8601"),
    ("readingTimeLocal", "2020-01-02T01:00:00", "local_only", "iso8601"),
    ("readingTimeLocal", "2020-01-02T01:00:00Z", "local_only", "iso8601"),
    ("timestamp", "2020-01-02T01:00:00", "ambiguous", "iso8601"),
    ("timestamp", "2020-01-02T01:00:00+02:00", "utc", "iso8601"),
    ("readingTimestamp", 1577926800, "utc", "epoch_seconds"),
    ("readingTimestamp", 1577926800000, "utc", "epoch_milliseconds"),
])
def test_hrv_import_preserves_timestamp_basis_for_utc_curve_readers(field, timestamp, basis, unit):
    from pipeline.hrv_backfill import run as import_hrv

    day = "2020-01-02"
    storage = HealthClient()
    storage.put_object(Key="summary/health_daily.csv", Body=b"Date,HRV Last Night Average\n2020-01-02,42\n",
                       ContentType="text/csv")

    class HrvSource:
        def get_hrv_data(self, requested):
            assert requested == day
            return {"hrvSummary": {"calendarDate": day, "lastNightAvg": 42},
                    "hrvReadings": [{field: timestamp, "hrvValue": 42}]}

    import_hrv(max_days=1, store=R2Store(client=storage, bucket="synthetic-signals"), garmin=HrvSource())
    item = read(storage, "health/hrv/2020/01/2020-01-02.json")["readings"][0]
    assert item.get("time_basis") == basis, "Source clock origin was lost at ingestion"
    assert item["timestamp_field"] == field
    assert item["timestamp_unit"] == unit
    assert item["timestamp"] == timestamp and item["raw"][field] == timestamp

def test_ordinary_refresh_keeps_valid_points_beside_explicit_missing_hr_samples(tmp_path):
    storage, source = HealthClient(), SignalGarmin()
    source.heart_reply = {"calendarDate": DAY, "heartRateValues": [
        [1790726400000, 80], [1790726520000, None], [1790726640000, 0], [1790726760000, 48],
    ]}
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
        client=source, now=NOW, request_pause=0)
    channel = read(storage)["channels"]["heart_rate"]
    assert channel["status"] == "available", "Missing readings discarded the observed curve"
    assert [(row["timestamp"], row["value"]) for row in channel["readings"]] == [
        (1790726400000, 80), (1790726760000, 48),
    ]
    assert channel["missing_value_count"] == 2
    assert "missing_values" in channel["quality_flags"]

@pytest.mark.parametrize("defect", ["read_error", "corrupt_gzip", "wrong_date", "decoded_oversize"])
def test_stored_signal_read_failure_never_overwrites_unknown_prior_data(tmp_path, monkeypatch, defect):
    from botocore.exceptions import ClientError

    from pipeline.r2_store import R2ReadBoundaryError

    storage, source = HealthClient(), SignalGarmin()
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
        client=source, now=NOW, request_pause=0)
    old_check = storage.objects["refresh/checks/v1/summary/health/recent.json"]["Body"]
    if defect == "corrupt_gzip":
        storage.objects[KEY]["Body"] = b"\x1f\x8bnot-gzip"
    elif defect == "wrong_date":
        storage.objects[KEY]["Body"] = gzip.compress(json.dumps({
            "schema_version": 1, "source": "garmin", "date": "2026-09-29", "channels": {},
        }).encode())
    elif defect == "decoded_oversize":
        storage.objects[KEY]["Body"] = gzip.compress(b" " * (1024 * 1024 + 1))
    else:
        original = storage.get_object

        def deny_signal(**kwargs):
            if kwargs["Key"] == KEY:
                raise ClientError({"Error": {"Code": "403"}}, "SyntheticGet")
            return original(**kwargs)

        monkeypatch.setattr(storage, "get_object", deny_signal)
    before = storage.objects[KEY]["Body"]
    storage.reset()
    with pytest.raises((ClientError, R2ReadBoundaryError)):
        run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
            client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert storage.objects[KEY]["Body"] == before
    assert KEY not in storage.writes
    assert storage.objects["refresh/checks/v1/summary/health/recent.json"]["Body"] == old_check

def test_hrv_source_import_index_slope_uses_known_utc_without_dropping_numeric_observations():
    from pipeline.hrv_backfill import run as import_hrv

    day = "2020-01-02"
    storage = HealthClient()
    storage.put_object(Key="summary/health_daily.csv", Body=b"Date,HRV Last Night Average\n2020-01-02,42\n",
                       ContentType="text/csv")

    class HrvSource:
        def get_hrv_data(self, requested):
            assert requested == day
            return {"hrvSummary": {"calendarDate": day, "lastNightAvg": 46},
                    "hrvReadings": [{"readingTimeGMT": "2020-01-01T22:00:00", "hrvValue": 42},
                                    {"readingTimeGMT": "2020-01-02T00:00:00Z", "hrvValue": 50},
                                    {"readingTimeLocal": "2020-01-02T01:00:00", "hrvValue": 999}]}

    import_hrv(max_days=1, store=R2Store(client=storage, bucket="synthetic-signals"), garmin=HrvSource())
    index = read(storage, "health/indexes/hrv/v1/2020/01.json")
    derived = index["days"][0]["derived"]
    assert derived["slope_ms_per_hour"] == 4, "Local-only readings contaminated the UTC slope"
    assert derived["valid_reading_count"] == 3
    assert derived["mean_ms"] == 363.667 and derived["median_ms"] == 50
    assert derived["maximum_ms"] == 999 and index["builder_revision"] == 3

@pytest.mark.parametrize("uncertain", [
    {"timestamp": "2020-01-02T01:00:00", "time_basis": "local_only"},
    {"timestamp": "2020-01-02T01:00:00Z", "timestamp_field": "readingTimeLocal"},
    {"timestamp": "2020-01-02T01:00:00Z", "raw": {"readingTimeLocal": "2020-01-02T01:00:00Z"}},
    {"timestamp": "2020-01-02T01:00:00"},
    {"timestamp": "2020-01-02T01:00:00Z", "time_basis": "ambiguous"},
    {"timestamp": "2020-01-02T01:00:00Z", "time_basis": "unknown"},
])
def test_stored_legacy_hrv_index_uses_gmt_evidence_and_excludes_uncertain_clocks(uncertain):
    from pipeline.granular import gzip_json
    from pipeline.health_history_index import sync_stream

    storage = HealthClient()
    storage.put_object(Key="health/hrv/2020/01/2020-01-02.json", Body=gzip_json({
        "date": "2020-01-02", "summary": {}, "readings": [
            {"timestamp": "2020-01-01T22:00:00", "hrv_ms": 42,
             "raw": {"readingTimeGMT": "2020-01-01T22:00:00"}},
            {"timestamp": 1577923200000, "hrv_ms": 50},
            {**uncertain, "hrv_ms": 999},
        ],
    }), ContentType="application/json", ContentEncoding="gzip")
    sync_stream("hrv", store=R2Store(client=storage, bucket="synthetic-signals"))
    derived = read(storage, "health/indexes/hrv/v1/2020/01.json")["days"][0]["derived"]
    assert derived["slope_ms_per_hour"] == 4
    assert derived["valid_reading_count"] == 3 and derived["maximum_ms"] == 999
    assert derived["mean_ms"] == 363.667