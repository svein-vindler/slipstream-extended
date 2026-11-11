"""Finite annual raw input, separate from the unchanged normalized product budget."""
import gzip
import hashlib
import json
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_night_baseline_corrections import calendar_store
from test_rolling_reference_sources import proof_for

from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.night_baseline_refresh import BaselineRefreshStop, prepare_reference


def dense_sources(*, count=300, last_count=None):
    first = date(2025, 1, 1)
    days = [(first + timedelta(days=index)).isoformat() for index in range(365)]
    store = calendar_store(first, 365, contexts=False)
    for key, snapshot in list(store.objects.items()):
        if key.startswith("health/signals/"):
            value = json.loads(snapshot.data)
            timestamp = int(datetime.fromisoformat(value["date"] + "T12:00:00+00:00").timestamp())
            size = last_count if last_count is not None and value["date"] == days[-1] else count
            value["channels"]["heart_rate"]["readings"] = [
                {"timestamp": timestamp, "value": 55, "time_basis": "utc"} for _ in range(size)]
            body = gzip.compress(json.dumps(value, separators=(",", ":")).encode(), mtime=0)
            store.objects[key] = SimpleNamespace(data=body, revision='"' + hashlib.sha256(body).hexdigest() + '"',
                                                 content_type="application/json", encoding="gzip")
    return store, days


def prepare(reader, days, nights):
    return prepare_reference(nights, {"start_date": days[0], "end_date": days[-1], "channel": "hrv"},
                             proof_for(reader, days))


def test_explicit_annual_raw_over_200k_still_prepares_small_normalized_product():
    store, days = dense_sources()
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    nights = [reader.night(day) for day in days]
    artifact = prepare(reader, days, nights)
    assert reader.input_points == 219365
    assert sum(len(channel["points"]) for night in nights for channel in night["channels"].values()) == 365
    assert artifact["args"]["start_date"] == "2025-01-01" and artifact["args"]["end_date"] == "2025-12-31"
    assert not store.writes and store.operations["put"] == 0


@pytest.mark.parametrize("prime", [False, True])
def test_legacy_and_default_prime_keep_200000_raw_limit(prime):
    store, days = dense_sources(count=1200)
    reader = NativeNightReader(store, timezone="UTC")
    if prime:
        reader.prime_rolling_sources(days[:90])
    with pytest.raises(NativeInputError, match="Stored native raw input point limit") as caught:
        for day in days[:90]:
            reader.night(day)
    assert caught.value.category == "native_raw_input_point_limit"
    assert 200000 < reader.input_points < 203000 and not store.writes


def test_annual_raw_cap_accepts_exact_811112_then_refuses_more_work():
    store, days = dense_sources(count=1110, last_count=1557)
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    nights = [reader.night(day) for day in days]
    assert reader.input_points == 811112
    assert prepare(reader, days, nights)["kind"] == "prepared-night-baseline"
    with pytest.raises(NativeInputError, match="Stored native raw input point limit") as caught:
        reader.night(days[-1])
    assert caught.value.category == "native_raw_input_point_limit"
    assert reader.input_points == 813780 and not store.writes


@pytest.mark.parametrize("prime", ["default", "inexact365", "other_marker"])
def test_other_or_failed_prime_does_not_retain_annual_budget(prime):
    store, days = dense_sources()
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    for day in days[:340]:
        reader.night(day)
    assert reader.input_points == 204340
    if prime == "default":
        reader.prime_rolling_sources([days[-1]])
    else:
        with pytest.raises(NativeInputError):
            reader.prime_rolling_sources(days[:-1] if prime == "inexact365" else days[:90],
                                         window_days=365 if prime == "inexact365" else 90)
    with pytest.raises(NativeInputError, match="Stored native raw input point limit") as caught:
        reader.night(days[-1])
    assert caught.value.category == "native_raw_input_point_limit" and not store.writes


def test_normalized_prepare_reference_still_refuses_more_than_200000_points():
    store, days = dense_sources()
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    nights = [reader.night(day) for day in days]
    # Synthetic normalized inputs are deliberately small in bytes. The public
    # prepare seam must enforce its point cap independently of raw admission.
    for night in nights:
        night["channels"]["hrv"]["points"] = [{}] * 549
    with pytest.raises(BaselineRefreshStop, match="Native reference point limit"):
        prepare(reader, days, nights)
    assert reader.input_points == 219365 and not store.writes
