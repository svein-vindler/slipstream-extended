"""Explicit365 byte admission through real native preparation and sealed products."""
import gzip
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_night_baseline_corrections import calendar_store
from test_reference_workset import declaration, inventory
from test_rolling_reference_sources import proof_for

from pipeline.history_state import Stop
from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.night_baseline_refresh import BaselineRefreshStop, _prepared_value, prepare_reference
from pipeline.night_products import (
    canonical,
    normalize_args,
    reference_input_byte_limit,
    storage_key,
)
from pipeline.reference_workset import ReferenceWorkset


def annual_sources():
    first = date(2025, 1, 1)
    days = [(first + timedelta(days=index)).isoformat() for index in range(365)]
    store = calendar_store(first, 365, contexts=False)
    for key, snapshot in list(store.objects.items()):
        value = json.loads(snapshot.data)
        midnight = datetime.fromisoformat(value["date"]).replace(tzinfo=UTC)
        if key.startswith("health/signals/"):
            value["channels"]["heart_rate"]["readings"] = [
                {"timestamp": int((midnight + timedelta(seconds=120 * index)).timestamp()),
                 "value": 55 + index % 4, "time_basis": "utc"} for index in range(720)]
        elif key.startswith("health/hrv/"):
            value["readings"] = [
                {"timestamp": int((midnight - timedelta(hours=2) + timedelta(seconds=300 * index)).timestamp()),
                 "hrv_ms": 45 + index % 4, "time_basis": "utc"} for index in range(96)]
            # Retained provider metadata is decoded work, not normalized points.
            value["synthetic_provider_metadata"] = "synthetic" * 6400
        body = gzip.compress(canonical(value).encode(), mtime=0)
        store.objects[key] = SimpleNamespace(data=body, revision='"' + hashlib.sha256(body).hexdigest() + '"',
                                             content_type="application/json", encoding="gzip")
    return store, days


def test_dense_explicit365_roundtrips_real_native_preparation_and_sealed_workset(tmp_path):
    store, days = annual_sources()
    binding, _ = declaration([days[-1]])
    binding["windows"] = [365]
    args = normalize_args(period="365", end_date=days[-1], channel="hrv")
    key = storage_key(args)
    targets = [{"target_id": key, "key": key, "args": args,
                "endpoint": days[-1], "window_days": 365, "channel": "hrv"}]
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets, target_subset=True)
    inventory(workset, store, tmp_path)
    while workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)["remaining"]:
        pass
    workset.seal_sources()
    calls = dict(store.operations)
    reader = NativeNightReader(workset.reader_store(), timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    nights = [reader.night(day) for day in days]
    artifact = prepare_reference(nights, args, proof_for(reader, days))
    value, body, _ = _prepared_value(artifact, args)
    assert reader.decoded_bytes > 33554432 and reader.read_bytes < 16777216
    assert value["resources"]["input_bytes"] > 16777216
    assert value["resources"]["input_points"] == 122640
    assert reader.input_points <= 811112 and len(body) <= 262144
    assert value["contributing_nights"] == 365
    workset.stage_product(key, artifact)
    workset.seal_build()
    reopened = ReferenceWorkset.open(workset.folder, binding)
    assert reopened.product_for(key) == artifact
    assert store.operations == calls and store.operations["put"] == 0 and not store.writes
    # Numeric synthetic measurements are preserved in the test log for review.
    measurements = {"decoded_bytes": reader.decoded_bytes, "stored_bytes": reader.read_bytes,
        "raw_points": reader.input_points, "normalized_bytes": value["resources"]["input_bytes"],
        "normalized_points": value["resources"]["input_points"], "product_bytes": len(body),
        "synthetic_operations": store.operations, "real_network": 0,
        "product_path": str(workset.folder / "products" / (artifact["sha256"] + ".bin"))}
    (tmp_path / "MEASUREMENTS.json").write_text(canonical(measurements), encoding="utf-8")


def sparse_prepared(tmp_path, width=365):
    first = date(2025, 1, 1)
    days = [(first + timedelta(days=index)).isoformat() for index in range(width)]
    store = calendar_store(first, 1, contexts=False)
    binding, _ = declaration([days[-1]])
    binding["windows"] = [width]
    args = normalize_args(period="custom", start_date=days[0], end_date=days[-1], channel="hrv")
    key = storage_key(args)
    targets = [{"target_id": key, "key": key, "args": args,
                "endpoint": days[-1], "window_days": width, "channel": "hrv"}]
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets, target_subset=True)
    inventory(workset, store, tmp_path)
    workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    workset.seal_sources()
    reader = NativeNightReader(workset.reader_store(), timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365 if width == 365 else None)
    nights = [reader.night(day) for day in days]
    return workset, reader, days, nights, args


def replace_resources(artifact, **resources):
    value = json.loads(artifact["body"])
    value["resources"].update(resources)
    artifact["body"] = canonical(value)
    artifact["sha256"] = hashlib.sha256(artifact["body"].encode()).hexdigest()
    return artifact


@pytest.mark.parametrize("width,resources", [
    (365, {"input_bytes": 68040933}), (90, {"input_bytes": 16777217}),
    (365, {"input_points": 200001}), (365, {"input_bytes": 1.5}),
    (365, {"invalid_points": 200001}),
])
@pytest.mark.parametrize("seam", ["prepared", "stage"])
def test_prepared_and_stage_reject_inflated_resource_metadata(tmp_path, width, resources, seam):
    workset, reader, days, nights, args = sparse_prepared(tmp_path, width)
    artifact = replace_resources(prepare_reference(nights, args, proof_for(reader, days)), **resources)
    if seam == "prepared":
        with pytest.raises(BaselineRefreshStop, match="Prepared reference resources changed"):
            _prepared_value(artifact, args)
    else:
        with pytest.raises(Stop, match="reference_workset_product_resources"):
            workset.stage_product(artifact["key"], artifact)


def decoded_boundary_sources(total):
    """65 real nights whose compressed sources inflate to an exact cumulative size."""
    first = date(2025, 1, 1)
    days = [(first + timedelta(days=index)).isoformat() for index in range(365)]
    store = calendar_store(first, 66, contexts=False)
    selected = days[:65]
    keys = {key for key in store.objects if any(day in key for day in selected)}
    keys.add("health/signals/v1/2024/12/2024-12-31.json")
    hrv = [f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json" for day in selected]
    ordinary = sum(len(store.objects[key].data) for key in keys if key not in hrv)
    target_sizes = [2097152] * 64 + [total - ordinary - 64 * 2097152]
    assert 0 < target_sizes[-1] <= 2097152
    for key, size in zip(hrv, target_sizes, strict=True):
        value = json.loads(store.objects[key].data)
        value["synthetic_padding"] = ""
        value["synthetic_padding"] = "x" * (size - len(canonical(value).encode()))
        raw = canonical(value).encode()
        assert len(raw) == size
        body = gzip.compress(raw, mtime=0)
        store.objects[key] = SimpleNamespace(data=body, revision='"' + hashlib.sha256(body).hexdigest() + '"',
                                             content_type="application/json", encoding="gzip")
    return store, days


@pytest.mark.parametrize("over", [False, True])
def test_annual_decoded_exact_boundary_and_one_byte_over(over):
    store, days = decoded_boundary_sources(136081864 + int(over))
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days, window_days=365)
    if over:
        with pytest.raises(NativeInputError, match="Stored native decoded input byte limit") as caught:
            for day in days[:65]:
                reader.night(day)
        assert caught.value.category == "native_decoded_input_byte_limit"
    else:
        for day in days[:65]:
            reader.night(day)
    assert reader.decoded_bytes == 136081864 + int(over)
    assert reader.read_bytes < 16777216 and not store.writes


@pytest.mark.parametrize("mode", ["legacy", "default", "reprime_default", "failed", "foreign", "direct", "after_error"])
def test_nonannual_reads_keep_cumulative_decoded_cap_after_prime_changes(mode):
    store, days = decoded_boundary_sources(136081864)
    store.add("health/sleep/v1/2024/12/2024-12-31.json", {"synthetic": True})
    reader = NativeNightReader(store, timezone="UTC")
    if mode in {"legacy", "default"}:
        if mode == "default":
            reader.prime_rolling_sources(days[:90])
    else:
        reader.prime_rolling_sources(days, window_days=365)
        for day in days[:17]:
            reader.night(day)
        assert reader.decoded_bytes > 33554432
        if mode == "reprime_default":
            reader.prime_rolling_sources(days[18:19])
        elif mode == "failed":
            with pytest.raises(NativeInputError):
                reader.prime_rolling_sources(days[:-1], window_days=365)
        elif mode == "after_error":
            # A malformed admitted night must not leave direct reads elevated.
            bad = "health/sleep/v1/2025/01/2025-01-18.json"
            value = json.loads(store.objects[bad].data)
            value["sleep_end_gmt"] = None
            store.add(bad, value)
            reader.prime_rolling_sources(days, window_days=365)
            with pytest.raises(NativeInputError):
                reader.night(days[17])
    prior = reader.decoded_bytes
    with pytest.raises(NativeInputError, match="Stored native decoded input byte limit") as caught:
        if mode in {"direct", "after_error"}:
            reader.read("health/hrv/2025/01/2025-01-19.json")
        elif mode == "foreign":
            reader.night("2024-12-31")
        elif mode in {"failed", "reprime_default"}:
            reader.night(days[18])
        else:
            for day in days[:65]:
                reader.night(day)
    assert caught.value.category == "native_decoded_input_byte_limit"
    assert reader.decoded_bytes > max(prior, 33554432) and not store.writes


@pytest.mark.parametrize("width,limit", [(90, 16777216), (365, 68040932)])
@pytest.mark.parametrize("over", [False, True])
def test_normalized_exact_boundary_and_one_byte_over(tmp_path, width, limit, over):
    workset, reader, days, nights, args = sparse_prepared(tmp_path, width)
    for night in nights:
        night["synthetic_padding"] = ""
    remaining = limit + int(over) - len(canonical(nights).encode())
    for night in nights:
        amount = min(remaining, 200000)
        night["synthetic_padding"] = "x" * amount
        remaining -= amount
    assert remaining == 0 and len(canonical(nights).encode()) == limit + int(over)
    if over:
        with pytest.raises(BaselineRefreshStop, match="Native reference input byte limit") as caught:
            prepare_reference(nights, args, proof_for(reader, days))
        assert caught.value.category == "native_reference_input_byte_limit"
    else:
        artifact = prepare_reference(nights, args, proof_for(reader, days))
        assert _prepared_value(artifact, args)[0]["resources"]["input_bytes"] == limit
        workset.stage_product(artifact["key"], artifact)
        workset.seal_build()
        assert workset.product_for(artifact["key"]) == artifact


def test_shared_annual_drain_byte_admission_requires_explicit_exact_calendar():
    first = date(2025, 1, 1)
    nights = [{"wake_date": (first + timedelta(days=index)).isoformat()} for index in range(365)]
    args = {"start_date": "2025-01-01", "end_date": "2025-12-31"}
    assert reference_input_byte_limit(args, nights, annual=True) == 68040932
    assert reference_input_byte_limit(args, nights) == 16777216
    assert reference_input_byte_limit(args, nights[:-1], annual=True) == 16777216
    assert reference_input_byte_limit(args, list(reversed(nights)), annual=True) == 16777216
    assert reference_input_byte_limit({**args, "end_date": "2025-12-30"}, nights, annual=True) == 16777216
