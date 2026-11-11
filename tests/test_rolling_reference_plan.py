"""Read-only planning through actual SDK, native projection and product contracts."""

import json
from datetime import date, timedelta

import pytest
from test_history_baseline_integration import sdk_store
from test_rolling_reference_sources import seeded

from pipeline.night_products import normalize_args, storage_key


def test_plan_uses_inclusive_windows_and_canonical_storage_without_writing():
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-08-04", 90)
    result = plan_references(sdk_store(server), ["2026-11-01"],
                             today="2026-11-01", timezone="Europe/Oslo", snapshot_cache={}, windows=(28, 30, 90))
    assert result["kind"] == "rolling-night-reference-plan"
    assert result["eligible_endpoints"] == ["2026-11-01"]
    assert len(result["targets"]) == 6
    for target in result["targets"]:
        width = target["window_days"]
        args = normalize_args(end_date="2026-11-01", period=str(width) if width != 30 else "custom",
                              start_date=(date(2026, 11, 1) - timedelta(days=width - 1)).isoformat(),
                              channel=target["channel"])
        assert target["args"] == args and target["storage_key"] == storage_key(args)
        assert target["expected_nights"] == target["contributing_nights"] == width
        assert target["coverage"] == 1 and target["gaps"] == []
        assert target["status"] == "ready"
        assert "bins" not in target and "source_manifest" not in target
    assert result["reads"]["operations"]["get"] == 271
    assert result["reads"]["operations"]["put"] == 0
    assert server.puts == []
    assert all(call[1] not in {"", "inventory"} for call in server.calls)
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("endpoints", [[], ["2026-11-01"] * 8,
                                       ["2026-10-01", "2026-10-09"], ["2026-1-01"]])
def test_invalid_explicit_batch_is_rejected_before_any_io(endpoints):
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-10-01", 1)
    with pytest.raises(ValueError):
        plan_references(sdk_store(server), endpoints, today="2026-11-01", timezone="UTC", windows=(28, 30, 90))
    assert server.calls == []


def test_denied_native_inventory_is_explicit_unknown_and_never_zero_coverage():
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-10-25", 1)
    server.deny_lists = True
    result = plan_references(sdk_store(server), ["2026-10-25"],
                             today="2026-10-25", timezone="Europe/Oslo", windows=(28, 30, 90))
    assert result["status"] == "blocked" and result["targets"] == []
    assert result["blocked"] == [{"endpoint": "2026-10-25", "reason": "native_input_unknown"}]
    assert result["reads"]["operations"] == {"get": 0, "list": 1, "put": 0}


def test_seven_adjacent_endpoints_reuse_union_and_resume_cache_under_400_gets():
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-08-04", 96, contexts=True)
    endpoints = [(date(2026, 11, 1) + timedelta(days=i)).isoformat() for i in range(7)]
    store, cache = sdk_store(server), {}
    result = plan_references(store, endpoints, today=endpoints[-1], timezone="Europe/Oslo",
                             snapshot_cache=cache, windows=(28, 30, 90))
    assert result["eligible_endpoints"] == endpoints and len(result["targets"]) == 42
    assert result["reads"]["operations"]["get"] == 385
    assert result["source_cache"]["objects"] <= 512
    assert result["source_cache"]["stored_bytes"] <= 16777216
    assert result["publication_estimate"]["requested_refresh_phases_at_most_two_targets"] == 21
    assert len(result["publication_estimate"]["remainder"]) == 32
    # A new phase/client owns a new component allowance while borrowing the
    # previously verified bytes; fresh bounded inventories are still performed.
    resumed = plan_references(sdk_store(server), endpoints[-3:], today=endpoints[-1], timezone="Europe/Oslo",
                              snapshot_cache=cache, windows=(28, 30, 90))
    assert resumed["reads"]["operations"]["get"] == 0
    assert resumed["reads"]["local_snapshot_reuse_lookups"] > 300
    assert resumed["reads"]["operations"]["list"] > 0
    assert server.puts == []
    print({"seven_endpoint_union": result["reads"], "reused_plan": resumed["reads"]})


def test_new_plan_epoch_refetches_only_corrected_cached_source():
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-10-25", 1)
    cache = {}
    old = plan_references(sdk_store(server), ["2026-10-25"], today="2026-10-25",
                          timezone="Europe/Oslo", snapshot_cache=cache, windows=(28, 30, 90))
    key = "health/hrv/2026/10/2026-10-25.json"
    value = json.loads(server.objects[key][0])
    value["readings"][0]["hrv_ms"] = 75
    server.objects[key] = (json.dumps(value).encode(), '"corrected"')
    new = plan_references(sdk_store(server), ["2026-10-25"], today="2026-10-25",
                          timezone="Europe/Oslo", snapshot_cache=cache, windows=(28, 30, 90))
    assert new["reads"]["operations"]["get"] == 1
    old_hrv = next(target for target in old["targets"] if target["channel"] == "hrv")
    new_hrv = next(target for target in new["targets"] if target["channel"] == "hrv")
    assert old_hrv["source_identity"] != new_hrv["source_identity"]


def test_missing_sleep_and_future_endpoint_are_explicitly_ineligible():
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-10-25", 1)
    result = plan_references(sdk_store(server), ["2026-10-25", "2026-10-26", "2026-10-27"],
                             today="2026-10-26", timezone="Europe/Oslo", windows=(28, 30, 90))
    assert result["eligible_endpoints"] == ["2026-10-25"]
    assert result["blocked"] == [{"endpoint": "2026-10-26", "reason": "sleep_not_stored"}]
    assert result["deferred"] == [{"endpoint": "2026-10-27", "reason": "future_wake_date"}]
    assert len(result["verified_sleep_sources"]) == 1
    for target in result["targets"]:
        assert target["contributing_nights"] == 1
        assert len(target["gaps"]) == target["window_days"] - 1
        assert target["coverage"] > 0


@pytest.mark.parametrize("mode", ["missing", "ambiguous"])
def test_hr_and_hrv_coverage_and_unknowns_are_independent(mode):
    from pipeline.rolling_reference_plan import plan_references

    server, _ = seeded("2026-10-25", 1)
    key = "health/hrv/2026/10/2026-10-25.json"
    if mode == "missing":
        del server.objects[key]
    else:
        value = json.loads(server.objects[key][0])
        value["readings"][0]["time_basis"] = "local"
        server.objects[key] = (json.dumps(value).encode(), '"ambiguous"')
    result = plan_references(sdk_store(server), ["2026-10-25"],
                             today="2026-10-25", timezone="Europe/Oslo", windows=(28, 30, 90))
    hr = [target for target in result["targets"] if target["channel"] == "heart_rate"]
    hrv = [target for target in result["targets"] if target["channel"] == "hrv"]
    assert all(target["status"] == "ready" and target["contributing_nights"] == 1 for target in hr)
    if mode == "missing":
        assert all(target["status"] == "ready" and target["contributing_nights"] == 0
                   and target["coverage"] == 0 for target in hrv)
    else:
        assert all(target["status"] == "blocked_unknown" and target["contributing_nights"] is None
                   and target["coverage"] is None and target["source_identity"] is None for target in hrv)


@pytest.mark.parametrize("endpoint", ["2026-03-29", "2026-10-25", "2027-01-01"])
def test_dst_and_year_endpoints_keep_existing_baseline_source_identity(endpoint):
    from pipeline.native_night import NativeNightReader
    from pipeline.night_products import baseline, canonical
    from pipeline.rolling_reference_plan import plan_references

    server, source = seeded(endpoint, 1, contexts=True)
    result = plan_references(sdk_store(server), [endpoint], today=endpoint, timezone="Europe/Oslo", windows=(28, 30, 90))
    night = NativeNightReader(source, timezone="Europe/Oslo").night(endpoint)
    for target in result["targets"]:
        # Missing nights remain explicit source entries in the shared producer.
        reader = NativeNightReader(source, timezone="Europe/Oslo")
        days = [(date.fromisoformat(target["args"]["start_date"]) + timedelta(days=i)).isoformat()
                for i in range(target["window_days"])]
        nights = [reader.night(day) for day in days]
        product = baseline(nights, target["args"], input_bytes=len(canonical(nights).encode()),
                           input_points=sum(len(ch["points"]) for ch in night["channels"].values()))["product"]
        assert target["source_identity"] == product["source_identity"]
