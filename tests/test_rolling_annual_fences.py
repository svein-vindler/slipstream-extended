"""Explicit annual inventories through actual SDK and physical source fences."""

import json
from datetime import date, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import Raw
from test_rolling_discovery import queue_source
from test_rolling_reference_sources import proof_for, sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.night_baseline_refresh import _IO, BaselineRefreshStop
from pipeline.night_reference_sources import (
    prime_rolling_list_bound,
    rolling_fence_list_bound,
    rolling_source_prefixes,
    validate_rolling_sources,
)
from pipeline.r2_store import R2ReadBoundaryError


def annual_days(end="2024-03-01"):
    last = date.fromisoformat(end)
    return [(last - timedelta(days=364 - index)).isoformat() for index in range(365)]


def test_explicit_365_uses_full_year_prefixes_for_13_month_source_and_both_fresh_fences():
    days = annual_days()
    server, _ = seeded(days[-1], 1, contexts=False)
    io = _IO(sdk_store(server))
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo", snapshot_cache={})
        assert prime_rolling_list_bound(days, window_days=365) == 14
        reader.prime_rolling_sources(days, window_days=365)
        nights = [reader.night(day) for day in days]
        assert sum(night["status"] == "available" for night in nights) == 1
        proof = proof_for(reader, days)
        assert rolling_fence_list_bound(proof, window_days=365) == 14
        validate_rolling_sources(io, proof, window_days=365)
        validate_rolling_sources(io, proof, window_days=365)
        assert io.counts() == {"get": 4, "list": 24, "put": 0}
        assert all(len(prefix.split("/")) <= 5 for method, prefix, query in server.calls
                   if query.get("list-type"))
    finally:
        io.close()


def add_context_inventory(server, count, *, first="2023-01-01"):
    for index in range(count):
        day = (date.fromisoformat(first) + timedelta(days=index // 100)).isoformat()
        identity = "nctx-" + f"{index:024x}"
        key = (f"health/night-context/v1/{day[:4]}/{day[5:7]}/{day}/"
               + day.replace("-", "") + f"T{index % 100:09d}-" + identity + ".json")
        server.objects[key] = (b"{}", '"synthetic-context"')


def primed_proof(server, days):
    io = _IO(sdk_store(server))
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo", snapshot_cache={})
        reader.prime_rolling_sources(days, window_days=365)
        for day in days:
            reader.night(day)
        return proof_for(reader, days)
    finally:
        io.close()


def test_year_crossing_signal_margin_covers_three_physical_years_without_global_scan():
    days = annual_days("2025-12-31")
    prefixes = rolling_source_prefixes(days, window_days=365)
    assert set(prefixes) == {
        "health/sleep/v1/2025/", "health/hrv/2025/", "health/night-context/v1/2025/",
        "health/signals/v1/2024/", "health/signals/v1/2025/", "health/signals/v1/2026/"}
    assert prime_rolling_list_bound(days, window_days=365) == 9
    server, _ = seeded("2025-12-31", 1)
    # Real valid outside-window dates share the physical prefix and are charged,
    # but they do not enter the selected night's logical source dependencies.
    extra, _ = seeded("2025-01-01", 1)
    server.objects.update(extra.objects)
    io = _IO(sdk_store(server))
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo")
        reader.prime_rolling_sources(days, window_days=365)
        assert reader.night("2025-12-31")["status"] == "available"
        assert io.counts()["list"] == 6
        assert all(prefix.endswith(("2024/", "2025/", "2026/"))
                   for method, prefix, query in server.calls if query.get("list-type"))
        assert not server.puts
    finally:
        io.close()


def test_full_year_context_inventory_charges_four_pages_and_ignores_valid_outside_months():
    days = annual_days()
    server, _ = seeded(days[-1], 1)
    outside, _ = seeded("2023-01-01", 1)
    server.objects.update(outside.objects)
    add_context_inventory(server, 3101)
    io = _IO(sdk_store(server))
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo")
        reader.prime_rolling_sources(days, window_days=365)
        assert reader.night(days[-1])["status"] == "available"
        context_calls = [query for method, prefix, query in server.calls
                         if prefix == "health/night-context/v1/2023/"]
        assert len(context_calls) == 4
        assert [call.get("continuation-token", ["0"])[0] for call in context_calls] == [
            "0", "1000", "2000", "3000"]
        assert io.counts()["list"] == 11
        assert io.counts()["get"] == 4
        assert not server.puts
    finally:
        io.close()


def test_real_annual_publication_detects_source_change_between_two_physical_fences():
    server, _ = seeded("2024-03-01", 1)
    annual_only_queue(server)
    original = server.send
    changed = False
    source_key = "health/hrv/2024/03/2024-03-01.json"

    def change_after_first_target(request):
        nonlocal changed
        response = original(request)
        if request.method == "PUT" and "/baseline/" in urlsplit(request.url).path and not changed:
            body, _ = server.objects[source_key]
            server.objects[source_key] = (body, '"concurrent-source-revision"')
            changed = True
        return response

    server.send = change_after_first_target
    with pytest.raises(BaselineRefreshStop) as caught:
        refresh.refresh_pending(sdk_store(server), today="2024-03-01", timezone="Europe/Oslo",
                                period_names=set(), rolling_endpoints=["2024-03-01"], rolling_windows=(365,))
    assert isinstance(caught.value.__cause__, NativeInputError)
    assert str(caught.value.__cause__) == "Night source revision changed"
    assert changed and len([key for key in server.puts if "/baseline/" in key]) == 1
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    retained = queue["pending"]["rolling/365/2024-03-01/heart_rate"]
    assert retained["phase"] == "publishing" and len(retained["completed"]) == 1
    assert "inflight" not in retained


def test_annual_fence_binds_only_a_consecutive_seven_endpoint_cohort():
    days = annual_days()
    extended = days + ["2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05", "2024-03-06", "2024-03-07"]
    proof = {"objects": {}, "lists": {
        f"health/night-context/v1/{day[:4]}/{day[5:7]}/{day}/": [] for day in extended}}
    assert rolling_fence_list_bound(proof, window_days=365) == 14
    proof["lists"]["health/night-context/v1/2024/03/2024-03-08/"] = []
    with pytest.raises(NativeInputError, match="Annual source window bound"):
        rolling_fence_list_bound(proof, window_days=365)
    del proof["lists"]["health/night-context/v1/2024/03/2024-03-08/"]
    del proof["lists"]["health/night-context/v1/2023/04/2023-04-01/"]
    with pytest.raises(NativeInputError, match="Annual source calendar bound"):
        rolling_fence_list_bound(proof, window_days=365)


def test_absent_sleep_dates_bind_annual_extent_without_requiring_empty_daily_context_lists():
    days = annual_days()
    server, _ = seeded(days[-1], 1)
    proof = primed_proof(server, days)
    proof["lists"] = {prefix: keys for prefix, keys in proof["lists"].items()
                      if days[-1] in prefix}
    assert len(proof["lists"]) == 1
    absent_key = "health/sleep/v1/2023/03/2023-03-03.json"
    assert proof["objects"][absent_key] == {"status": "absent"}
    io = _IO(sdk_store(server))
    try:
        validate_rolling_sources(io, proof, window_days=365)
        server.objects[absent_key] = (b"{}", '"appeared"')
        with pytest.raises(NativeInputError, match="Night source appeared during calculation"):
            validate_rolling_sources(io, proof, window_days=365)
        assert not server.puts
    finally:
        io.close()


@pytest.mark.parametrize("mutation", ["missing", "revision", "size", "appeared", "new_context"])
def test_second_annual_fence_rejects_changed_present_absent_or_context_evidence(mutation):
    days = annual_days()
    server, _ = seeded(days[-1], 1)
    proof = primed_proof(server, days)
    io = _IO(sdk_store(server))
    try:
        validate_rolling_sources(io, proof, window_days=365)
        key = "health/sleep/v1/2024/03/2024-03-01.json"
        body, revision = server.objects[key]
        if mutation == "missing":
            del server.objects[key]
        elif mutation == "revision":
            server.objects[key] = (body, '"changed"')
        elif mutation == "size":
            server.objects[key] = (body + b" ", revision)
        elif mutation == "appeared":
            server.objects["health/sleep/v1/2023/03/2023-03-03.json"] = (body, revision)
        else:
            add_context_inventory(server, 1, first=days[-1])
        with pytest.raises(NativeInputError):
            validate_rolling_sources(io, proof, window_days=365)
        assert not server.puts
    finally:
        io.close()


def test_annual_cache_reuse_still_requires_current_full_prefix_capacity_and_exact_revision():
    days = annual_days()
    server, _ = seeded(days[-1], 1)
    cache = {}
    io = _IO(sdk_store(server))
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo", snapshot_cache=cache)
        reader.prime_rolling_sources(days, window_days=365)
        assert reader.night(days[-1])["status"] == "available"
        before = io.counts()["get"]
        reader.prime_rolling_sources(days, window_days=365)
        assert reader.night(days[-1])["status"] == "available"
        assert io.counts()["get"] == before
        key = "health/sleep/v1/2024/03/2024-03-01.json"
        server.objects[key] = (server.objects[key][0], '"new-source-revision"')
        reader.prime_rolling_sources(days, window_days=365)
        assert reader.night(days[-1])["status"] == "available"
        assert io.counts()["get"] == before + 1
        add_context_inventory(server, 3102)
        with pytest.raises(R2ReadBoundaryError):
            reader.prime_rolling_sources(days, window_days=365)
        assert not server.puts
    finally:
        io.close()


def annual_only_queue(server, day="2024-03-01"):
    registry = {"eligible": [[day, day]], "dirty": {
        str(n): {channel: ([[day, day]] if n == 365 else [])
                 for channel in ("heart_rate", "hrv")} for n in (7, 28, 30, 90, 365)}}
    queue_source(server, [], rolling=registry)


@pytest.mark.parametrize("overflow", ["context_keys", "raw_keys", "physical_bytes", "page_bytes"])
def test_annual_run_stops_before_any_target_put_on_full_physical_prefix_resource_overflow(overflow):
    server, _ = seeded("2024-03-01", 1)
    annual_only_queue(server)
    if overflow == "context_keys":
        add_context_inventory(server, 3102)
    elif overflow == "raw_keys":
        for index in range(801):
            # Multiple years cannot spill into one physical raw prefix. Valid
            # raw date+gzip namespace permits <=732/year, so an 801st raw entry
            # is necessarily noncanonical as well as beyond physical capacity.
            server.objects[f"health/hrv/2023/synthetic-{index:04d}.json"] = (b"{}", '"opaque"')
    else:
        original = server.send

        def padded(request):
            response = original(request)
            if parse_qs(urlsplit(request.url).query).get("list-type"):
                body = response.raw.read()
                response.raw.close()
                padding = 1048577 if overflow == "page_bytes" else 750000
                body = body.replace(b"</ListBucketResult>", b" " * padding + b"</ListBucketResult>")
                return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))
            return response

        server.send = padded
    if overflow == "physical_bytes":
        report = refresh.refresh_pending(sdk_store(server), today="2024-03-01", timezone="Europe/Oslo",
                                         period_names=set(), rolling_endpoints=["2024-03-01"], rolling_windows=(365,))
        assert report["products_written"] == 0 and report["blocked"]
        io = _IO(sdk_store(server))
        try:
            with pytest.raises(NativeInputError, match="Annual inventory physical byte bound"):
                NativeNightReader(io).prime_rolling_sources(annual_days(), window_days=365)
            assert io.counts()["list"] == 3
        finally:
            io.close()
        assert not any("/baseline/" in key for key in server.puts)
        return
    with pytest.raises((NativeInputError, BaselineRefreshStop, R2ReadBoundaryError)) as caught:
        refresh.refresh_pending(sdk_store(server), today="2024-03-01", timezone="Europe/Oslo",
                                period_names=set(), rolling_endpoints=["2024-03-01"], rolling_windows=(365,))
    expected = {"context_keys": "schema_unverified", "raw_keys": "schema_unverified",
                "physical_bytes": "Annual inventory physical byte bound",
                "page_bytes": "Baseline declared body bound"}[overflow]
    chain = []
    error = caught.value
    while error is not None:
        chain.append(error.status if isinstance(error, R2ReadBoundaryError) else str(error))
        error = error.__cause__
    assert expected in chain
    if overflow == "context_keys":
        assert sum(prefix == "health/night-context/v1/2023/"
                   for method, prefix, query in server.calls if query.get("list-type")) == 4
    assert not any("/baseline/" in key for key in server.puts)


def test_unmarked_or_inexact_large_source_windows_and_legacy_24_prefix_proof_stay_rejected():
    days = annual_days()
    with pytest.raises(NativeInputError):
        rolling_source_prefixes(days)
    for marked_days, marker in ((days[:-1], 365), (days + ["2024-03-02"], 365),
                                (days[:200], True), (days[:90], 90)):
        with pytest.raises(NativeInputError):
            rolling_source_prefixes(marked_days, window_days=marker)
    with pytest.raises(NativeInputError):
        rolling_fence_list_bound({"objects": {}, "lists": {
            f"health/night-context/v1/{year}/01/{year}-01-01/": [] for year in range(2000, 2025)}})
