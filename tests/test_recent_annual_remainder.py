"""Ordinary recent caller drains retained annual work through the real fake SDK."""

import copy
import json
from collections import Counter
from datetime import date, timedelta
from urllib.parse import urlsplit

import pytest
from test_rolling_annual_fences import annual_only_queue
from test_rolling_discovery import queue_source
from test_rolling_reference_sources import sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline import rolling_night_references as rolling
from pipeline.night_products import normalize_args, storage_key

ENDPOINT = "2024-03-01"


@pytest.mark.parametrize("windows,remaining", [
    (None, 18), ((7, 28, 30, 90, 365), 30), ((365,), 6), ((7,), 6),
])
def test_recent_missing_timezone_counts_only_explicitly_selected_windows(windows, remaining):
    server, _ = seeded(ENDPOINT, 1)
    before = copy.deepcopy(server.objects)
    options = {} if windows is None else {"rolling_windows": windows}
    result = refresh.refresh_recent_night_references(
        sdk_store(server), wake_dates=[ENDPOINT, "2024-03-02", "2024-03-03"],
        timezone="Not/A_Real_Zone", **options)
    assert result == {"status": "calendar_timezone_required", "selected_remaining": remaining}
    assert server.objects == before
    assert server.calls == [] and server.puts == []


def annual_keys(endpoint=ENDPOINT):
    return {storage_key(normalize_args(
        start_date=(date.fromisoformat(endpoint) - timedelta(days=364)).isoformat(),
        end_date=endpoint, period="custom", channel=channel))
        for channel in ("heart_rate", "hrv")}


def recent(store, cache):
    try:
        return refresh.refresh_recent_night_references(
            store, wake_dates=[ENDPOINT], timezone="Europe/Oslo", snapshot_cache=cache,
            rolling_windows=(365,))
    except refresh.BaselineRefreshStop:
        print(json.dumps({"case": "recent_caller_stopped", "operations": store.operations,
                          "attempted_write_bytes": store._write_bytes}))
        raise


@pytest.mark.parametrize("guarded", [False, True])
def test_recent_retained_annual_both_channels_publish_and_retry_is_idempotent(tmp_path, guarded):
    from test_reference_bulk_independent_scope import old_unknown_row, seed_queue

    from pipeline.history_publish import StorageGuard, _ReferenceStore

    server, _ = seeded(ENDPOINT, 1)
    annual_only_queue(server)
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    old = old_unknown_row()
    queue["pending"]["week/2020-W01"] = old
    seed_queue(server, queue)
    store, cache = sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152), {}
    guard = None
    if guarded:
        evidence = tmp_path / "guard"
        evidence.mkdir()
        guard = StorageGuard(store, set(), evidence, {})
    reference = _ReferenceStore(store, guard) if guard is not None else store
    try:
        result = recent(reference, cache)
        if guarded:
            assert reference.annual_endpoints == {ENDPOINT}
            assert guard.counts["get"] <= 400 and guard.counts["list"] <= 100
            assert not guard.failed
    finally:
        if guard is not None:
            guard.close()

    assert result["status"] == "complete"
    assert result["selected_remaining"] == 0
    assert result["products_written"] == 2
    assert annual_keys() <= server.objects.keys()
    values = [json.loads(server.objects[key][0]) for key in annual_keys()]
    assert all(value["contributing_nights"] == 1 for value in values)
    inventories = Counter(prefix for method, prefix, query in server.calls
                          if query.get("list-type") and len(prefix.rstrip("/").split("/")[-1]) == 4)
    assert inventories and set(inventories.values()) == {3}  # assembly + TWO fresh fences
    retained = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert retained["pending"]["week/2020-W01"] == old
    accepted = {key: server.objects[key] for key in annual_keys()}
    puts = len(server.puts)
    resumed = recent(store, cache)
    assert resumed["selected_remaining"] == 0
    assert resumed["products_written"] == 0
    assert all(server.objects[key] == value for key, value in accepted.items())
    assert not set(server.puts[puts:]) & annual_keys()
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100
    assert store.operations["put"] <= 100 and store._write_bytes <= 2097152
    print(json.dumps({"case": "sparse_one_night_annual_recent_and_retry",
                      "guarded": guarded, "operations": store.operations,
                      "attempted_write_bytes": store._write_bytes,
                      "remaining": result["selected_remaining"]}))


@pytest.mark.parametrize("warm", [False, True])
def test_recent_annual_cold_defers_and_verified_warm_cache_can_progress(warm):
    # 396 raw objects: cold assembly leaves too few GETs for publication;
    # the existing <=512-snapshot cache can legitimately cover this fixture.
    first = (date.fromisoformat(ENDPOINT) - timedelta(days=131)).isoformat()
    server, sources = seeded(first, 132)
    annual_only_queue(server)
    store = sdk_store(server)
    cache = dict(sources.objects) if warm else {}
    result = recent(store, cache)
    assert result["selected_remaining"] == (0 if warm else 2)
    assert result["products_written"] == (2 if warm else 0)
    if not warm:
        assert result["status"] == "bounded_pending"
        assert result.get("budget_deferred")
        assert not annual_keys() & server.objects.keys()
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100
    assert store.operations["put"] <= 100 and store._write_bytes <= 2097152
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert not any(row.get("inflight") for row in queue["pending"].values())
    print(json.dumps({"case": "132_present_233_excluded", "warm": warm,
                      "operations": store.operations, "attempted_write_bytes": store._write_bytes,
                      "remaining": result["selected_remaining"]}))


def test_recent_dense_annual_history_remains_honestly_budget_deferred():
    first = (date.fromisoformat(ENDPOINT) - timedelta(days=364)).isoformat()
    server, _ = seeded(first, 365)
    annual_only_queue(server)
    store = sdk_store(server)
    result = recent(store, {})
    assert result["status"] == "bounded_pending"
    assert result["selected_remaining"] == 2
    assert result.get("budget_deferred")
    assert result["products_written"] == 0
    assert not annual_keys() & server.objects.keys()
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100
    assert store.operations["put"] <= 100 and store._write_bytes <= 2097152
    print(json.dumps({"case": "dense_365_cold", "operations": store.operations,
                      "attempted_write_bytes": store._write_bytes,
                      "remaining": result["selected_remaining"]}))


def test_recent_annual_persisted_checkpoint_resumes_without_rewriting_accepted_product():
    server, _ = seeded(ENDPOINT, 1)
    annual_only_queue(server)
    original, stopped = server.send, []

    def interrupt_after_persisted_checkpoint(request):
        response = original(request)
        if request.method == "PUT" and urlsplit(request.url).path.endswith(refresh.QUEUE_KEY):
            queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
            completed = [row for row in queue["pending"].values()
                         if row.get("completed") and not row.get("inflight")]
            if completed and not stopped:
                stopped.append(True)
                raise KeyboardInterrupt("synthetic process interruption after persisted checkpoint")
        return response

    server.send = interrupt_after_persisted_checkpoint
    cache = {}
    with pytest.raises(KeyboardInterrupt):
        recent(sdk_store(server), cache)
    accepted = {key: server.objects[key] for key in annual_keys() if key in server.objects}
    assert len(accepted) == 1
    checkpoint = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert any(row.get("completed") for row in checkpoint["pending"].values())
    assert not any(row.get("inflight") for row in checkpoint["pending"].values())
    resumed_store = sdk_store(server)
    resumed = recent(resumed_store, cache)
    assert resumed["selected_remaining"] == 0
    assert resumed["products_written"] == 1
    assert annual_keys() <= server.objects.keys()
    assert all(server.objects[key] == value for key, value in accepted.items())
    assert all(server.puts.count(key) == 1 for key in annual_keys())
    assert resumed_store.operations["get"] <= 400
    assert resumed_store.operations["list_pages"] <= 100


def test_normal_reference_entry_reaches_tail_after_retained_annual_recent_work(monkeypatch):
    from pipeline.latest_night import refresh_references

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    server, _ = seeded(ENDPOINT, 1)
    annual_only_queue(server)
    store = sdk_store(server)
    result = refresh_references(store, [ENDPOINT], rolling_windows=(365,))
    assert result["selected_remaining"] == 0
    assert result["products_written"] == 2
    assert result["calendar_refresh"]["products_written"] == 0
    assert annual_keys() <= server.objects.keys()
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100


@pytest.mark.parametrize("offset", [1, 7])
def test_combined_annual_batch_defers_without_widening_the_exact_source_window(offset):
    other = (date.fromisoformat(ENDPOINT) + timedelta(days=offset)).isoformat()
    server, _ = seeded(ENDPOINT, 1)
    ranges = rolling.merge([[ENDPOINT, ENDPOINT], [other, other]])
    registry = {"eligible": ranges, "dirty": {
        str(n): {channel: (ranges if n == 365 else []) for channel in ("heart_rate", "hrv")}
        for n in (7, 28, 30, 90, 365)}}
    registry = json.loads(json.dumps(registry))  # persisted queue lists do not alias
    queue = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {},
             "rolling": registry}
    # Persist the real producer's retained rows for both endpoints. One normal
    # materialization emits only the first endpoint per channel, not the rest.
    for _ in range(2):
        rolling.materialize(queue, today=other, limit=8, max_pending=120,
                            allowed_endpoints=[ENDPOINT, other], allowed_windows=(365,))
    queue_source(server, [], rolling=queue["rolling"], pending=queue["pending"])
    store = sdk_store(server)
    result = refresh.refresh_pending(store, today=other, timezone="Europe/Oslo", max_periods=8,
        period_names=set(), rolling_endpoints=[ENDPOINT, other], batch_local_preparation=True,
        rolling_windows=(365,))
    assert result["status"] == "bounded_pending" and result.get("budget_deferred")
    assert result["products_written"] == 0
    assert sum(item["count"] for item in result["rolling_remaining"]["365"].values()) == 4
    assert not any(query.get("list-type") for method, key, query in server.calls)
    assert not any("/baseline/" in key for key in server.puts)


@pytest.mark.parametrize("timezone, missing, status", [
    ("Not/A_Real_Zone", False, "calendar_timezone_required"),
    ("Europe/Oslo", True, "not_ready"),
])
def test_recent_missing_or_unknown_timezone_counts_all_supported_products(timezone, missing, status):
    server, _ = seeded("2024-02-29", 1)
    before = copy.deepcopy(server.objects)
    store = sdk_store(server)
    result = refresh.refresh_recent_night_references(
        store, wake_dates=[ENDPOINT, ENDPOINT], timezone=timezone, snapshot_cache={},
        rolling_windows=rolling.WINDOWS)
    assert result["status"] == status
    assert result["selected_remaining"] == 10
    if missing:
        assert result["missing_endpoints"] == 1
    else:
        assert not server.calls
    assert server.objects == before
    assert not server.puts
