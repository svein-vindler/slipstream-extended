"""Recent stored nights complete their six rolling products through the real SDK."""

import json
from datetime import date, timedelta
from urllib.parse import unquote, urlsplit

import pytest
from history_storage_fixture import BUCKET
from test_history_baseline_integration import sdk_store
from test_rolling_reference_sources import seeded

from pipeline import night_baseline_refresh as refresh
from pipeline.history_state import digest
from pipeline.night_products import normalize_args, storage_key


def product_keys(endpoints):
    return {
        storage_key(normalize_args(
            start_date=(date.fromisoformat(endpoint) - timedelta(days=width - 1)).isoformat(),
            end_date=endpoint, period="custom", channel=channel,
        ))
        for endpoint in endpoints for width in (28, 30, 90)
        for channel in ("heart_rate", "hrv")
    }


@pytest.mark.parametrize("days", [1, 3])
def test_recent_stored_sleep_endpoints_complete_both_channels_all_windows(days):
    server, _ = seeded("2026-10-23", days)
    store, cache = sdk_store(server), {}
    endpoints = [(date(2026, 10, 23) + timedelta(days=i)).isoformat() for i in range(days)]

    result = refresh.refresh_recent_night_references(
        store, wake_dates=endpoints, timezone="Europe/Oslo", snapshot_cache=cache,
    )

    expected = product_keys(endpoints)
    completed = expected & server.objects.keys()
    if days == 1:
        assert result["status"] == "complete"
        assert result["selected_remaining"] == 0
        assert len(completed) == 6
    else:
        assert result["status"] == "bounded_pending"
        assert result["selected_remaining"] == 6
        assert result["products_written"] == len(completed) == 12
        assert product_keys([endpoints[-1]]) <= completed
    assert len([key for key in server.puts if key in expected]) == len(completed)
    for key in completed:
        value = json.loads(server.objects[key][0])
        assert value["kind"] == "night-baseline"
        assert value["args"]["channel"] in {"heart_rate", "hrv"}
        assert value["contributing_nights"] > 0
    source_gets = [key for method, key, query in server.calls
                   if method == "GET" and not query.get("list-type")
                   and not key.startswith("health/night-products/")]
    # The same raw stored dependencies serve every overlapping channel/window.
    assert len(source_gets) <= 3 * days + 4
    assert cache
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100
    assert store.operations["put"] <= 250
    print({"endpoints": days, "products": len(completed), "raw_gets": len(source_gets),
           "operations": store.operations})
    if days == 3:
        accepted = {key: server.objects[key] for key in completed}
        initial_puts = len(server.puts)
        store.client.close()
        reopened = sdk_store(server)

        resumed = refresh.refresh_recent_night_references(
            reopened, wake_dates=endpoints, timezone="Europe/Oslo", snapshot_cache=cache,
        )

        assert resumed["status"] == "complete"
        assert resumed["selected_remaining"] == 0
        assert resumed["products_written"] == 6
        assert expected <= server.objects.keys()
        assert all(server.objects[key] == value for key, value in accepted.items())
        later_puts = server.puts[initial_puts:]
        assert not accepted.keys() & set(later_puts)
        assert len(set(later_puts) & expected) == 6
        assert all(key.startswith("health/night-products/") for key in later_puts)
        assert reopened.operations["get"] <= 400
        assert reopened.operations["list_pages"] <= 100
        assert reopened.operations["put"] <= 250
        print({"resumed_endpoints": days, "remaining_products_completed": 6,
               "operations": reopened.operations})


def test_recent_scope_keeps_unrelated_old_unknown_and_closed_calendar_tail():
    from test_reference_bulk_independent_scope import old_unknown_row, seed_queue

    server, _ = seeded("2026-10-25", 1)
    old = old_unknown_row()
    closed = {"phase": "dirty", "start_date": "2026-09-01", "end_date": "2026-09-30",
              "sources": {}, "completed": {}}
    week = {"phase": "dirty", "start_date": "2026-09-21", "end_date": "2026-09-27",
            "sources": {}, "completed": {}}
    seed_queue(server, {"schema_version": 1, "kind": "night-baseline-refresh-queue",
                        "pending": {"week/2020-W01": old, "month/2026-09": closed,
                                    "week/2026-W39": week}})
    store = sdk_store(server)

    result = refresh.refresh_recent_night_references(
        store, wake_dates=["2026-10-25"], timezone="Europe/Oslo", snapshot_cache={},
    )

    assert result["status"] == "complete"
    assert product_keys(["2026-10-25"]) <= server.objects.keys()
    pending = json.loads(server.objects[refresh.QUEUE_KEY][0])["pending"]
    assert pending["week/2020-W01"] == old
    assert pending["month/2026-09"] == closed
    assert pending["week/2026-W39"] == week
    assert old["inflight"]["key"] not in server.puts


def test_full_ninety_night_history_is_downloaded_once_for_six_recent_products():
    server, _ = seeded("2026-07-28", 90)
    store, cache = sdk_store(server), {}

    result = refresh.refresh_recent_night_references(
        store, wake_dates=["2026-10-25"], timezone="Europe/Oslo", snapshot_cache=cache,
    )

    assert result["status"] == "complete"
    expected = product_keys(["2026-10-25"])
    assert expected <= server.objects.keys()
    values = [json.loads(server.objects[key][0]) for key in expected]
    assert sorted(value["contributing_nights"] for value in values) == [28, 28, 30, 30, 90, 90]
    raw_gets = [key for method, key, query in server.calls
                if method == "GET" and not query.get("list-type")
                and not key.startswith("health/night-products/")]
    assert len(raw_gets) <= 275
    assert store.operations["get"] <= 400
    assert store.operations["list_pages"] <= 100
    assert store.operations["put"] <= 250
    print({"history_nights": 90, "raw_gets": len(raw_gets), "operations": store.operations})


def test_recent_capacity_deferral_retains_selected_work_without_a_quota_reset():
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server, max_writes_per_run=4)

    result = refresh.refresh_recent_night_references(
        store, wake_dates=["2026-10-25"], timezone="Europe/Oslo", snapshot_cache={},
    )

    assert result["status"] == "bounded_pending"
    assert store.operations["put"] <= 4
    assert len(product_keys(["2026-10-25"]) & server.objects.keys()) < 6
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert queue.get("rolling")


def test_recent_missing_sleep_does_not_claim_a_complete_scope():
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)

    result = refresh.refresh_recent_night_references(
        store, wake_dates=["2026-10-25", "2026-10-26"], timezone="Europe/Oslo", snapshot_cache={},
    )

    assert result["status"] in {"bounded_pending", "not_ready", "missing", "held"}
    assert not product_keys(["2026-10-26"]) & server.objects.keys()


def test_recent_selected_unknown_product_remains_held_and_is_never_adopted():
    from test_reference_bulk_independent_scope import seed_queue

    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)
    source = "health/sleep/v1/2026/10/2026-10-25.json"
    body, revision = server.objects[source]
    refresh.register_rolling_endpoints(
        store, [{"key": source, "revision": revision, "sha256": digest(body)}], today="2026-10-25",
    )
    key = storage_key(normalize_args(
        start_date="2026-07-28", end_date="2026-10-25", period="custom", channel="hrv",
    ))
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    name = "rolling/90/2026-10-25/hrv"
    held = {
        "rolling_window": 90, "channel": "hrv", "start_date": "2026-07-28",
        "end_date": "2026-10-25", "phase": "publishing", "sources": {}, "completed": {},
        "inflight": {"key": key, "sha256": "f" * 64, "revision": None},
    }
    queue["pending"][name] = held
    seed_queue(server, queue)

    result = refresh.refresh_recent_night_references(
        store, wake_dates=["2026-10-25"], timezone="Europe/Oslo", snapshot_cache={},
    )

    assert result["status"] == "held"
    assert result["selected_remaining"] == 1
    assert json.loads(server.objects[refresh.QUEUE_KEY][0])["pending"][name] == held
    assert key not in server.puts


def test_recent_source_revision_drift_after_product_write_retains_pending_ownership():
    server, _ = seeded("2026-10-25", 1)
    store, expected = sdk_store(server), product_keys(["2026-10-25"])
    original_send = store.client._endpoint.http_session.send
    changed = []

    def send(request):
        response = original_send(request)
        key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
        if request.method == "PUT" and key in expected and not changed:
            source = "health/hrv/2026/10/2026-10-25.json"
            body, _ = server.objects[source]
            server.objects[source] = (body, '"changed-after-product-write"')
            changed.append(source)
        return response

    store.client._endpoint.http_session.send = send
    try:
        result = refresh.refresh_recent_night_references(
            store, wake_dates=["2026-10-25"], timezone="Europe/Oslo", snapshot_cache={},
        )
    except refresh.BaselineRefreshStop:
        result = {"status": "held"}

    assert changed
    assert result["status"] != "complete"
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert any(row.get("rolling_window") and row["end_date"] == "2026-10-25"
               for row in queue["pending"].values())


def test_default_refresh_still_drains_only_two_rolling_rows():
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)
    key = "health/sleep/v1/2026/10/2026-10-25.json"
    body, revision = server.objects[key]
    refresh.register_rolling_endpoints(
        store, [{"key": key, "revision": revision, "sha256": digest(body)}], today="2026-10-25",
    )

    result = refresh.refresh_pending(
        store, today="2026-10-25", timezone="Europe/Oslo", period_names=set(),
        rolling_endpoints=["2026-10-25"], snapshot_cache={},
    )

    assert result["products_written"] == 2
    assert result["periods_completed"] == 2
    assert result["status"] == "bounded_pending"
    assert len(product_keys(["2026-10-25"]) & server.objects.keys()) == 2

