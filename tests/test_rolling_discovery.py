"""Admission of previously unregistered stored endpoints through the real consumer."""

import hashlib
import json
from urllib.parse import urlsplit

import pytest
from test_rolling_reference_sources import sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import normalize_args, storage_key


def queue_source(server, ranges, **extra):
    value = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {},
             "rolling_discovery": ranges, **extra}
    body = json.dumps(value).encode()
    server.objects[refresh.QUEUE_KEY] = (body, '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"')
    server.metadata[refresh.QUEUE_KEY] = {"content-type": "application/json"}
    return body


def test_old_hrv_admission_discovers_and_publishes_later_stored_sleep_same_phase():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    del server.objects["health/sleep/v1/2026/10/2026-10-25.json"]
    key = "health/hrv/2026/10/2026-10-25.json"
    source = json.loads(server.objects[key][0])
    source["readings"][0]["hrv_ms"] = 75
    assert refresh.publish_source(sdk_store(server), key, json.dumps(source).encode(), "application/json")
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo",
                                     period_names=set())
    assert result["products_written"] == 2
    args = normalize_args(start_date="2026-09-29", end_date="2026-10-26",
                          period="28", channel="hrv")
    product = json.loads(server.objects[storage_key(args)][0])
    assert product["contributing_nights"] == 1
    assert result["discovery_remaining"]["count"] == 0
    assert result["rolling_remaining"]["90"]["hrv"]["count"] == 1
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert queue["rolling"]["eligible"] == [["2026-10-26", "2026-10-26"]]
    print({"old_source_discovery_and_publication": result["component_operations"]})


def test_passive_discovery_checkpoints_seven_actual_endpoints_and_precise_rest():
    server, _ = seeded("2026-10-25", 10, contexts=False)
    queue_source(server, [["2026-10-25", "2027-01-22"]])
    cache = {}
    result = refresh.refresh_pending(sdk_store(server), today="2026-11-03", timezone="Europe/Oslo",
                                     period_names=set(), snapshot_cache=cache)
    assert result["discovery"]["registered_endpoints"] == 7
    assert result["discovery"]["sleep_objects_observed"] == 7
    assert result["discovery_remaining"]["ranges"] == [["2026-11-01", "2026-11-03"]]
    assert result["discovery_remaining"]["count"] == 3
    assert result["discovery_remaining"]["eligibility"] == "unknown_until_verified_stored_sleep"
    assert result["closed_pending"] > result["rolling_pending"]
    assert result["status"] == "bounded_pending"
    later = refresh.refresh_pending(sdk_store(server), today="2026-11-03", timezone="Europe/Oslo",
                                    period_names=set(), snapshot_cache=cache)
    assert later["discovery"]["registered_endpoints"] == 3
    assert later["discovery_remaining"]["count"] == 0
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert queue["rolling"]["eligible"] == [["2026-10-25", "2026-11-03"]]


def test_empty_discovery_checks_at_most_four_months_without_negative_daily_gets():
    server, _ = seeded("2026-10-25", 1, contexts=False)
    queue_source(server, [["2000-01-01", "2000-06-29"]])
    server.calls.clear()
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="Europe/Oslo")
    assert result["discovery"]["months_observed"] == 4
    assert result["discovery_remaining"]["ranges"] == [["2000-05-01", "2000-06-29"]]
    assert result["component_operations"] == {"get": 2, "list": 5, "put": 1}
    assert not any(method == "GET" and key.startswith("health/sleep/v1/")
                   and not query.get("list-type") for method, key, query in server.calls)
    assert result["status"] == "bounded_pending"


def test_unknown_context_intent_keeps_unregistered_dependent_horizon_hold():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    identity = "nctx-" + "a" * 24
    queue_source(server, [["2026-10-25", "2026-10-26"]], rolling_context_intents={identity: {
        "wake_date": "2026-10-25", "state": "intent", "sha256": "b" * 64,
        "key": "health/night-context/v1/2026/10/2026-10-25/20261025T120000000-" + identity + ".json",
    }})
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo")
    assert result["status"] == "held"
    assert result["products_written"] == 0
    assert result["discovery_remaining"]["count"] == 2
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}


def test_exact_endpoint_filter_skips_discovery_and_preserves_unrelated_known_work():
    server, fixture = seeded("2026-10-25", 2, contexts=False)
    queue_source(server, [["2026-10-25", "2026-10-26"]])
    sources = []
    for day in ("2026-10-25", "2026-10-26"):
        key = f"health/sleep/v1/2026/10/{day}.json"
        snapshot = fixture.objects[key]
        sources.append({"key": key, "revision": snapshot.revision,
                        "sha256": hashlib.sha256(snapshot.data).hexdigest()})
    refresh.register_rolling_endpoints(sdk_store(server), sources)
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo",
                                     period_names=set(), rolling_endpoints=["2026-10-26"])
    assert result["products_written"] == 2
    assert "discovery" not in result
    assert result["discovery_remaining"]["count"] == 2
    assert result["rolling_remaining"]["28"]["hrv"]["ranges"] == [["2026-10-25", "2026-10-25"]]
    assert not any("/baseline/2026/10/25/" in key for key in server.objects)


def test_raw_defer_skips_discovery_without_consuming_or_clipping_its_ranges():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    before = queue_source(server, [["2026-10-25", "2027-01-22"]])
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo",
                                     period_names=set(), rolling_sources=[])
    assert result["products_written"] == 0
    assert result["discovery_remaining"]["count"] == 90
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}


def test_discovery_checkpoint_cas_collision_preserves_remote_rest_and_can_retry_known_failure():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    before = queue_source(server, [["2026-10-25", "2026-10-26"]])
    server.mode = "conflict"
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo")
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert not any("/baseline/" in key for key in server.objects)
    server.mode = "normal"
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo")
    assert result["products_written"] == 2
    assert result["discovery_remaining"]["count"] == 0


def test_discovery_fresh_sleep_revision_collision_does_not_checkpoint_or_publish(monkeypatch):
    server, _ = seeded("2026-10-25", 1, contexts=False)
    before = queue_source(server, [["2026-10-25", "2026-10-25"]])
    key = "health/sleep/v1/2026/10/2026-10-25.json"
    original = server.send

    def changed(request):
        if request.method == "GET" and urlsplit(request.url).path.endswith(key):
            server.objects[key] = (server.objects[key][0], '"concurrent-new-revision"')
        return original(request)

    monkeypatch.setattr(server, "send", changed)
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="Europe/Oslo")
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert not any("/baseline/" in key for key in server.objects)


def test_unknown_canonical_intent_does_not_adopt_existing_sleep_endpoint():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    key = "health/hrv/2026/10/2026-10-25.json"
    before = queue_source(server, [["2026-10-25", "2026-10-26"]], pending={
        "week/2026-W43": {"start_date": "2026-10-19", "end_date": "2026-10-25",
            "phase": "dirty", "sources": {key: {"state": "intent", "sha256": "c" * 64}},
            "completed": {}},
    })
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-26", timezone="Europe/Oslo")
    assert result["status"] == "held"
    assert result["discovery_remaining"]["count"] == 2
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}
    assert server.objects[refresh.QUEUE_KEY][0] == before


def test_source_cohort_discovers_only_intersection_and_retains_other_ranges():
    server, _ = seeded("2026-10-25", 2, contexts=False)
    queue_source(server, [["2026-09-01", "2026-10-31"]])
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-31", timezone="Europe/Oslo",
                                     rolling_sources=["health/hrv/2026/10/2026-10-25.json"])
    assert result["discovery"]["registered_endpoints"] == 2
    assert result["discovery_remaining"]["ranges"] == [["2026-09-01", "2026-10-24"]]
    assert result["discovery_remaining"]["count"] == 54
