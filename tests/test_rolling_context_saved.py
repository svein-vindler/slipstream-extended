"""Confirmed context metadata resumes only qualified publication checkpoints."""

import copy
import hashlib
import json

import pytest
from test_rolling_discovery import queue_source
from test_rolling_reference_sources import sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline import rolling_night_references as rolling


def setup_saved(*, inflight=False):
    server, _ = seeded("2026-10-25", 1, contexts=False)
    key = "health/sleep/v1/2026/10/2026-10-25.json"
    body, revision = server.objects[key]
    refresh.register_rolling_endpoints(sdk_store(server), [{
        "key": key, "revision": revision, "sha256": hashlib.sha256(body).hexdigest(),
    }])
    first = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC")
    assert first["products_written"] == 2
    product_key = next(k for k in server.objects if "/baseline/" in k
                       and json.loads(server.objects[k][0])["args"]["channel"] == "hrv")
    product, product_revision = server.objects[product_key]
    supplied, _ = seeded("2026-10-25", 1, contexts=True)
    event_key = next(k for k in supplied.objects if k.startswith("health/night-context/"))
    event, event_revision = supplied.objects[event_key]
    server.objects[event_key] = (event, event_revision)
    server.metadata[event_key] = supplied.metadata[event_key]
    identity = json.loads(event)["context_id"]
    row = {"rolling_window": 28, "channel": "hrv", "start_date": "2026-09-28",
           "end_date": "2026-10-25", "phase": "publishing", "sources": {},
           "source_proof_sha256": hashlib.sha256(b"synthetic-known-old-generation").hexdigest(),
           "completed": {product_key: {"revision": product_revision,
                                       "sha256": hashlib.sha256(product).hexdigest()}}}
    if inflight:
        row["inflight"] = {"key": product_key, "revision": product_revision,
                           "sha256": hashlib.sha256(product).hexdigest()}
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    queue["pending"]["rolling/28/2026-10-25/hrv"] = row
    queue["rolling_context_intents"] = {identity: {
        "wake_date": "2026-10-25", "key": event_key, "sha256": hashlib.sha256(event).hexdigest(),
        "state": "saved", "saved_revision": event_revision,
    }}
    # Worker has durably enqueued the next known generation while retaining the
    # publisher row verbatim; the fixture models that external boundary.
    for n in rolling.WINDOWS:
        for channel in rolling.CHANNELS:
            queue["rolling"]["dirty"][str(n)][channel] = [["2026-10-25", "2026-10-25"]]
    queue_source(server, [], **{k: v for k, v in queue.items() if k not in {
        "schema_version", "kind", "pending", "rolling_discovery"}}, pending=queue["pending"])
    return server, identity, product_key, product


def test_known_append_busy_then_qualified_checkpoint_rebuilds_without_adopting_unknown_write():
    server, identity, product_key, previous = setup_saved(inflight=True)
    busy_before = copy.deepcopy(json.loads(server.objects[refresh.QUEUE_KEY][0]))
    busy = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC",
                                   period_names=set())
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert queue["rolling_context_intents"][identity]["state"] == "saved"
    assert queue["pending"]["rolling/28/2026-10-25/hrv"] == busy_before["pending"]["rolling/28/2026-10-25/hrv"]
    assert busy["saved_contexts"]["busy"] == 1
    assert server.objects[product_key][0] == previous
    # The original writer's known ACK/readback checkpoint removes its inflight
    # marker. Nothing here infers completion from time or merely matching bytes.
    store = sdk_store(server)
    writer = refresh._IO(store)
    try:
        snapshot = writer.get_snapshot(refresh.QUEUE_KEY, max_bytes=262144)
        queue["pending"]["rolling/28/2026-10-25/hrv"].pop("inflight")
        refresh._save_queue(writer, queue, snapshot)
    finally:
        writer.close()
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC",
                                     period_names=set())
    assert result["saved_contexts"]["completed"] == 1
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    assert identity not in queue.get("rolling_context_intents", {})
    assert result["products_written"] >= 1
    assert any(body == previous for key, (body, _) in server.objects.items()
               if "/history/" in key and key.endswith("/product.json"))
    print({"busy": busy["component_operations"], "qualified": result["component_operations"],
           "saved_contexts": result["saved_contexts"]})


def test_conditional_queue_conflict_retains_saved_marker_and_known_product_for_later_invocation():
    server, identity, product_key, previous = setup_saved()
    before = server.objects[refresh.QUEUE_KEY]
    puts = len(server.puts)
    server.mode = "conflict"
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", period_names=set())
    assert server.objects[refresh.QUEUE_KEY] == before
    assert server.objects[product_key][0] == previous
    assert server.puts[puts:] == [refresh.QUEUE_KEY]
    server.mode = "normal"
    resumed = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", period_names=set())
    assert resumed["saved_contexts"]["completed"] == 1
    assert identity not in json.loads(server.objects[refresh.QUEUE_KEY][0])["rolling_context_intents"]


def test_present_event_does_not_upgrade_unknown_append_intent():
    server, identity, product_key, previous = setup_saved()
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    queue["rolling_context_intents"][identity]["state"] = "intent"
    queue["rolling_context_intents"][identity].pop("saved_revision")
    queue_source(server, [], **{k: v for k, v in queue.items() if k not in {
        "schema_version", "kind", "pending", "rolling_discovery"}}, pending=queue["pending"])
    before = server.objects[refresh.QUEUE_KEY]
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", period_names=set())
    assert result["status"] == "held"
    assert result["saved_contexts"]["completed"] == 0
    assert server.objects[refresh.QUEUE_KEY] == before
    assert server.objects[product_key][0] == previous


def test_unknown_append_without_eligible_endpoints_is_still_held():
    server, _ = seeded("2026-10-25", 1, contexts=True)
    event_key = next(key for key in server.objects if key.startswith("health/night-context/"))
    event = server.objects[event_key][0]
    identity = json.loads(event)["context_id"]
    before = queue_source(server, [], rolling_context_intents={identity: {
        "wake_date": "2026-10-25", "key": event_key,
        "sha256": hashlib.sha256(event).hexdigest(), "state": "intent",
    }})
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC")
    assert result["status"] == "held" and result["held_periods"] == 1
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}


@pytest.mark.parametrize("change", ["event_revision", "event_bytes", "completed_revision", "completed_metadata", "source_intent"])
def test_unverifiable_saved_context_or_unknown_source_cannot_be_reconciled(change):
    server, identity, product_key, previous = setup_saved()
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    row = queue["pending"]["rolling/28/2026-10-25/hrv"]
    event_key = queue["rolling_context_intents"][identity]["key"]
    if change == "event_revision":
        server.objects[event_key] = (server.objects[event_key][0], '"changed"')
    elif change == "event_bytes":
        server.objects[event_key] = (b"{}", server.objects[event_key][1])
    elif change == "completed_revision":
        row["completed"][product_key]["revision"] = '"wrong-checkpoint"'
    elif change == "completed_metadata":
        server.metadata[product_key] = {"content-type": "text/plain"}
    else:
        row["sources"]["health/hrv/2026/10/2026-10-25.json"] = {
            "state": "intent", "sha256": "f" * 64, "revision": None,
        }
    queue_source(server, [], **{k: v for k, v in queue.items() if k not in {
        "schema_version", "kind", "pending", "rolling_discovery"}}, pending=queue["pending"])
    before = server.objects[refresh.QUEUE_KEY]
    if change == "source_intent":
        refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", period_names=set())
        assert identity in json.loads(server.objects[refresh.QUEUE_KEY][0])["rolling_context_intents"]
    else:
        with pytest.raises(refresh.BaselineRefreshStop):
            refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", period_names=set())
        assert server.objects[refresh.QUEUE_KEY] == before
    assert server.objects[product_key][0] == previous


def test_raw_staging_never_reconciles_or_reads_saved_context_events():
    server, _, _, _ = setup_saved()
    before = server.objects[refresh.QUEUE_KEY]
    start = len(server.calls)
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC",
                                     period_names=set(), rolling_sources=[])
    assert result["products_written"] == 0
    assert "saved_contexts" not in result
    assert server.objects[refresh.QUEUE_KEY] == before
    assert server.calls[start:] == [("GET", refresh.QUEUE_KEY, {})]


def test_known_write_budget_defer_does_not_report_or_persist_context_completion():
    server, identity, product_key, previous = setup_saved()
    before = server.objects[refresh.QUEUE_KEY]
    puts = len(server.puts)
    result = refresh.refresh_pending(sdk_store(server, max_write_bytes_per_run=1),
                                     today="2026-10-25", timezone="UTC", period_names=set())
    assert result["status"] == "bounded_pending"
    assert result.get("saved_contexts", {}).get("completed", 0) == 0
    assert server.objects[refresh.QUEUE_KEY] == before
    assert identity in json.loads(before[0])["rolling_context_intents"]
    assert server.objects[product_key][0] == previous
    assert len(server.puts) == puts


@pytest.mark.parametrize("first,last,expected", [
    ("2026-01-02", "2026-01-04", [["2026-01-01", "2026-01-01"], ["2026-01-05", "2026-01-05"]]),
    ("2025-12-31", "2026-01-01", [["2026-01-02", "2026-01-05"]]),
    ("2026-01-05", "2026-01-06", [["2026-01-01", "2026-01-04"]]),
    ("2025-12-31", "2026-01-06", []),
    ("2026-01-06", "2026-01-07", [["2026-01-01", "2026-01-05"]]),
])
def test_shared_inclusive_interval_subtraction(first, last, expected):
    assert rolling.subtract([["2026-01-01", "2026-01-05"]], first, last) == expected
