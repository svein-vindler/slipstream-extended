"""Actual sealed bulk publisher progression on external synthetic SDK transport."""

import json

import pytest
from test_reference_bulk_core import published_scope

from pipeline.history_state import encoded, read


def test_sealed_scope_larger_than_fourteen_requeues_existing_clean_endpoints(tmp_path):
    from test_history_baseline_integration import sdk_store

    from pipeline import rolling_night_references as rolling
    from pipeline.night_baseline_refresh import QUEUE_KEY
    from pipeline.reference_bulk import publish_bulk

    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(
        tmp_path, days=8, publish=False)
    queue = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {}}
    rolling.register(queue, workset.binding["endpoints"])
    for window in (28, 30):
        for channel in rolling.CHANNELS:
            queue["rolling"]["dirty"][str(window)][channel] = []
    server.objects[QUEUE_KEY] = (encoded(queue), '"synthetic-existing-eligible"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}
    result = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                          carried=carried, store_factory=lambda: sdk_store(server))
    reports = [json.loads(read(path))["report"]
               for path in sorted((folder / "publications").glob("*/batch-*/RESULT.json"))]
    print(json.dumps({"expected": len(workset.targets), "published": result["published_products"],
                      "status": result["status"], "batches": len(reports),
                      "batch_products": [report["products_written"] for report in reports],
                      "usage": result["usage"]["usage"]}))
    assert len(workset.targets) == 48
    assert result["published_products"] == len(workset.targets)
    assert len(result["generated_targets"]) == len(workset.targets)
    assert result["status"] == "verification_pending"
    assert result["total_products"] == 48 and result["unpublished_products"] == 0
    assert result["publication_complete"] is True


def test_existing_outstanding_pending_row_does_not_create_duplicate_dirty_work(tmp_path):
    from test_history_baseline_integration import sdk_store

    from pipeline import rolling_night_references as rolling
    from pipeline.night_baseline_refresh import QUEUE_KEY
    from pipeline.reference_bulk import publish_bulk

    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(
        tmp_path, days=3, publish=False)
    queue = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {}}
    rolling.register(queue, workset.binding["endpoints"])
    for channels in queue["rolling"]["dirty"].values():
        for channel in rolling.CHANNELS:
            channels[channel] = []
    target = workset.targets[0]
    name = f"rolling/{target['window_days']}/{target['endpoint']}/{target['channel']}"
    queue["pending"][name] = {"rolling_window": target["window_days"], "channel": target["channel"],
        "start_date": target["args"]["start_date"], "end_date": target["endpoint"],
        "phase": "dirty", "sources": {}, "completed": {}}
    server.objects[QUEUE_KEY] = (encoded(queue), '"synthetic-pending-existing"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}
    result = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                          carried=carried, store_factory=lambda: sdk_store(server))
    assert result["publication_complete"] is True and result["published_products"] == 18
    after = json.loads(server.objects[QUEUE_KEY][0])
    assert after["pending"] == {}
    assert all(not dirty for channels in after["rolling"]["dirty"].values() for dirty in channels.values())


def test_batch_limit_is_explicitly_incomplete_and_unverified_checkpoint_has_zero_retry_io(tmp_path):
    from test_history_baseline_integration import sdk_store

    from pipeline.reference_bulk import publish_bulk

    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(
        tmp_path, days=3, publish=False)
    result = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
        carried=carried, store_factory=lambda: sdk_store(server), max_batches=1)
    assert result["status"] == "verification_pending" and result["published_products"] == 6
    assert result["total_products"] == 18 and result["unpublished_products"] == 12
    assert result["publication_complete"] is False and result["verified_products"] == 0
    terminal = json.loads(read(next((folder / "publications").glob("*/TERMINAL.json"))))
    assert terminal["status"] == "bounded_pending"
    before = list(server.calls)
    repeated = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
        carried=carried, store_factory=lambda: pytest.fail("unverified checkpoint selected another SDK"))
    assert repeated == result and server.calls == before
