"""Actual finite publisher isolates selected work and preserves raw gzip identity."""

import gzip
import json

import pytest
from test_history_baseline_integration import sdk_store
from test_reference_bulk_core import published_scope

from pipeline.history_state import Stop, digest, encoded, read
from pipeline.night_baseline_refresh import QUEUE_KEY, register_rolling_endpoints
from pipeline.night_products import normalize_args, storage_key
from pipeline.reference_bulk import load_bulk_workset, publish_bulk


def seed_queue(server, queue):
    body = encoded(queue)
    server.objects[QUEUE_KEY] = (body, '"synthetic-queue"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}


def old_unknown_row():
    key = storage_key(normalize_args(
        start_date="2019-12-30", end_date="2020-01-05", period="custom", channel="hrv",
    ))
    return {
        "phase": "publishing", "start_date": "2019-12-30", "end_date": "2020-01-05",
        "sources": {}, "completed": {},
        "inflight": {"key": key, "sha256": "e" * 64, "revision": None},
    }


@pytest.mark.parametrize("old_held,compressed", [(True, False), (False, True), (True, True)])
def test_selected_bulk_publication_preserves_unrelated_unknown_and_existing_gzip(
    tmp_path, old_held, compressed,
):
    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(tmp_path, publish=False)
    old = old_unknown_row()
    if old_held:
        seed_queue(server, {"schema_version": 1, "kind": "night-baseline-refresh-queue",
                            "pending": {"week/2020-W01": old}})
    target = workset.targets[0]
    existing = None
    if compressed:
        existing = gzip.compress(workset.product_for(target["key"])["body"].encode(), mtime=0)
        server.objects[target["key"]] = (existing, '"opaque-existing-gzip"')
        server.metadata[target["key"]] = {
            "content-type": "application/json", "content-encoding": "gzip",
        }

    result = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                          carried=carried, store_factory=lambda: sdk_store(server))

    assert result["status"] == "verification_pending"
    assert result["published_products"] == 6
    assert len(result["generated_targets"]) == 6
    if old_held:
        assert json.loads(server.objects[QUEUE_KEY][0])["pending"]["week/2020-W01"] == old
        assert old["inflight"]["key"] not in server.puts
    if compressed:
        assert server.objects[target["key"]] == (existing, '"opaque-existing-gzip"')
        assert target["key"] not in server.puts
        observed = next(row for row in result["generated_targets"] if row["key"] == target["key"])
        assert observed["sha256"] == digest(existing)
        assert observed["revision"] == '"opaque-existing-gzip"'
        independent = sdk_store(server)
        snapshot = independent.get_snapshot(target["key"], max_bytes=262144)
        independent.client.close()
        assert snapshot.data == existing
        assert snapshot.revision == observed["revision"]
        assert snapshot.content_type == "application/json"
        assert snapshot.encoding == "gzip"
        assert json.loads(gzip.decompress(snapshot.data))["args"] == target["args"]


def test_selected_unknown_bulk_target_holds_without_claiming_publication(tmp_path):
    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(tmp_path, publish=False)
    target = workset.targets[0]
    day = target["endpoint"]
    key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
    body, revision = server.objects[key]
    store = sdk_store(server)
    register_rolling_endpoints(store, [{"key": key, "revision": revision, "sha256": digest(body)}],
                               today=day, prepared_manifest=workset)
    store.client.close()
    queue = json.loads(server.objects[QUEUE_KEY][0])
    name = f"rolling/{target['window_days']}/{day}/{target['channel']}"
    queue["pending"][name] = {
        "rolling_window": target["window_days"], "channel": target["channel"],
        "start_date": target["args"]["start_date"], "end_date": day,
        "phase": "publishing", "sources": {}, "completed": {},
        "inflight": {"key": target["key"], "sha256": "f" * 64, "revision": None},
    }
    seed_queue(server, queue)
    selected_unknown = queue["pending"][name]

    with pytest.raises(Stop):
        publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                     carried=carried, store_factory=lambda: sdk_store(server))

    reopened, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    assert reopened.status()["hold"] is not None
    assert json.loads(server.objects[QUEUE_KEY][0])["pending"][name] == selected_unknown
    assert target["key"] not in server.puts
    assert not (folder / "PUBLICATION-CHECKPOINTS.jsonl").exists()
    terminal = json.loads(read(sorted((folder / "publications").glob("*/TERMINAL.json"))[0]))
    assert terminal["status"] == "hold"

