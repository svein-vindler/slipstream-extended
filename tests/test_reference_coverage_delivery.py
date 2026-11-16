"""Shared stored-product selection and coverage barriers; synthetic SDK only."""
import copy
import json

import pytest
from test_history_baseline_integration import sdk_store
from test_reference_bulk_core import file_ref, make_scope
from test_rolling_reference_sources import seeded

from pipeline import reference_bulk as core
from pipeline.history_state import Job, Stop, digest, encoded
from pipeline.reference_bulk_budget import collection_plan, inventory_plan, publication_plan
from pipeline.reference_coverage import bulk_coverage


@pytest.fixture(scope="module")
def delivered_gap(tmp_path_factory):
    root = tmp_path_factory.mktemp("stored-product-coverage")
    job = Job.create(root / "job", "2026-10-25", "2026-10-26", ["hr", "hrv"],
                     "legacy", "synthetic")
    scope = make_scope(root, job, "2026-10-25", "2026-10-26")
    folder = root / "workset"
    carried = {"independent_lifetime_gets": 17}
    # Only the first declared endpoint has stored Sleep. The real build classifies
    # the second endpoint as known source absence rather than a fabricated product.
    server, _ = seeded("2026-10-25", 1)
    core.prepare_bulk(job, job.plan_sha, scope, folder)
    workset, _ = core.load_bulk_workset(job, job.plan_sha, scope, folder)
    core.inventory_bulk(job, job.plan_sha, scope, folder,
        resource_plan_ref=inventory_plan(workset, carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    workset, _ = core.load_bulk_workset(job, job.plan_sha, scope, folder)
    collected = core.collect_bulk(job, job.plan_sha, scope, folder,
        resource_plan_ref=collection_plan(workset, carried=carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    assert collected["status"] == "sources_complete"
    core.build_bulk(job, job.plan_sha, scope, folder, carried=carried)
    workset, _ = core.load_bulk_workset(job, job.plan_sha, scope, folder)
    result = core.publish_bulk(job, job.plan_sha, scope, folder,
        resource_plan_ref=publication_plan(workset, carried=carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    # Actual synthetic SDK postimages exercise decoding/variant/source identity.
    # This is the coverage seam, not an independent JSON/SSE postcheck claim.
    readbacks = []
    store = sdk_store(server)
    try:
        for index, target in enumerate(result["generated_targets"]):
            snapshot = store.get_snapshot(target["key"], max_bytes=262144)
            assert snapshot.revision == target["revision"]
            assert digest(snapshot.data) == target["sha256"]
            path = root / f"sdk-{index}.bin"
            path.write_bytes(snapshot.data)
            readbacks.append({"keySHA256": digest(target["key"].encode()),
                "body": file_ref(path), "metadataSHA256": digest(encoded({
                    "revision": snapshot.revision, "content_type": snapshot.content_type,
                    "encoding": snapshot.encoding}))})
    finally:
        store.client.close()
    binding = root / "coverage-binding.json"
    binding.write_bytes(encoded({}))
    proof = {"generated_targets": result["generated_targets"], "readbacks": readbacks,
             "binding_ref": file_ref(binding)}
    return job, workset, result, proof


def test_publication_targets_preserve_known_gaps_and_publish_only_sealed_products(delivered_gap):
    _, workset, result, _ = delivered_gap
    sealed = (workset.folder / "BUILD-SEAL.json").read_bytes()
    build = json.loads(sealed)
    targets = workset.publication_targets()
    assert len(workset.targets) == 12
    assert len(build["products"]) == len(build["outcomes"]) == len(targets) == 6
    assert {row["key"] for row in targets} == set(build["products"])
    assert not {row["key"] for row in targets}.intersection(build["outcomes"])
    assert {row["key"] for row in result["generated_targets"]} == set(build["products"])
    assert (workset.folder / "BUILD-SEAL.json").read_bytes() == sealed


def test_coverage_uses_all_planned_keys_and_is_idempotent_without_mutation(delivered_gap):
    _, workset, _, proof = delivered_gap
    before = copy.deepcopy(proof)
    report = bulk_coverage(workset, proof)
    assert report["coverage_complete"] is True
    assert report["counts"]["planned"] == 12
    assert report["counts"]["requested"] == report["counts"]["complete"] == 6
    assert report["counts"]["excluded"] == 6
    assert "wholeDataVerified" not in report
    assert bulk_coverage(workset, proof) == report and proof == before


def test_ack_coverage_gate_rejects_missing_product_without_checkpoint_or_source_ack(delivered_gap):
    job, workset, _, proof = delivered_gap
    missing = {**proof, "generated_targets": proof["generated_targets"][:-1],
               "readbacks": proof["readbacks"][:-1]}
    report = bulk_coverage(workset, missing)
    assert report["coverage_complete"] is False and report["counts"]["blocked"] == 1
    checkpoint = workset.folder / "PUBLICATION-CHECKPOINTS.jsonl"
    before = checkpoint.read_bytes()
    journal = (job.folder / "events.jsonl").read_bytes()
    # Both repository entrypoints call the same shared coverage algorithm after
    # their existing postcheck validation, before any verified checkpoint/ACK.
    gate = getattr(core, "validate_bulk_coverage", None)
    if gate is None:
        gate = core._validate_requested_coverage
    with pytest.raises(Stop, match="reference_bulk_requested_coverage_incomplete"):
        gate(workset, missing)
    assert checkpoint.read_bytes() == before
    assert (job.folder / "events.jsonl").read_bytes() == journal
