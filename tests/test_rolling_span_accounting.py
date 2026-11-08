"""Source budgets retain their cohort while qualified span epochs carry account cost."""
import json

import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_rolling_reference_sources import seeded
from test_rolling_reference_span import external_product_ack, proof_ref, span_manifest

from pipeline.history_publish import publish
from pipeline.history_reference_successor import _eligible, admit
from pipeline.history_state import Job, Stop, encoded, read
from pipeline.rolling_reference_span import (
    acknowledge_span,
    admit_span_epoch,
    effective_source_totals,
    next_plan,
    refresh_span,
    register_span,
    shared_span_usage,
    source_epoch_key,
    validate_span_account_usage,
)


def test_partial_source_shared_pool_then_qualified_foreign_epoch_does_not_poison_source(tmp_path, monkeypatch):
    import boto3

    server, _ = seeded("2026-10-19", 7)
    actual_factory = boto3.client

    def factory(*args, **kwargs):
        client = actual_factory(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    job = Job.create(tmp_path / "job", "2026-10-19", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, span_sha = span_manifest(job, tmp_path, lower="2026-10-19")
    (job.folder / "storage-target.json").write_bytes(encoded({
        "target_sha256": json.loads(read(manifest))["storage_target_sha256"]}))
    original_source = read(job.folder / "events.jsonl")
    source_totals = job.snapshot()["baseline_refresh"]["execution_totals"]
    original_totals = json.loads(json.dumps(source_totals))
    run = next(frame["event"]["run"] for frame in reversed(job.events()[0]) if frame["event"]["type"] == "end")
    source_receipt = json.loads(read(job.folder / "runs" / run / "RECEIPT.json"))
    key = source_epoch_key(source_totals)
    register_span(job, job.plan_sha, proof_ref(manifest))
    planned = next_plan(job, job.plan_sha, span_sha)
    assert planned["status"] == "planned" and len(planned["plan"]["targets"]) == 42
    before = len(server.calls)
    actual_phases = 0
    for index in range(14):
        phase = refresh_span(job, job.plan_sha, span_sha, confirm=True)
        if phase["status"] == "epoch_required":
            break
        actual_phases += 1
        assert phase["status"] == "verification_pending"
        assert phase["verified_cursor"] == 0
        ack = external_product_ack(tmp_path / f"ack-{index}", phase, server)
        state = acknowledge_span(job, job.plan_sha, span_sha, proof_ref(ack))
        assert state["verified_cursor"] == 0
    assert phase["status"] == "epoch_required" and actual_phases > 0 and len(server.calls) > before
    initial = shared_span_usage(job, key)
    assert initial["source_pool"]["publication_phases"] == actual_phases
    assert initial["separate_epochs"]["publication_phases"] == 0
    assert effective_source_totals(job, source_totals)["phases"] == 1 + actual_phases
    assert _eligible(job, source_receipt, "c" * 32, ["2026-10-19"]) == source_totals
    with pytest.raises(Stop, match="reference_successor_capacity_remains"):
        _eligible(job, source_receipt, "c" * 32, ["2026-10-19"], span_account_usage=None)
    assert effective_source_totals(job, source_totals, annotation=None) == source_totals

    parent_folder = tmp_path / "epoch-parent"
    parent_folder.mkdir()
    native = parent_folder / "native.json"
    native.write_bytes(encoded({"actualNative": 0, "allNativeDescendantsTerminal": True}))
    permission = parent_folder / "permission.json"
    permission.write_bytes(encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "data", "outcome": "allowed"}))
    meter_prefix = parent_folder / "meter.jsonl"
    meter_prefix.write_bytes(encoded({"sequence": 0, "previous": None, "kind": "initial", "count": 0}))
    meter = parent_folder / "meter.json"
    meter.write_bytes(encoded({"kind": "ACTUAL_SPAN_METER_LINK_V1", "meter_prefix": proof_ref(meter_prefix),
        "used": 0, "round_baseline": 0, "initial": 0, "current_round_id": None, "next_readback_reserve": 84}))
    parent = parent_folder / "parent.json"
    parent.write_bytes(encoded({"schema": 1, "kind": "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1",
        "span_sha256": span_sha, "chunk_index": phase["chunk_index"], "checkpoint_sha256": phase["checkpoint_sha256"],
        "ack_ref": state["last_ack_ref"], "terminal_receipt_ref": state["producer_receipt"],
        "native_terminal": proof_ref(native), "source_meter_link": proof_ref(meter), "qualification_ref": proof_ref(permission)}))
    epoch = admit_span_epoch(job, job.plan_sha, span_sha, proof_ref(parent), confirm=True)
    foreign = refresh_span(job, job.plan_sha, span_sha, confirm=True, reference_round_sha=epoch["reference_round_sha256"])
    assert foreign["status"] == "verification_pending"
    foreign_ack = external_product_ack(tmp_path / "foreign-ack", foreign, server)
    acknowledge_span(job, job.plan_sha, span_sha, proof_ref(foreign_ack))
    current = shared_span_usage(job, key)
    assert current["source_pool"] == initial["source_pool"]
    assert current["separate_epochs"]["publication_phases"] == 1
    assert current["lifetime"]["publication_phases"] == actual_phases + 1
    assert validate_span_account_usage(job, initial, key)["lifetime"] == initial["lifetime"]
    altered = json.loads(json.dumps(current))
    altered["source_pool"]["counts"]["get"] = 0
    with pytest.raises(Stop, match="rolling_span_usage_changed"):
        validate_span_account_usage(job, altered, key)
    owner = job.folder / "rolling-reference-execution" / "OWNER.lock"
    owner.write_bytes(encoded({"run": "unknown"}))
    before_hold = len(server.calls)
    with pytest.raises(Stop, match="unknown_owner_hold_no_takeover"):
        shared_span_usage(job, key)
    assert len(server.calls) == before_hold
    owner.unlink()  # The fixture removes only its own synthetic unknown-owner marker.
    assert read(job.folder / "events.jsonl") == original_source
    assert job.snapshot()["baseline_refresh"]["execution_totals"] == original_totals

    successor = admit(job, job.plan_sha, "c" * 32, ["2026-10-19"])
    proof = json.loads(read(job.folder / "reference-successors" / (successor + ".json")))
    assert proof["span_account_usage"] == current
    continued = publish(job, job.plan_sha, confirm=True, target_dates=[], finalize_dates=["2026-10-19"],
        execution_context={"round": "c" * 32, "reference_successor_sha256": successor})
    totals = continued["baseline_refresh"]["execution_totals"]
    assert totals["phases"] == 1 and totals["round_baseline"]["counts"] == original_totals["counts"]
    assert totals["span_account_usage"]["source_pool"]["publication_phases"] == 0
    assert totals["span_account_usage"]["lifetime"]["publication_phases"] == actual_phases + 1
    assert totals["counts"]["get"] < current["lifetime"]["counts"]["get"]
    assert not any(name.startswith(("health/signals/", "health/hrv/")) for name in server.puts)
    print(json.dumps({"actual_initial_span_phases": actual_phases,
        "actual_foreign_span_phases": 1, "source_primary_before": original_totals,
        "source_primary_after": totals, "account_usage": current}, sort_keys=True))
