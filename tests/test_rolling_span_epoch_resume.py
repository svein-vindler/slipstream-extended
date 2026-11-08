"""A qualified epoch survives multiple read-only chunks after the last real PUB."""
import json

import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_rolling_reference_sources import seeded
from test_rolling_reference_span import (
    external_empty_ack,
    external_product_ack,
    proof_ref,
    span_manifest,
)

from pipeline.history_backfill import main
from pipeline.history_publish import publish
from pipeline.history_state import Job, Stop, digest, encoded, read
from pipeline.r2_store import R2Store
from pipeline.rolling_reference_span import StoredReferenceJob, apply_epoch


@pytest.mark.parametrize("source_interleave", [False, True])
def test_actual_cli_pub_then_qualified_epoch_two_read_only_chunks_and_restart(
        tmp_path, monkeypatch, capsys, source_interleave):
    import boto3

    server, _ = seeded("2026-10-25", 1)
    factory = boto3.client

    def sdk(*args, **kwargs):
        client = factory(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", sdk)
    for key, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(key, value)
    job = Job.create(tmp_path / "job", "2026-10-23", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, span_sha = span_manifest(job, tmp_path, lower="2026-10-23", chunk_size=1)
    source_prefix = read(job.folder / "events.jsonl")
    epoch_sha = None

    def cli(action, *arguments):
        args = ["--state", str(job.folder), "rolling-span-" + action, "--plan-sha", job.plan_sha]
        if action != "register":
            args += ["--span-sha", span_sha]
        if epoch_sha is not None and action in {"next", "refresh"}:
            args += ["--reference-round-sha", epoch_sha]
        native = main(args + list(arguments))
        output = capsys.readouterr()
        assert native == 0, (action, output.err, output.out)
        return json.loads(output.out)

    cli("register", "--manifest", str(manifest), "--manifest-sha", span_sha)
    cli("next", "--confirm-r2-read")
    products = {}
    for index in range(3):
        published = cli("refresh", "--confirm-r2-write")
        products.update({item["key"]: item for item in published["generated_targets"]})
        ack = external_product_ack(tmp_path / f"product-ack-{index}", published, server)
        state = cli("ack", "--proof", str(ack), "--proof-sha", digest(read(ack)))
    assert len(products) == 6 and state["verified_cursor"] == 1
    actual_pub_totals = state["execution_totals"]
    assert actual_pub_totals["phases"] == 4  # One preserved Source phase and three actual Span PUBs.

    parent_folder = tmp_path / "qualified-parent"
    parent_folder.mkdir()
    meter_path = parent_folder / "meter.jsonl"
    previous, sequence = None, 0

    def append(kind, count):
        nonlocal previous, sequence
        body = encoded({"sequence": sequence, "previous": previous, "kind": kind, "count": count})
        with meter_path.open("ab") as handle:
            handle.write(body)
        previous, sequence = digest(body), sequence + 1

    append("initial", 0)
    verifier = R2Store()
    physical_before = len(server.calls)
    key = next(iter(products))
    try:
        for count in range(1, 251):
            append("attempt", count)
            snapshot = verifier.get_snapshot(key, max_bytes=262144)
            assert digest(snapshot.data) == products[key]["sha256"]
            append("known", count)
    finally:
        verifier.client.close()
    assert len(server.calls) - physical_before == 250
    meter = parent_folder / "meter.json"
    meter.write_bytes(encoded({"kind": "ACTUAL_SPAN_METER_LINK_V1", "meter_prefix": proof_ref(meter_path),
        "used": 250, "round_baseline": 0, "initial": 0, "current_round_id": None,
        "next_readback_reserve": 12}))
    native = parent_folder / "native.json"
    native.write_bytes(encoded({"actualNative": 0, "allNativeDescendantsTerminal": True}))
    permission = parent_folder / "permission.json"
    permission.write_bytes(encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "data", "outcome": "allowed"}))
    parent = parent_folder / "parent.json"
    parent.write_bytes(encoded({"schema": 1, "kind": "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1",
        "span_sha256": span_sha, "chunk_index": state["chunk_index"], "checkpoint_sha256": state["checkpoint_sha256"],
        "ack_ref": state["last_ack_ref"], "terminal_receipt_ref": state["producer_receipt"],
        "native_terminal": proof_ref(native), "source_meter_link": proof_ref(meter), "qualification_ref": proof_ref(permission)}))
    admitted = cli("admit-epoch", "--proof", str(parent), "--proof-sha", digest(read(parent)), "--confirm-r2-write")
    epoch_sha = admitted["reference_round_sha256"]
    baseline = admitted["proof"]["new_round_baseline"]
    assert baseline["counts"] == actual_pub_totals["counts"]
    assert admitted["proof"]["independent_pool_transition"] is True
    puts_before = list(server.puts)
    previous_counts = actual_pub_totals["counts"]
    expected_phases, source_cost = 0, dict.fromkeys(("get", "list", "put"), 0)
    for index in (1, 2):
        # Every public CLI invocation reconstructs Job/context from its durable evidence.
        planned = cli("next", "--confirm-r2-read")
        totals = planned["execution_totals"]
        assert planned["status"] == "verification_pending" and planned["generated_targets"] == []
        assert totals["reference_round_sha256"] == epoch_sha
        assert totals["round_baseline"] == baseline and totals["phases"] == expected_phases
        assert all(totals["counts"][kind] >= previous_counts[kind] for kind in previous_counts)
        assert totals["counts"]["list"] > previous_counts["list"]
        read_receipt = json.loads(read(planned["read_receipt_ref"]["path"]))
        assert totals["counts"] == {kind: previous_counts[kind] + source_cost[kind] + read_receipt["counts"][kind]
                                   for kind in previous_counts}
        assert totals["counts"]["get"] - baseline["counts"]["get"] <= 6000
        assert sum(totals["counts"][kind] - baseline["counts"][kind] for kind in ("list", "put")) <= 1000
        ack = external_empty_ack(tmp_path / f"gap-ack-{index}", planned)
        state = cli("ack", "--proof", str(ack), "--proof-sha", digest(read(ack)))
        assert state["verified_cursor"] == index + 1
        if index == 1 and source_interleave:
            source_published = publish(Job(job.folder), job.plan_sha, confirm=True, target_dates=[],
                finalize_dates=["2026-10-23"], execution_context={"round": "c" * 32})
            source_cost = source_published["baseline_refresh"]["publication_counts"]
            assert source_published["baseline_refresh"]["execution_totals"]["phases"] == 2
            assert any(source_cost.values())
            source_prefix = read(job.folder / "events.jsonl")
            expected_phases = 1
            puts_before = list(server.puts)
        previous_counts = totals["counts"]
    assert state["status"] == "complete_with_gaps" and state["remaining_chunks"] == 0
    assert read(job.folder / "events.jsonl") == source_prefix
    assert server.puts == puts_before
    execution = StoredReferenceJob(job, state["manifest"]["storage_target_sha256"])
    # The current producer is a READ; retrieve the real pre-epoch PUB pinned by admission.
    last_pub = json.loads(read(admitted["proof"]["terminal_receipt_ref"]["path"]))
    for fault in ("unrecorded_read", "changed_prior_pub", "forged_source_watermark"):
        context = {"counts": dict(state["execution_totals"]["counts"]),
            "body_bytes": state["execution_totals"]["body_bytes"], "phases": 4,
            "round_baseline": admitted["proof"]["prior_totals"]["round_baseline"]}
        publication = json.loads(json.dumps(last_pub))
        if fault == "unrecorded_read":
            context["counts"]["list"] += 1
        elif fault == "changed_prior_pub":
            publication["baseline_refresh"]["execution_totals"]["counts"]["get"] += 1
        else:
            context["source_watermark"] = json.loads(json.dumps(state["execution_totals"]["source_watermark"]))
            context["source_watermark"]["counts"]["get"] += 1
        before = len(server.calls)
        with pytest.raises(Stop, match="rolling_span_(epoch_parent_changed|source_ledger_regressed)"):
            apply_epoch(execution, epoch_sha, target_sha=state["manifest"]["storage_target_sha256"],
                execution_context=context, latest_receipt=publication, journal_tip=execution.events()[1])
        assert len(server.calls) == before
