"""A later Source append must not hide an already qualified Span epoch."""

import json
import sys

from history_storage_fixture import ACCOUNT, BUCKET
from test_rolling_reference_sources import seeded
from test_rolling_reference_span import external_product_ack, proof_ref, span_manifest

from pipeline.history_backfill import main
from pipeline.history_publish import publish
from pipeline.history_state import Job, Stop, digest, encoded, read
from pipeline.r2_store import R2Store


def test_cli_admits_next_epoch_after_repaired_checkpoint_and_source_append(
    tmp_path, monkeypatch, capsys
):
    """Public CLI, local SDK transport, real stored PUB/ACK and Source carry."""
    import boto3

    server, _ = seeded("2026-10-25", 1)
    original_client = boto3.client

    def sdk(*args, **kwargs):
        client = original_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", sdk)
    for key, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(key, value)
    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, span_sha = span_manifest(job, tmp_path)
    selected_epoch = None
    observations = []

    def cli(action, *arguments):
        command = ["--state", str(job.folder), "rolling-span-" + action, "--plan-sha", job.plan_sha]
        if action != "register":
            command += ["--span-sha", span_sha]
        if selected_epoch and action in {"next", "refresh"}:
            command += ["--reference-round-sha", selected_epoch]
        stops = []

        def observe_exception(frame, event, value):
            # Diagnostics only: preserve the CLI's real exception and catch path.
            if event == "exception" and isinstance(value[1], Stop):
                stops.append(
                    {
                        "category": value[1].category,
                        "module": frame.f_globals.get("__name__"),
                        "line": frame.f_lineno,
                    }
                )
            return observe_exception

        previous_trace = sys.gettrace()
        if action == "admit-epoch":
            sys.settrace(observe_exception)
        try:
            native = main(command + list(arguments))
        finally:
            sys.settrace(previous_trace)
        output = capsys.readouterr()
        observations.append(
            {
                "action": action,
                "native": native,
                "stdout": output.out,
                "stderr": output.err,
                "typedStops": stops,
            }
        )
        (tmp_path / "cli-observations.json").write_bytes(encoded(observations))
        assert native == 0, observations[-1]
        return json.loads(output.out)

    cli("register", "--manifest", str(manifest), "--manifest-sha", span_sha)
    cli("next", "--confirm-r2-read")
    published = cli("refresh", "--confirm-r2-write")
    assert len(published["generated_targets"]) == 2
    ack = external_product_ack(tmp_path / "product-ack", published, server)
    prior = cli("ack", "--proof", str(ack), "--proof-sha", digest(read(ack)))
    assert prior["status"] == "planned" and prior["remaining_chunks"] == 1

    parent_dir = tmp_path / "qualified-parent"
    parent_dir.mkdir()
    meter = parent_dir / "meter.jsonl"
    frames, tip = [], None
    verifier = R2Store()
    target = published["generated_targets"][0]
    physical_before = len(server.calls)
    try:
        for count in range(251):
            for kind in ("initial",) if count == 0 else ("attempt", "known"):
                if kind == "attempt":
                    snapshot = verifier.get_snapshot(target["key"], max_bytes=262144)
                    assert digest(snapshot.data) == target["sha256"]
                body = encoded(
                    {"sequence": len(frames), "previous": tip, "kind": kind, "count": count}
                )
                frames.append(body)
                tip = digest(body)
        meter.write_bytes(b"".join(frames))
    finally:
        verifier.client.close()
    assert len(server.calls) - physical_before == 250
    link = parent_dir / "meter-link.json"
    link.write_bytes(
        encoded(
            {
                "kind": "ACTUAL_SPAN_METER_LINK_V1",
                "meter_prefix": proof_ref(meter),
                "used": 250,
                "round_baseline": 0,
                "initial": 0,
                "current_round_id": None,
                "next_readback_reserve": 12,
            }
        )
    )
    native, permission = parent_dir / "native.json", parent_dir / "permission.json"
    native.write_bytes(encoded({"actualNative": 0, "allNativeDescendantsTerminal": True}))
    permission.write_bytes(
        encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "data", "outcome": "allowed"})
    )

    def parent_for(state, name):
        parent = parent_dir / name
        parent.write_bytes(
            encoded(
                {
                    "schema": 1,
                    "kind": "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1",
                    "span_sha256": span_sha,
                    "chunk_index": state["chunk_index"],
                    "checkpoint_sha256": state["checkpoint_sha256"],
                    "ack_ref": state["last_ack_ref"],
                    "terminal_receipt_ref": state["producer_receipt"],
                    "native_terminal": proof_ref(native),
                    "source_meter_link": proof_ref(link),
                    "qualification_ref": proof_ref(permission),
                }
            )
        )
        return parent

    parent = parent_for(prior, "before-source.json")
    admitted = cli(
        "admit-epoch",
        "--proof",
        str(parent),
        "--proof-sha",
        digest(read(parent)),
        "--confirm-r2-write",
    )
    selected_epoch = admitted["reference_round_sha256"]
    calls_before_repair = len(server.calls)
    repaired = cli("next", "--confirm-r2-read")
    assert len(server.calls) == calls_before_repair
    assert repaired["reference_round_sha256"] == selected_epoch
    assert repaired["execution_totals"]["round_baseline"] == admitted["proof"]["new_round_baseline"]
    assert repaired["execution_totals"]["phases"] == 0

    source_before = read(job.folder / "events.jsonl")
    later = publish(
        Job(job.folder),
        job.plan_sha,
        confirm=True,
        target_dates=[],
        finalize_dates=["2026-10-25"],
        execution_context={"round": "c" * 32},
    )
    assert any(later["baseline_refresh"]["publication_counts"].values())
    source_after = read(job.folder / "events.jsonl")
    assert source_after.startswith(source_before) and len(source_after) > len(source_before)

    # A fresh CLI invocation must validate the installed epoch before merging its
    # old PUB totals with this current checkpoint and the later Source carry.
    current = cli("register", "--manifest", str(manifest), "--manifest-sha", span_sha)
    assert current["checkpoint_sha256"] == repaired["checkpoint_sha256"]
    parent = parent_for(current, "after-source.json")
    calls_before_admission = len(server.calls)
    next_epoch = cli(
        "admit-epoch",
        "--proof",
        str(parent),
        "--proof-sha",
        digest(read(parent)),
        "--confirm-r2-write",
    )
    assert len(server.calls) == calls_before_admission
    assert next_epoch["reference_round_sha256"] != selected_epoch
    assert next_epoch["proof"]["prior_totals"]["reference_round_sha256"] == selected_epoch
    assert (
        next_epoch["proof"]["prior_totals"]["round_baseline"]
        == admitted["proof"]["new_round_baseline"]
    )
    assert next_epoch["proof"]["new_round_baseline"]["counts"] == {
        kind: repaired["execution_totals"]["counts"][kind]
        + later["baseline_refresh"]["publication_counts"][kind]
        for kind in ("get", "list", "put")
    }
    assert read(job.folder / "events.jsonl") == source_after
