"""Public CLI finite spans over actual SDK storage with synthetic HTTP only."""
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_rolling_reference_sources import seeded

from pipeline.history_backfill import main
from pipeline.history_state import Job, digest, encoded, read


def proof_ref(path):
    body = read(path)
    return {"path": str(path), "bytes": len(body), "sha256": digest(body)}


def span_manifest(job, folder, *, lower="2026-10-25", upper="2026-10-25", today="2026-10-25",
                  chunk_size=7):
    """Prior synthetic source evidence; reference actor never creates source rows."""
    from test_rolling_span_source_proof import raw_floor_fixture

    target = digest(("https://" + ACCOUNT + ".r2.cloudflarestorage.com\0" + BUCKET).encode())
    value = {"schema": 1, "kind": "rolling-reference-span",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": target,
        "today": today, "timezone": "UTC", "upper_endpoint": upper,
        "lower_endpoint": lower, "scope_status": "provisional", "chunk_size": chunk_size,
        "parent_span_sha256": None}
    floor = raw_floor_fixture(job, folder / "floor-source", value)
    value["raw_floor_proof"] = floor
    manifest = folder / "span.json"
    manifest.write_bytes(encoded(value))
    return manifest, digest(read(manifest))


def external_empty_ack(folder, result):
    """External Root boundary for known absence; never a product verification."""
    folder.mkdir()
    permission = folder / "permission.json"
    permission.write_bytes(encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "postcheck",
        "outcome": "allowed", "bindingSHA256": "a" * 64, "callerPID": os.getpid()}))
    summary = {key: result[key] for key in ("span_sha256", "chunk_index", "checkpoint_sha256", "producer_receipts")}
    summary["plan_sha256"] = result["plan_sha256"]
    summary["generated_targets"] = []
    postcheck = folder / "postcheck.json"
    postcheck.write_bytes(encoded({"kind": "ACTUAL_METERED_SPAN_POSTCHECK_V1", **summary,
        "storage_target_sha256": result["manifest"]["storage_target_sha256"],
        "bindingSHA256": "a" * 64, "qualification_ref": proof_ref(permission),
        "classification": "known_source_gaps", "componentCleanupConfirmed": True}))
    ack = folder / "ack.json"
    ack.write_bytes(encoded({"schema": 1, "kind": "VERIFIED_STORED_SPAN_PHASE_V1", **summary,
                             "postcheck": proof_ref(postcheck)}))
    return ack


def external_product_ack(folder, result, server):
    """External Root protocol fake; authentic private importer is tested separately."""
    folder.mkdir()
    readbacks, observations = [], []
    for index, target in enumerate(result["generated_targets"]):
        body = server.objects[target["key"]][0]
        sdk = folder / f"sdk-{index}.bin"
        sdk.write_bytes(body)
        readbacks.append({"keySHA256": digest(target["key"].encode()), "body": proof_ref(sdk),
            "metadataSHA256": digest(encoded({"revision": target["revision"], "content_type": "application/json", "encoding": None}))})
        stem = folder / f"reader-{index}"
        intent = encoded({"arguments": {"period": "custom", **target["args"]},
            "binding": {"target_sha256": result["manifest"]["storage_target_sha256"]},
            "sdk_product_sha256": [digest(body)]})
        response, receipt = encoded({"syntheticConnectedResult": True}), encoded({"syntheticRootObservation": True})
        claim = {"intent_sha256": digest(intent), "receipt_ref": f"synthetic-root-{index}", "receipt_sha256": digest(receipt)}
        for suffix, value in ((".intent.json", intent), (".response", response), (".execution-receipt.json", receipt),
                              (".claim.json", encoded(claim))):
            Path(str(stem) + suffix).write_bytes(value)
        observation = Path(str(stem) + ".observation.json")
        observation.write_bytes(encoded({**claim, "status": "matched", "response_sha256": digest(response),
                                         "response_bytes": len(response)}))
        observations.append(proof_ref(observation))
    port = folder / "port.json"
    port.write_bytes(encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "outcome": "allowed", "phase": "postcheck",
                             "bindingSHA256": "a" * 64, "callerPID": os.getpid()}))
    summary = {key: result[key] for key in ("span_sha256", "chunk_index", "checkpoint_sha256", "producer_receipts")}
    summary["plan_sha256"] = result["plan_sha256"]
    summary["generated_targets"] = [{key: row[key] for key in ("key", "args", "revision", "sha256")}
                                    for row in result["generated_targets"]]
    postcheck = folder / "postcheck.json"
    postcheck.write_bytes(encoded({**summary, "kind": "ACTUAL_METERED_SPAN_POSTCHECK_V1", "bindingSHA256": "a" * 64,
        "storage_target_sha256": result["manifest"]["storage_target_sha256"], "componentCleanupConfirmed": True,
        "qualification_ref": proof_ref(port), "readbacks": readbacks, "readerObservations": observations,
        **({"classification": "planning_only"} if not summary["generated_targets"] else {})}))
    ack = folder / "ack.json"
    ack.write_bytes(encoded({**summary, "schema": 1, "kind": "VERIFIED_STORED_SPAN_PHASE_V1", "postcheck": proof_ref(postcheck)}))
    return ack




def test_64_actual_cli_chunks_resume_known_gaps_and_planning_epoch(tmp_path, monkeypatch, capsys):
    import boto3
    from test_rolling_reference_sources import RevisionServer


    server = RevisionServer()
    original_factory = boto3.client

    def factory(*args, **kwargs):
        client = original_factory(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    job = Job.create(tmp_path / "job", "2026-08-23", "2026-10-25", ["hrv"], "legacy", "synthetic")
    meter_path = tmp_path / "independent.jsonl"
    meter_path.write_bytes(encoded({"sequence": 0, "previous": None, "kind": "initial",
        "identity": "a" * 64, "invocation": "synthetic-source", "count": 0}))
    counter = SimpleNamespace(path=meter_path, used=0, round_baseline=0)
    manifest, sha = span_manifest(job, tmp_path, lower="2026-08-23", upper="2026-10-25",
                                 today="2026-10-25", chunk_size=1)
    original_source = read(job.folder / "events.jsonl")

    def cli(action, *arguments, expected=0):
        command = ["--state", str(job.folder), "rolling-span-" + action, "--plan-sha", job.plan_sha]
        if action != "register":
            command += ["--span-sha", sha]
        if action in {"next", "refresh"} and epochs:
            command += ["--reference-round-sha", epochs[-1]]
        native = main(command + list(arguments))
        output = capsys.readouterr()
        assert native in (0, 2) if expected is None else native == expected, output.err
        return json.loads(output.out)

    epochs = []
    registered = cli("register", "--manifest", str(manifest), "--manifest-sha", sha)
    assert registered["chunks"] == 64
    maximum_phase = {kind: 0 for kind in ("get", "list", "put")}
    for index in range(64):
        state = cli("status")
        planned = cli("next", "--confirm-r2-read", expected=None)
        if planned["status"] == "epoch_required":
            parent_folder = tmp_path / f"epoch-{index}"
            parent_folder.mkdir()
            native = parent_folder / "native.json"
            native.write_bytes(encoded({"actualNative": 0, "allNativeDescendantsTerminal": True}))
            permission = parent_folder / "permission.json"
            permission.write_bytes(encoded({"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "data", "outcome": "allowed"}))
            meter = parent_folder / "meter.json"
            meter.write_bytes(encoded({"kind": "ACTUAL_SPAN_METER_LINK_V1", "meter_prefix": proof_ref(counter.path),
                "used": counter.used, "round_baseline": counter.round_baseline, "initial": 0,
                "current_round_id": None, "next_readback_reserve": 12}))
            parent = parent_folder / "parent.json"
            parent.write_bytes(encoded({"schema": 1, "kind": "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1",
                "span_sha256": sha, "chunk_index": state["chunk_index"], "checkpoint_sha256": state["checkpoint_sha256"],
                "ack_ref": state["last_ack_ref"], "terminal_receipt_ref": state["producer_receipt"],
                "native_terminal": proof_ref(native), "source_meter_link": proof_ref(meter),
                "qualification_ref": proof_ref(permission)}))
            admitted = cli("admit-epoch", "--proof", str(parent), "--proof-sha", digest(read(parent)), "--confirm-r2-write")
            epochs.append(admitted["reference_round_sha256"])
            planned = cli("next", "--confirm-r2-read")
        assert planned["status"] == "verification_pending" and planned["generated_targets"] == []
        for kind, count in planned["plan"]["reads"]["operations"].items():
            maximum_phase[kind] = max(maximum_phase[kind], count)
        totals = planned["execution_totals"]
        spent = {kind: totals["counts"][kind] - totals["round_baseline"]["counts"][kind] for kind in maximum_phase}
        assert spent["get"] <= totals["publication_get_limit"] <= 6000
        assert spent["list"] + spent["put"] <= 1000 and totals["phases"] <= 14
        assert planned["plan"]["blocked"] == [{"endpoint": state["next_endpoints"][0], "reason": "sleep_not_stored"}]
        assert cli("next", "--confirm-r2-read")["plan_sha256"] == planned["plan_sha256"]
        ack = external_empty_ack(tmp_path / f"ack-{index}", planned)
        acknowledged = cli("ack", "--proof", str(ack), "--proof-sha", digest(read(ack)))
        assert acknowledged["verified_cursor"] == index + 1
        assert cli("ack", "--proof", str(ack), "--proof-sha", digest(read(ack)))["verified_cursor"] == index + 1
    assert epochs and acknowledged["status"] == "complete_with_gaps"
    assert acknowledged["remaining_chunks"] == 0 and len(acknowledged["gap_endpoints"]) == 64
    assert read(job.folder / "events.jsonl") == original_source and not server.puts
    metrics = {"actual_public_cli_chunks": 64, "qualified_planning_epochs": len(epochs),
        "physical_sdk_requests": len(server.calls), "physical_lists": sum("list-type" in row[2] for row in server.calls),
        "products_written": 0, "known_missing_sleep_endpoints": 64, "full_90_coverage_claimed": False,
        "maximum_plan_phase_operations": maximum_phase, "final_execution_totals": acknowledged["execution_totals"],
        "source_journal_sha256": digest(read(job.folder / "events.jsonl")), "source_provider_calls": 0}
    destination = tmp_path / "64-METRICS-02.json"
    destination.write_bytes(encoded(metrics))


def test_retained_32_public_plans_do_not_trap_full_span(tmp_path, monkeypatch, capsys):
    import boto3

    server, _ = seeded("2026-10-25", 1)
    actual_factory = boto3.client

    def factory(*args, **kwargs):
        client = actual_factory(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    command = ["--state", str(job.folder), "rolling-plan", "--plan-sha", job.plan_sha,
               "--confirm-r2-read", "--endpoint", "2026-10-25", "--today", "2026-10-25",
               "--timezone", "UTC"]
    for index in range(32):
        command[command.index("--endpoint") + 1] = (date(2026, 11, 1) + timedelta(days=index)).isoformat()
        assert main(command) == 0, {"index": index, "retained": len(list((job.folder / "rolling-reference-plans" / "plans").glob("*")))}
        json.loads(capsys.readouterr().out)
    # The historical bounded namespace remains capped; the finite span does not
    # delete its history or turn its limit into an unbounded catalog.
    command[command.index("--endpoint") + 1] = "2026-12-03"
    assert main(command) == 2
    capsys.readouterr()
    retained = {path.name: digest(read(path / "PLAN.json")) for path in
                (job.folder / "rolling-reference-plans" / "plans").iterdir()}
    manifest, span_sha = span_manifest(job, tmp_path)
    original_source = read(job.folder / "events.jsonl")
    base = ["--state", str(job.folder)]
    assert main(base + ["rolling-span-register", "--plan-sha", job.plan_sha,
                       "--manifest", str(manifest), "--manifest-sha", span_sha]) == 0
    registered = json.loads(capsys.readouterr().out)
    assert registered["chunks"] == 1
    assert main(base + ["rolling-span-next", "--plan-sha", job.plan_sha,
                       "--span-sha", span_sha, "--confirm-r2-read"]) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["status"] == "planned" and planned["plan"]["eligible_endpoints"] == ["2026-10-25"]
    assert planned["plan"]["reads"]["operations"]["get"] == 4
    assert retained == {path.name: digest(read(path / "PLAN.json")) for path in
                        (job.folder / "rolling-reference-plans" / "plans").iterdir()}
    assert read(job.folder / "events.jsonl") == original_source
    assert main(base + ["rolling-span-refresh", "--plan-sha", job.plan_sha,
                       "--span-sha", span_sha, "--confirm-r2-write"]) == 0
    published = json.loads(capsys.readouterr().out)
    assert published["status"] == "verification_pending"
    assert len(published["generated_targets"]) == 2
    assert published["verified_cursor"] == 0 and published["publication_cursor"] == 0
    assert published["producer_receipt"]["sha256"] and published["remote_ref"]["sha256"]
    assert read(job.folder / "events.jsonl") == original_source




@pytest.mark.parametrize("fault", ["source_owner", "span_owner", "cache", "floor", "checkpoint", "confirmation"])
def test_unknown_owner_or_changed_proof_stops_before_remote_data(tmp_path, monkeypatch, capsys, fault):
    import boto3

    server, _ = seeded("2026-10-25", 1)
    original = boto3.client

    def factory(*args, **kwargs):
        client = original(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, sha = span_manifest(job, tmp_path)
    prefix = ["--state", str(job.folder)]
    assert main(prefix + ["rolling-span-register", "--plan-sha", job.plan_sha,
                         "--manifest", str(manifest), "--manifest-sha", sha]) == 0
    capsys.readouterr()
    assert main(prefix + ["rolling-span-next", "--plan-sha", job.plan_sha,
                         "--span-sha", sha, "--confirm-r2-read"]) == 0
    current = json.loads(capsys.readouterr().out)
    namespace = Path(current["execution_namespace"])
    owner = job.folder / "OWNER.lock" if fault == "source_owner" else namespace / "OWNER.lock"
    if fault.endswith("owner"):
        owner.write_bytes(encoded({"run": "a" * 32, "phase": "unknown"}))
    elif fault == "cache":
        path = next((namespace / "rolling-reference-plans" / "objects").iterdir())
        path.write_bytes(b"corrupt")
    elif fault == "floor":
        (tmp_path / "floor-source" / "sdk-0.bin").write_bytes(b"changed")
    elif fault == "checkpoint":
        path = job.folder / "rolling-reference-spans" / sha / "CHECKPOINTS.jsonl"
        body = path.read_bytes()
        path.write_bytes(body.replace(b'"schema": 1', b'"schema": 9', 1))
    before = list(server.calls)
    arguments = ["--confirm-r2-write"] if fault != "confirmation" else []
    assert main(prefix + ["rolling-span-refresh", "--plan-sha", job.plan_sha,
                         "--span-sha", sha] + arguments) in (2, 3)
    capsys.readouterr()
    assert server.calls == before and not server.puts
    if fault.endswith("owner"):
        assert owner.exists()




def test_public_cli_resumes_registered_floor_after_initial_permission_expires(tmp_path, monkeypatch, capsys):
    from pipeline import rolling_span_source_proof

    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, sha = span_manifest(job, tmp_path)
    command = ["--state", str(job.folder), "rolling-span-register", "--plan-sha", job.plan_sha,
               "--manifest", str(manifest), "--manifest-sha", sha]
    assert main(command) == 0
    first = json.loads(capsys.readouterr().out)
    checkpoint = job.folder / "rolling-reference-spans" / sha / "CHECKPOINTS.jsonl"
    before = checkpoint.read_bytes(), manifest.read_bytes(), read(job.folder / "events.jsonl")

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return (datetime.now(UTC) + timedelta(seconds=61)).astimezone(tz)

    monkeypatch.setattr(rolling_span_source_proof, "datetime", Later)
    assert main(command) == 0, capsys.readouterr().err
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["checkpoint_sha256"] == first["checkpoint_sha256"]
    assert resumed["execution_totals"] == first["execution_totals"]
    assert before == (checkpoint.read_bytes(), manifest.read_bytes(), read(job.folder / "events.jsonl"))


@pytest.mark.parametrize("fault", ["stale_first_registration", "changed_floor", "changed_manifest"])
def test_registration_still_rejects_stale_new_or_changed_evidence(tmp_path, monkeypatch, capsys, fault):
    from pipeline import rolling_span_source_proof

    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest, sha = span_manifest(job, tmp_path)
    prefix = ["--state", str(job.folder), "rolling-span-register", "--plan-sha", job.plan_sha,
              "--manifest", str(manifest), "--manifest-sha"]
    if fault != "stale_first_registration":
        assert main(prefix + [sha]) == 0
        capsys.readouterr()
    if fault == "changed_floor":
        (tmp_path / "floor-source" / "sdk-0.bin").write_bytes(b"changed")
    elif fault == "changed_manifest":
        value = json.loads(manifest.read_bytes())
        value["timezone"] = "Europe/Oslo"
        manifest.write_bytes(encoded(value))
        sha = digest(read(manifest))
    else:
        class Later(datetime):
            @classmethod
            def now(cls, tz=None):
                return (datetime.now(UTC) + timedelta(seconds=61)).astimezone(tz)

        monkeypatch.setattr(rolling_span_source_proof, "datetime", Later)
    assert main(prefix + [sha]) == 2
    output = capsys.readouterr()
    assert "Stored reference span HOLD" in output.err

