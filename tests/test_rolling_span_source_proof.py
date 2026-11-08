"""Raw floor joins at the immutable filesystem/SDK-observation boundary."""
import gzip
import inspect
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline.history_canonical import key as raw_key
from pipeline.history_state import Job, Stop, digest, encoded, read, write
from pipeline.rolling_span_source_proof import qualify_raw_floor, scope_digest


def ref(path):
    body = read(path)
    return {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}


def raw_floor_fixture(job, folder, manifest, *, delivered=None, new_dates=None, stored_dates=None):
    """Synthetic prior SOURCE evidence, outside the stored-only reference actor."""
    folder.mkdir(exist_ok=True)
    lower = manifest["lower_endpoint"]
    new_dates = new_dates or [lower]
    stored_dates = stored_dates or []
    delivered = delivered or [(lower, "hrv")]
    selection = {"type": "daily_round_started", "round": "c" * 32,
        "dates": new_dates + stored_dates, "new_dates": new_dates, "stored_dates": stored_dates,
        "finalize_dates": new_dates + stored_dates, "max_dates": 30, "max_http": 240}
    job.append(selection)
    source_refs = []
    for day, stream in delivered:
        root = job.folder / "runs" / digest((day + stream).encode())[:32] / day
        root.mkdir(parents=True)
        intent = write(root / "INTENT.json", {"date": day, "streams": [stream], "plan": job.plan_sha})
        wire, view = encoded({"source": "synthetic-wire", "date": day}), encoded({"source": "synthetic-view", "date": day})
        write(root / "wire.json", wire)
        write(root / "view.json", view)
        row = {"date": day, "stream": stream, "status": "ready", "source_verified": True,
            "wire": "wire.json", "view": "view.json", "wire_sha256": digest(wire), "view_sha256": digest(view)}
        source_sha = write(root / "RECEIPT.json", {"status": "complete", "cleanup_status": "complete",
            "intent_sha256": intent, "targets": [row]})
        write(root / "RESULT.json", {"native": 0, "receipt_sha256": source_sha})
        source_refs.extend(ref(root / name) for name in ("INTENT.json", "RECEIPT.json", "RESULT.json"))
        job.append({"type": "target", **row, "folder": str(root), "receipt_sha256": source_sha, "native": 0})
    run = digest(str(folder).encode())[:32]
    published = job.folder / "runs" / run
    published.mkdir()
    job.append({"type": "begin", "run": run})
    destinations, checks, readbacks = [], [], []
    for index, (day, stream) in enumerate(delivered):
        body = gzip.compress(encoded({"date": day, "canonical": True}), mtime=0)
        snapshot_file = f"source-postimage-{index:03d}.bin"
        write(published / snapshot_file, body)
        sdk_file = folder / f"sdk-{index}.bin"
        write(sdk_file, body)
        key, revision = raw_key(stream, day), f'"synthetic-{index}"'
        destinations.append({"date": day, "stream": stream, "key": key, "revision": revision,
            "sha256": digest(body), "snapshot": {"file": snapshot_file, "bytes": len(body),
                "content_type": "application/json", "encoding": "gzip"}})
        checks.append({"key": key, "revision": revision, "sha256": digest(body),
                       "contentType": "application/json", "encoding": "gzip", "observedReference": False})
        readbacks.append({"keySHA256": digest(key.encode()), "body": ref(sdk_file),
            "metadataSHA256": digest(encoded({"revision": revision, "content_type": "application/json", "encoding": "gzip"}))})
        job.append({"type": "target", "date": day, "stream": stream, "status": "published",
                    "destination_key": key, "revision": revision, "sha256": digest(body)})
    totals = {"counts": {"get": 1, "list": 0, "put": 1}, "body_bytes": sum(item["snapshot"]["bytes"] for item in destinations),
        "phases": 1, "round": selection["round"], "publication_get_limit": 6000, "independent_gets": 0,
        "round_baseline": {"counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0}}
    report = {"status": "bounded_pending", "execution_totals": totals,
              "finalize_dates": sorted(selection["finalize_dates"]), "cohort_complete": False}
    receipt = {"status": "complete", "run": run, "plan_sha256": job.plan_sha,
        "target_sha256": manifest["storage_target_sha256"], "source_calls": 0, "counts": totals["counts"],
        "body_bytes": totals["body_bytes"], "baseline_refresh": report, "destinations": destinations}
    write(published / "RECEIPT.json", receipt)
    write(published / "REMOTE-VERIFIED.json", {**receipt, "status": "remote_verified_cleanup_pending"})
    job.append({"type": "reference_refresh", "report": report})
    job.append({"type": "end", "run": run, "status": "published"})
    contract = Path(inspect.getfile(raw_key)).resolve()
    catalog = folder / "runtime.json"
    write(catalog, {"kind": "EXACT_FILE_CATALOG", "files": [ref(contract)]})
    binding = {"job": str(job.folder), "plan": ref(job.folder / "plan.json"),
        "accountSHA256": job.plan["account_sha256"], "targetSHA256": manifest["storage_target_sha256"],
        "invocation": "synthetic-source-current", "cwd": str(contract.parents[1]), "runtimeManifest": ref(catalog)}
    binding_path = folder / "binding.json"
    write(binding_path, binding)
    binding_sha = digest(encoded(binding))
    terminal = folder / "terminal.json"
    write(terminal, {"actualNative": 0, "allNativeDescendantsTerminal": True})
    outcome_refs = [ref(job.folder / "events.jsonl"), ref(job.folder / "plan.json"), *source_refs, ref(published / "RECEIPT.json")]
    context = {"round": selection["round"], "selection": selection, "producerReceipts": [ref(published / "RECEIPT.json")],
               "actualNative": 0, "sourceFailure": False}
    scope = folder / "scope.json"
    write(scope, {"kind": "CURRENT_DAILY_POSTCHECK", "bindingSHA256": binding_sha,
        "invocation": binding["invocation"], "dailyContext": context, "outcomeRefs": outcome_refs,
        "terminalEvidence": ref(terminal), "checks": checks})
    postcheck = folder / "postcheck.json"
    write(postcheck, {"kind": "ACTUAL_METERED_CURRENT_DAILY_POSTCHECK", "bindingSHA256": binding_sha,
        "verificationPlan": ref(scope), "dailyContext": context, "outcomeRefs": outcome_refs,
        "rawVerified": True, "publishedDestinationsVerified": True, "componentCleanupConfirmed": True,
        "wholeDataVerified": False, "dataVerified": False, "readbacks": readbacks})
    permission = folder / "permission.json"
    write(permission, {"kind": "ACTUAL_CURRENT_PRODUCT_PORT", "phase": "data", "outcome": "allowed",
                       "observedUTC": datetime.now(UTC).isoformat(), "callerPID": os.getpid()})
    qualification = folder / "qualification.json"
    write(qualification, {"kind": "ROOT_ACTUAL_ROLLING_SPAN_FLOOR_QUALIFICATION_V1", "today": manifest["today"],
        "source_observed_day": manifest["today"], "scope_sha256": scope_digest(manifest),
        "source_binding_sha256": binding_sha, "postcheck": ref(postcheck), "permission": ref(permission)})
    wrapper = folder / "floor.json"
    write(wrapper, {"schema": 2, "kind": "rolling-span-verified-raw-floor", "job_plan_sha256": job.plan_sha,
        "storage_target_sha256": manifest["storage_target_sha256"], "oldest_endpoint": lower,
        "observed_day": manifest["today"], "binding": ref(binding_path), "postcheck": ref(postcheck),
        "qualification": ref(qualification)})
    return ref(wrapper)


def test_floor_uses_only_delivered_new_dates_not_planned_or_reused_dates(tmp_path):
    job = Job.create(tmp_path / "job", "2026-10-22", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest = {"job_plan_sha256": job.plan_sha, "storage_target_sha256": "a" * 64,
        "today": "2026-11-08", "timezone": "UTC", "upper_endpoint": "2026-10-25",
        "lower_endpoint": "2026-10-24", "scope_status": "final", "parent_span_sha256": None}
    proof = raw_floor_fixture(job, tmp_path / "proof", manifest,
        delivered=[("2026-10-24", "hrv")], new_dates=["2026-10-25", "2026-10-24", "2026-10-23"],
        stored_dates=["2026-10-22"])
    result = qualify_raw_floor(job, job.plan_sha, proof, manifest=manifest, fresh=True)
    assert result["oldest_endpoint"] == "2026-10-24" and result["verified_raw_dates"] == ["2026-10-24"]
    assert result["destinations"][0]["wire_sha256"] != result["destinations"][0]["sha256"]
    assert result["stage_facts"][0]["whole_data_verified"] is False


@pytest.mark.parametrize("fault", ["planned_floor", "historical_final", "raw_flag", "sdk_bytes", "scope_context"])
def test_changed_or_authority_only_floor_is_rejected(tmp_path, fault):
    job = Job.create(tmp_path / "job", "2026-10-23", "2026-10-25", ["hrv"], "legacy", "synthetic")
    manifest = {"job_plan_sha256": job.plan_sha, "storage_target_sha256": "a" * 64,
        "today": "2026-11-08", "timezone": "UTC", "upper_endpoint": "2026-10-25",
        "lower_endpoint": "2026-10-24", "scope_status": "final", "parent_span_sha256": None}
    proof = raw_floor_fixture(job, tmp_path / "proof", manifest, new_dates=["2026-10-25", "2026-10-24", "2026-10-23"])
    if fault == "planned_floor":
        manifest["lower_endpoint"] = "2026-10-23"
    elif fault == "historical_final":
        data = json.loads(read(Path(proof["path"])))
        data["observed_day"] = "2026-11-07"
        Path(proof["path"]).write_bytes(encoded(data))
        proof = ref(Path(proof["path"]))
    elif fault == "sdk_bytes":
        (tmp_path / "proof" / "sdk-0.bin").write_bytes(b"changed")
    else:
        path = tmp_path / "proof" / ("postcheck.json" if fault == "raw_flag" else "scope.json")
        data = json.loads(read(path))
        if fault == "raw_flag":
            data["rawVerified"] = False
        else:
            data["dailyContext"]["selection"]["new_dates"] = ["2026-10-23"]
        path.write_bytes(encoded(data))
    with pytest.raises(Stop):
        qualify_raw_floor(job, job.plan_sha, proof, manifest=manifest, fresh=True)
