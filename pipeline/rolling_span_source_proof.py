"""Read-only joins of actual raw delivery, producer and independent SDK proof."""
from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from pathlib import Path

from .history_canonical import key as raw_key
from .history_state import Stop, digest, encoded, read
from .night_products import calendar_day


def pin(reference, *, prefix=False):
    if (type(reference) is not dict or set(reference) != {"path", "bytes", "sha256"}
            or type(reference["bytes"]) is not int or not 0 < reference["bytes"] <= 16777216):
        raise Stop("rolling_span_source_proof_shape")
    with Path(reference["path"]).open("rb") as handle:
        body = handle.read(reference["bytes"] + (not prefix))
    if len(body) != reference["bytes"] or digest(body) != reference["sha256"]:
        raise Stop("rolling_span_source_proof_changed")
    return body


def value(reference):
    return json.loads(pin(reference))


def scope_digest(manifest):
    fields = ("job_plan_sha256", "storage_target_sha256", "today", "timezone", "upper_endpoint",
              "lower_endpoint", "scope_status", "parent_span_sha256")
    return digest(encoded({**{field: manifest[field] for field in fields},
                           "chunk_size": manifest.get("chunk_size", 7)}))


def _journal(reference, job):
    if Path(reference["path"]).resolve() != (job.folder / "events.jsonl").resolve():
        raise Stop("rolling_span_source_journal_binding")
    body = pin(reference, prefix=True)
    if not body.endswith(b"\n"):
        raise Stop("rolling_span_source_journal_changed")
    frames, previous = [], None
    for index, line in enumerate(body.splitlines(keepends=True)):
        frame = json.loads(line)
        if frame.get("sequence") != index or frame.get("previous") != previous or frame.get("plan") != job.plan_sha:
            raise Stop("rolling_span_source_journal_changed")
        frames.append(frame["event"])
        previous = digest(line)
    return frames


def _leaf(row, refs, *, allow_stopped):
    folder = Path(row["folder"]).resolve()
    source = json.loads(read(folder / "RECEIPT.json"))
    result = json.loads(read(folder / "RESULT.json"))
    receipt_body = read(folder / "RECEIPT.json")
    if (not any(Path(item["path"]).resolve() == folder / "RECEIPT.json"
                and item["sha256"] == digest(receipt_body) for item in refs)
            or result.get("receipt_sha256") != digest(receipt_body)
            or row.get("receipt_sha256") != digest(receipt_body)
            or result.get("native") != row.get("native")
            or source.get("intent_sha256") != digest(read(folder / "INTENT.json"))
            or source.get("cleanup_status") != "complete"):
        raise Stop("rolling_span_source_leaf_unverified")
    positive = next((item for item in source.get("targets", [])
        if item.get("date") == row["date"] and item.get("stream") == row["stream"]
        and item.get("status") == "ready" and item.get("source_verified") is True), None)
    if positive is None or row.get("source_verified") is not True:
        raise Stop("rolling_span_source_leaf_unverified")
    native = result.get("native")
    if not ((native == 0 and source.get("status") == "complete")
            or (allow_stopped and native == 2 and source.get("status") == "stopped")):
        raise Stop("rolling_span_source_leaf_native_unverified")
    for field in ("wire", "view"):
        path = (folder / positive[field]).resolve()
        if (not path.is_relative_to(folder) or row.get(field) != positive[field]
                or digest(read(path, 5242880)) != positive[field + "_sha256"]
                or row.get(field + "_sha256") != positive[field + "_sha256"]):
            raise Stop("rolling_span_source_leaf_changed")
    return {"source_native": native, "source_verified": True,
            "source_receipt_sha256": digest(receipt_body),
            "wire_sha256": positive["wire_sha256"], "view_sha256": positive["view_sha256"]}


def observed_raw_delivery(job, binding_ref, postcheck_ref, *, target_sha):
    """Return verified raw dates only; no authority, current-day or whole-data claim."""
    binding = value(binding_ref)
    binding_sha = digest(encoded(binding))
    if (Path(binding["job"]).resolve() != job.folder.resolve()
            or binding["plan"]["sha256"] != job.plan_sha
            or pin(binding["plan"]) != read(job.folder / "plan.json")
            or binding.get("targetSHA256") != target_sha
            or binding.get("accountSHA256") != job.plan["account_sha256"]):
        raise Stop("rolling_span_source_binding")
    catalog = value(binding["runtimeManifest"])
    contract = Path(binding["cwd"]).resolve() / "pipeline" / "history_canonical.py"
    contract_ref = next((item for item in catalog.get("files", []) if Path(item["path"]).resolve() == contract), None)
    if contract_ref is None or digest(pin(contract_ref)) != digest(Path(inspect.getfile(raw_key)).read_bytes()):
        raise Stop("rolling_span_source_key_contract")
    stages, seen, rows, identity = [postcheck_ref], set(), {}, None
    facts = []
    while stages:
        reference = stages.pop()
        if reference["sha256"] in seen:
            continue
        seen.add(reference["sha256"])
        if len(seen) > 64:
            raise Stop("rolling_span_source_stage_bound")
        postcheck = value(reference)
        scope_ref = postcheck["verificationPlan"]
        scope = value(scope_ref)
        context = scope["dailyContext"]
        if (postcheck.get("kind") != "ACTUAL_METERED_CURRENT_DAILY_POSTCHECK"
                or scope.get("kind") != "CURRENT_DAILY_POSTCHECK"
                or any(postcheck.get(flag) is not True for flag in
                       ("rawVerified", "publishedDestinationsVerified", "componentCleanupConfirmed"))
                or postcheck.get("bindingSHA256") != binding_sha or scope.get("bindingSHA256") != binding_sha
                or scope.get("invocation") != binding["invocation"]
                or postcheck.get("dailyContext") != context or postcheck.get("outcomeRefs") != scope.get("outcomeRefs")):
            raise Stop("rolling_span_raw_postcheck_unqualified")
        selection = context["selection"]
        current_identity = (context["round"], encoded(selection))
        if (selection.get("type") != "daily_round_started" or selection.get("round") != context["round"]
                or identity is not None and current_identity != identity):
            raise Stop("rolling_span_raw_selection_changed")
        identity = current_identity
        new_dates, stored_dates = selection.get("new_dates"), selection.get("stored_dates")
        if (not isinstance(new_dates, list) or not 1 <= len(new_dates) <= 30
                or len(set(new_dates)) != len(new_dates) or not isinstance(stored_dates, list)
                or set(new_dates) & set(stored_dates) or not set(new_dates) <= set(selection.get("dates", []))):
            raise Stop("rolling_span_raw_selection_changed")
        for day in new_dates:
            calendar_day(day)
        refs = scope["outcomeRefs"]
        journal = next((item for item in refs if Path(item["path"]).resolve() == job.folder / "events.jsonl"), None)
        if journal is None:
            raise Stop("rolling_span_source_journal_binding")
        events = _journal(journal, job)
        if selection not in events:
            raise Stop("rolling_span_raw_selection_changed")
        for item in refs:
            if item != journal:
                pin(item)
        source_rows = {}
        for event in events:
            if event.get("type") == "target":
                identifier = (event.get("date"), event.get("stream"))
                source_rows[identifier] = {**source_rows.get(identifier, {}), **event}
        terminal = value(scope["terminalEvidence"])
        if terminal.get("allNativeDescendantsTerminal") is not True:
            raise Stop("rolling_span_source_terminal_unknown")
        allow_stopped = context.get("sourceFreeTerminal") is not None
        if allow_stopped:
            free = value(context["sourceFreeTerminal"])
            if free.get("actualNative") != 0 or free.get("allNativeDescendantsTerminal") is not True:
                raise Stop("rolling_span_source_terminal_unknown")
        facts.append({"invocation": scope["invocation"], "round": context["round"],
                      "source_episode_native": context.get("actualNative"),
                      "source_failure": context.get("sourceFailure"), "raw_verified": True,
                      "whole_data_verified": postcheck.get("wholeDataVerified", postcheck.get("dataVerified"))})
        for receipt_ref in context["producerReceipts"]:
            if receipt_ref not in refs:
                raise Stop("rolling_span_raw_producer_unbound")
            receipt = value(receipt_ref)
            remote_path = Path(receipt_ref["path"]).parent / "REMOTE-VERIFIED.json"
            remote = json.loads(read(remote_path))
            if (receipt.get("status") != "complete" or receipt.get("plan_sha256") != job.plan_sha
                    or receipt.get("target_sha256") != target_sha
                    or receipt.get("baseline_refresh", {}).get("execution_totals", {}).get("round") != context["round"]
                    or remote.get("status") != "remote_verified_cleanup_pending"
                    or {key: item for key, item in remote.items() if key != "status"}
                    != {key: item for key, item in receipt.items() if key != "status"}):
                raise Stop("rolling_span_raw_producer_unverified")
            for destination in receipt.get("destinations", []):
                day, stream, key = destination.get("date"), destination.get("stream"), destination.get("key")
                if day not in new_dates or stream not in {"hr", "hrv"} or key != raw_key(stream, day):
                    continue
                checks = [item for item in scope["checks"] if item.get("key") == key and item.get("observedReference") is False]
                if not checks:
                    continue  # Earlier stages can own this exact destination's ACK.
                check = checks[0]
                snapshot = destination["snapshot"]
                if (len(checks) != 1 or any(check.get(field) != destination.get(field) for field in ("revision", "sha256"))
                        or check.get("contentType") != "application/json" or check.get("encoding") != "gzip"
                        or snapshot.get("content_type") != "application/json" or snapshot.get("encoding") != "gzip"):
                    raise Stop("rolling_span_raw_scope_mismatch")
                readbacks = [item for item in postcheck["readbacks"] if item.get("keySHA256") == digest(key.encode())]
                if len(readbacks) != 1:
                    raise Stop("rolling_span_raw_sdk_unverified")
                body = pin(readbacks[0]["body"])
                metadata = {"revision": destination["revision"], "content_type": "application/json", "encoding": "gzip"}
                source_snapshot = (Path(receipt_ref["path"]).parent / snapshot["file"]).resolve()
                if (source_snapshot.parent != Path(receipt_ref["path"]).parent.resolve()
                        or digest(body) != destination["sha256"] or len(body) != snapshot["bytes"]
                        or read(source_snapshot, 262144) != body
                        or readbacks[0].get("metadataSHA256") != digest(encoded(metadata))):
                    raise Stop("rolling_span_raw_sdk_changed")
                source_row = source_rows.get((day, stream))
                if source_row is None or not Path(source_row["folder"]).resolve().is_relative_to(job.folder / "runs"):
                    raise Stop("rolling_span_source_leaf_unverified")
                leaf = _leaf(source_row, refs, allow_stopped=allow_stopped)
                rows[(key, destination["revision"], destination["sha256"])] = {
                    "date": day, "stream": stream, "key": key, "revision": destination["revision"],
                    "sha256": destination["sha256"], "publisher_receipt": receipt_ref, "postcheck": reference, **leaf}
        for prior in context.get("priorVerifications", []):
            old = value(prior["postcheck"])
            if old.get("verificationPlan") != prior["scope"]:
                raise Stop("rolling_span_raw_prior_stage_changed")
            stages.append(prior["postcheck"])
    if not rows:
        raise Stop("rolling_span_raw_floor_empty")
    dates = sorted({item["date"] for item in rows.values()})
    return {"oldest_endpoint": dates[0], "verified_raw_dates": dates, "destinations": list(rows.values()),
            "stage_facts": facts, "source_binding_sha256": binding_sha, "invocation": binding["invocation"],
            "round": identity[0]}


def qualify_raw_floor(job, plan_sha, source_ref, *, manifest, fresh=False):
    source = value(source_ref)
    if (source.get("schema") != 2 or source.get("kind") != "rolling-span-verified-raw-floor"
            or source.get("job_plan_sha256") != plan_sha or plan_sha != job.plan_sha
            or source.get("storage_target_sha256") != manifest["storage_target_sha256"]):
        raise Stop("rolling_span_floor_unverified")
    result = observed_raw_delivery(job, source["binding"], source["postcheck"], target_sha=manifest["storage_target_sha256"])
    if source.get("oldest_endpoint") != result["oldest_endpoint"] or manifest["lower_endpoint"] != result["oldest_endpoint"]:
        raise Stop("rolling_span_floor_not_observed")
    qualification = value(source["qualification"])
    if (qualification.get("kind") != "ROOT_ACTUAL_ROLLING_SPAN_FLOOR_QUALIFICATION_V1"
            or qualification.get("today") != manifest["today"]
            or qualification.get("source_observed_day") != source.get("observed_day")
            or qualification.get("scope_sha256") != scope_digest(manifest)
            or qualification.get("source_binding_sha256") != result["source_binding_sha256"]
            or qualification.get("postcheck") != source["postcheck"]):
        raise Stop("rolling_span_floor_qualification")
    calendar_day(source["observed_day"])
    if source["observed_day"] != manifest["today"] and manifest["scope_status"] != "provisional":
        raise Stop("rolling_span_floor_not_current_day")
    port = value(qualification["permission"])
    if port.get("kind") != "ACTUAL_CURRENT_PRODUCT_PORT" or port.get("outcome") != "allowed":
        raise Stop("rolling_span_floor_qualification")
    if fresh:
        instant = datetime.fromisoformat(port["observedUTC"].replace("Z", "+00:00"))
        age = (datetime.now(UTC) - instant).total_seconds()
        if instant.tzinfo is None or not 0 <= age <= 60:
            raise Stop("rolling_span_floor_qualification_stale")
    if source.get("source_curve_index") is not None:
        pin(source["source_curve_index"])
    return {**result, "observed_day": source["observed_day"], "current_day_delivery": source["observed_day"] == manifest["today"]}
