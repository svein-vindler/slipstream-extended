"""Read-only joins of actual raw delivery, producer and independent SDK proof."""
from __future__ import annotations

import inspect
import json
import math
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict

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
    if (not all(any(Path(item["path"]).resolve() == folder / name
                and item["sha256"] == digest(read(folder / name)) for item in refs)
                for name in ("INTENT.json", "RESULT.json", "RECEIPT.json"))
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


class KnownFirstemptyPartial(TypedDict):
    """Verified positive delivery with the original unresolved Source stop retained."""

    schema: Literal[1]
    kind: Literal["DAILY_KNOWN_FIRSTEMPTY_PARTIAL_V1"]
    round: str
    selection_sha256: str
    selected_dates: list[str]
    attempted_dates: list[str]
    unattempted_dates: list[str]
    unresolved_targets: list[dict]
    published_targets: list[dict]
    source_actual_native: Literal[2]
    source_failure: Literal[True]
    source_status: dict
    source_terminal_ref: dict
    source_outcome_ref: dict


def _known_firstempty(job, context, scope, events, binding_sha) -> KnownFirstemptyPartial | None:
    declaration = context.get("knownFirstempty")
    if declaration is None:
        return None
    def require(condition):
        if not condition:
            raise Stop("rolling_span_known_firstempty_unqualified")

    require(type(declaration) is dict and set(declaration) == {
        "schema", "kind", "sourceStatus", "sourceNativeTerminal", "sourceOutcome"}
        and declaration["schema"] == 1 and declaration["kind"] == "DAILY_KNOWN_FIRSTEMPTY_RAW_V1"
        and context.get("actualNative") == 2 and context.get("sourceFailure") is True
        and context.get("sourceFreeTerminal") is None)
    terminal, outcome = value(declaration["sourceNativeTerminal"]), value(declaration["sourceOutcome"])
    status, selection = declaration["sourceStatus"], context["selection"]
    require(declaration["sourceNativeTerminal"] == scope["terminalEvidence"]
        and type(terminal.get("actualNative")) is int and terminal["actualNative"] == 2
        and terminal.get("allNativeDescendantsTerminal") is True
        and terminal.get("bindingSHA256") == binding_sha
        and outcome.get("bindingSHA256") == binding_sha and outcome.get("actualNative") == 2
        and outcome.get("sourceFailed") is True and outcome.get("successClaimed") is False
        and outcome.get("journalReceipts") == scope["outcomeRefs"]
        and outcome.get("normalCLIStatus") == status
        and status.get("daily_status") == "source_stopped" and status.get("daily_source_stop") == "empty_unresolved"
        and status.get("daily_round") == context["round"]
        and status.get("locked") is False and status.get("active_run") is None)
    # Admission is storage-only: a genuine Source pause remains in force.
    job.admission(job.plan_sha, source=False)
    starts = [index for index, event in enumerate(events) if event == selection]
    require(len(starts) == 1)
    current = events[starts[0] + 1:]
    latest, positive, reserved, closed = {}, set(), set(), set()
    for event in current:
        kind = event.get("type")
        if kind == "end":
            require(event.get("status") in {"bounded_complete", "empty_unresolved", "published"})
        if kind in {"resume", "daily_round_started", "daily_round_finished"}:
            require(False)
        if kind == "date_reserved":
            require(event["id"] not in reserved)
            reserved.add(event["id"])
        if kind == "date_closed":
            require(event["id"] in reserved and event["id"] not in closed)
            closed.add(event["id"])
        if kind == "target":
            identifier = (event.get("date"), event.get("stream"))
            require(identifier[0] in selection["new_dates"])
            latest[identifier] = {**latest.get(identifier, {}), **event}
            if event.get("status") == "ready":
                positive.add(identifier)
    require(reserved == closed and any(event.get("type") == "end"
        and event.get("status") == "empty_unresolved" for event in current))
    published = {key for key, row in latest.items() if row.get("status") == "published"}
    unresolved = [{"date": day, "stream": stream, "status": "empty_unresolved"}
        for (day, stream), row in sorted(latest.items()) if row.get("status") == "empty_unresolved"]
    require(positive == published and positive and len(unresolved) == 1
        and all(row.get("status") in {"published", "empty_unresolved"} for row in latest.values()))
    attempted = {day for day, _ in latest}
    require(attempted == {identifier.rsplit(":", 1)[-1] for identifier in closed}
        and type(status.get("daily_new_dates")) is int and status["daily_new_dates"] == len(attempted)
        and set(latest) == {(row["date"], row["stream"]) for row in job.plan["targets"] if row["date"] in attempted})
    empty = latest[(unresolved[0]["date"], unresolved[0]["stream"])]
    folder = Path(empty["folder"]).resolve()
    require(folder.is_relative_to(job.folder / "runs"))
    receipt, result = json.loads(read(folder / "RECEIPT.json")), json.loads(read(folder / "RESULT.json"))
    require(all(any(Path(item["path"]).resolve() == folder / name
        and item["sha256"] == digest(read(folder / name)) for item in scope["outcomeRefs"])
        for name in ("INTENT.json", "RESULT.json", "RECEIPT.json"))
        and receipt.get("cleanup_status") == "complete" and receipt.get("status") in {"complete", "stopped"}
        and receipt.get("intent_sha256") == digest(read(folder / "INTENT.json"))
        and result.get("native") == empty.get("native") and type(result.get("native")) is int
        and result["native"] in {0, 2}
        and result.get("receipt_sha256") == empty.get("receipt_sha256") == digest(read(folder / "RECEIPT.json"))
        and any({name: row.get(name) for name in ("date", "stream", "status")} == unresolved[0]
            for row in receipt.get("targets", [])))
    return {"schema": 1, "kind": "DAILY_KNOWN_FIRSTEMPTY_PARTIAL_V1", "round": context["round"],
        "selection_sha256": digest(encoded(selection)), "selected_dates": selection["new_dates"],
        "attempted_dates": [day for day in selection["new_dates"] if day in attempted],
        "unattempted_dates": [day for day in selection["new_dates"] if day not in attempted],
        "unresolved_targets": unresolved, "published_targets": [], "source_actual_native": 2,
        "source_failure": True, "source_status": status, "source_terminal_ref": declaration["sourceNativeTerminal"],
        "source_outcome_ref": declaration["sourceOutcome"]}


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
    facts, partials = [], []
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
        partial = _known_firstempty(job, context, scope, events, binding_sha)
        if partial is not None:
            partials.append(partial)
        allow_stopped = context.get("sourceFreeTerminal") is not None or partial is not None
        if context.get("sourceFreeTerminal") is not None:
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
    result = {"oldest_endpoint": dates[0], "verified_raw_dates": dates, "destinations": list(rows.values()),
            "stage_facts": facts, "source_binding_sha256": binding_sha, "invocation": binding["invocation"],
            "round": identity[0]}
    if partials:
        if len(partials) != len(facts) or any(item != partials[0] for item in partials):
            raise Stop("rolling_span_known_firstempty_changed")
        partial = partials[0]
        expected = {(row["date"], row["stream"]) for row in job.plan["targets"]
            if row["date"] in partial["attempted_dates"]} - {
            (row["date"], row["stream"]) for row in partial["unresolved_targets"]}
        if {(row["date"], row["stream"]) for row in rows.values()} != expected or len(rows) != len(expected):
            raise Stop("rolling_span_known_firstempty_delivery_unverified")
        partial["published_targets"] = [{name: row[name] for name in ("date", "stream", "key", "revision", "sha256")}
            for row in sorted(rows.values(), key=lambda item: (item["date"], item["stream"]))]
        result["known_firstempty_partial"] = partial
    return result


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


def _ordinary_pin(reference):
    if (type(reference) is not dict or set(reference) != {"path", "bytes", "sha256"}
            or not isinstance(reference["path"], str)
            or not isinstance(reference["sha256"], str)
            or re.fullmatch(r"[a-f0-9]{64}", reference["sha256"]) is None):
        raise Stop("rolling_span_ordinary_admission_unverified")
    path = Path(reference["path"])
    if not path.is_absolute() or path.is_symlink() or path.is_junction():
        raise Stop("rolling_span_ordinary_admission_unverified")
    return pin(reference)


def qualify_ordinary_admission(job, plan_sha, manifest_ref, admission_ref):
    """Validate the retained native register intent, never renew floor evidence."""
    intent = json.loads(_ordinary_pin(admission_ref))
    stage_ref = intent["stagePlan"]
    stage = json.loads(_ordinary_pin(stage_ref))
    index = stage.get("index")
    stage_path, intent_path = Path(stage_ref["path"]), Path(admission_ref["path"])
    manifest = json.loads(_ordinary_pin(manifest_ref))
    plan_body = read(job.folder / "plan.json")
    plan_ref = {"path": str((job.folder / "plan.json").resolve()),
                "bytes": len(plan_body), "sha256": digest(plan_body)}
    binding, caller, invocation = (stage.get(field) for field in
                                    ("bindingSHA256", "callerPID", "invocation"))
    if (stage.get("kind") != "KNOWN_ORDINARY_SPAN_STAGE_V1" or stage.get("action") != "register"
            or type(index) is not int or not 0 <= index < 8192
            or stage_path.name != f"SPAN-{index:04d}-PLAN.json"
            or intent_path != stage_path.with_name(f"SPAN-{index:04d}-INTENT.json")
            or stage.get("job") != str(job.folder) or stage.get("plan") != plan_ref
            or plan_ref["sha256"] != plan_sha or plan_sha != job.plan_sha
            or stage.get("spanManifest") != manifest_ref
            or stage.get("span_sha256") != manifest_ref["sha256"]
            or stage.get("targetSHA256") != manifest.get("storage_target_sha256")
            or manifest.get("job_plan_sha256") != plan_sha
            or not isinstance(binding, str) or re.fullmatch(r"[a-f0-9]{64}", binding) is None
            or type(caller) is not int or caller <= 0 or not isinstance(invocation, str) or not invocation
            or intent.get("bindingSHA256") != binding
            or any(type(intent.get(field)) is not int or intent[field] != 0
                   for field in ("sourceCalls", "sourceReservations", "archiveReloads"))):
        raise Stop("rolling_span_ordinary_admission_binding")
    started, deadline = stage.get("startedMonotonic"), stage.get("deadlineMonotonic")
    if (type(started) not in (int, float) or type(deadline) not in (int, float)
            or not math.isfinite(started) or not math.isfinite(deadline)
            or not 0 <= deadline - started <= 21600 or not started <= time.monotonic() < deadline):
        raise Stop("rolling_span_ordinary_admission_deadline")
    _ordinary_pin(stage["interpreter"])
    _ordinary_pin(stage["spanCliModule"])
    argv = [stage["interpreter"]["path"], "-B", "-X", "utf8", stage["spanCliModule"]["path"],
            "--stage-proof", stage_ref["path"], "--stage-sha256", stage_ref["sha256"]]
    command = digest(encoded(argv))
    if intent.get("argv") != argv or intent.get("commandSHA256") != command:
        raise Stop("rolling_span_ordinary_admission_command")
    # Only the already guarded native entry may use this TTL exception.
    # public_main(command) preserves that entry's actual process argv; a
    # direct history CLI cannot substitute mutually consistent supplied files.
    if (sys.argv != argv[4:]
            or Path(sys.executable).resolve() != Path(stage["interpreter"]["path"]).resolve()):
        raise Stop("rolling_span_ordinary_admission_entry")
    port = json.loads(_ordinary_pin(intent["permission"]))
    if (port.get("kind") != "ACTUAL_CURRENT_PRODUCT_PORT" or port.get("outcome") != "allowed"
            or port.get("phase") != "data" or port.get("bindingSHA256") != binding
            or type(port.get("callerPID")) is not int or port["callerPID"] != caller
            or port.get("invocation") != invocation or port.get("commandSHA256") != command):
        raise Stop("rolling_span_ordinary_admission_permission")
    instant = datetime.fromisoformat(port["observedUTC"].replace("Z", "+00:00"))
    if instant.tzinfo is None or not 0 <= (datetime.now(UTC) - instant).total_seconds() <= 60:
        raise Stop("rolling_span_ordinary_admission_stale")
    return admission_ref
