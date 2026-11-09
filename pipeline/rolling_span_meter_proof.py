"""Read-only replay of the public source-linked independent-read proof codec."""
import json
from decimal import Decimal
from pathlib import Path

from .history_state import Stop, digest, encoded


def _need(condition):
    if not condition:
        raise Stop("rolling_span_meter_link_changed")


def _pin(reference, *, prefix=False):
    _need(isinstance(reference, dict) and set(reference) == {"path", "bytes", "sha256"}
          and type(reference["bytes"]) is int and 0 < reference["bytes"] <= 16777216)
    with Path(reference["path"]).open("rb") as handle:
        body = handle.read(reference["bytes"] + (not prefix))
    _need(len(body) == reference["bytes"] and digest(body) == reference["sha256"])
    return body


def _json(reference):
    return json.loads(_pin(reference))


def _release_identity(binding):
    fields = ("main", "tree", "ciRunID", "runtimeManifest", "environmentManifest",
              "interpreter", "job", "plan", "operation", "accountSHA256", "targetSHA256")
    return digest(encoded({key: binding[key] for key in fields}))


def _file_pin(reference):
    _need(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}
          and type(reference["bytes"]) is int and reference["bytes"] >= 0)
    import hashlib

    measured, hasher = 0, hashlib.sha256()
    with Path(reference["path"]).open("rb") as handle:
        while block := handle.read(1048576):
            measured += len(block)
            _need(measured <= reference["bytes"])
            hasher.update(block)
    _need(measured == reference["bytes"] and hasher.hexdigest() == reference["sha256"])


def _released(binding, *, operation="rolling_span"):
    release = _json(binding["releaseEvidence"])
    _need(binding.get("ready") is True and binding.get("operation") == operation
          and release.get("kind") == "ROOT_MERGED_RUNTIME_RELEASE" and release.get("reviewClosed") is True
          and all(release.get(key) == binding[key] for key in ("main", "tree", "ciRunID")))
    for field in ("runtimeManifest", "environmentManifest", "interpreter"):
        _file_pin(binding[field])
        _need(release.get(field + "SHA256") == binding[field]["sha256"])
        if field.endswith("Manifest"):
            catalog = _json(binding[field])
            _need(catalog.get("kind") == "EXACT_FILE_CATALOG" and catalog.get("files"))
            for reference in catalog["files"]:
                _file_pin(reference)
    _pin(binding["plan"])
    for field, reference in binding.items():
        if field.endswith("Module"):
            _file_pin(reference)
            _need(release.get(field + "SHA256") == reference["sha256"])
    if operation == "rolling_span":
        _pin(binding["spanManifest"])


def _workflow_states(rows):
    _need(type(rows) is list and len(rows) == 14 and len({row["id"] for row in rows}) == 14
          and all(type(row.get("id")) is int and isinstance(row.get("path"), str)
                  and isinstance(row.get("state"), str) for row in rows))
    return sorted((row["id"], row["path"], row["state"]) for row in rows)


def _checkpoint(reference, binding):
    path = Path(binding["job"]).resolve() / "rolling-reference-spans" / binding["spanManifest"]["sha256"]
    _need(Path(reference["path"]).resolve() == path / "CHECKPOINTS.jsonl")
    body = _pin(reference, prefix=True)
    previous, state = None, None
    _need(body.endswith(b"\n"))
    for sequence, line in enumerate(body.splitlines(keepends=True)):
        frame = json.loads(line)
        _need(frame.get("schema") == 1 and frame.get("sequence") == sequence
              and frame.get("previous") == previous
              and frame.get("span_sha256") == binding["spanManifest"]["sha256"]
              and Path(frame["state_ref"]["path"]).resolve().parent == path / "checkpoints")
        state, previous = _json(frame["state_ref"]), digest(line)
    _need(state is not None)
    return body, state


def _meter_state(state):
    return {key: state[key] for key in ("count", "round", "baseline", "initial", "span_gets",
        "source_pool_gets", "separate_epoch_gets", "sequence", "previous")}


def _semantic_load(body):
    def pairs(items):
        result = {}
        for key, value in items:
            _need(key not in result)
            result[key] = value
        return result
    try:
        return json.loads(body, parse_float=Decimal, object_pairs_hook=pairs,
                          parse_constant=lambda _: _need(False))
    except (ValueError, UnicodeError, RecursionError):
        _need(False)


def _semantic_same(left, right):
    if type(left) is bool or type(right) is bool:
        return type(left) is type(right) and left == right
    if isinstance(left, (int, Decimal)) and isinstance(right, (int, Decimal)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(_semantic_same(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(_semantic_same(a, b) for a, b in zip(left, right, strict=True))
    return left == right


def _reader_result(body, receipt):
    if receipt.get("response_format") == "json":
        envelope = _semantic_load(body)
    else:
        _need(receipt.get("response_format") == "sse")
        finals = []
        for event in body.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
            data = "\n".join(line[5:].lstrip(" ") for line in event.splitlines() if line.startswith("data:"))
            if data:
                message = _semantic_load(data)
                if "id" in message:
                    finals.append(message)
                else:
                    _need(message.get("method") == "notifications/progress")
        _need(len(finals) == 1)
        envelope = finals[0]
    _need(isinstance(envelope, dict))
    if "jsonrpc" in envelope:
        _need(envelope.get("jsonrpc") == "2.0" and "error" not in envelope)
        envelope = envelope["result"]
    _need(envelope.get("isError", False) is False)
    texts = [_semantic_load(item["text"]) for item in envelope.get("content", []) if item.get("type") == "text"]
    _need(len(texts) <= 1)
    structured = envelope.get("structuredContent")
    _need(structured is not None or len(texts) == 1)
    if structured is not None and texts:
        _need(_semantic_same(structured, texts[0]))
    return structured if structured is not None else texts[0]


def _accepted_postchecks(proof, old, stop, known, prepared):
    from .history_state import read
    from .rolling_reference_span import _qualified_postcheck

    body, _ = _checkpoint(proof["checkpointBefore"], old)
    accepted, tips = {}, set()
    for line in body.splitlines(keepends=True):
        frame = json.loads(line)
        tips.add(digest(line))
        state = _json(frame["state_ref"])
        reference = state.get("last_ack_ref")
        if reference is not None:
            ack = _json(reference)
            _need(ack.get("kind") == "VERIFIED_STORED_SPAN_PHASE_V1")
            accepted[ack["postcheck"]["sha256"]] = ack
    references = [row["ref"] for row in stop["postchecks"]]
    _need(references and references == known.get("verifiedPostchecks") == prepared.get("postchecks")
          and len({item["sha256"] for item in references}) == len(references))
    for row in stop["postchecks"]:
        postcheck = _json(row["ref"])
        ack = accepted.get(row["ref"]["sha256"])
        targets = postcheck.get("generated_targets", [])
        _need(ack is not None and ack["postcheck"] == row["ref"] and ack["checkpoint_sha256"] in tips
              and ack["span_sha256"] == old["spanManifest"]["sha256"]
              and postcheck.get("bindingSHA256") == digest(encoded(old))
              and row.get("products") == len(targets) and row.get("planningOnly") is (not targets))
        _qualified_postcheck(ack, targets, old["targetSHA256"])
        if not targets:
            continue
        readbacks, readers = postcheck["readbacks"], postcheck["readerObservations"]
        _need(postcheck.get("sdkAndNormalReaderVerified") is True
              and len(readbacks) == len(readers) == len(targets))
        products = {item["sha256"]: _semantic_load(_pin(next(row["body"] for row in readbacks
                    if row.get("keySHA256") == digest(item["key"].encode())))) for item in targets}
        body_sizes = {row["body"]["sha256"]: row["body"]["bytes"] for row in readbacks}
        seen = set()
        for reference in readers:
            observation = _semantic_load(_pin(reference))
            path = Path(reference["path"])
            stem = path.name.removesuffix(".observation.json")
            intent = _semantic_load(read(path.with_name(stem + ".intent.json")))
            receipt = _semantic_load(read(path.with_name(stem + ".execution-receipt.json")))
            response = read(path.with_name(stem + ".response"))
            _need(observation.get("status") == "matched" and observation.get("reason") is None
                  and len(intent["sdk_product_sha256"]) == 1
                  and intent.get("tool") == old["normalReader"]["tool"]
                      == "mcp__codex_apps__slipstream_oauth_night_baseline"
                  and encoded(intent["binding"]) == encoded(old["normalReader"]["binding"])
                  and receipt.get("schema_version") == 2
                  and receipt.get("kind") == "ROOT_NIGHT_BASELINE_OUTER_OBSERVATION_V2"
                  and receipt.get("terminal_state") == "response"
                  and receipt.get("cleanup") == "terminal_shared_connector_retained"
                  and receipt.get("observation_source") == "root_connected_tool_invocation"
                  and receipt.get("transport_replay") == "unobserved"
                  and receipt.get("response_sha256") == digest(response) and receipt.get("response_bytes") == len(response)
                  and all(_semantic_same(receipt.get(key), intent[key]) for key in ("intent_id", "nonce", "episode", "invocation",
                      "round_id", "plan_sha256", "binding_sha256", "binding", "arguments", "tool", "sdk_product_sha256")))
            sha = intent["sdk_product_sha256"][0]
            _need(sha in products and sha not in seen and intent["decoded_bytes"] == body_sizes[sha])
            product = products[sha]
            projection = {key: product[key] for key in ("args", "source_identity", "nights_requested",
                "source_nights", "contributing_nights", "unit", "bins", "excluded", "method")}
            projection.update(status="available", window={key: product["args"][key] for key in ("start_date", "end_date")},
                              quality_notes=product.get("quality_notes", []))
            expected = {"status": "available", "method_version": "night-baseline-v1", "current": projection,
                        "reference": None, "quality_flags": []}
            result = _reader_result(response, receipt)
            _need(_semantic_same(intent["expected"], expected)
                  and _semantic_same({key: result[key] for key in expected}, expected)
                  and _semantic_same(result["resources"], observation["resources"])
                  and result["resources"]["decoded_bytes"] == intent["decoded_bytes"]
                  and result["resources"]["r2_get_attempts"] == intent["reserved_gets"] == 1)
            seen.add(sha)
        _need(seen == set(products))


def _closed_session_postchecks(closure_ref):
    """Anchor historical ACKs in a checkpoint tip from pinned native output."""
    from .history_state import read

    closure = _json(closure_ref)
    session, prepared = _json(closure["session_ref"]), _json(closure["prepared_ref"])
    binding = session["binding"]
    _need(prepared.get("session_ref") == closure["session_ref"]
          and prepared.get("bindingSHA256") == digest(encoded(binding)))
    tip = None
    for reference in prepared["native_terminal_refs"]:
        native = _json(reference)
        _need(native.get("allNativeDescendantsTerminal") is True
              and native.get("bindingSHA256") == digest(encoded(binding)))
        if "output" not in native:
            continue
        for line in _pin(native["output"]).splitlines():
            if line.startswith(b"{"):
                result = json.loads(line)
                if result.get("checkpoint_sha256") is not None:
                    _need(result.get("span_sha256") == binding["spanManifest"]["sha256"]
                          and result.get("span_source_calls") == 0)
                    tip = result["checkpoint_sha256"]
    _need(tip is not None)
    path = Path(binding["job"]).resolve() / "rolling-reference-spans" / binding["spanManifest"]["sha256"] / "CHECKPOINTS.jsonl"
    prefix = bytearray()
    for line in read(path).splitlines(keepends=True):
        prefix.extend(line)
        if digest(line) == tip:
            break
    _need(prefix and digest(line) == tip)
    # The native result commits the hash-chained tip; no new ACK is invented.
    checkpoint = {"path": str(path), "bytes": len(prefix), "sha256": digest(bytes(prefix))}
    references = prepared["postchecks"]
    rows = [{"ref": item, "products": len(_json(item).get("generated_targets", [])),
             "planningOnly": not _json(item).get("generated_targets", [])} for item in references]
    _accepted_postchecks({"checkpointBefore": checkpoint}, binding, {"postchecks": rows},
        {"verifiedPostchecks": references}, prepared)


def _failed_metadata_archive(proof, original, old, failed):
    closure = _json(proof["failedMetadataClosure"])
    _need(closure.get("kind") == "FAILED_METADATA_NO_SOURCE_CLOSURE_V1"
          and closure.get("policyVersion") == 2
          and type(closure.get("actualOuterNative")) is int and closure["actualOuterNative"] == 1
          and all(closure.get(key) is False for key in
                  ("adoptionCompleted", "registerAttempted", "newSourceSessionStarted", "failedOwnerRootLeaseHeld"))
          and all(closure.get(key) is True for key in ("originalPartialHold", "allOwnerProcessesTerminal",
                  "noDataNativeConstructed", "all14Restored", "AApplicationHoldPreserved"))
          and type(closure.get("sourceCalls")) is int and closure["sourceCalls"] == 0
          and closure.get("sourceMeterUsed") == original["expectedMeterState"]["count"]
          and closure.get("failedBinding") == original["newBinding"]
          and closure.get("spanReleaseTransition") == proof["originalTransition"]
          and closure.get("sourceMeter") == original["sourceMeterPrefix"]
          and closure.get("spanMeter") == original["spanMeterPrefix"])
    archive = _json(closure["ownerArchive"])
    request = _json(archive["request"])
    before = _json(archive["stateBefore"])
    row = before.get("currentLiveExecutor", {})
    _need(archive.get("kind") == "ROOT_EXACT_FAILED_METADATA_OWNER_ARCHIVE_V1"
          and type(archive.get("actualOuterNative")) is int and archive["actualOuterNative"] == 1
          and archive.get("adoptionCompleted") is False
          and archive.get("registerAttempted") is False and archive.get("originalPartialHold") is True
          and archive.get("stateBefore") == closure["stateBefore"]
          and _pin(archive["stateBefore"]) == _pin(request["controlBefore"])
          and archive.get("originalExecutor") == row == _json(archive["originalExecutorRef"])
          and row.get("owner") == "Root" and row.get("invocation") == failed["invocation"]
          and row.get("bindingSHA256") == digest(encoded(failed)) and row.get("phase") == "cloud_pause"
          and type(row.get("callerPID")) is int and row["callerPID"] > 0
          and row.get("running") is True and row.get("terminalObserved") is False
          and row.get("resolved") is not True
          and before.get("sourceRunning") is False and before.get("R2Running") is False
          and request.get("failedBinding") == original["newBinding"]
          and request.get("transition") == proof["originalTransition"]
          and request.get("ownerClosure") == closure["ownerClosure"]
          and request.get("restore") == closure["independentRestore"]
          and request.get("sourceMeter") == original["sourceMeterPrefix"]
          and request.get("spanMeter") == original["spanMeterPrefix"])
    for key in ("ownerClosure", "restore", "restoreLeaseClosed", "transition", "partialAdmission", "rootClaim"):
        _need(_pin(archive["preserved"][key]) == _pin(request[key]))
    physical = _json(request["ownerClosure"])
    partial = _json(request["partialAdmission"])
    _need(physical.get("kind") == "ROOT_ACTUAL_METADATA_ADOPTION_PRE_DATA_FAILURE_PHYSICALLY_TERMINAL"
          and type(physical.get("actualOuterNative")) is int and physical["actualOuterNative"] == 1
          and type(physical.get("rootPID")) is int and physical["rootPID"] == row["callerPID"]
          and all(physical.get(key) is False for key in
                  ("rootPresent", "sourceNativeConstructed", "dataIntentPresent"))
          and physical.get("noDataPermissionRequested") is True
          and physical.get("checkpointRepairAndPartialAdmissionHeld") is True
          and all(type(physical.get(key)) is int and physical[key] == 0 for key in ("sourceCalls", "R2S3Calls"))
          and physical.get("partialAdmission") == request["partialAdmission"]
          and physical.get("sourceMeter") == original["sourceMeterPrefix"]
          and physical.get("spanMeter") == original["spanMeterPrefix"]
          and partial.get("rootClaim") == request["rootClaim"]
          and partial.get("leasePID") == row.get("callerPID"))
    claim = _json(request["rootClaim"])
    _need(all(claim.get("currentLiveExecutor", {}).get(key) == row.get(key)
              for key in ("owner", "callerPID", "invocation", "bindingSHA256", "phase"))
          and claim.get("sourceRunning") is False and claim.get("R2Running") is False)
    validate_span_release_frame(proof["originalTransition"], request["partialAdmission"],
        _release_identity(old), _release_identity(failed), original["spanMeterPrefix"]["path"],
        original["expectedMeterState"])
    restored, lease = _json(request["restore"]), _json(request["restoreLeaseClosed"])
    _need(restored.get("kind") == "ROOT_INDEPENDENT_EXACT14_RESTORE_AFTER_KNOWN_PRE_DATA_OWNER_EXIT"
          and restored.get("all14StatesMatchOriginal") is True and restored.get("originalOwnerNative") == 1
          and all(restored.get(key) is True for key in ("originalHoldPreserved", "noSourceOrR2Calls", "meterUnchanged"))
          and restored.get("previousClosure") == request["ownerClosure"]
          and restored.get("baseline") == request["baseline"]
          and _workflow_states(_json(request["baseline"])) == _workflow_states(restored["states"])
          and lease.get("held") is False and lease.get("noDataProcessesConstructed") is True
          and lease.get("PID") == restored.get("PID")
          and _json(closure["handoffOwnerLeaseClosed"]).get("held") is False)
    return closure, archive, partial


def _post_source_reader_pins(postcheck, binding):
    from .history_state import read

    _need(postcheck.get("readerObservations") and postcheck.get("readbacks"))
    sdk_hashes = set()
    for row in postcheck["readbacks"]:
        body = _pin(row["body"])
        sdk_hashes.add(digest(body))
    for reference in postcheck["readerObservations"]:
        observation = _semantic_load(_pin(reference))
        path = Path(reference["path"])
        stem = path.name.removesuffix(".observation.json")
        intent = _semantic_load(read(path.with_name(stem + ".intent.json")))
        receipt = _semantic_load(read(path.with_name(stem + ".execution-receipt.json")))
        response = read(path.with_name(stem + ".response"))
        _need(observation.get("status") == "matched" and observation.get("reason") is None
              and intent.get("invocation") == binding["invocation"]
              and intent.get("tool") == binding["normalReader"]["tool"]
              and _semantic_same(intent.get("binding"), binding["normalReader"]["binding"])
              and 1 <= len(intent["sdk_product_sha256"]) <= 2
              and set(intent["sdk_product_sha256"]) <= sdk_hashes
              and receipt.get("schema_version") == 2
              and receipt.get("kind") == "ROOT_NIGHT_BASELINE_OUTER_OBSERVATION_V2"
              and receipt.get("terminal_state") == "response"
              and receipt.get("cleanup") == "terminal_shared_connector_retained"
              and receipt.get("observation_source") == "root_connected_tool_invocation"
              and receipt.get("transport_replay") == "unobserved"
              and receipt.get("response_sha256") == digest(response) and receipt.get("response_bytes") == len(response)
              and all(_semantic_same(receipt.get(key), intent[key]) for key in ("intent_id", "nonce", "episode",
                  "invocation", "round_id", "plan_sha256", "binding_sha256", "binding", "arguments", "tool",
                  "sdk_product_sha256")))
        result = _reader_result(response, receipt)
        _need(_semantic_same({key: result[key] for key in intent["expected"]}, intent["expected"])
              and _semantic_same(result["resources"], observation["resources"])
              and result["resources"]["r2_get_attempts"] <= intent["reserved_gets"] <= 2)


def _known_partial_daily_closure(closure, daily, terminal, restored, outcome, lease, state):
    """Join closed Root evidence; enclosing failure is never a success claim."""
    completion = _json(closure["rootCompletion"])
    _need(completion.get("kind") == "ROOT_SOURCE05_RAW_DELIVERED_REFERENCE_PENDING_EXACT14_RESTORED"
          and all(type(completion.get(key)) is int and completion[key] == native for key, native in (
              ("sourceRootOuterNative", 1), ("managedDataNative", 2), ("lastManagedReferenceNative", 2),
              ("restoreOwnOuterNative", 0), ("controlFinalizerOwnNative", 0)))
          and all(completion.get(key) is True for key in ("all14Restored", "allLiveProcessesTerminal",
              "primaryHoldCauseUnobserved", "oldFullSpanHoldPreserved", "newGarminForbiddenThisRun"))
          and all(completion.get(key) is False for key in ("sourceFailed", "wholeReferenceComplete",
              "fullHistoryComplete", "dataOrRootRerunPerformed"))
          and type(completion.get("referenceCohortRemaining")) is int
          and completion["referenceCohortRemaining"] > 0
          and completion.get("lastPostcheck") == closure["postcheck"]
          and completion.get("restoreResult") == closure["restore"]
          and completion.get("controlFinalizerLeaseClosed") == closure["leaseClosed"])
    physical = _json(completion["physicalClosure"])
    finalizer = _json(completion["controlFinalization"])
    disposition = _json(finalizer["disposition"])
    restore_lease = _json(completion["restoreLeaseClosed"])
    raw = _json(completion["rawDelivery"])
    _need(physical.get("kind") == "ROOT_ACTUAL_SOURCE05_POST_VERIFIED_REFERENCE_EXIT_PHYSICALLY_TERMINAL"
          and physical.get("binding") == closure["binding"]
          and physical.get("baseline") == closure["baseline"]
          and physical.get("lastPostcheck") == closure["postcheck"]
          and type(physical.get("actualOuterNative")) is int and physical["actualOuterNative"] == 1
          and type(physical.get("historicalPrimarySourceNative")) is int
          and physical["historicalPrimarySourceNative"] == 2
          and physical.get("primaryExceptionType") == "Hold"
          and all(physical.get(key) is True for key in ("allNativeTreesPhysicallyTerminal",
              "rootProcessTerminalObservedThroughOwnTool", "lastPostcheckComponentCleanupConfirmed",
              "primaryControlledReasonUnobserved", "raw60DestinationVerified"))
          and all(physical.get(key) is False for key in ("rootPresent", "sourceFailed", "wholeReferenceComplete"))
          and physical.get("remainingReferenceCohort") == completion["referenceCohortRemaining"]
          and physical.get("rootPID") == state.get("currentLiveExecutor", {}).get("callerPID")
          and type(outcome.get("normalCLIStatus")) is dict
          and outcome["normalCLIStatus"].get("daily_status") == "baselines_pending"
          and outcome.get("successClaimed") is False)
    trees = physical.get("nativeTrees")
    _need(type(trees) is list and 1 <= len(trees) <= 65
          and len({encoded(row["proof"]) for row in trees}) == len(trees)
          and closure["terminal"] in [row["proof"] for row in trees])
    total = 0
    for row in trees:
        native = _json(row["proof"])
        _need(native.get("bindingSHA256") == digest(encoded(daily))
              and native.get("allNativeDescendantsTerminal") is True
              and native.get("parentPhysicallyTerminal") is True
              and type(native.get("activeNativeProcesses")) is int and native["activeNativeProcesses"] == 0
              and type(native.get("actualNative")) is int and native["actualNative"] in (0, 2)
              and native["actualNative"] == row.get("actualNative")
              and type(native.get("parentPID")) is int and native["parentPID"] > 0
              and native["parentPID"] == row.get("parentPID")
              and type(native.get("totalNativeProcesses")) is int and native["totalNativeProcesses"] > 0
              and native["totalNativeProcesses"] == row.get("totalNativeProcesses"))
        total += native["totalNativeProcesses"]
    _need(type(physical.get("totalNativeProcesses")) is int and physical["totalNativeProcesses"] == total
          and terminal.get("actualNative") == 2
          and restored.get("kind") == "ROOT_SOURCE05_INDEPENDENT_EXACT14_RESTORE"
          and restored.get("closure") == completion["physicalClosure"]
          and restored.get("baseline") == closure["baseline"]
          and restored.get("originalOwnerOuterNative") == 1
          and restored.get("originalNative2AndUnobservedHoldReasonPreserved") is True
          and restored.get("wholeReferenceComplete") is False and restored.get("noSourceOrR2Calls") is True
          and type(restored.get("PID")) is int and restored["PID"] > 0
          and restore_lease.get("PID") == restored["PID"] and restore_lease.get("held") is False
          and all(restore_lease.get(key) is True for key in ("leaseAcquired", "noDataProcessesConstructed",
              "noLockRemovalOrTakeover")))
    _need(disposition.get("kind") == "ROOT_KNOWN_MANAGED_DAILY_PENDING_AFTER_CONTROLLER_EXIT"
          and disposition.get("closure") == completion["physicalClosure"]
          and disposition.get("restore") == closure["restore"]
          and disposition.get("restoreLeaseClosed") == completion["restoreLeaseClosed"]
          and disposition.get("stateBefore") == finalizer.get("before")
          and disposition.get("rootControllerOuterNative") == 1
          and disposition.get("managedDataNative") == disposition.get("latestManagedReferenceNative") == 2
          and disposition.get("referenceCohortRemaining") == completion["referenceCohortRemaining"]
          and all(disposition.get(key) is True for key in ("raw60Verified", "originalFailureNotReclassified",
              "originalHoldCauseUnobserved", "controlOnlyPhysicalDispositionResolved", "noRootOrDataRerun"))
          and disposition.get("sourceFailed") is False and disposition.get("wholeReferenceComplete") is False
          and finalizer.get("kind") == "ROOT_CONTROL_KNOWN_PARTIAL_FINALIZED"
          and finalizer.get("noDataOrNetworkCalls") is True
          and finalizer.get("oldController1AndManaged2Preserved") is True
          and type(finalizer.get("PID")) is int and finalizer["PID"] > 0
          and lease.get("PID") == finalizer["PID"] and lease.get("noDataCalls") is True)
    # The finalizer's after ref names mutable CONTROL. Its historical content is
    # not replayed: currentState has its own immutable pin and ordinary CAS gate.
    before_owner = _json(finalizer["before"]).get("currentLiveExecutor", {})
    _need(before_owner.get("owner") == "Root"
          and before_owner.get("invocation") == daily["invocation"]
          and before_owner.get("bindingSHA256") == digest(encoded(daily))
          and type(before_owner.get("callerPID")) is int
          and before_owner["callerPID"] == physical["rootPID"])
    _need(raw.get("kind") == "ROOT_RAW60_DESTINATION_VERIFIED_REFERENCE_WORK_STILL_PENDING"
          and raw.get("invocation") == daily["invocation"]
          and raw.get("dataActualNative") == 2 and raw.get("dataStatus") == "baselines_pending"
          and raw.get("sourceFailed") is False and raw.get("referencePackageComplete") is False
          and all(raw.get(key) is True for key in ("all60RawDestinationVerified",
              "all63DataNativeProcessesTerminal", "noMoreGarminFromThisRun"))
          and raw.get("physicalDataTerminal") in [row["proof"] for row in trees])
    raw_postcheck = _json(raw["postcheck"])
    _need(raw_postcheck.get("bindingSHA256") == digest(encoded(daily))
          and all(raw_postcheck.get(key) is True for key in ("rawVerified",
              "publishedDestinationsVerified", "componentCleanupConfirmed")))
    return raw


def _post_source_span_release(proof):
    from .history_state import Job
    from .rolling_span_source_proof import observed_raw_delivery

    extras = {"originalTransition", "failedMetadataClosure", "laterDailyClosure", "currentState",
              "currentSourceMeterPrefix", "laterSourceLink"}
    original = _json(proof["originalTransition"])
    _need(original.get("kind") == "ROOT_CLOSED_SPAN_RELEASE_TRANSITION_V1"
          and set(proof) == set(original) | extras
          and all(proof[key] == value for key, value in original.items()
                  if key not in {"kind", "schema", "newBinding", "stateSHA256"}))
    original, old, failed = validate_span_release_transition(proof["originalTransition"])
    new = _json(proof["newBinding"])
    _released(new)
    _need(_release_identity(old) != _release_identity(new)
          and all(new[key] == old[key] == failed[key] for key in
                  ("job", "plan", "accountSHA256", "targetSHA256", "spanManifest")))
    failed_closure, archive, _ = _failed_metadata_archive(proof, original, old, failed)
    closure = _json(proof["laterDailyClosure"])
    partial = closure.get("kind") == "ROOT_KNOWN_PARTIAL_DAILY_FOR_POST_SOURCE_SPAN_V1"
    fields = {"kind", "binding", "postcheck", "terminal", "restore", "baseline", "outcome",
              "leaseClosed", "rootTerminal", "actualOuterNative", "dataNative"}
    _need(set(closure) == fields | ({"rootCompletion"} if partial else set())
          and (partial or closure.get("kind") == "ROOT_CLOSED_DAILY_FOR_POST_SOURCE_SPAN_V1")
          and type(closure.get("actualOuterNative")) is int
          and closure["actualOuterNative"] in ((1,) if partial else (0, 2))
          and type(closure.get("dataNative")) is int and closure["dataNative"] in (0, 2)
          and closure.get("rootTerminal") == proof["currentState"]
          and proof["currentState"]["sha256"] == proof["stateSHA256"])
    daily, postcheck, terminal, restored, outcome, lease, state = (
        _json(closure[key]) for key in ("binding", "postcheck", "terminal", "restore", "outcome", "leaseClosed", "rootTerminal"))
    _released(daily, operation="daily")
    daily_sha = digest(encoded(daily))
    _need(all(daily[key] == new[key] for key in ("job", "plan", "accountSHA256", "targetSHA256"))
          and all(item.get("bindingSHA256") == daily_sha for item in (
              (postcheck, terminal, outcome) if partial else (postcheck, terminal, restored, outcome, lease)))
          and outcome.get("sourceFailed") is False and type(outcome.get("actualNative")) is int
          and outcome["actualNative"] == closure["dataNative"]
          and terminal.get("allNativeDescendantsTerminal") is True and terminal.get("parentPhysicallyTerminal") is True
          and terminal.get("activeNativeProcesses") == 0
          and type(terminal.get("actualNative")) is int and terminal["actualNative"] in (0, 2)
          and restored.get("all14StatesMatchOriginal") is True
          and _workflow_states(_json(closure["baseline"])) == _workflow_states(restored["states"])
          and lease.get("held") is False and type(lease.get("PID")) is int and lease["PID"] > 0
          and all(postcheck.get(key) is True for key in ("rawVerified", "publishedDestinationsVerified",
                  "sdkAndNormalReaderVerified", "componentCleanupConfirmed")))
    current = state.get("currentLiveExecutor", {})
    _need(state.get("sourceRunning") is False and state.get("R2Running") is False
          and current.get("owner") == "Root" and current.get("invocation") == daily["invocation"]
          and current.get("bindingSHA256") == daily_sha and current.get("running") is False
          and current.get("terminalObserved") is True and current.get("physicalTreeKnownStopped") is True
          and type(current.get("actualNative")) is int and current["actualNative"] == terminal["actualNative"]
          and type(current.get("callerPID")) is int and current["callerPID"] > 0
          and any(row.get("ownerArchive") == failed_closure["ownerArchive"]
                  and row.get("originalExecutor") == archive["originalExecutor"]
                  and type(row.get("actualOuterNative")) is int and row["actualOuterNative"] == 1
                  and row.get("originalPartialHold") is True
                  for row in state.get("sourceFirstOwnerHandoffs", [])))
    raw = _known_partial_daily_closure(closure, daily, terminal, restored, outcome, lease, state) if partial else None
    replay = _Replay()
    source_link, source_binding = replay.source_link(proof["laterSourceLink"])
    _need(source_binding == daily and source_link["source_binding_ref"] == closure["binding"]
          and source_link["source_postcheck_ref"] == closure["postcheck"]
          and source_link["source_prefix"] == proof["currentSourceMeterPrefix"]
          and Path(proof["currentSourceMeterPrefix"]["path"]).resolve()
              == Path(original["sourceMeterPrefix"]["path"]).resolve())
    historical = _pin(original["sourceMeterPrefix"], prefix=True)
    current_body = _pin(proof["currentSourceMeterPrefix"], prefix=True)
    _need(len(current_body) > len(historical) and current_body.startswith(historical))
    current_source = replay.scan(proof["currentSourceMeterPrefix"], span=False)
    _need(not current_source["sessions"] and current_source["count"] >= original["expectedMeterState"]["count"]
          and current_source["initial"] == original["expectedMeterState"]["initial"]
          and current_source["identity"] == _release_identity(daily))
    delivery = observed_raw_delivery(Job(daily["job"]), closure["binding"], closure["postcheck"],
                                     target_sha=daily["targetSHA256"])
    _need(delivery["destinations"] and all(row["source_failure"] is False for row in delivery["stage_facts"]))
    if partial:
        _need(closure["dataNative"] == 2 and current.get("sourceFailed") is False
              and any(row["postcheck"] == raw["postcheck"] for row in delivery["destinations"]))
    _post_source_reader_pins(postcheck, daily)
    return proof, old, new


def validate_span_release_transition(reference):
    """Replay the immutable closed-span graph; never grant or mutate anything."""
    proof = _json(reference)
    if (type(proof) is dict and proof.get("kind") == "ROOT_POST_SOURCE_SPAN_RELEASE_TRANSITION_V1"
            and proof.get("schema") == 1):
        return _post_source_span_release(proof)
    _need(type(proof) is dict and set(proof) == {
        "kind", "schema", "oldBinding", "newBinding", "oldClosure", "oldExecutor", "stateSHA256",
        "originalBaseline", "spanMeterPrefix", "sourceMeterPrefix", "sourceSessionClosed",
        "checkpointBefore", "qualifiedEpoch", "repairPlan", "expectedMeterState"}
        and proof["kind"] == "ROOT_CLOSED_SPAN_RELEASE_TRANSITION_V1" and proof["schema"] == 1)
    old, new = _json(proof["oldBinding"]), _json(proof["newBinding"])
    _released(old)
    _released(new)
    _need(_release_identity(old) != _release_identity(new)
          and all(old[key] == new[key] for key in
                  ("job", "plan", "operation", "accountSHA256", "targetSHA256", "spanManifest")))
    root = Path(old["job"]).resolve() / "operator-enclosure-20261106"
    _need(Path(proof["spanMeterPrefix"]["path"]).resolve() == root / "span-independent-gets.jsonl"
          and Path(proof["sourceMeterPrefix"]["path"]).resolve() == root / "independent-gets.jsonl")
    replay = _Replay()
    meter = replay.scan(proof["spanMeterPrefix"], span=True)
    source = replay.scan(proof["sourceMeterPrefix"], span=False)
    expected = proof["expectedMeterState"]
    linked_source, source_binding = replay.source_link(meter["source_ref"])
    _need(_meter_state(meter) == expected and meter["identity"] == _release_identity(old)
          and source["identity"] == linked_source["source_identity"]
          and source["count"] == meter["count"] and source["initial"] == meter["initial"]
          and not source["sessions"] and source_binding["plan"] == old["plan"]
          and source_binding["targetSHA256"] == old["targetSHA256"]
          and expected["round"]["epoch_ref"] == proof["qualifiedEpoch"])
    stop = _json(proof["oldClosure"])
    old_sha = digest(encoded(old))
    _need(stop.get("kind") == "ROOT_ACTUAL_FULLSPAN_CONTROL_ERROR_EXACT14_RESTORED_PHYSICALLY_CLOSED"
          and stop.get("binding") == proof["oldBinding"] and stop.get("manifest") == old["spanManifest"]
          and stop.get("invocation") == old["invocation"] and type(stop.get("actualOuterNative")) is int
          and stop["actualOuterNative"] == 1 and stop.get("originalControlError") == "span_epoch_unqualified"
          and all(stop.get(key) is False for key in
                  ("originalControlErrorFixed", "automaticRetry", "RootLeaseHeld", "fullSpanComplete"))
          and all(stop.get(key) is True for key in ("sourceSessionSettled", "all14WorkflowStatesRestored",
                  "allNativeDataTreesTerminal", "allNativeHandlesClosed", "allLiveActorsTerminal"))
          and all(type(stop.get(key)) is int and stop[key] == 0
                  for key in ("sourceCalls", "newGarminHTTP", "newReloads"))
          and stop.get("epochAdmitted") == proof["qualifiedEpoch"]["sha256"]
          and stop.get("followingNextReportedRound") != stop["epochAdmitted"]
          and type(stop.get("sourceMeterCurrentUsed")) is int and stop["sourceMeterCurrentUsed"] == expected["count"])
    executor = proof["oldExecutor"]
    _need(executor.get("owner") == "Root" and executor.get("invocation") == old["invocation"]
          and executor.get("bindingSHA256") == old_sha and executor.get("running") is False
          and executor.get("terminalObserved") is True
          and isinstance(proof["stateSHA256"], str) and len(proof["stateSHA256"]) == 64)
    owner = _json(stop["ownerClose"])
    known = owner["knownRuntimeState"]
    _need(owner.get("kind") == "ACTUAL_KNOWN_CONTROL_ERROR_CLOSED_WITH_PENDING_SCOPE"
          and all(owner.get(key) is False for key in ("RootLeaseHeld", "fullSpanDelivered",
                  "normalStartReturned", "originalControlErrorResolved"))
          and owner.get("allNativeHandlesClosed") is True and owner.get("restore") == stop["restore"]
          and known.get("kind") == "KNOWN_ORDINARY_SPAN_SESSION_V1" and known.get("restored") is True
          and known.get("sourceCalls") == 0 and known.get("pendingReason") == "span_epoch_unqualified"
          and known.get("spanManifest") == old["spanManifest"]
          and owner.get("originalControlError") == stop["controlError"])
    control = _json(stop["controlError"])
    _need(control.get("kind") == "ACTUAL_ROOT_SPAN_CONTROL_ERROR"
          and control.get("error") == "operator_adapter.Hold: span_epoch_unqualified"
          and control.get("automaticRetry") is False and control.get("fixedOrResolved") is False
          and control.get("originalOuterStillRetained") is True and control.get("unknownWriteClaimed") is False
          and type(control.get("sourceCalls")) is int and control["sourceCalls"] == 0)
    restored = _json(stop["restore"])
    _need(restored.get("all14StatesMatchOriginal") is True and restored.get("bindingSHA256") == old_sha
          and _workflow_states(_json(proof["originalBaseline"])) == _workflow_states(restored["states"]))
    closure = _json(proof["sourceSessionClosed"])
    session, prepared = _json(closure["session_ref"]), _json(closure["prepared_ref"])
    _need(closure.get("kind") == "KNOWN_SPAN_SOURCE_SESSION_CLOSED_V1" and closure.get("leaseClosed") is True
          and session.get("kind") == "ACTUAL_SPAN_SOURCE_SESSION_INTENT_V1" and session.get("binding") == old
          and prepared.get("kind") == "PREPARED_SPAN_SOURCE_SESSION_CLOSE_V1"
          and prepared.get("session_ref") == closure["session_ref"] and prepared.get("bindingSHA256") == old_sha
          and prepared.get("restore_ref") == stop["restore"] and prepared.get("meter_prefix") == proof["spanMeterPrefix"])
    terminal = _json(prepared["root_terminal_ref"])
    claim = terminal.get("currentLiveExecutor", {})
    _need(claim.get("invocation") == old["invocation"] and claim.get("bindingSHA256") == old_sha
          and claim.get("running") is False and claim.get("terminalObserved") is True
          and claim.get("resolved") is True and terminal.get("sourceRunning") is False
          and terminal.get("R2Running") is False)
    _need(prepared.get("native_terminal_refs"))
    for item in prepared["native_terminal_refs"]:
        native = _json(item)
        _need(native.get("bindingSHA256") == old_sha and native.get("allNativeDescendantsTerminal") is True)
    _accepted_postchecks(proof, old, stop, known, prepared)
    source_rows = [json.loads(line) for line in _pin(proof["sourceMeterPrefix"], prefix=True).splitlines()]
    _need(any(frame.get("kind") == "span_session_closed" and frame.get("closure_ref") == proof["sourceSessionClosed"]
              and frame.get("session_ref") == closure["session_ref"] for frame in source_rows))
    for frame in source_rows:
        if frame.get("kind") == "span_session_closed" and frame["closure_ref"] != proof["sourceSessionClosed"]:
            _closed_session_postchecks(frame["closure_ref"])
    _, before = _checkpoint(proof["checkpointBefore"], old)
    plan, epoch = _json(proof["repairPlan"]), _json(proof["qualifiedEpoch"])
    _need(plan.get("kind") == "job-rolling-night-reference-plan" and proof["repairPlan"]["sha256"] == before.get("plan_sha256")
          and plan.get("job_plan_sha256") == old["plan"]["sha256"] and plan.get("storage_target_sha256") == old["targetSHA256"]
          and plan.get("span", {}).get("span_sha256") == old["spanManifest"]["sha256"]
          and plan["span"].get("chunk_index") == before.get("chunk_index")
          and epoch.get("purpose") == "rolling_reference_span_only" and epoch.get("span_sha256") == old["spanManifest"]["sha256"]
          and epoch.get("job_plan_sha256") == old["plan"]["sha256"] and epoch.get("storage_target_sha256") == old["targetSHA256"])
    return proof, old, new


def validate_span_release_frame(transition_ref, admission_ref, old_identity, new_identity, meter_path, state):
    """Validate a historical identity segment against its checked immutable state."""
    proof, old, new = validate_span_release_transition(transition_ref)
    _need(old_identity == _release_identity(old) and new_identity == _release_identity(new)
          and Path(meter_path).resolve() == Path(proof["spanMeterPrefix"]["path"]).resolve()
          and state == proof["expectedMeterState"])
    admission = _json(admission_ref)
    _need(set(admission) == {"kind", "schema", "transition", "permission", "bindingSHA256", "invocation",
          "leasePID", "rootControl", "rootLeasePath", "state", "checkpointBefore", "checkpointAfter", "qualifiedEpoch", "rootClaim"}
          and admission.get("kind") == "QUALIFIED_SPAN_RELEASE_TRANSITION_V1" and admission.get("schema") == 1
          and admission.get("transition") == transition_ref and admission.get("bindingSHA256") == digest(encoded(new))
          and admission.get("invocation") == new["invocation"] and type(admission.get("leasePID")) is int
          and admission["leasePID"] > 0 and admission.get("state") == state
          and admission.get("checkpointBefore") == proof["checkpointBefore"]
          and admission.get("qualifiedEpoch") == proof["qualifiedEpoch"]
          and Path(admission["rootLeasePath"]).resolve() == Path(admission["rootControl"]).resolve() / ".root-live-operation.lock")
    permission = _json(admission["permission"])
    claim = _json(admission["rootClaim"]).get("currentLiveExecutor", {})
    _need(permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT" and permission.get("outcome") == "allowed"
          and permission.get("phase") == "pause" and permission.get("commandSHA256") is None
          and permission.get("spanReleaseTransition") == transition_ref
          and permission.get("bindingSHA256") == admission["bindingSHA256"]
          and permission.get("invocation") == admission["invocation"] and permission.get("callerPID") == admission["leasePID"]
          and claim.get("owner") == "Root" and claim.get("bindingSHA256") == admission["bindingSHA256"]
          and claim.get("invocation") == admission["invocation"] and claim.get("callerPID") == admission["leasePID"]
          and claim.get("running") is True and claim.get("terminalObserved") is False)
    prior_bytes, before = _checkpoint(proof["checkpointBefore"], old)
    after_bytes, after = _checkpoint(admission["checkpointAfter"], new)
    _need(len(after_bytes) > len(prior_bytes) and after_bytes.startswith(prior_bytes)
          and {key: value for key, value in before.items() if key != "execution_totals"}
              == {key: value for key, value in after.items() if key != "execution_totals"}
          and after["execution_totals"].get("reference_round_sha256") == proof["qualifiedEpoch"]["sha256"]
          and after["execution_totals"].get("round_baseline") == _json(proof["qualifiedEpoch"])["new_round_baseline"])
    return new_identity


def _settled_daily_release_frame(frame, path, state, replay):
    proof, admission = _json(frame["transition_ref"]), _json(frame["admission_ref"])
    suffix = _json(proof["settledSpanSuffix"])
    _need(suffix.get("kind") == "SETTLED_SPAN_SOURCE_SUFFIX_FOR_DAILY_RELEASE_V1" and suffix.get("schema") == 1
          and set(suffix) == {"kind", "schema", "spanReleaseTransition"}
          and proof.get("kind") == "METER_RELEASE_TRANSITION" and proof.get("schema") == 1)
    span_proof, old_span, new_span = validate_span_release_transition(suffix["spanReleaseTransition"])
    old, new = _json(proof["oldBinding"]), _json(proof["newBinding"])
    _released(old, operation="daily")
    _released(new, operation="daily")
    _need(old.get("operation") == new.get("operation") == "daily"
          and state["identity"] == _release_identity(old) and frame["identity"] == _release_identity(new)
          and state["identity"] != frame["identity"]
          and span_proof["sourceMeterPrefix"] == proof["meterPrefix"]
          and Path(path).resolve() == Path(proof["meterPrefix"]["path"]).resolve()
          and all(old[key] == new[key] == old_span[key] for key in ("job", "plan", "accountSHA256", "targetSHA256"))
          and all(new[key] == new_span[key] for key in
                  ("main", "tree", "ciRunID", "runtimeManifest", "environmentManifest", "interpreter"))
          and all(new.get(key) == new_span.get(key) and isinstance(new.get(key), dict)
                  for key in ("spanMeterModule", "spanReleaseAdoptionModule")))
    for key in ("spanMeterModule", "spanReleaseAdoptionModule"):
        _file_pin(new[key])
    completion = proof["oldCompletion"]
    _need(set(completion) == {"outcome", "terminal", "postcheck", "baseline", "restore", "closure"})
    outcome, terminal, postcheck, baseline, restored, closure = (
        _json(completion[key]) for key in ("outcome", "terminal", "postcheck", "baseline", "restore", "closure"))
    old_sha = digest(encoded(old))
    _need(all(value.get("bindingSHA256") == old_sha for value in (outcome, terminal, postcheck, restored))
          and type(outcome.get("actualNative")) is int and outcome["actualNative"] == 0
          and outcome.get("sourceFailed") is False and terminal.get("actualNative") == 0
          and terminal.get("allNativeDescendantsTerminal") is True and terminal.get("parentPhysicallyTerminal") is True
          and terminal.get("activeNativeProcesses") == 0 and postcheck.get("componentCleanupConfirmed") is True
          and restored.get("all14StatesMatchOriginal") is True
          and _workflow_states(baseline) == _workflow_states(restored["states"])
          and closure.get("kind") == "ROOT_ACTUAL_ORDINARY_CLOSURE_OBSERVED_FOR_WAIT60_RELEASE"
          and closure.get("ordinaryInvocation") == old["invocation"]
          and closure.get("dataNative") == closure.get("actualOuterNative") == 0
          and all(closure.get(key) is True for key in ("rootObserved", "outcomeKnown", "allNativeDescendantsTerminal",
              "exact14Restored", "SDKAndNormalReaderVerified", "ownLibraryHandleClosedAndRootLeaseReleased"))
          and closure.get("localDataOwnerActive") is False)
    original = postcheck["meterJournal"]
    original_body, current_body = _pin(original, prefix=True), _pin(proof["meterPrefix"], prefix=True)
    _need(current_body.startswith(original_body) and len(current_body) > len(original_body))
    first = replay.scan(original, span=False)
    current = replay.scan(proof["meterPrefix"], span=False)
    _need(not first["sessions"] and not current["sessions"] and first["identity"] == current["identity"] == state["identity"]
          and first["count"] == postcheck["cumulativeIndependentGETs"]
          and current["count"] == span_proof["expectedMeterState"]["count"] == state["count"]
          and current["round"] == first["round"] == state["round"]
          and current["baseline"] == first["baseline"] == state["baseline"]
          and current["initial"] == first["initial"] == state["initial"]
          and current["sequence"] == state["sequence"] and current["previous"] == state["previous"])
    intents, closed, delta = {}, {}, 0
    for line in current_body[len(original_body):].splitlines():
        row = json.loads(line)
        _need(row["identity"] == state["identity"] and row["kind"] in
              {"span_session_intent", "span_session_closed", "span_usage"})
        if row["kind"] == "span_usage":
            _need(type(row["delta"]) is int and row["delta"] >= 0)
            delta += row["delta"]
            continue
        session = _json(row["session_ref"])
        linked, _ = replay.source_link(session["source_link_ref"])
        _need(linked["source_binding_ref"] == proof["oldBinding"]
              and linked["source_postcheck_ref"] == proof["oldCompletion"]["postcheck"]
              and linked["source_prefix"] == original and linked["source_identity"] == state["identity"])
        identifier = session["session_id"]
        if row["kind"] == "span_session_intent":
            _need(identifier not in intents)
            intents[identifier] = row["session_ref"]
        else:
            _need(intents.get(identifier) == row["session_ref"] and identifier not in closed)
            closed[identifier] = row["closure_ref"]
    _need(len(intents) == len(closed) == 2 and intents.keys() == closed.keys()
          and span_proof["sourceSessionClosed"] in closed.values()
          and delta == current["count"] - first["count"] > 0)
    permission = _json(admission["permission"])
    saved_state = {key: state[key] for key in ("count", "round", "baseline", "initial", "sequence", "previous")}
    new_sha = digest(encoded(new))
    _need(admission.get("kind") == "QUALIFIED_METER_RELEASE_TRANSITION_V1"
          and admission.get("transition") == frame["transition_ref"] and admission.get("state") == saved_state
          and admission.get("bindingSHA256") == new_sha and admission.get("invocation") == new["invocation"]
          and frame.get("invocation") == new["invocation"] and type(admission.get("leasePID")) is int
          and Path(admission["rootLeasePath"]).resolve() == Path(admission["rootControl"]).resolve() / ".root-live-operation.lock"
          and permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT" and permission.get("outcome") == "allowed"
          and permission.get("phase") == "data" and permission.get("bindingSHA256") == new_sha
          and permission.get("invocation") == new["invocation"] and permission.get("callerPID") == admission["leasePID"]
          and permission.get("commandSHA256") == admission.get("commandSHA256")
          and permission.get("meterReleaseTransition") == frame["transition_ref"])


class _Replay:
    def __init__(self):
        self.memo, self.active = {}, set()

    def source_link(self, reference):
        value = _json(reference)
        _need(value.get("kind") == "ACTUAL_SPAN_SOURCE_LINK_V1")
        binding, postcheck = _json(value["source_binding_ref"]), _json(value["source_postcheck_ref"])
        source_path = Path(value["source_meter_path"]).resolve()
        _need(source_path == Path(binding["job"]).resolve() / "operator-enclosure-20261106/independent-gets.jsonl"
              and Path(value["source_prefix"]["path"]).resolve() == source_path
              and Path(value["source_journal_prefix"]["path"]).resolve() == Path(binding["job"]).resolve() / "events.jsonl")
        journal = value["source_journal_prefix"]
        if journal["bytes"]:
            _pin(journal, prefix=True)
        else:
            _need(journal["sha256"] == digest(b""))
        permission = _json(value["qualification_ref"])
        _need(postcheck.get("componentCleanupConfirmed") is True
              and postcheck.get("bindingSHA256") == digest(encoded(binding))
              and postcheck.get("cumulativeIndependentGETs") == value["source_used"]
              and postcheck.get("meterJournal") == value["source_prefix"]
              and permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT"
              and permission.get("outcome") == "allowed"
              and permission.get("phase") in {"data", "postcheck"}
              and type(permission.get("callerPID")) is int)
        source = self.scan(value["source_prefix"], span=False)
        _need(source["count"] == value["source_used"] and source["baseline"] == value["source_baseline"]
              and source["initial"] == value["source_initial"] and source["round"] == value["source_round"]
              and source["identity"] == value["source_identity"])
        return value, binding

    def round_proof(self, proof, previous=None):
        _need(isinstance(proof, dict) and proof.get("schema") == 3
              and proof.get("kind") == "SOURCE_LINKED_SPAN_READ_ROUND_V1")
        source, binding = self.source_link(proof["source_link_ref"])
        initial_round = (source["source_round"]["round_id"] if source["source_round"] else
            "span-source-" + digest(encoded({"path": source["source_meter_path"],
                "plan": binding["plan"], "target": binding["targetSHA256"]})))
        _need(proof["plan_sha256"] == binding["plan"]["sha256"]
              and proof["target_sha256"] == binding["targetSHA256"]
              and proof["initial_carried"] == source["source_initial"])
        if proof["epoch_ref"] is None:
            _need(proof["round_id"] == initial_round and proof["previous_round_id"] is None)
        else:
            epoch = _json(proof["epoch_ref"])
            parent = _json(epoch["qualified_parent_ref"])
            _need(epoch.get("schema") == 1 and epoch.get("purpose") == "rolling_reference_span_only"
                  and epoch.get("source_carry") is False and epoch.get("independent_pool_transition") is True
                  and epoch["job_plan_sha256"] == proof["plan_sha256"]
                  and epoch["storage_target_sha256"] == proof["target_sha256"]
                  and parent.get("kind") == "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1"
                  and parent.get("schema") == 1 and parent["span_sha256"] == epoch["span_sha256"]
                  and parent.get("ack_ref") is not None
                  and proof["round_id"] == "span-epoch-" + proof["epoch_ref"]["sha256"])
            prior = self.link(parent["source_meter_link"])
            _need(proof["previous_round_id"] == prior["current_round_id"]
                  and (previous is None or proof["previous_round_id"] == previous["round_id"]))
            _pin(epoch["terminal_receipt_ref"])
            if epoch.get("remote_ref") is not None:
                _pin(epoch["remote_ref"])
            ack = _json(parent["ack_ref"])
            _pin(ack["postcheck"])
            native, permission = _json(parent["native_terminal"]), _json(parent["qualification_ref"])
            _need(native.get("allNativeDescendantsTerminal") is True and native.get("actualNative") in (0, 2)
                  and permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT"
                  and permission.get("phase") == "data" and permission.get("outcome") == "allowed")
        return source

    def scan(self, reference, *, span):
        key = (reference["path"], reference["bytes"], reference["sha256"], span)
        if key in self.memo:
            return self.memo[key]
        _need(key not in self.active and len(self.active) < 128 and len(self.memo) < 4096)
        self.active.add(key)
        body = _pin(reference, prefix=True)
        _need(body.endswith(b"\n"))
        state = {"count": 0, "initial": None, "baseline": 0, "round": None, "identity": None,
                 "span_gets": 0, "source_pool_gets": 0, "separate_epoch_gets": 0}
        previous, pending, source_ref, linked, excluded = None, None, None, False, 0
        imports, external_ids, sessions = {}, set(), {}
        for sequence, line in enumerate(body.splitlines(keepends=True)):
            _need(sequence < 16384)
            frame = json.loads(line)
            kind, count, before = frame["kind"], frame["count"], state["count"]
            _need(frame["sequence"] == sequence and frame["previous"] == previous
                  and type(count) is int and count >= 0)
            identity = frame.get("identity")
            if sequence == 0:
                state["identity"] = identity
            elif kind not in {"release_transition", "span_release_transition"}:
                _need(identity == state["identity"])
            if kind == "initial":
                _need(sequence == 0)
                state["initial"] = count
            elif kind == "source_link" and span:
                source, _ = self.source_link(frame["link_ref"])
                snapshot = self.scan(frame["snapshot_ref"], span=False)
                _need(pending is None and count == snapshot["count"] and count >= before
                      and Path(frame["snapshot_ref"]["path"]).resolve() == Path(source["source_meter_path"]).resolve()
                      and source["source_initial"] == state["initial"])
                if linked:
                    _need(frame["proof"] == state["round"] and frame["baseline"] == state["baseline"])
                else:
                    _need(frame["baseline"] == source["source_baseline"])
                self.round_proof(frame["proof"])
                state["round"], state["baseline"] = frame["proof"], frame["baseline"]
                source_ref, linked = frame["link_ref"], True
            elif kind == "round":
                _need(pending is None and count == before and type(frame["baseline"]) is int)
                if span:
                    _need(linked and frame["baseline"] == count and frame["proof"]["epoch_ref"] is not None)
                    self.round_proof(frame["proof"], state["round"])
                else:
                    _need(frame["baseline"] == (0 if frame["proof"].get("initial_round") else count))
                    if state["round"] is not None:
                        _need(frame["proof"]["previous_round_id"] == state["round"]["round_id"])
                    excluded = 0
                state["round"], state["baseline"] = frame["proof"], frame["baseline"]
            elif kind == "attempt":
                _need(pending is None and count == before + 1)
                pending = ("sdk",)
            elif kind == "known":
                _need(pending == ("sdk",) and count == before)
                pending = None
            elif kind == "night_baseline_intent":
                intent = frame["intent"]
                _need(pending is None and state["round"] is not None
                      and intent["round_id"] == state["round"]["round_id"]
                      and type(intent["reserved_gets"]) is int and 1 <= intent["reserved_gets"] <= 2
                      and count == before + intent["reserved_gets"] and intent["intent_id"] not in external_ids)
                external_ids.add(intent["intent_id"])
                pending = ("reader", intent, frame["intent_sha256"])
            elif kind == "night_baseline_ack":
                _need(pending == ("reader", frame["intent"], frame["intent_sha256"]) and count == before
                      and type(frame["reported_gets"]) is int and 0 <= frame["reported_gets"] <= frame["intent"]["reserved_gets"])
                _pin(frame["observation_ref"])
                pending = None
            elif kind == "span_usage" and not span:
                observed = self.scan(frame["span_meter_prefix"], span=True)
                path = str(Path(frame["span_meter_prefix"]["path"]).resolve())
                _need(Path(path) == Path(reference["path"]).resolve().parent / "span-independent-gets.jsonl")
                old = imports.get(path, dict.fromkeys(("span_gets", "source_pool_gets", "separate_epoch_gets"), 0))
                for field, delta in (("span_gets", "delta"), ("source_pool_gets", "source_pool_delta"),
                                     ("separate_epoch_gets", "separate_epoch_delta")):
                    _need(frame[field] == observed[field] and frame[delta] == observed[field] - old[field] >= 0)
                _need(count == before + frame["delta"] and frame["delta"] == frame["source_pool_delta"] + frame["separate_epoch_delta"]
                      and frame["span_identity"] == observed["identity"] and frame["source_link_ref"] == observed["source_ref"])
                linked_source, _ = self.source_link(frame["source_link_ref"])
                _need(linked_source["source_identity"] == identity)
                imports[path], excluded = frame, excluded + frame["separate_epoch_delta"]
            elif kind == "span_session_intent" and not span:
                session = _json(frame["session_ref"])
                claim = _json(session["root_claim_ref"]).get("currentLiveExecutor", {})
                _need(count == before and pending is None and session.get("kind") == "ACTUAL_SPAN_SOURCE_SESSION_INTENT_V1"
                      and claim.get("running") is True and claim.get("terminalObserved") is False
                      and claim.get("bindingSHA256") == digest(encoded(session["binding"]))
                      and claim.get("callerPID") == session["callerPID"] and session["session_id"] not in sessions)
                sessions[session["session_id"]] = frame["session_ref"]
            elif kind == "span_session_closed" and not span:
                closure, session = _json(frame["closure_ref"]), _json(frame["session_ref"])
                prepared = _json(closure["prepared_ref"])
                _need(count == before and pending is None and sessions.get(session["session_id"]) == frame["session_ref"]
                      and closure.get("kind") == "KNOWN_SPAN_SOURCE_SESSION_CLOSED_V1" and closure.get("leaseClosed") is True
                      and closure["session_ref"] == frame["session_ref"] and prepared["session_ref"] == frame["session_ref"])
                for field in ("root_terminal_ref", "restore_ref"):
                    _pin(prepared[field])
                for field in ("native_terminal_refs", "postchecks"):
                    for item in prepared[field]:
                        _pin(item)
                self.scan(prepared["meter_prefix"], span=True)
                del sessions[session["session_id"]]
            elif kind == "release_transition" and not span:
                _need(pending is None and count == before)
                _pin(frame["transition_ref"])
                _pin(frame["admission_ref"])
                if "settledSpanSuffix" in _json(frame["transition_ref"]):
                    _need(not sessions)
                    state["sequence"], state["previous"] = sequence, previous
                    _settled_daily_release_frame(frame, reference["path"], state, self)
                state["identity"] = identity
            elif kind == "span_release_transition" and span:
                _need(linked and pending is None and count == before
                      and frame.get("round") == state["round"] and frame.get("baseline") == state["baseline"]
                      and frame.get("initial") == state["initial"])
                state["sequence"], state["previous"] = sequence, previous
                validate_span_release_frame(frame["transition_ref"], frame["admission_ref"],
                    state["identity"], identity, reference["path"], _meter_state(state))
                _need(frame.get("invocation") == _json(_json(frame["transition_ref"])["newBinding"])["invocation"])
                state["identity"] = identity
            else:
                raise Stop("rolling_span_meter_link_unknown")
            if span:
                if kind in {"attempt", "night_baseline_intent"}:
                    _need(linked)
                    delta = count - before
                    state["span_gets"] += delta
                    field = "source_pool_gets" if state["round"]["epoch_ref"] is None else "separate_epoch_gets"
                    state[field] += delta
                for field in ("span_gets", "source_pool_gets", "separate_epoch_gets"):
                    _need(frame[field] == state[field])
            _need(0 <= count - state["baseline"] - (0 if span else excluded) <= 256)
            state["count"], previous = count, digest(line)
        _need(pending is None and (not span or linked))
        state["source_ref"] = source_ref
        state["sequence"], state["previous"], state["sessions"] = sequence + 1, previous, sessions
        self.active.remove(key)
        self.memo[key] = state
        return state

    def link(self, reference):
        value = _json(reference)
        _need(value.get("kind") == "ACTUAL_SPAN_METER_LINK_V1")
        state = self.scan(value["meter_prefix"], span=True)
        _need(value["used"] == state["count"] and value["round_baseline"] == state["baseline"]
              and value["initial"] == state["initial"] and value["current_round_id"] == state["round"]["round_id"])
        return value


def validate_source_linked_meter(reference):
    """Validate pinned historical bytes only; current Root permission is separate."""
    return _Replay().link(reference)
