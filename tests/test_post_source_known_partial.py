"""Read-only public validator over frozen synthetic 178 -> 187 component evidence."""
import copy
import importlib.util
import json
import os
import sys
import types
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("SLIPSTREAM_FROZEN_POST_SOURCE"),
    reason="Opt-in regression requires the frozen synthetic post-Source component package",
)


def load(reference):
    return json.loads(Path(reference["path"]).read_bytes())


@pytest.fixture(scope="session")
def codec():
    root = Path(__file__).resolve().parents[1]
    dependency = Path(os.environ["SLIPSTREAM_FROZEN_POST_SOURCE"]) / "private/pipeline"
    package = types.ModuleType("known_partial_candidate")
    package.__path__ = [str(root / "pipeline"), str(dependency)]
    sys.modules[package.__name__] = package
    spec = importlib.util.spec_from_file_location(
        package.__name__ + ".rolling_span_meter_proof", root / "pipeline/rolling_span_meter_proof.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def baseline(codec):
    root = Path(os.environ["SLIPSTREAM_FROZEN_POST_SOURCE"])
    path = root / "reports/ordinary-green01/fixture/test_post_source_metadata_then0/metadata/POST-TRANSITION.json"
    body = path.read_bytes()
    reference = {"path": str(path), "bytes": len(body), "sha256": codec.digest(body)}
    proof, _, _ = codec.validate_span_release_transition(reference)
    assert proof["expectedMeterState"]["count"] == 178
    assert load(proof["laterSourceLink"])["source_used"] == 187
    return proof


def save(codec, directory, name, value):
    path = directory / name
    body = codec.encoded(value)
    path.write_bytes(body)
    return {"path": str(path), "bytes": len(body), "sha256": codec.digest(body)}


def partial_graph(codec, directory, baseline):
    proof = copy.deepcopy(baseline)
    closure = load(proof["laterDailyClosure"])
    daily = load(closure["binding"])
    terminal = load(closure["terminal"])
    terminal.update(actualNative=2)
    closure["terminal"] = save(codec, directory, "managed-terminal.json", terminal)
    outcome = load(closure["outcome"])
    outcome.update(actualNative=2, normalCLIStatus={"daily_status": "baselines_pending"}, successClaimed=False)
    closure["outcome"] = save(codec, directory, "managed-outcome.json", outcome)
    state = load(proof["currentState"])
    state["currentLiveExecutor"].update(actualNative=2, sourceFailed=False,
        phase="known_managed_daily_pending_after_controller_exit_exact14_restored")
    proof["currentState"] = save(codec, directory, "current-state.json", state)
    proof["stateSHA256"] = proof["currentState"]["sha256"]
    closure.update(rootTerminal=proof["currentState"], actualOuterNative=1, dataNative=2,
                   kind="ROOT_KNOWN_PARTIAL_DAILY_FOR_POST_SOURCE_SPAN_V1")
    # These are synthetic external Root records with the actual closed metadata
    # schemas. Genuine producer, SDK, reader and old failed-owner facts are reused.
    physical = {
        "kind": "ROOT_ACTUAL_SOURCE05_POST_VERIFIED_REFERENCE_EXIT_PHYSICALLY_TERMINAL",
        "binding": closure["binding"], "baseline": closure["baseline"],
        "lastPostcheck": closure["postcheck"], "actualOuterNative": 1,
        "historicalPrimarySourceNative": 2, "sourceFailed": False,
        "primaryControlledReasonUnobserved": True, "primaryExceptionType": "Hold",
        "raw60DestinationVerified": True, "remainingReferenceCohort": 276,
        "wholeReferenceComplete": False, "rootPID": state["currentLiveExecutor"]["callerPID"],
        "rootPresent": False, "rootProcessTerminalObservedThroughOwnTool": True,
        "allNativeTreesPhysicallyTerminal": True, "lastPostcheckComponentCleanupConfirmed": True,
        "totalNativeProcesses": terminal["totalNativeProcesses"],
        "nativeTrees": [{"proof": closure["terminal"], "actualNative": 2,
                         "parentPID": terminal["parentPID"],
                         "totalNativeProcesses": terminal["totalNativeProcesses"]}],
    }
    physical_ref = save(codec, directory, "physical.json", physical)
    restore = {"kind": "ROOT_SOURCE05_INDEPENDENT_EXACT14_RESTORE", "PID": 777,
               "all14StatesMatchOriginal": True, "states": load(closure["baseline"]),
               "closure": physical_ref, "baseline": closure["baseline"],
               "originalOwnerOuterNative": 1,
               "originalNative2AndUnobservedHoldReasonPreserved": True,
               "wholeReferenceComplete": False, "noSourceOrR2Calls": True}
    closure["restore"] = save(codec, directory, "restore.json", restore)
    restore_lease = save(codec, directory, "restore-lease.json", {
        "PID": 777, "held": False, "leaseAcquired": True,
        "noDataProcessesConstructed": True, "noLockRemovalOrTakeover": True})
    disposition = {"kind": "ROOT_KNOWN_MANAGED_DAILY_PENDING_AFTER_CONTROLLER_EXIT",
        "closure": physical_ref, "restore": closure["restore"], "restoreLeaseClosed": restore_lease,
        "rootControllerOuterNative": 1, "managedDataNative": 2, "latestManagedReferenceNative": 2,
        "sourceFailed": False, "raw60Verified": True, "referenceCohortRemaining": 276,
        "wholeReferenceComplete": False, "originalFailureNotReclassified": True,
        "originalHoldCauseUnobserved": True, "controlOnlyPhysicalDispositionResolved": True,
        "noRootOrDataRerun": True, "stateBefore": closure["rootTerminal"]}
    disposition_ref = save(codec, directory, "disposition.json", disposition)
    finalizer = save(codec, directory, "finalizer.json", {
        "kind": "ROOT_CONTROL_KNOWN_PARTIAL_FINALIZED", "PID": 888,
        "disposition": disposition_ref, "before": closure["rootTerminal"],
        "after": {"path": "intentionally-unreadable-mutable-CONTROL.json", "bytes": 1, "sha256": "f" * 64},
        "noDataOrNetworkCalls": True, "oldController1AndManaged2Preserved": True})
    closure["leaseClosed"] = save(codec, directory, "finalizer-lease.json", {
        "PID": 888, "held": False, "noDataCalls": True})
    raw = save(codec, directory, "raw-delivery.json", {
        "kind": "ROOT_RAW60_DESTINATION_VERIFIED_REFERENCE_WORK_STILL_PENDING",
        "invocation": daily["invocation"], "postcheck": closure["postcheck"],
        "physicalDataTerminal": closure["terminal"], "dataActualNative": 2,
        "dataStatus": "baselines_pending", "sourceFailed": False,
        "all60RawDestinationVerified": True, "all63DataNativeProcessesTerminal": True,
        "referencePackageComplete": False, "noMoreGarminFromThisRun": True})
    completion = {"kind": "ROOT_SOURCE05_RAW_DELIVERED_REFERENCE_PENDING_EXACT14_RESTORED",
        "physicalClosure": physical_ref, "controlFinalization": finalizer,
        "controlFinalizerLeaseClosed": closure["leaseClosed"], "rawDelivery": raw,
        "restoreResult": closure["restore"], "restoreLeaseClosed": restore_lease,
        "sourceRootOuterNative": 1, "managedDataNative": 2, "lastManagedReferenceNative": 2,
        "sourceFailed": False, "primaryHoldCauseUnobserved": True,
        "oldFullSpanHoldPreserved": True, "referenceCohortRemaining": 276,
        "wholeReferenceComplete": False, "fullHistoryComplete": False,
        "all14Restored": True, "allLiveProcessesTerminal": True,
        "restoreOwnOuterNative": 0, "controlFinalizerOwnNative": 0,
        "dataOrRootRerunPerformed": False, "newGarminForbiddenThisRun": True,
        "lastPostcheck": closure["postcheck"]}
    closure["rootCompletion"] = save(codec, directory, "completion.json", completion)
    proof["laterDailyClosure"] = save(codec, directory, "partial-closure.json", closure)
    return proof, closure


def test_known_partial_raw_delivery_preserves_controller_failure(codec, baseline, tmp_path):
    proof, closure = partial_graph(codec, tmp_path, baseline)
    protected = [Path(proof[key]["path"]) for key in
                 ("spanMeterPrefix", "sourceMeterPrefix", "currentSourceMeterPrefix", "checkpointBefore", "currentState")]
    before = [path.read_bytes() for path in protected]
    result, _, _ = codec.validate_span_release_transition(save(codec, tmp_path, "transition.json", proof))
    assert result["laterDailyClosure"] == proof["laterDailyClosure"]
    assert closure["actualOuterNative"] == 1 and closure["dataNative"] == 2
    assert load(closure["outcome"])["normalCLIStatus"]["daily_status"] == "baselines_pending"
    assert load(closure["rootCompletion"])["referenceCohortRemaining"] == 276
    assert result["originalTransition"] == baseline["originalTransition"]
    assert load(result["oldClosure"])["actualOuterNative"] == 1
    assert [path.read_bytes() for path in protected] == before


def test_legacy_closed_daily_contract_is_preserved(codec, baseline, tmp_path):
    before = codec.encoded(baseline)
    result, _, _ = codec.validate_span_release_transition(save(codec, tmp_path, "legacy.json", baseline))
    assert codec.encoded(result) == before
    assert load(result["laterDailyClosure"])["actualOuterNative"] == 0


@pytest.fixture(scope="session")
def qualified_partial(codec, baseline, tmp_path_factory):
    directory = tmp_path_factory.mktemp("qualified-partial")
    proof, closure = partial_graph(codec, directory, baseline)
    codec.validate_span_release_transition(save(codec, directory, "qualified.json", proof))
    return proof, closure


@pytest.mark.parametrize("fault", ["source-failed", "unknown-cleanup", "raw-not-verified", "restore-wrong",
                                 "finalizer-before-other-executor"])
def test_known_partial_rejects_unqualified_delivery_without_mutation(
    codec, qualified_partial, tmp_path, fault
):
    proof, closure = copy.deepcopy(qualified_partial)
    completion = load(closure["rootCompletion"])
    if fault == "source-failed":
        outcome = load(closure["outcome"])
        outcome["sourceFailed"] = True
        closure["outcome"] = save(codec, tmp_path, "failed-outcome.json", outcome)
    elif fault == "unknown-cleanup":
        physical = load(completion["physicalClosure"])
        physical["allNativeTreesPhysicallyTerminal"] = None
        completion["physicalClosure"] = save(codec, tmp_path, "unknown-cleanup.json", physical)
    elif fault == "raw-not-verified":
        raw = load(completion["rawDelivery"])
        raw["all60RawDestinationVerified"] = False
        completion["rawDelivery"] = save(codec, tmp_path, "raw-not-verified.json", raw)
    elif fault == "restore-wrong":
        restored = load(closure["restore"])
        original_state = restored["states"][0]["state"]
        restored["states"][0]["state"] = "active" if original_state != "active" else "disabled_manually"
        assert restored["states"][0]["state"] != original_state
        closure["restore"] = save(codec, tmp_path, "wrong-restore.json", restored)
        completion["restoreResult"] = closure["restore"]
        finalizer = load(completion["controlFinalization"])
        disposition = load(finalizer["disposition"])
        disposition["restore"] = closure["restore"]
        finalizer["disposition"] = save(codec, tmp_path, "wrong-disposition.json", disposition)
        completion["controlFinalization"] = save(codec, tmp_path, "wrong-finalizer.json", finalizer)
    else:
        finalizer = load(completion["controlFinalization"])
        before_state = load(finalizer["before"])
        before_state["currentLiveExecutor"]["callerPID"] += 1
        finalizer["before"] = save(codec, tmp_path, "other-executor.json", before_state)
        disposition = load(finalizer["disposition"])
        disposition["stateBefore"] = finalizer["before"]
        finalizer["disposition"] = save(codec, tmp_path, "other-disposition.json", disposition)
        completion["controlFinalization"] = save(codec, tmp_path, "other-finalizer.json", finalizer)
    closure["rootCompletion"] = save(codec, tmp_path, "completion.json", completion)
    proof["laterDailyClosure"] = save(codec, tmp_path, "closure.json", closure)
    protected = [Path(proof[key]["path"]) for key in (
        "spanMeterPrefix", "sourceMeterPrefix", "currentSourceMeterPrefix", "checkpointBefore", "currentState")]
    before = [path.read_bytes() for path in protected]
    with pytest.raises(codec.Stop, match="^rolling_span_meter_link_changed$"):
        codec.validate_span_release_transition(save(codec, tmp_path, "ineligible.json", proof))
    assert [path.read_bytes() for path in protected] == before


def test_legacy_outer_failure_does_not_gain_generic_adoption(codec, baseline, tmp_path):
    proof = copy.deepcopy(baseline)
    closure = load(proof["laterDailyClosure"])
    closure["actualOuterNative"] = 1
    proof["laterDailyClosure"] = save(codec, tmp_path, "untyped-failure.json", closure)
    before = [Path(proof[key]["path"]).read_bytes() for key in (
        "spanMeterPrefix", "sourceMeterPrefix", "currentSourceMeterPrefix", "checkpointBefore", "currentState")]
    with pytest.raises(codec.Stop, match="^rolling_span_meter_link_changed$"):
        codec.validate_span_release_transition(save(codec, tmp_path, "untyped-transition.json", proof))
    assert [Path(proof[key]["path"]).read_bytes() for key in (
        "spanMeterPrefix", "sourceMeterPrefix", "currentSourceMeterPrefix", "checkpointBefore", "currentState")] == before
