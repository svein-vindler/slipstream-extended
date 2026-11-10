"""Explicit, source-free completion of one physically closed deadline stop.

Historical failure and time are retained. A new phase spends the same account
allowance and qualifies current destinations before the normal publisher runs.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .history_state import Stop, digest, encoded, read, write

KIND = "STOPPED_BULK_COMPLETION_V1"
FIELDS = {"schema", "kind", "new_invocation", "job", "plan", "scope", "workset_manifest",
          "source_seal", "build_seal", "account", "resource_plan_ref", "checkpoint_prefix",
          "verified_postchecks", "expected_verified_products", "stopped", "started_utc", "deadline_utc"}
STOPPED_FIELDS = {"binding", "control", "stop_restore", "child_terminal", "controller_exit",
                  "client_closed", "exact_restore", "lease_closed"}


def need(value, category="bulk_completion_unqualified"):
    if not value:
        raise Stop(category)


def pin(reference, maximum=16777216):
    need(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}
         and type(reference["bytes"]) is int and 0 < reference["bytes"] <= maximum
         and isinstance(reference["path"], str) and Path(reference["path"]).is_absolute()
         and re.fullmatch(r"[a-f0-9]{64}", reference["sha256"] or ""), "bulk_completion_reference")
    path = Path(reference["path"])
    need(path.is_file() and not path.is_symlink() and not path.is_junction(), "bulk_completion_reference")
    body = read(path, maximum)
    need(len(body) == reference["bytes"] and digest(body) == reference["sha256"], "bulk_completion_changed")
    return body


def file_ref(path):
    body = read(path)
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def utc(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        need(result.tzinfo is not None and result.utcoffset().total_seconds() == 0,
             "bulk_completion_deadline")
        return result
    except (TypeError, ValueError, AttributeError):
        raise Stop("bulk_completion_deadline") from None


def validate_request(reference):
    """Pure pinned history qualification, not a grant or a live observation."""
    value = json.loads(pin(reference))
    need(type(value) is dict and set(value) == FIELDS and value["schema"] == 1
         and value["kind"] == KIND and isinstance(value["new_invocation"], str)
         and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value["new_invocation"])
         and Path(value["job"]).is_absolute() and type(value["stopped"]) is dict
         and set(value["stopped"]) == STOPPED_FIELDS
         and type(value["expected_verified_products"]) is int
         and 0 < value["expected_verified_products"] < 2436
         and isinstance(value["verified_postchecks"], list) and value["verified_postchecks"],
         "bulk_completion_request")
    for key in ("plan", "scope", "workset_manifest", "source_seal", "build_seal", "account",
                "resource_plan_ref"):
        pin(value[key])
    prefix(value["checkpoint_prefix"], value["checkpoint_prefix"]["path"])
    stopped = {key: json.loads(pin(item)) for key, item in value["stopped"].items()}
    for item in value["verified_postchecks"]:
        pin(item)
    old, anchor, close = stopped["binding"], stopped["stop_restore"], stopped["client_closed"]
    original = json.loads(pin(value["account"]))
    need(old.get("operation") == "rolling_bulk" and old.get("invocation") != value["new_invocation"]
         and old.get("job") == value["job"] and old.get("plan") == value["plan"]
         and old.get("bulkScope") == value["scope"]
         and anchor.get("kind") == "ROOT_LOCAL07_STOP_AND_EXACT14_RESTORE_DATA_HOLD"
         and anchor.get("originalInvocation") == old["invocation"]
         and type(anchor.get("controllerOuterNative")) is int and anchor["controllerOuterNative"] == 1
         and type(anchor.get("publicationChildNative")) is int and anchor["publicationChildNative"] == 2
         and anchor.get("allOriginalLiveTreesPhysicallyTerminal") is True
         and anchor.get("allOriginalHandlesAndLeaseLegitimatelyClosed") is True
         and anchor.get("all14OriginalStatesExactlyRestored") is True
         and anchor.get("workflowRestoreNative") == 0
         and anchor.get("producerPrimaryFailure") == "bulk_resource_deadline"
         and anchor.get("accountFailedPreserved") == original.get("failed") == "bulk_storage_unknown"
         and anchor.get("originalDeadlineUTC") == original.get("deadline_utc")
         and anchor.get("stillVerifiedProducts") == value["expected_verified_products"]
         and anchor.get("controllerExit") == value["stopped"]["controller_exit"]
         and close.get("ownedSDKClosed") is True and close.get("componentCleanupConfirmed") is True
         and close.get("primaryFailureCategory") == "bulk_resource_deadline"
         and close.get("cleanupFailureCategory") is None
         and original.get("pending_wire") is None,
         "bulk_completion_stopped_history")
    native = stopped["child_terminal"]
    controller = stopped["controller_exit"]
    child_observation = json.loads(pin(controller["childObservation"]))
    stages = child_observation.get("stages", [])
    physical = (child_observation == native or any(
        isinstance(item, dict) and item.get("terminalObserved") is True
        and isinstance(item.get("proof"), dict)
        and all(item["proof"].get(key) == native.get(key) for key in (
            "actualNative", "allNativeDescendantsTerminal", "activeNativeProcesses",
            "parentPhysicallyTerminal", "parentPID", "totalNativeProcesses")) for item in stages))
    need(type(native.get("actualNative")) is int and native["actualNative"] == 2
         and native.get("allNativeDescendantsTerminal") is True
         and type(native.get("activeNativeProcesses")) is int and native["activeNativeProcesses"] == 0
         and native.get("parentPhysicallyTerminal") is True
         and type(controller.get("actualControllerOuterExit")) is int and controller["actualControllerOuterExit"] == 1
         and controller.get("controllerPhysicallyExited") is True and physical,
         "bulk_completion_physical_stop")
    start, deadline = utc(value["started_utc"]), utc(value["deadline_utc"])
    need(utc(original["deadline_utc"]) <= start < deadline
         and 0 < (deadline - start).total_seconds() <= 21600, "bulk_completion_deadline")
    return value


def create_request(*, output, **fields):
    """Create-only concrete input; it confers no Root/live authorization."""
    need(set(fields) == FIELDS - {"schema", "kind"}, "bulk_completion_request")
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(encoded({"schema": 1, "kind": KIND, **fields}))
    reference = file_ref(path)
    validate_request(reference)
    return reference


def prefix(reference, path):
    need(Path(reference["path"]).resolve() == Path(path).resolve(), "bulk_completion_prefix")
    body = read(path, 16777216)
    need(len(body) >= reference["bytes"] and digest(body[:reference["bytes"]]) == reference["sha256"],
         "bulk_completion_prefix")


@dataclass(frozen=True)
class SealedCompletionRead:
    reference_body: bytes
    workset_manifest: dict
    source_seal: dict
    build_seal: dict
    hold_prefix: tuple

    def revalidate(self, workset):
        reference = json.loads(self.reference_body)
        value = json.loads(pin(reference))
        # open() validated the workset descriptor; assert_sealed() below still
        # pins both seals. Do not reread three large manifests per product.
        need(self.workset_manifest == value["workset_manifest"]
             and workset._source_seal == self.source_seal == value["source_seal"]
             and workset._build_seal == self.build_seal == value["build_seal"]
             and tuple(encoded(row) for row in workset._holds) == self.hold_prefix,
             "bulk_completion_sealed_read_changed")
        return True


def qualify_workset(workset, reference):
    value = validate_request(reference)
    request = json.loads(read(workset.folder / "REQUEST.json"))
    need(value["workset_manifest"] == file_ref(workset.folder / "WORKSET.json")
         and value["source_seal"] == workset._source_seal
         and value["build_seal"] == workset._build_seal
         and request["scope"] == value["scope"]
         and workset.binding["job_plan_sha256"] == value["plan"]["sha256"]
         and workset.folder.is_relative_to(Path(value["job"]).resolve()), "bulk_completion_workset")
    # The workset need only be a descendant of its original job, not its direct child.
    prefix(value["checkpoint_prefix"], workset.folder / "PUBLICATION-CHECKPOINTS.jsonl")
    return SealedCompletionRead(encoded(reference), value["workset_manifest"], value["source_seal"],
                                value["build_seal"], tuple(encoded(row) for row in workset._holds))


def attach_account(account, reference):
    """Append the explicit phase; retain the original failure, time and pool."""
    value = validate_request(reference)
    original = json.loads(pin(value["account"]))
    need(original["scope_sha256"] == account.scope_sha
         and account.state["scope_sha256"] == original["scope_sha256"]
         and account.state["carried"] == original["carried"]
         and account.state["deadline_utc"] == original["deadline_utc"]
         and account.state["failed"] == original["failed"]
         and account.state["pending_wire"] is None
         and account.state["plans"] == original["plans"]
         and account.state["active_plan"] == original["active_plan"] == value["resource_plan_ref"],
         "bulk_completion_account_changed")
    if "completion" not in account.state:
        need(account._body == pin(value["account"]), "bulk_completion_account_changed")
        need(utc(value["started_utc"]) <= datetime.now(UTC) < utc(value["deadline_utc"]),
             "bulk_completion_deadline")
        account.state["completion"] = {"kind": KIND, "reference": reference,
            "origin_account": value["account"], "started_utc": value["started_utc"],
            "deadline_utc": value["deadline_utc"], "failed": None}
        account.checkpoint()
    else:
        current = account.state["completion"]
        need(current.get("kind") == KIND and current.get("reference") == reference
             and current.get("origin_account") == value["account"]
             and current.get("started_utc") == value["started_utc"]
             and current.get("deadline_utc") == value["deadline_utc"], "bulk_completion_account_changed")
        for key in ("get", "list", "put"):
            need(account.execution_context["counts"][key] >= original["context"]["counts"][key],
                 "bulk_completion_account_changed")
        for key in ("body_bytes", "independent_gets", "bulk_stages"):
            need(account.execution_context[key] >= original["context"][key], "bulk_completion_account_changed")


def check_account(account):
    phase = account.state.get("completion", {})
    need(phase.get("reference") == account.completion_ref and phase.get("failed") is None
         and account.state.get("failed") == "bulk_storage_unknown", "bulk_resource_account_held")
    need(utc(phase["started_utc"]) <= datetime.now(UTC) < utc(phase["deadline_utc"]),
         "bulk_resource_deadline")


def completion_directory(workset, reference):
    return workset.folder / "completions" / digest(encoded(reference))


def publication_intent(workset, reference):
    if reference is None:
        return workset.folder / "PUBLICATION-INTENT.json"
    return completion_directory(workset, reference) / "PUBLICATION-INTENT.json"


def original_producer_receipts(value):
    """Index only the explicitly frozen local producer artifacts, never R2 LIST."""
    anchor = json.loads(pin(value["stopped"]["stop_restore"]))
    handoff = json.loads(pin(anchor["snapshotHandoff"]))
    catalog_ref = handoff["allProducerArtifactPins"]
    catalog = json.loads(pin(catalog_ref))
    need(type(catalog.get("files")) is list and len(catalog["files"]) <= 100000,
         "bulk_completion_producer_manifest")
    refs = {str(Path(row["path"]).resolve()): row for row in catalog["files"]}
    need(len(refs) == len(catalog["files"]), "bulk_completion_producer_manifest")
    command = Path(catalog["command"]).resolve()
    need(command.is_relative_to(Path(json.loads(pin(value["stopped"]["binding"]))["bulkWorkset"]).resolve()),
         "bulk_completion_producer_manifest")
    by_key = {}
    for path, ref in refs.items():
        item = Path(path)
        if item.name != "RESULT.json" or not item.parent.name.startswith("batch-"):
            continue
        result = json.loads(pin(ref))
        need(item.is_relative_to(command) and result.get("kind") == "reference-bulk-batch-result"
             and result.get("build_seal") == value["build_seal"], "bulk_completion_original_receipt")
        terminal_ref = refs.get(str(item.with_name("TERMINAL.json")))
        if terminal_ref is not None:
            terminal = json.loads(pin(terminal_ref))
            need(terminal.get("result") == ref, "bulk_completion_original_receipt")
        for row in result["generated_targets"]:
            observation = row.get("producer_observation")
            need(observation == refs.get(str(Path(observation["path"]).resolve())),
                 "bulk_completion_original_receipt")
            pin(observation)
            by_key.setdefault(row["key"], []).append((row, [ref, *([terminal_ref] if terminal_ref else [])]))
    return catalog_ref, by_key


def current_completion_cleanup(workset, reference, phase):
    root = completion_directory(workset, reference)
    if phase == "reconcile":
        path = root / "RECONCILIATION-CLIENT-CLOSED.json"
    else:
        intent = json.loads(read(publication_intent(workset, reference)))
        path = Path(intent["command"]) / "CLIENT-CLOSED.json"
    value = json.loads(read(path))
    need(value.get("ownedSDKClosed") is True and value.get("componentCleanupConfirmed") is True,
         "bulk_completion_cleanup_unknown")
    return file_ref(path)


class CompletionDestinationFence:
    """A later publisher GET must not turn a new valid destination into old cache."""
    def __init__(self, store, workset, keys):
        self.store, self.workset, self.keys = store, workset, keys

    def __getattr__(self, name):
        return getattr(self.store, name)

    def new_stage(self):
        return type(self)(self.store.new_stage(), self.workset, self.keys)

    def get_snapshot(self, key, **kwargs):
        from .rolling_span_meter_proof import _semantic_load, _semantic_same

        snapshot = self.store.get_snapshot(key, **kwargs)
        if key in self.keys:
            product = _semantic_load(snapshot.data)
            sealed = _semantic_load(self.workset.product_for(key)["body"].encode())
            need(snapshot.content_type == "application/json" and snapshot.encoding is None
                 and _semantic_same({name: item for name, item in product.items() if name != "resources"},
                                   {name: item for name, item in sealed.items() if name != "resources"}),
                 "bulk_completion_destination_conflict")
        return snapshot


class _SourceFenceCleanupUnknown(Stop):
    def __init__(self):
        super().__init__("bulk_completion_cleanup_unknown")


def _source_fence_groups(proofs):
    """Merge overlapping sealed proofs under the ordinary paired LIST bound."""
    from .native_night import NativeInputError
    from .night_baseline_refresh import _combined_reference_proof
    from .night_reference_sources import rolling_fence_list_bound

    groups, keys, combined = [], [], {"objects": {}, "lists": {}}
    for key, proof in proofs.items():
        candidate = _combined_reference_proof([{"proof": combined}, {"proof": proof}])
        try:
            fits = 0 < 2 * rolling_fence_list_bound(candidate) <= 100
        except NativeInputError:
            fits = False
        if keys and not fits:
            groups.append((keys, combined))
            keys, candidate = [], proof
        need(0 < 2 * rolling_fence_list_bound(candidate) <= 100,
             "bulk_completion_source_fence_bound")
        keys.append(key)
        combined = candidate
    if keys:
        groups.append((keys, combined))
    return groups


class _FreshSourceFence:
    """Observe actual guarded LIST calls; no inventory is reused between fences."""
    def __init__(self, io, guard):
        self.io, self.guard, self.calls = io, guard, []

    def list_revisions_bounded(self, prefix, *, max_keys):
        before = self.guard.counts["list"]
        rows = self.io.list_revisions_bounded(prefix, max_keys=max_keys)
        pages = self.guard.counts["list"] - before
        need(0 < pages <= (max_keys + 999) // 1000, "bulk_completion_source_fence_unmetered")
        self.calls.append({"prefix": prefix, "max_keys": max_keys, "pages": pages})
        return rows


def _qualify_reused_sources(store, account, workset, completion_ref, resource_plan_ref, out, proofs):
    """Use the existing fresh pre/post contract before readonly reuse qualifies."""
    from .history_publish import StorageGuard, _ReferenceStore
    from .night_reference_sources import validate_rolling_sources

    references = []
    for index, (keys, proof) in enumerate(_source_fence_groups(proofs)):
        folder = out / f"source-fence-{index:04d}"
        folder.mkdir()
        write(folder / "SOURCE-PROOF.json", proof)
        proof_ref = file_ref(folder / "SOURCE-PROOF.json")
        guard = StorageGuard(store.new_stage(), set(), folder,
            execution_context=account.execution_context, bulk_account=account)
        observations = {}
        try:
            io = _ReferenceStore(guard.store, guard)
            for phase in ("pre_qualification", "post_qualification"):
                fresh = _FreshSourceFence(io, guard)
                validate_rolling_sources(fresh, proof)
                observations[phase] = fresh.calls
                # The ordinary unchanged-product path also has no product PUT
                # between its two fresh fences. These bracket qualification,
                # not the earlier all-pending destination observations.
        finally:
            try:
                guard.close()
            except BaseException:
                raise _SourceFenceCleanupUnknown() from None
        need(guard.counts["get"] == guard.counts["put"] == 0 and guard.body_bytes == 0,
             "bulk_completion_source_fence_write")
        write(folder / "QUALIFIED.json", {"schema": 1,
            "kind": "FRESH_COMPLETION_SOURCE_QUALIFICATION_GROUP_V1",
            "completion_ref": completion_ref, "scope_sha256": workset.scope_sha256,
            "build_seal": workset.assert_sealed(), "resource_plan_ref": resource_plan_ref,
            "target_keys": keys, "source_proof": proof_ref,
            "pre_qualification": observations["pre_qualification"],
            "post_qualification": observations["post_qualification"],
            "physical_counts": dict(guard.counts), "guardClosed": True,
            "wire": [file_ref(path) for path in sorted(folder.glob("wire-*.json"))]})
        references.append(file_ref(folder / "QUALIFIED.json"))
    return references


def _validate_source_fences(workset, value, result, proofs):
    """Replay bound metered fence receipts; this is not a new fresh observation."""
    from .night_reference_sources import _proof_prefixes

    groups = _source_fence_groups(proofs)
    refs = result.get("source_fences")
    need(type(refs) is list and len(refs) == len(groups), "bulk_completion_source_fence_required")
    root = completion_directory(workset, result["completion_ref"])
    for reference, (keys, proof) in zip(refs, groups, strict=True):
        observed = json.loads(pin(reference))
        need(Path(reference["path"]).resolve().is_relative_to(root)
             and observed.get("schema") == 1
             and observed.get("kind") == "FRESH_COMPLETION_SOURCE_QUALIFICATION_GROUP_V1"
             and observed.get("completion_ref") == result["completion_ref"]
             and observed.get("scope_sha256") == workset.scope_sha256
             and observed.get("build_seal") == value["build_seal"]
             and observed.get("resource_plan_ref") == value["resource_plan_ref"]
             and observed.get("target_keys") == keys and observed.get("guardClosed") is True
             and encoded(json.loads(pin(observed["source_proof"]))) == encoded(proof),
             "bulk_completion_source_fence_changed")
        expected = sorted(_proof_prefixes(proof).items())
        count = 0
        for phase in ("pre_qualification", "post_qualification"):
            calls = observed.get(phase)
            need(type(calls) is list and len(calls) == len(expected), "bulk_completion_source_fence_changed")
            for call, (prefix, maximum) in zip(calls, expected, strict=True):
                need(call.get("prefix") == prefix and call.get("max_keys") == maximum
                     and type(call.get("pages")) is int and 0 < call["pages"] <= (maximum + 999) // 1000,
                     "bulk_completion_source_fence_changed")
                count += call["pages"]
        need(0 < count <= 100 and observed.get("physical_counts") == {"get": 0, "list": count, "put": 0}
             and type(observed.get("wire")) is list and len(observed["wire"]) == count,
             "bulk_completion_source_fence_changed")
        for sequence, wire in enumerate(observed["wire"], 1):
            row = json.loads(pin(wire))
            need(Path(wire["path"]).resolve().parent == Path(reference["path"]).resolve().parent
                 and row.get("kind") == "list"
                 and row.get("counts") == {"get": 0, "list": sequence, "put": 0},
                 "bulk_completion_source_fence_changed")


def validate_completion_receipt(workset, reference):
    """Accept a fresh read observation honestly; never bless an old failed producer."""
    from .rolling_span_meter_proof import _semantic_load, _semantic_same

    receipt = json.loads(pin(reference))
    need(receipt.get("kind") == "reference-bulk-reconciliation-receipt"
         and receipt.get("scope_sha256") == workset.scope_sha256
         and receipt.get("status") == "complete" and receipt.get("ownedSDKClosed") is True
         and receipt.get("componentCleanupConfirmed") is True, "bulk_completion_receipt")
    completion_ref = receipt["completion_ref"]
    value = validate_request(completion_ref)
    need(Path(reference["path"]).resolve().is_relative_to(completion_directory(workset, completion_ref)),
         "bulk_completion_receipt")
    closed, result = json.loads(pin(receipt["clientClose"])), json.loads(pin(receipt["result"]))
    need(closed.get("ownedSDKClosed") is True and closed.get("componentCleanupConfirmed") is True
         and closed.get("primaryFailureCategory") is None and closed.get("cleanupFailureCategory") is None
         and closed.get("completion_ref") == completion_ref
         and result.get("kind") == "reference-bulk-reconciliation-result"
         and result.get("completion_ref") == completion_ref
         and result.get("build_seal") == value["build_seal"] == workset.assert_sealed()
         and result.get("scope_sha256") == workset.scope_sha256
         and result.get("resource_plan_ref") == receipt.get("resource_plan_ref") == value["resource_plan_ref"],
         "bulk_completion_receipt")
    proofs = {}
    for row in result["generated_targets"]:
        observation = json.loads(pin(row["producer_observation"]))
        body = pin(observation["body"])
        artifact = workset.product_for(row["key"])
        sealed = _semantic_load(artifact["body"].encode())
        proofs[row["key"]] = artifact["source_proof"]
        actual = _semantic_load(body)
        need(observation.get("kind") == "FRESH_COMPLETION_DESTINATION_MATCH_V1"
             and observation.get("completion_ref") == completion_ref
             and observation.get("key") == row["key"]
             and observation.get("revision") == row["revision"]
             and digest(body) == row["sha256"] == observation.get("sha256")
             and observation.get("content_type") == "application/json" and observation.get("encoding") is None
             and _semantic_same({key: item for key, item in actual.items() if key != "resources"},
                                   {key: item for key, item in sealed.items() if key != "resources"}),
             "bulk_completion_destination_changed")
        for old in observation["original_receipts"]:
            pin(old)
    _validate_source_fences(workset, value, result, proofs)
    return result


def read_destination_batches(store, account, folder, keys):
    """Known single-object results; rotate the real physical guard every32GETs."""
    from botocore.exceptions import ClientError

    from .history_publish import StorageGuard, _ReferenceStore
    from .r2_store import missing_object

    need(len(keys) <= 2436 and len(set(keys)) == len(keys), "bulk_completion_target_bound")
    guard = None
    try:
        for index, key in enumerate(keys):
            account.check_deadline()
            if index % 32 == 0:
                if guard is not None:
                    guard.close()
                batch = Path(folder) / ("reads-" + str(index // 32))
                batch.mkdir()
                guard = StorageGuard(store.new_stage(), set(), batch,
                    execution_context=account.execution_context, bulk_account=account)
            try:
                snapshot = _ReferenceStore(guard.store, guard).get_snapshot(key, max_bytes=262144)
            except ClientError as error:
                status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if not (missing_object(error) and type(status) is int and status == 404):
                    raise
                snapshot = None
            yield [(key, snapshot)]
    finally:
        if guard is not None:
            guard.close()


def reconcile_bulk(job, plan_sha, scope_ref, folder, *, resource_plan_ref, carried,
                   completion_ref, store_factory=None):
    """Fresh bounded GETs of every unverified product, before any product PUT."""
    from .reference_bulk import (
        _publication_checkpoint,
        _publication_result,
        _publication_state,
        load_bulk_workset,
    )
    from .reference_bulk_budget import BulkAccount
    from .rolling_reference_cli import _open_store, _target
    from .rolling_span_meter_proof import _semantic_load, _semantic_same

    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder, completion_ref=completion_ref)
    value = validate_request(completion_ref)
    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried,
                               deadline_utc=scope["deadline_utc"], completion_ref=completion_ref)
    account.activate(resource_plan_ref)
    need(resource_plan_ref == value["resource_plan_ref"], "bulk_completion_plan_changed")
    state = _publication_state(workset)
    need(set(state["published"]) == set(state["verified"]), "bulk_completion_prefix")
    producer_manifest, originals = original_producer_receipts(value)
    pending = [target for target in workset.targets if target["key"] not in state["verified"]]
    need(len(state["verified"]) == value["expected_verified_products"], "bulk_completion_prefix")
    out = completion_directory(workset, completion_ref)
    out.mkdir(parents=True, exist_ok=False)
    write(out / "RECONCILIATION-INTENT.json", {"completion_ref": completion_ref,
          "targets": [row["key"] for row in pending], "accountBefore": account.snapshot()})
    primary = cleanup = close = reads = None
    closed = False
    generated, missing, observations, proofs = [], [], [], {}
    source_fences = []
    try:
        store, close = _open_store() if store_factory is None else (store_factory(), None)
        close = close or store.client.close
        need(_target(store) == workset.binding["storage_target_sha256"], "storage_target_changed")
        reads = read_destination_batches(store, account, out, [target["key"] for target in pending])
        snapshots = (row for batch in reads for row in batch)
        for index, (target, (key, snapshot)) in enumerate(zip(pending, snapshots, strict=True)):
            need(key == target["key"], "bulk_completion_target_changed")
            if snapshot is None:
                missing.append(target["key"])
                write(out / f"observation-{index:04d}.json", {"kind": "FRESH_COMPLETION_DESTINATION_ABSENT_V1",
                    "completion_ref": completion_ref, "key": target["key"], "httpStatus": 404})
                observations.append(file_ref(out / f"observation-{index:04d}.json"))
                continue
            product = _semantic_load(snapshot.data)
            artifact = workset.product_for(target["key"])
            sealed = _semantic_load(artifact["body"].encode())
            need(snapshot.content_type == "application/json" and snapshot.encoding is None
                 and isinstance(snapshot.revision, str) and snapshot.revision
                 and _semantic_same({key: item for key, item in product.items() if key != "resources"},
                                   {key: item for key, item in sealed.items() if key != "resources"}),
                 "bulk_completion_destination_conflict")
            body_path = out / f"destination-{index:04d}.bin"
            write(body_path, snapshot.data)
            observation = {"kind": "FRESH_COMPLETION_DESTINATION_MATCH_V1", "completion_ref": completion_ref,
                "key": target["key"], "revision": snapshot.revision, "sha256": digest(snapshot.data),
                "content_type": snapshot.content_type, "encoding": snapshot.encoding,
                "body": file_ref(body_path), "original_receipts": []}
            for original, refs in originals.get(target["key"], []):
                need(original.get("revision") == snapshot.revision and original.get("sha256") == digest(snapshot.data)
                     and original.get("args") == target["args"], "bulk_completion_original_destination_changed")
                observation["original_receipts"].extend(refs)
            write(out / f"observation-{index:04d}.json", observation)
            observations.append(file_ref(out / f"observation-{index:04d}.json"))
            generated.append({key: observation[key] for key in ("key", "revision", "sha256")}
                             | {"args": target["args"], "producer_observation": observations[-1]})
            proofs[target["key"]] = artifact["source_proof"]
        source_fences = _qualify_reused_sources(store, account, workset, completion_ref,
                                               resource_plan_ref, out, proofs)
    except BaseException as error:
        primary = error
        if type(error) is _SourceFenceCleanupUnknown:
            cleanup = error
    finally:
        if reads is not None:
            try:
                reads.close()
            except BaseException as error:
                cleanup = error
        if close is not None:
            try:
                close()
                closed = True
            except BaseException as error:
                cleanup = cleanup or error
    closure = {"completion_ref": completion_ref, "ownedSDKClosed": closed,
        "componentCleanupConfirmed": cleanup is None,
        "primaryFailureCategory": getattr(primary, "category", "bulk_completion_reconciliation_unknown") if primary else None,
        "cleanupFailureCategory": "bulk_completion_cleanup_unknown" if cleanup else None}
    write(out / "RECONCILIATION-CLIENT-CLOSED.json", closure)
    if primary is not None or cleanup is not None:
        account.mark_failed(getattr(primary, "category", "bulk_completion_cleanup_unknown"))
        raise primary or cleanup
    result = {"kind": "reference-bulk-reconciliation-result", "scope_sha256": workset.scope_sha256,
        "completion_ref": completion_ref, "build_seal": workset.assert_sealed(),
        "resource_plan_ref": resource_plan_ref, "generated_targets": generated,
        "missing_keys": missing, "observations": observations, "sdk_gets": len(pending), "product_puts": 0,
        "original_producer_manifest": producer_manifest, "source_fences": source_fences}
    write(out / "RECONCILIATION-RESULT.json", result)
    receipt = {"kind": "reference-bulk-reconciliation-receipt", "scope_sha256": workset.scope_sha256,
        "completion_ref": completion_ref, "resource_plan_ref": resource_plan_ref,
        "result": file_ref(out / "RECONCILIATION-RESULT.json"),
        "clientClose": file_ref(out / "RECONCILIATION-CLIENT-CLOSED.json"),
        "status": "complete", "ownedSDKClosed": True, "componentCleanupConfirmed": True}
    write(out / "RECONCILIATION-RECEIPT.json", receipt)
    receipt_ref = file_ref(out / "RECONCILIATION-RECEIPT.json")
    # One successful readonly stage is appended only after every current GET is known.
    terminal = {"schema": 1, "kind": "reference-bulk-stage-terminal", "scope_sha256": workset.scope_sha256,
        "phase": "publication", "resource_plan_ref": resource_plan_ref, "status": "bounded_pending",
        "ownedSDKClosed": True, "componentCleanupConfirmed": True,
        "generated_targets": [{key: row[key] for key in ("key", "args", "revision", "sha256")}
                              | {"producer_receipt": receipt_ref} for row in generated],
        "publication_receipts": [receipt_ref] if generated else []}
    write(out / "RECONCILIATION-TERMINAL.json", terminal)
    if generated:
        _publication_checkpoint(workset, state, "published", file_ref(out / "RECONCILIATION-TERMINAL.json"))
    return {**_publication_result(workset, account, resource_plan_ref, _publication_state(workset)),
            "phase": "reconcile", "reconciliation_ref": file_ref(out / "RECONCILIATION-RESULT.json"),
            "reused_products": len(generated), "missing_products": len(missing)}
