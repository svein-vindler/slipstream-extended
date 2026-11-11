"""Explicit whole-scope stored-reference work; legacy span plans stay separate."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .history_state import Stop, digest, encoded, read, write
from .native_night import zone
from .night_products import calendar_day, normalize_args, storage_key
from .rolling_night_references import LEGACY_WINDOWS, window_selection


def _require(condition, category):
    if not condition:
        raise Stop(category)


def _reference(path):
    body = read(path, 16777216)
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def _pin(reference, maximum=16777216):
    _require(isinstance(reference, dict) and set(reference) == {"path", "bytes", "sha256"}
             and isinstance(reference["path"], str) and type(reference["bytes"]) is int
             and 0 < reference["bytes"] <= maximum
             and re.fullmatch(r"[a-f0-9]{64}", reference["sha256"] or "") is not None,
             "reference_bulk_artifact_unqualified")
    body = read(reference["path"], maximum)
    _require(len(body) == reference["bytes"] and digest(body) == reference["sha256"],
             "reference_bulk_artifact_changed")
    return body


def _windows(declaration):
    if "windows" not in declaration:
        return LEGACY_WINDOWS
    _require(type(declaration["windows"]) is list and declaration["windows"], "reference_bulk_windows")
    try:
        return window_selection(declaration["windows"])
    except ValueError:
        raise Stop("reference_bulk_windows") from None


def _carry_windows(source, destination):
    if "windows" in source:
        destination["windows"] = list(_windows(source))


def _targets(endpoints, windows=LEGACY_WINDOWS):
    result = []
    for endpoint in endpoints:
        for width in window_selection(windows):
            for channel in ("heart_rate", "hrv"):
                args = normalize_args(end_date=endpoint, period="custom" if width == 30 else str(width),
                    start_date=(calendar_day(endpoint) - timedelta(days=width - 1)).isoformat(), channel=channel)
                key = storage_key(args)
                result.append({"target_id": key, "key": key, "args": args,
                               "endpoint": endpoint, "window_days": width, "channel": channel})
    return result


def _affected_targets(job, pairs, universe, endpoints):
    from .rolling_night_references import source_range
    from .rolling_span_source_proof import observed_raw_delivery

    _require(isinstance(pairs, list) and 1 <= len(pairs) <= 64, "reference_bulk_source_delivery_required")
    rows = {}
    for pair in pairs:
        _require(isinstance(pair, dict) and set(pair) == {"binding", "postcheck"},
                 "reference_bulk_source_delivery_required")
        observed = observed_raw_delivery(job, pair["binding"], pair["postcheck"],
            target_sha=universe["storage_target_sha256"])
        _require(all(fact["source_failure"] is False and type(fact["source_episode_native"]) is int
                     and fact["source_episode_native"] in {0, 2} for fact in observed["stage_facts"]),
                 "reference_bulk_source_failure")
        for row in observed["destinations"]:
            identity = (row["key"], row["revision"], row["sha256"])
            kind = "signals" if row["stream"] == "hr" else "hrv"
            rows[identity] = {key: row[key] for key in ("key", "revision", "sha256", "date", "stream")}
            rows[identity]["ranges"] = [{"window_days": width, "first": first, "last": last}
                for width in _windows(universe) for first, last in [source_range(kind, row["date"], width)]]
    proofs = [rows[key] for key in sorted(rows)]
    targets = [target for target in _targets(endpoints, _windows(universe)) if any(
        window["window_days"] == target["window_days"]
        and window["first"] <= target["endpoint"] <= window["last"]
        for row in proofs for window in row["ranges"])]
    return targets, proofs


def _daily_endpoint_universe(job, plan_sha, source_delivery_refs, policy_ref):
    from .rolling_span_source_proof import observed_raw_delivery

    policy = json.loads(_pin(policy_ref))
    fields = {"schema", "kind", "job_plan_sha256", "storage_target_sha256", "today", "timezone",
              "deadline_utc", "latest_allowed_wake_date", "max_source_dates", "max_endpoints", "historical_refs"}
    _require(isinstance(policy, dict) and set(policy) in (fields, fields | {"windows"}) and policy["schema"] == 1
             and policy["kind"] == "DAILY_REFERENCE_ENDPOINT_POLICY_V1"
             and policy["job_plan_sha256"] == plan_sha == job.plan_sha
             and re.fullmatch(r"[a-f0-9]{64}", policy["storage_target_sha256"] or "") is not None
             and type(policy["max_source_dates"]) is int and policy["max_source_dates"] == 30
             and type(policy["max_endpoints"]) is int and policy["max_endpoints"] == 406,
             "reference_bulk_daily_policy_unqualified")
    maximum = max(_windows(policy))
    job.admission(plan_sha, source=False)
    latest, today = calendar_day(policy["latest_allowed_wake_date"]), calendar_day(policy["today"])
    _require(latest <= today and zone(policy["timezone"]) is not None, "reference_bulk_daily_policy_bound")
    deadline = datetime.fromisoformat(policy["deadline_utc"])
    _require(deadline.tzinfo is not None and deadline.utcoffset() == timedelta(0)
             and deadline.astimezone(UTC).isoformat() == policy["deadline_utc"],
             "reference_bulk_fixed_deadline_required")
    _require(isinstance(policy["historical_refs"], list) and len(policy["historical_refs"]) <= 64,
             "reference_bulk_scope_references_bound")
    for reference in policy["historical_refs"]:
        _pin(reference)
    _require(isinstance(source_delivery_refs, list) and 1 <= len(source_delivery_refs) <= 64,
             "reference_bulk_source_delivery_required")
    identity, selected, delivered = None, None, set()
    for pair in source_delivery_refs:
        _require(isinstance(pair, dict) and set(pair) == {"binding", "postcheck"},
                 "reference_bulk_source_delivery_required")
        observed = observed_raw_delivery(job, pair["binding"], pair["postcheck"],
            target_sha=policy["storage_target_sha256"])
        postcheck = json.loads(_pin(pair["postcheck"]))
        selection = postcheck["dailyContext"]["selection"]
        current_identity = (observed["source_binding_sha256"], observed["round"], encoded(selection))
        _require(identity is None or identity == current_identity, "reference_bulk_daily_selection_changed")
        _require(all(fact["source_failure"] is False and type(fact["source_episode_native"]) is int
                     and fact["source_episode_native"] in {0, 2} for fact in observed["stage_facts"]),
                 "reference_bulk_source_failure")
        identity, selected = current_identity, selection["new_dates"]
        delivered.update(observed["verified_raw_dates"])
    _require(isinstance(selected, list) and 1 <= len(selected) <= policy["max_source_dates"]
             and len(set(selected)) == len(selected) and set(selected) <= delivered,
             "reference_bulk_daily_selection_unverified")
    endpoints = set()
    for value in selected:
        first = calendar_day(value)
        calendar_day((first - timedelta(days=maximum - 1)).isoformat())
        _require(first <= latest, "reference_bulk_daily_latest_wake_bound")
        last = min(first + timedelta(days=maximum - 1), latest)
        endpoints.update((first + timedelta(days=index)).isoformat() for index in range((last - first).days + 1))
    _require(len(endpoints) <= policy["max_endpoints"], "reference_bulk_daily_scope_capacity")
    result = {name: policy[name] for name in ("job_plan_sha256", "storage_target_sha256", "today",
        "timezone", "deadline_utc", "historical_refs")}
    result.update(schema=1, kind="reference-bulk-daily-universe", policy_ref=policy_ref,
                  source_delivery_refs=source_delivery_refs, source_dates=sorted(selected), endpoints=sorted(endpoints))
    _carry_windows(policy, result)
    return result


def create_daily_endpoint_scope(job, plan_sha, source_delivery_refs, policy_ref, *, output):
    """Freeze this verified Source cohort's own dates and bounded later rolling endpoints."""
    result = _daily_endpoint_universe(job, plan_sha, source_delivery_refs, policy_ref)
    write(Path(output), result)
    return _reference(output)


def _scope(job, plan_sha, reference):
    scope = json.loads(_pin(reference))
    if isinstance(scope, dict) and scope.get("kind") == "reference-bulk-daily-universe":
        expected = _daily_endpoint_universe(job, plan_sha, scope["source_delivery_refs"], scope["policy_ref"])
        _require(scope == expected, "reference_bulk_daily_universe_changed")
        return scope, scope["endpoints"]
    if isinstance(scope, dict) and scope.get("kind") == "reference-bulk-affected-scope":
        fields = {"schema", "kind", "job_plan_sha256", "storage_target_sha256", "today", "timezone",
                  "deadline_utc", "historical_refs", "source_delivery_refs", "endpoint_scope_ref",
                  "targets", "source_range_proofs"}
        _require(set(scope) in (fields, fields | {"windows"}) and scope["schema"] == 2, "reference_bulk_scope_unqualified")
        universe, endpoints = _scope(job, plan_sha, scope["endpoint_scope_ref"])
        _require(universe["kind"] in {"reference-bulk-scope", "reference-bulk-daily-universe"}
            and (universe["kind"] != "reference-bulk-daily-universe"
                 or universe["source_delivery_refs"] == scope["source_delivery_refs"])
            and ("windows" in scope) == ("windows" in universe) and _windows(scope) == _windows(universe)
            and all(scope[name] == universe[name] for name in
            ("job_plan_sha256", "storage_target_sha256", "today", "timezone", "deadline_utc", "historical_refs")),
            "reference_bulk_affected_scope_changed")
        targets, proofs = _affected_targets(job, scope["source_delivery_refs"], universe, endpoints)
        _require(targets and scope["targets"] == targets and scope["source_range_proofs"] == proofs,
                 "reference_bulk_affected_scope_changed")
        return scope, sorted({target["endpoint"] for target in targets})
    fields = {"schema", "kind", "job_plan_sha256", "storage_target_sha256", "today", "timezone",
              "lower_endpoint", "upper_endpoint", "deadline_utc", "historical_refs", "source_delivery_refs"}
    _require(isinstance(scope, dict) and set(scope) in (fields, fields | {"windows"}) and scope["schema"] == 1
             and scope["kind"] == "reference-bulk-scope" and plan_sha == job.plan_sha
             and scope["job_plan_sha256"] == plan_sha
             and re.fullmatch(r"[a-f0-9]{64}", scope["storage_target_sha256"] or "") is not None,
             "reference_bulk_scope_unqualified")
    job.admission(plan_sha, source=False)
    lower, upper, today = (calendar_day(scope[key]) for key in
                            ("lower_endpoint", "upper_endpoint", "today"))
    calendar_day((lower - timedelta(days=max(_windows(scope)) - 1)).isoformat())
    _require(lower <= upper <= today and (upper - lower).days < 406
             and zone(scope["timezone"]) is not None, "reference_bulk_scope_bound")
    deadline = datetime.fromisoformat(scope["deadline_utc"])
    _require(deadline.tzinfo is not None and deadline.utcoffset() == timedelta(0)
             and deadline.astimezone(UTC).isoformat() == scope["deadline_utc"],
             "reference_bulk_fixed_deadline_required")
    for name in ("historical_refs", "source_delivery_refs"):
        _require(isinstance(scope[name], list) and len(scope[name]) <= 64,
                 "reference_bulk_scope_references_bound")
    for reference in scope["historical_refs"]:
        _pin(reference)
    for pair in scope["source_delivery_refs"]:
        _require(isinstance(pair, dict) and set(pair) == {"binding", "postcheck"},
                 "reference_bulk_source_delivery_required")
        _pin(pair["binding"])
        _pin(pair["postcheck"])
    endpoints = [(lower + timedelta(days=index)).isoformat()
                 for index in range((upper - lower).days + 1)]
    return scope, endpoints


def create_affected_scope(job, plan_sha, source_delivery_refs, endpoint_scope_ref, *,
                          today, timezone, deadline_utc, output):
    """Compile only declared rolling targets affected by independently verified raw delivery."""
    universe, endpoints = _scope(job, plan_sha, endpoint_scope_ref)
    _require(universe["kind"] in {"reference-bulk-scope", "reference-bulk-daily-universe"}
             and (universe["kind"] != "reference-bulk-daily-universe"
                  or universe["source_delivery_refs"] == source_delivery_refs)
             and universe["today"] == today
             and universe["timezone"] == timezone and universe["deadline_utc"] == deadline_utc,
             "reference_bulk_affected_scope_changed")
    targets, proofs = _affected_targets(job, source_delivery_refs, universe, endpoints)
    if not targets:
        return None
    result = {name: universe[name] for name in ("job_plan_sha256", "storage_target_sha256", "today",
        "timezone", "deadline_utc", "historical_refs")}
    result.update(schema=2, kind="reference-bulk-affected-scope", source_delivery_refs=source_delivery_refs,
                  endpoint_scope_ref=endpoint_scope_ref, targets=targets, source_range_proofs=proofs)
    _carry_windows(universe, result)
    write(Path(output), result)
    return _reference(output)


def _seed_source_deliveries(job, workset, scope):
    from .r2_store import R2ObjectSnapshot
    from .rolling_span_source_proof import observed_raw_delivery

    if not scope["source_delivery_refs"]:
        return 0
    selected = {row["key"]: row for row in workset.required_sources()}
    snapshots, objects, facts = {}, {}, []
    for pair in scope["source_delivery_refs"]:
        observed = observed_raw_delivery(job, pair["binding"], pair["postcheck"],
            target_sha=workset.binding["storage_target_sha256"])
        facts.extend(observed["stage_facts"])
        for row in observed["destinations"]:
            key = row["key"]
            if key not in selected:
                continue
            _require(selected[key]["revision"] == row["revision"], "reference_bulk_seed_stale")
            receipt = json.loads(_pin(row["publisher_receipt"]))
            destination = next(item for item in receipt["destinations"] if item["key"] == key
                               and item["revision"] == row["revision"] and item["sha256"] == row["sha256"])
            snapshot = destination["snapshot"]
            path = Path(row["publisher_receipt"]["path"]).parent / snapshot["file"]
            body = read(path, 262144)
            _require(digest(body) == row["sha256"] and len(body) == selected[key]["size"]
                     and (key not in snapshots or snapshots[key].data == body), "reference_bulk_seed_stale")
            snapshots[key] = R2ObjectSnapshot(body, row["revision"], "application/json", "gzip")
            objects[key] = {name: row[name] for name in ("key", "revision", "sha256", "publisher_receipt", "postcheck")}
    provenance = {"schema": 1, "kind": "reference-bulk-observed-raw-source-seed",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": workset.binding["storage_target_sha256"],
        "source_delivery_refs": scope["source_delivery_refs"], "stage_facts": facts,
        "objects": [objects[key] for key in sorted(objects)]}
    _carry_windows(scope, provenance)
    path = workset.folder / "SOURCE-DELIVERY-SEED.json"
    if path.exists():
        _require(read(path) == encoded(provenance), "reference_bulk_seed_changed")
    else:
        write(path, provenance)
    return workset.seed_verified_local(snapshots, _reference(path))


def prepare_bulk(job, plan_sha, scope_ref, folder):
    """Create one finite offline workset; neither source nor R2 client is selected."""
    from .reference_workset import ReferenceWorkset

    scope, endpoints = _scope(job, plan_sha, scope_ref)
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = endpoints
    _carry_windows(scope, binding)
    targets = scope["targets"] if scope["schema"] == 2 else _targets(endpoints, _windows(scope))
    workset = ReferenceWorkset.create(Path(folder), binding, targets, target_subset=scope["schema"] == 2)
    write(workset.folder / "REQUEST.json", {"schema": 1, "kind": "reference-bulk-request",
        "scope": scope_ref, "scope_sha256": workset.scope_sha256,
        "deadline_utc": scope["deadline_utc"], "historical_refs": scope["historical_refs"],
        "source_delivery_refs": scope["source_delivery_refs"]})
    return {"kind": "reference-bulk-status", "status": "prepared",
        "scope_sha256": workset.scope_sha256, "endpoint_count": len(endpoints),
        "target_count": len(targets), "product_puts": 0, "source_calls": 0}


def load_bulk_workset(job, plan_sha, scope_ref, folder, *, completion_ref=None):
    """Revalidate the immutable request and return its local workset and scope."""
    from .reference_workset import ReferenceWorkset

    scope, endpoints = _scope(job, plan_sha, scope_ref)
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = endpoints
    _carry_windows(scope, binding)
    workset = ReferenceWorkset.open(Path(folder), binding)
    _require(workset.targets == (scope["targets"] if scope["schema"] == 2 else _targets(endpoints, _windows(scope))),
             "reference_bulk_declared_targets_changed")
    request = json.loads(read(workset.folder / "REQUEST.json", 1048576))
    _require(request["scope"] == scope_ref and request["scope_sha256"] == workset.scope_sha256
             and request["deadline_utc"] == scope["deadline_utc"], "reference_bulk_request_changed")
    if completion_ref is not None:
        workset.qualify_completion(completion_ref)
    return workset, scope


def _read_stage(workset, scope, phase, resource_plan_ref, carried, store_factory, perform):
    from .history_publish import StorageGuard
    from .night_baseline_refresh import _IO
    from .reference_bulk_budget import BulkAccount
    from .rolling_reference_cli import _open_store, _ReadOnlySources, _target

    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried,
                               deadline_utc=scope["deadline_utc"])
    account.activate(resource_plan_ref)
    _require(account.plan["phase"] == phase, "reference_bulk_resource_phase")
    workset.bind_resource_plan(phase, resource_plan_ref)
    stage = workset.folder / "stages" / uuid.uuid4().hex
    stage.mkdir(parents=True)
    guard = io = close = primary = cleanup = None
    sdk_closed = False
    result = None
    try:
        if store_factory is None:
            store, close = _open_store()
        else:
            store = store_factory()
            close = store.client.close
        _require(_target(store) == workset.binding["storage_target_sha256"],
                 "storage_target_changed")
        guard = StorageGuard(store, set(), stage, execution_context=account.execution_context,
                             bulk_account=account)
        sources = _ReadOnlySources(store, guard, workset.binding["endpoints"], workset.binding["today"],
                                   windows=_windows(workset.binding))
        io = _IO(sources)
        result = perform(io, guard, account, stage)
    except BaseException as error:
        primary = error
    finally:
        if io is not None:
            try:
                io.close()
            except BaseException as error:
                cleanup = error
        if guard is not None:
            try:
                guard.close()
            except BaseException as error:
                cleanup = cleanup or error
        if close is not None:
            try:
                close()
                sdk_closed = True
            except BaseException as error:
                cleanup = cleanup or error
        if primary is not None or cleanup is not None:
            category = getattr(primary, "category", "reference_bulk_stage_failed")
            if cleanup is not None:
                category = "reference_bulk_cleanup_unknown"
            account.mark_failed(category)
            workset.mark_hold(category)
        receipt = {"schema": 1, "kind": "reference-bulk-stage-terminal",
            "scope_sha256": workset.scope_sha256, "phase": phase, "resource_plan_ref": resource_plan_ref,
            "status": "hold" if primary is not None or cleanup is not None else result["status"],
            "counts": dict(guard.counts) if guard is not None else dict.fromkeys(("get", "list", "put"), 0),
            "body_bytes": guard.body_bytes if guard is not None else 0,
            "componentCleanupConfirmed": cleanup is None and sdk_closed,
            "ownedSDKClosed": sdk_closed,
            "primaryFailureCategory": getattr(primary, "category", "reference_bulk_stage_failed")
            if primary is not None else None,
            "cleanupFailureCategory": "reference_bulk_cleanup_unknown" if cleanup is not None else None,
            "usage": account.snapshot()}
        write(stage / "TERMINAL.json", receipt)
        try:
            workset.record_step(_reference(stage / "TERMINAL.json"))
        except BaseException:
            if primary is None and cleanup is None:
                raise
        if primary is not None:
            raise primary
        if cleanup is not None:
            raise cleanup
    return result, account


def inventory_bulk(job, plan_sha, scope_ref, folder, *, resource_plan_ref, carried,
                   store_factory=None):
    """Record complete qualified revision LISTs under one finite account."""
    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder)

    def perform(sources, guard, account, stage):
        before = len(workset.inventory_requests())
        for index, request in enumerate(workset.inventory_requests()):
            pages = (request["max_keys"] + 999) // 1000
            if (guard.counts["list"] + pages > 100 or account.remaining()["class_a"] < pages
                    or guard.raw_bytes + pages * 1048576 > 16777216):
                break
            rows = sources.list_revisions_bounded(request["prefix"], max_keys=request["max_keys"])
            proof = stage / f"INVENTORY-{index:03d}.json"
            write(proof, {"schema": 1, "kind": "reference-bulk-native-inventory",
                "scope_sha256": workset.scope_sha256, "prefix": request["prefix"],
                "max_keys": request["max_keys"], "rows": rows})
            workset.record_inventory(request["prefix"], rows, _reference(proof))
        return {"status": "complete" if not workset.inventory_requests() else "bounded_pending",
                "completed_inventory_prefixes": before - len(workset.inventory_requests())}

    result, account = _read_stage(workset, scope, "inventory", resource_plan_ref, carried,
                                  store_factory, perform)
    return {"status": "inventory_complete" if result["status"] == "complete" else "bounded_pending",
            "scope_sha256": workset.scope_sha256, "source_calls": 0,
            "completed_inventory_prefixes": result["completed_inventory_prefixes"], "usage": account.snapshot()}


def collect_bulk(job, plan_sha, scope_ref, folder, *, resource_plan_ref, carried,
                 store_factory=None, max_items=400):
    """Fetch each selected version once; known capacity stops before fetch intent."""
    _require(type(max_items) is int and 1 <= max_items <= 400, "reference_bulk_fetch_bound")
    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder)
    _seed_source_deliveries(job, workset, scope)

    def perform(sources, guard, account, stage):
        reserved_decoded = 0

        def admit(row):
            nonlocal reserved_decoded
            decoded = (32768 if row["key"].startswith("health/night-context/") else
                       1048576 if row["key"].startswith("health/signals/") else 2097152)
            if (guard.counts["get"] >= 400 or guard.raw_bytes + row["size"] > 16777216
                    or max(guard.decoded_bytes, reserved_decoded) + decoded > 33554432
                    or not account.can_fetch(row)):
                return False
            reserved_decoded += decoded
            return True

        result = workset.fetch_missing(sources, max_items=max_items,
                                       admit_fetch=admit)
        return {"status": "bounded_pending" if result.get("deferred") else "complete",
                "fetched_sources": result["fetched"], "remaining_sources": result["remaining"]}

    result, account = _read_stage(workset, scope, "collection", resource_plan_ref, carried,
                                  store_factory, perform)
    complete = all(row["status"] == "present" for row in workset.required_sources())
    if complete:
        workset.seal_sources()
    return {"status": "sources_complete" if complete else result["status"],
            "scope_sha256": workset.scope_sha256, "source_calls": 0,
            "fetched_sources": result["fetched_sources"], "remaining_sources": result["remaining_sources"],
            "usage": account.snapshot()}


def build_bulk(job, plan_sha, scope_ref, folder, *, carried, max_targets=2436):
    """Stage immutable products from local bytes; no publisher/client is opened."""
    from .native_night import NativeNightReader
    from .night_baseline_refresh import _proof, prepare_reference
    from .reference_bulk_budget import BulkAccount

    _require(type(max_targets) is int and 1 <= max_targets <= 2436, "reference_bulk_build_bound")
    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder)
    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried,
                               deadline_utc=scope["deadline_utc"])
    source_seal = workset.seal_sources()
    resource_plan = workset.status()["resource_plans"].get("collection")
    _require(resource_plan is not None, "reference_bulk_collection_required")
    stage = workset.folder / "stages" / uuid.uuid4().hex
    stage.mkdir(parents=True)
    primary, built = None, 0
    try:
        local = workset.reader_store()
        sources = {row["key"] for row in workset.required_sources()}
        pending = workset.pending_targets()[:max_targets]
        for endpoint in dict.fromkeys(target["endpoint"] for target in pending):
            account.check_deadline()
            targets = [target for target in pending if target["endpoint"] == endpoint]
            key = f"health/sleep/v1/{endpoint[:4]}/{endpoint[5:7]}/{endpoint}.json"
            if key not in sources and key + ".gz" not in sources:
                for target in targets:
                    workset.classify_target(target["target_id"], "sleep_not_stored", source_seal)
                    built += 1
                continue
            last = calendar_day(endpoint)
            maximum = max(target["window_days"] for target in targets)
            days = [(last - timedelta(days=maximum - 1 - index)).isoformat() for index in range(maximum)]
            reader = NativeNightReader(local, timezone=scope["timezone"])
            reader.prime_rolling_sources(days, **({"window_days": 365} if maximum == 365 else {}))
            values = [reader.night(day) for day in days]
            for target in targets:
                account.check_deadline()
                width = target["window_days"]
                artifact = prepare_reference(values[-width:], target["args"], _proof(reader, days[-width:]))
                workset.stage_product(target["target_id"], artifact)
                built += 1
    except BaseException as error:
        primary = error
        workset.mark_hold(getattr(error, "category", "reference_bulk_build_unverifiable"))
    complete = primary is None and not workset.pending_targets()
    status = "hold" if primary is not None else "complete" if complete else "bounded_pending"
    write(stage / "TERMINAL.json", {"schema": 1, "kind": "reference-bulk-stage-terminal",
        "scope_sha256": workset.scope_sha256, "phase": "build", "resource_plan_ref": resource_plan,
        "status": status, "counts": dict.fromkeys(("get", "list", "put"), 0), "body_bytes": 0,
        "componentCleanupConfirmed": True, "primaryFailureCategory":
            getattr(primary, "category", "reference_bulk_build_unverifiable") if primary is not None else None,
        "cleanupFailureCategory": None, "usage": account.snapshot()})
    workset.record_step(_reference(stage / "TERMINAL.json"))
    if primary is not None:
        raise primary
    if complete:
        seal = workset.seal_build()
        return {"status": "build_complete", "built_targets": built, "build_seal": seal,
                **workset.build_summary(), "source_calls": 0, "product_puts": 0}
    return {"status": "bounded_pending", "built_targets": built,
            "remaining_targets": len(workset.pending_targets()), "source_calls": 0, "product_puts": 0}


def _publication_state(workset):
    path = workset.folder / "PUBLICATION-CHECKPOINTS.jsonl"
    state = {"published": {}, "verified": {}, "tip": None, "sequence": 0}
    if not path.exists():
        return state
    body = read(path, 16777216)
    _require(body.endswith(b"\n"), "reference_bulk_checkpoint_torn")
    for line in body.splitlines(keepends=True):
        frame = json.loads(line)
        _require(frame.get("schema") == 1 and frame.get("scope_sha256") == workset.scope_sha256
                 and frame.get("sequence") == state["sequence"] and frame.get("previous") == state["tip"],
                 "reference_bulk_checkpoint_changed")
        value = json.loads(_pin(frame["reference"]))
        if frame["event"] == "published":
            _require(value.get("kind") == "reference-bulk-stage-terminal"
                     and value.get("scope_sha256") == workset.scope_sha256
                     and value.get("phase") == "publication" and value.get("status") != "hold"
                     and value.get("componentCleanupConfirmed") is True and value.get("ownedSDKClosed") is True,
                     "reference_bulk_publication_unverified")
            for target in value["generated_targets"]:
                _pin(target["producer_receipt"])
                _require(target["key"] not in state["published"], "reference_bulk_duplicate_publication")
                state["published"][target["key"]] = target
        elif frame["event"] == "verified":
            _require(value.get("kind") == "ACTUAL_METERED_BULK_POSTCHECK_V1",
                     "reference_bulk_postcheck_required")
            _validate_bulk_postcheck(workset, frame["reference"], value["generated_targets"],
                                     value["publication_receipts"])
            for target in value["generated_targets"]:
                _require(state["published"].get(target["key"]) == target,
                         "reference_bulk_verification_changed")
                state["verified"][target["key"]] = frame["reference"]
        else:
            raise Stop("reference_bulk_checkpoint_changed")
        state["tip"], state["sequence"] = digest(line), state["sequence"] + 1
    return state


def _publication_checkpoint(workset, state, event, reference):
    current = _publication_state(workset)
    _require((current["tip"], current["sequence"]) == (state["tip"], state["sequence"]),
             "reference_bulk_checkpoint_changed")
    body = encoded({"schema": 1, "scope_sha256": workset.scope_sha256,
        "sequence": state["sequence"], "previous": state["tip"], "event": event, "reference": reference})
    with (workset.folder / "PUBLICATION-CHECKPOINTS.jsonl").open("ab") as handle:
        _require(handle.write(body) == len(body), "reference_bulk_checkpoint_short_write")
        handle.flush()
        os.fsync(handle.fileno())


def _publication_result(workset, account, resource_plan_ref, state):
    pending = [state["published"][target["key"]] for target in workset.targets
               if target["key"] in state["published"] and target["key"] not in state["verified"]]
    summary = workset.build_summary()
    receipts = []
    for target in pending:
        if target["producer_receipt"] not in receipts:
            receipts.append(target["producer_receipt"])
    return {"schema": 1, "kind": "reference-bulk-result", "scope_sha256": workset.scope_sha256,
        "phase": "publication", "status": "verification_pending" if pending else
            "complete" if len(state["verified"]) == summary["product_count"] else "bounded_pending",
        "build_seal": summary["build_seal"], "resource_plan_ref": resource_plan_ref,
        "generated_targets": pending, "publication_receipts": receipts,
        "total_products": summary["product_count"],
        "unpublished_products": summary["product_count"] - len(state["published"]),
        "publication_complete": len(state["published"]) == summary["product_count"],
        "published_products": len(state["published"]), "verified_products": len(state["verified"]),
        "remaining_products": summary["product_count"] - len(state["verified"]),
        "known_gap_targets": summary["known_gap_count"], "source_calls": 0, "usage": account.snapshot()}


def publish_bulk(job, plan_sha, scope_ref, folder, *, resource_plan_ref, carried,
                 store_factory=None, max_batches=2436, completion_ref=None):
    """Publish sealed outstanding slots; unverified output alone is not scope completion."""
    from .history_publish import StorageGuard, _ReferenceStore
    from .night_baseline_refresh import refresh_pending, register_rolling_endpoints
    from .reference_bulk_budget import BulkAccount
    from .rolling_reference_cli import _open_store, _target
    from .rolling_reference_span import _CapturedStore

    _require(type(max_batches) is int and 1 <= max_batches <= 2436, "reference_bulk_publication_bound")
    from .reference_completion import completion_directory, publication_intent

    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder, completion_ref=completion_ref)
    workset.assert_sealed()
    state = _publication_state(workset)
    intent_path = publication_intent(workset, completion_ref)
    _require(not intent_path.exists(), "reference_bulk_publication_pending")
    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried,
                               deadline_utc=scope["deadline_utc"], completion_ref=completion_ref)
    account.activate(resource_plan_ref)
    _require(account.plan["phase"] == "publication", "reference_bulk_resource_phase")
    workset.bind_resource_plan("publication", resource_plan_ref)
    summary = _publication_result(workset, account, resource_plan_ref, state)
    if summary["status"] == "complete" or (summary["status"] == "verification_pending"
            and (completion_ref is None or summary["unpublished_products"] == 0)):
        return summary
    if completion_ref is not None:
        from .reference_completion import validate_completion_receipt, validate_request

        completion = validate_request(completion_ref)
        reconciliation = completion_directory(workset, completion_ref) / "RECONCILIATION-RECEIPT.json"
        validate_completion_receipt(workset, _reference(reconciliation))
        _require(completion["resource_plan_ref"] == resource_plan_ref, "bulk_completion_plan_changed")
    sources = {row["key"] for row in workset.required_sources()}
    targets = []
    for target in workset.targets:
        day = target["endpoint"]
        key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
        if key in sources or key + ".gz" in sources:
            workset.product_for(target["key"])
            targets.append(target)
    endpoints = list(dict.fromkeys(target["endpoint"] for target in targets))
    groups = [endpoints[index:index + 7] for index in range(0, len(endpoints), 7)]
    command = (completion_directory(workset, completion_ref) / "publications" if completion_ref is not None
               else workset.folder / "publications") / uuid.uuid4().hex
    command.mkdir(parents=True)
    write(intent_path, {"schema": 1, "kind": "reference-bulk-publication-intent",
        "scope_sha256": workset.scope_sha256, "build_seal": workset.assert_sealed(),
        "resource_plan_ref": resource_plan_ref, "checkpoint": state["tip"], "command": str(command)})
    intent = _reference(intent_path)
    before_counts = dict(account.execution_context["counts"])
    before_bytes = account.execution_context["body_bytes"]
    close = guard = primary = cleanup = None
    sdk_closed, registered, batches, observed = False, set(), [], {}
    try:
        if store_factory is None:
            root_store, close = _open_store()
        else:
            root_store = store_factory()
            close = root_store.client.close
        _require(_target(root_store) == workset.binding["storage_target_sha256"], "storage_target_changed")
        if completion_ref is not None:
            from .reference_completion import CompletionDestinationFence

            root_store = CompletionDestinationFence(root_store, workset,
                {target["key"] for target in targets} - state["published"].keys())
        local = workset.reader_store()
        for ordinal in range(max_batches):
            account.check_deadline()
            remaining = {target["key"] for target in targets} - state["published"].keys() - observed.keys()
            if not remaining:
                break
            group_index = next(index for index, group in enumerate(groups)
                if any(target["endpoint"] in group and target["key"] in remaining for target in targets))
            group = groups[group_index]
            group_keys = {target["key"] for target in targets
                          if target["endpoint"] in group and target["key"] in remaining}
            batch = command / f"batch-{ordinal:04d}"
            batch.mkdir()
            guard = StorageGuard(root_store.new_stage(), set(), batch,
                execution_context=account.execution_context, bulk_account=account)
            capture = _CapturedStore(_ReferenceStore(guard.store, guard), batch / "readbacks",
                                    [{**target, "storage_key": target["key"]} for target in targets])
            batch_primary = None
            try:
                if group_index not in registered:
                    sleep = []
                    for day in group:
                        key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
                        if key not in sources:
                            key += ".gz"
                        snapshot = local.get_snapshot(key, max_bytes=524288)
                        sleep.append({"key": key, "revision": snapshot.revision, "sha256": digest(snapshot.data)})
                    register_rolling_endpoints(capture, sleep, today=scope["today"],
                        prepared_manifest=workset, prepared_target_keys=group_keys)
                    registered.add(group_index)
                report = refresh_pending(capture, today=scope["today"], timezone=scope["timezone"],
                    max_periods=8, period_names=set(), rolling_endpoints=group,
                    prepared_manifest=workset, prepared_target_keys=group_keys)
                if report.get("blocked"):
                    raise Stop("reference_bulk_publication_held")
                generated = [value for key, value in capture.observed.items()
                    if key in remaining and (value["written"] or key in capture.completed_keys)]
                write(batch / "RESULT.json", {"schema": 1, "kind": "reference-bulk-batch-result",
                    "scope_sha256": workset.scope_sha256, "build_seal": workset.assert_sealed(),
                    "resource_plan_ref": resource_plan_ref, "report": report,
                    "generated_targets": generated, "history_receipts": capture.history})
                batches.append(batch)
                observed.update({value["key"]: (value, batch) for value in generated})
                if not generated:
                    _require(report.get("budget_deferred"), "reference_bulk_publication_no_progress")
                    break
            except BaseException as error:
                batch_primary = error
                raise
            finally:
                try:
                    guard.close()
                except BaseException as error:
                    if batch_primary is None:
                        raise
                    cleanup = cleanup or error
                guard = None
    except BaseException as error:
        primary = error
    finally:
        if guard is not None:
            try:
                guard.close()
            except BaseException as error:
                cleanup = error
        if close is not None:
            try:
                close()
                sdk_closed = True
            except BaseException as error:
                cleanup = cleanup or error
    if primary is not None or cleanup is not None:
        account.mark_failed(getattr(primary, "category", "reference_bulk_cleanup_unknown"))
        workset.mark_hold(getattr(primary, "category", "reference_bulk_publication_unknown"))
    write(command / "CLIENT-CLOSED.json", {"scope_sha256": workset.scope_sha256,
        "ownedSDKClosed": sdk_closed, "componentCleanupConfirmed": cleanup is None,
        "primaryFailureCategory": getattr(primary, "category", "reference_bulk_publication_failed")
        if primary is not None else None,
        "cleanupFailureCategory": "reference_bulk_cleanup_unknown" if cleanup is not None else None})
    receipts = {}
    for batch in batches:
        write(batch / "TERMINAL.json", {"schema": 1, "kind": "reference-bulk-publication-receipt",
            "scope_sha256": workset.scope_sha256, "resource_plan_ref": resource_plan_ref,
            "result": _reference(batch / "RESULT.json"), "clientClose": _reference(command / "CLIENT-CLOSED.json"),
            "ownedSDKClosed": sdk_closed, "componentCleanupConfirmed": cleanup is None,
            "status": "hold" if primary is not None or cleanup is not None else "complete"})
        receipts[batch] = _reference(batch / "TERMINAL.json")
    generated = [{**{key: observed[target["key"]][0][key] for key in
                    ("key", "args", "revision", "sha256", "content_type", "encoding", "decoded_sha256")},
                  "producer_receipt": receipts[observed[target["key"]][1]]}
                 for target in targets if target["key"] in observed]
    terminal = {"schema": 1, "kind": "reference-bulk-stage-terminal",
        "scope_sha256": workset.scope_sha256, "phase": "publication", "resource_plan_ref": resource_plan_ref,
        "status": "hold" if primary is not None or cleanup is not None else
            "complete" if len(state["published"]) + len(generated) == summary["total_products"]
            else "bounded_pending",
        "counts": {key: value - before_counts[key] for key, value in account.execution_context["counts"].items()},
        "body_bytes": account.execution_context["body_bytes"] - before_bytes,
        "componentCleanupConfirmed": cleanup is None and sdk_closed, "ownedSDKClosed": sdk_closed,
        "primaryFailureCategory": getattr(primary, "category", "reference_bulk_publication_failed")
        if primary is not None else None,
        "cleanupFailureCategory": "reference_bulk_cleanup_unknown" if cleanup is not None else None,
        "generated_targets": generated, "publication_receipts": list(receipts.values()), "usage": account.snapshot()}
    write(command / "TERMINAL.json", terminal)
    workset.record_step(_reference(command / "TERMINAL.json"))
    if primary is not None:
        raise primary
    if cleanup is not None:
        raise cleanup
    _publication_checkpoint(workset, state, "published", _reference(command / "TERMINAL.json"))
    _require(_reference(intent_path) == intent, "reference_bulk_publication_intent_changed")
    intent_path.unlink()
    return _publication_result(workset, account, resource_plan_ref, _publication_state(workset))


def _after_source_context(workset, proof, binding):
    from .history_state import Job
    from .rolling_span_source_proof import observed_raw_delivery

    context = json.loads(_pin(proof["after_source_ref"]))
    request = json.loads(read(workset.folder / "REQUEST.json", 1048576))
    _require(context.get("schema") == 1 and context.get("kind") == "SAME_OWNER_BULK_AFTER_SOURCE_V1"
             and binding.get("operation") == "daily" and binding.get("deferReferenceBuildToBulk") is True
             and context.get("source_binding_ref") == proof["binding_ref"]
             and context.get("bulk_scope_ref") == request["scope"]
             and Path(context["bulk_workset"]).resolve() == workset.folder.resolve()
             and type(context.get("source_actual_native")) is int and context["source_actual_native"] in {0, 2}
             and context.get("source_status", {}).get("daily_source_stop") is None
             and context["source_status"].get("daily_status") in {"bounded_complete", "baselines_pending"},
             "reference_bulk_after_source_unqualified")
    source_terminal = json.loads(_pin(context["source_native_terminal_ref"]))
    _require(type(source_terminal.get("actualNative")) is int
             and source_terminal["actualNative"] == context["source_actual_native"]
             and source_terminal.get("allNativeDescendantsTerminal") is True
             and source_terminal.get("bindingSHA256") == digest(encoded(binding)),
             "reference_bulk_after_source_native_unknown")
    scope = json.loads(_pin(request["scope"]))
    _require(scope.get("schema") == 2 and scope.get("kind") == "reference-bulk-affected-scope"
             and scope.get("source_delivery_refs") == [{"binding": context["source_binding_ref"],
                 "postcheck": context["source_postcheck_ref"]}], "reference_bulk_after_source_scope")
    job = Job(binding["job"])
    observed = observed_raw_delivery(job, context["source_binding_ref"], context["source_postcheck_ref"],
        target_sha=workset.binding["storage_target_sha256"])
    _require(all(fact["source_failure"] is False and fact["source_episode_native"] == context["source_actual_native"]
                 for fact in observed["stage_facts"]), "reference_bulk_source_failure")
    after_request = json.loads(_pin(binding["bulkAfterSource"]))
    daily_universe = json.loads(_pin(context["daily_endpoint_scope_ref"]))
    _require(scope["endpoint_scope_ref"] == context["daily_endpoint_scope_ref"]
             and daily_universe.get("kind") == "reference-bulk-daily-universe"
             and daily_universe.get("policy_ref") == after_request["endpoint_scope_ref"],
             "reference_bulk_after_source_scope")
    original_carried = json.loads(_pin(after_request["carried_ref"]))
    carried = json.loads(_pin(context["bulk_carried_ref"]))
    source_proof = json.loads(_pin(context["source_postcheck_ref"]))
    total, delta = source_proof["cumulativeIndependentGETs"], source_proof["newGETs"]
    original_total = original_carried.get("independent_lifetime_gets", 0)
    _require(type(total) is int and type(delta) is int and type(original_total) is int
             and total >= delta >= 0 and original_total >= total - delta,
             "reference_bulk_after_source_meter_changed")
    _require(carried == {**original_carried, "independent_lifetime_gets": original_total + delta,
        "source_independent_transition": {"before": total - delta, "after": total, "delta": delta},
        "after_source_delivery": {"binding": context["source_binding_ref"], "postcheck": context["source_postcheck_ref"]}}
        and Path(context["bulk_workset"]).resolve().is_relative_to(Path(after_request["workset_root"]).resolve())
        and scope["endpoint_scope_ref"] == context["daily_endpoint_scope_ref"],
        "reference_bulk_after_source_scope")
    return context, observed


def _validate_bulk_postcheck(workset, reference, targets, receipts):
    from .rolling_span_meter_proof import _reader_result, _semantic_load, _semantic_same

    proof = json.loads(_pin(reference))
    _require(proof.get("schema") == 1 and proof.get("kind") == "ACTUAL_METERED_BULK_POSTCHECK_V1"
             and proof.get("scope_sha256") == workset.scope_sha256
             and proof.get("build_seal") == workset.assert_sealed()
             and proof.get("generated_targets") == targets and proof.get("publication_receipts") == receipts
             and proof.get("componentCleanupConfirmed") is True and proof.get("ownedSDKClosed") is True,
             "reference_bulk_postcheck_unqualified")
    binding = json.loads(_pin(proof["binding_ref"]))
    request = json.loads(read(workset.folder / "REQUEST.json", 1048576))
    if binding.get("operation") == "daily":
        _after_source_context(workset, proof, binding)
    else:
        _require(binding.get("operation") == "rolling_bulk" and binding.get("bulkScope") == request["scope"]
                 and "after_source_ref" not in proof, "reference_bulk_postcheck_binding")
    _require(binding.get("targetSHA256") == workset.binding["storage_target_sha256"]
             and binding.get("plan", {}).get("sha256") == workset.binding["job_plan_sha256"]
             and proof.get("bindingSHA256") == digest(encoded(binding))
             and proof.get("invocation") == binding.get("invocation"), "reference_bulk_postcheck_binding")
    permission = json.loads(_pin(proof["qualification_ref"]))
    _require(permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT" and permission.get("outcome") == "allowed"
             and permission.get("phase") in {"postcheck", "reader-result"}
             and permission.get("bindingSHA256") == proof["bindingSHA256"]
             and type(permission.get("callerPID")) is int, "reference_bulk_postcheck_permission")
    native = json.loads(_pin(proof["native_terminal"]))
    _require(type(native.get("actualNative")) is int and native["actualNative"] == 0
             and native.get("allNativeDescendantsTerminal") is True
             and native.get("activeNativeProcesses") == 0
             and native.get("bindingSHA256") == proof["bindingSHA256"], "reference_bulk_native_unqualified")
    output = _pin(native["output"])
    answers = [json.loads(line) for line in output.splitlines() if line.startswith(b"{")]
    _require(answers and answers[-1].get("scope_sha256") == workset.scope_sha256
             and answers[-1].get("generated_targets") == targets
             and answers[-1].get("publication_receipts") == receipts,
             "reference_bulk_native_scope_changed")
    _require(proof.get("resource_plan_ref") == workset.status()["resource_plans"].get("publication"),
             "reference_bulk_resource_phase")
    planned = {target["key"]: target for target in workset.targets}
    from .reference_verification_policy import validate_policy

    verification = validate_policy(workset, binding, targets)
    canary = verification["policy_ref"] is not None
    if canary:
        _require(proof.get("verification_policy_ref") == verification["policy_ref"]
                 and encoded(proof.get("verification_coverage")) == encoded(verification["coverage"])
                 and encoded(proof.get("verification_gets")) == encoded(verification["get_counts"])
                 and type(proof["account_snapshot"]["context"]["independent_gets"]) is int
                 and proof["account_snapshot"]["context"]["independent_gets"]
                    == verification["prior_independent_gets"] + verification["get_counts"]["independent"],
                 "reference_bulk_verification_policy_changed")
    else:
        _require("verification_policy_ref" not in proof and "verification_coverage" not in proof
                 and "verification_gets" not in proof,
                 "reference_bulk_verification_policy_unbound")
    products, sizes, completion_receipts = {}, {}, {}
    readbacks, readers = proof.get("readbacks"), proof.get("readerObservations")
    _require(isinstance(readbacks, list) and isinstance(readers, list)
             and len(readbacks) == len(targets) and len(readers) == len(verification["selected_keys"]),
             "reference_bulk_independent_proof_required")
    for target in targets:
        _require(target["key"] in planned and target["args"] == planned[target["key"]]["args"],
                 "reference_bulk_target_changed")
        receipt = json.loads(_pin(target["producer_receipt"]))
        reconciled = receipt.get("kind") == "reference-bulk-reconciliation-receipt"
        _require(receipt.get("kind") in {"reference-bulk-publication-receipt", "reference-bulk-reconciliation-receipt"}
                 and receipt.get("scope_sha256") == workset.scope_sha256
                 and receipt.get("resource_plan_ref") == proof["resource_plan_ref"]
                 and receipt.get("status") == "complete" and receipt.get("ownedSDKClosed") is True
                 and receipt.get("componentCleanupConfirmed") is True, "reference_bulk_producer_unqualified")
        close = json.loads(_pin(receipt["clientClose"]))
        _require(close.get("ownedSDKClosed") is True and close.get("componentCleanupConfirmed") is True
                 and close.get("primaryFailureCategory") is None and close.get("cleanupFailureCategory") is None,
                 "reference_bulk_producer_unqualified")
        if reconciled:
            from .reference_completion import validate_completion_receipt

            _require(binding.get("bulkCompletion") == receipt.get("completion_ref"),
                     "bulk_completion_receipt_binding")
            identity = digest(encoded(target["producer_receipt"]))
            if identity not in completion_receipts:
                completion_receipts[identity] = validate_completion_receipt(workset, target["producer_receipt"])
            batch = completion_receipts[identity]
        else:
            batch = json.loads(_pin(receipt["result"]))
        actual = [row for row in batch["generated_targets"] if row["key"] == target["key"]]
        _require(batch.get("scope_sha256") == workset.scope_sha256 and batch.get("build_seal") == proof["build_seal"]
                 and len(actual) == 1 and all(actual[0][key] == target[key]
                     for key in ("key", "args", "revision", "sha256")), "reference_bulk_producer_changed")
        if "decoded_sha256" in target:
            _require(all(actual[0].get(key) == target[key]
                         for key in ("content_type", "encoding", "decoded_sha256")),
                     "reference_bulk_producer_metadata_changed")
        _pin(actual[0]["producer_observation"])
        candidates = [row for row in readbacks if row.get("keySHA256") == digest(target["key"].encode())]
        _require(len(candidates) == 1, "reference_bulk_sdk_required")
        from .night_baseline_refresh import decode_product_snapshot
        from .r2_store import R2ObjectSnapshot

        raw = _pin(candidates[0]["body"])
        metadata = {"revision": target["revision"], "content_type": target.get("content_type", "application/json"),
                    "encoding": target.get("encoding")}
        body = decode_product_snapshot(R2ObjectSnapshot(raw, **metadata))
        product = _semantic_load(body)
        if "decoded_sha256" in target:
            decoded_ref = candidates[0].get("decoded_body", candidates[0]["body"] if metadata["encoding"] is None else None)
            _require(decoded_ref is not None and digest(body) == target["decoded_sha256"] and _pin(decoded_ref) == body,
                     "reference_bulk_decoded_postimage_changed")
        _require(digest(raw) == target["sha256"] and _semantic_same(product.get("args"), target["args"])
                 and candidates[0].get("metadataSHA256") == digest(encoded(metadata)),
                 "reference_bulk_sdk_changed")
        if canary:
            # Every SDK postimage, including non-canaries, must retain the
            # sealed source/context semantics. The existing publisher ignores
            # only diagnostic resource counters for unchanged canonical bytes.
            artifact = workset.product_for(target["key"])
            sealed = _semantic_load(artifact["body"].encode())
            _require(_semantic_same({key: value for key, value in product.items() if key != "resources"},
                                    {key: value for key, value in sealed.items() if key != "resources"}),
                     "reference_bulk_sdk_sealed_postimage_changed")
        products[digest(body)], sizes[digest(body)] = product, len(body)
    selected = {target.get("decoded_sha256", target["sha256"]) for target in targets
                if target["key"] in verification["selected_keys"]}
    seen = set()
    for reference in readers:
        observation = _semantic_load(_pin(reference))
        path = Path(reference["path"])
        _require(path.name.endswith(".observation.json"), "reference_bulk_reader_required")
        stem = path.name.removesuffix(".observation.json")
        intent_body = read(path.with_name(stem + ".intent.json"))
        response = read(path.with_name(stem + ".response"))
        receipt_body = read(path.with_name(stem + ".execution-receipt.json"))
        claim = json.loads(read(path.with_name(stem + ".claim.json")))
        intent, receipt = _semantic_load(intent_body), _semantic_load(receipt_body)
        _require(observation.get("status") == "matched" and observation.get("reason") is None
                 and observation.get("intent_sha256") == digest(intent_body)
                 and observation.get("response_sha256") == digest(response)
                 and observation.get("response_bytes") == len(response)
                 and observation.get("receipt_sha256") == digest(receipt_body)
                 and claim == {key: observation[key] for key in ("intent_sha256", "receipt_ref", "receipt_sha256")},
                 "reference_bulk_reader_changed")
        _require(intent.get("invocation") == binding["invocation"]
                 and intent.get("plan_sha256") == workset.binding["job_plan_sha256"]
                 and intent.get("tool") == binding["normalReader"]["tool"]
                     == "mcp__codex_apps__slipstream_oauth_night_baseline"
                 and _semantic_same(intent.get("binding"), binding["normalReader"]["binding"])
                 and intent["binding"].get("target_sha256") == workset.binding["storage_target_sha256"]
                 and len(intent["sdk_product_sha256"]) == 1
                 and receipt.get("schema_version") == 2
                 and receipt.get("kind") == "ROOT_NIGHT_BASELINE_OUTER_OBSERVATION_V2"
                 and receipt.get("terminal_state") == "response"
                 and receipt.get("cleanup") == "terminal_shared_connector_retained"
                 and receipt.get("observation_source") == "root_connected_tool_invocation"
                 and receipt.get("transport_replay") == "unobserved"
                 and receipt.get("response_sha256") == digest(response) and receipt.get("response_bytes") == len(response)
                 and all(_semantic_same(receipt.get(key), intent[key]) for key in ("intent_id", "nonce", "episode",
                     "invocation", "round_id", "plan_sha256", "binding_sha256", "binding", "arguments", "tool",
                     "sdk_product_sha256")), "reference_bulk_reader_binding")
        sha = intent["sdk_product_sha256"][0]
        _require(sha in selected and sha not in seen and intent["decoded_bytes"] == sizes[sha],
                 "reference_bulk_reader_changed")
        product = products[sha]
        projection = {key: product[key] for key in ("args", "source_identity", "nights_requested", "source_nights",
            "contributing_nights", "unit", "bins", "excluded", "method")}
        projection.update(status="available", window={key: product["args"][key] for key in ("start_date", "end_date")},
                          quality_notes=product.get("quality_notes", []))
        expected = {"status": "available", "method_version": "night-baseline-v1", "current": projection,
                    "reference": None, "quality_flags": []}
        answer = _reader_result(response, receipt)
        _require(_semantic_same(intent["expected"], expected)
                 and _semantic_same({key: answer[key] for key in expected}, expected)
                 and _semantic_same(answer["resources"], observation["resources"])
                 and answer["resources"]["decoded_bytes"] == intent["decoded_bytes"]
                 and answer["resources"]["r2_get_attempts"] == intent["reserved_gets"] == 1,
                 "reference_bulk_reader_sdk_mismatch")
        seen.add(sha)
    _require(seen == selected, "reference_bulk_reader_required")
    return proof


def _source_completion_event(workset, proof_ref, observed):
    return {"type": "daily_round_finished", "round": observed["round"],
        "bulk_scope_sha256": workset.scope_sha256, "bulk_postcheck": proof_ref,
        "calendar_references_complete": False,
        "calendar_postcheck": json.loads(_pin(proof_ref))["calendar_postcheck_ref"]}


class CalendarSlice:
    """A revalidated, finite calendar continuation of this exact raw and rolling proof."""

    def __init__(self, scope_ref, folder, proof_ref, max_periods=3):
        self.scope_ref, self.folder, self.proof_ref = scope_ref, str(Path(folder).resolve()), proof_ref
        self.max_periods = max_periods

    def revalidate(self, job, plan_sha):
        from .night_baseline_refresh import affected_periods, is_closed

        _require(type(self.max_periods) is int and 1 <= self.max_periods <= 3, "reference_bulk_calendar_bound")
        workset, scope = load_bulk_workset(job, plan_sha, self.scope_ref, self.folder)
        proof = json.loads(_pin(self.proof_ref))
        _validate_bulk_postcheck(workset, self.proof_ref, proof["generated_targets"], proof["publication_receipts"])
        binding = json.loads(_pin(proof["binding_ref"]))
        context, observed = _after_source_context(workset, proof, binding)
        periods = {}
        for row in observed["destinations"]:
            periods.update(affected_periods("signals" if row["stream"] == "hr" else "hrv", row["date"]))
        self.all_periods = {name: periods[name] for name in sorted(periods)}
        self.period_names = sorted(name for name, period in periods.items() if is_closed(period, scope["today"]))[:self.max_periods]
        self.dates, self.round = observed["verified_raw_dates"], observed["round"]
        self.today, self.timezone = scope["today"], scope["timezone"]
        self.context, self.workset = context, workset
        source_proof = json.loads(_pin(context["source_postcheck_ref"]))
        self.independent_used = source_proof["roundIndependentGETs"]
        _require(type(self.independent_used) is int and 0 <= self.independent_used <= 256,
                 "reference_bulk_calendar_meter_changed")
        return self


def calendar_bulk(job, plan_sha, scope_ref, folder, rolling_postcheck_ref, *, resource_plan_ref,
                  carried, store_factory=None, max_periods=3):
    """One old-pool calendar slice, after independently verified rolling publication."""
    from .history_publish import _completed_context, _open_publication_store, publish
    from .reference_bulk_budget import BulkAccount
    from .rolling_reference_cli import _target
    from .rolling_reference_span import _CapturedStore

    selected = CalendarSlice(scope_ref, folder, rolling_postcheck_ref, max_periods).revalidate(job, plan_sha)
    workset = selected.workset
    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried)
    selected.deadline_account = account
    _require(account.state["active_plan"] == resource_plan_ref, "reference_bulk_resource_phase")
    directory = workset.folder / "calendar"
    _require(not directory.exists(), "reference_bulk_calendar_already_observed")
    directory.mkdir()
    context = {"round": selected.round, "independent_gets": selected.independent_used}
    _completed_context(job, job.snapshot(), workset.binding["storage_target_sha256"], context, selected.dates)
    before = _reference(job.folder / "events.jsonl")
    scope = {"schema": 1, "kind": "reference-bulk-calendar-slice", "scope_sha256": workset.scope_sha256,
        "rolling_postcheck_ref": rolling_postcheck_ref, "after_source_ref": selected.context and
            json.loads(_pin(rolling_postcheck_ref))["after_source_ref"], "periods": selected.all_periods,
        "selected_periods": selected.period_names, "max_periods": selected.max_periods,
        "source_dates": selected.dates, "source_round": selected.round,
        "source_journal_before": before, "execution_before": {key: value for key, value in context.items()
            if key != "snapshot_cache"}}
    write(directory / "SCOPE.json", scope)
    baseline = context.get("round_baseline", {"counts": dict.fromkeys(("get", "list", "put"), 0), "body_bytes": 0})
    counts = context.get("counts", dict.fromkeys(("get", "list", "put"), 0))
    shared = context.get("span_account_usage", {}).get("source_pool", {
        "counts": dict.fromkeys(("get", "list", "put"), 0), "body_bytes": 0, "publication_phases": 0})
    delta = {key: counts[key] - baseline["counts"][key] + shared["counts"][key] for key in counts}
    capacity = (context.get("phases", 0) + shared["publication_phases"] < 14
        and delta["get"] + 400 <= context.get("publication_get_limit", 6000)
        and delta["get"] + 400 + context["independent_gets"] <= 6256
        and delta["list"] + delta["put"] + 200 <= 1000
        and context.get("body_bytes", 0) - baseline["body_bytes"] + shared["body_bytes"] + 2097152 <= 10485760)
    targets = []
    for name in selected.period_names:
        first, last = selected.all_periods[name]
        for channel in ("heart_rate", "hrv"):
            args = normalize_args(start_date=first, end_date=last, period="custom", channel=channel)
            targets.append({"storage_key": storage_key(args), "args": args, "endpoint": last,
                            "window_days": None, "channel": channel, "calendar_period": name})
    capture, primary, cleanup, close, closed = None, None, None, None, True
    receipts = []
    try:
        if capacity and selected.period_names:
            if store_factory:
                root = store_factory()
                close = root.client.close
            else:
                root, close = _open_publication_store()
            closed = False
            _require(_target(root) == workset.binding["storage_target_sha256"], "storage_target_changed")
            capture = _CapturedStore(root, directory / "readbacks", targets)
            result = publish(job, plan_sha, confirm=True, store=capture, target_dates=[],
                finalize_dates=selected.dates, execution_context=context, reference_max_periods=selected.max_periods,
                calendar_slice=selected)
            frames = job.events()[0]
            run = next(frame["event"]["run"] for frame in reversed(frames) if frame["event"]["type"] == "end")
            receipts = [_reference(job.folder / "runs" / run / "RECEIPT.json")]
            _require(not result["baseline_refresh"].get("held_periods")
                     and not result["baseline_refresh"].get("blocked"), "reference_bulk_calendar_unknown")
        else:
            result = job.status()
    except BaseException as error:
        primary = error
    finally:
        if close is not None:
            try:
                close()
                closed = True
            except BaseException as error:
                cleanup = error
    completed_keys = {target["storage_key"] for target in targets
                      if capture and target["calendar_period"] not in capture.queue_rows}
    observed_rows = [row for row in (capture.observed.values() if capture else [])
                     if row["written"] or row["key"] in completed_keys]
    generated = [{**{name: row[name] for name in ("key", "args", "revision", "sha256")},
                  "producer_receipt": receipts[0]} for row in observed_rows]
    terminal = {**(result if primary is None else job.status()), "schema": 1,
        "kind": "reference-bulk-calendar-result", "phase": "calendar", "scope_sha256": workset.scope_sha256,
        "status": "hold" if primary or cleanup else "calendar_pending" if not capacity or not selected.period_names
            or len(generated) < len(targets) or len(selected.period_names) < len(selected.all_periods) else "calendar_complete",
        "calendar_scope": _reference(directory / "SCOPE.json"), "original_rolling_postcheck_ref": rolling_postcheck_ref,
        "after_source_ref": json.loads(_pin(rolling_postcheck_ref))["after_source_ref"],
        "generated_targets": generated, "producer_receipts": receipts, "source_execution_totals": {
            key: value for key, value in context.items() if key != "snapshot_cache"},
        "source_journal_before": before, "source_journal_after": _reference(job.folder / "events.jsonl"),
        "resource_plan_ref": resource_plan_ref, "usage": account.snapshot(), "source_calls": 0,
        "componentCleanupConfirmed": closed, "ownedSDKClosed": closed,
        "primaryFailureCategory": getattr(primary, "category", "reference_bulk_calendar_failed") if primary else None,
        "cleanupFailureCategory": "reference_bulk_calendar_cleanup_unknown" if cleanup else None,
        "calendar_observation": "bounded_attempt" if capacity else "known_budget_deferred",
        "producer_observations": [row["producer_observation"] for row in observed_rows]}
    write(directory / "RESULT.json", terminal)
    if primary is not None:
        workset.mark_hold(terminal["primaryFailureCategory"], _reference(directory / "RESULT.json"))
        raise primary
    if cleanup is not None:
        workset.mark_hold(terminal["cleanupFailureCategory"], _reference(directory / "RESULT.json"))
        raise cleanup
    return {**terminal, "result_ref": _reference(directory / "RESULT.json")}


def validate_calendar_execution(job, plan_sha, result_ref):
    """Join one calendar slice to its exact Source-prefix append and genuine publisher receipt."""
    from .rolling_span_source_proof import _journal, pin

    result = json.loads(_pin(result_ref))
    scope = json.loads(_pin(result["calendar_scope"]))
    proof = json.loads(_pin(result["original_rolling_postcheck_ref"]))
    context = json.loads(_pin(proof["after_source_ref"]))
    selected = CalendarSlice(context["bulk_scope_ref"], context["bulk_workset"],
        result["original_rolling_postcheck_ref"], scope["max_periods"]).revalidate(job, plan_sha)
    _require(result.get("schema") == 1 and result.get("kind") == "reference-bulk-calendar-result"
             and result.get("status") in {"calendar_complete", "calendar_pending"}
             and result.get("scope_sha256") == selected.workset.scope_sha256
             and result.get("source_calls") == 0 and result.get("componentCleanupConfirmed") is True
             and result.get("ownedSDKClosed") is True and result.get("primaryFailureCategory") is None
             and result.get("cleanupFailureCategory") is None
             and scope.get("periods") == {name: list(period) for name, period in selected.all_periods.items()}
             and scope.get("selected_periods") == selected.period_names
             and scope.get("source_dates") == selected.dates and scope.get("source_round") == selected.round
             and scope.get("rolling_postcheck_ref") == result["original_rolling_postcheck_ref"]
             and result.get("after_source_ref") == proof["after_source_ref"], "reference_bulk_calendar_unqualified")
    before = pin(result["source_journal_before"], prefix=True)
    after = pin(result["source_journal_after"], prefix=True)
    old_events, new_events = _journal(result["source_journal_before"], job), _journal(result["source_journal_after"], job)
    _require(after.startswith(before) and scope["source_journal_before"] == result["source_journal_before"],
             "reference_bulk_calendar_journal_changed")
    suffix = new_events[len(old_events):]
    _require(new_events[:len(old_events)] == old_events, "reference_bulk_calendar_journal_changed")
    receipts = result["producer_receipts"]
    if receipts:
        _require(len(receipts) == 1 and len(suffix) == 3
                 and [event["type"] for event in suffix] == ["begin", "reference_refresh", "end"],
                 "reference_bulk_calendar_journal_changed")
        receipt = json.loads(_pin(receipts[0]))
        remote = json.loads(read(Path(receipts[0]["path"]).parent / "REMOTE-VERIFIED.json"))
        _require(receipt.get("status") == "complete" and receipt.get("destinations") == []
                 and receipt.get("source_calls") == 0 and receipt.get("plan_sha256") == plan_sha
                 and receipt.get("target_sha256") == selected.workset.binding["storage_target_sha256"]
                 and remote == {**receipt, "status": "remote_verified_cleanup_pending"}
                 and suffix[0]["run"] == suffix[2]["run"] == receipt["run"]
                 and suffix[2]["status"] == "published" and suffix[1]["report"] == receipt["baseline_refresh"]
                 and receipt["baseline_refresh"].get("calendar_only") is True
                 and receipt["baseline_refresh"].get("calendar_period_names") == selected.period_names,
                 "reference_bulk_calendar_receipt_changed")
    else:
        _require(not suffix and not result["generated_targets"], "reference_bulk_calendar_journal_changed")
    prior, current = scope["execution_before"], result["source_execution_totals"]
    _require(prior["round"] == current["round"] == selected.round
             and prior["round_baseline"] == current["round_baseline"]
             and prior["publication_get_limit"] == current["publication_get_limit"]
             and all(current["counts"][key] >= prior["counts"][key] for key in ("get", "list", "put"))
             and current["body_bytes"] >= prior["body_bytes"]
             and current["phases"] == prior["phases"] + bool(receipts)
             and current["independent_gets"] == prior["independent_gets"],
             "reference_bulk_calendar_budget_changed")
    planned = {storage_key(normalize_args(start_date=selected.all_periods[name][0],
        end_date=selected.all_periods[name][1], period="custom", channel=channel))
        for name in selected.period_names for channel in ("heart_rate", "hrv")}
    observations = [json.loads(_pin(reference)) for reference in result["producer_observations"]]
    _require(len(observations) == len(result["generated_targets"]), "reference_bulk_calendar_products_changed")
    for target in result["generated_targets"]:
        matching = [row for row in observations if row["key"] == target["key"]]
        _require(target["key"] in planned and len(matching) == 1 and target["producer_receipt"] in receipts
                 and all(matching[0][key] == target[key] for key in ("key", "args", "revision", "sha256")),
                 "reference_bulk_calendar_products_changed")
    return result


def _validate_calendar_postcheck(workset, proof):
    from .history_state import Job
    from .rolling_span_meter_proof import _post_source_reader_pins

    calendar = json.loads(_pin(proof["calendar_postcheck_ref"]))
    binding = json.loads(_pin(proof["binding_ref"]))
    job = Job(binding["job"])
    result = validate_calendar_execution(job, job.plan_sha, calendar["result_ref"])
    _require(json.loads(_pin(result["original_rolling_postcheck_ref"])) == {
        key: value for key, value in proof.items() if key != "calendar_postcheck_ref"},
        "reference_bulk_calendar_rolling_changed")
    _require(calendar.get("schema") == 1 and calendar.get("kind") == "ACTUAL_METERED_BULK_CALENDAR_POSTCHECK_V1"
             and calendar.get("scope_sha256") == workset.scope_sha256 and calendar.get("binding_ref") == proof["binding_ref"]
             and calendar.get("bindingSHA256") == proof["bindingSHA256"]
             and calendar.get("componentCleanupConfirmed") is True and calendar.get("ownedSDKClosed") is True,
             "reference_bulk_calendar_postcheck_unqualified")
    native = json.loads(_pin(calendar["calendar_native_terminal"]))
    _require(type(native.get("actualNative")) is int and native["actualNative"] == 0
             and native.get("allNativeDescendantsTerminal") is True and native.get("activeNativeProcesses") == 0
             and native.get("bindingSHA256") == proof["bindingSHA256"], "reference_bulk_calendar_native_unknown")
    outputs = [json.loads(line) for line in _pin(native["output"]).splitlines() if line.startswith(b"{")]
    _require(outputs and outputs[-1].get("result_ref") == calendar["result_ref"], "reference_bulk_calendar_native_unknown")
    port = json.loads(_pin(calendar["qualification_ref"]))
    _require(port.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT" and port.get("outcome") == "allowed"
             and port.get("phase") in {"postcheck", "reader-result"} and port.get("bindingSHA256") == proof["bindingSHA256"],
             "reference_bulk_calendar_postcheck_permission")
    postcheck = json.loads(_pin(calendar["postcheck_ref"]))
    _require(postcheck.get("kind") in {"ACTUAL_METERED_CURRENT_DAILY_POSTCHECK", "ACTUAL_METERED_INDEPENDENT_POSTCHECK"}
             and postcheck.get("bindingSHA256") == proof["bindingSHA256"]
             and postcheck.get("componentCleanupConfirmed") is True,
             "reference_bulk_calendar_postcheck_unqualified")
    source = json.loads(_pin(json.loads(_pin(proof["after_source_ref"]))["source_postcheck_ref"]))
    _require(calendar["round_independent_before"] == source["roundIndependentGETs"]
             and calendar["round_independent_after"] == postcheck["roundIndependentGETs"]
             and type(calendar["round_independent_after"]) is int
             and 0 <= calendar["round_independent_before"] <= calendar["round_independent_after"] <= 256
             and postcheck["cumulativeIndependentGETs"] - source["cumulativeIndependentGETs"]
                 == calendar["round_independent_after"] - calendar["round_independent_before"],
             "reference_bulk_calendar_meter_changed")
    generated, readbacks = result["generated_targets"], postcheck["readbacks"]
    _require(len(readbacks) == len(generated), "reference_bulk_calendar_sdk_required")
    for target in generated:
        rows = [row for row in readbacks if row["keySHA256"] == digest(target["key"].encode())]
        _require(len(rows) == 1 and digest(_pin(rows[0]["body"])) == target["sha256"]
                 and rows[0]["metadataSHA256"] == digest(encoded({"revision": target["revision"],
                     "content_type": "application/json", "encoding": None})), "reference_bulk_calendar_sdk_changed")
    if generated:
        _require(postcheck.get("sdkAndNormalReaderVerified") is True, "reference_bulk_calendar_reader_required")
        _post_source_reader_pins(postcheck, binding)
    else:
        _require(not postcheck.get("readerObservations"), "reference_bulk_calendar_reader_changed")
    return calendar


def validate_bulk_source_completion(job, plan_sha, completion_ref):
    """Validate the one qualified Source-round append; historical prefixes remain immutable."""
    from .rolling_span_source_proof import _journal, pin

    completion = json.loads(_pin(completion_ref))
    _require(completion.get("schema") == 1 and completion.get("kind") == "BULK_AFFECTED_SOURCE_ROUND_COMPLETION_V1"
             and plan_sha == job.plan_sha, "reference_bulk_source_completion_unqualified")
    workset, _ = load_bulk_workset(job, plan_sha, completion["scope_ref"], completion["workset"])
    proof = json.loads(_pin(completion["ack_ref"]))
    _validate_bulk_postcheck(workset, completion["ack_ref"], proof["generated_targets"], proof["publication_receipts"])
    _validate_calendar_postcheck(workset, proof)
    binding = json.loads(_pin(proof["binding_ref"]))
    context, observed = _after_source_context(workset, proof, binding)
    _require(completion["after_source_ref"] == proof["after_source_ref"]
             and Path(binding["job"]).resolve() == job.folder.resolve()
             and completion["event"] == _source_completion_event(workset, completion["ack_ref"], observed),
             "reference_bulk_source_completion_unqualified")
    previous = pin(completion["prior_journal_prefix"], prefix=True)
    current = pin(completion["journal_prefix"], prefix=True)
    before, after = _journal(completion["prior_journal_prefix"], job), _journal(completion["journal_prefix"], job)
    _require(current.startswith(previous) and len(after) == len(before) + 1
             and after[:-1] == before and after[-1] == completion["event"],
             "reference_bulk_source_completion_journal_changed")
    active = None
    for event in before:
        if event["type"] == "daily_round_started":
            _require(active is None, "reference_bulk_source_completion_round")
            active = event
        elif event["type"] == "daily_round_finished":
            _require(active is not None and active["round"] == event["round"], "reference_bulk_source_completion_round")
            active = None
    _require(active is not None and active["round"] == observed["round"], "reference_bulk_source_completion_round")
    state = _publication_state(workset)
    _require(len(state["verified"]) == workset.build_summary()["product_count"]
             and all(state["verified"].get(target["key"]) == completion["ack_ref"]
                     for target in proof["generated_targets"]), "reference_bulk_source_completion_unverified")
    return {name: completion[name] for name in
            ("prior_journal_prefix", "event", "journal_prefix", "after_source_ref", "ack_ref")}


def _finish_source_round(job, plan_sha, scope_ref, workset, proof_ref, proof):
    path = workset.folder / "SOURCE-ROUND-COMPLETION.json"
    if path.exists():
        reference = _reference(path)
        validate_bulk_source_completion(job, plan_sha, reference)
        _require(json.loads(_pin(reference))["ack_ref"] == proof_ref, "reference_bulk_source_completion_changed")
        return reference
    binding = json.loads(_pin(proof["binding_ref"]))
    _, observed = _after_source_context(workset, proof, binding)
    event = _source_completion_event(workset, proof_ref, observed)
    events = [frame["event"] for frame in job.events()[0]]
    active = None
    for item in events:
        if item["type"] == "daily_round_started":
            _require(active is None, "reference_bulk_source_completion_round")
            active = item
        elif item["type"] == "daily_round_finished":
            _require(active is not None and active["round"] == item["round"], "reference_bulk_source_completion_round")
            active = None
    journal_path = job.folder / "events.jsonl"
    if events and events[-1] == event:
        body = read(journal_path)
        previous = body[:body.rfind(b"\n", 0, len(body) - 1) + 1]
        prior = {"path": str(journal_path.resolve()), "bytes": len(previous), "sha256": digest(previous)}
    else:
        _require(active is not None and active["round"] == observed["round"]
                 and all(row["status"] == "published" for row in job.snapshot()["rows"]
                         if row["date"] in active["dates"]), "reference_bulk_source_completion_round")
        prior = _reference(journal_path)
        job.append(event)
    write(path, {"schema": 1, "kind": "BULK_AFFECTED_SOURCE_ROUND_COMPLETION_V1", "scope_ref": scope_ref,
        "workset": str(workset.folder.resolve()), "prior_journal_prefix": prior, "event": event,
        "journal_prefix": _reference(journal_path), "after_source_ref": proof["after_source_ref"], "ack_ref": proof_ref})
    reference = _reference(path)
    validate_bulk_source_completion(job, plan_sha, reference)
    return reference


def validate_closed_source_cohort(job, plan_sha, round_id, *, before_sequence):
    """Qualify only the typed affected-rolling/calendar close before a new Source selection."""
    frames = job.events()[0]
    matches = [frame for frame in frames if frame["event"].get("type") == "daily_round_finished"
        and frame["event"].get("round") == round_id and "bulk_postcheck" in frame["event"]]
    if not matches:
        return False
    _require(len(matches) == 1 and matches[0]["sequence"] < before_sequence,
             "reference_bulk_source_completion_round")
    proof = json.loads(_pin(matches[0]["event"]["bulk_postcheck"]))
    context = json.loads(_pin(proof["after_source_ref"]))
    path = Path(context["bulk_workset"]) / "SOURCE-ROUND-COMPLETION.json"
    completion = validate_bulk_source_completion(job, plan_sha, _reference(path))
    _require(completion["event"] == matches[0]["event"], "reference_bulk_source_completion_changed")
    return True


def acknowledge_bulk(job, plan_sha, scope_ref, folder, proof_ref, *, resource_plan_ref, carried, completion_ref=None):
    """Advance only after the whole currently published set has genuine aggregate evidence."""
    from .reference_bulk_budget import BulkAccount

    workset, scope = load_bulk_workset(job, plan_sha, scope_ref, folder, completion_ref=completion_ref)
    state = _publication_state(workset)
    account = BulkAccount.open(workset.folder / "account", workset.scope_sha256, carried,
                               deadline_utc=scope["deadline_utc"], completion_ref=completion_ref)
    account.activate(resource_plan_ref)
    current = _publication_result(workset, account, resource_plan_ref, state)
    repeated = proof_ref in state["verified"].values()
    original = json.loads(_pin(proof_ref)) if repeated else current
    proof = _validate_bulk_postcheck(workset, proof_ref, original["generated_targets"], original["publication_receipts"])
    if "after_source_ref" in proof:
        _validate_calendar_postcheck(workset, proof)
    if not repeated:
        _require(proof["account_snapshot"] == account.snapshot(), "reference_bulk_meter_changed")
        _publication_checkpoint(workset, state, "verified", proof_ref)
    result = {**_publication_result(workset, account, resource_plan_ref, _publication_state(workset)), "phase": "ack"}
    if "verification_policy_ref" in proof:
        result.update(verification_policy_ref=proof["verification_policy_ref"],
                      verification_coverage=proof["verification_coverage"], verification_gets=proof["verification_gets"])
    if "after_source_ref" in proof:
        _require(result["status"] == "complete", "reference_bulk_source_completion_unverified")
        result["source_round_completion"] = _finish_source_round(job, plan_sha, scope_ref, workset, proof_ref, proof)
    return result


def run_command(job, args):
    reference = _reference(args.scope)
    _require(reference["sha256"] == args.scope_sha, "reference_bulk_scope_changed")
    if args.phase == "prepare":
        return prepare_bulk(job, args.plan_sha, reference, args.workset)
    _require(args.carried is not None and args.resource_plan is not None,
             "reference_bulk_resource_plan_required")
    carried_ref, resource = _reference(args.carried), _reference(args.resource_plan)
    _require(carried_ref["sha256"] == args.carried_sha and resource["sha256"] == args.resource_plan_sha,
             "reference_bulk_resource_plan_changed")
    carried = json.loads(_pin(carried_ref))
    _require(isinstance(carried, dict), "reference_bulk_carried_required")
    completion_ref = None
    if getattr(args, "completion", None) is not None:
        completion_ref = _reference(args.completion)
        _require(completion_ref["sha256"] == args.completion_sha
                 and args.phase in {"reconcile", "publication", "ack"}, "bulk_completion_phase")
    if args.phase == "reconcile":
        _require(completion_ref is not None, "bulk_completion_required")
        result = reconcile_bulk(job, args.plan_sha, reference, args.workset,
            resource_plan_ref=resource, carried=carried, completion_ref=completion_ref)
    elif args.phase == "inventory":
        result = inventory_bulk(job, args.plan_sha, reference, args.workset,
                                resource_plan_ref=resource, carried=carried)
    elif args.phase == "collect":
        result = collect_bulk(job, args.plan_sha, reference, args.workset,
                              resource_plan_ref=resource, carried=carried, max_items=args.max_items)
    elif args.phase == "build":
        workset, _ = load_bulk_workset(job, args.plan_sha, reference, args.workset)
        _require(workset.status()["resource_plans"].get("collection") == resource,
                 "reference_bulk_resource_phase")
        result = build_bulk(job, args.plan_sha, reference, args.workset,
                            carried=carried, max_targets=args.max_targets)
    elif args.phase == "publication":
        result = publish_bulk(job, args.plan_sha, reference, args.workset,
                              resource_plan_ref=resource, carried=carried, max_batches=args.max_batches,
                              completion_ref=completion_ref)
    else:
        _require(args.phase in {"calendar", "ack"} and args.proof is not None, "reference_bulk_postcheck_required")
        proof = _reference(args.proof)
        _require(proof["sha256"] == args.proof_sha, "reference_bulk_postcheck_changed")
        if args.phase == "calendar":
            result = calendar_bulk(job, args.plan_sha, reference, args.workset, proof,
                                   resource_plan_ref=resource, carried=carried)
        else:
            result = acknowledge_bulk(job, args.plan_sha, reference, args.workset, proof,
                                      resource_plan_ref=resource, carried=carried, completion_ref=completion_ref)
    return {"schema": 1, "kind": "reference-bulk-result", "phase": args.phase, **result}


def reconcile_bulk(job, plan_sha, scope_ref, folder, *, resource_plan_ref, carried,
                   completion_ref, store_factory=None):
    from .reference_completion import reconcile_bulk as reconcile

    return reconcile(job, plan_sha, scope_ref, folder, resource_plan_ref=resource_plan_ref,
                     carried=carried, completion_ref=completion_ref, store_factory=store_factory)


def failure_result(job, args, error):
    """Controlled CLI projection only; in-process callers keep the original error."""
    category = error.category if isinstance(error, Stop) else "reference_bulk_failed"
    scope_sha, terminal = None, None
    try:
        scope_ref = _reference(args.scope)
        if scope_ref["sha256"] == args.scope_sha:
            workset, _ = load_bulk_workset(job, args.plan_sha, scope_ref, args.workset)
            scope_sha = workset.scope_sha256
            steps = workset.status()["steps"]
            terminal = steps[-1] if steps else None
    except BaseException:
        pass
    return {"schema": 1, "kind": "reference-bulk-result", "phase": args.phase,
        "scope_sha256": scope_sha, "status": "hold", "primaryFailureCategory": category,
        "terminal_ref": terminal, "source_calls": 0}
