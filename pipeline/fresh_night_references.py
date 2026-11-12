"""One current stored night, five explicit windows, ordinary producer receipts.

The local workset survives every bounded collection stage in this invocation.
It grants no Garmin access and never calls the Root-only independent bulk ACK.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .history_state import Job, Stop, digest, write
from .rolling_night_references import WINDOWS


class _SameRunStore:
    """Keep the caller's original R2 write allowance across bulk stage views."""

    def __init__(self, store):
        self.store = store

    def __getattr__(self, name):
        return getattr(self.store, name)

    def new_stage(self):
        # StorageGuard owns fresh physical phase counters. The normal R2Store
        # retains its run writes/bytes, bucket inventory and unknown-send latch.
        return self


def refresh_current_night(store, wake_date, *, now=None, folder=None):
    """Compute all ten before publication; at most two ordinary eight-row passes."""
    from .latest_night import _checked_heart_rate
    from .native_night import NativeNightReader
    from .night_followup import _stored_current_night
    from .reference_bulk import (
        build_bulk,
        collect_bulk,
        inventory_bulk,
        load_bulk_workset,
        prepare_bulk,
        publish_bulk,
    )
    from .reference_bulk_budget import collection_plan, inventory_plan, publication_plan
    from .rolling_reference_cli import _target

    instant = now or datetime.now(UTC)
    calendar = ZoneInfo(os.environ.get("HEALTH_TIMEZONE") or "UTC")
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Fresh reference clock must be aware")
    if wake_date != instant.astimezone(calendar).date().isoformat():
        return {"status": "not_run_not_current", "selected_remaining": 10}
    if not _stored_current_night(store, wake_date, instant):
        return {"status": "bounded_pending", "selected_remaining": 10, "reason": "sources_pending"}
    night = NativeNightReader(store, timezone=str(calendar)).night(wake_date)
    days = night["channels"]["heart_rate"]["source_dates"]
    if not days or any(_checked_heart_rate(store, day, wake_date, instant, calendar) is None
                       for day in days):
        return {"status": "bounded_pending", "selected_remaining": 10, "reason": "sources_pending"}

    root = Path(folder) if folder is not None else Path(tempfile.mkdtemp(prefix="fresh-night-references-"))
    root.mkdir(parents=True, exist_ok=True)
    target = _target(store)
    # This plan is exclusively stored-reference work: no source admission is
    # called. Its account identity is the actual storage target, not a source ACK.
    job = Job.create(root / "job", wake_date, wake_date, ["hr", "hrv"], "legacy", target)
    scope_path = root / "scope.json"
    write(scope_path, {"schema": 1, "kind": "reference-bulk-scope",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": target,
        "today": wake_date, "timezone": str(calendar), "lower_endpoint": wake_date,
        "upper_endpoint": wake_date, "windows": list(WINDOWS),
        "deadline_utc": (datetime.now(UTC) + timedelta(minutes=25)).isoformat(),
        "historical_refs": [], "source_delivery_refs": []})
    body = scope_path.read_bytes()
    scope_ref = {"path": str(scope_path.resolve()), "bytes": len(body), "sha256": digest(body)}
    carried = {"ordinary_before": dict(store.operations), "write_bytes_before": store._write_bytes}
    folder = root / "workset"
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    view = _SameRunStore(store)
    def factory():
        return view
    inventory = inventory_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=inventory_plan(workset, carried), carried=carried, store_factory=factory)
    if inventory["status"] != "inventory_complete":
        raise Stop("fresh_night_inventory_incomplete")
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    plan = collection_plan(workset, carried=carried)
    # Every successful phase consumes at least one of this finite source set.
    # It is not a retry: held/unknown phases raise and the next phase is not run.
    phases = 0
    for _ in range(len(workset.required_sources()) + 1):
        collected = collect_bulk(job, job.plan_sha, scope_ref, folder,
            resource_plan_ref=plan, carried=carried, store_factory=factory, max_items=400)
        phases += 1
        if collected["status"] == "sources_complete":
            break
        if collected["fetched_sources"] == 0:
            raise Stop("fresh_night_collection_no_progress")
    else:
        raise Stop("fresh_night_collection_incomplete")
    built = build_bulk(job, job.plan_sha, scope_ref, folder, carried=carried, max_targets=10)
    if built["status"] != "build_complete" or built["product_count"] != 10:
        raise Stop("fresh_night_build_incomplete")
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    published = publish_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=publication_plan(workset, carried=carried), carried=carried,
        store_factory=factory, max_batches=2)
    reports = [json.loads(path.read_bytes())["report"] for path in
               sorted((folder / "publications").glob("*/batch-*/RESULT.json"))]
    result = {"status": "complete" if published["publication_complete"] else "bounded_pending",
        "windows": list(WINDOWS), "selected_remaining": published["unpublished_products"],
        "products_written": sum(report["products_written"] for report in reports),
        "products_unchanged": sum(report["products_unchanged"] for report in reports),
        "collection_phases": phases, "producer_publication_complete": published["publication_complete"],
        "independent_bulk_verified": False, "wholeDataVerified": False,
        "source_calls": 0, "usage": published["usage"]["usage"],
        "component_operations": {kind: published["usage"]["usage"][kind]
                                 for kind in ("get", "list", "put")},
        "workset_sha256": workset.scope_sha256}
    write(root / "RESULT.json", result)
    return result
