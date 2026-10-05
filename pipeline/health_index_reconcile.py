"""Recent monthly R2 index repair with independently checkpointed weekly scans.

This checks derived indexes against stored canonical objects, never Garmin.
Manual full builds remain available through pipeline.health_history_index.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

from .granular import json_bytes
from .health_history_index import BUILDER_REVISION, SCHEMA_VERSION, STREAMS, sync_stream
from .r2_store import R2Store, missing_object

INTERVAL = timedelta(days=7)
POLICY_REVISION = 1


def checkpoint_key(stream):
    if stream not in STREAMS:
        raise ValueError(f"Unsupported health history stream: {stream}")
    return f"refresh/checks/v1/health-index/{stream}/reconciliation.json"


def _read_check(store, stream):
    try:
        raw = store.get(checkpoint_key(stream))
    except KeyError:
        return None
    except ClientError as exc:
        if not missing_object(exc):
            raise
        return None
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None


def _recent_allowed(check, stream, now):
    if not isinstance(check, dict):
        return False
    try:
        checked = datetime.fromisoformat(check["checked_at"])
        return (check.get("schema_version") == SCHEMA_VERSION
                and check.get("kind") == "health-index-reconciliation"
                and check.get("policy_revision") == POLICY_REVISION
                and check.get("builder_revision") == BUILDER_REVISION
                and check.get("stream") == stream and check.get("scope") == "full"
                and check.get("status") == "checked" and checked.tzinfo is not None
                and timedelta(0) <= now - checked < INTERVAL)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def recent_month_dates(now):
    current = now.date().replace(day=1)
    previous = (current - timedelta(days=1)).replace(day=1)
    # Garmin-local dates can be a day ahead of UTC at the month boundary.
    adjacent = (now.date() + timedelta(days=1)).replace(day=1)
    return sorted({day.isoformat() for day in (previous, current, adjacent)})


def run(*, store=None, streams=STREAMS, now=None):
    started = time.perf_counter()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Index reconciliation clock must include a timezone")
    now = now.astimezone(timezone.utc)
    streams = tuple(dict.fromkeys(streams))
    for stream in streams:
        checkpoint_key(stream)
    store = store or R2Store()
    before = dict(store.operations)
    results = {}
    for stream in streams:
        wide = not _recent_allowed(_read_check(store, stream), stream, now)
        result = sync_stream(stream, store=store, only_dates=None if wide else recent_month_dates(now))
        complete = result["invalid_days"] == 0
        result.update(scope="full" if wide else "recent", status="complete" if complete else "partial",
                      checkpoint_written=False)
        if wide and complete:
            receipt = {"schema_version": SCHEMA_VERSION, "kind": "health-index-reconciliation",
                       "policy_revision": POLICY_REVISION, "builder_revision": BUILDER_REVISION,
                       "stream": stream, "scope": "full", "status": "checked",
                       "checked_at": now.isoformat()}
            # Only after every index in this stream was successfully reconciled.
            store.put(checkpoint_key(stream), json_bytes(receipt), "application/json")
            result["checkpoint_written"] = True
        results[stream] = result
    report = {"schema_version": 1, "kind": "health-index-maintenance",
              "status": "complete" if all(r["status"] == "complete" for r in results.values()) else "partial",
              "results": results, "r2_operations": {k: store.operations[k] - v for k, v in before.items()},
              "elapsed_ms": round((time.perf_counter() - started) * 1000)}
    print(json.dumps(report))
    return report
