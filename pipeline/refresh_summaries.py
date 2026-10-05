"""Bounded summary refresh with separately checkpointed daily reconciliation."""
from __future__ import annotations

import argparse
import json
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from .granular import json_bytes
from .health_writer import write_health_dataset
from .r2_store import R2Store, missing_object
from .sources import garmin, garmin_health
from .summary_export import run as export_summaries
from .summary_restore import decode_summary, restore_summaries
from .writer import write_dataset

POLICY = {"activities": {"recent": 7, "wide": 30}, "health": {"recent": 3, "wide": 14}}
PREFIX = "refresh/checks/v1/summary"
RECONCILE_INTERVAL = timedelta(hours=24)


def checkpoint_key(stream: str, wide: bool = True) -> str:
    return f"{PREFIX}/{stream}/{'reconciliation' if wide else 'recent'}.json"


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
        start = datetime.fromisoformat(check["start_date"]).date()
        end = datetime.fromisoformat(check["end_date"]).date()
        expected_span = POLICY[stream]["wide"] - (stream == "health")
        return (check.get("schema_version") == 1 and check.get("kind") == "summary-source-check"
                and check.get("stream") == stream and check.get("status") == "checked"
                and check.get("scope") == "reconciliation"
                and checked.tzinfo is not None and timedelta(0) <= now - checked < RECONCILE_INTERVAL
                and (end - start).days == expected_span
                and end <= now.date() and end >= now.date() - timedelta(days=1)
                and isinstance(check.get("summary_sha256"), str) and len(check["summary_sha256"]) == 64)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def select_windows(store, data_dir, now, *, force_reconcile=False, required_wide_scopes=()):
    """Missing/invalid receipts or missing datasets require the retained wide window."""
    windows = {}
    for stream, filename, header in [("activities", "activities.csv", "Activity ID,"),
                                     ("health", "health_daily.csv", "Date,")]:
        path = Path(data_dir) / filename
        exists = path.is_file()
        if exists:
            decode_summary(path.read_bytes(), header)
        check = _read_check(store, stream)
        wide = force_reconcile or stream in required_wide_scopes or not exists or not _recent_allowed(check, stream, now)
        days = POLICY[stream]["wide" if wide else "recent"]
        # Keep the existing activity adapter's inclusive `days_back` semantics.
        start = now.date() - timedelta(days=days - (stream == "health"))
        windows[stream] = {"mode": "reconciliation" if wide else "recent",
                           "start": start, "end": now.date(), "days_argument": days}
    return windows


def _restore_missing(store, data_dir):
    missing = {stream for stream, filename in [("activities", "activities.csv"), ("health", "health_daily.csv")]
               if not (Path(data_dir) / filename).is_file()}
    if not missing:
        return missing
    try:
        store.get("summary/manifest.json")
    except (KeyError, ClientError) as exc:
        if isinstance(exc, ClientError) and not missing_object(exc):
            raise
        # Bootstrap is safe only when no existing current summaries would be
        # overwritten. An interrupted/legacy installation needs explicit repair.
        for key in ("summary/activities.csv", "summary/health_daily.csv"):
            try:
                store.get(key)
            except KeyError:
                continue
            except ClientError as missing_exc:
                if not missing_object(missing_exc):
                    raise
                continue
            raise ValueError("Existing R2 summaries require a valid manifest before refresh") from exc
    else:
        restore_summaries(data_dir, store)
    return missing


def _valid_activity(activity):
    return (activity.start is not None and isinstance(activity.source_id, str)
            and activity.source_id.isascii() and activity.source_id.isdigit()
            and int(activity.source_id) > 0)


@contextmanager
def provider_diagnostics(client):
    """Count SDK connectapi invocations, including SDK activity pagination.

    SDK/HTTP internal retries and login requests are outside this counter.
    Never record URLs, parameters, source bodies or exception messages.
    """
    stats = {"garmin_api_calls": 0, "garmin_api_errors": 0}
    original = client.connectapi

    def counted(*args, **kwargs):
        stats["garmin_api_calls"] += 1
        try:
            return original(*args, **kwargs)
        except Exception:
            stats["garmin_api_errors"] += 1
            raise

    client.connectapi = counted
    began = time.perf_counter()
    try:
        yield stats
    finally:
        client.connectapi = original
        stats["elapsed_ms"] = round((time.perf_counter() - began) * 1000)


def run(data_dir="data", *, store=None, client=None, now=None, force_reconcile=False,
        request_pause=0.15):
    started = time.perf_counter()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Summary refresh clock must include a timezone")
    now = now.astimezone(timezone.utc)
    store = store or R2Store()
    missing = _restore_missing(store, data_dir)
    windows = select_windows(store, data_dir, now, force_reconcile=force_reconcile, required_wide_scopes=missing)
    client = client or garmin._login()
    health_stats = {"sdk_calls": 0, "source_errors": 0, "invalid_dates": 0}
    activity_window, health_window = windows["activities"], windows["health"]
    with provider_diagnostics(client) as calls:
        activities = garmin.fetch(days_back=activity_window["days_argument"], data_dir=data_dir,
                                  client=client, end_date=activity_window["end"])
        health = garmin_health.fetch(health_window["start"], health_window["end"], client=client,
                                    request_pause=request_pause, diagnostics=health_stats)
    # An activity without a UTC start cannot be serialized. Preserve prior data
    # and leave that scope due rather than recording a successful reconciliation.
    activity_complete = all(_valid_activity(activity) for activity in activities)
    covered_days = {row["Date"] for row in health}
    expected_days = (health_window["end"] - health_window["start"]).days + 1
    health_complete = (health_stats["source_errors"] == health_stats["invalid_dates"] == 0
                       and len(covered_days) == expected_days)
    write_dataset([activity for activity in activities if _valid_activity(activity)],
                  data_dir, preserve_existing=True)
    write_health_dataset(health, data_dir, preserve_existing=True)
    # Receipts are published only after both summaries, snapshots and manifest
    # have been successfully exported. Partial metrics retain old CSV values.
    manifest = export_summaries(data_dir, store=store)
    hashes = {entry["name"]: entry["sha256"] for entry in manifest["files"]}
    ready = {"activities": activity_complete, "health": health_complete}
    checkpoints = 0
    for stream, filename in [("activities", "activities.csv"), ("health", "health_daily.csv")]:
        if not ready[stream]:
            continue
        window = windows[stream]
        receipt = {"schema_version": 1, "kind": "summary-source-check", "stream": stream,
                   "status": "checked", "scope": window["mode"],
                   "checked_at": now.isoformat(), "start_date": window["start"].isoformat(),
                   "end_date": window["end"].isoformat(), "summary_sha256": hashes[filename]}
        if window["mode"] == "reconciliation":
            store.put(checkpoint_key(stream), json_bytes(receipt), "application/json")
            checkpoints += 1
        store.put(checkpoint_key(stream, wide=False), json_bytes(receipt), "application/json")
        checkpoints += 1
    report = {"schema_version": 1, "kind": "summary-window-refresh",
              "status": "complete" if all(ready.values()) else "partial",
              "windows": {stream: {"mode": window["mode"],
                                    "calendar_days": (window["end"] - window["start"]).days + 1,
                                    "source_checked": ready[stream]}
                          for stream, window in windows.items()},
              "activity_rows_fetched": len(activities), "health_rows_fetched": len(health),
              "health_diagnostics": health_stats, "provider_diagnostics": calls,
              "checkpoints_written": checkpoints,
              "elapsed_ms": round((time.perf_counter() - started) * 1000)}
    print(json.dumps(report))
    return report


def main():
    parser = argparse.ArgumentParser(description="Refresh recent summaries and reconcile daily.")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--force-reconcile", action="store_true")
    args = parser.parse_args()
    run(args.data_dir, force_reconcile=args.force_reconcile)


if __name__ == "__main__":
    main()
