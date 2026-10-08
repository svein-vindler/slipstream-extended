"""Resumable, bounded Garmin activity backfill into private R2 storage."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from .activity_pagination import (
    MAX_CHECKPOINT_BYTES,
    ActivityPager,
    PaginationLimitError,
    encode_progress,
    load_cursor,
    observed_status,
    range_status,
    reset_scan,
)
from .granular import activity_type, is_endurance_activity
from .granular_export import activity_artifact_keys, export_activity
from .r2_store import R2BudgetError, R2Store
from .sources.activity_page import MetadataBudget, bounded_int, page_params
from .sources.garmin import _login

MAX_DATE_RANGE_DAYS = 366
MAX_ACTIVITIES_PER_RUN = 50
MAX_FAILURE_ATTEMPTS = 3
# Historical total includes explicitly authorized retries beyond the automatic
# threshold. Keep it monotonic rather than turning 4/7 back into 3 or 0.
MAX_HISTORICAL_FAILURE_ATTEMPTS = 2**31 - 1
BACKFILL_SCHEMA_VERSION = 2


def validate_backfill_request(start_date: str, end_date: str, max_activities: int):
    page_params(start_date, end_date, 0)
    if type(max_activities) is not int or not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}")


def is_supported_activity(activity: dict[str, Any]) -> bool:
    kind = activity_type(activity)
    return "strength" in kind or is_endurance_activity(kind)


def is_job_stopping_error(exc: Exception) -> bool:
    """Keep account, rate-limit, and service failures out of activity tombstones."""
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is None and isinstance(response, dict):
        status = response.get("status") or response.get("status_code")
    if status in {401, 403, 429, 500, 502, 503, 504}:
        return True
    message = f"{type(exc).__name__}: {exc}".lower()
    return any(token in message for token in (
        "too many requests",
        "rate limit",
        "ratelimit",
        "unauthorized",
        "forbidden",
        "timed out",
        "timeout",
    ))


def is_activity_complete(activity: dict[str, Any], existing_keys: set[str]) -> bool:
    return all(key in existing_keys for key in activity_artifact_keys(activity))


def select_backfill_batch(
    activities: list[dict[str, Any]],
    existing_keys: set[str],
    failures: dict[str, dict[str, Any]],
    *,
    limit: int,
    retry_failures: bool = False,
) -> list[dict[str, Any]]:
    selected = []
    for activity in activities:
        activity_id = str(activity.get("activityId") or "")
        if not activity_id or not is_supported_activity(activity):
            continue
        if is_activity_complete(activity, existing_keys):
            continue
        attempts = int(failures.get(activity_id, {}).get("attempts", 0))
        if attempts >= MAX_FAILURE_ATTEMPTS and not retry_failures:
            continue
        selected.append(activity)
        if len(selected) == limit:
            break
    return selected


def progress_key(start_date: str, end_date: str) -> str:
    return f"backfill/activities/v{BACKFILL_SCHEMA_VERSION}/{start_date}_{end_date}.json"


def _load_progress(store: R2Store, key: str, progress_keys: set[str]) -> dict[str, Any]:
    if key not in progress_keys:
        return {}
    try:
        raw = store.get(key)
        if len(raw) > MAX_CHECKPOINT_BYTES:
            raise ValueError("Activity backfill progress exceeds payload limit")
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 activity backfill progress is invalid: {key}") from exc
    if (not isinstance(value, dict) or type(value.get("schema_version", 2)) is not int
            or value.get("schema_version", 2) not in {1, 2}):
        raise ValueError("Unsupported activity backfill progress schema")
    return value


def run(
    *,
    start_date: str,
    end_date: str,
    max_activities: int = 20,
    retry_failures: bool = False,
    reconcile: bool = False,
    output_dir: str = ".granular/backfill",
    garmin=None,
    store: R2Store | None = None,
    metadata_budget: MetadataBudget | None = None,
) -> dict[str, Any]:
    validate_backfill_request(start_date, end_date, max_activities)
    if type(retry_failures) is not bool or type(reconcile) is not bool:
        raise ValueError("Backfill flags must be booleans")
    budget = metadata_budget or MetadataBudget()
    calls_before = budget.calls
    store = store or R2Store()
    output = Path(output_dir)
    existing_keys = store.list_keys("activities/")
    progress_keys = store.list_keys("backfill/activities/")
    range_progress_key = progress_key(start_date, end_date)
    previous = _load_progress(store, range_progress_key, progress_keys)
    cursor = load_cursor(previous, start_date, end_date, reconcile=reconcile)
    # Completed legacy plans remain read-only. Reconciliation is an explicit
    # local operation; no automatic migration/backfill of historical ranges.
    missing_known_files = any(
        k not in existing_keys
        for row in cursor["records"].values()
        for k in row.get("keys", [])
    )
    failures = previous.get("failures", {})
    if not isinstance(failures, dict):
        raise ValueError("Invalid activity backfill failures")
    for failure in failures.values():
        if not isinstance(failure, dict):
            raise ValueError("Invalid activity backfill failure")
        bounded_int(failure.get("attempts"), 0, MAX_HISTORICAL_FAILURE_ATTEMPTS)
        if "attempts_saturated" in failure and type(failure["attempts_saturated"]) is not bool:
            raise ValueError("Invalid activity backfill failure saturation state")
    if (previous and not reconcile and not retry_failures and not missing_known_files
            and range_status(previous) == "complete"):
        result = dict(previous, attempted_this_run=0, metadata_calls_this_run=0)
        if "pagination" in previous:
            result["pagination"] = cursor
        return result
    if missing_known_files and cursor["source_exhausted"]:
        reset_scan(cursor)
    if retry_failures and cursor["source_exhausted"] and not cursor["pending"]:
        reset_scan(cursor)
    garmin = garmin or _login()
    pager = ActivityPager(cursor, garmin=garmin, budget=budget,
                          start_date=start_date, end_date=end_date)
    completed, failed, attempted_ids = [], [], set()
    stop, stopping_error = "metadata_call_limit", None

    def process_pending():
        nonlocal stopping_error, stop
        for aid, item in list(cursor["pending"].items()):
            if len(attempted_ids) >= max_activities:
                return
            if aid in attempted_ids:
                continue
            activity = item["activity"]
            if not item["force"] and is_activity_complete(activity, existing_keys):
                cursor["pending"].pop(aid)
                failures.pop(aid, None)
                continue
            attempts = failures.get(aid, {}).get("attempts", 0)
            if attempts >= MAX_FAILURE_ATTEMPTS and not retry_failures:
                continue
            attempted_ids.add(aid)
            try:
                options = {"force": True} if item["force"] else {}
                record = export_activity(activity, garmin, store, output,
                                         existing_keys=existing_keys, **options)
                completed.append(record)
                existing_keys.update(activity_artifact_keys(activity))
                cursor["pending"].pop(aid)
                failures.pop(aid, None)
            except R2BudgetError:
                # Leave the last durable cursor untouched. Replaying that page
                # reuses successful files, and the failed pending item survives.
                raise
            except Exception as exc:
                if is_job_stopping_error(exc) or isinstance(exc, (OSError, ClientError)):
                    stop, stopping_error = "temporary_error", exc
                    return
                failure = {"attempts": min(attempts + 1, MAX_HISTORICAL_FAILURE_ATTEMPTS),
                           "last_error": "activity_import_failed",
                           "last_attempt": datetime.now(timezone.utc).isoformat()}
                if attempts >= MAX_HISTORICAL_FAILURE_ATTEMPTS:
                    failure["attempts_saturated"] = True
                failures[aid] = failure
                failed.append({"id": aid, **failure})

    try:
        if not cursor["source_exhausted"] and budget.remaining:
            pager.prepare()
        process_pending()
        while (not stopping_error and len(attempted_ids) < max_activities
               and not cursor["source_exhausted"] and budget.remaining):
            pager.next_page(existing_keys, is_supported_activity)
            process_pending()
    except R2BudgetError:
        raise
    except PaginationLimitError as exc:
        stop, stopping_error = "checkpoint_limit", exc
    except Exception as exc:
        stop, stopping_error = "temporary_error", exc
    if not stopping_error:
        if len(attempted_ids) >= max_activities:
            stop = "batch_limit"
        elif cursor["source_exhausted"]:
            stop = "source_end"
    records = cursor["records"]
    eligible = {aid: row for aid, row in records.items() if row["keys"]}
    # Counts cover unique observed activities, not unknown pages. Pending forced
    # replacements remain incomplete even when filenames already exist.
    incomplete = {aid for aid, row in eligible.items()
                  if aid in cursor["pending"] or not all(k in existing_keys for k in row["keys"])}
    blocked = sum(failures.get(aid, {}).get("attempts", 0) >= MAX_FAILURE_ATTEMPTS
                  for aid in incomplete)
    manifest = {
        "schema_version": BACKFILL_SCHEMA_VERSION,
        "kind": "activity-backfill",
        "start_date": start_date, "end_date": end_date,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "activities_found": len(records), "eligible_activities": len(eligible),
        "complete_activities": len(eligible) - len(incomplete),
        # Older schedulers only understand remaining > blocked. Reserve one
        # outstanding scan task until source end, so they cannot prematurely
        # finalize a partially enumerated range whose observed files all exist.
        "remaining_activities": len(incomplete) + int(not cursor["source_exhausted"]),
        "known_remaining_activities": len(incomplete),
        "remaining_count_is_exact": cursor["source_exhausted"],
        "blocked_after_three_failures": blocked,
        "attempted_this_run": len(attempted_ids),
        "completed_this_run": completed, "failed_this_run": failed,
        "failures": failures, "pagination": cursor,
        "stop_reason": stop, "counts_scope": "observed_unique_activities",
        "metadata_calls_this_run": budget.calls - calls_before,
        "restarted_this_run": pager.restarted,
    }
    # This manifest was just constructed from the validated working cursor.
    # Check counts without copying/decoding the entire checkpoint a second time.
    manifest["status"] = observed_status(manifest["remaining_activities"], blocked,
                                         cursor["source_exhausted"])
    manifest_data = encode_progress(manifest)
    target = output / range_progress_key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(manifest_data)
    store.put(range_progress_key, manifest_data, "application/json")
    # Print compact counts only: no activity metadata, object names or exceptions.
    print(json.dumps({key: manifest[key] for key in (
        "status", "stop_reason", "attempted_this_run", "metadata_calls_this_run",
        "complete_activities", "remaining_activities", "blocked_after_three_failures",
    )}))
    if stopping_error:
        raise RuntimeError("Activity backfill paused after a source or import error; progress retained") from None
    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Backfill detailed Garmin activity artifacts into private R2."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-activities", type=int, default=20)
    parser.add_argument("--retry-failures", action="store_true")
    parser.add_argument("--reconcile", action="store_true")
    parser.add_argument("--output-dir", default=".granular/backfill")
    args = parser.parse_args()
    try:
        validate_backfill_request(
            args.start_date, args.end_date, args.max_activities
        )
    except ValueError as exc:
        parser.error(str(exc))
    run(
        start_date=args.start_date,
        end_date=args.end_date,
        max_activities=args.max_activities,
        retry_failures=args.retry_failures,
        reconcile=args.reconcile,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
