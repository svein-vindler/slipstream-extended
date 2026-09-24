"""Resumable, bounded Garmin activity backfill into private R2 storage."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .granular import activity_type, is_endurance_activity, json_bytes
from .granular_export import activity_artifact_keys, export_activity
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login

MAX_DATE_RANGE_DAYS = 366
MAX_ACTIVITIES_PER_RUN = 50
MAX_FAILURE_ATTEMPTS = 3
BACKFILL_SCHEMA_VERSION = 2


def validate_backfill_request(start_date: str, end_date: str, max_activities: int):
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("Dates must use YYYY-MM-DD") from exc
    if start > end:
        raise ValueError("start_date cannot be after end_date")
    if (end - start).days + 1 > MAX_DATE_RANGE_DAYS:
        raise ValueError(
            f"Activity backfill cannot exceed {MAX_DATE_RANGE_DAYS} days per run"
        )
    if not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(
            f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}"
        )


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
        value = json.loads(store.get(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 activity backfill progress is invalid: {key}") from exc
    return value if isinstance(value, dict) else {}


def run(
    *,
    start_date: str,
    end_date: str,
    max_activities: int = 20,
    retry_failures: bool = False,
    output_dir: str = ".granular/backfill",
    garmin=None,
    store: R2Store | None = None,
) -> dict[str, Any]:
    validate_backfill_request(start_date, end_date, max_activities)
    store = store or R2Store()
    garmin = garmin or _login()
    output = Path(output_dir)

    existing_keys = store.list_keys("activities/")
    progress_keys = store.list_keys("backfill/activities/")
    range_progress_key = progress_key(start_date, end_date)
    previous = _load_progress(store, range_progress_key, progress_keys)
    failures = previous.get("failures")
    if not isinstance(failures, dict):
        failures = {}

    activities = garmin.get_activities_by_date(
        start_date, end_date, sortorder="desc"
    )
    eligible = [activity for activity in activities if is_supported_activity(activity)]
    batch = select_backfill_batch(
        activities,
        existing_keys,
        failures,
        limit=max_activities,
        retry_failures=retry_failures,
    )

    completed = []
    failed = []
    for activity in batch:
        activity_id = str(activity.get("activityId"))
        try:
            record = export_activity(
                activity,
                garmin,
                store,
                output,
                existing_keys=existing_keys,
            )
            completed.append(record)
            existing_keys.update(activity_artifact_keys(activity))
            failures.pop(activity_id, None)
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError(
                    "Garmin service or authentication error; stopping this batch "
                    "without counting an activity failure"
                ) from exc
            attempts = int(failures.get(activity_id, {}).get("attempts", 0)) + 1
            failure = {
                "attempts": attempts,
                "last_error": str(exc),
                "last_attempt": datetime.now(timezone.utc).isoformat(),
            }
            failures[activity_id] = failure
            failed.append({"id": activity_id, **failure})

    complete_count = sum(
        is_activity_complete(activity, existing_keys) for activity in eligible
    )
    blocked_count = sum(
        not is_activity_complete(activity, existing_keys)
        and int(failures.get(str(activity.get("activityId")), {}).get("attempts", 0))
        >= MAX_FAILURE_ATTEMPTS
        for activity in eligible
    )
    manifest = {
        "schema_version": BACKFILL_SCHEMA_VERSION,
        "kind": "activity-backfill",
        "start_date": start_date,
        "end_date": end_date,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "activities_found": len(activities),
        "eligible_activities": len(eligible),
        "complete_activities": complete_count,
        "remaining_activities": len(eligible) - complete_count,
        "blocked_after_three_failures": blocked_count,
        "attempted_this_run": len(batch),
        "completed_this_run": completed,
        "failed_this_run": failed,
        "failures": failures,
    }
    manifest_data = json_bytes(manifest)
    target = output / range_progress_key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(manifest_data)
    store.put(range_progress_key, manifest_data, "application/json")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Backfill detailed Garmin activity artifacts into private R2."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--max-activities", type=int, default=20)
    parser.add_argument("--retry-failures", action="store_true")
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
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
