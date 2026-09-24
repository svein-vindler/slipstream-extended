"""Resumable, newest-first Garmin HRV history backfill into private R2."""

from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import date, datetime, timezone
from typing import Any

from .activity_backfill import is_job_stopping_error
from .granular import gzip_json, json_bytes, normalize_hrv
from .health_history_index import sync_dates
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login
from .summary_restore import decode_summary

PLAN_KEY = "backfill/hrv/plan.json"
MAX_HRV_DAYS_PER_RUN = 100
MAX_FAILURE_ATTEMPTS = 3


def hrv_key(day: str) -> str:
    return f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"


def history_dates(health_csv: bytes) -> list[str]:
    raw = decode_summary(health_csv, "Date,")
    rows = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    dates: set[str] = set()
    for row in rows:
        value = str(row.get("Date") or "")[:10]
        if not str(row.get("HRV Last Night Average") or "").strip():
            continue
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            continue
        dates.add(parsed.isoformat())
    if not dates:
        raise ValueError("R2 health summary contains no dates with HRV data")
    return sorted(dates, reverse=True)


def history_bounds(health_csv: bytes) -> tuple[date, date]:
    dates = history_dates(health_csv)
    return date.fromisoformat(dates[-1]), date.fromisoformat(dates[0])


def new_plan(health_csv: bytes) -> dict[str, Any]:
    dates = history_dates(health_csv)
    return {
        "schema_version": 1,
        "kind": "hrv-backfill-plan",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "history_start": dates[-1],
        "history_end": dates[0],
        "status": "active",
        "target_dates": dates,
        "failures": {},
    }


def _load_json(store: R2Store, key: str) -> dict[str, Any]:
    try:
        value = json.loads(store.get(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 JSON object is invalid: {key}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"R2 JSON object must contain an object: {key}")
    return value


def select_hrv_batch(
    target_dates: list[str],
    existing_keys: set[str],
    failures: dict[str, dict[str, Any]],
    *,
    limit: int,
    retry_failures: bool = False,
) -> list[str]:
    selected = []
    for day in target_dates:
        if hrv_key(day) in existing_keys:
            continue
        attempts = int(failures.get(day, {}).get("attempts", 0))
        if attempts >= MAX_FAILURE_ATTEMPTS and not retry_failures:
            continue
        selected.append(day)
        if len(selected) == limit:
            break
    return selected


def _payload(day: str, raw: Any) -> dict[str, Any]:
    payload = normalize_hrv(day, raw)
    payload["available"] = bool(payload["reading_count"])
    if not payload["available"]:
        payload["unavailable_reason"] = "garmin_returned_no_detailed_readings"
        payload["checked_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def _counts(
    target_dates: list[str],
    existing_keys: set[str],
    failures: dict[str, dict[str, Any]],
) -> tuple[int, int, int]:
    complete = sum(hrv_key(day) in existing_keys for day in target_dates)
    blocked = sum(
        hrv_key(day) not in existing_keys
        and int(failures.get(day, {}).get("attempts", 0)) >= MAX_FAILURE_ATTEMPTS
        for day in target_dates
    )
    return len(target_dates), complete, blocked


def _refresh_plan_dates(store: R2Store, plan: dict[str, Any]) -> bool:
    """Add newly available HRV dates without losing historical progress."""
    current_dates = history_dates(store.get("summary/health_daily.csv"))
    stored = plan.get("target_dates")
    if not isinstance(stored, list) or not all(isinstance(day, str) for day in stored):
        raise ValueError("R2 HRV backfill plan has invalid target dates")
    merged = sorted(set(stored).union(current_dates), reverse=True)
    if merged == stored:
        return False
    plan.update({
        "status": "active",
        "history_start": merged[-1],
        "history_end": merged[0],
        "target_dates": merged,
    })
    plan.pop("completed_at", None)
    return True


def run(
    *,
    max_days: int = 50,
    retry_failures: bool = False,
    store: R2Store | None = None,
    garmin=None,
) -> dict[str, Any]:
    if not 1 <= max_days <= MAX_HRV_DAYS_PER_RUN:
        raise ValueError(
            f"max_days must be between 1 and {MAX_HRV_DAYS_PER_RUN}"
        )

    store = store or R2Store()
    progress_keys = store.list_keys("backfill/hrv/")
    if PLAN_KEY in progress_keys:
        plan = _load_json(store, PLAN_KEY)
        previous_status = plan.get("status")
        changed = _refresh_plan_dates(store, plan)
        if previous_status in {"complete", "complete_with_blocked"}:
            retrying = previous_status == "complete_with_blocked" and retry_failures
            if not changed and not retrying:
                print(json.dumps({
                    key: value for key, value in plan.items() if key != "target_dates"
                }, indent=2, ensure_ascii=False))
                return plan
            plan["status"] = "active"
            plan.pop("completed_at", None)
    else:
        plan = new_plan(store.get("summary/health_daily.csv"))

    try:
        target_dates = plan["target_dates"]
        if not isinstance(target_dates, list) or not target_dates:
            raise ValueError
        target_dates = [date.fromisoformat(str(day)).isoformat() for day in target_dates]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("R2 HRV backfill plan has invalid target dates") from exc

    failures = plan.get("failures")
    if not isinstance(failures, dict):
        failures = {}
    existing_keys = store.list_keys("health/hrv/")
    batch = select_hrv_batch(
        target_dates,
        existing_keys,
        failures,
        limit=max_days,
        retry_failures=retry_failures,
    )

    completed = []
    failed = []
    if batch:
        garmin = garmin or _login()
    for day in batch:
        try:
            payload = _payload(day, garmin.get_hrv_data(day))
            data = gzip_json(payload)
            key = hrv_key(day)
            store.put(key, data, "application/json", encoding="gzip")
            existing_keys.add(key)
            failures.pop(day, None)
            completed.append({
                "date": day,
                "available": payload["available"],
                "readings": payload["reading_count"],
                "bytes": len(data),
            })
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError(
                    "Garmin service or authentication error; stopping this HRV batch "
                    "without counting a date failure"
                ) from exc
            attempts = int(failures.get(day, {}).get("attempts", 0)) + 1
            failure = {
                "attempts": attempts,
                "last_error": str(exc),
                "last_attempt": datetime.now(timezone.utc).isoformat(),
            }
            failures[day] = failure
            failed.append({"date": day, **failure})

    if completed and hasattr(store, "list_object_revisions"):
        sync_dates(store, "hrv", (item["date"] for item in completed))

    total, complete, blocked = _counts(target_dates, existing_keys, failures)
    remaining = total - complete
    if remaining == 0:
        status = "complete"
    elif remaining <= blocked:
        status = "complete_with_blocked"
    else:
        status = "active"

    plan.update({
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_days": total,
        "complete_days": complete,
        "remaining_days": remaining,
        "blocked_after_three_failures": blocked,
        "attempted_this_run": len(batch),
        "completed_this_run": completed,
        "failed_this_run": failed,
        "failures": failures,
    })
    if status in {"complete", "complete_with_blocked"}:
        plan["completed_at"] = datetime.now(timezone.utc).isoformat()
    store.put(PLAN_KEY, json_bytes(plan), "application/json")
    print(json.dumps({
        key: value for key, value in plan.items() if key != "target_dates"
    }, indent=2, ensure_ascii=False))
    return plan


def main():
    parser = argparse.ArgumentParser(
        description="Advance the automatic detailed HRV backfill by one safe batch."
    )
    parser.add_argument("--max-days", type=int, default=50)
    parser.add_argument("--retry-failures", action="store_true")
    args = parser.parse_args()
    try:
        run(max_days=args.max_days, retry_failures=args.retry_failures)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
