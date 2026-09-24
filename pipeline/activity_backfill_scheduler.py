"""Advance newest-first activity ranges with one shared per-run budget."""

from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import date, datetime, timezone
from typing import Any

from .activity_backfill import (
    MAX_ACTIVITIES_PER_RUN,
    progress_key,
)
from .activity_backfill import (
    run as run_backfill,
)
from .granular import json_bytes
from .r2_store import R2Store
from .sources.garmin import _login
from .summary_restore import decode_summary

PLAN_KEY = "backfill/activities/plan-v2.json"


def history_bounds(activities_csv: bytes) -> tuple[date, date]:
    raw = decode_summary(activities_csv, "Activity ID,")
    rows = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    dates = []
    for row in rows:
        value = str(row.get("Activity Date") or "")[:10]
        try:
            dates.append(date.fromisoformat(value))
        except ValueError:
            continue
    if not dates:
        raise ValueError("R2 activity summary contains no valid activity dates")
    return min(dates), max(dates)


def year_ranges(start: date, end: date) -> list[dict[str, Any]]:
    ranges = []
    for year in range(end.year, start.year - 1, -1):
        range_start = max(start, date(year, 1, 1))
        range_end = min(end, date(year, 12, 31))
        ranges.append({
            "start_date": range_start.isoformat(),
            "end_date": range_end.isoformat(),
            "status": "pending",
        })
    return ranges


def new_plan(activities_csv: bytes) -> dict[str, Any]:
    start, end = history_bounds(activities_csv)
    return {
        "schema_version": 2,
        "kind": "activity-backfill-plan",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "history_start": start.isoformat(),
        "history_end": end.isoformat(),
        "status": "active",
        "ranges": year_ranges(start, end),
    }


def _load_json(store: R2Store, key: str) -> dict[str, Any]:
    try:
        value = json.loads(store.get(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 JSON object is invalid: {key}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"R2 JSON object must contain an object: {key}")
    return value


def _next_range(
    store: R2Store,
    plan: dict[str, Any],
    existing_progress_keys: set[str],
) -> dict[str, Any] | None:
    ranges = plan.get("ranges")
    if not isinstance(ranges, list):
        raise ValueError("R2 activity backfill plan has no ranges")

    for item in ranges:
        if not isinstance(item, dict):
            continue
        start_date = str(item.get("start_date") or "")
        end_date = str(item.get("end_date") or "")
        key = progress_key(start_date, end_date)
        if key not in existing_progress_keys:
            item["status"] = "pending"
            return item

        progress = _load_json(store, key)
        remaining = int(progress.get("remaining_activities", 0))
        blocked = int(progress.get("blocked_after_three_failures", 0))
        item.update({
            "complete_activities": int(progress.get("complete_activities", 0)),
            "remaining_activities": remaining,
            "blocked_activities": blocked,
        })
        if remaining == 0:
            item["status"] = "complete"
            continue
        if remaining <= blocked:
            item["status"] = "blocked"
            continue
        item["status"] = "active"
        return item
    return None


def run(
    *,
    max_activities: int = 50,
    store: R2Store | None = None,
    garmin=None,
) -> dict[str, Any]:
    if not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(
            f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}"
        )
    store = store or R2Store()
    progress_keys = store.list_keys("backfill/activities/")

    if PLAN_KEY in progress_keys:
        plan = _load_json(store, PLAN_KEY)
        if plan.get("status") == "complete":
            print(json.dumps(plan, indent=2, ensure_ascii=False))
            return plan
    else:
        plan = new_plan(store.get("summary/activities.csv"))

    remaining_budget = max_activities
    processed_ranges = []
    garmin_client = garmin
    while remaining_budget > 0:
        selected = _next_range(store, plan, progress_keys)
        if selected is None:
            ranges = plan.get("ranges", [])
            has_blocked = any(
                isinstance(item, dict) and item.get("status") == "blocked"
                for item in ranges
            )
            plan["status"] = "complete_with_blocked" if has_blocked else "complete"
            plan["completed_at"] = datetime.now(timezone.utc).isoformat()
            break

        garmin_client = garmin_client or _login()
        result = run_backfill(
            start_date=selected["start_date"],
            end_date=selected["end_date"],
            max_activities=remaining_budget,
            garmin=garmin_client,
            store=store,
        )
        attempted = int(result.get("attempted_this_run", 0))
        remaining = int(result["remaining_activities"])
        blocked = int(result["blocked_after_three_failures"])
        status = (
            "complete" if remaining == 0
            else "blocked" if remaining <= blocked
            else "active"
        )
        selected.update({
            "status": status,
            "complete_activities": result["complete_activities"],
            "remaining_activities": remaining,
            "blocked_activities": blocked,
        })
        current_range = {
            "start_date": selected["start_date"],
            "end_date": selected["end_date"],
            "attempted": attempted,
            "status": status,
        }
        processed_ranges.append(current_range)
        plan["last_range"] = {
            "start_date": selected["start_date"],
            "end_date": selected["end_date"],
        }
        progress_keys.add(progress_key(selected["start_date"], selected["end_date"]))
        remaining_budget -= attempted

        # An active range still has actionable activities. Do not retry failures
        # in the same job; the next scheduled run will resume it safely.
        if status == "active":
            break

    if plan.get("status") not in {"complete", "complete_with_blocked"}:
        plan["status"] = "active"
        plan.pop("completed_at", None)
    plan["last_run_at"] = datetime.now(timezone.utc).isoformat()
    plan["last_run_ranges"] = processed_ranges
    store.put(PLAN_KEY, json_bytes(plan), "application/json")
    print(json.dumps(plan, indent=2, ensure_ascii=False))
    return plan


def main():
    parser = argparse.ArgumentParser(
        description="Advance the automatic activity backfill by one safe batch."
    )
    parser.add_argument("--max-activities", type=int, default=50)
    args = parser.parse_args()
    try:
        run(max_activities=args.max_activities)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
