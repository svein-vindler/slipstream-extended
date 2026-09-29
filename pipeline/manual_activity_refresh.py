"""Prioritize newly imported workouts in an on-demand Garmin refresh."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .activity_backfill import is_job_stopping_error, is_supported_activity
from .activity_refresh import refresh_activity
from .coach_backfill import run as run_coach_backfill
from .granular import activity_type
from .granular_export import activity_artifact_keys
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login

MAX_ACTIVITIES = 10
RECENT_DAYS = 3


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Activity ID" not in reader.fieldnames:
            raise ValueError(f"Activity summary has no Activity ID column: {path}")
        return [
            {key: str(value or "") for key, value in row.items() if key is not None}
            for row in reader
            if str(row.get("Activity ID") or "").startswith("garmin-")
        ]


def snapshot(summary: Path, destination: Path) -> None:
    """Save only IDs; no private activity content enters the workflow artifact."""
    ids = sorted({row["Activity ID"] for row in _rows(summary)})
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(ids), encoding="utf-8")


def _baseline(path: Path) -> set[str]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or any(
        not isinstance(item, str)
        or not item.startswith("garmin-")
        or not item.removeprefix("garmin-").isdigit()
        for item in value
    ):
        raise ValueError("On-demand activity baseline is invalid")
    return set(value)


def _recent(row: dict[str, str], today: date) -> bool:
    try:
        activity_day = date.fromisoformat(row.get("Activity Date", "")[:10])
    except ValueError:
        return False
    return today - timedelta(days=RECENT_DAYS - 1) <= activity_day <= today


def _missing_artifacts(row: dict[str, str], keys: set[str]) -> bool:
    activity_id = row["Activity ID"].removeprefix("garmin-")
    year = row.get("Activity Date", "")[:4]
    if not year.isdigit():
        return False
    prefix = f"activities/{year}/{activity_id}"
    if (
        f"{prefix}/activity.fit" not in keys
        or f"{prefix}/activity.v1.json" not in keys
    ):
        return True
    if "run" in row.get("Activity Type", "").lower():
        return any(
            f"{prefix}/{name}" not in keys
            for name in ("activity.tcx", "activity.endurance.v1.json")
        )
    return False


def _complete_activity_details(
    activity: dict[str, Any], row: dict[str, str]
) -> dict[str, Any]:
    """Fill fields omitted by Garmin's detail endpoint from its saved summary."""
    activity_id = row["Activity ID"].removeprefix("garmin-")
    if str(activity.get("activityId") or "") != activity_id:
        raise ValueError("Garmin activity details do not match the requested ID")
    complete = dict(activity)
    summary_type = row.get("Activity Type", "").strip()
    if not is_supported_activity(complete) and is_supported_activity(
        {"activityType": summary_type}
    ):
        complete["activityType"] = {"typeKey": summary_type}
    if not (complete.get("startTimeLocal") or complete.get("startTimeGMT")):
        summary_date = row.get("Activity Date", "").strip()
        if summary_date:
            # activities.csv stores UTC, not the device's local time.
            complete["startTimeGMT"] = summary_date
    if not complete.get("activityName"):
        complete["activityName"] = row.get("Activity Name", "")
    return complete


def run(
    *,
    baseline: Path,
    summary: Path,
    run_id: str | None = None,
    max_activities: int = MAX_ACTIVITIES,
    store: Any = None,
    garmin: Any = None,
    today: date | None = None,
) -> dict[str, Any]:
    if not 1 <= max_activities <= MAX_ACTIVITIES:
        raise ValueError(f"max_activities must be between 1 and {MAX_ACTIVITIES}")
    if run_id is not None and (not run_id.isdigit() or not run_id):
        raise ValueError("run_id must contain digits only")

    previous_ids = _baseline(baseline)
    rows = _rows(summary)
    current_day = today or date.today()
    new_rows = [row for row in rows if row["Activity ID"] not in previous_ids]
    new_rows.sort(key=lambda row: "run" not in row.get("Activity Type", "").lower())
    store = store or R2Store()
    existing_keys = store.list_keys("activities/")
    fallback_rows = [
        row for row in rows
        if row["Activity ID"] in previous_ids
        and _recent(row, current_day)
        and (
            _missing_artifacts(row, existing_keys)
            or "run" in row.get("Activity Type", "").lower()
        )
    ]
    candidates = (new_rows + fallback_rows)[:max_activities]
    garmin = garmin or (_login() if candidates else None)
    results: list[dict[str, Any]] = []
    running_ids: set[str] = set()

    for row in candidates:
        activity_id = row["Activity ID"].removeprefix("garmin-")
        result: dict[str, Any] = {
            "activity_id": row["Activity ID"],
            "date": row.get("Activity Date", "")[:10],
            "name": row.get("Activity Name", ""),
            "is_new": row["Activity ID"] not in previous_ids,
            "files_ready": False,
            "coach_status": "not_applicable",
        }
        try:
            activity = garmin.get_activity(activity_id)
            if not isinstance(activity, dict):
                raise ValueError("Garmin did not return activity details")
            activity = _complete_activity_details(activity, row)
            refreshed = refresh_activity(
                activity,
                garmin=garmin,
                store=store,
                existing_keys=existing_keys,
                output=Path(".granular/on-demand-refresh"),
            )
            result["file_status"] = refreshed["status"]
            result["files_ready"] = all(
                key in existing_keys for key in activity_artifact_keys(activity)
            ) if refreshed["status"] != "unsupported" else False
            if "run" in activity_type(activity):
                running_ids.add(activity_id)
                result["coach_status"] = "pending" if result["files_ready"] else "missing_artifacts"
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError("Garmin service or authentication error") from exc
            result["file_status"] = "error"
            result["error"] = str(exc)[:300]
            if "run" in row.get("Activity Type", "").lower():
                result["coach_status"] = "error"
        results.append(result)

    # Prioritize the specific workouts that this refresh just imported. The
    # ordinary scheduled coach backfill still handles the remaining history.
    coach_plan = run_coach_backfill(
        max_activities=MAX_ACTIVITIES,
        store=store,
        priority_activity_ids=running_ids,
    ) if running_ids else None
    processed = coach_plan.get("processed_sources", {}) if coach_plan else {}
    skipped = {
        item.get("activity_id")
        for item in coach_plan.get("skipped_this_run", [])
    } if coach_plan else set()
    blocked = {
        item.get("activity_id"): item.get("reason")
        for item in coach_plan.get("blocked_activities", [])
    } if coach_plan else {}
    for result in results:
        activity_id = result["activity_id"].removeprefix("garmin-")
        if activity_id not in running_ids or not result["files_ready"]:
            continue
        coach_prefix = (
            f"activities/{result['date'][:4]}/{activity_id}/coach-input/v1/canonical/"
        )
        if (
            activity_id in processed
            and activity_id not in skipped
            and store.list_keys(coach_prefix)
        ):
            result["coach_status"] = "ready"
        else:
            result["coach_status"] = str(blocked.get(activity_id) or "pending")

    report = {
        "schema_version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "new_activity_count": len(new_rows),
        "checked_activity_count": len(results),
        "remaining_candidate_count": max(0, len(new_rows) + len(fallback_rows) - len(results)),
        "activities": results,
    }
    if run_id is not None:
        store.put(
            f"refresh/reports/{run_id}.json",
            json.dumps(report, separators=(",", ":"), ensure_ascii=False).encode(),
            "application/json",
        )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Prioritize workouts in an on-demand refresh")
    subparsers = parser.add_subparsers(dest="action", required=True)
    before = subparsers.add_parser("snapshot")
    before.add_argument("--summary", type=Path, required=True)
    before.add_argument("--output", type=Path, required=True)
    process = subparsers.add_parser("process")
    process.add_argument("--baseline", type=Path, required=True)
    process.add_argument("--summary", type=Path, required=True)
    process.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if args.action == "snapshot":
        snapshot(args.summary, args.output)
    else:
        run(baseline=args.baseline, summary=args.summary, run_id=args.run_id)


if __name__ == "__main__":
    main()
