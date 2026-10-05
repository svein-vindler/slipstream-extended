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
from .coach_backfill import _float, _json
from .coach_backfill import run_one as run_coach_one
from .granular import activity_prefix, activity_type, gzip_json
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
    # Garmin's single-activity endpoint nests source timestamps in summaryDTO;
    # the activity-list endpoint exposes the same fields at the top level.
    source_summary = activity.get("summaryDTO")
    if isinstance(source_summary, dict):
        for field in ("startTimeLocal", "startTimeGMT"):
            if not complete.get(field) and source_summary.get(field):
                complete[field] = source_summary[field]
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


def _coach_metadata(activity: dict[str, Any], row: dict[str, str], store) -> dict[str, Any] | None:
    """Use Garmin-local dates, including a canonical fallback, never CSV UTC."""
    local = activity.get("startTimeLocal")
    if not local:
        canonical = _json(store, f"{activity_prefix(activity)}/activity.v1.json")
        canonical_activity = canonical.get("activity", {})
        if str(canonical_activity.get("id")) != str(activity["activityId"]):
            return None
        local = canonical_activity.get("start_time_local")
    try:
        day = date.fromisoformat(str(local)[:10]).isoformat()
    except ValueError:
        return None
    moving = _float(row.get("Moving Time"))
    if moving is not None and moving.is_integer():
        moving = int(moving)
    return {"id": str(activity["activityId"]), "date": day,
            "name": activity.get("activityName"), "type": activity_type(activity),
            "moving_seconds": moving}


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
                result["coach_status"] = "pending" if result["files_ready"] else "missing_artifacts"
                if result["files_ready"]:
                    try:
                        if refreshed["status"] in {"baseline", "unchanged"} and activity.get("startTimeLocal"):
                            canonical_key = f"{activity_prefix(activity)}/activity.v1.json"
                            canonical = _json(store, canonical_key)
                            metadata = canonical.get("activity", {})
                            if (isinstance(metadata, dict)
                                    and str(metadata.get("id")) == activity_id
                                    and not metadata.get("start_time_local")):
                                metadata["start_time_local"] = activity["startTimeLocal"]
                                store.put(canonical_key, gzip_json(canonical), "application/json", encoding="gzip")
                        metadata = _coach_metadata(activity, row, store)
                        if metadata is None:
                            result["coach_status"] = "activity_date_unknown"
                        else:
                            plan = run_coach_one(activity=metadata, store=store)
                            blocked = plan.get("blocked_activities", [])
                            result["coach_status"] = (
                                "ready" if activity_id in plan.get("processed_sources", {})
                                else str(blocked[0]["reason"]) if blocked else "pending"
                            )
                            result["coach_reused"] = activity_id in plan.get("reused_sources", {})
                    except R2BudgetError:
                        raise
                    except Exception as exc:
                        result["coach_status"] = "error"
                        print(f"On-demand coach input failed: {type(exc).__name__}")
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
