"""Fetch one recent Garmin activity through its summary, files and coach input."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .activity_backfill import is_job_stopping_error, is_supported_activity
from .activity_refresh import refresh_activity
from .coach_backfill import _json
from .coach_backfill import run_one as run_coach_one
from .freshness import activity_scope, publish, receipt, recent_day
from .granular import activity_type, gzip_json
from .granular_export import activity_artifact_keys
from .manual_activity_refresh import _complete_activity_details, _rows
from .r2_store import R2BudgetError, R2Store
from .schema import Activity
from .sources import garmin as garmin_source
from .summary_export import run as export_summaries
from .writer import write_dataset

MAX_LOOKBACK_DAYS = 7


def _garmin_row(activity: Activity) -> dict[str, str]:
    return {
        "Activity ID": f"garmin-{activity.source_id}",
        "Activity Date": activity.start.strftime("%Y-%m-%d %H:%M:%S") if activity.start else "",
        "Activity Name": activity.name or "",
        "Activity Type": activity.raw_sport or activity.sport or "",
    }


def _supported(activity: Activity) -> bool:
    return is_supported_activity({"activityType": {"typeKey": activity.raw_sport or activity.sport or ""}})


def _coach_status(store: R2Store, activity: Activity, plan: dict[str, Any]) -> str:
    activity_id = str(activity.source_id)
    processed = plan.get("processed_sources", {})
    skipped = {
        str(item.get("activity_id"))
        for item in plan.get("skipped_this_run", [])
        if isinstance(item, dict)
    }
    blocked = {
        str(item.get("activity_id")): str(item.get("reason") or "blocked")
        for item in plan.get("blocked_activities", [])
        if isinstance(item, dict)
    }
    year = (activity.local_start_date or activity.start.date().isoformat())[:4]
    prefix = (
        f"activities/{year}/{activity_id}/"
        "coach-input/v1/canonical/"
    )
    if activity_id in processed and activity_id not in skipped and store.list_keys(prefix):
        return "ready"
    return blocked.get(activity_id, "pending")


def run(
    *,
    run_id: str,
    data_dir: str = "data",
    lookback_days: int = MAX_LOOKBACK_DAYS,
    requested_activity_id: str | None = None,
    new_activity_expected: bool = True,
    expected_date: str | None = None,
    request_id: str | None = None,
    repair_only: bool = False,
    store: R2Store | None = None,
    garmin=None,
) -> dict[str, Any]:
    if not run_id.isdigit():
        raise ValueError("run_id must contain digits only")
    if not 1 <= lookback_days <= MAX_LOOKBACK_DAYS:
        raise ValueError(f"lookback_days must be between 1 and {MAX_LOOKBACK_DAYS}")
    if requested_activity_id is not None and (
        not requested_activity_id.isdigit() or len(requested_activity_id) > 20
    ):
        raise ValueError("requested_activity_id must be a numeric Garmin ID")
    if expected_date is not None:
        try:
            parsed_date = date.fromisoformat(expected_date)
        except ValueError as exc:
            raise ValueError("expected_date must use YYYY-MM-DD") from exc
        if len(expected_date) != 10 or parsed_date.isoformat() != expected_date:
            raise ValueError("expected_date must use YYYY-MM-DD")

    if expected_date:
        recent_day(expected_date)
    store = store or R2Store()
    scope = activity_scope(requested_activity_id, expected_date, new_activity_expected)
    receipt(store, run_id, request_id, scope)
    if repair_only:
        if not requested_activity_id or not expected_date:
            raise ValueError("R2 repair requires an activity ID and local day")
        recent_day(expected_date)
        prefix = f"activities/{expected_date[:4]}/{requested_activity_id}"
        decoded = _json(store, f"{prefix}/activity.v1.json")
        canonical = decoded.get("activity", {})
        local = str(canonical.get("start_time_local") or "")
        if str(canonical.get("id")) != requested_activity_id or local[:10] != expected_date:
            raise ValueError("Canonical activity does not match requested ID and local date")
        activity = {
            "id": requested_activity_id, "date": expected_date,
            "name": canonical.get("name"), "type": canonical.get("type"),
            "moving_seconds": None,
        }
        plan = run_coach_one(activity=activity, store=store)
        blocked = plan.get("blocked_activities", [])
        report = {"schema_version": 1, "kind": "activity-repair",
                  "activity_id": f"garmin-{requested_activity_id}",
                  "activity_date": expected_date,
                  "status": "coach_pending" if blocked else "ready",
                  "coach_status": blocked[0]["reason"] if blocked else "ready"}
        return publish(store, run_id, scope, report, source_checked=False)
    garmin = garmin or garmin_source._login()
    summary_path = Path(data_dir) / "activities.csv"
    previous_ids = {
        row["Activity ID"] for row in _rows(summary_path)
    } if summary_path.exists() else set()
    fetch_end = max(date.today(), date.fromisoformat(expected_date)) if expected_date else None
    fetched = garmin_source.fetch(days_back=lookback_days, client=garmin, end_date=fetch_end)
    candidates = [item for item in fetched if item.start and _supported(item)]
    matching = [item for item in candidates if item.local_start_date == expected_date]
    ambiguous = bool(expected_date and not requested_activity_id and len(matching) > 1)
    candidates.sort(key=lambda item: item.start, reverse=True)
    selected = next(
        (item for item in candidates if str(item.source_id) == requested_activity_id),
        None,
    ) if requested_activity_id else next(iter(matching if expected_date else candidates), None)

    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "latest-activity",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "status": "no_recent_activity",
        "activity_id": None,
        "activity_date": None,
        "activity_started_at_utc": None,
        "activity_started_at_garmin_local": None,
        "expected_date": expected_date,
        "already_in_slipstream": False,
        "activity_name": None,
        "summary_updated": False,
        "files_ready": False,
        "file_status": "not_checked",
        "coach_status": "not_checked",
    }
    if ambiguous:
        report["status"] = "ambiguous_activity"
        report["candidate_ids"] = [f"garmin-{item.source_id}" for item in matching[:20]]
        return publish(store, run_id, scope, report)
    if expected_date and selected is None:
        report["status"] = "expected_activity_missing"
    if selected:
        report.update({
            "activity_id": f"garmin-{selected.source_id}",
            "activity_date": selected.local_start_date,
            "activity_started_at_utc": selected.start.isoformat(),
            "activity_started_at_garmin_local": selected.local_start_time,
            "activity_name": selected.name or "",
            "already_in_slipstream": f"garmin-{selected.source_id}" in previous_ids,
        })
        if not selected.local_start_date:
            report["status"] = "activity_date_unknown"
            return publish(store, run_id, scope, report)
        if expected_date and report["activity_date"] != expected_date:
            report["status"] = "expected_activity_missing"
            return publish(store, run_id, scope, report)
        activity_id = str(selected.source_id)
        activity_year = (selected.local_start_date or selected.start.date().isoformat())[:4]
        prefix = f"activities/{activity_year}/{activity_id}"
        existing_keys = store.list_keys(f"{prefix}/")
        is_running = "run" in (selected.raw_sport or selected.sport or "").lower()
        required = activity_artifact_keys({
            "activityId": activity_id,
            "activityType": {"typeKey": selected.raw_sport or selected.sport or ""},
            "startTimeLocal": (
                f"{selected.local_start_date} 00:00:00"
                if selected.local_start_date else None
            ),
            "startTimeGMT": selected.start.strftime("%Y-%m-%d %H:%M:%S"),
        })
        coach_present = not is_running or bool(store.list_keys(
            f"{prefix}/coach-input/v1/canonical/"
        ))
        if (
            new_activity_expected and not requested_activity_id and not expected_date
            and report["activity_id"] in previous_ids
            and all(key in existing_keys for key in required)
            and coach_present
        ):
            report["status"] = "no_new_activity"
            report["file_status"] = "already_stored"
            report["files_ready"] = True
            report["coach_status"] = "ready" if is_running else "not_applicable"
            return publish(store, run_id, scope, report)
        # Publish the same recent summaries used to select the activity. This
        # keeps list_activities and the selected ID consistent after the run.
        write_dataset(fetched, data_dir, preserve_existing=True)
        export_summaries(data_dir, store=store)
        report["summary_updated"] = True

        try:
            details = garmin.get_activity(str(selected.source_id))
            if not isinstance(details, dict):
                raise ValueError("Garmin returned no activity details")
            if details.get("activityId") is not None and str(details["activityId"]) != activity_id:
                raise ValueError("Garmin returned details for a different activity")
            details = _complete_activity_details(details, _garmin_row(selected))
            if details.get("startTimeLocal") and str(details["startTimeLocal"])[:10] != selected.local_start_date:
                raise ValueError("Garmin details disagree with selected local date")
            if not details.get("startTimeLocal"):
                details["startTimeLocal"] = selected.local_start_time or f"{selected.local_start_date} 00:00:00"
            refreshed = refresh_activity(
                details,
                garmin=garmin,
                store=store,
                existing_keys=existing_keys,
                output=Path(".granular/latest-activity"),
                force=False,
            )
            report["file_status"] = refreshed["status"]
            if refreshed["status"] in {"baseline", "unchanged"}:
                decoded = _json(store, f"{prefix}/activity.v1.json")
                metadata = decoded.get("activity", {})
                if isinstance(metadata, dict) and not metadata.get("start_time_local"):
                    metadata["start_time_local"] = details["startTimeLocal"]
                    store.put(f"{prefix}/activity.v1.json", gzip_json(decoded),
                              "application/json", encoding="gzip")
            report["files_ready"] = refreshed["status"] != "unsupported" and all(
                key in existing_keys for key in activity_artifact_keys(details)
            )
            if report["files_ready"]:
                report["status"] = "ready"
                if "run" in activity_type(details):
                    try:
                        plan = run_coach_one(activity={
                            "id": activity_id, "date": selected.local_start_date,
                            "name": selected.name, "type": selected.raw_sport or selected.sport,
                            "moving_seconds": selected.moving_s,
                        }, store=store)
                        report["coach_status"] = _coach_status(store, selected, plan)
                    except R2BudgetError:
                        raise
                    except Exception as exc:
                        report["coach_status"] = "error"
                        print(f"Latest activity coach input failed: {type(exc).__name__}")
                    if report["coach_status"] != "ready":
                        report["status"] = "coach_pending"
                else:
                    report["coach_status"] = "not_applicable"
            else:
                report["status"] = "files_unavailable"
                report["coach_status"] = "waiting_for_files"
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError("Garmin service or authentication error") from exc
            report["status"] = "files_unavailable"
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            report["file_status"] = (
                "garmin_not_ready" if status_code in {404, 409} else "import_error"
            )
            report["coach_status"] = "waiting_for_files"
            # Do not write provider exception details into an MCP-visible report.
            print(f"Latest activity files unavailable: {type(exc).__name__}")

    return publish(store, run_id, scope, report,
                   source_checked=report["file_status"] != "import_error")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import one recent Garmin activity")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--lookback-days", type=int, default=MAX_LOOKBACK_DAYS)
    parser.add_argument("--activity-id")
    parser.add_argument("--request-id")
    parser.add_argument("--repair-only", action="store_true")
    parser.add_argument("--new-activity-expected", choices=("true", "false"), default="true")
    parser.add_argument("--expected-date")
    args = parser.parse_args()
    run(
        run_id=args.run_id,
        data_dir=args.data_dir,
        lookback_days=args.lookback_days,
        requested_activity_id=args.activity_id,
        new_activity_expected=args.new_activity_expected == "true",
        expected_date=args.expected_date,
        request_id=args.request_id,
        repair_only=args.repair_only,
    )


if __name__ == "__main__":
    main()
