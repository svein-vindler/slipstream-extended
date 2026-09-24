"""Detect and refresh changed recent Garmin activity artifacts in private R2."""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .activity_backfill import is_job_stopping_error, is_supported_activity
from .granular import activity_prefix, activity_type, json_bytes, sha256
from .granular_export import activity_artifact_keys, export_activity
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login

MAX_LOOKBACK_DAYS = 90
MAX_ACTIVITIES_PER_RUN = 50
MANIFEST_NAME = "source-manifest.v1.json"
FINGERPRINT_FIELDS = (
    "activityId",
    "activityUUID",
    "activityName",
    "description",
    "activityType",
    "eventType",
    "startTimeGMT",
    "startTimeLocal",
    "duration",
    "elapsedDuration",
    "movingDuration",
    "distance",
    "elevationGain",
    "averageHR",
    "maxHR",
    "calories",
    "avgPower",
    "averagePower",
    "manualActivity",
    "privacy",
    "updateTimestamp",
    "lastModifiedDate",
    "lastModifiedTime",
)


def manifest_key(activity: dict[str, Any]) -> str:
    return f"{activity_prefix(activity)}/{MANIFEST_NAME}"


def _hash(value: Any) -> str:
    return sha256(json_bytes(value))


def activity_fingerprint(activity: dict[str, Any]) -> str:
    """Hash stable Garmin metadata that changes after common activity edits."""
    return _hash({key: activity.get(key) for key in FINGERPRINT_FIELDS})


def exercise_sets_fingerprint(exercise_sets: Any) -> str | None:
    if not isinstance(exercise_sets, dict):
        return None
    return _hash(exercise_sets)


def _load_manifest(store: R2Store, key: str, existing_keys: set[str]):
    if key not in existing_keys:
        return None
    try:
        value = json.loads(store.get(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 activity source manifest is invalid: {key}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"R2 activity source manifest must be an object: {key}")
    return value


def refresh_activity(
    activity: dict[str, Any],
    *,
    garmin,
    store: R2Store,
    existing_keys: set[str],
    output: Path,
    force: bool = False,
) -> dict[str, Any]:
    activity_id = str(activity.get("activityId") or "")
    if not activity_id:
        raise ValueError("Activity has no Garmin activityId")
    if not is_supported_activity(activity):
        return {"id": activity_id, "status": "unsupported"}

    exercise_sets = None
    if "strength" in activity_type(activity):
        exercise_sets = garmin.get_activity_exercise_sets(activity_id)

    key = manifest_key(activity)
    previous = _load_manifest(store, key, existing_keys)
    metadata_hash = activity_fingerprint(activity)
    sets_hash = exercise_sets_fingerprint(exercise_sets)
    artifacts_complete = all(
        artifact in existing_keys for artifact in activity_artifact_keys(activity)
    )
    changed = bool(
        previous
        and (
            previous.get("activity_fingerprint") != metadata_hash
            or previous.get("exercise_sets_fingerprint") != sets_hash
        )
    )

    if previous and not force and not changed and artifacts_complete:
        return {"id": activity_id, "status": "unchanged"}

    now = datetime.now(timezone.utc).isoformat()
    if previous is None and not force and artifacts_complete:
        manifest = {
            "schema_version": 1,
            "kind": "activity-source-manifest",
            "activity_id": activity_id,
            "activity_fingerprint": metadata_hash,
            "exercise_sets_fingerprint": sets_hash,
            "observed_at": now,
            "status": "baseline",
        }
        store.put(key, json_bytes(manifest), "application/json")
        existing_keys.add(key)
        return {"id": activity_id, "status": "baseline"}

    exported = export_activity(
        activity,
        garmin,
        store,
        output,
        existing_keys=existing_keys,
        force=True,
        exercise_sets=exercise_sets,
    )
    manifest = {
        "schema_version": 1,
        "kind": "activity-source-manifest",
        "activity_id": activity_id,
        "activity_fingerprint": metadata_hash,
        "exercise_sets_fingerprint": sets_hash,
        "observed_at": now,
        "status": "refreshed",
        "reason": "forced" if force else "source_changed" if changed else "missing_artifact",
        "files": exported.get("files", []),
    }
    store.put(key, json_bytes(manifest), "application/json")
    existing_keys.update(activity_artifact_keys(activity))
    existing_keys.add(key)
    return {"id": activity_id, "status": "refreshed", "reason": manifest["reason"]}


def run(
    *,
    lookback_days: int = 30,
    max_activities: int = 50,
    activity_id: str | None = None,
    force: bool = False,
    request_pause: float = 0.15,
    output_dir: str = ".granular/activity-refresh",
    store: R2Store | None = None,
    garmin=None,
) -> dict[str, Any]:
    if not 1 <= lookback_days <= MAX_LOOKBACK_DAYS:
        raise ValueError(f"lookback_days must be between 1 and {MAX_LOOKBACK_DAYS}")
    if not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(
            f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}"
        )
    if force and not activity_id:
        raise ValueError("force requires an explicit activity_id")

    store = store or R2Store()
    garmin = garmin or _login()
    existing_keys = store.list_keys("activities/")
    output = Path(output_dir)

    if activity_id:
        activity = garmin.get_activity(activity_id)
        if not isinstance(activity, dict):
            raise ValueError(f"Garmin activity was not found: {activity_id}")
        activities = [activity]
    else:
        end = date.today()
        start = end - timedelta(days=lookback_days - 1)
        activities = garmin.get_activities_by_date(
            start.isoformat(), end.isoformat(), sortorder="desc"
        )
        activities = [
            activity for activity in activities if is_supported_activity(activity)
        ][:max_activities]

    results = []
    for index, activity in enumerate(activities):
        try:
            results.append(refresh_activity(
                activity,
                garmin=garmin,
                store=store,
                existing_keys=existing_keys,
                output=output,
                force=force,
            ))
            if request_pause and index + 1 < len(activities):
                time.sleep(request_pause)
        except R2BudgetError:
            raise
        except Exception as exc:
            if is_job_stopping_error(exc):
                raise RuntimeError(
                    "Garmin service or authentication error; stopping activity "
                    "change detection"
                ) from exc
            results.append({
                "id": str(activity.get("activityId") or ""),
                "status": "error",
                "error": str(exc),
            })

    counts: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    report = {
        "schema_version": 1,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback_days,
        "forced_activity_id": activity_id if force else None,
        "activities_checked": len(results),
        "by_status": counts,
        "activities": results,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Refresh changed recent Garmin activity artifacts."
    )
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--max-activities", type=int, default=50)
    parser.add_argument("--activity-id")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        run(
            lookback_days=args.lookback_days,
            max_activities=args.max_activities,
            activity_id=args.activity_id,
            force=args.force,
        )
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
