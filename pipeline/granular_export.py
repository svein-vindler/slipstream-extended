"""Run a deliberately small Garmin granular-data pilot and upload it to R2."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .granular import (
    activity_prefix,
    activity_type,
    decode_fit,
    extract_fit,
    gzip_bytes,
    gzip_json,
    is_endurance_activity,
    normalize_endurance_session,
    normalize_hrv,
    select_activity_sample,
    sha256,
)
from .health_history_index import sync_dates
from .sources.garmin import _login

MAX_HRV_DAYS_PER_RUN = 60
MAX_ACTIVITY_LOOKBACK_DAYS = 3660
MAX_ACTIVITY_SAMPLE_PER_RUN = 50


def validate_granular_request(
    hrv_days: int,
    activity_days: int,
    cardio_count: int,
    strength_count: int,
):
    values = (hrv_days, activity_days, cardio_count, strength_count)
    if min(values) < 0:
        raise ValueError("Counts and day windows cannot be negative")
    if hrv_days > MAX_HRV_DAYS_PER_RUN:
        raise ValueError(f"hrv_days cannot exceed {MAX_HRV_DAYS_PER_RUN} per run")
    if activity_days > MAX_ACTIVITY_LOOKBACK_DAYS:
        raise ValueError(
            f"activity_days cannot exceed {MAX_ACTIVITY_LOOKBACK_DAYS} per run"
        )
    if cardio_count + strength_count > MAX_ACTIVITY_SAMPLE_PER_RUN:
        raise ValueError(
            "combined cardio_count and strength_count cannot exceed "
            f"{MAX_ACTIVITY_SAMPLE_PER_RUN} per run"
        )


def _write_or_upload(
    output: Path,
    key: str,
    data: bytes,
    content_type: str,
    store,
    *,
    encoding: str | None = None,
):
    target = output / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    if store:
        store.put(key, data, content_type, encoding=encoding)
    return {"key": key, "bytes": len(data), "sha256": sha256(data)}


def activity_artifact_keys(activity: dict[str, Any]) -> tuple[str, ...]:
    prefix = activity_prefix(activity)
    keys = [f"{prefix}/activity.fit", f"{prefix}/activity.v1.json"]
    if is_endurance_activity(activity_type(activity)):
        keys.extend((
            f"{prefix}/activity.tcx",
            f"{prefix}/activity.endurance.v1.json",
        ))
    return tuple(keys)


def export_activity(
    activity: dict[str, Any],
    garmin,
    store,
    output: Path,
    *,
    existing_keys: set[str] | None = None,
    force: bool = False,
    exercise_sets: Any = None,
) -> dict[str, Any]:
    """Export missing artifacts, or replace every canonical artifact when forced."""
    activity_id = activity.get("activityId")
    if not activity_id:
        raise ValueError("Activity has no Garmin activityId")
    kind = activity_type(activity)
    prefix = activity_prefix(activity)
    known = set() if force else (existing_keys if existing_keys is not None else set())
    fit_key = f"{prefix}/activity.fit"
    json_key = f"{prefix}/activity.v1.json"
    tcx_key = f"{prefix}/activity.tcx"
    endurance_key = f"{prefix}/activity.endurance.v1.json"
    required = activity_artifact_keys(activity)
    record: dict[str, Any] = {
        "id": str(activity_id),
        "type": kind,
        "required_keys": list(required),
        "files": [],
    }
    if not force and existing_keys is not None and all(key in known for key in required):
        record["status"] = "already_complete"
        return record

    fit_data = None
    if fit_key not in known:
        from garminconnect import Garmin

        original = garmin.download_activity(
            activity_id, dl_fmt=Garmin.ActivityDownloadFormat.ORIGINAL
        )
        fit_data = extract_fit(original)
        record["files"].append(
            _write_or_upload(
                output, fit_key, fit_data, "application/octet-stream", store
            )
        )

    if json_key not in known:
        if fit_data is None:
            if store is None:
                raise ValueError("Existing FIT data requires an R2 store for decoding")
            fit_data = store.get(fit_key)
        if "strength" in kind:
            if exercise_sets is None:
                try:
                    exercise_sets = garmin.get_activity_exercise_sets(activity_id)
                except Exception as exc:
                    exercise_sets = {"error": str(exc)}
        decoded = decode_fit(fit_data, activity=activity, exercise_sets=exercise_sets)
        record["fit_messages"] = decoded["message_counts"]
        record["strength_sets"] = len(decoded["normalized_strength_sets"])
        record["files"].append(
            _write_or_upload(
                output,
                json_key,
                gzip_json(decoded),
                "application/json",
                store,
                encoding="gzip",
            )
        )

    if is_endurance_activity(kind):
        tcx = None
        if tcx_key not in known:
            from garminconnect import Garmin

            tcx = garmin.download_activity(
                activity_id, dl_fmt=Garmin.ActivityDownloadFormat.TCX
            )
            compressed_tcx = gzip_bytes(tcx)
            tcx_record = _write_or_upload(
                output,
                tcx_key,
                compressed_tcx,
                "application/vnd.garmin.tcx+xml",
                store,
                encoding="gzip",
            )
            tcx_record["uncompressed_bytes"] = len(tcx)
            tcx_record["content_encoding"] = "gzip"
            record["files"].append(tcx_record)

        if endurance_key not in known:
            if tcx is None:
                if store is None:
                    raise ValueError("Existing TCX data requires an R2 store for normalization")
                tcx = store.get(tcx_key)
            endurance = normalize_endurance_session(activity, tcx)
            record["endurance_trackpoints"] = endurance["summary"]["trackpoint_count"]
            record["files"].append(
                _write_or_upload(
                    output,
                    endurance_key,
                    gzip_json(endurance),
                    "application/json",
                    store,
                    encoding="gzip",
                )
            )

    record["status"] = "updated"
    return record


def run(
    *,
    hrv_days: int = 7,
    activity_days: int = 180,
    cardio_count: int = 2,
    strength_count: int = 2,
    output_dir: str = ".granular",
    upload_r2: bool = False,
) -> dict[str, Any]:
    validate_granular_request(
        hrv_days, activity_days, cardio_count, strength_count
    )

    store = None
    if upload_r2:
        from .r2_store import R2Store

        store = R2Store()

    output = Path(output_dir)
    garmin = _login()
    manifest: dict[str, Any] = {"schema_version": 1, "hrv": [], "activities": []}

    today = date.today()
    for offset in range(hrv_days - 1, -1, -1):
        day = (today - timedelta(days=offset)).isoformat()
        try:
            payload = normalize_hrv(day, garmin.get_hrv_data(day))
            data = gzip_json(payload)
            key = f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"
            item = _write_or_upload(
                output, key, data, "application/json", store, encoding="gzip"
            )
            item["date"] = day
            item["readings"] = payload["reading_count"]
            manifest["hrv"].append(item)
        except Exception as exc:
            manifest["hrv"].append({"date": day, "error": str(exc)})

    if store and hasattr(store, "list_object_revisions"):
        completed_hrv_dates = [
            item["date"] for item in manifest["hrv"]
            if isinstance(item, dict) and "error" not in item and "date" in item
        ]
        if completed_hrv_dates:
            sync_dates(store, "hrv", completed_hrv_dates)

    sample = []
    if cardio_count or strength_count:
        start = (today - timedelta(days=activity_days)).isoformat()
        activities = garmin.get_activities_by_date(start, today.isoformat())
        sample = select_activity_sample(
            activities, cardio_count=cardio_count, strength_count=strength_count
        )
    for activity in sample:
        try:
            record = export_activity(activity, garmin, store, output)
        except Exception as exc:
            record = {
                "id": str(activity.get("activityId")),
                "type": activity_type(activity),
                "files": [],
                "error": str(exc),
            }
        manifest["activities"].append(record)

    manifest_data = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
    _write_or_upload(
        output, "pilot/manifest.json", manifest_data, "application/json", store
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Export a small granular Garmin pilot.")
    parser.add_argument("--hrv-days", type=int, default=7)
    parser.add_argument("--activity-days", type=int, default=180)
    parser.add_argument("--cardio-count", type=int, default=2)
    parser.add_argument("--strength-count", type=int, default=2)
    parser.add_argument("--output-dir", default=".granular")
    parser.add_argument("--upload-r2", action="store_true")
    args = parser.parse_args()
    try:
        validate_granular_request(
            args.hrv_days, args.activity_days, args.cardio_count, args.strength_count
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from None
    run(
        hrv_days=args.hrv_days,
        activity_days=args.activity_days,
        cardio_count=args.cardio_count,
        strength_count=args.strength_count,
        output_dir=args.output_dir,
        upload_r2=args.upload_r2,
    )


if __name__ == "__main__":
    main()
