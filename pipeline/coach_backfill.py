"""Bounded, resumable coach-input generation using R2 data only."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
from datetime import datetime, timezone
from typing import Any

from .coach import ANALYZER_VERSION, build_coach_input, select_profile
from .granular import gzip_json, sha256
from .r2_store import R2Store

PLAN_KEY = "backfill/coach-input/v1/plan.json"
PROFILE_PREFIX = "coach/profiles/v1/"
MAX_ACTIVITIES_PER_RUN = 50


def _decode(data: bytes) -> bytes:
    return gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data


def _json(store, key: str) -> dict[str, Any]:
    value = json.loads(_decode(store.get(key)))
    if not isinstance(value, dict):
        raise ValueError(f"R2 object {key} is not a JSON object")
    return value


def _activities(data: bytes) -> list[dict[str, Any]]:
    text = _decode(data).decode("utf-8-sig")
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        activity_id = str(row.get("Activity ID") or "")
        if activity_id.startswith("garmin-"):
            raw_id = activity_id.removeprefix("garmin-")
        else:
            raw_id = activity_id
        activity_type = str(row.get("Activity Type") or "")
        if not raw_id.isdigit() or "run" not in activity_type.lower():
            continue
        raw_date = str(row.get("Activity Date") or "")
        rows.append({
            "id": raw_id,
            "date": raw_date[:10],
            "name": row.get("Activity Name"),
            "type": activity_type,
            "moving_seconds": _float(row.get("Moving Time")),
        })
    return sorted(rows, key=lambda item: (item["date"], item["id"]), reverse=True)


def _float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _profiles(store) -> list[dict[str, Any]]:
    profiles = []
    for key in sorted(store.list_keys(PROFILE_PREFIX)):
        try:
            profiles.append(_json(store, key))
        except (ValueError, json.JSONDecodeError, KeyError):
            continue
    return profiles


def _latest_context_keys(all_keys: set[str]) -> dict[str, str]:
    """Find the newest context key per activity from one shared R2 listing."""
    latest: dict[str, str] = {}
    marker = "/context/v1/"
    for key in all_keys:
        if marker not in key:
            continue
        prefix = key.split(marker, 1)[0]
        if key > latest.get(prefix, ""):
            latest[prefix] = key
    return latest


def _manifest_revisions(
    store,
    all_keys: set[str],
    object_revisions: dict[str, str],
    running_ids: set[str],
) -> dict[str, str]:
    """Read change revisions from listing metadata, with a test-store fallback."""
    revisions: dict[str, str] = {}
    suffix = "/source-manifest.v1.json"
    for key in sorted(item for item in all_keys if item.endswith(suffix)):
        parts = key.split("/")
        if len(parts) < 4 or parts[0] != "activities" or parts[2] not in running_ids:
            continue
        revision = object_revisions.get(key)
        revisions[parts[2]] = revision if revision is not None else sha256(store.get(key))
    return revisions


def _source_signature(
    activity: dict[str, Any],
    profile: dict[str, Any],
    *,
    context_key: str | None,
    manifest_revision: str | None,
) -> str:
    """Identify the inputs that decide whether one coach analysis is stale."""
    value = {
        "analyzer_version": ANALYZER_VERSION,
        "activity": activity,
        "profile": profile,
        "context_key": context_key,
        "manifest_revision": manifest_revision,
    }
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def run(*, max_activities: int = 50, store: R2Store | None = None) -> dict[str, Any]:
    if not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}")
    store = store or R2Store()
    summary_bytes = store.get("summary/activities.csv")
    profiles = _profiles(store)
    activities = _activities(summary_bytes)
    if hasattr(store, "list_object_revisions"):
        object_revisions = store.list_object_revisions("activities/")
        all_keys = set(object_revisions)
    else:
        object_revisions = {}
        all_keys = store.list_keys("activities/")
    running_ids = {activity["id"] for activity in activities}
    context_keys = _latest_context_keys(all_keys)
    manifest_revisions = _manifest_revisions(
        store, all_keys, object_revisions, running_ids
    )
    fingerprint_parts = [
        f"analyzer:{ANALYZER_VERSION}",
        sha256(summary_bytes),
        *sorted(store.list_keys(PROFILE_PREFIX)),
        *sorted(
            key
            for key in all_keys
            if any(
                marker in key
                for marker in (
                    "/activity.v1.json",
                    "/activity.endurance.v1.json",
                    "/activity.tcx",
                    "/context/v1/",
                )
            )
        ),
    ]
    # Activity refreshes replace canonical objects under the same keys. Their
    # small manifests make those replacements visible without downloading the
    # FIT, TCX and normalized JSON for every previously processed activity.
    fingerprint_parts.extend(
        f"manifest:{activity_id}:{revision}"
        for activity_id, revision in sorted(manifest_revisions.items())
    )
    source_fingerprint = sha256("\n".join(fingerprint_parts).encode())
    previous: dict[str, Any] = {}
    if PLAN_KEY in store.list_keys("backfill/coach-input/v1/"):
        previous = _json(store, PLAN_KEY)
        if previous.get("source_fingerprint") == source_fingerprint and previous.get("status") in {
            "complete",
            "complete_with_blocked",
            "waiting_for_profile",
        }:
            return previous
    processed_sources = previous.get("processed_sources")
    if not isinstance(processed_sources, dict):
        processed_sources = {}
    processed_sources = {
        str(activity_id): str(signature)
        for activity_id, signature in processed_sources.items()
        if str(activity_id) in running_ids and isinstance(signature, str)
    }

    pending = []
    blocked = []
    already_complete = 0
    for activity in activities:
        profile = select_profile(profiles, activity["date"])
        if not profile:
            blocked.append({"activity_id": activity["id"], "reason": "no_effective_profile"})
            processed_sources.pop(activity["id"], None)
            continue
        prefix = f"activities/{activity['date'][:4]}/{activity['id']}"
        fit_key = f"{prefix}/activity.v1.json"
        endurance_key = f"{prefix}/activity.endurance.v1.json"
        tcx_key = f"{prefix}/activity.tcx"
        missing = [key for key in (fit_key, endurance_key, tcx_key) if key not in all_keys]
        if missing:
            blocked.append(
                {"activity_id": activity["id"], "reason": "missing_artifacts", "keys": missing}
            )
            processed_sources.pop(activity["id"], None)
            continue
        context_key = context_keys.get(prefix)
        signature = _source_signature(
            activity,
            profile,
            context_key=context_key,
            manifest_revision=manifest_revisions.get(activity["id"]),
        )
        if processed_sources.get(activity["id"]) == signature:
            already_complete += 1
            continue
        pending.append(
            {
                "activity": activity,
                "profile": profile,
                "prefix": prefix,
                "fit_key": fit_key,
                "endurance_key": endurance_key,
                "tcx_key": tcx_key,
                "context_key": context_key,
                "source_signature": signature,
            }
        )

    completed = []
    skipped = []
    for item in pending[:max_activities]:
        activity = item["activity"]
        endurance = _json(store, item["endurance_key"])
        if endurance.get("available") is False:
            processed_sources[activity["id"]] = item["source_signature"]
            skipped.append({
                "activity_id": activity["id"],
                "reason": str(endurance.get("reason") or "endurance_data_unavailable"),
            })
            continue
        decoded_fit = _json(store, item["fit_key"])
        tcx = store.get(item["tcx_key"])
        context = _json(store, item["context_key"]) if item["context_key"] else None
        coach = build_coach_input(
            activity=activity,
            decoded_fit=decoded_fit,
            endurance=endurance,
            profile=item["profile"],
            tcx_sha256=sha256(tcx),
            context=context,
        )
        key = f"{item['prefix']}/coach-input/v1/canonical/{coach['analysis_id']}.json"
        payload = gzip_json(coach)
        store.put(key, payload, "application/json", encoding="gzip")
        all_keys.add(key)
        processed_sources[activity["id"]] = item["source_signature"]
        completed.append(
            {
                "activity_id": coach["activity_id"],
                "analysis_id": coach["analysis_id"],
                "key": key,
            }
        )

    # Missing source artifacts remain visible but do not keep this R2-only job
    # spinning. Pending work is now derived from per-activity source revisions,
    # so later runs do not reread artifacts that were already analyzed.
    processed_this_run = len(completed) + len(skipped)
    remaining = max(0, len(pending) - processed_this_run)
    status = (
        "complete_with_blocked"
        if remaining == 0 and blocked
        else "complete"
        if remaining == 0
        else "active"
    )
    if not profiles:
        status = "waiting_for_profile"
    now = datetime.now(timezone.utc).isoformat()
    plan = {
        "schema_version": 1,
        "status": status,
        "updated_at": now,
        "source_fingerprint": source_fingerprint,
        "processed_sources": processed_sources,
        "processed_source_count": len(processed_sources),
        "running_activities": len(activities),
        "already_current_activities": already_complete,
        "attempted_this_run": processed_this_run,
        "generated_this_run": len(completed),
        "completed_this_run": completed,
        "skipped_this_run": skipped,
        "remaining_activities": remaining,
        "blocked_activity_count": len(blocked),
        "blocked_activities": blocked[:100],
        "profile_count": len(profiles),
    }
    if status in {"complete", "complete_with_blocked"}:
        plan["completed_at"] = now
    store.put(PLAN_KEY, json.dumps(plan, separators=(",", ":")).encode(), "application/json")
    print(json.dumps(plan, indent=2, ensure_ascii=False))
    return plan


def main():
    parser = argparse.ArgumentParser(description="Generate coach inputs from existing R2 artifacts.")
    parser.add_argument("--max-activities", type=int, default=50)
    args = parser.parse_args()
    try:
        run(max_activities=args.max_activities)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
