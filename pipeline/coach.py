"""Deterministic coach-input helpers built from private R2 artifacts.

The analyzer deliberately contains no AI calls and no network access.  It turns
the already-normalized FIT/TCX artifacts into stable JSON that a coaching chat
can consume without repeatedly parsing a multi-megabyte TCX file.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Any

COACH_SCHEMA_VERSION = 1
ANALYZER_VERSION = "1.0.0"


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_text(value: Any, *, limit: int = 160) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.replace("\x00", " ").split()).strip()
    return cleaned[:limit] or None


def validate_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a user-owned heart-rate analysis profile."""
    effective_from = str(profile.get("effective_from") or "")
    try:
        date.fromisoformat(effective_from)
    except ValueError as exc:
        raise ValueError("effective_from must be YYYY-MM-DD") from exc

    raw_zones = profile.get("zones")
    if not isinstance(raw_zones, list) or not 1 <= len(raw_zones) <= 10:
        raise ValueError("zones must contain between 1 and 10 ranges")
    zones = []
    previous_max = -1.0
    for raw in raw_zones:
        if not isinstance(raw, dict):
            raise ValueError("every zone must be an object")
        label = _safe_text(raw.get("label"), limit=24)
        minimum = _number(raw.get("min_bpm"))
        maximum = _number(raw.get("max_bpm"))
        if not label or minimum is None or maximum is None or minimum > maximum:
            raise ValueError("every zone needs label and an ordered BPM range")
        if minimum <= previous_max:
            raise ValueError("heart-rate zones must be ordered and non-overlapping")
        previous_max = maximum
        zones.append({"label": label, "min_bpm": int(minimum), "max_bpm": int(maximum)})

    references = profile.get("references")
    if not isinstance(references, dict):
        references = {}
    normalized_references: dict[str, Any] = {}
    for key in ("lt1_min_bpm", "lt1_max_bpm", "lt2_min_bpm", "lt2_max_bpm"):
        value = _number(references.get(key))
        normalized_references[key] = int(value) if value is not None else None
    thresholds = references.get("interval_thresholds_bpm")
    normalized_references["interval_thresholds_bpm"] = sorted({
        int(value) for item in thresholds if (value := _number(item)) is not None
    }) if isinstance(thresholds, list) else []

    profile_id = _safe_text(profile.get("profile_id"), limit=80)
    if not profile_id:
        raise ValueError("profile_id is required")
    return {
        "schema_version": COACH_SCHEMA_VERSION,
        "profile_id": profile_id,
        "name": _safe_text(profile.get("name"), limit=80) or "Heart-rate profile",
        "sport": _safe_text(profile.get("sport"), limit=32) or "running",
        "effective_from": effective_from,
        "default_time_basis": "elapsed",
        "zones": zones,
        "references": normalized_references,
        "created_at": profile.get("created_at"),
        "source": "user",
    }


def select_profile(profiles: list[dict[str, Any]], activity_date: str) -> dict[str, Any] | None:
    eligible = []
    for profile in profiles:
        try:
            normalized = validate_profile(profile)
        except ValueError:
            continue
        if normalized["effective_from"] <= activity_date:
            eligible.append(normalized)
    return max(
        eligible,
        key=lambda item: (str(item["effective_from"]), str(item.get("created_at") or "")),
        default=None,
    )


def _message_rows(decoded_fit: dict[str, Any], *names: str) -> list[dict[str, Any]]:
    messages = decoded_fit.get("messages")
    if not isinstance(messages, dict):
        return []
    for name in names:
        rows = messages.get(name)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def workout_structure(decoded_fit: dict[str, Any]) -> dict[str, Any]:
    """Keep Garmin's planned workout and actual lap execution separate."""
    workout_rows = _message_rows(decoded_fit, "workout_mesgs", "workouts")
    step_rows = _message_rows(decoded_fit, "workout_step_mesgs", "workout_steps")
    lap_rows = _message_rows(decoded_fit, "lap_mesgs", "laps")

    planned = []
    step_lookup: dict[int, dict[str, Any]] = {}
    for ordinal, row in enumerate(step_rows):
        raw_index = _number(row.get("message_index"))
        index = int(raw_index) if raw_index is not None else ordinal
        step = {
            "step_index": index,
            "name": _safe_text(row.get("wkt_step_name") or row.get("name")),
            "intensity": _safe_text(row.get("intensity"), limit=32),
            "duration_type": _safe_text(row.get("duration_type"), limit=40),
            "duration_value": _number(row.get("duration_value")),
            "target_type": _safe_text(row.get("target_type"), limit=40),
            "target_value": _number(row.get("target_value")),
            "repeat_steps": int(value) if (value := _number(row.get("repeat_steps"))) is not None else None,
            "repeat_from_step": int(value) if (value := _number(row.get("duration_step"))) is not None else None,
        }
        planned.append(step)
        step_lookup[index] = step

    executed = []
    for lap_number, row in enumerate(lap_rows, start=1):
        raw_index = _number(row.get("wkt_step_index"))
        index = int(raw_index) if raw_index is not None else None
        plan = step_lookup.get(index if index is not None else -1, {})
        intensity = plan.get("intensity") or _safe_text(row.get("intensity"), limit=32)
        normalized_intensity = str(intensity or "").lower()
        section = (
            "warmup" if "warm" in normalized_intensity
            else "cooldown" if "cool" in normalized_intensity
            else "main"
        )
        executed.append({
            "lap": lap_number,
            "workout_step_index": index,
            "step_name": plan.get("name"),
            "intensity": intensity,
            "section": section,
            "start_time": row.get("start_time") or row.get("timestamp"),
            "elapsed_seconds": _number(row.get("total_elapsed_time")),
            "moving_seconds": _number(row.get("total_timer_time")),
            "distance_m": _number(row.get("total_distance")),
            "average_speed_mps": _number(row.get("enhanced_avg_speed") or row.get("avg_speed")),
            "average_heart_rate_bpm": _number(row.get("avg_heart_rate")),
            "maximum_heart_rate_bpm": _number(row.get("max_heart_rate")),
            "ascent_m": _number(row.get("total_ascent")),
            "descent_m": _number(row.get("total_descent")),
        })

    workout_name = None
    if workout_rows:
        workout_name = _safe_text(
            workout_rows[0].get("wkt_name") or workout_rows[0].get("sport")
        )
    return {
        "source": "garmin_workout" if planned else "none",
        "workout_name": workout_name,
        "planned_steps": planned,
        "executed_laps": executed,
        "has_structured_workout": bool(planned and any(
            item.get("workout_step_index") is not None for item in executed
        )),
    }


def _section_summaries(structure: dict[str, Any], summary: dict[str, Any]):
    if not structure.get("has_structured_workout"):
        return [{
            "section": "main",
            "source": "continuous_activity",
            "elapsed_seconds": summary.get("duration_seconds"),
            "distance_m": summary.get("distance_m"),
            "average_heart_rate_bpm": summary.get("average_heart_rate_bpm"),
            "note": "No Garmin workout structure was available; the full activity is the main section.",
        }]
    output = []
    rows = structure.get("executed_laps") or []
    for section in ("warmup", "main", "cooldown"):
        selected = [row for row in rows if row.get("section") == section]
        if not selected:
            continue
        elapsed = sum(_number(row.get("elapsed_seconds")) or 0 for row in selected)
        distance = sum(_number(row.get("distance_m")) or 0 for row in selected)
        weighted_hr = sum(
            (_number(row.get("average_heart_rate_bpm")) or 0)
            * (_number(row.get("elapsed_seconds")) or 0)
            for row in selected
        )
        output.append({
            "section": section,
            "source": "garmin_executed_laps",
            "laps": len(selected),
            "elapsed_seconds": round(elapsed, 3),
            "distance_m": round(distance, 3),
            "average_heart_rate_bpm": round(weighted_hr / elapsed, 2)
            if elapsed > 0 and weighted_hr > 0 else None,
        })
    return output


def _zone_distribution(seconds_by_bpm: dict[str, Any], profile: dict[str, Any]):
    buckets = {zone["label"]: 0.0 for zone in profile["zones"]}
    below = 0.0
    above = 0.0
    observed = 0.0
    first_min = profile["zones"][0]["min_bpm"]
    last_max = profile["zones"][-1]["max_bpm"]
    for raw_bpm, raw_seconds in seconds_by_bpm.items():
        bpm = _number(raw_bpm)
        seconds = _number(raw_seconds)
        if bpm is None or seconds is None or seconds <= 0:
            continue
        observed += seconds
        match = next((zone for zone in profile["zones"]
                      if zone["min_bpm"] <= bpm <= zone["max_bpm"]), None)
        if match:
            buckets[match["label"]] += seconds
        elif bpm < first_min:
            below += seconds
        elif bpm > last_max:
            above += seconds
    rows = []
    for zone in profile["zones"]:
        seconds = buckets[zone["label"]]
        rows.append({**zone, "seconds": round(seconds, 2),
                     "percent": round(100 * seconds / observed, 2) if observed else 0.0})
    return {
        "time_basis": "elapsed",
        "observed_seconds": round(observed, 2),
        "below_first_zone_seconds": round(below, 2),
        "above_last_zone_seconds": round(above, 2),
        "zones": rows,
    }


def _threshold_times(seconds_by_bpm: dict[str, Any], thresholds: list[int]):
    parsed = [(_number(bpm), _number(seconds)) for bpm, seconds in seconds_by_bpm.items()]
    return {
        str(threshold): round(sum(seconds or 0 for bpm, seconds in parsed
                                  if bpm is not None and bpm >= threshold), 2)
        for threshold in thresholds
    }


def analysis_id(
    *, activity_id: str, fit_sha256: str | None, tcx_sha256: str,
    profile_id: str, context_revision: str | None,
) -> str:
    value = {
        "activity_id": activity_id,
        "analyzer_version": ANALYZER_VERSION,
        "fit_sha256": fit_sha256,
        "tcx_sha256": tcx_sha256,
        "profile_id": profile_id,
        "context_revision": context_revision,
    }
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()[:24]


def build_coach_input(
    *, activity: dict[str, Any], decoded_fit: dict[str, Any],
    endurance: dict[str, Any], profile: dict[str, Any], tcx_sha256: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = validate_profile(profile)
    activity_id = str(activity.get("id") or endurance.get("activity", {}).get("id") or "")
    context_revision = str(context.get("context_id")) if context else None
    identifier = analysis_id(
        activity_id=activity_id,
        fit_sha256=decoded_fit.get("source_fit_sha256"),
        tcx_sha256=tcx_sha256,
        profile_id=str(profile.get("profile_id")),
        context_revision=context_revision,
    )
    summary = endurance.get("summary") if isinstance(endurance.get("summary"), dict) else {}
    bpm_seconds = endurance.get("heart_rate_seconds_by_bpm")
    if not isinstance(bpm_seconds, dict):
        bpm_seconds = {}
    halves = endurance.get("distance_halves")
    if not isinstance(halves, dict):
        halves = None
    drift = None
    if halves:
        first = halves.get("first") if isinstance(halves.get("first"), dict) else {}
        second = halves.get("second") if isinstance(halves.get("second"), dict) else {}
        first_hr = _number(first.get("average_heart_rate_bpm"))
        second_hr = _number(second.get("average_heart_rate_bpm"))
        if first_hr is not None and second_hr is not None:
            drift = round(second_hr - first_hr, 2)
    structure = workout_structure(decoded_fit)
    return {
        "schema_version": COACH_SCHEMA_VERSION,
        "analyzer_version": ANALYZER_VERSION,
        "analysis_id": identifier,
        "activity_id": activity_id,
        "activity": {
            "date": activity.get("date"),
            "name": activity.get("name") or endurance.get("activity", {}).get("name"),
            "type": activity.get("type") or endurance.get("activity", {}).get("type"),
        },
        "profile": {
            "profile_id": profile.get("profile_id"),
            "name": profile.get("name"),
            "effective_from": profile.get("effective_from"),
            "zones": profile["zones"],
            "references": profile["references"],
        },
        "user_context": context,
        "time_basis": "elapsed",
        "summary": {
            "elapsed_seconds": summary.get("duration_seconds"),
            "moving_seconds": activity.get("moving_seconds"),
            "distance_m": summary.get("distance_m"),
            "average_speed_mps": summary.get("average_speed_mps"),
            "average_heart_rate_bpm": summary.get("average_heart_rate_bpm"),
            "maximum_heart_rate_bpm": summary.get("maximum_heart_rate_bpm"),
            "trackpoint_count": summary.get("trackpoint_count"),
            "sampled_trackpoint_count": summary.get("sampled_trackpoint_count"),
        },
        "heart_rate": {
            "zones_total": _zone_distribution(bpm_seconds, profile),
            "seconds_at_or_above_bpm": _threshold_times(
                bpm_seconds, profile["references"]["interval_thresholds_bpm"]
            ),
            "first_to_second_distance_half_drift_bpm": drift,
            "aerobic_decoupling_percent": summary.get("aerobic_decoupling_percent"),
        },
        "workout_structure": structure,
        "sections": _section_summaries(structure, summary),
        "distance_halves": halves,
        "kilometer_splits": endurance.get("kilometer_splits") or [],
        "source": {
            "fit_sha256": decoded_fit.get("source_fit_sha256"),
            "tcx_sha256": tcx_sha256,
            "derived_from": ["activity.v1.json", "activity.endurance.v1.json", "activity.tcx"],
        },
        "limitations": [
            "User context is included only when explicitly supplied.",
            "Unstructured sessions are not auto-classified as intervals.",
            "Garmin planned steps and actual executed laps are kept separate.",
        ],
    }
