"""Pure helpers for granular Garmin exports.

Raw FIT/TCX and detailed health streams live in private object storage, not Git.
The helpers here deliberately have no network side effects so they are easy to
test before a pilot touches real Garmin data.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import zipfile
from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree

SCHEMA_VERSION = 1
ENDURANCE_WORDS = (
    "run", "cycl", "bike", "walk", "hike", "swim", "cardio", "row",
    "ski", "elliptical", "stair", "snowshoe", "snow_shoe", "paddle",
    "kayak", "canoe", "triathlon", "multisport",
)


def _json_default(value: Any):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
        allow_nan=False,
    ).encode("utf-8")


def gzip_bytes(value: bytes) -> bytes:
    return gzip.compress(value, compresslevel=9, mtime=0)


def gzip_json(value: Any) -> bytes:
    return gzip_bytes(json_bytes(value))


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_fit(original: bytes) -> bytes:
    """Extract the FIT payload from Garmin's ORIGINAL ZIP response."""
    if original[8:12] == b".FIT":
        return original
    try:
        with zipfile.ZipFile(io.BytesIO(original)) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".fit")]
            if not names:
                raise ValueError("Garmin ORIGINAL archive contains no FIT file")
            return archive.read(names[0])
    except zipfile.BadZipFile as exc:
        raise ValueError("Garmin ORIGINAL response is neither FIT nor ZIP") from exc


def normalize_hrv(day: str, raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    summary = source.get("hrvSummary")
    if not isinstance(summary, dict):
        summary = {}
    readings = source.get("hrvReadings")
    if not isinstance(readings, list):
        readings = []

    normalized = []
    for reading in readings:
        if not isinstance(reading, dict):
            continue
        timestamp = next((reading.get(key) for key in (
            "readingTimeGMT", "readingTimeLocal", "timestamp", "readingTimestamp",
        ) if reading.get(key) is not None), None)
        value = next((reading.get(key) for key in (
            "hrvValue", "value", "hrv", "rmssd",
        ) if reading.get(key) is not None), None)
        normalized.append({"timestamp": timestamp, "hrv_ms": value, "raw": reading})

    return {
        "schema_version": SCHEMA_VERSION,
        "source": "garmin",
        "date": day,
        "sleep_start_gmt": source.get("sleepStartTimestampGMT"),
        "sleep_end_gmt": source.get("sleepEndTimestampGMT"),
        "sleep_start_garmin_local": source.get("sleepStartTimestampLocal"),
        "sleep_end_garmin_local": source.get("sleepEndTimestampLocal"),
        "summary": summary,
        "reading_count": len(normalized),
        "readings": normalized,
    }


def activity_type(activity: dict[str, Any]) -> str:
    raw = activity.get("activityType")
    if isinstance(raw, dict):
        return str(raw.get("typeKey") or "").lower()
    return str(raw or "").lower()


def is_endurance_activity(kind: str) -> bool:
    normalized = kind.lower()
    return any(word in normalized for word in ENDURANCE_WORDS)


def select_activity_sample(
    activities: list[dict[str, Any]], *, cardio_count: int = 2, strength_count: int = 2
) -> list[dict[str, Any]]:
    """Pick recent representative activities without duplicating activity IDs."""
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()

    def take(predicate, limit: int):
        count = 0
        for item in activities:
            activity_id = str(item.get("activityId") or "")
            if not activity_id or activity_id in seen or not predicate(activity_type(item)):
                continue
            selected.append(item)
            seen.add(activity_id)
            count += 1
            if count == limit:
                break

    take(is_endurance_activity, cardio_count)
    take(lambda kind: "strength" in kind, strength_count)
    return selected


def activity_prefix(activity: dict[str, Any]) -> str:
    activity_id = str(activity.get("activityId"))
    timestamp = str(activity.get("startTimeLocal") or activity.get("startTimeGMT") or "unknown")
    year = timestamp[:4] if len(timestamp) >= 4 and timestamp[:4].isdigit() else "unknown"
    return f"activities/{year}/{activity_id}"


def _number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def normalize_strength_session(activity_id: str, exercise_sets: Any) -> dict[str, Any]:
    """Create a compact strength-analysis layer from Garmin's exercise sets.

    The Garmin response and decoded FIT messages are still retained separately.
    This view contains active sets only and attaches an immediately following
    rest message to the active set that preceded it.
    """
    source = exercise_sets if isinstance(exercise_sets, dict) else {}
    rows = source.get("exerciseSets")
    if not isinstance(rows, list):
        rows = []

    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or str(row.get("setType") or "").upper() != "ACTIVE":
            continue

        exercises = row.get("exercises")
        candidates = [item for item in exercises if isinstance(item, dict)] \
            if isinstance(exercises, list) else []
        chosen = max(
            candidates,
            key=lambda item: _number(item.get("probability")) or 0,
            default={},
        )

        rest_after = None
        if index + 1 < len(rows):
            following = rows[index + 1]
            if isinstance(following, dict) and str(
                following.get("setType") or ""
            ).upper() == "REST":
                rest_after = _number(following.get("duration"))

        known = {
            "exercises", "duration", "repetitionCount", "weight", "setType",
            "startTime", "wktStepIndex", "messageIndex",
        }
        performance_metrics = {
            key: _json_safe(value) for key, value in row.items()
            if key not in known and value is not None
        }
        weight_grams = _number(row.get("weight"))
        weight_kg = round(weight_grams / 1000, 3) if weight_grams is not None else None
        normalized.append({
            "set_number": len(normalized) + 1,
            "message_index": row.get("messageIndex"),
            "exercise": {
                "category": chosen.get("category"),
                "name": chosen.get("name"),
                "probability": _number(chosen.get("probability")),
            },
            "repetitions": _number(row.get("repetitionCount")),
            "weight_kg": weight_kg,
            "active_duration_seconds": _number(row.get("duration")),
            "rest_after_seconds": rest_after,
            "start_time": row.get("startTime"),
            "workout_step_index": row.get("wktStepIndex"),
            "performance_metrics": performance_metrics,
        })

    by_exercise: dict[str, dict[str, Any]] = {}
    by_category: dict[str, dict[str, Any]] = {}
    total_reps = 0
    external_volume = 0.0
    total_active = 0.0
    total_rest = 0.0
    for item in normalized:
        category = item["exercise"].get("category") or "UNKNOWN"
        exercise_name = item["exercise"].get("name") or category
        reps = item.get("repetitions") or 0
        weight = item.get("weight_kg") or 0
        duration = item.get("active_duration_seconds") or 0
        rest = item.get("rest_after_seconds") or 0
        for bucket, key in (
            (by_exercise, exercise_name),
            (by_category, category),
        ):
            summary = bucket.setdefault(key, {
                "sets": 0, "repetitions": 0, "external_volume_kg": 0.0,
            })
            summary["sets"] += 1
            summary["repetitions"] += reps
            summary["external_volume_kg"] += reps * weight
        total_reps += reps
        external_volume += reps * weight
        total_active += duration
        total_rest += rest

    return {
        "schema_version": SCHEMA_VERSION,
        "activity_id": str(activity_id),
        "sets": normalized,
        "summary": {
            "active_sets": len(normalized),
            "total_repetitions": total_reps,
            "external_volume_kg": round(external_volume, 3),
            "active_duration_seconds": round(total_active, 3),
            "rest_duration_seconds": round(total_rest, 3),
            "by_exercise": by_exercise,
            "by_category": by_category,
        },
    }


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(element, name: str):
    return next(
        (child for child in element if _local_name(child.tag) == name),
        None,
    )


def _child_text(element, name: str) -> str | None:
    child = _child(element, name)
    return child.text.strip() if child is not None and child.text else None


def _nested_number(element, parent: str, child: str) -> float | int | None:
    container = _child(element, parent)
    return _number(_child_text(container, child)) if container is not None else None


def _timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _interpolate_time(points: list[dict[str, Any]], distance_m: float) -> float | None:
    before = None
    for point in points:
        current_distance = point.get("distance_m")
        current_time = point.get("epoch_seconds")
        if current_distance is None or current_time is None:
            continue
        if current_distance >= distance_m:
            if before is None:
                return float(current_time)
            previous_distance = float(before["distance_m"])
            distance_delta = float(current_distance) - previous_distance
            if distance_delta <= 0:
                return float(current_time)
            fraction = (distance_m - previous_distance) / distance_delta
            return float(before["epoch_seconds"]) + fraction * (
                float(current_time) - float(before["epoch_seconds"])
            )
        before = point
    return None


def _range_stats(
    points: list[dict[str, Any]], start_m: float, end_m: float
) -> dict[str, Any]:
    selected = [
        point for point in points
        if point.get("distance_m") is not None
        and start_m <= float(point["distance_m"]) <= end_m
    ]
    heart_values = [
        float(point["heart_rate_bpm"])
        for point in selected if point.get("heart_rate_bpm") is not None
    ]
    altitudes = [
        float(point["altitude_m"])
        for point in selected if point.get("altitude_m") is not None
    ]
    start_time = _interpolate_time(points, start_m)
    end_time = _interpolate_time(points, end_m)
    duration = end_time - start_time if start_time is not None and end_time is not None else None
    distance = max(0.0, end_m - start_m)
    speed = distance / duration if duration and duration > 0 else None
    average_hr = sum(heart_values) / len(heart_values) if heart_values else None
    return {
        "distance_m": round(distance, 3),
        "duration_seconds": round(duration, 3) if duration is not None else None,
        "pace_seconds_per_km": round(duration / (distance / 1000), 3)
        if duration and distance > 0 else None,
        "average_speed_mps": round(speed, 4) if speed is not None else None,
        "average_heart_rate_bpm": round(average_hr, 2)
        if average_hr is not None else None,
        "maximum_heart_rate_bpm": round(max(heart_values), 2)
        if heart_values else None,
        "elevation_change_m": round(altitudes[-1] - altitudes[0], 2)
        if len(altitudes) >= 2 else None,
    }


def normalize_endurance_session(
    activity: dict[str, Any], tcx_data: bytes
) -> dict[str, Any]:
    """Create a compact, GPS-free analysis layer from a Garmin TCX export."""
    if tcx_data[:2] == b"\x1f\x8b":
        tcx_data = gzip.decompress(tcx_data)
    try:
        root = ElementTree.fromstring(tcx_data)
    except ElementTree.ParseError as exc:
        raise ValueError("Garmin TCX export is invalid XML") from exc

    activity_node = next(
        (node for node in root.iter() if _local_name(node.tag) == "Activity"),
        None,
    )
    if activity_node is None:
        raise ValueError("Garmin TCX export contains no activity")

    laps = []
    points: list[dict[str, Any]] = []
    for lap_number, lap in enumerate(
        (node for node in activity_node if _local_name(node.tag) == "Lap"),
        start=1,
    ):
        laps.append({
            "lap": lap_number,
            "start_time": lap.attrib.get("StartTime"),
            "duration_seconds": _number(_child_text(lap, "TotalTimeSeconds")),
            "distance_m": _number(_child_text(lap, "DistanceMeters")),
            "maximum_speed_mps": _number(_child_text(lap, "MaximumSpeed")),
            "calories": _number(_child_text(lap, "Calories")),
            "average_heart_rate_bpm": _nested_number(
                lap, "AverageHeartRateBpm", "Value"
            ),
            "maximum_heart_rate_bpm": _nested_number(
                lap, "MaximumHeartRateBpm", "Value"
            ),
            "intensity": _child_text(lap, "Intensity"),
            "trigger_method": _child_text(lap, "TriggerMethod"),
        })
        for trackpoint in (
            node for node in lap.iter() if _local_name(node.tag) == "Trackpoint"
        ):
            time_text = _child_text(trackpoint, "Time")
            point_time = _timestamp(time_text)
            if point_time is None:
                continue
            speed = next((
                _number(node.text)
                for node in trackpoint.iter()
                if _local_name(node.tag) == "Speed" and node.text
            ), None)
            points.append({
                "timestamp": time_text,
                "epoch_seconds": point_time.timestamp(),
                "distance_m": _number(_child_text(trackpoint, "DistanceMeters")),
                "altitude_m": _number(_child_text(trackpoint, "AltitudeMeters")),
                "heart_rate_bpm": _nested_number(trackpoint, "HeartRateBpm", "Value"),
                "cadence_rpm": _number(_child_text(trackpoint, "Cadence")),
                "speed_mps": speed,
            })

    points.sort(key=lambda point: point["epoch_seconds"])
    if not points:
        duration = sum(
            float(lap["duration_seconds"])
            for lap in laps if lap.get("duration_seconds") is not None
        )
        distance = sum(
            float(lap["distance_m"])
            for lap in laps if lap.get("distance_m") is not None
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "garmin-tcx",
            "privacy": "GPS coordinates and route geometry removed",
            "available": False,
            "reason": "no_timed_trackpoints",
            "activity": {
                "id": str(activity.get("activityId")),
                "name": activity.get("activityName"),
                "type": activity_type(activity),
                "sport": activity_node.attrib.get("Sport"),
                "start_time": laps[0].get("start_time") if laps else None,
            },
            "summary": {
                "duration_seconds": round(duration, 3),
                "distance_m": round(distance, 3),
                "average_speed_mps": None,
                "average_pace_seconds_per_km": None,
                "average_heart_rate_bpm": None,
                "maximum_heart_rate_bpm": None,
                "minimum_altitude_m": None,
                "maximum_altitude_m": None,
                "aerobic_decoupling_percent": None,
                "trackpoint_count": 0,
                "sampled_trackpoint_count": 0,
                "observed_heart_rate_seconds": 0.0,
            },
            "laps": laps,
            "kilometer_splits": [],
            "distance_halves": None,
            "heart_rate_seconds_by_bpm": {},
            "sample_interval_seconds": 10,
            "sampled_trackpoints": [],
        }
    first_epoch = float(points[0]["epoch_seconds"])
    for index, point in enumerate(points):
        point["elapsed_seconds"] = round(float(point["epoch_seconds"]) - first_epoch, 3)
        if point.get("speed_mps") is None and index > 0:
            previous = points[index - 1]
            time_delta = float(point["epoch_seconds"]) - float(previous["epoch_seconds"])
            current_distance = point.get("distance_m")
            previous_distance = previous.get("distance_m")
            if time_delta > 0 and current_distance is not None and previous_distance is not None:
                point["speed_mps"] = max(
                    0.0, (float(current_distance) - float(previous_distance)) / time_delta
                )

    distance_values = [
        float(point["distance_m"])
        for point in points if point.get("distance_m") is not None
    ]
    total_distance = max(distance_values, default=0.0)
    elapsed = float(points[-1]["epoch_seconds"]) - first_epoch
    heart_values = [
        float(point["heart_rate_bpm"])
        for point in points if point.get("heart_rate_bpm") is not None
    ]
    altitude_values = [
        float(point["altitude_m"])
        for point in points if point.get("altitude_m") is not None
    ]
    hr_seconds_by_bpm: dict[str, float] = {}
    observed_hr_seconds = 0.0
    for index, point in enumerate(points[:-1]):
        heart_rate = point.get("heart_rate_bpm")
        interval = min(
            30.0,
            max(0.0, float(points[index + 1]["epoch_seconds"]) - float(point["epoch_seconds"])),
        )
        if heart_rate is not None and interval > 0:
            bucket = str(round(float(heart_rate)))
            hr_seconds_by_bpm[bucket] = hr_seconds_by_bpm.get(bucket, 0.0) + interval
            observed_hr_seconds += interval

    split_count = math.ceil(total_distance / 1000) if total_distance > 0 else 0
    kilometer_splits = []
    for split_number in range(1, split_count + 1):
        start_m = (split_number - 1) * 1000.0
        end_m = min(split_number * 1000.0, total_distance)
        split = _range_stats(points, start_m, end_m)
        split.update({
            "split": split_number,
            "partial": end_m - start_m < 999.5,
        })
        kilometer_splits.append(split)

    halves = None
    aerobic_decoupling = None
    if total_distance > 0:
        halfway = total_distance / 2
        first_half = _range_stats(points, 0.0, halfway)
        second_half = _range_stats(points, halfway, total_distance)
        halves = {"first": first_half, "second": second_half}
        first_hr = first_half.get("average_heart_rate_bpm")
        second_hr = second_half.get("average_heart_rate_bpm")
        first_speed = first_half.get("average_speed_mps")
        second_speed = second_half.get("average_speed_mps")
        if first_hr and second_hr and first_speed and second_speed:
            first_efficiency = first_speed / first_hr
            second_efficiency = second_speed / second_hr
            aerobic_decoupling = round(
                100 * (first_efficiency - second_efficiency) / first_efficiency,
                2,
            )

    sampled_points = []
    last_sample = -math.inf
    for index, point in enumerate(points):
        elapsed_seconds = float(point["elapsed_seconds"])
        if index not in {0, len(points) - 1} and elapsed_seconds - last_sample < 10:
            continue
        last_sample = elapsed_seconds
        sampled_points.append({
            key: point.get(key) for key in (
                "timestamp", "elapsed_seconds", "distance_m", "altitude_m",
                "heart_rate_bpm", "cadence_rpm", "speed_mps",
            )
        })

    average_hr = sum(heart_values) / len(heart_values) if heart_values else None
    average_speed = total_distance / elapsed if elapsed > 0 else None
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "garmin-tcx",
        "privacy": "GPS coordinates and route geometry removed",
        "available": True,
        "activity": {
            "id": str(activity.get("activityId")),
            "name": activity.get("activityName"),
            "type": activity_type(activity),
            "sport": activity_node.attrib.get("Sport"),
            "start_time": points[0]["timestamp"],
        },
        "summary": {
            "duration_seconds": round(elapsed, 3),
            "distance_m": round(total_distance, 3),
            "average_speed_mps": round(average_speed, 4) if average_speed else None,
            "average_pace_seconds_per_km": round(elapsed / (total_distance / 1000), 3)
            if total_distance > 0 else None,
            "average_heart_rate_bpm": round(average_hr, 2)
            if average_hr is not None else None,
            "maximum_heart_rate_bpm": round(max(heart_values), 2)
            if heart_values else None,
            "minimum_altitude_m": round(min(altitude_values), 2)
            if altitude_values else None,
            "maximum_altitude_m": round(max(altitude_values), 2)
            if altitude_values else None,
            "aerobic_decoupling_percent": aerobic_decoupling,
            "trackpoint_count": len(points),
            "sampled_trackpoint_count": len(sampled_points),
            "observed_heart_rate_seconds": round(observed_hr_seconds, 2),
        },
        "laps": laps,
        "kilometer_splits": kilometer_splits,
        "distance_halves": halves,
        "heart_rate_seconds_by_bpm": {
            key: round(value, 2)
            for key, value in sorted(hr_seconds_by_bpm.items(), key=lambda item: int(item[0]))
        },
        "sample_interval_seconds": 10,
        "sampled_trackpoints": sampled_points,
    }


def decode_fit(fit_data: bytes, *, activity: dict[str, Any], exercise_sets: Any = None):
    from garmin_fit_sdk import Decoder, Stream

    stream = Stream.from_byte_array(bytearray(fit_data))
    decoder = Decoder(stream)
    if not decoder.is_fit():
        raise ValueError("Downloaded payload is not a valid FIT stream")
    messages, errors = decoder.read()
    counts = {
        name: len(rows) if isinstance(rows, list) else 1
        for name, rows in messages.items()
    }
    fit_sets = []
    for key in ("sets", "set", "set_mesgs"):
        rows = messages.get(key)
        if isinstance(rows, list):
            fit_sets.extend(rows)
    activity_id = str(activity.get("activityId"))
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "garmin-fit",
        "activity": {
            "id": activity_id,
            "name": activity.get("activityName"),
            "type": activity_type(activity),
            "start_time_gmt": activity.get("startTimeGMT"),
            "start_time_local": activity.get("startTimeLocal"),
        },
        "source_fit_sha256": sha256(fit_data),
        "decode_errors": [str(error) for error in errors],
        "message_counts": counts,
        "normalized_strength_sets": fit_sets,
        "normalized_strength_session": normalize_strength_session(
            activity_id, exercise_sets
        ),
        "garmin_exercise_sets": exercise_sets,
        "messages": messages,
    }
