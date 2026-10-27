"""Build bounded, offline temporal activity/night associations from stored exports.

No provider, R2, upload, health inference, or backfill operation is performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import struct
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

METHOD = "night-training-v1"
MAX_BYTES = 16 * 1024 * 1024
MAX_PRODUCT_BYTES = 1024 * 1024
MAX_ACTIVITIES = 5000
CSV_TIME_CONTRACT = "slipstream-activities-csv-utc-v1"
UNITS = {"heart_rate": "bpm", "hrv": "ms", "stress": "points", "body_battery": "points"}
CONTEXT_FIELDS = (
    "alcohol",
    "late_bedtime",
    "travel",
    "illness",
    "sleep_quality",
    "morning_condition",
    "actual_night_timezone",
)


def canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def hash_material(value: Any) -> Any:
    """Typed semantic numeric identity uses finite IEEE754 binary64 bits, with signed zero normalized."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(value):
            raise ValueError("Invalid numeric value")
        return "f64:" + struct.pack(">d", 0.0 if value == 0 else float(value)).hex()
    if isinstance(value, list):
        return [hash_material(item) for item in value]
    if isinstance(value, dict):
        return {key: hash_material(item) for key, item in value.items()}
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(hash_material(value)).encode("utf-8")).hexdigest()


def calendar(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value or parsed.year < 1970:
        raise ValueError("Invalid calendar date")
    return parsed


def arguments(start: str, end: str) -> dict:
    first, last = calendar(start), calendar(end)
    if not 1 <= (last - first).days + 1 <= 366:
        raise ValueError("Training product range must contain1 through366 dates")
    return {"start_date": start, "end_date": end}


def utc(value: Any, *, csv_contract: bool = False) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        if csv_contract and re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value):
            result = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        elif re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value
        ):
            result = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        else:
            return None
        return result if 1970 <= result.year <= 2100 else None
    except (ValueError, OverflowError):
        return None


def iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value is not None else None


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) and result >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    return (
        ordered[middle]
        if len(ordered) % 2
        else ordered[middle - 1] + (ordered[middle] - ordered[middle - 1]) / 2
    )


def read_exports(paths: list[str]) -> tuple[list[dict], int, int, list[dict]]:
    if not 1 <= len(paths) <= 64:
        raise ValueError("Provide1 through64 native export files")
    nights = []
    total_bytes = 0
    points = 0
    files = []
    for name in paths:
        with Path(name).open("rb") as handle:
            raw = handle.read(MAX_BYTES - total_bytes + 1)
        total_bytes += len(raw)
        if total_bytes > MAX_BYTES:
            raise ValueError("Native export inputs exceed16MiB")
        files.append({"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("Invalid JSON number")),
        )
        if isinstance(payload, dict) and isinstance(payload.get("structuredContent"), dict):
            payload = payload["structuredContent"]
        batch = (
            payload.get("nights")
            if isinstance(payload, dict) and "nights" in payload
            else [payload]
        )
        if not isinstance(batch, list) or len(batch) > 366:
            raise ValueError("Unsupported export shape")
        for night in batch:
            if not isinstance(night, dict) or not isinstance(night.get("channels") or {}, dict):
                raise ValueError("Unsupported native night")
            for channel in (night.get("channels") or {}).values():
                if isinstance(channel, dict) and isinstance(channel.get("points"), list):
                    points += len(channel["points"])
            if points > 200000:
                raise ValueError("Input exports exceed200000 points")
            nights.append(night)
    return nights, total_bytes, points, files


def read_activities(path: str | None, state: str, remaining: int) -> tuple[list[dict], dict, int]:
    if path is None:
        return (
            [],
            {
                "status": state,
                "sha256": None,
                "bytes": 0,
                "rows": 0,
                "duplicates": 0,
                "time_contract": CSV_TIME_CONTRACT,
                "coverage": "provided_export_only_provider_completeness_unknown",
            },
            0,
        )
    if state != "not_stored":
        raise ValueError("An activities file cannot also declare missing/error")
    with Path(path).open("rb") as handle:
        raw = handle.read(remaining + 1)
    if len(raw) > remaining:
        raise ValueError("Combined inputs exceed16MiB")
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    required = {
        "Activity ID",
        "Activity Date",
        "Activity Type",
        "Elapsed Time",
        "Moving Time",
        "Source",
    }
    if reader.fieldnames is None or not required <= set(reader.fieldnames):
        raise ValueError("Not a supported stored activity CSV")
    records = []
    for row in reader:
        records.append(row)
        if len(records) > MAX_ACTIVITIES:
            raise ValueError("Too many activity rows")
    if len(records) > MAX_ACTIVITIES:
        raise ValueError("Too many activity rows")
    variants: dict[str, dict[str, dict]] = {}
    invalid = []
    duplicates = 0
    for row in records:
        identity = row.get("Activity ID", "")
        source = row.get("Source", "")
        if (
            not isinstance(identity, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", identity)
            or not re.fullmatch(r"[a-z0-9_-]{1,32}", source)
        ):
            invalid.append({"start": None, "end": None, "reason": "invalid_identity"})
            continue
        start = utc(row.get("Activity Date"), csv_contract=True)
        duration = number(row.get("Elapsed Time"))
        # writer.py serializes recorded Elapsed Time as integer seconds. Moving Time cannot establish end.
        if duration is not None and (not duration.is_integer() or duration > 7 * 86400):
            duration = None
        end = (
            start + timedelta(seconds=duration)
            if start is not None and duration is not None
            else None
        )
        activity = {
            "activity_id": identity,
            "activity_type": (row.get("Activity Type") or "other")[:64],
            "source": source,
            "start_utc": iso(start),
            "end_utc": iso(end),
            "duration_seconds": duration,
            "average_hr_bpm": number(row.get("Average Heart Rate")),
            "time_basis": CSV_TIME_CONTRACT
            if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", row.get("Activity Date") or "")
            else "explicit_offset",
            "intensity": None,
            "training_load": None,
        }
        variants.setdefault(identity, {})[digest(activity)] = {
            "start": start,
            "end": end,
            "activity": activity,
        }
    selected = []
    for group in variants.values():
        if len(group) > 1:
            invalid.append({"start": None, "end": None, "reason": "duplicate_conflict"})
        else:
            selected.append(next(iter(group.values())))
    duplicates = len(records) - len(selected) - len(invalid)
    metadata = {
        "status": "available" if records else "no_data",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "rows": len(records),
        "duplicates": max(0, duplicates),
        "time_contract": CSV_TIME_CONTRACT,
        "coverage": "provided_export_only_provider_completeness_unknown",
    }
    return selected + invalid, metadata, len(raw)


def actual_zone(value: Any) -> str | None:
    if not isinstance(value, str) or not 1 <= len(value) <= 100:
        return None
    try:
        ZoneInfo(value)
        return value
    except (ZoneInfoNotFoundError, ValueError):
        return None


SOURCE_COUNTS = (
    "invalid",
    "ambiguous_time",
    "outside_interval",
    "duplicates",
    "conflicting_timestamps",
    "gaps",
)
SOURCE_FLAGS = {
    "invalid_observations",
    "ambiguous_time_excluded",
    "conflicting_instants_excluded",
    "observed_gaps",
    "coverage_unknown",
    "minimum_window_not_covered",
    "source_quality_metadata_invalid",
    "source_retained_revision_unknown",
} | {
    "source_" + value
    for value in (
        "available",
        "partial",
        "no_data",
        "not_stored",
        "read_error",
        "invalid_schema",
        "invalid_response",
        "ambiguous_time",
    )
}
OBSERVATION_FLAGS = {
    "missing_values",
    "partial_source",
    "retained_previous_observations",
    "source_completeness_unknown",
    "retained_revision_unknown",
    "invalid_source_shape",
    "missing_or_conflicting_source_date",
    "invalid_or_oversize_curve",
    "invalid_point",
    "invalid_timestamp",
    "invalid_timestamp_unit_or_range",
    "invalid_value",
    "conflicting_source_observations",
    "duplicate_source_observations",
    "unverified_source_observations",
}


def export_quality(raw: dict) -> dict:
    """Keep bounded upstream quality distinct from exclusions computed here."""
    invalid = False
    present = any(
        key in raw
        for key in (
            "quality",
            "quality_flags",
            "source_quality_flags",
            "source_missing_value_count",
        )
    )
    counts = None
    source = raw.get("quality")
    if source is not None:
        if not isinstance(source, dict):
            invalid = True
        else:
            counts = {}
            invalid = any(key not in SOURCE_COUNTS for key in source)
            for key in SOURCE_COUNTS:
                value = source.get(key)
                valid = (
                    isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 200000
                )
                counts[key] = value if valid else None
                if key in source and not valid:
                    invalid = True

    def flags(value, allowed, limit):
        nonlocal invalid
        if value is None:
            return []
        if not isinstance(value, list) or len(value) > limit:
            invalid = True
            return []
        result = []
        for flag in value:
            if not isinstance(flag, str) or flag not in allowed:
                invalid = True
            elif flag not in result:
                result.append(flag)
        return result

    quality_flags = flags(raw.get("quality_flags"), SOURCE_FLAGS, 32)
    observation_flags = flags(raw.get("source_quality_flags"), OBSERVATION_FLAGS, 16)
    missing = raw.get("source_missing_value_count")
    valid_missing = (
        isinstance(missing, int) and not isinstance(missing, bool) and 0 <= missing <= 100000
    )
    if "source_missing_value_count" in raw and not valid_missing:
        invalid = True
    return {
        "metadata_status": "invalid" if invalid else "provided" if present else "not_provided",
        "channel_status": raw["status"],
        "counts": counts,
        "quality_flags": quality_flags,
        "observation_flags": observation_flags,
        "missing_value_count": missing if valid_missing else None,
    }


def uncertain_export_quality(quality: dict) -> bool:
    benign = {"coverage_unknown", "minimum_window_not_covered", "source_no_data"}
    return (
        quality["metadata_status"] == "invalid"
        or any(value for value in (quality["counts"] or {}).values())
        or bool(quality["observation_flags"])
        or bool(quality["missing_value_count"])
        or any(flag not in benign for flag in quality["quality_flags"])
    )


def channel_summary(raw: Any, name: str, start: datetime, end: datetime) -> dict:
    result = {
        "status": "not_stored",
        "unit": UNITS[name],
        "valid_points": 0,
        "invalid_points": 0,
        "median": None,
        "observed_cadence_seconds": None,
        "coverage": None,
        "coverage_basis": "occupied-observed-cadence-slots-v1",
        "largest_gap_seconds": None,
        "quality_flags": [],
        "source_revisions": [],
    }
    if not isinstance(raw, dict):
        return result
    status = raw.get("status")
    if (
        status
        not in {
            "available",
            "partial",
            "no_data",
            "not_stored",
            "read_error",
            "invalid_schema",
            "invalid_response",
            "ambiguous_time",
        }
        or raw.get("unit") != UNITS[name]
    ):
        result["status"] = "invalid_schema"
        return result
    result["status"] = status
    result["source_quality"] = export_quality(raw)
    source_uncertain = uncertain_export_quality(result["source_quality"])
    revisions = raw.get("source_revisions", [])
    if not isinstance(revisions, list) or len(revisions) > 16:
        raise ValueError("Invalid source revisions")
    for revision in revisions:
        if (
            not isinstance(revision, dict)
            or not isinstance(revision.get("revision"), str)
            or not 1 <= len(revision["revision"]) <= 256
        ):
            raise ValueError("Invalid source revision")
        calendar(revision.get("date"))
        result["source_revisions"].append(
            {"date": revision["date"], "revision": revision["revision"]}
        )
    if status not in {"available", "partial"}:
        if status == "no_data" and source_uncertain:
            result["status"] = (
                "ambiguous_time"
                if (result["source_quality"]["counts"] or {}).get("ambiguous_time")
                else "partial"
            )
        return result
    points = raw.get("points")
    if not isinstance(points, list):
        result["status"] = "invalid_schema"
        return result
    grouped: dict[datetime, set[float]] = {}
    for point in points:
        if not isinstance(point, dict):
            result["invalid_points"] += 1
            continue
        instant = utc(point.get("timestamp_utc"))
        value = number(point.get("value"))
        if (
            point.get("time_basis") != "utc"
            or instant is None
            or value is None
            or (name in {"heart_rate", "hrv"} and value <= 0)
            or (name in {"stress", "body_battery"} and value > 100)
            or not start <= instant < end
        ):
            result["invalid_points"] += 1
            continue
        grouped.setdefault(instant, set()).add(value)
    conflicts = sum(len(values) > 1 for values in grouped.values())
    valid = [
        (instant, next(iter(values)))
        for instant, values in sorted(grouped.items())
        if len(values) == 1
    ]
    result["valid_points"] = len(valid)
    result["median"] = median([value for _instant, value in valid])
    if conflicts:
        result["quality_flags"].append("conflicting_timestamps_excluded")
    if result["invalid_points"]:
        result["quality_flags"].append("invalid_or_ambiguous_or_outside_points_excluded")
    if not valid:
        result["status"] = (
            "ambiguous_time"
            if (result["source_quality"]["counts"] or {}).get("ambiguous_time")
            else "partial"
        )
    elif source_uncertain:
        result["status"] = "partial"
    differences = [
        (right[0] - left[0]).total_seconds() for left, right in zip(valid, valid[1:], strict=False)
    ]
    cadence = median(differences)
    if cadence is not None and cadence > 0:
        expected = math.ceil((end - start).total_seconds() / cadence)
        occupied = len(
            {int((instant - start).total_seconds() // cadence) for instant, _value in valid}
        )
        result["observed_cadence_seconds"] = cadence
        result["coverage"] = min(1, occupied / expected)
        result["largest_gap_seconds"] = max(differences)
    return result


def context_snapshot(night: dict, day: str) -> dict:
    view = night.get("view") if isinstance(night.get("view"), dict) else {}
    status = view.get("context_status")
    raw = night.get("user_context")
    if status == "unavailable":
        return {
            "status": "unavailable",
            "basis": "export_snapshot",
            "revision": None,
            "values": None,
        }
    if raw is None:
        return {
            "status": "not_stored" if status == "not_stored" else "not_provided",
            "basis": "export_snapshot",
            "revision": None,
            "values": None,
        }
    if (
        not isinstance(raw, dict)
        or raw.get("source") != "user"
        or raw.get("wake_date") != day
        or not isinstance(raw.get("context_id"), str)
        or not 1 <= len(raw["context_id"]) <= 128
    ):
        return {
            "status": "unavailable",
            "basis": "export_snapshot",
            "revision": None,
            "values": None,
        }
    values: dict = {"source": "user"}
    for field in CONTEXT_FIELDS:
        value = raw.get(field)
        valid = value is None
        if field in {"alcohol", "late_bedtime", "travel", "illness"}:
            valid |= isinstance(value, bool)
        elif field in {"sleep_quality", "morning_condition"}:
            valid |= isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 5
        elif isinstance(value, str) and len(value) <= 100:
            try:
                ZoneInfo(value)
                valid = True
            except (ZoneInfoNotFoundError, ValueError):
                pass
        if not valid:
            return {
                "status": "unavailable",
                "basis": "export_snapshot",
                "revision": None,
                "values": None,
            }
        values[field] = value
    declared = raw.get("declared_fields", [])
    values["declared_fields"] = (
        sorted({field for field in declared if field in CONTEXT_FIELDS})
        if isinstance(declared, list)
        else []
    )
    return {
        "status": "available",
        "basis": "export_snapshot",
        "revision": raw["context_id"],
        "values": values,
    }


def build(
    nights: list[dict],
    activities: list[dict],
    activity_source: dict,
    args: dict,
    input_bytes: int,
    input_points: int,
    export_files: list[dict],
) -> dict:
    first, last = calendar(args["start_date"]), calendar(args["end_date"])
    requested = [
        (first + timedelta(days=offset)).isoformat() for offset in range((last - first).days + 1)
    ]
    variants: dict[str, dict[str, dict]] = {}
    for night in nights:
        day = night.get("wake_date")
        try:
            calendar(day)
        except (ValueError, TypeError):
            continue
        snapshot = {key: value for key, value in night.items() if key not in {"resources", "view"}}
        variants.setdefault(day, {})[digest(snapshot)] = night
    if len(variants) > 366:
        raise ValueError("Export association context exceeds366 nights")
    rows = []
    manifest = []
    usable = []
    for day in sorted(set(requested) | set(variants)):
        group = variants.get(day, {})
        raw = next(iter(group.values())) if len(group) == 1 else None
        context = (
            context_snapshot(raw, day)
            if raw is not None
            else {
                "status": "not_provided",
                "basis": "export_snapshot",
                "revision": None,
                "values": None,
            }
        )
        start = utc(raw.get("sleep_start_utc")) if raw else None
        end = utc(raw.get("sleep_end_utc")) if raw else None
        status = "available"
        if not group:
            status = "not_in_export"
        elif len(group) > 1:
            status = "duplicate_conflict"
        elif raw.get("method_version") != "night-signals-v1" or raw.get("resolution") != "native":
            status = "unsupported_source_method"
        elif start is None or end is None or not 0 < (end - start).total_seconds() <= 36 * 3600:
            status = "invalid_sleep_interval"
        elif raw.get("status") not in {"available", "partial"}:
            status = "source_unavailable"
        zone = actual_zone(raw.get("actual_night_timezone")) if raw else None
        flags = [] if zone is not None else ["actual_timezone_unknown"]
        if raw and raw.get("actual_night_timezone") is not None and zone is None:
            flags.append("invalid_actual_timezone")
        if raw and raw.get("status") == "partial":
            flags.append("source_partial")
        if context["status"] == "unavailable":
            flags.append("context_unavailable")
        row = {
            "wake_date": day,
            "status": status,
            "sleep_start_utc": iso(start),
            "sleep_end_utc": iso(end),
            "actual_night_timezone": zone,
            "quality_flags": flags,
            "context": context,
            "channels": {},
            "training": {
                "status": activity_source["status"],
                "confirmed_activity_count": None,
                "uncertain_activity_count": None,
                "activities": [],
                "recorded_duration_seconds": None,
                "known_recorded_duration_seconds": None,
                "intensity": None,
                "training_load": None,
                "coverage": "provided_export_only_provider_completeness_unknown",
            },
        }
        if status == "available":
            channels = raw.get("channels") or {}
            row["channels"] = {
                name: channel_summary(channels.get(name), name, start, end) for name in UNITS
            }
            usable.append((start, end, row))
        elif status in {"duplicate_conflict", "source_unavailable", "unsupported_source_method"}:
            # A documented window still blocks reassignment when its measurements are unusable.
            for variant in group.values():
                vstart, vend = (
                    utc(variant.get("sleep_start_utc")),
                    utc(variant.get("sleep_end_utc")),
                )
                if (
                    variant.get("method_version") == "night-signals-v1"
                    and variant.get("resolution") in {"native", "15m", "30m"}
                    and vstart is not None
                    and vend is not None
                    and 0 < (vend - vstart).total_seconds() <= 36 * 3600
                ):
                    usable.append((vstart, vend, row))
        rows.append(row)
        manifest.append(
            {
                "wake_date": day,
                "snapshot_sha256": digest(sorted(group))
                if len(group) > 1
                else next(iter(group), None),
                "context_revision": context["revision"],
            }
        )
    usable.sort(key=lambda entry: entry[0])
    all_windows = list(usable)
    if len(all_windows) > 366:
        raise ValueError("Export association context exceeds366 windows")
    ambiguous = set()
    for index, (start, end, row) in enumerate(all_windows):
        for other_start, other_end, other in all_windows[index + 1 :]:
            if other_start >= end:
                break
            if start < other_end:
                ambiguous.update((id(row), id(other)))
    for _start, _end, row in all_windows:
        if id(row) in ambiguous and row["status"] == "available":
            row["status"] = "ambiguous_sleep_window"
            row["quality_flags"].append("ambiguous_sleep_windows")
            row["channels"] = {}
    usable = [entry for entry in all_windows if entry[2]["status"] == "available"]
    counts = {
        key: 0
        for key in (
            "unknown_start",
            "unknown_end",
            "overlaps_recorded_sleep",
            "ambiguous_night_window",
            "outside_window",
            "duplicate_conflict",
            "invalid_identity",
        )
    }
    uncertainty: list[dict] = []
    for record in activities:
        start, end = record["start"], record["end"]
        if start is None:
            counts[record.get("reason", "unknown_start")] += 1
            uncertainty.append(record)
            continue
        if end is None:
            counts["unknown_end"] += 1
            uncertainty.append(record)
            continue
        if any(
            start < sleep_end and end > sleep_start for sleep_start, sleep_end, _row in all_windows
        ):
            counts["overlaps_recorded_sleep"] += 1
            uncertainty.append(record)
            continue
        candidates = [
            (sleep_start, row)
            for sleep_start, _sleep_end, row in all_windows
            if end <= sleep_start and sleep_start - end <= timedelta(hours=48)
        ]
        if not candidates:
            counts["outside_window"] += 1
            continue
        _sleep_start, row = candidates[0]
        if row["status"] != "available":
            counts["ambiguous_night_window"] += 1
            uncertainty.append(record)
            continue
        if row["wake_date"] not in requested:
            counts["outside_window"] += 1
            continue
        row["training"]["activities"].append(record["activity"])
        if len(row["training"]["activities"]) > 100:
            raise ValueError("More than100 confirmed activities for one night")
    for start, end, row in usable:
        if activity_source["status"] in {"not_stored", "read_error"}:
            continue
        linked = row["training"]["activities"]
        linked.sort(key=lambda item: (item["end_utc"], item["activity_id"]))
        uncertain = sum(
            record["start"] is None
            or record["start"] <= start
            and start - record["start"] <= timedelta(hours=48)
            or record["start"] < end
            and (record["end"] is None or record["end"] > start)
            for record in uncertainty
        )
        known = sum(item["duration_seconds"] for item in linked)
        row["training"].update(
            {
                "status": "partial" if uncertain else activity_source["status"],
                "confirmed_activity_count": len(linked),
                "uncertain_activity_count": uncertain,
                "known_recorded_duration_seconds": known,
                "recorded_duration_seconds": known if not uncertain else None,
            }
        )
    snapshots = {item["wake_date"]: item["snapshot_sha256"] for item in manifest}
    association_windows = [
        {
            "wake_date": row["wake_date"],
            "sleep_start_utc": iso(start),
            "sleep_end_utc": iso(end),
            "status": row["status"],
            "snapshot_sha256": snapshots[row["wake_date"]],
        }
        for start, end, row in all_windows
    ]
    rows = [row for row in rows if row["wake_date"] in requested]
    manifest = [item for item in manifest if item["wake_date"] in requested]
    source_manifest = {
        "activity_source": activity_source,
        "export_files": export_files,
        "nights": manifest,
        "association_windows": association_windows,
    }
    product = {
        "schema_version": 1,
        "kind": "night-training",
        "method_version": METHOD,
        "args": args,
        "args_sha256": digest(args),
        "source_manifest": source_manifest,
        "source_identity": digest(source_manifest),
        "association": "temporal_only_not_causal",
        "link_method": "earliest-documented-sleep-after-recorded-end-within48h-overlap-excluded-v1",
        "activity_time_contract": CSV_TIME_CONTRACT,
        "activity_dataset_status": activity_source["status"],
        "nights_requested": len(rows),
        "nights": rows,
        "unlinked_counts": counts,
        "resources": {
            "input_bytes": input_bytes,
            "input_points": input_points,
            "activity_rows": activity_source["rows"],
        },
    }
    product["product_identity"] = digest(product)
    if len(canonical(product).encode("utf-8")) > MAX_PRODUCT_BYTES:
        raise ValueError("Training product exceeds1MiB")
    end = args["end_date"]
    key = f"health/night-products/v1/training/{end[:4]}/{end[5:7]}/{end[8:10]}/{digest(args)}.json"
    return {"storage_key": key, "product": product}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    command = parser.add_subparsers(dest="command", required=True).add_parser("build")
    command.add_argument("--input", action="append", required=True)
    command.add_argument("--activities")
    command.add_argument(
        "--activity-state", choices=["not_stored", "read_error"], default="not_stored"
    )
    command.add_argument("--start-date", required=True)
    command.add_argument("--end-date", required=True)
    options = parser.parse_args(argv)
    try:
        args = arguments(options.start_date, options.end_date)
        nights, input_bytes, input_points, export_files = read_exports(options.input)
        activities, activity_source, csv_bytes = read_activities(
            options.activities, options.activity_state, MAX_BYTES - input_bytes
        )
        output = build(
            nights,
            activities,
            activity_source,
            args,
            input_bytes + csv_bytes,
            input_points,
            export_files,
        )
    except (ValueError, TypeError, OSError, OverflowError, csv.Error):
        parser.exit(2, "Cannot build training product: invalid or oversized input/arguments.\n")
    print(canonical(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
