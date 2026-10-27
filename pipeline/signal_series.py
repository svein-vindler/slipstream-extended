"""Retain existing daily Garmin signal responses within normal storage budgets."""
from __future__ import annotations

import gzip
import io
import json
import math
from datetime import date
from typing import Any

from botocore.exceptions import ClientError

from .granular import gzip_json, json_bytes, sha256
from .r2_store import R2ReadBoundaryError, missing_object

MAX_STORED_BYTES = 256 * 1024
MAX_DECODED_BYTES = 1024 * 1024
MAX_POINTS = 10000
CHANNELS = {
    "heart_rate": ("heartRateValues", "bpm", "get_heart_rates"),
    "body_battery": ("bodyBatteryValuesArray", "points", "get_body_battery"),
    "stress": ("stressValuesArray", "points", "get_stress_data"),
}


def signal_key(day: str) -> str:
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("Invalid signal calendar date")
    return f"health/signals/v1/{day[:4]}/{day[5:7]}/{day}.json"


def _channel(day: str, name: str, raw: Any) -> dict[str, Any]:
    field, unit, method = CHANNELS[name]
    result = {"status": "invalid_response", "unit": unit, "readings": [],
              "quality_flags": [], "source_method": method, "source_field": field}
    if name in {"body_battery", "stress"}:
        result["measurement_kind"] = "garmin_derived"
    if not isinstance(raw, dict):
        result["quality_flags"] = ["invalid_source_shape"]
        return result
    declarations = [raw[key] for key in ("date", "calendarDate", "summaryDate")
                    if raw.get(key) is not None]
    if not declarations or any(value != day for value in declarations):
        result["quality_flags"] = ["missing_or_conflicting_source_date"]
        return result
    values = raw.get(field)
    if not isinstance(values, list) or len(values) > MAX_POINTS:
        result["quality_flags"] = ["invalid_or_oversize_curve"]
        return result
    readings = []
    missing_values = 0
    for item in values:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            result["quality_flags"] = ["invalid_point"]
            return result
        timestamp, value = item
        if (isinstance(timestamp, bool) or not isinstance(timestamp, (int, float))
                or not math.isfinite(timestamp)):
            result["quality_flags"] = ["invalid_timestamp"]
            return result
        if 946684800 <= timestamp <= 4102444800:
            timestamp_unit = "epoch_seconds"
        elif 946684800000 <= timestamp <= 4102444800000:
            timestamp_unit = "epoch_milliseconds"
        else:
            result["quality_flags"] = ["invalid_timestamp_unit_or_range"]
            return result
        if (value is None or (name == "heart_rate" and type(value) in (int, float) and value == 0)
                or name == "stress" and type(value) in (int, float) and value in {-1, -2}):
            missing_values += 1
            continue
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0
                or name == "heart_rate" and value == 0
                or name in {"body_battery", "stress"} and value > 100):
            result["quality_flags"] = ["invalid_value"]
            return result
        readings.append({"timestamp": timestamp, "value": value, "time_basis": "utc",
                         "timestamp_field": f"{field}[][0]", "timestamp_unit": timestamp_unit})
    result.update(status="available" if readings else "no_data" if not values else "invalid_response",
                  readings=readings, missing_value_count=missing_values,
                  quality_flags=["missing_values"] if missing_values else [],
                  source_sha256=sha256(json_bytes({"date": day, field: values})))
    source = raw.get("_slipstream_source")
    if isinstance(source, dict) and source.get("provider") == "personal":
        result["source_provider"] = "personal"
        result["source_sampling"] = source.get("sampling", "resolution_unverified")
        result["quality_flags"] = list(dict.fromkeys([*result["quality_flags"], "source_completeness_unknown"]))
    return result


def _source_revision(value: Any) -> str | None:
    return value if (isinstance(value, str) and len(value) == 64
                     and all(char in "0123456789abcdef" for char in value)) else None


def _point_instant(point: dict[str, Any]) -> float | None:
    value, unit = point.get("timestamp"), point.get("timestamp_unit")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    if point.get("time_basis") != "utc":
        return None
    if unit == "epoch_seconds":
        return value * 1000
    return value if unit == "epoch_milliseconds" else None


def _unambiguous_points(readings: list[Any], *, derived: bool) -> tuple[list[dict[str, Any]], int, int, int]:
    groups: dict[float, list[dict[str, Any]]] = {}
    invalid = 0
    for point in readings:
        if not isinstance(point, dict):
            invalid += 1
            continue
        instant, value = _point_instant(point), point.get("value")
        if (instant is None or isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or not (0 <= value <= 100 if derived else value > 0)):
            invalid += 1
            continue
        groups.setdefault(instant, []).append(point)
    points, duplicates, conflicts = [], 0, 0
    for group in groups.values():
        if len({point["value"] for point in group}) > 1:
            conflicts += 1
            continue
        duplicates += len(group) - 1
        points.append(dict(group[0]))
    return points, duplicates, conflicts, invalid

def _preserve_observations(incoming: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    """A new valid instant can replace itself; absence cannot delete an observation.

    Counts describe source observations, before reader deduplication/clipping.
    A response array does not by itself prove that a source day is complete.
    """
    if incoming["status"] != "available":
        return incoming
    composition = previous.get("source_composition")
    if (_source_revision(incoming.get("source_sha256")) is not None
            and incoming.get("source_sha256") == previous.get("source_sha256")
            and isinstance(composition, dict)
            and composition.get("method_version") == "preserve-good-observations-v1"):
        return previous  # The same source revision cannot create a new composition/write.
    current, duplicates, conflicts, invalid = _unambiguous_points(incoming["readings"], derived=incoming["unit"] == "points")
    previous_points = previous.get("readings", []) if previous.get("status") == "available" else []
    prior, _, prior_conflicts, prior_invalid = _unambiguous_points(previous_points, derived=incoming["unit"] == "points")
    current_instants = {_point_instant(point) for point in current}
    prior_instants = {_point_instant(point) for point in prior}
    retained = [dict(point) for point in prior if _point_instant(point) not in current_instants]
    revision = _source_revision(incoming.get("source_sha256"))
    for point in current:
        point.update(source_revision=revision, observation_origin="incoming")
    for point in retained:
        if "source_revision" in point:
            # Explicit unknown/invalid provenance cannot acquire a later response's hash.
            point_revision = _source_revision(point["source_revision"])
        elif "source_composition" not in previous and "observation_origin" not in point:
            # Only an uncomposed legacy curve documents all its points by channel hash.
            point_revision = _source_revision(previous.get("source_sha256"))
        else:
            point_revision = None
        point.update(source_revision=point_revision, observation_origin="retained")
    observations = current + retained
    if len(observations) > MAX_POINTS:
        raise R2ReadBoundaryError("point_limit")
    observations.sort(key=lambda point: (_point_instant(point) is None, _point_instant(point) or 0))
    revisions = sorted({revision for point in observations
                        if (revision := _source_revision(point.get("source_revision"))) is not None})
    if revision is not None and revision not in revisions:
        revisions = sorted([*revisions, revision])
    flags = list(incoming.get("quality_flags", []))
    if incoming.get("missing_value_count", 0) or retained or conflicts or invalid or prior_conflicts or prior_invalid:
        flags.extend(["partial_source", "source_completeness_unknown"])
    if conflicts:
        flags.append("conflicting_source_observations")
    if duplicates:
        flags.append("duplicate_source_observations")
    if invalid or prior_conflicts or prior_invalid:
        flags.append("unverified_source_observations")
    if retained:
        flags.append("retained_previous_observations")
        if any(point["source_revision"] is None for point in retained):
            flags.append("retained_revision_unknown")
    return {**incoming, "status": "available" if observations else "invalid_response",
            "readings": observations, "quality_flags": list(dict.fromkeys(flags)),
            "source_composition": {
                "method_version": "preserve-good-observations-v1", "completeness": "unknown",
                "incoming_valid_points": len(current), "retained_valid_points": len(retained),
                "conflicting_source_timestamps": conflicts, "duplicate_source_points": duplicates,
                "new_valid_points": sum(_point_instant(point) not in prior_instants for point in current),
                "replaced_valid_points": sum(_point_instant(point) in prior_instants for point in current),
                "source_revisions": revisions[:16], "source_revision_count": len(revisions),
                "source_revisions_truncated": len(revisions) > 16,
            }}

class DailySignalCollector:
    """Buffer existing replies, then publish one combined additive object per day."""

    def __init__(self, store):
        self.store = store
        self.days: dict[str, dict[str, Any]] = {}

    def collect(self, day: str, name: str, raw: Any) -> None:
        signal_key(day)
        self.days.setdefault(day, {})[name] = _channel(day, name, raw)

    def _previous(self, day: str) -> tuple[dict[str, Any], bytes | None]:
        try:
            snapshot = self.store.get_snapshot(signal_key(day), max_bytes=MAX_STORED_BYTES)
        except KeyError:
            return {}, None
        except ClientError as exc:
            if missing_object(exc):
                return {}, None
            raise
        data = snapshot.data
        if data.startswith(b"\x1f\x8b"):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as handle:
                    decoded = handle.read(MAX_DECODED_BYTES + 1)
            except (OSError, EOFError) as exc:
                raise R2ReadBoundaryError("schema_unverified") from exc
        else:
            decoded = data
        if len(decoded) > MAX_DECODED_BYTES:
            raise R2ReadBoundaryError("oversize")
        try:
            payload = json.loads(decoded)
        except (ValueError, UnicodeDecodeError) as exc:
            raise R2ReadBoundaryError("schema_unverified") from exc
        if (not isinstance(payload, dict) or payload.get("schema_version") != 1
                or payload.get("source") != "garmin" or payload.get("date") != day
                or not isinstance(payload.get("channels"), dict)):
            raise R2ReadBoundaryError("schema_unverified")
        channels = payload["channels"]
        if any(name not in CHANNELS or not isinstance(value, dict)
               or value.get("status") not in {"available", "no_data", "invalid_response"}
               or not isinstance(value.get("readings"), list)
               for name, value in channels.items()):
            raise R2ReadBoundaryError("schema_unverified")
        return channels, data

    def publish(self) -> dict[str, int]:
        counts = {"objects_written": 0, "objects_unchanged": 0, "stored_bytes": 0,
                  "get_attempts": 0, "put_attempts": 0, "preserved_channels": 0,
                  "read_bytes": 0, "write_bytes": 0, "valid_points": 0}
        for day, incoming in sorted(self.days.items()):
            counts["get_attempts"] += 1
            channels, previous_bytes = self._previous(day)
            counts["read_bytes"] += len(previous_bytes or b"")
            for name, value in incoming.items():
                previous = channels.get(name, {})
                if previous.get("status") == "available" and value["status"] != "available":
                    counts["preserved_channels"] += 1
                    continue
                channels[name] = _preserve_observations(value, previous)
            decoded = json_bytes({"schema_version": 1, "source": "garmin", "date": day,
                                  "channels": channels})
            data = gzip_json(json.loads(decoded))
            if len(data) > MAX_STORED_BYTES or len(decoded) > MAX_DECODED_BYTES:
                raise R2ReadBoundaryError("oversize")
            if data == previous_bytes:
                changed = False
            else:
                counts["put_attempts"] += 1
                self.store.put(signal_key(day), data, "application/json", encoding="gzip")
                changed = True
                counts["write_bytes"] += len(data)
            counts["objects_written" if changed else "objects_unchanged"] += 1
            counts["stored_bytes"] += len(data)
            counts["valid_points"] += sum(len(channel["readings"]) for channel in channels.values())
        return counts
