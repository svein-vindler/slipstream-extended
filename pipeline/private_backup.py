"""Optional local encrypted backup CLI. Never contacts Garmin or writes remote storage."""

from __future__ import annotations

import argparse
import base64
import binascii
import getpass
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import warnings
import zlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.argon2 import Argon2id

from .backup_source import (
    DEFAULT_LIMITS,
    BackupError,
    Limits,
    LocalSource,
    ObjectVersion,
    ReadOnlySource,
    Scope,
    SourceChanged,
    allowed_key,
    key_info,
    safe_file,
    safe_path,
)
from .coach import validate_profile
from .coach_stored_context import HRV_METRICS, SLEEP_METRICS

MAGIC = b"SLIPSTREAM-BACKUP-1\n"
FORMAT = "slipstream-user-backup"
VERSION = 1
MAX_RETRIES = 1
# Frozen statuses of the version-1.3 archived projection. The offline reader
# must not import provider/parser modules to interpret this data-only enum.
_STORED_TRAINING_ATTEMPT_STATUSES = frozenset({
    "available", "partial", "no_observation", "auth_required", "source_error",
    "timeout", "oversize", "schema_unverified", "wrong_date",
})


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BackupError("duplicate_json_key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise BackupError("invalid_json_number")


def _loads(data: bytes, maximum: int, limits: Limits, node_budget: list[int] | None = None) -> Any:
    if len(data) > maximum:
        raise BackupError("json_size_limit")
    # Bound nesting before the parser allocates containers (ignore quoted braces).
    depth = nodes_before_parse = 0
    quoted = escaped = False
    for char in data:
        if quoted:
            if escaped:
                escaped = False
            elif char == 92:
                escaped = True
            elif char == 34:
                quoted = False
        elif char == 34:
            quoted = True
            nodes_before_parse += 1
        elif char in (91, 123):
            depth += 1
            nodes_before_parse += 1
            if depth > 32:
                raise BackupError("json_depth_limit")
        elif char in (93, 125):
            depth -= 1
        elif char in (44, 58):
            nodes_before_parse += 1
        if nodes_before_parse > (node_budget[0] if node_budget is not None else limits.json_nodes):
            raise BackupError("json_node_limit")
    try:
        value = json.loads(data, object_pairs_hook=_pairs, parse_constant=_reject_constant)
    except (UnicodeError, ValueError, RecursionError):
        raise BackupError("invalid_json") from None
    pending = [value]
    nodes = 0
    while pending:
        item = pending.pop()
        nodes += 1
        if nodes > limits.json_nodes:
            raise BackupError("json_node_limit")
        if isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise BackupError("invalid_json_number")
    if node_budget is not None:
        node_budget[0] -= max(nodes, nodes_before_parse)
        if node_budget[0] < 0:
            raise BackupError("json_node_limit")
    return value


def _decode(data: bytes, limits: Limits) -> bytes:
    if data.startswith(b"\x1f\x8b"):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
                decoded = stream.read(limits.decoded_object + 1)
        except (OSError, EOFError, zlib.error):
            raise BackupError("invalid_object_encoding") from None
    else:
        decoded = data
    if len(decoded) > limits.decoded_object:
        raise BackupError("decoded_object_limit")
    return decoded


def _exact(value, fields: set[str]):
    if not isinstance(value, dict) or set(value) != fields:
        raise BackupError("invalid_format_fields")


def _scope_data(scope: Scope) -> list[dict[str, str]]:
    return [{"year": year, "activity_id": identifier} for year, identifier in scope.activities]


def _scope(value, limits: Limits) -> Scope:
    if not isinstance(value, list) or len(value) > limits.activities:
        raise BackupError("invalid_scope")
    for item in value:
        _exact(item, {"year", "activity_id"})
    scope = Scope(tuple((item["year"], item["activity_id"]) for item in value))
    scope.validate(limits)
    return scope


def _fernet(password: bytes, salt: bytes) -> Fernet:
    if not isinstance(password, bytes) or not 12 <= len(password) <= 1024:
        raise BackupError("password_length_invalid")
    # Library recipe, fixed RFC 9106 memory-constrained profile; no custom cipher.
    key = Argon2id(salt=salt, length=32, iterations=3, lanes=4, memory_cost=2**16).derive(password)
    return Fernet(base64.urlsafe_b64encode(key))


def _seal(document: dict, password: bytes, limits: Limits) -> bytes:
    data = _json_bytes(document)
    if len(data) > limits.document:
        raise BackupError("document_size_limit")
    salt = os.urandom(16)
    result = MAGIC + salt + _fernet(password, salt).encrypt(data)
    if len(result) > limits.archive:
        raise BackupError("archive_size_limit")
    return result


def _snapshot(source: ReadOnlySource, scope: Scope, limits: Limits) -> list[ObjectVersion]:
    objects = {}
    total = 0
    for item in source.inventory(scope):
        allowed_key(item.key, scope)
        if (
            item.key in objects
            or type(item.size) is not int
            or item.size < 1
            or item.size > limits.stored_object
            or not isinstance(item.revision, str)
            or not 1 <= len(item.revision) <= 256
        ):
            raise BackupError("invalid_source_inventory")
        objects[item.key] = item
        total += item.size
        if len(objects) > limits.objects or total > limits.stored_total:
            raise BackupError("source_size_limit")
    return sorted(objects.values())


def _date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise BackupError("invalid_object_date")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise BackupError("invalid_object_date") from None


def _timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise BackupError("invalid_object_timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
    except ValueError:
        raise BackupError("invalid_object_timestamp") from None


_PROFILE_FIELDS = {
    "schema_version",
    "profile_id",
    "name",
    "sport",
    "effective_from",
    "default_time_basis",
    "zones",
    "references",
    "created_at",
    "source",
}
_CONTEXT_FIELDS = {
    "schema_version",
    "activity_id",
    "context_id",
    "rpe",
    "conditions",
    "note",
    "workout_correction",
    "source",
    "created_at",
}
_ANALYSIS_FIELDS = {
    "schema_version",
    "analyzer_version",
    "analysis_id",
    "activity_id",
    "activity",
    "profile",
    "user_context",
    "time_basis",
    "summary",
    "heart_rate",
    "workout_structure",
    "sections",
    "distance_halves",
    "kilometer_splits",
    "source",
    "limitations",
}
_FORBIDDEN_FIELDS = {
    "latitude",
    "longitude",
    "lat",
    "lon",
    "position_lat",
    "position_long",
    "gps",
    "coordinates",
    "access_token",
    "refresh_token",
    "password",
    "secret",
    "api_key",
    "aws_secret_access_key",
}


_NUMBER = (int, float, type(None))
_TEXT = (str, type(None))
_RANGE_FIELDS = {
    name: _NUMBER
    for name in (
        "distance_m",
        "duration_seconds",
        "pace_seconds_per_km",
        "average_speed_mps",
        "average_heart_rate_bpm",
        "maximum_heart_rate_bpm",
        "elevation_change_m",
    )
}


def _record(value, fields: dict):
    _exact(value, set(fields))
    if any(type(value[name]) not in types for name, types in fields.items()):
        raise BackupError("invalid_object_value_type")


def _records(value, fields: dict):
    if not isinstance(value, list):
        raise BackupError("invalid_object_value_type")
    for item in value:
        _record(item, fields)


def _profile_shape(value):
    for name in ("name", "sport", "default_time_basis"):
        if not isinstance(value[name], str):
            raise BackupError("invalid_profile")
    _records(value["zones"], {"label": (str,), "min_bpm": (int,), "max_bpm": (int,)})
    _record(
        value["references"],
        {
            **{
                name: (int, type(None))
                for name in ("lt1_min_bpm", "lt1_max_bpm", "lt2_min_bpm", "lt2_max_bpm")
            },
            "interval_thresholds_bpm": (list,),
        },
    )
    if any(type(item) is not int for item in value["references"]["interval_thresholds_bpm"]):
        raise BackupError("invalid_profile")


def _analysis_shape(value):
    if (
        not isinstance(value["analyzer_version"], str)
        or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value["analyzer_version"])
        or value["time_basis"] != "elapsed"
    ):
        raise BackupError("unsupported_analysis_schema")
    _record(value["activity"], {"date": (str,), "name": _TEXT, "type": _TEXT})
    _record(
        value["summary"],
        {
            name: _NUMBER
            for name in (
                "elapsed_seconds",
                "moving_seconds",
                "distance_m",
                "average_speed_mps",
                "average_heart_rate_bpm",
                "maximum_heart_rate_bpm",
                "trackpoint_count",
                "sampled_trackpoint_count",
            )
        },
    )
    _record(
        value["heart_rate"],
        {
            "zones_total": (dict,),
            "seconds_at_or_above_bpm": (dict,),
            "first_to_second_distance_half_drift_bpm": _NUMBER,
            "aerobic_decoupling_percent": _NUMBER,
        },
    )
    heart = value["heart_rate"]
    _record(
        heart["zones_total"],
        {
            "time_basis": (str,),
            "zones": (list,),
            "observed_seconds": _NUMBER,
            "below_first_zone_seconds": _NUMBER,
            "above_last_zone_seconds": _NUMBER,
        },
    )
    _records(
        heart["zones_total"]["zones"],
        {
            "label": (str,),
            "min_bpm": (int,),
            "max_bpm": (int,),
            "seconds": _NUMBER,
            "percent": _NUMBER,
        },
    )
    if any(
        not re.fullmatch(r"[0-9]{1,3}", key) or type(number) not in (int, float)
        for key, number in heart["seconds_at_or_above_bpm"].items()
    ):
        raise BackupError("invalid_analysis_thresholds")
    _record(
        value["workout_structure"],
        {
            "source": (str,),
            "workout_name": _TEXT,
            "has_structured_workout": (bool,),
            "planned_steps": (list,),
            "executed_laps": (list,),
        },
    )
    _records(
        value["workout_structure"]["planned_steps"],
        {
            **{name: _TEXT for name in ("name", "intensity", "duration_type", "target_type")},
            **{
                name: _NUMBER
                for name in (
                    "step_index",
                    "duration_value",
                    "target_value",
                    "repeat_steps",
                    "repeat_from_step",
                )
            },
        },
    )
    _records(
        value["workout_structure"]["executed_laps"],
        {
            **{name: _TEXT for name in ("step_name", "intensity", "section", "start_time")},
            **{
                name: _NUMBER
                for name in (
                    "lap",
                    "workout_step_index",
                    "elapsed_seconds",
                    "moving_seconds",
                    "distance_m",
                    "average_speed_mps",
                    "average_heart_rate_bpm",
                    "maximum_heart_rate_bpm",
                    "ascent_m",
                    "descent_m",
                )
            },
        },
    )
    if not isinstance(value["sections"], list):
        raise BackupError("invalid_analysis_sections")
    for section in value["sections"]:
        if not isinstance(section, dict):
            raise BackupError("invalid_analysis_sections")
        extension = {"laps": (int,)} if "laps" in section else {"note": (str,)}
        coverage = (("heart_rate_coverage" in section or value["analyzer_version"] == "1.4.0") and "laps" in section
                    and value["analyzer_version"] in ("1.0.1", "1.1.0", "1.2.0", "1.3.0", "1.4.0"))
        if coverage:
            extension["heart_rate_coverage"] = (dict,)
        if value["analyzer_version"] == "1.4.0" and "laps" in section:
            extension["measurement_coverage"] = (dict,)
        _record(
            section,
            {
                "section": (str,),
                "source": (str,),
                "elapsed_seconds": _NUMBER,
                "distance_m": _NUMBER,
                "average_heart_rate_bpm": _NUMBER,
                **extension,
            },
        )
        if coverage:
            heart = section["heart_rate_coverage"]
            _record(heart, {"basis": (str,), "covered_seconds": (int, float),
                            "contributing_laps": (int,), "status": (str,)})
            seconds, laps = heart["covered_seconds"], heart["contributing_laps"]
            try:
                finite_seconds = math.isfinite(seconds)
            except OverflowError:
                finite_seconds = False
            if (heart["basis"] != "lap_elapsed"
                    or heart["status"] not in {"complete", "partial", "none", "unknown"}
                    or not finite_seconds or seconds < 0
                    or not 0 <= laps <= section["laps"] or (seconds == 0) != (laps == 0)
                    or (heart["status"] == "none" and seconds != 0)):
                raise BackupError("invalid_section_hr_coverage")
            elapsed = section["elapsed_seconds"]
            try:
                finite_elapsed = type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 0
            except OverflowError:
                finite_elapsed = False
            if (heart["status"] in {"complete", "partial"}
                    and (not laps or not finite_elapsed
                         or (heart["status"] == "complete" and abs(seconds - elapsed) > 0.001)
                         or (heart["status"] == "partial" and not 0 < seconds < elapsed))):
                raise BackupError("invalid_section_hr_coverage")
        if value["analyzer_version"] == "1.4.0" and "laps" in section:
            totals = section["measurement_coverage"]
            _record(totals, {"elapsed_seconds": (dict,), "distance_m": (dict,)})
            for name, total in totals.items():
                _record(total, {"status": (str,), "observed_laps": (int,), "total_laps": (int,)})
                observed, count = total["observed_laps"], total["total_laps"]
                status = "complete" if observed == count else "partial" if observed else "unknown"
                number = section[name]
                try:
                    finite = type(number) in (int, float) and math.isfinite(number) and number >= 0
                except OverflowError:
                    finite = False
                if (not 0 <= observed <= count or count < 1 or count != section["laps"]
                        or total["status"] != status
                        or (status == "complete" and not finite)
                        or (status != "complete" and number is not None)):
                    raise BackupError("invalid_section_measurement_coverage")
    if value["distance_halves"] is not None:
        _record(value["distance_halves"], {"first": (dict,), "second": (dict,)})
        for half in value["distance_halves"].values():
            _record(half, _RANGE_FIELDS)
    _records(value["kilometer_splits"], {**_RANGE_FIELDS, "split": (int,), "partial": (bool,)})
    if not isinstance(value["limitations"], list) or any(
        not isinstance(item, str) for item in value["limitations"]
    ):
        raise BackupError("invalid_analysis_limitations")


def _training_effect_shape(block, analysis):
    _record(block, {"schema_version": (int,), "source": (str,), "method": (str,), "scale": (str,),
                    "activity_id": (str,), "association": (str,), "aerobic": (dict,), "anaerobic": (dict,),
                    **{name: _TEXT for name in ("local_start_time", "local_date", "session_start_time_utc",
                                                "measured_at_utc", "source_fit_sha256")}})
    if (block["schema_version"] != 1 or block["source"] != "fit_session"
            or block["method"] != "fit_recorded_estimate" or block["scale"] != "0_to_5"
            or block["activity_id"] != analysis["activity_id"]
            or block["association"] not in {"matched", "source_missing", "source_unreadable", "invalid_schema",
                                            "decode_errors", "id_mismatch", "time_mismatch",
                                            "sessions_missing", "sessions_ambiguous"}):
        raise BackupError("invalid_training_effect")
    digest = block["source_fit_sha256"]
    if digest is not None and (not re.fullmatch(r"[a-f0-9]{64}", digest)
                               or digest != analysis["source"]["fit_sha256"]):
        raise BackupError("invalid_training_effect_source")
    local = block["local_start_time"]
    if local is not None:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?", local):
            raise BackupError("invalid_training_effect_time")
        try:
            datetime.fromisoformat(local)
        except ValueError:
            raise BackupError("invalid_training_effect_time") from None
    if block["local_date"] != (local[:10] if local else None):
        raise BackupError("invalid_training_effect_time")
    for name in ("session_start_time_utc", "measured_at_utc"):
        if block[name] is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", block[name]):
                raise BackupError("invalid_training_effect_time")
            _timestamp(block[name])
    start, measured = block["session_start_time_utc"], block["measured_at_utc"]
    matched = block["association"] == "matched"
    if (matched and (start is None or local is not None and block["local_date"] != analysis["activity"]["date"])
            or not matched and (start is not None or measured is not None)
            or measured is not None and (start is None or measured < start)):
        raise BackupError("invalid_training_effect_time")
    recorded = analysis["recorded_running_metrics"]
    if any(block[name] is not None and recorded[name] is not None and block[name] != recorded[name]
           for name in ("session_start_time_utc", "measured_at_utc", "local_start_time", "local_date")):
        raise BackupError("invalid_training_effect_time")
    for name, field in (("aerobic", "total_training_effect"), ("anaerobic", "total_anaerobic_training_effect")):
        metric = block[name]
        _record(metric, {"status": (str,), "value": _NUMBER, "source_field": (str,)})
        number, status = metric["value"], metric["status"]
        if metric["source_field"] != field or status not in (
                {"available", "missing", "invalid"} if matched else {"unavailable"}):
            raise BackupError("invalid_training_effect_value")
        if status == "available":
            try:
                valid = (number is not None and math.isfinite(number) and 0 <= number <= 5
                         and abs(number * 10 - round(number * 10)) <= 1e-9)
            except OverflowError:
                valid = False
            if not valid:
                raise BackupError("invalid_training_effect_value")
        elif number is not None:
            raise BackupError("invalid_training_effect_value")


def _recorded_running_metrics_shape(block, analysis):
    metrics = {
        "average_power": ("W", "avg_power"),
        "average_step_length": ("mm", "avg_step_length"),
        "average_ground_contact_time": ("ms", "avg_stance_time"),
        "average_vertical_oscillation": ("mm", "avg_vertical_oscillation"),
        "average_vertical_ratio": ("%", "avg_vertical_ratio"),
    }
    _record(block, {
        "schema_version": (int,), "source": (str,), "method": (str,),
        "activity_id": (str,), "association": (str,),
        **{name: _TEXT for name in (
            "local_start_time", "local_date", "session_start_time_utc", "measured_at_utc",
            "source_fit_sha256",
        )},
        **{name: (dict,) for name in metrics},
    })
    if (block["schema_version"] != 1 or block["source"] != "garmin_fit_session"
            or block["method"] != "fit_recorded_measurement"
            or block["activity_id"] != analysis["activity_id"]
            or block["association"] not in (
                "matched", "source_missing", "invalid_schema", "decode_errors", "id_mismatch",
                "time_mismatch", "sessions_missing", "sessions_ambiguous", "unsupported_sport",
            )):
        raise BackupError("invalid_recorded_running_metrics")
    digest = block["source_fit_sha256"]
    if digest is not None and (not re.fullmatch(r"[a-f0-9]{64}", digest)
                               or digest != analysis["source"]["fit_sha256"]):
        raise BackupError("invalid_recorded_running_source")
    local = block["local_start_time"]
    if local is not None:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?", local):
            raise BackupError("invalid_recorded_running_time")
        try:
            datetime.fromisoformat(local)
        except ValueError:
            raise BackupError("invalid_recorded_running_time") from None
    if block["local_date"] != (local[:10] if local else None):
        raise BackupError("invalid_recorded_running_time")
    for name in ("session_start_time_utc", "measured_at_utc"):
        stamp = block[name]
        if stamp is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", stamp):
                raise BackupError("invalid_recorded_running_time")
            _timestamp(stamp)
    start, measured = block["session_start_time_utc"], block["measured_at_utc"]
    matched = block["association"] == "matched"
    if (matched and (start is None or local is not None
                     and block["local_date"] != analysis["activity"]["date"])
            or not matched and (start is not None or measured is not None)
            or measured is not None and (start is None or measured < start)):
        raise BackupError("invalid_recorded_running_time")
    for name, (unit, field) in metrics.items():
        metric = block[name]
        _record(metric, {"status": (str,), "value": _NUMBER, "unit": (str,),
                         "source_field": (str,)})
        status, number = metric["status"], metric["value"]
        if (metric["unit"] != unit or metric["source_field"] != field
                or status not in (("available", "missing", "invalid") if matched
                                  else ("unavailable",))):
            raise BackupError("invalid_recorded_running_metric")
        if status == "available":
            try:
                valid = number is not None and math.isfinite(number)
            except OverflowError:
                valid = False
            if not valid:
                raise BackupError("invalid_recorded_running_metric")
        elif number is not None:
            raise BackupError("invalid_recorded_running_metric")


def _running_segments_shape(block, analysis):
    if not isinstance(block, dict):
        raise BackupError("invalid_running_segments")
    extras = {"average_ground_contact_balance", "laps", "kilometer_splits", "distance_halves"}
    base = {key: value for key, value in block.items() if key not in extras}
    _recorded_running_metrics_shape({**base, "schema_version": 1}, analysis)
    if type(block["schema_version"]) is not int or block["schema_version"] != 2:
        raise BackupError("invalid_running_segments")
    fields = {
        "average_power": ("W", "power"), "average_step_length": ("mm", "step_length"),
        "average_ground_contact_time": ("ms", "stance_time"),
        "average_vertical_oscillation": ("mm", "vertical_oscillation"),
        "average_vertical_ratio": ("%", "vertical_ratio"),
        "average_ground_contact_balance": ("%", "stance_time_balance"),
    }

    def is_finite(number):
        try:
            return type(number) in (int, float) and math.isfinite(number)
        except OverflowError:
            return False

    def metric(value, unit, field, *, aggregate=False):
        shape = {"status": (str,), "value": _NUMBER, "unit": (str,), "source_field": (str,)}
        balance = field in ("stance_time_balance", "avg_stance_time_balance")
        if balance:
            shape["side_semantics"] = (str,)
        if aggregate:
            shape["coverage"] = (dict,)
        _record(value, shape)
        if (value["unit"] != unit or value["source_field"] != field
                or value["status"] not in ("available", "missing", "invalid", "unavailable")
                or balance and value["side_semantics"] != "unknown"):
            raise BackupError("invalid_running_segment_metric")
        number = value["value"]
        try:
            finite = number is not None and math.isfinite(number)
        except OverflowError:
            finite = False
        if (value["status"] == "available") != finite or value["status"] != "available" and number is not None:
            raise BackupError("invalid_running_segment_metric")
        if aggregate:
            cover = value["coverage"]
            _record(cover, {"valid_seconds": _NUMBER, "eligible_seconds": _NUMBER,
                            "excluded_gap_seconds": _NUMBER, "contributing_intervals": (int,),
                            "status": (str,)})
            for key in ("valid_seconds", "eligible_seconds", "excluded_gap_seconds"):
                if not is_finite(cover[key]) or cover[key] < 0:
                    raise BackupError("invalid_running_segment_coverage")
            valid = cover["valid_seconds"]
            if (valid > cover["eligible_seconds"] + 0.001 or cover["contributing_intervals"] < 0
                    or (cover["status"] == "none") != (cover["contributing_intervals"] == 0)
                    or cover["status"] == "none" and valid != 0
                    or cover["status"] not in ("none", "partial", "complete")
                    or cover["status"] == "complete" and
                    (cover["excluded_gap_seconds"] != 0 or abs(valid - cover["eligible_seconds"]) > 0.001)
                    or (value["status"] == "available") != (cover["status"] != "none")
                    or balance and value["status"] != "unavailable"):
                raise BackupError("invalid_running_segment_coverage")

    metric(block["average_ground_contact_balance"], "%", "avg_stance_time_balance")
    matched = block["association"] == "matched"
    if (block["average_ground_contact_balance"]["status"] == "unavailable") == matched:
        raise BackupError("invalid_running_segment_metric")
    for name in ("laps", "kilometer_splits", "distance_halves"):
        group = block[name]
        group_shape = {"status": (str,), "reason": _TEXT, "method": (str,), "items": (list,)}
        distance = name != "laps"
        if distance:
            group_shape.update(interval_policy=(str,), distance_allocation=(str,), balance_aggregation=(str,))
        _record(group, group_shape)
        if (group["status"] not in ("available", "missing", "invalid", "unavailable")
                or (group["status"] == "available") != bool(group["items"])
                or (group["reason"] is None) != (group["status"] == "available")
                or group["items"] and not matched
                or group["method"] != ("fit_record_time_weighted_mean" if distance else "fit_recorded_measurement")):
            raise BackupError("invalid_running_segment_group")
        if distance and (group["interval_policy"] != "left_sample_hold_distance_progressing_max_30s"
                         or group["distance_allocation"] != "linear_between_record_distances"
                         or group["balance_aggregation"] != "not_computed_without_verified_side_semantics"):
            raise BackupError("invalid_running_segment_policy")
        previous = 0 if distance else block["session_start_time_utc"]
        for ordinal, row in enumerate(group["items"], start=1):
            row_shape = {key: (dict,) for key in fields}
            if distance:
                row_shape.update(start_distance_m=_NUMBER, end_distance_m=_NUMBER, distance_m=_NUMBER)
                row_shape["split" if name == "kilometer_splits" else "segment"] = (int,) if name == "kilometer_splits" else (str,)
            else:
                row_shape.update(lap=(int,), start_time_utc=(str,), end_time_utc=(str,), distance_m=_NUMBER)
            _record(row, row_shape)
            if distance:
                start, end, length = row["start_distance_m"], row["end_distance_m"], row["distance_m"]
                if (any(not is_finite(value) for value in (start, end, length))
                        or start != previous or end <= start or abs(end - start - length) > 0.001
                        or name == "kilometer_splits" and row["split"] != ordinal
                        or name == "distance_halves" and (ordinal > 2 or row["segment"] != ("first" if ordinal == 1 else "second"))):
                    raise BackupError("invalid_running_segment_bounds")
                previous = end
            else:
                start, end = row["start_time_utc"], row["end_time_utc"]
                for stamp in (start, end):
                    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", stamp):
                        raise BackupError("invalid_running_segment_time")
                    _timestamp(stamp)
                if (row["lap"] != ordinal or previous is None or start < previous or end < start
                        or block["measured_at_utc"] is None or end > block["measured_at_utc"]
                        or row["distance_m"] is not None and
                        (not is_finite(row["distance_m"]) or row["distance_m"] < 0)):
                    raise BackupError("invalid_running_segment_time")
                previous = end
            for key, (unit, field) in fields.items():
                metric(row[key], unit, field if distance else "avg_" + field, aggregate=distance)
        if name == "distance_halves" and group["items"]:
            if (len(group["items"]) != 2
                    or any(abs(row["distance_m"] - previous / 2) > 0.000500001 for row in group["items"])):
                raise BackupError("invalid_running_segment_bounds")


def _context_clock(value, *, local=False, nullable=True):
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not 1 <= len(value) <= 40:
        raise BackupError("invalid_stored_context_clock")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise BackupError("invalid_stored_context_clock") from None
    if (parsed.tzinfo is not None if local else
            parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0):
        raise BackupError("invalid_stored_context_clock")
    return parsed


def _context_digest(value, *, nullable=True):
    if value is None and nullable:
        return
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise BackupError("invalid_stored_context_source")


def _context_metric(value, unit, *, available, integral=False, low=0, high=None):
    _record(value, {"status": (str,), "value": _NUMBER, "unit": (str,)})
    if value["unit"] != unit or value["status"] not in (
            ("available", "missing", "invalid") if available else ("unavailable",)):
        raise BackupError("invalid_stored_context_metric")
    number = value["value"]
    if value["status"] != "available":
        if number is not None:
            raise BackupError("invalid_stored_context_metric")
        return
    try:
        valid = (type(number) in ((int,) if integral else (int, float)) and math.isfinite(number)
                 and number >= low and (high is None or number <= high))
    except OverflowError:
        valid = False
    if not valid:
        raise BackupError("invalid_stored_context_metric")


def _stored_health_shape(value, kind, clock, reference, checked_clocks):
    fields = {
        "status": (str,), "source": (str,), "source_revision": _TEXT,
        "source_checked_at": _TEXT, "wake_date": _TEXT, "night_of": _TEXT,
        "local_time_source": (str,), "observation_start_utc": _TEXT,
        "observation_end_utc": _TEXT, "metrics": (dict,),
    }
    detailed = isinstance(value, dict) and "sleep_start_local" in value
    if detailed:
        fields.update(sleep_start_local=_TEXT, sleep_end_local=_TEXT)
    _record(value, fields)
    empty = {"not_collected", "source_error", "oversize", "invalid_schema"}
    if value["source"] != "garmin" or value["status"] not in (
            {"available", "wrong_date", "time_unknown", "local_time_unknown", "after_activity"}
            if detailed else empty):
        raise BackupError("invalid_stored_health_context")
    _context_digest(value["source_revision"])
    checked = _context_clock(value["source_checked_at"])
    if checked is not None:
        checked_clocks.append(checked)
    start = _context_clock(value["observation_start_utc"])
    end = _context_clock(value["observation_end_utc"])
    for name in ("wake_date", "night_of"):
        if value[name] is not None:
            _date(value[name])
    if not detailed:
        if (value["wake_date"] != clock["local_date"] or value["night_of"] is not None
                or value["source_revision"] is not None or checked is not None
                or start is not None or end is not None or value["local_time_source"] != "unavailable"):
            raise BackupError("invalid_stored_health_context")
    else:
        local_clocks = []
        for name, utc in (("sleep_start_local", start), ("sleep_end_local", end)):
            local = value[name]
            if local is not None:
                if not isinstance(local, str) or len(local) > 40:
                    raise BackupError("invalid_stored_context_clock")
                try:
                    local = datetime.fromisoformat(local)
                    valid = (local.tzinfo is not None and abs(local.utcoffset().total_seconds()) <= 14 * 3600
                                 and utc is not None)
                    if valid and value["local_time_source"] in {"garmin_local", "unavailable"}:
                        # The producer infers a minute offset but preserves
                        # Garmin's original wall clock, even for a partial pair.
                        minutes = (local.replace(tzinfo=None) - utc.replace(tzinfo=None)).total_seconds() / 60
                        offset = round(minutes)
                        valid = (local.utcoffset().total_seconds() == offset * 60
                                 and abs(minutes - offset) <= 0.5)
                    elif valid:
                        valid = abs((local - utc).total_seconds()) < 0.001
                except ValueError:
                    valid = False
                if not valid:
                    raise BackupError("invalid_stored_context_clock")
            local_clocks.append(local)
        local_start, local_end = local_clocks
        both_local = local_start is not None and local_end is not None
        if (value["local_time_source"] not in {"unavailable", "garmin_local", "configured_timezone"}
                or (value["local_time_source"] != "unavailable") != both_local
                or value["night_of"] != (local_start.date().isoformat() if local_start else None)):
            raise BackupError("invalid_stored_health_context")
        expected = ("time_unknown" if clock["local_date"] is None
                    else "wrong_date" if value["wake_date"] != clock["local_date"]
                    else "time_unknown" if reference is None or start is None or end is None or end <= start
                    else "local_time_unknown" if not both_local
                    else "wrong_date" if local_end.date().isoformat() != value["wake_date"]
                    else "after_activity" if end > reference else "available")
        if value["status"] != expected:
            raise BackupError("invalid_stored_health_attribution")
    names = SLEEP_METRICS if kind == "sleep" else {name: "ms" for name in HRV_METRICS}
    _exact(value["metrics"], set(names))
    for name, unit in names.items():
        _context_metric(value["metrics"][name], unit, available=value["status"] == "available",
                        high=100 if unit in {"score_0_100", "%"} else None)


def _stored_training_shape(value, day, reference, checked_clocks):
    fields = {"status": (str,), "selection_status": (str,), "selected_slot": _TEXT,
              "temporal_status": (str,), "readiness": (dict,), "recovery": (dict,)}
    stored = isinstance(value, dict) and value.get("status") == "stored"
    if stored:
        fields.update({
            "source": (str,), "sdk_version": (str,), "normalizer_revision": (int,),
            "source_revision": (str,), "date": (str,), "timezone": (str,),
            "input_context": _TEXT, "last_attempt": (dict,), "source_checked_at": _TEXT,
            "time_provenance": (dict,),
        })
    _record(value, fields)
    temporal = value["temporal_status"]
    if stored:
        _date(value["date"])
        _context_digest(value["source_revision"], nullable=False)
        if (value["source"] != "garmin-connect" or value["sdk_version"] != "0.3.17"
                or value["normalizer_revision"] != 1 or value["date"] != day
                or value["selection_status"] not in {
                    "available", "ambiguous", "no_observation", "selected_device_unavailable", "unknown"}
                or temporal not in {"before_activity", "after_activity", "time_unknown", "selection_unavailable"}
                or value["selected_slot"] not in {None, "morning", "latest"}
                or (value["selected_slot"] is None) != (temporal == "selection_unavailable")
                or value["selected_slot"] is not None and value["selection_status"] != "available"
                or value["input_context"] not in {None, "AFTER_WAKEUP_RESET"}
                or not 1 <= len(value["timezone"]) <= 128
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.+-]*(?:/[A-Za-z][A-Za-z0-9_.+-]*)*",
                                    value["timezone"])):
            raise BackupError("invalid_stored_training_context")
        # This is archived provenance, not a request to resolve a timezone.
        # Offline verification must not require the producer's local tz database.
        _record(value["last_attempt"], {"status": (str,), "attempted_at": (str,)})
        if value["last_attempt"]["status"] not in _STORED_TRAINING_ATTEMPT_STATUSES:
            raise BackupError("invalid_stored_training_context")
        _context_clock(value["last_attempt"]["attempted_at"], nullable=False)
        checked = _context_clock(value["source_checked_at"])
        if checked is not None:
            checked_clocks.append(checked)
        _exact(value["time_provenance"], {"readiness", "recovery"})
    elif (value["status"] not in {"not_collected", "source_error", "oversize", "invalid_schema"}
            or value["selection_status"] != "unknown" or value["selected_slot"] is not None
            or temporal != "selection_unavailable"):
        raise BackupError("invalid_stored_training_context")
    for name, unit, low, high in (("readiness", "score_1_100", 1, 100), ("recovery", "minutes", 0, 2**31 - 1)):
        metric = value[name]
        _record(metric, {"value": _NUMBER, "status": (str,), "unit": (str,),
                         "observation_at": _TEXT, "source_checked_at": _TEXT, "retained_previous": (bool,)})
        observed = _context_clock(metric["observation_at"])
        checked = _context_clock(metric["source_checked_at"])
        if checked is not None:
            checked_clocks.append(checked)
        available = metric["status"] in {"available", "conflicting"}
        if (metric["status"] not in {"available", "conflicting", "invalid", "no_observation", "unavailable"}
                or metric["unit"] != unit or metric["status"] == "conflicting" and name != "recovery"
                or available and (temporal != "before_activity" or reference is None or observed is None
                                  or observed > reference or checked is None)
                or metric["status"] in {"invalid", "no_observation"} and checked is not None):
            raise BackupError("invalid_stored_training_attribution")
        _context_metric({"unit": unit, "value": metric["value"],
                         "status": "available" if available else "missing"},
                        unit, available=True, integral=True, low=low, high=high)
        if not stored:
            if (metric["status"] != "unavailable" or observed is not None or checked is not None
                    or metric["retained_previous"]):
                raise BackupError("invalid_stored_training_context")
        else:
            provenance = value["time_provenance"][name]
            _record(provenance, {"observation_local": _TEXT, "time_source": (str,)})
            local = _context_clock(provenance["observation_local"], local=True)
            if (provenance["time_source"] not in {
                    "garmin_offset", "configured_timezone", "unavailable", "timezone_conflict"}
                    or local is not None and local.date().isoformat() != day):
                raise BackupError("invalid_stored_training_context")


def _stored_context_shape(value, analysis):
    _record(value, {"schema_version": (int,), "snapshot_at": _TEXT, "reference_time_utc": _TEXT,
                    "semantics": (str,), "sleep": (dict,), "hrv": (dict,), "training_day": (dict,)})
    clock = analysis["recorded_running_metrics"]
    if (value["schema_version"] != 1 or value["semantics"] != "retrospective_pre_activity"
            or value["reference_time_utc"] != clock["session_start_time_utc"]):
        raise BackupError("invalid_stored_context")
    snapshot = _context_clock(value["snapshot_at"])
    reference = _context_clock(value["reference_time_utc"])
    checked_clocks = []
    for kind in ("sleep", "hrv"):
        _stored_health_shape(value[kind], kind, clock, reference, checked_clocks)
    _stored_training_shape(value["training_day"], analysis["activity"]["date"], reference, checked_clocks)
    if snapshot is not None and any(checked > snapshot for checked in checked_clocks):
        raise BackupError("invalid_stored_context_clock")


def _native_lap_binding_shape(value):
    from .recorded_running_metrics import _utc

    def index(item):
        return type(item) is int and 0 <= item <= 2**53 - 1

    def indexed(rows, field):
        result = {}
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, dict) and index(row.get(field)):
                result.setdefault(row[field], []).append(row)
        return result

    block = value.get("native_lap_workout_binding")
    _exact(block, {"schema_version", "method", "measurement_source", "activity_id",
                   "source_fit_sha256", "status", "reason", "items"})
    recorded = value["recorded_running_metrics"]
    if (type(block["schema_version"]) is not int or block["schema_version"] != 1
            or block["method"] != "fit_lap_wkt_step_index"
            or block["measurement_source"] != "recorded_running_metrics.laps"
            or block["activity_id"] != value["activity_id"]):
        raise BackupError("invalid_native_lap_binding")
    digest = recorded.get("source_fit_sha256")
    source_ready = (recorded["association"] == "matched" and isinstance(digest, str)
                    and re.fullmatch(r"[a-f0-9]{64}", digest))
    if block["source_fit_sha256"] != (digest if source_ready else None):
        raise BackupError("invalid_native_lap_binding")
    if not source_ready or recorded["laps"]["status"] != "available":
        reason = "source_unavailable" if not source_ready else "recorded_laps_unavailable"
        if block["status"] != "unavailable" or block["reason"] != reason or block["items"] != []:
            raise BackupError("invalid_native_lap_binding")
        return
    items, laps = block["items"], recorded["laps"]["items"]
    if not isinstance(items, list) or len(items) != len(laps) or not laps:
        raise BackupError("invalid_native_lap_binding")
    structure = value["workout_structure"]
    planned = indexed(structure.get("planned_steps"), "step_index")
    executed = indexed(structure.get("executed_laps"), "lap")
    seen = set()
    bound = 0
    for item, lap in zip(items, laps, strict=True):
        _exact(item, {"lap", "source_message_index", "status", "reason", "workout_step_index"})
        if not index(item["lap"]) or item["lap"] != lap["lap"]:
            raise BackupError("invalid_native_lap_binding")
        source_index = item["source_message_index"]
        if source_index is not None:
            if not index(source_index) or source_index in seen:
                raise BackupError("invalid_native_lap_binding")
            seen.add(source_index)
        if item["status"] == "unbound":
            if (item["workout_step_index"] is not None
                    or item["reason"] not in ("lap_identity_unverified", "workout_step_unverified", "executed_lap_mismatch")
                    or (item["reason"] == "lap_identity_unverified") != (source_index is None)):
                raise BackupError("invalid_native_lap_binding")
            continue
        step = item["workout_step_index"]
        matches = executed.get(lap["lap"], [])
        if (item["status"] != "bound" or item["reason"] is not None or source_index is None
                or not index(step) or len(planned.get(step, [])) != 1 or len(matches) != 1):
            raise BackupError("invalid_native_lap_binding")
        execution = matches[0]
        distance = execution.get("distance_m")
        if (not index(execution.get("workout_step_index")) or execution["workout_step_index"] != step
                or _utc(execution.get("start_time")) != lap["start_time_utc"]
                or distance is not None and (type(distance) not in (int, float) or not math.isfinite(distance) or distance < 0)
                or distance != lap["distance_m"]):
            raise BackupError("invalid_native_lap_binding")
        bound += 1
    status, reason = (("available", None) if bound == len(items) else
                      ("partial", "some_laps_unbound") if bound else
                      ("unavailable", "no_verified_lap_workout_links"))
    if block["status"] != status or block["reason"] != reason:
        raise BackupError("invalid_native_lap_binding")


def _logical_bouts_shape(value):
    """Validate stored references, without fetching FIT or reanalysing history."""
    from .coach_segment_metrics import _epoch, _frame

    block = value.get("logical_bouts")
    if isinstance(block, dict) and type(block.get("schema_version")) is int and block["schema_version"] == 2:
        _source_order_bouts_shape(value)
        return
    _exact(block, {"schema_version", "method", "clock_basis", "activity_id", "source_fit_sha256",
                   "status", "reason", "occurrence_basis", "items", "unassigned_laps"})
    if (type(block["schema_version"]) is not int or block["schema_version"] != 1
            or block["method"] != "verified_native_lap_contiguity"
            or block["clock_basis"] != "fit_start_time_plus_total_elapsed_time"
            or block["activity_id"] != value["activity_id"]
            or block["source_fit_sha256"] not in (None, value["source"]["fit_sha256"])
            or block["occurrence_basis"] != "derived_verified_bout_order_per_step"
            or not isinstance(block["items"], list) or not isinstance(block["unassigned_laps"], list)):
        raise BackupError("invalid_logical_bouts")
    recorded = value["recorded_running_metrics"]
    ready = recorded["association"] == "matched" and recorded["source_fit_sha256"] is not None
    if block["source_fit_sha256"] != (recorded["source_fit_sha256"] if ready else None):
        raise BackupError("invalid_logical_bouts")
    if not ready or recorded["laps"]["status"] != "available":
        reason = "source_unavailable" if not ready else "recorded_laps_unavailable"
        if (block["status"] != "unavailable" or block["reason"] != reason
                or block["items"] or block["unassigned_laps"]):
            raise BackupError("invalid_logical_bouts")
        return
    native = {row["lap"]: row for row in recorded["laps"]["items"]}
    bindings = {row["lap"]: row for row in value["native_lap_workout_binding"]["items"]}
    planned = {row["step_index"]: row for row in value["workout_structure"]["planned_steps"]}
    executions = {row["lap"]: row for row in value["workout_structure"]["executed_laps"]}
    segments = value["segment_running_metrics"]["executed_intervals"]
    intervals = {row["interval"]: row for row in segments["items"]}
    roles = {"active": "work", "rest": "recovery", "recovery": "recovery",
             "warmup": "warmup", "cooldown": "cooldown"}
    covered, occurrences, previous = [], {}, None
    for ordinal, group in enumerate(block["items"], 1):
        _exact(group, {"bout", "role", "source_intensity", "source_laps", "source_message_indices",
                       "workout_step_index", "observed_step_occurrence", "planned_repeat_iteration",
                       "start_time_utc", "end_time_utc", "record_aggregate"})
        laps, indices, step = group["source_laps"], group["source_message_indices"], group["workout_step_index"]
        start, end = _epoch(group["start_time_utc"]), _epoch(group["end_time_utc"])
        if (type(group["bout"]) is not int or group["bout"] != ordinal
                or not isinstance(laps, list) or not laps or any(type(lap) is not int for lap in laps)
                or laps != list(range(laps[0], laps[0] + len(laps)))
                or not isinstance(indices, list) or len(indices) != len(laps)
                or any(type(index) is not int or index < 0 for index in indices)
                or type(step) is not int or step < 0 or step not in planned
                or not isinstance(group["source_intensity"], str) or not isinstance(group["role"], str)
                or group["role"] != roles.get(group["source_intensity"])
                or group["role"] not in roles.values()
                or planned[step].get("intensity") != group["source_intensity"]
                or group["planned_repeat_iteration"] is not None
                or start is None or end is None or end <= start):
            raise BackupError("invalid_logical_bouts")
        occurrences[step] = occurrences.get(step, 0) + 1
        if (type(group["observed_step_occurrence"]) is not int
                or group["observed_step_occurrence"] != occurrences[step]):
            raise BackupError("invalid_logical_bouts")
        elapsed_end = None
        for lap, index in zip(laps, indices, strict=True):
            binding, execution = bindings.get(lap), executions.get(lap)
            if (lap not in native or binding is None or execution is None
                    or binding["status"] != "bound" or binding["source_message_index"] != index
                    or binding["workout_step_index"] != step or execution["workout_step_index"] != step
                    or execution["intensity"] != group["source_intensity"]):
                raise BackupError("invalid_logical_bouts")
            frame = _frame({"start_time": execution.get("start_time"),
                            "total_elapsed_time": execution.get("elapsed_seconds")})
            if (frame is None or frame["end"] <= frame["start"]
                    or elapsed_end is not None and frame["start_time_utc"] != elapsed_end):
                raise BackupError("invalid_logical_bouts")
            elapsed_end = frame["end_time_utc"]
        if group["end_time_utc"] != elapsed_end:
            raise BackupError("invalid_logical_bouts")
        if (group["start_time_utc"] != native[laps[0]]["start_time_utc"]
                or previous is not None and (laps[0] <= previous["source_laps"][-1]
                                            or start < _epoch(previous["end_time_utc"]))):
            raise BackupError("invalid_logical_bouts")
        if (previous is not None and previous["source_laps"][-1] + 1 == laps[0]
                and previous["workout_step_index"] == step and previous["role"] == group["role"]
                and previous["end_time_utc"] == group["start_time_utc"]):
            raise BackupError("invalid_logical_bouts")
        reference = group["record_aggregate"]
        _exact(reference, {"status", "interval", "reason"})
        if reference["status"] == "available":
            row = intervals.get(reference["interval"]) if type(reference["interval"]) is int else None
            if (row is None or group["role"] != "work" or segments["status"] != "available"
                    or row["source_laps"] != laps or row["workout_step_index"] != step
                    or row["start_time_utc"] != group["start_time_utc"]
                    or row["end_time_utc"] != group["end_time_utc"] or reference["reason"] is not None):
                raise BackupError("invalid_logical_bouts")
        elif (reference["status"] != "unavailable" or reference["interval"] is not None
              or not isinstance(reference["reason"], str) or not reference["reason"]):
            raise BackupError("invalid_logical_bouts")
        covered.extend(laps)
        previous = group
    unassigned = []
    for row in block["unassigned_laps"]:
        _exact(row, {"lap", "reason"})
        if (type(row["lap"]) is not int or row["lap"] not in native
                or row["reason"] not in ("native_binding_unverified", "role_unverified", "clock_basis_unverified")):
            raise BackupError("invalid_logical_bouts")
        unassigned.append(row["lap"])
    if len(set(covered + unassigned)) != len(covered + unassigned):
        raise BackupError("invalid_logical_bouts")
    if sorted(covered + unassigned) != sorted(native):
        raise BackupError("invalid_logical_bouts")
    if block["items"]:
        expected_status = "partial" if unassigned else "available"
        expected_reason = "some_laps_unassigned" if unassigned else None
        if (block["source_fit_sha256"] is None
                or block["status"] != expected_status or block["reason"] != expected_reason):
            raise BackupError("invalid_logical_bouts")
    elif block["status"] != "unavailable" or block["reason"] != "no_verified_bouts":
        raise BackupError("invalid_logical_bouts")


def _source_order_bouts_shape(value):
    """Stored source order is independent of elapsed timing and numeric scope."""
    from .coach_segment_metrics import _epoch, _frame

    block = value["logical_bouts"]
    _exact(block, {"schema_version", "method", "grouping_basis", "activity_id", "source_fit_sha256",
                   "status", "reason", "occurrence_basis", "items", "unassigned_laps", "source_coverage"})
    if (block["method"] != "verified_native_source_order"
            or block["grouping_basis"] != "verified_native_source_order"
            or block["occurrence_basis"] != "derived_verified_source_order_per_step"
            or block["activity_id"] != value["activity_id"]
            or not isinstance(block["items"], list) or not isinstance(block["unassigned_laps"], list)):
        raise BackupError("invalid_logical_bouts")
    recorded = value["recorded_running_metrics"]
    ready = recorded["association"] == "matched" and recorded["source_fit_sha256"] is not None
    if block["source_fit_sha256"] != (recorded["source_fit_sha256"] if ready else None):
        raise BackupError("invalid_logical_bouts")
    coverage = block["source_coverage"]
    if not ready or recorded["laps"]["status"] != "available":
        reason = "source_unavailable" if not ready else "recorded_laps_unavailable"
        if (coverage != {"status": "unavailable", "reason": reason}
                or block["status"] != "unavailable" or block["reason"] != reason
                or block["items"] or block["unassigned_laps"]):
            raise BackupError("invalid_logical_bouts")
        return
    native = {row["lap"]: row for row in recorded["laps"]["items"]}
    bindings = {row["lap"]: row for row in value["native_lap_workout_binding"]["items"]}
    structure = value["workout_structure"]
    executions = {row["lap"]: row for row in structure["executed_laps"]}
    planned = {row["step_index"]: row for row in structure["planned_steps"]}
    _exact(coverage, {"status", "reason", "raw_lap_count", "native_lap_count", "binding_count", "execution_count"})
    counts = [coverage[name] for name in ("raw_lap_count", "native_lap_count", "binding_count", "execution_count")]
    if (any(count is not None and (type(count) is not int or count < 0) for count in counts)
            or counts[1:] != [len(native), len(bindings), len(structure["executed_laps"])]):
        raise BackupError("invalid_logical_bouts")
    if coverage["status"] == "unavailable":
        if (coverage["reason"] != "source_coverage_unverified" or block["items"]
                or block["status"] != "unavailable" or block["reason"] != "source_coverage_unverified"
                or block["unassigned_laps"] != [{"lap": lap, "reason": "source_coverage_unverified"} for lap in native]):
            raise BackupError("invalid_logical_bouts")
        return
    if coverage["status"] != "complete" or coverage["reason"] is not None or counts != [len(native)] * 4 or not native:
        raise BackupError("invalid_logical_bouts")
    if any(isinstance(group, dict) and isinstance(group.get("timing"), dict)
           and group["timing"].get("status") == "available" for group in block["items"]):
        # A local lap frame does not prove the complete session clock contract.
        # Summarytimestamp is deliberately not used as an elapsed-end fallback.
        segment = value["segment_running_metrics"]
        start, end = _epoch(segment["session_start_time_utc"]), _epoch(segment["session_end_time_utc"])
        if (segment["association"] != "matched" or segment["activity_id"] != block["activity_id"]
                or segment["source_fit_sha256"] != block["source_fit_sha256"]
                or segment["session_start_time_utc"] != recorded["session_start_time_utc"]
                or start is None or end is None or end <= start):
            raise BackupError("invalid_logical_bouts")
        previous_end = start
        for ordinal, (execution, lap) in enumerate(zip(structure["executed_laps"], recorded["laps"]["items"], strict=True), 1):
            frame = _frame({"start_time": execution.get("start_time"),
                            "total_elapsed_time": execution.get("elapsed_seconds")})
            if (execution.get("lap") != ordinal or lap["lap"] != ordinal or frame is None
                    or frame["start_time_utc"] != lap["start_time_utc"] or frame["end"] <= frame["start"]
                    or frame["start"] < previous_end or frame["end"] > end):
                raise BackupError("invalid_logical_bouts")
            previous_end = frame["end"]
    segments = value["segment_running_metrics"]["executed_intervals"]
    intervals = {row["interval"]: row for row in segments["items"]}
    roles = {"active": "work", "rest": "recovery", "recovery": "recovery", "warmup": "warmup", "cooldown": "cooldown"}
    covered, occurrences, previous = set(), {}, None
    for ordinal, group in enumerate(block["items"], 1):
        _exact(group, {"bout", "role", "source_intensity", "source_laps", "source_message_indices",
                       "workout_step_index", "observed_step_occurrence", "planned_repeat_iteration", "timing", "record_aggregate"})
        laps, indices, step = group["source_laps"], group["source_message_indices"], group["workout_step_index"]
        if (type(group["bout"]) is not int or group["bout"] != ordinal
                or not isinstance(laps, list) or not laps or any(type(lap) is not int or lap < 1 for lap in laps)
                or laps != list(range(laps[0], laps[0] + len(laps)))
                or not isinstance(indices, list) or len(indices) != len(laps)
                or any(type(index) is not int or index < 0 or index > 9007199254740991 for index in indices)
                or type(step) is not int or step not in planned
                or not isinstance(group["source_intensity"], str) or not isinstance(group["role"], str)
                or group["role"] != roles.get(group["source_intensity"]) or group["role"] not in roles.values()
                or planned[step].get("intensity") != group["source_intensity"] or group["planned_repeat_iteration"] is not None):
            raise BackupError("invalid_logical_bouts")
        occurrences[step] = occurrences.get(step, 0) + 1
        if type(group["observed_step_occurrence"]) is not int or group["observed_step_occurrence"] != occurrences[step]:
            raise BackupError("invalid_logical_bouts")
        timing = group["timing"]
        _exact(timing, {"status", "basis", "reason", "start_time_utc", "end_time_utc"})
        if timing["basis"] != "fit_start_time_plus_total_elapsed_time":
            raise BackupError("invalid_logical_bouts")
        elapsed_end = None
        for lap, index in zip(laps, indices, strict=True):
            binding, execution = bindings.get(lap), executions.get(lap)
            if (lap not in native or lap in covered or binding is None or execution is None
                    or binding["status"] != "bound" or binding["source_message_index"] != index
                    or binding["workout_step_index"] != step or execution["workout_step_index"] != step
                    or execution["intensity"] != group["source_intensity"]):
                raise BackupError("invalid_logical_bouts")
            if timing["status"] == "available":
                frame = _frame({"start_time": execution.get("start_time"), "total_elapsed_time": execution.get("elapsed_seconds")})
                if (frame is None or frame["end"] <= frame["start"]
                        or elapsed_end is not None and frame["start_time_utc"] != elapsed_end):
                    raise BackupError("invalid_logical_bouts")
                elapsed_end = frame["end_time_utc"]
            covered.add(lap)
        if timing["status"] == "available":
            if (timing["reason"] is not None or timing["start_time_utc"] != native[laps[0]]["start_time_utc"]
                    or timing["end_time_utc"] != elapsed_end):
                raise BackupError("invalid_logical_bouts")
        elif (timing["status"] != "unavailable" or timing["reason"] != "clock_basis_unverified"
              or timing["start_time_utc"] is not None or timing["end_time_utc"] is not None):
            raise BackupError("invalid_logical_bouts")
        if (previous is not None and (laps[0] <= previous["source_laps"][-1]
                or previous["source_laps"][-1] + 1 == laps[0] and previous["workout_step_index"] == step
                and previous["role"] == group["role"])):
            raise BackupError("invalid_logical_bouts")
        reference = group["record_aggregate"]
        _exact(reference, {"status", "interval", "reason"})
        if reference["status"] == "available":
            row = intervals.get(reference["interval"]) if type(reference["interval"]) is int else None
            if (row is None or timing["status"] != "available" or group["role"] != "work"
                    or segments["status"] != "available" or reference["reason"] is not None
                    or row["source_laps"] != laps or row["workout_step_index"] != step
                    or row["start_time_utc"] != timing["start_time_utc"] or row["end_time_utc"] != timing["end_time_utc"]):
                raise BackupError("invalid_logical_bouts")
        elif (reference["status"] != "unavailable" or reference["interval"] is not None
              or not isinstance(reference["reason"], str) or not reference["reason"]
              or timing["status"] == "unavailable" and reference["reason"] != timing["reason"]):
            raise BackupError("invalid_logical_bouts")
        previous = group
    for row in block["unassigned_laps"]:
        _exact(row, {"lap", "reason"})
        if (type(row["lap"]) is not int or row["lap"] not in native or row["lap"] in covered
                or row["reason"] not in ("native_binding_unverified", "role_unverified")):
            raise BackupError("invalid_logical_bouts")
        covered.add(row["lap"])
    status = "partial" if block["items"] and block["unassigned_laps"] else "available" if block["items"] else "unavailable"
    reason = "some_laps_unassigned" if status == "partial" else None if status == "available" else "no_verified_bouts"
    if covered != set(native) or block["status"] != status or block["reason"] != reason:
        raise BackupError("invalid_logical_bouts")


def _recorded_cadence_shape(value):
    """Validate the additive native running-cadence contract, never infer values."""
    block = value["recorded_cadence"]
    if isinstance(block, dict) and type(block.get("schema_version")) is int and block["schema_version"] == 2:
        _exact(block, {"schema_version", "method", "activity_id", "source_fit_sha256", "association", "session", "laps", "record_aggregates"})
        native = {**{name: item for name, item in block.items() if name != "record_aggregates"}, "schema_version": 1}
        _recorded_cadence_shape({**value, "recorded_cadence": native})
        _record_cadence_halves_shape(value)
        return
    _exact(block, {"schema_version", "method", "activity_id", "source_fit_sha256", "association", "session", "laps"})
    if (type(block["schema_version"]) is not int or block["schema_version"] != 1
            or block["method"] != "fit_running_native_cadence_conversion" or block["activity_id"] != value["activity_id"]):
        raise BackupError("invalid_recorded_cadence")
    source_reasons = {"source_unmatched", "source_identity_unverified", "session_ambiguous",
                      "session_identity_unverified", "running_sport_unverified"}
    precision = {"unverified", "base_unavailable", "base_invalid", "conflicting_base", "fraction_unavailable", "fraction_invalid"}
    reasons = source_reasons | {"base_missing_or_invalid", "base_invalid", "base_alias_conflict", "fraction_missing_or_invalid", "fraction_invalid"}

    def metric(row, name):
        _exact(row, {"status", "value", "unit", "source_fields", "precision", "reason"})
        prefix = "avg" if name == "average" else "max"
        fields = row["source_fields"]
        allowed = {f"{prefix}_cadence", f"{prefix}_running_cadence", f"{prefix}_fractional_cadence"}
        if (row["unit"] != "steps_per_minute" or not isinstance(fields, list)
                or any(not isinstance(field, str) or field not in allowed for field in fields)
                or len(fields) != len(set(fields)) or len(fields) > 2):
            raise BackupError("invalid_recorded_cadence")
        if row["status"] == "available":
            number = row["value"]
            # FIT uint8 base/fraction encoding, not a physiological cutoff.
            if (type(number) not in (int, float) or not math.isfinite(number) or not 0 <= number <= 511.96875
                    or number * 64 != int(number * 64) or row["reason"] is not None
                    or row["precision"] != "recorded_base_and_fraction"
                    or fields not in ([f"{prefix}_cadence", f"{prefix}_fractional_cadence"],
                                      [f"{prefix}_running_cadence", f"{prefix}_fractional_cadence"])):
                raise BackupError("invalid_recorded_cadence")
        elif (row["status"] != "unavailable" or row["value"] is not None
              or not isinstance(row["reason"], str) or row["reason"] not in reasons
              or not isinstance(row["precision"], str) or row["precision"] not in precision):
            raise BackupError("invalid_recorded_cadence")

    _exact(block["session"], {"average", "maximum"})
    _exact(block["laps"], {"status", "reason", "items"})
    for name in ("average", "maximum"):
        metric(block["session"][name], name)
    laps, recorded = block["laps"], value["recorded_running_metrics"]
    if not isinstance(laps["items"], list):
        raise BackupError("invalid_recorded_cadence")
    if block["association"] != "matched":
        reason = block["association"]
        if (not isinstance(reason, str) or reason not in source_reasons or block["source_fit_sha256"] is not None
                or laps != {"status": "unavailable", "reason": reason, "items": []}
                or any(row["status"] != "unavailable" or row["reason"] != reason for row in block["session"].values())):
            raise BackupError("invalid_recorded_cadence")
        return
    if (recorded["association"] != "matched" or not block["source_fit_sha256"]
            or block["source_fit_sha256"] != recorded["source_fit_sha256"]):
        raise BackupError("invalid_recorded_cadence")
    if laps["status"] == "unavailable":
        if laps["reason"] not in ("native_laps_unavailable", "lap_identity_unverified") or laps["items"]:
            raise BackupError("invalid_recorded_cadence")
        return
    native = recorded["laps"]
    if (laps["status"] != "available" or laps["reason"] is not None or native["status"] != "available"
            or len(laps["items"]) != len(native["items"])):
        raise BackupError("invalid_recorded_cadence")
    for row, original in zip(laps["items"], native["items"], strict=True):
        _exact(row, {"lap", "average", "maximum"})
        if type(row["lap"]) is not int or row["lap"] != original["lap"]:
            raise BackupError("invalid_recorded_cadence")
        for name in ("average", "maximum"):
            metric(row[name], name)


def _record_cadence_halves_shape(value):
    groups = value["recorded_cadence"]["record_aggregates"]
    if not isinstance(groups, dict) or not {"whole_distance_halves"} <= set(groups) <= {"whole_distance_halves", "main_distance_halves", "executed_intervals", "recovery_bouts", "kilometer_splits"}:
        raise BackupError("invalid_recorded_cadence")
    for name in groups:
        _record_cadence_half_shape(value, name)


def _recovery_cadence_references(value):
    """Bind cadence to already validated logical recovery and elapsed lap facts."""
    from .coach_segment_metrics import _epoch, _frame

    logical = value["logical_bouts"]
    if (logical["schema_version"] != 2 or logical["status"] != "available"
            or logical["source_coverage"]["status"] != "complete" or logical["unassigned_laps"]
            or value["workout_structure"]["has_structured_workout"] is not True):
        raise BackupError("invalid_recorded_cadence")
    executed = {row["lap"]: row for row in value["workout_structure"]["executed_laps"]}
    items = []
    for row in logical["items"]:
        if row["role"] != "recovery":
            continue
        timing = row["timing"]
        selected = [executed[lap] for lap in row["source_laps"]]
        frames = [_frame({"start_time": source["start_time"], "total_elapsed_time": source["elapsed_seconds"]})
                  for source in selected]
        if (timing["status"] != "available" or not selected
                or any(frame is None or frame["end"] <= frame["start"] for frame in frames)
                or any(source["workout_step_index"] != row["workout_step_index"]
                       or source["intensity"] not in ("recovery", "rest") for source in selected)
                or any(left["end"] != right["start"] for left, right in zip(frames, frames[1:], strict=False))
                or _epoch(timing["start_time_utc"]) != frames[0]["start"]
                or _epoch(timing["end_time_utc"]) != frames[-1]["end"]):
            raise BackupError("invalid_recorded_cadence")
        distance = sum(source["distance_m"] for source in selected)
        if not math.isfinite(distance) or distance <= 0:
            raise BackupError("invalid_recorded_cadence")
        items.append({"bout": row["bout"], "source_laps": row["source_laps"],
                      "workout_step_index": row["workout_step_index"], "distance_m": distance,
                      "start_time_utc": timing["start_time_utc"], "end_time_utc": timing["end_time_utc"],
                      "elapsed_seconds": frames[-1]["end"] - frames[0]["start"]})
    return {"status": "available", "distance_basis": "fit_verified_recovery_bout_distance", "items": items}


def _record_cadence_half_shape(value, group_name):
    """Check stored record scope/coverage against the unchanged strict geometry."""
    block = value["recorded_cadence"]
    group = block["record_aggregates"][group_name]
    if not isinstance(group, dict):
        raise BackupError("invalid_recorded_cadence")
    work, recovery = group_name == "executed_intervals", group_name == "recovery_bouts"
    kilometers = group_name == "kilometer_splits"
    basis = ("tcx_endurance_kilometer_splits" if kilometers else "fit_verified_recovery_bout_distance"
             if recovery else "fit_executed_active_distance" if work else "tcx_endurance_distance")
    if group_name == "main_distance_halves":
        basis = group.get("distance_basis")
        if basis not in ("fit_executed_main_distance", "tcx_endurance_distance_continuous_main"):
            raise BackupError("invalid_recorded_cadence")
    references = group_name == "main_distance_halves" and basis == "fit_executed_main_distance" and group.get("status") == "available"
    literals = {"method": "fit_record_time_weighted_mean", "distance_basis": basis,
                "clock_basis": "fit_start_time_plus_total_elapsed_time",
                "interval_policy": "left_sample_hold_distance_progressing_max_30s",
                "distance_allocation": "linear_between_record_distances", "input_unit": "rpm", "unit": "steps_per_minute",
                "conversion": "2 * (cadence + fractional_cadence)", "valid_record_precision": "recorded_base_and_fraction"}
    _exact(group, set(literals) | {"status", "reason", "items", "source_tcx_sha256", "source_fields", "session_start_time_utc",
                                  "session_end_time_utc", "session_summary_timestamp_utc", "source_window"}
           | ({"source_laps", "source_windows"} if references else set()))
    if (any(group[name] != item for name, item in literals.items())
            or group["source_tcx_sha256"] != value["source"]["tcx_sha256"]
            or group["source_fields"] != ["cadence", "fractional_cadence"] or not isinstance(group["items"], list)):
        raise BackupError("invalid_recorded_cadence")
    window = group["source_window"]
    _exact(window, {"disposition", "original_record_count", "boundary_excluded_record_count", "boundary_excluded_pair_count"})
    count, excluded, pairs = window["original_record_count"], window["boundary_excluded_record_count"], window["boundary_excluded_pair_count"]
    if (type(excluded) is not int or type(pairs) is not int or excluded not in (0, 1) or pairs != excluded
            or count is not None and (type(count) is not int or count < 2)
            or window["disposition"] != ("unverified" if count is None else "verified_terminal_exclusion" if excluded else "verified_in_frame")
            or count is None and excluded != 0):
        raise BackupError("invalid_recorded_cadence")
    segment = value["segment_running_metrics"]
    for name in ("session_start_time_utc", "session_end_time_utc", "session_summary_timestamp_utc"):
        if group[name] is not None and group[name] != segment[name]:
            raise BackupError("invalid_recorded_cadence")
    if group["status"] != "available":
        if (group["status"] != "unavailable" or not isinstance(group["reason"], str) or not group["reason"] or group["items"]):
            raise BackupError("invalid_recorded_cadence")
        return
    original = ({**value["recorded_running_metrics"]["kilometer_splits"], "distance_basis": basis} if kilometers
                else _recovery_cadence_references(value) if recovery else segment[group_name])
    if (block["association"] != "matched" or group["reason"] is not None or original["status"] != "available"
            or original["distance_basis"] != basis or len(group["items"]) != (len(original["items"]) if work or recovery or kilometers else 2)
            or (work or recovery or kilometers) and not group["items"] or window != segment["source_window"]
            or any(group[name] is None or group[name] != segment[name]
                   for name in ("session_start_time_utc", "session_end_time_utc"))):
        raise BackupError("invalid_recorded_cadence")
    if references:
        from .coach_segment_metrics import _epoch, _frame

        structure = value["workout_structure"]
        selected = [row for row in structure["executed_laps"] if row["section"] == "main"]
        if (structure["has_structured_workout"] is not True or not selected
                or group["source_laps"] != [row["lap"] for row in selected]
                or not isinstance(group["source_windows"], list) or len(group["source_windows"]) != len(selected)):
            raise BackupError("invalid_recorded_cadence")
        for row, source in zip(group["source_windows"], selected, strict=True):
            _exact(row, {"lap", "workout_step_index", "start_time_utc", "end_time_utc", "distance_m"})
            frame = _frame({"start_time": source["start_time"], "total_elapsed_time": source["elapsed_seconds"]})
            if (frame is None or type(row["lap"]) is not int or row["lap"] != source["lap"]
                    or type(row["workout_step_index"]) is not int or row["workout_step_index"] != source["workout_step_index"]
                    or type(row["distance_m"]) not in (int, float) or row["distance_m"] != source["distance_m"]
                    or _epoch(row["start_time_utc"]) != frame["start"] or _epoch(row["end_time_utc"]) != frame["end"]):
                raise BackupError("invalid_recorded_cadence")
    elif group_name == "main_distance_halves" and value["workout_structure"]["has_structured_workout"] is not False:
        raise BackupError("invalid_recorded_cadence")
    identity = ({"bout" if recovery else "interval", "source_laps", "workout_step_index", "distance_m", "start_time_utc", "end_time_utc"}
                if work or recovery else {"split" if kilometers else "segment", "start_distance_m", "end_distance_m", "distance_m"})
    for row, source in zip(group["items"], original["items"], strict=True):
        _exact(row, identity | {"average_cadence"})
        if _json_bytes({name: row[name] for name in identity}) != _json_bytes({name: source[name] for name in identity}):
            raise BackupError("invalid_recorded_cadence")
        metric = row["average_cadence"]
        _exact(metric, {"status", "value", "unit", "source_fields", "coverage"})
        if metric["unit"] != "steps_per_minute" or metric["source_fields"] != ["cadence", "fractional_cadence"]:
            raise BackupError("invalid_recorded_cadence")
        coverage = metric["coverage"]
        _exact(coverage, {"valid_seconds", "eligible_seconds", "excluded_gap_seconds", "contributing_intervals", "status", "boundary_excluded_seconds"})
        times = [coverage[name] for name in ("valid_seconds", "eligible_seconds", "excluded_gap_seconds", "boundary_excluded_seconds")]
        contributions = coverage["contributing_intervals"]
        reference = None if recovery else source["average_power"]["coverage"]
        if (any(type(number) not in (int, float) or not math.isfinite(number) or number < 0 for number in times)
                or type(contributions) is not int or contributions < 0 or count is None or contributions > count - 1 - excluded
                or coverage["valid_seconds"] > coverage["eligible_seconds"] + 0.001
                or not recovery and not kilometers and any(coverage[name] != reference[name] for name in ("eligible_seconds", "excluded_gap_seconds", "boundary_excluded_seconds"))
                or recovery and (sum(coverage[name] for name in ("eligible_seconds", "excluded_gap_seconds", "boundary_excluded_seconds"))
                                 > source["elapsed_seconds"] + 0.001
                                 or contributions > count - 1 - excluded
                                 or not excluded and coverage["boundary_excluded_seconds"] != 0)
                or coverage["status"] not in ("none", "partial", "complete")
                or (coverage["status"] == "none") != (contributions == 0)
                or contributions == 0 and coverage["valid_seconds"] != 0
                or coverage["status"] == "complete" and (coverage["excluded_gap_seconds"] or coverage["boundary_excluded_seconds"]
                    or abs(coverage["valid_seconds"] - coverage["eligible_seconds"]) > 0.001)):
            raise BackupError("invalid_recorded_cadence")
        if kilometers and (
                contributions > count - 1 - excluded
                or not excluded and (coverage["boundary_excluded_seconds"] != 0
                    or any(coverage[name] != reference[name] for name in ("eligible_seconds", "excluded_gap_seconds")))
                # Legacy km coverage includes the entire terminal pair; strict
                # elapsed allocation may only remove time, never borrow it.
                or excluded and sum(times[1:]) > reference["eligible_seconds"] + reference["excluded_gap_seconds"] + 0.001):
            raise BackupError("invalid_recorded_cadence")
        if metric["status"] == "available":
            number = metric["value"]
            if type(number) not in (int, float) or not math.isfinite(number) or not 0 <= number <= 511.9688 or not contributions:
                raise BackupError("invalid_recorded_cadence")
        elif metric["status"] not in ("missing", "invalid") or metric["value"] is not None or contributions:
            raise BackupError("invalid_recorded_cadence")


def _workout_targets_shape(value):
    """Validate explicit stored typed units; never expand unknown target codes."""
    block = value["workout_targets"]
    _exact(block, {"schema_version", "method", "activity_id", "source_fit_sha256", "status", "reason", "items"})
    if (type(block["schema_version"]) is not int or block["schema_version"] != 1
            or block["method"] != "fit_active_typed_workout_fields" or block["activity_id"] != value["activity_id"]
            or not isinstance(block["items"], list)):
        raise BackupError("invalid_workout_targets")
    if block["status"] == "unavailable":
        reason = block["reason"]
        if (not isinstance(reason, str) or reason not in ("source_unavailable", "no_planned_steps", "step_identity_unverified")
                or block["items"] or reason == "source_unavailable" and block["source_fit_sha256"] is not None
                or reason != "source_unavailable" and block["source_fit_sha256"] != value["source"]["fit_sha256"]):
            raise BackupError("invalid_workout_targets")
        return
    planned = value["workout_structure"]["planned_steps"]
    if (block["status"] != "available" or block["reason"] is not None or not block["items"]
            or len(block["items"]) != len(planned) or value["recorded_running_metrics"]["association"] != "matched"
            or not block["source_fit_sha256"] or block["source_fit_sha256"] != value["source"]["fit_sha256"]):
        raise BackupError("invalid_workout_targets")

    def number(item):
        return type(item) in (int, float) and math.isfinite(item) and item >= 0

    def raw(item):
        return item is None or type(item) is int and 0 <= item < 4294967295 or isinstance(item, str) and len(item) <= 80

    seen = set()
    for row, original in zip(block["items"], planned, strict=True):
        _exact(row, {"step_index", "duration", "target"})
        step = row["step_index"]
        if type(step) is not int or not 0 <= step < 65535 or step in seen or step != original["step_index"]:
            raise BackupError("invalid_workout_targets")
        seen.add(step)
        duration = row["duration"]
        _exact(duration, {"status", "type", "value", "unit", "source_field", "reason"})
        kind = duration["type"]
        if kind != original["duration_type"] or kind is not None and not isinstance(kind, str):
            raise BackupError("invalid_workout_targets")
        expected = {"time": ("duration_time", "s"), "repetition_time": ("duration_time", "s"),
                    "distance": ("duration_distance", "m")}.get(kind, (None, None))
        if (duration["source_field"], duration["unit"]) != expected:
            raise BackupError("invalid_workout_targets")
        if duration["status"] == "available":
            if expected[0] is None or not number(duration["value"]) or duration["reason"] is not None:
                raise BackupError("invalid_workout_targets")
        elif (duration["status"] != "unavailable" or duration["value"] is not None
              or duration["reason"] != ("active_duration_missing_or_invalid" if expected[0] else "duration_unit_unverified")):
            raise BackupError("invalid_workout_targets")
        target = row["target"]
        if not isinstance(target, dict):
            raise BackupError("invalid_workout_targets")
        kind = target.get("type")
        extra = {"unverified_source_values"} if kind not in ("open", "speed") else set()
        _exact(target, {"type", "mode", "raw_value", "range"} | extra)
        if kind != original["target_type"] or kind is not None and not isinstance(kind, str) or not raw(target["raw_value"]):
            raise BackupError("invalid_workout_targets")
        if target["raw_value"] is not None and target["raw_value"] != original["target_value"]:
            raise BackupError("invalid_workout_targets")
        span = target["range"]
        _exact(span, {"status", "low", "high", "unit", "source_fields", "reason"})
        if kind == "speed":
            mode = target["mode"]
            if (mode not in ("custom", "zone", "unverified") or span["unit"] != "m/s"
                    or span["source_fields"] != ["custom_target_speed_low", "custom_target_speed_high"]
                    or mode == "custom" and target["raw_value"] != 0
                    or mode == "zone" and (type(target["raw_value"]) is not int or not 0 < target["raw_value"] < 65535)):
                raise BackupError("invalid_workout_targets")
            if span["status"] == "available":
                if (mode != "custom" or not number(span["low"]) or not number(span["high"])
                        or span["low"] > span["high"] or span["reason"] is not None):
                    raise BackupError("invalid_workout_targets")
            elif (span["status"] != "unavailable" or span["low"] is not None or span["high"] is not None
                  or span["reason"] != {"custom": "custom_range_missing_or_invalid", "zone": "zone_not_expanded",
                                        "unverified": "active_target_unverified"}[mode]):
                raise BackupError("invalid_workout_targets")
        else:
            reason = "open_target" if kind == "open" else "target_unit_unverified"
            if (target["mode"] != ("open" if kind == "open" else "unverified")
                    or span != {"status": "unavailable", "low": None, "high": None, "unit": None,
                                "source_fields": [], "reason": reason}):
                raise BackupError("invalid_workout_targets")
            if extra:
                values = target["unverified_source_values"]
                names = {"custom_target_value_low", "custom_target_value_high", "custom_target_heart_rate_low",
                         "custom_target_heart_rate_high", "custom_target_power_low", "custom_target_power_high",
                         "custom_target_cadence_low", "custom_target_cadence_high"}
                if not isinstance(values, dict) or not set(values) <= names or not all(raw(item) for item in values.values()):
                    raise BackupError("invalid_workout_targets")


def _validate_value(key: str, value: Any):
    kind, parts = key_info(key)
    if kind == "context" and isinstance(value, dict) and type(value.get("schema_version")) is int and value["schema_version"] == 2:
        from .gct_sides import valid_declaration

        _exact(value, _CONTEXT_FIELDS | {"gct_balance_side", "activity_date", "date_source"})
        legacy = {name: item for name, item in value.items() if name in _CONTEXT_FIELDS}
        _validate_value(key, {**legacy, "schema_version": 1})
        _date(value["activity_date"])
        if (value["date_source"] not in ("garmin_local", "utc_fallback")
                or not valid_declaration(value["gct_balance_side"])):
            raise BackupError("invalid_gct_declaration")
        return
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") == "1.9.0":
        legacy = {name: item for name, item in value.items() if name not in ("logical_bouts", "recorded_cadence", "workout_targets")}
        sides = value.get("ground_contact_sides")
        derived_sides = isinstance(sides, dict) and type(sides.get("schema_version")) is int and sides["schema_version"] == 2
        if derived_sides:
            # Validate the unchanged native subset with the old strict contract.
            # This is only a validation view; stored bytes and old schemas stay untouched.
            legacy["ground_contact_sides"] = {
                **{name: item for name, item in sides.items() if name != "record_aggregates"}, "schema_version": 1,
            }
        _validate_value(key, {**legacy, "analyzer_version": "1.8.0"})
        _logical_bouts_shape(value)
        if "recorded_cadence" in value:
            _recorded_cadence_shape(value)
        if "workout_targets" in value:
            _workout_targets_shape(value)
        if derived_sides:
            from .gct_sides import ground_contact_sides

            expected = ground_contact_sides(value["recorded_running_metrics"], value["user_context"],
                                            segments=value["segment_running_metrics"])
            if _json_bytes(sides) != _json_bytes(expected):
                raise BackupError("invalid_gct_sides")
        return
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") == "1.8.0":
        legacy = {name: item for name, item in value.items() if name != "native_lap_workout_binding"}
        _validate_value(key, {**legacy, "analyzer_version": "1.7.0"})
        _native_lap_binding_shape(value)
        return
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") == "1.7.0":
        from .coach_segment_metrics import validate_partial_segment_metrics

        legacy = {name: item for name, item in value.items() if name != "segment_running_metrics"}
        _validate_value(key, {**legacy, "analyzer_version": "1.5.0"})
        try:
            validate_partial_segment_metrics(value.get("segment_running_metrics"), value)
        except (KeyError, TypeError, ValueError, OverflowError, StopIteration) as exc:
            raise BackupError("invalid_segment_running_metrics") from exc
        return
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") == "1.6.0":
        from .coach_segment_metrics import validate_segment_metrics

        legacy = {name: item for name, item in value.items() if name != "segment_running_metrics"}
        _validate_value(key, {**legacy, "analyzer_version": "1.5.0"})
        try:
            validate_segment_metrics(value.get("segment_running_metrics"), value)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise BackupError("invalid_segment_running_metrics") from exc
        return
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") == "1.5.0":
        from .gct_sides import ground_contact_sides

        legacy = {name: item for name, item in value.items() if name != "ground_contact_sides"}
        # Reuse the complete 1.4 shape without broadening historical allowlists.
        _validate_value(key, {**legacy, "analyzer_version": "1.4.0"})
        expected = ground_contact_sides(value["recorded_running_metrics"], value["user_context"])
        if _json_bytes(value.get("ground_contact_sides")) != _json_bytes(expected):
            raise BackupError("invalid_gct_sides")
        return
    fields = {"profile": _PROFILE_FIELDS, "context": _CONTEXT_FIELDS, "analysis": _ANALYSIS_FIELDS}[
        kind
    ]
    if kind == "analysis" and isinstance(value, dict) and value.get("analyzer_version") in ("1.1.0", "1.2.0", "1.3.0", "1.4.0"):
        fields = fields | {"recorded_running_metrics"}
        if value["analyzer_version"] == "1.3.0":
            fields = fields | {"stored_context_snapshot"}
        if value["analyzer_version"] == "1.4.0":
            fields = fields | {"training_effect"}
            if "stored_context_snapshot" in value:
                fields = fields | {"stored_context_snapshot"}
    _exact(value, fields)
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise BackupError("unsupported_object_version")
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            if any(name.lower() in _FORBIDDEN_FIELDS for name in item):
                raise BackupError("excluded_object_field")
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    if kind == "profile":
        _profile_shape(value)
        _date(value["effective_from"])
        _timestamp(value["created_at"])
        if value["profile_id"] != parts[1] or value["effective_from"] != parts[0]:
            raise BackupError("profile_reference_mismatch")
        if value["source"] != "user":
            raise BackupError("invalid_profile_source")
        try:
            validate_profile(value)
        except (ValueError, TypeError, OverflowError):
            raise BackupError("invalid_profile") from None
    elif kind == "context":
        if value["activity_id"] != parts[1] or value["context_id"] != parts[3]:
            raise BackupError("context_reference_mismatch")
        _timestamp(value["created_at"])
        stamp = re.sub(r"[-:.Z]", "", value["created_at"])
        if stamp != parts[2] or value["source"] != "user":
            raise BackupError("invalid_context_source")
        rpe = value["rpe"]
        if rpe is not None and (type(rpe) not in (int, float) or not 0 <= rpe <= 10):
            raise BackupError("invalid_context_rpe")
        for name, maximum in (("conditions", 240), ("note", 2000), ("workout_correction", 1000)):
            text = value[name]
            if text is not None and (not isinstance(text, str) or not 1 <= len(text) <= maximum):
                raise BackupError("invalid_context_text")
    else:
        _analysis_shape(value)
        if value["analysis_id"] != parts[2] or value["activity_id"] != parts[1]:
            raise BackupError("analysis_reference_mismatch")
        activity = value["activity"]
        if not isinstance(activity, dict):
            raise BackupError("invalid_analysis_activity")
        _date(activity.get("date"))
        if activity["date"][:4] != parts[0]:
            raise BackupError("analysis_reference_mismatch")
        source = value["source"]
        _exact(source, {"fit_sha256", "tcx_sha256", "derived_from"})
        if source["derived_from"] != [
            "activity.v1.json",
            "activity.endurance.v1.json",
            "activity.tcx",
        ]:
            raise BackupError("invalid_analysis_sources")
        for name in ("fit_sha256", "tcx_sha256"):
            digest = source[name]
            if not (name == "fit_sha256" and digest is None):
                if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                    raise BackupError("invalid_analysis_source_hash")
        if value["analyzer_version"] == "1.1.0":
            _recorded_running_metrics_shape(value["recorded_running_metrics"], value)
        elif value["analyzer_version"] in ("1.2.0", "1.3.0", "1.4.0"):
            _running_segments_shape(value["recorded_running_metrics"], value)
            if value["analyzer_version"] == "1.3.0" or "stored_context_snapshot" in value:
                _stored_context_shape(value["stored_context_snapshot"], value)
            if value["analyzer_version"] == "1.4.0":
                _training_effect_shape(value["training_effect"], value)


def _references(values: dict[str, dict]) -> list[dict]:
    profiles = {}
    contexts = {}
    for key, value in values.items():
        kind, _parts = key_info(key)
        if kind == "profile":
            identifier = value["profile_id"]
            if identifier in profiles:
                raise BackupError("duplicate_profile_id")
            profiles[identifier] = key
        elif kind == "context":
            identifier = (value["activity_id"], value["context_id"])
            if identifier in contexts:
                raise BackupError("duplicate_context_id")
            contexts[identifier] = key
    references = []
    for key, value in sorted(values.items()):
        kind, parts = key_info(key)
        if kind != "analysis":
            continue
        embedded = value["profile"]
        if not isinstance(embedded, dict) or not isinstance(embedded.get("profile_id"), str):
            raise BackupError("invalid_analysis_profile")
        profile_key = profiles.get(embedded["profile_id"])
        if not profile_key:
            raise BackupError("missing_profile_reference")
        expected = validate_profile(values[profile_key])
        if expected["effective_from"] > value["activity"]["date"]:
            raise BackupError("profile_effective_date_mismatch")
        projection = {
            name: expected[name]
            for name in ("profile_id", "name", "effective_from", "zones", "references")
        }
        if embedded != projection:
            raise BackupError("profile_version_mismatch")
        context = value["user_context"]
        context_key = None
        if context is not None:
            if not isinstance(context, dict) or not isinstance(context.get("context_id"), str):
                raise BackupError("invalid_analysis_context")
            context_key = contexts.get((value["activity_id"], context["context_id"]))
            if not context_key or values[context_key] != context:
                raise BackupError("context_version_mismatch")
        base = f"activities/{parts[0]}/{parts[1]}/"
        references.append(
            {
                "analysis": key,
                "profile": profile_key,
                "context": context_key,
                "external_sources": [base + name for name in value["source"]["derived_from"]],
                "source_hashes": {
                    name: value["source"][name] for name in ("fit_sha256", "tcx_sha256")
                },
                "ready": False,
                "reason": "external_sources_not_backed_up",
            }
        )
    return references


@dataclass
class VerifiedBackup:
    manifest: dict
    objects: dict[str, bytes]

    def summary(self) -> dict:
        return {
            "format_version": VERSION,
            "objects": len(self.objects),
            "stored_bytes": sum(len(data) for data in self.objects.values()),
            "decoded_bytes": self.manifest["decoded_bytes"],
            "historical_analyses": len(self.manifest["references"]),
            "ready_analyses": 0,
            "complete_for_declared_scope": True,
        }


def _validate_document(document, limits: Limits) -> VerifiedBackup:
    _exact(
        document,
        {
            "format",
            "version",
            "created_at",
            "scope",
            "complete",
            "objects",
            "stored_bytes",
            "decoded_bytes",
            "references",
        },
    )
    if (
        document["format"] != FORMAT
        or type(document["version"]) is not int
        or document["version"] != VERSION
    ):
        raise BackupError("unsupported_backup_version")
    if document["complete"] is not True:
        raise BackupError("incomplete_backup")
    _timestamp(document["created_at"])
    scope = _scope(document["scope"], limits)
    entries = document["objects"]
    if not isinstance(entries, list) or len(entries) > limits.objects:
        raise BackupError("object_count_limit")
    objects = {}
    values = {}
    stored_total = decoded_total = 0
    node_budget = [limits.json_nodes]
    names = set()
    for entry in entries:
        _exact(entry, {"key", "type", "revision", "size", "decoded_size", "sha256", "data"})
        key = entry["key"]
        kind = allowed_key(key, scope)
        if key.casefold() in names or entry["type"] != kind:
            raise BackupError("duplicate_or_invalid_object_type")
        names.add(key.casefold())
        if (
            type(entry["size"]) is not int
            or not 1 <= entry["size"] <= limits.stored_object
            or type(entry["decoded_size"]) is not int
            or not 1 <= entry["decoded_size"] <= limits.decoded_object
            or not isinstance(entry["revision"], str)
            or not 1 <= len(entry["revision"]) <= 256
            or not isinstance(entry["data"], str)
            or len(entry["data"]) != 4 * ((entry["size"] + 2) // 3)
        ):
            raise BackupError("invalid_object_metadata")
        stored_total += entry["size"]
        decoded_total += entry["decoded_size"]
        if stored_total > limits.stored_total or decoded_total > limits.decoded_total:
            raise BackupError("total_size_limit")
        try:
            data = base64.b64decode(entry["data"], validate=True)
        except (ValueError, binascii.Error):
            raise BackupError("invalid_object_encoding") from None
        if len(data) != entry["size"] or _digest(data) != entry["sha256"]:
            raise BackupError("object_checksum_mismatch")
        decoded = _decode(data, limits)
        if len(decoded) != entry["decoded_size"]:
            raise BackupError("decoded_size_mismatch")
        value = _loads(decoded, limits.decoded_object, limits, node_budget)
        _validate_value(key, value)
        objects[key] = data
        values[key] = value
    if (
        type(document["stored_bytes"]) is not int
        or document["stored_bytes"] != stored_total
        or type(document["decoded_bytes"]) is not int
        or document["decoded_bytes"] != decoded_total
        or document["references"] != _references(values)
    ):
        raise BackupError("manifest_reference_or_size_mismatch")
    return VerifiedBackup(document, objects)


def _private_destination(destination: Path) -> Path:
    destination = safe_path(destination)
    for parent in (destination.parent, *destination.parent.parents):
        if (parent / ".git").exists():
            raise BackupError("private_directory_must_be_outside_git")
    if destination.exists():
        raise BackupError("destination_exists")
    if not destination.parent.is_dir():
        raise BackupError("destination_parent_required")
    return destination


def _publish(destination: Path, data: bytes):
    destination = _private_destination(destination)
    descriptor, temporary = tempfile.mkstemp(prefix=".slbk-part-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # Same-filesystem exclusive publication: never overwrite another backup.
        os.link(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


def create_backup(
    source: ReadOnlySource,
    scope: Scope,
    destination: Path,
    password: bytes,
    *,
    limits: Limits = DEFAULT_LIMITS,
    retries: int = 0,
) -> dict:
    scope.validate(limits)
    if not isinstance(password, bytes) or not 12 <= len(password) <= 1024:
        raise BackupError("password_length_invalid")
    if type(retries) is not int or not 0 <= retries <= MAX_RETRIES:
        raise BackupError("invalid_retry_limit")
    destination = _private_destination(destination)
    reads = inventory_passes = 0
    read_bytes = 0
    for attempt in range(retries + 1):
        try:
            inventory_passes += 1
            before = _snapshot(source, scope, limits)
            entries = []
            values = {}
            decoded_total = 0
            node_budget = [limits.json_nodes]
            for item in before:
                reads += 1
                data = source.read(item, limits.stored_object)
                read_bytes += len(data)
                if len(data) != item.size:
                    raise SourceChanged("source_changed")
                decoded = _decode(data, limits)
                decoded_total += len(decoded)
                if decoded_total > limits.decoded_total:
                    raise BackupError("decoded_total_limit")
                value = _loads(decoded, limits.decoded_object, limits, node_budget)
                _validate_value(item.key, value)
                values[item.key] = value
                entries.append(
                    {
                        "key": item.key,
                        "type": allowed_key(item.key, scope),
                        "revision": item.revision,
                        "size": len(data),
                        "decoded_size": len(decoded),
                        "sha256": _digest(data),
                        "data": base64.b64encode(data).decode("ascii"),
                    }
                )
            inventory_passes += 1
            if _snapshot(source, scope, limits) != before:
                raise SourceChanged("source_changed")
            document = {
                "format": FORMAT,
                "version": VERSION,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "scope": _scope_data(scope),
                "complete": True,
                "objects": entries,
                "stored_bytes": sum(item.size for item in before),
                "decoded_bytes": decoded_total,
                "references": _references(values),
            }
            verified = _validate_document(document, limits)
            encrypted = _seal(document, password, limits)
            _publish(destination, encrypted)
            return {
                **verified.summary(),
                "archive_bytes": len(encrypted),
                "attempts": attempt + 1,
                "object_reads": reads,
                "source_read_bytes": getattr(source, "read_bytes", read_bytes),
                "inventory_passes": inventory_passes,
                "storage_operations": dict(
                    getattr(
                        source,
                        "operations",
                        {"get": reads, "inventory": inventory_passes, "put": 0},
                    )
                ),
            }
        except SourceChanged:
            if attempt == retries:
                raise SourceChanged("source_not_consistent") from None
    raise BackupError("source_not_consistent")


def verify_backup(
    archive: Path, password: bytes, *, limits: Limits = DEFAULT_LIMITS
) -> VerifiedBackup:
    archive = safe_file(archive)
    with archive.open("rb") as stream:
        data = stream.read(limits.archive + 1)
    if len(data) > limits.archive:
        raise BackupError("archive_size_limit")
    if not data.startswith(MAGIC) or len(data) <= len(MAGIC) + 16:
        raise BackupError("invalid_backup_header")
    salt = data[len(MAGIC) : len(MAGIC) + 16]
    token = data[len(MAGIC) + 16 :]
    # Reject trailing garbage and noncanonical base64 that Fernet's decoder accepts.
    try:
        if (
            base64.urlsafe_b64encode(base64.b64decode(token, altchars=b"-_", validate=True))
            != token
        ):
            raise ValueError
        plaintext = _fernet(password, salt).decrypt(token)
    except (InvalidToken, ValueError, binascii.Error):
        raise BackupError("backup_authentication_failed") from None
    return _validate_document(_loads(plaintext, limits.document, limits), limits)


def restore_plan(
    archive: Path, password: bytes, target: Path, *, limits: Limits = DEFAULT_LIMITS
) -> dict:
    verified = verify_backup(archive, password, limits=limits)
    target = _private_destination(target)
    return {
        **verified.summary(),
        "mode": "isolated_local_only",
        "would_write_objects": len(verified.objects),
        "would_write_bytes": verified.summary()["stored_bytes"],
        "derived_pointers_created": 0,
        "production_restore_supported": False,
    }


def restore_local(
    archive: Path, password: bytes, target: Path, *, limits: Limits = DEFAULT_LIMITS
) -> dict:
    # Authenticate and validate everything before creating the explicitly chosen directory.
    verified = verify_backup(archive, password, limits=limits)
    target = _private_destination(target)
    target.mkdir(mode=0o700, exist_ok=False)
    incomplete = target / "RESTORE-INCOMPLETE"
    incomplete.write_bytes(b"isolated-local-restore\n")
    for key, data in verified.objects.items():
        path = safe_path(target / "objects" / key)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(data)
        with safe_file(path).open("rb") as stream:
            restored = stream.read(limits.stored_object + 1)
        if len(restored) != len(data) or _digest(restored) != _digest(data):
            raise BackupError("local_restore_checksum_failed")
    # Private manifest; no object payload duplication, no live pointers or indexes.
    manifest = {
        **verified.manifest,
        "objects": [
            {name: value for name, value in item.items() if name != "data"}
            for item in verified.manifest["objects"]
        ],
    }
    with (target / "manifest.private.json").open("xb") as stream:
        stream.write(_json_bytes(manifest))
    with (target / "RESTORE-COMPLETE").open("xb") as stream:
        stream.write(b"historical-only; no sources or readiness pointers\n")
    incomplete.unlink()
    return {**verified.summary(), "mode": "isolated_local_only", "derived_pointers_created": 0}


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        # argparse's usual errors may echo private paths or accidental passwords.
        self.print_usage(sys.stderr)
        self.exit(2, "backup_error: invalid_arguments\n")


def main(argv=None) -> int:
    parser = _Parser(description="Private, bounded local backup; interactive password only.")
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_Parser)
    create = commands.add_parser("create")
    create.add_argument("--source", type=Path, required=True)
    create.add_argument("--scope", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--retries", type=int, choices=(0, 1), default=0)
    for name in ("verify", "plan", "restore-local"):
        command = commands.add_parser(name)
        command.add_argument("--backup", type=Path, required=True)
        if name != "verify":
            command.add_argument("--target", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            password = getpass.getpass("Backup password (12-1024 UTF-8 bytes): ").encode("utf-8")
            if not 12 <= len(password) <= 1024:
                raise BackupError("password_length_invalid")
            if args.command == "create":
                confirmation = getpass.getpass("Repeat password: ").encode("utf-8")
                if password != confirmation:
                    raise BackupError("password_confirmation_failed")
        if args.command == "create":
            with safe_file(args.scope).open("rb") as stream:
                scope = _scope(_loads(stream.read(64 * 1024 + 1), 64 * 1024, Limits()), Limits())
            result = create_backup(
                LocalSource(args.source), scope, args.output, password, retries=args.retries
            )
        elif args.command == "verify":
            result = verify_backup(args.backup, password).summary()
        elif args.command == "plan":
            result = restore_plan(args.backup, password, args.target)
        else:
            result = restore_local(args.backup, password, args.target)
        print(json.dumps(result, sort_keys=True))
        return 0
    except BackupError as exc:
        print(f"backup_error: {exc}", file=sys.stderr)
    except (OSError, EOFError, getpass.GetPassWarning):
        print("backup_error: local_io_or_private_prompt_failed", file=sys.stderr)
    except KeyboardInterrupt:
        print("backup_error: interrupted", file=sys.stderr)
    except Exception:
        print("backup_error: operation_failed", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
