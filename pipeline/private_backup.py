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

MAGIC = b"SLIPSTREAM-BACKUP-1\n"
FORMAT = "slipstream-user-backup"
VERSION = 1
MAX_RETRIES = 1


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
    if value["distance_halves"] is not None:
        _record(value["distance_halves"], {"first": (dict,), "second": (dict,)})
        for half in value["distance_halves"].values():
            _record(half, _RANGE_FIELDS)
    _records(value["kilometer_splits"], {**_RANGE_FIELDS, "split": (int,), "partial": (bool,)})
    if not isinstance(value["limitations"], list) or any(
        not isinstance(item, str) for item in value["limitations"]
    ):
        raise BackupError("invalid_analysis_limitations")


def _validate_value(key: str, value: Any):
    kind, parts = key_info(key)
    fields = {"profile": _PROFILE_FIELDS, "context": _CONTEXT_FIELDS, "analysis": _ANALYSIS_FIELDS}[
        kind
    ]
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
