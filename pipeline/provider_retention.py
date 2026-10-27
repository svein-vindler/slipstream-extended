"""Preserve good HRV observations when a selected provider returns a partial day."""

from __future__ import annotations

import gzip
import io
import json
import math
from datetime import UTC, datetime

from botocore.exceptions import ClientError

from .r2_store import missing_object

MAX_STORED_BYTES = 512 * 1024
MAX_DECODED_BYTES = 2 * 1024 * 1024
MAX_READINGS = 5000


def _instant(value, *, gmt=False):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            if not gmt:
                return None
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat()
    except ValueError:
        return None


def _identity(point):
    if not isinstance(point, dict):
        raise ValueError("Invalid retained HRV point.")
    value = point.get("hrv_ms")
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError("Invalid retained HRV point.")
    raw = point.get("raw")
    gmt = raw.get("readingTimeGMT") if isinstance(raw, dict) else None
    if gmt is not None:
        instant = _instant(gmt, gmt=True)
        if instant is None:
            raise ValueError("Invalid retained HRV clock.")
        return "utc", instant
    instant = _instant(point.get("timestamp"))
    if instant is not None:
        return "utc", instant
    timestamp = point.get("timestamp")
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float, str)):
        raise ValueError("Invalid retained HRV clock.")
    # A naive clock with no GMT provenance cannot acquire a later provider's UTC.
    return "unverified", json.dumps(timestamp, ensure_ascii=True)


def _keep_fields(previous, incoming):
    if isinstance(previous, dict) and isinstance(incoming, dict):
        return {
            key: _keep_fields(previous.get(key), incoming[key])
            if key in incoming
            else previous[key]
            for key in previous.keys() | incoming.keys()
        }
    return previous if incoming is None and previous is not None else incoming


def preserve_provider_hrv(store, key, day, payload, provider):
    if provider not in {"legacy", "personal"}:
        raise ValueError("Unknown HRV provider.")
    try:
        stored = store.get_bounded(key, MAX_STORED_BYTES)
    except ClientError as error:
        if not missing_object(error):
            raise
        stored = None
    previous = None
    if stored is not None:
        if not isinstance(stored, bytes) or len(stored) > MAX_STORED_BYTES:
            raise ValueError("Retained HRV object exceeds its limit.")
        try:
            if stored[:2] == b"\x1f\x8b":
                with gzip.GzipFile(fileobj=io.BytesIO(stored)) as stream:
                    decoded = stream.read(MAX_DECODED_BYTES + 1)
            else:
                decoded = stored
            if len(decoded) > MAX_DECODED_BYTES:
                raise ValueError("Retained HRV decode exceeds its limit.")
            previous = json.loads(decoded)
        except (OSError, UnicodeError, ValueError, RecursionError):
            raise ValueError("Retained HRV object cannot be verified.") from None
        if (
            not isinstance(previous, dict)
            or previous.get("source") != "garmin"
            or previous.get("date") != day
            or not isinstance(previous.get("readings"), list)
            or previous.get("reading_count") != len(previous["readings"])
        ):
            raise ValueError("Retained HRV schema cannot be verified.")
    prior = previous["readings"] if previous is not None else []
    incoming = payload["readings"]
    if len(prior) + len(incoming) > MAX_READINGS:
        raise ValueError("HRV observation limit exceeded.")
    observations = {}
    for point in prior:
        identity = _identity(point)
        if identity in observations:
            raise ValueError("Retained HRV clock is ambiguous.")
        observations[identity] = {
            **point,
            "source_provider": point.get("source_provider", "unknown"),
        }
    incoming_keys = set()
    for point in incoming:
        identity = _identity(point)
        if identity in incoming_keys:
            raise ValueError("Incoming HRV clock is ambiguous.")
        incoming_keys.add(identity)
        observations[identity] = {**point, "source_provider": provider}
    readings = [observations[identity] for identity in sorted(observations)]
    result = _keep_fields(previous or {}, payload)
    # Prior consumer composition counts do not describe this newly merged curve.
    result.pop("source_composition", None)
    result.update(
        readings=readings,
        reading_count=len(readings),
        source_provider=provider,
        provider_composition={
            "method_version": "preserve-provider-observations-v1",
            "retained_reading_count": len(set(observations) - incoming_keys),
            "source_providers": sorted({point["source_provider"] for point in readings}),
            "completeness": "unknown",
        },
    )
    flags = set(result.get("quality_flags", []))
    flags.add("source_completeness_unknown")
    retained = len(set(observations) - incoming_keys)
    if retained:
        flags.update({"partial_source", "retained_previous_observations"})
    if len(flags) > 16 or not all(isinstance(flag, str) for flag in flags):
        raise ValueError("Retained HRV quality metadata exceeds its limit.")
    result["quality_flags"] = sorted(flags)
    encoded = json.dumps(result, sort_keys=True, allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_DECODED_BYTES:
        raise ValueError("HRV result exceeds its limit.")
    return json.loads(encoded)
