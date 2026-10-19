"""Data-only Training Effect already recorded in a single decoded FIT session.

The neutral source describes the record format, not the device manufacturer.
No fetching, coaching label, rescaling or lap/half aggregation is performed.
"""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

_CLOCK = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?")
FIELDS = (("aerobic", "total_training_effect"), ("anaerobic", "total_anaerobic_training_effect"))


def _clock(value: Any) -> str | None:
    if not isinstance(value, str) or not _CLOCK.fullmatch(value):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat(timespec="milliseconds" if parsed.microsecond else "seconds")


def _utc(value: Any, *, allow_naive: bool = False) -> str | None:
    if isinstance(value, datetime):
        value = value.isoformat()
    if not isinstance(value, str):
        return None
    explicit = value.endswith(("Z", "+00:00"))
    if not explicit and not allow_naive:
        return None
    local = value.removesuffix("Z").removesuffix("+00:00") if explicit else value
    parsed = _clock(local)
    return datetime.fromisoformat(parsed).isoformat(timespec="milliseconds") + "Z" if parsed else None


def _effect(row, field):
    value = row.get(field)
    valid = False
    if type(value) in (int, float):
        try:
            valid = (math.isfinite(value) and 0 <= value <= 5
                     and abs(value * 10 - round(value * 10)) <= 1e-9)
        except OverflowError:
            pass
    return {"status": "available" if valid else "missing" if value is None else "invalid",
            "value": value if valid else None, "source_field": field}


def recorded_training_effect(activity, decoded, endurance):
    """Project only a schema/identity/clock-associated single session estimate."""
    raw_id = str(activity.get("id") or "").removeprefix("garmin-")
    result = {
        "schema_version": 1, "source": "fit_session", "method": "fit_recorded_estimate", "scale": "0_to_5",
        "activity_id": raw_id, "association": "source_missing",
        "local_start_time": None, "local_date": None, "session_start_time_utc": None,
        "measured_at_utc": None, "source_fit_sha256": None,
        **{name: {"status": "unavailable", "value": None, "source_field": field} for name, field in FIELDS},
    }

    def fail(reason):
        result["association"] = reason
        return result

    if decoded is None:
        return result
    if (not isinstance(decoded, dict) or type(decoded.get("schema_version")) not in (int, float)
            or decoded["schema_version"] != 1 or decoded.get("source") != "garmin-fit"
            or not isinstance(decoded.get("decode_errors"), list)):
        return fail("invalid_schema")
    if decoded["decode_errors"]:
        return fail("decode_errors")
    identity, tcx_identity = decoded.get("activity"), endurance.get("activity")
    if (not re.fullmatch(r"[0-9]{1,20}", raw_id) or not isinstance(identity, dict)
            or identity.get("id") != raw_id or isinstance(tcx_identity, dict)
            and tcx_identity.get("id") is not None
            and str(tcx_identity["id"]).removeprefix("garmin-") != raw_id):
        return fail("id_mismatch")
    source_start = _utc(identity.get("start_time_gmt"), allow_naive=True)
    if source_start is None:
        return fail("time_mismatch")
    local = identity.get("start_time_local")
    if local not in (None, ""):
        result["local_start_time"] = _clock(local)
        if result["local_start_time"] is None:
            return fail("time_mismatch")
        result["local_date"] = result["local_start_time"][:10]
        if result["local_date"] != activity.get("date"):
            return fail("time_mismatch")
    digest = decoded.get("source_fit_sha256")
    if isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest):
        result["source_fit_sha256"] = digest
    messages = decoded.get("messages")
    if messages is not None and not isinstance(messages, dict):
        return fail("invalid_schema")
    sessions = messages.get("session_mesgs") if messages else None
    if sessions is None or isinstance(sessions, list) and not sessions:
        return fail("sessions_missing")
    if not isinstance(sessions, list):
        return fail("invalid_schema")
    if len(sessions) != 1:
        return fail("sessions_ambiguous")
    session = sessions[0]
    if not isinstance(session, dict):
        return fail("invalid_schema")
    start = _utc(session.get("start_time"))
    if start is None or start != source_start:
        return fail("time_mismatch")
    result["association"] = "matched"
    result["session_start_time_utc"] = start
    measured = _utc(session.get("timestamp"))
    result["measured_at_utc"] = measured if measured and measured >= start else None
    result.update({name: _effect(session, field) for name, field in FIELDS})
    return result
