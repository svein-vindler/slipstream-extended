"""Active decoded FIT fields, never estimated goals or workout completion."""
from .native_lap_workout_binding import _index
from .recorded_running_metrics import _finite_number

SCHEMA_VERSION = 1


def _raw(value):
    if type(value) is int and 0 <= value < 4294967295:
        return value
    return value if isinstance(value, str) and len(value) <= 80 else None


def _duration(row):
    kind = row.get("duration_type")
    field, unit = {"time": ("duration_time", "s"), "repetition_time": ("duration_time", "s"),
                   "distance": ("duration_distance", "m")}.get(kind, (None, None)) if isinstance(kind, str) else (None, None)
    value = _finite_number(row.get(field)) if field else None
    valid = value is not None and value >= 0
    return {"status": "available" if valid else "unavailable", "type": kind if isinstance(kind, str) else None,
            "value": value if valid else None, "unit": unit, "source_field": field,
            "reason": None if valid else "active_duration_missing_or_invalid" if field else "duration_unit_unverified"}


def _target(row):
    kind, raw = row.get("target_type"), _raw(row.get("target_value"))
    result = {"type": kind if isinstance(kind, str) else None, "mode": "unverified", "raw_value": raw,
              "range": {"status": "unavailable", "low": None, "high": None, "unit": None,
                        "source_fields": [], "reason": "target_unit_unverified"}}
    if kind == "open":
        result["mode"], result["range"]["reason"] = "open", "open_target"
    elif kind == "speed":
        fields = ["custom_target_speed_low", "custom_target_speed_high"]
        low, high = (_finite_number(row.get(field)) for field in fields)
        zone = _index(raw)
        verified = zone is not None and type(row.get("target_speed_zone")) is int and row["target_speed_zone"] == zone
        custom = verified and zone == 0
        available = custom and low is not None and high is not None and 0 <= low <= high
        result["mode"] = "custom" if custom else "zone" if verified else "unverified"
        result["range"] = {"status": "available" if available else "unavailable",
                           "low": low if available else None, "high": high if available else None,
                           "unit": "m/s", "source_fields": fields,
                           "reason": None if available else "custom_range_missing_or_invalid" if custom
                           else "zone_not_expanded" if verified else "active_target_unverified"}
    else:
        # Preserve codes, not a guessed bpm/watts/%/steps target. Typed mixed
        # units and offset sentinels require their own verified interpretation.
        names = ("custom_target_value_low", "custom_target_value_high", "custom_target_heart_rate_low",
                 "custom_target_heart_rate_high", "custom_target_power_low", "custom_target_power_high",
                 "custom_target_cadence_low", "custom_target_cadence_high")
        result["unverified_source_values"] = {name: _raw(row[name]) for name in names if name in row}
    return result


def workout_targets(recorded, decoded, structure):
    result = {"schema_version": SCHEMA_VERSION, "method": "fit_active_typed_workout_fields",
              "activity_id": recorded["activity_id"], "source_fit_sha256": None,
              "status": "unavailable", "reason": "source_unavailable", "items": []}
    identity = decoded.get("activity")
    if (recorded.get("association") != "matched" or not recorded.get("source_fit_sha256")
            or decoded.get("decode_errors") != [] or decoded.get("source_fit_sha256") != recorded["source_fit_sha256"]
            or not isinstance(identity, dict) or identity.get("id") != recorded["activity_id"]):
        return result
    result["source_fit_sha256"] = recorded["source_fit_sha256"]
    rows = decoded.get("messages", {}).get("workout_step_mesgs")
    planned = structure.get("planned_steps")
    if not isinstance(rows, list) or not rows:
        result["reason"] = "no_planned_steps"
        return result
    seen = set()
    if not isinstance(planned, list) or len(planned) != len(rows):
        result["reason"] = "step_identity_unverified"
        return result
    for row, plan in zip(rows, planned, strict=True):
        step = _index(row.get("message_index")) if isinstance(row, dict) else None
        if step is None or step in seen or plan.get("step_index") != step:
            result["reason"] = "step_identity_unverified"
            return result
        seen.add(step)
    result.update(status="available", reason=None, items=[
        {"step_index": row["message_index"], "duration": _duration(row), "target": _target(row)} for row in rows])
    return result
