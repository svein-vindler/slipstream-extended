"""Pure projection of five already-scaled, recorded FIT session measurements.

Association failure makes only this optional block unavailable. No source fetch,
clock inference, physiological estimate or unit conversion is performed here.
"""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

FIELDS = (
    ("average_power", "avg_power", "W"),
    ("average_step_length", "avg_step_length", "mm"),
    ("average_ground_contact_time", "avg_stance_time", "ms"),
    ("average_vertical_oscillation", "avg_vertical_oscillation", "mm"),
    ("average_vertical_ratio", "avg_vertical_ratio", "%"),
)
_CLOCK = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?")


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
    normalized = _clock(local)
    if normalized is None:
        return None
    return datetime.fromisoformat(normalized).isoformat(timespec="milliseconds") + "Z"


def recorded_running_metrics(
    activity: dict[str, Any], decoded: Any, endurance: dict[str, Any],
) -> dict[str, Any]:
    """Use the caller's local calendar and the FIT's actual UTC session start."""
    raw_id = str(activity.get("id") or "").removeprefix("garmin-")
    result = {
        "schema_version": 1, "source": "garmin_fit_session", "method": "fit_recorded_measurement",
        "activity_id": raw_id, "association": "source_missing",
        "local_start_time": None, "local_date": None, "session_start_time_utc": None,
        "measured_at_utc": None, "source_fit_sha256": None,
        **{name: {"status": "unavailable", "value": None, "unit": unit, "source_field": field}
           for name, field, unit in FIELDS},
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
    identity = decoded.get("activity")
    tcx_identity = endurance.get("activity")
    if (not re.fullmatch(r"[0-9]{1,20}", raw_id) or not isinstance(identity, dict)
            or identity.get("id") != raw_id
            or isinstance(tcx_identity, dict) and tcx_identity.get("id") is not None
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
    activity_sports = [activity.get("type"), identity.get("type")]
    declared_sports = [value for value in [*activity_sports, session.get("sport")]
                       if value is not None and (not isinstance(value, str) or value.strip())]
    if (not any(isinstance(value, str) and "run" in value.lower() for value in activity_sports)
            or any(value is not None and (not isinstance(value, str) or "run" not in value.lower())
                   for value in declared_sports)):
        return fail("unsupported_sport")
    result["association"] = "matched"
    result["session_start_time_utc"] = start
    measured = _utc(session.get("timestamp"))
    result["measured_at_utc"] = measured if measured and measured >= start else None
    for name, source_field, unit in FIELDS:
        value = session.get(source_field)
        status = "missing" if value is None else "invalid"
        if type(value) in (int, float):
            try:
                if math.isfinite(value):
                    status = "available"
            except OverflowError:
                pass
        result[name] = {"status": status, "value": value if status == "available" else None,
                        "unit": unit, "source_field": source_field}
    return result


def _recorded_value(row, field, unit, *, unavailable=False):
    value = row.get(field)
    status = "unavailable" if unavailable else "missing" if value is None else "invalid"
    if not unavailable and type(value) in (int, float):
        try:
            if math.isfinite(value):
                status = "available"
        except OverflowError:
            pass
    metric = {"status": status, "value": value if status == "available" else None,
              "unit": unit, "source_field": field}
    if field in ("avg_stance_time_balance", "stance_time_balance"):
        # FIT's profile documents a percentage, but not its left/right convention.
        metric["side_semantics"] = "unknown"
    return metric


def expanded_recorded_running_metrics(activity, decoded, endurance):
    """Versioned optional measurements; no model-dependent capability inference."""
    result = recorded_running_metrics(activity, decoded, endurance)
    result["schema_version"] = 2
    result["average_ground_contact_balance"] = _recorded_value(
        {}, "avg_stance_time_balance", "%", unavailable=True,
    )
    laps = {"status": "unavailable", "reason": result["association"],
            "method": "fit_recorded_measurement", "items": []}
    result["laps"] = laps
    result["kilometer_splits"] = _distance_group("unavailable", result["association"])
    result["distance_halves"] = _distance_group("unavailable", result["association"])
    if result["association"] != "matched":
        return result
    messages = decoded["messages"]
    session = messages["session_mesgs"][0]
    result["average_ground_contact_balance"] = _recorded_value(
        session, "avg_stance_time_balance", "%",
    )
    result.update(_distance_metrics(decoded, endurance, result))
    rows = messages.get("lap_mesgs")
    if rows is None or rows == []:
        laps.update(status="missing", reason="no_recorded_laps")
        return result
    if not isinstance(rows, list):
        laps.update(status="invalid", reason="invalid_lap_schema")
        return result
    end = result["measured_at_utc"]
    if end is None:
        laps["reason"] = "session_end_unknown"
        return result
    previous_end = result["session_start_time_utc"]
    fields = (*FIELDS, ("average_ground_contact_balance", "avg_stance_time_balance", "%"))
    items = []
    for ordinal, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            laps.update(status="invalid", reason="invalid_lap_schema")
            return result
        start, finish = _utc(row.get("start_time")), _utc(row.get("timestamp"))
        if start is None or finish is None or not previous_end <= start <= finish <= end:
            laps.update(status="invalid", reason="lap_time_mismatch")
            return result
        previous_end = finish
        distance = row.get("total_distance")
        if type(distance) not in (int, float) or not math.isfinite(distance) or distance < 0:
            distance = None
        items.append({"lap": ordinal, "start_time_utc": start, "end_time_utc": finish,
                      "distance_m": distance,
                      **{name: _recorded_value(row, field, unit) for name, field, unit in fields}})
    laps.update(status="available", reason=None, items=items)
    return result


_RECORD_FIELDS = (
    ("average_power", "power", "W"),
    ("average_step_length", "step_length", "mm"),
    ("average_ground_contact_time", "stance_time", "ms"),
    ("average_vertical_oscillation", "vertical_oscillation", "mm"),
    ("average_vertical_ratio", "vertical_ratio", "%"),
    ("average_ground_contact_balance", "stance_time_balance", "%"),
)


def _finite_number(value):
    if type(value) in (int, float):
        try:
            return value if math.isfinite(value) else None
        except OverflowError:
            pass
    return None


def _distance_group(status, reason):
    return {"status": status, "reason": reason, "method": "fit_record_time_weighted_mean",
            "interval_policy": "left_sample_hold_distance_progressing_max_30s",
            "distance_allocation": "linear_between_record_distances",
            "balance_aggregation": "not_computed_without_verified_side_semantics", "items": []}


def _distance_metrics(decoded, endurance, association):
    """Allocate original FIT intervals to the existing TCX distance partitions.

    A ratio is the weighted mean of recorded ratios, not a ratio of other means.
    Stationary intervals and gaps over 30 seconds are not filled. Values at the
    right endpoint are never borrowed to fill a missing left sample.
    """
    names = ("kilometer_splits", "distance_halves")
    output = {name: _distance_group("missing", "no_distance_partition") for name in names}
    summary = endurance.get("summary")
    total = _finite_number(summary.get("distance_m")) if isinstance(summary, dict) else None
    groups = {}
    for name in names:
        rows = endurance.get(name)
        if name == "distance_halves" and isinstance(rows, dict):
            rows = [rows.get("first"), rows.get("second")]
        if not isinstance(rows, list) or not rows:
            continue
        distances = [_finite_number(row.get("distance_m")) if isinstance(row, dict) else None
                     for row in rows]
        if name == "distance_halves":
            if (total is None or total <= 0 or len(distances) != 2
                    or any(value is None or abs(value - total / 2) > 0.000500001 for value in distances)):
                output[name] = _distance_group("invalid", "invalid_distance_partition")
                continue
            # The TCX normalizer rounds each half independently to millimetres.
            # Recover that declared equal-distance boundary from its exact total;
            # never stretch the FIT sample geometry to fill an uncovered endpoint.
            distances = [total / 2, total / 2]
        if (total is None or total <= 0 or any(value is None or value <= 0 for value in distances)
                or round(sum(distances), 3) != round(total, 3)):
            output[name] = _distance_group("invalid", "invalid_distance_partition")
            continue
        start, cells = 0.0, []
        for index, distance in enumerate(distances):
            cells.append({"segment": "first" if index == 0 else "second"} if name == "distance_halves"
                         else {"split": index + 1})
            cells[-1].update(start_distance_m=start, end_distance_m=start + distance,
                             distance_m=distance)
            start += distance
        groups[name] = cells
    if not groups:
        return output
    records = decoded["messages"].get("record_mesgs")
    if not isinstance(records, list) or len(records) < 2:
        for name in groups:
            output[name] = _distance_group("missing", "no_recorded_samples")
        return output
    points = []
    reason = None
    for row in records:
        stamp = _utc(row.get("timestamp")) if isinstance(row, dict) else None
        distance = _finite_number(row.get("distance")) if isinstance(row, dict) else None
        if stamp is None or distance is None or distance < 0:
            reason = "record_geometry_unverified"
            break
        if (stamp < association["session_start_time_utc"]
                or association["measured_at_utc"] is None or stamp > association["measured_at_utc"]
                or points and (stamp <= points[-1][0] or distance < points[-1][1])):
            reason = "record_order_unverified"
            break
        points.append((stamp, distance, row))
    # FIT distances have centimetre precision; do not align different endpoints
    # by stretching a series or substituting session.total_distance.
    if reason is None and round(points[-1][1], 2) != round(total, 2):
        reason = "distance_alignment_unverified"
    if reason:
        for name in groups:
            output[name] = _distance_group("unavailable", reason)
        return output
    accumulators = {}
    cursors = {}
    for name, cells in groups.items():
        cursors[name] = 0
        accumulators[name] = [
            {"eligible": 0.0, "gap": 0.0,
             "fields": {metric: {"mean": 0.0, "seconds": 0.0, "count": 0, "invalid": False}
                        for metric, _, _ in _RECORD_FIELDS}}
            for _ in cells
        ]
    for left, right in zip(points, points[1:], strict=False):
        delta_distance = right[1] - left[1]
        if delta_distance <= 0:
            continue
        seconds = (datetime.fromisoformat(right[0].replace("Z", "+00:00"))
                   - datetime.fromisoformat(left[0].replace("Z", "+00:00"))).total_seconds()
        for name, cells in groups.items():
            index = cursors[name]
            while index < len(cells) and cells[index]["end_distance_m"] <= left[1]:
                index += 1
            cursors[name] = index
            while index < len(cells) and cells[index]["start_distance_m"] < right[1]:
                cell, accumulator = cells[index], accumulators[name][index]
                overlap = min(right[1], cell["end_distance_m"]) - max(left[1], cell["start_distance_m"])
                weight = seconds * overlap / delta_distance
                if weight > 0 and seconds > 30:
                    accumulator["gap"] += weight
                elif weight > 0:
                    accumulator["eligible"] += weight
                    for metric, source_field, unit in _RECORD_FIELDS:
                        if source_field == "stance_time_balance":
                            continue
                        value = _recorded_value(left[2], source_field, unit)
                        field = accumulator["fields"][metric]
                        if value["status"] == "available":
                            duration = field["seconds"] + weight
                            # Scale before adding: finite FIT values multiplied by
                            # seconds can overflow even when their mean is finite.
                            field["mean"] = (field["mean"] * (field["seconds"] / duration)
                                             + value["value"] * (weight / duration))
                            field["seconds"] = duration
                            field["count"] += 1
                        elif value["status"] == "invalid":
                            field["invalid"] = True
                index += 1
    for name, cells in groups.items():
        for cell, accumulator in zip(cells, accumulators[name], strict=True):
            complete_geometry = (points[0][1] <= cell["start_distance_m"]
                                 and points[-1][1] >= cell["end_distance_m"])
            for metric, source_field, unit in _RECORD_FIELDS:
                field = accumulator["fields"][metric]
                value = _recorded_value({}, source_field, unit)
                if field["seconds"] > 0:
                    value.update(status="available", value=round(field["mean"], 4))
                elif field["invalid"]:
                    value["status"] = "invalid"
                if source_field == "stance_time_balance":
                    value["status"] = "unavailable"
                value["coverage"] = {
                    "valid_seconds": round(field["seconds"], 3),
                    "eligible_seconds": round(accumulator["eligible"], 3),
                    "excluded_gap_seconds": round(accumulator["gap"], 3),
                    "contributing_intervals": field["count"],
                    "status": "none" if not field["seconds"] else "complete"
                    if complete_geometry and not accumulator["gap"]
                    and abs(field["seconds"] - accumulator["eligible"]) < 1e-8 else "partial",
                }
                cell[metric] = value
        output[name] = _distance_group("available", None)
        output[name]["items"] = cells
    return output
