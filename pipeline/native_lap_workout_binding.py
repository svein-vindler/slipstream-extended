"""Reference native recorded laps without recalculating or copying measurements."""
from __future__ import annotations

import re
from collections import Counter

from .recorded_running_metrics import _finite_number, _utc


def _index(value):
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


def _indexed(rows, field):
    grouped = {}
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, dict) and (index := _index(row.get(field))) is not None:
            grouped.setdefault(index, []).append(row)
    return grouped


def _distance(value):
    value = _finite_number(value)
    return value if value is not None and value >= 0 else None


def native_lap_workout_binding(recorded, decoded, structure):
    result = {
        "schema_version": 1, "method": "fit_lap_wkt_step_index",
        "measurement_source": "recorded_running_metrics.laps",
        "activity_id": recorded["activity_id"], "source_fit_sha256": None,
        "status": "unavailable", "reason": "source_unavailable", "items": [],
    }
    digest = recorded.get("source_fit_sha256")
    if (recorded.get("association") != "matched" or not isinstance(digest, str)
            or re.fullmatch(r"[a-f0-9]{64}", digest) is None):
        return result
    result["source_fit_sha256"] = digest
    laps = recorded["laps"]
    if laps["status"] != "available":
        result["reason"] = "recorded_laps_unavailable"
        return result
    messages = decoded.get("messages", {})
    raw_laps = messages.get("lap_mesgs", [])
    counts = Counter(_index(row.get("message_index")) for row in raw_laps if isinstance(row, dict))
    steps = _indexed(messages.get("workout_step_mesgs"), "message_index")
    planned = _indexed(structure.get("planned_steps"), "step_index")
    executed = _indexed(structure.get("executed_laps"), "lap")
    for lap in laps["items"]:
        row = raw_laps[lap["lap"] - 1]
        index = _index(row.get("message_index"))
        item = {"lap": lap["lap"], "source_message_index": None, "status": "unbound",
                "reason": "lap_identity_unverified", "workout_step_index": None}
        result["items"].append(item)
        if (index is None or counts[index] != 1
                or _utc(row.get("start_time")) != lap["start_time_utc"]
                or _utc(row.get("timestamp")) != lap["end_time_utc"]
                or _distance(row.get("total_distance")) != lap["distance_m"]):
            continue
        item["source_message_index"] = index
        item["reason"] = "workout_step_unverified"
        step = _index(row.get("wkt_step_index"))
        if step is None or len(steps.get(step, [])) != 1:
            continue
        item["reason"] = "executed_lap_mismatch"
        matches = executed.get(lap["lap"], [])
        if len(planned.get(step, [])) != 1 or len(matches) != 1:
            continue
        execution = matches[0]
        if (_index(execution.get("workout_step_index")) != step
                or _utc(execution.get("start_time")) != lap["start_time_utc"]
                or execution.get("distance_m") != lap["distance_m"]):
            continue
        item.update(status="bound", reason=None, workout_step_index=step)
    bound = sum(item["status"] == "bound" for item in result["items"])
    result.update(
        status="available" if bound == len(result["items"]) else "partial" if bound else "unavailable",
        reason=None if bound == len(result["items"]) else "some_laps_unbound" if bound else "no_verified_lap_workout_links",
    )
    return result
