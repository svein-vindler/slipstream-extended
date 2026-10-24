"""Source-ordered native groups, with separate strict timing and record references."""
from __future__ import annotations

from .coach_segment_metrics import _epoch, _frame
from .native_lap_workout_binding import _distance, _index, _indexed
from .recorded_running_metrics import _utc

_ROLES = {"active": "work", "rest": "recovery", "recovery": "recovery",
          "warmup": "warmup", "cooldown": "cooldown"}
_CLOCK = "fit_start_time_plus_total_elapsed_time"
SCHEMA_VERSION = 2


def _one(index, key):
    rows = index.get(key, [])
    return rows[0] if len(rows) == 1 else None


def _identity(lap, raw, references, raw_laps, raw_steps, planned, executed):
    reference = _one(references, lap["lap"])
    if not reference or reference.get("status") != "bound":
        return None, "native_binding_unverified"
    source = _index(reference.get("source_message_index"))
    step = _index(reference.get("workout_step_index"))
    identified = _one(raw_laps, source) if source is not None else None
    raw_step = _one(raw_steps, step) if step is not None else None
    plan, execution = _one(planned, step), _one(executed, lap["lap"])
    if (identified is not raw or raw_step is None or plan is None or execution is None
            or _index(raw.get("wkt_step_index")) != step
            or _index(execution.get("workout_step_index")) != step
            or _utc(raw.get("start_time")) != lap["start_time_utc"]
            or _utc(raw.get("timestamp")) != lap["end_time_utc"]
            or _utc(execution.get("start_time")) != lap["start_time_utc"]
            or execution.get("distance_m") != lap["distance_m"]):
        return None, "native_binding_unverified"
    intensity = raw_step.get("intensity")
    role = _ROLES.get(intensity) if isinstance(intensity, str) else None
    if (role is None or plan.get("intensity") != intensity
            or execution.get("intensity") != intensity
            or raw.get("intensity") is not None and raw["intensity"] != intensity):
        return None, "role_unverified"
    return {"lap": lap["lap"], "source": source, "step": step, "role": role,
            "intensity": intensity}, None


def _complete_source_rows(raw, rows, references, executions):
    if (not all(isinstance(value, list) for value in (raw, rows, references, executions))
            or not raw or not len(raw) == len(rows) == len(references) == len(executions)):
        return False
    seen = set()
    for ordinal, (source, lap, reference, execution) in enumerate(zip(raw, rows, references, executions, strict=True), 1):
        if (not all(isinstance(value, dict) for value in (source, lap, reference, execution))
                or any(_index(value.get("lap")) != ordinal for value in (lap, reference, execution))):
            return False
        identity = _index(source.get("message_index"))
        if (identity is None or identity in seen
                or reference.get("source_message_index") is not None
                and _index(reference["source_message_index"]) != identity
                or _utc(source.get("start_time")) != lap.get("start_time_utc")
                or _utc(source.get("timestamp")) != lap.get("end_time_utc")
                or _distance(source.get("total_distance")) != lap.get("distance_m")
                or _utc(execution.get("start_time")) != lap.get("start_time_utc")
                or execution.get("distance_m") != lap.get("distance_m")):
            return False
        seen.add(identity)
    return True


def _elapsed_frames(raw, session, recorded):
    if (session is None or session["end"] <= session["start"]
            or session["start_time_utc"] != recorded["session_start_time_utc"]):
        return None
    frames, previous_end = [], None
    for row in raw:
        frame = _frame(row)
        if (frame is None or frame["end"] <= frame["start"]
                or frame["start"] < session["start"] or frame["end"] > session["end"]
                or previous_end is not None and frame["start"] < previous_end):
            return None
        previous_end = frame["end"]
        frames.append(frame)
    return frames


def _timing(members):
    result = {"status": "unavailable", "basis": _CLOCK, "reason": "clock_basis_unverified",
              "start_time_utc": None, "end_time_utc": None}
    previous = None
    for member in members:
        frame = member["frame"]
        if frame is None or previous is not None and previous["end"] != frame["start"]:
            return result
        previous = frame
    result.update(status="available", reason=None, start_time_utc=members[0]["frame"]["start_time_utc"],
                  end_time_utc=members[-1]["frame"]["end_time_utc"])
    return result


def _record_intervals(segments, recorded, session):
    group = segments.get("executed_intervals", {})
    if (session is None or segments.get("association") != "matched"
            or segments.get("activity_id") != recorded["activity_id"]
            or segments.get("source_fit_sha256") != recorded["source_fit_sha256"]
            or _epoch(segments.get("session_start_time_utc")) != session["start"]
            or _epoch(segments.get("session_end_time_utc")) != session["end"]
            or group.get("status") != "available" or group.get("clock_basis") != _CLOCK):
        return {}, group.get("reason") or "no_verified_record_aggregate"
    indexed = {}
    for row in group["items"]:
        key = (tuple(row["source_laps"]), row["workout_step_index"])
        indexed[key] = None if key in indexed else row
    return indexed, "no_verified_record_aggregate"


def logical_bouts(recorded, decoded, structure, native, segments):
    """Pure source projection; source order never substitutes for elapsed/record evidence."""
    result = {
        "schema_version": SCHEMA_VERSION, "method": "verified_native_source_order",
        "grouping_basis": "verified_native_source_order",
        "activity_id": recorded["activity_id"], "source_fit_sha256": None,
        "status": "unavailable", "reason": "source_unavailable",
        "occurrence_basis": "derived_verified_source_order_per_step", "items": [], "unassigned_laps": [],
        "source_coverage": {"status": "unavailable", "reason": "source_unavailable"},
    }
    digest = recorded.get("source_fit_sha256")
    identity = decoded.get("activity")
    if (recorded.get("association") != "matched" or not digest
            or decoded.get("decode_errors") != [] or decoded.get("source_fit_sha256") != digest
            or not isinstance(identity, dict) or identity.get("id") != recorded["activity_id"]
            or native.get("method") != "fit_lap_wkt_step_index"
            or native.get("activity_id") != recorded["activity_id"] or native.get("source_fit_sha256") != digest):
        return result
    result["source_fit_sha256"] = digest
    laps = recorded["laps"]
    if laps["status"] != "available":
        result["reason"] = result["source_coverage"]["reason"] = "recorded_laps_unavailable"
        return result
    messages = decoded.get("messages", {})
    rows = laps["items"]
    raw = messages.get("lap_mesgs")
    reference_rows = native.get("items")
    execution_rows = structure.get("executed_laps")
    unassigned = result["unassigned_laps"]
    coverage = result["source_coverage"]
    coverage.update({name: len(value) if isinstance(value, list) else None for name, value in (
        ("raw_lap_count", raw), ("native_lap_count", rows), ("binding_count", reference_rows),
        ("execution_count", execution_rows))})
    if not _complete_source_rows(raw, rows, reference_rows, execution_rows):
        result["reason"] = coverage["reason"] = "source_coverage_unverified"
        unassigned.extend({"lap": lap["lap"], "reason": "source_coverage_unverified"} for lap in rows)
        return result
    coverage.update(status="complete", reason=None)
    sessions = messages.get("session_mesgs")
    session = (_frame(sessions[0]) if isinstance(sessions, list) and len(sessions) == 1
               and isinstance(sessions[0], dict) else None)
    frames = _elapsed_frames(raw, session, recorded)
    references = _indexed(reference_rows, "lap")
    raw_laps = _indexed(raw, "message_index")
    raw_steps = _indexed(messages.get("workout_step_mesgs"), "message_index")
    planned = _indexed(structure.get("planned_steps"), "step_index")
    executed = _indexed(execution_rows, "lap")
    groups, occurrences, previous = result["items"], {}, None
    for position, (lap, source) in enumerate(zip(rows, raw, strict=True)):
        node, reason = _identity(lap, source, references, raw_laps, raw_steps, planned, executed)
        if node is None:
            unassigned.append({"lap": lap["lap"], "reason": reason})
            previous = None
            continue
        node["frame"] = frames[position] if frames is not None else None
        if (previous is not None and previous["lap"] + 1 == node["lap"]
                and previous["step"] == node["step"] and previous["role"] == node["role"]):
            group = groups[-1]
            group["source_laps"].append(node["lap"])
            group["source_message_indices"].append(node["source"])
            group["_members"].append(node)
        else:
            step = node["step"]
            occurrences[step] = occurrences.get(step, 0) + 1
            groups.append({
                "bout": len(groups) + 1, "role": node["role"], "source_intensity": node["intensity"],
                "source_laps": [node["lap"]], "source_message_indices": [node["source"]], "workout_step_index": step,
                "observed_step_occurrence": occurrences[step], "planned_repeat_iteration": None,
                "_members": [node],
            })
        previous = node
    intervals, unavailable = _record_intervals(segments, recorded, session)
    for group in groups:
        timing = group["timing"] = _timing(group.pop("_members"))
        row = intervals.get((tuple(group["source_laps"]), group["workout_step_index"]))
        matched = (timing["status"] == "available" and group["role"] == "work" and row is not None
                   and _epoch(row.get("start_time_utc")) == _epoch(timing["start_time_utc"])
                   and _epoch(row.get("end_time_utc")) == _epoch(timing["end_time_utc"]))
        group["record_aggregate"] = {"status": "available" if matched else "unavailable",
                                     "interval": row["interval"] if matched else None,
                                     "reason": None if matched else timing["reason"] or unavailable}
    result.update(status="partial" if groups and unassigned else "available" if groups else "unavailable",
                  reason="some_laps_unassigned" if groups and unassigned else None if groups else "no_verified_bouts")
    return result
