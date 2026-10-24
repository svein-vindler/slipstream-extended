"""Convert verified native running cadence without changing recorded metrics."""
from __future__ import annotations

import re

from .coach_segment_metrics import (
    _epoch,
    _frame,
    _halves,
    _lap_windows,
    _means,
    _pieces,
    _points,
    _whole_halves,
)
from .logical_bouts import _ROLES
from .native_lap_workout_binding import _distance, _index
from .recorded_running_metrics import _finite_number, _utc

SCHEMA_VERSION = 2


def _unavailable(reason, fields=(), precision="unverified"):
    return {"status": "unavailable", "value": None, "unit": "steps_per_minute",
            "source_fields": list(fields), "precision": precision, "reason": reason}


def _component(value, scale):
    number = _finite_number(value)
    if number is None:
        return None
    # FIT uint8 reserves raw255 as invalid. This is an encoding check, not
    # a physiological limit. The input fraction has ALREADY been scaled.
    raw = number * scale
    if raw < 0 or raw >= 255 or raw != int(raw):
        return None
    return number


def _measurement(row, base, alias, fraction):
    present = [name for name in (base, alias) if name in row]
    if not present:
        return _unavailable("base_missing_or_invalid", precision="base_unavailable")
    fields = [present[0], *([fraction] if fraction in row else [])]
    values = [_component(row[name], 1) for name in present]
    if any(value is None for value in values):
        return _unavailable("base_invalid", fields, "base_invalid")
    if len(values) == 2 and values[0] != values[1]:
        return _unavailable("base_alias_conflict", fields, "conflicting_base")
    if fraction not in row:
        return _unavailable("fraction_missing_or_invalid", fields, "fraction_unavailable")
    remainder = _component(row[fraction], 128)
    if remainder is None:
        return _unavailable("fraction_invalid", fields, "fraction_invalid")
    return {"status": "available", "value": (values[0] + remainder) * 2,
            "unit": "steps_per_minute", "source_fields": fields,
            "precision": "recorded_base_and_fraction", "reason": None}


def _measurements(row):
    return {
        "average": _measurement(row, "avg_cadence", "avg_running_cadence", "avg_fractional_cadence"),
        "maximum": _measurement(row, "max_cadence", "max_running_cadence", "max_fractional_cadence"),
    }


def _session(recorded, decoded):
    if (recorded.get("association") != "matched"
            or type(recorded.get("schema_version")) is not int or recorded["schema_version"] != 2
            or type(decoded.get("schema_version")) is not int or decoded["schema_version"] != 1
            or decoded.get("source") != "garmin-fit"
            or not isinstance(decoded.get("decode_errors"), list) or decoded["decode_errors"]):
        return None, "source_unmatched"
    digest = recorded.get("source_fit_sha256")
    identity = decoded.get("activity")
    activity_id = recorded.get("activity_id")
    if (not isinstance(digest, str) or re.fullmatch(r"[a-f0-9]{64}", digest) is None
            or decoded.get("source_fit_sha256") != digest
            or not isinstance(activity_id, str) or re.fullmatch(r"[0-9]{1,20}", activity_id) is None
            or not isinstance(identity, dict) or identity.get("id") != activity_id):
        return None, "source_identity_unverified"
    messages = decoded.get("messages")
    sessions = messages.get("session_mesgs") if isinstance(messages, dict) else None
    if not isinstance(sessions, list) or len(sessions) != 1 or not isinstance(sessions[0], dict):
        return None, "session_ambiguous"
    session = sessions[0]
    start = _utc(session.get("start_time"))
    if (start is None or start != recorded.get("session_start_time_utc")
            or start != _utc(identity.get("start_time_gmt"), allow_naive=True)
            or _utc(session.get("timestamp")) != recorded.get("measured_at_utc")):
        return None, "session_identity_unverified"
    if session.get("sport") != "running":
        return None, "running_sport_unverified"
    return session, None


def _laps(recorded, decoded):
    result = {"status": "unavailable", "reason": "native_laps_unavailable", "items": []}
    native = recorded.get("laps")
    raw = decoded["messages"].get("lap_mesgs")
    if (not isinstance(native, dict) or native.get("status") != "available"
            or not isinstance(native.get("items"), list) or not isinstance(raw, list)
            or len(native["items"]) != len(raw) or not raw):
        return result
    identities = set()
    for ordinal, (lap, row) in enumerate(zip(native["items"], raw, strict=True), 1):
        if not isinstance(lap, dict) or not isinstance(row, dict):
            result["reason"] = "lap_identity_unverified"
            return result
        index = _index(row.get("message_index"))
        start, end = _utc(row.get("start_time")), _utc(row.get("timestamp"))
        if (index is None or index in identities
                or type(lap.get("lap")) is not int or lap["lap"] != ordinal
                or start is None or end is None
                or start != lap.get("start_time_utc") or end != lap.get("end_time_utc")
                or _distance(row.get("total_distance")) != lap.get("distance_m")):
            result["reason"] = "lap_identity_unverified"
            return result
        identities.add(index)
    # All raw/native rows are accounted for before projecting any lap values.
    # An absent lap sport inherits only this already verified single running
    # session; an explicit conflicting lap sport never inherits it.
    result.update(status="available", reason=None)
    for ordinal, row in enumerate(raw, 1):
        measurements = (_measurements(row) if row.get("sport") in (None, "running")
                        else {name: _unavailable("running_sport_unverified")
                              for name in ("average", "maximum")})
        result["items"].append({"lap": ordinal, **measurements})
    return result


def _record_value(row, field, unit):
    converted = _measurement(row, field, None, "fractional_cadence")
    status = ("available" if converted["status"] == "available" else "invalid"
              if converted["reason"] in ("base_invalid", "fraction_invalid") else "missing")
    return {"status": status, "value": converted["value"], "unit": unit,
            "source_fields": ["cadence", "fractional_cadence"]}


def _work_partition(recorded, decoded, frame, points, tcx_sha256, segments):
    unavailable = "verified_work_intervals_unavailable"
    if (not isinstance(segments, dict) or segments.get("association") != "matched"
            or segments.get("activity_id") != recorded["activity_id"]
            or segments.get("source_fit_sha256") != recorded["source_fit_sha256"]
            or segments.get("source_tcx_sha256") != tcx_sha256
            or _epoch(segments.get("session_start_time_utc")) != frame["start"]
            or _epoch(segments.get("session_end_time_utc")) != frame["end"]):
        return None, None, unavailable
    verified = segments.get("executed_intervals")
    if (not isinstance(verified, dict) or verified.get("status") != "available"
            or verified.get("clock_basis") != "fit_start_time_plus_total_elapsed_time"
            or verified.get("method") != "fit_record_time_weighted_mean"
            or verified.get("distance_basis") != "fit_executed_active_distance"
            or not isinstance(verified.get("items"), list) or not verified["items"]):
        return None, None, unavailable
    windows, reason = _lap_windows(decoded, points, frame)
    if windows is None:
        return None, None, reason
    by_lap = {row["lap"]: row for row in windows}
    cells, pieces, previous_lap = [], [], 0
    for row in verified["items"]:
        if not isinstance(row, dict):
            return None, None, unavailable
        sources, step = row.get("source_laps"), _index(row.get("workout_step_index"))
        if (type(row.get("interval")) is not int or row["interval"] != len(cells) + 1
                or not isinstance(sources, list) or not sources or step is None
                or any(type(lap) is not int for lap in sources) or sources[0] <= previous_lap):
            return None, None, unavailable
        selected = []
        for lap in sources:
            window = by_lap.get(lap)
            if (window is None or not window["active"] or window["step"] != step
                    or selected and (selected[-1]["lap"] + 1 != lap
                                     or selected[-1]["end"] != window["start"])):
                return None, None, unavailable
            selected.append(window)
        total = sum(window["distance"] for window in selected)
        if (total <= 0 or _finite_number(row.get("distance_m")) != total
                or _epoch(row.get("start_time_utc")) != selected[0]["start"]
                or _epoch(row.get("end_time_utc")) != selected[-1]["end"]):
            return None, None, unavailable
        index = len(cells)
        cell = {name: row[name] for name in (
            "interval", "workout_step_index", "distance_m", "start_time_utc", "end_time_utc",
        )}
        cell["source_laps"] = list(sources)
        cells.append(cell)
        pieces.extend((*piece[:4], index) for piece in _pieces(
            selected, [{"start_distance_m": 0, "end_distance_m": total}],
        ))
        previous_lap = sources[-1]
    return cells, pieces, None


def _recovery_partition(recorded, decoded, frame, points, logical):
    unavailable = "verified_recovery_bouts_unavailable"
    if (not isinstance(logical, dict) or type(logical.get("schema_version")) is not int
            or logical["schema_version"] != 2 or logical.get("status") != "available"
            or logical.get("method") != "verified_native_source_order"
            or logical.get("grouping_basis") != "verified_native_source_order"
            or logical.get("activity_id") != recorded["activity_id"]
            or logical.get("source_fit_sha256") != recorded["source_fit_sha256"]
            or logical.get("unassigned_laps") != []
            or not isinstance(logical.get("items"), list) or not logical["items"]):
        return None, None, unavailable
    windows, reason = _lap_windows(decoded, points, frame)
    if windows is None:
        return None, None, reason
    if _laps(recorded, decoded)["status"] != "available":
        return None, None, "recovery_native_identity_unverified"
    coverage = logical.get("source_coverage")
    if (not isinstance(coverage, dict) or coverage.get("status") != "complete"
            or coverage.get("reason") is not None
            or any(type(coverage.get(name)) is not int or coverage[name] != len(windows)
                   for name in ("raw_lap_count", "native_lap_count", "binding_count", "execution_count"))):
        return None, None, "recovery_source_coverage_unverified"
    if any(window["end"] <= window["start"] for window in windows):
        return None, None, "recovery_clock_unverified"
    # These are complete, unfiltered raw rows projected once by _lap_windows.
    raw = decoded["messages"]["lap_mesgs"]
    by_lap = {window["lap"]: (window, source)
              for window, source in zip(windows, raw, strict=True)}
    steps = {row["message_index"]: row for row in decoded["messages"]["workout_step_mesgs"]}
    cells, pieces, accounted = [], [], 0
    for ordinal, row in enumerate(logical["items"], 1):
        if not isinstance(row, dict) or _index(row.get("bout")) != ordinal:
            return None, None, unavailable
        sources, indices = row.get("source_laps"), row.get("source_message_indices")
        step, intensity = _index(row.get("workout_step_index")), row.get("source_intensity")
        role = _ROLES.get(intensity) if isinstance(intensity, str) else None
        if (not isinstance(sources, list) or not sources or not isinstance(indices, list)
                or len(sources) != len(indices) or step is None
                or role is None or row.get("role") != role
                or any(type(lap) is not int for lap in sources)
                or sources != list(range(accounted + 1, accounted + len(sources) + 1))):
            return None, None, unavailable
        selected = []
        for lap, identity in zip(sources, indices, strict=True):
            pair = by_lap.get(lap)
            if pair is None:
                return None, None, unavailable
            window, source = pair
            if (_index(identity) is None or identity != source.get("message_index")
                    or window["step"] != step or steps[step].get("intensity") != intensity
                    or source.get("intensity") is not None and source["intensity"] != intensity):
                return None, None, "recovery_source_identity_unverified"
            selected.append(window)
        accounted += len(sources)
        if role != "recovery":
            continue
        timing = row.get("timing")
        if (not isinstance(timing, dict) or timing.get("status") != "available"
                or timing.get("basis") != "fit_start_time_plus_total_elapsed_time"
                or timing.get("reason") is not None
                or _epoch(timing.get("start_time_utc")) != selected[0]["start"]
                or _epoch(timing.get("end_time_utc")) != selected[-1]["end"]
                or any(left["end"] != right["start"]
                       for left, right in zip(selected, selected[1:], strict=False))):
            return None, None, "recovery_clock_unverified"
        total = sum(window["distance"] for window in selected)
        if _finite_number(total) is None or total <= 0:
            return None, None, "recovery_distance_unverified"
        index = len(cells)
        cells.append({"bout": row["bout"], "source_laps": list(sources),
                      "workout_step_index": step, "distance_m": total,
                      "start_time_utc": timing["start_time_utc"], "end_time_utc": timing["end_time_utc"]})
        pieces.extend((*piece[:4], index) for piece in _pieces(
            selected, [{"start_distance_m": 0, "end_distance_m": total}],
        ))
    if accounted != len(windows):
        return None, None, "recovery_source_coverage_unverified"
    if not cells:
        return None, None, "no_verified_recovery_bouts"
    return cells, pieces, None


def _kilometer_partition(recorded, endurance, points):
    verified = recorded.get("kilometer_splits")
    if (not isinstance(verified, dict) or verified.get("status") != "available"
            or verified.get("reason") is not None
            or verified.get("method") != "fit_record_time_weighted_mean"
            or verified.get("interval_policy") != "left_sample_hold_distance_progressing_max_30s"
            or verified.get("distance_allocation") != "linear_between_record_distances"
            or not isinstance(verified.get("items"), list) or not verified["items"]):
        return None, None, "verified_kilometer_partition_unavailable"
    summary, rows = endurance.get("summary"), endurance.get("kilometer_splits")
    total = _finite_number(summary.get("distance_m")) if isinstance(summary, dict) else None
    if not isinstance(rows, list) or not rows:
        return None, None, "no_distance_partition"
    distances = [_finite_number(row.get("distance_m")) if isinstance(row, dict) else None
                 for row in rows]
    # Same declared TCX partition and FIT endpoint guards as the existing metrics.
    if (total is None or total <= 0 or any(value is None or value <= 0 for value in distances)
            or round(sum(distances), 3) != round(total, 3)):
        return None, None, "invalid_distance_partition"
    if round(points[-1][1], 2) != round(total, 2):
        return None, None, "distance_alignment_unverified"
    if len(verified["items"]) != len(distances):
        return None, None, "kilometer_partition_identity_unverified"
    cells, start = [], 0.0
    for split, (distance, reference) in enumerate(zip(distances, verified["items"], strict=True), 1):
        expected = {"start_distance_m": start, "end_distance_m": start + distance,
                    "distance_m": distance}
        if (not isinstance(reference, dict) or type(reference.get("split")) is not int
                or reference["split"] != split
                or any(_finite_number(reference.get(name)) != value for name, value in expected.items())):
            return None, None, "kilometer_partition_identity_unverified"
        cells.append({name: reference[name] for name in (
            "split", "start_distance_m", "end_distance_m", "distance_m",
        )})
        start += distance
    pieces = [(row["start_distance_m"], row["end_distance_m"], points[0][0], points[-1][0], index)
              for index, row in enumerate(cells)]
    return cells, pieces, None


def _record_groups(recorded, decoded, session, endurance, tcx_sha256, *,
                   scope="whole", structure=None, segments=None, logical=None):
    main, work, recovery = scope == "main", scope == "work", scope == "recovery"
    kilometers = scope == "km"
    digest = (tcx_sha256 if isinstance(tcx_sha256, str)
              and re.fullmatch(r"[a-f0-9]{64}", tcx_sha256) else None)
    group = {
        "status": "unavailable", "reason": "source_unverified", "items": [],
        "method": "fit_record_time_weighted_mean",
        "distance_basis": ("tcx_endurance_kilometer_splits" if kilometers else
                           "fit_verified_recovery_bout_distance" if recovery else
                           "fit_executed_active_distance" if work else
                           "fit_executed_main_distance" if main else "tcx_endurance_distance"),
        "clock_basis": "fit_start_time_plus_total_elapsed_time",
        "interval_policy": "left_sample_hold_distance_progressing_max_30s",
        "distance_allocation": "linear_between_record_distances",
        "source_tcx_sha256": digest, "source_fields": ["cadence", "fractional_cadence"],
        "input_unit": "rpm", "unit": "steps_per_minute",
        "conversion": "2 * (cadence + fractional_cadence)",
        "valid_record_precision": "recorded_base_and_fraction",
        "session_start_time_utc": None, "session_end_time_utc": None,
        "session_summary_timestamp_utc": None,
        "source_window": {"disposition": "unverified", "original_record_count": None,
                          "boundary_excluded_record_count": 0, "boundary_excluded_pair_count": 0},
    }
    if session is None:
        return group
    structured = structure.get("has_structured_workout") if isinstance(structure, dict) else None
    if (main or work or recovery) and type(structured) is not bool:
        group["reason"] = ("recovery_structure_unverified" if recovery else
                           "work_structure_unverified" if work else "main_structure_unverified")
        return group
    if (work or recovery) and not structured:
        group["reason"] = "no_verified_structured_workout"
        return group
    if main and not structured:
        group["distance_basis"] = "tcx_endurance_distance_continuous_main"
    if digest is None:
        group["reason"] = "tcx_source_unverified"
        return group
    if not isinstance(endurance, dict) or endurance.get("available") is False:
        group["reason"] = "endurance_source_unavailable"
        return group
    identity = endurance.get("activity")
    if not isinstance(identity, dict) or str(identity.get("id")) != recorded["activity_id"]:
        group["reason"] = "endurance_identity_unverified"
        return group
    frame = _frame(session)
    if frame is None or frame["start_time_utc"] != recorded["session_start_time_utc"]:
        group["reason"] = "clock_basis_unverified"
        return group
    group.update(session_start_time_utc=frame["start_time_utc"],
                 session_end_time_utc=frame["end_time_utc"],
                 session_summary_timestamp_utc=frame["summary_timestamp_utc"])
    points = _points(decoded, frame, allow_terminal=True)
    if points is None:
        group["reason"] = "record_geometry_unverified"
        return group
    excluded = int(points[-1][0] > frame["end"])
    group["source_window"].update(
        disposition="verified_terminal_exclusion" if excluded else "verified_in_frame",
        original_record_count=len(points), boundary_excluded_record_count=excluded,
        boundary_excluded_pair_count=excluded,
    )
    if kilometers:
        cells, pieces, reason = _kilometer_partition(recorded, endurance, points)
        group["reason"] = reason
        if cells is None:
            return group
    elif recovery:
        cells, pieces, reason = _recovery_partition(recorded, decoded, frame, points, logical)
        group["reason"] = reason
        if cells is None:
            return group
    elif work:
        cells, pieces, reason = _work_partition(recorded, decoded, frame, points, tcx_sha256, segments)
        group["reason"] = reason
        if cells is None:
            return group
    elif main and structured:
        windows, reason = _lap_windows(decoded, points, frame)
        group["reason"] = reason
        if windows is None:
            return group
        main_windows = [row for row in windows if row["role"] == "main"]
        total = sum(row["distance"] for row in main_windows)
        if total <= 0:
            group["reason"] = "main_distance_missing"
            return group
        cells = _halves(total)
        pieces = _pieces(main_windows, cells)
        group["source_laps"] = [row["lap"] for row in main_windows]
        group["source_windows"] = [
            {"lap": row["lap"], "workout_step_index": row["step"],
             "start_time_utc": row["start_time_utc"], "end_time_utc": row["end_time_utc"],
             "distance_m": row["distance"]} for row in main_windows
        ]
    else:
        cells, reason = _whole_halves(endurance, points)
        group["reason"] = reason
        if cells is None:
            return group
        pieces = [(row["start_distance_m"], row["end_distance_m"], points[0][0], points[-1][0], index)
                  for index, row in enumerate(cells)]
    _means(points, cells, pieces, frame_end=frame["end"],
           metric_fields=(("average_cadence", "cadence", "steps_per_minute"),),
           metric_reader=_record_value)
    group.update(status="available", reason=None, items=cells)
    return group


def recorded_cadence(recorded, decoded, *, endurance=None, tcx_sha256=None,
                     structure=None, segments=None, logical=None):
    """Pure native conversion and strict verified record-scope projections."""
    activity_id = recorded.get("activity_id") if isinstance(recorded, dict) else None
    result = {
        "schema_version": SCHEMA_VERSION, "method": "fit_running_native_cadence_conversion",
        "activity_id": activity_id, "source_fit_sha256": None, "association": "source_unmatched",
        "session": {name: _unavailable("source_unmatched") for name in ("average", "maximum")},
        "laps": {"status": "unavailable", "reason": "source_unmatched", "items": []},
        "record_aggregates": {
            "whole_distance_halves": _record_groups(None, None, None, endurance, tcx_sha256),
            "main_distance_halves": _record_groups(
                None, None, None, endurance, tcx_sha256, scope="main", structure=structure),
            "executed_intervals": _record_groups(
                None, None, None, endurance, tcx_sha256, scope="work", structure=structure, segments=segments),
            "recovery_bouts": _record_groups(
                None, None, None, endurance, tcx_sha256, scope="recovery", structure=structure, logical=logical),
            "kilometer_splits": _record_groups(None, None, None, endurance, tcx_sha256, scope="km"),
        },
    }
    if not isinstance(recorded, dict) or not isinstance(decoded, dict):
        return result
    session, reason = _session(recorded, decoded)
    if session is None:
        result["association"] = reason
        result["session"] = {name: _unavailable(reason) for name in ("average", "maximum")}
        result["laps"]["reason"] = reason
        for group in result["record_aggregates"].values():
            group["reason"] = reason
        return result
    result.update(association="matched", source_fit_sha256=recorded["source_fit_sha256"],
                  session=_measurements(session), laps=_laps(recorded, decoded),
                  record_aggregates={
                      "whole_distance_halves": _record_groups(recorded, decoded, session, endurance, tcx_sha256),
                      "main_distance_halves": _record_groups(
                          recorded, decoded, session, endurance, tcx_sha256, scope="main", structure=structure),
                      "executed_intervals": _record_groups(
                          recorded, decoded, session, endurance, tcx_sha256,
                          scope="work", structure=structure, segments=segments),
                      "recovery_bouts": _record_groups(
                          recorded, decoded, session, endurance, tcx_sha256,
                          scope="recovery", structure=structure, logical=logical),
                      "kilometer_splits": _record_groups(
                          recorded, decoded, session, endurance, tcx_sha256, scope="km"),
                  })
    return result
