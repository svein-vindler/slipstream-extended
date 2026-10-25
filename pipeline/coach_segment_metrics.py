"""Versioned statistics of original FIT records; no storage or provider access."""
from __future__ import annotations

import bisect
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal

from .recorded_running_metrics import _RECORD_FIELDS, _finite_number, _recorded_value, _utc

POLICY = "raw_scalar_time_weighted_mean_side_unknown"


def _epoch(value):
    stamp = _utc(value)
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() if stamp else None


def _frame(row):
    """FIT summaries span start+elapsed; their timestamp is separate metadata."""
    start = _utc(row.get("start_time"))
    elapsed = _finite_number(row.get("total_elapsed_time"))
    if start is None or elapsed is None or elapsed < 0:
        return None
    milliseconds = Decimal(str(elapsed)) * 1000
    if milliseconds != milliseconds.to_integral_value():
        return None
    try:
        first = datetime.fromisoformat(start.replace("Z", "+00:00"))
        last = first + timedelta(milliseconds=int(milliseconds))
    except (ValueError, OverflowError):
        return None
    return {"start": first.timestamp(), "end": last.timestamp(),
            "start_time_utc": start, "end_time_utc": last.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "summary_timestamp_utc": _utc(row.get("timestamp"))}


def _group(basis, reason):
    return {"status": "unavailable", "reason": reason, "distance_basis": basis,
            "method": "fit_record_time_weighted_mean", "clock_basis": "fit_start_time_plus_total_elapsed_time",
            "interval_policy": "left_sample_hold_distance_progressing_max_30s",
            "distance_allocation": "linear_between_record_distances",
            "balance_aggregation": POLICY, "items": []}


def _points(decoded, frame, *, allow_terminal=False):
    rows = decoded.get("messages", {}).get("record_mesgs")
    if not isinstance(rows, list) or len(rows) < 2:
        return None
    start, end = frame["start"], frame["end"]
    points = []
    for ordinal, row in enumerate(rows):
        stamp = _epoch(row.get("timestamp")) if isinstance(row, dict) else None
        distance = _finite_number(row.get("distance")) if isinstance(row, dict) else None
        if (stamp is None or distance is None or distance < 0
                or not (start <= stamp <= end or allow_terminal and ordinal == len(rows) - 1 and stamp > end)
                or points and (stamp <= points[-1][0] or distance < points[-1][1])):
            return None
        points.append((stamp, distance, row))
    return points


def _distance_at(points, stamps, stamp, *, frame_end=None):
    index = bisect.bisect_left(stamps, stamp)
    if index < len(points) and points[index][0] == stamp:
        return points[index][1]
    if not 0 < index < len(points):
        return None
    left, right = points[index - 1:index + 1]
    if frame_end is not None and right[0] > frame_end:
        return None
    if right[0] - left[0] > 30:
        return None
    return left[1] + (right[1] - left[1]) * (stamp - left[0]) / (right[0] - left[0])


def _lap_windows(decoded, points, session):
    """Project each validated raw message once, never join filtered lap arrays."""
    rows = decoded.get("messages", {}).get("lap_mesgs")
    if not isinstance(rows, list) or not rows:
        return None, "recorded_laps_missing"
    steps = {}
    raw_steps = decoded.get("messages", {}).get("workout_step_mesgs")
    if not isinstance(raw_steps, list):
        return None, "step_binding_unverified"
    for step in raw_steps:
        if not isinstance(step, dict):
            return None, "step_binding_unverified"
        index = step.get("message_index")
        if type(index) is not int or index < 0 or index in steps:
            return None, "step_binding_unverified"
        steps[index] = step
    stamps = [point[0] for point in points]
    windows = []
    for ordinal, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            return None, "lap_schema_unverified"
        frame = _frame(row)
        distance = _finite_number(row.get("total_distance"))
        if frame is None:
            return None, "clock_basis_unverified"
        start, end = frame["start"], frame["end"]
        if (start < session["start"] or end > session["end"]
                or windows and start < windows[-1]["end"]):
            return None, "clock_basis_unverified"
        if distance is None or distance < 0:
            return None, "main_distance_unverified"
        index = row.get("wkt_step_index")
        if type(index) is not int or index not in steps:
            return None, "step_binding_unverified"
        plan = steps.get(index, {})
        intensity = str(plan.get("intensity") or row.get("intensity") or "").lower()
        role = "warmup" if "warm" in intensity else "cooldown" if "cool" in intensity else "main"
        windows.append({"lap": ordinal, "start": start, "end": end,
                        "distance": distance, "step": index,
                        "active": bool(plan) and intensity == "active", "role": role,
                        "start_time_utc": frame["start_time_utc"], "end_time_utc": frame["end_time_utc"]})
    # Establish the complete clock ordering before interpreting local geometry.
    for window in windows:
        first = _distance_at(points, stamps, window["start"], frame_end=session["end"])
        last = _distance_at(points, stamps, window["end"], frame_end=session["end"])
        if first is None or last is None or round(last - first, 2) != round(window["distance"], 2):
            return None, "main_distance_alignment_unverified"
        window.update(start_distance=first, end_distance=last)
    return windows, None


def _pieces(windows, cells):
    """Map contiguous local distance cuts to original FIT space and clocks."""
    pieces = []
    offset = 0.0
    for window in windows:
        for index, cell in enumerate(cells):
            start = max(offset, cell["start_distance_m"])
            end = min(offset + window["distance"], cell["end_distance_m"])
            if end > start:
                pieces.append((window["start_distance"] + start - offset,
                               window["start_distance"] + end - offset,
                               window["start"], window["end"], index))
        offset += window["distance"]
    return pieces


def _means(points, cells, pieces, *, frame_end=None,
           metric_fields=_RECORD_FIELDS, metric_reader=_recorded_value):
    """An ordered record/piece sweep; no records-times-all-laps scan."""
    accumulators = [{"eligible": 0.0, "gap": 0.0, "boundary": 0.0, "geometry": True,
                     "fields": {name: {"mean": 0.0, "seconds": 0.0, "count": 0, "last_pair": None, "invalid": False}
                                for name, _, _ in metric_fields}} for _ in cells]
    for start_d, end_d, _start_t, _end_t, index in pieces:
        if points[0][1] > start_d or points[-1][1] < end_d:
            accumulators[index]["geometry"] = False
    cursor = 0
    for pair_index, (left, right) in enumerate(zip(points, points[1:], strict=False)):
        delta = right[1] - left[1]
        if delta <= 0:
            continue
        seconds = right[0] - left[0]
        while cursor < len(pieces) and (pieces[cursor][1] <= left[1] or pieces[cursor][3] <= left[0]):
            cursor += 1
        index = cursor
        while index < len(pieces) and pieces[index][0] < right[1]:
            start_d, end_d, start_t, end_t, cell = pieces[index]
            lo, hi = max(left[0], start_t), min(right[0], end_t)
            if frame_end is not None:
                hi = min(hi, frame_end)
            if hi > lo:
                lo_d = left[1] + delta * (lo - left[0]) / seconds
                hi_d = left[1] + delta * (hi - left[0]) / seconds
                overlap = min(hi_d, end_d) - max(lo_d, start_d)
                weight = seconds * max(0.0, overlap) / delta
                accumulator = accumulators[cell]
                if weight and frame_end is not None and right[0] > frame_end:
                    accumulator["boundary"] += weight
                elif weight and seconds > 30:
                    accumulator["gap"] += weight
                elif weight:
                    accumulator["eligible"] += weight
                    for name, field, unit in metric_fields:
                        value = metric_reader(left[2], field, unit)
                        saved = accumulator["fields"][name]
                        if value["status"] == "available":
                            duration = saved["seconds"] + weight
                            saved["mean"] = (saved["mean"] * (saved["seconds"] / duration)
                                             + value["value"] * (weight / duration))
                            saved["seconds"] = duration
                            if saved["last_pair"] != pair_index:
                                saved["count"] += 1
                                saved["last_pair"] = pair_index
                        elif value["status"] == "invalid":
                            saved["invalid"] = True
            index += 1
    for cell, accumulator in zip(cells, accumulators, strict=True):
        for name, field, unit in metric_fields:
            saved = accumulator["fields"][name]
            value = metric_reader({}, field, unit)
            if saved["seconds"]:
                value.update(status="available", value=round(saved["mean"], 4))
            elif saved["invalid"]:
                value["status"] = "invalid"
            value["coverage"] = {
                "valid_seconds": round(saved["seconds"], 3),
                "eligible_seconds": round(accumulator["eligible"], 3),
                "excluded_gap_seconds": round(accumulator["gap"], 3),
                "contributing_intervals": saved["count"],
                "status": "none" if not saved["count"] else "complete"
                if accumulator["geometry"] and not accumulator["gap"] and not accumulator["boundary"]
                and abs(saved["seconds"] - accumulator["eligible"]) < 1e-8 else "partial",
            }
            if frame_end is not None:
                value["coverage"]["boundary_excluded_seconds"] = round(accumulator["boundary"], 3)
            cell[name] = value
    return cells


def _halves(total):
    return [{"segment": name, "start_distance_m": index * total / 2,
             "end_distance_m": (index + 1) * total / 2, "distance_m": total / 2}
            for index, name in enumerate(("first", "second"))]


def _whole_halves(endurance, points):
    summary, halves = endurance.get("summary"), endurance.get("distance_halves")
    total = _finite_number(summary.get("distance_m")) if isinstance(summary, dict) else None
    if not isinstance(halves, dict):
        return None, "no_distance_partition"
    distances = [_finite_number(halves[name].get("distance_m"))
                 if isinstance(halves.get(name), dict) else None for name in ("first", "second")]
    if (total is None or total <= 0 or any(value is None or abs(value - total / 2) > 0.000500001
                                         for value in distances)):
        return None, "invalid_distance_partition"
    if round(points[-1][1], 2) != round(total, 2):
        return None, "distance_alignment_unverified"
    return _halves(total), None


def _available(group, items):
    if items:
        group.update(status="available", reason=None, items=items)


def segment_running_metrics(activity, decoded, endurance, *, recorded, structure, tcx_sha256, partial=False):
    result = {"schema_version": 2 if partial else 1, "activity_id": str(activity["id"]),
              "source_fit_sha256": recorded["source_fit_sha256"], "source_tcx_sha256": tcx_sha256,
              "association": recorded["association"],
              "session_start_time_utc": recorded["session_start_time_utc"],
              "session_end_time_utc": None, "session_summary_timestamp_utc": recorded["measured_at_utc"],
              "executed_intervals": _group("fit_executed_active_distance", "source_unverified"),
              "whole_distance_halves": _group("tcx_endurance_distance", "source_unverified"),
              "main_distance_halves": _group("fit_executed_main_distance" if structure["has_structured_workout"]
                                             else "tcx_endurance_distance_continuous_main", "source_unverified")}
    if partial:
        result["source_window"] = {"disposition": "unverified", "original_record_count": None,
                                   "boundary_excluded_record_count": 0, "boundary_excluded_pair_count": 0}
    if recorded["association"] != "matched" or not recorded["source_fit_sha256"]:
        return result
    sessions = decoded.get("messages", {}).get("session_mesgs")
    frame = _frame(sessions[0]) if isinstance(sessions, list) and len(sessions) == 1 and isinstance(sessions[0], dict) else None
    if frame is None or frame["start_time_utc"] != recorded["session_start_time_utc"]:
        for group in (result[name] for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves")):
            group["reason"] = "clock_basis_unverified"
        return result
    result["session_end_time_utc"] = frame["end_time_utc"]
    result["session_summary_timestamp_utc"] = frame["summary_timestamp_utc"]
    points = _points(decoded, frame, allow_terminal=partial)
    if points is None:
        for group in (result[name] for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves")):
            group["reason"] = "record_geometry_unverified"
        return result
    if partial:
        excluded = int(points[-1][0] > frame["end"])
        result["source_window"].update(
            disposition="verified_terminal_exclusion" if excluded else "verified_in_frame",
            original_record_count=len(points), boundary_excluded_record_count=excluded,
            boundary_excluded_pair_count=excluded)
    whole = result["whole_distance_halves"]
    old_whole = recorded["distance_halves"]
    cells, reason = _whole_halves(endurance, points)
    whole["reason"] = reason
    if cells is not None:
        pieces = [(row["start_distance_m"], row["end_distance_m"], points[0][0], points[-1][0], index)
                  for index, row in enumerate(cells)]
        _means(points, cells, pieces, frame_end=frame["end"] if partial else None)
        if not partial and old_whole["status"] == "available":
            for cell, old in zip(cells, old_whole["items"], strict=True):
                for name, _, _ in _RECORD_FIELDS[:-1]:
                    cell[name] = deepcopy(old[name])
        _available(whole, cells)
    main = result["main_distance_halves"]
    intervals = result["executed_intervals"]
    if not structure["has_structured_workout"]:
        main.update(deepcopy(whole))
        main["distance_basis"] = "tcx_endurance_distance_continuous_main"
        intervals["reason"] = "no_verified_structured_workout"
        return result
    windows, reason = _lap_windows(decoded, points, frame)
    if windows is None:
        intervals["reason"] = main["reason"] = reason
        return result
    main_windows = [row for row in windows if row["role"] == "main"]
    total = sum(row["distance"] for row in main_windows)
    main["reason"] = "main_distance_missing"
    if total > 0:
        cells = _halves(total)
        _available(main, _means(points, cells, _pieces(main_windows, cells), frame_end=frame["end"] if partial else None))
    groups = []
    for window in windows:
        if not window["active"]:
            continue
        if (groups and groups[-1][-1]["lap"] + 1 == window["lap"]
                and groups[-1][-1]["end"] == window["start"]
                and groups[-1][-1]["step"] == window["step"]):
            groups[-1].append(window)
        else:
            groups.append([window])
    cells, pieces = [], []
    for group in groups:
        total = sum(row["distance"] for row in group)
        if total <= 0:
            continue
        index = len(cells)
        cell = {"interval": index + 1, "source_laps": [row["lap"] for row in group],
                "workout_step_index": group[0]["step"], "distance_m": total,
                "start_time_utc": group[0]["start_time_utc"], "end_time_utc": group[-1]["end_time_utc"]}
        cells.append(cell)
        cuts = [{"start_distance_m": 0, "end_distance_m": total}]
        pieces.extend((*piece[:4], index) for piece in _pieces(group, cuts))
    intervals["reason"] = "no_verified_active_groups"
    _available(intervals, _means(points, cells, pieces, frame_end=frame["end"] if partial else None))
    return result


def recovery_running_metrics(recorded, decoded, logical, segments, tcx_sha256):
    """Original record means on the already strict verified recovery partition."""
    from .recorded_cadence import _recovery_partition

    result = {"schema_version": 1, "activity_id": recorded["activity_id"],
              "source_fit_sha256": recorded["source_fit_sha256"], "source_tcx_sha256": tcx_sha256,
              "session_start_time_utc": segments["session_start_time_utc"],
              "session_end_time_utc": segments["session_end_time_utc"],
              "source_window": deepcopy(segments["source_window"]),
              **_group("fit_verified_recovery_bout_distance", "source_unverified")}
    if recorded["association"] != "matched" or not recorded["source_fit_sha256"]:
        return result
    sessions = decoded.get("messages", {}).get("session_mesgs")
    frame = _frame(sessions[0]) if isinstance(sessions, list) and len(sessions) == 1 else None
    if frame is None or frame["start_time_utc"] != recorded["session_start_time_utc"]:
        result["reason"] = "clock_basis_unverified"
        return result
    points = _points(decoded, frame, allow_terminal=True)
    if points is None or segments["source_window"]["disposition"] == "unverified":
        result["reason"] = "record_geometry_unverified"
        return result
    cells, pieces, reason = _recovery_partition(recorded, decoded, frame, points, logical)
    result["reason"] = reason
    if cells is not None:
        _available(result, _means(points, cells, pieces, frame_end=frame["end"]))
    return result


def validate_segment_metrics(block, analysis):
    """Strict stored1.6 contract, without recomputing records or fetching sources."""
    def require(condition):
        if not condition:
            raise ValueError("invalid_segment_running_metrics")

    def shape(value, keys):
        require(isinstance(value, dict) and set(value) == set(keys))

    def number(value):
        return _finite_number(value) is not None

    def clock(value):
        return isinstance(value, str) and _utc(value) == value

    shape(block, ("schema_version", "activity_id", "source_fit_sha256", "source_tcx_sha256", "association",
                  "session_start_time_utc", "session_end_time_utc", "session_summary_timestamp_utc",
                  "executed_intervals", "whole_distance_halves", "main_distance_halves"))
    require(type(block["schema_version"]) is int and block["schema_version"] == 1)
    source, recorded = analysis["source"], analysis["recorded_running_metrics"]
    require(block["activity_id"] == str(analysis["activity_id"]))
    require(block["source_tcx_sha256"] == source["tcx_sha256"])
    require(block["source_fit_sha256"] is None or block["source_fit_sha256"] == source["fit_sha256"])
    require(block["association"] == recorded["association"])
    require(block["session_start_time_utc"] == recorded["session_start_time_utc"])
    for name in ("session_start_time_utc", "session_end_time_utc", "session_summary_timestamp_utc"):
        require(block[name] is None or clock(block[name]))
    if recorded["measured_at_utc"] is not None:
        require(block["session_summary_timestamp_utc"] == recorded["measured_at_utc"])
    start, end = _epoch(block["session_start_time_utc"]), _epoch(block["session_end_time_utc"])
    require(end is None or start is not None and end >= start)
    session_span = end - start if end is not None and start is not None else None
    structure = analysis["workout_structure"]
    main_basis = ("fit_executed_main_distance" if structure["has_structured_workout"]
                  else "tcx_endurance_distance_continuous_main")
    group_bases = {"executed_intervals": "fit_executed_active_distance",
                   "whole_distance_halves": "tcx_endurance_distance", "main_distance_halves": main_basis}
    source_rows = structure["executed_laps"]
    by_lap = {item["lap"]: item for item in source_rows}
    require(len(by_lap) == len(source_rows))
    for name, basis in group_bases.items():
        group = block[name]
        shape(group, ("status", "reason", "distance_basis", "method", "clock_basis", "interval_policy",
                      "distance_allocation", "balance_aggregation", "items"))
        require(group["distance_basis"] == basis and group["method"] == "fit_record_time_weighted_mean")
        require(group["clock_basis"] == "fit_start_time_plus_total_elapsed_time")
        require(group["interval_policy"] == "left_sample_hold_distance_progressing_max_30s")
        require(group["distance_allocation"] == "linear_between_record_distances")
        require(group["balance_aggregation"] == POLICY and isinstance(group["items"], list))
        require(group["status"] in ("available", "unavailable"))
        available = group["status"] == "available"
        require(available == bool(group["items"]))
        require(group["reason"] is None if available else isinstance(group["reason"], str) and bool(group["reason"]))
        if not available:
            continue
        require(block["association"] == "matched" and bool(block["source_fit_sha256"]) and session_span is not None)
        half = name != "executed_intervals"
        require(not half or len(group["items"]) == 2)
        previous_distance, previous_lap, previous_end = 0.0, 0, start
        for ordinal, row in enumerate(group["items"], 1):
            keys = ("segment", "start_distance_m", "end_distance_m", "distance_m") if half else (
                "interval", "source_laps", "workout_step_index", "distance_m", "start_time_utc", "end_time_utc")
            shape(row, (*keys, *(field[0] for field in _RECORD_FIELDS)))
            require(number(row["distance_m"]) and row["distance_m"] > 0)
            span = session_span
            if half:
                require(row["segment"] == ("first" if ordinal == 1 else "second"))
                a, b = row["start_distance_m"], row["end_distance_m"]
                require(number(a) and number(b) and a >= 0 and b > a)
                require(abs(a - previous_distance) <= 0.000001 and abs(b - a - row["distance_m"]) <= 0.000001)
                previous_distance = b
            else:
                require(type(row["interval"]) is int and row["interval"] == ordinal)
                require(type(row["workout_step_index"]) is int and row["workout_step_index"] >= 0)
                require(clock(row["start_time_utc"]) and clock(row["end_time_utc"]))
                first, last = _epoch(row["start_time_utc"]), _epoch(row["end_time_utc"])
                require(previous_end <= first < last <= end)
                previous_end, span = last, last - first
                laps = row["source_laps"]
                require(isinstance(laps, list) and bool(laps))
                require(all(type(lap) is int and lap > 0 for lap in laps))
                require(laps[0] > previous_lap and all(b == a + 1 for a, b in zip(laps, laps[1:], strict=False)))
                previous_lap = laps[-1]
                require(all(lap in by_lap for lap in laps))
                source_rows = [by_lap[lap] for lap in laps]
                frames = []
                for original in source_rows:
                    require(original["workout_step_index"] == row["workout_step_index"]
                            and str(original["intensity"] or "").lower() == "active")
                    frame = _frame({"start_time": original["start_time"],
                                    "total_elapsed_time": original["elapsed_seconds"]})
                    require(frame is not None)
                    frames.append(frame)
                require(all(a["end"] == b["start"] for a, b in zip(frames, frames[1:], strict=False)))
                require(frames[0]["start_time_utc"] == row["start_time_utc"]
                        and frames[-1]["end_time_utc"] == row["end_time_utc"])
                distances = [original["distance_m"] for original in source_rows]
                require(all(number(value) and value >= 0 for value in distances)
                        and abs(sum(distances) - row["distance_m"]) <= 0.001)
            common = None
            for metric_name, source_field, unit in _RECORD_FIELDS:
                metric = row[metric_name]
                keys = ("status", "value", "unit", "source_field", "coverage")
                if source_field == "stance_time_balance":
                    keys = (*keys, "side_semantics")
                shape(metric, keys)
                require(metric["unit"] == unit and metric["source_field"] == source_field)
                require(metric["status"] in ("available", "missing", "invalid", "unavailable"))
                if source_field == "stance_time_balance":
                    require(metric["side_semantics"] == "unknown")
                covered = metric["coverage"]
                shape(covered, ("valid_seconds", "eligible_seconds", "excluded_gap_seconds", "contributing_intervals", "status"))
                require(all(number(covered[key]) and covered[key] >= 0
                            for key in ("valid_seconds", "eligible_seconds", "excluded_gap_seconds")))
                count = covered["contributing_intervals"]
                require(type(count) is int and count >= 0 and covered["status"] in ("none", "partial", "complete"))
                valid, eligible, gap = (covered[key] for key in ("valid_seconds", "eligible_seconds", "excluded_gap_seconds"))
                require(valid <= eligible + 0.001 and eligible + gap <= span + 0.001)
                require((covered["status"] == "none") == (count == 0))
                require(count > 0 or valid == 0)
                require(covered["status"] != "complete" or gap == 0 and abs(valid - eligible) <= 0.001)
                require((metric["status"] == "available") == (count > 0))
                require(number(metric["value"]) if count else metric["value"] is None)
                pair = eligible, gap
                require(common is None or common == pair)
                common = pair
        if half:
            items = group["items"]
            total = items[-1]["end_distance_m"]
            total_span = session_span
            require(all(abs(item["distance_m"] - total / 2) <= 0.000500001 for item in items))
            if name == "whole_distance_halves" or basis == "tcx_endurance_distance_continuous_main":
                require(number(analysis["summary"]["distance_m"])
                        and abs(total - analysis["summary"]["distance_m"]) <= 0.001)
            else:
                sections = [section for section in analysis["sections"] if section["section"] == "main"]
                require(len(sections) == 1 and number(sections[0]["distance_m"])
                        and abs(total - sections[0]["distance_m"]) <= 0.001)
                total_span = sections[0]["elapsed_seconds"]
                require(number(total_span) and total_span >= 0)
            require(sum(item["average_power"]["coverage"]["eligible_seconds"]
                        + item["average_power"]["coverage"]["excluded_gap_seconds"]
                        for item in items) <= total_span + 0.002)


def validate_partial_segment_metrics(block, analysis):
    """Schema2 extends, but never broadens, the genuine schema1 validator."""
    def require(condition):
        if not condition:
            raise ValueError("invalid_segment_running_metrics")

    require(isinstance(block, dict) and type(block.get("schema_version")) is int and block["schema_version"] == 2)
    window = block.get("source_window")
    require(isinstance(window, dict) and set(window) == {
        "disposition", "original_record_count", "boundary_excluded_record_count", "boundary_excluded_pair_count"})
    disposition = window["disposition"]
    require(disposition in ("unverified", "verified_in_frame", "verified_terminal_exclusion"))
    count = window["original_record_count"]
    excluded = 1 if disposition == "verified_terminal_exclusion" else 0
    require(all(type(window[name]) is int and window[name] == excluded
                for name in ("boundary_excluded_record_count", "boundary_excluded_pair_count")))
    require(count is None if disposition == "unverified" else type(count) is int and 2 <= count <= 2**53 - 1)
    if disposition != "unverified":
        require(block["association"] == "matched" and bool(block["source_fit_sha256"])
                and block["session_start_time_utc"] is not None and block["session_end_time_utc"] is not None)
    require(block["source_fit_sha256"] == analysis["recorded_running_metrics"]["source_fit_sha256"])
    legacy = deepcopy(block)
    del legacy["source_window"]
    legacy["schema_version"] = 1
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        group = legacy[name]
        require(disposition != "unverified" or group["status"] != "available")
        for row in group["items"]:
            common = None
            for field, _, _ in _RECORD_FIELDS:
                coverage = row[field]["coverage"]
                require(set(coverage) == {"valid_seconds", "eligible_seconds", "excluded_gap_seconds",
                                          "boundary_excluded_seconds", "contributing_intervals", "status"})
                boundary = coverage.pop("boundary_excluded_seconds")
                require(_finite_number(boundary) is not None and boundary >= 0)
                require(disposition == "verified_terminal_exclusion" or boundary == 0)
                require(boundary == 0 or coverage["status"] != "complete")
                require(type(coverage["contributing_intervals"]) is int and count is not None
                        and coverage["contributing_intervals"] <= count - 1 - excluded)
                require(common is None or common == boundary)
                common = boundary
    validate_segment_metrics(legacy, analysis)
    session_span = (_epoch(block["session_end_time_utc"]) - _epoch(block["session_start_time_utc"])
                    if disposition != "unverified" else None)
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        group = block[name]
        if group["status"] != "available":
            continue
        span = session_span
        if name == "main_distance_halves" and group["distance_basis"] == "fit_executed_main_distance":
            span = next(row["elapsed_seconds"] for row in analysis["sections"] if row["section"] == "main")
            require(_finite_number(span) is not None and span >= 0)
        total = 0.0
        for row in group["items"]:
            limit = span
            if name == "executed_intervals":
                limit = _epoch(row["end_time_utc"]) - _epoch(row["start_time_utc"])
            coverage = row["average_power"]["coverage"]
            duration = (coverage["eligible_seconds"] + coverage["excluded_gap_seconds"]
                        + coverage["boundary_excluded_seconds"])
            require(duration <= limit + 0.001)
            total += duration
        if name != "executed_intervals":
            require(total <= span + 0.002)
