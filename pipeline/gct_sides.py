"""Explicit activity/FIT-scoped user declarations, never manufacturer inference."""
import math
import re


def valid_declaration(value):
    return (isinstance(value, dict)
            and set(value) == {"raw_side", "source_fit_sha256", "source"}
            and value["raw_side"] in ("left", "right")
            and value["source"] == "user_activity_declaration"
            and isinstance(value["source_fit_sha256"], str)
            and re.fullmatch(r"[a-f0-9]{64}", value["source_fit_sha256"]) is not None)


def ground_contact_sides(metrics, context, *, segments=None):
    """Project only associated session/actual laps; raw metrics remain untouched."""
    declaration = context.get("gct_balance_side") if isinstance(context, dict) else None
    ready = metrics["association"] == "matched" and bool(metrics["source_fit_sha256"])
    valid = (isinstance(context, dict) and type(context.get("schema_version")) is int
             and context["schema_version"] == 2 and context.get("activity_id") == metrics["activity_id"]
             and context.get("source") == "user"
             and (metrics["local_date"] is None or context.get("activity_date") == metrics["local_date"])
             and isinstance(context.get("context_id"), str) and bool(context["context_id"])
             and valid_declaration(declaration))
    matched = ready and valid and declaration["source_fit_sha256"] == metrics["source_fit_sha256"]
    reason = ("source_unavailable" if not ready else "declaration_missing" if declaration is None
              else "invalid_declaration" if not valid else "source_mismatch" if not matched else None)

    def project(metric):
        result = {"status": "unverified" if ready else "unavailable",
                  "left_percent": None, "right_percent": None,
                  "recorded_side": None, "calculated_side": None, "method": "not_computed"}
        if not matched:
            return result
        value = metric["value"]
        status = metric["status"]
        if status == "available":
            try:
                valid_value = type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 100
            except OverflowError:
                valid_value = False
            status = "available" if valid_value else "invalid"
        result["status"] = status
        if status == "available":
            side = declaration["raw_side"]
            other = "right" if side == "left" else "left"
            result.update({side + "_percent": value, other + "_percent": 100 - value,
                           "recorded_side": side, "calculated_side": other,
                           "method": "recorded_and_complement"})
        return result

    laps = metrics["laps"]
    result = {
        "schema_version": 1, "activity_id": metrics["activity_id"],
        "source_fit_sha256": metrics["source_fit_sha256"],
        "status": "available" if matched else "unverified" if ready else "unavailable",
        "reason": reason,
        "declaration": {**declaration, "context_id": context["context_id"]} if matched else None,
        "session": project(metrics["average_ground_contact_balance"]),
        "laps": {"status": laps["status"], "reason": laps["reason"], "items": [
            {"lap": lap["lap"], "start_time_utc": lap["start_time_utc"],
             "end_time_utc": lap["end_time_utc"], **project(lap["average_ground_contact_balance"])}
            for lap in laps["items"]]},
    }
    if segments is None:
        return result
    # New current projections remain distinct from genuine schema1 history.
    result["schema_version"] = 2
    result["record_aggregates"] = {}
    associated = (isinstance(segments, dict) and segments.get("association") == "matched"
                  and segments.get("activity_id") == metrics["activity_id"]
                  and segments.get("source_fit_sha256") == metrics["source_fit_sha256"]
                  and segments.get("session_start_time_utc") == metrics["session_start_time_utc"])
    for name in ("executed_intervals", "whole_distance_halves", "main_distance_halves"):
        source_group = segments.get(name, {}) if associated else {}
        source_ready = source_group.get("status") == "available"
        group = {"status": "available" if matched and source_ready else "unverified" if ready else "unavailable",
                 "reason": reason or source_group.get("reason") or (None if source_ready else "source_unavailable"),
                 "source_metric": f"segment_running_metrics.{name}.average_ground_contact_balance", "items": []}
        if source_ready:
            for row in source_group["items"]:
                metric = row["average_ground_contact_balance"]
                projected = project(metric)
                identity = {key: row[key] for key in ("interval", "segment", "source_laps") if key in row}
                group["items"].append({**identity, "status": projected["status"],
                                       "left_percent": projected["left_percent"], "right_percent": projected["right_percent"],
                                       "raw_side": projected["recorded_side"], "calculated_side": projected["calculated_side"],
                                       "both_sides_derived": True,
                                       "method": "user_declared_record_mean_and_complement"
                                       if projected["status"] == "available" else "not_computed",
                                       "coverage": dict(metric["coverage"])})
        result["record_aggregates"][name] = group
    return result
