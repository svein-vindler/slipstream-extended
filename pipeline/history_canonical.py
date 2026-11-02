"""Pure history preparation through existing canonical collectors."""

from __future__ import annotations

import copy
import gzip
import io
import json
import math
from datetime import date, datetime, timezone

from .granular import normalize_hrv
from .history_state import Stop, digest
from .signal_series import prepare_signal_day, signal_key


def decode(body):
    if body.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as handle:
            body = handle.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise Stop("canonical_body_bound")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise Stop("canonical_shape")
    return value


def key(stream, day):
    date.fromisoformat(day)
    return signal_key(day) if stream == "hr" else f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"


def _validate_missing_hr(day, raw):
    """One verified legacy envelope declares absence; summaries are not samples."""
    clocks = ("startTimestampGMT", "endTimestampGMT", "startTimestampLocal", "endTimestampLocal")
    summaries = ("restingHeartRate", "lastSevenDaysAvgRestingHeartRate")
    expected = {
        "userProfilePK",
        "calendarDate",
        "heartRateValues",
        "heartRateValueDescriptors",
        "maxHeartRate",
        "minHeartRate",
        *clocks,
        *summaries,
    }
    try:
        if (
            set(raw) != expected
            or type(raw["userProfilePK"]) is not int
            or any(
                raw[k] is not None
                for k in (
                    "heartRateValues",
                    "heartRateValueDescriptors",
                    "maxHeartRate",
                    "minHeartRate",
                )
            )
            or any(type(raw[k]) not in (int, float) or not math.isfinite(raw[k]) for k in summaries)
        ):
            raise Stop("invalid_null_hr")
    except OverflowError:
        raise Stop("invalid_null_hr") from None
    for field in clocks:
        value = raw[field]
        try:
            if not isinstance(value, str) or len(value) <= 10 or value[10] not in {"T", " "}:
                raise ValueError()
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if abs((instant.date() - date.fromisoformat(day)).days) > 1:
                raise ValueError()
        except ValueError:
            raise Stop("invalid_null_hr_clock") from None


def prepare(stream, day, raw, previous=None, *, wire=b""):
    if not isinstance(raw, dict):
        raise Stop("source_shape")
    declarations = [raw[k] for k in ("calendarDate", "date", "summaryDate") if k in raw]
    if isinstance(raw.get("hrvSummary"), dict) and "calendarDate" in raw["hrvSummary"]:
        declarations.append(raw["hrvSummary"]["calendarDate"])
    if not declarations or any(d != day for d in declarations):
        raise Stop("wrong_date")
    if stream == "hr":
        points = raw.get("heartRateValues")
        if "heartRateValues" in raw and points is None:
            _validate_missing_hr(day, raw)
            return None
        if isinstance(points, list):
            for point in points:
                if (
                    not isinstance(point, list)
                    or len(point) != 2
                    or type(point[0]) not in (int, float)
                    or not math.isfinite(point[0])
                ):
                    raise Stop("invalid_source_clock")
                epoch = point[0] / 1000 if point[0] > 4102444800 else point[0]
                instant = datetime.fromtimestamp(epoch, timezone.utc)
                if abs((instant.date() - date.fromisoformat(day)).days) > 1:
                    raise Stop("source_clock_outside_date")

        value = prepare_signal_day(day, {"heart_rate": raw})
        channel = value["channels"]["heart_rate"]
        if channel["status"] == "no_data":
            return None
        if channel["status"] != "available":
            # An explicit all-null HR response is unresolved, not invalid absence.
            points = raw.get("heartRateValues")
            if (
                isinstance(points, list)
                and points
                and channel.get("missing_value_count") == len(points)
            ):
                return None
            raise Stop("invalid_source")
        return value
    if stream != "hrv":
        raise Stop("unsupported")
    filtered = copy.deepcopy(raw)
    nulls = []
    points = filtered.get("hrvReadings")
    if not isinstance(points, list):
        raise Stop("invalid_hrv")
    positive = []
    for ordinal, point in enumerate(points):
        if isinstance(point, dict) and "hrvValue" in point and point["hrvValue"] is None:
            if set(point) != {"hrvValue", "readingTimeGMT", "readingTimeLocal"}:
                raise Stop("invalid_null_hrv")
            for field in ("readingTimeGMT", "readingTimeLocal"):
                value = point[field]
                if not isinstance(value, str) or "T" not in value:
                    raise Stop("invalid_null_hrv_clock")
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if abs((parsed.date() - date.fromisoformat(day)).days) > 1:
                    raise Stop("invalid_null_hrv_clock")
            nulls.append({"source_ordinal": ordinal, **point})
        else:
            positive.append(point)
    filtered["hrvReadings"] = positive
    value = normalize_hrv(day, filtered)
    if not value["readings"]:
        return None
    result = copy.deepcopy(previous) if previous is not None else value
    result.update(readings=value["readings"], reading_count=len(value["readings"]), available=True)
    result.pop("unavailable_reason", None)
    if nulls:
        result["archive_hrv_quality_v1"] = {
            "source_wire_sha256": digest(wire),
            "missing_value_count": len(nulls),
            "missing_readings": nulls,
        }
    return result


def has_curve(stream, day, value):
    if (
        value.get("schema_version") != 1
        or value.get("source") != "garmin"
        or value.get("date") != day
    ):
        raise Stop("stored_schema")
    if stream == "hr":
        channel = value.get("channels", {}).get("heart_rate", {})
        if channel.get("status") != "available" or not channel.get("readings"):
            return False
        if channel.get("unit") != "bpm":
            raise Stop("stored_hr_unit")
        raw = {
            "calendarDate": day,
            "heartRateValues": [[r["timestamp"], r["value"]] for r in channel["readings"]],
        }
        if any(r.get("time_basis") != "utc" for r in channel["readings"]):
            raise Stop("stored_clock")
        return prepare(stream, day, raw) is not None
    rows = value.get("readings", [])
    if not isinstance(rows, list):
        raise Stop("stored_schema")
    if not rows:
        return False
    normalize_hrv(
        day,
        {
            "calendarDate": day,
            "hrvReadings": [{"timestamp": r["timestamp"], "hrvValue": r["hrv_ms"]} for r in rows],
        },
    )
    return True
