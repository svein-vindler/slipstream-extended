"""Fetch daily Garmin health summaries without storing raw time-series data."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from .garmin import _login


def _days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _chunks(start: date, end: date, size: int = 28):
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=size - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _first(mapping: dict[str, Any], *keys: str):
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    return None


def _number(value: Any, digits: int = 1):
    if value is None or value == "":
        return None
    try:
        result = round(float(value), digits)
    except (TypeError, ValueError):
        return None
    return int(result) if result == int(result) else result


def _date_of(row: dict[str, Any]):
    return _first(row, "calendarDate", "date", "summaryDate")


def _call(label: str, fn: Callable[[], Any], pause: float):
    """Retry rate limits, while allowing unavailable metrics to be skipped."""
    delays = (30, 60, 120)
    for attempt in range(len(delays) + 1):
        try:
            result = fn()
            if pause:
                time.sleep(pause)
            return result
        except Exception as exc:  # garminconnect exposes several exception types
            rate_limited = "TooManyRequests" in type(exc).__name__ or "429" in str(exc)
            if rate_limited and attempt < len(delays):
                wait = delays[attempt]
                print(f"[garmin-health] rate limited in {label}; waiting {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            print(f"[garmin-health] skipped {label}: {exc}", file=sys.stderr)
            return None
    return None


def _heart_values(raw: dict[str, Any]):
    values = []
    for item in raw.get("heartRateValues") or []:
        value = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else None
        number = _number(value)
        if number is not None and number > 0:
            values.append(number)
    return values


def _sleep_fields(raw: dict[str, Any]):
    dto = raw.get("dailySleepDTO") if isinstance(raw.get("dailySleepDTO"), dict) else raw
    scores = dto.get("sleepScores") if isinstance(dto.get("sleepScores"), dict) else {}
    overall = scores.get("overall") if isinstance(scores.get("overall"), dict) else {}
    return {
        "Sleep Seconds": _first(dto, "sleepTimeSeconds", "sleepSeconds", "totalSleepSeconds"),
        "Deep Sleep Seconds": _first(dto, "deepSleepSeconds", "deepSeconds"),
        "Light Sleep Seconds": _first(dto, "lightSleepSeconds", "lightSeconds"),
        "REM Sleep Seconds": _first(dto, "remSleepSeconds", "remSeconds"),
        "Awake Sleep Seconds": _first(dto, "awakeSleepSeconds", "awakeSeconds"),
        "Sleep Score": _first(dto, "sleepScore") or overall.get("value"),
    }


def _hrv_rows(raw: Any):
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return []
    for key in ("hrvSummaries", "hrvSummaryList", "dailyHrvSummaries"):
        if isinstance(raw.get(key), list):
            return raw[key]
    summary = raw.get("hrvSummary")
    return [summary] if isinstance(summary, dict) else []


def _weight_kg(value: Any):
    number = _number(value, 3)
    if number is None:
        return None
    return _number(number / 1000, 3) if number > 500 else number


def _weight_rows(raw: Any):
    """Normalize both legacy weigh-in rows and Garmin's daily summaries."""
    if not isinstance(raw, dict):
        return []
    items = (
        raw.get("dateWeightList")
        or raw.get("weightList")
        or raw.get("dailyWeightSummaries")
        or []
    )
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            continue
        latest = item.get("latestWeight")
        detail = latest if isinstance(latest, dict) else item
        day = _date_of(detail) or _date_of(item)
        value = _first(detail, "weight", "value")
        if value is None:
            value = _first(item, "latestWeightValue", "maxWeight", "minWeight")
        normalized.append((day, _weight_kg(value)))
    return normalized


def fetch(start: date, end: date, *, request_pause: float = 0.15) -> list[dict[str, object]]:
    if start > end:
        raise ValueError("health start date cannot be after end date")
    g = _login()
    rows: dict[str, dict[str, object]] = {}

    def merge(day: Any, values: dict[str, Any]):
        day = str(day or "")[:10]
        clean = {key: value for key, value in values.items() if value is not None and value != ""}
        if not day or not clean:
            return
        rows.setdefault(day, {"Date": day, "Source": "garmin"}).update(clean)

    for chunk_start, chunk_end in _chunks(start, end):
        first, last = chunk_start.isoformat(), chunk_end.isoformat()

        for item in _call(
            "steps",
            lambda first=first, last=last: g.get_daily_steps(first, last),
            request_pause,
        ) or []:
            if isinstance(item, dict):
                merge(_date_of(item), {"Steps": _first(item, "totalSteps", "steps")})

        for item in _call(
            "sleep",
            lambda first=first, last=last: g.get_sleep_daily(first, last),
            request_pause,
        ) or []:
            if isinstance(item, dict):
                merge(_date_of(item), _sleep_fields(item))

        for item in _hrv_rows(_call(
            "HRV",
            lambda first=first, last=last: g.get_hrv_data_range(first, last),
            request_pause,
        )):
            if isinstance(item, dict):
                merge(_date_of(item), {
                    "HRV Weekly Average": _first(item, "weeklyAvg", "weeklyAverage"),
                    "HRV Last Night Average": _first(item, "lastNightAvg", "lastNightAverage"),
                    "HRV Status": item.get("status"),
                })

        for item in _call(
            "Body Battery",
            lambda first=first, last=last: g.get_body_battery(first, last),
            request_pause,
        ) or []:
            if not isinstance(item, dict):
                continue
            samples = [
                _number(sample[1]) for sample in item.get("bodyBatteryValuesArray") or []
                if isinstance(sample, (list, tuple)) and len(sample) > 1
                and _number(sample[1]) is not None and _number(sample[1]) >= 0
            ]
            highest = _first(item, "highest", "bodyBatteryHighestValue")
            lowest = _first(item, "lowest", "bodyBatteryLowestValue")
            merge(_date_of(item), {
                "Body Battery Highest": highest if highest is not None else (max(samples) if samples else None),
                "Body Battery Lowest": lowest if lowest is not None else (min(samples) if samples else None),
                "Body Battery Charged": _first(item, "charged", "bodyBatteryChargedValue"),
                "Body Battery Drained": _first(item, "drained", "bodyBatteryDrainedValue"),
            })

        weights = _call(
            "weight",
            lambda first=first, last=last: g.get_weigh_ins(first, last),
            request_pause,
        ) or {}
        for weight_day, weight_kg in _weight_rows(weights):
            merge(weight_day, {"Weight KG": weight_kg})

    total_days = (end - start).days + 1
    for index, current in enumerate(_days(start, end), start=1):
        day = current.isoformat()
        if index == 1 or index % 30 == 0 or index == total_days:
            print(f"[garmin-health] daily summaries {index}/{total_days}: {day}", file=sys.stderr)

        stats = _call(f"daily stats {day}", lambda d=day: g.get_stats(d), request_pause) or {}
        if isinstance(stats, dict):
            merge(day, {
                "Steps": _first(stats, "totalSteps", "steps"),
                "Resting Heart Rate": stats.get("restingHeartRate"),
                "Minimum Heart Rate": stats.get("minHeartRate"),
                "Maximum Heart Rate": stats.get("maxHeartRate"),
                "Body Battery Highest": stats.get("bodyBatteryHighestValue"),
                "Body Battery Lowest": stats.get("bodyBatteryLowestValue"),
                "Body Battery Charged": stats.get("bodyBatteryChargedValue"),
                "Body Battery Drained": stats.get("bodyBatteryDrainedValue"),
                "Average Stress": stats.get("averageStressLevel"),
                "Maximum Stress": stats.get("maxStressLevel"),
                "Stress Duration Seconds": stats.get("stressDuration"),
            })

        sleep = _call(f"sleep details {day}", lambda d=day: g.get_sleep_data(d), request_pause) or {}
        if isinstance(sleep, dict):
            dto = sleep.get("dailySleepDTO") if isinstance(sleep.get("dailySleepDTO"), dict) else sleep
            merge(_date_of(dto) or day, _sleep_fields(sleep))

        heart = _call(f"heart rate {day}", lambda d=day: g.get_heart_rates(d), request_pause) or {}
        if isinstance(heart, dict):
            values = _heart_values(heart)
            minimum = heart.get("minHeartRate")
            maximum = heart.get("maxHeartRate")
            merge(day, {
                "Resting Heart Rate": heart.get("restingHeartRate"),
                "Minimum Heart Rate": minimum if minimum is not None else (min(values) if values else None),
                "Maximum Heart Rate": maximum if maximum is not None else (max(values) if values else None),
                "Average Heart Rate": _number(sum(values) / len(values), 1) if values else None,
            })

        respiration = _call(f"respiration {day}", lambda d=day: g.get_respiration_data(d), request_pause) or {}
        if isinstance(respiration, dict):
            merge(day, {
                "Average Respiration": _first(respiration, "avgWakingRespirationValue", "averageRespirationValue", "avgRespirationValue"),
                "Lowest Respiration": _first(respiration, "lowestRespirationValue", "minRespirationValue"),
                "Highest Respiration": _first(respiration, "highestRespirationValue", "maxRespirationValue"),
            })

    return [rows[day] for day in sorted(rows)]
