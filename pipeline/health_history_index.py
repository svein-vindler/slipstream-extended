"""Build compact monthly HRV and sleep indexes for bounded MCP history queries.

The canonical per-day objects remain unchanged.  This module reads those private
R2 objects, derives small allow-listed summaries locally, and writes one gzip
index per month.  It never calls Garmin.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from .granular import gzip_json
from .r2_store import R2Store

SCHEMA_VERSION = 1
BUILDER_REVISION = 2
STREAMS = ("hrv", "sleep")
SOURCE_PREFIXES = {
    "hrv": "health/hrv/",
    "sleep": "health/sleep/v1/",
}
INDEX_PREFIXES = {
    "hrv": "health/indexes/hrv/v1/",
    "sleep": "health/indexes/sleep/v1/",
}


def index_key(stream: str, month: str) -> str:
    _validate_stream(stream)
    return f"{INDEX_PREFIXES[stream]}{month[:4]}/{month[5:7]}.json"


def _validate_stream(stream: str) -> None:
    if stream not in STREAMS:
        raise ValueError(f"Unsupported health history stream: {stream}")


def _decode_json(data: bytes, key: str) -> dict[str, Any]:
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"R2 JSON object is invalid: {key}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"R2 JSON object must contain an object: {key}")
    return value


def _number(value: Any) -> float | int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    rounded = round(result, 3)
    return int(rounded) if rounded == int(rounded) else rounded


def _first_number(mapping: dict[str, Any], *keys: str) -> float | int | None:
    for key in keys:
        number = _number(mapping.get(key))
        if number is not None:
            return number
    return None


def _first_string(mapping: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)


def _timestamp_seconds(value: Any) -> float | None:
    number = _number(value)
    if number is not None:
        number = float(number)
        return number / 1000 if number > 10_000_000_000 else number
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _linear_slope_per_hour(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    origin = points[0][0]
    xs = [(timestamp - origin) / 3600 for timestamp, _ in points]
    ys = [value for _, value in points]
    x_mean = mean(xs)
    y_mean = mean(ys)
    denominator = sum((value - x_mean) ** 2 for value in xs)
    if denominator <= 0:
        return None
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True)) / denominator
    return round(slope, 3)


def summarize_hrv_payload(day: str, payload: dict[str, Any]) -> dict[str, Any]:
    readings = payload.get("readings")
    if not isinstance(readings, list):
        readings = []

    values: list[float] = []
    timed: list[tuple[float, float]] = []
    for reading in readings:
        if not isinstance(reading, dict):
            continue
        value = _number(reading.get("hrv_ms"))
        if value is None:
            continue
        numeric = float(value)
        values.append(numeric)
        timestamp = _timestamp_seconds(reading.get("timestamp"))
        if timestamp is not None:
            timed.append((timestamp, numeric))

    summary = payload.get("summary")
    summary = summary if isinstance(summary, dict) else {}
    garmin = {
        "last_night_avg_ms": _first_number(
            summary, "lastNightAvg", "lastNightAverage", "lastNightAvgMs"
        ),
        "last_night_5_min_high_ms": _first_number(
            summary, "lastNight5MinHigh", "lastNightFiveMinHigh", "lastNight5MinHighMs"
        ),
        "weekly_avg_ms": _first_number(summary, "weeklyAvg", "weeklyAverage", "weeklyAvgMs"),
        "status": _first_string(summary, "status", "statusKey", "hrvStatus"),
        "baseline_low_ms": _first_number(
            summary, "baselineBalancedLower", "baselineLow", "baselineLower"
        ),
        "baseline_high_ms": _first_number(
            summary, "baselineBalancedUpper", "baselineHigh", "baselineUpper"
        ),
    }
    if not values and not any(value is not None for value in garmin.values()):
        return {"date": day, "status": "no_data"}
    midpoint = len(values) // 2
    first = values[:midpoint]
    second = values[midpoint:]
    first_mean = round(mean(first), 3) if first else None
    second_mean = round(mean(second), 3) if second else None
    derived = {
        "valid_reading_count": len(values),
        "minimum_ms": round(min(values), 3) if values else None,
        "maximum_ms": round(max(values), 3) if values else None,
        "mean_ms": round(mean(values), 3) if values else None,
        "median_ms": round(median(values), 3) if values else None,
        "p10_ms": _percentile(values, 0.10),
        "p90_ms": _percentile(values, 0.90),
        "first_half_mean_ms": first_mean,
        "second_half_mean_ms": second_mean,
        "second_minus_first_ms": round(second_mean - first_mean, 3)
        if first_mean is not None and second_mean is not None
        else None,
        "slope_ms_per_hour": _linear_slope_per_hour(sorted(timed)),
    }
    return {
        "date": day,
        "status": "available",
        "detailed_readings_available": bool(values),
        "sleep_start_gmt": payload.get("sleep_start_gmt"),
        "sleep_end_gmt": payload.get("sleep_end_gmt"),
        "sleep_start_garmin_local": payload.get("sleep_start_garmin_local"),
        "sleep_end_garmin_local": payload.get("sleep_end_garmin_local"),
        "garmin": garmin,
        "derived": derived,
    }


def summarize_sleep_payload(day: str, payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return {"date": day, "status": "invalid_schema"}
    if summary.get("sleep_seconds") is None and not payload.get("stages"):
        return {"date": day, "status": "no_data"}
    allowed_summary = {
        key: _number(summary.get(key))
        for key in (
            "sleep_seconds",
            "deep_sleep_seconds",
            "light_sleep_seconds",
            "rem_sleep_seconds",
            "awake_sleep_seconds",
            "unmeasurable_sleep_seconds",
            "nap_seconds",
            "sleep_score",
            "average_spo2_percent",
            "lowest_spo2_percent",
            "average_respiration_brpm",
            "lowest_respiration_brpm",
            "highest_respiration_brpm",
            "average_sleep_stress",
        )
    }
    score_breakdown: dict[str, dict[str, Any]] = {}
    raw_breakdown = payload.get("score_breakdown")
    if isinstance(raw_breakdown, dict):
        for name, item in raw_breakdown.items():
            if not isinstance(item, dict):
                continue
            qualifier = item.get("qualifier")
            score_breakdown[str(name)] = {
                "value": _number(item.get("value")),
                "qualifier": qualifier if isinstance(qualifier, str) else None,
            }
    return {
        "date": day,
        "status": "available",
        "sleep_start_gmt": payload.get("sleep_start_gmt"),
        "sleep_end_gmt": payload.get("sleep_end_gmt"),
        "sleep_start_garmin_local": payload.get("sleep_start_garmin_local"),
        "sleep_end_garmin_local": payload.get("sleep_end_garmin_local"),
        "confirmed": payload.get("confirmed")
        if isinstance(payload.get("confirmed"), bool)
        else None,
        "summary": allowed_summary,
        "score_breakdown": score_breakdown,
        "stage_count": int(payload.get("stage_count") or 0),
    }


def _source_date(key: str) -> str | None:
    filename = key.rsplit("/", 1)[-1]
    day = filename.removesuffix(".gz").removesuffix(".json")
    try:
        return date.fromisoformat(day).isoformat()
    except ValueError:
        return None


def _preferred_sources(revisions: dict[str, str]) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for key, revision in sorted(revisions.items(), reverse=True):
        day = _source_date(key)
        if day is None:
            continue
        current = result.get(day)
        if current is None or (current[0].endswith(".json.gz") and key.endswith(".json")):
            result[day] = (key, revision)
    return result


def _month_sources(revisions: dict[str, str]) -> dict[str, dict[str, tuple[str, str]]]:
    grouped: dict[str, dict[str, tuple[str, str]]] = {}
    for day, source in _preferred_sources(revisions).items():
        grouped.setdefault(day[:7], {})[day] = source
    return grouped


def build_month_index(
    stream: str,
    month: str,
    sources: dict[str, tuple[str, str]],
    store: R2Store,
) -> dict[str, Any]:
    _validate_stream(stream)
    days = []
    for day, (key, _) in sorted(sources.items()):
        try:
            payload = _decode_json(store.get(key), key)
            summary = (
                summarize_hrv_payload(day, payload)
                if stream == "hrv"
                else summarize_sleep_payload(day, payload)
            )
        except (OSError, ValueError, TypeError):
            summary = {"date": day, "status": "invalid_schema"}
        days.append(summary)
    return {
        "schema_version": SCHEMA_VERSION,
        "builder_revision": BUILDER_REVISION,
        "kind": f"slipstream-{stream}-month-index",
        "month": month,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_revisions": {key: revision for _, (key, revision) in sorted(sources.items())},
        "days": days,
    }


def _selected_months(
    available: Iterable[str],
    *,
    start_date: str | None,
    end_date: str | None,
    recent_months: int | None,
) -> list[str]:
    months = sorted(set(available))
    if start_date:
        start = date.fromisoformat(start_date)
        months = [month for month in months if month >= start.isoformat()[:7]]
    if end_date:
        end = date.fromisoformat(end_date)
        months = [month for month in months if month <= end.isoformat()[:7]]
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    if recent_months is not None:
        if recent_months <= 0:
            raise ValueError("recent_months must be greater than zero")
        months = months[-recent_months:]
    return months


def sync_stream(
    stream: str,
    *,
    store: R2Store,
    start_date: str | None = None,
    end_date: str | None = None,
    recent_months: int | None = None,
    only_dates: Iterable[str] | None = None,
) -> dict[str, Any]:
    _validate_stream(stream)
    source_revisions = store.list_object_revisions(SOURCE_PREFIXES[stream])
    grouped = _month_sources(source_revisions)
    selected = _selected_months(
        grouped,
        start_date=start_date,
        end_date=end_date,
        recent_months=recent_months,
    )
    if only_dates is not None:
        requested = {date.fromisoformat(day).isoformat()[:7] for day in only_dates}
        selected = [month for month in selected if month in requested]

    index_revisions = store.list_object_revisions(INDEX_PREFIXES[stream])
    written: list[dict[str, Any]] = []
    unchanged: list[str] = []
    for position, month in enumerate(selected, start=1):
        key = index_key(stream, month)
        expected_revisions = {
            source_key: revision for _, (source_key, revision) in sorted(grouped[month].items())
        }
        if key in index_revisions:
            try:
                current = _decode_json(store.get(key), key)
            except ValueError:
                current = {}
            if (
                current.get("builder_revision") == BUILDER_REVISION
                and current.get("source_revisions") == expected_revisions
            ):
                unchanged.append(month)
                if position % 10 == 0 or position == len(selected):
                    print(
                        f"[health-index] {stream} {position}/{len(selected)} months checked",
                        file=sys.stderr,
                    )
                continue
        payload = build_month_index(stream, month, grouped[month], store)
        data = gzip_json(payload)
        store.put(key, data, "application/json", encoding="gzip")
        written.append({"month": month, "days": len(payload["days"]), "bytes": len(data)})
        if position % 10 == 0 or position == len(selected):
            print(
                f"[health-index] {stream} {position}/{len(selected)} months checked",
                file=sys.stderr,
            )
    return {
        "stream": stream,
        "months_considered": len(selected),
        "months_written": written,
        "months_unchanged": unchanged,
    }


def sync_dates(store: R2Store, stream: str, days: Iterable[str]) -> dict[str, Any]:
    normalized = sorted({date.fromisoformat(day).isoformat() for day in days})
    if not normalized:
        return {
            "stream": stream,
            "months_considered": 0,
            "months_written": [],
            "months_unchanged": [],
        }
    return sync_stream(stream, store=store, only_dates=normalized)


def run(
    *,
    streams: Iterable[str] = STREAMS,
    start_date: str | None = None,
    end_date: str | None = None,
    recent_months: int | None = None,
    store: R2Store | None = None,
) -> dict[str, Any]:
    store = store or R2Store()
    results = {
        stream: sync_stream(
            stream,
            store=store,
            start_date=start_date,
            end_date=end_date,
            recent_months=recent_months,
        )
        for stream in dict.fromkeys(streams)
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "complete",
        "results": results,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def main() -> None:
    from .local_bootstrap import load_env_file

    parser = argparse.ArgumentParser(
        description="Build compact monthly HRV/sleep history indexes from private R2."
    )
    parser.add_argument("--stream", action="append", choices=STREAMS)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--recent-months", type=int)
    parser.add_argument("--env-file", type=Path, default=Path(".env.local-bootstrap"))
    args = parser.parse_args()
    load_env_file(args.env_file)
    try:
        run(
            streams=args.stream or STREAMS,
            start_date=args.start_date,
            end_date=args.end_date,
            recent_months=args.recent_months,
        )
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
