"""Normalize detailed Garmin sleep and body-composition responses."""

from __future__ import annotations

import math
from typing import Any

from .granular import SCHEMA_VERSION


def _first(mapping: dict[str, Any], *keys: str):
    for key in keys:
        value = mapping.get(key)
        if value is not None and value != "":
            return value
    return None


def _number(value: Any, digits: int = 3):
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        result = round(float(value), digits)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return int(result) if result == int(result) else result


def _kg(value: Any):
    number = _number(value)
    if number is None:
        return None
    return _number(number / 1000) if number > 500 else number


def _percent(value: Any):
    number = _number(value, 2)
    if number is None:
        return None
    return _number(number / 100, 2) if number > 100 else number


def _boolean(value: Any):
    if isinstance(value, bool):
        return value
    if value in (1, "1", "true", "TRUE", "True"):
        return True
    if value in (0, "0", "false", "FALSE", "False"):
        return False
    return None


def _score_breakdown(dto: dict[str, Any]) -> dict[str, Any]:
    source = dto.get("sleepScores")
    if not isinstance(source, dict):
        return {}
    result = {}
    for name, item in source.items():
        if not isinstance(item, dict):
            continue
        qualifier = _first(item, "qualifierKey", "qualifier")
        result[str(name)] = {
            "value": _number(item.get("value"), 1),
            "qualifier": str(qualifier) if qualifier is not None else None,
        }
    return result


def _sleep_levels(raw: dict[str, Any]) -> list[dict[str, Any]]:
    source = raw.get("sleepLevels")
    if not isinstance(source, list):
        source = raw.get("sleepLevelData")
    if not isinstance(source, list):
        return []
    levels = []
    for item in source:
        if not isinstance(item, dict):
            continue
        start = _first(
            item,
            "startGMT",
            "startTimeGMT",
            "startTimestampGMT",
            "startTime",
        )
        end = _first(
            item,
            "endGMT",
            "endTimeGMT",
            "endTimestampGMT",
            "endTime",
        )
        stage = _first(item, "activityLevel", "sleepLevel", "stage", "type")
        if start is None and end is None and stage is None:
            continue
        levels.append({"start_gmt": start, "end_gmt": end, "stage": stage})
    return levels


def normalize_sleep_detail(day: str, raw: Any) -> dict[str, Any]:
    """Create a stable, GPS-free nightly sleep representation."""
    source = raw if isinstance(raw, dict) else {}
    dto = source.get("dailySleepDTO")
    if not isinstance(dto, dict):
        dto = source
    stages = _sleep_levels(source)
    summary = {
        "sleep_seconds": _number(
            _first(dto, "sleepTimeSeconds", "sleepSeconds", "totalSleepSeconds")
        ),
        "deep_sleep_seconds": _number(
            _first(dto, "deepSleepSeconds", "deepSeconds")
        ),
        "light_sleep_seconds": _number(
            _first(dto, "lightSleepSeconds", "lightSeconds")
        ),
        "rem_sleep_seconds": _number(_first(dto, "remSleepSeconds", "remSeconds")),
        "awake_sleep_seconds": _number(
            _first(dto, "awakeSleepSeconds", "awakeSeconds")
        ),
        "unmeasurable_sleep_seconds": _number(dto.get("unmeasurableSleepSeconds")),
        "nap_seconds": _number(_first(dto, "napTimeSeconds", "napSeconds")),
        "sleep_score": _number(dto.get("sleepScore"), 1),
        "average_spo2_percent": _percent(
            _first(dto, "averageSpO2Value", "averageSpo2Value")
        ),
        "lowest_spo2_percent": _percent(
            _first(dto, "lowestSpO2Value", "lowestSpo2Value")
        ),
        "average_respiration_brpm": _number(dto.get("averageRespirationValue"), 2),
        "lowest_respiration_brpm": _number(dto.get("lowestRespirationValue"), 2),
        "highest_respiration_brpm": _number(dto.get("highestRespirationValue"), 2),
        "average_sleep_stress": _number(
            _first(dto, "avgSleepStress", "averageSleepStress"), 1
        ),
    }
    if summary["sleep_score"] is None:
        overall = _score_breakdown(dto).get("overall")
        if isinstance(overall, dict):
            summary["sleep_score"] = overall.get("value")
    if summary["sleep_seconds"] is None and not stages:
        raise ValueError("Garmin returned no detailed sleep data")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "garmin",
        "date": day,
        "sleep_start_gmt": _first(
            dto, "sleepStartTimestampGMT", "autoSleepStartTimestampGMT"
        ),
        "sleep_end_gmt": _first(
            dto, "sleepEndTimestampGMT", "autoSleepEndTimestampGMT"
        ),
        "confirmed": _boolean(dto.get("sleepWindowConfirmed")),
        "summary": summary,
        "score_breakdown": _score_breakdown(dto),
        "stage_count": len(stages),
        "stages": stages,
    }


def _body_items(source: dict[str, Any]) -> list[dict[str, Any]]:
    top_level_metrics = source.get("allWeightMetrics")
    if isinstance(top_level_metrics, list) and top_level_metrics:
        return [item for item in top_level_metrics if isinstance(item, dict)]
    containers = (
        source.get("dateWeightList")
        or source.get("weightList")
        or source.get("dailyWeightSummaries")
        or []
    )
    if not isinstance(containers, list):
        return []
    if not containers:
        average = source.get("totalAverage")
        if isinstance(average, dict):
            return [{**average, "isDailyAverage": True}]
    result = []
    for container in containers:
        if not isinstance(container, dict):
            continue
        common = {
            key: container.get(key)
            for key in ("calendarDate", "date", "summaryDate")
            if container.get(key) is not None
        }
        metrics = container.get("allWeightMetrics")
        if isinstance(metrics, list) and metrics:
            result.extend(
                {**common, **item}
                for item in metrics
                if isinstance(item, dict)
            )
            continue
        latest = container.get("latestWeight")
        if isinstance(latest, dict):
            result.append({**common, **latest})
            continue
        average = container.get("totalAverage")
        if isinstance(average, dict):
            result.append({**common, **average, "isDailyAverage": True})
            continue
        result.append(container)
    return result


def normalize_body_composition(day: str, raw: Any) -> dict[str, Any]:
    """Create a stable view of all Garmin weigh-ins for one date."""
    source = raw if isinstance(raw, dict) else {}
    measurements = []
    for item in _body_items(source):
        item_day = str(_first(item, "calendarDate", "date", "summaryDate") or day)[:10]
        if item_day != day:
            continue
        weight = _kg(_first(item, "weight", "value", "latestWeightValue"))
        if weight is None:
            weight = _kg(_first(item, "maxWeight", "minWeight"))
        measurements.append({
            "timestamp_gmt": _first(
                item, "timestampGMT", "gmtTimestamp", "measurementTimeGMT"
            ),
            "timestamp_local": _first(
                item, "timestampLocal", "dateTimestamp", "measurementTimeLocal"
            ),
            "weight_kg": weight,
            "bmi": _number(_first(item, "bmi", "bodyMassIndex"), 2),
            "body_fat_percent": _percent(
                _first(item, "bodyFat", "percentFat", "bodyFatPercentage")
            ),
            "body_water_percent": _percent(
                _first(item, "bodyWater", "percentHydration", "bodyWaterPercentage")
            ),
            "muscle_mass_kg": _kg(_first(item, "muscleMass", "skeletalMuscleMass")),
            "bone_mass_kg": _kg(item.get("boneMass")),
            "visceral_fat_rating": _number(
                _first(item, "visceralFat", "visceralFatRating"), 1
            ),
            "metabolic_age": _number(item.get("metabolicAge"), 1),
            "physique_rating": _number(item.get("physiqueRating"), 1),
            "basal_metabolic_rate_kcal": _number(
                _first(item, "basalMet", "basalMetabolicRate"), 1
            ),
            "source_type": str(item["sourceType"])
            if item.get("sourceType") is not None
            else None,
            "measurement_id": _first(
                item, "samplePk", "samplePrimaryKey", "weightPk", "measurementId"
            ),
            "is_daily_average": bool(item.get("isDailyAverage")),
        })
    measurements = [
        item
        for item in measurements
        if any(
            value is not None
            for key, value in item.items()
            if key != "is_daily_average"
        )
    ]
    if not measurements:
        raise ValueError("Garmin returned no body-composition measurements")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "garmin",
        "date": day,
        "measurement_count": len(measurements),
        "measurements": measurements,
    }
