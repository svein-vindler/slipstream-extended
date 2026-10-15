"""Generate synthetic actual-producer scalar leaves for authenticated MCP tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.granular import normalize_strength_session  # noqa: E402
from pipeline.health_detail import normalize_sleep_detail  # noqa: E402
from scripts.build_training_context_contract import strength_label_contract  # noqa: E402

DAY = "2026-09-30"


def strength_row():
    return {
        "setType": "ACTIVE", "duration": "12.5", "repetitionCount": "0", "weight": 60000,
        "messageIndex": 0, "startTime": "2026-09-30T12:00:00",
        "exercises": [{"category": "__proto__", "name": "__proto__", "probability": 95}],
        "avgConcentricMeanVelocity": 0.42, "tal_grit": float("nan"),
        "__proto__": 0, "constructor": False, "future_numeric_text": "0",
        "future_text": "scalar", "none_metric": None, "future_true": True,
    }


def sleep_source():
    return {"dailySleepDTO": {
        "calendarDate": DAY, "sleepTimeSeconds": 25200, "sleepScore": "82",
        "sleepWindowConfirmed": True,
        "sleepStartTimestampGMT": "2026-09-29T21:30:00Z",
        "sleepEndTimestampGMT": "2026-09-30T04:30:00Z",
        "sleepStartTimestampLocal": "2026-09-29T23:30:00",
        "sleepEndTimestampLocal": "2026-09-30T06:30:00",
        "sleepScores": {
            "overall": {"value": "82", "qualifierKey": "GOOD"},
            "future_zero": {"value": 0, "qualifier": None},
            "future_null": {"value": None, "qualifier": "UNKNOWN"},
            "future_numeric_qualifier": {"value": "0", "qualifierKey": 0},
            "future_boolean": {"value": 0, "qualifierKey": False},
            "future_fallback": {"value": 1, "qualifierKey": "", "qualifier": "GOOD"},
            "future_history": {"value": 0, "qualifier": "{'historical': ['synthetic text']}"},
        }}, "sleepLevels": [{"startGMT": "2026-09-29T21:30:00Z",
                                "endGMT": "2026-09-30T04:30:00Z", "activityLevel": 1}]}


def build():
    return {
        "strength": normalize_strength_session("123", {"exerciseSets": [strength_row()]}),
        "sleep": normalize_sleep_detail(DAY, sleep_source()),
    }


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build(), ensure_ascii=False, allow_nan=False), encoding="utf-8")
    target.with_name("strength-label-contract.json").write_text(
        json.dumps(strength_label_contract(), allow_nan=False), encoding="utf-8",
    )
