"""Synthetic native-lap links through the real public Coach producer."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.coach import build_coach_input  # noqa: E402
from scripts.build_running_segments_contract import partial_source_case  # noqa: E402


def native_source_case():
    source = partial_source_case()
    messages = source["decoded_fit"]["messages"]
    messages["workout_step_mesgs"] = [{"message_index": 3, "intensity": "active",
                                      "wkt_step_name": "Synthetic repeated step"}]
    messages["lap_mesgs"] = [
        {"message_index": 7, "wkt_step_index": 3, "start_time": "2026-01-01T00:00:00Z",
         "timestamp": "2026-01-01T00:00:04Z", "total_elapsed_time": 4.5,
         "total_timer_time": 4.5, "total_distance": 400, "avg_power": 111,
         "avg_step_length": 1000, "avg_stance_time": 200, "avg_vertical_oscillation": 10,
         "avg_vertical_ratio": 5, "avg_stance_time_balance": 101},
        {"message_index": 9, "wkt_step_index": 3, "start_time": "2026-01-01T00:00:04Z",
         "timestamp": "2026-01-01T00:00:09Z", "total_elapsed_time": 4.5,
         "total_timer_time": 4.5, "total_distance": 500, "avg_power": 222,
         "avg_step_length": 1100, "avg_stance_time": 210, "avg_vertical_oscillation": 11,
         "avg_vertical_ratio": 6, "avg_stance_time_balance": 103},
    ]
    return source


def build():
    return build_coach_input(**native_source_case())


def build_cases():
    """Actual producer analyses, without claiming publication or MCP validation."""
    sparse = native_source_case()
    first, second = sparse["decoded_fit"]["messages"]["lap_mesgs"]
    first.update(avg_power=None, avg_step_length=True, total_distance=None)
    second["avg_power"] = 0
    unbound = native_source_case()
    unbound["decoded_fit"]["messages"]["workout_step_mesgs"][0].pop("message_index")
    return {"complete": build(), "sparse": build_coach_input(**sparse),
            "unbound": build_coach_input(**unbound)}


if __name__ == "__main__":
    Path(sys.argv[1]).write_text(json.dumps(build_cases(), separators=(",", ":")), encoding="utf-8")
