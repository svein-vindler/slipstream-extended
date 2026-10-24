"""Recorded zero speed from real FIT bytes through export and Coach publication."""
import gzip
import json

import pytest
from garmin_fit_sdk import Encoder
from garminconnect import Garmin

from pipeline import coach_backfill
from pipeline.coach import build_coach_input
from pipeline.granular_export import export_activity
from scripts.benchmark_coach_reuse import CountingStore
from scripts.build_coach_training_effect_contract import StructuredGarmin
from scripts.build_running_segments_contract import ACTIVITY, synthetic_tcx
from scripts.build_training_context_contract import DTO, START


class SpeedGarmin(StructuredGarmin):
    def download_activity(self, activity_id, *, dl_fmt):
        if dl_fmt != Garmin.ActivityDownloadFormat.ORIGINAL:
            return synthetic_tcx()
        encoder = Encoder()
        encoder.write_mesg({"mesg_num": 27, "message_index": 0, "intensity": "active"})
        encoder.write_mesg({"mesg_num": 19, "message_index": 0, "wkt_step_index": 0,
                            "start_time": START, "timestamp": START, "total_elapsed_time": 40,
                            "total_distance": 0, "enhanced_avg_speed": 0})
        encoder.write_mesg({"mesg_num": 18, "start_time": START, "timestamp": START, "sport": "running"})
        return bytes(encoder.close())


def test_zero_enhanced_speed_from_fit_is_preserved_in_coach(tmp_path):
    store = CountingStore()
    store.objects = {"summary/activities.csv": (
        b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        b"garmin-42,2026-10-07 23:30:00,Synthetic run,Run,40,2026-10-08\n"),
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-speed", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode()}
    export_activity(DTO, SpeedGarmin(), store, tmp_path)
    raw = json.loads(gzip.decompress(store.objects["activities/2026/42/activity.v1.json"]))
    lap = raw["messages"]["lap_mesgs"][0]
    assert lap["enhanced_avg_speed"] == 0
    assert lap.get("avg_speed") is None
    result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    analysis = json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))
    assert analysis["workout_structure"]["executed_laps"][0]["average_speed_mps"] == 0


@pytest.mark.parametrize(("enhanced", "fallback", "expected"), [
    (0, 3, 0), (None, 3, 3), (2, 3, 2), (False, 3, None),
    ("", 3, None), (float("nan"), 3, None), (float("inf"), 3, None), ("2.5", 3, 2.5),
])
def test_public_builder_speed_fallback_distinguishes_zero_missing_and_invalid(enhanced, fallback, expected):
    analysis = build_coach_input(
        activity={"id": "42", "date": "2026-10-23"},
        decoded_fit={"source_fit_sha256": "synthetic", "messages": {
            "workout_step_mesgs": [{"message_index": 0, "intensity": "active"}],
            "lap_mesgs": [{"wkt_step_index": 0, "total_elapsed_time": 40,
                           "enhanced_avg_speed": enhanced, "avg_speed": fallback}],
        }},
        endurance={}, profile={"profile_id": "synthetic-speed", "effective_from": "2026-01-01",
                               "zones": [{"label": "test", "min_bpm": 1, "max_bpm": 255}]},
        tcx_sha256="synthetic",
    )
    assert analysis["workout_structure"]["executed_laps"][0]["average_speed_mps"] == expected
