"""Executed-lap averages through the public Coach Input builder."""
from pipeline.coach import build_coach_input


def section(laps):
    return build_coach_input(
        activity={"id": "1", "date": "2026-10-14"},
        decoded_fit={"source_fit_sha256": "synthetic", "messages": {
            "workout_step_mesgs": [{"message_index": 0, "intensity": "active"}],
            "lap_mesgs": [{"wkt_step_index": 0, "avg_heart_rate": hr,
                          "total_elapsed_time": duration} for hr, duration in laps],
        }},
        endurance={},
        profile={"profile_id": "synthetic", "effective_from": "2026-01-01",
                 "zones": [{"label": "test", "min_bpm": 1, "max_bpm": 255}]},
        tcx_sha256="synthetic",
    )["sections"][0]


def test_missing_lap_hr_does_not_dilute_observed_average():
    value = section([(150, 60), (None, 60)])
    assert value["average_heart_rate_bpm"] == 150
    assert value["elapsed_seconds"] == 120
    assert value["heart_rate_coverage"] == {
        "basis": "lap_elapsed", "covered_seconds": 60, "contributing_laps": 1,
        "status": "partial",
    }

def test_unequal_durations_use_time_weighted_observed_hr():
    value = section([(100, 60), (200, 120)])
    assert value["average_heart_rate_bpm"] == 166.67
    assert value["heart_rate_coverage"]["covered_seconds"] == 180
    assert value["heart_rate_coverage"]["status"] == "complete"


def test_all_missing_hr_remains_none():
    value = section([(None, 60), (None, 60)])
    assert value["average_heart_rate_bpm"] is None
    assert value["heart_rate_coverage"]["status"] == "none"


def test_numeric_zero_is_not_missing_and_fractional_positive_duration_is_valid():
    value = section([(0, 0.5), (None, 0.5), (150, 0)])
    assert value["average_heart_rate_bpm"] == 0
    assert value["heart_rate_coverage"]["covered_seconds"] == 0.5
    assert value["heart_rate_coverage"]["contributing_laps"] == 1
    assert value["heart_rate_coverage"]["status"] == "partial"


def test_bool_and_nonfinite_hr_do_not_contribute():
    value = section([(True, 60), (float("nan"), 60), (float("inf"), 60), (150, 60)])
    assert value["average_heart_rate_bpm"] == 150
    assert value["heart_rate_coverage"]["covered_seconds"] == 60
    assert value["heart_rate_coverage"]["status"] == "partial"


def test_unknown_or_negative_time_does_not_become_full_coverage():
    value = section([(120, 0.5), (180, None), (180, True), (180, float("inf")), (180, -1)])
    assert value["average_heart_rate_bpm"] == 120
    assert value["heart_rate_coverage"]["covered_seconds"] == 0.5
    assert value["heart_rate_coverage"]["status"] == "unknown"