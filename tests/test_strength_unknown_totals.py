"""Unknown measurements stay unknown in the public strength summary."""
from pipeline.granular import normalize_strength_session


def test_missing_load_or_duration_does_not_publish_a_complete_partial_total():
    result = normalize_strength_session("42", {"exerciseSets": [
        {"setType": "ACTIVE", "repetitionCount": 10, "weight": 10000, "duration": 30,
         "exercises": [{"category": "SQUAT", "name": "Squat"}]},
        {"setType": "REST", "duration": 20},
        {"setType": "ACTIVE", "repetitionCount": 5,
         "exercises": [{"category": "SQUAT", "name": "Squat"}]},
    ]})
    assert result["sets"][1]["weight_kg"] is None
    assert result["summary"] == {
        "active_sets": 2, "total_repetitions": 15, "external_volume_kg": None,
        "active_duration_seconds": None, "rest_duration_seconds": None,
        "by_exercise": {"Squat": {"sets": 2, "repetitions": 15, "external_volume_kg": None}},
        "by_category": {"SQUAT": {"sets": 2, "repetitions": 15, "external_volume_kg": None}},
    }


def test_unknown_repetitions_affect_only_their_totals_and_buckets():
    summary = normalize_strength_session("42", {"exerciseSets": [
        {"setType": "ACTIVE", "weight": 10000, "duration": 10,
         "exercises": [{"category": "SQUAT", "name": "Squat"}]},
        {"setType": "REST", "duration": 0},
        {"setType": "ACTIVE", "repetitionCount": 8, "weight": 0, "duration": 0,
         "exercises": [{"category": "PULL_UP", "name": "Pull-up"}]},
        {"setType": "REST", "duration": 0},
    ]})["summary"]
    assert summary["total_repetitions"] is None
    assert summary["external_volume_kg"] is None
    assert summary["active_duration_seconds"] == 10
    assert summary["rest_duration_seconds"] == 0
    assert summary["by_exercise"]["Squat"]["repetitions"] is None
    assert summary["by_category"]["SQUAT"]["external_volume_kg"] is None
    assert summary["by_exercise"]["Pull-up"] == {
        "sets": 1, "repetitions": 8, "external_volume_kg": 0,
    }
    assert summary["by_category"]["PULL_UP"]["external_volume_kg"] == 0


def test_recorded_zero_and_empty_session_keep_valid_zero_totals():
    for rows, count in (([], 0), ([
        {"setType": "ACTIVE", "repetitionCount": 0, "weight": 0, "duration": 0},
        {"setType": "REST", "duration": 0},
    ], 1)):
        summary = normalize_strength_session("42", {"exerciseSets": rows})["summary"]
        assert summary["active_sets"] == count
        assert summary["total_repetitions"] == 0
        assert summary["external_volume_kg"] == 0
        assert summary["active_duration_seconds"] == 0
        assert summary["rest_duration_seconds"] == 0
