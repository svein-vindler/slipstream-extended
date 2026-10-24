"""Stored-source context through the public Coach Input builder seam."""

import copy
import json
from pathlib import Path

import pytest

from pipeline.coach import build_coach_input
from pipeline.health_sync import normalize_day
from pipeline.training_day import merge_day, normalize

ACTIVITY = {"id": "42", "date": "2026-10-19", "type": "Run"}
DECODED = {
    "schema_version": 1,
    "source": "garmin-fit",
    "decode_errors": [],
    "source_fit_sha256": "a" * 64,
    "activity": {
        "id": "42",
        "start_time_gmt": "2026-10-19T07:00:00Z",
        "start_time_local": "2026-10-19T09:00:00",
    },
    "messages": {"session_mesgs": [{
        "start_time": "2026-10-19T07:00:00Z",
        "timestamp": "2026-10-19T07:30:00Z",
        "sport": "running",
    }]},
}
PROFILE = {
    "profile_id": "synthetic-context-profile",
    "effective_from": "2026-01-01",
    "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
}
USER_CONTEXT = {"context_id": "synthetic-user-note", "rpe": 4, "source": "user"}


def stored_snapshot(score=80, *, observed_utc="2026-10-19T06:00:00Z",
                    observed_local="2026-10-19T08:00:00"):
    """Actual existing canonical producer; no provider/storage substitute."""
    config = {"status": "enabled", "timezone": "Europe/Oslo",
              "device_id": None, "date": "2026-10-19"}
    # This was checked/stored after the workout. Its observation is before it:
    # retrospective context does not pretend it was known before the workout.
    devices, status = normalize({
        "deviceId": "90000042", "calendarDate": "2026-10-19",
        "timestamp": observed_utc,
        "timestampLocal": observed_local,
        "inputContext": "AFTER_WAKEUP_RESET", "score": score,
        "recoveryTime": 0,
    }, config, "2026-10-19T08:00:00+00:00")
    day = merge_day(None, devices, status, config, "2026-10-19T08:00:00+00:00")
    return {"schema_version": 1, "snapshot_at": "2026-10-19T10:00:00Z",
            "training_day": day}


def coach(**kwargs):
    return build_coach_input(
        activity=copy.deepcopy(ACTIVITY), decoded_fit=copy.deepcopy(DECODED),
        endurance={"activity": {"id": "42"}, "summary": {"distance_m": 5000}},
        profile=copy.deepcopy(PROFILE), tcx_sha256="b" * 64,
        context=copy.deepcopy(USER_CONTEXT), **kwargs,
    )


def test_stored_pre_activity_observation_is_replayable_context_not_a_user_note():
    snapshot = stored_snapshot()
    original = copy.deepcopy(snapshot)
    result = coach(stored_context_snapshot=snapshot)
    context = result["stored_context_snapshot"]

    assert context["reference_time_utc"] == "2026-10-19T07:00:00.000Z"
    assert context["snapshot_at"] == "2026-10-19T10:00:00Z"
    assert context["semantics"] == "retrospective_pre_activity"
    training = context["training_day"]
    assert training["selection_status"] == "available"
    assert training["source"] == "garmin-connect"
    assert training["readiness"] == {
        "value": 80, "status": "available", "unit": "score_1_100",
        "observation_at": "2026-10-19T06:00:00+00:00",
        "source_checked_at": "2026-10-19T08:00:00+00:00",
        "retained_previous": False,
    }
    assert training["recovery"] == {
        "value": 0, "status": "available", "unit": "minutes",
        "observation_at": "2026-10-19T06:00:00+00:00",
        "source_checked_at": "2026-10-19T08:00:00+00:00",
        "retained_previous": False,
    }
    assert result["user_context"] == USER_CONTEXT
    assert "90000042" not in json.dumps(context)
    assert snapshot == original
    assert coach(stored_context_snapshot=stored_snapshot())["analysis_id"] == result["analysis_id"]
    assert coach(stored_context_snapshot=stored_snapshot(81))["analysis_id"] != result["analysis_id"]
    assert coach()["analysis_id"] != result["analysis_id"]


def test_observation_after_the_workout_is_not_backdated_into_pre_activity_context():
    snapshot = stored_snapshot(observed_utc="2026-10-19T08:00:00Z",
                               observed_local="2026-10-19T10:00:00")
    result = coach(stored_context_snapshot=snapshot)
    training = result["stored_context_snapshot"]["training_day"]

    assert training["selection_status"] == "available"
    assert training["temporal_status"] == "after_activity"
    assert training["readiness"]["status"] == "unavailable"
    assert training["readiness"]["value"] is None
    assert training["recovery"]["status"] == "unavailable"
    assert training["recovery"]["value"] is None
    assert result["user_context"] == USER_CONTEXT


def test_unknown_observation_time_keeps_the_workout_readable_without_pre_activity_values():
    snapshot = stored_snapshot(observed_utc=None, observed_local=None)
    result = coach(stored_context_snapshot=snapshot)
    training = result["stored_context_snapshot"]["training_day"]

    assert training["selection_status"] == "available"
    assert training["temporal_status"] == "time_unknown"
    assert training["readiness"]["value"] is None
    assert training["recovery"]["value"] is None
    assert training["readiness"]["observation_at"] is None
    assert result["activity"]["date"] == "2026-10-19"
    assert result["user_context"] == USER_CONTEXT


def test_multiple_devices_do_not_merge_complementary_readiness_and_recovery():
    config = {"status": "enabled", "timezone": "Europe/Oslo",
              "device_id": None, "date": "2026-10-19"}
    rows = [
        {"deviceId": "90000042", "calendarDate": "2026-10-19",
         "timestamp": "2026-10-19T06:00:00Z", "timestampLocal": "2026-10-19T08:00:00",
         "score": 80, "recoveryTime": None},
        {"deviceId": "90000043", "calendarDate": "2026-10-19",
         "timestamp": "2026-10-19T06:00:00Z", "timestampLocal": "2026-10-19T08:00:00",
         "score": None, "recoveryTime": 30},
    ]
    devices, status = normalize(rows, config, "2026-10-19T08:00:00+00:00")
    day = merge_day(None, devices, status, config, "2026-10-19T08:00:00+00:00")
    snapshot = {"schema_version": 1, "snapshot_at": "2026-10-19T10:00:00Z", "training_day": day}

    result = coach(stored_context_snapshot=snapshot)
    training = result["stored_context_snapshot"]["training_day"]

    assert training["selection_status"] == "ambiguous"
    assert training["temporal_status"] == "selection_unavailable"
    assert training["readiness"]["value"] is None
    assert training["recovery"]["value"] is None
    assert training["selected_slot"] is None
    assert "90000042" not in json.dumps(training)
    assert "90000043" not in json.dumps(training)
    assert result["user_context"] == USER_CONTEXT


def health_snapshot(*, end_utc="2026-10-19T05:00:00Z", end_local="2026-10-19T07:00:00",
                    sleep_score=82, raw_spo2=None):
    snapshot = stored_snapshot()
    sleep = normalize_day("sleep", "2026-10-19", {
        "dailySleepDTO": {
            "calendarDate": "2026-10-19", "sleepTimeSeconds": 25200,
            "sleepStartTimestampGMT": "2026-10-18T22:00:00Z",
            "sleepEndTimestampGMT": end_utc,
            "sleepStartTimestampLocal": "2026-10-19T00:00:00",
            "sleepEndTimestampLocal": end_local,
            "sleepWindowConfirmed": True, "sleepScore": sleep_score,
            "averageSpO2Value": raw_spo2,
        },
        "sleepLevels": [{"startGMT": "2026-10-18T22:00:00Z",
                         "endGMT": end_utc, "activityLevel": 1}],
    })
    hrv = normalize_day("hrv", "2026-10-19", {
        "sleepStartTimestampGMT": "2026-10-18T22:00:00Z",
        "sleepEndTimestampGMT": end_utc,
        "sleepStartTimestampLocal": "2026-10-19T00:00:00",
        "sleepEndTimestampLocal": end_local,
        "hrvSummary": {"calendarDate": "2026-10-19", "lastNightAvg": 50, "weeklyAvg": 48},
        "hrvReadings": [
            {"readingTimeGMT": "2026-10-18T23:00:00Z", "hrvValue": 40,
             "providerOnly": "synthetic-provider-field"},
            {"readingTimeGMT": "2026-10-19T03:00:00Z", "hrvValue": 60},
        ],
    })
    snapshot["sleep"] = {"data": sleep, "source_revision": "c" * 64,
                         "source_checked_at": "2026-10-19T08:00:00Z"}
    snapshot["hrv"] = {"data": hrv, "source_revision": "d" * 64,
                       "source_checked_at": "2026-10-19T08:00:00Z"}
    return snapshot


def test_completed_local_night_reaches_coach_as_recorded_sleep_and_hrv_context():
    snapshot = health_snapshot()
    original = copy.deepcopy(snapshot)
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["sleep"]["wake_date"] == "2026-10-19"
    assert context["sleep"]["night_of"] == "2026-10-19"
    assert context["sleep"]["local_time_source"] == "garmin_local"
    assert context["sleep"]["observation_end_utc"] == "2026-10-19T05:00:00.000Z"
    assert context["sleep"]["source_checked_at"] == "2026-10-19T08:00:00Z"
    assert context["sleep"]["metrics"]["sleep_seconds"] == {
        "status": "available", "value": 25200, "unit": "s"}
    assert context["sleep"]["metrics"]["sleep_score"] == {
        "status": "available", "value": 82, "unit": "score_0_100"}
    assert context["hrv"]["status"] == "available"
    assert context["hrv"]["metrics"]["last_night_avg_ms"] == {
        "status": "available", "value": 50, "unit": "ms"}
    assert context["hrv"]["metrics"]["weekly_avg_ms"] == {
        "status": "available", "value": 48, "unit": "ms"}
    assert "synthetic-provider-field" not in json.dumps(context)
    assert "readings" not in context["hrv"]
    assert "stages" not in context["sleep"]
    assert snapshot == original


def test_night_finishing_after_workout_is_not_presented_as_pre_activity_context():
    snapshot = health_snapshot(end_utc="2026-10-19T08:00:00Z", end_local="2026-10-19T10:00:00")
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "after_activity"
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] is None
    assert context["hrv"]["status"] == "after_activity"
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] is None
    assert context["training_day"]["readiness"]["value"] == 80


def test_another_stored_wake_date_is_not_substituted_for_the_workout_night():
    case = json.loads((Path(__file__).parent / "fixtures/health-contract.json").read_text())["cases"][0]
    snapshot = stored_snapshot()
    for stream in ("sleep", "hrv"):
        snapshot[stream] = {
            "data": normalize_day(stream, case["date"], case[stream]),
            "source_revision": "e" * 64, "source_checked_at": None,
        }
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "wrong_date"
    assert context["hrv"]["status"] == "wrong_date"
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] is None
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] is None
    assert context["training_day"]["readiness"]["value"] == 80


def test_sleep_and_hrv_remain_available_when_readiness_was_not_collected():
    snapshot = health_snapshot()
    del snapshot["training_day"]
    result = coach(stored_context_snapshot=snapshot)
    context = result["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["hrv"]["status"] == "available"
    assert context["training_day"]["status"] == "not_collected"
    assert context["training_day"]["selection_status"] == "unknown"
    assert context["training_day"]["readiness"]["value"] is None
    assert context["training_day"]["recovery"]["value"] is None
    assert result["analyzer_version"] == "1.9.1"
    assert coach()["analyzer_version"] == "1.9.1"


def test_retained_field_with_unknown_time_does_not_inherit_the_new_observation_clock():
    snapshot = stored_snapshot(observed_utc=None, observed_local=None)
    config = {"status": "enabled", "timezone": "Europe/Oslo",
              "device_id": None, "date": "2026-10-19"}
    devices, status = normalize({
        "deviceId": "90000042", "calendarDate": "2026-10-19",
        "timestamp": "2026-10-19T06:30:00Z", "timestampLocal": "2026-10-19T08:30:00",
        "score": None, "recoveryTime": 30,
    }, config, "2026-10-19T08:30:00+00:00")
    snapshot["training_day"] = merge_day(
        snapshot["training_day"], devices, status, config, "2026-10-19T08:30:00+00:00")
    training = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]["training_day"]

    assert training["temporal_status"] == "before_activity"
    assert training["readiness"]["status"] == "unavailable"
    assert training["readiness"]["value"] is None
    assert training["readiness"]["observation_at"] is None
    assert training["readiness"]["source_checked_at"] == "2026-10-19T08:00:00+00:00"
    assert training["readiness"]["retained_previous"] is True
    assert training["recovery"]["value"] == 30
    assert training["recovery"]["observation_at"] == "2026-10-19T06:30:00+00:00"


def test_morning_before_workout_is_labelled_morning_not_latest_after_workout():
    config = {"status": "enabled", "timezone": "Europe/Oslo",
              "device_id": None, "date": "2026-10-19"}
    devices, status = normalize([
        {"deviceId": "90000042", "calendarDate": "2026-10-19",
         "timestamp": "2026-10-19T06:00:00Z", "timestampLocal": "2026-10-19T08:00:00",
         "inputContext": "AFTER_WAKEUP_RESET", "score": 80, "recoveryTime": 0},
        {"deviceId": "90000042", "calendarDate": "2026-10-19",
         "timestamp": "2026-10-19T08:00:00Z", "timestampLocal": "2026-10-19T10:00:00",
         "score": 90, "recoveryTime": 30},
    ], config, "2026-10-19T08:30:00+00:00")
    snapshot = {"schema_version": 1, "snapshot_at": "2026-10-19T10:00:00Z",
                "training_day": merge_day(None, devices, status, config, "2026-10-19T08:30:00+00:00")}
    training = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]["training_day"]

    assert training["selected_slot"] == "morning"
    assert training["temporal_status"] == "before_activity"
    assert training["input_context"] == "AFTER_WAKEUP_RESET"
    assert training["readiness"]["value"] == 80
    assert training["recovery"]["value"] == 0


def test_missing_hrv_does_not_hide_available_sleep_or_claim_unsupported_hardware():
    snapshot = health_snapshot()
    snapshot["hrv"] = None
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["hrv"]["status"] == "not_collected"
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] is None
    assert "unsupported" not in json.dumps(context)


def test_missing_local_night_clock_is_unknown_not_the_runner_or_home_calendar():
    snapshot = health_snapshot()
    for stream in ("sleep", "hrv"):
        snapshot[stream]["data"]["sleep_start_garmin_local"] = None
        snapshot[stream]["data"]["sleep_end_garmin_local"] = None
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "local_time_unknown"
    assert context["hrv"]["status"] == "local_time_unknown"
    assert context["sleep"]["night_of"] is None
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] is None
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] is None


def test_explicit_configured_timezone_can_resolve_stored_utc_night_without_local_clocks():
    snapshot = health_snapshot()
    snapshot["timezone"] = "Europe/Oslo"
    for stream in ("sleep", "hrv"):
        snapshot[stream]["data"]["sleep_start_garmin_local"] = None
        snapshot[stream]["data"]["sleep_end_garmin_local"] = None
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["sleep"]["local_time_source"] == "configured_timezone"
    assert context["sleep"]["sleep_end_local"] == "2026-10-19T07:00:00+02:00"
    assert context["hrv"]["status"] == "available"
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] == 25200


def test_corrupt_optional_sleep_schema_does_not_hide_good_hrv_and_workout_data():
    snapshot = health_snapshot()
    snapshot["sleep"]["data"]["schema_version"] = True
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "invalid_schema"
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] is None
    assert context["hrv"]["status"] == "available"
    assert context["training_day"]["readiness"]["value"] == 80


def test_corrupt_optional_training_day_does_not_hide_good_sleep_and_hrv():
    snapshot = health_snapshot()
    snapshot["training_day"]["schema_version"] = True
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["training_day"]["status"] == "invalid_schema"
    assert context["training_day"]["readiness"]["value"] is None
    assert context["sleep"]["status"] == "available"
    assert context["hrv"]["status"] == "available"


def test_stored_source_read_failure_is_not_relabelled_as_no_collection():
    snapshot = health_snapshot()
    snapshot["hrv"] = {"status": "source_error"}
    snapshot["training_day"] = {"status": "source_error"}
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["hrv"]["status"] == "source_error"
    assert context["training_day"]["status"] == "source_error"
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] is None
    assert context["training_day"]["readiness"]["value"] is None


def test_training_attempt_only_exposes_named_provenance_not_extra_raw_fields():
    snapshot = health_snapshot()
    snapshot["training_day"]["last_attempt"].update({
        "deviceId": "90000042", "raw": {"marker": "synthetic-private-provider-object"},
    })
    original = copy.deepcopy(snapshot)
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["training_day"]["last_attempt"] == {
        "status": "available", "attempted_at": "2026-10-19T08:00:00+00:00"}
    assert "90000042" not in json.dumps(context)
    assert "synthetic-private-provider-object" not in json.dumps(context)
    assert context["training_day"]["source_revision"] == snapshot["training_day"]["source_revision"]
    assert context["training_day"]["recovery"]["value"] == 0
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] == 25200
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] == 50
    assert snapshot == original


def test_unrepresentable_stored_metric_keeps_other_context_readable():
    snapshot = health_snapshot()
    snapshot["sleep"]["data"]["summary"]["sleep_score"] = 10**1000
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "available"
    assert context["sleep"]["metrics"]["sleep_score"] == {
        "status": "invalid", "value": None, "unit": "score_0_100"}
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] == 25200
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] == 50
    assert context["training_day"]["recovery"]["value"] == 0


def test_unrepresentable_stored_clock_does_not_hide_other_context():
    snapshot = health_snapshot()
    snapshot["sleep"]["data"]["sleep_start_gmt"] = 10**1000
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]

    assert context["sleep"]["status"] == "time_unknown"
    assert context["sleep"]["observation_start_utc"] is None
    assert context["sleep"]["metrics"]["sleep_seconds"]["value"] is None
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] == 50
    assert context["training_day"]["recovery"]["value"] == 0


@pytest.mark.parametrize("score,raw_spo2,source_spo2,status,expected_score,expected_spo2", [
    (101, 20000, 200, "invalid", None, None),
    (0, 0, 0, "available", 0, 0),
    (100, 10000, 100, "available", 100, 100),
])
def test_recorded_score_and_percent_respect_declared_domain_without_clamping(
    score, raw_spo2, source_spo2, status, expected_score, expected_spo2,
):
    snapshot = health_snapshot(sleep_score=score, raw_spo2=raw_spo2)
    assert snapshot["sleep"]["data"]["summary"]["sleep_score"] == score
    assert snapshot["sleep"]["data"]["summary"]["average_spo2_percent"] == source_spo2
    context = coach(stored_context_snapshot=snapshot)["stored_context_snapshot"]
    sleep = context["sleep"]

    assert sleep["status"] == "available"
    assert sleep["metrics"]["sleep_score"] == {
        "status": status, "value": expected_score, "unit": "score_0_100"}
    assert sleep["metrics"]["average_spo2_percent"] == {
        "status": status, "value": expected_spo2, "unit": "%"}
    assert sleep["metrics"]["sleep_seconds"]["value"] == 25200
    assert context["hrv"]["metrics"]["last_night_avg_ms"]["value"] == 50
    assert context["training_day"]["recovery"]["value"] == 0
