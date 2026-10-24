"""Recorded running measurements through the real FIT decoder/Coach seam."""
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from garmin_fit_sdk import Encoder

from pipeline.coach import LOGICAL_BOUT_ANALYZER_VERSION, build_coach_input
from pipeline.granular import decode_fit, json_bytes

START = datetime(2026, 10, 7, 23, 30, tzinfo=timezone.utc)
DTO = {"activityId": 42, "activityName": "Synthetic recorded run",
       "activityType": {"typeKey": "running"},
       "startTimeGMT": "2026-10-07 23:30:00", "startTimeLocal": "2026-10-08 01:30:00"}
ACTIVITY = {"id": "42", "date": "2026-10-08", "type": "Run",
            "name": "Synthetic recorded run", "moving_seconds": 1800.0}
PROFILE = {"profile_id": "synthetic", "effective_from": "2026-01-01",
           "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}]}


def fit_bytes(**fields):
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "message_index": 0, "start_time": START,
                        "timestamp": START + timedelta(minutes=30), "sport": "running",
                        "avg_power": 250, "avg_step_length": 1234.5, "avg_stance_time": 241.2,
                        "avg_vertical_oscillation": 86.4, "avg_vertical_ratio": 6.78,
                        **fields})
    return bytes(encoder.close())


def decoded_source(**fields):
    # Production stores the decoder's datetime values as ISO strings.
    return json.loads(json_bytes(decode_fit(fit_bytes(**fields), activity=DTO)))


def analyze(decoded=None, *, activity=None, endurance=None):
    return build_coach_input(
        activity=ACTIVITY if activity is None else activity,
        decoded_fit=decoded_source() if decoded is None else decoded,
        # TCX can begin after the FIT start: its first sample is not session start.
        endurance={"available": True, "activity": {"id": "garmin-42",
                   "start_time": "2026-10-07T23:30:05Z"}, "summary": {"distance_m": 5000}}
        if endurance is None else endurance,
        profile=PROFILE, tcx_sha256="a" * 64,
    )


def test_real_fit_measurements_reach_coach_without_rescaling():
    block = analyze()["recorded_running_metrics"]
    assert block["average_power"] == {"status": "available", "value": 250,
                                      "unit": "W", "source_field": "avg_power"}
    assert block["average_step_length"] == {"status": "available", "value": 1234.5,
                                            "unit": "mm", "source_field": "avg_step_length"}
    assert block["average_ground_contact_time"] == {"status": "available", "value": 241.2,
                                                   "unit": "ms", "source_field": "avg_stance_time"}
    assert block["average_vertical_oscillation"] == {"status": "available", "value": 86.4,
                                                   "unit": "mm", "source_field": "avg_vertical_oscillation"}
    assert block["average_vertical_ratio"] == {"status": "available", "value": 6.78,
                                             "unit": "%", "source_field": "avg_vertical_ratio"}
    assert block["association"] == "matched"
    assert block["activity_id"] == "42"
    assert block["local_start_time"] == "2026-10-08T01:30:00"
    assert block["local_date"] == "2026-10-08"  # Not the UTC date.
    assert block["session_start_time_utc"] == "2026-10-07T23:30:00.000Z"
    assert block["measured_at_utc"] == "2026-10-08T00:00:00.000Z"
    assert block["source_fit_sha256"] == decoded_source()["source_fit_sha256"]
    assert analyze()["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION


@pytest.mark.parametrize("value,status,expected", [
    (0, "available", 0), (None, "missing", None),
    (True, "invalid", None), (False, "invalid", None), ("0", "invalid", None),
    (float("nan"), "invalid", None), (float("inf"), "invalid", None),
])
def test_malformed_values_are_not_coerced_or_confused_with_zero(value, status, expected):
    decoded = decoded_source()
    decoded["messages"]["session_mesgs"][0]["avg_power"] = value
    metric = analyze(decoded)["recorded_running_metrics"]["average_power"]
    assert metric == {"status": status, "value": expected, "unit": "W", "source_field": "avg_power"}


@pytest.mark.parametrize("field,value", [("avg_power", None), ("avg_power", 65535),
                                       ("avg_step_length", 6553.5)])
def test_actual_fit_null_or_invalid_sentinel_stays_missing(field, value):
    decoded = decoded_source(**{field: value})
    name = "average_power" if field == "avg_power" else "average_step_length"
    assert field not in decoded["messages"]["session_mesgs"][0]
    metric = analyze(decoded)["recorded_running_metrics"][name]
    assert metric["status"] == "missing" and metric["value"] is None


@pytest.mark.parametrize("case,expected", [
    ("id", "id_mismatch"), ("tcx_id", "id_mismatch"), ("utc", "time_mismatch"),
    ("local_date", "time_mismatch"), ("bad_local", "time_mismatch"),
    ("session_start", "time_mismatch"), ("naive_session", "time_mismatch"),
    ("ambiguous", "sessions_ambiguous"), ("missing", "sessions_missing"),
    ("bad_session", "invalid_schema"), ("schema_bool", "invalid_schema"),
    ("source", "invalid_schema"), ("errors", "decode_errors"),
    ("session_sport", "unsupported_sport"), ("activity_sport", "unsupported_sport"),
])
def test_association_failure_is_local_to_optional_metrics(case, expected):
    decoded, activity = decoded_source(), copy.deepcopy(ACTIVITY)
    endurance = {"available": True, "activity": {"id": "42"}, "summary": {"distance_m": 5000}}
    session = decoded["messages"]["session_mesgs"][0]
    if case == "id":
        decoded["activity"]["id"] = "43"
    elif case == "tcx_id":
        endurance["activity"]["id"] = "43"
    elif case == "utc":
        decoded["activity"]["start_time_gmt"] = "2026-10-07 23:31:00"
    elif case == "local_date":
        activity["date"] = "2026-10-07"
    elif case == "bad_local":
        decoded["activity"]["start_time_local"] = "2026-02-30 01:30:00"
    elif case == "session_start":
        session["start_time"] = "2026-10-07T23:30:01Z"
    elif case == "naive_session":
        session["start_time"] = "2026-10-07T23:30:00"
    elif case == "ambiguous":
        decoded["messages"]["session_mesgs"].append(copy.deepcopy(session))
    elif case == "missing":
        decoded["messages"]["session_mesgs"] = []
    elif case == "bad_session":
        decoded["messages"]["session_mesgs"] = [None]
    elif case == "schema_bool":
        decoded["schema_version"] = True
    elif case == "source":
        decoded["source"] = "other"
    elif case == "errors":
        decoded["decode_errors"] = ["synthetic invalid"]
    elif case == "session_sport":
        session["sport"] = "cycling"
    else:
        activity["type"] = "Cycling"
    result = analyze(decoded, activity=activity, endurance=endurance)
    block = result["recorded_running_metrics"]
    assert block["association"] == expected
    assert block["average_power"]["status"] == "unavailable"
    assert block["average_power"]["value"] is None
    assert result["summary"]["distance_m"] == 5000


def test_no_local_clock_is_not_invented_and_absent_sport_does_not_block():
    decoded = decoded_source()
    del decoded["activity"]["start_time_local"]
    del decoded["messages"]["session_mesgs"][0]["sport"]
    block = analyze(decoded)["recorded_running_metrics"]
    assert block["association"] == "matched"
    assert block["local_start_time"] is block["local_date"] is None


def test_repair_caller_can_rely_on_decoded_running_type_but_not_infer_it():
    activity = {key: value for key, value in ACTIVITY.items() if key != "type"}
    decoded = decoded_source()
    assert analyze(decoded, activity=activity)["recorded_running_metrics"]["association"] == "matched"
    decoded["activity"].pop("type")
    assert analyze(decoded, activity=activity)["recorded_running_metrics"]["association"] == "unsupported_sport"


def test_declared_decoded_cycling_cannot_be_overridden_by_running_caller():
    decoded = decoded_source()
    decoded["activity"]["type"] = "cycling"
    assert analyze(decoded)["recorded_running_metrics"]["association"] == "unsupported_sport"


def test_blank_caller_sport_is_missing_metadata_not_a_contradiction():
    activity = {**ACTIVITY, "type": "  "}
    assert analyze(activity=activity)["recorded_running_metrics"]["association"] == "matched"
    assert analyze(activity={**ACTIVITY, "type": "cycling"})["recorded_running_metrics"]["association"] == "unsupported_sport"


@pytest.mark.parametrize("timestamp", [None, "invalid", "2026-10-07T23:29:59Z"])
def test_measurement_timestamp_is_never_fabricated(timestamp):
    decoded = decoded_source()
    decoded["messages"]["session_mesgs"][0]["timestamp"] = timestamp
    block = analyze(decoded)["recorded_running_metrics"]
    assert block["association"] == "matched"
    assert block["measured_at_utc"] is None


def test_portable_reader_fixture_contains_real_produced_measurements():
    import base64
    import gzip

    from scripts.build_recorded_running_metrics_contract import build

    fixture = build()
    summary = base64.b64decode(fixture["objects_base64"]["summary/activities.csv"]).decode()
    assert summary.splitlines()[1].split(",")[1] == "2026-10-07 23:30:00"
    prefix = "activities/2026/42/coach-input/v1/"
    pointer = json.loads(base64.b64decode(fixture["objects_base64"][prefix + "latest-ready.json"]))
    canonical = json.loads(gzip.decompress(base64.b64decode(
        fixture["objects_base64"][pointer["analysis_key"]])))
    assert canonical["analysis_id"] == fixture["analysis_id"]
    assert canonical["analyzer_version"] == LOGICAL_BOUT_ANALYZER_VERSION
    assert canonical["stored_context_snapshot"]["reference_time_utc"] == "2026-10-07T23:30:00.000Z"
    block = canonical["recorded_running_metrics"]
    assert block["association"] == "matched" and block["activity_id"] == "42"
    assert [block[name]["value"] for name in (
        "average_power", "average_step_length", "average_ground_contact_time",
        "average_vertical_oscillation", "average_vertical_ratio",
    )] == [250, 1234.5, 241.2, 86.4, 6.78]
