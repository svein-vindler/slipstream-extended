"""Invented FIT bytes through the real decoder and Coach Input interface."""
import json
from datetime import timedelta

from garmin_fit_sdk import Encoder
from test_recorded_running_metrics import DTO, START, analyze, decoded_source

from pipeline.granular import decode_fit, json_bytes


def test_recorded_lap_measurements_are_independent_and_not_copied_from_session():
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "start_time": START,
                        "timestamp": START + timedelta(minutes=30), "sport": "running",
                        "avg_power": 250, "avg_stance_time_balance": 49.25})
    encoder.write_mesg({"mesg_num": 19, "message_index": 0, "start_time": START,
                        "timestamp": START + timedelta(minutes=10), "total_distance": 1000,
                        "avg_power": 300, "avg_step_length": 1200,
                        "avg_stance_time_balance": 48.75})
    encoder.write_mesg({"mesg_num": 19, "message_index": 1,
                        "start_time": START + timedelta(minutes=10),
                        "timestamp": START + timedelta(minutes=30), "total_distance": 2000,
                        "avg_power": 0})
    decoded = json.loads(json_bytes(decode_fit(bytes(encoder.close()), activity=DTO)))

    output = analyze(decoded)

    metrics = output["recorded_running_metrics"]
    assert metrics["schema_version"] == 2
    assert metrics["average_ground_contact_balance"] == {
        "status": "available", "value": 49.25, "unit": "%",
        "source_field": "avg_stance_time_balance", "side_semantics": "unknown",
    }
    laps = metrics["laps"]
    assert laps["status"] == "available" and laps["method"] == "fit_recorded_measurement"
    first, second = laps["items"]
    assert first["lap"] == 1 and second["lap"] == 2
    assert first["average_power"]["value"] == 300
    assert first["average_step_length"]["value"] == 1200
    assert first["average_ground_contact_balance"]["value"] == 48.75
    assert second["average_power"] == {
        "status": "available", "value": 0, "unit": "W", "source_field": "avg_power",
    }
    assert second["average_step_length"]["status"] == "missing"
    assert second["average_step_length"]["value"] is None
    assert second["average_ground_contact_balance"]["value"] is None
    assert metrics["average_power"]["value"] == 250
    assert output["summary"]["distance_m"] == 5000


def test_split_and_half_metrics_use_original_fit_intervals_not_session_or_sampled_tcx():
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "start_time": START,
                        "timestamp": START + timedelta(seconds=40), "sport": "running",
                        "avg_power": 999, "avg_step_length": 1999})
    for seconds, distance, power, length, balance in (
        (0, 0, 100, 1000, 49), (10, 500, 200, 1100, 50),
        (20, 1000, 300, None, 51), (40, 2000, 400, 1300, 52),
    ):
        row = {"mesg_num": 20, "timestamp": START + timedelta(seconds=seconds),
               "distance": distance, "power": power, "stance_time_balance": balance}
        if length is not None:
            row["step_length"] = length
        encoder.write_mesg(row)
    decoded = json.loads(json_bytes(decode_fit(bytes(encoder.close()), activity=DTO)))
    endurance = {"activity": {"id": "42"}, "summary": {"distance_m": 2000},
                 "kilometer_splits": [{"distance_m": 1000}, {"distance_m": 1000}],
                 "distance_halves": {"first": {"distance_m": 1000},
                                     "second": {"distance_m": 1000}},
                 # This series is deliberately unrelated and must not supply metrics.
                 "sampled_trackpoints": [{"power": 888, "step_length": 888}]}

    metrics = analyze(decoded, endurance=endurance)["recorded_running_metrics"]

    splits = metrics["kilometer_splits"]
    assert splits["status"] == "available"
    assert splits["method"] == "fit_record_time_weighted_mean"
    first, second = splits["items"]
    assert first["average_power"]["value"] == 150
    assert second["average_power"]["value"] == 300
    assert first["average_step_length"]["value"] == 1050
    assert first["average_power"]["source_field"] == "power"
    assert second["average_step_length"]["value"] is None
    assert second["average_step_length"]["status"] == "missing"
    assert first["average_power"]["coverage"] == {
        "valid_seconds": 20, "eligible_seconds": 20, "excluded_gap_seconds": 0,
        "contributing_intervals": 2, "status": "complete",
    }
    assert first["average_ground_contact_balance"]["value"] is None
    assert first["average_ground_contact_balance"]["status"] == "unavailable"
    assert first["average_ground_contact_balance"]["side_semantics"] == "unknown"
    assert metrics["average_power"]["value"] == 999
    halves = metrics["distance_halves"]
    assert halves["status"] == "available"
    assert halves["items"][0]["average_power"]["value"] == 150
    assert halves["items"][1]["average_power"]["value"] == 300


def test_balance_preserves_native_fit_value_without_borrowing_another_interfaces_range():
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "start_time": START,
                        "timestamp": START + timedelta(minutes=30), "sport": "running",
                        "avg_stance_time_balance": 101})
    decoded = json.loads(json_bytes(decode_fit(bytes(encoder.close()), activity=DTO)))

    metric = analyze(decoded)["recorded_running_metrics"]["average_ground_contact_balance"]

    assert metric == {"status": "available", "value": 101, "unit": "%",
                      "source_field": "avg_stance_time_balance", "side_semantics": "unknown"}


def test_unknown_optional_tcx_summary_does_not_hide_recorded_fit_measurements():
    output = analyze(endurance={"summary": []})
    metrics = output["recorded_running_metrics"]
    assert metrics["association"] == "matched"
    assert metrics["average_power"]["value"] == 250
    assert metrics["kilometer_splits"]["status"] == "missing"
    assert output["summary"]["distance_m"] is None


def test_finite_record_values_do_not_overflow_when_weighted_by_duration():
    decoded = decoded_source()
    decoded["messages"]["record_mesgs"] = [
        {"timestamp": (START + timedelta(seconds=seconds)).isoformat(),
         "distance": distance, "power": 1e308}
        for seconds, distance in ((0, 0), (10, 500), (20, 1000))
    ]
    output = analyze(decoded, endurance={"summary": {"distance_m": 1000},
                                        "kilometer_splits": [{"distance_m": 1000}]})
    metric = output["recorded_running_metrics"]["kilometer_splits"]["items"][0]["average_power"]

    assert metric["status"] == "available"
    assert metric["value"] == 1e308
    # Canonical JSON must never contain NaN/Infinity from intermediate arithmetic.
    json.dumps(output, allow_nan=False)


def test_sample_gaps_missing_sensors_and_stationary_time_have_explicit_partial_coverage():
    decoded = decoded_source()
    decoded["messages"]["record_mesgs"] = [
        {"timestamp": (START + timedelta(seconds=seconds)).isoformat(),
         "distance": distance, "power": power}
        for seconds, distance, power in (
            (0, 0, 100), (10, 500, None), (20, 1000, 300),
            (30, 1000, 999), (70, 1500, 400), (80, 2000, 500),
        )
    ]
    output = analyze(decoded, endurance={"summary": {"distance_m": 2000},
                                        "kilometer_splits": [{"distance_m": 1000}, {"distance_m": 1000}]})
    first, second = output["recorded_running_metrics"]["kilometer_splits"]["items"]

    assert first["average_power"]["value"] == 100
    assert first["average_power"]["coverage"] == {
        "valid_seconds": 10, "eligible_seconds": 20, "excluded_gap_seconds": 0,
        "contributing_intervals": 1, "status": "partial",
    }
    assert second["average_power"]["value"] == 400
    assert second["average_power"]["coverage"] == {
        "valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 40,
        "contributing_intervals": 1, "status": "partial",
    }
    assert second["average_step_length"]["status"] == "missing"
    assert second["average_step_length"]["coverage"]["status"] == "none"


def test_equal_distance_halves_keep_normalizers_millimeter_rounding_without_stretching_fit():
    from pipeline.granular import normalize_endurance_session
    from scripts.build_running_segments_contract import synthetic_fit, synthetic_tcx

    tcx = synthetic_tcx().replace(b"2000", b"2000.001")
    endurance = normalize_endurance_session(DTO, tcx)
    decoded = json.loads(json_bytes(decode_fit(synthetic_fit(), activity=DTO)))

    halves = analyze(decoded, endurance=endurance)["recorded_running_metrics"]["distance_halves"]

    assert endurance["summary"]["distance_m"] == 2000.001
    assert halves["status"] == "available"
    first, second = halves["items"]
    assert first["start_distance_m"] == 0
    assert first["end_distance_m"] == 1000.0005
    assert second["start_distance_m"] == 1000.0005
    assert second["end_distance_m"] == 2000.001
    assert second["average_power"]["value"] == 300
    # FIT stops at 2000.00m; centimetre agreement must not fill the last millimetre.
    assert second["average_power"]["coverage"]["status"] == "partial"


def test_unequal_distance_halves_are_invalid_without_hiding_coach_summary():
    from pipeline.granular import normalize_endurance_session
    from scripts.build_running_segments_contract import synthetic_fit, synthetic_tcx

    endurance = normalize_endurance_session(DTO, synthetic_tcx())
    endurance["distance_halves"]["first"]["distance_m"] = 500
    endurance["distance_halves"]["second"]["distance_m"] = 1500
    before = json.dumps(endurance, sort_keys=True)
    decoded = json.loads(json_bytes(decode_fit(synthetic_fit(), activity=DTO)))

    output = analyze(decoded, endurance=endurance)

    halves = output["recorded_running_metrics"]["distance_halves"]
    assert halves["status"] == "invalid"
    assert halves["reason"] == "invalid_distance_partition"
    assert halves["items"] == []
    assert output["summary"]["distance_m"] == 2000
    assert output["summary"]["elapsed_seconds"] == 40
    metrics = output["recorded_running_metrics"]
    assert metrics["association"] == "matched"
    assert metrics["average_power"]["value"] == 225
    assert metrics["laps"]["status"] == "available"
    assert [row["average_power"]["value"] for row in metrics["kilometer_splits"]["items"]] == [150, 300]
    assert json.dumps(endurance, sort_keys=True) == before


def test_one_distance_part_is_not_presented_as_two_halves():
    from pipeline.granular import normalize_endurance_session
    from scripts.build_running_segments_contract import synthetic_fit, synthetic_tcx

    endurance = normalize_endurance_session(DTO, synthetic_tcx())
    endurance["distance_halves"] = [{"distance_m": 2000}]
    decoded = json.loads(json_bytes(decode_fit(synthetic_fit(), activity=DTO)))

    output = analyze(decoded, endurance=endurance)

    halves = output["recorded_running_metrics"]["distance_halves"]
    assert halves["status"] == "invalid"
    assert halves["reason"] == "invalid_distance_partition"
    assert halves["items"] == []
    assert output["summary"]["distance_m"] == 2000
    assert output["recorded_running_metrics"]["kilometer_splits"]["status"] == "available"


def test_three_distance_parts_are_not_presented_as_two_halves():
    from pipeline.granular import normalize_endurance_session
    from scripts.build_running_segments_contract import synthetic_fit, synthetic_tcx

    endurance = normalize_endurance_session(DTO, synthetic_tcx())
    endurance["distance_halves"] = [{"distance_m": 500}, {"distance_m": 500}, {"distance_m": 1000}]
    decoded = json.loads(json_bytes(decode_fit(synthetic_fit(), activity=DTO)))

    output = analyze(decoded, endurance=endurance)

    halves = output["recorded_running_metrics"]["distance_halves"]
    assert halves["status"] == "invalid"
    assert halves["reason"] == "invalid_distance_partition"
    assert halves["items"] == []
    assert output["summary"]["distance_m"] == 2000
    assert output["recorded_running_metrics"]["kilometer_splits"]["status"] == "available"
