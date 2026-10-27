"""Public offline training -> following-night product behavior using synthetic exports."""

import csv
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HEADERS = [
    "Activity ID",
    "Activity Date",
    "Activity Type",
    "Elapsed Time",
    "Moving Time",
    "Average Heart Rate",
    "Source",
]


def exported_night(day, start, end, values, *, zone=None):
    return {
        "date": day,
        "wake_date": day,
        "method_version": "night-signals-v1",
        "resolution": "native",
        "status": "available",
        "sleep_start_utc": start,
        "sleep_end_utc": end,
        "actual_night_timezone": zone,
        "user_context": None,
        "channels": {
            "hrv": {
                "status": "available",
                "unit": "ms",
                "source_revisions": [{"date": day, "revision": "synthetic-native-v1"}],
                "points": [
                    {"timestamp_utc": instant, "value": value, "time_basis": "utc"}
                    for instant, value in values
                ],
            }
        },
    }


def run_training(
    tmp_path, nights, rows=None, *, start="2026-10-25", end="2026-10-26", extra=(), check=True
):
    exported = tmp_path / "native.json"
    exported.write_text(json.dumps({"nights": nights}), encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "pipeline.night_training",
        "build",
        "--input",
        str(exported),
        "--start-date",
        start,
        "--end-date",
        end,
    ]
    if rows is not None:
        activities = tmp_path / "activities.csv"
        with activities.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(HEADERS)
            writer.writerows(rows)
        command += ["--activities", str(activities)]
    result = subprocess.run(
        [*command, *extra],
        cwd=ROOT,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        capture_output=True,
        check=False,
    )
    if check:
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)
    return result


def test_public_cli_links_known_ends_to_following_night_across_dst_and_travel(tmp_path):
    nights = [
        exported_night(
            "2026-10-25",
            "2026-10-24T21:58:00Z",
            "2026-10-25T06:48:00Z",
            [("2026-10-25T00:30:00Z", 40), ("2026-10-25T01:30:00Z", 80)],
            zone="Europe/Oslo",
        ),
        exported_night(
            "2026-10-26",
            "2026-10-25T14:30:00Z",
            "2026-10-25T22:00:00Z",
            [("2026-10-25T15:00:00Z", 70)],
            zone="Asia/Tokyo",
        ),
    ]
    rows = [
        ["garmin-1", "2026-10-24 20:00:00", "Run", 3600, 3000, 140, "garmin"],
        ["garmin-2", "2026-10-24 23:30:00", "Walk", 1800, 1800, 90, "garmin"],
        ["garmin-3", "2026-10-25T23:00:00+09:00", "Ride", 900, 800, 130, "garmin"],
        ["garmin-4", "2026-10-24 19:00:00", "Run", "", 1800, "", "garmin"],
    ]
    output = run_training(tmp_path, nights, rows)
    product = output["product"]
    assert product["method_version"] == "night-training-v1"
    assert product["association"] == "temporal_only_not_causal"
    first, second = product["nights"]
    assert [item["activity_id"] for item in first["training"]["activities"]] == ["garmin-1"]
    assert [item["activity_id"] for item in second["training"]["activities"]] == ["garmin-3"]
    assert first["training"]["activities"][0]["end_utc"] == "2026-10-24T21:00:00Z"
    assert second["training"]["activities"][0]["start_utc"] == "2026-10-25T14:00:00Z"
    assert first["training"]["confirmed_activity_count"] == 1
    # Two distinct uncertain records: one unknown end and one overlap with documented sleep.
    assert first["training"]["uncertain_activity_count"] == 2
    assert first["training"]["recorded_duration_seconds"] is None
    assert first["training"]["known_recorded_duration_seconds"] == 3600
    assert first["training"]["training_load"] is None
    assert first["training"]["intensity"] is None
    assert first["channels"]["hrv"]["median"] == 60
    assert second["channels"]["hrv"]["median"] == 70
    assert product["unlinked_counts"]["overlaps_recorded_sleep"] == 1
    assert product["unlinked_counts"]["unknown_end"] == 1
    assert output["storage_key"].startswith("health/night-products/v1/training/2026/10/26/")


@pytest.mark.parametrize("zone", [{"private": "DO_NOT_PROJECT"}, "Invalid/Zone"])
def test_public_cli_projects_invalid_actual_zone_as_unknown_without_discarding_valid_utc(
    tmp_path, zone
):
    night = exported_night(
        "2026-10-25",
        "2026-10-25T00:00:00Z",
        "2026-10-25T01:00:00Z",
        [("2026-10-25T00:30:00Z", 50)],
        zone=zone,
    )
    output = run_training(tmp_path, [night], [], start="2026-10-25", end="2026-10-25")
    row = output["product"]["nights"][0]
    assert row["actual_night_timezone"] is None
    assert "actual_timezone_unknown" in row["quality_flags"]
    assert "invalid_actual_timezone" in row["quality_flags"]
    assert row["channels"]["hrv"]["median"] == 50
    assert "DO_NOT_PROJECT" not in json.dumps(output)


@pytest.mark.parametrize("rows,state,expected", [(None, "not_stored", None), ([], "no_data", 0)])
def test_public_cli_distinguishes_missing_activity_dataset_from_zero_recorded_rows(
    tmp_path, rows, state, expected
):
    night = exported_night(
        "2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [("2026-10-25T00:30:00Z", 50)]
    )
    product = run_training(tmp_path, [night], rows, start="2026-10-25", end="2026-10-25")["product"]
    row = product["nights"][0]
    assert product["activity_dataset_status"] == state
    assert row["training"]["confirmed_activity_count"] == expected
    assert row["training"]["known_recorded_duration_seconds"] == expected
    assert row["training"]["training_load"] is None
    assert row["training"]["intensity"] is None
    assert row["training"]["coverage"] == "provided_export_only_provider_completeness_unknown"


def test_public_cli_retains_explicit_activity_read_error_without_claiming_empty_training(tmp_path):
    night = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [])
    product = run_training(
        tmp_path,
        [night],
        extra=["--activity-state", "read_error"],
        start="2026-10-25",
        end="2026-10-25",
    )["product"]
    assert product["activity_dataset_status"] == "read_error"
    assert product["nights"][0]["training"]["confirmed_activity_count"] is None


def test_public_cli_observes_exact48hour_and_end_at_sleep_boundaries(tmp_path):
    night = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [])
    rows = [
        ["garmin-exact", "2026-10-22 23:00:00", "Run", 3600, 3600, "", "garmin"],
        ["garmin-late", "2026-10-22 22:59:59", "Run", 3600, 3600, "", "garmin"],
        ["garmin-boundary", "2026-10-25 00:00:00", "Walk", 0, 0, "", "garmin"],
    ]
    product = run_training(tmp_path, [night], rows, start="2026-10-25", end="2026-10-25")["product"]
    assert [item["activity_id"] for item in product["nights"][0]["training"]["activities"]] == [
        "garmin-exact",
        "garmin-boundary",
    ]
    assert product["unlinked_counts"]["outside_window"] == 1
    assert product["nights"][0]["training"]["recorded_duration_seconds"] == 3600


def test_public_cli_never_assigns_generic_naive_or_numeric_activity_clocks_to_utc(tmp_path):
    night = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [])
    rows = [
        ["garmin-naive", "2026-10-24T20:00:00", "Run", 3600, 3600, 100, "garmin"],
        ["garmin-epoch", "1792872000", "Run", 3600, 3600, 100, "garmin"],
        ["garmin-moving", "2026-10-24 20:00:00", "Run", "", 3600, 100, "garmin"],
    ]
    product = run_training(tmp_path, [night], rows, start="2026-10-25", end="2026-10-25")["product"]
    assert product["nights"][0]["training"]["activities"] == []
    assert product["nights"][0]["training"]["uncertain_activity_count"] == 3
    assert product["unlinked_counts"]["unknown_start"] == 2
    assert product["unlinked_counts"]["unknown_end"] == 1
    assert product["nights"][0]["training"]["recorded_duration_seconds"] is None


def test_public_cli_summarizes_only_unique_valid_utc_points_without_interpolation(tmp_path):
    night = exported_night(
        "2026-10-25",
        "2026-10-25T00:00:00Z",
        "2026-10-25T01:00:00Z",
        [
            ("2026-10-25T00:10:00Z", 50),
            ("2026-10-25T00:10:00Z", 50),
            ("2026-10-25T00:20:00Z", 40),
            ("2026-10-25T00:20:00Z", 80),
            ("2026-10-25T00:30:00", 600),
            ("2026-10-25T01:00:00Z", 1000),
        ],
    )
    output = run_training(tmp_path, [night], [], start="2026-10-25", end="2026-10-25")
    channel = output["product"]["nights"][0]["channels"]["hrv"]
    assert channel["median"] == 50
    assert channel["valid_points"] == 1
    assert channel["invalid_points"] == 2
    assert channel["coverage"] is None
    assert channel["observed_cadence_seconds"] is None
    assert "conflicting_timestamps_excluded" in channel["quality_flags"]


@pytest.mark.parametrize("conflict,expected", [(False, 1), (True, 0)])
def test_public_cli_never_double_counts_duplicate_activity_identity(tmp_path, conflict, expected):
    night = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [])
    first = ["garmin-same", "2026-10-24 20:00:00", "Run", 3600, 3600, 100, "garmin"]
    second = [*first]
    if conflict:
        second[3] = 7200
    product = run_training(
        tmp_path, [night], [first, second], start="2026-10-25", end="2026-10-25"
    )["product"]
    assert product["nights"][0]["training"]["confirmed_activity_count"] == expected
    assert product["unlinked_counts"]["duplicate_conflict"] == (1 if conflict else 0)


def test_public_cli_limits_period_and_malformed_csv_without_exposing_source_text(tmp_path):
    night = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [])
    bad = tmp_path / "bad.csv"
    bad.write_text("private_header\nPRIVATE_SOURCE_TEXT\n", encoding="utf-8")
    invalid = run_training(
        tmp_path,
        [night],
        [],
        extra=["--activities", str(bad)],
        start="2026-10-25",
        end="2026-10-25",
        check=False,
    )
    assert invalid.returncode == 2
    assert "PRIVATE_SOURCE_TEXT" not in invalid.stderr
    assert invalid.stdout == ""
    too_long = run_training(
        tmp_path, [night], [], start="2025-01-01", end="2026-10-25", check=False
    )
    assert too_long.returncode == 2


def test_public_cli_preserves_exact_export_file_hashes_while_numeric_zero_identity_is_semantic(
    tmp_path,
):
    night = exported_night(
        "2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", [("2026-10-25T00:30:00Z", 50)]
    )
    night["channels"]["body_battery"] = {
        "status": "available",
        "unit": "points",
        "source_revisions": [],
        "points": [{"timestamp_utc": "2026-10-25T00:20:00Z", "value": -0.0, "time_basis": "utc"}],
    }
    negative = run_training(tmp_path, [night], [], start="2026-10-25", end="2026-10-25")["product"]
    raw = (tmp_path / "native.json").read_bytes()
    assert negative["source_manifest"]["export_files"] == [
        {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    ]
    night["channels"]["body_battery"]["points"][0]["value"] = 0.0
    positive = run_training(tmp_path, [night], [], start="2026-10-25", end="2026-10-25")["product"]
    assert (
        negative["source_manifest"]["nights"][0]["snapshot_sha256"]
        == positive["source_manifest"]["nights"][0]["snapshot_sha256"]
    )
    assert negative["source_identity"] != positive["source_identity"]


def test_public_cli_does_not_choose_an_arbitrary_wake_date_for_overlapping_night_windows(tmp_path):
    first = exported_night("2026-10-25", "2026-10-24T21:58:00Z", "2026-10-25T06:48:00Z", [])
    second = exported_night("2026-10-26", "2026-10-24T21:58:00Z", "2026-10-25T06:48:00Z", [])
    rows = [["garmin-before", "2026-10-24 20:00:00", "Run", 3600, 3600, 140, "garmin"]]
    product = run_training(tmp_path, [first, second], rows)["product"]
    assert [row["status"] for row in product["nights"]] == [
        "ambiguous_sleep_window",
        "ambiguous_sleep_window",
    ]
    assert all(row["training"]["confirmed_activity_count"] is None for row in product["nights"])
    assert product["unlinked_counts"]["ambiguous_night_window"] == 1


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("ambiguous", "ambiguous_time"),
        ("documented_empty", "no_data"),
        ("available_empty", "partial"),
        ("mixed", "partial"),
    ],
)
def test_public_cli_preserves_export_quality_separately_from_local_exclusions(
    tmp_path, kind, expected
):
    values = [("2026-10-25T00:30:00Z", 50)] if kind == "mixed" else []
    raw = exported_night("2026-10-25", "2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z", values)
    channel = raw["channels"]["hrv"]
    ambiguous = 2 if kind in {"ambiguous", "mixed"} else 0
    channel["status"] = "no_data" if kind == "documented_empty" else "available"
    channel["quality"] = {
        "invalid": 0,
        "ambiguous_time": ambiguous,
        "outside_interval": 0,
        "duplicates": 0,
        "conflicting_timestamps": 0,
        "gaps": 0,
    }
    channel["quality_flags"] = (
        ["ambiguous_time_excluded", "coverage_unknown"] if ambiguous else ["coverage_unknown"]
    )
    product = run_training(tmp_path, [raw], [], start="2026-10-25", end="2026-10-25")["product"]
    result = product["nights"][0]["channels"]["hrv"]
    assert result["status"] == expected
    assert result["valid_points"] == len(values)
    assert result["invalid_points"] == 0
    assert result["quality_flags"] == []
    assert result["source_quality"]["counts"]["ambiguous_time"] == ambiguous
    assert result["source_quality"]["channel_status"] == channel["status"]
    assert result["source_quality"]["quality_flags"] == channel["quality_flags"]
    assert result["median"] == (50 if values else None)
    assert result["coverage"] is None


def association_nights():
    return [
        exported_night("2026-10-25", "2026-10-24T22:00:00Z", "2026-10-25T06:00:00Z", []),
        exported_night("2026-10-26", "2026-10-25T22:00:00Z", "2026-10-26T06:00:00Z", []),
    ]


def test_public_cli_narrowing_output_does_not_reassign_the_true_earliest_night(tmp_path):
    nights = association_nights()
    activity = [["garmin-before", "2026-10-24 20:00:00", "Run", 3600, 3600, 140, "garmin"]]
    wide = run_training(tmp_path, nights, activity)["product"]
    narrow = run_training(tmp_path, nights, activity, start="2026-10-26", end="2026-10-26")[
        "product"
    ]
    assert wide["nights"][0]["training"]["confirmed_activity_count"] == 1
    assert wide["nights"][1]["training"]["confirmed_activity_count"] == 0
    assert narrow["nights"][0]["training"]["confirmed_activity_count"] == 0
    assert narrow["nights"][0]["training"]["known_recorded_duration_seconds"] == 0
    assert narrow["unlinked_counts"]["outside_window"] == 1
    assert len(narrow["source_manifest"]["association_windows"]) == 2


def test_public_cli_keeps_recorded_sleep_overlap_outside_the_output_period(tmp_path):
    activity = [["garmin-overlap", "2026-10-25 05:30:00", "Run", 3600, 3600, 140, "garmin"]]
    narrow = run_training(
        tmp_path, association_nights(), activity, start="2026-10-26", end="2026-10-26"
    )["product"]
    training = narrow["nights"][0]["training"]
    assert narrow["unlinked_counts"]["overlaps_recorded_sleep"] == 1
    assert training["confirmed_activity_count"] == 0
    assert training["uncertain_activity_count"] == 1
    assert training["recorded_duration_seconds"] is None
    assert training["known_recorded_duration_seconds"] == 0
    assert training["status"] == "partial"


def test_public_cli_reports_ambiguous_window_when_only_the_other_overlap_is_outside_range(tmp_path):
    nights = association_nights()
    nights[0]["sleep_start_utc"] = "2026-10-25T21:30:00Z"
    nights[0]["sleep_end_utc"] = "2026-10-26T05:00:00Z"
    narrow = run_training(tmp_path, nights, [], start="2026-10-26", end="2026-10-26")["product"]
    assert narrow["nights"][0]["status"] == "ambiguous_sleep_window"
    assert narrow["nights"][0]["training"]["confirmed_activity_count"] is None


def test_public_cli_unavailable_measurements_do_not_erase_an_outside_documented_window(tmp_path):
    nights = association_nights()
    nights[0]["status"] = "read_error"
    activity = [["garmin-before", "2026-10-24 20:00:00", "Run", 3600, 3600, 140, "garmin"]]
    result = run_training(tmp_path, nights, activity, start="2026-10-26", end="2026-10-26")[
        "product"
    ]
    assert result["nights"][0]["training"]["confirmed_activity_count"] == 0
    assert result["unlinked_counts"]["ambiguous_night_window"] == 1
    assert result["nights"][0]["training"]["recorded_duration_seconds"] is None


def test_public_cli_projects_unknown_or_malformed_source_quality_without_echoing_it(tmp_path):
    raw = association_nights()[0]
    raw["channels"]["hrv"].update(
        status="no_data",
        quality={"ambiguous_time": "PRIVATE_UNKNOWN_COUNT"},
        quality_flags=["PRIVATE_UNKNOWN_FLAG"],
    )
    result = run_training(tmp_path, [raw], [], start="2026-10-25", end="2026-10-25")["product"]
    channel = result["nights"][0]["channels"]["hrv"]
    assert channel["status"] == "partial"
    assert channel["source_quality"]["metadata_status"] == "invalid"
    assert channel["source_quality"]["counts"]["ambiguous_time"] is None
    assert "PRIVATE_UNKNOWN" not in json.dumps(result)
