"""Stored synthetic native curves -> public offline CLI reference products."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def night(day, value, count=1, *, zone=None, context=None):
    return {
        "date": day,
        "wake_date": day,
        "method_version": "night-signals-v1",
        "resolution": "native",
        "status": "available",
        "sleep_start_utc": f"{day}T00:00:00Z",
        "sleep_end_utc": f"{day}T01:00:00Z",
        "actual_night_timezone": zone,
        "user_context": context,
        "channels": {
            "hrv": {
                "status": "available",
                "unit": "ms",
                "source_revisions": [{"date": day, "revision": "synthetic-source-v1"}],
                "points": [
                    {"timestamp_utc": f"{day}T00:{i:02}:00Z", "value": value, "time_basis": "utc"}
                    for i in range(count)
                ],
            }
        },
    }


def run_baseline(tmp_path, nights, *extra):
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"nights": nights}), encoding="utf-8")
    command = [
        sys.executable,
        "-m",
        "pipeline.night_products",
        "baseline",
        "--input",
        str(source),
        "--end-date",
        "2026-10-25",
        "--period",
        "7",
        "--alignment",
        "sleep_start",
        "--bin-minutes",
        "15",
        "--channel",
        "hrv",
        *extra,
    ]
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        command, cwd=ROOT, env=environment, text=True, capture_output=True, check=False
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_public_cli_weights_nights_equally_before_linear_reference_quantiles(tmp_path):
    exported = [
        night("2026-10-22", 40),
        night("2026-10-23", 60, 3),
        night("2026-10-24", 80, 5),
        night("2026-10-25", 100, 7),
    ]
    output = run_baseline(tmp_path, exported)
    product = output["product"]
    assert product["kind"] == "night-baseline"
    assert product["method_version"] == "night-baseline-v1"
    assert product["args"]["start_date"] == "2026-10-19"
    assert product["bins"][0] == {
        "bin_index": 0,
        "offset_minutes": 0,
        "p10": 46,
        "p25": 55,
        "p50": 70,
        "p75": 85,
        "p90": 94,
        "nights": 4,
        "valid_points": 16,
    }
    assert product["nights_requested"] == 7
    assert product["contributing_nights"] == 4
    assert output["storage_key"].startswith("health/night-products/v1/baseline/2026/10/25/")


def user_context(day, **values):
    return {
        "schema_version": 1,
        "source": "user",
        "wake_date": day,
        "context_id": f"context-{day}",
        "created_at": f"{day}T12:00:00Z",
        "previous_context_id": None,
        "changed_fields": sorted(values),
        **values,
    }


def test_public_cli_reports_explicit_exclusions_and_requires_documented_user_filter_values(
    tmp_path,
):
    exported = [
        night("2026-10-21", 20, context=user_context("2026-10-21", illness=None)),
        night("2026-10-22", 40, context=user_context("2026-10-22", illness=False)),
        night(
            "2026-10-23",
            60,
            context=user_context("2026-10-23", illness=False, note="PRIVATE_NOTE_MUST_NOT_APPEAR"),
        ),
        night(
            "2026-10-24",
            80,
            context={**user_context("2026-10-24", illness=False), "source": "garmin"},
        ),
        night("2026-10-25", 100, context=user_context("2026-10-25", illness=False)),
    ]
    output = run_baseline(
        tmp_path, exported, "--context-filter", '{"illness":false}', "--exclude-date", "2026-10-25"
    )
    product = output["product"]
    assert product["bins"][0] == {
        "bin_index": 0,
        "offset_minutes": 0,
        "p10": 42,
        "p25": 45,
        "p50": 50,
        "p75": 55,
        "p90": 58,
        "nights": 2,
        "valid_points": 2,
    }
    assert {entry["wake_date"]: entry["reason"] for entry in product["excluded"]} == {
        "2026-10-19": "not_in_export",
        "2026-10-20": "not_in_export",
        "2026-10-21": "context_field_unknown",
        "2026-10-24": "context_source_unverified",
        "2026-10-25": "explicit_date_exclusion",
    }
    assert "PRIVATE_NOTE_MUST_NOT_APPEAR" not in json.dumps(output)


def test_public_cli_wake_alignment_uses_negative_utc_offsets_and_explicit_empty_bins(tmp_path):
    output = run_baseline(tmp_path, [night("2026-10-25", 50, 3)], "--alignment", "wake")
    bins = output["product"]["bins"]
    assert [(entry["bin_index"], entry["p50"], entry["nights"]) for entry in bins] == [
        (-4, 50, 1),
        (-3, None, 0),
        (-2, None, 0),
        (-1, None, 0),
    ]


def test_public_cli_clock_alignment_combines_dst_fold_before_one_night_vote(tmp_path):
    fold = night("2026-10-25", 40, zone="Europe/Oslo")
    fold["sleep_end_utc"] = "2026-10-25T03:00:00Z"
    fold["channels"]["hrv"]["points"] = [
        {"timestamp_utc": "2026-10-25T00:30:00Z", "value": 40, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T01:30:00Z", "value": 80, "time_basis": "utc"},
    ]
    regular = night("2026-10-26", 100, zone="Europe/Oslo")
    regular["sleep_end_utc"] = "2026-10-26T03:00:00Z"
    regular["channels"]["hrv"]["points"][0]["timestamp_utc"] = "2026-10-26T01:30:00Z"
    output = run_baseline(
        tmp_path,
        [night("2026-10-24", 10), fold, regular],
        "--alignment",
        "clock",
        "--end-date",
        "2026-10-26",
    )
    product = output["product"]
    bin_at_0230 = next(entry for entry in product["bins"] if entry["bin_index"] == 10)
    assert bin_at_0230 == {
        "bin_index": 10,
        "offset_minutes": 150,
        "p10": 64,
        "p25": 70,
        "p50": 80,
        "p75": 90,
        "p90": 96,
        "nights": 2,
        "valid_points": 3,
    }
    assert {"wake_date": "2026-10-24", "reason": "unknown_actual_timezone"} in product["excluded"]
    assert {"wake_date": "2026-10-25", "flags": ["dst_fold_combined"]} in product["quality_notes"]


def test_public_cli_never_double_votes_duplicate_chunks_or_selects_a_conflicting_night_silently(
    tmp_path,
):
    exported = [
        night("2026-10-22", 40),
        night("2026-10-23", 60, 3),
        night("2026-10-24", 80, 5),
        night("2026-10-25", 100, 7),
    ]
    duplicated = [*exported, {**exported[1], "resources": {"elapsed_ms": 123}}]
    output = run_baseline(tmp_path, duplicated)
    assert output["product"]["bins"][0]["nights"] == 4
    assert output["product"]["bins"][0]["valid_points"] == 16
    conflicting = run_baseline(tmp_path, [*duplicated, night("2026-10-25", 10)])
    assert conflicting["product"]["bins"][0]["p50"] == 60
    assert conflicting["product"]["bins"][0]["nights"] == 3
    assert {"wake_date": "2026-10-25", "reason": "duplicate_conflict"} in conflicting["product"][
        "excluded"
    ]


def test_public_cli_rejects_ambiguous_invalid_and_duplicate_instants_before_nightly_median(
    tmp_path,
):
    exported = night("2026-10-25", 40)
    exported["channels"]["hrv"]["points"] = [
        {"timestamp_utc": "2026-10-25T00:00:00Z", "value": 40, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:00:00Z", "value": 40, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:01:00Z", "value": 80, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:01:00Z", "value": 10, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:02:00Z", "value": 60, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:03:00Z", "value": 0, "time_basis": "utc"},
        {"timestamp_utc": "2026-10-25T00:04:00Z", "value": 50, "time_basis": "local_only"},
        {"timestamp_utc": "2026-10-24T23:59:59Z", "value": 20, "time_basis": "utc"},
    ]
    output = run_baseline(tmp_path, [exported])
    assert output["product"]["bins"][0]["p50"] == 50
    assert output["product"]["bins"][0]["valid_points"] == 2
    assert output["product"]["resources"]["invalid_points"] == 6


def test_public_cli_excludes_unknown_export_methods_even_when_numeric_points_look_plausible(
    tmp_path,
):
    exported = night("2026-10-25", 50)
    exported["method_version"] = "unreviewed-source-v99"
    output = run_baseline(tmp_path, [exported])
    assert output["product"]["contributing_nights"] == 0
    assert output["product"]["bins"] == []
    assert {"wake_date": "2026-10-25", "reason": "unsupported_source_method"} in output["product"][
        "excluded"
    ]


@pytest.mark.parametrize(
    ("period", "start"),
    [("7", "2026-10-19"), ("28", "2026-09-28"), ("90", "2026-07-28"), ("365", "2025-10-26")],
)
def test_public_cli_presets_derive_exact_inclusive_calendar_windows(tmp_path, period, start):
    output = run_baseline(tmp_path, [night("2026-10-25", 50)], "--period", period)
    assert output["product"]["args"]["start_date"] == start
    assert output["product"]["nights_requested"] == int(period)


def test_public_cli_rejects_oversized_exports_and_unknown_filters_without_raw_notes(tmp_path):
    source = tmp_path / "oversized.json"
    source.write_text(
        json.dumps({"nights": [], "padding": "x" * (16 * 1024 * 1024)}), encoding="utf-8"
    )
    base = [
        sys.executable,
        "-m",
        "pipeline.night_products",
        "baseline",
        "--input",
        str(source),
        "--end-date",
        "2026-10-25",
    ]
    result = subprocess.run(base, cwd=ROOT, text=True, capture_output=True, check=False)
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    source.write_text(json.dumps({"nights": [night("2026-10-25", 50)]}), encoding="utf-8")
    result = subprocess.run(
        [*base, "--context-filter", '{"note":"PRIVATE_FILTER_MUST_NOT_LEAK"}'],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert "PRIVATE_FILTER_MUST_NOT_LEAK" not in result.stderr
    assert "Traceback" not in result.stderr


def test_public_cli_enforces_the_point_budget_before_point_processing(tmp_path):
    exported = night("2026-10-25", 50)
    exported["channels"]["hrv"]["points"] = [None] * 200001
    source = tmp_path / "points.json"
    source.write_text(json.dumps({"nights": [exported]}), encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline.night_products",
            "baseline",
            "--input",
            str(source),
            "--end-date",
            "2026-10-25",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Traceback" not in result.stderr


def test_public_cli_excludes_failed_export_even_when_old_channel_points_remain(tmp_path):
    exported = night("2026-10-25", 50)
    exported["status"] = "read_error"
    output = run_baseline(tmp_path, [exported])
    assert output["product"]["contributing_nights"] == 0
    assert {"wake_date": "2026-10-25", "reason": "channel_unavailable"} in output["product"][
        "excluded"
    ]


@pytest.mark.parametrize("channel", ["body_battery", "stress"])
def test_public_cli_keeps_recorded_zero_for_garmin_derived_point_channels(tmp_path, channel):
    exported = night("2026-10-25", 50)
    exported["channels"] = {
        channel: {
            "status": "available",
            "unit": "points",
            "source_revisions": [],
            "points": [
                {"timestamp_utc": "2026-10-25T00:00:00Z", "value": 0, "time_basis": "utc"},
                {"timestamp_utc": "2026-10-25T00:01:00Z", "value": 100, "time_basis": "utc"},
                {"timestamp_utc": "2026-10-25T00:02:00Z", "value": -1, "time_basis": "utc"},
            ],
        }
    }
    output = run_baseline(tmp_path, [exported], "--channel", channel)
    assert output["product"]["bins"][0]["p50"] == 50
    assert output["product"]["bins"][0]["valid_points"] == 2
    assert output["product"]["resources"]["invalid_points"] == 1


def test_public_cli_custom_window_is_inclusive_and_bounded_to366_days(tmp_path):
    output = run_baseline(
        tmp_path, [night("2026-10-25", 50)], "--period", "custom", "--start-date", "2025-10-25"
    )
    assert output["product"]["nights_requested"] == 366
