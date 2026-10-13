"""Activity refresh contracts against synthetic provider responses and CSV output."""
import csv
from datetime import date

import pytest

from pipeline.sources.garmin import fetch
from pipeline.writer import write_dataset


class Provider:
    def __init__(self, row):
        self.row = row

    def get_activities_by_date(self, *_args):
        return [self.row]


def sync(root, fields):
    raw = {"activityId": 123, "startTimeGMT": "2026-10-13 08:00:00", **fields}
    activities = fetch(client=Provider(raw), end_date=date(2026, 10, 13))
    write_dataset(activities, str(root), preserve_existing=True)
    with (root / "activities.csv").open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))[0]


def test_partial_activity_retains_known_metrics_and_updates_supplied_name(tmp_path):
    before = sync(tmp_path, {"activityName": "Synthetic run", "activityType": {"typeKey": "running"},
                            "distance": 5000, "movingDuration": 1800, "duration": 1900,
                            "averageHR": 150, "maxHR": 175, "calories": 350})
    after = sync(tmp_path, {"activityName": "Renamed run"})
    assert after == {**before, "Activity Name": "Renamed run"}


def test_zero_metrics_replace_previous_values_without_power_alias_fallback(tmp_path):
    sync(tmp_path, {"distance": 5000, "movingDuration": 1800, "duration": 1900,
                    "averageHR": 150, "maxHR": 175, "calories": 350,
                    "avgPower": 200, "elevationGain": 50})
    after = sync(tmp_path, {"distance": 0, "movingDuration": 0, "duration": 0,
                           "averageHR": 0, "maxHR": 0, "calories": 0,
                           "avgPower": 0, "averagePower": 100, "elevationGain": 0})
    assert {key: after[key] for key in ("Distance", "Moving Time", "Elapsed Time",
                                      "Average Heart Rate", "Max Heart Rate", "Calories",
                                      "Average Watts", "Elevation Gain")} == {
        "Distance": "0", "Moving Time": "0", "Elapsed Time": "0",
        "Average Heart Rate": "0", "Max Heart Rate": "0", "Calories": "0",
        "Average Watts": "0", "Elevation Gain": "0"}
    assert after["Average Speed"] == ""


@pytest.mark.parametrize("invalid", [None, -1, True, "bad", float("nan"), float("inf")])
def test_invalid_metrics_do_not_replace_known_observations(tmp_path, invalid):
    before = sync(tmp_path, {"distance": 5000, "movingDuration": 1800, "duration": 1900,
                            "averageHR": 150, "maxHR": 175, "calories": 350,
                            "avgPower": 200, "elevationGain": 50})
    after = sync(tmp_path, {key: invalid for key in ("distance", "movingDuration", "duration",
                                                   "averageHR", "maxHR", "calories",
                                                   "avgPower", "elevationGain")})
    assert after == before


def test_elapsed_only_update_retains_known_moving_time(tmp_path):
    sync(tmp_path, {"distance": 5000, "movingDuration": 1800, "duration": 1900})
    after = sync(tmp_path, {"duration": 2000})
    assert (after["Moving Time"], after["Elapsed Time"], after["Average Speed"]) == (
        "1800", "2000", "2.778")


@pytest.mark.parametrize("invalid", ["-1", "nan", "inf", "bad"])
def test_sparse_refresh_does_not_carry_obviously_invalid_stored_metrics(tmp_path, invalid):
    before = sync(tmp_path, {"distance": 5000, "movingDuration": 1800, "duration": 1900,
                            "averageHR": 150, "maxHR": 175, "calories": 350,
                            "avgPower": 200, "elevationGain": 50})
    numeric = ("Distance", "Moving Time", "Elapsed Time", "Average Heart Rate", "Max Heart Rate",
               "Calories", "Average Watts", "Elevation Gain", "Average Speed")
    with (tmp_path / "activities.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(before))
        writer.writeheader()
        writer.writerow({**before, **dict.fromkeys(numeric, invalid)})
    after = sync(tmp_path, {})
    assert {key: after[key] for key in numeric} == dict.fromkeys(numeric, "")


@pytest.mark.parametrize("invalid", [None, "", "  ", 123, True, {}, []])
def test_invalid_name_or_sport_does_not_replace_known_text(tmp_path, invalid):
    before = sync(tmp_path, {"activityName": "Synthetic run", "activityType": {"typeKey": "running"}})
    after = sync(tmp_path, {"activityName": invalid, "activityType": {"typeKey": invalid}})
    assert after == before


def test_metric_updates_recompute_speed_and_repeated_sparse_refresh_is_idempotent(tmp_path):
    sync(tmp_path, {"activityName": "First", "distance": 5000, "movingDuration": 1800,
                    "averageHR": 150, "activityType": {"typeKey": "running"}})
    after = sync(tmp_path, {"distance": 6000, "averageHR": 155,
                           "activityType": {"typeKey": "cycling"}})
    assert (after["Distance"], after["Average Heart Rate"], after["Average Speed"],
            after["Activity Type"], after["Raw Sport"]) == ("6", "155", "3.333", "Ride", "cycling")
    original = (tmp_path / "activities.csv").read_bytes()
    assert sync(tmp_path, {}) == after
    assert (tmp_path / "activities.csv").read_bytes() == original
    assert sync(tmp_path, {}) == after
    assert (tmp_path / "activities.csv").read_bytes() == original


def test_new_activity_keeps_elapsed_fallback_and_missing_fields_blank(tmp_path):
    row = sync(tmp_path, {"duration": 1200})
    assert (row["Moving Time"], row["Elapsed Time"], row["Distance"], row["Average Speed"],
            row["Average Heart Rate"]) == ("1200", "1200", "", "", "")
