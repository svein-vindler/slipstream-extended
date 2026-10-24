"""Requested sample sizes through actual export; Garmin is synthetic."""
import pytest
from garminconnect import Garmin

from pipeline import granular_export
from scripts.build_running_segments_contract import synthetic_tcx
from scripts.build_training_context_contract import synthetic_fit


class SyntheticGarmin:
    def __init__(self):
        self.downloads = []
        self.fit = synthetic_fit()

    def get_activities_by_date(self, *args):
        rows = [
            {"activityId": index, "activityType": {"typeKey": kind},
             "startTimeLocal": "2026-10-24 12:00:00", "startTimeGMT": "2026-10-24 10:00:00"}
            for kind, ids in (("running", range(1, 61)), ("strength_training", range(101, 161)))
            for index in ids
        ]
        return [rows[0], *rows, rows[-1]]

    def download_activity(self, activity_id, *, dl_fmt):
        self.downloads.append(activity_id)
        return self.fit if dl_fmt == Garmin.ActivityDownloadFormat.ORIGINAL else synthetic_tcx()

    def get_activity_exercise_sets(self, activity_id):
        return {"exerciseSets": []}


@pytest.mark.parametrize("cardio,strength,expected", [
    (0, 1, ["101"]), (1, 0, ["1"]), (0, 0, []), (1, 1, ["1", "101"]),
    (25, 25, [str(i) for i in range(1, 26)] + [str(i) for i in range(101, 126)]),
])
def test_export_honors_sample_sizes_before_downloads(monkeypatch, tmp_path, cardio, strength, expected):
    garmin = SyntheticGarmin()
    monkeypatch.setattr("pipeline.granular_export._login", lambda: garmin)
    result = granular_export.run(
        hrv_days=0, cardio_count=cardio, strength_count=strength, output_dir=str(tmp_path)
    )
    assert [item["id"] for item in result["activities"]] == expected
    assert all(item.get("status") == "updated" for item in result["activities"])
    assert set(garmin.downloads) == {int(value) for value in expected}


def test_oversized_request_stops_before_authentication(monkeypatch, tmp_path):
    def unexpected():
        pytest.fail("Invalid request must stop before any Garmin access")

    monkeypatch.setattr("pipeline.granular_export._login", unexpected)
    with pytest.raises(ValueError, match="combined"):
        granular_export.run(hrv_days=0, cardio_count=26, strength_count=25, output_dir=str(tmp_path))
    assert not list(tmp_path.iterdir())
