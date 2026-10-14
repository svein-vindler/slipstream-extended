"""Partial moving-time data stays consistent from summary to immutable coach."""
import csv
import gzip
import json
from datetime import date

import pytest
from test_latest_activity import FakeStore

from pipeline import latest_activity
from pipeline.coach_backfill import run_one
from pipeline.sources.garmin import fetch
from pipeline.writer import write_dataset
from scripts.benchmark_coach_reuse import CountingStore


class Provider:
    def __init__(self, **fields):
        self.row = {"activityId": 1, "startTimeGMT": "2026-09-30 08:00:00",
                    "startTimeLocal": "2026-09-30 10:00:00",
                    "activityType": {"typeKey": "running"}, "activityName": "Synthetic run", **fields}

    def get_activities_by_date(self, *_args):
        return [self.row]

    def get_activity(self, _activity_id):
        return self.row


@pytest.fixture(autouse=True)
def fixed_recent_window(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 30)
    monkeypatch.setattr("pipeline.freshness.date", FixedDate)


def canonical(store):
    pointer = json.loads(store.objects["activities/2026/1/coach-input/v1/latest-ready.json"])
    key = pointer["analysis_key"]
    return key, store.objects[key], json.loads(gzip.decompress(store.objects[key]))


def import_latest(root, store, source, run_id):
    return latest_activity.run(run_id=run_id, data_dir=str(root), requested_activity_id="1",
                               expected_date="2026-09-30", store=store, garmin=source)


@pytest.mark.parametrize("moving,expected", [(None, 1800), (2000, 2000), (0, 0)],
                         ids=["elapsed-only-retains", "explicit-moving-updates", "zero-updates"])
def test_latest_keeps_merged_moving_in_csv_and_immutable_canonical(tmp_path, moving, expected):
    write_dataset(fetch(client=Provider(distance=5000, movingDuration=1800, duration=1900)), str(tmp_path))
    (tmp_path / "health_daily.csv").write_text("Date,Source\n", encoding="utf-8")
    store = FakeStore(tmp_path)
    store.objects.update(CountingStore().objects)
    store.objects["activities/2026/1/activity.fit"] = b"synthetic-fit"
    run_one(activity={"id": "1", "date": "2026-09-30", "name": "Synthetic run",
                      "type": "running", "moving_seconds": 1800}, store=store)
    old_key, old_bytes, _ = canonical(store)
    source = Provider(duration=2000, **({"movingDuration": moving} if moving is not None else {}))
    result = import_latest(tmp_path, store, source, "123")
    assert result["status"] == "ready"
    with (tmp_path / "activities.csv").open(newline="", encoding="utf-8") as stream:
        row = next(csv.DictReader(stream))
    assert (row["Moving Time"], row["Elapsed Time"]) == (str(expected), "2000")
    assert canonical(store)[2]["summary"]["moving_seconds"] == expected
    assert store.objects[old_key] == old_bytes
    current_key, current_bytes, _ = canonical(store)
    current_csv = (tmp_path / "activities.csv").read_bytes()
    assert import_latest(tmp_path, store, source, "124")["status"] == "ready"
    assert canonical(store)[:2] == (current_key, current_bytes)
    assert (tmp_path / "activities.csv").read_bytes() == current_csv
    assert store.objects[old_key] == old_bytes


def test_new_latest_activity_uses_elapsed_fallback_in_csv_and_canonical(tmp_path):
    (tmp_path / "health_daily.csv").write_text("Date,Source\n", encoding="utf-8")
    store = FakeStore(tmp_path)
    store.objects.update(CountingStore().objects)
    store.objects["activities/2026/1/activity.fit"] = b"synthetic-fit"
    source = Provider(duration=2000)
    assert import_latest(tmp_path, store, source, "125")["status"] == "ready"
    with (tmp_path / "activities.csv").open(newline="", encoding="utf-8") as stream:
        row = next(csv.DictReader(stream))
    assert (row["Moving Time"], row["Elapsed Time"]) == ("2000", "2000")
    assert canonical(store)[2]["summary"]["moving_seconds"] == 2000
    current_key, current_bytes, _ = canonical(store)
    assert import_latest(tmp_path, store, source, "126")["status"] == "ready"
    assert canonical(store)[:2] == (current_key, current_bytes)
