"""Safety checks for targeted, existing-object night repairs."""

from __future__ import annotations

import gzip
import json

import pytest

from pipeline import night_local_repair as repair


class MemoryStore:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.writes = []

    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}

    def get(self, key):
        return self.objects[key]

    def put(self, key, data, content_type, *, encoding=None):
        self.objects[key] = data
        self.writes.append((key, content_type, encoding))


def test_date_file_margin_deduplicates_and_rejects_large_ranges(tmp_path):
    path = tmp_path / "dates.local"
    path.write_text("2026-07-07..2026-07-08\n2026-07-08 # overlap\n")
    assert repair.dates_from_file(path) == [
        "2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09",
    ]
    path.write_text("2026-01-01..2026-12-31\n")
    with pytest.raises(ValueError, match="too large"):
        repair.dates_from_file(path)


def test_plan_does_not_offer_absent_or_already_local_objects():
    local = {"sleep_start_gmt": 1, "sleep_end_gmt": 2,
             "sleep_start_garmin_local": 1, "sleep_end_garmin_local": 2}
    store = MemoryStore({
        repair.object_key("sleep", "2026-07-07"): json.dumps({"date": "2026-07-07"}).encode(),
        repair.object_key("hrv", "2026-07-07"): gzip.compress(json.dumps(local).encode()),
    })
    pending, counts = repair.plan(store, ["2026-07-07", "2026-07-08"])
    assert pending == [("sleep", "2026-07-07")]
    assert counts == {"target_days": 2, "existing_sleep": 1, "existing_hrv": 1,
                      "already_local": 1, "missing_objects": 2}


def test_valid_local_window_accepts_travel_offset_and_rejects_bad_offset():
    payload = {
        "sleep_start_gmt": 1783610700000,
        "sleep_start_garmin_local": 1783643100000,
        "sleep_end_gmt": 1783636860000,
        "sleep_end_garmin_local": 1783669260000,
    }
    assert repair.valid_local_window(payload)
    assert not repair.valid_local_window({**payload, "sleep_start_garmin_local": 1783700700000})


def test_apply_backs_up_before_replacing_and_syncs_only_changed_dates(tmp_path, monkeypatch):
    day = "2026-07-10"
    key = repair.object_key("sleep", day)
    old = gzip.compress(json.dumps({"date": day, "summary": {"sleep_seconds": 1200}}).encode())
    store = MemoryStore({key: old})

    class Garmin:
        def get_sleep_data(self, requested):
            assert requested == day
            return {"dailySleepDTO": {
                "calendarDate": day,
                "sleepTimeSeconds": 7200,
                "sleepStartTimestampGMT": 1783610700000,
                "sleepStartTimestampLocal": 1783643100000,
                "sleepEndTimestampGMT": 1783636860000,
                "sleepEndTimestampLocal": 1783669260000,
            }}

    synced = []
    monkeypatch.setattr(repair, "sync_dates", lambda store, stream, dates: synced.append((stream, dates)))
    result = repair.apply(store, Garmin(), [("sleep", day)], backup_dir=tmp_path,
                          request_pause=0)
    assert result["completed"] == {"sleep": [day], "hrv": []}
    assert (tmp_path / key).read_bytes() == old
    assert repair._json_object(store.get(key))["sleep_start_garmin_local"] == 1783643100000
    assert store.writes == [(key, "application/json", "gzip")]
    assert synced == [("sleep", [day])]


def test_apply_never_overwrites_when_garmin_local_window_is_missing(tmp_path):
    day = "2026-07-10"
    key = repair.object_key("sleep", day)
    old = b'{"date":"2026-07-10"}'
    store = MemoryStore({key: old})

    class Garmin:
        def get_sleep_data(self, _requested):
            return {"dailySleepDTO": {"sleepTimeSeconds": 3600}}

    result = repair.apply(store, Garmin(), [("sleep", day)], backup_dir=tmp_path,
                          request_pause=0)
    assert not store.writes
    assert result["skipped"][0]["reason"] == "Garmin local sleep window is absent or invalid"

