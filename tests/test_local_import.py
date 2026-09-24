import gzip
import json

from pipeline.health_detail_backfill import date_windows
from pipeline.local_import import (
    SPECS,
    import_file,
    indexed_records,
    read_export,
    run_import_cycle,
    run_local_import,
)


class MemoryStore:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []

    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}

    def get(self, key):
        return self.objects[key]

    def put(self, key, data, content_type, *, encoding=None):
        self.objects[key] = data
        self.puts.append((key, content_type, encoding))


def test_date_windows_groups_sparse_dates_in_bounded_newest_first_ranges():
    assert date_windows(
        ["2026-09-21", "2026-09-20", "2026-08-23", "2026-07-01"],
        max_span_days=31,
    ) == [
        ("2026-08-23", "2026-09-21", ["2026-09-21", "2026-09-20", "2026-08-23"]),
        ("2026-07-01", "2026-07-01", ["2026-07-01"]),
    ]


def test_read_export_accepts_raw_and_combined_files(tmp_path):
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps([{"hrvSummary": {"calendarDate": "2026-09-21"}}]))
    assert len(read_export(raw, SPECS["hrv"])) == 1

    combined = tmp_path / "combined.json"
    combined.write_text(json.dumps([
        {"metric": "activities", "data": [{"activityId": 1}]},
        {"metric": "sleep", "data": [{
            "dailySleepDTO": {
                "calendarDate": "2026-09-21",
                "sleepTimeSeconds": 25000,
            }
        }]},
    ]))
    assert len(read_export(combined, SPECS["sleep"])) == 1


def test_indexed_records_skips_errors_and_tracks_duplicate_dates(tmp_path):
    source = tmp_path / "hrv.json"
    source.write_text(json.dumps([
        {"hrvSummary": {"calendarDate": "2026-09-21"}},
        {"hrvSummary": {"calendarDate": "2026-09-21"}},
        {"date": "2026-09-20", "error": "rate limit"},
        {"unexpected": True},
    ]))

    records, stats = indexed_records(source, SPECS["hrv"])

    assert list(records) == ["2026-09-21"]
    assert stats == {
        "source_records": 4,
        "source_errors": 1,
        "missing_dates": 1,
        "duplicate_dates": 1,
    }


def test_import_file_skips_existing_and_writes_canonical_gzip(tmp_path):
    source = tmp_path / "hrv.json"
    source.write_text(json.dumps([
        {
            "hrvSummary": {"calendarDate": "2026-09-21", "lastNightAvg": 44},
            "hrvReadings": [{"readingTimeGMT": "2026-09-21T01:00:00Z", "hrvValue": 42}],
        },
        {
            "hrvSummary": {"calendarDate": "2026-09-20", "lastNightAvg": 41},
            "hrvReadings": [],
        },
    ]))
    existing_key = SPECS["hrv"].key("2026-09-21")
    store = MemoryStore({existing_key: b"existing"})

    result = import_file(
        source,
        SPECS["hrv"],
        store=store,
        limit=10,
    )

    imported_key = SPECS["hrv"].key("2026-09-20")
    payload = json.loads(gzip.decompress(store.objects[imported_key]))
    assert result["already_present"] == 1
    assert result["imported_this_run"] == 1
    assert result["status"] == "complete"
    assert payload["date"] == "2026-09-20"
    assert payload["available"] is False
    assert store.puts == [(imported_key, "application/json", "gzip")]


def test_import_cycle_shares_one_bounded_write_budget_across_streams(tmp_path):
    hrv = tmp_path / "hrv.json"
    hrv.write_text(json.dumps([
        {"hrvSummary": {"calendarDate": "2026-09-21"}},
        {"hrvSummary": {"calendarDate": "2026-09-20"}},
    ]))
    sleep = tmp_path / "sleep.json"
    sleep.write_text(json.dumps([
        {"dailySleepDTO": {"calendarDate": "2026-09-21", "sleepTimeSeconds": 25000}}
    ]))
    store = MemoryStore()

    result = run_import_cycle(
        files={"hrv": hrv, "sleep": sleep},
        batch_size=2,
        store=store,
    )

    assert result["hrv"]["imported_this_run"] == 2
    assert "sleep" not in result
    assert len(store.puts) == 2


def test_local_import_is_resumable_across_cycles(tmp_path):
    source = tmp_path / "hrv.json"
    source.write_text(json.dumps([
        {"hrvSummary": {"calendarDate": "2026-09-21"}},
        {"hrvSummary": {"calendarDate": "2026-09-20"}},
    ]))
    store = MemoryStore()
    sleeps = []

    result = run_local_import(
        files={"hrv": source},
        max_hours=1,
        pause_seconds=0,
        batch_size=1,
        status_file=tmp_path / "status.json",
        lock_file=tmp_path / "lock",
        store_factory=lambda: store,
        monotonic=lambda: 0,
        sleep=sleeps.append,
    )

    assert result["status"] == "complete"
    assert result["cycles"] == 2
    assert len(store.puts) == 2
    assert sleeps == [0]
    assert json.loads((tmp_path / "status.json").read_text())["status"] == "complete"
