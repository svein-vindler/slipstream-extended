"""Synthetic recent-health operation counts; excludes legacy backfill plans."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.granular import gzip_json, normalize_hrv  # noqa: E402
from pipeline.health_detail import normalize_sleep_detail  # noqa: E402
from pipeline.health_history_index import (  # noqa: E402
    INDEX_PREFIXES,
    SOURCE_PREFIXES,
    _decode_json,
    _month_sources,
    build_month_index,
    index_key,
)
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.recent_health import run  # noqa: E402
from scripts.benchmark_summary_reuse import SummaryClient  # noqa: E402


class HealthClient(SummaryClient):
    def __init__(self):
        super().__init__()
        self.reset()

    def reset(self):
        self.counts = dict.fromkeys(self.counts, 0)
        self.reads = []
        self.writes = []
        self.prefixes = []

    def get_object(self, **kwargs):
        self.reads.append(kwargs["Key"])
        return super().get_object(**kwargs)

    def put_object(self, **kwargs):
        self.writes.append(kwargs["Key"])
        return super().put_object(**kwargs)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, **kwargs):
                client.counts["list"] += 1
                prefix = kwargs.get("Prefix", "")
                client.prefixes.append(prefix)
                return [{"Contents": [{"Key": key, "Size": len(value["Body"]),
                                       "ETag": '"' + hashlib.md5(value["Body"], usedforsecurity=False).hexdigest() + '"'}
                                      for key, value in sorted(client.objects.items()) if key.startswith(prefix)]}]

        return Paginator()


class SyntheticGarmin:
    def __init__(self):
        self.calls = []
        self.responses = {}

    def _fetch(self, stream, day):
        self.calls.append((stream, day))
        if (stream, day) in self.responses:
            value = self.responses[stream, day]
            if isinstance(value, Exception):
                raise value
            return value
        if stream == "sleep":
            return {"dailySleepDTO": {"calendarDate": day, "sleepTimeSeconds": 25200,
                                     "sleepStartTimestampGMT": f"{day}T00:00:00Z",
                                     "sleepEndTimestampGMT": f"{day}T07:00:00Z",
                                     "sleepStartTimestampLocal": f"{day}T00:00:00",
                                     "sleepEndTimestampLocal": f"{day}T07:00:00"},
                    "sleepLevels": [{"startGMT": f"{day}T00:00:00Z",
                                     "endGMT": f"{day}T07:00:00Z", "activityLevel": 1}]}
        return {"hrvSummary": {"calendarDate": day, "lastNightAvg": 50},
                "hrvReadings": [{"readingTimeGMT": f"{day}T00:{index // 60:02}:{index % 60:02}Z",
                                 "hrvValue": 40 + index % 20} for index in range(1200)]}

    def get_sleep_data(self, day):
        return self._fetch("sleep", day)

    def get_hrv_data(self, day):
        return self._fetch("hrv", day)


def baseline(store, garmin, today, days):
    """Previous unconditional day writes and global index inventories."""
    for stream in ("sleep", "hrv"):
        for offset in range(days):
            day = (today - timedelta(days=offset)).isoformat()
            raw = garmin.get_sleep_data(day) if stream == "sleep" else garmin.get_hrv_data(day)
            payload = normalize_sleep_detail(day, raw) if stream == "sleep" else normalize_hrv(day, raw)
            key = f"{SOURCE_PREFIXES[stream]}{day[:4]}/{day[5:7]}/{day}.json"
            store.put(key, gzip_json(payload), "application/json", encoding="gzip")
        grouped = _month_sources(store.list_object_revisions(SOURCE_PREFIXES[stream]))
        indexes = store.list_object_revisions(INDEX_PREFIXES[stream])
        requested = {(today - timedelta(days=offset)).isoformat()[:7] for offset in range(days)}
        for month in sorted(requested):
            key = index_key(stream, month)
            expected = {source_key: revision for source_key, revision in grouped[month].values()}
            if key in indexes:
                current = _decode_json(store.get(key), key)
                if current.get("builder_revision") == 2 and current.get("source_revisions") == expected:
                    continue
            store.put(key, gzip_json(build_month_index(stream, month, grouped[month], store)),
                      "application/json", encoding="gzip")


def measure(client, garmin, *, old=False):
    client.reset()
    garmin.calls = []
    store = R2Store(client=client, bucket="synthetic-health")
    today = date(2026, 9, 30)
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        if old:
            baseline(store, garmin, today, 3)
        else:
            run(store=store, garmin=garmin, today=today)
    def canonical(key):
        return any(key.startswith(prefix) for prefix in SOURCE_PREFIXES.values())
    return {**client.counts, "provider_calls": len(garmin.calls),
            "canonical_put": sum(canonical(key) for key in client.writes),
            "canonical_get": sum(canonical(key) for key in client.reads),
            "index_put": sum(key.startswith("health/indexes/") for key in client.writes),
            "checkpoint_put": sum(key.startswith("refresh/checks/") for key in client.writes)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    client, garmin = HealthClient(), SyntheticGarmin()
    initial = measure(client, garmin, old=args.baseline)
    repeated = measure(client, garmin, old=args.baseline)
    print(json.dumps({"mode": "baseline" if args.baseline else "incremental",
                      "initial": initial, "unchanged": repeated}, indent=2))


if __name__ == "__main__":
    main()
