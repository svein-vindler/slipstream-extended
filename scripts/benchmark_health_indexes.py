"""Reproducible full/recent R2 index comparison using synthetic paginated history."""
from __future__ import annotations

import hashlib
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.granular import gzip_json  # noqa: E402
from pipeline.health_history_index import SOURCE_PREFIXES  # noqa: E402
from pipeline.health_history_index import run as full_run
from pipeline.health_index_reconcile import run  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient  # noqa: E402

NOW = datetime(2026, 10, 5, tzinfo=timezone.utc)


class PaginatedHealthClient(HealthClient):
    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, **kwargs):
                prefix = kwargs.get("Prefix", "")
                items = [{"Key": key, "Size": len(value["Body"]),
                          "ETag": '"' + hashlib.md5(value["Body"], usedforsecurity=False).hexdigest() + '"'}
                         for key, value in sorted(client.objects.items()) if key.startswith(prefix)]
                for start in range(0, max(1, len(items)), 1000):
                    client.counts["list"] += 1
                    client.prefixes.append(prefix)
                    yield {"Contents": items[start:start + 1000]}

        return Paginator()


def seed(client, months, *, days=28):
    for month in months:
        for offset in range(days):
            day = f"{month}-{offset + 1:02}"
            for stream in SOURCE_PREFIXES:
                payload = {"date": day, "summary": {"lastNightAvg": 50}, "readings": []} if stream == "hrv" else {
                    "date": day, "summary": {"sleep_seconds": 25200}, "stages": [], "stage_count": 0}
                key = f"{SOURCE_PREFIXES[stream]}{month[:4]}/{month[5:7]}/{day}.json"
                client.objects[key] = {"Body": gzip_json(payload), "ContentType": "application/json", "ContentEncoding": "gzip"}
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        run(store=R2Store(client=client, bucket="synthetic-indexes", max_writes_per_run=500), now=NOW)
    client.reset()


def measure(client, *, full=False):
    client.reset()
    store = R2Store(client=client, bucket="synthetic-indexes")
    started = perf_counter()
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        report = full_run(store=store) if full else run(store=store, now=NOW)
    return {"r2_operations": store.operations, "months_considered": sum(
        r["months_considered"] for r in report["results"].values()),
        "local_elapsed_ms": round((perf_counter() - started) * 1000, 3)}


def main():
    months = [f"{year:04}-{month:02}" for year in range(2016, 2027) for month in range(1, 13)
              if "2016-11" <= f"{year:04}-{month:02}" <= "2026-10"]
    client = PaginatedHealthClient()
    seed(client, months)
    baseline = measure(client, full=True)
    recent = measure(client)
    print(json.dumps({"fixture": {"months_per_stream": len(months), "days_per_month": 28},
                      "provider_calls": 0, "full": baseline, "recent": recent}, indent=2))


if __name__ == "__main__":
    main()
