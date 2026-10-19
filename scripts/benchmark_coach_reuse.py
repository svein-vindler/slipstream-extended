"""Measure targeted coach work with synthetic data and no external services.

Run from the repository root: python scripts/benchmark_coach_reuse.py
The TCX payload is deliberately 1 MiB; timings measure local Python work only.
"""

from __future__ import annotations

import hashlib
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from time import perf_counter

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.coach_backfill import run_one  # noqa: E402
from pipeline.r2_store import R2ObjectSnapshot, R2ReadBoundaryError  # noqa: E402


class CountingStore:
    def __init__(self):
        prefix = "activities/2026/1"
        self.objects = {
            f"{prefix}/activity.v1.json": json.dumps({
                "activity": {"id": "1"}, "source_fit_sha256": "synthetic-fit",
            }).encode(),
            f"{prefix}/activity.endurance.v1.json": json.dumps({
                "available": True, "summary": {"distance_m": 5000},
            }).encode(),
            f"{prefix}/activity.tcx": b"x" * 1024 * 1024,
            "coach/profiles/v1/2026-01-01/test.json": json.dumps({
                "profile_id": "synthetic-profile", "effective_from": "2026-01-01",
                "zones": [{"label": "test", "min_bpm": 100, "max_bpm": 150}],
            }).encode(),
        }
        self.reset_counts()

    def reset_counts(self):
        self.counts = {"get": 0, "list": 0, "put": 0, "download_bytes": 0}

    def get(self, key):
        self.counts["get"] += 1
        self.counts["download_bytes"] += len(self.objects[key])
        return self.objects[key]

    def get_snapshot(self, key, *, max_bytes=None):
        """Synthetic outer storage with the production bounded-read interface."""
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Invalid storage read limit")
        if key not in self.objects:
            self.counts["get"] += 1
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        if max_bytes is not None and len(self.objects[key]) > max_bytes:
            self.counts["get"] += 1
            raise R2ReadBoundaryError("oversize")
        data = self.get(key)
        if not isinstance(data, bytes):
            raise R2ReadBoundaryError("schema_unverified")
        return R2ObjectSnapshot(data, '"' + hashlib.sha256(data).hexdigest() + '"')

    def list_keys(self, prefix=""):
        self.counts["list"] += 1
        return {key for key in self.objects if key.startswith(prefix)}

    def list_object_revisions(self, prefix):
        self.counts["list"] += 1
        return {key: hashlib.sha256(value).hexdigest()
                for key, value in self.objects.items() if key.startswith(prefix)}

    def put(self, key, data, content_type, *, encoding=None):
        self.counts["put"] += 1
        self.objects[key] = data
        self.last_write_revision = hashlib.sha256(data).hexdigest()

    def put_if_absent(self, key, data, content_type, *, encoding=None):
        if key in self.objects:
            return False
        self.put(key, data, content_type, encoding=encoding)
        return True


def measure(store, activity):
    store.reset_counts()
    started = perf_counter()
    with redirect_stdout(io.StringIO()):
        result = run_one(activity=activity, store=store)
    return {**store.counts, "local_elapsed_ms": round((perf_counter() - started) * 1000, 3),
            "analysis_id": result["processed_sources"]["1"]}


def main():
    store = CountingStore()
    activity = {"id": "1", "date": "2026-09-30", "name": "Synthetic run", "moving_seconds": 1800}
    initial = measure(store, activity)
    repeated = measure(store, activity)
    repair = {key: value for key, value in activity.items() if key != "moving_seconds"}
    measure(store, repair)
    repeated_repair = measure(store, repair)
    assert initial["analysis_id"] == repeated["analysis_id"] == repeated_repair["analysis_id"]
    print(json.dumps({"fixture": "synthetic-1MiB-tcx", "provider_calls": 0,
                      "initial": initial, "unchanged": repeated,
                      "unchanged_repair": repeated_repair}, indent=2))


if __name__ == "__main__":
    main()
