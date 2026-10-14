"""Offline synthetic resource evidence for actual summary export/normal recovery.

Run this same harness in separate processes against a frozen baseline and the
candidate with --source-repo. It never creates credentials or uses a provider.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import socket
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from unittest.mock import patch

from botocore.exceptions import ClientError


class Storage:
    """Count synthetic SDK requests, including failed requests and body bytes."""
    def __init__(self):
        self.objects = {}
        self.reset()
    def reset(self):
        self.counts = dict.fromkeys(("get", "head", "list", "put", "download_bytes", "upload_bytes", "full_inventory"), 0)
    @staticmethod
    def etag(raw):
        return '"' + hashlib.md5(raw, usedforsecurity=False).hexdigest() + '"'
    def get_object(self, *, Bucket, Key):
        self.counts["get"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        obj = self.objects[Key]
        self.counts["download_bytes"] += len(obj["Body"])
        return {"Body": io.BytesIO(obj["Body"]), **self.metadata(obj)}
    def metadata(self, obj):
        return {"ContentLength": len(obj["Body"]), "ETag": self.etag(obj["Body"]),
                "ContentType": obj.get("ContentType"), "ContentEncoding": obj.get("ContentEncoding")}
    def head_object(self, *, Bucket, Key):
        self.counts["head"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return self.metadata(self.objects[Key])
    def put_object(self, **kw):
        self.counts["put"] += 1
        self.counts["upload_bytes"] += len(kw["Body"])
        previous = self.objects.get(kw["Key"])
        if (kw.get("IfNoneMatch") == "*" and previous is not None or kw.get("IfMatch") is not None
                and (previous is None or kw["IfMatch"] != self.etag(previous["Body"]))):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[kw["Key"]] = kw.copy()
        return {"ETag": self.etag(kw["Body"])}
    def list_objects_v2(self, *, Bucket, Prefix, MaxKeys):
        self.counts["list"] += 1
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k, "Size": len(self.objects[k]["Body"])} for k in keys[:MaxKeys]],
                "IsTruncated": len(keys) > MaxKeys}
    def get_paginator(self, name):
        client = self
        class Paginator:
            def paginate(self, **kwargs):
                client.counts["list"] += 1
                prefix = kwargs.get("Prefix", "")
                client.counts["full_inventory"] += not prefix
                return [{"Contents": [{"Key": k, "Size": len(v["Body"]), "ETag": client.etag(v["Body"])}
                                      for k, v in client.objects.items() if k.startswith(prefix)]}]
        return Paginator()


def seed(directory, activities, health):
    directory.mkdir(parents=True)
    for name, header, count in [("activities.csv", ["Activity ID", "Activity Date", "Activity Name", "Moving Time"], activities),
                                ("health_daily.csv", ["Date", "Steps", "Source"], health)]:
        with (directory / name).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            for index in range(count):
                day = (datetime(2020, 1, 1) + timedelta(days=index % 1000)).date().isoformat()
                writer.writerow([f"garmin-{index+1}", day+" 10:00:00", f"Synthetic {index}", 1800]
                                if name == "activities.csv" else [day, 1000, "garmin"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    sys.path.insert(0, str(args.source_repo.resolve()))
    from pipeline import refresh, refresh_summaries, summary_export, summary_restore
    from pipeline.r2_store import R2Store
    from scripts.benchmark_summary_windows import SummaryGarmin
    # Refuse any unexpected networking even if a future source path changes.
    def blocked(*args, **kwargs):
        raise AssertionError("Offline benchmark forbids external networking")
    socket.create_connection = socket.socket.connect = socket.socket.connect_ex = blocked
    for key in list(os.environ):
        if any(word in key.upper() for word in ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "ACCESS_KEY")):
            os.environ.pop(key, None)
    os.environ["TRAINING_CONTEXT_ENABLED"] = "false"
    class Clock(datetime):
        day = 13
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, cls.day, 12, tzinfo=timezone.utc)
    class Garmin(SummaryGarmin):
        def get_hrv_data(self, day):
            return self.connectapi("hrv-day", {"day": day})
    results = []
    with TemporaryDirectory(prefix="synthetic-summary-recovery-") as temp, patch.object(summary_export, "datetime", Clock), patch.object(refresh_summaries, "datetime", Clock), patch("pipeline.sources.garmin_health.time.sleep", lambda seconds: None), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
        for activities, health in [(1, 1), (1000, 365), (10000, 3650)]:
            Clock.day = 13
            root = Path(temp) / str(activities)
            data = root / "seed"
            seed(data, activities, health)
            storage = Storage()
            def factory(storage=storage):
                return R2Store(client=storage, bucket="synthetic")
            def measure(name, operation, sdk=None, storage=storage):
                storage.reset()
                source_before = len(sdk.calls) if sdk else 0
                started = perf_counter()
                result = operation()
                return {"scenario": name, **storage.counts, "garmin_calls": len(sdk.calls)-source_before if sdk else 0,
                        "elapsed_ms": round((perf_counter()-started)*1000, 3),
                        "stored_objects": len(storage.objects), "stored_bytes": sum(len(v["Body"]) for v in storage.objects.values()),
                        "status": result.get("status", "verified")}
            cases = [measure("initial_explicit", lambda data=data, factory=factory: summary_export.run(str(data), store=factory())),
                     measure("unchanged_explicit", lambda data=data, factory=factory: summary_export.run(str(data), store=factory()))]
            raw = (data / "activities.csv").read_bytes()
            (data / "activities.csv").write_bytes(raw.replace(b"Synthetic 0", b"Changed 0", 1))
            cases.append(measure("changed_metadata", lambda data=data, factory=factory: summary_export.run(str(data), store=factory())))
            Clock.day = 14
            cases.append(measure("new_utc_day", lambda data=data, factory=factory: summary_export.run(str(data), store=factory())))
            cases.append(measure("cold_restore", lambda root=root, factory=factory: summary_restore.restore_summaries(str(root / "restored"), factory())))
            sdk = Garmin()
            for iteration in range(2):
                cases.append(measure("normal_cold_reconcile" if iteration == 0 else "normal_cold_recent_repeat",
                                     lambda root=root, iteration=iteration, factory=factory, sdk=sdk: refresh.run(refresh.RefreshRequest(), data_dir=str(root / f"normal{iteration}"),
                                                         store_factory=factory, login=lambda: sdk), sdk))
            results.append({"activity_rows": activities, "health_rows": health,
                            "local_input_bytes": sum((data / n).stat().st_size for n in ("activities.csv", "health_daily.csv")),
                            "cases": cases})
    print(json.dumps({"synthetic": True, "measurements": results,
                      "limits": "Unchanged stage/bucket guards; no live calls, provider retries, queue or billing measured"}, indent=2))


if __name__ == "__main__":
    main()
