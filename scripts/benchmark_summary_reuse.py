"""Synthetic summary exports, with operation counts and an always-write baseline."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.summary_export import build_summary_exports, run  # noqa: E402


class SummaryClient:
    def __init__(self):
        self.objects = {}
        self.counts = {"get": 0, "head": 0, "list": 0, "put": 0, "upload_bytes": 0}

    def head_object(self, *, Bucket, Key):
        self.counts["head"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        stored = self.objects[Key]
        return {"ETag": '"' + hashlib.md5(stored["Body"], usedforsecurity=False).hexdigest() + '"',
                "ContentLength": len(stored["Body"]), "ContentType": stored["ContentType"],
                "ContentEncoding": stored.get("ContentEncoding")}

    def get_object(self, *, Bucket, Key):
        self.counts["get"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key]["Body"])}

    def put_object(self, **kwargs):
        self.counts["put"] += 1
        self.counts["upload_bytes"] += len(kwargs["Body"])
        self.objects[kwargs["Key"]] = kwargs.copy()

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, **kwargs):
                client.counts["list"] += 1
                return [{"Contents": [{"Key": key, "Size": len(value["Body"])}
                                      for key, value in client.objects.items()]}]

        return Paginator()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true", help="Original five unconditional uploads")
    args = parser.parse_args()
    client = SummaryClient()
    with TemporaryDirectory(prefix="slipstream-summary-") as directory:
        root = Path(directory)
        (root / "activities.csv").write_text("Activity ID,Activity Name\ngarmin-1,Synthetic run\n", encoding="utf-8")
        (root / "health_daily.csv").write_text("Date,Source\n2026-09-30,garmin\n", encoding="utf-8")
        observations = []
        for _ in range(2):
            client.counts = dict.fromkeys(client.counts, 0)
            store = R2Store(client=client, bucket="synthetic-bucket")
            started = perf_counter()
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                if args.baseline:
                    objects, _ = build_summary_exports(directory)
                    for item in objects:
                        store.put(item["key"], item["data"], item["content_type"], encoding=item["encoding"])
                else:
                    run(directory, store=store)
            observations.append({**client.counts, "local_elapsed_ms": round((perf_counter() - started) * 1000, 3)})
    print(json.dumps({"mode": "baseline" if args.baseline else "incremental", "provider_calls": 0,
                      "initial": observations[0], "unchanged": observations[1]}, indent=2))


if __name__ == "__main__":
    main()
