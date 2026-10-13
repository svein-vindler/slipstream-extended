"""Synthetic Python-to-MCP Coach Input selection contract. No external services."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import sys
import threading
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import coach_backfill  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402

PREFIX = "activities/2026/1"
ACTIVITY = {"id": "1", "date": "2026-09-30", "name": "Synthetic run",
            "type": "Run", "moving_seconds": 1800.0}


class MemoryS3:
    """Synthetic S3 boundary, including atomic create-only writes and counters."""

    def __init__(self):
        self.objects = {
            f"{PREFIX}/activity.v1.json": b'{"source_fit_sha256":"synthetic-fit","messages":{}}',
            f"{PREFIX}/activity.endurance.v1.json": b'{"available":true,"summary":{"distance_m":5000}}',
            f"{PREFIX}/activity.tcx": b"synthetic tcx",
            "coach/profiles/v1/2026-01-01/test.json": json.dumps({
                "profile_id": "synthetic-profile", "effective_from": "2026-01-01",
                "zones": [{"label": "test", "min_bpm": 100, "max_bpm": 150}],
            }).encode(),
        }
        self.lock = threading.RLock()
        self.puts = []
        self.set_activity(ACTIVITY)

    def set_activity(self, activity):
        self.objects["summary/activities.csv"] = (
            "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time\n"
            f"garmin-{activity['id']},{activity['date']},{activity['name']},"
            f"{activity['type']},{activity['moving_seconds']}\n"
        ).encode()

    def get_object(self, *, Bucket, Key):
        with self.lock:
            if Key not in self.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            data = self.objects[Key]
            return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, *, Bucket, Prefix=""):
                with client.lock:
                    contents = [{"Key": key, "Size": len(data),
                                 "ETag": hashlib.md5(data, usedforsecurity=False).hexdigest()}
                                for key, data in client.objects.items() if key.startswith(Prefix)]
                return [{"Contents": contents}]

        return Paginator()

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        with self.lock:
            if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
                raise ClientError({"Error": {"Code": "PreconditionFailed"},
                                   "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
            self.objects[Key] = Body
            self.puts.append(Key)
            return {"ETag": hashlib.md5(Body, usedforsecurity=False).hexdigest()}

    def store(self):
        return R2Store(client=self, bucket="synthetic-bucket")

    def read(self, key):
        data = self.store().get(key)
        return json.loads(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)


def build_contract():
    fixtures = {}
    for mode in ("bulk", "targeted", "mixed"):
        client = MemoryS3()
        client.objects[f"{PREFIX}/activity.v1.json"] = json.dumps({
            "schema_version": 1, "source_fit_sha256": "synthetic-fit", "messages": {},
            "activity": {"id": "1", "type": "running", "start_time_local": "2026-09-30T10:00:00"},
        }).encode()
        client.objects[f"{PREFIX}/activity.fit"] = b"synthetic FIT marker"
        client.objects[f"{PREFIX}/activity.endurance.v1.json"] = json.dumps({
            "available": True, "activity": {"id": "1"}, "summary": {"distance_m": 5000},
        }).encode()
        steps = []
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            for i, moving in enumerate((1800.0, 1900.0, 1800.0)):
                activity = {**ACTIVITY, "moving_seconds": moving}
                client.set_activity(activity)
                if mode == "bulk" or mode == "mixed" and i != 1:
                    result = coach_backfill.run(store=client.store())["completed_this_run"][0]
                    identifier = result["analysis_id"]
                else:
                    result = coach_backfill.run_one(activity=activity, store=client.store())
                    identifier = result["processed_sources"]["1"]
                steps.append({"expected_id": identifier, "moving_seconds": moving,
                              "objects": {key: base64.b64encode(data).decode()
                                          for key, data in client.objects.items()}})
        fixtures[mode] = steps
    recoveries = {}
    for mode, failure in (("bulk", "before_pointer"), ("targeted", "before_pointer"),
                          ("bulk", "pointer_reply_lost"), ("targeted", "pointer_reply_lost"),
                          ("bulk", "before_plan")):
        client = MemoryS3()
        activity = ACTIVITY.copy()
        steps = []

        def generate(mode=mode, client=client, activity=activity):
            if mode == "bulk":
                return coach_backfill.run(store=client.store())["completed_this_run"][0]["analysis_id"]
            return coach_backfill.run_one(activity=activity, store=client.store())["processed_sources"]["1"]

        def capture(identifier, moving, client=client):
            return {"expected_id": identifier, "moving_seconds": moving,
                    "objects": {key: base64.b64encode(body).decode() for key, body in client.objects.items()}}

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            a_id = generate()
            steps.append(capture(a_id, 1800.0))
            activity["moving_seconds"] = 1900.0
            client.set_activity(activity)
            original = client.put_object
            pointer_key = f"{PREFIX}/coach-input/v1/latest-ready.json"

            def interrupted(*, failure=failure, pointer_key=pointer_key, original=original, **kwargs):
                if failure == "before_pointer" and kwargs["Key"] == pointer_key:
                    raise TimeoutError("synthetic interruption before selection")
                if failure == "before_plan" and kwargs["Key"] == coach_backfill.PLAN_KEY:
                    raise TimeoutError("synthetic interruption before plan")
                result = original(**kwargs)
                if failure == "pointer_reply_lost" and kwargs["Key"] == pointer_key:
                    raise TimeoutError("synthetic selection reply lost")
                return result

            client.put_object = interrupted
            try:
                generate()
            except TimeoutError:
                pass
            else:
                raise AssertionError("Synthetic interruption did not occur")
            b_key = next(key for key in client.objects if "/canonical/" in key and a_id not in key)
            b_id = b_key.rsplit("/", 1)[1].removesuffix(".json")
            steps.append(capture(a_id if failure == "before_pointer" else b_id,
                                 1800.0 if failure == "before_pointer" else 1900.0))
            client.put_object = original
            steps.append(capture(generate(), 1900.0))
        recoveries[f"{mode}-{failure}"] = steps
    fixtures["recoveries"] = recoveries
    return fixtures


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build_contract(), separators=(",", ":")), encoding="utf-8")
