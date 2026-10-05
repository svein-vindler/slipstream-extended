"""Export real pipeline bytes for local Worker contract tests; never opens a network client."""
from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.health_history_index import index_key, sync_dates  # noqa: E402
from pipeline.health_writer import write_health_dataset  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.recent_health import run  # noqa: E402
from pipeline.summary_export import run as export_summaries  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "tests/fixtures/health-contract.json"


class ContractClient(HealthClient):
    fail_key = None

    def put_object(self, **kwargs):
        if kwargs["Key"] == self.fail_key:
            raise RuntimeError("Synthetic interrupted index write")
        return super().put_object(**kwargs)


class FixtureGarmin:
    def __init__(self, cases):
        self.responses = {(stream, case["date"]): copy.deepcopy(case[stream])
                          for case in cases for stream in ("sleep", "hrv")}
        self.calls = []

    def _fetch(self, stream, day):
        self.calls.append([stream, day])
        return copy.deepcopy(self.responses[stream, day])

    def get_sleep_data(self, day):
        return self._fetch("sleep", day)

    def get_hrv_data(self, day):
        return self._fetch("hrv", day)


class FixtureClock(datetime):
    current = datetime(2026, 11, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.current if tz else cls.current.replace(tzinfo=None)


class SummaryClock(FixtureClock):
    current = datetime(2026, 11, 1, tzinfo=timezone.utc)


def snapshot(client):
    return {key: {"body": base64.b64encode(value["Body"]).decode("ascii"),
                  "etag": hashlib.md5(value["Body"], usedforsecurity=False).hexdigest(),
                  "contentType": value["ContentType"],
                  "contentEncoding": value.get("ContentEncoding")}
            for key, value in sorted(client.objects.items())}


def build_contract():
    FixtureClock.current = datetime(2026, 11, 1, tzinfo=timezone.utc)
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    cases = fixture["cases"]
    client, garmin = ContractClient(), FixtureGarmin(cases)
    states, operations = {}, {}

    def store():
        return R2Store(client=client, bucket="synthetic-health-contract")

    def refresh(day, streams=("sleep", "hrv")):
        return run(days=1, streams=streams, store=store(), garmin=garmin,
                   today=date.fromisoformat(day))

    def capture(name):
        states[name] = snapshot(client)
        operations[name] = {"reads": client.reads[:], "writes": client.writes[:],
                            "source_calls": garmin.calls[:]}
        client.reset()
        garmin.calls.clear()

    def summaries():
        rows = [{"Date": case["date"],
                 "Sleep Seconds": garmin.responses["sleep", case["date"]]["dailySleepDTO"]["sleepTimeSeconds"],
                 "HRV Last Night Average": garmin.responses["hrv", case["date"]]["hrvSummary"]["lastNightAvg"]}
                for case in cases]
        with TemporaryDirectory(prefix="synthetic-health-contract-") as directory:
            write_health_dataset(rows, directory)
            (Path(directory) / "activities.csv").write_text("Activity ID,Activity Name\n", encoding="utf-8")
            export_summaries(directory, store=store())

    with (patch("pipeline.health_sync.datetime", FixtureClock),
          patch("pipeline.health_history_index.datetime", FixtureClock),
          patch("pipeline.summary_export.datetime", SummaryClock),
          redirect_stdout(StringIO()), redirect_stderr(StringIO())):
        for case in cases:
            refresh(case["date"])
        summaries()
        capture("initial")
        FixtureClock.current = datetime(2026, 11, 2, tzinfo=timezone.utc)
        for case in cases:
            refresh(case["date"])
        summaries()
        capture("repeated")
        ordinary = cases[0]
        day = ordinary["date"]
        for stream in ("sleep", "hrv"):
            response = garmin.responses[stream, day]
            if stream == "sleep":
                response["dailySleepDTO"]["sleepTimeSeconds"] = ordinary["changed"]["sleep_seconds"]
            else:
                response["hrvReadings"][0]["hrvValue"] = ordinary["changed"]["first_hrv_ms"]
                response["hrvSummary"]["lastNightAvg"] = ordinary["changed"]["hrv_mean_ms"]
            client.fail_key = index_key(stream, day[:7])
            try:
                refresh(day, (stream,))
            except RuntimeError as exc:
                if str(exc) != "Synthetic interrupted index write":
                    raise
            else:
                raise AssertionError("Expected the injected index failure")
            capture(f"interrupted_{stream}")
            client.fail_key = None
            # Derived-index recovery needs no Garmin re-download or receipt advance.
            sync_dates(store(), stream, [day])
            capture(f"repaired_{stream}")
        FixtureClock.current = datetime(2026, 11, 3, tzinfo=timezone.utc)
        refresh(day)
        summaries()
        capture("completed")
        # Wrong date and empty/incomplete responses must preserve successful bytes/checks.
        for stream, dto in (("sleep", "dailySleepDTO"), ("hrv", "hrvSummary")):
            garmin.responses[stream, day][dto]["calendarDate"] = "2026-09-29"
        refresh(day)
        capture("wrong_date")
        garmin.responses["sleep", day] = {"dailySleepDTO": {"calendarDate": day}}
        garmin.responses["hrv", day] = {"hrvSummary": {"calendarDate": day}, "hrvReadings": []}
        refresh(day)
        capture("incomplete")
        for stream in ("sleep", "hrv"):
            del client.objects[index_key(stream, day[:7])]
        capture("missing_indexes")
        for stream in ("sleep", "hrv"):
            sync_dates(store(), stream, [day])
        capture("recovered_missing")
        for stream in ("sleep", "hrv"):
            client.objects[index_key(stream, day[:7])]["Body"] = b"invalid synthetic index"
        capture("corrupt_indexes")
        for stream in ("sleep", "hrv"):
            sync_dates(store(), stream, [day])
        capture("recovered_corrupt")
    return {"fixture": fixture, "states": states, "operations": operations}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = build_contract()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print("Built synthetic Python-to-Worker health contract")


if __name__ == "__main__":
    main()
