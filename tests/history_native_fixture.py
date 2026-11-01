"""Synthetic native child using installed SDK HTTP boundary; never real credentials."""

import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import requests

from pipeline.history_collect import date_phase
from pipeline.history_source import HistorySource
from pipeline.history_state import Job
from pipeline.sources.provider_config import ProviderSelection


class Clock:
    value = 100.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds

    def now(self):
        from datetime import datetime, timezone

        return datetime(2026, 4, 30, tzinfo=timezone.utc)


mode, state, run, day = sys.argv[1:]


def send(adapter, request, **kwargs):
    path = urlsplit(request.url).path
    response = requests.Response()
    response.status_code = (
        429 if mode == "quota" and day.endswith("28") and "dailyHeartRate" in path else 200
    )
    value = (
        {"displayName": "synthetic", "profileId": 123}
        if path.endswith("socialProfile")
        else {"userData": {}}
        if path.endswith("user-settings")
        else {
            "userProfilePK": 123,
            "calendarDate": day,
            "heartRateValues": [[1777420800000, 55], [1777420860000, None]],
        }
    )
    if "hrv-service" in path:
        value = {
            "userProfilePK": 123,
            "calendarDate": day,
            "hrvSummary": {"calendarDate": day, "lastNightAvg": 45},
            "hrvReadings": [
                {
                    "hrvValue": 30,
                    "readingTimeGMT": day + "T01:00:00",
                    "readingTimeLocal": day + "T03:00:00",
                },
                {
                    "hrvValue": None,
                    "readingTimeGMT": day + "T01:05:00",
                    "readingTimeLocal": day + "T03:05:00",
                },
            ],
        }
    if mode == "empty" and "dailyHeartRate" in path:
        value["heartRateValues"] = []
    response._content = json.dumps(value).encode()
    response._content_consumed = True
    response.headers.update(
        {
            "Content-Type": "application/json",
            "Content-Length": str(len(response._content)),
            "Retry-After": "90000",
        }
    )
    return response


requests.adapters.HTTPAdapter.send = send
chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')


def source(plan, intent, folder):
    return HistorySource(plan, intent, folder, chosen=chosen, clock=Clock())


if mode == "unknown":
    raise SystemExit(77)
if mode == "io3":
    import os

    def fail(*args):
        raise OSError("synthetic fsync")

    os.fsync = fail
try:
    native = date_phase(Job(state), run, day, sys.stdin.buffer.read(1025), source_factory=source)
except BaseException as error:
    from pipeline.history_state import DurabilityError

    native = 3 if isinstance(error, DurabilityError) else 2
raise SystemExit(native)
