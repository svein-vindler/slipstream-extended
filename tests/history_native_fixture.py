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
stream_reads = {}


def send(adapter, request, **kwargs):
    path = urlsplit(request.url).path
    stream_reads[path] = stream_reads.get(path, 0) + 1
    response = requests.Response()
    response.status_code = (
        429 if mode == "quota" and day.endswith("28") and "dailyHeartRate" in path else 200
    )
    if mode == "sibling_denial_close" and "hrv-service" in path:
        response.status_code = 403
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
    if mode == "reload_sibling":
        if "dailyHeartRate" in path and stream_reads[path] == 1:
            value["heartRateValues"] = []
        if "hrv-service" in path:
            if stream_reads[path] == 1:
                value["hrvReadings"] = []
            else:
                response.status_code = 403
        if request.method == "POST":
            response.status_code = 204
    response._content = b"" if response.status_code == 204 else json.dumps(value).encode()
    if mode == "unknown_after_hr" and "hrv-service" in path:
        response._content = b"{"
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
if mode in {"sibling_denial_close", "source_cleanup_ambient"}:

    def close_failure(self):
        raise OSError("synthetic session close")

    requests.Session.close = close_failure
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
    handoff = sys.stdin.buffer.read(1025)
    if mode == "source_cleanup_ambient":
        try:
            raise ValueError("unrelated caller exception")
        except ValueError:
            native = date_phase(Job(state), run, day, handoff, source_factory=source)
    else:
        native = date_phase(Job(state), run, day, handoff, source_factory=source)
except BaseException as error:
    from pipeline.history_state import DurabilityError

    native = 3 if isinstance(error, DurabilityError) else 2
raise SystemExit(native)
