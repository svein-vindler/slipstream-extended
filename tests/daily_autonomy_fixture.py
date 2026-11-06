"""Synthetic physical transports for the real daily source/publication system loop."""

import hashlib
import json
import socket
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit

import boto3
import requests
from history_storage_fixture import ACCOUNT, BUCKET, Server
from test_history_round_completion import Clock, memory_filesystem

from pipeline import history_collect, night_baseline_refresh
from pipeline.history_source import HistorySource
from pipeline.history_state import Job
from pipeline.sources.provider_config import ProviderSelection


class MeasuredServer(Server):
    def __init__(self):
        super().__init__()
        self.attempted_write_bytes = 0
        self.response_bytes = 0

    def send(self, request):
        response = super().send(request)
        if request.method == "PUT":
            assert response.status_code in {200, 201, 204}
            self.attempted_write_bytes += len(self.objects[self.puts[-1]][0])
        self.response_bytes += int(response.headers["content-length"])
        return response


def daily_system(monkeypatch):
    """Keep SDK/R2Store/Meter/Job real; replace I/O, clock and child launch only."""
    files = memory_filesystem(monkeypatch)
    clock = Clock()
    monkeypatch.setattr(history_collect, "time", clock)
    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")

    def no_network(*args, **kwargs):
        raise AssertionError("Only synthetic physical transports are admitted")

    monkeypatch.setattr(socket.socket, "connect", no_network)

    class Calendar(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 2, 10, 12, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(night_baseline_refresh, "datetime", Calendar)
    chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')
    job = Job.create("daily-capacity", "2025-12-27", "2026-01-26", ["hr", "hrv"],
                     "legacy", "synthetic")
    active, source_calls, dated_reads, native_results = {}, [], Counter(), []

    def source_wire(adapter, request, **kwargs):
        day = active["day"]
        path = urlsplit(request.url).path
        source_calls.append((day, request.method, path))
        value = {"displayName": "synthetic", "profileId": 123}
        status = 200
        if path.endswith("user-settings"):
            value = {"userData": {}}
        if "dailyHeartRate" in path or "hrv-service" in path:
            stream = "hr" if "dailyHeartRate" in path else "hrv"
            dated_reads[day, stream] += 1
            ready = not active["reload"] or dated_reads[day, stream] == 2
            stamp = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
            value = {"userProfilePK": 123, "calendarDate": day}
            if stream == "hr":
                value["heartRateValues"] = (
                    [[stamp + 3600000, 55], [stamp + 3660000, None]] if ready else []
                )
            else:
                value.update(
                    hrvSummary={"calendarDate": day, "lastNightAvg": 45},
                    hrvReadings=[
                        {"hrvValue": 45, "readingTimeGMT": day + "T01:00:00",
                         "readingTimeLocal": day + "T01:00:00"},
                        {"hrvValue": None, "readingTimeGMT": day + "T01:05:00",
                         "readingTimeLocal": day + "T01:05:00"},
                    ] if ready else [],
                )
        if request.method == "POST":
            status = 204
        response = requests.Response()
        response.status_code = status
        response._content = b"" if status == 204 else json.dumps(value).encode()
        response._content_consumed = True
        response.headers.update({"Content-Type": "application/json",
                                 "Content-Length": str(len(response._content))})
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", source_wire)

    def process(args, **kwargs):
        day = args[args.index("--date") + 1]
        active["day"] = day
        run = args[args.index("--run") + 1]
        intent = json.loads(files[str(job.folder / "runs" / run / day / "INTENT.json")])
        active["reload"] = intent["archive_reload"]
        native = history_collect.date_phase(
            job, run, day, kwargs["input"],
            source_factory=lambda plan, intent, folder: HistorySource(
                plan, intent, folder, chosen=chosen, clock=clock
            ),
        )
        native_results.append(native)
        return SimpleNamespace(returncode=native)

    monkeypatch.setattr(history_collect.subprocess, "run", process)
    server = MeasuredServer()
    for offset in range(31):
        day = (datetime(2025, 12, 27) + timedelta(days=offset)).date()
        text = day.isoformat()
        key = f"health/sleep/v1/{day:%Y/%m}/{text}.json"
        body = json.dumps({"schema_version": 1, "source": "garmin", "date": text,
                           "sleep_start_gmt": (day - timedelta(days=1)).isoformat() + "T22:00:00Z",
                           "sleep_end_gmt": text + "T06:00:00Z"}).encode()
        server.objects[key] = (body, '"' + hashlib.sha256(body).hexdigest() + '"')
        server.metadata[key] = {"content-type": "application/json"}
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
                        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic"}.items():
        monkeypatch.setenv(name, value)
    return SimpleNamespace(job=job, files=files, clock=clock, server=server,
                           source_calls=source_calls, native_results=native_results)


def transport_counts(calls):
    counts = Counter({"GET": 0, "HEAD": 0, "LIST": 0, "PUT": 0})
    counts.update("LIST" if query.get("list-type") == ["2"] else method
                  for method, _, query in calls)
    return counts
