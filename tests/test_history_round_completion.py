"""Full serial round through source HTTP, publication SDK and synthetic storage."""

import hashlib
import io
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from types import SimpleNamespace
from urllib.parse import urlsplit

import boto3
import requests
from history_storage_fixture import ACCOUNT, BUCKET, Server

from pipeline import (
    history_backfill,
    history_collect,
    history_publish,
    history_state,
)
from pipeline.history_canonical import decode
from pipeline.history_source import HistorySource
from pipeline.history_state import Job
from pipeline.night_baseline_refresh import QUEUE_KEY


def memory_filesystem(monkeypatch):
    """Replace only filesystem I/O; retain real Job journal and receipt semantics."""
    files, directories = {}, set()

    class MemoryFile(io.BytesIO):
        def __init__(self, path, mode):
            self.path, self.mode = path, mode
            if "x" in mode and path in files:
                raise FileExistsError(path)
            if "r" in mode and path not in files:
                raise FileNotFoundError(path)
            super().__init__(files.get(path, b""))
            if "a" in mode:
                self.seek(0, 2)

        def fileno(self):
            return 123

        def flush(self):
            if any(mode in self.mode for mode in "xaw"):
                files[self.path] = self.getvalue()

        def close(self):
            if not self.closed:
                self.flush()
            super().close()

    class MemoryPath:
        def __init__(self, name):
            self.path = PurePosixPath(str(name))

        def __str__(self):
            return str(self.path)

        def __truediv__(self, part):
            return type(self)(self.path / str(part))

        @property
        def parent(self):
            return type(self)(self.path.parent)

        @property
        def name(self):
            return self.path.name

        def absolute(self):
            return self

        def is_relative_to(self, other):
            return self.path.is_relative_to(str(other))

        def mkdir(self, **kwargs):
            directories.add(str(self))

        def exists(self):
            return str(self) in files or str(self) in directories

        def is_file(self):
            return str(self) in files

        def is_dir(self):
            return str(self) in directories

        def unlink(self):
            del files[str(self)]

        def open(self, mode):
            return MemoryFile(str(self), mode)

        def read_bytes(self):
            return files[str(self)]

        def glob(self, pattern):
            return iter(
                type(self)(name)
                for name in sorted(files)
                if PurePosixPath(name).is_relative_to(self.path)
                and PurePosixPath(name).relative_to(self.path).match(pattern)
            )

        def iterdir(self):
            names = {
                str(PurePosixPath(name).relative_to(self.path)).split("/")[0]
                for name in files
                if PurePosixPath(name).is_relative_to(self.path)
            }
            return iter(self / name for name in sorted(names))

    monkeypatch.setattr(history_state, "Path", MemoryPath)
    monkeypatch.setattr(history_publish, "Path", MemoryPath)
    monkeypatch.setattr(history_state.os, "fsync", lambda descriptor: None)
    return files


class Clock:
    def __init__(self):
        self.value = 100.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds

    def now(self):
        return datetime(2026, 2, 10, 12, tzinfo=timezone.utc)


def test_thirty_date_daily_finishes_closed_references_with_shared_measured_budgets(monkeypatch):
    from pipeline import night_baseline_refresh
    from pipeline.sources.provider_config import ProviderSelection

    files = memory_filesystem(monkeypatch)
    clock = Clock()
    monkeypatch.setattr(history_collect, "time", clock)
    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")

    class Calendar(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 2, 10, 12, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(night_baseline_refresh, "datetime", Calendar)
    chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')
    job = Job.create("round", "2025-12-28", "2026-01-26", ["hr", "hrv"], "legacy", "synthetic")
    source_calls, dated_reads = [], Counter()
    active = {}

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
            ready = dated_reads[day, stream] == 2
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
                        {
                            "hrvValue": 45,
                            "readingTimeGMT": day + "T01:00:00",
                            "readingTimeLocal": day + "T01:00:00",
                        },
                        {
                            "hrvValue": None,
                            "readingTimeGMT": day + "T01:05:00",
                            "readingTimeLocal": day + "T01:05:00",
                        },
                    ]
                    if ready
                    else [],
                )
        if request.method == "POST":
            status = 204
        response = requests.Response()
        response.status_code = status
        response._content = b"" if status == 204 else json.dumps(value).encode()
        response._content_consumed = True
        response.headers.update(
            {"Content-Type": "application/json", "Content-Length": str(len(response._content))}
        )
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", source_wire)

    def process(args, **kwargs):
        active["day"] = args[args.index("--date") + 1]
        run = args[args.index("--run") + 1]
        native = history_collect.date_phase(
            job,
            run,
            active["day"],
            kwargs["input"],
            source_factory=lambda plan, intent, folder: HistorySource(
                plan, intent, folder, chosen=chosen, clock=clock
            ),
        )
        return SimpleNamespace(returncode=native)

    monkeypatch.setattr(history_collect.subprocess, "run", process)
    server = Server()
    for offset in range(30):
        day = (datetime(2025, 12, 28) + timedelta(days=offset)).date()
        text = day.isoformat()
        key = f"health/sleep/v1/{day:%Y/%m}/{text}.json"
        body = json.dumps(
            {
                "schema_version": 1,
                "source": "garmin",
                "date": text,
                "sleep_start_gmt": (day - timedelta(days=1)).isoformat() + "T22:00:00Z",
                "sleep_end_gmt": text + "T06:00:00Z",
            }
        ).encode()
        server.objects[key] = (body, '"' + hashlib.sha256(body).hexdigest() + '"')
        server.metadata[key] = {"content-type": "application/json"}
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    args = SimpleNamespace(
        confirm_cloud_jobs_paused=True,
        confirm_r2_write=True,
        max_dates=30,
        max_http=240,
        plan_sha=job.plan_sha,
        resume=False,
        archive_reload=True,
        ledger=[],
    )
    result = history_backfill.daily(job, args)
    print(
        json.dumps(
            {
                "daily_status": result["daily_status"],
                "phases": result["daily_publication_phases"],
                "source_HTTP": result["daily_HTTP"],
                "baseline_refresh": result["baseline_refresh"],
                "wire_count": len(server.calls),
            },
            sort_keys=True,
        )
    )
    assert result["daily_status"] == "baselines_pending"
    assert result["counts"] == {"published": 60}
    assert result["daily_HTTP"] == 210
    assert len(source_calls) == 210
    assert sum(method == "POST" for _, method, _ in source_calls) == 30
    raw_writes = Counter(
        key for key in server.puts if key.startswith(("health/signals/", "health/hrv/"))
    )
    assert len(raw_writes) == 60 and set(raw_writes.values()) == {1}
    first_reference = next(index for index, key in enumerate(server.puts) if "/baseline/" in key)
    assert first_reference > max(
        index for index, key in enumerate(server.puts) if key in raw_writes
    )
    assert first_reference > max(
        index for index, key in enumerate(server.puts) if key.startswith("health/indexes/hrv/")
    )
    for key in raw_writes:
        value = decode(server.objects[key][0])
        if key.startswith("health/signals/"):
            assert value["channels"]["heart_rate"]["missing_value_count"] == 1
        else:
            assert value["archive_hrv_quality_v1"]["missing_readings"][0]["hrvValue"] is None
    queue = json.loads(server.objects[QUEUE_KEY][0])
    assert all(row.get("rolling_window") for row in queue["pending"].values())
    products = [
        json.loads(body) for key, (body, _) in server.objects.items() if "/baseline/" in key
    ]
    assert len(products) >= 16
    # Literal ISO Monday-Sunday ranges include the prior ISO year and the
    # incomplete edge weeks. Thirty collected dates do not fill either month.
    expected = {
        ("2025-12-01", "2025-12-31"): 4,
        ("2026-01-01", "2026-01-31"): 26,
        ("2025-12-22", "2025-12-28"): 1,
        ("2025-12-29", "2026-01-04"): 7,
        ("2026-01-05", "2026-01-11"): 7,
        ("2026-01-12", "2026-01-18"): 7,
        ("2026-01-19", "2026-01-25"): 7,
        ("2026-01-26", "2026-02-01"): 1,
    }
    calendar_products = [p for p in products
                         if (p["args"]["start_date"], p["args"]["end_date"]) in expected]
    assert Counter((p["args"]["start_date"], p["args"]["end_date"]) for p in calendar_products) == Counter(
        {key: 2 for key in expected}
    )
    for product in calendar_products:
        assert (
            product["contributing_nights"]
            == expected[product["args"]["start_date"], product["args"]["end_date"]]
        )
        assert {row["p50"] for row in product["bins"] if row["nights"]} == {
            55 if product["args"]["channel"] == "heart_rate" else 45
        }
        assert all(row["p50"] is None for row in product["bins"] if not row["nights"])
    reports = result["daily_baseline_reports"]
    assert result["daily_publication_phases"] == len(reports) <= 14
    for report in reports:
        assert report["publication_counts"]["get"] <= 400
        assert report["publication_counts"]["list"] <= 100
        assert report["publication_counts"]["put"] <= 100
        assert report["publication_body_bytes"] <= 2097152
    totals = reports[-1]["execution_totals"]
    assert totals["counts"]["get"] <= 2000
    assert totals["counts"]["list"] + totals["counts"]["put"] <= 1000
    assert totals["body_bytes"] <= 10485760
    wire = Counter(
        "list" if query.get("list-type") == ["2"] else "put" if method == "PUT" else "get"
        for method, _, query in server.calls
    )
    assert dict(wire) == totals["counts"]
    assert (
        sum(
            json.loads(body).get("HTTP", 0)
            for name, body in files.items()
            if name.endswith("/RECEIPT.json") and json.loads(body).get("targets")
        )
        == 210
    )
    # Ordinary R2-only completion check reopens the same genuine journal and
    # consumes no Garmin call or duplicate canonical PUT.
    before_source, before_wire, before_puts = (
        len(source_calls),
        len(server.calls),
        list(server.puts),
    )
    repeat = history_backfill.daily(Job(job.folder), args)
    assert repeat["daily_status"] == "baselines_pending"
    assert len(source_calls) == before_source
    assert len(server.puts) > len(before_puts) and len(server.calls) > before_wire
    assert Counter(key for key in server.puts if key in raw_writes) == raw_writes
    repeat_totals = repeat["baseline_refresh"]["execution_totals"]
    assert all(repeat_totals["counts"][kind] > totals["counts"][kind] for kind in ("get", "list", "put"))
    repeat_wire = Counter(
        "list" if query.get("list-type") == ["2"] else "put" if method == "PUT" else "get"
        for method, _, query in server.calls
    )
    assert repeat_totals["counts"] == dict(repeat_wire)
    assert repeat_totals["body_bytes"] > totals["body_bytes"]
    final_run = next(frame["event"]["run"] for frame in reversed(job.events()[0])
                     if frame["event"]["type"] == "end"
                     and frame["event"].get("status") == "published")
    receipt = json.loads(files[str(job.folder / "runs" / final_run / "REMOTE-VERIFIED.json")])
    assert receipt["baseline_refresh"]["execution_totals"] == repeat_totals
    print(json.dumps({"repeat_prior_totals": totals, "repeat_totals": repeat_totals,
                      "repeat_physical_delta": {kind: repeat_totals["counts"][kind] - totals["counts"][kind]
                                                for kind in ("get", "list", "put")}}))


def test_daily_finishes_prior_cohort_and_collects_new_dates_without_rewrites(monkeypatch):
    from daily_autonomy_fixture import daily_system, transport_counts

    system = daily_system(monkeypatch)
    job, server = system.job, system.server
    old_day = "2025-12-27"
    history_collect.collect(job, job.plan_sha, confirm=True, max_dates=1,
                            target_dates=[old_day])
    initial = history_publish.publish(
        job, job.plan_sha, confirm=True,
        target_dates=[old_day], finalize_dates=[old_day], defer_references=True,
    )
    assert initial["counts"] == {"published": 2, "pending": 60}
    assert initial["baseline_refresh"]["closed_pending"] > 0
    first_raw = {key: value for key, value in server.objects.items()
                 if key.startswith(("health/signals/", "health/hrv/"))}
    source_count = len(system.source_calls)
    args = SimpleNamespace(
        confirm_cloud_jobs_paused=True, confirm_r2_write=True,
        max_dates=30, max_http=240, plan_sha=job.plan_sha,
        resume=False, archive_reload=True, ledger=[],
    )
    result = history_backfill.daily(job, args)
    # A pending prior publication cohort joins the new round. Its stored date
    # cannot finish the daily target or trigger a duplicate source/raw write.
    assert result["daily_status"] == "baselines_pending"
    assert result["daily_HTTP"] == 210
    assert result["counts"] == {"published": 62}
    assert old_day not in {day for day, _, _ in system.source_calls[source_count:]}
    assert len({day for day, _, _ in system.source_calls[source_count:]}) == 30
    assert old_day in result["baseline_refresh"]["finalize_dates"]
    assert len(result["baseline_refresh"]["finalize_dates"]) == 31
    assert result["baseline_refresh"]["closed_pending"] > 0
    assert result["daily_publication_phases"] == 13
    assert result["daily_reference_epoch_phases"] == 14
    assert result["daily_publication_phases_total"] == 14
    wire = transport_counts(server.calls)
    totals = result["baseline_refresh"]["execution_totals"]
    assert totals["counts"] == {"get": wire["GET"] + wire["HEAD"],
                                "list": wire["LIST"], "put": wire["PUT"]}
    assert totals["body_bytes"] == server.attempted_write_bytes
    assert totals["counts"]["get"] > initial["baseline_refresh"]["execution_totals"]["counts"]["get"]
    assert {key: server.objects[key] for key in first_raw} == first_raw
    assert Counter(key for key in server.puts if key in first_raw) == Counter(
        {key: 1 for key in first_raw})

def test_daily_with_unknown_legacy_cohort_keeps_closed_global_references_pending(monkeypatch):
    from test_history_baseline_integration import memory_job, sdk_store

    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")
    job, _ = memory_job(monkeypatch, [{"date": "2025-12-15", "stream": "hr"}])
    server = Server()
    first = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True
    )
    assert first["counts"] == {"published": 1}
    assert first["baseline_refresh"].get("finalize_dates") is None
    assert first["baseline_refresh"]["closed_pending"] == (
        3 + first["baseline_refresh"]["discovery_eligible_remaining"]["count"])
    before_puts = list(server.puts)
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    args = SimpleNamespace(
        confirm_cloud_jobs_paused=True,
        confirm_r2_write=True,
        max_dates=30,
        max_http=240,
        plan_sha=job.plan_sha,
        resume=False,
        archive_reload=False,
        ledger=[],
    )
    result = history_backfill.daily(job, args)
    assert result["daily_status"] == "baselines_pending"
    assert result["daily_HTTP"] == 0
    assert result["baseline_refresh"]["closed_pending"] == (
        3 + result["baseline_refresh"]["discovery_eligible_remaining"]["count"])
    assert result["counts"] == {"published": 1}
    assert server.puts == before_puts
