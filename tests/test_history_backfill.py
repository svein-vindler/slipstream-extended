import csv
import io
import json
import os
import subprocess
import sys


def cli(root, *args):
    env = dict(
        os.environ, GARMINTOKENS_FILE="must-not-read", GARMIN_PERSONAL_API_KEY_FILE="must-not-read"
    )
    return subprocess.run(
        [sys.executable, "-B", "-m", "pipeline.history_backfill", "--state", str(root), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_offline_plan_status_and_missing_native_without_credentials(tmp_path):
    state = tmp_path / "job"
    r = cli(
        state,
        "plan",
        "--start",
        "2026-04-28",
        "--end",
        "2026-04-29",
        "--streams",
        "hr,hrv",
        "--provider",
        "legacy",
        "--account",
        "synthetic",
    )
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["targets"] == 4
    status = cli(state, "status")
    assert status.returncode == 0
    assert json.loads(status.stdout)["pending"] == 4
    missing = cli(state, "missing")
    assert missing.returncode == 0
    rows = list(csv.DictReader(io.StringIO(missing.stdout)))
    assert [(r["date"], r["stream"], r["status"]) for r in rows] == [
        ("2026-04-29", "hr", "pending"),
        ("2026-04-29", "hrv", "pending"),
        ("2026-04-28", "hr", "pending"),
        ("2026-04-28", "hrv", "pending"),
    ]
    assert (
        cli(
            state,
            "plan",
            "--start",
            "2026-04-28",
            "--end",
            "2026-04-29",
            "--streams",
            "hr",
            "--provider",
            "legacy",
            "--account",
            "synthetic",
        ).returncode
        == 2
    )


def test_pause_survives_native_restart_and_early_resume_reads_no_credentials(tmp_path):
    state = tmp_path / "job"
    assert (
        cli(
            state,
            "plan",
            "--start",
            "2026-04-29",
            "--end",
            "2026-04-29",
            "--streams",
            "hr",
            "--provider",
            "legacy",
            "--account",
            "synthetic",
        ).returncode
        == 0
    )
    assert (
        cli(
            state, "pause", "--until", "2099-01-01T00:00:00Z", "--reason", "operator_quota"
        ).returncode
        == 0
    )
    sha = json.loads(cli(state, "status").stdout)["plan_sha256"]
    r = cli(state, "resume", "--plan-sha", sha, "--confirm-cloud-jobs-paused")
    assert r.returncode == 2
    status = json.loads(cli(state, "status").stdout)
    assert status["pause"] == "operator_quota"
    assert status["not_before"] == "2099-01-01T00:00:00+00:00"
    assert not (state / "OWNER.lock").exists()
    assert not (state / "runs").exists()


class FakeClock:
    def __init__(self):
        self.value = 100.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds

    def now(self):
        from datetime import datetime, timezone

        return datetime(2026, 4, 30, tzinfo=timezone.utc)


def test_legacy_archive_is_dated_raw_sdk_capture_and_one_reload(monkeypatch, tmp_path):
    from urllib.parse import urlsplit

    import requests

    from pipeline.history_source import HistorySource
    from pipeline.history_state import digest
    from pipeline.sources.provider_config import ProviderSelection

    day = "2026-04-29"
    calls = []
    stamp = 1777420800000
    raw = {
        "userProfilePK": 123,
        "calendarDate": day,
        "heartRateValues": [[stamp, 55], [stamp + 60000, None]],
    }

    def send(adapter, request, **kwargs):
        path = urlsplit(request.url).path
        calls.append((request.method, path))
        response = requests.Response()
        response.status_code = 204 if request.method == "POST" else 200
        value = (
            {"displayName": "synthetic", "profileId": 123}
            if path.endswith("socialProfile")
            else {"userData": {}}
            if path.endswith("user-settings")
            else {**raw, "heartRateValues": []}
            if len(calls) < 5
            else raw
        )
        response._content = b"" if response.status_code == 204 else json.dumps(value).encode()
        response.headers.update(
            {"Content-Type": "application/json", "Content-Length": str(len(response._content))}
        )
        response.request = request
        response._content_consumed = True
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')
    intent = {"date": day, "max_http": 8, "archive_reload": True}
    source = HistorySource(
        {"provider": "legacy", "account_sha256": digest(b"synthetic")},
        intent,
        tmp_path,
        chosen=chosen,
        clock=FakeClock(),
    )
    assert source.read("hr")["status"] == "empty_unresolved"
    source.reload()
    before = source.clock.monotonic()
    source.wait_archive()
    assert source.clock.monotonic() - before == 60
    result = source.read("hr")
    assert result["status"] == "ready"
    assert json.loads((tmp_path / result["wire"]).read_bytes()) == raw
    assert source.http_count == 5
    assert [c for c in calls if c[0] == "POST"] == [
        ("POST", "/wellness-service/wellness/epoch/request/2026-04-29")
    ]
    import pytest

    from pipeline.history_state import Stop

    with pytest.raises(Stop):
        source.client.request_reload("2026-04-28")
    assert len(calls) == 5
    source.close()


def test_nullable_hrv_preparation_preserves_context_summary_and_timed_nulls():
    from pipeline.history_canonical import prepare

    day = "2026-04-29"
    raw = {
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
    previous = {
        "schema_version": 1,
        "source": "garmin",
        "date": day,
        "readings": [],
        "summary": {"lastNightAvg": 40, "nullable": None},
        "context": {"travel": ["synthetic"]},
    }
    value = prepare("hrv", day, raw, previous, wire=json.dumps(raw).encode())
    assert value["summary"] == previous["summary"]
    assert value["context"] == previous["context"]
    assert [r["hrv_ms"] for r in value["readings"]] == [30]
    assert value["archive_hrv_quality_v1"]["missing_value_count"] == 1
    assert value["archive_hrv_quality_v1"]["missing_readings"][0]["hrvValue"] is None
    assert (
        value["archive_hrv_quality_v1"]["missing_readings"][0]["readingTimeGMT"]
        == day + "T01:05:00"
    )


def native_runner(mode):
    from pathlib import Path

    def run(args, handoff, folder):
        return subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(__file__).with_name("history_native_fixture.py")),
                mode,
                args[args.index("--state") + 1],
                args[args.index("--run") + 1],
                args[args.index("--date") + 1],
            ],
            input=handoff,
            capture_output=True,
            check=False,
        )

    return run


def test_native_collection_keeps_positive_date_and_persistent_real_429_hold(tmp_path):
    import pytest

    from pipeline.history_collect import collect
    from pipeline.history_state import Job, Stop

    job = Job.create(tmp_path / "job", "2026-04-28", "2026-04-29", ["hr"], "legacy", "synthetic")
    result = collect(job, job.plan_sha, confirm=True, date_runner=native_runner("quota"))
    assert result["counts"] == {"ready": 1, "rate_limited": 1}
    assert result["pause"] == "rate_limited"
    assert result["not_before"] == "2026-05-01T01:00:00+00:00"
    assert not result["locked"]
    with pytest.raises(Stop):
        collect(
            Job(job.folder),
            job.plan_sha,
            resume=True,
            confirm=True,
            now=FakeClock().now(),
            date_runner=lambda *args: pytest.fail("early resume attempted source"),
        )


def test_unknown_and_io_native_leave_owner_hold_without_projection(tmp_path):
    import pytest

    from pipeline.history_collect import collect
    from pipeline.history_state import DurabilityError, Job, Stop

    for mode, error in [("unknown", Stop), ("io3", DurabilityError)]:
        job = Job.create(tmp_path / mode, "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
        with pytest.raises(error):
            collect(job, job.plan_sha, confirm=True, date_runner=native_runner(mode))
        assert job.status()["locked"]
        assert job.status()["counts"] == {"pending": 1}
        with pytest.raises(Stop):
            collect(
                job,
                job.plan_sha,
                resume=True,
                confirm=True,
                date_runner=lambda *a: pytest.fail("takeover"),
            )


def test_publish_verified_native_raw_conditional_ack_readback_and_skip_good(tmp_path, monkeypatch):
    import boto3
    from botocore.config import Config
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.history_collect import collect
    from pipeline.history_publish import publish
    from pipeline.history_state import Job
    from pipeline.r2_store import R2Store

    job = Job.create(tmp_path / "job", "2026-04-28", "2026-04-29", ["hr"], "legacy", "synthetic")
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("quota"))
    import pytest

    monkeypatch.delenv("HEALTH_TIMEZONE", raising=False)
    monkeypatch.delenv("CLOUDFLARE_ACCOUNT_ID", raising=False)
    with pytest.raises(KeyError):
        publish(job, job.plan_sha, confirm=True)
    assert not job.status()["locked"]  # Known zero-HTTP preflight failure can be corrected.
    server = Server()
    client = boto3.client(
        "s3",
        endpoint_url="https://" + ACCOUNT + ".r2.cloudflarestorage.com",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic",
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
    client._endpoint.http_session.send = server.send
    store = R2Store(client=client, bucket=BUCKET)
    result = publish(job, job.plan_sha, confirm=True, store=store)
    assert result["counts"] == {"published": 1, "rate_limited": 1}
    assert result["pause"] == "rate_limited"
    assert len(server.puts) == 3  # dirty intent, canonical source, ready checkpoint
    before = dict(server.objects)
    assert publish(job, job.plan_sha, confirm=True, store=store)["counts"] == {
        "published": 1,
        "rate_limited": 1,
    }
    assert server.objects == before
    assert len(server.puts) == 3


def test_personal_archive_unsupported_offline_and_two_empty_batches_pause(tmp_path):
    import pytest

    from pipeline.history_collect import collect
    from pipeline.history_state import Job, Stop

    personal = Job.create(
        tmp_path / "personal", "2026-04-29", "2026-04-29", ["hr"], "personal", "synthetic"
    )
    result = collect(
        personal,
        personal.plan_sha,
        confirm=True,
        reload=True,
        date_runner=lambda *a: pytest.fail("unsupported provider attempted source"),
    )
    assert result["counts"] == {"unsupported": 1}
    job = Job.create(tmp_path / "legacy", "2026-04-27", "2026-04-29", ["hr"], "legacy", "synthetic")
    collect(job, job.plan_sha, confirm=True, max_dates=1, date_runner=native_runner("empty"))
    result = collect(
        job, job.plan_sha, confirm=True, max_dates=1, date_runner=native_runner("empty")
    )
    assert result["pause"] == "no_more_observed"
    assert result["not_before"] is None
    assert result["counts"] == {"empty_unresolved": 2, "pending": 1}
    with pytest.raises(Stop):
        collect(
            job,
            job.plan_sha,
            confirm=True,
            date_runner=lambda *a: pytest.fail("paused job sent source"),
        )


def test_hrv_conditional_merge_preserves_existing_summary_and_month_days(tmp_path):
    import boto3
    from botocore.config import Config
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.granular import gzip_json
    from pipeline.history_canonical import decode
    from pipeline.history_collect import collect
    from pipeline.history_publish import publish
    from pipeline.history_state import Job
    from pipeline.r2_store import R2Store

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hrv"], "legacy", "synthetic")
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("normal"))
    server = Server()
    existing = {
        "schema_version": 1,
        "source": "garmin",
        "date": "2026-04-29",
        "readings": [],
        "summary": {"lastNightAvg": 40, "nullable": None},
        "context": {"travel": ["synthetic"]},
    }
    server.objects["health/hrv/2026/04/2026-04-29.json"] = (gzip_json(existing), '"old"')
    unrelated = {
        "schema_version": 1,
        "source": "garmin",
        "date": "2026-04-28",
        "readings": [{"timestamp": "2026-04-28T01:00:00", "hrv_ms": 33}],
        "summary": {"lastNightAvg": 42},
    }
    server.objects["health/hrv/2026/04/2026-04-28.json"] = (gzip_json(unrelated), '"other"')
    client = boto3.client(
        "s3",
        endpoint_url="https://" + ACCOUNT + ".r2.cloudflarestorage.com",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic",
        region_name="auto",
        config=Config(
            request_checksum_calculation="when_required",
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
        ),
    )
    client._endpoint.http_session.send = server.send
    result = publish(job, job.plan_sha, confirm=True, store=R2Store(client=client, bucket=BUCKET))
    assert result["counts"] == {"published": 1}
    stored = decode(server.objects["health/hrv/2026/04/2026-04-29.json"][0])
    assert stored["summary"] == existing["summary"]
    assert stored["context"] == existing["context"]
    assert stored["archive_hrv_quality_v1"]["missing_value_count"] == 1
    assert server.objects["health/hrv/2026/04/2026-04-28.json"][0] == gzip_json(unrelated)
    index = decode(server.objects["health/indexes/hrv/v1/2026/04.json"][0])
    assert {r["date"] for r in index["days"]} == {"2026-04-28", "2026-04-29"}
    assert len(server.puts) == 4


def test_publication_denial_or_unknown_ack_stops_without_next_put_or_adoption(tmp_path):
    import boto3
    import pytest
    from botocore.config import Config
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.history_collect import collect
    from pipeline.history_publish import publish
    from pipeline.history_state import Job, Stop
    from pipeline.night_baseline_refresh import BaselineRefreshStop
    from pipeline.r2_store import R2Store

    for mode in ("conflict", "missing_ack", "lost_ack"):
        job = Job.create(tmp_path / mode, "2026-04-28", "2026-04-29", ["hr"], "legacy", "synthetic")
        collect(job, job.plan_sha, confirm=True, date_runner=native_runner("normal"))
        server = Server(mode)
        client = boto3.client(
            "s3",
            endpoint_url="https://" + ACCOUNT + ".r2.cloudflarestorage.com",
            aws_access_key_id="synthetic",
            aws_secret_access_key="synthetic",
            region_name="auto",
            config=Config(
                request_checksum_calculation="when_required",
                retries={"total_max_attempts": 1},
                s3={"addressing_style": "path"},
            ),
        )
        client._endpoint.http_session.send = server.send
        expected = BaselineRefreshStop if mode == "conflict" else Stop
        with pytest.raises(expected) as error:
            publish(job, job.plan_sha, confirm=True, store=R2Store(client=client, bucket=BUCKET))
        assert type(error.value) is expected
        if mode == "conflict":
            assert str(error.value) == "Night baseline storage denied or unknown"
        else:
            assert error.value.category == {
                "missing_ack": "unknown_write", "lost_ack": "unknown_storage_transport"
            }[mode]
        assert len(server.puts) == 1
        assert job.status()["counts"] == {"ready": 2}
        assert job.status()["locked"]
        with pytest.raises(Stop):
            publish(job, job.plan_sha, confirm=True, store=R2Store(client=client, bucket=BUCKET))
        assert len(server.puts) == 1


def test_stop_finishes_known_date_then_explicit_collection_continues_pending(tmp_path):
    from pipeline.history_collect import collect
    from pipeline.history_state import Job

    job = Job.create(tmp_path / "job", "2026-04-28", "2026-04-29", ["hr"], "legacy", "synthetic")
    runner = native_runner("normal")

    def stop_after_date(args, handoff, folder):
        outcome = runner(args, handoff, folder)
        assert cli(job.folder, "stop").returncode == 0
        return outcome

    stopped = collect(job, job.plan_sha, confirm=True, date_runner=stop_after_date)
    assert stopped["counts"] == {"ready": 1, "pending": 1}
    assert stopped["last_run_status"] == "user_stop"
    assert not stopped["locked"]
    resumed = collect(job, job.plan_sha, confirm=True, date_runner=runner)
    assert resumed["counts"] == {"ready": 2}


def test_personal_ordinary_read_uses_existing_adapter_preserves_wire_no_fallback(
    monkeypatch, tmp_path
):
    from urllib.parse import urlsplit

    import requests

    from pipeline.history_source import HistorySource
    from pipeline.history_state import digest
    from pipeline.sources.provider_config import ProviderSelection

    day = "2026-04-29"
    paths = []
    raw = {
        "calendarDate": {"localDate": day},
        "heartRateValues": [
            [{"date": day + "T01:00:00Z"}, 55],
            [{"date": day + "T01:01:00Z"}, None],
        ],
    }

    def send(adapter, request, **kwargs):
        target = urlsplit(request.url)
        assert target.netloc == "personal.connectapi.garmin.com"
        paths.append((request.method, target.path))
        response = requests.Response()
        response.status_code = 200
        token = request.method == "POST"
        body = (
            b"synthetic-token"
            if token
            else json.dumps(
                {"displayName": "synthetic"} if target.path.endswith("settings") else raw
            ).encode()
        )
        response._content = body
        response._content_consumed = True
        response.headers.update(
            {
                "Content-Length": str(len(body)),
                "Content-Type": "text/plain" if token else "application/json",
            }
        )
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    source = HistorySource(
        {"provider": "personal", "account_sha256": digest(b"synthetic")},
        {"date": day, "streams": ["hr"], "max_http": 8, "archive_reload": False},
        tmp_path,
        chosen=ProviderSelection("personal", "synthetic", "synthetic-key"),
        clock=FakeClock(),
    )
    row = source.read("hr")
    assert row["status"] == "ready"
    assert json.loads((tmp_path / row["wire"]).read_bytes()) == raw
    view = json.loads((tmp_path / row["view"]).read_bytes())
    assert view["_slipstream_source"]["provider"] == "personal"
    assert view["heartRateValues"][1][1] is None
    assert len(paths) == 3
    source.close()
