"""Narrow regressions for the aggregated portable-history review round."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from test_history_backfill import cli, native_runner

from pipeline.history_state import Job, Stop
from pipeline.r2_store import R2BudgetError


def interleave_pause(monkeypatch, job, until):
    original = Path.open
    fired = []

    def open_at_owner(path, mode="r", *args, **kwargs):
        if path == job.folder / "OWNER.lock" and mode == "xb" and not fired:
            fired.append(True)
            result = cli(job.folder, "pause", "--until", until, "--reason", "operator_quota")
            assert result.returncode == 0, result.stderr
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_at_owner)
    return fired


def test_admission_rechecks_later_native_pause_before_source_or_resume(monkeypatch, tmp_path):
    from pipeline.history_collect import collect
    from pipeline.sources.provider_config import ProviderSelection

    monkeypatch.setattr(
        ProviderSelection, "credential", lambda self: pytest.fail("credential read")
    )
    for resume in (False, True):
        job = Job.create(
            tmp_path / str(resume), "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic"
        )
        if resume:
            assert (
                cli(
                    job.folder,
                    "pause",
                    "--until",
                    "2000-01-01T00:00:00Z",
                    "--reason",
                    "operator_quota",
                ).returncode
                == 0
            )
        with monkeypatch.context() as local:
            fired = interleave_pause(local, job, "2099-01-01T00:00:00Z")
            with pytest.raises(Stop):
                collect(
                    job,
                    job.plan_sha,
                    confirm=True,
                    resume=resume,
                    date_runner=lambda *a: pytest.fail("source called"),
                )
            assert fired
        state = Job(job.folder).status()
        assert state["pause"] == "operator_quota"
        assert state["not_before"] == "2099-01-01T00:00:00+00:00"
        assert not state["locked"]


def test_pause_computes_extension_under_owner(monkeypatch, tmp_path):
    from pipeline.history_backfill import main

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    interleave_pause(monkeypatch, job, "2099-01-01T00:00:00Z")
    assert (
        main(
            [
                "--state",
                str(job.folder),
                "pause",
                "--until",
                "2090-01-01T00:00:00Z",
                "--reason",
                "operator_quota",
            ]
        )
        == 0
    )
    assert Job(job.folder).status()["not_before"] == "2099-01-01T00:00:00+00:00"


def test_malformed_public_cli_never_echoes_private_arguments(tmp_path):
    sentinel = "PRIVATE_SENTINEL_no_echo_82721"
    for args in (
        [sentinel],
        ["status", "--unknown", sentinel],
        ["collect", "--plan-sha", "synthetic", "--max-dates", sentinel],
        [
            "plan",
            "--start",
            "2026-04-29",
            "--end",
            "2026-04-29",
            "--streams",
            "hr",
            "--provider",
            sentinel,
            "--account",
            "synthetic",
        ],
    ):
        result = cli(tmp_path / "unused", *args)
        assert result.returncode == 2
        assert sentinel not in result.stdout + result.stderr
        assert "Traceback" not in result.stdout + result.stderr


def cleanup_boundary(mode, folder):
    import boto3
    from botocore.awsrequest import AWSResponse
    from botocore.config import Config
    from history_storage_fixture import ACCOUNT, BUCKET

    from pipeline.granular import gzip_json
    from pipeline.history_publish import StorageGuard
    from pipeline.r2_store import R2Store

    class Raw:
        def stream(self, *args, **kwargs):
            if mode == "read_and_close":
                raise Stop("synthetic_primary_read")
            if mode == "io_and_close_stop":
                raise OSError("synthetic_primary_io")
            if mode == "bound_and_close":
                yield b"x" * 524289
            else:
                yield gzip_json({"schema_version": 1, "source": "garmin", "date": "2026-04-29"})

        def close(self):
            if mode == "io_and_close_stop":
                raise Stop("synthetic_secondary_close_stop")
            raise OSError("synthetic_secondary_close")

    client = boto3.client(
        "s3",
        endpoint_url="https://" + ACCOUNT + ".r2.cloudflarestorage.com",
        aws_access_key_id="synthetic",
        aws_secret_access_key="synthetic",
        region_name="auto",
        config=Config(retries={"total_max_attempts": 1}, s3={"addressing_style": "path"}),
    )
    client._endpoint.http_session.send = lambda request: AWSResponse(
        request.url,
        200,
        {"content-type": "application/json", "content-encoding": "gzip", "etag": '"synthetic"'},
        Raw(),
    )
    store = R2Store(client=client, bucket=BUCKET)
    key = "health/signals/v1/2026/04/2026-04-29.json"
    guard = StorageGuard(store, {key}, folder)
    try:
        store.get_snapshot(key, max_bytes=262144)
    finally:
        guard.close()


def test_normal_sdk_raw_cleanup_preserves_primary_native_class(tmp_path):
    import subprocess
    import sys

    for mode, expected in [
        ("bound_and_close", 2),
        ("read_and_close", 2),
        ("close_only", 3),
        ("io_and_close_stop", 3),
    ]:
        folder = tmp_path / mode
        folder.mkdir()
        result = subprocess.run(
            [sys.executable, "-B", str(Path(__file__)), mode, str(folder)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == expected, result.stdout + result.stderr
        assert "Traceback" not in result.stdout + result.stderr


def ready_job(folder, streams=("hr",)):
    from pipeline.history_collect import collect

    job = Job.create(folder, "2026-04-29", "2026-04-29", list(streams), "legacy", "synthetic")
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("normal"))
    return job


def storage(server):
    import boto3
    from botocore.config import Config
    from history_storage_fixture import ACCOUNT, BUCKET

    from pipeline.r2_store import R2Store

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
    return R2Store(client=client, bucket=BUCKET)


def test_incomplete_or_invalid_list_pages_stop_before_inventory_cache_or_put(tmp_path):
    import re
    from urllib.parse import parse_qs, urlsplit

    from botocore.awsrequest import AWSResponse
    from history_storage_fixture import Raw, Server

    from pipeline.history_publish import publish

    modes = [
        "missing_truncation",
        "missing_size",
        "negative_size",
        "bool_size",
        "invalid_size",
        "missing_contents",
        "truncated_no_token",
    ]
    for mode in modes:

        class InvalidList(Server):
            def send(self, request):
                response = super().send(request)
                if parse_qs(urlsplit(request.url).query).get("list-type") == ["2"]:
                    body = response.raw.read()
                    response.raw.close()
                    if self.mode == "missing_truncation":
                        body = body.replace(b"<IsTruncated>false</IsTruncated>", b"")
                    elif self.mode == "missing_size":
                        body = re.sub(rb"<Size>.*?</Size>", b"", body)
                    elif self.mode in {"negative_size", "bool_size", "invalid_size"}:
                        value = {
                            "negative_size": b"-7",
                            "bool_size": b"true",
                            "invalid_size": b"not-an-int",
                        }[self.mode]
                        body = re.sub(rb"<Size>.*?</Size>", b"<Size>" + value + b"</Size>", body)
                    elif self.mode == "missing_contents":
                        body = re.sub(rb"<Contents>.*?</Contents>", b"", body)
                    else:
                        body = body.replace(
                            b"<IsTruncated>false</IsTruncated>", b"<IsTruncated>true</IsTruncated>"
                        )
                    return AWSResponse(
                        request.url, 200, {"content-length": str(len(body))}, Raw(body)
                    )
                return response

        job = ready_job(tmp_path / mode)
        server = InvalidList(mode)
        server.objects["unrelated.bin"] = (b"1234567", '"other"')
        store = storage(server)
        with pytest.raises(
            (
                Stop,
                ValueError,
                R2BudgetError,
            )
        ):
            publish(job, job.plan_sha, confirm=True, store=store)
        assert server.puts == []
        assert not store.write_budget_initialized
        assert job.status()["counts"] == {"ready": 1}


def test_existing_hr_unit_is_verified_before_preserve_or_published(tmp_path):
    from history_storage_fixture import Server

    from pipeline.granular import gzip_json
    from pipeline.history_canonical import key, prepare
    from pipeline.history_publish import publish

    for unit in ("ms", None, "bpm"):
        job = ready_job(tmp_path / str(unit))
        value = prepare(
            "hr",
            "2026-04-29",
            {"calendarDate": "2026-04-29", "heartRateValues": [[1777420800000, 55]]},
        )
        if unit is None:
            value["channels"]["heart_rate"].pop("unit")
        else:
            value["channels"]["heart_rate"]["unit"] = unit
        server = Server()
        name = key("hr", "2026-04-29")
        previous = (gzip_json(value), '"old-good-or-invalid"')
        server.objects[name] = previous
        if unit == "bpm":
            assert publish(job, job.plan_sha, confirm=True, store=storage(server))["counts"] == {
                "published": 1
            }
        else:
            with pytest.raises(Stop):
                publish(job, job.plan_sha, confirm=True, store=storage(server))
            assert job.status()["counts"] == {"ready": 1}
            assert job.status()["locked"]
        assert server.puts == []
        assert server.objects[name] == previous


def test_month_preflight_holds_valid_legacy_only_but_accepts_canonical_shadows(tmp_path):
    from history_storage_fixture import Server

    from pipeline.granular import gzip_json
    from pipeline.history_canonical import decode, prepare
    from pipeline.history_publish import publish

    for mode in ("legacy_only", "shadow", "invalid_legacy"):
        job = ready_job(tmp_path / mode, ("hrv",))
        server = Server()
        old = prepare(
            "hrv",
            "2026-04-28",
            {
                "calendarDate": "2026-04-28",
                "hrvSummary": {"lastNightAvg": 42},
                "hrvReadings": [{"readingTimeGMT": "2026-04-28T01:00:00", "hrvValue": 33}],
            },
        )
        old_bytes = gzip_json(old)
        if mode == "shadow":
            server.objects["health/hrv/2026/04/2026-04-28.json"] = (old_bytes, '"canonical-old"')
        legacy = (
            "health/hrv/2026/04/"
            + ("not-a-date" if mode == "invalid_legacy" else "2026-04-28")
            + ".json.gz"
        )
        server.objects[legacy] = (old_bytes, '"legacy-old"')
        if mode == "legacy_only":
            with pytest.raises(Stop):
                publish(job, job.plan_sha, confirm=True, store=storage(server))
            assert server.puts == []
            assert job.status()["counts"] == {"ready": 1}
        else:
            result = publish(job, job.plan_sha, confirm=True, store=storage(server))
            assert result["counts"] == {"published": 1}
            assert len(server.puts) == 4
            index = decode(server.objects["health/indexes/hrv/v1/2026/04.json"][0])
            expected = {"2026-04-29", "2026-04-28"} if mode == "shadow" else {"2026-04-29"}
            assert {r["date"] for r in index["days"]} == expected
            assert (
                sum(c[2].get("list-type") == ["2"] and not c[2].get("prefix") for c in server.calls)
                == 1
            )
        assert server.objects[legacy] == (old_bytes, '"legacy-old"')
        if mode == "shadow":
            assert server.objects["health/hrv/2026/04/2026-04-28.json"] == (
                old_bytes,
                '"canonical-old"',
            )


def test_outer_sdk_cleanup_keeps_publication_primary_and_attempts_both_closes(
    monkeypatch, tmp_path
):
    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.history_publish import publish
    from pipeline.night_baseline_refresh import BaselineRefreshStop

    job = ready_job(tmp_path / "job")
    server = Server("conflict")
    client = storage(server).client
    observed = []
    primaries = []
    original_unregister = client.meta.events.unregister
    original_first = client.meta.events.register_first
    original_register = client.meta.events.register
    registered = []

    def register_first(event_name, handler, *args, **kwargs):
        if not kwargs.get("unique_id", "").startswith("night-baseline-"):
            registered.append((event_name, handler))
        return original_first(event_name, handler, *args, **kwargs)

    def register(event_name, handler, *args, **kwargs):
        if not kwargs.get("unique_id", "").startswith("night-baseline-"):
            registered.append((event_name, handler))
        return original_register(event_name, handler, *args, **kwargs)

    def unregister(*args, **kwargs):
        if kwargs.get("unique_id", "").startswith("night-baseline-"):
            return original_unregister(*args, **kwargs)
        observed.append((args[0], args[1]))
        primaries.append(sys.exception())
        raise OSError("secondary guard cleanup")

    def close():
        observed.append(("client", None))
        primaries.append(sys.exception())
        raise OSError("secondary client cleanup")

    monkeypatch.setattr(client.meta.events, "register_first", register_first)
    monkeypatch.setattr(client.meta.events, "register", register)
    monkeypatch.setattr(client.meta.events, "unregister", unregister)
    monkeypatch.setattr(client, "close", close)
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(BaselineRefreshStop) as error:
        publish(job, job.plan_sha, confirm=True)
    assert type(error.value) is BaselineRefreshStop
    assert str(error.value) == "Night baseline storage denied or unknown"
    assert all(primary is error.value for primary in primaries)
    assert [name for name, _ in registered] == ["before-send.s3", "after-call.s3"]
    assert [name for name, _ in observed] == ["before-send.s3", "after-call.s3", "client"]
    for (registered_name, registered_handler), (removed_name, removed_handler) in zip(
        registered, observed[:2], strict=True
    ):
        assert removed_name == registered_name
        assert removed_handler.__self__ is registered_handler.__self__
        assert removed_handler.__func__ is registered_handler.__func__
    assert registered[0][1].__self__ is registered[1][1].__self__
    assert len(server.puts) == 1
    assert job.status()["counts"] == {"ready": 1} and job.status()["locked"]
    calls = len(server.calls)
    monkeypatch.setattr(
        boto3, "client", lambda *args, **kwargs: pytest.fail("credential construction after HOLD")
    )
    with pytest.raises(Stop, match="unknown_owner_hold_no_takeover"):
        publish(job, job.plan_sha, confirm=True)
    assert len(server.calls) == calls
    assert [name for name, _ in observed] == ["before-send.s3", "after-call.s3", "client"]


def test_valid_list_continuation_preserves_single_cached_inventory(tmp_path):
    from urllib.parse import parse_qs, urlsplit

    from botocore.awsrequest import AWSResponse
    from history_storage_fixture import BUCKET, Raw, Server

    from pipeline.history_publish import publish

    class Pages(Server):
        def send(self, request):
            query = parse_qs(urlsplit(request.url).query)
            if query.get("list-type") != ["2"]:
                return super().send(request)
            self.calls.append((request.method, "", query))
            second = "continuation-token" in query
            if second:
                assert query["continuation-token"] == ["next-page"]
            name = "second.bin" if second else "first.bin"
            body = (
                "<ListBucketResult><Name>"
                + BUCKET
                + "</Name><IsTruncated>"
                + ("false" if second else "true")
                + "</IsTruncated><KeyCount>1</KeyCount><Contents><Key>"
                + name
                + "</Key><Size>2</Size></Contents>"
                + ("" if second else "<NextContinuationToken>next-page</NextContinuationToken>")
                + "</ListBucketResult>"
            ).encode()
            return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))

    job = ready_job(tmp_path / "job")
    server = Pages()
    store = storage(server)
    assert publish(job, job.plan_sha, confirm=True, store=store)["counts"] == {"published": 1}
    lists = [c for c in server.calls if c[2].get("list-type") == ["2"]]
    assert len(lists) == 2
    assert sum("continuation-token" not in c[2] for c in lists) == 1
    assert store.write_budget_initialized
    assert len(server.puts) == 3


if __name__ == "__main__":
    import sys

    try:
        cleanup_boundary(sys.argv[1], Path(sys.argv[2]))
    except Stop:
        raise SystemExit(2) from None
    except OSError:
        raise SystemExit(3) from None
    raise SystemExit(77)
