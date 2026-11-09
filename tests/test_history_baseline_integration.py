"""Portable CLI + merged canonical/reference boundary; memory artifacts, real SDK."""

import json
from collections import Counter
from pathlib import PurePosixPath

import boto3
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from history_storage_fixture import ACCOUNT, BUCKET, Raw, Server

from pipeline import history_publish, history_state
from pipeline.history_canonical import prepare
from pipeline.history_state import Job, digest, encoded
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.r2_store import R2Store


def test_canonical_hr_preparation_stays_offline_with_merged_importer():
    value = prepare(
        "hr",
        "2026-04-29",
        {
            "calendarDate": "2026-04-29",
            "heartRateValues": [[1777420800000, 55], [1777420860000, None]],
        },
    )
    assert value["date"] == "2026-04-29"
    channel = value["channels"]["heart_rate"]
    assert channel["unit"] == "bpm"
    assert [point["value"] for point in channel["readings"]] == [55]
    assert channel["missing_value_count"] == 1


def memory_job(monkeypatch, targets):
    from datetime import datetime, timezone

    from pipeline import night_baseline_refresh, reference_bulk

    class Calendar(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 1, 12, tzinfo=timezone.utc).astimezone(tz or timezone.utc)

    monkeypatch.setattr(night_baseline_refresh, "datetime", Calendar)
    files = {}

    class MemoryPath:
        def __init__(self, name):
            self.name = str(name)

        def __str__(self):
            return self.name

        def __truediv__(self, part):
            return type(self)(str(PurePosixPath(self.name) / str(part)))

        def is_relative_to(self, other):
            return PurePosixPath(self.name).is_relative_to(str(other))

        def mkdir(self, **kwargs):
            pass

        def exists(self):
            return self.name in files

        def unlink(self):
            del files[self.name]

    def read(path, maximum=16 * 1024 * 1024):
        body = files[str(path)]
        assert len(body) <= maximum
        return body

    def write(path, value):
        assert str(path) not in files, str(path)
        files[str(path)] = value if isinstance(value, bytes) else encoded(value)
        return digest(files[str(path)])

    class MemoryJob(Job):
        def append(self, event):
            frames, previous = self.events()
            frame = {
                "sequence": len(frames),
                "previous": previous,
                "plan": self.plan_sha,
                "event": event,
            }
            files[str(self.folder / "events.jsonl")] += encoded(frame)

    monkeypatch.setattr(history_publish, "Path", MemoryPath)
    monkeypatch.setattr(history_publish, "read", read)
    monkeypatch.setattr(history_publish, "write", write)
    monkeypatch.setattr(history_state, "read", read)
    # Import before patching shared state, then scope both aliases explicitly.
    # A first lazy consumer import inside the fixture must not retain fake I/O.
    monkeypatch.setattr(reference_bulk, "read", read)
    monkeypatch.setattr(reference_bulk, "write", write)
    job = MemoryJob.__new__(MemoryJob)
    job.folder = MemoryPath("job")
    job.plan = {"targets": targets}
    job.plan_sha = "a" * 64
    files["job/events.jsonl"] = b""
    for row in targets:
        day, stream = row["date"], row["stream"]
        folder = job.folder / "runs" / (day + stream)
        stamp = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        raw = (
            {"calendarDate": day, "heartRateValues": [[stamp, 55]]}
            if stream == "hr"
            else {
                "calendarDate": day,
                "hrvSummary": {"calendarDate": day, "lastNightAvg": 44},
                "hrvReadings": [
                    {
                        "hrvValue": 44,
                        "readingTimeGMT": day + "T01:00:00",
                        "readingTimeLocal": day + "T03:00:00",
                    }
                ],
            }
        )
        wire = encoded(raw)
        write(folder / "wire.json", wire)
        write(folder / "view.json", wire)
        intent_sha = write(
            folder / "INTENT.json",
            {
                "run": digest((day + stream).encode())[:32],
                "plan": job.plan_sha,
                "date": day,
                "streams": [stream],
                "max_http": 8,
                "max_seconds": 899,
                "archive_reload": False,
                "nonce": digest(b"synthetic-nonce"),
            },
        )
        receipt_sha = write(
            folder / "RECEIPT.json", {"status": "complete", "intent_sha256": intent_sha}
        )
        write(folder / "RESULT.json", {"native": 0, "receipt_sha256": receipt_sha})
        job.append(
            {
                "type": "target",
                **row,
                "status": "ready",
                "folder": str(folder),
                "native": 0,
                "receipt_sha256": receipt_sha,
                "wire": "wire.json",
                "view": "view.json",
                "wire_sha256": digest(wire),
                "view_sha256": digest(wire),
            }
        )
    return job, files


def test_memory_fixture_does_not_leak_into_later_disk_artifact_reads(tmp_path):
    from pytest import MonkeyPatch

    with MonkeyPatch.context() as patch:
        memory_job(patch, [])
        # Import at the same point as the typed-cohort continuation path.
        from pipeline import reference_bulk

    body = b"synthetic-disk-artifact"
    path = tmp_path / "scope.json"
    path.write_bytes(body)
    reference = {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}
    assert reference_bulk._pin(reference) == body


class BaselineServer(Server):
    def __init__(self):
        super().__init__()
        self.headers = []

    def send(self, request):
        from urllib.parse import unquote, urlsplit

        key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
        headers = {
            str(k).lower(): v.decode() if isinstance(v, bytes) else v
            for k, v in request.headers.items()
        }
        self.headers.append((request.method, key, headers))
        return super().send(request)


def sdk_store(server, **limits):
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
    return R2Store(client=client, bucket=BUCKET, **limits)


def test_publish_links_source_intent_and_exact_readback_without_provider(monkeypatch):
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    report = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert report["counts"] == {"published": 1}
    assert server.puts == [QUEUE_KEY, "health/signals/v1/2026/04/2026-04-29.json", QUEUE_KEY]
    queue = json.loads(server.objects[QUEUE_KEY][0])
    assert queue["pending"]["month/2026-04"]["sources"][server.puts[1]]["state"] == "ready"
    assert all(
        "if-match" in h or h.get("if-none-match") == "*"
        for method, _, h in server.headers
        if method == "PUT"
    )
    assert not job.status()["locked"]
    assert Counter(method for method, _, _ in server.calls)["PUT"] == 3
    receipt = next(
        json.loads(body) for name, body in files.items() if name.endswith("REMOTE-VERIFIED.json")
    )
    assert receipt["counts"] == {"get": 8, "list": 1, "put": 3}
    print(json.dumps({"integration_ledger": "one_new_hr", "physical": receipt["counts"]}))


def test_later_publish_drains_closed_references_with_no_new_source(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    first = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert first["baseline_refresh"]["status"] == "bounded_pending"
    source_key = "health/signals/v1/2026/04/2026-04-29.json"
    source = server.objects[source_key]
    server.puts.clear()
    second = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert second["baseline_refresh"]["products_written"] >= 2
    assert server.objects[source_key] == source
    assert source_key not in server.puts
    products = [
        json.loads(body) for key, (body, _) in server.objects.items() if "/baseline/" in key
    ]
    assert products and all(p["contributing_nights"] == 0 for p in products)
    assert all(not p["bins"] for p in products)
    assert second["counts"] == {"published": 1}


def test_low_write_budget_defers_before_dirty_intent_then_explicit_publish_can_finish(monkeypatch):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server, max_writes_per_run=2)
    )
    assert report["counts"] == {"ready": 1}
    assert report["baseline_refresh"]["source_targets_deferred"] == 1
    assert not report["locked"] and server.puts == [] and server.objects == {}
    report = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert report["counts"] == {"published": 1}
    assert len(server.puts) == 3


def test_hrv_merge_uses_dirty_helper_preserves_summary_nulls_and_month_index(monkeypatch):
    from pipeline.granular import gzip_json

    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hrv"}])
    folder = "job/runs/2026-04-29hrv/"
    raw = json.loads(files[folder + "view.json"])
    raw["hrvReadings"].append(
        {
            "hrvValue": None,
            "readingTimeGMT": "2026-04-29T01:05:00",
            "readingTimeLocal": "2026-04-29T03:05:00",
        }
    )
    body = encoded(raw)
    files[folder + "wire.json"] = files[folder + "view.json"] = body
    job.append(
        {
            **job.snapshot()["rows"][0],
            "type": "target",
            "wire_sha256": digest(body),
            "view_sha256": digest(body),
        }
    )
    server = BaselineServer()
    key = "health/hrv/2026/04/2026-04-29.json"
    old = {
        "schema_version": 1,
        "source": "garmin",
        "date": "2026-04-29",
        "readings": [],
        "available": False,
        "summary": {"last_night_avg": 42},
        "context": {"travel": True, "actual_night_timezone": "Europe/Oslo"},
    }
    before = gzip_json(old)
    server.objects[key] = (before, '"old-hrv"')
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    value = __import__("pipeline.history_canonical", fromlist=["decode"]).decode(
        server.objects[key][0]
    )
    assert value["summary"] == old["summary"] and value["context"] == old["context"]
    assert value["readings"][0]["hrv_ms"] == 44
    assert value["archive_hrv_quality_v1"]["missing_readings"][0]["hrvValue"] is None
    assert server.puts == [QUEUE_KEY, key, QUEUE_KEY, "health/indexes/hrv/v1/2026/04.json"]
    index = __import__("pipeline.history_canonical", fromlist=["decode"]).decode(
        server.objects[server.puts[-1]][0]
    )
    assert index["days"][0]["date"] == "2026-04-29"
    assert result["counts"] == {"published": 1}
    conditions = [h for method, name, h in server.headers if method == "PUT" and name == key]
    assert conditions[0]["if-match"] == '"old-hrv"'


def test_unknown_first_dirty_write_keeps_owner_and_blocks_reentry(monkeypatch):
    import pytest

    from pipeline.history_state import Stop

    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    server.mode = "lost_ack"
    store = sdk_store(server)
    with pytest.raises(Stop):
        history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    assert server.puts == [QUEUE_KEY]
    assert job.status()["locked"] and job.status()["counts"] == {"ready": 1}
    calls = len(server.calls)
    with pytest.raises(Stop):
        history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    assert len(server.calls) == calls


def test_existing_good_hr_skips_source_write_and_keeps_other_channels(monkeypatch):
    from pipeline.granular import gzip_json

    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    key = "health/signals/v1/2026/04/2026-04-29.json"
    old = prepare(
        "hr", "2026-04-29", {"calendarDate": "2026-04-29", "heartRateValues": [[1777420800000, 61]]}
    )
    old["channels"]["stress"] = {"status": "no_data", "readings": [], "unit": "points"}
    server.objects[key] = (gzip_json(old), '"good-old"')
    before = dict(server.objects)
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["counts"] == {"published": 1}
    assert server.objects == before and server.puts == []


def test_full_open_queue_defers_new_date_without_poisoning_owner(monkeypatch):
    import calendar

    monkeypatch.delenv("HEALTH_TIMEZONE", raising=False)
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    pending = {}
    for ordinal in range(128):
        year, month = 2090 + ordinal // 12, ordinal % 12 + 1
        pending[f"month/{year}-{month:02d}"] = {
            "phase": "dirty",
            "sources": {},
            "completed": {},
            "start_date": f"{year}-{month:02d}-01",
            "end_date": f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}",
        }
    queue = encoded(
        {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": pending}
    )
    server.objects[QUEUE_KEY] = (queue, '"queue-full"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["counts"] == {"ready": 1} and not result["locked"]
    assert result["baseline_refresh"]["source_targets_deferred"] == 1
    assert server.objects[QUEUE_KEY][0] == queue and server.puts == []


def test_seven_date_publication_keeps_two_month_indexes_and_defers_eighth_date(monkeypatch):
    from datetime import date, timedelta

    monkeypatch.delenv("HEALTH_TIMEZONE", raising=False)
    days = [(date(2026, 4, 6) - timedelta(days=i)).isoformat() for i in range(8)]
    targets = [{"date": day, "stream": stream} for day in days for stream in ("hr", "hrv")]
    job, _ = memory_job(monkeypatch, targets)
    server = BaselineServer()
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["counts"] == {"published": 14, "ready": 2}
    assert len(server.puts) == 44  # 14*(intent + canonical + ready) + two month indexes
    indexes = {key for key in server.objects if key.startswith("health/indexes/hrv/")}
    assert indexes == {"health/indexes/hrv/v1/2026/03.json", "health/indexes/hrv/v1/2026/04.json"}
    assert "health/hrv/2026/03/2026-03-30.json" not in server.objects
    receipts = [
        json.loads(body) for name, body in _.items() if name.endswith("REMOTE-VERIFIED.json")
    ]
    assert receipts[0]["counts"]["put"] == 44
    assert receipts[0]["counts"]["get"] <= 400 and receipts[0]["counts"]["list"] <= 100
    print(
        json.dumps(
            {
                "integration_ledger": "seven_dates_two_months",
                "physical": receipts[0]["counts"],
                "acknowledged": receipts[0]["acknowledged_writes"],
            }
        )
    )


def test_full_month_and_new_source_share_postpublication_400_get_cap(monkeypatch):
    from test_night_baseline_corrections import calendar_store, queued_period

    monkeypatch.setenv("HEALTH_TIMEZONE", "UTC")
    initial = calendar_store()
    queued_period(
        initial,
        "2026-10-01",
        "2026-10-31",
        [key for key in initial.objects if key.startswith("health/hrv/")],
    )
    server = BaselineServer()
    for key, snap in initial.objects.items():
        server.objects[key] = (snap.data, snap.revision)
        server.metadata[key] = {"content-type": snap.content_type}
        if snap.encoding:
            server.metadata[key]["content-encoding"] = snap.encoding
    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hrv"}])
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["baseline_refresh"]["status"] == "bounded_pending"
    assert result["counts"] == {"published": 1} and not result["locked"]
    first = next(
        json.loads(body) for name, body in files.items() if name.endswith("REMOTE-VERIFIED.json")
    )
    assert first["counts"]["get"] <= 400 and first["counts"]["list"] <= 100
    assert first["counts"]["put"] <= 100
    assert "health/hrv/2026/04/2026-04-29.json" in server.objects
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["counts"] == {"published": 1}
    print(
        json.dumps(
            {
                "integration_ledger": "full_month_after_sources",
                "physical": first["counts"],
                "deferred_source_targets": first["baseline_refresh"]["source_targets_deferred"],
            }
        )
    )


def test_cleanup_only_io_retains_verified_remote_evidence_and_owner(monkeypatch, capsys):
    from pipeline import history_backfill

    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    client = sdk_store(server).client
    closes = []

    def close():
        closes.append(job.status()["locked"])
        raise OSError("synthetic private cleanup sentinel")

    monkeypatch.setattr(client, "close", close)
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    monkeypatch.setattr(history_backfill, "Job", lambda path: job)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    native = history_backfill.main(["publish", "--plan-sha", job.plan_sha, "--confirm-r2-write"])
    output = capsys.readouterr()
    assert native == 3 and closes == [True]
    assert "sentinel" not in output.out + output.err
    assert job.status()["locked"] and job.status()["counts"] == {"ready": 1}
    remote = next(
        json.loads(body) for name, body in files.items() if name.endswith("REMOTE-VERIFIED.json")
    )
    assert remote["acknowledged_writes"] == 3 and len(remote["destinations"]) == 1
    assert not any(
        name.endswith("/RECEIPT.json") and "/2026-04-29hr/" not in name for name in files
    )
    calls = len(server.calls)
    monkeypatch.setattr(
        boto3,
        "client",
        lambda *a, **kw: (_ for _ in ()).throw(
            AssertionError("credential construction after HOLD")
        ),
    )
    assert history_backfill.main(["publish", "--plan-sha", job.plan_sha, "--confirm-r2-write"]) == 2
    assert len(server.calls) == calls


def test_unverified_raw_inventory_never_becomes_write_authority(monkeypatch):
    from urllib.parse import parse_qs, urlsplit

    import pytest

    from pipeline.r2_store import R2BudgetError

    class InvalidInventory(BaselineServer):
        def send(self, request):
            response = super().send(request)
            if parse_qs(urlsplit(request.url).query).get("list-type") == ["2"]:
                body = response.raw.read().replace(
                    b"<IsTruncated>false</IsTruncated>", b"<IsTruncated>unverified</IsTruncated>"
                )
                response.raw.close()
                return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))
            return response

    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = InvalidInventory()
    with pytest.raises(R2BudgetError):
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert server.puts == [] and job.status()["locked"]


def test_inventory_consuming_list_reserve_defers_before_source_or_index_intent(monkeypatch):
    from urllib.parse import parse_qs, urlsplit

    class PagedInventory(BaselineServer):
        def send(self, request):
            query = parse_qs(urlsplit(request.url).query)
            if query.get("list-type") != ["2"]:
                return super().send(request)
            assert not query.get("prefix")
            self.calls.append((request.method, "", query))
            page = int(query.get("continuation-token", ["0"])[0])
            last = page == 99
            body = (
                f"<ListBucketResult><Name>{BUCKET}</Name><IsTruncated>{str(not last).lower()}</IsTruncated>"
                f"<KeyCount>1</KeyCount><Contents><Key>unrelated-{page}.bin</Key><Size>1</Size></Contents>"
                + ("" if last else f"<NextContinuationToken>{page + 1}</NextContinuationToken>")
                + "</ListBucketResult>"
            ).encode()
            return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))

    job, files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hrv"}])
    server = PagedInventory()
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server))
    assert result["counts"] == {"ready": 1} and not result["locked"]
    assert server.puts == []
    receipt = next(
        json.loads(body) for name, body in files.items() if name.endswith("REMOTE-VERIFIED.json")
    )
    assert receipt["counts"] == {"get": 1, "list": 100, "put": 0}
    assert receipt["source_deferred"] is True


def test_pure_hr_preparation_matches_normal_merged_collector_canonical_bytes(monkeypatch):
    from pipeline.granular import gzip_json
    from pipeline.signal_series import DailySignalCollector

    monkeypatch.delenv("HEALTH_TIMEZONE", raising=False)
    raw = {
        "calendarDate": "2026-04-29",
        "heartRateValues": [[1777420800000, 55], [1777420860000, None]],
    }
    expected = gzip_json(prepare("hr", "2026-04-29", raw))
    server = BaselineServer()
    collector = DailySignalCollector(sdk_store(server))
    collector.collect("2026-04-29", "heart_rate", raw)
    report = collector.publish()
    assert report["objects_written"] == 1
    assert server.objects["health/signals/v1/2026/04/2026-04-29.json"][0] == expected
