"""Correction06: actual SDK inventory parity and known byte-capacity admission."""

from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import BUCKET, Raw
from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_publish
from pipeline.history_state import Stop
from pipeline.night_baseline_refresh import QUEUE_KEY


class PaginationServer(BaselineServer):
    def __init__(self, whitespace=False):
        super().__init__()
        self.whitespace = whitespace

    def send(self, request):
        query = parse_qs(urlsplit(request.url).query)
        if query.get("list-type") != ["2"]:
            return super().send(request)
        self.calls.append((request.method, "", query))
        assert not query.get("prefix")
        last = "continuation-token" in query
        if last:
            assert query["continuation-token"] == ["next-page"]
        truncation = "false" if last else " true " if self.whitespace else "true"
        name = "second.bin" if last else "first.bin"
        body = (
            f"<ListBucketResult><Name>{BUCKET}</Name><KeyCount>1</KeyCount>"
            "<Prefix></Prefix><MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType>"
            + ("<ContinuationToken>next-page</ContinuationToken>" if last else "")
            + f"<IsTruncated>{truncation}</IsTruncated><Contents><Key>{name}</Key>"
            "<Size>2</Size></Contents>"
            + ("" if last else "<NextContinuationToken>next-page</NextContinuationToken>")
            + "</ListBucketResult>"
        ).encode()
        return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))


def test_raw_sdk_truncation_disagreement_cannot_authorize_source_intent(monkeypatch):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = PaginationServer(whitespace=True)
    store = sdk_store(server)
    stopped = None
    try:
        history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    except Stop as error:
        stopped = error.category
    assert (stopped, server.puts) == ("storage_list_projection_mismatch", [])
    assert not store.write_budget_initialized
    assert job.status()["locked"] and job.status()["counts"] == {"ready": 1}
    assert len([c for c in server.calls if c[2].get("list-type") == ["2"]]) == 1


def test_valid_physical_pagination_completes_one_inventory_and_conditional_source(monkeypatch):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = PaginationServer()
    store = sdk_store(server)
    result = history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    assert result["counts"] == {"published": 1}
    assert server.puts == [QUEUE_KEY, "health/signals/v1/2026/04/2026-04-29.json", QUEUE_KEY]
    assert len([c for c in server.calls if c[2].get("list-type") == ["2"]]) == 2
    assert store.write_budget_initialized


@pytest.mark.parametrize("byte_cap", [2097152, 1200000])
def test_aggregate_staging_bytes_admit_prefix_then_explicit_publish_finishes(monkeypatch, byte_cap):
    import base64
    import hashlib
    import json
    import random
    from datetime import date, timedelta

    from pipeline.granular import gzip_json

    monkeypatch.delenv("HEALTH_TIMEZONE", raising=False)
    days = [(date(2026, 4, 6) - timedelta(days=i)).isoformat() for i in range(7)]
    job, files = memory_job(monkeypatch, [{"date": day, "stream": "hrv"} for day in days])
    server = BaselineServer()
    originals = {}
    for ordinal, day in enumerate(days):
        key = f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json"
        padding = base64.b64encode(random.Random(ordinal).randbytes(250000)).decode("ascii")
        old = {
            "schema_version": 1,
            "source": "garmin",
            "date": day,
            "readings": [],
            "summary": {"last_night_avg": 42, "preserved_source_note": padding},
        }
        body = gzip_json(old)
        assert len(body) < 262144
        originals[key] = (body, '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"')
    assert sum(len(body) for body, _ in originals.values()) + 2 * 262144 > 2097152
    server.objects.update(originals)
    first = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server, max_write_bytes_per_run=byte_cap)
    )
    assert 0 < first["counts"]["published"] < 7
    assert first["counts"]["published"] + first["counts"]["ready"] == 7
    assert not first["locked"]
    remaining = [row for row in job.snapshot()["rows"] if row["status"] == "ready"]
    for row in remaining:
        key = f"health/hrv/{row['date'][:4]}/{row['date'][5:7]}/{row['date']}.json"
        assert server.objects[key] == originals[key]
    first_receipt = next(
        json.loads(body) for name, body in files.items() if name.endswith("REMOTE-VERIFIED.json")
    )
    assert first_receipt["source_deferred"] is True
    # Physical byte spending includes changing queue bodies; use the guard's
    # exact pre-send totals from the receipt once a fitting prefix is admitted.
    assert first_receipt["body_bytes"] <= byte_cap
    result = first
    for _ in range(3):
        if result["counts"] == {"published": 7}:
            break
        previous = result["counts"]["published"]
        result = history_publish.publish(
            job,
            job.plan_sha,
            confirm=True,
            store=sdk_store(server, max_write_bytes_per_run=byte_cap),
        )
        assert result["counts"]["published"] > previous and not result["locked"]
        latest = [
            json.loads(body)
            for name, body in files.items()
            if name.endswith("REMOTE-VERIFIED.json")
        ][-1]
        assert latest["body_bytes"] <= byte_cap
    assert result["counts"] == {"published": 7} and not result["locked"]
    assert set(key for key in server.objects if key.startswith("health/indexes/hrv/")) == {
        "health/indexes/hrv/v1/2026/03.json",
        "health/indexes/hrv/v1/2026/04.json",
    }
    print(
        json.dumps(
            {
                "correction_ledger": "aggregate_bytes_prefix",
                "first_counts": first["counts"],
                "first_physical": first_receipt["counts"],
                "first_body_bytes": first_receipt["body_bytes"],
                "configured_byte_cap": byte_cap,
                "final_counts": result["counts"],
            }
        )
    )
