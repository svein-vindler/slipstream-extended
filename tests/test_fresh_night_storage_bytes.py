"""Normal SDK construction, existing queue, and physical pre-send byte bounds."""
import json
import socket
from datetime import UTC, date, datetime, timedelta
from urllib.parse import unquote, urlsplit

import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_fresh_night_bulk_followup import qualify
from test_recent_night_production import NightServer, clock

from pipeline.fresh_night_references import refresh_current_night
from pipeline.history_state import Stop
from pipeline.night_baseline_refresh import QUEUE_KEY, affected_periods
from pipeline.night_products import normalize_args, storage_key
from pipeline.r2_store import R2Store


def old_queue(days):
    queue = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {}}
    for offset in range(days):
        day = (date(2026, 1, 1) + timedelta(days=offset)).isoformat()
        for kind, root in (("sleep", "sleep/v1"), ("hrv", "hrv"), ("signals", "signals/v1")):
            key = f"health/{root}/{day[:4]}/{day[5:7]}/{day}.json"
            for name, (first, last) in affected_periods(kind, day).items():
                row = queue["pending"].setdefault(name, {"start_date": first,
                    "end_date": last, "phase": "dirty", "sources": {}, "following_sources": {}})
                row["sources"][key] = {"state": "ready", "sha256": "a" * 64,
                                        "revision": '"synthetic-old-revision"'}
    return queue


class NormalServer(NightServer):
    def send(self, request):
        response = super().send(request)
        if request.method == "PUT" and response.status_code == 200:
            key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
            metadata = self.metadata.get(key, {})
            # aws-chunked is a transport encoding, never persisted object metadata.
            encoding = metadata.pop("content-encoding", "")
            encoding = ",".join(part for part in encoding.split(",") if part != "aws-chunked")
            if encoding:
                metadata["content-encoding"] = encoding
        return response


def normal_store(monkeypatch, server):
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic", "R2_BUCKET": BUCKET,
        "AWS_REQUEST_CHECKSUM_CALCULATION": "when_supported"}.items():
        monkeypatch.setenv(name, value)
    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected external network access")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    store = R2Store()
    store.client._endpoint.http_session.send = server.send
    return store


def test_normal_constructor_fresh_with_prior_writes_and_existing_queue(tmp_path, monkeypatch):
    day = "2026-09-30"
    clock(monkeypatch, day)
    server = NormalServer()
    store = normal_store(monkeypatch, server)
    observed = []
    def observe(request, **kwargs):
        if request.method != "PUT":
            return
        headers = {str(k).lower(): v.decode() if isinstance(v, bytes) else v
                   for k, v in request.headers.items()}
        observed.append({"aws_chunked": "aws-chunked" in headers.get("content-encoding", ""),
                         "content_length": int(headers.get("content-length", "-1")),
                         "decoded_content_length": int(headers.get("x-amz-decoded-content-length", "-1"))})
    store.client.meta.events.register_first("before-send.s3", observe)
    try:
        queue = old_queue(24)
        store.put(QUEUE_KEY, json.dumps(queue, sort_keys=True, separators=(",", ":")).encode(), "application/json")
        # Ordinary summary/source bytes already consumed; no allowance is reset.
        store.put("summary/synthetic-prior.csv", b"synthetic," * 18000, "text/csv")
        now = datetime(2026, 9, 30, 7, tzinfo=UTC)
        garmin = qualify(store, day, now)
        calls = list(garmin.calls)
        before_bytes = store._write_bytes
        old = json.loads(store.get(QUEUE_KEY))["pending"]
        try:
            result = refresh_current_night(store, day, now=now, folder=tmp_path / "fresh")
        except Stop as error:
            print(json.dumps({"failure_category": error.category, "last_put_metadata": observed[-1],
                              "prior_write_bytes": before_bytes, "queue_bytes": len(json.dumps(queue).encode())}))
            raise
        assert result["status"] == "complete" and result["selected_remaining"] == 0
        assert result["products_written"] == 10
        for width in (7, 28, 30, 90, 365):
            first = (date.fromisoformat(day) - timedelta(days=width - 1)).isoformat()
            for channel in ("heart_rate", "hrv"):
                key = storage_key(normalize_args(period="custom", start_date=first,
                                                end_date=day, channel=channel))
                assert key in server.objects
        assert garmin.calls == calls
        assert all(json.loads(store.get(QUEUE_KEY))["pending"].get(key) == value
                   for key, value in old.items())
        assert store._write_bytes >= before_bytes
        assert store._bucket_budget.write_bytes == store._write_bytes
        terminals = [json.loads(path.read_bytes()) for path in
                     (tmp_path / "fresh/workset/publications").glob("*/TERMINAL.json")]
        assert terminals and all(row["body_bytes"] <= 2 * 2097152 for row in terminals)
        print(json.dumps({"producer_targets": 10, "prior_write_bytes": before_bytes,
                          "last_put_metadata": observed[-1], "operations": store.operations}))
    finally:
        store.client.close()

@pytest.mark.parametrize("reason", ["body_length_unknown", "phase_write_bytes", "execution_write_bytes"])
def test_byte_rejections_are_before_wire_and_safely_distinguished(tmp_path, monkeypatch, reason):
    import boto3
    from botocore.config import Config

    from pipeline.failure_diagnostics import failure_details
    from pipeline.history_publish import StorageGuard

    server = NormalServer()
    store = normal_store(monkeypatch, server)
    if reason == "body_length_unknown":
        store.client.close()
        # A caller-supplied SDK client can still use chunked checksums. Reject it;
        # never trust its decoded-length header as a physical admission bypass.
        client = boto3.client("s3", endpoint_url="https://" + ACCOUNT + ".r2.cloudflarestorage.com",
            aws_access_key_id="synthetic", aws_secret_access_key="synthetic", region_name="auto",
            config=Config(retries={"total_max_attempts": 1}, request_checksum_calculation="when_supported"))
        client._endpoint.http_session.send = server.send
        store = R2Store(client=client, bucket=BUCKET)
    context = {"body_bytes": 10485759} if reason == "execution_write_bytes" else {}
    folder = tmp_path / reason
    folder.mkdir()
    guard = StorageGuard(store, {QUEUE_KEY}, folder, context)
    guard.expected[QUEUE_KEY] = ("if-none-match", "*")
    payload = b"xx" if reason != "phase_write_bytes" else b"x" * 2097153
    try:
        with pytest.raises(Stop, match="storage_bytes") as caught:
            store.create_with_receipt(QUEUE_KEY, payload, "application/json")
        details = failure_details(caught.value)
        assert details["reason"] == reason
        counters = details["byte_budget"]
        assert counters["request_bytes"] == (-1 if reason == "body_length_unknown" else len(payload))
        assert counters["phase_bytes_before"] == 0
        assert counters["phase_limit_bytes"] == 2097152
        assert counters["execution_bytes_before"] == context["body_bytes"]
        assert counters["execution_limit_bytes"] == 10485760
        assert guard.counts["put"] == 0 and guard.body_bytes == 0
        assert server.puts == []
        # Only these numeric counters and a closed reason may be projected.
        caught.value.storage_byte_details["key"] = "SYNTHETIC_PRIVATE_KEY"
        assert "SYNTHETIC_PRIVATE_KEY" not in json.dumps(failure_details(caught.value))
        print(json.dumps({"rejection": reason, "details": details,
                          "physical_puts": len(server.puts)}))
    finally:
        guard.close()
        store.client.close()
