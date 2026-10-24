"""Installed S3 SDK through public storage contracts, synthetic HTTP only."""
import io
import os
import socket
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import boto3
import botocore.auth
import botocore.credentials
import pytest
from botocore.config import Config
from botocore.exceptions import IncompleteReadError
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.response import HTTPResponse

from pipeline.backup_source import R2ReadOnlySource, Scope
from pipeline.r2_store import R2BudgetError, R2Store

ACCOUNT = "a" * 32
HOST = ACCOUNT + ".r2.cloudflarestorage.com"


@pytest.fixture
def sdk_storage(monkeypatch, tmp_path):
    # Explicit invented credentials and isolated config, never ambient discovery.
    for name in tuple(os.environ):
        if name.startswith("AWS_") or name.lower() in {
            "http_proxy", "https_proxy", "all_proxy", "no_proxy",
        }:
            monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-credentials"))
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", ACCOUNT)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "synthetic-access-key")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "synthetic-secret-key")
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("External discovery or live socket attempted")

    monkeypatch.setattr(botocore.credentials.CredentialResolver, "load_credentials", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(botocore.auth, "get_current_datetime",
                        lambda: datetime(2026, 10, 24, 12, tzinfo=timezone.utc))
    replies, requests, bodies = [], [], []

    def respond(connection, method, url, **kwargs):
        assert connection.host == HOST and connection.scheme == "https"
        requests.append((method, url, {
            key.lower(): value.decode() if isinstance(value, bytes) else value
            for key, value in kwargs["headers"].items()
        }))
        assert replies, "Unexpected extra HTTP request"
        status, headers, payload = replies.pop(0)
        raw = HTTPResponse(
            body=io.BytesIO(payload), status=status, headers=headers,
            preload_content=False, decode_content=False,
            enforce_content_length=kwargs.get("enforce_content_length", True),
        )
        bodies.append(raw)
        return raw

    # Replace external I/O below real client/model/signing/session behavior.
    monkeypatch.setattr(HTTPSConnectionPool, "urlopen", respond)
    storage = R2Store(bucket="synthetic-bucket", max_writes_per_run=1)
    try:
        yield storage, replies, requests, bodies
    finally:
        storage.client.close()


def test_installed_sdk_preserves_snapshot_bytes_revision_and_fixed_signed_endpoint(sdk_storage):
    storage, replies, requests, bodies = sdk_storage
    replies.append((200, {
        "content-length": "4", "etag": '"opaque-revision"',
        "content-type": "application/json",
    }, b"data"))
    snapshot = storage.get_snapshot("synthetic/object", max_bytes=16)
    assert (snapshot.data, snapshot.revision, snapshot.content_type) == (
        b"data", '"opaque-revision"', "application/json",
    )
    assert len(requests) == 1 and not replies and bodies[0].closed
    method, url, headers = requests[0]
    assert (method, url) == ("GET", "/synthetic-bucket/synthetic/object")
    assert headers["x-amz-date"] == "20261024T120000Z"
    assert headers["authorization"].startswith(
        "AWS4-HMAC-SHA256 Credential=synthetic-access-key/20261024/auto/s3/aws4_request,"
    )


def test_installed_sdk_bounded_read_rejects_content_length_truncation_without_retry(sdk_storage):
    """Content-Length mismatch, not chunked-transfer framing or upstream-fix proof."""
    storage, replies, requests, bodies = sdk_storage
    replies.append((200, {"content-length": "10"}, b"short"))
    with pytest.raises(IncompleteReadError):
        storage.get_bounded("synthetic/object", 16)
    assert len(requests) == 1 and not replies and bodies[0].closed
    assert storage.operations == {
        "get": 1, "head": 0, "list_pages": 0, "listed_objects": 0, "put": 0,
    }


def test_installed_sdk_create_only_write_keeps_signed_precondition_and_receipt(sdk_storage):
    storage, replies, requests, _bodies = sdk_storage
    inventory = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    replies.extend([
        (200, {"content-length": str(len(inventory))}, inventory),
        (200, {"content-length": "0", "etag": '"opaque-ack"'}, b""),
    ])
    receipt = storage.create_with_receipt("synthetic/object", b"new", "application/json")
    assert receipt.accepted and receipt.revision == '"opaque-ack"'
    assert len(requests) == 2 and not replies
    method, url, headers = requests[1]
    assert (method, url) == ("PUT", "/synthetic-bucket/synthetic/object")
    assert headers["if-none-match"] == "*"
    assert "if-none-match;" in headers["authorization"]


def test_installed_sdk_stale_conditional_write_is_rejected_but_remains_charged(sdk_storage):
    storage, replies, requests, _bodies = sdk_storage
    inventory = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
    failure = b"<Error><Code>PreconditionFailed</Code></Error>"
    replies.extend([
        (200, {"content-length": str(len(inventory))}, inventory),
        (412, {"content-length": str(len(failure))}, failure),
    ])
    receipt = storage.put_if_revision(
        "synthetic/object", b"new", "application/json", revision='"stale-revision"',
    )
    assert not receipt.accepted and receipt.revision is None
    assert len(requests) == 2 and not replies
    assert requests[1][2]["if-match"] == '"stale-revision"'
    assert "if-match;" in requests[1][2]["authorization"]
    with pytest.raises(R2BudgetError):
        storage.put("synthetic/object", b"more", "application/json")
    assert len(requests) == 2 and storage.operations["put"] == 1


def test_installed_sdk_backup_source_keeps_bounded_listing_and_conditional_read(sdk_storage):
    _storage, replies, requests, bodies = sdk_storage
    key = "coach/profiles/v1/2026-10-24/synthetic.json"
    inventory = (
        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents>"
        f"<Key>{key}</Key><ETag>&quot;opaque-revision&quot;</ETag><Size>4</Size>"
        "</Contents></ListBucketResult>"
    ).encode()
    replies.extend([
        (200, {"content-length": str(len(inventory))}, inventory),
        (200, {"content-length": "4", "etag": '"opaque-revision"'}, b"data"),
    ])
    client = boto3.client(
        "s3", endpoint_url=f"https://{HOST}", region_name="auto",
        aws_access_key_id="synthetic-access-key", aws_secret_access_key="synthetic-secret-key",
        config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=30,
                      retries={"total_max_attempts": 1, "mode": "standard"},
                      s3={"addressing_style": "path"}),
    )
    try:
        source = R2ReadOnlySource(client, "synthetic-bucket")
        versions = list(source.inventory(Scope(())))
        assert len(versions) == 1
        assert source.read(versions[0], 16) == b"data"
        assert source.operations == {"get": 1, "list_pages": 1, "listed_objects": 1, "put": 0}
    finally:
        client.close()
    assert len(requests) == 2 and not replies and bodies[1].closed
    query = parse_qs(urlsplit(requests[0][1]).query)
    assert query["prefix"] == ["coach/profiles/v1/"] and query["max-keys"] == ["1000"]
    assert requests[1][0:2] == ("GET", "/synthetic-bucket/" + key)
    assert requests[1][2]["if-match"] == '"opaque-revision"'
    assert "if-match;" in requests[1][2]["authorization"]
