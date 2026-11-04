"""Bounded storage reads through the installed SDK and synthetic HTTP transport."""

import io

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config

from pipeline.r2_store import R2ReadBoundaryError, R2Store


class FragmentedRaw(io.BytesIO):
    def __init__(self, payload, *, close_error=None):
        super().__init__(payload)
        self.close_error = close_error
        self.eof_reads = 0
        self.closes = 0

    def read(self, amount=None):
        chunk = super().read(min(amount, 2) if amount is not None else amount)
        self.eof_reads += not chunk
        return chunk

    def close(self):
        self.closes += 1
        super().close()
        if self.close_error:
            raise self.close_error


def sdk_store(raw, *, length=3, encoding=None):
    client = boto3.client(
        "s3", endpoint_url="https://synthetic.r2.cloudflarestorage.com",
        aws_access_key_id="synthetic", aws_secret_access_key="synthetic",
        region_name="auto", config=Config(retries={"total_max_attempts": 1}),
    )
    headers = {"content-length": str(length), "etag": '"synthetic"',
               "content-type": "application/json"}
    if encoding:
        headers["content-encoding"] = encoding
    client._endpoint.http_session.send = lambda request: AWSResponse(
        request.url, 200, headers, raw
    )
    return R2Store(client=client, bucket="synthetic")


def test_bounded_snapshot_reads_fragments_through_real_sdk_until_eof():
    raw = FragmentedRaw(b"abc")
    store = sdk_store(raw)
    snapshot = store.get_snapshot("synthetic.json", max_bytes=3)
    assert (snapshot.data, snapshot.revision, snapshot.content_type) == (
        b"abc", '"synthetic"', "application/json"
    )
    assert raw.eof_reads == 1 and raw.closes == 1
    assert store.operations["get"] == 1 and store.bounded_read_bytes == 3


@pytest.mark.parametrize("reader", ["snapshot", "bounded"])
def test_bounded_sdk_rejection_preserves_primary_when_body_close_also_fails(reader):
    raw = FragmentedRaw(b"abc", close_error=RuntimeError("synthetic cleanup"))
    store = sdk_store(raw, length=3)
    with pytest.raises(R2ReadBoundaryError) as caught:
        if reader == "snapshot":
            store.get_snapshot("synthetic.json", max_bytes=2)
        else:
            store.get_bounded("synthetic.json", 2)
    assert caught.value.status == "oversize" and raw.closes == 1
    assert store.operations["get"] == 1


@pytest.mark.parametrize("reader", ["snapshot", "bounded"])
def test_successful_bounded_sdk_read_still_reports_cleanup_failure(reader):
    raw = FragmentedRaw(b"abc", close_error=RuntimeError("synthetic cleanup"))
    store = sdk_store(raw)
    with pytest.raises(RuntimeError, match="synthetic cleanup"):
        if reader == "snapshot":
            store.get_snapshot("synthetic.json", max_bytes=3)
        else:
            store.get_bounded("synthetic.json", 3)
    assert raw.closes == 1


@pytest.mark.parametrize("reader", ["snapshot", "bounded"])
def test_unrelated_caller_exception_cannot_hide_successful_read_cleanup_failure(reader):
    raw = FragmentedRaw(b"abc", close_error=RuntimeError("synthetic cleanup"))
    store = sdk_store(raw)
    try:
        raise ValueError("unrelated caller context")
    except ValueError:
        with pytest.raises(RuntimeError, match="synthetic cleanup"):
            if reader == "snapshot":
                store.get_snapshot("synthetic.json", max_bytes=3)
            else:
                store.get_bounded("synthetic.json", 3)
    assert raw.closes == 1
