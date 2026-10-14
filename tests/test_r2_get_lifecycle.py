"""Legacy GET bytes/errors and SDK-body lifetime; synthetic external SDK only."""
import gzip
import io

import pytest
from botocore.exceptions import ClientError, IncompleteReadError
from botocore.response import StreamingBody

from pipeline.r2_store import R2Store


class ObservedStream(io.BytesIO):
    def __init__(self, payload, *, read_error=None, close_error=None):
        super().__init__(payload)
        self.read_error = read_error
        self.close_error = close_error
        self.reads = 0
        self.closes = 0

    def read(self, amount=None):
        self.reads += 1
        if self.read_error is not None:
            raise self.read_error
        return super().read(amount)

    def close(self):
        self.closes += 1
        super().close()
        if self.close_error is not None:
            raise self.close_error


class SingleResponseClient:
    def __init__(self, body=None, *, error=None):
        self.body = body
        self.error = error
        self.requests = []

    def get_object(self, **kwargs):
        self.requests.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"Body": self.body}


def test_get_preserves_bytes_and_closes_acquired_sdk_body():
    payload = b"legacy\x00bytes\xff"
    raw = ObservedStream(payload)
    client = SingleResponseClient(StreamingBody(raw, len(payload)))
    store = R2Store(client=client, bucket="synthetic")
    assert store.get("synthetic/object") == payload
    assert raw.closed and raw.closes == 1 and raw.reads == 1
    assert client.requests == [{"Bucket": "synthetic", "Key": "synthetic/object"}]
    assert store.operations == {"get": 1, "head": 0, "list_pages": 0, "listed_objects": 0, "put": 0}


def test_get_preserves_read_failure_when_close_also_fails():
    failure = OSError("synthetic read failure")
    cleanup = RuntimeError("synthetic cleanup failure")
    raw = ObservedStream(b"", read_error=failure, close_error=cleanup)
    store = R2Store(client=SingleResponseClient(StreamingBody(raw, 0)), bucket="synthetic")
    with pytest.raises(OSError) as caught:
        store.get("synthetic/object")
    assert caught.value is failure
    assert raw.closes == 1 and raw.reads == 1
    assert store.operations["get"] == 1


@pytest.mark.parametrize("payload", [b"", gzip.compress(b'{"legacy":0,"missing":null}', mtime=0),
                                     b"x" * (8 * 1024 * 1024 + 1)], ids=["empty", "gzip", "large"])
def test_get_keeps_existing_unbounded_byte_contract(payload):
    raw = ObservedStream(payload)
    client = SingleResponseClient(StreamingBody(raw, len(payload)))
    store = R2Store(client=client, bucket="synthetic")
    assert store.get("synthetic/object") == payload
    assert raw.closed and raw.closes == 1 and raw.reads == 1
    assert len(client.requests) == store.operations["get"] == 1
    assert sum(store.operations.values()) == 1


def test_get_closes_body_on_read_failure():
    failure = OSError("synthetic read failure")
    raw = ObservedStream(b"", read_error=failure)
    store = R2Store(client=SingleResponseClient(StreamingBody(raw, 0)), bucket="synthetic")
    with pytest.raises(OSError) as caught:
        store.get("synthetic/object")
    assert caught.value is failure and raw.closed and raw.closes == 1


def test_get_preserves_sdk_truncation_failure_and_closes_body():
    raw = ObservedStream(b"short")
    store = R2Store(client=SingleResponseClient(StreamingBody(raw, 10)), bucket="synthetic")
    with pytest.raises(IncompleteReadError):
        store.get("synthetic/object")
    assert raw.closed and raw.closes == 1


def test_get_does_not_return_success_when_close_fails():
    cleanup = RuntimeError("synthetic cleanup failure")
    raw = ObservedStream(b"data", close_error=cleanup)
    store = R2Store(client=SingleResponseClient(StreamingBody(raw, 4)), bucket="synthetic")
    with pytest.raises(RuntimeError) as caught:
        store.get("synthetic/object")
    assert caught.value is cleanup and raw.closes == 1


@pytest.mark.parametrize("code", ["NoSuchKey", "AccessDenied", "InternalError"])
def test_get_preserves_sdk_failure_before_body_acquisition(code):
    failure = ClientError({"Error": {"Code": code}}, "GetObject")
    client = SingleResponseClient(error=failure)
    store = R2Store(client=client, bucket="synthetic")
    with pytest.raises(ClientError) as caught:
        store.get("synthetic/object")
    assert caught.value is failure
    assert len(client.requests) == store.operations["get"] == 1
    assert sum(store.operations.values()) == 1


def test_targeted_coach_preserves_gzip_sources_and_closes_acquired_bodies():
    from pipeline import coach_backfill
    from scripts.build_coach_versions_contract import ACTIVITY, PREFIX, MemoryS3

    class ObservedCoachClient(MemoryS3):
        def __init__(self):
            super().__init__()
            self.bodies = []

        def get_object(self, **kwargs):
            response = super().get_object(**kwargs)
            raw = response["Body"]
            self.bodies.append(raw)
            response["Body"] = StreamingBody(raw, response["ContentLength"])
            return response

    client = ObservedCoachClient()
    key = f"{PREFIX}/activity.endurance.v1.json"
    client.objects[key] = gzip.compress(client.objects[key], mtime=0)
    source = client.objects.copy()
    store = client.store()
    result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    assert result["processed_sources"]["1"]
    assert all(client.objects[key] == value for key, value in source.items())
    assert client.bodies and all(body.closed for body in client.bodies)
