"""Actual R2Store snapshot/conditional interface with synthetic SDK responses."""
import io

import pytest
from botocore.exceptions import ClientError

from pipeline.r2_store import R2BudgetError, R2Store
from scripts.benchmark_summary_reuse import SummaryClient


class RevisionClient(SummaryClient):
    def __init__(self):
        super().__init__()
        self.revision = '"opaque-sdk-revision"'
        self.requests = []

    def get_object(self, **kwargs):
        self.counts["get"] += 1
        return {"Body": io.BytesIO(b"synthetic body"), "ETag": self.revision,
                "ContentType": "application/json", "ContentLength": 14}

    def put_object(self, **kwargs):
        self.requests.append(kwargs.copy())
        if kwargs.get("IfMatch") != self.revision:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.counts["put"] += 1
        self.objects[kwargs["Key"]] = kwargs.copy()
        self.revision = '"opaque-sdk-ack"'
        return {"ETag": self.revision}


def test_snapshot_revision_and_bytes_are_from_one_sdk_response():
    client = RevisionClient()
    storage = R2Store(client=client, bucket="synthetic")
    snapshot = storage.get_snapshot("synthetic/manifest", max_bytes=64)
    assert (snapshot.data, snapshot.revision) == (b"synthetic body", '"opaque-sdk-revision"')
    assert client.counts["get"] == 1


def test_conditional_ack_preserves_opaque_revision():
    client = RevisionClient()
    storage = R2Store(client=client, bucket="synthetic")
    response = storage.put_if_revision("synthetic/manifest", b"new", "application/json",
                                       revision='"opaque-sdk-revision"')
    assert response.accepted and response.revision == '"opaque-sdk-ack"'
    assert client.requests[0]["IfMatch"] == '"opaque-sdk-revision"'
    assert storage.operations["put"] == 1


def test_conditional_reject_remains_charged_to_guarded_attempts():
    client = RevisionClient()
    storage = R2Store(client=client, bucket="synthetic", max_writes_per_run=1)
    response = storage.put_if_revision("synthetic/manifest", b"new", "application/json",
                                       revision='"stale-revision"')
    assert not response.accepted
    with pytest.raises(R2BudgetError):
        storage.put_if_revision("synthetic/manifest", b"new", "application/json",
                                revision='"opaque-sdk-revision"')
    assert len(client.requests) == 1
