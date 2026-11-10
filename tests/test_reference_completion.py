"""Actual bounded SDK/Meter reads on synthetic external HTTP, never live R2."""
from datetime import UTC, date, datetime, timedelta

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import Raw
from test_history_baseline_integration import BaselineServer, sdk_store

from pipeline.history_state import Stop, digest, encoded
from pipeline.night_products import normalize_args, storage_key
from pipeline.reference_bulk_budget import BulkAccount, make_plan


def account_and_keys(tmp_path, count):
    deadline = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    account = BulkAccount.open(tmp_path / "account", "a" * 64, {}, deadline_utc=deadline)
    seal = tmp_path / "build-seal.json"
    seal.write_bytes(encoded({"synthetic": True}))
    reference = {"path": str(seal.resolve()), "bytes": seal.stat().st_size,
                 "sha256": digest(seal.read_bytes())}
    plan = make_plan(tmp_path / "plans", phase="publication", workset_sha256="a" * 64,
        target_count=count, inventory_requests=0, source_count=0, source_bytes=0,
        product_count=count, product_bytes=0, build_seal=reference, carried={},
        old_product_bytes=0, fence_list_pages=0)
    account.activate(plan)
    keys = [storage_key(normalize_args(end_date=(date(2026, 1, 1) + timedelta(days=index)).isoformat(),
                period="28", channel="heart_rate")) for index in range(count)]
    return account, keys, plan


def test_current_destination_reads_cross_four_hundred_without_refilling_meter(tmp_path):
    from pipeline.reference_completion import read_destination_batches

    account, keys, plan = account_and_keys(tmp_path, 408)
    before = account.snapshot()
    server = BaselineServer()
    store = sdk_store(server)
    out = tmp_path / "reads"
    out.mkdir()
    try:
        rows = [row for batch in read_destination_batches(store, account, out, keys) for row in batch]
    finally:
        store.client.close()
    assert rows == [(key, None) for key in keys]
    assert len(server.calls) == 408 and all(row[0] == "GET" for row in server.calls)
    after = account.snapshot()
    assert after["usage"] == {"get": 408, "list": 0, "put": 0, "body_bytes": 0, "independent_gets": 0}
    assert after["context"]["bulk_stages"] == 13
    assert after["active_plan"] == plan and after["plans"] == before["plans"]
    assert after["deadline_utc"] == before["deadline_utc"] and after["carried"] == before["carried"]
    assert len(list(out.glob("reads-*/wire-*.json"))) == 408


def test_completion_cannot_create_a_replacement_account(tmp_path):
    folder = tmp_path / "nonexistent-account"
    with pytest.raises(Stop, match="bulk_completion_account_missing"):
        BulkAccount.open(folder, "a" * 64, {},
            deadline_utc=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            completion_ref={"unqualified": "synthetic"})
    assert not (folder / "ACCOUNT.json").exists()


@pytest.mark.parametrize(("status", "code"), [(403, "NoSuchKey"), (404, "AccessDenied"), (503, "NoSuchKey")])
def test_auth_or_unknown_http_is_not_absence_and_stops_first_attempt(tmp_path, status, code):
    from pipeline.reference_completion import read_destination_batches

    account, keys, _ = account_and_keys(tmp_path, 2)

    class Denied(BaselineServer):
        def send(self, request):
            self.calls.append((request.method, "synthetic-redacted-key"))
            body = ("<Error><Code>" + code + "</Code></Error>").encode()
            return AWSResponse(request.url, status, {"content-length": str(len(body))}, Raw(body))

    server = Denied()
    store = sdk_store(server)
    out = tmp_path / "reads"
    out.mkdir()
    try:
        with pytest.raises(Stop, match="storage_denied_or_unknown"):
            list(read_destination_batches(store, account, out, keys))
    finally:
        store.client.close()
    assert len(server.calls) == 1 and account.snapshot()["usage"]["get"] == 1
    assert account.snapshot()["usage"]["put"] == 0
    assert account.state["failed"] == "unknown_storage_transport"
