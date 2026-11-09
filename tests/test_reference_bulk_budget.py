"""A finite bulk allowance is separate from, and never resets, carried usage."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.history_state import Stop
from pipeline.reference_bulk_budget import BulkAccount, make_plan


def test_resource_artifacts_and_presend_checkpoint_are_fsynced(tmp_path, monkeypatch):
    from pipeline import reference_bulk_budget as budget

    events = []
    original_fsync, original_replace = budget.os.fsync, budget.os.replace

    def fsync(descriptor):
        original_fsync(descriptor)
        events.append("fsync")

    def replace(source, target):
        assert events[-1] == "fsync"
        original_replace(source, target)
        events.append("replace")

    monkeypatch.setattr(budget.os, "fsync", fsync)
    monkeypatch.setattr(budget.os, "replace", replace)
    carried = {"independent_lifetime_gets": 680}
    plan = make_plan(tmp_path / "plans", phase="collection", workset_sha256="a" * 64,
        target_count=6, inventory_requests=1, source_count=1, source_bytes=2, carried=carried)
    assert events == ["fsync"]
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    assert events == ["fsync", "fsync"]
    account.activate(plan)
    account.execution_context["counts"]["get"] += 1
    account.start_wire("get")
    # A new process must see the charged intent before transport can begin.
    assert events[-2:] == ["fsync", "replace"]
    with pytest.raises(Stop, match="held"):
        BulkAccount.open(tmp_path / "account", "a" * 64, carried)
    account.finish_wire()
    assert events == ["fsync", "fsync"] + ["fsync", "replace"] * 3


def allowance(tmp_path, *, phase="publication", count=300):
    return make_plan(tmp_path / "plans", phase=phase, workset_sha256="a" * 64,
        target_count=count, inventory_requests=4, source_count=5, source_bytes=2048,
        product_count=count if phase == "publication" else 0,
        product_bytes=count * 100 if phase == "publication" else 0,
        build_seal={"path": "synthetic-seal", "bytes": 1, "sha256": "b" * 64}
        if phase == "publication" else None,
        carried={"independent_lifetime_gets": 680, "source_reload": 30, "source_http": 240,
                 "source_watermark": {"get": 1}, "shared_span_usage": {"get": 2},
                 "prefix_pins": [{"bytes": 1, "sha256": "c" * 64}]}, queue_bytes=1024)


def test_finite_whole_plan_keeps_carried_counts_and_crosses_legacy_256(tmp_path):
    deadline = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    carried = {"independent_lifetime_gets": 680, "source_reload": 30, "source_http": 240,
               "source_watermark": {"get": 1}, "shared_span_usage": {"get": 2},
               "prefix_pins": [{"bytes": 1, "sha256": "c" * 64}]}
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried, deadline_utc=deadline)
    plan = allowance(tmp_path)
    account.activate(plan)
    for ordinal in range(260):
        account.charge_independent("sdk", format(ordinal, "064x"))
    assert account.snapshot()["independent_lifetime_gets"] == 940
    reopened = BulkAccount.open(tmp_path / "account", "a" * 64, carried)
    assert reopened.snapshot()["independent_lifetime_gets"] == 940
    assert reopened.snapshot()["carried"] == carried
    assert reopened.snapshot()["deadline_utc"] == deadline
    with pytest.raises(Stop, match="deadline"):
        BulkAccount.open(tmp_path / "account", "a" * 64, carried,
            deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=2)).isoformat())


def test_collection_has_no_product_write_allowance_and_no_same_phase_refill(tmp_path):
    carried = {"independent_lifetime_gets": 680, "source_reload": 30, "source_http": 240,
               "source_watermark": {"get": 1}, "shared_span_usage": {"get": 2},
               "prefix_pins": [{"bytes": 1, "sha256": "c" * 64}]}
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    plan = allowance(tmp_path, phase="collection")
    account.activate(plan)
    with pytest.raises(Stop, match="budget"):
        account.admit("put", byte_count=1)
    with pytest.raises(Stop, match="plan"):
        account.activate(allowance(tmp_path, phase="collection", count=301))
    assert account.snapshot()["usage"]["put"] == 0


def test_expired_account_and_unknown_failure_never_gain_new_time(tmp_path):
    carried = {"independent_lifetime_gets": 680}
    with pytest.raises(Stop, match="deadline"):
        BulkAccount.open(tmp_path / "expired", "a" * 64, carried,
            deadline_utc=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat())
    account = BulkAccount.open(tmp_path / "held", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    account.mark_failed("unknown_storage_transport")
    with pytest.raises(Stop, match="held"):
        BulkAccount.open(tmp_path / "held", "a" * 64, carried)


def test_bulk_guard_charges_actual_sdk_and_preserves_legacy_context(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import RevisionServer

    from pipeline.history_publish import StorageGuard

    key = "health/hrv/2026/10/2026-10-25.json"
    server = RevisionServer()
    server.objects[key] = (b"{}", '"opaque"')
    server.metadata[key] = {"content-type": "application/json"}
    store = sdk_store(server)
    carried = {"independent_lifetime_gets": 680}
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    plan = make_plan(tmp_path / "plans", phase="collection", workset_sha256="a" * 64,
        target_count=6, inventory_requests=1, source_count=1, source_bytes=2, carried=carried)
    account.activate(plan)
    evidence = tmp_path / "wire"
    evidence.mkdir()
    guard = StorageGuard(store, {key}, evidence, account.execution_context, bulk_account=account)
    try:
        assert store.get_snapshot(key, max_bytes=16).data == b"{}"
    finally:
        guard.close()
        store.client.close()
    result = account.snapshot()
    assert result["usage"]["get"] == 1
    assert result["pending_wire"] is None
    assert result["independent_lifetime_gets"] == 680
    assert len(server.calls) == 1



def test_forged_resource_caps_are_rejected_before_activation(tmp_path):
    import json

    from pipeline.history_state import digest, encoded

    carried = {"independent_lifetime_gets": 680, "source_reload": 30, "source_http": 240,
               "source_watermark": {"get": 1}, "shared_span_usage": {"get": 2},
               "prefix_pins": [{"bytes": 1, "sha256": "c" * 64}]}
    reference = allowance(tmp_path)
    value = json.loads(Path(reference["path"]).read_bytes())
    value["limits"]["class_a"] = 10**15
    body = encoded(value)
    path = tmp_path / "forged.json"
    path.write_bytes(body)
    forged = {"path": str(path), "bytes": len(body), "sha256": digest(body)}
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    with pytest.raises(Stop, match="plan"):
        account.activate(forged)
    assert account.snapshot()["active_plan"] is None


def test_unknown_physical_send_is_charged_once_and_reopen_stays_held(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import RevisionServer

    from pipeline.history_publish import StorageGuard

    server = RevisionServer()
    store = sdk_store(server)
    sends = []

    def lost(request):
        sends.append(request.method)
        raise TimeoutError("synthetic transport marker")

    store.client._endpoint.http_session.send = lost
    carried = {"independent_lifetime_gets": 680}
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    account.activate(make_plan(tmp_path / "plans", phase="collection", workset_sha256="a" * 64,
        target_count=6, inventory_requests=1, source_count=2, source_bytes=4, carried=carried))
    key = "health/hrv/2026/10/2026-10-25.json"
    evidence = tmp_path / "wire"
    evidence.mkdir()
    guard = StorageGuard(store, {key}, evidence, account.execution_context, bulk_account=account)
    try:
        with pytest.raises(Stop, match="unknown_storage_transport"):
            store.get_snapshot(key, max_bytes=16)
        with pytest.raises(Stop):
            store.get_snapshot(key, max_bytes=16)
    finally:
        guard.close()
        store.client.close()
    assert sends == ["GET"]
    state = account.snapshot()
    assert state["usage"]["get"] == 1 and state["pending_wire"] is not None
    with pytest.raises(Stop, match="held"):
        BulkAccount.open(tmp_path / "account", "a" * 64, carried)


def test_later_stage_uses_original_deadline_before_any_send(tmp_path, monkeypatch):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import RevisionServer

    from pipeline import reference_bulk_budget as budget
    from pipeline.history_publish import StorageGuard

    carried = {"independent_lifetime_gets": 680}
    now = datetime.now(timezone.utc)
    deadline = (now + timedelta(hours=1)).isoformat()
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried, deadline_utc=deadline)
    account.activate(make_plan(tmp_path / "plans", phase="collection", workset_sha256="a" * 64,
        target_count=6, inventory_requests=1, source_count=1, source_bytes=2, carried=carried))
    server = RevisionServer()
    store = sdk_store(server)
    evidence = tmp_path / "wire"
    evidence.mkdir()
    guard = StorageGuard(store, {"health/hrv/2026/10/2026-10-25.json"}, evidence,
        account.execution_context, bulk_account=account)

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return now + timedelta(hours=2)

    monkeypatch.setattr(budget, "datetime", Later)
    try:
        with pytest.raises(Stop, match="deadline"):
            store.get_snapshot("health/hrv/2026/10/2026-10-25.json", max_bytes=16)
    finally:
        guard.close()
        store.client.close()
    assert server.calls == [] and account.snapshot()["deadline_utc"] == deadline
    assert account.snapshot()["usage"]["get"] == 0


def test_calendar_keeps_source_caps_and_original_bulk_deadline_without_double_charge(tmp_path, monkeypatch):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import RevisionServer

    from pipeline import reference_bulk_budget as budget
    from pipeline.history_publish import StorageGuard

    carried = {"independent_lifetime_gets": 680}
    now = datetime.now(timezone.utc)
    account = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(now + timedelta(hours=1)).isoformat())
    account.activate(make_plan(tmp_path / "plans", phase="collection", workset_sha256="a" * 64,
        target_count=6, inventory_requests=1, source_count=1, source_bytes=2, carried=carried))
    before = account.snapshot()
    key = "health/hrv/2026/10/2026-10-25.json"
    server = RevisionServer()
    server.objects[key] = (b"{}", '"opaque"')
    server.metadata[key] = {"content-type": "application/json"}
    store = sdk_store(server)
    evidence = tmp_path / "wire"
    evidence.mkdir()
    source_context = {}
    guard = StorageGuard(store, {key}, evidence, source_context, deadline_account=account)

    class Later(datetime):
        @classmethod
        def now(cls, tz=None):
            return now + timedelta(hours=2)

    try:
        assert store.get_snapshot(key, max_bytes=16).data == b"{}"
        assert guard.total_get_limit == 6256 and guard.class_a_limit == 1000
        monkeypatch.setattr(budget, "datetime", Later)
        with pytest.raises(Stop, match="deadline"):
            store.get_snapshot(key, max_bytes=16)
    finally:
        guard.close()
        store.client.close()
    assert len(server.calls) == 1 and source_context["counts"]["get"] == 1
    assert account.snapshot() == before


def test_concurrent_account_copy_cannot_overwrite_durable_usage(tmp_path):
    import json

    carried = {"independent_lifetime_gets": 680, "source_reload": 30, "source_http": 240,
               "source_watermark": {"get": 1}, "shared_span_usage": {"get": 2},
               "prefix_pins": [{"bytes": 1, "sha256": "c" * 64}]}
    first = BulkAccount.open(tmp_path / "account", "a" * 64, carried,
        deadline_utc=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    first.activate(allowance(tmp_path))
    other = BulkAccount.open(tmp_path / "account", "a" * 64, carried)
    first.charge_independent("sdk", "d" * 64)
    before = (tmp_path / "account/ACCOUNT.json").read_bytes()
    with pytest.raises(Stop, match="account_changed"):
        other.charge_independent("sdk", "e" * 64)
    assert (tmp_path / "account/ACCOUNT.json").read_bytes() == before
    assert json.loads(before)["context"]["independent_gets"] == 1


def test_legacy_guard_still_rejects_unqualified_257_pool(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import RevisionServer

    from pipeline.history_publish import StorageGuard

    server = RevisionServer()
    store = sdk_store(server)
    try:
        with pytest.raises(Stop, match="storage_execution_budget"):
            StorageGuard(store, set(), tmp_path, {"independent_gets": 257})
    finally:
        store.client.close()
    assert server.calls == []


def test_plan_prices_small_technical_batches_and_separates_retained_growth(tmp_path):
    import json

    reference = allowance(tmp_path)
    value = json.loads(Path(reference["path"]).read_bytes())
    basis, limits = value["basis"], value["limits"]
    # A large body may force one-product batches. Scope-wide allowance must not
    # assume every batch is full simply because eight is the technical maximum.
    assert limits["class_a"] >= 3 * basis["inventory_requests"] * basis["product_count"] + 16 * basis["product_count"]
    assert limits["retained_growth_bytes"] < limits["write_bytes"]
    assert limits["retained_object_growth"] >= 3 * basis["product_count"]
