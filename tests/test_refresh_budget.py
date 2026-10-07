"""Run-scoped inventory, separate stage limits, and uncertain writes."""
from types import SimpleNamespace

import pytest

from pipeline import refresh
from pipeline.r2_store import R2BudgetError, R2Store
from scripts.benchmark_refresh_budget import BudgetClient, compare


def test_paginated_refresh_guard_reduces_lists_without_changing_writes():
    result = compare(2001)
    assert result["outputs_identical"] and result["provider_calls"] == 0
    assert result["separate"]["list"] == 12
    assert result["shared"]["list"] == 3
    assert result["shared"]["put"] == result["separate"]["put"] == 4
    assert result["shared"]["upload_bytes"] == result["separate"]["upload_bytes"]


@pytest.mark.parametrize("limits,match", [
    ({"max_writes_per_run": 1}, "write limit"),
    ({"max_write_bytes_per_run": 3}, "per-run byte limit"),
])
def test_new_stage_preserves_limits_but_resets_stage_allowance(limits, match):
    client = BudgetClient(0)
    first = R2Store(client=client, bucket="synthetic", **limits)
    first.put("first", b"123", "text/plain")
    with pytest.raises(R2BudgetError, match=match):
        first.put("blocked", b"1", "text/plain")
    second = first.new_stage()
    assert second.client is first.client and second.bucket == first.bucket
    assert all(value == 0 for value in second.operations.values())
    second.put("second", b"123", "text/plain")
    with pytest.raises(R2BudgetError, match=match):
        second.put("blocked", b"1", "text/plain")
    assert client.counts["put"] == 2 and client.counts["list"] == 1


@pytest.mark.parametrize("limits,match", [
    ({"max_bucket_objects": 2}, "object safety limit"),
    ({"max_bucket_bytes": 6}, "storage safety limit"),
])
def test_cross_stage_growth_cannot_reset_bucket_limits_even_for_overwrites(limits, match):
    client = BudgetClient(0)
    store = R2Store(client=client, bucket="synthetic", **limits)
    for _ in range(2):
        store.put("same-key", b"123", "text/plain")
        store = store.new_stage()
    with pytest.raises(R2BudgetError, match=match):
        store.put("same-key", b"1", "text/plain")
    assert client.counts["list"] == 1 and client.counts["put"] == 2


def test_identical_objects_and_read_only_stages_do_not_start_inventory():
    client = BudgetClient(0)
    client.put_object(Key="same", Body=b"unchanged", ContentType="text/plain")
    client.counts = dict.fromkeys(client.counts, 0)
    first = R2Store(client=client, bucket="synthetic")
    assert first.get("same") == b"unchanged"
    second = first.new_stage()
    assert not second.put_if_changed("same", b"unchanged", "text/plain")
    assert client.counts["list"] == client.counts["put"] == 0
    assert second._initial_inventory is None


def test_new_run_inventories_again_and_observes_external_growth():
    client = BudgetClient(0)
    R2Store(client=client, bucket="synthetic", max_bucket_bytes=10).put("one", b"123", "text/plain")
    client.put_object(Key="external", Body=b"123456", ContentType="text/plain")
    store = R2Store(client=client, bucket="synthetic", max_bucket_bytes=10)
    with pytest.raises(R2BudgetError, match="storage safety limit"):
        store.put("blocked", b"12", "text/plain")
    assert client.counts["list"] == 2
    assert "blocked" not in client.objects


def test_partial_inventory_failure_is_not_cached_or_allowed_to_write(monkeypatch):
    client = BudgetClient(2001)
    original = client.get_paginator
    def failed(name):
        def pages(**kwargs):
            yield {"Contents": [{"Size": 1}]}
            raise RuntimeError("synthetic inventory failure")
        return SimpleNamespace(paginate=pages)
    monkeypatch.setattr(client, "get_paginator", failed)
    first = R2Store(client=client, bucket="synthetic")
    with pytest.raises(RuntimeError, match="inventory failure"):
        first.put("blocked", b"1", "text/plain")
    assert first._initial_inventory is None and client.counts["put"] == 0
    monkeypatch.setattr(client, "get_paginator", original)
    first.new_stage().put("allowed", b"1", "text/plain")
    assert client.counts["list"] == 3


@pytest.mark.parametrize("limits,match", [
    ({"max_bucket_objects": 1}, "object safety limit"),
    ({"max_bucket_bytes": 3}, "storage safety limit"),
])
def test_ambiguous_failed_put_stays_charged_across_stages(monkeypatch, limits, match):
    client = BudgetClient(0)
    original = client.put_object
    def committed_then_timeout(**kwargs):
        original(**kwargs)
        raise TimeoutError("synthetic response lost after write")
    monkeypatch.setattr(client, "put_object", committed_then_timeout)
    first = R2Store(client=client, bucket="synthetic", **limits)
    with pytest.raises(TimeoutError):
        first.put("committed", b"123", "text/plain")
    monkeypatch.setattr(client, "put_object", original)
    with pytest.raises(R2BudgetError, match=match):
        first.new_stage().put("blocked", b"1", "text/plain")
    assert client.counts["put"] == 1 and "blocked" not in client.objects


def test_failed_put_consumes_stage_attempt_budget(monkeypatch):
    client = BudgetClient(0)
    def fail(**kwargs):
        raise TimeoutError("synthetic uncertain write")
    monkeypatch.setattr(client, "put_object", fail)
    store = R2Store(client=client, bucket="synthetic", max_writes_per_run=1)
    with pytest.raises(TimeoutError):
        store.put("uncertain", b"1", "text/plain")
    with pytest.raises(R2BudgetError, match="write limit"):
        store.put("retry", b"1", "text/plain")
    assert store.operations["put"] == 1


def test_orchestrator_shares_inventory_through_read_only_final_stage_and_diagnostics(monkeypatch, tmp_path):
    client = BudgetClient(2001)
    stores = []
    factories = []
    def factory():
        factories.append(True)
        return R2Store(client=client, bucket="synthetic", max_writes_per_run=1)
    def write(*args, store, **kwargs):
        stores.append(store)
        store.put(f"synthetic/{len(stores)}", b"changed", "text/plain")
        return {"status": "complete"}
    def indexes(*, store):
        stores.append(store)
        return {"status": "complete"}
    monkeypatch.setattr(refresh.summary_restore, "restore_summaries", lambda *args: None)
    for module in (refresh.refresh_summaries, refresh.recent_health, refresh.recent_body):
        monkeypatch.setattr(module, "run", write)
    monkeypatch.setattr(refresh.health_index_reconcile, "run", indexes)
    root = tmp_path / "data"
    root.mkdir()
    (root / "health_daily.csv").write_text("Date,Weight KG\n", encoding="utf-8")
    report = refresh.run(refresh.RefreshRequest(scheduled=True, run_id="123"), data_dir=root,
                         store_factory=factory, login=lambda: SimpleNamespace())
    assert factories == [True]
    assert len({id(store) for store in stores}) == 4
    assert report["status"] == "complete"
    assert client.counts["list"] == 3 and client.counts["put"] == 4
    assert json_report(client, "123") == report
    assert sum(stage["r2_sdk_operations"]["list_pages"] for stage in report["stages"]) == 3
    assert sum(stage["r2_sdk_operations"]["put"] for stage in report["stages"]) == 3


def json_report(client, run_id):
    import json
    return json.loads(client.objects[f"refresh/diagnostics/v1/{run_id}.json"]["Body"])
