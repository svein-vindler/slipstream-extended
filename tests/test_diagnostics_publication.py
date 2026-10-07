"""Publication after read-only stages uses an existing, bounded write budget."""
import json
from types import SimpleNamespace

import pytest

from pipeline import refresh
from pipeline.r2_store import R2BudgetError, R2Store
from scripts.benchmark_health_reuse import HealthClient


@pytest.fixture
def pipeline(monkeypatch, tmp_path):
    def prepare(*, writes=True, limits=None, clock=None):
        class Client(HealthClient):
            def put_object(self, **kwargs):
                if clock is not None:
                    clock.now += 0.1
                return super().put_object(**kwargs)
        client, stores = Client(), []
        original_new_stage = R2Store.new_stage
        def new_stage(self):
            store = original_new_stage(self)
            stores.append(store)
            return store
        monkeypatch.setattr(R2Store, "new_stage", new_stage)
        data = tmp_path / "data"
        data.mkdir(exist_ok=True)

        def factory():
            store = R2Store(client=client, bucket="synthetic", **(limits or {}))
            stores.append(store)
            return store

        def summary(*args, store, **kwargs):
            (data / "health_daily.csv").write_text("Date,Weight KG\n", encoding="utf-8")
            if writes:
                store.put("synthetic/summary", b"{}", "application/json")
            return {"status": "complete"}

        def writer(key):
            def operation(*, store, **kwargs):
                if writes:
                    store.put(key, b"{}", "application/json")
                return {"status": "complete"}
            return operation

        def indexes(*, store):
            if "synthetic/summary" in client.objects:
                store.get("synthetic/summary")
            return {"status": "complete"}

        monkeypatch.setattr(refresh.summary_restore, "restore_summaries", lambda *args: None)
        monkeypatch.setattr(refresh.refresh_summaries, "run", summary)
        monkeypatch.setattr(refresh.recent_health, "run", writer("synthetic/health"))
        monkeypatch.setattr(refresh.recent_body, "run", writer("synthetic/body"))
        monkeypatch.setattr(refresh.health_index_reconcile, "run", indexes)
        return client, stores, {"data_dir": data, "store_factory": factory,
                               "login": lambda: SimpleNamespace(connectapi=lambda *args: {})}
    return prepare


def publication(output):
    return next(json.loads(line) for line in output.splitlines()
                if '"kind": "refresh-diagnostics-publication"' in line)


def test_read_only_final_stage_reuses_run_inventory_for_publication(pipeline, capsys):
    client, stores, options = pipeline()
    report = refresh.run(refresh.RefreshRequest(scheduled=True, run_id="321"), **options)
    assert client.prefixes == [""]  # One shared guarded inventory.
    assert len(client.writes) == 4  # Three data PUTs and one diagnostic PUT.
    assert stores[-1].write_budget_initialized
    assert stores[-1].operations["put"] == 1
    assert stores[-2].operations["put"] == 1
    assert json.loads(client.objects["refresh/diagnostics/v1/321.json"]["Body"]) == report
    event = publication(capsys.readouterr().out)
    assert event["status"] == "stored"
    assert event["r2_sdk_operations"]["list_pages"] == 0
    assert event["r2_sdk_operations"]["put"] == 1
    assert all(stage["r2_inventory_ms"] >= 0 for stage in report["stages"])


def test_all_read_only_stages_keep_logs_without_creating_inventory_or_put(pipeline, capsys, tmp_path):
    client, _, options = pipeline(writes=False)
    report = refresh.run(refresh.RefreshRequest(scheduled=True, run_id="322"), **options,
                         diagnostics_file=tmp_path / "report.json")
    assert client.prefixes == client.writes == []
    assert report["diagnostics_persistence_skipped"] is True
    assert json.loads((tmp_path / "report.json").read_text()) == report
    assert publication(capsys.readouterr().out)["reason"] == "no_write_inventory"


@pytest.mark.parametrize("object_limit", [100, 4])
def test_a_final_index_write_updates_the_budget_used_for_publication(pipeline, monkeypatch, object_limit):
    client, stores, options = pipeline(limits={"max_bucket_objects": object_limit})
    def write_index(*, store):
        store.put("synthetic/index", b"{}", "application/json")
        return {"status": "complete"}
    monkeypatch.setattr(refresh.health_index_reconcile, "run", write_index)
    if object_limit == 4:
        with pytest.raises(R2BudgetError):
            refresh.run(refresh.RefreshRequest(scheduled=True, run_id="326"), **options)
        assert "refresh/diagnostics/v1/326.json" not in client.objects
        assert len(client.objects) == 4
    else:
        refresh.run(refresh.RefreshRequest(scheduled=True, run_id="326"), **options)
        assert stores[-1].operations["put"] == 2
        assert stores[-2].operations["put"] == 1
    assert client.prefixes == [""]  # Final writes retain shared conservative accounting.


@pytest.mark.parametrize("limits", [
    {"max_writes_per_run": 3}, {"max_write_bytes_per_run": 6},
    {"max_bucket_objects": 3}, {"max_bucket_bytes": 6},
])
def test_publication_never_bypasses_or_switches_an_exhausted_budget(limits, capsys):
    client = HealthClient()
    store = R2Store(client=client, bucket="synthetic", **limits)
    for key in ("synthetic/summary", "synthetic/health", "synthetic/body"):
        store.put(key, b"{}", "application/json")
    with pytest.raises(R2BudgetError):
        refresh._publish_diagnostics({}, store=store, run_id="323", original_failure=None)
    assert client.prefixes == [""]
    assert client.writes == ["synthetic/summary", "synthetic/health", "synthetic/body"]
    assert publication(capsys.readouterr().out)["status"] == "failed"


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_source_failure_or_interrupt_is_not_masked_by_publication(pipeline, monkeypatch, error, capsys):
    client, _, options = pipeline(limits={"max_writes_per_run": 1})
    original = refresh.refresh_summaries.run
    def fail(*args, **kwargs):
        original(*args, **kwargs)
        raise error("synthetic-private-source-detail")
    monkeypatch.setattr(refresh.refresh_summaries, "run", fail)
    def index_write(*, store):
        store.put("synthetic/index", b"{}", "application/json")
        return {"status": "complete"}
    monkeypatch.setattr(refresh.health_index_reconcile, "run", index_write)
    with pytest.raises(error, match="synthetic-private-source-detail"):
        refresh.run(refresh.RefreshRequest(scheduled=True, run_id="324"), **options)
    output = capsys.readouterr()
    assert "synthetic-private-source-detail" not in output.out + output.err
    assert client.writes == (["synthetic/summary"] if error is KeyboardInterrupt
                             else ["synthetic/summary", "synthetic/index"])
    event = publication(output.out)
    assert event["status"] == ("skipped" if error is KeyboardInterrupt else "failed")
    if error is KeyboardInterrupt:
        assert event["reason"] == "interrupted"


def test_publication_timing_is_separate_and_needs_only_one_put(pipeline, monkeypatch, capsys):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr("pipeline.refresh.time.perf_counter", lambda: clock.now)
    client, _, options = pipeline(clock=clock)
    report = refresh.run(refresh.RefreshRequest(scheduled=True, run_id="325"), **options)
    event = publication(capsys.readouterr().out)
    assert report["elapsed_ms"] == 300
    assert event["elapsed_ms"] == 100
    assert client.writes.count("refresh/diagnostics/v1/325.json") == 1
    assert "elapsed_ms" not in event["r2_sdk_operations"]
