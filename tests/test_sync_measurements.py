"""Synthetic clocks and transports; no private data or external access."""
import io
import json
from datetime import date
from types import SimpleNamespace

import pytest

from pipeline import refresh
from pipeline.measurements import measured_component
from pipeline.r2_store import R2BudgetError, R2Store


def test_garmin_timer_separates_login_and_includes_failed_attempts(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr("pipeline.refresh.time.perf_counter", lambda: clock.now)
    def connectapi(*args, fail=False, **kwargs):
        clock.now += 0.25
        if fail:
            raise ValueError("synthetic provider failure")
        return {}
    client = SimpleNamespace(connectapi=connectapi)
    def login():
        clock.now += 0.5
        return client
    provider = refresh.MeasuredGarmin(login)
    assert provider.get() is client
    client.connectapi("synthetic-route")
    with pytest.raises(ValueError):
        client.connectapi("synthetic-route", fail=True)
    assert provider.login_ms == 500
    assert provider.elapsed_ms == 500
    assert provider.calls == 2 and provider.errors == 1


def test_storage_times_get_body_list_and_put_without_extra_calls(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr("pipeline.measurements.time.perf_counter", lambda: clock.now)
    calls = []

    class Body(io.BytesIO):
        def read(self, *args):
            clock.now += 0.075
            return super().read(*args)

    class Client:
        def get_object(self, **kwargs):
            calls.append("get")
            clock.now += 0.125
            return {"Body": Body(b"synthetic")}

        def put_object(self, **kwargs):
            calls.append("put")
            clock.now += 0.05

        def get_paginator(self, name):
            def pages(**kwargs):
                calls.append("list")
                clock.now += 0.04
                yield {"Contents": []}
            return SimpleNamespace(paginate=pages)

    store = R2Store(client=Client(), bucket="synthetic")
    assert store.get("synthetic-key") == b"synthetic"
    store.put("synthetic-key", b"synthetic", "application/json")
    assert calls == ["get", "list", "put"]
    assert store.timings_ms == pytest.approx({"get": 200, "head": 0, "list": 40, "put": 50, "inventory": 40})
    assert store.operations["list_pages"] == store.operations["put"] == 1


def test_full_inventory_timing_excludes_prefix_reads_and_includes_failed_scan(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr("pipeline.measurements.time.perf_counter", lambda: clock.now)
    client = SimpleNamespace(fail=False)
    def paginate(**kwargs):
        clock.now += 0.04
        if client.fail:
            raise RuntimeError("synthetic-list-failure")
        yield {"Contents": []}
    client.get_paginator = lambda name: SimpleNamespace(paginate=paginate)
    store = R2Store(client=client, bucket="synthetic")
    store.list_keys("synthetic-prefix/")
    assert store.timings_ms["inventory"] == 0
    assert not store.write_budget_initialized
    client.fail = True
    with pytest.raises(RuntimeError, match="synthetic-list-failure"):
        store.put("synthetic-key", b"{}", "application/json")
    assert not store.write_budget_initialized
    assert store.operations["list_pages"] == 1
    assert store.operations["put"] == 0
    assert store.timings_ms["inventory"] == pytest.approx(40)
    assert store.timings_ms["list"] == pytest.approx(80)


def test_component_timing_includes_failures_and_does_not_record_arguments(monkeypatch):
    ticks = iter([1.0, 1.25, 2.0, 2.5])
    monkeypatch.setattr("pipeline.measurements.time.perf_counter", lambda: next(ticks))
    store = SimpleNamespace()

    @measured_component("coach_input")
    def component(*, store, fail=False, private_argument="never-store-this"):
        if fail:
            raise ValueError(private_argument)
        return True

    assert component(store=store)
    with pytest.raises(ValueError):
        component(store=store, fail=True)
    assert store.component_timings_ms == {"coach_input": 750}
    assert "never-store-this" not in json.dumps(store.component_timings_ms)


def test_pending_night_persists_small_redacted_diagnostics_on_existing_budget(monkeypatch):
    from test_fresh_data import DAY, NightGarmin

    from scripts.benchmark_health_reuse import HealthClient

    class Clock(date):
        @classmethod
        def today(cls):
            return cls.fromisoformat(DAY)
    monkeypatch.setattr("pipeline.freshness.date", Clock)
    client = HealthClient()
    stores = []
    def factory():
        store = R2Store(client=client, bucket="synthetic")
        stores.append(store)
        return store

    report = refresh.run(refresh.RefreshRequest(mode="night", wake_date=DAY, run_id="222"),
                         store_factory=factory, login=lambda: NightGarmin(sleep={}, hrv={}))
    assert len(stores) == 1
    assert report["status"] == "partial"
    assert report["source_checks"][0]["checked"] is True
    assert report["source_checks"][0]["outcome"] == "not_ready"
    assert report["stages"][0]["garmin_fetch_ms"] is None  # Fixture has no connectapi instrument.
    assert report["stages"][0]["activity_file_import_ms"] is None
    assert report["stages"][0]["r2_read_ms"] >= 0
    assert stores[0].operations["list_pages"] == 1  # Existing write guard only.
    stored = client.objects["refresh/diagnostics/v1/222.json"]["Body"]
    assert json.loads(stored) == report
    assert DAY not in stored.decode()  # Source dates and requests are not included.
    assert "dailySleepDTO" not in stored.decode()
    assert len(stored) < 64 * 1024


def test_diagnostic_publication_obeys_existing_write_budget(monkeypatch):
    from test_fresh_data import DAY, NightGarmin

    from scripts.benchmark_health_reuse import HealthClient

    class Clock(date):
        @classmethod
        def today(cls):
            return cls.fromisoformat(DAY)
    monkeypatch.setattr("pipeline.freshness.date", Clock)
    client = HealthClient()
    # A successful empty check writes its report and receipt, using two PUTs.
    with pytest.raises(R2BudgetError):
        refresh.run(refresh.RefreshRequest(mode="night", wake_date=DAY, run_id="223"),
                    store_factory=lambda: R2Store(client=client, bucket="synthetic", max_writes_per_run=2),
                    login=lambda: NightGarmin(sleep={}, hrv={}))
    assert "refresh/diagnostics/v1/223.json" not in client.objects
    assert json.loads(client.objects["refresh/reports/223.json"]["Body"])["status"] == "pending"
