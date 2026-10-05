import json
from datetime import date
from types import SimpleNamespace

import pytest

from pipeline import refresh


@pytest.fixture
def routed(monkeypatch, tmp_path):
    calls, stores, logins = [], [], []
    client = SimpleNamespace(connectapi=lambda *a, **kw: {})

    def factory():
        store = SimpleNamespace(operations={"get": 0, "head": 0, "list_pages": 0, "put": 0})
        stores.append(store)
        return store

    def login():
        logins.append(True)
        return client

    def operation(name):
        def run(*args, **kwargs):
            calls.append((name, args, kwargs))
            store = kwargs.get("store") or (args[1] if name == "restore" else None)
            if store:
                store.operations["get"] += 2
            source = kwargs.get("garmin") or kwargs.get("client")
            if source:
                source.connectapi("synthetic-private-route", secret="never-log-this")
            if name in {"summary", "fetch"}:
                (tmp_path / "data").mkdir(exist_ok=True)
                (tmp_path / "data/health_daily.csv").write_text("Date,Weight KG\n", encoding="utf-8")
            return {"status": "complete", "private_metric": "never-log-this"}
        return run

    for module, method, name in [
        (refresh.summary_restore, "restore_summaries", "restore"),
        (refresh.refresh_summaries, "run", "summary"),
        (refresh.recent_health, "run", "night_overlap"),
        (refresh.recent_body, "run", "body_overlap"),
        (refresh.manual_activity_refresh, "snapshot", "baseline"),
        (refresh.manual_activity_refresh, "run", "manual_activity"),
        (refresh.latest_activity, "run", "activity"),
        (refresh.latest_night, "run", "night"),
        (refresh.fetch, "run", "fetch"),
        (refresh.summary_export, "run", "export"),
        (refresh.health_index_reconcile, "run", "indexes"),
    ]:
        monkeypatch.setattr(module, method, operation(name))
    return calls, stores, logins, {"data_dir": str(tmp_path / "data"), "store_factory": factory, "login": login}


@pytest.mark.parametrize("scheduled,granular,names", [
    (False, False, ["restore", "summary", "night_overlap", "body_overlap"]),
    (True, False, ["restore", "summary", "night_overlap", "body_overlap", "indexes"]),
    (False, True, ["restore", "baseline", "summary", "night_overlap", "body_overlap", "manual_activity"]),
])
def test_general_preserves_order_bounds_budgets_and_one_login(routed, scheduled, granular, names, tmp_path):
    calls, stores, logins, options = routed
    report = refresh.run(refresh.RefreshRequest(scheduled=scheduled, include_granular=granular), **options,
                         diagnostics_file=tmp_path / "report.json")
    assert [call[0] for call in calls] == names
    assert len(logins) == 1
    assert len({id(store) for store in stores}) == len(stores) == len(names) - int(granular)
    health = next(call[2] for call in calls if call[0] == "night_overlap")
    body = next(call[2] for call in calls if call[0] == "body_overlap")
    assert health["days"] == body["max_days"] == 3
    assert json.loads((tmp_path / "report.json").read_text()) == report
    assert "never-log-this" not in json.dumps(report)
    assert "synthetic-private-route" not in json.dumps(report)
    assert sum(stage["garmin_connectapi_calls"] for stage in report["stages"]) == 3 + int(granular)
    assert report["garmin_connectapi_available"] is True
    assert all(stage["elapsed_ms"] >= 0 for stage in report["stages"])


@pytest.mark.parametrize("mode,repair,names", [
    ("night", False, ["night"]), ("activity", False, ["restore", "activity"]),
    ("activity", True, ["activity"]),
])
def test_targeted_modes_never_run_general_or_history(routed, mode, repair, names, tmp_path):
    calls, stores, logins, options = routed
    request = refresh.RefreshRequest(mode=mode, repair_only=repair,
                                     wake_date=date.today().isoformat(), run_id="123")
    report = refresh.run(request, **options)
    assert [call[0] for call in calls] == names
    assert len(logins) == int(not repair)
    assert len(stores) == len(names)
    assert calls[-1][2]["run_id"] == "123"
    assert report["status"] == "complete"
    if repair:
        assert calls[-1][2]["garmin"] is None
        assert report["stages"][0]["garmin_connectapi_calls"] == 0


def test_year_backfill_runs_newest_first_then_exports_and_recent_overlap(routed):
    calls, _, _, options = routed
    refresh.run(refresh.RefreshRequest(backfill_start_year="2024", backfill_end_year="2025"), **options)
    assert [call[0] for call in calls] == ["restore", "fetch", "fetch", "export", "night_overlap", "body_overlap"]
    assert [call[2]["health_start"] for call in calls if call[0] == "fetch"] == [date(2025, 1, 1), date(2024, 1, 1)]
    assert all(call[2]["skip_activities"] for call in calls if call[0] == "fetch")


@pytest.mark.parametrize("interrupted", [False, True])
def test_failed_source_stops_downstream_but_scheduled_index_repair_obeys_interrupt(routed, monkeypatch, tmp_path, interrupted):
    calls, _, _, options = routed
    def fail(*args, **kwargs):
        raise KeyboardInterrupt() if interrupted else RuntimeError("never-log-this")
    monkeypatch.setattr(refresh.refresh_summaries, "run", fail)
    path = tmp_path / "diagnostics.json"
    with pytest.raises(KeyboardInterrupt if interrupted else RuntimeError):
        refresh.run(refresh.RefreshRequest(scheduled=True), **options, diagnostics_file=path)
    assert [call[0] for call in calls] == (["restore"] if interrupted else ["restore", "indexes"])
    report = json.loads(path.read_text())
    assert report["status"] == "failed"
    assert "never-log-this" not in path.read_text()


@pytest.mark.parametrize("kwargs", [
    {"mode": "invalid"}, {"mode": "night"}, {"repair_only": True},
    {"health_start": "2026-01-01"}, {"health_start": "2026-02-30", "health_end": "2026-03-01"},
    {"backfill_start_year": "2025"}, {"backfill_start_year": "2025", "backfill_end_year": "2024"},
    {"mode": "activity", "health_start": "2026-01-01", "health_end": "2026-01-02"},
    {"scheduled": True, "include_granular": True}, {"run_id": "not-numeric"},
    {"activity_id": "private-non-numeric"}, {"request_id": "not-a-request-id"},
])
def test_invalid_requests_fail_before_external_access(kwargs):
    with pytest.raises(ValueError):
        refresh.run(refresh.RefreshRequest(**kwargs),
                    store_factory=lambda: pytest.fail("Unexpected storage"),
                    login=lambda: pytest.fail("Unexpected Garmin login"))


def test_provider_counts_failures_and_restores_existing_client(monkeypatch, tmp_path):
    def original(*a, **kw):
        raise RuntimeError("private-provider-response")
    client = SimpleNamespace(connectapi=original)
    monkeypatch.setattr(refresh.latest_night, "run", lambda **kwargs: kwargs["garmin"].connectapi("private-route"))
    path = tmp_path / "diagnostics.json"
    for _ in range(2):
        with pytest.raises(RuntimeError):
            refresh.run(refresh.RefreshRequest(mode="night", wake_date=date.today().isoformat()),
                        store_factory=lambda: SimpleNamespace(operations={}), login=lambda: client,
                        diagnostics_file=path)
        report = json.loads(path.read_text())
        assert report["stages"][0]["garmin_connectapi_calls"] == 1
        assert report["stages"][0]["garmin_connectapi_errors"] == 1
        assert client.connectapi is original
        assert "private-provider-response" not in path.read_text()


def test_workflow_input_adapter_preserves_targeting_and_correlation(monkeypatch):
    captured = []
    monkeypatch.setattr("sys.argv", ["refresh", "--workflow"])
    for key, value in {"ACTIVITY_ONLY": "true", "NIGHT_ONLY": "false", "INCLUDE_GRANULAR": "true",
                       "REFRESH_EVENT": "workflow_dispatch", "GITHUB_RUN_ID": "123",
                       "LATEST_R2_ONLY": "true", "LATEST_ACTIVITY_ID": "456",
                       "LATEST_NEW_ACTIVITY_EXPECTED": "false", "SYNC_REQUEST_ID": "a" * 36,
                       "LATEST_EXPECTED_DATE": "", "HEALTH_START": "", "HEALTH_END": "",
                       "BACKFILL_START_YEAR": "", "BACKFILL_END_YEAR": ""}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(refresh, "run", lambda request, **kwargs: captured.append(request))
    refresh.main()
    request = captured[0]
    assert request.mode == "activity" and request.repair_only and request.include_granular
    assert request.run_id == "123" and request.activity_id == "456"
    assert request.request_id == "a" * 36 and request.new_activity_expected is False
    monkeypatch.setenv("NIGHT_ONLY", "true")
    with pytest.raises(SystemExit):
        refresh.main()
    assert len(captured) == 1


def test_real_targeted_night_pipeline_keeps_receipts_and_reuses_canonical_bytes(monkeypatch):
    from test_fresh_data import DAY, NightGarmin

    from pipeline.r2_store import R2Store
    from scripts.benchmark_health_reuse import HealthClient
    class Clock(date):
        @classmethod
        def today(cls):
            return cls.fromisoformat(DAY)
    monkeypatch.setattr("pipeline.freshness.date", Clock)
    client = HealthClient()
    source = NightGarmin()
    request = refresh.RefreshRequest(mode="night", wake_date=DAY, run_id="123")
    for _ in range(2):
        report = refresh.run(request, store_factory=lambda: R2Store(client=client, bucket="synthetic-refresh"),
                             login=lambda: source)
        assert report["status"] == "complete"
        persisted = json.loads(client.objects["refresh/reports/123.json"]["Body"])
        assert persisted["status"] == "stored" and persisted["source_checked"] is True
    assert persisted["sleep_reused"] and persisted["hrv_reused"]
