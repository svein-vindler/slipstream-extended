"""Actual startup/legacy producer contracts with guarded synthetic R2."""
import json
from datetime import date

import pytest
from test_fresh_data import NightGarmin
from test_latest_activity import FakeStore

from pipeline import refresh
from pipeline.freshness import receipt

ID = "12345678-1234-4234-8234-123456789012"
DAY = "2026-09-30"
CONTEXT = {"GITHUB_REPOSITORY": "synthetic/slipstream",
           "GITHUB_WORKFLOW_REF": "synthetic/slipstream/.github/workflows/refresh.yml@refs/heads/main",
           "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": "a" * 40,
           "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ID": "55"}


@pytest.fixture(autouse=True)
def fixed_recent_window(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 30)
    monkeypatch.setattr("pipeline.freshness.date", FixedDate)


def test_actual_night_child_cannot_downgrade_the_immutable_start_receipt():
    store, garmin = FakeStore(), NightGarmin()
    result = refresh.run(refresh.RefreshRequest(mode="night", run_id="55", request_id=ID, wake_date=DAY),
                         store_factory=lambda: store, login=lambda: garmin, workflow_context=CONTEXT)
    saved = json.loads(store.get(f"refresh/requests/{ID}.json"))
    assert saved["kind"] == "refresh-dispatch-receipt"
    assert saved["request"]["mode"] == "night"
    assert saved["request"]["scope"] == f"night/{DAY}"
    assert saved["source"]["source_sha"] == "a" * 40
    assert garmin.calls == [("sleep", DAY), ("hrv", DAY)]
    assert result["correlation_id"] == ID


def test_legacy_same_id_conflict_cannot_overwrite_original_receipt_bytes():
    store = FakeStore()
    receipt(store, "55", ID, f"night/{DAY}")
    before = store.get(f"refresh/requests/{ID}.json")
    with pytest.raises(ValueError, match="conflict"):
        receipt(store, "56", ID, f"night/{DAY}")
    assert store.get(f"refresh/requests/{ID}.json") == before


@pytest.mark.parametrize("fields", [
    {"request_id": "------------------------------------"},
    {"request_id": "00000000-0000-0000-8000-000000000000"},
    {"include_granular": "false"}, {"new_activity_expected": 0}, {"repair_only": "false"},
])
def test_correlation_rejects_untyped_request_before_storage_or_login(fields):
    with pytest.raises(ValueError):
        refresh.run(refresh.RefreshRequest(**fields),
                    store_factory=lambda: pytest.fail("storage touched"),
                    login=lambda: pytest.fail("provider touched"))


@pytest.mark.parametrize("name", ["ACTIVITY_ONLY", "NIGHT_ONLY", "INCLUDE_GRANULAR",
                                  "LATEST_R2_ONLY", "LATEST_NEW_ACTIVITY_EXPECTED"])
def test_workflow_boolean_cannot_be_silently_converted_to_known_false(monkeypatch, name):
    monkeypatch.setattr("sys.argv", ["refresh", "--workflow"])
    for key in ["ACTIVITY_ONLY", "NIGHT_ONLY", "INCLUDE_GRANULAR", "LATEST_R2_ONLY", "LATEST_NEW_ACTIVITY_EXPECTED"]:
        monkeypatch.setenv(key, "false")
    for key, value in {**CONTEXT, "REFRESH_EVENT": "workflow_dispatch", "SYNC_REQUEST_ID": ID}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv(name, "not-a-boolean")
    monkeypatch.setattr(refresh, "run", lambda *a, **k: pytest.fail("invalid inputs reached producer"))
    with pytest.raises(SystemExit):
        refresh.main()


@pytest.mark.parametrize("field,value", [
    ("GITHUB_RUN_ID", "56"), ("GITHUB_EVENT_NAME", "schedule"),
    ("GITHUB_SHA", "not-a-sha"), ("GITHUB_REF", "refs/tags/main"),
    ("GITHUB_REPOSITORY", "other/repo"), ("GITHUB_WORKFLOW_REF", "other/repo/.github/workflows/refresh.yml@refs/heads/main"),
    ("GITHUB_SHA", ""), ("GITHUB_WORKFLOW_REF", "synthetic/slipstream/../../refresh.yml@refs/heads/main"),
])
def test_incomplete_workflow_source_stops_before_storage_and_login(field, value):
    context = {**CONTEXT, field: value}
    with pytest.raises(ValueError, match="source"):
        refresh.run(refresh.RefreshRequest(run_id="55", request_id=ID), workflow_context=context,
                    store_factory=lambda: pytest.fail("storage touched"),
                    login=lambda: pytest.fail("provider touched"))


def test_matching_start_retry_preserves_exact_bytes_and_charges_failed_conditional_write():
    from pipeline.dispatch_receipt import publish_start, workflow_receipt
    store = FakeStore()
    evidence = workflow_receipt(refresh.RefreshRequest(run_id="55", request_id=ID), CONTEXT)
    publish_start(store, evidence)
    before = store.get(f"refresh/requests/{ID}.json")
    calls = dict(store.client.counts)
    publish_start(store, evidence)
    assert store.get(f"refresh/requests/{ID}.json") == before
    assert store.client.counts["put"] - calls["put"] == 1
    assert store.client.counts["list"] - calls["list"] == 0
    assert store.client.counts["get"] - calls["get"] == 2  # matching read plus the test's byte check.


@pytest.mark.parametrize("defect", ["run", "source", "scope", "repair", "legacy"])
def test_conflicting_start_never_replaces_old_bytes_or_contacts_provider(defect):
    from pipeline.dispatch_receipt import publish_start, workflow_receipt
    from pipeline.r2_store import R2Store
    from scripts.benchmark_summary_reuse import SummaryClient
    client = SummaryClient()
    setup = R2Store(client=client, bucket="synthetic")
    evidence = workflow_receipt(refresh.RefreshRequest(run_id="55", request_id=ID), CONTEXT)
    if defect == "run":
        evidence["run_id"] = 56
    elif defect == "source":
        evidence["source"]["source_sha"] = "b" * 40
    elif defect == "scope":
        evidence["request"]["scope"] = f"night/{DAY}"
    elif defect == "repair":
        evidence["request"]["repair_only"] = True
    if defect == "legacy":
        receipt(setup, "55", ID, f"night/{DAY}")
    else:
        publish_start(setup, evidence)
    old = {key: value["Body"] for key, value in client.objects.items()}
    store = R2Store(client=client, bucket="synthetic")
    with pytest.raises(ValueError, match="conflict"):
        refresh.run(refresh.RefreshRequest(run_id="55", request_id=ID), workflow_context=CONTEXT,
                    store_factory=lambda: store, login=lambda: pytest.fail("provider touched"))
    assert {key: value["Body"] for key, value in client.objects.items()} == old
    assert store.operations["put"] == 1 and store.operations["get"] == 1


@pytest.mark.parametrize("limits", [
    {"max_writes_per_run": 1}, {"max_write_bytes_per_run": 1},
    {"max_bucket_objects": 1}, {"max_bucket_bytes": 1},
])
def test_start_metadata_is_inside_existing_stage_and_bucket_limits(limits):
    from pipeline.r2_store import R2BudgetError, R2Store
    from scripts.benchmark_summary_reuse import SummaryClient
    client = SummaryClient()
    client.objects["synthetic/history-marker.json"] = {"Body": b"original"}
    store = R2Store(client=client, bucket="synthetic", **limits)
    if "max_writes_per_run" in limits:
        store.put("synthetic/history-marker.json", b"original", "application/json")
    before_puts = client.counts["put"]
    with pytest.raises(R2BudgetError):
        refresh.run(refresh.RefreshRequest(run_id="55", request_id=ID), workflow_context=CONTEXT,
                    store_factory=lambda: store, login=lambda: pytest.fail("provider touched"))
    assert client.counts["put"] == before_puts
    assert {key: value["Body"] for key, value in client.objects.items()} == {"synthetic/history-marker.json": b"original"}
