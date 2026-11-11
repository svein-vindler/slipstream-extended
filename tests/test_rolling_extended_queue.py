"""Extended rolling windows through the durable queue and source seams."""

import copy
import hashlib
import json

import pytest
from test_rolling_discovery import queue_source
from test_rolling_reference_sources import sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline import rolling_night_references as rolling
from pipeline.night_products import normalize_args, storage_key


def test_explicit_window_selection_is_canonical_and_default_keeps_legacy_windows():
    assert rolling.window_selection() == (28, 30, 90)
    assert rolling.window_selection(rolling.WINDOWS) == (7, 28, 30, 90, 365)
    assert rolling.window_selection([7, 28, 365]) == (7, 28, 365)
    assert rolling.window_selection((90,)) == (90,)
    for invalid in ([], (), [True], [7.0], ["7"], [366], {7}, "7", 7, [28, 7], [7, 7]):
        with pytest.raises(ValueError):
            rolling.window_selection(invalid)


def test_implicit_registration_and_status_keep_extended_windows_unselected():
    queue = {"pending": {}}
    rolling.register(queue, ["2026-10-25", "2026-10-26"])
    assert set(rolling.status(queue)) == {"28", "30", "90"}
    before = copy.deepcopy(queue)
    all_windows = rolling.status(queue, windows=rolling.WINDOWS)
    assert all_windows["7"]["heart_rate"] == {"count": 0, "ranges": []}
    assert all_windows["365"]["hrv"] == {"count": 0, "ranges": []}
    assert queue == before


def test_explicit_annual_selection_on_legacy_endpoint_is_idempotent_and_does_not_widen_peers():
    queue = {"pending": {}}
    rolling.register(queue, ["2026-10-25", "2026-10-26"])
    rolling.register(queue, ["2026-10-25"], windows=(365,))
    selected = copy.deepcopy(queue)
    rolling.register(queue, ["2026-10-25"], windows=(365,))
    assert queue == selected
    rolling.invalidate(queue, "hrv", "2026-10-25")
    annual = rolling.status(queue, windows=(365,))
    assert set(annual) == {"365"}
    assert annual["365"] == {channel: {"count": 1, "ranges": [["2026-10-25", "2026-10-25"]]}
                            for channel in ("heart_rate", "hrv")}
    assert set(rolling.status(queue)) == {"28", "30", "90"}


@pytest.mark.parametrize("later_windows", [(30,), None])
def test_explicit_legacy_subset_admits_later_selection_without_hiding_missing_slots(later_windows):
    day = "2026-10-25"
    queue = {"pending": {}}
    rolling.register(queue, [day], windows=(28,))
    before = copy.deepcopy(queue)
    rolling.register(queue, [day], windows=(28,))
    assert queue == before
    rolling.register(queue, [day], windows=later_windows)
    selected = {28, *rolling.window_selection(later_windows)}
    status = rolling.status(queue, windows=rolling.WINDOWS)
    assert {n for n in rolling.WINDOWS if status[str(n)]["heart_rate"]["count"]} == selected
    for n in rolling.WINDOWS:
        for channel in rolling.CHANNELS:
            assert status[str(n)][channel]["ranges"] == ([[day, day]] if n in selected else [])
    before = copy.deepcopy(queue)
    rolling.register(queue, [day], windows=later_windows)
    assert queue == before
    rolling.validate(queue)


def test_explicit_legacy_subsets_do_not_invalidate_unselected_peer_slots():
    first, peer = "2026-10-25", "2026-10-26"
    queue = {"pending": {}}
    rolling.register(queue, [first], windows=(28,))
    rolling.register(queue, [peer], windows=(90,))
    for channels in queue["rolling"]["dirty"].values():
        for ranges in channels.values():
            ranges.clear()  # Completed products retain admission, not dirty work.
    rolling.invalidate(queue, "hrv", first)
    status = rolling.status(queue, windows=rolling.WINDOWS)
    for n in rolling.WINDOWS:
        expected = [[first, first]] if n == 28 else [[peer, peer]] if n == 90 else []
        for channel in rolling.CHANNELS:
            assert status[str(n)][channel]["ranges"] == expected
    rolling.validate(queue)


def legacy_registry(*, first="2025-01-01", last="2026-10-25"):
    return {"eligible": [[first, last]], "dirty": {
        "28": {"heart_rate": [], "hrv": []},
        "30": {"heart_rate": [], "hrv": []},
        "90": {"heart_rate": [], "hrv": []},
    }}


def test_legacy_registry_read_has_empty_new_windows_and_preserves_exact_queue_bytes():
    server, _ = seeded("2026-10-25", 1, contexts=False)
    before = queue_source(server, [], rolling=legacy_registry())
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25",
                                     timezone="Europe/Oslo", period_names=set(), rolling_sources=[],
                                     rolling_windows=rolling.WINDOWS)
    assert result["rolling_remaining"]["7"]["heart_rate"] == {"count": 0, "ranges": []}
    assert result["rolling_remaining"]["365"]["hrv"] == {"count": 0, "ranges": []}
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}


def test_passive_consumer_does_not_upgrade_or_enqueue_clean_legacy_history():
    server, _ = seeded("2026-10-25", 1, contexts=False)
    before = queue_source(server, [], rolling=legacy_registry())
    result = refresh.refresh_pending(sdk_store(server), today="2026-10-25",
                                     timezone="Europe/Oslo", period_names=set())
    assert result["products_written"] == 0
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert result["component_operations"] == {"get": 1, "list": 0, "put": 0}
    queue = {"pending": {}, "rolling": legacy_registry()}
    assert not rolling.materialize(queue, today="2026-10-25", limit=2, max_pending=24)
    assert set(queue["rolling"]["dirty"]) == {"28", "30", "90"}


def test_actual_source_change_adds_only_affected_eligible_extended_windows_to_legacy_queue():
    server, _ = seeded("2026-10-25", 1, contexts=False)
    registry = legacy_registry(last="2027-10-25")
    registry["dirty"]["28"]["hrv"] = [["2025-01-02", "2025-01-02"]]
    queue = {"pending": {}, "rolling": registry}
    rolling.register(queue, ["2026-10-25"], windows=rolling.WINDOWS)
    queue_source(server, [], rolling=queue["rolling"])
    key = "health/hrv/2026/10/2026-10-25.json"
    source = json.loads(server.objects[key][0])
    source["readings"][0]["hrv_ms"] = 75
    assert refresh.publish_source(sdk_store(server), key, json.dumps(source).encode(),
                                  "application/json")
    queue = json.loads(server.objects[refresh.QUEUE_KEY][0])
    status = refresh.rolling_cohort_status(queue, rolling_windows=rolling.WINDOWS)
    assert status["7"]["hrv"] == {"count": 1, "ranges": [["2026-10-25", "2026-10-25"]]}
    assert status["365"]["heart_rate"] == {
        "count": 1, "ranges": [["2026-10-25", "2026-10-25"]]}
    assert status["28"]["hrv"]["ranges"][0] == ["2025-01-02", "2025-01-02"]
    assert queue["rolling"]["eligible"] == [["2025-01-01", "2027-10-25"]]
    assert all("if-match" in headers or headers.get("if-none-match") == "*"
               for method, _, headers in server.headers if method == "PUT")


def test_new_endpoint_upgrade_preserves_clean_legacy_history_and_existing_pending_owner():
    queue = {"pending": {"week/2026-W43": {"phase": "publishing", "inflight": {"key": "known"}}},
             "rolling": legacy_registry(last="2026-10-24")}
    old_pending = copy.deepcopy(queue["pending"])
    rolling.register(queue, ["2026-10-25"], windows=rolling.WINDOWS)
    assert queue["pending"] == old_pending
    assert rolling.status(queue, windows=rolling.WINDOWS)["365"]["hrv"] == {
        "count": 1, "ranges": [["2026-10-25", "2026-10-25"]]}
    assert rolling.status(queue, windows=rolling.WINDOWS)["7"]["heart_rate"]["count"] == 1
    rolling.validate(queue)


@pytest.mark.parametrize(("end", "start7", "start365"), [
    ("2021-01-01", "2020-12-26", "2020-01-03"),
    ("2024-02-29", "2024-02-23", "2023-03-02"),
])
def test_materialization_uses_exact_inclusive_days_across_iso_year_and_leap_year(end, start7, start365):
    queue = {"pending": {}}
    rolling.register(queue, [end], windows=rolling.WINDOWS)
    assert rolling.materialize(queue, today=end, limit=10, max_pending=24, allowed_windows=rolling.WINDOWS)
    assert len(queue["pending"]) == 10
    for n, first in ((7, start7), (365, start365)):
        for channel in ("heart_rate", "hrv"):
            name = f"rolling/{n}/{end}/{channel}"
            row = queue["pending"][name]
            assert row["start_date"] == first
            assert row["end_date"] == end
            assert rolling.row_identity(name, row)
    rolling.validate(queue)


def test_legacy_dirty_materialization_preserves_legacy_registry_keys():
    queue = {"pending": {}, "rolling": legacy_registry()}
    queue["rolling"]["dirty"]["28"]["hrv"] = [["2026-10-25", "2026-10-25"]]
    assert rolling.materialize(queue, today="2026-10-25", limit=2, max_pending=24)
    assert set(queue["pending"]) == {"rolling/28/2026-10-25/hrv"}
    assert rolling.status(queue, windows=rolling.WINDOWS)["7"]["hrv"]["count"] == 0
    assert rolling.status(queue, windows=rolling.WINDOWS)["365"]["hrv"]["count"] == 0
    assert set(queue["rolling"]["dirty"]) == {"28", "30", "90"}


@pytest.mark.parametrize("held", ["canonical", "context"])
def test_extended_materialization_respects_durable_unknown_source_and_context_holds(held):
    queue = {"pending": {}}
    rolling.register(queue, ["2026-10-25", "2027-10-24"], windows=rolling.WINDOWS)
    if held == "canonical":
        queue["pending"]["week/2026-W43"] = {
            "phase": "dirty", "sources": {"health/hrv/2026/10/2026-10-25.json": {
                "state": "intent", "sha256": "a" * 64}}, "completed": {}}
    else:
        queue["rolling_context_intents"] = {"synthetic": {"wake_date": "2026-10-25"}}
    before = copy.deepcopy(queue)
    assert not rolling.materialize(queue, today="2027-10-24", limit=10, max_pending=24,
                                   allowed_sources=[("hrv", "2026-10-25")])
    assert queue == before
    assert rolling.status(queue, windows=rolling.WINDOWS)["365"]["heart_rate"]["count"] == 2


def test_invalidation_preserves_unknown_publishing_owner_and_reopens_only_safe_dirty_rows():
    queue = {"pending": {}}
    rolling.register(queue, ["2026-10-25"], windows=rolling.WINDOWS)
    rolling.materialize(queue, today="2026-10-25", limit=10, max_pending=24, allowed_windows=rolling.WINDOWS)
    held = queue["pending"]["rolling/365/2026-10-25/hrv"]
    held.update(phase="publishing", inflight={"key": "synthetic-retained"},
                completed={"synthetic": {"revision": '"old"'}})
    retained = copy.deepcopy(held)
    safe = queue["pending"]["rolling/7/2026-10-25/heart_rate"]
    safe.update(blocked="native_input_unverified", completed={"synthetic": {}},
                validated_progress={"synthetic": {}})
    rolling.invalidate(queue, "hrv", "2026-10-25")
    assert held == retained
    assert safe["completed"] == {}
    assert "blocked" not in safe and "validated_progress" not in safe
    assert queue["rolling"]["dirty"]["365"]["hrv"] == [["2026-10-25", "2026-10-25"]]
    assert queue["rolling"]["dirty"]["7"]["heart_rate"] == []


@pytest.mark.parametrize("dirty_keys", [("7", "28", "30", "90"), ("28", "30"),
                                         ("28", "30", "90", "366")])
def test_unknown_or_partially_upgraded_window_registry_fails_closed_at_sdk_boundary(dirty_keys):
    server, _ = seeded("2026-10-25", 1, contexts=False)
    registry = legacy_registry()
    registry["dirty"] = {n: {"heart_rate": [], "hrv": []} for n in dirty_keys}
    before = queue_source(server, [], rolling=registry)
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC")
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert not server.puts


@pytest.mark.parametrize("fault", ["missing_window", "unknown_window", "invalid_range"])
def test_malformed_window_admission_fails_closed_without_rewriting_stored_queue(fault):
    server, _ = seeded("2026-10-25", 1, contexts=False)
    registry = legacy_registry()
    registry["window_eligible"] = {str(width): [] for width in rolling.WINDOWS}
    if fault == "missing_window":
        del registry["window_eligible"]["365"]
    elif fault == "unknown_window":
        registry["window_eligible"]["366"] = []
    else:
        registry["window_eligible"]["365"] = [["2026-10-26", "2026-10-25"]]
    before = queue_source(server, [], rolling=registry)
    with pytest.raises(refresh.BaselineRefreshStop):
        refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="UTC", rolling_windows=(365,))
    assert server.objects[refresh.QUEUE_KEY][0] == before
    assert not server.puts


def test_sdk_consumer_publishes_extended_inclusive_products_with_conditional_writes():
    day = "2024-02-29"
    server, _ = seeded(day, 1, contexts=False)
    key = "health/sleep/v1/2024/02/2024-02-29.json"
    body, revision = server.objects[key]
    refresh.register_rolling_endpoints(sdk_store(server), [{
        "key": key, "revision": revision, "sha256": hashlib.sha256(body).hexdigest()}],
        rolling_windows=rolling.WINDOWS)
    reports = []
    cache = {}
    for _ in range(12):
        report = refresh.refresh_pending(sdk_store(server), today=day, timezone="Europe/Oslo",
                                         period_names=set(), snapshot_cache=cache, rolling_windows=rolling.WINDOWS)
        reports.append(report)
        if report["status"] == "complete":
            break
    assert sum(report["products_written"] for report in reports) == 10
    for n in (7, 365):
        for channel in ("heart_rate", "hrv"):
            args = normalize_args(end_date=day, period=str(n), channel=channel)
            product = json.loads(server.objects[storage_key(args)][0])
            assert product["nights_requested"] == n
            assert product["contributing_nights"] == 1
    assert reports[-1]["status"] == "complete"
    assert all("if-match" in headers or headers.get("if-none-match") == "*"
               for method, _, headers in server.headers if method == "PUT")
