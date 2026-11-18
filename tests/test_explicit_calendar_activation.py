"""Missing explicit periods must publish through the ordinary fenced CAS path."""
import copy
import hashlib
import json
from datetime import date, timedelta

import pytest
from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import normalize_args, storage_key
from scripts.night_baseline_fixtures import native_fixture
from test_night_baseline_corrections import calendar_store
from test_night_baseline_refresh import WritableSnapshots


def test_known_empty_drain_then_explicit_period_and_repeat_preserves_products():
    store = WritableSnapshots()
    store.objects = native_fixture().objects.copy()
    # A real nonempty global queue with unrelated work reproduces the actual
    # calendar no-op. The selected closed week is absent.
    store.add(refresh.QUEUE_KEY, {"schema_version": 1, "kind": "night-baseline-refresh-queue",
        "pending": {"month/2026-11": {"start_date": "2026-11-01", "end_date": "2026-11-30",
            "phase": "dirty", "sources": {}, "completed": {}}}})
    selected = {"week/2026-W43": ("2026-10-19", "2026-10-25")}
    old = refresh.refresh_pending(store, today="2026-11-02", timezone="Europe/Oslo",
        max_periods=1, period_names=set(selected), rolling_sources=[])
    assert old["periods_completed"] == old["products_written"] == 0
    assert old["component_operations"] == {"get": 1, "list": 0, "put": 0}
    assert store.writes == []
    result = refresh.refresh_pending(store, today="2026-11-02", timezone="Europe/Oslo",
        max_periods=1, period_names=set(selected), rolling_sources=[], calendar_periods=selected)
    assert result["periods_completed"] == 1
    assert result["products_written"] == 2
    keys = [storage_key(normalize_args(start_date="2026-10-19", end_date="2026-10-25",
        period="custom", channel=channel)) for channel in ("heart_rate", "hrv")]
    products = {key: store.objects[key] for key in keys}
    assert json.loads(store.objects[refresh.QUEUE_KEY].data)["pending"].keys() == {"month/2026-11"}
    writes = list(store.writes)
    again = refresh.refresh_pending(store, today="2026-11-02", timezone="Europe/Oslo",
        max_periods=1, period_names=set(selected), rolling_sources=[], calendar_periods=selected)
    assert again["products_written"] == 0 and again["products_unchanged"] == 2
    assert all(store.objects[key].data == products[key].data
        and store.objects[key].revision == products[key].revision for key in keys)
    assert not any(key in keys for key in store.writes[len(writes):])
    assert not any("/rolling/" in key for key in store.writes)


@pytest.mark.parametrize("fault", ["scope", "range", "open", "rolling"])
def test_explicit_activation_does_not_expand_period_or_rolling_scope(fault):
    store = WritableSnapshots()
    selected = {"week/2026-W43": ("2026-10-19", "2026-10-25")}
    names = set(selected)
    sources = []
    if fault == "scope":
        names = {"month/2026-10"}
    elif fault == "range":
        selected["week/2026-W43"] = ("2026-10-18", "2026-10-25")
    elif fault == "open":
        selected = {"month/2026-11": ("2026-11-01", "2026-11-30")}
        names = set(selected)
    else:
        sources = None
    with pytest.raises(refresh.BaselineRefreshStop, match="calendar"):
        refresh.refresh_pending(store, today="2026-11-02", timezone="Europe/Oslo",
            max_periods=1, period_names=names, rolling_sources=sources, calendar_periods=selected)
    assert store.writes == []


@pytest.mark.parametrize("cached", [False, True])
def test_unchanged_source_missing_calendar_queue_completes_eight_periods_and_preserves_rolling(cached):
    store = calendar_store(date(2025, 5, 1), 61)
    key = "health/hrv/2025/05/2025-05-17.json"
    source = store.objects[key]
    if cached:
        store.matches_object = lambda name, data, kind, **kw: (
            store.objects[name].data == data and store.objects[name].content_type == kind
            and store.objects[name].encoding == kw.get("encoding"))
    assert refresh.publish_source(store, key, source.data, source.content_type,
                                  encoding=source.encoding) is False
    assert store.writes == [] and refresh.QUEUE_KEY not in store.objects
    sleep = "health/sleep/v1/2025/05/2025-05-17.json"
    snapshot = store.objects[sleep]
    report = refresh.register_rolling_endpoints(store, [{"key": sleep, "revision": snapshot.revision,
        "sha256": hashlib.sha256(snapshot.data).hexdigest()}], rolling_windows=[7, 28, 30, 90, 365])
    assert set(report["rolling_remaining"]) == {"7", "28", "30", "90", "365"}
    queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
    rolling = copy.deepcopy({k: v for k, v in queue.items() if k != "pending"})
    selected = {"month/2025-05": ("2025-05-01", "2025-05-31"),
                "month/2025-06": ("2025-06-01", "2025-06-30")}
    for week, first in zip(range(20, 26), (12, 19, 26, 2, 9, 16), strict=True):
        month = 5 if week < 23 else 6
        start = date(2025, month, first)
        selected[f"week/2025-W{week}"] = (start.isoformat(), (start + timedelta(days=6)).isoformat())
    raw = {name: (obj.data, obj.revision) for name, obj in store.objects.items()
           if name.startswith(("health/sleep/", "health/hrv/", "health/signals/"))}
    products = 0
    items = list(selected.items())
    for item in items:
        reopened = WritableSnapshots()
        reopened.objects, reopened.writes = store.objects, store.writes
        store = reopened
        periods = dict([item])
        result = refresh.refresh_pending(store, today="2026-11-16", timezone="Europe/Oslo",
            max_periods=1, period_names=set(periods), rolling_sources=[], calendar_periods=periods)
        assert result["periods_completed"] == len(periods) and result["blocked"] == []
        products += result["products_written"]
    assert products == 16
    assert {name: (store.objects[name].data, store.objects[name].revision) for name in raw} == raw
    assert {k: v for k, v in json.loads(store.objects[refresh.QUEUE_KEY].data).items() if k != "pending"} == rolling
    writes = list(store.writes)
    for item in items:
        reopened = WritableSnapshots()
        reopened.objects, reopened.writes = store.objects, store.writes
        store = reopened
        periods = dict([item])
        result = refresh.refresh_pending(store, today="2026-11-16", timezone="Europe/Oslo",
            max_periods=1, period_names=set(periods), rolling_sources=[], calendar_periods=periods)
        assert result["products_written"] == 0 and result["products_unchanged"] == 2 * len(periods)
    assert not any("/baseline/" in name for name in store.writes[len(writes):])
    assert {k: v for k, v in json.loads(store.objects[refresh.QUEUE_KEY].data).items() if k != "pending"} == rolling
