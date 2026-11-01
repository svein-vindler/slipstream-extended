"""Memory-only CI contrasts; no original temp/FS scenario is rerouted."""

import copy
import json
import os
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import pytest

from pipeline import recent_health
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.signal_series import DailySignalCollector, signal_key
from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin
from scripts.benchmark_summary_windows import SummaryGarmin


class _SourceDay(date):
    @classmethod
    def today(cls):
        return cls(2026, 10, 14)


class _SourceInstant(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 10, 14, 12, tzinfo=UTC)
        return value if tz else value.replace(tzinfo=None)


def _bind_source_clocks(stack):
    from pipeline import health_sync, night_baseline_refresh

    stack.enter_context(patch.object(recent_health, "date", _SourceDay))
    stack.enter_context(patch.object(health_sync, "datetime", _SourceInstant))
    stack.enter_context(patch.object(night_baseline_refresh, "datetime", _SourceInstant))


def test_recent_source_clock_also_controls_reference_period_closure():
    client = HealthClient()
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"HEALTH_TIMEZONE": "Europe/Oslo"}, clear=True))
        _bind_source_clocks(stack)
        report = recent_health.run(
            days=3, store=R2Store(client=client, bucket="synthetic"), garmin=SyntheticGarmin()
        )
    assert report["status"] == "complete"
    assert report["baseline_refresh"]["status"] == "waiting_until_close"
    assert report["baseline_refresh"]["products_written"] == 0


def signal_day(collector, day, stress=35):
    collector.collect(day, "heart_rate", SummaryGarmin().get_heart_rates(day))
    collector.collect(
        day,
        "stress",
        {"calendarDate": day, "stressValuesArray": [[1790726400000, 0], [1790726520000, stress]]},
    )


def test_low_source_write_budget_stops_before_intent_and_any_canonical_attempt():
    peer = HealthClient()
    with patch.dict(os.environ, {}, clear=True):
        initial = DailySignalCollector(R2Store(client=peer, bucket="synthetic"))
        signal_day(initial, "2026-09-30")
        initial.publish()
        frozen = copy.deepcopy(peer.objects)
        next_peer = HealthClient()
        next_peer.objects = copy.deepcopy(frozen)
        store = R2Store(client=next_peer, bucket="synthetic", max_writes_per_run=1)
        changed = DailySignalCollector(store)
        signal_day(changed, "2026-09-30", 40)
        with pytest.raises(R2BudgetError):
            changed.publish()
    assert next_peer.objects == frozen
    assert next_peer.writes == []
    assert store.operations["put"] == 0
    print(
        json.dumps(
            {
                "synthetic_operation_ledger": "write_budget1",
                "operations": store.operations,
                "reads": next_peer.reads,
                "writes": next_peer.writes,
            }
        )
    )


def test_unchanged_signal_repeat_adds_one_bounded_queue_read_and_no_signal_write():
    peer = HealthClient()
    days = [(date(2026, 9, 30) - timedelta(days=i)).isoformat() for i in range(3)]
    with patch.dict(os.environ, {}, clear=True):
        first = DailySignalCollector(R2Store(client=peer, bucket="synthetic"))
        for day in days:
            signal_day(first, day)
        first.publish()
        next_peer = HealthClient()
        next_peer.objects = copy.deepcopy(peer.objects)
        store = R2Store(client=next_peer, bucket="synthetic")
        repeated = DailySignalCollector(store)
        for day in days:
            signal_day(repeated, day)
        report = repeated.publish()
    assert report["objects_written"] == 0 and report["put_attempts"] == 0
    assert next_peer.reads == [signal_key(day) for day in sorted(days)] + [QUEUE_KEY]
    assert store.operations["get"] == 4 and store.operations["put"] == 0
    assert next_peer.writes == []
    print(
        json.dumps(
            {
                "synthetic_operation_ledger": "unchanged_repeat",
                "operations": store.operations,
                "reads": next_peer.reads,
                "writes": next_peer.writes,
            }
        )
    )


def test_mixed_valid_and_failed_stream_keeps_independent_reference_drain():
    peer, source = HealthClient(), SyntheticGarmin()
    for day in ("2026-10-14", "2026-10-13", "2026-10-12"):
        source.responses["hrv", day] = {"unexpected": "PRIVATE_RESPONSE_MARKER"}
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"HEALTH_TIMEZONE": "Europe/Oslo"}, clear=True))
        _bind_source_clocks(stack)
        report = recent_health.run(
            days=3, store=R2Store(client=peer, bucket="synthetic"), garmin=source
        )
    assert report["status"] == "partial"
    assert report["streams"]["sleep"]["days_ready"] == 3
    assert report["streams"]["hrv"]["source_errors"] == 3
    assert report["baseline_refresh"]["status"] == "waiting_until_close"
    assert len(source.calls) == 6
    assert not any(key.startswith("health/hrv/") for key in peer.writes)
