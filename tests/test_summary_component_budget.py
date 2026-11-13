"""Ordinary14-day caller, real S3 SDK, synthetic transport and provider only."""
import json

import pytest
from test_history_baseline_integration import BaselineServer, sdk_store

from pipeline.night_baseline_refresh import (
    _IO,
    ComponentBudgetDeferred,
    source_capacity,
    summary_component_budget,
)
from pipeline.refresh_summaries import run
from scripts.benchmark_summary_windows import NOW, SummaryGarmin


class SummaryServer(BaselineServer):
    def send(self, request):
        # The shared HTTP fixture expects streaming even for native SDK LISTs.
        # Only the fake transport adapts this; production guards run unchanged.
        original = request.stream_output
        request.stream_output = True
        try:
            return super().send(request)
        finally:
            request.stream_output = original


def test_ordinary_fourteen_day_summary_keeps_its_existing_component_reads(tmp_path, monkeypatch):
    monkeypatch.setenv('HEALTH_TIMEZONE', 'UTC')
    peer = SummaryServer()
    store = sdk_store(peer)
    # Model prior importer/reference queue reads through the real public
    # admission helper. Each spends one real SDK GET on this same component;
    # no counter mutation or exact reconstruction of the live438-stage total.
    for _ in range(400):
        source_capacity(store, 14)
    report = run(tmp_path, store=store, client=SummaryGarmin(), now=NOW,
                 request_pause=0, force_reconcile=True)
    assert report['status'] == 'complete'
    assert report['windows']['health']['calendar_days'] == 14
    assert store.operations['get'] > 400
    assert not getattr(store.client, '_night_baseline_observer_active', False)
    (tmp_path / 'SDK-COUNTS.json').write_text(json.dumps({
        'preexisting_real_sdk_component_GETs': 400,
        'synthetic_ordinary14_stage_operations': store.operations,
        'health_calendar_days': report['windows']['health']['calendar_days'],
        'actual_network_calls': 0}), encoding='utf-8')


def test_summary_source_admission_uses_same_frame_without_resetting_or_leaking_scope():
    peer = SummaryServer()
    store = sdk_store(peer)
    with summary_component_budget(store):
        for _ in range(410):
            assert source_capacity(store, 14) == 14
    sent = len(peer.calls)
    assert store.operations['get'] == 410
    assert source_capacity(store, 1) == 0
    assert len(peer.calls) == sent  # Ordinary400 guard denies before SDK send.


@pytest.mark.parametrize('kind,limit,summary', [
    ('get', 800, True), ('get', 400, False), ('list', 100, True), ('put', 250, True),
])
def test_component_wire_ceiling_denies_next_send_and_io_uses_same_limit(kind, limit, summary):
    from contextlib import nullcontext

    peer = SummaryServer()
    peer.objects['synthetic/object.json'] = (b'{}', '"synthetic-object"')
    peer.metadata['synthetic/object.json'] = {'content-type': 'application/json'}
    store = sdk_store(peer)
    with summary_component_budget(store) if summary else nullcontext():
        io = _IO(store)
        try:
            def operation():
                if kind == 'get':
                    return store.get_snapshot('synthetic/object.json', max_bytes=16)
                if kind == 'list':
                    return store.client.list_objects_v2(Bucket=store.bucket, MaxKeys=1000)
                return store.client.put_object(Bucket=store.bucket, Key='synthetic/object.json', Body=b'{}')

            for _ in range(limit):
                operation()
            sent = len(peer.calls)
            assert sent == limit
            with pytest.raises(ComponentBudgetDeferred, match='component operation budget'):
                operation()
            assert len(peer.calls) == sent
            if kind == 'get':
                with pytest.raises(ComponentBudgetDeferred, match='Known remaining component budget'):
                    io.get_snapshot('synthetic/object.json', max_bytes=16)
                assert len(peer.calls) == sent
        finally:
            io.close()
    assert not getattr(store.client, '_night_baseline_observer_active', False)
