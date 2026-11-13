"""Sealed bulk packing through the actual caller, SDK store and physical meter."""
import json
import socket
from urllib.parse import unquote, urlsplit

import pytest
from test_history_baseline_integration import sdk_store
from test_reference_bulk_core import published_scope

from pipeline.history_state import Stop, encoded, read
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.r2_store import R2BudgetError
from pipeline.reference_bulk import publish_bulk


@pytest.fixture(autouse=True)
def no_external_connection(monkeypatch):
    attempted = []

    def forbidden(*args, **kwargs):
        attempted.append(True)
        raise RuntimeError('OFFLINE_NETWORK_DENIED')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    yield
    assert not attempted, 'An offline test attempted a real connection'


def bounded_store(server):
    return sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152)


@pytest.mark.parametrize('literal_variant', [False, True])
def test_bulk_fills_eight_declared_rows_even_with_fewer_window_channel_groups(tmp_path, literal_variant):
    job, scope, folder, carried, workset, server, plan, _ = published_scope(tmp_path, days=7, publish=False)
    for target in workset.targets:
        artifact = workset.product_for(target['key'])
        data = encoded(json.loads(artifact['body'])) if literal_variant else artifact['body'].encode()
        server.objects[target['key']] = (data, '"synthetic-unchanged"')
        server.metadata[target['key']] = {'content-type': 'application/json'}
    result = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                          store_factory=lambda: bounded_store(server), max_batches=1)
    terminal = json.loads(read(next((folder / 'publications').glob('*/TERMINAL.json'))))
    assert len(result['generated_targets']) == 8
    assert terminal['counts']['put'] <= 100 and terminal['body_bytes'] <= 2097152
    assert result['publication_complete'] is False
    for target in result['generated_targets']:
        artifact = workset.product_for(target['key'])
        expected = encoded(json.loads(artifact['body'])) if literal_variant else artifact['body'].encode()
        assert server.objects[target['key']][0] == expected


@pytest.mark.parametrize('long_revisions', [False, True])
def test_bulk_prices_history_and_queue_separately_without_raising_two_mib(tmp_path, long_revisions):
    job, scope, folder, carried, workset, server, plan, _ = published_scope(tmp_path, days=7, publish=False)
    originals = {}
    for target in workset.targets:
        old = json.loads(workset.product_for(target['key'])['body'])
        old['synthetic_previous'] = True
        originals[target['key']] = encoded(old)
        server.objects[target['key']] = (originals[target['key']], '"synthetic-previous"')
        server.metadata[target['key']] = {'content-type': 'application/json'}
    unrelated = {'start_date': '2027-01-01', 'end_date': '2027-01-31', 'phase': 'dirty',
                 'sources': {}, 'completed': {}, 'synthetic_padding': 'x' * 55000}
    server.objects[QUEUE_KEY] = (encoded({'schema_version': 1, 'kind': 'night-baseline-refresh-queue',
                                         'pending': {'month/2027-01': unrelated}}), '"synthetic-queue"')
    server.metadata[QUEUE_KEY] = {'content-type': 'application/json'}
    send = server.send
    if long_revisions:
        def send_with_long_revisions(request):
            response = send(request)
            if request.method == 'PUT' and response.status_code == 200:
                from history_storage_fixture import BUCKET
                key = unquote(urlsplit(request.url).path).removeprefix('/' + BUCKET + '/')
                revision = '"' + '\\' * 1022 + '"'
                server.objects[key] = (server.objects[key][0], revision)
                response.headers['etag'] = revision
            return response
        server.send = send_with_long_revisions
    result = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                          store_factory=lambda: bounded_store(server), max_batches=1)
    terminal = json.loads(read(next((folder / 'publications').glob('*/TERMINAL.json'))))
    assert len(result['generated_targets']) >= 3
    assert terminal['body_bytes'] <= 2097152 and terminal['counts']['put'] <= 100
    for target in result['generated_targets']:
        assert server.objects[target['key']][0] == workset.product_for(target['key'])['body'].encode()
        assert originals[target['key']] in [body for key, (body, _) in server.objects.items()
                                           if key.endswith('/product.json')]
    assert json.loads(server.objects[QUEUE_KEY][0])['pending']['month/2027-01'] == unrelated


def test_packed_checkpoint_does_not_republish_without_verification(tmp_path):
    job, scope, folder, carried, _, server, plan, _ = published_scope(tmp_path, days=7, publish=False)
    result = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                          store_factory=lambda: bounded_store(server), max_batches=1)
    assert len(result['generated_targets']) == 8
    before = list(server.calls)
    repeated = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
        store_factory=lambda: pytest.fail('An unverified packed checkpoint opened another client'))
    assert repeated == result and server.calls == before


def test_packed_cohort_unknown_accepted_product_stops_before_any_next_send_or_retry(tmp_path):
    from history_storage_fixture import BUCKET

    job, scope, folder, carried, _, server, plan, _ = published_scope(tmp_path, days=7, publish=False)
    send = server.send
    accepted = []
    after_unknown = []

    def lose_ack(request):
        key = unquote(urlsplit(request.url).path).removeprefix('/' + BUCKET + '/')
        if accepted:
            after_unknown.append(True)
            raise RuntimeError('synthetic request after unknown write')
        response = send(request)
        if request.method == 'PUT' and '/night-products/v1/baseline/' in key:
            accepted.append(key)
            raise RuntimeError('synthetic lost product ACK')
        return response

    server.send = lose_ack
    with pytest.raises((Stop, R2BudgetError)):
        publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                     store_factory=lambda: bounded_store(server))
    assert len(accepted) == 1 and not after_unknown
    before = list(server.calls)
    with pytest.raises(Stop):
        publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
            store_factory=lambda: pytest.fail('An unknown write opened another client'))
    assert server.calls == before


@pytest.mark.parametrize('fault', ['post_fence_source_drift', 'product_cas_conflict'])
def test_packed_cohort_preserves_fresh_post_fence_and_cas_rejection(tmp_path, fault):
    from history_storage_fixture import BUCKET

    job, scope, folder, carried, workset, server, plan, _ = published_scope(tmp_path, days=7, publish=False)
    send = server.send
    products = {target['key'] for target in workset.targets}
    written, injected = [], []
    source = next(key for key in server.objects if key.startswith('health/sleep/v1/'))

    def inject(request):
        key = unquote(urlsplit(request.url).path).removeprefix('/' + BUCKET + '/')
        if fault == 'product_cas_conflict' and request.method == 'PUT' and key in products:
            injected.append(True)
            server.mode = 'conflict'
            try:
                return send(request)
            finally:
                server.mode = 'normal'
        if fault == 'post_fence_source_drift' and written and not injected and 'list-type=2' in request.url:
            injected.append(True)
            server.objects[source] = (server.objects[source][0], '"synthetic-source-drift"')
        response = send(request)
        if request.method == 'PUT' and key in products and response.status_code == 200:
            written.append(key)
        return response

    server.send = inject
    with pytest.raises((Stop, R2BudgetError)):
        publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                     store_factory=lambda: bounded_store(server))
    assert injected
    if fault == 'post_fence_source_drift':
        assert len(written) == 8
    else:
        assert not written
    assert not (folder / 'PUBLICATION-CHECKPOINTS.jsonl').exists()
    before = list(server.calls)
    with pytest.raises(Stop):
        publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan, carried=carried,
                     store_factory=lambda: pytest.fail('Rejected cohort reopened an SDK client'))
    assert server.calls == before
