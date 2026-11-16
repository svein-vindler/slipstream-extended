"""Synthetic consumer-level coverage cases; no data clients or product engine."""
import pytest

from pipeline.coverage_check import check_coverage


def planned(key='synthetic/night'):
    return {'key': key, 'args': {'channel': 'hrv', 'window_days': 7},
            'kind': 'night-baseline', 'schema_version': 1,
            'method_version': 'night-baseline-v1', 'source_identity': 'source-new'}


def observed(key='synthetic/night'):
    return {**planned(key), 'revision': 'revision-one', 'sha256': 'a' * 64,
            'valid': True}


def test_missing_requested_product_cannot_complete():
    report = check_coverage([planned()], [], [])
    assert report['coverage_complete'] is False
    assert report['counts']['requested'] == 1
    assert report['rows'][0]['reason'] == 'missing_publication'


def test_success_is_private_deterministic_and_does_not_mutate_inputs():
    import copy
    import json
    inputs = ([planned()], [observed()], [observed()])
    before = copy.deepcopy(inputs)
    report = check_coverage(*inputs)
    assert report['coverage_complete'] is True
    assert report['counts'] == {'planned': 1, 'requested': 1, 'published': 1,
        'verified': 1, 'complete': 1, 'excluded': 0, 'pending': 0, 'blocked': 0,
        'source_partial': 0}
    assert report['rows'][0]['status'] == 'complete'
    assert 'synthetic/night' not in json.dumps(report)
    assert 'source-new' not in json.dumps(report)
    assert report == check_coverage(*inputs)
    assert inputs == before


@pytest.mark.parametrize('field,value,reason', [
    ('args', {'channel': 'heart_rate', 'window_days': 7}, 'wrong_variant'),
    ('kind', 'another-product', 'wrong_kind'),
    ('schema_version', 2, 'wrong_schema_version'),
    ('method_version', 'night-baseline-v0', 'wrong_method_version'),
    ('source_identity', 'source-old', 'stale_source_identity'),
    ('valid', False, 'invalid_product'),
    ('valid', None, 'invalid_product'),
    ('revision', 'old-revision', 'publication_verification_mismatch'),
    ('sha256', 'b' * 64, 'publication_verification_mismatch'),
])
def test_wrong_or_invalid_readback_cannot_complete(field, value, reason):
    bad = {**observed(), field: value}
    report = check_coverage([planned()], [observed()], [bad])
    assert report['coverage_complete'] is False
    assert report['rows'][0]['reason'] == reason


def test_source_partial_delivery_is_complete_without_claiming_full_source():
    report = check_coverage([planned()], [observed()],
                            [{**observed(), 'source_partial': True}])
    assert report['coverage_complete'] is True
    assert report['source_partial'] is True
    assert report['counts']['source_partial'] == 1
    assert 'wholeDataVerified' not in report


@pytest.mark.parametrize('reason', ['source_missing', 'unrequested', 'open_period'])
def test_explicit_unrequested_scope_is_accounted_for_separately(reason):
    plan = {**planned(), 'requested': False}
    report = check_coverage([plan], [], [],
                            explicit_exclusions=[{'key': plan['key'], 'reason': reason}])
    assert report['coverage_complete'] is True
    assert report['counts']['requested'] == 0
    assert report['counts']['excluded'] == 1
    assert report['rows'][0]['reason'] == reason


def test_recent_source_pending_is_not_a_missing_product_or_complete():
    report = check_coverage([planned()], [], [],
                            pending=[{'key': planned()['key'], 'reason': 'recent_source'}])
    assert report['coverage_complete'] is False
    assert report['counts']['pending'] == 1
    assert report['counts']['blocked'] == 0
    assert report['rows'][0]['reason'] == 'recent_source'


@pytest.mark.parametrize('case', ['duplicate_plan', 'duplicate_publication',
    'duplicate_verification', 'unexpected_product', 'excluded_requested',
    'excluded_published', 'pending_published', 'pending_excluded', 'unknown_exclusion'])
def test_duplicate_or_contradictory_evidence_blocks(case):
    plans, publications, readbacks = [planned()], [observed()], [observed()]
    exclusions, pending = [], []
    if case == 'duplicate_plan':
        plans += [{**planned(), 'method_version': 'different-version'}]
    elif case == 'duplicate_publication':
        publications += [observed()]
    elif case == 'duplicate_verification':
        readbacks += [observed()]
    elif case == 'unexpected_product':
        publications += [observed('synthetic/unplanned')]
    elif case == 'excluded_requested':
        publications, readbacks = [], []
        exclusions = [{'key': planned()['key'], 'reason': 'source_missing'}]
    elif case == 'excluded_published':
        plans[0]['requested'] = False
        exclusions = [{'key': planned()['key'], 'reason': 'source_missing'}]
    elif case == 'pending_published':
        pending = [{'key': planned()['key'], 'reason': 'recent_source'}]
    elif case == 'pending_excluded':
        plans[0]['requested'] = False
        publications, readbacks = [], []
        exclusions = [{'key': planned()['key'], 'reason': 'source_missing'}]
        pending = [{'key': planned()['key'], 'reason': 'recent_source'}]
    else:
        exclusions = [{'key': 'synthetic/unplanned', 'reason': 'source_missing'}]
    report = check_coverage(plans, publications, readbacks,
                            explicit_exclusions=exclusions, pending=pending)
    assert report['coverage_complete'] is False
    assert report['counts']['blocked'] > 0


@pytest.mark.parametrize('plans', [None, [], [{'key': 'synthetic/night'}],
    [{**planned(), 'requested': 1}], [{**planned(), 'schema_version': True}],
    [{**planned(), 'args': None}], [{**planned(), 'method_version': ''}],
    [{**planned(), 'source_identity': None}], [{**planned(), 'key': 42}]])
def test_absent_or_malformed_plan_is_fail_closed(plans):
    evidence = [{**observed(), **plans[0]}] if isinstance(plans, list) and plans else []
    report = check_coverage(plans, evidence, evidence)
    assert report['coverage_complete'] is False
    assert report['counts']['blocked'] > 0


def test_unverified_publication_and_invalid_publication_cannot_complete():
    missing = check_coverage([planned()], [observed()], [])
    invalid = check_coverage([planned()], [{**observed(), 'valid': False}], [observed()])
    assert missing['rows'][0]['reason'] == 'missing_verification'
    assert invalid['rows'][0]['reason'] == 'invalid_product'


@pytest.mark.parametrize('field,value', [('args', {'value': float('nan')}),
                                       ('source_partial', 'unknown')])
def test_malformed_evidence_is_reported_without_accepting_completion(field, value):
    report = check_coverage([planned()], [observed()], [{**observed(), field: value}])
    assert report['coverage_complete'] is False
    assert report['rows'][0]['reason'] == 'invalid_product'
