"""Synthetic producer reports must retain real checked source provenance."""

import pytest

from scripts.build_source_status_contract import build_case


@pytest.mark.parametrize('mode,outcome,run_id', [
    ('night', 'pending', 61),
    ('night', 'valid', 62),
    ('general', 'full_valid', 76),
])
def test_source_status_contract_uses_valid_ordinary_hr(mode, outcome, run_id):
    case = build_case(mode, outcome, run_id)

    checks = case['diagnostic']['source_checks']
    assert all(check['checked'] for check in checks)
    assert case['canonical_bytes_preserved'] is True
    if mode == 'night':
        report = case['objects'][f'refresh/reports/{run_id}.json']
        assert report['status'] == ('pending' if outcome == 'pending' else 'stored')
        assert report['heart_rate_status'] == 'stored'
    else:
        assert [check['scope'] for check in checks] == [
            'activities_summary', 'health_summary', 'sleep', 'hrv', 'heart_rate', 'body',
        ]
        assert case['diagnostic']['status'] == 'partial'
        stage = next(item for item in case['diagnostic']['stages'] if item['stage'] == 'recent_health')
        assert stage['baseline_refresh']['status'] == 'calendar_timezone_required'
        assert stage['baseline_refresh']['selected_remaining'] == 18


@pytest.mark.parametrize('mode,outcome,run_id', [
    ('night', 'heart_rate_invalid', 82),
    ('night', 'heart_rate_pending', 83),
    ('general', 'heart_rate_invalid', 84),
    ('general', 'heart_rate_error', 85),
])
def test_source_status_contract_keeps_hr_pending_separate_from_unknown(mode, outcome, run_id):
    case = build_case(mode, outcome, run_id)

    checks = case['diagnostic']['source_checks']
    if outcome == 'heart_rate_pending':
        assert all(check['checked'] for check in checks)
        assert case['objects'][f'refresh/reports/{run_id}.json']['status'] == 'pending'
    elif mode == 'night':
        assert checks == [{'scope': 'night', 'checked': False, 'checked_at': None, 'outcome': 'failed'}]
        assert case['objects'][f'refresh/reports/{run_id}.json']['heart_rate_status'] == 'import_error'
    else:
        hr = next(check for check in checks if check['scope'] == 'heart_rate')
        assert hr['checked'] is False and hr['outcome'] == 'failed'
    assert case['canonical_bytes_preserved'] is True
    if outcome != 'heart_rate_pending':
        assert case['checkpoint_bytes_preserved'][
            'refresh/checks/v1/health/signals/2026-10-14.json'] is True
