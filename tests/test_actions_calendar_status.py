"""Real workflow/receipt entry and source checkpoints; synthetic producer statuses.

The v1 full producer tests remain authoritative for product bytes and budgets.
These cases isolate the public child-result boundary without rerunning cold365.
"""
import copy
import json
from datetime import UTC, datetime

import pytest
from test_fresh_night_bulk_followup import qualify
from test_history_baseline_integration import sdk_store
from test_recent_night_production import DAY, NightServer, clock

from pipeline import actions_receipt, refresh
from pipeline.reference_coverage import CalendarCoverage


@pytest.fixture
def morning(monkeypatch, tmp_path):
    clock(monkeypatch)
    for name, value in {'GITHUB_EVENT_NAME': 'schedule', 'REFRESH_EVENT': 'schedule',
                        'ACTIONS_CRON_EXPRESSION': '0 8 * * 1-5', 'GITHUB_RUN_ID': '800',
                        'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': 'a' * 40}.items():
        monkeypatch.setenv(name, value)
    server = NightServer()
    store = sdk_store(server)
    source = qualify(store, DAY, datetime(2026, 9, 30, 7, tzinfo=UTC))
    original_calls = list(source.calls)
    checkpoints = {key: value for key, value in server.objects.items() if key.startswith((
        'health/sleep/', 'health/hrv/', 'health/signals/', 'refresh/checks/v1/health/',
        'refresh/checks/v1/night/'))}
    current = {'status': 'complete', 'windows': [7, 28, 30, 90, 365],
               'selected_remaining': 0, 'producer_publication_complete': True,
               'wholeDataVerified': False, 'products_written': 10,
               'coverage': {'coverage_complete': True}}
    # Public producer-result seams: the actual parent routing and status logic
    # execute, while expensive byte/publication paths reuse the frozen v1 proof.
    monkeypatch.setattr('pipeline.fresh_night_references.refresh_current_night',
                        lambda *args, **kwargs: copy.deepcopy(current))
    calendar_result = {}
    requested = []
    # The mocked calendar producer selects no rows through the real observer.
    # Its held/pending status still controls the parent independently of this
    # explicitly empty selected-product scope.
    empty_coverage = {'coverage_complete': True, 'source_partial': False,
                      'scope': 'no_selected_calendar_products',
                      'counts': {'planned': 0, 'requested': 0, 'published': 0, 'verified': 0},
                      'rows': []}

    def calendar(store, **kwargs):
        requested.append(kwargs)
        return copy.deepcopy(calendar_result)

    monkeypatch.setattr('pipeline.night_baseline_refresh.refresh_pending', calendar)
    actual_main = refresh.main
    observed = []

    def forbidden_login():
        pytest.fail('Acknowledged current source must be reused')

    def entry():
        result = actual_main(store_factory=lambda: sdk_store(server), login=forbidden_login)
        observed.append(result)
        return result

    monkeypatch.setattr(refresh, 'main', entry)

    def invoke(value):
        calendar_result.clear()
        calendar_result.update(value)
        path = tmp_path / f'receipt-{len(observed)}.json'
        assert actions_receipt.main(['--workflow', 'refresh.yml', '--receipt', str(path), '--',
                                     '--workflow', '--diagnostics-file', str(tmp_path / 'diagnostics.json')]) == 0
        report = observed[-1]
        receipt = json.loads(path.read_bytes())
        baseline = report['stages'][0]['baseline_refresh']
        assert baseline['selected_remaining'] == 0
        assert baseline['producer_publication_complete'] is True
        assert baseline['status'] == 'complete'
        assert baseline['coverage'] == current['coverage']
        assert baseline['calendar_refresh'] == {**value, 'coverage': empty_coverage}
        assert set(requested[-1]) == {'rolling_endpoints', 'coverage_observer'}
        assert requested[-1]['rolling_endpoints'] == []
        observer = requested[-1]['coverage_observer']
        assert isinstance(observer, CalendarCoverage)
        assert observer.result() == empty_coverage
        assert calendar_result == value
        assert source.calls == original_calls
        assert all(server.objects[key] == value for key, value in checkpoints.items())
        return report, receipt

    return invoke


@pytest.mark.parametrize('calendar,expected', [
    ({'status': 'held', 'closed_pending': 1, 'held_periods': 1}, 'partial'),
    ({'status': 'bounded_pending', 'closed_pending': 1}, 'partial'),
    ({'status': 'complete', 'closed_pending': 0}, 'complete'),
    ({'status': 'waiting_until_close', 'closed_pending': 0, 'deferred_open': 2}, 'complete'),
    ({'status': 'bounded_pending', 'closed_pending': 7,
      'rolling_eligible_remaining': {'hr': {'28': {'count': 5}}},
      'discovery_eligible_remaining': {'count': 2}}, 'complete'),
    ({'status': 'held', 'closed_pending': 7, 'held_periods': 7,
      'rolling_eligible_remaining': {'hr': {'28': {'count': 5}}},
      'discovery_eligible_remaining': {'count': 2}}, 'complete'),
])
def test_requested_closed_calendar_controls_parent_and_receipt_only(morning, calendar, expected):
    report, receipt = morning(calendar)
    assert report['status'] == expected
    assert receipt['data_outcome'] == expected
    assert receipt['native_exit'] == 0 and receipt['command_outcome'] == 'success'


def test_held_calendar_resume_preserves_acknowledged_sources_and_complete_day_products(morning):
    first, receipt = morning({'status': 'held', 'closed_pending': 1, 'held_periods': 1})
    assert first['status'] == receipt['data_outcome'] == 'partial'
    second, receipt = morning({'status': 'complete', 'closed_pending': 0})
    assert second['status'] == receipt['data_outcome'] == 'complete'
