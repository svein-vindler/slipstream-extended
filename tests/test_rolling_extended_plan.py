"""Explicit rolling lengths through real planner/native reader and fake SDK HTTP."""
from test_history_baseline_integration import sdk_store
from test_rolling_reference_sources import seeded

from pipeline.rolling_reference_plan import plan_references


def test_normal_reference_plan_includes_seven_and365_day_lengths():
    server, _ = seeded('2026-10-31', 2)
    result = plan_references(sdk_store(server), ['2026-11-01'],
        today='2026-11-01', timezone='Europe/Oslo')
    assert {row['window_days'] for row in result['targets']} == {7, 28, 30, 90, 365}
    assert len(result['targets']) == 10


def test_job_plan_binds_explicit_window_choice_through_existing_public_api(tmp_path):
    from test_rolling_reference_cli import job_at

    from pipeline.rolling_reference_cli import create_plan, load_plan

    job = job_at(tmp_path)
    server, _ = seeded('2026-10-25', 1)
    created = create_plan(job, job.plan_sha, store=sdk_store(server), endpoints=['2026-10-25'],
        today='2026-10-25', timezone='Europe/Oslo', windows=[7])
    binding, _, _, plan = load_plan(job, job.plan_sha, created['plan_sha256'], store=sdk_store(server))
    assert binding['windows'] == plan['windows'] == [7]
    assert len(plan['targets']) == 2
    assert {row['window_days'] for row in plan['targets']} == {7}
    assert server.puts == [] and not job.status()['locked']


def test_source_change_discovers_full_year_dependency_without_scanning_on_enqueue():
    from pipeline.rolling_discovery import discovery_status, schedule_discovery

    queue = {'pending': {}}
    schedule_discovery(queue, 'hrv', '2025-01-01')
    assert discovery_status(queue, today='2026-01-01')['ranges'] == [['2025-01-01', '2025-12-31']]


def test_expanded_plan_preserves_legacy_products_partial_coverage_and_verified_cache_reuse():
    server, _ = seeded('2026-10-30', 3)
    cache = {}
    legacy = plan_references(sdk_store(server), ['2026-11-01'], today='2026-11-01',
        timezone='Europe/Oslo', snapshot_cache=cache, windows=[28, 30, 90])
    expanded = plan_references(sdk_store(server), ['2026-11-01'], today='2026-11-01',
        timezone='Europe/Oslo', snapshot_cache=cache)
    assert legacy['reads']['operations']['get'] > 0
    assert expanded['reads']['operations']['get'] == 0
    assert expanded['reads']['operations']['list'] > 0  # Cache is not a new source fence.
    assert {row['storage_key']: row for row in legacy['targets']} == {
        row['storage_key']: row for row in expanded['targets'] if row['window_days'] in {28, 30, 90}}
    for width in (7, 365):
        for row in (row for row in expanded['targets'] if row['window_days'] == width):
            assert row['expected_nights'] == width and row['contributing_nights'] == 3
            assert row['coverage'] == 3 / width and len(row['gaps']) == width - 3
    assert server.puts == []


def test_rolling_week_and_year_keep_exact_day_counts_across_leap_day():
    from pipeline.night_products import normalize_args

    week = normalize_args(end_date='2024-03-01', period='7', channel='hrv')
    year = normalize_args(end_date='2024-03-01', period='365', channel='hrv')
    assert week['start_date'] == '2024-02-24' and week['end_date'] == '2024-03-01'
    assert year['start_date'] == '2023-03-03' and year['end_date'] == '2024-03-01'


def test_guarded_publisher_does_not_admit_unbound_years_or_bounds(tmp_path):
    import pytest

    from pipeline.history_publish import StorageGuard, _ReferenceStore
    from pipeline.history_state import Stop

    server, _ = seeded('2026-10-30', 3)
    store = sdk_store(server)
    evidence = tmp_path / 'guard'
    evidence.mkdir()
    guard = StorageGuard(store, set(), evidence, {})
    reference = _ReferenceStore(store, guard)
    try:
        with pytest.raises(Stop, match='reference_prefix_outside_contract'):
            reference.list_revisions_bounded('health/hrv/2025/', max_keys=800)
        with pytest.raises(Stop, match='reference_prefix_outside_contract'):
            reference.list_revisions_bounded('health/hrv/2025/', max_keys=None)
        reference.authorize_annual_endpoints(['2026-11-01'])
        with pytest.raises(Stop, match='reference_prefix_outside_contract'):
            reference.list_revisions_bounded('health/hrv/2024/', max_keys=800)
        with pytest.raises(Stop, match='reference_prefix_outside_contract'):
            reference.list_revisions_bounded('health/hrv/2025/', max_keys=801)
        with pytest.raises(Stop, match='annual_reference_endpoint_bound'):
            reference.authorize_annual_endpoints(['2026-11-09'])
        assert server.calls == [] and server.puts == []
    finally:
        guard.close()
        store.client.close()
