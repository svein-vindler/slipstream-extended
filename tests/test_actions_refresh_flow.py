"""Scheduled refresh entry, real bounded engines, synthetic SDK/source boundaries."""
import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
from history_storage_fixture import BUCKET
from test_history_baseline_integration import sdk_store
from test_recent_night_production import DAY, Garmin, GeneralGarmin, NightServer, clock

from pipeline import refresh
from pipeline.actions_receipt_schema import configured_schedules, validate_receipt


def test_receipt_registry_exposes_current_oslo_routes_and_retains_legacy_reads():
    assert configured_schedules('refresh.yml') == (
        {'cron_expression':'15 3 * * *', 'timezone':'Europe/Oslo'},
        {'cron_expression':'0 8 * * 1-5', 'timezone':'Europe/Oslo'},
        {'cron_expression':'0 10 * * 0,6', 'timezone':'Europe/Oslo'},
    )
    assert configured_schedules('activity-refresh.yml') == (
        {'cron_expression':'0 12,18 * * *', 'timezone':'Europe/Oslo'},
    )
    value = json.loads((Path(__file__).parent / 'fixtures/actions_run/receipt.json').read_bytes())
    value.update(schema_version=2, workflow='activity-refresh.yml',
                 cron_expression='15 23 * * *', cron_timezone='UTC')
    assert validate_receipt(value) == value


@pytest.mark.parametrize('coach,expected', [('ready', 'complete'), ('no_effective_profile', 'partial')])
def test_activity_receipt_does_not_claim_pending_coach_complete(monkeypatch, tmp_path, coach, expected):
    from pipeline import actions_receipt, activity_refresh

    monkeypatch.setattr(activity_refresh, 'run', lambda **kwargs: {
        'schema_version':1, 'activities_checked':1, 'by_status':{'refreshed':1},
        'activities':[{'id':'synthetic-private-id', 'status':'refreshed', 'coach_status':coach}]})
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule')
    path = tmp_path / 'receipt.json'
    assert actions_receipt.main(['--workflow', 'activity-refresh.yml', '--receipt', str(path), '--']) == 0
    value = json.loads(path.read_bytes())
    assert value['data_outcome'] == expected
    assert 'synthetic-private-id' not in path.read_text()


def test_morning_pending_night_needs_no_summary_or_activity_history(monkeypatch, tmp_path):
    clock(monkeypatch)
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule')
    source = Garmin()
    source.responses['sleep', DAY]['dailySleepDTO']['sleepTimeSeconds'] = 0
    source.responses['hrv', DAY]['hrvReadings'] = []
    server = NightServer()
    old = b'synthetic old preserved data'
    server.objects['synthetic-history'] = (old, 'old-revision')
    report = refresh.run(refresh.RefreshRequest(scheduled=True), data_dir=tmp_path / 'data',
        store_factory=lambda: sdk_store(server), login=lambda: source,
        workflow_context={'REFRESH_EVENT':'schedule', 'ACTIONS_CRON_EXPRESSION':'0 8 * * 1-5'})
    assert [row['stage'] for row in report['stages']] == ['latest_night', 'health_indexes']
    assert report['status'] == 'partial'
    assert source.calls == [['sleep', DAY], ['hrv', DAY]]
    assert server.objects['synthetic-history'][0] == old
    assert not (tmp_path / 'data').exists()


def test_actual_workflow_entry_morning_publishes_current_references_and_reuses_checks(monkeypatch, tmp_path):
    clock(monkeypatch)
    monkeypatch.setenv('GITHUB_EVENT_NAME', 'schedule')
    monkeypatch.setenv('REFRESH_EVENT', 'schedule')
    monkeypatch.setenv('ACTIONS_CRON_EXPRESSION', '0 8 * * 1-5')
    monkeypatch.setenv('GITHUB_RUN_ID', '500')
    monkeypatch.setattr(sys, 'argv', ['refresh', '--workflow', '--data-dir', str(tmp_path / 'data'),
                                    '--diagnostics-file', str(tmp_path / 'diagnostics.json')])
    server, source = NightServer(), Garmin()
    result = refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert result['status'] == 'complete'
    reference = result['stages'][0]['baseline_refresh']
    assert reference['windows'] == [7, 28, 30, 90, 365]
    assert reference['selected_remaining'] == 0 and reference['producer_publication_complete']
    assert reference['wholeDataVerified'] is False
    assert 'calendar_refresh' in reference
    assert not (tmp_path / 'data').exists()
    original = list(source.calls)
    again = refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert again['status'] == 'complete'
    assert source.calls == original
    assert json.loads((tmp_path / 'diagnostics.json').read_bytes()) == again


@pytest.mark.parametrize('failure', ['auth', 'schema', 'transport'])
def test_morning_source_failures_remain_failed_commands(monkeypatch, tmp_path, failure):
    from garminconnect import GarminConnectAuthenticationError

    clock(monkeypatch)
    for key, value in {'GITHUB_EVENT_NAME':'schedule', 'REFRESH_EVENT':'schedule',
                       'ACTIONS_CRON_EXPRESSION':'0 10 * * 0,6', 'GITHUB_RUN_ID':'501'}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, 'argv', ['refresh', '--workflow', '--diagnostics-file', str(tmp_path / 'failed.json')])

    class Broken(Garmin):
        def get_sleep_data(self, day):
            if failure == 'auth':
                raise GarminConnectAuthenticationError('synthetic provider text')
            if failure == 'transport':
                raise TimeoutError('synthetic provider text')
            return {'unknown-schema': True}

    server, source = NightServer(), Broken()
    with pytest.raises(SystemExit) as error:
        refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert error.value.code == 1
    report = json.loads((tmp_path / 'failed.json').read_bytes())
    assert report['status'] == 'failed'
    assert report['stages'][0]['status'] == 'failed'
    assert 'synthetic provider text' not in (tmp_path / 'failed.json').read_text()


def test_morning_interrupt_after_source_checkpoints_resumes_without_source_replay(monkeypatch, tmp_path):
    clock(monkeypatch)
    for key, value in {'GITHUB_EVENT_NAME':'schedule', 'REFRESH_EVENT':'schedule',
                       'ACTIONS_CRON_EXPRESSION':'0 8 * * 1-5', 'GITHUB_RUN_ID':'502'}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, 'argv', ['refresh', '--workflow', '--diagnostics-file', str(tmp_path / 'resume.json')])
    night_key = f'refresh/checks/v1/night/{DAY}.json'

    class InterruptedRead(NightServer):
        interrupt = True

        def send(self, request):
            key = unquote(urlsplit(request.url).path).removeprefix('/' + BUCKET + '/')
            stored = self.objects.get(night_key)
            if self.interrupt and request.method == 'GET' and key == night_key and stored:
                proof = json.loads(stored[0])
                if proof.get('status') == 'stored' and all(
                    f'refresh/checks/v1/health/signals/{day}.json' in self.objects
                    for day in ('2026-09-29', DAY)
                ):
                    self.interrupt = False
                    raise KeyboardInterrupt()
            return super().send(request)

    server, source = InterruptedRead(), Garmin()
    with pytest.raises(SystemExit) as interrupted:
        refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert interrupted.value.code == 130
    calls = list(source.calls)
    acknowledged = {key: value for key, value in server.objects.items()
                    if key.startswith(('health/sleep/', 'health/hrv/', 'health/signals/', 'refresh/checks/v1/health/'))}
    assert acknowledged and not server.interrupt
    result = refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert source.calls == calls
    assert all(server.objects[key] == value for key, value in acknowledged.items())
    assert result['status'] == 'complete'
    assert result['stages'][0]['baseline_refresh']['selected_remaining'] == 0


def test_actual_night_entry_keeps_ordinary_incremental_general_pipeline(monkeypatch, tmp_path):
    clock(monkeypatch)
    for key, value in {'GITHUB_EVENT_NAME':'schedule', 'REFRESH_EVENT':'schedule',
                       'ACTIONS_CRON_EXPRESSION':'15 3 * * *', 'GITHUB_RUN_ID':'503',
                       'TRAINING_READINESS_ENABLED':'false'}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, 'argv', ['refresh', '--workflow', '--data-dir', str(tmp_path / 'data'),
                                    '--diagnostics-file', str(tmp_path / 'night.json')])
    server, source = NightServer(), GeneralGarmin()
    result = refresh.main(store_factory=lambda: sdk_store(server), login=lambda: source)
    assert [row['stage'] for row in result['stages']] == [
        'restore_summaries', 'refresh_summaries', 'recent_health', 'recent_body', 'health_indexes']
    assert (tmp_path / 'data/activities.csv').exists()
    assert (tmp_path / 'data/health_daily.csv').exists()
    assert not any(key.startswith('backfill/') for key in server.objects)
