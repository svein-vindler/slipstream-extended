"""Synthetic SDK wire responses through public refresh, real adapters and R2 storage."""

import gzip
import hashlib
import io
import json
import socket
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

import boto3
import pytest
import requests
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from garmin_fit_sdk import Encoder
from garminconnect import Garmin

from pipeline import activity_refresh
from pipeline.r2_store import R2Store
from pipeline.refresh import MeasuredGarmin
from pipeline.sources.garmin_readonly import ReadOnlyGarmin


class Raw:
    def __init__(self, data):
        self.body = io.BytesIO(data)

    def stream(self, amt=1024, **kwargs):
        while data := self.body.read(amt):
            yield data

    def read(self, amt=None):
        return self.body.read(amt)

    def close(self):
        self.body.close()


class StorageWire:
    def __init__(self):
        self.objects = {}
        self.metadata = {}

    def send(self, request):
        target = urlsplit(request.url)
        query = parse_qs(target.query)
        key = unquote(target.path).removeprefix('/synthetic/')
        headers, body, status = {}, b'', 200
        if request.method == 'GET' and query.get('list-type') == ['2']:
            rows = ''.join('<Contents><Key>' + escape(k) + '</Key><Size>'
                           + str(len(v)) + '</Size></Contents>'
                           for k, v in self.objects.items()
                           if k.startswith(query.get('prefix', [''])[0]))
            body = ('<ListBucketResult><IsTruncated>false</IsTruncated>'
                    + rows + '</ListBucketResult>').encode()
        elif request.method == 'PUT':
            body = request.body if isinstance(request.body, bytes) else request.body.read()
            self.objects[key] = body
            self.metadata[key] = {k.lower(): v.decode() if isinstance(v, bytes) else v
                                  for k, v in request.headers.items()
                                  if k.lower() in ('content-type', 'content-encoding')}
            headers['etag'] = '"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"'
            body = b''
        elif key in self.objects:
            body = self.objects[key] if request.method == 'GET' else b''
            headers = {**self.metadata[key], 'content-length': str(len(self.objects[key]))}
        else:
            status, body = 404, b'<Error><Code>NoSuchKey</Code></Error>'
        return AWSResponse(request.url, status, headers, Raw(body))


@pytest.fixture
def runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('Unexpected network/authentication')

    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    sdk = Garmin(retry_attempts=0)
    sdk.display_name = 'synthetic-owner'
    monkeypatch.setattr(sdk.client, 'get_api_headers', lambda: {})
    monkeypatch.setattr(sdk.client, '_refresh_session', forbidden)
    detail = {'activityId': 123, 'activityName': 'PRIVATE_SYNTHETIC_RUN',
              'activityTypeDTO': {'typeKey': 'running'},
              'summaryDTO': {'startTimeLocal': '2027-01-01 00:30:00',
                             'startTimeGMT': '2026-12-31 23:30:00'}}
    start = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
    encoder = Encoder()
    encoder.write_mesg({'mesg_num': 18, 'start_time': start,
                        'timestamp': start + timedelta(minutes=30), 'sport': 'running'})
    fit = bytes(encoder.close())
    tcx = (b'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
           b'<Id>2026-12-31T23:30:00Z</Id><Lap StartTime="2026-12-31T23:30:00Z">'
           b'<TotalTimeSeconds>1800</TotalTimeSeconds><DistanceMeters>5000</DistanceMeters>'
           b'<Track><Trackpoint><Time>2026-12-31T23:30:00Z</Time><DistanceMeters>0</DistanceMeters>'
           b'</Trackpoint><Trackpoint><Time>2027-01-01T00:00:00Z</Time><DistanceMeters>5000</DistanceMeters>'
           b'</Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>')
    calls = []

    def send(method, url, **kwargs):
        calls.append((method, url))
        assert method == 'GET' and kwargs['allow_redirects'] is False
        response = requests.Response()
        response.status_code = 200
        if '/activity-service/activity/123' in url:
            response._content = json.dumps(detail).encode()
        elif '/download-service/files/activity/123' in url:
            response._content = fit
        elif '/download-service/export/tcx/activity/123' in url:
            response._content = tcx
        else:
            raise AssertionError('Unexpected synthetic source path: ' + url)
        return response

    monkeypatch.setattr(sdk.client._api_session, 'request', send)
    guarded = ReadOnlyGarmin(sdk)
    measured = MeasuredGarmin(lambda: guarded)
    source = measured.get()
    wire = StorageWire()
    client = boto3.client('s3', endpoint_url='https://synthetic.invalid',
                          aws_access_key_id='synthetic', aws_secret_access_key='synthetic',
                          region_name='auto', config=Config(
                              request_checksum_calculation='when_required',
                              retries={'total_max_attempts': 1}, s3={'addressing_style': 'path'}))
    monkeypatch.setattr(client._endpoint.http_session, 'send', wire.send)
    store = R2Store(client=client, bucket='synthetic')
    yield detail, source, store, calls, measured
    client.close()
    sdk.client._api_session.close()


def test_explicit_sdk_detail_import_is_retrievable_in_local_calendar_year(runtime, tmp_path):
    detail, source, store, calls, measured = runtime
    report = activity_refresh.run(activity_id='123', garmin=source, store=store,
                                  request_pause=0, output_dir=str(tmp_path))
    assert report['by_status'] == {'refreshed': 1}
    assert 'activities/2027/123/source-manifest.v1.json' in store.list_keys('activities/')
    canonical = json.loads(gzip.decompress(store.get('activities/2027/123/activity.v1.json')))
    assert canonical['activity']['start_time_local'] == '2027-01-01 00:30:00'
    assert canonical['activity']['start_time_gmt'] == '2026-12-31 23:30:00'
    assert measured.calls == 1 and measured.errors == 0
    assert len(calls) == 3
    assert 'startTimeLocal' not in detail


def test_hosted_supported_import_reports_fixed_status_counts_without_source_fields(runtime, tmp_path, capsys):
    _, source, store, _, _ = runtime
    activity_refresh.run(activity_id='123', garmin=source, store=store,
                         request_pause=0, output_dir=str(tmp_path))
    output = capsys.readouterr()
    report = json.loads(output.out)
    assert report['by_status'] == {'refreshed': 1}
    assert 'PRIVATE_SYNTHETIC_RUN' not in output.out + output.err
    assert '2027-01-01' not in output.out + output.err
    assert 'activities/' not in output.out + output.err


@pytest.mark.parametrize('local,gmt,expected_year', [
    ('2028-01-01 00:30:00', '2027-12-31 23:30:00', '2028'),
    (None, None, '2026'),
])
def test_detail_import_preserves_explicit_clocks_and_does_not_invent_local_time(
    runtime, tmp_path, local, gmt, expected_year,
):
    detail, source, store, _, _ = runtime
    if local is not None:
        detail['startTimeLocal'], detail['startTimeGMT'] = local, gmt
    else:
        detail['summaryDTO'].pop('startTimeLocal')
    report = activity_refresh.run(activity_id='123', garmin=source, store=store,
                                  request_pause=0, output_dir=str(tmp_path))
    assert report['by_status'] == {'refreshed': 1}
    prefix = f'activities/{expected_year}/123'
    canonical = json.loads(gzip.decompress(store.get(prefix + '/activity.v1.json')))
    assert canonical['activity']['start_time_local'] == local
    assert canonical['activity']['start_time_gmt'] == (gmt or '2026-12-31 23:30:00')


def test_explicit_unsupported_sport_is_reported_without_download_or_storage_write(runtime, tmp_path, capsys):
    detail, source, store, calls, _ = runtime
    detail['activityTypeDTO'] = {'typeKey': 'meditation'}
    report = activity_refresh.run(activity_id='123', garmin=source, store=store,
                                  request_pause=0, output_dir=str(tmp_path))
    assert report['by_status'] == {'unsupported': 1}
    assert store.list_keys('activities/') == set()
    assert len(calls) == 1 and store.operations['put'] == 0
    assert json.loads(capsys.readouterr().out)['by_status'] == {'unsupported': 1}


def test_explicit_detail_for_another_activity_is_rejected_before_import(runtime, tmp_path):
    detail, source, store, calls, _ = runtime
    detail['activityId'] = 456
    with pytest.raises(ValueError, match='requested ID'):
        activity_refresh.run(activity_id='123', garmin=source, store=store,
                             request_pause=0, output_dir=str(tmp_path))
    assert len(calls) == 1 and store.operations['put'] == 0


def test_personal_capability_outcome_is_typed_and_never_falls_back(runtime, tmp_path, capsys):
    from pipeline.sources.garmin_personal import PersonalGarmin

    _, _, store, calls, _ = runtime
    client = PersonalGarmin('synthetic-key', expected_account='synthetic-owner')
    report = activity_refresh.run(activity_id='123', garmin=client, store=store,
                                  request_pause=0, output_dir=str(tmp_path))
    assert report['status'] == 'unsupported'
    hosted = json.loads(capsys.readouterr().out)
    assert hosted['status'] == 'unsupported'
    assert hosted['garmin_provider'] == 'personal'
    assert hosted['unsupported_features'] == ['activity_detail']
    assert calls == [] and not any(store.operations.values())


def test_repeated_detail_import_is_unchanged_without_more_source_downloads(runtime, tmp_path, capsys):
    _, source, store, calls, _ = runtime
    activity_refresh.run(activity_id='123', garmin=source, store=store,
                         request_pause=0, output_dir=str(tmp_path))
    first_calls, first_puts = len(calls), store.operations['put']
    capsys.readouterr()
    report = activity_refresh.run(activity_id='123', garmin=source, store=store,
                                  request_pause=0, output_dir=str(tmp_path))
    assert report['by_status'] == {'unchanged': 1}
    assert len(calls) == first_calls + 1 and store.operations['put'] == first_puts
    assert json.loads(capsys.readouterr().out)['by_status'] == {'unchanged': 1}


def test_hosted_outcomes_reject_uncontrolled_keys_and_invalid_counts(capsys):
    from pipeline.output_privacy import log_report

    log_report('activity_refresh', {
        'by_status': {'refreshed': 1, 'unchanged': True, 'baseline': None,
                      'error': -1, 'unsupported': 2**53, 'unknown': 0,
                      'PRIVATE_SOURCE': 2},
        'garmin_provider': 'PRIVATE_PROVIDER',
        'unsupported_features': ['PRIVATE_FEATURE', 'activity_detail'],
        'activities': [{'name': 'PRIVATE_SOURCE', 'error': 'PRIVATE_ERROR'}],
    })
    output = capsys.readouterr().out
    assert json.loads(output) == {
        'kind': 'activity_refresh', 'activities_count': 1,
        'by_status': {'refreshed': 1, 'unknown': 0},
        'unsupported_features': ['activity_detail'],
    }
    assert 'PRIVATE' not in output


def test_imported_native_local_year_artifacts_feed_real_targeted_coach(runtime, tmp_path):
    from pipeline.coach_backfill import run_one

    _, source, store, _, _ = runtime
    store.put('coach/profiles/v1/2026-01-01/synthetic.json', json.dumps({
        'profile_id': 'synthetic-profile', 'effective_from': '2026-01-01',
        'zones': [{'label': 'synthetic', 'min_bpm': 100, 'max_bpm': 150}],
    }).encode(), 'application/json')
    activity_refresh.run(activity_id='123', garmin=source, store=store,
                         request_pause=0, output_dir=str(tmp_path))
    result = run_one(activity={'id': '123', 'date': '2027-01-01', 'name': 'Synthetic run',
                               'type': 'Run', 'moving_seconds': 1800}, store=store)
    assert result['blocked_activities'] == []
    assert '123' in result['processed_sources']
    pointer = json.loads(store.get('activities/2027/123/coach-input/v1/latest-ready.json'))
    analysis = json.loads(gzip.decompress(store.get(pointer['analysis_key'])))
    assert analysis['activity']['date'] == '2027-01-01'
