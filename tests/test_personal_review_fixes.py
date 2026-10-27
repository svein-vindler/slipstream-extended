"""Consolidated regression cases for independent Standards/Spec findings."""

import hashlib
import io
import zipfile
from datetime import datetime, timedelta, timezone

import pytest
from test_garmin_personal import HTTP, TOKEN, Response, authenticated


def synthetic_fit():
    from garmin_fit_sdk import Encoder

    encoder = Encoder()
    instant = datetime(2026, 10, 25, 10, tzinfo=timezone.utc)
    encoder.write_mesg(
        {"mesg_num": 0, "type": "activity", "manufacturer": "development", "time_created": instant}
    )
    encoder.write_mesg(
        {
            "mesg_num": 18,
            "start_time": instant,
            "timestamp": instant + timedelta(seconds=60),
            "sport": "running",
        }
    )
    return bytes(encoder.close())


def test_capability_cli_is_offline_and_usable_for_installation(monkeypatch, capsys):
    import os

    import requests

    from pipeline.provider_setup import main

    monkeypatch.setattr(os, "environ", {"GARMIN_PROVIDER_FILE": "must-not-open"})
    monkeypatch.setattr(requests, "Session", lambda: pytest.fail("No HTTP for coverage display"))
    main(["--capabilities"])
    text = capsys.readouterr().out
    assert "Garmin Personal API" in text and "not available in this adapter" in text


def test_normal_personal_summary_refresh_crosses_real_export_and_diagnostics(tmp_path):
    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from pipeline.sources.garmin_personal import PersonalGarmin
    from scripts.benchmark_summary_windows import HealthClient

    now = datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
    first = (now.date() - timedelta(days=13)).isoformat()
    last = now.date().isoformat()
    rows = [
        ("GET", "/activitylist-service/activities/search/activities", Response([])),
        ("GET", "/usersummary-service/stats/steps/daily/" + first + "/" + last, Response([])),
        ("GET", "/sleep-service/sleep/dailySleepSummariesByDate", Response([])),
        ("GET", "/hrv-service/hrv/daily/" + first + "/" + last, Response({"hrvSummaries": []})),
        ("GET", "/wellness-service/wellness/bodyBattery/reports/daily", Response([])),
        ("GET", "/weight-service/weight/all/" + first + "/" + last, Response([])),
    ]
    for offset in reversed(range(14)):
        day = (now.date() - timedelta(days=offset)).isoformat()
        rows.extend(
            [
                (
                    "GET",
                    "/usersummary-service/usersummary/daily",
                    Response({"calendarDate": day, "totalSteps": 0}),
                ),
                (
                    "GET",
                    "/wellness-service/wellness/dailyHeartRate",
                    Response({"calendarDate": day, "heartRateValues": []}),
                ),
            ]
        )
    http = authenticated(*rows)
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = run(
        str(tmp_path),
        store=R2Store(client=HealthClient(), bucket="synthetic"),
        client=client,
        now=now,
        request_pause=0,
    )
    assert result["status"] == "complete"
    assert result["provider_diagnostics"]["garmin_api_calls"] == 34
    assert result["provider_diagnostics"]["auth_http_attempts"] == 1
    assert result["provider_diagnostics"]["identity_http_attempts"] == 1
    assert not http.rows


def test_multiple_same_day_weigh_ins_keep_all_entries_and_latest_explicit_gmt():
    from test_provider_integration import health_http

    from pipeline.sources.garmin_health import fetch
    from pipeline.sources.garmin_personal import PersonalGarmin

    day = "2026-10-25"
    rows = [
        {"calendarDate": day, "weight": 72.5, "timestampGMT": day + "T09:00:00Z"},
        {"calendarDate": day, "weight": 73.5, "timestampGMT": day + "T08:00:00Z"},
    ]
    http = health_http()
    weight = ("GET", "/weight-service/weight/all/" + day + "/" + day, Response(rows))
    http.rows[6] = weight
    http.rows.insert(2, weight)
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_weigh_ins(day, day)
    assert result["dateWeightList"] == rows
    result = fetch(
        datetime.fromisoformat(day).date(),
        datetime.fromisoformat(day).date(),
        client=client,
        request_pause=0,
    )
    assert result[0]["Weight KG"] == 72.5
    assert not http.rows


def test_invalid_response_calendar_date_is_terminal_before_later_endpoint():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    http = authenticated(
        (
            "GET",
            "/usersummary-service/stats/steps/daily/2026-10-01/2026-10-28",
            Response([{"calendarDate": "2026-10-0a", "totalSteps": 1}]),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with pytest.raises(PersonalSourceError):
        client.get_daily_steps("2026-10-01", "2026-10-28")
    with pytest.raises(PersonalSourceError):
        client.get_hrv_data("2026-10-25")
    assert len(http.sent) == 3


@pytest.mark.parametrize(
    "path,params,token,binary",
    [
        ("/usersummary-service/usersummary/daily", {"calendarDate": "2026-10-25"}, True, False),
        (TOKEN, None, False, False),
        ("/unreviewed-fitness-route", None, False, False),
        (
            "/wellness-service/wellness/dailyHeartRate",
            {"date": "2026-10-25", "method": "POST"},
            False,
            False,
        ),
        ("/wellness-service/wellness/dailyHeartRate?date=2026-10-25", None, False, False),
    ],
)
def test_shared_transport_policy_is_closed_before_credentials_or_http(path, params, token, binary):
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    class FutureAdapter(PersonalGarmin):
        def future_operation(self):
            return self._send(path, params=params, token=token, binary=binary)

    http = HTTP([])
    client = FutureAdapter("synthetic-key", expected_account="synthetic-account", session=http)
    for _ in range(2):
        with pytest.raises(PersonalSourceError):
            client.future_operation()
    assert not http.sent


@pytest.mark.parametrize("damage", [False, True], ids=["valid-fit", "corrupt-fit-crc"])
def test_original_fit_crc_and_association_evidence_are_honest(damage):
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    content = bytearray(synthetic_fit())
    if damage:
        content[-1] ^= 1
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("original.fit", bytes(content))
    body = stream.getvalue()
    http = authenticated(
        (
            "GET",
            "/download-service/files/activity/123",
            Response(body, content_type="application/zip"),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    if damage:
        with pytest.raises(PersonalSourceError):
            client.download_activity(123)
    else:
        assert client.download_activity(123) == body
        metadata = client.original_validation
        assert metadata["archive_sha256"] == hashlib.sha256(body).hexdigest()
        assert metadata["members"][0]["integrity"] == "fit_crc_verified"
        assert metadata["association"] == "request_route_only"
        assert metadata["activity_association_verified"] is False


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
def test_original_xml_rejects_dtd_entities_independent_of_encoding(encoding):
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    content = (
        '<?xml version="1.0" encoding="'
        + encoding
        + '"?><!DOCTYPE gpx [<!ENTITY x "synthetic">]><gpx>&x;</gpx>'
    ).encode(encoding)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("original.gpx", content)
    http = authenticated(
        (
            "GET",
            "/download-service/files/activity/123",
            Response(stream.getvalue(), content_type="application/zip"),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with pytest.raises(PersonalSourceError):
        client.download_activity(123)
    with pytest.raises(PersonalSourceError):
        client.get_stats("2026-10-25")
    assert len(http.sent) == 3


@pytest.mark.parametrize("kind", ["large-weight-integer", "nonfinite-json-exponent"])
def test_out_of_range_json_number_is_terminal_before_further_http(kind):
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    day = "2026-10-25"
    if kind == "large-weight-integer":
        route = "/weight-service/weight/all/" + day + "/" + day
        response = Response([{"calendarDate": day, "weight": 10**400}])
    else:
        route = "/usersummary-service/usersummary/daily"
        response = Response(b'{"calendarDate":"2026-10-25","totalSteps":1e400}')
    http = authenticated(("GET", route, response))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with pytest.raises(PersonalSourceError):
        if kind == "large-weight-integer":
            client.get_weigh_ins(day, day)
        else:
            client.get_stats(day)
    with pytest.raises(PersonalSourceError):
        client.get_hrv_data(day)
    assert len(http.sent) == 3


@pytest.mark.parametrize("extension,root", [("gpx", "gpx"), ("tcx", "TrainingCenterDatabase")])
@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
def test_original_xml_keeps_valid_encoded_original_bytes(extension, root, encoding):
    from pipeline.sources.garmin_personal import PersonalGarmin

    content = ('<?xml version="1.0" encoding="' + encoding + '"?><' + root + "/>").encode(encoding)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("original." + extension, content)
    body = stream.getvalue()
    http = authenticated(
        (
            "GET",
            "/download-service/files/activity/123",
            Response(body, content_type="application/zip"),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.download_activity(123) == body
    assert client.original_validation["members"][0]["integrity"] == "xml_root_verified"
    assert client.original_validation["activity_association_verified"] is False


def test_original_xml_depth_is_bounded_before_canonical_use():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    content = b"<gpx>" + b"<x>" * 129 + b"</x>" * 129 + b"</gpx>"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("original.gpx", content)
    http = authenticated(
        (
            "GET",
            "/download-service/files/activity/123",
            Response(stream.getvalue(), content_type="application/zip"),
        )
    )
    with pytest.raises(PersonalSourceError):
        PersonalGarmin(
            "synthetic-key", expected_account="synthetic-account", session=http
        ).download_activity(123)
