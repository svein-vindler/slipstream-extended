"""Synthetic HTTP contract tests for the complete Personal collector interface."""

import json
from datetime import UTC
from urllib.parse import urlsplit

import pytest

HOST = "personal.connectapi.garmin.com"
TOKEN = "/personalapitoken-service/personal-api/personal-api-key/token"


class Response:
    def __init__(self, payload, *, status=200, content_type="application/json"):
        self.status_code = status
        self.body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(self.body)),
        }

    def iter_content(self, chunk_size=8192):
        yield self.body

    def close(self):
        pass


class HTTP:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sent = []

    def request(self, method, url, **kwargs):
        location = urlsplit(url)
        assert location.scheme == "https" and location.netloc == HOST
        assert kwargs["allow_redirects"] is False
        expected, path, response = self.rows.pop(0)
        self.sent.append((method, location.path, kwargs))
        assert (method, location.path) == (expected, path)
        return response


def authenticated(*rows, account="synthetic-account"):
    return HTTP(
        [
            (
                "POST",
                TOKEN,
                Response(b"synthetic-access-token", content_type="text/plain"),
            ),
            (
                "GET",
                "/userprofile-service/userprofile/settings",
                Response({"displayName": account}),
            ),
            *rows,
        ]
    )


def test_personal_hrv_uses_documented_path_and_returns_unchanged_readings():
    from pipeline.sources.garmin_personal import PersonalGarmin

    payload = {
        "hrvSummary": {"calendarDate": "2026-10-25", "lastNightAvg": 47},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:05:00", "hrvValue": 46}],
    }
    http = authenticated(("GET", "/hrv-service/hrv/2026-10-25", Response(payload)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_hrv_data("2026-10-25")
    assert result["hrvReadings"] == payload["hrvReadings"]
    assert result["hrvSummary"] == payload["hrvSummary"]
    assert not http.rows


def test_auth_failure_stops_later_reads_without_reauth_or_provider_fallback():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    http = HTTP([("POST", TOKEN, Response(b"private-provider-error", status=401))])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    for _ in range(2):
        with pytest.raises(PersonalSourceError) as failure:
            client.get_hrv_data("2026-10-25")
        assert failure.value.status == "auth_required"
        assert "private-provider-error" not in str(failure.value)
    assert len(http.sent) == 1


def test_wrong_account_is_rejected_before_any_fitness_payload_is_read():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    http = authenticated(account="another-synthetic-account")
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with pytest.raises(PersonalSourceError) as failure:
        client.get_hrv_data("2026-10-25")
    assert failure.value.status == "account_mismatch"
    assert [row[1] for row in http.sent] == [
        TOKEN,
        "/userprofile-service/userprofile/settings",
    ]


@pytest.mark.parametrize(
    "day", ["2026-02-30", "2026-10-25?method=POST", "//other-host", "2026-10-25/.."]
)
def test_invalid_input_is_rejected_before_authentication(day):
    from pipeline.sources.garmin_personal import PersonalGarmin

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with pytest.raises(ValueError):
        client.get_hrv_data(day)
    assert http.sent == []


@pytest.mark.parametrize(
    "status,category",
    [(302, "redirect_rejected"), (429, "rate_limited"), (401, "auth_required")],
)
def test_provider_rejection_stops_future_http_without_leaking_response(status, category):
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    http = authenticated(
        (
            "GET",
            "/hrv-service/hrv/2026-10-25",
            Response(b"sensitive-error-body", status=status),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    for _ in range(2):
        with pytest.raises(PersonalSourceError) as failure:
            client.get_hrv_data("2026-10-25")
        assert failure.value.status == category
        assert "sensitive-error-body" not in str(failure.value)
    assert len(http.sent) == 3


@pytest.mark.parametrize(
    "method,args,path,payload",
    [
        (
            "get_sleep_daily",
            ("2026-10-25", "2026-10-26"),
            "/sleep-service/sleep/dailySleepSummariesByDate",
            [
                {
                    "calendarDate": "2026-10-25",
                    "sleepTimeSeconds": 25200,
                    "deepSleepSeconds": 3600,
                }
            ],
        ),
        (
            "get_daily_steps",
            ("2026-10-25", "2026-10-26"),
            "/usersummary-service/stats/steps/daily/2026-10-25/2026-10-26",
            [{"calendarDate": "2026-10-25", "totalSteps": 0}],
        ),
        (
            "get_body_battery",
            ("2026-10-25", "2026-10-26"),
            "/wellness-service/wellness/bodyBattery/reports/daily",
            [
                {
                    "date": "2026-10-25",
                    "charged": 0,
                    "drained": 31,
                    "bodyBatteryValuesArray": [[1792886400000, 53]],
                }
            ],
        ),
        (
            "get_hrv_data_range",
            ("2026-10-25", "2026-10-26"),
            "/hrv-service/hrv/daily/2026-10-25/2026-10-26",
            {"hrvSummaries": [{"calendarDate": "2026-10-25", "lastNightAvg": 47}]},
        ),
        (
            "get_stats",
            ("2026-10-25",),
            "/usersummary-service/usersummary/daily",
            {"calendarDate": "2026-10-25", "totalSteps": 0, "restingHeartRate": None},
        ),
    ],
)
def test_documented_health_operations_keep_units_nulls_and_zero(method, args, path, payload):
    from pipeline.sources.garmin_personal import PersonalGarmin

    http = authenticated(("GET", path, Response(payload)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = getattr(client, method)(*args)
    if method == "get_body_battery":
        assert [
            {key: item[key] for key in expected}
            for item, expected in zip(result, payload, strict=True)
        ] == payload
    else:
        assert result == payload
    assert not http.rows


def test_documented_range_limits_are_applied_before_auth():
    from pipeline.sources.garmin_personal import PersonalGarmin

    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=HTTP([]))
    with pytest.raises(ValueError):
        client.get_daily_steps("2026-10-01", "2026-10-29")


def test_activity_page_preserves_stable_id_and_source_local_time():
    from pipeline.sources.garmin_personal import PersonalGarmin

    row = {
        "activityId": 123,
        "activityType": {"typeKey": "running"},
        "startTimeLocal": "2026-10-25 02:30:00",
        "startTimeGMT": "2026-10-25 01:30:00",
        "distance": 5000,
        "duration": 1500,
        "averageHR": None,
    }
    http = authenticated(
        ("GET", "/activitylist-service/activities/search/activities", Response([row]))
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.get_activity_page("2026-10-25", "2026-10-25") == [row]
    assert http.sent[-1][2]["params"]["limit"] == "20"


def test_activity_detail_maps_summary_without_inventing_missing_measurements():
    from pipeline.sources.garmin_personal import PersonalGarmin

    wire = {
        "activityId": 123,
        "activityTypeDTO": {"typeKey": "running"},
        "summaryDTO": {
            "startTimeGMT": "2026-10-25T01:30:00",
            "startTimeLocal": "2026-10-25T02:30:00",
            "distance": 5000,
            "duration": 1500,
            "averageHR": None,
        },
    }
    http = authenticated(("GET", "/activity-service/activity/123", Response(wire)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_activity("123")
    assert result["activityId"] == 123
    assert result["activityType"] == {"typeKey": "running"}
    assert result["distance"] == 5000
    assert result["averageHR"] is None
    assert result["summaryDTO"] == wire["summaryDTO"]


def test_original_zip_is_validated_and_returned_without_conversion():
    import io
    import zipfile

    from pipeline.sources.garmin_personal import PersonalGarmin

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        from test_personal_review_fixes import synthetic_fit

        archive.writestr("123_ACTIVITY.fit", synthetic_fit())
    original = buffer.getvalue()
    http = authenticated(
        (
            "GET",
            "/download-service/files/activity/123",
            Response(original, content_type="application/x-zip-compressed"),
        )
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.download_activity(123) == original


def test_unsupported_detail_requests_are_local_and_do_not_probe_garmin():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalUnsupported

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    for method, args in [
        ("get_sleep_data", ("2026-10-25",)),
        ("get_activity_exercise_sets", (123,)),
        ("get_daily_weigh_ins", ("2026-10-25",)),
    ]:
        with pytest.raises(PersonalUnsupported):
            getattr(client, method)(*args)
    assert http.sent == []


def test_respiration_reuses_same_day_summary_without_extra_http():
    from pipeline.sources.garmin_personal import PersonalGarmin

    payload = {
        "calendarDate": "2026-10-25",
        "avgWakingRespirationValue": 15.2,
        "lowestRespirationValue": 9.0,
        "highestRespirationValue": None,
    }
    http = authenticated(("GET", "/usersummary-service/usersummary/daily", Response(payload)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.get_stats("2026-10-25") == payload
    assert client.get_respiration_data("2026-10-25") == payload
    assert len(http.sent) == 3


def test_weight_summary_uses_documented_kilograms_without_heuristic_scaling():
    from pipeline.sources.garmin_personal import PersonalGarmin

    payload = [{"calendarDate": "2026-10-25", "weight": 72.5}]
    http = authenticated(
        ("GET", "/weight-service/weight/all/2026-10-25/2026-10-25", Response(payload))
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.get_weigh_ins("2026-10-25", "2026-10-25")["dateWeightList"][0]["weight"] == 72.5


def test_heart_rate_descriptors_are_used_instead_of_guessing_column_order():
    from pipeline.sources.garmin_personal import PersonalGarmin

    wire = {
        "calendarDate": "2026-10-25",
        "heartRateValueDescriptors": [
            {"index": 1, "key": "timestamp"},
            {"index": 0, "key": "heartrate"},
        ],
        "heartRateValues": [[48, 1792886400000], [None, 1792886520000]],
    }
    http = authenticated(("GET", "/wellness-service/wellness/dailyHeartRate", Response(wire)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_heart_rates("2026-10-25")
    assert result["heartRateValues"] == [[1792886400000, 48], [1792886520000, None]]


def test_readiness_and_raw_stress_are_explicitly_unsupported_without_http():
    from pipeline.sources.garmin_personal import PersonalGarmin

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    assert client.get_training_readiness_bounded("2026-10-25").status == "unsupported"
    assert client.get_stress_data("2026-10-25").status == "unsupported"
    assert http.sent == []


def test_provider_capabilities_distinguish_summary_from_detailed_night():
    from pipeline.sources.garmin_personal import PersonalGarmin

    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=HTTP([]))
    assert client.capabilities()["sleep_summary"] is True
    assert client.capabilities()["sleep_stages"] is False
    assert client.capabilities()["activity_detail"] is False


def test_personal_activity_import_uses_existing_identity_and_explicit_utc():
    from datetime import date, datetime

    from pipeline.sources.garmin import fetch
    from pipeline.sources.garmin_personal import PersonalGarmin

    row = {
        "activityId": 123,
        "activityType": {"typeKey": "running"},
        "startTimeLocal": "2026-10-25T02:30:00",
        "startTimeGMT": "2026-10-25T01:30:00",
        "distance": 5000,
        "duration": 1500,
    }
    http = authenticated(
        ("GET", "/activitylist-service/activities/search/activities", Response([row]))
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = fetch(days_back=0, end_date=date(2026, 10, 25), client=client)
    assert result[0].source == "garmin"
    assert result[0].source_id == "123"
    assert result[0].start == datetime(2026, 10, 25, 1, 30, tzinfo=UTC)
    assert result[0].local_start_time == "2026-10-25T02:30:00"


def test_personal_hrv_declares_provider_for_the_shared_preservation_writer():
    from pipeline.sources.garmin_personal import PersonalGarmin

    payload = {
        "hrvSummary": {"calendarDate": "2026-10-25", "lastNightAvg": 47},
        "hrvReadings": [],
    }
    http = authenticated(("GET", "/hrv-service/hrv/2026-10-25", Response(payload)))
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_hrv_data("2026-10-25")
    assert result["_slipstream_source"]["provider"] == "personal"
    assert result["hrvSummary"] == payload["hrvSummary"]


def test_body_battery_descriptors_keep_level_and_mark_interval_sampling():
    from pipeline.sources.garmin_personal import PersonalGarmin

    wire = [
        {
            "date": "2026-10-25",
            "charged": 0,
            "bodyBatteryValuesArray": [[1, 1792886400000, 2, 53]],
            "bodyBatteryValueDescriptorDTOList": [
                {
                    "bodyBatteryValueDescriptorIndex": 0,
                    "bodyBatteryValueDescriptorKey": "bodyBatteryStatus",
                },
                {
                    "bodyBatteryValueDescriptorIndex": 1,
                    "bodyBatteryValueDescriptorKey": "timestamp",
                },
                {
                    "bodyBatteryValueDescriptorIndex": 2,
                    "bodyBatteryValueDescriptorKey": "bodyBatteryVersion",
                },
                {
                    "bodyBatteryValueDescriptorIndex": 3,
                    "bodyBatteryValueDescriptorKey": "bodyBatteryLevel",
                },
            ],
        }
    ]
    http = authenticated(
        ("GET", "/wellness-service/wellness/bodyBattery/reports/daily", Response(wire))
    )
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = client.get_body_battery("2026-10-25", "2026-10-25")[0]
    assert result["bodyBatteryValuesArray"] == [[1792886400000, 53]]
    assert result["charged"] == 0
    assert result["_slipstream_source"]["sampling"] == "interval_extrema"
