"""Swagger date wrappers retain local calendar identity and explicit UTC instants."""

import pytest
from test_garmin_personal import Response, authenticated


def test_documented_heart_date_wrappers_preserve_dst_day_without_guessing_offset():
    from pipeline.sources.garmin_personal import PersonalGarmin

    raw = {
        "calendarDate": {"date": "2026-10-24T22:00:00Z", "localDate": "2026-10-25"},
        "heartRateValues": [
            [{"date": "2026-10-25T00:30:00Z", "localDate": "2026-10-25"}, 51],
            [{"date": "2026-10-25T01:30:00Z", "localDate": "2026-10-25"}, 52],
        ],
    }
    http = authenticated(("GET", "/wellness-service/wellness/dailyHeartRate", Response(raw)))
    result = PersonalGarmin(
        "synthetic-key", expected_account="synthetic-account", session=http
    ).get_heart_rates("2026-10-25")
    assert result["calendarDate"] == "2026-10-25"
    assert result["heartRateValues"][1][0] - result["heartRateValues"][0][0] == 3600000
    assert result["_slipstream_source"]["calendar_date"] == raw["calendarDate"]


def test_local_date_alone_is_not_a_utc_timestamp():
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    raw = {
        "calendarDate": {"localDate": "2026-10-25"},
        "heartRateValues": [[{"localDate": "2026-10-25"}, 51]],
    }
    http = authenticated(("GET", "/wellness-service/wellness/dailyHeartRate", Response(raw)))
    with pytest.raises(PersonalSourceError):
        PersonalGarmin(
            "synthetic-key", expected_account="synthetic-account", session=http
        ).get_heart_rates("2026-10-25")
