"""Normal refresh paths distinguish unsupported features from successful source checks."""

import json
from datetime import date

import pytest
from test_garmin_personal import HTTP, Response, authenticated


def personal(http):
    from pipeline.sources.garmin_personal import PersonalGarmin

    return PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)


@pytest.mark.parametrize("module", ["recent_body", "recent_health"])
def test_unsupported_recent_stream_has_no_source_or_storage_calls(module):
    import importlib

    class Store:
        def __getattr__(self, name):
            raise AssertionError("No storage for unsupported-only stream")

    http = HTTP([])
    kwargs = {"streams": ("sleep",)} if module == "recent_health" else {}
    result = importlib.import_module("pipeline." + module).run(
        garmin=personal(http), store=Store(), **kwargs
    )
    assert result["status"] in {"unsupported", "partial"}
    if module == "recent_health":
        assert result["streams"]["sleep"]["status"] == "unsupported"
    assert not http.sent


def test_latest_personal_night_saves_hrv_without_advancing_full_night_freshness():
    from botocore.exceptions import ClientError

    from pipeline.latest_night import run

    day = date.today().isoformat()
    raw = {
        "hrvSummary": {"calendarDate": day},
        "hrvReadings": [{"readingTimeGMT": day + "T01:00:00", "hrvValue": 47}],
    }
    http = authenticated(("GET", "/hrv-service/hrv/" + day, Response(raw)))

    class Store:
        def __init__(self):
            self.objects = {}

        def get_bounded(self, *args):
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        def put(self, key, body, *args, **kwargs):
            self.objects[key] = body

    store = Store()
    result = run(run_id="123", wake_date=day, store=store, garmin=personal(http))
    assert result["sleep_status"] == "unsupported"
    assert result["hrv_status"] == "stored"
    assert result["source_checked"] is False
    assert not any(key.startswith("refresh/checks/v1/night/") for key in store.objects)
    assert not http.rows


def test_refresh_freezes_provider_before_the_lazy_first_source_call(monkeypatch, tmp_path):
    import os

    from pipeline.refresh import MeasuredGarmin, _login

    path = tmp_path / ".garmin-provider.local.json"
    path.write_text(
        json.dumps({"schema_version": 1, "provider": "personal", "account": "synthetic-account"})
    )
    monkeypatch.setattr(
        os,
        "environ",
        {"GARMIN_PROVIDER_FILE": str(path), "GARMIN_PERSONAL_API_KEY": "synthetic-key"},
    )
    measured = MeasuredGarmin(_login)
    path.write_text(
        json.dumps({"schema_version": 1, "provider": "legacy", "account": "synthetic-account"})
    )
    assert measured.get().provider == "personal"


def test_standalone_fetch_uses_one_selected_client_for_all_summary_streams(monkeypatch, tmp_path):
    from test_provider_integration import DAY, health_http

    from pipeline import fetch
    from pipeline.sources import garmin

    http = health_http()
    http.rows.insert(2, ("GET", "/activitylist-service/activities/search/activities", Response([])))
    client = personal(http)
    selections = []

    def login():
        selections.append(1)
        return client

    monkeypatch.setattr(garmin, "_login", login)
    fetch.run(
        days_back=0,
        data_dir=str(tmp_path),
        health_start=date.fromisoformat(DAY),
        health_end=date.fromisoformat(DAY),
    )
    assert selections == [1]
    assert not http.rows


def test_personal_cannot_rewrite_historical_local_clock_objects(tmp_path):
    from pipeline.night_local_repair import apply

    http = HTTP([])
    with pytest.raises(ValueError, match="unsupported"):
        apply(
            object(), personal(http), [("sleep", "2026-10-25")], backup_dir=tmp_path / "uncreated"
        )
    assert not (tmp_path / "uncreated").exists()
    assert not http.sent
