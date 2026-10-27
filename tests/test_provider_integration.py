"""Public pipeline seams retain units and stop unsupported work without source calls."""

import json
from datetime import date
from pathlib import Path

from test_garmin_personal import HTTP, Response, authenticated

DAY = "2026-10-25"


def health_http():
    return authenticated(
        (
            "GET",
            "/usersummary-service/stats/steps/daily/" + DAY + "/" + DAY,
            Response([]),
        ),
        (
            "GET",
            "/sleep-service/sleep/dailySleepSummariesByDate",
            Response([{"calendarDate": DAY, "sleepTimeSeconds": 25200, "sleepScore": 0}]),
        ),
        (
            "GET",
            "/hrv-service/hrv/daily/" + DAY + "/" + DAY,
            Response({"hrvSummaries": []}),
        ),
        ("GET", "/wellness-service/wellness/bodyBattery/reports/daily", Response([])),
        (
            "GET",
            "/weight-service/weight/all/" + DAY + "/" + DAY,
            Response([{"calendarDate": DAY, "weight": 1000}]),
        ),
        (
            "GET",
            "/usersummary-service/usersummary/daily",
            Response({"calendarDate": DAY}),
        ),
        (
            "GET",
            "/wellness-service/wellness/dailyHeartRate",
            Response({"calendarDate": DAY, "heartRateValues": []}),
        ),
    )


def test_personal_summary_pipeline_keeps_documented_kg_and_zero_without_unsupported_errors():
    from pipeline.sources.garmin_health import fetch
    from pipeline.sources.garmin_personal import PersonalGarmin

    http = health_http()
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    diagnostics = {}
    result = fetch(
        date.fromisoformat(DAY),
        date.fromisoformat(DAY),
        client=client,
        request_pause=0,
        diagnostics=diagnostics,
        signal_sink=lambda *args: None,
        collect_stress=True,
    )
    assert result[0]["Weight KG"] == 1000
    assert result[0]["Sleep Score"] == 0
    assert diagnostics.get("source_errors", 0) == 0
    assert diagnostics["stress"]["status"] == "unsupported"
    assert not http.rows


def test_direct_refresh_is_unsupported_before_manifest_read_or_source_call(tmp_path):
    from pipeline.activity_refresh import refresh_activity
    from pipeline.sources.garmin_personal import PersonalGarmin

    class Store:
        component_timings_ms = {}

        def __getattr__(self, name):
            raise AssertionError("No storage for unsupported import")

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = refresh_activity(
        {"activityId": 123, "activityType": {"typeKey": "strength_training"}},
        garmin=client,
        store=Store(),
        existing_keys=set(),
        output=Path(tmp_path),
        force=True,
    )
    assert result["status"] == "unsupported"
    assert not http.sent


def test_unsupported_bootstrap_jobs_are_terminal_and_visible():
    from pipeline.local_bootstrap import all_phases_terminal, compact_result

    result = {
        "status": "unsupported",
        "garmin_provider": "personal",
        "unsupported_features": ["activity_detail"],
    }
    assert all_phases_terminal(("activity",), {"activity": result})
    assert compact_result("activities", result)["unsupported_features"] == ["activity_detail"]


def test_relative_provider_settings_resolve_against_local_environment_file(monkeypatch, tmp_path):
    import os

    from pipeline.local_bootstrap import prepare_environment

    monkeypatch.setattr(os, "environ", dict(os.environ))
    names = [
        "GARMIN_PROVIDER",
        "GARMIN_ACCOUNT",
        "GARMIN_PROVIDER_FILE",
        "GARMIN_PERSONAL_API_KEY",
        "GARMIN_PERSONAL_API_KEY_FILE",
    ]
    for name in names:
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".garmin-provider.local.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "provider": "personal",
                "account": "synthetic-account",
            }
        )
    )
    (tmp_path / "key.txt").write_text("synthetic-key")
    env = tmp_path / "local.env"
    env.write_text(
        "GARMIN_PROVIDER_FILE=.garmin-provider.local.json\nGARMIN_PERSONAL_API_KEY_FILE=key.txt\n"
    )
    prepare_environment(env)
    import os

    from pipeline.sources.provider_config import selection

    assert selection(os.environ).provider == "personal"
    assert selection(os.environ).credential() == "synthetic-key"


def test_personal_signal_metadata_does_not_claim_full_sampling():
    from pipeline.signal_series import DailySignalCollector

    class Store:
        pass

    sink = DailySignalCollector(Store())
    sink.collect(
        DAY,
        "body_battery",
        {
            "date": DAY,
            "bodyBatteryValuesArray": [[1792886400000, 53]],
            "_slipstream_source": {
                "provider": "personal",
                "sampling": "interval_extrema",
                "chunks": 288,
            },
        },
    )
    channel = sink.days[DAY]["body_battery"]
    assert channel["source_provider"] == "personal"
    assert channel["source_sampling"] == "interval_extrema"
    assert "source_completeness_unknown" in channel["quality_flags"]
