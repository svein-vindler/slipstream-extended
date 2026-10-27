"""User-visible provider selection; all credentials and payloads are synthetic."""

import pytest


def test_existing_installation_keeps_legacy_without_new_settings():
    from pipeline.sources.provider_config import selection

    assert selection({}).provider == "legacy"


@pytest.mark.parametrize("value", ["", "Personal", "auto", "legacy,personal"])
def test_invalid_provider_is_rejected_instead_of_guessing(value):
    from pipeline.sources.provider_config import selection

    with pytest.raises(ValueError, match="GARMIN_PROVIDER"):
        selection({"GARMIN_PROVIDER": value})


def test_personal_uses_only_its_key_and_freezes_choice_for_next_job(tmp_path):
    from pipeline.sources.provider_config import selection

    keyfile = tmp_path / "synthetic-key.txt"
    keyfile.write_text("synthetic-personal-key\n")
    env = {
        "GARMIN_PROVIDER": "personal",
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMIN_PERSONAL_API_KEY_FILE": str(keyfile),
        "GARMINTOKENS_FILE": str(tmp_path / "must-not-be-read"),
    }
    chosen = selection(env)
    env["GARMIN_PROVIDER"] = "legacy"
    assert chosen.provider == "personal"
    assert chosen.credential() == "synthetic-personal-key"
    assert "synthetic-personal-key" not in repr(chosen)
    assert selection(env).provider == "legacy"


def test_personal_requires_explicit_existing_account_binding():
    from pipeline.sources.provider_config import selection

    with pytest.raises(ValueError, match="GARMIN_ACCOUNT"):
        selection({"GARMIN_PROVIDER": "personal", "GARMIN_PERSONAL_API_KEY": "synthetic-key"})


def test_shared_login_selects_personal_without_reading_legacy_credentials(monkeypatch, tmp_path):
    import requests

    from pipeline.sources.garmin import _login

    monkeypatch.setenv("GARMIN_PROVIDER", "personal")
    monkeypatch.setenv("GARMIN_ACCOUNT", "synthetic-account")
    monkeypatch.setenv("GARMIN_PERSONAL_API_KEY", "synthetic-key")
    monkeypatch.setenv("GARMINTOKENS_FILE", str(tmp_path / "must-not-be-read"))
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    from test_garmin_personal import Response, authenticated

    payload = {
        "hrvSummary": {"calendarDate": "2026-10-25", "lastNightAvg": 47},
        "hrvReadings": [],
    }
    http = authenticated(("GET", "/hrv-service/hrv/2026-10-25", Response(payload)))
    monkeypatch.setattr(requests, "Session", lambda: http)
    assert _login().get_hrv_data("2026-10-25")["hrvSummary"] == payload["hrvSummary"]


def test_legacy_default_still_returns_only_guarded_reads(monkeypatch):
    from garminconnect import Garmin

    from pipeline.sources.garmin import _login

    monkeypatch.delenv("GARMIN_PROVIDER", raising=False)
    monkeypatch.delenv("GARMIN_ACCOUNT", raising=False)
    monkeypatch.delenv("GARMINTOKENS_FILE", raising=False)
    monkeypatch.setenv("GARMINTOKENS", "synthetic-legacy-token")
    monkeypatch.setattr(
        Garmin,
        "login",
        lambda self, token: setattr(self, "display_name", "synthetic-account"),
    )
    client = _login()
    with pytest.raises(AttributeError):
        client.set_activity_name("123", "forbidden")


def test_local_bootstrap_reads_only_selected_personal_key(monkeypatch, tmp_path):
    from pipeline.local_bootstrap import prepare_environment, validate_environment

    settings = tmp_path / "local.env"
    settings.write_text("")
    key = tmp_path / "personal.txt"
    key.write_text("synthetic-personal-key")
    monkeypatch.setenv("GARMIN_PROVIDER", "personal")
    monkeypatch.setenv("GARMIN_ACCOUNT", "synthetic-account")
    monkeypatch.setenv("GARMIN_PERSONAL_API_KEY_FILE", str(key))
    monkeypatch.delenv("GARMIN_PERSONAL_API_KEY", raising=False)
    monkeypatch.setenv("GARMINTOKENS_FILE", str(tmp_path / "absent-legacy-file"))
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    for name in ("CLOUDFLARE_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "synthetic-only")
    prepare_environment(settings)
    validate_environment(("hrv",))
    import os

    from pipeline.sources.provider_config import selection

    assert selection(os.environ).credential() == "synthetic-personal-key"


def test_unset_optional_account_secret_keeps_hosted_legacy_compatible():
    from pipeline.sources.provider_config import selection

    assert selection({"GARMIN_PROVIDER": "legacy", "GARMIN_ACCOUNT": ""}).provider == "legacy"


def test_local_source_choice_is_private_json_and_frozen_for_one_job(tmp_path):
    import json

    from pipeline.sources.provider_config import selection

    path = tmp_path / ".garmin-provider.local.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "provider": "personal",
                "account": "synthetic-account",
            }
        )
    )
    env = {
        "GARMIN_PROVIDER_FILE": str(path),
        "GARMIN_PERSONAL_API_KEY": "synthetic-key",
    }
    chosen = selection(env)
    path.write_text(
        json.dumps({"schema_version": 1, "provider": "legacy", "account": "synthetic-account"})
    )
    assert chosen.provider == "personal"
    assert selection(env).provider == "legacy"
    with pytest.raises(ValueError, match="conflict"):
        selection({**env, "GARMIN_PROVIDER": "personal"})


def test_legacy_serialized_json_keeps_sdk_whitespace():
    from pipeline.sources.provider_config import selection

    serialized = '{\n  "di_token": "synthetic-token", "di_refresh_token": "synthetic-refresh"\n}'
    assert selection({"GARMINTOKENS": serialized}).credential() == serialized


def test_running_local_job_keeps_its_source_when_settings_change(monkeypatch, tmp_path):
    import json

    import requests
    from test_garmin_personal import Response, authenticated

    from pipeline.local_bootstrap import LazyGarmin

    settings = tmp_path / ".garmin-provider.local.json"
    settings.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "provider": "personal",
                "account": "synthetic-account",
            }
        )
    )
    monkeypatch.delenv("GARMIN_PROVIDER", raising=False)
    monkeypatch.delenv("GARMIN_ACCOUNT", raising=False)
    monkeypatch.setenv("GARMIN_PROVIDER_FILE", str(settings))
    monkeypatch.setenv("GARMIN_PERSONAL_API_KEY", "synthetic-key")
    monkeypatch.delenv("GARMINTOKENS", raising=False)
    monkeypatch.delenv("GARMINTOKENS_FILE", raising=False)
    client = LazyGarmin()
    settings.write_text(
        json.dumps({"schema_version": 1, "provider": "legacy", "account": "synthetic-account"})
    )
    payload = {"hrvSummary": {"calendarDate": "2026-10-25"}, "hrvReadings": []}
    http = authenticated(("GET", "/hrv-service/hrv/2026-10-25", Response(payload)))
    monkeypatch.setattr(requests, "Session", lambda: http)
    assert client.get_hrv_data("2026-10-25")["_slipstream_source"]["provider"] == "personal"
    assert LazyGarmin().provider == "legacy"
    assert len(http.sent) == 3


@pytest.mark.parametrize("bound", [False, True])
def test_legacy_hrv_tracks_source_only_after_explicit_account_binding(monkeypatch, bound):
    from garminconnect import Garmin

    from pipeline.sources.garmin import _login

    payload = {"hrvSummary": {"calendarDate": "2026-10-25"}, "hrvReadings": []}
    monkeypatch.setattr(
        Garmin,
        "login",
        lambda self, token: setattr(self, "display_name", "synthetic-account"),
    )
    monkeypatch.setattr(Garmin, "get_hrv_data", lambda self, cdate: payload)
    env = {"GARMINTOKENS": "synthetic-legacy-session"}
    if bound:
        env["GARMIN_ACCOUNT"] = "synthetic-account"
    result = _login(env=env).get_hrv_data("2026-10-25")
    if bound:
        assert result["_slipstream_source"]["provider"] == "legacy"
    else:
        assert result == payload
