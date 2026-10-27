"""Account-checked switching uses real collector interfaces and synthetic transports."""

import pytest
from test_garmin_personal import authenticated


def test_switch_to_personal_checks_existing_account_without_legacy_auth():
    from pipeline.provider_setup import prepare_switch

    http = authenticated()
    env = {
        "GARMIN_PROVIDER": "legacy",
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMIN_PERSONAL_API_KEY": "synthetic-key",
        "GARMINTOKENS_FILE": "must-not-be-read",
    }
    assert prepare_switch("personal", env, session=http) == {
        "schema_version": 1,
        "provider": "personal",
        "account": "synthetic-account",
    }
    assert len(http.sent) == 2


def test_switch_rejects_a_different_personal_account():
    from pipeline.provider_setup import prepare_switch
    from pipeline.sources.garmin_personal import PersonalSourceError

    http = authenticated(account="another-synthetic-account")
    env = {
        "GARMIN_PROVIDER": "legacy",
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMIN_PERSONAL_API_KEY": "synthetic-key",
    }
    with pytest.raises(PersonalSourceError):
        prepare_switch("personal", env, session=http)


def test_switch_back_uses_bound_legacy_account_without_personal_key(monkeypatch):
    from garminconnect import Garmin

    from pipeline.provider_setup import prepare_switch

    monkeypatch.setattr(
        Garmin,
        "login",
        lambda self, token: setattr(self, "display_name", "synthetic-account"),
    )
    env = {
        "GARMIN_PROVIDER": "personal",
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMINTOKENS": "synthetic-legacy-session",
    }
    assert prepare_switch("legacy", env) == {
        "schema_version": 1,
        "provider": "legacy",
        "account": "synthetic-account",
    }


def test_new_personal_install_can_bind_its_own_account_without_legacy_tokens():
    from pipeline.provider_setup import prepare_switch

    http = authenticated()
    env = {"GARMIN_PERSONAL_API_KEY": "synthetic-key"}
    assert prepare_switch("personal", env, new_install=True, session=http) == {
        "schema_version": 1,
        "provider": "personal",
        "account": "synthetic-account",
    }
    assert len(http.sent) == 2


def test_new_install_flag_cannot_replace_an_existing_account_binding():
    from pipeline.provider_setup import prepare_switch

    env = {
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMIN_PERSONAL_API_KEY": "synthetic-key",
    }
    with pytest.raises(ValueError):
        prepare_switch("personal", env, new_install=True, session=authenticated())


def test_local_configuration_is_written_only_after_account_check(tmp_path):
    import json

    from pipeline.provider_setup import configure

    path = tmp_path / ".garmin-provider.local.json"
    env = {
        "GARMIN_ACCOUNT": "synthetic-account",
        "GARMIN_PERSONAL_API_KEY": "synthetic-key",
    }
    configure("personal", path, env, session=authenticated())
    assert json.loads(path.read_text()) == {
        "schema_version": 1,
        "provider": "personal",
        "account": "synthetic-account",
    }
    assert "synthetic-key" not in path.read_text()
    previous = path.read_bytes()
    from pipeline.sources.garmin_personal import PersonalSourceError

    with pytest.raises(PersonalSourceError):
        configure(
            "personal",
            path,
            {"GARMIN_PERSONAL_API_KEY": "synthetic-key"},
            session=authenticated(account="other-account"),
        )
    assert path.read_bytes() == previous


def test_switch_does_not_save_a_choice_overridden_by_process_environment(tmp_path):
    import pytest

    from pipeline.provider_setup import configure

    with pytest.raises(ValueError, match="GARMIN_PROVIDER"):
        configure(
            "legacy",
            tmp_path / ".garmin-provider.local.json",
            {"GARMIN_PROVIDER": "personal", "GARMIN_ACCOUNT": "synthetic-account"},
        )
    assert not (tmp_path / ".garmin-provider.local.json").exists()
