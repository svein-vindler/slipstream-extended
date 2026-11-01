"""Synthetic hosted failure diagnostics, without credentials or network."""
import socket

import garminconnect
import pytest

from pipeline.output_privacy import safe_cli
from pipeline.sources.garmin import _login

PRIVATE = "SYNTHETIC_PRIVATE token email@example.invalid health=71"

def test_cli_identifies_rejected_garmin_session_without_disclosing_provider_text(monkeypatch, capsys):
    def blocked(*args, **kwargs):
        raise AssertionError("Network forbidden")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    def denied(self, *args, **kwargs):
        raise garminconnect.GarminConnectAuthenticationError(PRIVATE)
    monkeypatch.setattr(garminconnect.Garmin, "login", denied)
    def operation():
        return _login(env={"GARMIN_PROVIDER": "legacy", "GARMINTOKENS": "{}"})
    with pytest.raises(SystemExit) as result:
        safe_cli(operation)()
    assert result.value.code == 1
    output = capsys.readouterr()
    assert output.err == "Pipeline failed: garmin_authentication_required.\n"
    assert PRIVATE not in output.out + output.err

@pytest.mark.parametrize("case,category", [
    ("rate", "garmin_rate_limited"), ("connection", "garmin_connection_failed"),
    ("r2_denied", "r2_access_denied"), ("r2_expired", "r2_authentication_required"),
    ("wrapped_auth", "garmin_authentication_required"),
    ("personal_auth", "garmin_authentication_required"),
    ("personal_bad_status", "operation_failed"), ("spoofed_name", "operation_failed"),
    ("unknown_private_text", "operation_failed"),
])
def test_hosted_cli_reports_only_typed_fixed_failure_categories(case, category, capsys):
    from botocore.exceptions import ClientError

    from pipeline.sources.garmin_personal import PersonalSourceError
    errors = {
        "rate": garminconnect.GarminConnectTooManyRequestsError(PRIVATE),
        "connection": garminconnect.GarminConnectConnectionError(PRIVATE),
        "r2_denied": ClientError({"Error": {"Code": "AccessDenied", "Message": PRIVATE}}, PRIVATE),
        "r2_expired": ClientError({"Error": {"Code": "ExpiredToken", "Message": PRIVATE}}, PRIVATE),
        "personal_auth": PersonalSourceError("auth_required", 401),
        "personal_bad_status": PersonalSourceError("source_error"),
        "spoofed_name": type("GarminConnectAuthenticationError", (RuntimeError,), {})(PRIVATE),
        "unknown_private_text": RuntimeError(PRIVATE + " 401 unauthorized"),
        "wrapped_auth": RuntimeError(PRIVATE),
    }
    errors["personal_bad_status"].status = {"raw": PRIVATE}
    errors["wrapped_auth"].__cause__ = garminconnect.GarminConnectAuthenticationError(PRIVATE)
    def fail():
        raise errors[case]
    with pytest.raises(SystemExit) as result:
        safe_cli(fail)()
    assert result.value.code == 1
    output = capsys.readouterr()
    assert output.err == f"Pipeline failed: {category}.\n"
    assert PRIVATE not in output.out + output.err
