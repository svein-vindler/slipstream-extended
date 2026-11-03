"""Connection checks exercise the selected provider and never create an R2 client."""
import json
import socket

import garminconnect
import pytest
import requests

from pipeline import provider_check


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("External network or R2 client forbidden")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    import boto3
    monkeypatch.setattr(boto3, "client", blocked)
    monkeypatch.setattr(boto3.session.Session, "client", blocked)

def test_legacy_check_authenticates_identity_without_fetching_fitness(monkeypatch):
    attempts = []
    def login(sdk, *args, **kwargs):
        attempts.append(1)
        sdk.display_name = "synthetic"
    monkeypatch.setattr(garminconnect.Garmin, "login", login)
    result = provider_check.run(env={"GARMIN_PROVIDER": "legacy", "GARMINTOKENS": "{}", "GARMIN_ACCOUNT": "synthetic"})
    assert result == {"kind": "garmin-provider-check", "provider": "legacy", "status": "authenticated",
                      "fitness_data_requested": False, "r2_operations": 0}
    assert attempts == [1]

@pytest.mark.parametrize("account,expected", [("synthetic", "authenticated"), ("different", "account_mismatch")])
def test_personal_check_only_exchanges_key_and_reads_identity(monkeypatch, account, expected):
    calls = []
    def send(session, request, **kwargs):
        calls.append((request.method, request.url))
        response = requests.Response()
        response.status_code = 200
        response._content = (b"synthetic-token" if request.method == "POST" else json.dumps({"displayName": account}).encode())
        response.headers["Content-Type"] = "text/plain" if request.method == "POST" else "application/json"
        response.headers["Content-Length"] = str(len(response._content))
        import io
        response.raw = io.BytesIO(response._content)
        return response
    monkeypatch.setattr(requests.Session, "send", send)
    values = {"GARMIN_PROVIDER": "personal", "GARMIN_ACCOUNT": "synthetic", "GARMIN_PERSONAL_API_KEY": "synthetic-key"}
    if expected == "authenticated":
        assert provider_check.run(env=values)["status"] == expected
    else:
        from pipeline.sources.garmin_personal import PersonalSourceError
        with pytest.raises(PersonalSourceError) as error:
            provider_check.run(env=values)
        assert error.value.status == expected
    assert len(calls) == 2
    assert calls[0][0] == "POST" and calls[1][0] == "GET"
    assert calls[1][1].endswith("/userprofile-service/userprofile/settings")

def test_diagnostic_workflow_is_manual_and_has_no_r2_credentials():
    from pathlib import Path
    value = (Path(__file__).parents[1] / ".github/workflows/check-garmin-connection.yml").read_text()
    assert "  workflow_dispatch:" in value
    assert "schedule:" not in value and "workflow_run:" not in value
    assert "  group: garmin-sync" in value
    assert "contents: read" in value
    assert "R2_" not in value and "CLOUDFLARE_" not in value
    assert "run: python -m pipeline.provider_check" in value

def test_actual_connection_cli_stops_after_rejected_login_and_hides_identity():
    import os
    import subprocess
    import sys
    from pathlib import Path
    driver = """
import os, socket, sys
import garminconnect
from pipeline import provider_check

def blocked(*args, **kwargs): raise AssertionError("External network forbidden")
socket.socket.connect = blocked
for key in list(os.environ):
    if key.startswith(("GARMIN", "R2_", "CLOUDFLARE_")): os.environ.pop(key)
os.environ.update(GARMIN_PROVIDER="legacy", GARMINTOKENS="{}", GARMIN_ACCOUNT="synthetic")
attempts=[]
def denied(self, *args, **kwargs):
    attempts.append(1)
    raise garminconnect.GarminConnectAuthenticationError("SYNTHETIC_PRIVATE token")
garminconnect.Garmin.login=denied
sys.argv=["provider_check"]
try:
    provider_check.main()
except SystemExit as error:
    assert attempts == [1]
    raise
"""
    result = subprocess.run([sys.executable, "-B", "-c", driver], cwd=Path(__file__).parents[1],
                            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 1
    assert result.stdout == ""
    message, diagnostic_line = result.stderr.splitlines()
    assert message == "Pipeline failed: garmin_authentication_required."
    assert diagnostic_line.startswith("Pipeline diagnostic: ")
    diagnostic = json.loads(diagnostic_line.removeprefix("Pipeline diagnostic: "))
    assert diagnostic == {
        "category": "garmin_authentication_required",
        "component": "garmin",
        "operation": "authenticate",
        "stage": "cli",
        "site": diagnostic["site"],
    }
    assert set(diagnostic["site"]) == {"module", "line"}
    assert diagnostic["site"]["module"] == "pipeline.sources.garmin"
    assert type(diagnostic["site"]["line"]) is int
    assert 1 <= diagnostic["site"]["line"] <= 1_000_000
    assert not {"http_status", "phase", "reason", "physical_http_attempts"} & diagnostic.keys()
    for private in ("SYNTHETIC_PRIVATE", "synthetic", "token", "https://", "Traceback", str(Path(__file__).parents[1])):
        assert private not in result.stderr
