"""Real SDK login with only the external physical transports synthesized."""

import base64
import io
import json
import socket
from types import SimpleNamespace

import pytest
import requests
from garminconnect import client as sdk_client

from pipeline import provider_check
from pipeline.sources.provider_diagnostic import ProviderDiagnosticError


def token(exp=4102444800):
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').decode().rstrip("=")
    return header + "." + payload + ".synthetic-signature"


def environment(exp=4102444800):
    return {
        "GARMIN_PROVIDER": "legacy",
        "GARMIN_ACCOUNT": "synthetic",
        "GARMINTOKENS": json.dumps(
            {
                "di_token": token(exp),
                "di_refresh_token": "synthetic-refresh",
                "di_client_id": "synthetic-client",
            }
        ),
    }


def response(status, value):
    body = json.dumps(value).encode()
    result = requests.Response()
    result.status_code = status
    result.headers.update({"Content-Type": "application/json", "Content-Length": str(len(body))})
    result._content = body
    result.raw = io.BytesIO(body)
    return result


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("No external network or R2 permitted")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    from pipeline.sources import provider_diagnostic

    clock = [0.0]
    monkeypatch.setattr(
        provider_diagnostic,
        "time",
        SimpleNamespace(
            monotonic=lambda: clock[0],
            sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        ),
    )
    import boto3

    monkeypatch.setattr(boto3, "client", blocked)
    monkeypatch.setattr(boto3.session.Session, "client", blocked)


def install_wire(monkeypatch, *, api_status=401, auth_status=200):
    sends = []

    def physical(method, url, kwargs):
        sends.append((method, url, kwargs))
        if method == "POST":
            if isinstance(auth_status, BaseException):
                raise auth_status
            return response(
                auth_status, {"access_token": token(), "refresh_token": "synthetic-refresh"}
            )
        if url.endswith("socialProfile"):
            if isinstance(api_status, BaseException):
                raise api_status
            status = api_status.pop(0) if isinstance(api_status, list) else api_status
            return response(status, {"displayName": "synthetic", "profileId": 7})
        return response(200, {"userData": {"measurementSystem": "metric"}})

    def send(adapter, request, **kwargs):
        return physical(request.method, request.url, kwargs)

    def post(url, **kwargs):
        return physical("POST", url, kwargs)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    if sdk_client.HAS_CFFI:
        monkeypatch.setattr(sdk_client.cffi_requests, "post", post)
    return sends


def test_real_sdk_diagnostic_stops_at_first_identity_denial_before_auth_replay(monkeypatch):
    sends = install_wire(monkeypatch)
    with pytest.raises(ProviderDiagnosticError) as error:
        provider_check.run(env=environment())
    assert error.value.evidence() == {
        "kind": "garmin-provider-check",
        "status": "stopped",
        "category": "garmin_authentication_required",
        "physical_http_attempts": 1,
        "phase": "identity_profile",
        "http_status": 401,
    }
    assert [(method, url.rsplit("/", 1)[-1]) for method, url, _ in sends] == [
        ("GET", "socialProfile")
    ]


@pytest.mark.parametrize(
    "status,category",
    [
        (403, "garmin_access_rejected"),
        (429, "garmin_rate_limited"),
        (503, "garmin_connection_failed"),
        (302, "garmin_connection_failed"),
        (requests.ReadTimeout("PRIVATE_SENTINEL"), "garmin_connection_failed"),
    ],
)
def test_real_sdk_identity_failure_stops_without_another_send(monkeypatch, status, category):
    sends = install_wire(monkeypatch, api_status=status)
    with pytest.raises(ProviderDiagnosticError) as error:
        provider_check.run(env=environment())
    evidence = error.value.evidence()
    assert len(sends) == 1
    assert evidence["physical_http_attempts"] == 1
    assert evidence["http_status"] == (status if type(status) is int else None)
    assert evidence["phase"] == "identity_profile" and evidence["category"] == category
    assert "PRIVATE_SENTINEL" not in str(error.value) + json.dumps(evidence)


def test_real_sdk_proactive_auth_before_first_denial_remains_allowed(monkeypatch):
    sends = install_wire(monkeypatch, api_status=200)
    assert provider_check.run(env=environment(exp=1))["status"] == "authenticated"
    assert [method for method, _, _ in sends] == ["POST", "GET", "GET"]
    assert sends[0][2]["allow_redirects"] is False
    assert sends[0][2]["stream"] is True


@pytest.mark.parametrize("status", [302, 401, requests.ReadTimeout("PRIVATE_AUTH_SENTINEL")])
def test_real_sdk_cffi_auth_failure_cannot_redirect_or_fall_through(monkeypatch, status):
    # This is the real Client._http_post CFFI path, not api_session or cs.
    assert sdk_client.HAS_CFFI
    sends = install_wire(monkeypatch, api_status=200, auth_status=status)
    with pytest.raises(ProviderDiagnosticError) as error:
        provider_check.run(env=environment(exp=1))
    assert len(sends) == 1 and sends[0][0] == "POST"
    assert sends[0][2]["allow_redirects"] is False
    assert sends[0][2]["timeout"] == (5, 20)
    assert error.value.evidence()["phase"] == "auth_refresh"
    assert error.value.evidence()["http_status"] == (status if type(status) is int else None)


def test_ordinary_real_sdk_login_still_refreshes_after_identity401(monkeypatch):
    from pipeline.sources.garmin import _login

    sends = install_wire(monkeypatch, api_status=[401, 200])
    result = _login(env=environment())
    assert result.account_identity == "synthetic"
    assert [method for method, _, _ in sends] == ["GET", "POST", "GET", "GET"]


def test_real_sdk_diagnostic_does_not_persist_refreshed_credentials(monkeypatch):
    import builtins
    import os

    def forbidden(*args, **kwargs):
        raise AssertionError("Credential persistence is forbidden")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(os, "open", forbidden)
    sends = install_wire(monkeypatch, api_status=200)
    assert provider_check.run(env=environment(exp=1))["status"] == "authenticated"
    assert len(sends) == 3


@pytest.mark.parametrize(
    "fault,category",
    [
        ("invalid_json", "garmin_response_unverified"),
        ("declared_oversize", "garmin_response_limit"),
        ("short_body", "garmin_response_unverified"),
    ],
)
def test_real_sdk_unverified_or_oversized_identity_stops_before_next_send(
    monkeypatch, fault, category
):
    sends = []

    def send(adapter, request, **kwargs):
        sends.append(request.method)
        result = response(200, {"displayName": "synthetic"})
        if fault == "invalid_json":
            result._content = b"not-json PRIVATE_SENTINEL"
            result.raw = io.BytesIO(result._content)
            result.headers["Content-Length"] = str(len(result._content))
        elif fault == "declared_oversize":
            result.headers["Content-Length"] = str(5 * 1024 * 1024 + 1)
        else:
            result.headers["Content-Length"] = str(len(result._content) + 1)
        return result

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with pytest.raises(ProviderDiagnosticError) as error:
        provider_check.run(env=environment())
    assert sends == ["GET"]
    assert error.value.evidence()["category"] == category
    assert error.value.evidence()["http_status"] == 200


def test_first_denial_survives_response_cleanup_failure(monkeypatch):
    sends = []

    def send(adapter, request, **kwargs):
        sends.append(request.method)
        result = response(403, {})

        def broken_close():
            raise OSError("PRIVATE_CLOSE_SENTINEL")

        result.close = broken_close
        return result

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    with pytest.raises(ProviderDiagnosticError) as error:
        provider_check.run(env=environment())
    assert sends == ["GET"]
    assert error.value.evidence()["category"] == "garmin_access_rejected"
    assert error.value.evidence()["http_status"] == 403


def assert_cli_diagnostic(line, *, category, http_status):
    assert line.startswith("Pipeline diagnostic: ")
    diagnostic = json.loads(line.removeprefix("Pipeline diagnostic: "))
    assert diagnostic == {
        "category": category,
        "component": "garmin",
        "operation": "authenticate",
        "stage": "cli",
        "reason": "identity_profile",
        "physical_http_attempts": 1,
        "http_status": http_status,
        "site": diagnostic["site"],
    }
    assert type(diagnostic["physical_http_attempts"]) is int
    assert type(diagnostic["http_status"]) is int
    assert set(diagnostic["site"]) == {"module", "line"}
    assert diagnostic["site"]["module"] == "pipeline.sources.provider_diagnostic"
    assert type(diagnostic["site"]["line"]) is int
    assert 1 <= diagnostic["site"]["line"] <= 1_000_000


def test_public_diagnostic_cli_exposes_only_closed_failure_evidence(monkeypatch, capsys):
    sends = install_wire(monkeypatch, api_status=403)
    for key in list(__import__("os").environ):
        if key.startswith("GARMIN"):
            monkeypatch.delenv(key)
    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("sys.argv", ["provider_check"])
    with pytest.raises(SystemExit) as error:
        provider_check.main()
    assert error.value.code == 1
    output = capsys.readouterr()
    evidence, message, diagnostic = output.err.splitlines()
    assert json.loads(evidence) == {
        "kind": "garmin-provider-check",
        "status": "stopped",
        "category": "garmin_access_rejected",
        "physical_http_attempts": 1,
        "phase": "identity_profile",
        "http_status": 403,
    }
    assert message == "Pipeline failed: garmin_access_rejected."
    assert_cli_diagnostic(diagnostic, category="garmin_access_rejected", http_status=403)
    assert output.out == "" and len(sends) == 1
    for private in ("synthetic", "https://", "Traceback", "PRIVATE_", "access_token", "refresh_token"):
        assert private not in output.err


def test_actual_native_cli_first_identity401_has_one_send_and_private_output():
    import os
    import subprocess
    import sys
    from pathlib import Path

    driver = r"""
import os, socket, sys
sys.path.insert(0, 'tests')
from test_provider_diagnostic import environment, response
import requests
from garminconnect import client
from pipeline import provider_check
for key in list(os.environ):
    if key.startswith(('GARMIN', 'R2_', 'CLOUDFLARE_')): os.environ.pop(key)
os.environ.update(environment())
sends=[]
def blocked(*args, **kwargs): raise AssertionError('External network forbidden')
socket.socket.connect=blocked
def send(adapter, request, **kwargs):
    sends.append(request.method)
    return response(401, {'message':'PRIVATE_NATIVE_SENTINEL'})
requests.adapters.HTTPAdapter.send=send
def auth_post(*args, **kwargs):
    sends.append('AUTH_POST')
    return response(200, {'access_token':'PRIVATE_NATIVE_SENTINEL'})
if client.HAS_CFFI: client.cffi_requests.post=auth_post
sys.argv=['provider_check']
try: provider_check.main()
except SystemExit:
    assert sends==['GET'], sends
    raise
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", driver],
        cwd=Path(__file__).parents[1],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 1 and result.stdout == ""
    evidence, message, diagnostic = result.stderr.splitlines()
    assert json.loads(evidence) == {
        "kind": "garmin-provider-check",
        "status": "stopped",
        "category": "garmin_authentication_required",
        "physical_http_attempts": 1,
        "phase": "identity_profile",
        "http_status": 401,
    }
    assert message == "Pipeline failed: garmin_authentication_required."
    assert_cli_diagnostic(diagnostic, category="garmin_authentication_required", http_status=401)
    for private in ("PRIVATE_NATIVE_SENTINEL", "synthetic", "https://", "Traceback", "access_token", "refresh_token"):
        assert private not in result.stderr
