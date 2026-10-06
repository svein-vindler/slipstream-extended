"""Exercise the real pinned SDK against fake HTTP only, never a Garmin account."""
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest
from garminconnect import Garmin

from pipeline.refresh import MeasuredGarmin
from pipeline.refresh_summaries import provider_diagnostics
from pipeline.sources.garmin import _login
from pipeline.sources.garmin_readonly import (
    READ_METHODS,
    GarminReadOnlyViolation,
    ReadOnlyGarmin,
)

DAY = "2026-09-28"
PATH = "/activity-service/activity/123"


@pytest.fixture
def guarded(monkeypatch):
    sdk, calls = Garmin(retry_attempts=0), []
    sdk.display_name = "synthetic-owner"
    monkeypatch.setattr(sdk.client, "get_api_headers", lambda: {})

    def send(method, url, **kwargs):
        calls.append((method, url, kwargs))
        payload = [] if "activitylist-service" in url else {"individualStats": []}
        return SimpleNamespace(status_code=200, content=b"synthetic-export", json=lambda: payload)

    monkeypatch.setattr(sdk.client._api_session, "request", send)
    # Authentication is not used in these tests; an unexpected auth attempt
    # must fail locally instead of silently contacting a real account.
    monkeypatch.setattr(sdk.client, "_refresh_session", lambda: pytest.fail("Unexpected auth refresh"))
    return ReadOnlyGarmin(sdk), sdk, calls


@pytest.mark.parametrize("name,args", [
    ("get_activities_by_date", (DAY, DAY)),
    ("get_activity", ("123",)),
    ("get_activity_exercise_sets", (123,)),
    ("download_activity", ("123",)),
    ("get_daily_steps", (DAY, DAY)),
    ("get_sleep_daily", (DAY, DAY)),
    ("get_hrv_data_range", (DAY, DAY)),
    ("get_body_battery", (DAY, DAY)),
    ("get_weigh_ins", (DAY, DAY)),
    ("get_stats", (DAY,)),
    ("get_sleep_data", (DAY,)),
    ("get_hrv_data", (DAY,)),
    ("get_daily_weigh_ins", (DAY,)),
    ("get_heart_rates", (DAY,)),
    ("get_respiration_data", (DAY,)),
])
def test_all_current_pipeline_reads_use_the_guarded_sdk(guarded, name, args):
    client, _, calls = guarded
    getattr(client, name)(*args)
    assert calls and all(method == "GET" and options["allow_redirects"] is False
                         for method, _, options in calls)


@pytest.mark.parametrize("fmt", [Garmin.ActivityDownloadFormat.ORIGINAL,
                                 Garmin.ActivityDownloadFormat.TCX,
                                 Garmin.ActivityDownloadFormat.GPX])
def test_current_activity_export_formats_remain_readable(guarded, fmt):
    client, _, calls = guarded
    assert client.download_activity("123", dl_fmt=fmt) == b"synthetic-export"
    assert len(calls) == 1


@pytest.mark.parametrize("name", [
    "upload_activity", "import_activity", "delete_activity", "set_activity_name",
    "set_activity_type", "set_activity_exercise_sets", "add_body_composition",
    "delete_weigh_in", "schedule_workout", "get_unknown_future_method",
    "add_hydration_data", "add_weigh_in", "add_weigh_in_with_timestamps",
    "update_menstrual_daily_log", "update_menstrual_calendar", "init_menstrual_cycle_setup",
    "confirm_menstrual_period_start", "update_menstrual_settings", "unschedule_workout",
    "client", "garth", "request", "post", "put", "delete", "download", "login",
])
def test_write_low_level_and_unknown_operations_are_not_exposed(guarded, name):
    client, _, calls = guarded
    with pytest.raises(AttributeError, match="read-only"):
        getattr(client, name)
    assert not calls


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "HEAD", "GET\nPOST", "get"])
def test_write_verbs_fail_before_auth_refresh_or_network(guarded, method):
    _, sdk, calls = guarded
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client._run_request(method, PATH)
    assert not calls


@pytest.mark.parametrize("path", [
    "/upload-service/upload", "/activity-service/activity/123/delete",
    "https://connectapi.garmin.com" + PATH, "//evil.invalid" + PATH,
    PATH + "?method=DELETE", PATH + "#fragment", PATH + ";method=DELETE",
    "/activity-service/activity/../123", "/activity-service/activity/%2e%2e/123",
    "/activity-service/activity/%252e%252e/123", "/activity-service/activity/123%2fdelete",
    PATH + "\\delete", PATH + "\n", PATH + "' OR 1=1 --",
])
def test_unknown_endpoints_and_injected_path_syntax_are_rejected(guarded, path):
    client, _, calls = guarded
    with pytest.raises(GarminReadOnlyViolation):
        client.connectapi(path)
    assert not calls


@pytest.mark.parametrize("kwargs", [
    {"json": {"name": "changed"}}, {"data": "DELETE"}, {"files": {}},
    {"method": "DELETE"}, {"params": {"_method": "DELETE"}},
    {"headers": {"X-HTTP-Method-Override": "DELETE"}},
    {"headers": {"Authorization": "synthetic-not-a-real-token"}},
    {"headers": {"Accept": "*/*\r\nX-HTTP-Method-Override: DELETE"}},
    {"allow_redirects": True}, {"params": [("_method", "DELETE")]},
])
def test_bodies_and_method_overrides_are_rejected(guarded, kwargs):
    client, _, calls = guarded
    with pytest.raises((GarminReadOnlyViolation, TypeError)):
        client.connectapi(PATH, **kwargs)
    assert not calls


@pytest.mark.parametrize("value", ["123;delete", "123/../456", "123' OR 1=1 --", "１２３", "0", True])
def test_activity_id_syntax_fails_before_the_sdk(guarded, value):
    client, _, calls = guarded
    with pytest.raises(GarminReadOnlyViolation):
        client.get_activity(value)
    assert not calls


@pytest.mark.parametrize("value", ["2026-02-30", DAY + ";delete", "２０２６-０９-２８", DAY + "\n"])
def test_date_syntax_fails_before_the_sdk(guarded, value):
    client, _, calls = guarded
    with pytest.raises(GarminReadOnlyViolation):
        client.get_sleep_data(value)
    assert not calls


@pytest.mark.parametrize("method,url,kwargs", [
    ("DELETE", "https://connectapi.garmin.com" + PATH, {}),
    ("GET", "https://evil.invalid" + PATH, {}),
    ("GET", "https://connectapi.garmin.com.evil.invalid" + PATH, {}),
    ("GET", "https://connectapi.garmin.com:443" + PATH, {}),
    ("GET", "http://connectapi.garmin.com" + PATH, {}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"json": {}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"headers": {"X-HTTP-Method-Override": "DELETE"}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"headers": {"X-Method-Override": "PUT"}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"proxies": {"https": "https://evil.invalid"}}),
])
def test_http_session_cannot_bypass_the_transport_check(guarded, method, url, kwargs):
    _, sdk, calls = guarded
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client._api_session.request(method, url, **kwargs)
    assert not calls


def test_http_redirect_is_never_followed(monkeypatch):
    sdk, calls = Garmin(retry_attempts=0), []

    def redirect(method, url, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=302)

    monkeypatch.setattr(sdk.client._api_session, "request", redirect)
    ReadOnlyGarmin(sdk)
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client._api_session.request("GET", "https://connectapi.garmin.com" + PATH)
    assert len(calls) == 1 and calls[0]["allow_redirects"] is False


def test_diagnostics_wrapping_preserves_both_policy_and_call_counts(guarded):
    client, sdk, calls = guarded
    provider = MeasuredGarmin(lambda: client)
    try:
        with provider_diagnostics(provider.get()) as measured:
            client.get_activity("123")
            with pytest.raises(GarminReadOnlyViolation):
                client.connectapi("/upload-service/upload")
        assert provider.calls == measured["garmin_api_calls"] == 2
        assert provider.errors == measured["garmin_api_errors"] == 1
        assert len(calls) == 1
        with pytest.raises(GarminReadOnlyViolation):
            sdk.client.request("DELETE", "connectapi", PATH)
    finally:
        client.connectapi = provider.original


def test_shared_login_installs_guard_before_login_and_returns_facade(monkeypatch, guarded):
    _, sdk, calls = guarded
    logins = []

    def login(_token):
        logins.append(True)
        sdk.client.connectapi("/userprofile-service/socialProfile")
        sdk.client.connectapi("/userprofile-service/userprofile/user-settings")
        with pytest.raises(GarminReadOnlyViolation):
            sdk.client.post("connectapi", "/upload-service/upload", json={})

    monkeypatch.setenv("GARMINTOKENS", "synthetic-session")
    monkeypatch.setattr("garminconnect.Garmin", lambda: sdk)
    monkeypatch.setattr(sdk, "login", login)
    assert isinstance(_login(), ReadOnlyGarmin)
    assert logins == [True] and len(calls) == 2


def test_unknown_sdk_transport_fails_closed():
    with pytest.raises(GarminReadOnlyViolation):
        ReadOnlyGarmin(SimpleNamespace(client=SimpleNamespace()))


def test_allowed_read_can_renew_auth_but_rejected_write_cannot(guarded, monkeypatch):
    client, sdk, calls = guarded
    refreshed = []
    sdk.client.di_token = "synthetic-not-a-real-token"
    monkeypatch.setattr(sdk.client, "_token_expires_soon", lambda: True)
    monkeypatch.setattr(sdk.client, "_refresh_session", lambda: refreshed.append(True))
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client.post("connectapi", PATH, json={})
    assert not refreshed and not calls
    client.get_activity("123")
    assert refreshed == [True] and len(calls) == 1


def test_policy_cannot_be_disabled_by_write_configuration(monkeypatch, guarded):
    monkeypatch.setenv("MCP_WRITES_ENABLED", "true")
    monkeypatch.setenv("GARMIN_WRITES_ENABLED", "true")
    client, sdk, calls = guarded
    with pytest.raises(AttributeError):
        client.add_body_composition(75)
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client.put("connectapi", PATH, json={})
    assert not calls


def test_blocked_input_is_not_logged_by_sdk_decorators(guarded, caplog):
    client, _, calls = guarded
    marker = "synthetic-private-marker"
    with pytest.raises(GarminReadOnlyViolation):
        client.connectapi(PATH + "?token=" + marker)
    assert not calls and marker not in caplog.text


def test_pipeline_reads_are_covered_and_sdk_construction_is_centralized():
    root = Path(__file__).resolve().parents[1] / "pipeline"
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name) and node.func.id == "Garmin":
                assert path == root / "sources/garmin.py"
            if (isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in {"g", "garmin", "garmin_client"}
                    and node.func.attr.startswith(("get_", "download_"))):
                assert node.func.attr in READ_METHODS
