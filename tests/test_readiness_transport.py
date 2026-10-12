"""Pinned real SDK, synthetic HTTP only. No authentication or external sockets."""
import copy
import io
import socket

import pytest
import requests
from garminconnect import Garmin

from pipeline.sources import garmin_readonly, training_readiness
from pipeline.sources.garmin_readonly import GarminReadOnlyViolation, ReadOnlyGarmin
from pipeline.sources.training_readiness import MAX_BODY_BYTES

DAY = "2026-10-08"
PATH = f"/metrics-service/metrics/trainingreadiness/{DAY}"


class Raw:
    def __init__(self, data):
        self.source = io.BytesIO(data)
        self.count = 0
        self.reads = []

    def read1(self, amount, *, decode_content):
        assert decode_content is False
        self.reads.append(amount)
        chunk = self.source.read(amount)
        self.count += len(chunk)
        return chunk


class Response:
    def __init__(self, data=b"[]", status=200, headers=None):
        self.raw = Raw(data)
        self.status_code = status
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.closed = False

    def close(self):
        self.closed = True

    def json(self):
        pytest.fail("Unbounded parser used")

    @property
    def text(self):
        pytest.fail("Error text consumed")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *a, **kw: pytest.fail("External network forbidden"))


def guarded(monkeypatch, response, *, auth="DI", domain="garmin.com"):
    sdk = Garmin(is_cn=domain == "garmin.cn", retry_attempts=0)
    sdk.client.di_token = "synthetic-di" if auth == "DI" else None
    sdk.client.jwt_web = "synthetic-jwt" if auth == "JWT_WEB" else None
    sdk.client.csrf_token = "synthetic-csrf"
    calls = []
    def send(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return response
    monkeypatch.setattr(sdk.client._api_session, "request", send)
    for name in ("_refresh_session", "_refresh_di_token", "login"):
        monkeypatch.setattr(sdk.client, name, lambda *a, **kw: pytest.fail("Component auth forbidden"))
    monkeypatch.setattr(sdk.client.cs, "request", lambda *a, **kw: pytest.fail("Auth HTTP forbidden"))
    return ReadOnlyGarmin(sdk), sdk, calls


@pytest.mark.parametrize("auth", ["DI", "JWT_WEB"])
@pytest.mark.parametrize("status", [200, 401, 403, 404, 429, 500])
def test_one_fitness_zero_auth_no_token_mutation_close_before_parse(monkeypatch, auth, status, caplog):
    response = Response(b"[ ]" if status == 200 else b"private-error-secret" * 50000, status)
    reader, sdk, calls = guarded(monkeypatch, response, auth=auth)
    before = {key: copy.deepcopy(getattr(sdk.client, key)) for key in
              ("di_token", "di_refresh_token", "di_client_id", "jwt_web", "csrf_token", "_tokenstore_path")}
    original_parser = training_readiness.strict_json
    def parse(body):
        assert response.closed
        return original_parser(body)
    monkeypatch.setattr(training_readiness, "strict_json", parse)
    result = reader.get_training_readiness_bounded(DAY)
    assert len(calls) == result.metrics["fitness_http_attempts"] == 1
    assert result.metrics["component_auth_http_attempts"] == 0
    assert response.closed and result.metrics["response_closed"]
    assert result.status == ("available" if status == 200 else "auth_required" if status == 401 else "source_error")
    assert response.raw.count == (3 if status == 200 else 0)
    assert before == {key: getattr(sdk.client, key) for key in before}
    options = calls[0][2]
    assert options["stream"] is True and options["verify"] is True and options["allow_redirects"] is False
    assert options["headers"]["Accept-Encoding"] == "identity"
    assert "Authorization" in options["headers"] if auth == "DI" else "Cookie" in options["headers"]
    assert not caplog.records


@pytest.mark.parametrize("length", [MAX_BODY_BYTES - 1, MAX_BODY_BYTES, MAX_BODY_BYTES + 10])
def test_bound_before_parse_even_without_length(monkeypatch, length):
    response = Response(b"[]" + b" " * (length - 2))
    reader, _, _ = guarded(monkeypatch, response)
    result = reader.get_training_readiness_bounded(DAY)
    assert result.status == ("available" if length <= MAX_BODY_BYTES else "oversize")
    assert response.raw.count <= MAX_BODY_BYTES + 1
    assert response.closed


@pytest.mark.parametrize("headers,data,status,count", [
    ({"Content-Length": str(MAX_BODY_BYTES + 1)}, b"[]", "oversize", 0),
    ({"Content-Length": "2"}, b"[ ]", "source_error", 3),
    ({"Content-Length": "-1"}, b"[]", "source_error", 0),
    ({"Content-Encoding": "gzip"}, b"compressed-private-body", "source_error", 0),
    ({"Content-Type": "text/html"}, b"private-body", "source_error", 0),
    ({}, b"[NaN]", "schema_unverified", 5),
    ({}, b"\xff", "schema_unverified", 1),
])
def test_metadata_encoding_invalid_json(monkeypatch, headers, data, status, count):
    response = Response(data, headers=headers)
    reader, _, _ = guarded(monkeypatch, response)
    assert reader.get_training_readiness_bounded(DAY).status == status
    assert response.closed and response.raw.count == count


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_redirects_are_closed_policy_failures_without_consuming(monkeypatch, status):
    response = Response(b"private-redirect" * 10000, status, {"Location": "https://example.invalid/private"})
    reader, _, calls = guarded(monkeypatch, response)
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_training_readiness_bounded(DAY)
    assert response.closed and response.raw.count == 0 and len(calls) == 1


@pytest.mark.parametrize("route", [PATH, PATH + "?x=1", PATH + "/../upload", PATH.replace("-08", "%2d08")])
def test_generic_sdk_native_and_stream_cannot_enter_new_route(monkeypatch, route):
    reader, sdk, calls = guarded(monkeypatch, Response())
    for action in (
        lambda: reader.connectapi(route), lambda: sdk.client.connectapi(route),
        lambda: sdk.client._run_request("GET", route),
        lambda: sdk.client._api_session.request("GET", "https://connectapi.garmin.com" + route, stream=True),
        lambda: sdk.get_training_readiness(DAY),
    ):
        with pytest.raises(GarminReadOnlyViolation):
            action()
    assert not calls


@pytest.mark.parametrize("day", ["2026-02-30", "2026-10-08?x=1", "2026-10-08/../upload", None, 42])
def test_date_upper_guard_before_network(monkeypatch, day):
    reader, _, calls = guarded(monkeypatch, Response())
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_training_readiness_bounded(day)
    assert not calls


@pytest.mark.parametrize("drift", ["session", "request", "adapter", "retry", "headers", "version"])
def test_drift_fails_closed_before_send(monkeypatch, drift):
    reader, sdk, calls = guarded(monkeypatch, Response())
    session = sdk.client._api_session
    if drift == "session":
        sdk.client._api_session = requests.Session()
    elif drift == "request":
        session.request = lambda *a, **kw: pytest.fail("unguarded")
    elif drift == "adapter":
        session.mount("https://", object())
    elif drift == "retry":
        session.get_adapter("https://connectapi.garmin.com").max_retries.total = 1
    elif drift == "headers":
        sdk.client.get_api_headers = lambda: {}
    else:
        monkeypatch.setattr(garmin_readonly, "version", lambda _: "0.3.18")
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_training_readiness_bounded(DAY)
    assert not calls


def test_slow_stream_budget_and_timeout_close_without_retry(monkeypatch):
    response = Response(b"[]")
    ticks = iter([0, 0, 31, 31])
    result = training_readiness.bounded_read(lambda *a: response, {}, clock=lambda: next(ticks))
    assert result.status == "timeout" and response.closed and response.raw.count == 0
    reader, _, calls = guarded(monkeypatch, response)
    def failed_read(*a, **kw):
        raise requests.Timeout("private-raw-exception")
    response.raw.read1 = failed_read
    result = reader.get_training_readiness_bounded(DAY)
    assert result.status == "source_error" and len(calls) == 1 and response.closed


def test_trickling_stream_checks_budget_between_single_reads(monkeypatch):
    response = Response()
    now = [0]
    def trickle(amount, *, decode_content):
        assert decode_content is False
        now[0] += 1
        response.raw.count += 1
        return b" "
    response.raw.read1 = trickle
    original = garmin_readonly.bounded_read
    monkeypatch.setattr(garmin_readonly, "bounded_read", lambda send, headers: original(send, headers, clock=lambda: now[0]))
    reader, _, calls = guarded(monkeypatch, response)
    result = reader.get_training_readiness_bounded(DAY)
    assert result.status == "timeout" and result.metrics["body_bytes"] == 30
    assert result.metrics["fitness_http_attempts"] == len(calls) == 1 and response.closed


def test_failed_send_is_counted_once_without_any_auth_or_retry(monkeypatch):
    sdk = Garmin(retry_attempts=0)
    sdk.client.di_token = "synthetic-di"
    calls = []
    def fail(*a, **kw):
        calls.append(1)
        raise requests.Timeout("private-error")
    monkeypatch.setattr(sdk.client._api_session, "request", fail)
    monkeypatch.setattr(sdk.client, "_refresh_session", lambda: pytest.fail("Auth forbidden"))
    result = ReadOnlyGarmin(sdk).get_training_readiness_bounded(DAY)
    assert result.status == "source_error" and result.metrics["fitness_http_attempts"] == len(calls) == 1
    assert result.metrics["component_auth_http_attempts"] == result.metrics["body_bytes"] == 0


@pytest.mark.parametrize("opened", [False, True])
def test_policy_exception_propagates_unchanged_and_closes_open_response(opened, caplog):
    policy = GarminReadOnlyViolation("fixed policy rejection")
    response = Response()
    def reject(*a, **kw):
        raise policy
    if opened:
        response.raw.read1 = reject
    with pytest.raises(GarminReadOnlyViolation) as raised:
        training_readiness.bounded_read((lambda *a: response) if opened else reject, {})
    assert raised.value is policy
    assert response.closed is opened
    assert response.raw.count == 0 and not caplog.records


def test_lower_guard_drift_inside_bounded_flow_propagates_without_send(monkeypatch, caplog):
    response = Response()
    reader, sdk, calls = guarded(monkeypatch, response)
    original = garmin_readonly.bounded_read
    def enter(send, headers):
        # Upper guard already passed. The real lower guard must reject drift
        # after that point, and the bounded reader must preserve its exception.
        sdk.client._api_session.get_adapter("https://connectapi.garmin.com").max_retries.total = 1
        return original(send, headers)
    monkeypatch.setattr(garmin_readonly, "bounded_read", enter)
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_training_readiness_bounded(DAY)
    assert not calls and not response.closed and not caplog.records


@pytest.mark.parametrize("error", [OSError("synthetic socket failure"), requests.Timeout("synthetic timeout")])
def test_genuine_network_errors_remain_controlled_and_close(error):
    response = Response()
    def failed(*a, **kw):
        raise error
    response.raw.read1 = failed
    result = training_readiness.bounded_read(lambda *a: response, {})
    assert result.status == "source_error" and response.closed and result.metrics["response_closed"]
    assert result.metrics["component_auth_http_attempts"] == result.metrics["body_bytes"] == 0


@pytest.mark.parametrize("status", [200, 401, 302])
def test_close_failure_is_fixed_and_does_not_hide_redirect_policy(monkeypatch, status, caplog):
    response = Response(status=status)
    response.close = lambda: (_ for _ in ()).throw(RuntimeError("private-close-secret"))
    reader, _, _ = guarded(monkeypatch, response)
    if status == 302:
        with pytest.raises(GarminReadOnlyViolation):
            reader.get_training_readiness_bounded(DAY)
    else:
        assert reader.get_training_readiness_bounded(DAY).status == "source_error"
    assert not caplog.records


def test_cn_fixed_host_and_missing_auth(monkeypatch):
    reader, _, calls = guarded(monkeypatch, Response(), domain="garmin.cn")
    assert reader.get_training_readiness_bounded(DAY).status == "available"
    assert calls[0][1] == f"https://connectapi.garmin.cn{PATH}"
    reader, _, calls = guarded(monkeypatch, Response(), auth="missing")
    assert reader.get_training_readiness_bounded(DAY).status == "auth_required"
    assert not calls
