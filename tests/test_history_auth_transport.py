"""Real SDK0.3.20 login; only physical HTTP, clock and durable storage synthesized."""

import base64
import io
import json
import socket
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest
import requests
from garminconnect import client as sdk_client
from garminconnect.api import base as sdk_base
from garminconnect.client import tokens as sdk_tokens

from pipeline import history_source as source_module
from pipeline.history_source import HistorySource
from pipeline.history_state import Stop, digest
from pipeline.sources.provider_config import ProviderSelection

DAY = "2026-04-30"
PRIVATE = "SYNTHETIC_PRIVATE_AUTH_BODY"


def token(exp):
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return encode({"alg": "RS256"}) + "." + encode({"exp": exp}) + ".synthetic"


def response(status, data):
    body = b"" if status == 204 else data if isinstance(data, bytes) else json.dumps(data).encode()
    result = requests.Response()
    result.status_code = status
    result.headers.update({"Content-Type": "application/json", "Content-Length": str(len(body))})
    result._content = body
    result.raw = io.BytesIO(body)
    return result


class Folder(PurePosixPath):
    def exists(self):
        return False

    def absolute(self):
        return self


class Clock:
    def __init__(self):
        self.tick = 2000.0

    def monotonic(self):
        return self.tick

    def sleep(self, amount):
        self.tick += amount

    def now(self):
        return datetime(2026, 4, 30, tzinfo=UTC)


class CffiWire:
    """Physical CFFI streaming contract; cached/unbounded .content is forbidden."""

    def __init__(self, status, body, *, close_error=False):
        self.status_code = status
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}
        self.body = body
        self.closed = False
        self.close_error = close_error

    @property
    def content(self):
        raise AssertionError("CFFI content must be bounded before SDK parsing")

    def iter_content(self):
        if isinstance(self.body, Exception):
            raise self.body
        yield self.body

    def close(self):
        self.closed = True
        if self.close_error:
            raise OSError(PRIVATE)


@pytest.fixture
def fixture(monkeypatch):
    assert (
        Path(source_module.__file__).resolve()
        == Path(__file__).resolve().parents[1] / "pipeline/history_source.py"
    )

    def blocked(*args, **kwargs):
        raise AssertionError("Actual network/R2 forbidden")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    import boto3

    monkeypatch.setattr(boto3, "client", blocked)
    monkeypatch.setattr(sdk_base, "time", SimpleNamespace(sleep=lambda seconds: None))
    saved = {}

    def write(path, value):
        body = value if isinstance(value, bytes) else json.dumps(value).encode()
        saved[str(path)] = body
        return digest(body)

    monkeypatch.setattr(source_module, "write", write)
    credential = json.dumps(
        {
            "di_token": token(1),
            "di_refresh_token": "synthetic-refresh",
            "di_client_id": "synthetic-client",
        }
    )
    chosen = ProviderSelection("legacy", "synthetic", credential)
    intent = {
        "date": DAY,
        "max_http": 8,
        "max_seconds": 899,
        "streams": ["hr"],
        "archive_reload": True,
    }
    plan = {"provider": "legacy", "account_sha256": digest(b"synthetic")}
    clock = Clock()
    reader = HistorySource(plan, intent, Folder("/source/date"), chosen=chosen, clock=clock)
    sends = []
    state = SimpleNamespace(
        auth_status=401,
        auth_value={"message": PRIVATE},
        auth_error=None,
        close_error=False,
        auth_headers={},
        fitness_status=200,
        profile_status=200,
        responses=[],
        wall_now=2000,
        fitness_values=[],
        request_times=[],
    )
    monkeypatch.setattr(sdk_tokens, "time", SimpleNamespace(time=lambda: state.wall_now))

    def physical(method, url, kwargs):
        sends.append((method, url, kwargs))
        state.request_times.append(clock.tick)
        if method == "POST" and "/epoch/request/" in url:
            result = response(204, None)
            result._content = b""
            result.raw = io.BytesIO(b"")
            result.headers["Content-Length"] = "0"
            return result
        if method == "POST":
            if state.auth_error:
                raise state.auth_error
            value = state.auth_value
            body = value if isinstance(value, bytes) else json.dumps(value).encode()
            if not sdk_client.HAS_CFFI:
                return response(state.auth_status, state.auth_value)
            result = CffiWire(state.auth_status, body, close_error=state.close_error)
            result.headers.update(state.auth_headers)
            if getattr(state, "body_error", None):
                result.body = state.body_error
            state.responses.append(result)
            return result
        if url.endswith("socialProfile"):
            return response(state.profile_status, {"displayName": "synthetic", "profileId": 7})
        if url.endswith("user-settings"):
            return response(200, {"userData": {"measurementSystem": "metric"}})
        instant = int(datetime(2026, 4, 30, 12, tzinfo=UTC).timestamp() * 1000)
        if state.fitness_values:
            value = state.fitness_values.pop(0)
            status, value = value if isinstance(value, tuple) else (state.fitness_status, value)
            return response(status, value)
        return response(
            state.fitness_status,
            {"calendarDate": DAY, "userProfilePK": 7, "heartRateValues": [[instant, 60]]},
        )

    def send(adapter, request, **kwargs):
        return physical(request.method, request.url, kwargs)

    def post(url, **kwargs):
        return physical("POST", url, kwargs)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    # Exercise the installed SDK CFFI branch even on a test host without curl.
    monkeypatch.setattr(sdk_client, "HAS_CFFI", True)
    monkeypatch.setattr(sdk_client, "cffi_requests", SimpleNamespace(post=post))
    return reader, sends, saved, clock, state


def test_real_sdk_expired_session_denial_is_one_counted_auth_send_no_replay(fixture):
    reader, sends, saved, _, state = fixture
    problem = None
    try:
        reader.read("hr")
    except BaseException as error:
        problem = error
    finally:
        reader.close()
    assert [method for method, _, _ in sends] == ["POST"], (
        "First physical auth denial must stop before SDK/profile/data replay"
    )
    assert isinstance(problem, Stop) and problem.category == "denied"
    assert reader.http_count == 1 and reader.failed is True
    assert json.loads(saved["/source/date/http-01.json"])["kind"] == "identity_auth"
    assert json.loads(saved["/source/date/http-result-01.json"])["http_status"] == 401
    assert PRIVATE not in repr(saved)

    assert all(item.closed for item in state.responses)
    assert sends[0][2]["allow_redirects"] is False
    assert sends[0][2]["stream"] is True
    assert sends[0][2]["timeout"] == (5, 20)
    assert sends[0][2]["verify"] is True
    with pytest.raises(Stop) as again:
        reader.read("hr")
    assert again.value is problem
    assert len(sends) == 1


def allow_auth(state):
    state.auth_status = 200
    state.auth_value = {"access_token": token(10000000000), "refresh_token": "synthetic-refresh"}


def test_real_sdk_success_counts_cffi_auth_and_bounded_fitness_wire(fixture):
    reader, sends, saved, clock, state = fixture
    allow_auth(state)
    try:
        result = reader.read("hr")
        assert result["status"] == "ready"
        assert [method for method, _, _ in sends] == ["POST", "GET", "GET", "GET"]
        assert reader.http_count == 4
        assert clock.tick == 2004.5
        assert reader.body_bytes > len(saved["/source/date/" + result["wire"]])
        assert all(item.closed for item in state.responses)
        assert not any("access_token" in body.decode() for body in saved.values())
    finally:
        reader.close()


@pytest.mark.parametrize(
    "status,category", [(302, "failed"), (403, "denied"), (429, "rate_limited"), (500, "failed")]
)
def test_real_sdk_first_non200_auth_stops_without_redirect_or_replay(fixture, status, category):
    reader, sends, saved, _, state = fixture
    state.auth_status = status
    state.auth_headers["Location"] = "https://untrusted.invalid/private"
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == category
        assert len(sends) == reader.http_count == 1
        assert state.responses[0].closed
        assert json.loads(saved["/source/date/http-result-01.json"])["http_status"] == status
        assert sends[0][2]["allow_redirects"] is False
        assert PRIVATE not in repr(saved)
        if status == 429:
            assert problem.value.not_before == "2026-05-01T00:00:00+00:00"
    finally:
        reader.close()


def test_real_sdk_auth_transport_failure_is_sanitized_and_not_retried(fixture):
    reader, sends, saved, _, state = fixture
    state.auth_error = requests.exceptions.ReadTimeout(PRIVATE)
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "transport_failed"
        assert len(sends) == reader.http_count == 1
        assert json.loads(saved["/source/date/http-result-01.json"]) == {
            "attempt": 1,
            "http_status": None,
            "exception_class": "ReadTimeout",
        }
        assert PRIVATE not in repr(saved)
    finally:
        reader.close()


@pytest.mark.parametrize(
    "value,headers,category",
    [
        (b"not-json", {}, "response_unverified"),
        ({"access_token": None}, {}, "response_unverified"),
        ({"access_token": "synthetic"}, {"Content-Length": str(5 * 1024 * 1024 + 1)}, "body_bound"),
        ({"access_token": "synthetic"}, {"Content-Length": "1"}, "body_length"),
    ],
)
def test_real_sdk_invalid_auth_body_stops_before_profile(fixture, value, headers, category):
    reader, sends, _, _, state = fixture
    state.auth_status, state.auth_value, state.auth_headers = 200, value, headers
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == category
        assert len(sends) == reader.http_count == 1
        assert state.responses[0].closed
    finally:
        reader.close()


def test_real_sdk_auth_close_failure_preserves_first_denial(fixture):
    reader, sends, _, _, state = fixture
    state.close_error = True
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "denied"
        assert len(sends) == 1
    finally:
        reader.close()


def test_real_sdk_later_proactive_auth_is_logged_as_auth_not_fitness(fixture):
    reader, sends, saved, clock, state = fixture
    reader.chosen = ProviderSelection(
        "legacy",
        "synthetic",
        json.dumps(
            {
                "di_token": token(5000),
                "di_refresh_token": "synthetic-refresh",
                "di_client_id": "synthetic-client",
            }
        ),
    )
    allow_auth(state)
    try:
        assert reader.read("hr")["status"] == "ready"
        state.wall_now = 6000
        assert reader.read("hr")["status"] == "ready"
        assert [method for method, _, _ in sends] == ["GET", "GET", "GET", "POST", "GET"]
        assert json.loads(saved["/source/date/http-04.json"])["kind"] == "identity_auth"
        assert json.loads(saved["/source/date/http-05.json"])["kind"] == "hr"
    finally:
        reader.close()


def test_real_sdk_fitness_denial_never_refreshes_or_replays(fixture):
    reader, sends, _, _, state = fixture
    reader.chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')
    state.fitness_status = 401
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "denied"
        assert [method for method, _, _ in sends] == ["GET", "GET", "GET"]
    finally:
        reader.close()


def test_real_sdk_requests_auth_fallback_is_counted_once(fixture, monkeypatch):
    reader, sends, _, _, state = fixture
    monkeypatch.setattr(sdk_client, "HAS_CFFI", False)
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "denied"
        assert [method for method, _, _ in sends] == ["POST"]
        assert reader.http_count == 1
    finally:
        reader.close()


def test_real_sdk_auth_uses_the_same_physical_budget_before_profile(fixture):
    reader, sends, _, _, state = fixture
    allow_auth(state)
    reader.intent["max_http"] = 1
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "http_budget"
        assert len(sends) == reader.http_count == 1
    finally:
        reader.close()


def test_real_sdk_auth_respects_remaining_date_deadline_before_transport(fixture):
    reader, sends, _, clock, state = fixture
    allow_auth(state)
    clock.tick += 870
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "date_deadline"
        assert len(sends) == reader.http_count == 0
    finally:
        reader.close()


def test_real_sdk_streamed_auth_body_and_close_only_failure_are_closed(fixture):
    reader, sends, saved, _, state = fixture
    allow_auth(state)
    state.body_error = requests.exceptions.ChunkedEncodingError(PRIVATE)
    state.close_error = True
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "transport_failed"
        assert state.responses[0].closed
        assert len(sends) == reader.http_count == 1
        assert (
            json.loads(saved["/source/date/http-error-01.json"])["exception_class"]
            == "ChunkedEncodingError"
        )
        assert PRIVATE not in repr(saved)
    finally:
        reader.close()


def test_real_sdk_auth_close_only_failure_prevents_profile(fixture):
    reader, sends, _, _, state = fixture
    allow_auth(state)
    state.close_error = True
    try:
        with pytest.raises(Stop) as problem:
            reader.read("hr")
        assert problem.value.category == "transport_failed"
        assert len(sends) == reader.http_count == 1
    finally:
        reader.close()


def missing_hr():
    """Synthetic schema only; no values copied from a real response."""
    return {
        "userProfilePK": 7,
        "calendarDate": DAY,
        "startTimestampGMT": DAY + "T00:00:00.0",
        "endTimestampGMT": DAY + "T23:59:59.0",
        "startTimestampLocal": DAY + "T02:00:00.0",
        "endTimestampLocal": "2026-05-01T01:59:59.0",
        "maxHeartRate": None,
        "minHeartRate": None,
        "restingHeartRate": 50,
        "lastSevenDaysAvgRestingHeartRate": 51,
        "heartRateValueDescriptors": None,
        "heartRateValues": None,
    }


def dated_call(fixture, monkeypatch, before, after, *, reload=True, max_seconds=899):
    from pipeline import history_collect, history_state

    reader, sends, saved, clock, state = fixture
    state.fitness_values = [before, after]
    chosen = ProviderSelection("legacy", "synthetic", '{"di_token":"synthetic"}')
    root = Folder("/source")
    run = "a" * 32
    nonce = "synthetic-nonce"
    folder = root / "runs" / run / DAY

    def write(path, value):
        body = value if isinstance(value, bytes) else json.dumps(value).encode()
        saved[str(path)] = body
        return digest(body)

    def read(path, *args):
        return saved[str(path)]

    plan = {
        "schema": 1,
        "state_dir": str(root),
        "provider": "legacy",
        "account_sha256": digest(b"synthetic"),
        "targets": [{"date": DAY, "stream": "hr"}],
    }
    write(root / "plan.json", plan)
    monkeypatch.setattr(history_state, "Path", Folder)
    monkeypatch.setattr(history_state, "read", read)
    job = history_state.Job(root)
    lease = {"run": run, "plan": job.plan_sha, "phase": "collect", "nonce": digest(nonce.encode())}
    write(root / "OWNER.lock", lease)
    write(
        folder / "INTENT.json",
        {
            "run": run,
            "date": DAY,
            "streams": ["hr"],
            "plan": job.plan_sha,
            "nonce": lease["nonce"],
            "max_http": 8,
            "max_seconds": max_seconds,
            "archive_reload": reload,
        },
    )
    monkeypatch.setattr(history_collect, "read", read)
    monkeypatch.setattr(history_collect, "write", write)

    def source(plan, intent, folder):
        return HistorySource(plan, intent, folder, chosen=chosen, clock=clock)

    native = history_collect.date_phase(
        job, run, DAY, json.dumps({"nonce": nonce}), source_factory=source
    )
    return native, json.loads(saved[str(folder / "RECEIPT.json")]), folder


def test_real_sdk_explicit_null_hr_reaches_opt_in_reload_and_full_wait(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    after = {"userProfilePK": 7, "calendarDate": DAY, "heartRateValues": [[1777550400000, 60]]}
    native, receipt, folder = dated_call(fixture, monkeypatch, before, after)
    assert native == 0, receipt
    assert receipt["status"] == "complete" and receipt["targets"][0]["status"] == "ready"
    assert [method for method, _, _ in sends] == ["GET", "GET", "GET", "POST", "GET"]
    assert (
        len([url for method, url, _ in sends if method == "POST" and "/epoch/request/" in url]) == 1
    )
    assert state.request_times[4] - state.request_times[3] == 60
    assert json.loads(saved[str(folder / "hr-03.wire")]) == before
    assert json.loads(saved[str(folder / receipt["targets"][0]["wire"])]) == after
    assert receipt["HTTP"] == 5


def test_real_sdk_http200_whole_body_null_stops_before_reload(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    after = {"userProfilePK": 7, "calendarDate": DAY, "heartRateValues": [[1777550400000, 60]]}
    native, receipt, folder = dated_call(fixture, monkeypatch, None, after)
    assert native == 2, receipt
    assert receipt["status"] == "stopped" and receipt["category"] == "source_shape"
    assert [method for method, _, _ in sends] == ["GET", "GET", "GET"]
    assert receipt["HTTP"] == 3


def test_real_sdk_http204_remains_empty_and_can_reach_opt_in_reload(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    after = {"userProfilePK": 7, "calendarDate": DAY, "heartRateValues": [[1777550400000, 60]]}
    native, receipt, folder = dated_call(fixture, monkeypatch, (204, None), after)
    assert native == 0, receipt
    assert receipt["targets"][0]["status"] == "ready" and receipt["HTTP"] == 5
    assert [method for method, _, _ in sends] == ["GET", "GET", "GET", "POST", "GET"]
    assert saved[str(folder / "hr-03.wire")] == b""
    assert state.request_times[4] - state.request_times[3] == 60


def test_real_sdk_explicit_null_without_reload_is_unresolved_not_summary_curve(
    fixture, monkeypatch
):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    native, receipt, folder = dated_call(fixture, monkeypatch, before, before, reload=False)
    assert native == 0 and receipt["status"] == "complete"
    row = receipt["targets"][0]
    assert row["status"] == "empty_unresolved" and receipt["HTTP"] == 3
    assert [m for m, _, _ in sends] == ["GET", "GET", "GET"]
    assert json.loads(saved[str(folder / row["wire"])]) == before
    assert json.loads(saved[str(folder / row["view"])]) == before


def test_real_sdk_still_null_after_one_reload_remains_unresolved(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    native, receipt, folder = dated_call(fixture, monkeypatch, before, before)
    assert native == 0 and receipt["targets"][0]["status"] == "empty_unresolved"
    assert receipt["HTTP"] == 5
    assert [m for m, _, _ in sends] == ["GET", "GET", "GET", "POST", "GET"]
    assert state.request_times[4] - state.request_times[3] == 60
    assert json.loads(saved[str(folder / "hr-05.wire")])["heartRateValues"] is None


@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_real_sdk_archive_wait_stops_without_reread_or_extra_reload(fixture, monkeypatch, stop):
    reader, sends, saved, clock, state = fixture
    cancelled = False
    sleep = clock.sleep

    def interruptible_sleep(seconds):
        nonlocal cancelled
        sleep(seconds)
        if stop == "cancel" and sends and sends[-1][0] == "POST":
            cancelled = True

    monkeypatch.setattr(clock, "sleep", interruptible_sleep)
    monkeypatch.setattr(Folder, "exists", lambda path: cancelled and path.name == "CANCEL")
    before = missing_hr()
    native, receipt, folder = dated_call(
        fixture, monkeypatch, before, before, max_seconds=40 if stop == "deadline" else 899
    )
    assert native == 2 and receipt["status"] == "stopped"
    assert receipt["category"] == "cancelled_or_deadline"
    assert receipt["reload_ACK"] == 1 and receipt["HTTP"] == 4
    assert [method for method, _, _ in sends] == ["GET", "GET", "GET", "POST"]
    assert clock.monotonic() - state.request_times[3] == (1 if stop == "cancel" else 36)
    assert json.loads(saved[str(folder / "hr-03.wire")]) == before
    assert state.fitness_values == [before]


@pytest.mark.parametrize(
    "case",
    [
        "wrong_date",
        "wrong_profile",
        "missing_curve_key",
        "unknown_null_schema",
        "bad_descriptor",
        "bad_point",
        "bad_clock",
        "bad_summary",
        "non_null_max",
    ],
)
def test_real_sdk_unverified_hr_never_reaches_reload(fixture, monkeypatch, case):
    reader, sends, saved, clock, state = fixture
    value = missing_hr()
    if case == "wrong_date":
        value["calendarDate"] = "2026-04-29"
    elif case == "wrong_profile":
        value["userProfilePK"] = 8
    elif case == "missing_curve_key":
        value.pop("heartRateValues")
    elif case == "unknown_null_schema":
        value["unknownSchemaVersion"] = 2
    elif case == "bad_descriptor":
        value["heartRateValueDescriptors"] = {"unverified": True}
    elif case == "bad_point":
        value["heartRateValues"] = [[1777550400000]]
    elif case == "bad_clock":
        value["startTimestampGMT"] = "unverified"
    elif case == "bad_summary":
        value["restingHeartRate"] = True
    elif case == "non_null_max":
        value["maxHeartRate"] = 60
    native, receipt, folder = dated_call(fixture, monkeypatch, value, value)
    assert native == 2 and receipt["status"] == "stopped"
    assert receipt["HTTP"] == 3
    assert [m for m, _, _ in sends] == ["GET", "GET", "GET"]
    assert all(row["status"] != "ready" for row in receipt["targets"])


@pytest.mark.parametrize("body", [[], b"not-json"])
def test_real_sdk_unbound_hr_body_stops_before_sdk_reload(fixture, monkeypatch, body):
    reader, sends, saved, clock, state = fixture
    native, receipt, folder = dated_call(fixture, monkeypatch, body, body)
    assert native == 2 and receipt["category"] == "source_shape"
    assert receipt["HTTP"] == 3 and [m for m, _, _ in sends] == ["GET", "GET", "GET"]
    assert str(folder / "hr-03.wire") in saved


def test_real_sdk_oversized_integer_summary_has_controlled_receipt_no_reload(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    before["restingHeartRate"] = 10**400
    native, receipt, folder = dated_call(fixture, monkeypatch, before, before)
    assert native == 2 and receipt["status"] == "stopped"
    assert receipt["category"] == "invalid_null_hr" and receipt["HTTP"] == 3
    assert [m for m, _, _ in sends] == ["GET", "GET", "GET"]


@pytest.mark.parametrize(
    "field", ["startTimestampGMT", "endTimestampGMT", "startTimestampLocal", "endTimestampLocal"]
)
def test_real_sdk_date_only_clock_stops_before_reload(fixture, monkeypatch, field):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    before[field] = DAY
    native, receipt, folder = dated_call(fixture, monkeypatch, before, before)
    assert native == 2 and receipt["status"] == "stopped"
    assert receipt["category"] == "invalid_null_hr_clock" and receipt["HTTP"] == 3
    assert [m for m, _, _ in sends] == ["GET", "GET", "GET"]


def test_real_sdk_full_space_separated_source_clocks_are_preserved(fixture, monkeypatch):
    reader, sends, saved, clock, state = fixture
    before = missing_hr()
    for field in (
        "startTimestampGMT",
        "endTimestampGMT",
        "startTimestampLocal",
        "endTimestampLocal",
    ):
        before[field] = before[field].replace("T", " ")
    native, receipt, folder = dated_call(fixture, monkeypatch, before, before, reload=False)
    assert native == 0 and receipt["targets"][0]["status"] == "empty_unresolved"
    assert receipt["HTTP"] == 3
    row = receipt["targets"][0]
    assert json.loads(saved[str(folder / row["wire"])]) == before
    assert json.loads(saved[str(folder / row["view"])]) == before
