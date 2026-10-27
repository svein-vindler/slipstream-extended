"""Bounded stress source -> canonical collection, synthetic HTTP and R2 only."""
import gzip
import io
import json
import socket

import pytest
import requests
from garminconnect import Garmin

from pipeline.sources.garmin_readonly import GarminReadOnlyViolation, ReadOnlyGarmin

DAY = "2026-09-30"
PATH = f"/wellness-service/wellness/dailyStress/{DAY}"


class StressResponse:
    def __init__(self, data, status=200, headers=None):
        self.source = io.BytesIO(data)
        self.raw = self
        self.status_code = status
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.count = 0
        self.closed = False

    def read1(self, amount, *, decode_content):
        assert decode_content is False
        data = self.source.read(amount)
        self.count += len(data)
        return data

    def close(self):
        self.closed = True

    def json(self):
        pytest.fail("Unbounded parser used")


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *a, **kw: pytest.fail("External socket forbidden"))


def guarded(monkeypatch, response, *, is_cn=False):
    sdk = Garmin(is_cn=is_cn)
    sdk.client.di_token = "synthetic-stress-session"
    calls = []

    def send(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return response(method, url, kwargs) if callable(response) else response

    monkeypatch.setattr(sdk.client._api_session, "request", send)
    for name in ("_refresh_session", "_refresh_di_token", "login"):
        monkeypatch.setattr(sdk.client, name, lambda *a, **kw: pytest.fail("Stress auth forbidden"))
    monkeypatch.setattr(sdk.client.cs, "request", lambda *a, **kw: pytest.fail("Stress auth HTTP forbidden"))
    return ReadOnlyGarmin(sdk), sdk, calls


def test_stress_facade_reads_one_exact_get_and_closes_before_return(monkeypatch):
    payload = {"calendarDate": DAY, "stressValuesArray": [[1790726400000, 0], [1790726520000, 35]]}
    data = json.dumps(payload).encode()
    response = StressResponse(data)
    reader, sdk, calls = guarded(monkeypatch, response)
    result = reader.get_stress_data(DAY)
    assert result.status == "available" and result.payload == payload
    assert response.closed and result.metrics["response_closed"]
    assert result.metrics["fitness_http_attempts"] == len(calls) == 1
    assert result.metrics["component_auth_http_attempts"] == 0
    assert result.metrics["body_bytes"] == len(data)
    assert calls[0][0:2] == ("GET", "https://connectapi.garmin.com" + PATH)
    options = calls[0][2]
    assert options["stream"] is True and options["verify"] is True
    assert options["allow_redirects"] is False
    assert options["headers"]["Accept-Encoding"] == "identity"
    assert "params" not in options and "json" not in options
    assert sdk.client.di_token == "synthetic-stress-session"

def test_ordinary_refresh_retains_stress_for_only_two_latest_source_dates(monkeypatch, tmp_path):
    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW, HealthClient, SummaryGarmin

    responses = []

    def reply(method, url, options):
        day = url.rsplit("/", 1)[1]
        response = StressResponse(json.dumps({
            "calendarDate": day, "stressValuesArray": [[1790726400000, 0], [1790726520000, -1],
                                                       [1790726640000, 35]],
        }).encode())
        responses.append(response)
        return response

    reader, _, calls = guarded(monkeypatch, reply)

    class StressSummary(SummaryGarmin):
        get_stress_data = reader.get_stress_data

    storage = HealthClient()
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
                 client=StressSummary(), now=NOW, request_pause=0)
    key = "health/signals/v1/2026/09/2026-09-30.json"
    payload = json.loads(gzip.decompress(storage.objects[key]["Body"]))
    assert "stress" in payload["channels"], "Ordinary collection discarded bounded stress data"
    channel = payload["channels"]["stress"]
    assert channel["status"] == "available" and channel["unit"] == "points"
    assert [row["value"] for row in channel["readings"]] == [0, 35]
    assert channel["missing_value_count"] == 1 and channel["measurement_kind"] == "garmin_derived"
    assert [url.rsplit("/", 1)[1] for _, url, _ in calls] == ["2026-09-29", DAY]
    assert result["health_diagnostics"]["stress"]["fitness_http_attempts"] == 2
    assert result["health_diagnostics"]["stress"]["body_bytes"] == sum(response.count for response in responses)
    assert all(response.closed for response in responses)
    assert result["health_diagnostics"]["sdk_calls"] == 63
    assert result["provider_diagnostics"]["garmin_api_calls"] == 66

@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_stress_errors_do_not_refresh_auth_retry_or_consume_error_body(monkeypatch, status):
    response = StressResponse(b"private-error" * 100000, status=status)
    reader, sdk, calls = guarded(monkeypatch, response)
    monkeypatch.setattr(sdk.client, "_token_expires_soon", lambda: pytest.fail("Token check forbidden"))
    result = reader.get_stress_data(DAY)
    assert len(calls) == result.metrics["fitness_http_attempts"] == 1
    assert result.metrics["component_auth_http_attempts"] == 0
    assert response.closed and response.count == 0
    assert result.status == ("auth_required" if status == 401 else "source_error")


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_stress_redirects_are_closed_policy_failures(monkeypatch, status):
    response = StressResponse(b"private-redirect", status, {"Location": "https://evil.invalid"})
    reader, _, calls = guarded(monkeypatch, response)
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_stress_data(DAY)
    assert response.closed and response.count == 0 and len(calls) == 1


@pytest.mark.parametrize("day", ["2026-02-30", DAY + "?write=1", DAY + "/../upload", DAY + "\n", None, True])
def test_stress_date_upper_guard_rejects_before_fitness_or_auth(monkeypatch, day):
    reader, _, calls = guarded(monkeypatch, StressResponse(b"{}"))
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_stress_data(day)
    assert calls == []


@pytest.mark.parametrize("kwargs", [
    {"method": "POST"}, {"json": {}}, {"data": b"body"}, {"params": {"date": DAY}},
    {"url": "https://evil.invalid"}, {"allow_redirects": True},
    {"headers": {"X-HTTP-Method-Override": "DELETE"}}, {"timeout": 1000},
])
def test_stress_facade_accepts_no_method_host_query_body_or_header_options(monkeypatch, kwargs):
    reader, _, calls = guarded(monkeypatch, StressResponse(b"{}"))
    with pytest.raises(TypeError):
        reader.get_stress_data(DAY, **kwargs)
    assert calls == []


def test_stress_generic_sdk_routes_and_alias_remain_blocked(monkeypatch):
    reader, sdk, calls = guarded(monkeypatch, StressResponse(b"{}"))
    for action in (
        lambda: reader.get_all_day_stress(DAY),
        lambda: reader.connectapi(PATH),
        lambda: sdk.get_stress_data(DAY),
        lambda: sdk.get_all_day_stress(DAY),
        lambda: sdk.client.connectapi(PATH),
        lambda: sdk.client._run_request("GET", PATH),
    ):
        with pytest.raises((AttributeError, GarminReadOnlyViolation)):
            action()
    assert calls == []


@pytest.mark.parametrize("method,url,kwargs", [
    ("POST", "https://connectapi.garmin.com" + PATH, {}),
    ("DELETE", "https://connectapi.garmin.com" + PATH, {}),
    ("GET", "https://evil.invalid" + PATH, {}),
    ("GET", "https://connectapi.garmin.com.evil.invalid" + PATH, {}),
    ("GET", "https://connectapi.garmin.com:443" + PATH, {}),
    ("GET", "http://connectapi.garmin.com" + PATH, {}),
    ("GET", "https://connectapi.garmin.com" + PATH + "?date=" + DAY, {}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"params": {"write": True}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"json": {}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"headers": {"X-Method-Override": "POST"}}),
    ("GET", "https://connectapi.garmin.com" + PATH, {"_stress_context": (object(), {})}),
    ("GET", "https://connectapi.garmin.com" + PATH.replace(DAY, "2026-02-30"), {}),
])
def test_stress_lower_transport_cannot_bypass_fixed_capability(monkeypatch, method, url, kwargs):
    _, sdk, calls = guarded(monkeypatch, StressResponse(b"{}"))
    with pytest.raises(GarminReadOnlyViolation):
        sdk.client._api_session.request(method, url, **kwargs)
    assert calls == []


@pytest.mark.parametrize("data,headers,expected,max_read", [
    (b"{}" + b" " * (256 * 1024), {}, "oversize", 256 * 1024 + 1),
    (b"{}", {"Content-Length": str(256 * 1024 + 1)}, "oversize", 0),
    (b"{}", {"Content-Encoding": "gzip"}, "source_error", 0),
    (b"{}", {"Content-Type": "text/html"}, "source_error", 0),
    (b"{\"x\":NaN}", {}, "schema_unverified", 9),
], ids=["oversize-stream", "oversize-length", "compressed", "html", "nonfinite-json"] )
def test_stress_bounded_identity_json_closes_on_invalid_or_oversize(monkeypatch, data, headers, expected, max_read):
    response = StressResponse(data, headers=headers)
    reader, _, calls = guarded(monkeypatch, response)
    result = reader.get_stress_data(DAY)
    assert result.status == expected and response.closed and len(calls) == 1
    assert response.count <= max_read


def test_stress_stream_timeout_closes_without_second_attempt(monkeypatch):
    response = StressResponse(b"{}")

    def fail_read(*args, **kwargs):
        raise requests.Timeout("private-timeout-message")

    response.read1 = fail_read
    reader, _, calls = guarded(monkeypatch, response)
    result = reader.get_stress_data(DAY)
    assert result.status == "source_error" and response.closed and len(calls) == 1
    assert result.metrics["component_auth_http_attempts"] == 0


def test_stress_exact_china_origin_is_accepted_only_on_its_configured_sdk(monkeypatch):
    reader, _, calls = guarded(monkeypatch, StressResponse(b"{}"), is_cn=True)
    assert reader.get_stress_data(DAY).status == "available"
    assert calls[0][1] == "https://connectapi.garmin.cn" + PATH

def refresh_environment(monkeypatch):
    from scripts.benchmark_summary_windows import HealthClient, SummaryGarmin

    state = {"mode": "available", "value": 35}
    responses = []

    def reply(method, url, options):
        day = url.rsplit("/", 1)[1]
        mode = state["mode"]
        if mode == "timeout":
            raise requests.Timeout("synthetic-private-message")
        payload = {"calendarDate": day,
                   "stressValuesArray": [[1790726400000, 0], [1790726520000, state["value"]]]}
        if mode == "empty":
            payload["stressValuesArray"] = []
        elif mode == "wrong_date":
            payload["calendarDate"] = "2026-09-28"
        elif mode == "malformed":
            payload["stressValuesArray"] = [[1790726400000, 0], [1790726520000, "invalid"]]
        elif mode == "sentinel_only":
            payload["stressValuesArray"] = [[1790726400000, -1], [1790726520000, -2]]
        response = StressResponse(json.dumps(payload).encode(), status=401 if mode == "auth" else 200)
        responses.append(response)
        return response

    reader, sdk, calls = guarded(monkeypatch, reply)

    class StressSummary(SummaryGarmin):
        get_stress_data = reader.get_stress_data

    return HealthClient(), StressSummary(), state, responses, calls, sdk


@pytest.mark.parametrize("mode", ["empty", "wrong_date", "malformed", "sentinel_only", "auth", "timeout"])
def test_unavailable_stress_preserves_old_good_channel_without_new_put(monkeypatch, tmp_path, mode):
    from datetime import timedelta

    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW

    storage, source, state, responses, calls, _ = refresh_environment(monkeypatch)
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
        client=source, now=NOW, request_pause=0)
    key = "health/signals/v1/2026/09/2026-09-30.json"
    original = storage.objects[key]["Body"]
    storage.reset()
    calls.clear()
    state["mode"] = mode
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
                 client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert storage.objects[key]["Body"] == original
    assert key not in storage.writes and result["signals"]["put_attempts"] == 0
    assert result["signals"]["preserved_channels"] == 2
    assert len(calls) == result["health_diagnostics"]["stress"]["fitness_http_attempts"] == 2
    assert all(response.closed for response in responses)
    assert result["health_diagnostics"]["sdk_calls"] == 19
    assert result["provider_diagnostics"]["garmin_api_calls"] == 19


def test_identical_stress_repeat_uses_existing_object_and_no_extra_keys(monkeypatch, tmp_path):
    from datetime import timedelta

    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW

    storage, source, _, _, calls, _ = refresh_environment(monkeypatch)
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
        client=source, now=NOW, request_pause=0)
    signal_keys = {key for key in storage.objects if key.startswith("health/signals/")}
    storage.reset()
    calls.clear()
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
                 client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert signal_keys == {key for key in storage.objects if key.startswith("health/signals/")}
    assert len(signal_keys) == 14 and len(calls) == 2
    assert result["signals"]["get_attempts"] == 3
    assert result["signals"]["put_attempts"] == result["signals"]["write_bytes"] == 0
    assert not any(key.startswith("health/signals/") for key in storage.writes)


def test_stress_extension_keeps_existing_r2_write_budget_and_failed_scope_receipt(monkeypatch, tmp_path):
    from datetime import timedelta

    from pipeline.r2_store import R2BudgetError, R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW

    storage, source, state, _, _, _ = refresh_environment(monkeypatch)
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
        client=source, now=NOW, request_pause=0)
    key = "health/signals/v1/2026/09/2026-09-30.json"
    check = "refresh/checks/v1/summary/health/recent.json"
    original, previous_check = storage.objects[key]["Body"], storage.objects[check]["Body"]
    state["value"] = 40
    storage.reset()
    with pytest.raises(R2BudgetError):
        run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress", max_writes_per_run=1),
            client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert storage.objects[key]["Body"] == original
    assert storage.objects[check]["Body"] == previous_check
    assert len([key for key in storage.writes if key.startswith("health/signals/")]) == 1

@pytest.mark.parametrize("drift", ["session", "request", "adapter", "retry", "headers", "version"])
def test_stress_transport_drift_fails_closed_without_fallback(monkeypatch, drift):
    from pipeline.sources import garmin_readonly

    reader, sdk, calls = guarded(monkeypatch, StressResponse(b"{}"))
    session = sdk.client._api_session
    if drift == "session":
        sdk.client._api_session = requests.Session()
    elif drift == "request":
        session.request = lambda *a, **kw: pytest.fail("unguarded transport")
    elif drift == "adapter":
        session.mount("https://", object())
    elif drift == "retry":
        session.get_adapter("https://connectapi.garmin.com").max_retries.total = 1
    elif drift == "headers":
        sdk.client.get_api_headers = lambda: {}
    else:
        monkeypatch.setattr(garmin_readonly, "version", lambda _: "unexpected-version")
    with pytest.raises(GarminReadOnlyViolation):
        reader.get_stress_data(DAY)
    assert calls == []


def test_stress_cooperative_deadline_closes_open_response_without_late_success(monkeypatch):
    from pipeline.sources import garmin_readonly

    response = StressResponse(b"{}")
    original = garmin_readonly.bounded_read
    ticks = iter([0, 0, 31, 31])
    monkeypatch.setattr(garmin_readonly, "bounded_read",
                        lambda send, headers: original(send, headers, clock=lambda: next(ticks)))
    reader, _, calls = guarded(monkeypatch, response)
    result = reader.get_stress_data(DAY)
    assert result.status == "timeout" and response.closed and response.count == 0
    assert len(calls) == result.metrics["fitness_http_attempts"] == 1


def test_stress_storage_read_error_preserves_good_curve_and_scope_check(monkeypatch, tmp_path):
    from datetime import timedelta

    from botocore.exceptions import ClientError

    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW

    storage, source, _, _, _, _ = refresh_environment(monkeypatch)
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
        client=source, now=NOW, request_pause=0)
    key = "health/signals/v1/2026/09/2026-09-30.json"
    check = "refresh/checks/v1/summary/health/recent.json"
    original, previous_check = storage.objects[key]["Body"], storage.objects[check]["Body"]
    get = storage.get_object

    def denied(**kwargs):
        if kwargs["Key"] == key:
            raise ClientError({"Error": {"Code": "403"}}, "SyntheticStressRead")
        return get(**kwargs)

    monkeypatch.setattr(storage, "get_object", denied)
    storage.reset()
    with pytest.raises(ClientError):
        run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-stress"),
            client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    assert storage.objects[key]["Body"] == original
    assert storage.objects[check]["Body"] == previous_check
    assert key not in storage.writes

def test_missing_stress_auth_is_reported_without_any_auth_or_fitness_http(monkeypatch):
    reader, sdk, calls = guarded(monkeypatch, StressResponse(b"{}"))
    sdk.client.di_token = None
    sdk.client.jwt_web = None
    result = reader.get_stress_data(DAY)
    assert result.status == "auth_required" and calls == []
    assert result.metrics["component_auth_http_attempts"] == result.metrics["fitness_http_attempts"] == 0

def test_retention_treats_derived_zero_as_new_measurement_for_battery_and_stress(monkeypatch, tmp_path):
    from datetime import timedelta

    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import NOW

    storage, original_source, state, _, _, _ = refresh_environment(monkeypatch)

    class Source(type(original_source)):
        battery = 65

        def get_body_battery(self, first, last):
            super().get_body_battery(first, last)
            return [{"date": DAY, "bodyBatteryValuesArray": [[1790726400000, self.battery]]}]

    source = Source()
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-derived-zero"),
        client=source, now=NOW, request_pause=0)
    source.battery = state["value"] = 0
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-derived-zero"),
        client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    payload = json.loads(gzip.decompress(storage.objects["health/signals/v1/2026/09/2026-09-30.json"]["Body"]))
    for name in ("body_battery", "stress"):
        channel = payload["channels"][name]
        assert [point["value"] for point in channel["readings"]] == ([0] if name == "body_battery" else [0, 0])
        assert channel["source_composition"]["retained_valid_points"] == 0
        assert all(point["observation_origin"] == "incoming" for point in channel["readings"])