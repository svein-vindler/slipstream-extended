"""Synthetic hosted failure diagnostics, without credentials or network."""

import socket

import garminconnect
import pytest

from pipeline.output_privacy import safe_cli
from pipeline.sources.garmin import _login

PRIVATE = "SYNTHETIC_PRIVATE token email@example.invalid health=71"


def test_cli_identifies_rejected_garmin_session_without_disclosing_provider_text(
    monkeypatch, capsys
):
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
    assert output.err.startswith("Pipeline failed: garmin_authentication_required.\n")
    assert "Pipeline diagnostic: " in output.err
    assert PRIVATE not in output.out + output.err


@pytest.mark.parametrize(
    "case,category",
    [
        ("rate", "garmin_rate_limited"),
        ("connection", "garmin_connection_failed"),
        ("r2_denied", "r2_access_denied"),
        ("r2_expired", "r2_authentication_required"),
        ("wrapped_auth", "garmin_authentication_required"),
        ("personal_auth", "garmin_authentication_required"),
        ("personal_bad_status", "operation_failed"),
        ("spoofed_name", "operation_failed"),
        ("unknown_private_text", "operation_failed"),
    ],
)
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
    assert output.err.startswith(f"Pipeline failed: {category}.\n")
    assert "Pipeline diagnostic: " in output.err
    assert PRIVATE not in output.out + output.err


def test_real_storage_write_budget_failure_has_safe_actionable_diagnostic(monkeypatch, capsys):
    import json
    import platform

    from botocore.awsrequest import AWSResponse
    from history_storage_fixture import Raw
    from test_history_baseline_integration import sdk_store

    class BaselineServer:
        def __init__(self):
            self.puts = []

        def send(self, request):
            if request.method == "GET":
                body = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"
            else:
                assert request.method == "PUT"
                self.puts.append(True)
                body = b""
            return AWSResponse(
                request.url,
                200,
                {"content-length": str(len(body)), "etag": '"synthetic"'},
                Raw(body),
            )

    for name in ("system", "release", "machine"):
        monkeypatch.setattr(platform, name, lambda: "synthetic")
    server = BaselineServer()
    store = sdk_store(server, max_writes_per_run=1)
    seen = []

    @safe_cli
    def operation():
        try:
            store.create_with_receipt(
                "private/synthetic-key", b"synthetic", "application/octet-stream"
            )
            store.create_with_receipt(
                "private/synthetic-next-key", b"synthetic", "application/octet-stream"
            )
        except Exception as error:
            seen.append(error)
            raise

    with pytest.raises(SystemExit) as result:
        operation()
    assert result.value.code == 1
    from pipeline.r2_store import R2BudgetError

    assert len(seen) == 1 and isinstance(seen[0], R2BudgetError)
    output = capsys.readouterr()
    lines = [line for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    assert lines, output.err
    detail = json.loads(lines[-1].removeprefix("Pipeline diagnostic: "))
    assert detail["category"] == "r2_write_attempt_limit"
    assert detail["reason"] == "write_attempt_limit"
    assert detail["component"] == "r2" and detail["operation"] == "put"
    assert detail["site"]["module"] == "pipeline.r2_store"
    assert type(detail["site"]["line"]) is int
    assert len(server.puts) == 1
    assert "private/synthetic" not in output.out + output.err


def test_parser_caught_operational_value_error_keeps_diagnosis_and_native2(capsys):
    import json

    from pipeline.output_privacy import SafeArgumentParser

    @safe_cli
    def operation():
        parser = SafeArgumentParser(prog="synthetic")
        try:
            raise ValueError(PRIVATE)
        except ValueError as error:
            parser.error(str(error))

    with pytest.raises(SystemExit) as result:
        operation()
    assert result.value.code == 2
    output = capsys.readouterr()
    lines = [line for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    assert lines
    assert (
        json.loads(lines[-1].removeprefix("Pipeline diagnostic: "))["category"] == "invalid_value"
    )
    assert PRIVATE not in output.out + output.err and "Traceback" not in output.err


@pytest.mark.parametrize("mode", ["implicit", "explicit", "cycle", "suppressed", "bounded"])
def test_safe_cli_bounds_nested_context_without_exposing_private_text(mode, capsys):
    import json

    error = RuntimeError(PRIVATE)
    if mode == "implicit":
        try:
            raise garminconnect.GarminConnectAuthenticationError(PRIVATE)
        except Exception:
            try:
                raise RuntimeError(PRIVATE)
            except Exception as caught:
                error = caught
    elif mode == "explicit":
        error.__cause__ = garminconnect.GarminConnectAuthenticationError(PRIVATE)
    elif mode == "cycle":
        error.__cause__ = error
    elif mode == "suppressed":
        error.__context__ = garminconnect.GarminConnectAuthenticationError(PRIVATE)
        error.__suppress_context__ = True
    else:
        leaf = error
        for _ in range(9):
            leaf.__cause__ = RuntimeError(PRIVATE)
            leaf = leaf.__cause__
        leaf.__cause__ = garminconnect.GarminConnectAuthenticationError(PRIVATE)

    @safe_cli
    def operation():
        raise error

    with pytest.raises(SystemExit) as native:
        operation()
    assert native.value.code == 1
    output = capsys.readouterr()
    detail = json.loads(output.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == (
        "garmin_authentication_required" if mode in {"implicit", "explicit"} else "operation_failed"
    )
    assert PRIVATE not in output.err + output.out
    assert len(output.err) < 1024
    assert error.__cause__ is error if mode == "cycle" else True


@pytest.mark.parametrize("status", [401, 403, 429, 503, True, 401.0, {"private": PRIVATE}])
def test_observed_r2_http_status_is_validated_and_no_provider_text_is_logged(status, capsys):
    import json

    from botocore.exceptions import ClientError

    @safe_cli
    def operation():
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": PRIVATE},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            "GetObject",
        )

    with pytest.raises(SystemExit):
        operation()
    output = capsys.readouterr()
    detail = json.loads(output.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == "r2_access_denied" and detail["operation"] == "get"
    if type(status) is int:
        assert detail["http_status"] == status
    else:
        assert "http_status" not in detail
    assert PRIVATE not in output.out + output.err


def test_actual_environment_key_error_has_code_only_location_without_key(monkeypatch, capsys):
    import json

    from pipeline.r2_store import R2Store

    monkeypatch.delenv("CLOUDFLARE_ACCOUNT_ID", raising=False)

    @safe_cli
    def operation():
        R2Store()

    with pytest.raises(SystemExit) as native:
        operation()
    assert native.value.code == 1
    output = capsys.readouterr()
    detail = json.loads(output.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == "required_field_missing"
    assert detail["site"]["module"] == "pipeline.r2_store" and detail["site"]["line"] > 0
    assert "CLOUDFLARE_ACCOUNT_ID" not in output.err


def test_handled_optional_provider_failure_has_stage_but_keeps_private_behavior(capsys):
    import json

    from pipeline.sources.garmin_health import _call

    attempts = []

    def failed():
        attempts.append(True)
        raise garminconnect.GarminConnectConnectionError(PRIVATE)

    @safe_cli
    def operation():
        return _call(PRIVATE, failed, 0)

    assert operation() is None
    assert attempts == [True]
    output = capsys.readouterr()
    lines = [line for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    detail = json.loads(lines[0].removeprefix("Pipeline diagnostic: "))
    assert detail["category"] == "garmin_connection_failed" and detail["stage"] == "garmin_health"
    assert PRIVATE not in output.err + output.out


def test_invalid_arguments_stay_native2_and_do_not_echo_private_choice(capsys):
    from pipeline.output_privacy import SafeArgumentParser

    parser = SafeArgumentParser(prog="synthetic")
    parser.add_argument("--mode", choices=["safe"])
    with pytest.raises(SystemExit) as native:
        parser.parse_args(["--mode", PRIVATE])
    assert native.value.code == 2
    assert PRIVATE not in capsys.readouterr().err


def test_r2_budget_error_preserves_exception_args_and_rejects_private_reason(capsys):
    import json

    from pipeline.r2_store import R2BudgetError

    error = R2BudgetError(PRIVATE, 17, reason=PRIVATE)
    assert error.args == (PRIVATE, 17)
    assert R2BudgetError().args == ()

    @safe_cli
    def operation():
        raise error

    with pytest.raises(SystemExit):
        operation()
    detail = json.loads(capsys.readouterr().err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == "r2_budget_exhausted" and "reason" not in detail
    assert PRIVATE not in json.dumps(detail)


def test_provider_observed403_is_access_rejection_not_inferred_expiry(capsys):
    import json

    from pipeline.sources.provider_diagnostic import ProviderDiagnosticError

    @safe_cli
    def operation():
        raise ProviderDiagnosticError("garmin_access_rejected", 1, "identity_profile", 403)

    with pytest.raises(SystemExit):
        operation()
    out = capsys.readouterr()
    detail = json.loads(out.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == "garmin_access_rejected" and detail["http_status"] == 403
    assert detail["physical_http_attempts"] == 1 and detail["reason"] == "identity_profile"
    assert "expired" not in out.err


@pytest.mark.parametrize(
    "limit,category",
    [
        ({"max_write_bytes_per_run": 1}, "r2_write_byte_limit"),
        ({"max_bucket_objects": 1}, "r2_bucket_object_limit"),
        ({"max_bucket_bytes": 1}, "r2_bucket_byte_limit"),
    ],
)
def test_actual_store_distinguishes_remaining_three_budgets_without_put(limit, category, capsys):
    import json

    from pipeline.r2_store import R2Store
    from scripts.benchmark_refresh_budget import BudgetClient

    client = BudgetClient(1)
    store = R2Store(client=client, bucket="synthetic", **limit)

    @safe_cli
    def operation():
        store.put("synthetic", b"12", "text/plain")

    with pytest.raises(SystemExit):
        operation()
    out = capsys.readouterr()
    detail = json.loads(out.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == category and client.counts["put"] == 0


@pytest.mark.parametrize(
    "error,category",
    [
        ("timeout", "network_timeout"),
        ("connection", "network_connection_failed"),
        ("http", "upstream_http_failed"),
    ],
)
def test_optional_requests_failures_do_not_echo_uri_body_or_headers(error, category, capsys):
    import json

    import requests

    if error == "http":
        response = requests.Response()
        response.status_code = 429
        response._content = PRIVATE.encode()
        exc = requests.HTTPError(PRIVATE, response=response)
    else:
        exc = (requests.Timeout if error == "timeout" else requests.ConnectionError)(PRIVATE)

    @safe_cli
    def operation():
        raise exc

    with pytest.raises(SystemExit):
        operation()
    out = capsys.readouterr()
    detail = json.loads(out.err.split("Pipeline diagnostic: ")[-1])
    assert detail["category"] == category
    assert PRIVATE not in out.err
    assert detail.get("http_status") == (429 if error == "http" else None)


def test_interactive_login_cli_failure_does_not_echo_sdk_text_or_persist(monkeypatch, capsys):
    import scripts.garmin_login as command

    calls = []

    class Garmin:
        def __init__(self, *args, **kwargs):
            pass

        def login(self):
            raise garminconnect.GarminConnectAuthenticationError(PRIVATE)

    monkeypatch.setattr(command, "Garmin", Garmin)
    monkeypatch.setattr("builtins.input", lambda *_: "synthetic@example.invalid")
    monkeypatch.setattr(command.getpass, "getpass", lambda *_: PRIVATE)
    monkeypatch.setattr(command, "_write_local_token", lambda *args: calls.append("persist"))
    monkeypatch.setattr(command, "detect_repo", lambda: calls.append("repository"))
    monkeypatch.setattr(
        "sys.argv", ["synthetic", "--local-only", "--local-token-file", "synthetic.local"]
    )
    with pytest.raises(SystemExit) as native:
        command.main()
    assert calls == []
    output = capsys.readouterr()
    assert PRIVATE not in output.err + output.out + str(native.value.code)
    assert native.value.code == 1
    assert "Pipeline diagnostic: " in output.err


@pytest.mark.parametrize("status", [401, 403, 429])
def test_personal_typed_observed_status_uses_existing_status_code_field(status, capsys):
    import json

    from pipeline.sources.garmin_personal import PersonalSourceError

    @safe_cli
    def operation():
        raise PersonalSourceError("schema_unverified", status)

    with pytest.raises(SystemExit):
        operation()
    out = capsys.readouterr()
    detail = json.loads(out.err.split("Pipeline diagnostic: ")[-1])
    assert detail["http_status"] == status and detail["category"] == "garmin_response_unverified"


@pytest.fixture
def activity_backfill_memory(monkeypatch):
    from test_activity_pagination import Store

    from pipeline import activity_backfill as backfill

    files = {}

    class MemoryPath:
        def __init__(self, name):
            self.name = name

        def __truediv__(self, name):
            return MemoryPath(self.name + "/" + name)

        @property
        def parent(self):
            return self

        def mkdir(self, **kwargs):
            pass

        def write_bytes(self, data):
            files[self.name] = data

    monkeypatch.setattr(backfill, "Path", MemoryPath)
    return Store(), files


def test_activity_backfill_partial_import_keeps_diagnosis_and_private_checkpoint(
    activity_backfill_memory, capsys
):
    import json

    from test_activity_pagination import END, START, Source, activity

    from pipeline import activity_backfill as backfill
    from pipeline.failure_diagnostics import diagnostic_scope

    class ImportSource(Source):
        def download_activity(self, activity_id, **kwargs):
            self.calls.append("download")
            return PRIVATE.encode()

    store, files = activity_backfill_memory
    source = ImportSource([activity(1)])
    with diagnostic_scope():
        result = backfill.run(start_date=START, end_date=END, max_activities=1,
                              store=store, garmin=source, output_dir="memory")
    assert result["status"] == "active" and result["stop_reason"] == "batch_limit"
    assert result["failed_this_run"][0]["last_error"] == "activity_import_failed"
    assert result["failures"]["1"]["attempts"] == 1 and not result["completed_this_run"]
    assert source.calls == [0, "download"]
    assert len(files) == len(store.puts) == 1
    assert store.progress()["failures"] == result["failures"]
    output = capsys.readouterr()
    details = [json.loads(line.removeprefix("Pipeline diagnostic: "))
               for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    assert len(details) == 1
    assert details[0]["category"] == "invalid_value"
    assert details[0]["stage"] == "activity_backfill"
    assert details[0]["site"]["module"] == "pipeline.granular"
    assert PRIVATE not in output.out + output.err


@pytest.mark.parametrize("kind,category", [("authentication", "garmin_authentication_required"),
                                          ("r2", "r2_service_failed")])
def test_activity_backfill_pagination_logs_original_before_masked_stop(
    activity_backfill_memory, capsys, kind, category
):
    import json

    from botocore.exceptions import ClientError
    from test_activity_pagination import END, START

    from pipeline import activity_backfill as backfill
    from pipeline.failure_diagnostics import diagnostic_scope

    class FailedSource:
        def __init__(self):
            self.calls = 0

        def get_activity_page(self, *args, **kwargs):
            self.calls += 1
            if kind == "authentication":
                raise garminconnect.GarminConnectAuthenticationError(PRIVATE)
            raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": PRIVATE},
                               "ResponseMetadata": {"HTTPStatusCode": 503}}, "GetObject")

    source = FailedSource()
    store, files = activity_backfill_memory
    with diagnostic_scope(), pytest.raises(RuntimeError, match="progress retained") as failure:
        backfill.run(start_date=START, end_date=END, store=store, garmin=source,
                     output_dir="memory")
    assert type(failure.value) is RuntimeError and failure.value.__suppress_context__ is True
    checkpoint = store.progress()
    assert checkpoint["stop_reason"] == "temporary_error" and not checkpoint["failures"]
    assert checkpoint["status"] == "active" and checkpoint["pagination"]["offset"] == 0
    assert source.calls == len(files) == len(store.puts) == 1
    output = capsys.readouterr()
    details = [json.loads(line.removeprefix("Pipeline diagnostic: "))
               for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    assert len(details) == 1
    assert details[0]["category"] == category
    assert details[0]["stage"] == "activity_backfill" and details[0]["operation"] == "get"
    assert details[0]["site"]["module"] == "pipeline.sources.activity_page"
    assert details[0].get("http_status") == (503 if kind == "r2" else None)
    assert PRIVATE not in output.out + output.err


@pytest.mark.parametrize("kind,category", [("timeout", "network_timeout"),
                                          ("connection", "network_connection_failed")])
@pytest.mark.parametrize("failure_at", [1, 3])
def test_personal_adapter_logs_original_transport_failure_then_stays_latched(
    capsys, kind, category, failure_at
):
    import json

    import requests
    from test_garmin_personal import Response, authenticated

    from pipeline.failure_diagnostics import diagnostic_scope
    from pipeline.sources.garmin_personal import PersonalGarmin, PersonalSourceError

    day = "2026-10-25"
    http = authenticated(("GET", "/hrv-service/hrv/" + day, Response({})))
    original = http.request
    sends = []
    error = (requests.Timeout if kind == "timeout" else requests.ConnectionError)(PRIVATE)

    def transport(method, url, **kwargs):
        sends.append(method)
        if len(sends) == failure_at:
            raise error
        return original(method, url, **kwargs)

    http.request = transport
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    with diagnostic_scope():
        for _ in range(2):
            with pytest.raises(PersonalSourceError) as result:
                client.get_hrv_data(day)
            assert result.value.status == "source_error" and result.value.status_code is None
            assert result.value.args == ("Garmin Personal source: source_error",)
            assert result.value.__suppress_context__ is True
    assert len(sends) == failure_at
    assert sum(client.metrics[key] for key in ("fitness_http_attempts", "auth_http_attempts",
                                               "identity_http_attempts")) == failure_at
    assert client.account_identity == ("synthetic-account" if failure_at == 3 else None)
    output = capsys.readouterr()
    details = [json.loads(line.removeprefix("Pipeline diagnostic: "))
               for line in output.err.splitlines() if line.startswith("Pipeline diagnostic: ")]
    assert len(details) == 1
    assert details[0]["category"] == category and details[0]["stage"] == "garmin_personal"
    assert details[0]["operation"] == ("authenticate" if failure_at == 1 else "get")
    assert details[0]["site"]["module"] == "pipeline.sources.garmin_personal"
    assert "http_status" not in details[0]
    assert PRIVATE not in output.out + output.err
