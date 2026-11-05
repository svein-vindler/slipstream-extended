"""Minimized Linux-CI expectation regressions at memory state/real SDK boundary."""

import sys

import boto3
import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_backfill, history_publish
from pipeline.history_state import Stop
from pipeline.night_baseline_refresh import QUEUE_KEY, BaselineRefreshStop


@pytest.mark.parametrize("mode", ["conflict", "missing_ack", "lost_ack"])
def test_first_denial_keeps_its_specific_class_and_owner_without_next_request(monkeypatch, mode):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    server.mode = mode
    store = sdk_store(server)
    expected = BaselineRefreshStop if mode == "conflict" else Stop
    with pytest.raises(expected) as error:
        history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    assert type(error.value) is expected
    if mode == "conflict":
        assert str(error.value) == "Night baseline storage denied or unknown"
    else:
        assert (
            error.value.category
            == {"missing_ack": "unknown_write", "lost_ack": "unknown_storage_transport"}[mode]
        )
    assert server.puts == [QUEUE_KEY]
    assert "health/signals/v1/2026/04/2026-04-29.json" not in server.objects
    assert store.operations["put"] == 1
    assert job.status()["counts"] == {"ready": 1} and job.status()["locked"]
    calls = len(server.calls)
    with pytest.raises(Stop, match="unknown_owner_hold_no_takeover"):
        history_publish.publish(job, job.plan_sha, confirm=True, store=store)
    assert len(server.calls) == calls


def configure_owned_client(monkeypatch, client):
    monkeypatch.setattr(boto3, "client", lambda *a, **kw: client)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)


def test_denied_primary_is_identical_through_both_outer_cleanup_failures(monkeypatch):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    server.mode = "conflict"
    client = sdk_store(server).client
    unregister = client.meta.events.unregister
    observed = []
    original_first = client.meta.events.register_first
    original_register = client.meta.events.register
    registered = []

    def register_first(event_name, handler, *args, **kwargs):
        if not kwargs.get("unique_id", "").startswith("night-baseline-"):
            registered.append((event_name, handler))
        return original_first(event_name, handler, *args, **kwargs)

    def register(event_name, handler, *args, **kwargs):
        if not kwargs.get("unique_id", "").startswith("night-baseline-"):
            registered.append((event_name, handler))
        return original_register(event_name, handler, *args, **kwargs)

    def fail_outer_unregister(*args, **kwargs):
        if kwargs.get("unique_id", "").startswith("night-baseline-"):
            return unregister(*args, **kwargs)
        observed.append((args[0], args[1], sys.exception()))
        raise OSError("synthetic secondary guard cleanup")

    def fail_client_close():
        observed.append(("client", None, sys.exception()))
        raise OSError("synthetic secondary client cleanup")

    monkeypatch.setattr(client.meta.events, "register_first", register_first)
    monkeypatch.setattr(client.meta.events, "register", register)
    monkeypatch.setattr(client.meta.events, "unregister", fail_outer_unregister)
    monkeypatch.setattr(client, "close", fail_client_close)
    configure_owned_client(monkeypatch, client)
    with pytest.raises(BaselineRefreshStop) as error:
        history_publish.publish(job, job.plan_sha, confirm=True)
    assert type(error.value) is BaselineRefreshStop
    assert str(error.value) == "Night baseline storage denied or unknown"
    assert [name for name, _ in registered] == ["before-send.s3", "after-call.s3"]
    assert [name for name, _, _ in observed] == ["before-send.s3", "after-call.s3", "client"]
    for (registered_name, registered_handler), (removed_name, removed_handler, _) in zip(
        registered, observed[:2], strict=True
    ):
        assert removed_name == registered_name
        assert removed_handler.__self__ is registered_handler.__self__
        assert removed_handler.__func__ is registered_handler.__func__
    assert registered[0][1].__self__ is registered[1][1].__self__
    assert all(primary is error.value for _, _, primary in observed)
    assert len(server.puts) == 1 and job.status()["locked"]
    assert job.status()["counts"] == {"ready": 1}
    calls = len(server.calls)
    monkeypatch.setattr(
        boto3, "client", lambda *a, **kw: pytest.fail("credential construction after HOLD")
    )
    with pytest.raises(Stop, match="unknown_owner_hold_no_takeover"):
        history_publish.publish(job, job.plan_sha, confirm=True)
    assert len(server.calls) == calls
    assert [name for name, _, _ in observed] == ["before-send.s3", "after-call.s3", "client"]


def test_actual_cli_classifies_denied_first_write_as_native2_and_reentry_is_offline(
    monkeypatch, capsys
):
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    server.mode = "conflict"
    configure_owned_client(monkeypatch, sdk_store(server).client)
    monkeypatch.setattr(history_backfill, "Job", lambda path: job)
    args = ["publish", "--plan-sha", job.plan_sha, "--confirm-r2-write"]
    assert history_backfill.main(args) == 2
    assert len(server.puts) == 1
    assert job.status()["locked"] and job.status()["counts"] == {"ready": 1}
    calls = len(server.calls)
    forbidden_client_calls = []

    def forbidden_client(*args, **kwargs):
        forbidden_client_calls.append(1)
        pytest.fail("credential construction after HOLD")

    monkeypatch.setattr(boto3, "client", forbidden_client)
    assert history_backfill.main(args) == 2
    assert forbidden_client_calls == []
    assert len(server.calls) == calls
    output = capsys.readouterr()
    assert "Traceback" not in output.out + output.err
