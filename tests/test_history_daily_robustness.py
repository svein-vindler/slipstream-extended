"""Local daily progress survives a known sibling failure without inventing success."""

import json

from test_history_backfill import native_runner

from pipeline.history_collect import collect
from pipeline.history_state import Job


def test_sdk_sibling_denial_keeps_verified_curve_and_primary_after_session_cleanup(tmp_path):
    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hr", "hrv"], "legacy", "synthetic"
    )
    result = collect(
        job, job.plan_sha, confirm=True, date_runner=native_runner("sibling_denial_close")
    )
    assert result["counts"] == {"ready": 1, "denied": 1}
    assert result["last_run_status"] == "denied"
    assert not result["locked"]
    row = next(r for r in job.snapshot()["rows"] if r["stream"] == "hr")
    receipt = json.loads((__import__("pathlib").Path(row["folder"]) / "RECEIPT.json").read_bytes())
    assert receipt["status"] == "stopped" and receipt["category"] == "denied"
    assert receipt["HTTP"] == 4
    assert receipt["cleanup_status"] == "failed"


def test_verified_sibling_can_publish_without_refetch_or_parent_success(tmp_path):
    from history_storage_fixture import Server
    from test_history_backfill_review import storage

    from pipeline.history_publish import publish

    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hr", "hrv"], "legacy", "synthetic"
    )
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("sibling_denial_close"))
    server = Server()
    result = publish(job, job.plan_sha, confirm=True, store=storage(server))
    assert result["counts"] == {"published": 1, "denied": 1}
    assert "health/signals/v1/2026/04/2026-04-29.json" in server.objects
    assert collect(
        job,
        job.plan_sha,
        confirm=True,
        date_runner=lambda *a: __import__("pytest").fail("refetched known curve"),
    )["counts"] == {"published": 1, "denied": 1}


def journal(path, events):
    from pipeline.history_state import digest, encoded

    previous = None
    lines = []
    for sequence, event in enumerate(events):
        line = encoded({"sequence": sequence, "previous": previous, "event": event})
        previous = digest(line)
        lines.append(line)
    path.write_bytes(b"".join(lines))
    return path


def test_collect_counts_old_and_manual_actual_journals_across_midnight_before_credentials(tmp_path):
    from datetime import datetime, timezone

    import pytest

    from pipeline.history_state import Stop

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    paths = []
    for label, count in (("old", 17), ("manual", 13)):
        events = [{"type": "round_start", "UTC": "2026-04-29T18:00:00+00:00"}]
        for index in range(count):
            identifier = f"{label}:{index}"
            events.extend(
                [
                    {
                        "type": "date_reserved",
                        "id": identifier,
                        "UTC": "2026-04-29T23:00:00+00:00",
                        "HTTP_reserved": 8,
                    },
                    {
                        "type": "date_closed",
                        "id": identifier,
                        "UTC": "2026-04-29T23:01:00+00:00",
                        "physicalHTTP": 8,
                        "reload_ACK": 1,
                        "reload_intent": 1,
                    },
                ]
            )
        paths.append(journal(tmp_path / f"{label}.jsonl", events))
    with pytest.raises(Stop) as failure:
        collect(
            job,
            job.plan_sha,
            confirm=True,
            reload=True,
            ledger_paths=paths,
            now=datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc),
            date_runner=lambda *a: pytest.fail("source called after rolling budget"),
        )
    assert failure.value.category == "rolling_source_budget"
    assert not (job.folder / "runs").exists()
    assert not job.status()["locked"]


def test_daily_cli_completes_native_collect_and_checks_stored_reference_queue_on_repeat(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    import subprocess
    from pathlib import Path

    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.history_backfill import main

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    server = Server()
    native_client = boto3.client

    def client_factory(*args, **kwargs):
        client = native_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    monkeypatch.setattr(boto3, "client", client_factory)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    native_run = subprocess.run
    children = []

    def process(args, **kwargs):
        if not isinstance(args, list) or "--run" not in args:
            return native_run(args, **kwargs)
        children.append(args)
        return native_run(
            [
                args[0],
                "-B",
                str(Path(__file__).with_name("history_native_fixture.py")),
                "normal",
                str(job.folder),
                args[args.index("--run") + 1],
                args[args.index("--date") + 1],
            ],
            input=kwargs["input"],
            capture_output=True,
        )

    monkeypatch.setattr(subprocess, "run", process)
    argv = [
        "--state",
        str(job.folder),
        "daily",
        "--plan-sha",
        job.plan_sha,
        "--confirm-cloud-jobs-paused",
        "--confirm-r2-write",
    ]
    assert main(argv) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["counts"] == {"published": 1}
    assert len(children) == 1
    before = list(server.calls)
    assert main(argv) == 0
    assert json.loads(capsys.readouterr().out)["counts"] == {"published": 1}
    assert len(children) == 1 and len(server.calls) == len(before) + 1
    assert server.calls[-1][0] == "GET"


def test_unknown_old_reservation_stops_collection_without_expiry_refund(tmp_path):
    from datetime import datetime, timezone

    import pytest

    from pipeline.history_state import Stop

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    old = journal(
        tmp_path / "old.jsonl",
        [
            {
                "type": "date_reserved",
                "id": "old:unknown",
                "UTC": "2026-04-01T12:00:00Z",
                "HTTP_reserved": 8,
            }
        ],
    )
    with pytest.raises(Stop) as failure:
        collect(
            job,
            job.plan_sha,
            confirm=True,
            ledger_paths=[old],
            now=datetime(2026, 4, 30, tzinfo=timezone.utc),
            date_runner=lambda *a: pytest.fail("continued after unknown reservation"),
        )
    assert failure.value.category == "unknown_source_reservation_hold"
    assert not (job.folder / "runs").exists()


def test_reload_preserves_first_completed_stream_when_next_physical_read_is_denied(tmp_path):
    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hr", "hrv"], "legacy", "synthetic"
    )
    result = collect(
        job, job.plan_sha, confirm=True, reload=True, date_runner=native_runner("reload_sibling")
    )
    assert result["counts"] == {"ready": 1, "denied": 1}
    assert result["last_run_status"] == "denied" and not result["locked"]
    row = next(r for r in job.snapshot()["rows"] if r["stream"] == "hr")
    receipt = json.loads((__import__("pathlib").Path(row["folder"]) / "RECEIPT.json").read_bytes())
    assert receipt["reload_ACK"] == 1 and receipt["HTTP"] == 7
    assert receipt["timings"]["source_wait_seconds"] == 67.5
    folder = __import__("pathlib").Path(row["folder"])
    requests = [json.loads(path.read_bytes()) for path in sorted(folder.glob("http-??.json"))]
    assert [request["kind"] for request in requests] == [
        "identity_auth", "identity_auth", "hr", "hrv", "reload", "hr", "hrv"
    ]


def test_publish_cleanup_failure_inside_unrelated_caller_except_never_becomes_success(
    monkeypatch, tmp_path, capsys
):
    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET, Server
    from test_history_backfill_review import storage

    from pipeline.history_backfill import main

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    collect(job, job.plan_sha, confirm=True, date_runner=native_runner("normal"))
    client = storage(Server()).client

    def close():
        raise OSError("synthetic cleanup")

    monkeypatch.setattr(client, "close", close)
    monkeypatch.setattr(boto3, "client", lambda *a, **kw: client)
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        monkeypatch.setenv(name, value)
    try:
        raise ValueError("unrelated caller exception")
    except ValueError:
        code = main(
            [
                "--state",
                str(job.folder),
                "publish",
                "--plan-sha",
                job.plan_sha,
                "--confirm-r2-write",
            ]
        )
    assert code == 3
    assert job.status()["locked"] and job.status()["counts"] == {"ready": 1}
    assert "synthetic cleanup" not in capsys.readouterr().err


def test_source_cleanup_inside_caller_except_is_stopped_and_keeps_completed_curve(tmp_path):
    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    result = collect(
        job, job.plan_sha, confirm=True, date_runner=native_runner("source_cleanup_ambient")
    )
    assert result["last_run_status"] == "cleanup_failed"
    assert result["counts"] == {"ready": 1} and not result["locked"]


def test_phase_times_measure_source_wait_and_storage_without_inventing_external_control(tmp_path):
    from history_storage_fixture import Server
    from test_history_backfill_review import storage

    from pipeline.history_publish import publish

    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hr", "hrv"], "legacy", "synthetic"
    )
    source = collect(
        job, job.plan_sha, confirm=True, reload=True, date_runner=native_runner("reload_sibling")
    )
    times = source["last_run_timings"]
    assert times["source_wait_seconds"] == 67.5
    assert times["network_seconds"] >= 0 and times["local_compute_seconds"] >= 0
    assert times["r2_seconds"] is None and times["control_seconds"] >= 0
    published = publish(job, job.plan_sha, confirm=True, store=storage(Server()))
    times = published["last_run_timings"]
    assert times["r2_seconds"] >= 0 and times["local_compute_seconds"] >= 0
    assert times["source_wait_seconds"] is None and times["control_seconds"] is None


def test_unknown_sdk_unwind_keeps_durable_individual_source_proof_without_adoption(tmp_path):
    import pytest

    from pipeline.history_state import digest

    job = Job.create(
        tmp_path / "job", "2026-04-29", "2026-04-29", ["hr", "hrv"], "legacy", "synthetic"
    )
    with pytest.raises((OSError, __import__("pipeline.history_state", fromlist=["Stop"]).Stop)):
        collect(job, job.plan_sha, confirm=True, date_runner=native_runner("unknown_after_hr"))
    assert job.status()["locked"] and job.status()["counts"] == {"pending": 2}
    proofs = list((job.folder / "runs").glob("*/2026-04-29/SOURCE-hr-*.json"))
    assert len(proofs) == 1
    proof = json.loads(proofs[0].read_bytes())
    assert proof["target"]["status"] == "ready" and proof["enclosing_status"] == "pending"
    assert (
        digest((proofs[0].parent / proof["target"]["wire"]).read_bytes())
        == proof["target"]["wire_sha256"]
    )


def test_bound_old_journal_cannot_be_truncated_to_refund_quota(tmp_path):
    from datetime import datetime, timezone

    import pytest

    from pipeline.history_state import Stop

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hr"], "legacy", "synthetic")
    old = journal(
        tmp_path / "old.jsonl",
        [
            {
                "type": "date_reserved",
                "id": "old:unknown",
                "UTC": "2026-04-29T12:00:00Z",
                "HTTP_reserved": 8,
            }
        ],
    )
    options = {
        "confirm": True,
        "now": datetime(2026, 4, 30, tzinfo=timezone.utc),
        "date_runner": lambda *a: pytest.fail("source after unverified quota"),
    }
    with pytest.raises(Stop):
        collect(job, job.plan_sha, ledger_paths=[old], **options)
    old.write_bytes(b"")
    with pytest.raises(Stop) as failure:
        collect(job, job.plan_sha, **options)
    assert failure.value.category == "quota_journal_changed"
