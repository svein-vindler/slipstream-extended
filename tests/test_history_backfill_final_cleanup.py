"""Success-then-close failure at the normal SDK and real native CLI boundary."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from test_history_backfill_review import ready_job, storage

from pipeline.history_state import Job, Stop


def native_publish(mode, job_folder, capture_folder):
    import boto3
    from history_storage_fixture import ACCOUNT, BUCKET, Server

    from pipeline.history_backfill import main

    job = Job(job_folder)
    server = Server()
    client = storage(server).client
    observations = []
    unregister = client.meta.events.unregister
    close = client.close

    def guard_close(*args, **kwargs):
        if args and args[0] == "before-send.s3" and not kwargs.get("unique_id"):
            observations.append(
                {
                    "closer": "guard",
                    "owner": json.loads((job.folder / "OWNER.lock").read_bytes())
                    if (job.folder / "OWNER.lock").exists()
                    else None,
                }
            )
            if mode in {"guard", "both"}:
                raise OSError("synthetic outer guard close")
        return unregister(*args, **kwargs)

    def client_close():
        observations.append(
            {
                "closer": "client",
                "owner": json.loads((job.folder / "OWNER.lock").read_bytes())
                if (job.folder / "OWNER.lock").exists()
                else None,
            }
        )
        if mode in {"client", "both"}:
            raise OSError("synthetic outer client close")
        return close()

    client.meta.events.unregister = guard_close
    client.close = client_close
    boto3.client = lambda *args, **kwargs: client
    for name, value in {
        "CLOUDFLARE_ACCOUNT_ID": ACCOUNT,
        "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic",
        "R2_SECRET_ACCESS_KEY": "synthetic",
    }.items():
        os.environ[name] = value
    code = main(
        ["--state", str(job.folder), "publish", "--plan-sha", job.plan_sha, "--confirm-r2-write"]
    )
    result = {
        "native": code,
        "closes": observations,
        "puts": server.puts,
        "remote": [
            {
                "key": key,
                "sha256": hashlib.sha256(row[0]).hexdigest(),
                "revision": row[1].strip('"'),
            }
            for key, row in server.objects.items()
        ],
        "gets": sum(c[0] == "GET" and not c[2].get("list-type") for c in server.calls),
    }
    (capture_folder / "observed.json").write_text(json.dumps(result), encoding="utf-8")
    return code


def test_success_then_outer_cleanup_io_holds_before_terminal_projection(monkeypatch, tmp_path):
    import boto3

    from pipeline.history_collect import collect
    from pipeline.history_publish import publish
    from pipeline.sources.provider_config import ProviderSelection

    for mode in ("guard", "client", "both", "normal"):
        capture = tmp_path / mode
        capture.mkdir()
        job = ready_job(capture / "job")
        native = subprocess.run(
            [sys.executable, "-B", str(Path(__file__)), mode, str(job.folder), str(capture)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert native.returncode == (0 if mode == "normal" else 3), native.stdout + native.stderr
        observed = json.loads((capture / "observed.json").read_bytes())
        assert observed["native"] == (0 if mode == "normal" else 3)
        assert len(observed["puts"]) == 3
        assert (
            observed["gets"] == 8
        )  # initial queue + staged preimage + helper queue/preimage + 3 readbacks + final canonical
        assert [c["closer"] for c in observed["closes"]] == ["guard", "client"]
        owner = observed["closes"][0]["owner"]
        assert owner is not None and owner["phase"] == "publish" and owner["plan"] == job.plan_sha
        assert all(c["owner"] == owner for c in observed["closes"])
        status = Job(job.folder).status()
        run = job.folder / "runs" / owner["run"]
        if mode == "normal":
            assert not status["locked"] and status["active_run"] is None
            assert status["counts"] == {"published": 1}
            assert json.loads((run / "RECEIPT.json").read_bytes())["status"] == "complete"
        else:
            assert json.loads((job.folder / "OWNER.lock").read_bytes()) == owner
            assert status["locked"] and status["active_run"] == owner["run"]
            assert status["counts"] == {"ready": 1}
            assert not (run / "RECEIPT.json").exists()
            assert not any(
                f["event"]["type"] == "end" and f["event"]["run"] == owner["run"]
                for f in job.events()[0]
            )
        evidence = json.loads((run / "REMOTE-VERIFIED.json").read_bytes())
        assert evidence["status"] == "remote_verified_cleanup_pending"
        assert evidence["acknowledged_writes"] == 3
        assert evidence["destinations"][0]["sha256"] == next(
            row["sha256"]
            for row in observed["remote"]
            if row["key"] == evidence["destinations"][0]["key"]
        )
        canonical = next(
            row for row in observed["remote"] if row["key"] == evidence["destinations"][0]["key"]
        )
        assert evidence["destinations"][0]["revision"].strip('"') == canonical["revision"]
        assert hashlib.sha256(
            next(run.glob("source-postimage-*.bin")).read_bytes()
        ).hexdigest() == next(
            row["sha256"]
            for row in observed["remote"]
            if row["key"] == evidence["destinations"][0]["key"]
        )
        with monkeypatch.context() as local:
            local.setattr(
                ProviderSelection,
                "credential",
                lambda self: pytest.fail("credential read after HOLD"),
            )
            local.setattr(
                boto3, "client", lambda *a, **kw: pytest.fail("storage admission after HOLD")
            )
            if mode == "normal":
                assert collect(
                    job,
                    job.plan_sha,
                    confirm=True,
                    date_runner=lambda *a: pytest.fail("source after complete"),
                )["counts"] == {"published": 1}
                # A later explicit publish may drain queued references; unlike
                # collect it deliberately requires R2 admission even with no ready rows.
            else:
                with pytest.raises(Stop):
                    collect(
                        job,
                        job.plan_sha,
                        confirm=True,
                        date_runner=lambda *a: pytest.fail("source admission after HOLD"),
                    )
                with pytest.raises(Stop):
                    publish(job, job.plan_sha, confirm=True)


if __name__ == "__main__":
    raise SystemExit(native_publish(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])))
