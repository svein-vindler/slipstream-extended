"""Bounded serial native date collection; no credentials in the parent process."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime

from .history_state import DurabilityError, Stop, digest, encoded, read, write


def collect(
    job,
    plan_sha,
    *,
    resume=False,
    confirm=False,
    reload=False,
    max_dates=7,
    max_http=56,
    date_runner=None,
    now=None,
):
    if (
        not confirm
        or type(max_dates) is not int
        or not 1 <= max_dates <= 7
        or type(max_http) is not int
        or not 1 <= max_http <= 56
    ):
        raise Stop("explicit_bounded_run_required")
    state = job.admission(plan_sha, resume, now)
    pending = [r for r in state["rows"] if r["status"] == "pending"]
    dates = list(dict.fromkeys(r["date"] for r in pending))[:max_dates]
    if not dates:
        return job.status()
    run = uuid.uuid4().hex
    nonce = uuid.uuid4().hex
    folder = job.folder / "runs" / run
    folder.mkdir(parents=True)
    owner = {"run": run, "plan": job.plan_sha, "phase": "collect", "nonce": digest(nonce.encode())}
    write(job.folder / "OWNER.lock", owner)
    try:
        state = job.admission(plan_sha, resume, now, owner=owner)
    except Stop:
        # This known preflight pause occurs before begin/credentials/source.
        if json.loads(read(job.folder / "OWNER.lock")) == owner:
            (job.folder / "OWNER.lock").unlink()
        raise
    pending = [r for r in state["rows"] if r["status"] == "pending"]
    dates = list(dict.fromkeys(r["date"] for r in pending))[:max_dates]
    if not dates:
        (job.folder / "OWNER.lock").unlink()
        return job.status()
    job.append({"type": "begin", "run": run})
    if resume and state["pause"]:
        job.append({"type": "resume"})
    if reload and job.plan["provider"] != "legacy":
        for row in pending:
            if row["date"] in dates:
                job.append(
                    {
                        "type": "target",
                        "date": row["date"],
                        "stream": row["stream"],
                        "status": "unsupported",
                    }
                )
        job.append({"type": "end", "run": run, "status": "unsupported", "HTTP": 0})
        (job.folder / "OWNER.lock").unlink()
        return job.status()
    http = 0
    empty_dates = 0
    terminal = None
    started = time.monotonic()
    try:
        for day in dates:
            if (folder / "STOP").exists():
                terminal = "user_stop"
                break
            remaining_seconds = 2400 - (time.monotonic() - started)
            if remaining_seconds <= 31 or http >= max_http:
                break
            if http:
                time.sleep(1.5)  # Cross-child pacing; each child also uses monotonic pacing.
            target_streams = [r["stream"] for r in pending if r["date"] == day]
            dated = folder / day
            dated.mkdir()
            intent = {
                "run": run,
                "date": day,
                "streams": target_streams,
                "plan": job.plan_sha,
                "max_http": min(8, max_http - http),
                "max_seconds": min(899, remaining_seconds - 30),
                "archive_reload": reload,
                "nonce": digest(nonce.encode()),
            }
            write(dated / "INTENT.json", intent)
            args = [
                sys.executable,
                "-B",
                "-m",
                "pipeline.history_backfill",
                "--state",
                str(job.folder),
                "_date",
                "--run",
                run,
                "--date",
                day,
            ]
            if date_runner is None:
                outcome = subprocess.run(
                    args,
                    input=encoded({"nonce": nonce}),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(930, remaining_seconds),
                    check=False,
                )
            else:
                outcome = date_runner(args, encoded({"nonce": nonce}), dated)
            if type(outcome.returncode) is not int or outcome.returncode not in (0, 2):
                if outcome.returncode == 3:
                    raise DurabilityError()
                raise Stop("unknown_native_hold")
            receipt_bytes = read(dated / "RECEIPT.json")
            receipt = json.loads(receipt_bytes)
            if (
                receipt["intent_sha256"] != digest(read(dated / "INTENT.json"))
                or receipt["HTTP"] > intent["max_http"]
            ):
                raise Stop("native_receipt_changed")
            if (outcome.returncode == 0) != (receipt["status"] == "complete"):
                raise Stop("native_semantics_mismatch")
            http += receipt["HTTP"]
            write(
                dated / "RESULT.json",
                {"native": outcome.returncode, "receipt_sha256": digest(receipt_bytes)},
            )
            positive = False
            for row in receipt["targets"]:
                if row["date"] != day or row["stream"] not in target_streams:
                    raise Stop("source_target_mismatch")
                if row["status"] == "ready":
                    for field in ("wire", "view"):
                        body = read(dated / row[field], 5 * 1024 * 1024)
                        if digest(body) != row[field + "_sha256"]:
                            raise Stop("raw_changed")
                    positive = True
                job.append(
                    {
                        "type": "target",
                        **row,
                        "folder": str(dated),
                        "receipt_sha256": digest(receipt_bytes),
                        "native": outcome.returncode,
                    }
                )
            empty_dates = 0 if positive else empty_dates + 1
            if outcome.returncode == 2:
                terminal = receipt["category"]
                if terminal == "rate_limited":
                    job.append(
                        {"type": "pause", "reason": terminal, "not_before": receipt["not_before"]}
                    )
                break
        # Empty observations are neither a quota nor permanent absence. The
        # operator explicitly resumes; no automatic next day or retry loop.
        closed = [f["event"] for f in job.events()[0] if f["event"]["type"] == "batch"]
        all_empty = empty_dates == len(dates) and len(dates) > 0 and terminal is None
        job.append({"type": "batch", "all_empty": all_empty})
        if all_empty and closed and closed[-1]["all_empty"]:
            job.append({"type": "pause", "reason": "no_more_observed", "not_before": None})
        job.append(
            {"type": "end", "run": run, "HTTP": http, "status": terminal or "bounded_complete"}
        )
        (job.folder / "OWNER.lock").unlink()
        return job.status()
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        write(folder / "CANCEL", {"stop": True})
        raise Stop("unknown_native_hold") from None


def date_phase(job, run, day, handoff, *, source_factory=None):
    if len(run) != 32 or any(c not in "0123456789abcdef" for c in run):
        raise Stop("invalid_run")
    datetime.strptime(day, "%Y-%m-%d")
    folder = job.folder / "runs" / run / day
    intent = json.loads(read(folder / "INTENT.json"))
    lease = json.loads(read(job.folder / "OWNER.lock"))
    nonce = json.loads(handoff)["nonce"]
    if (
        intent["run"] != run
        or intent["date"] != day
        or intent["plan"] != job.plan_sha
        or lease
        != {"run": run, "plan": job.plan_sha, "phase": "collect", "nonce": digest(nonce.encode())}
        or intent["nonce"] != lease["nonce"]
    ):
        raise Stop("date_admission_rejected")
    planned = {r["stream"] for r in job.plan["targets"] if r["date"] == day}
    if (
        not intent["streams"]
        or not set(intent["streams"]) <= planned
        or len(set(intent["streams"])) != len(intent["streams"])
        or type(intent["max_http"]) is not int
        or not 1 <= intent["max_http"] <= 8
        or type(intent["archive_reload"]) is not bool
    ):
        raise Stop("date_outside_plan")
    write(folder / "DATE.lock", {"intent_sha256": digest(read(folder / "INTENT.json"))})
    if source_factory is None:
        from .history_source import HistorySource

        source_factory = HistorySource
    targets = []
    receipt = {"intent_sha256": digest(read(folder / "INTENT.json")), "targets": targets, "HTTP": 0}
    source = None
    try:
        source = source_factory(job.plan, intent, folder)
        for stream in intent["streams"]:
            targets.append(source.read(stream))
        missing = [r["stream"] for r in targets if r["status"] == "empty_unresolved"]
        if missing and intent["archive_reload"]:
            source.reload()
            source.wait_archive()
            targets[:] = [
                source.read(r["stream"]) if r["stream"] in missing else r for r in targets
            ]
        receipt["status"] = "complete"
    except Stop as error:
        receipt.update(status="stopped", category=error.category, not_before=error.not_before)
        # The failed native date cannot promote positive siblings. Raw evidence remains.
        targets[:] = [
            {
                "date": day,
                "stream": s,
                "status": error.category
                if error.category in {"rate_limited", "denied", "unsupported", "failed"}
                else "failed",
                "reason": error.category,
            }
            for s in intent["streams"]
        ]
    finally:
        if source is not None:
            receipt["HTTP"] = source.http_count
            import sys

            primary = sys.exception() is not None
            try:
                source.close()
            except BaseException:
                if not primary:
                    raise
    write(folder / "RECEIPT.json", receipt)
    return 0 if receipt["status"] == "complete" else 2
