"""Bounded serial native date collection; no credentials in the parent process."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

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
    ledger_paths=(),
    target_dates=None,
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
    pending = [
        r
        for r in state["rows"]
        if r["status"] == "pending" and (target_dates is None or r["date"] in target_dates)
    ]
    dates = list(dict.fromkeys(r["date"] for r in pending))[:max_dates]
    if not dates:
        return job.status()
    from .history_state import rolling_usage

    usage = rolling_usage(job, ledger_paths, now)
    if usage["unknown_reservations"]:
        raise Stop("unknown_source_reservation_hold")
    if usage["HTTP"] >= 240 or (reload and usage["reloads"] >= 30):
        raise Stop("rolling_source_budget")
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
    pending = [
        r
        for r in state["rows"]
        if r["status"] == "pending" and (target_dates is None or r["date"] in target_dates)
    ]
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
    child_seconds = parent_wait = 0.0
    timing_totals = {
        "source_wait_seconds": 0.0,
        "network_seconds": 0.0,
        "local_compute_seconds": 0.0,
    }
    try:
        for day in dates:
            if (folder / "STOP").exists():
                terminal = "user_stop"
                break
            remaining_seconds = 2400 - (time.monotonic() - started)
            if remaining_seconds <= 31 or http >= max_http:
                break
            if http:
                wait_started = time.monotonic()
                time.sleep(1.5)  # Cross-child pacing; each child also uses monotonic pacing.
                parent_wait += time.monotonic() - wait_started
            usage = rolling_usage(job, ledger_paths, now)
            if usage["unknown_reservations"]:
                raise Stop("unknown_source_reservation_hold")
            if usage["HTTP"] >= 240 or (reload and usage["reloads"] >= 30):
                terminal = "rolling_source_budget"
                break
            target_streams = [r["stream"] for r in pending if r["date"] == day]
            dated = folder / day
            dated.mkdir()
            intent = {
                "run": run,
                "date": day,
                "streams": target_streams,
                "plan": job.plan_sha,
                "max_http": min(8, max_http - http, 240 - usage["HTTP"]),
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
            reservation = run + ":" + day
            job.append(
                {
                    "type": "date_reserved",
                    "id": reservation,
                    "UTC": (now or datetime.now(timezone.utc)).isoformat(),
                    "HTTP_reserved": intent["max_http"],
                    "reload_reserved": int(reload),
                }
            )
            child_started = time.monotonic()
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
            child_seconds += time.monotonic() - child_started
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
            for field in timing_totals:
                value = receipt.get("timings", {}).get(field)
                timing_totals[field] = (
                    timing_totals[field] + value
                    if value is not None and timing_totals[field] is not None
                    else None
                )
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
            job.append(
                {
                    "type": "date_closed",
                    "id": reservation,
                    "UTC": (now or datetime.now(timezone.utc)).isoformat(),
                    "physicalHTTP": receipt["HTTP"],
                    "reload_ACK": receipt.get("reload_ACK", 0),
                    "reload_intent": receipt.get("reload_intent", int(reload)),
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
        if timing_totals["source_wait_seconds"] is not None:
            timing_totals["source_wait_seconds"] += parent_wait
        job.append(
            {
                "type": "end",
                "run": run,
                "HTTP": http,
                "status": terminal or "bounded_complete",
                "timings": {
                    **timing_totals,
                    "r2_seconds": None,
                    "control_seconds": max(
                        0.0, time.monotonic() - started - child_seconds - parent_wait
                    ),
                },
            }
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
    unhandled = False
    observations = {}

    def observed(stream):
        row = {**source.read(stream), "source_verified": True}
        observations[stream] = observations.get(stream, 0) + 1
        write(
            folder / f"SOURCE-{stream}-{observations[stream]:02d}.json",
            {
                "intent_sha256": receipt["intent_sha256"],
                "target": row,
                "enclosing_status": "pending",
            },
        )
        return row

    try:
        source = source_factory(job.plan, intent, folder)
        for stream in intent["streams"]:
            targets.append(observed(stream))
        missing = [r["stream"] for r in targets if r["status"] == "empty_unresolved"]
        if missing and intent["archive_reload"]:
            source.reload()
            source.wait_archive()
            for index, row in enumerate(targets):
                if row["stream"] in missing:
                    targets[index] = observed(row["stream"])
        receipt["status"] = "complete"
    except Stop as error:
        receipt.update(status="stopped", category=error.category, not_before=error.not_before)
        # Each completed stream keeps its own durable proof. The enclosing
        # native result remains stopped; unfinished streams receive the failure.
        finished = {row["stream"] for row in targets if row["status"] == "ready"}
        targets[:] = [row for row in targets if row["stream"] in finished] + [
            {
                "date": day,
                "stream": s,
                "status": error.category
                if error.category in {"rate_limited", "denied", "unsupported", "failed"}
                else "failed",
                "reason": error.category,
            }
            for s in intent["streams"]
            if s not in finished
        ]
    except BaseException:
        unhandled = True
        raise
    finally:
        if source is not None:
            receipt["HTTP"] = source.http_count
            receipt["reload_intent"] = int(source.reload_used)
            receipt["reload_ACK"] = int(getattr(source, "reload_ack", False))
            primary = unhandled or receipt.get("status") == "stopped"
            try:
                source.close()
                receipt["cleanup_status"] = "complete"
            except BaseException:
                receipt["cleanup_status"] = "failed"
                if not primary:
                    receipt.update(status="stopped", category="cleanup_failed", not_before=None)
            receipt["timings"] = source.timings()
    write(folder / "RECEIPT.json", receipt)
    return 0 if receipt["status"] == "complete" else 2
