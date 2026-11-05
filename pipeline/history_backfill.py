"""Opt-in local HR/HRV history. Offline commands never import credential clients."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
from pathlib import Path

from .history_state import DurabilityError, Job, Stop, write
from .output_privacy import SafeArgumentParser


def daily(job, args):
    """A bounded serial source/publication run, including closed references."""
    if job.plan.get("quota_experiment") is not None:
        raise Stop("experiment_requires_single_date_collect")
    from .history_collect import collect
    from .history_publish import publish

    if (
        not args.confirm_cloud_jobs_paused
        or not args.confirm_r2_write
        or not 1 <= args.max_dates <= 30
        or not 1 <= args.max_http <= 240
    ):
        raise Stop("explicit_bounded_run_required")
    initial = job.snapshot()
    dates = list(
        dict.fromkeys(row["date"] for row in initial["rows"] if row["status"] in {"pending", "ready"})
    )[: args.max_dates]
    source_needed = any(
        row["status"] == "pending" and row["date"] in dates for row in initial["rows"]
    )
    state = job.admission(args.plan_sha, args.resume, source=source_needed)
    http = phases = 0
    execution_context = {}
    reports = []
    raw_deferred = False

    def publish_phase(target_dates=None):
        nonlocal phases
        result = publish(
            job, args.plan_sha, confirm=True, target_dates=target_dates,
            execution_context=execution_context, finalize_dates=dates or None,
        )
        phases += 1
        reports.append(result["baseline_refresh"])
        return result

    for offset in range(0, len(dates), 7):
        chunk = dates[offset : offset + 7]
        pending = any(
            row["status"] == "pending" and row["date"] in chunk for row in job.snapshot()["rows"]
        )
        if pending:
            if http >= args.max_http:
                raw_deferred = True
                break
            before = len(job.events()[0])
            result = collect(
                job,
                args.plan_sha,
                resume=args.resume,
                confirm=True,
                reload=args.archive_reload,
                max_dates=len(chunk),
                max_http=min(56, args.max_http - http),
                ledger_paths=args.ledger,
                target_dates=chunk,
            )
            http += sum(
                frame["event"]["physicalHTTP"]
                for frame in job.events()[0][before:]
                if frame["event"]["type"] == "date_closed"
            )
            if result["last_run_status"] != "bounded_complete":
                return {
                    **result, "daily_status": "source_stopped", "daily_HTTP": http,
                    "daily_publication_phases": phases, "daily_baseline_reports": reports,
                }
        elif state["last_run_status"] not in {None, "bounded_complete", "published", "user_stop"}:
            return {
                **job.status(), "daily_status": "source_stopped", "daily_HTTP": http,
                "daily_publication_phases": phases, "daily_baseline_reports": reports,
            }
        if any(row["status"] == "ready" and row["date"] in chunk for row in job.snapshot()["rows"]):
            publish_phase(chunk)
        if any(
            row["status"] in {"ready", "pending"} and row["date"] in chunk
            for row in job.snapshot()["rows"]
        ):
            raw_deferred = True
            break

    # A previous report cannot prove today's queue/period closure. This ordinary
    # R2-only phase needs no Garmin credential and respects a source-only pause.
    if not dates:
        publish_phase([])
    if not raw_deferred:
        while phases < 5 and reports:
            reference = reports[-1]
            if (
                reference.get("status") != "bounded_pending"
                or not reference.get("closed_pending")
                or reference.get("held_periods")
                or reference.get("blocked")
            ):
                break
            result = publish_phase([])
            current = result["baseline_refresh"]
            if not (current.get("periods_completed") or current.get("products_written")):
                break  # A known capacity/no-progress result is not a retry loop.

    result = job.status()
    reference = result["baseline_refresh"]
    references_complete = (
        reference.get("status") in {"complete", "waiting_until_close"}
        and reference.get("closed_pending") == 0
        and not reference.get("held_periods")
        and not reference.get("blocked")
    )
    return {
        **result,
        "daily_status": "raw_pending" if raw_deferred else
        "bounded_complete" if references_complete else "baselines_pending",
        "daily_HTTP": http,
        "daily_publication_phases": phases,
        "daily_baseline_reports": reports,
    }


def main(argv=None):
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path(".granular/history-backfill"))
    commands = parser.add_subparsers(dest="command", required=True, parser_class=SafeArgumentParser)
    plan = commands.add_parser(
        "plan", help="Freeze an offline private dated plan; no credentials or network."
    )
    plan.add_argument("--start", required=True)
    plan.add_argument("--end", required=True)
    plan.add_argument("--streams", required=True)
    plan.add_argument("--provider", choices=["legacy", "personal"], required=True)
    plan.add_argument(
        "--account", required=True, help="Same private GARMIN_ACCOUNT identity; never printed."
    )
    plan.add_argument("--experiment-max-reloads", type=int,
                      help="Opt-in immutable absolute rolling reload ceiling for this finite experiment.")
    plan.add_argument("--experiment-max-http", type=int,
                      help="Opt-in immutable absolute rolling HTTP ceiling for this finite experiment.")
    plan.add_argument("--ledger", action="append", type=Path, default=[],
                      help="Every original/manual/prior-experiment journal; required for experiment plans.")
    commands.add_parser("status", help="Offline verified local progress.")
    commands.add_parser("missing", help="Offline CSV of unresolved or unpublished targets.")
    pause = commands.add_parser(
        "pause", help="Persist an operator quota/not-before without network."
    )
    pause.add_argument("--until", required=True)
    pause.add_argument("--reason", choices=["operator_quota", "no_more_observed"], required=True)
    commands.add_parser("stop", help="Finish the current date then stop this bounded run.")
    for command in ("collect", "resume", "daily"):
        run = commands.add_parser(
            command,
            help="Bounded serial collection and R2 publication."
            if command == "daily"
            else "Explicit bounded source phase; zero R2.",
        )
        run.add_argument("--plan-sha", required=True)
        run.add_argument("--confirm-cloud-jobs-paused", action="store_true")
        run.add_argument("--archive-reload", action="store_true")
        run.add_argument("--max-dates", type=int, default=7)
        run.add_argument("--max-http", type=int, default=56)
        run.add_argument(
            "--ledger",
            action="append",
            type=Path,
            default=[],
            help="Bind an old/manual hash-chained source journal; repeat for each.",
        )
        if command == "daily":
            run.add_argument("--confirm-r2-write", action="store_true")
            run.add_argument("--resume", action="store_true")
    publish = commands.add_parser(
        "publish", help="Separate verified R2-only phase; zero Garmin calls."
    )
    publish.add_argument("--plan-sha", required=True)
    publish.add_argument("--confirm-r2-write", action="store_true")
    child = commands.add_parser("_date", help=argparse.SUPPRESS)
    child.add_argument("--run", required=True)
    child.add_argument("--date", required=True)
    args = parser.parse_args(argv)
    logging.disable(logging.CRITICAL)
    try:
        if args.command == "plan":
            job = Job.create(
                args.state,
                args.start,
                args.end,
                args.streams.split(","),
                args.provider,
                args.account,
                experiment_limits=({"reloads": args.experiment_max_reloads,
                                    "HTTP": args.experiment_max_http}
                                   if args.experiment_max_reloads is not None
                                   or args.experiment_max_http is not None else None),
                ledger_paths=args.ledger,
            )
            print(json.dumps(job.status()))
        else:
            job = Job(args.state)
            if args.command == "pause":
                print(json.dumps(job.pause(args.until, args.reason)))
            elif args.command == "stop":
                from .history_state import read

                lease = json.loads(read(job.folder / "OWNER.lock"))
                run = lease["run"]
                if len(run) != 32 or any(c not in "0123456789abcdef" for c in run):
                    raise Stop("invalid_owner")
                flag = job.folder / "runs" / run / "STOP"
                if not flag.exists():
                    write(flag, {"stop": True})
                print("Stop requested after current date.")
            elif args.command == "daily":
                result = daily(job, args)
                print(json.dumps(result))
                if result["daily_status"] != "bounded_complete":
                    return 2
            elif args.command in ("collect", "resume"):
                from .history_collect import collect

                result = collect(
                    job,
                    args.plan_sha,
                    resume=args.command == "resume",
                    confirm=args.confirm_cloud_jobs_paused,
                    reload=args.archive_reload,
                    max_dates=args.max_dates,
                    max_http=args.max_http,
                    ledger_paths=args.ledger,
                )
                print(json.dumps(result))
                if result["last_run_status"] not in {
                    None,
                    "bounded_complete",
                    "user_stop",
                    "published",
                }:
                    return 2
            elif args.command == "publish":
                from .history_publish import publish

                print(json.dumps(publish(job, args.plan_sha, confirm=args.confirm_r2_write)))
            elif args.command == "_date":
                import sys

                from .history_collect import date_phase

                return date_phase(job, args.run, args.date, sys.stdin.buffer.read(1025))
            elif args.command == "status":
                print(json.dumps(job.status()))
            elif args.command == "missing":
                output = io.StringIO(newline="")
                writer = csv.writer(output, lineterminator="\n")
                writer.writerow(["date", "stream", "status"])
                for row in job.snapshot()["rows"]:
                    if row["status"] != "published":
                        writer.writerow([row["date"], row["stream"], row["status"]])
                print(output.getvalue(), end="")
        return 0
    except (DurabilityError, OSError) as error:
        from .failure_diagnostics import log_failure

        log_failure(error, stage="history_backfill", force=True)
        print("Local history persistence HOLD.", file=__import__("sys").stderr)
        return 3
    except BaseException as error:
        from .failure_diagnostics import log_failure

        log_failure(error, stage="history_backfill", force=True)
        print(
            "Local history HOLD; inspect offline status. No automatic retry.",
            file=__import__("sys").stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
