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
    commands.add_parser("status", help="Offline verified local progress.")
    commands.add_parser("missing", help="Offline CSV of unresolved or unpublished targets.")
    pause = commands.add_parser(
        "pause", help="Persist an operator quota/not-before without network."
    )
    pause.add_argument("--until", required=True)
    pause.add_argument("--reason", choices=["operator_quota", "no_more_observed"], required=True)
    commands.add_parser("stop", help="Finish the current date then stop this bounded run.")
    for command in ("collect", "resume"):
        run = commands.add_parser(command, help="Explicit bounded source phase; zero R2.")
        run.add_argument("--plan-sha", required=True)
        run.add_argument("--confirm-cloud-jobs-paused", action="store_true")
        run.add_argument("--archive-reload", action="store_true")
        run.add_argument("--max-dates", type=int, default=7)
        run.add_argument("--max-http", type=int, default=56)
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
