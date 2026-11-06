"""Opt-in local HR/HRV history. Offline commands never import credential clients."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import uuid
from pathlib import Path

from .history_state import DurabilityError, Job, Stop, write
from .output_privacy import SafeArgumentParser


def daily(job, args, *, clock=None):
    """A bounded serial source/publication run, including closed references."""
    if job.plan.get("quota_experiment") is not None:
        raise Stop("experiment_requires_single_date_collect")
    from .history_collect import collect
    from .history_publish import publish
    from .history_source import Clock
    from .history_state import rolling_usage, utc

    clock = clock or Clock()
    wait_limit = getattr(args, "max_wait_seconds", 3600)

    if (
        not args.confirm_cloud_jobs_paused
        or not args.confirm_r2_write
        or not 1 <= args.max_dates <= 30
        or not 1 <= args.max_http <= 240
        or type(wait_limit) is not int or not 0 <= wait_limit <= 21600
    ):
        raise Stop("explicit_bounded_run_required")
    initial = job.admission(args.plan_sha, args.resume, source=False)
    previous = initial["baseline_refresh"]
    previous_dates = previous.get("finalize_dates", [])
    previous_complete = previous.get("original_cohort_complete", previous.get("cohort_complete", (
        previous.get("status") in {"complete", "waiting_until_close"}
        and previous.get("closed_pending") == 0
        and not previous.get("held_periods") and not previous.get("blocked")
    )))
    continue_cohort = bool(previous_dates) and (
        not previous_complete or any(
            row["status"] in {"pending", "ready"} and row["date"] in previous_dates
            for row in initial["rows"]
        )
    )
    # The durable round owns a fixed selection across process restarts. Stored
    # curves and a prior publication cohort never consume the new-source goal.
    active_round = None
    round_start = 0
    frames = job.events()[0]
    for index, frame in enumerate(frames):
        event = frame["event"]
        if event["type"] == "daily_round_started":
            if active_round is not None:
                raise Stop("daily_round_unclosed")
            active_round, round_start = event, index + 1
        elif event["type"] == "daily_round_finished":
            if active_round is None or active_round["round"] != event["round"]:
                raise Stop("daily_round_binding_changed")
            active_round = None
    if active_round is None:
        stored_dates = list(dict.fromkeys(
            row["date"] for row in initial["rows"] if row["status"] == "ready"
        ))
        source_dates = list(dict.fromkeys(
            row["date"] for row in initial["rows"]
            if row["status"] == "pending" and row["date"] not in stored_dates
        ))[:args.max_dates]
        dates = list(dict.fromkeys(stored_dates + source_dates))
        cohort_dates = list(dict.fromkeys(
            (previous_dates if continue_cohort else []) + dates
        ))
        active_round = {
            "type": "daily_round_started", "round": uuid.uuid4().hex,
            "dates": dates, "new_dates": source_dates, "stored_dates": stored_dates,
            "finalize_dates": cohort_dates, "max_dates": args.max_dates,
            "max_http": args.max_http,
        }
        job.append(active_round)
        round_start = len(job.events()[0])
    elif (active_round["max_dates"], active_round["max_http"]) != (args.max_dates, args.max_http):
        raise Stop("daily_round_parameters_changed")
    planned_dates = {row["date"] for row in job.plan["targets"]}
    if (
        not isinstance(active_round.get("round"), str)
        or len(active_round["round"]) != 32
        or any(char not in "0123456789abcdef" for char in active_round["round"])
        or any(
            not isinstance(active_round.get(field), list)
            or any(not isinstance(day, str) or day not in planned_dates for day in active_round[field])
            or len(set(active_round[field])) != len(active_round[field])
            for field in ("dates", "new_dates", "stored_dates", "finalize_dates")
        )
        or not set(active_round["new_dates"]) <= set(active_round["dates"])
        or not set(active_round["stored_dates"]) <= set(active_round["dates"])
        or set(active_round["new_dates"]) & set(active_round["stored_dates"])
        or not set(active_round["dates"]) <= set(active_round["finalize_dates"])
        or len(active_round["new_dates"]) > args.max_dates
    ):
        raise Stop("daily_round_binding_changed")
    dates = active_round["dates"]
    cohort_dates = active_round["finalize_dates"]
    source_dates = active_round["new_dates"]
    state = initial
    # Persisted cohort dates keep a stored-only continuation scoped to this
    # round, rather than recalculating unrelated history in a multi-year plan.
    # One bounded slot per affected period also covers a known split product
    # checkpoint. Requests/bytes still share the unchanged execution budget.
    phase_limit = 14
    round_events = [frame["event"] for frame in job.events()[0][round_start:]]
    source_stop = None
    for event in round_events:
        if event["type"] == "end" and event.get("status") == "empty_unresolved":
            source_stop = "empty_unresolved"
        elif event["type"] == "resume":
            source_stop = None
    if source_stop is not None and args.resume:
        # Explicit resume still uses the ordinary pause/clock/owner admission.
        # Collect records the actual resume only when it can start safely.
        job.admission(args.plan_sha, True, clock.now())
        source_stop = None
    http = sum(event["physicalHTTP"] for event in round_events if event["type"] == "date_closed")
    # Keep the existing 256-GET allowance for independent destination checks.
    execution_context = {"publication_get_limit": 6000, "round": active_round["round"]}
    reports = [event["report"] for event in round_events if event["type"] == "reference_refresh"]
    phases = len(reports)
    raw_deferred = False
    wait_seconds = 0.0
    next_source = None

    def publish_phase(target_dates=None, *, defer_references=False):
        nonlocal phases
        result = publish(
            job, args.plan_sha, confirm=True, target_dates=target_dates,
            execution_context=execution_context, finalize_dates=cohort_dates,
            defer_references=defer_references,
        )
        phases += 1
        reports.append(result["baseline_refresh"])
        return result

    def result_fields():
        closed_dates = {
            frame["event"]["id"].rsplit(":", 1)[-1]
            for frame in job.events()[0][round_start:]
            if frame["event"]["type"] == "date_closed"
        }
        return {
            "daily_HTTP": http, "daily_publication_phases": phases,
            "daily_baseline_reports": reports,
            "daily_new_date_target": active_round["max_dates"],
            "daily_new_dates": len(closed_dates.intersection(source_dates)),
            "daily_stored_dates_reused": len(active_round["stored_dates"]),
            "daily_round": active_round["round"],
            "daily_wait_seconds": wait_seconds,
            "daily_not_before": next_source,
            "daily_source_stop": source_stop,
        }

    def source_admitted():
        nonlocal wait_seconds, next_source
        # A real provider/operator pause is never converted into a local wait.
        # Only expiry of fully closed, validated rolling usage can resume here.
        while True:
            job.admission(args.plan_sha, args.resume, clock.now())
            usage = rolling_usage(job, args.ledger, clock.now())
            if usage["unknown_reservations"]:
                raise Stop("unknown_source_reservation_hold")
            next_source = usage["next_date_not_before"]
            if next_source is None:
                raise Stop("unknown_source_reservation_hold")
            remaining = (utc(next_source) - clock.now()).total_seconds()
            if remaining <= 0:
                next_source = None
                return True
            allowance = wait_limit - wait_seconds
            if allowance <= 0 or remaining > allowance:
                return False
            duration = min(60.0, remaining)
            started = clock.monotonic()
            clock.sleep(duration)
            elapsed = clock.monotonic() - started
            if elapsed <= 0:
                raise Stop("source_wait_clock_unknown")
            wait_seconds += elapsed

    for offset in range(0, len(dates), 7):
        if source_stop is not None:
            break
        chunk = dates[offset : offset + 7]
        pending = any(
            row["status"] == "pending" and row["date"] in chunk for row in job.snapshot()["rows"]
        )
        while pending:
            if http >= args.max_http:
                raw_deferred = True
                break
            if not source_admitted():
                return {
                    **job.status(), "daily_status": "source_budget_pending", **result_fields(),
                }
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
                now=clock.now,
            )
            http += sum(
                frame["event"]["physicalHTTP"]
                for frame in job.events()[0][before:]
                if frame["event"]["type"] == "date_closed"
            )
            if result["last_run_status"] == "rolling_source_budget":
                continue
            if result["last_run_status"] == "empty_unresolved":
                source_stop = "empty_unresolved"
                break
            if result["last_run_status"] != "bounded_complete":
                return {
                    **result, "daily_status": "source_stopped", **result_fields(),
                }
            break
        if source_stop is not None or raw_deferred:
            break
        if not pending and state["last_run_status"] not in {
            None, "bounded_complete", "published", "user_stop"
        }:
            return {
                **job.status(), "daily_status": "source_stopped", **result_fields(),
            }
        if any(row["status"] == "ready" and row["date"] in chunk for row in job.snapshot()["rows"]):
            if phases >= phase_limit:
                raw_deferred = True
                break
            publish_phase(chunk, defer_references=True)
        if any(
            row["status"] in {"ready", "pending"} and row["date"] in chunk
            for row in job.snapshot()["rows"]
        ):
            raw_deferred = True
            break

    if source_stop is not None:
        ready = any(
            row["status"] == "ready" and row["date"] in dates
            for row in job.snapshot()["rows"]
        )
        if not ready:
            # The durable source stop survives publication's later end status.
            # Reopening it is not an automatic reference retry; explicit R2-only
            # publish remains available under the source-only pause.
            return {
                **job.status(), "daily_status": "source_stopped", **result_fields(),
            }
        if phases < phase_limit:
            publish_phase(dates, defer_references=True)
        raw_deferred = any(
            row["status"] == "ready" and row["date"] in dates
            for row in job.snapshot()["rows"]
        )

    # A previous report cannot prove today's queue/period closure. This ordinary
    # R2-only phase needs no Garmin credential and respects a source-only pause.
    if not raw_deferred and phases < phase_limit:
        # Raw chunks deliberately leave assembly for a fresh bounded phase after
        # every selected canonical curve/index has its ACK and exact readback.
        # Even an unknown pending count from a capacity checkpoint cannot consume
        # this reserved first finishing slot.
        publish_phase([])
        while phases < phase_limit:
            reference = reports[-1]
            progress = reference.get("periods_completed") or reference.get("products_written")
            if (
                reference.get("cohort_complete")
                or reference.get("status") not in {"bounded_pending", "held"}
                or reference.get("cohort_closed_pending", reference.get("closed_pending")) == 0
                or reference.get("cohort_held_periods", reference.get("held_periods"))
                or reference.get("cohort_blocked", reference.get("blocked"))
                or not progress
            ):
                break  # Known capacity/no progress stays pending, without retry.
            publish_phase([])

    result = job.status()
    reference = result["baseline_refresh"]
    global_complete = (
        reference.get("status") in {"complete", "waiting_until_close"}
        and reference.get("closed_pending") == 0
        and not reference.get("held_periods")
        and not reference.get("blocked")
    )
    # An older journal without a bound cohort cannot turn an empty period
    # filter into proof that its still-pending references have completed.
    references_complete = (
        reference.get("cohort_complete", global_complete) if cohort_dates else global_complete
    )
    if source_stop is None and not raw_deferred and references_complete:
        job.append({"type": "daily_round_finished", "round": active_round["round"]})
    return {
        **result,
        "daily_status": "source_stopped" if source_stop is not None else
        "raw_pending" if raw_deferred else
        "bounded_complete" if references_complete else "baselines_pending",
        **result_fields(),
    }


def continue_publication(job, args):
    """Explicit, receipt-bound R2-only completion; never admits source work."""
    from .history_publish import admit_continuation, publish

    admission = args.continuation_sha
    if admission:
        if args.prior_receipt_sha is not None or args.independent_gets is not None or args.period:
            raise Stop("continuation_binding_unverified")
    else:
        if args.prior_receipt_sha is None or args.independent_gets is None or not args.period:
            raise Stop("continuation_binding_unverified")
        admission = admit_continuation(
            job, args.plan_sha, confirm=args.confirm_r2_write,
            prior_receipt_sha256=args.prior_receipt_sha,
            independent_gets=args.independent_gets, period_names=args.period,
        )
    reports = []
    for _ in range(14):
        try:
            result = publish(
                job, args.plan_sha, confirm=args.confirm_r2_write,
                continuation_sha256=admission,
            )
        except Stop as error:
            state = job.status()
            if error.category != "storage_execution_budget" or state["locked"]:
                raise
            return {
                **state, "continuation_status": "budget_pending",
                "publication_continuation_sha256": admission,
                "continuation_reports": reports,
            }
        reference = result["baseline_refresh"]
        reports.append(reference)
        if reference.get("continuation_complete"):
            return {
                **result, "continuation_status": "bounded_complete",
                "publication_continuation_sha256": admission,
                "continuation_reports": reports,
            }
        if (
            reference.get("status") != "bounded_pending"
            or reference.get("held_periods") or reference.get("blocked")
            or not (reference.get("products_written") or reference.get("periods_completed"))
        ):
            break
    return {
        **job.status(), "continuation_status": "references_pending",
        "publication_continuation_sha256": admission, "continuation_reports": reports,
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
            run.add_argument("--max-wait-seconds", type=int, default=3600,
                             help="Wait only for validated rolling-source expiry; 0..21600 seconds.")
    publish = commands.add_parser(
        "publish", help="Separate verified R2-only phase; zero Garmin calls."
    )
    publish.add_argument("--plan-sha", required=True)
    publish.add_argument("--confirm-r2-write", action="store_true")
    continuation = commands.add_parser(
        "continue-publication", help="Explicit receipt-bound R2-only reference continuation."
    )
    continuation.add_argument("--plan-sha", required=True)
    continuation.add_argument("--confirm-r2-write", action="store_true")
    continuation.add_argument("--prior-receipt-sha")
    continuation.add_argument("--independent-gets", type=int)
    continuation.add_argument("--period", action="append", default=[])
    continuation.add_argument("--continuation-sha", help="Resume the existing immutable admission.")
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
            elif args.command == "continue-publication":
                result = continue_publication(job, args)
                print(json.dumps(result))
                if result["continuation_status"] != "bounded_complete":
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
