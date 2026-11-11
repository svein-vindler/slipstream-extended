"""Opt-in local HR/HRV history. Offline commands never import credential clients."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import uuid
from contextlib import ExitStack
from pathlib import Path

from .history_state import DurabilityError, Job, Stop, write
from .output_privacy import SafeArgumentParser


def _reference_batch_selection(args):
    selected = getattr(args, "reference_max_periods", 2)
    if type(selected) is not int or not 1 <= selected <= 8:
        raise Stop("reference_batch_limit")
    return selected


def daily(job, args, *, clock=None):
    """Run the explicit local batch choice without changing other callers."""
    selected = _reference_batch_selection(args)
    if selected == 2:
        return _daily(job, args, clock=clock)
    with ExitStack() as publication_owner:
        result = _daily(job, args, clock=clock, reference_max_periods=selected,
                        publication_owner=publication_owner)
    # Command-owned cleanup must succeed before this durable terminal event.
    if result["daily_status"] == "bounded_complete":
        job.append({"type": "daily_round_finished", "round": result["daily_round"]})
    return result


def _daily(job, args, *, clock=None, reference_max_periods=2, publication_owner=None):
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
    totals = reports[-1].get("execution_totals", {}) if reports else {}
    phases = totals.get("phases", len(reports))
    invocation_reports = []
    if totals.get("reference_successor_sha256") is not None:
        execution_context["reference_successor_sha256"] = totals["reference_successor_sha256"]
    if (reports and cohort_dates and not reports[-1].get("cohort_complete")
            and not any(row["status"] in {"ready", "pending"} and row["date"] in cohort_dates
                        for row in initial["rows"])):
        from .history_reference_successor import admit, capacity_exhausted
        from .rolling_reference_span import effective_source_totals

        if capacity_exhausted(effective_source_totals(job, totals)):
            execution_context["reference_successor_sha256"] = admit(
                job, args.plan_sha, active_round["round"], cohort_dates,
            )
            phases = 0  # The publisher validates and applies this exact proof before I/O.
    raw_deferred = False
    wait_seconds = 0.0
    next_source = None

    publication_root = None

    def publish_phase(target_dates=None, *, defer_references=False):
        nonlocal phases, publication_root
        kwargs = {}
        if publication_owner is not None:
            from .history_publish import publication_store

            if publication_root is None:
                publication_root = publication_owner.enter_context(publication_store())
            kwargs["store"] = publication_root.new_stage()
        result = publish(
            job, args.plan_sha, confirm=True, target_dates=target_dates,
            execution_context=execution_context, finalize_dates=cohort_dates,
            defer_references=defer_references, reference_max_periods=reference_max_periods, **kwargs,
        )
        phases = max(phases + 1, result["baseline_refresh"].get("execution_totals", {}).get("phases", 0))
        reports.append(result["baseline_refresh"])
        invocation_reports.append(result["baseline_refresh"])
        return result

    def reference_capacity_available():
        if phases == 0 or not reports:
            return True
        from .history_reference_successor import capacity_exhausted
        from .rolling_reference_span import effective_source_totals

        return not capacity_exhausted(effective_source_totals(job, reports[-1]["execution_totals"]))

    def result_fields():
        closed_dates = {
            frame["event"]["id"].rsplit(":", 1)[-1]
            for frame in job.events()[0][round_start:]
            if frame["event"]["type"] == "date_closed"
        }
        return {
            "daily_HTTP": http, "daily_publication_phases": len(invocation_reports),
            "daily_publication_phases_total": sum(
                frame["event"]["type"] == "reference_refresh" for frame in job.events()[0]
            ),
            "daily_reference_epoch_phases": phases,
            "daily_baseline_reports": invocation_reports,
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
        # A stored-only chunk made no source attempt in this invocation. Its
        # prior terminal source status belongs to the preserved old receipts;
        # new source attempts above still stop on their own actual outcome.
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
    if (not getattr(args, "defer_reference_build_to_bulk", False)
            and not raw_deferred and phases < phase_limit and reference_capacity_available()):
        # Raw chunks deliberately leave assembly for a fresh bounded phase after
        # every selected canonical curve/index has its ACK and exact readback.
        # Even an unknown pending count from a capacity checkpoint cannot consume
        # this reserved first finishing slot.
        publish_phase([])
        while phases < phase_limit:
            reference = reports[-1]
            progress = (reference.get("periods_completed") or reference.get("products_written")
                        or reference.get("discovery", {}).get("changed"))
            if (
                reference.get("cohort_complete")
                or reference.get("status") not in {"bounded_pending", "held"}
                or reference.get("cohort_closed_pending", reference.get("closed_pending")) == 0
                or reference.get("cohort_held_periods", reference.get("held_periods"))
                or reference.get("cohort_blocked", reference.get("blocked"))
                or not progress
                or not reference_capacity_available()
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
    if publication_owner is None and source_stop is None and not raw_deferred and references_complete:
        job.append({"type": "daily_round_finished", "round": active_round["round"]})
    return {
        **result,
        "daily_status": "source_stopped" if source_stop is not None else
        "raw_pending" if raw_deferred else
        "bounded_complete" if references_complete else "baselines_pending",
        **result_fields(),
    }


def publish_source_free(job, args):
    selected = _reference_batch_selection(args)
    if selected == 2:
        return _publish_source_free(job, args)
    with ExitStack() as publication_owner:
        return _publish_source_free(job, args, reference_max_periods=selected,
                                    publication_owner=publication_owner)


def _publish_source_free(job, args, *, reference_max_periods=2, publication_owner=None):
    """Finish only saved positive work from one physically closed source stage."""
    from .history_publish import publish, source_free_binding

    if not args.confirm_r2_write:
        raise Stop("explicit_storage_phase_required")
    admission = args.source_proof, args.source_proof_sha
    proof, active = source_free_binding(job, args.plan_sha, *admission)
    context = {"publication_get_limit": 6000, "round": active["round"],
               "independent_gets": proof["independent_gets"],
               "source_free_proof_sha256": args.source_proof_sha}
    frames = job.events()[0]
    start = next(index for index, frame in enumerate(frames)
                 if frame["event"].get("type") == "daily_round_started"
                 and frame["event"].get("round") == active["round"])
    events = [frame["event"] for frame in frames[start + 1:]]
    reports = [event["report"] for event in events if event["type"] == "reference_refresh"]
    phases = len(reports)

    publication_root = None

    def phase(raw):
        nonlocal phases, publication_root
        kwargs = {}
        if publication_owner is not None:
            from .history_publish import publication_store

            if publication_root is None:
                publication_root = publication_owner.enter_context(publication_store())
            kwargs["store"] = publication_root.new_stage()
        result = publish(
            job, args.plan_sha, confirm=True, target_dates=active["dates"] if raw else [],
            finalize_dates=active["finalize_dates"], execution_context=context,
            defer_references=raw, source_free_proof=admission,
            reference_max_periods=reference_max_periods, **kwargs,
        )
        phases += 1
        reports.append(result["baseline_refresh"])

    def ready_count():
        return sum(row["status"] == "ready" and row["date"] in active["dates"]
                   for row in job.snapshot()["rows"])

    while ready_count() and phases < 14:
        before = ready_count()
        phase(True)
        if ready_count() >= before:
            break  # A known capacity checkpoint is not a retry instruction.
    if not ready_count() and phases < 14:
        # Reuse a completed admitted phase on an explicit restart. A raw phase
        # deliberately defers references and cannot supply that completion.
        already_complete = bool(reports) and not reports[-1].get("reference_work_deferred") and (
            reports[-1].get("cohort_complete") is True
        )
        if not already_complete:
            phase(False)
            while phases < 14:
                reference = reports[-1]
                if (reference.get("cohort_complete")
                        or reference.get("status") not in {"bounded_pending", "held"}
                        or reference.get("cohort_closed_pending", reference.get("closed_pending")) == 0
                        or reference.get("cohort_held_periods", reference.get("held_periods"))
                        or reference.get("cohort_blocked", reference.get("blocked"))
                        or not (reference.get("periods_completed") or reference.get("products_written"))):
                    break
                phase(False)
    status = job.status()
    reference = status["baseline_refresh"]
    return {
        **status, "daily_status": "source_stopped", "daily_round": active["round"],
        "source_free_round": active["round"], "source_free_source_native": 2,
        "source_free_status": "raw_pending" if ready_count() else
        "bounded_complete" if reference.get("cohort_complete") else "baselines_pending",
        "daily_publication_phases": phases, "daily_baseline_reports": reports,
        "daily_HTTP": sum(event["physicalHTTP"] for event in events if event["type"] == "date_closed"),
        "daily_source_stop": next((event["status"] for event in reversed(events)
                                   if event["type"] == "end" and event["status"] != "published"), None),
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
            run.add_argument("--defer-reference-build-to-bulk", action="store_true",
                             help="Leave reference assembly pending for the explicit sealed bulk lifecycle.")
            run.add_argument("--reference-max-periods", type=int, choices=range(1, 9), default=2,
                             help="Explicit local row ceiling; original phase budgets still apply.")
            run.add_argument("--resume", action="store_true")
            run.add_argument("--max-wait-seconds", type=int, default=3600,
                             help="Wait only for validated rolling-source expiry; 0..21600 seconds.")
    publish = commands.add_parser(
        "publish", help="Separate verified R2-only phase; zero Garmin calls."
    )
    publish.add_argument("--plan-sha", required=True)
    publish.add_argument("--confirm-r2-write", action="store_true")
    references = commands.add_parser(
        "rolling-plan", help="Explicit bounded stored-only reference plan; zero provider calls or R2 writes.",
    )
    references.add_argument("--plan-sha", required=True)
    references.add_argument("--confirm-r2-read", action="store_true")
    references.add_argument("--endpoint", action="append", required=True)
    references.add_argument("--today", required=True)
    references.add_argument("--timezone", required=True)
    references.add_argument("--window-days", action="append", type=int, choices=[7, 28, 30, 90, 365],
                            help="Explicit sorted rolling lengths; omitted keeps legacy28/30/90.")
    refresh = commands.add_parser(
        "rolling-refresh", help="One plan-bound reference-only publication phase; zero Garmin calls.",
    )
    refresh.add_argument("--plan-sha", required=True)
    refresh.add_argument("--reference-plan-sha", required=True)
    refresh.add_argument("--confirm-r2-write", action="store_true")
    refresh.add_argument("--reference-max-periods", type=int, choices=range(1, 9), default=2,
                         help="Opt-in local row ceiling; original phase budgets still apply.")
    round_choice = refresh.add_mutually_exclusive_group()
    round_choice.add_argument("--reference-round-sha")
    round_choice.add_argument("--admit-reference-round", action="store_true")
    for action in ("register", "status", "next", "refresh", "ack", "admit-epoch"):
        span_command = commands.add_parser("rolling-span-" + action,
                                          help="Explicit finite stored-only reference span.")
        span_command.add_argument("--plan-sha", required=True)
        if action == "register":
            span_command.add_argument("--manifest", type=Path, required=True)
            span_command.add_argument("--manifest-sha", required=True)
            span_command.add_argument("--ordinary-admission", type=Path)
            span_command.add_argument("--ordinary-admission-sha")
        else:
            span_command.add_argument("--span-sha", required=True)
        if action == "next":
            span_command.add_argument("--confirm-r2-read", action="store_true")
        if action in {"refresh", "admit-epoch"}:
            span_command.add_argument("--confirm-r2-write", action="store_true")
        if action in {"next", "refresh"}:
            span_command.add_argument("--reference-round-sha")
        if action in {"ack", "admit-epoch"}:
            span_command.add_argument("--proof", type=Path, required=True)
            span_command.add_argument("--proof-sha", required=True)
    bulk = commands.add_parser(
        "rolling-bulk", help="Explicit whole-scope stored-only reference workset."
    )
    bulk.add_argument("--phase", choices=["prepare", "inventory", "collect", "build", "publication", "calendar", "ack", "reconcile"],
                      required=True)
    bulk.add_argument("--plan-sha", required=True)
    bulk.add_argument("--scope", type=Path, required=True)
    bulk.add_argument("--scope-sha", required=True)
    bulk.add_argument("--workset", type=Path, required=True)
    bulk.add_argument("--resource-plan", type=Path)
    bulk.add_argument("--resource-plan-sha")
    bulk.add_argument("--completion", type=Path)
    bulk.add_argument("--completion-sha")
    bulk.add_argument("--carried", type=Path)
    bulk.add_argument("--carried-sha")
    bulk.add_argument("--proof", type=Path)
    bulk.add_argument("--proof-sha")
    bulk.add_argument("--max-items", type=int, default=400)
    bulk.add_argument("--max-targets", type=int, default=2436)
    bulk.add_argument("--max-batches", type=int, default=2436)
    source_free = commands.add_parser(
        "publish-source-free", help="Finish one proof-bound stopped daily round; zero Garmin calls."
    )
    source_free.add_argument("--plan-sha", required=True)
    source_free.add_argument("--confirm-r2-write", action="store_true")
    source_free.add_argument("--reference-max-periods", type=int, choices=range(1, 9), default=2)
    source_free.add_argument("--source-proof", type=Path, required=True)
    source_free.add_argument("--source-proof-sha", required=True)
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
            elif args.command == "publish-source-free":
                result = publish_source_free(job, args)
                print(json.dumps(result))
                if result["source_free_status"] != "bounded_complete":
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
            elif args.command.startswith("rolling-span-"):
                from . import rolling_reference_span as span

                action = args.command.removeprefix("rolling-span-")
                if action == "register":
                    proof = span._ref(args.manifest)
                    if proof["sha256"] != args.manifest_sha:
                        raise Stop("rolling_span_proof_changed")
                    admission = None
                    if (args.ordinary_admission is None) != (args.ordinary_admission_sha is None):
                        raise Stop("rolling_span_ordinary_admission_required")
                    if args.ordinary_admission is not None:
                        admission = span._ref(args.ordinary_admission)
                        if admission["sha256"] != args.ordinary_admission_sha:
                            raise Stop("rolling_span_ordinary_admission_changed")
                    result = span.register_span(job, args.plan_sha, proof,
                                                ordinary_admission_ref=admission)
                elif action == "status":
                    result = span.span_status(job, args.plan_sha, args.span_sha)
                elif action == "next":
                    if not args.confirm_r2_read:
                        raise Stop("explicit_storage_read_required")
                    result = span.next_plan(job, args.plan_sha, args.span_sha,
                                            reference_round_sha=args.reference_round_sha)
                elif action == "refresh":
                    result = span.refresh_span(job, args.plan_sha, args.span_sha,
                        confirm=args.confirm_r2_write, reference_round_sha=args.reference_round_sha)
                else:
                    proof = span._ref(args.proof)
                    if proof["sha256"] != args.proof_sha:
                        raise Stop("rolling_span_proof_changed")
                    if action == "ack":
                        result = span.acknowledge_span(job, args.plan_sha, args.span_sha, proof)
                    else:
                        result = span.admit_span_epoch(job, args.plan_sha, args.span_sha, proof,
                                                       confirm=args.confirm_r2_write)
                print(json.dumps(result))
                if result.get("status") in {"epoch_required", "blocked_unknown"}:
                    return 2
            elif args.command == "rolling-bulk":
                from .reference_bulk import run_command

                print(json.dumps(run_command(job, args)))
            elif args.command == "rolling-plan":
                from .rolling_night_references import LEGACY_WINDOWS
                from .rolling_reference_cli import create_plan

                if not args.confirm_r2_read:
                    raise Stop("explicit_storage_read_required")
                result = create_plan(job, args.plan_sha, endpoints=args.endpoint,
                                     today=args.today, timezone=args.timezone,
                                     windows=args.window_days if args.window_days is not None else LEGACY_WINDOWS)
                observation = result["plan"]
                print(json.dumps({
                    "reference_plan_sha256": result["plan_sha256"],
                    "requested_endpoints": len(observation["requested_endpoints"]),
                    "eligible_endpoints": len(observation["eligible_endpoints"]),
                    "targets": len(observation["targets"]),
                    "reads": observation["reads"],
                }))
            elif args.command == "rolling-refresh":
                from .rolling_reference_cli import admit_reference_round, refresh_plan

                reference_round = args.reference_round_sha
                if args.admit_reference_round:
                    reference_round = admit_reference_round(
                        job, args.plan_sha, args.reference_plan_sha,
                        confirm=args.confirm_r2_write,
                    )["reference_round_sha256"]
                result = refresh_plan(
                    job, args.plan_sha, args.reference_plan_sha,
                    confirm=args.confirm_r2_write, reference_round_sha=reference_round,
                    reference_max_periods=args.reference_max_periods,
                )
                print(json.dumps({
                    "status": result["status"], "reference_plan_sha256": args.reference_plan_sha,
                    "reference_round_sha256": reference_round,
                    "publication_phases": result["publication_phases"],
                    "execution_totals": result["execution_totals"],
                    "remaining": result["reports"][-1].get("rolling_plan_remaining")
                    if result["reports"] else None,
                }))
                if result["status"] not in {"complete", "no_eligible_endpoints"}:
                    return 2
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
        if args.command == "rolling-bulk":
            from .reference_bulk import failure_result

            print(json.dumps(failure_result(locals().get("job"), args, error)))
            print("Stored reference bulk persistence HOLD.", file=__import__("sys").stderr)
            return 3
        if args.command.startswith("rolling-span-"):
            print("Stored reference span persistence HOLD.", file=__import__("sys").stderr)
            return 3
        from .failure_diagnostics import log_failure

        log_failure(error, stage="history_backfill", force=True)
        print("Local history persistence HOLD.", file=__import__("sys").stderr)
        return 3
    except BaseException as error:
        if args.command == "rolling-bulk":
            from .reference_bulk import failure_result

            print(json.dumps(failure_result(locals().get("job"), args, error)))
            print("Stored reference bulk HOLD. No automatic retry.", file=__import__("sys").stderr)
            return 2
        if args.command.startswith("rolling-span-"):
            print("Stored reference span HOLD. No automatic retry.", file=__import__("sys").stderr)
            return 2
        from .failure_diagnostics import log_failure

        log_failure(error, stage="history_backfill", force=True)
        print(
            "Local history HOLD; inspect offline status. No automatic retry.",
            file=__import__("sys").stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
