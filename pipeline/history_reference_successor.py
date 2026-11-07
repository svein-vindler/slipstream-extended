"""Finite ordinary reference successors; source selection and usage never roll over."""

import json
import re

from .history_state import Stop, digest, encoded, read, write

LIMITS = {"phases": 14, "get": 6000, "total_get": 6256,
          "independent_get": 256, "class_a": 1000, "body_bytes": 10485760}


def capacity_exhausted(totals):
    baseline = totals.get("round_baseline", {"counts": dict.fromkeys(("get", "list", "put"), 0),
                                             "body_bytes": 0})
    counts = totals.get("counts", {})
    if (any(type(counts.get(kind)) is not int or counts[kind] < 0 for kind in ("get", "list", "put"))
            or type(totals.get("phases")) is not int or not 0 <= totals["phases"] <= 14
            or type(totals.get("body_bytes")) is not int or totals["body_bytes"] < 0
            or type(totals.get("publication_get_limit")) is not int
            or not 0 <= totals["publication_get_limit"] <= 6000
            or type(totals.get("independent_gets", 0)) is not int
            or not 0 <= totals.get("independent_gets", 0) <= 256
            or any(type(baseline["counts"].get(kind)) is not int
                   or not 0 <= baseline["counts"][kind] <= counts[kind] for kind in counts)
            or type(baseline.get("body_bytes")) is not int
            or not 0 <= baseline["body_bytes"] <= totals["body_bytes"]):
        raise Stop("reference_successor_usage_unverified")
    return (totals["phases"] >= 14
            or counts["get"] - baseline["counts"]["get"] + 400 > totals["publication_get_limit"]
            or sum(counts[k] - baseline["counts"][k] for k in ("list", "put")) + 200 > 1000
            or totals["body_bytes"] - baseline["body_bytes"] + 2097152 > 10485760)


def _path(job, sha):
    if not isinstance(sha, str) or re.fullmatch(r"[a-f0-9]{64}", sha) is None:
        raise Stop("reference_successor_binding_changed")
    return job.folder / "reference-successors" / (sha + ".json")


def _eligible(job, receipt, round_id, cohort):
    reference = receipt.get("baseline_refresh", {})
    totals = reference.get("execution_totals", {})
    state = job.snapshot()
    if (receipt.get("status") != "complete" or receipt.get("plan_sha256") != job.plan_sha
            or type(receipt.get("source_calls")) is not int or receipt["source_calls"] != 0
            or totals.get("round") != round_id or reference.get("finalize_dates") != sorted(cohort)
            or reference.get("cohort_complete") is not False
            or reference.get("cohort_held_periods", reference.get("held_periods"))
            or reference.get("cohort_blocked", reference.get("blocked"))
            or any(row["status"] in {"ready", "pending"} and row["date"] in cohort
                   for row in state["rows"])):
        raise Stop("reference_successor_cohort_unverified")
    if not capacity_exhausted(totals):
        raise Stop("reference_successor_capacity_remains")
    # Another finite epoch is useful only after proven progress in its predecessor.
    # The first may follow successful canonical source delivery without references.
    previous = totals.get("reference_successor_sha256")
    if previous is not None and not any(
        event.get("type") == "reference_refresh"
        and event.get("report", {}).get("execution_totals", {}).get("reference_successor_sha256") == previous
        and (event["report"].get("periods_completed") or event["report"].get("products_written")
             or event["report"].get("discovery", {}).get("changed"))
        for event in (frame["event"] for frame in job.events()[0])
    ):
        raise Stop("reference_successor_no_progress")
    return totals


def admit(job, plan_sha, round_id, cohort):
    """Admit one known terminal cohort successor, with no R2/provider operation."""
    from .history_publish import _completed_context

    state = job.admission(plan_sha, source=False)
    if state["last_run_status"] != "published":
        raise Stop("reference_successor_terminal_required")
    target = json.loads(read(job.folder / "storage-target.json"))["target_sha256"]
    previous = state["baseline_refresh"].get("execution_totals", {}).get("reference_successor_sha256")
    context = {"round": round_id, "publication_get_limit": 6000}
    if previous is not None:
        context["reference_successor_sha256"] = previous
    tip = _completed_context(job, state, target, context, cohort)
    end = next(frame["event"] for frame in reversed(job.events()[0])
               if frame["event"].get("type") == "end")
    body = read(job.folder / "runs" / end["run"] / "RECEIPT.json")
    receipt = json.loads(body)
    totals = _eligible(job, receipt, round_id, cohort)
    proof = {"schema": 1, "purpose": "ordinary_daily_references_only",
             "plan_sha256": plan_sha, "target_sha256": target, "round": round_id,
             "cohort": sorted(cohort), "prior_receipt_sha256": digest(body),
             "prior_run": end["run"], "journal_tip": tip, "prior_totals": totals,
             "baseline": {"counts": dict(totals["counts"]), "body_bytes": totals["body_bytes"]},
             "limits": LIMITS}
    encoded_proof = encoded(proof)
    sha = digest(encoded_proof)
    path = _path(job, sha)
    path.parent.mkdir(exist_ok=True)
    if path.exists():
        if read(path, 65536) != encoded_proof:
            raise Stop("reference_successor_binding_changed")
    else:
        write(path, encoded_proof)
    return sha


def apply(job, sha, *, target_sha, context, latest, journal_tip, cohort):
    """Restore an existing epoch, or apply a qualified successor exactly once."""
    body = read(_path(job, sha), 65536)
    proof = json.loads(body)
    if (digest(body) != sha or proof.get("schema") != 1 or proof.get("limits") != LIMITS
            or proof.get("purpose") != "ordinary_daily_references_only"
            or proof.get("plan_sha256") != job.plan_sha or proof.get("target_sha256") != target_sha
            or proof.get("round") != context.get("round") or proof.get("cohort") != sorted(cohort or [])
            or not isinstance(latest, dict)):
        raise Stop("reference_successor_binding_changed")
    totals = latest.get("baseline_refresh", {}).get("execution_totals", {})
    if totals.get("reference_successor_sha256") == sha:
        if totals.get("round_baseline") != proof["baseline"]:
            raise Stop("reference_successor_replay_changed")
        return  # _completed_context already restored cumulative usage and phases.
    _eligible(job, latest, context["round"], proof["cohort"])
    if (digest(encoded(latest)) != proof["prior_receipt_sha256"] or latest.get("run") != proof["prior_run"]
            or journal_tip != proof["journal_tip"] or totals != proof["prior_totals"]
            or context.get("counts") != proof["baseline"]["counts"]
            or context.get("body_bytes") != proof["baseline"]["body_bytes"]):
        raise Stop("reference_successor_prior_checkpoint_changed")
    context["round_baseline"] = proof["baseline"]
    context["phases"] = 0
    context["publication_get_limit"] = proof["prior_totals"]["publication_get_limit"]
