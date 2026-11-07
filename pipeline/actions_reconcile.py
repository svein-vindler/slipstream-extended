"""Bounded offline comparison of pinned schedule configuration and observations.

Configuration is expectation metadata, never evidence of an actual trigger.
All input documents are explicit, size/hash pinned and read only. This module
does not import a network client, read credentials, or collect observations.
"""

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pipeline.actions_receipt_schema import WORKFLOWS, utc_time, validate_receipt

MAX_FILE_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 8 * MAX_FILE_BYTES
MAX_MANIFEST_BYTES = 65536
LIMITS = {"workflow_configs": 16, "runs": 512, "jobs": 2048, "receipts": 1024,
          "trigger_evidence": 512, "pauses": 128}
EVENTS = {"schedule", "workflow_dispatch", "workflow_run", "unknown"}
STATUSES = {"queued", "in_progress", "completed", "waiting", "requested", "pending"}
CONCLUSIONS = {None, "success", "failure", "cancelled", "skipped", "timed_out",
               "action_required", "neutral", "stale", "startup_failure"}


def require(condition):
    if not condition:
        raise ValueError("Invalid reconciliation input")


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def identity(value):
    require(isinstance(value, dict))
    require(value.get("workflow") in WORKFLOWS)
    require(isinstance(value.get("run_id"), str) and re.fullmatch(r"[1-9][0-9]{0,19}", value["run_id"]))
    require(type(value.get("run_attempt")) is int and 1 <= value["run_attempt"] <= 1000000)
    return value["workflow"], value["run_id"], value["run_attempt"]


def no_duplicates(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value)
        value[key] = item
    return value


class Inputs:
    def __init__(self, manifest_path):
        self.directory = manifest_path.parent
        self.total = 0
        self.paths = {manifest_path.resolve()}

    def read(self, path, limit):
        require(limit <= MAX_TOTAL_BYTES - self.total)
        require(path.is_file())
        with path.open("rb") as handle:
            data = handle.read(limit + 1)
        require(len(data) <= limit)
        self.total += len(data)
        require(self.total <= MAX_TOTAL_BYTES)
        self.paths.add(path.resolve())
        return data

    def json(self, data):
        return json.loads(data, object_pairs_hook=no_duplicates)

    def pinned(self, ref):
        require(isinstance(ref, dict) and set(ref) == {"path", "bytes", "sha256"})
        require(isinstance(ref["path"], str) and 0 < len(ref["path"]) <= 4096)
        require(type(ref["bytes"]) is int and 0 < ref["bytes"] <= MAX_FILE_BYTES)
        require(isinstance(ref["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]))
        path = Path(ref["path"])
        if not path.is_absolute():
            path = self.directory / path
        data = self.read(path, ref["bytes"])
        require(len(data) == ref["bytes"] and hashlib.sha256(data).hexdigest() == ref["sha256"])
        return self.json(data)


def cron_field(text, low, high, names=None):
    require(isinstance(text, str) and 0 < len(text) <= 128)
    if names:
        for index, name in enumerate(names):
            text = re.sub(name, str(index + low), text, flags=re.IGNORECASE)
    require(re.fullmatch(r"[0-9*,/\-]+", text))
    values = set()
    for term in text.split(","):
        step_parts = term.split("/")
        require(len(step_parts) <= 2)
        step = int(step_parts[1]) if len(step_parts) == 2 else 1
        require(1 <= step <= high - low + 1)
        base = step_parts[0]
        if base == "*":
            first, last = low, high
        elif "-" in base:
            bounds = base.split("-")
            require(len(bounds) == 2)
            first, last = int(bounds[0]), int(bounds[1])
        else:
            first = int(base)
            last = high if len(step_parts) == 2 else first
        require(low <= first <= last <= high)
        values.update(range(first, last + 1, step))
    return values


def cron_schedule(schedule):
    require(isinstance(schedule, dict))
    expression, zone = schedule.get("cron_expression"), schedule.get("timezone")
    require(isinstance(expression, str) and 0 < len(expression) <= 256)
    require(isinstance(zone, str) and 0 < len(zone) <= 100)
    require(re.fullmatch(r"[A-Za-z0-9_+\-/]+", zone))
    tz = ZoneInfo(zone)
    parts = expression.split()
    require(len(parts) == 5)
    fields = [cron_field(parts[0], 0, 59), cron_field(parts[1], 0, 23),
              cron_field(parts[2], 1, 31),
              cron_field(parts[3], 1, 12, ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]),
              cron_field(parts[4], 0, 7, ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"])]
    fields[4] = {day % 7 for day in fields[4]}
    return expression, tz, parts, fields


def cron_matches(local, parts, fields):
    dom, dow = local.day in fields[2], (local.weekday() + 1) % 7 in fields[4]
    day_matches = dom and dow if "*" in (parts[2], parts[4]) else dom or dow
    return local.minute in fields[0] and local.hour in fields[1] and local.month in fields[3] and day_matches


def has_skipped_match(tz, parts, fields, start, end):
    """Flag skipped local cron times without guessing GitHub's adjusted slot.

    GitHub documents advance-to-next-valid-time behavior. V1 conservatively
    preserves that boundary as unresolved instead of asserting zero expected.
    """
    day = start.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    last = end.astimezone(tz).replace(tzinfo=None)
    while day <= last:
        for hour in fields[1]:
            for minute in fields[0]:
                local = day.replace(hour=hour, minute=minute)
                if not cron_matches(local, parts, fields):
                    continue
                instant = local.replace(tzinfo=tz).astimezone(timezone.utc)
                if start <= instant < end and instant.astimezone(tz).replace(tzinfo=None) != local:
                    return True
        day += timedelta(days=1)
    return False


def occurrences(configs, start, end):
    values, unsupported, expressions = {}, [], {}
    seen = set()
    for config in configs:
        require(isinstance(config, dict) and config.get("workflow") in WORKFLOWS)
        workflow = config["workflow"]
        require(workflow not in seen)
        seen.add(workflow)
        overlap = False
        require(isinstance(config.get("schedules"), list) and 1 <= len(config["schedules"]) <= 16)
        for schedule in config["schedules"]:
            try:
                expression, tz, parts, fields = cron_schedule(schedule)
            except (ValueError, TypeError, ZoneInfoNotFoundError):
                unsupported.append({"workflow": workflow, "reason": "unsupported_schedule"})
                continue
            if has_skipped_match(tz, parts, fields, start, end):
                unsupported.append({"workflow": workflow, "reason": "dst_skipped_hour_unresolved"})
            repeated_hour = False
            # Iterate actual UTC instants: both fall-back hours survive and nonexistent
            # local spring-forward minutes never become invented occurrences.
            when = start
            while when < end:
                local = when.astimezone(tz)
                if cron_matches(local, parts, fields):
                    if local.replace(fold=0).utcoffset() != local.replace(fold=1).utcoffset():
                        repeated_hour = True
                    key = workflow, iso(when)
                    if key in values:
                        overlap = True
                    values[key] = {"workflow": workflow, "scheduled_for": key[1]}
                    if local.replace(fold=0).utcoffset() != local.replace(fold=1).utcoffset():
                        values[key]["expectation_ambiguity"] = "dst_repeated_hour_ambiguous"
                    expressions.setdefault(key, set()).add(expression)
                    require(len(values) <= 8192)
                when += timedelta(minutes=1)
            if repeated_hour:
                unsupported.append({"workflow": workflow, "reason": "dst_repeated_hour_ambiguous"})
        if overlap:
            unsupported.append({"workflow": workflow, "reason": "overlapping_schedules_ambiguous"})
    return sorted(values.values(), key=lambda item: (item["scheduled_for"], item["workflow"])), unsupported, expressions


def data_status(run, jobs, receipts, as_of):
    key = identity(run)
    matching_jobs = [job for job in jobs if identity(job) == key]
    matching_receipts = [item for item in receipts if
                         (item["workflow"], item["run_id"], item["run_attempt"]) == key]
    if not matching_receipts:
        return "unknown", "receipt_unavailable", {}
    if any(utc_time(item["finished_at"]) > as_of for item in matching_receipts):
        return "unknown", "receipt_after_as_of", {}
    if any(item["head_sha"] != run["head_sha"] or item["event"] != run["event"]
           or item["context_status"] != "valid" for item in matching_receipts):
        return "unknown", "receipt_context_mismatch", {}
    if not matching_jobs or any(job["conclusion"] is None for job in matching_jobs):
        return "unknown", "job_evidence_unavailable", {}
    if run["status"] != "completed" or run["conclusion"] is None:
        return "unknown", "run_incomplete", {}
    if any(item != matching_receipts[0] for item in matching_receipts[1:]):
        return "unknown", "receipt_conflict", {}
    failed_job = any(job["conclusion"] != "success" for job in matching_jobs)
    command_success = all(item["command_outcome"] == "success" for item in matching_receipts)
    if (run["conclusion"] == "success" and (failed_job or not command_success)) or (
        run["conclusion"] != "success" and command_success
    ) or (
        not command_success and matching_receipts[0]["data_outcome"] == "complete"
    ):
        return "unknown", "status_conflict", {}
    return matching_receipts[0]["data_outcome"], "receipt_data_outcome", matching_receipts[0]["counters"]


def trigger_bindings(proofs, inputs, runs, expressions, as_of):
    """Pin and bind explicit source metadata; receipts alone cannot assert slots.

    Offline pinning establishes reproducibility, not provider authenticity. The
    caller must preserve the documented original metadata provenance. Standard
    workflow-run API records do not contain this source occurrence field.
    """
    bindings, conflicts = {}, set()
    keys = {identity(run): run for run in runs}
    for proof in proofs:
        key = identity(proof)
        group = key[:2]
        require(proof.get("provenance") == "github-trigger-metadata")
        require(proof.get("source_field") == "scheduled_for")
        source = inputs.pinned(proof.get("source_ref"))
        require(isinstance(source, dict) and source.get("kind") == "github-trigger-metadata")
        fields = ("workflow", "run_id", "run_attempt", "head_sha", "event", "scheduled_for",
                  "cron_expression", "provenance", "source_field")
        require(all(field in proof and source.get(field) == proof[field] for field in fields))
        when = utc_time(proof["scheduled_for"])
        slot = key[0], iso(when)
        run = keys.get(key)
        valid = (run is not None and proof["event"] == run["event"] == "schedule"
                 and proof["head_sha"] == run["head_sha"] and when <= as_of
                 and proof["cron_expression"] in expressions.get(slot, set()))
        if not valid:
            conflicts.add(group)
            continue
        if group in bindings and bindings[group] != slot:
            conflicts.add(group)
        bindings[group] = slot
    for group in list(bindings):
        origin = [run for run in runs if identity(run)[:2] == group]
        if len({(run["head_sha"], run["event"]) for run in origin}) != 1:
            conflicts.add(group)
        if group in conflicts:
            del bindings[group]
    return bindings, conflicts


def reconcile(manifest, inputs):
    fields = {"schema_version", "kind", "day_start", "day_end", "as_of", "observation_start",
              "observation_end", "delay_window_minutes", *LIMITS}
    require(isinstance(manifest, dict) and set(manifest) == fields)
    require(type(manifest["schema_version"]) is int and manifest["schema_version"] == 1)
    require(manifest["kind"] == "actions-reconcile-input")
    start_date, end_date = date.fromisoformat(manifest["day_start"]), date.fromisoformat(manifest["day_end"])
    require(0 <= (end_date - start_date).days < 31)
    start = datetime.combine(start_date, datetime.min.time(), timezone.utc)
    end = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), timezone.utc)
    as_of = utc_time(manifest["as_of"])
    observed_start, observed_end = utc_time(manifest["observation_start"]), utc_time(manifest["observation_end"])
    require(observed_start <= observed_end <= as_of)
    delay_minutes = manifest["delay_window_minutes"]
    require(type(delay_minutes) is int and 0 <= delay_minutes <= 10080)
    delay = timedelta(minutes=delay_minutes)
    documents = {}
    for name, maximum in LIMITS.items():
        refs = manifest[name]
        require(isinstance(refs, list) and len(refs) <= maximum)
        documents[name] = [inputs.pinned(ref) for ref in refs]
    require(bool(documents["workflow_configs"]))
    expected, unsupported, expressions = occurrences(documents["workflow_configs"], start, end)
    runs, seen_runs = documents["runs"], set()
    for run in runs:
        workflow, run_id, attempt = identity(run)
        require((workflow, run_id, attempt) not in seen_runs)
        seen_runs.add((workflow, run_id, attempt))
        require(isinstance(run.get("head_sha"), str) and re.fullmatch(r"[0-9a-f]{40}", run["head_sha"]))
        require(run.get("event") in EVENTS and run.get("status") in STATUSES)
        require(run.get("conclusion") in CONCLUSIONS)
        created = utc_time(run["created_at"])
        require(created <= as_of)
        if run.get("run_started_at") is not None:
            require(created <= utc_time(run["run_started_at"]) <= as_of)
    for job in documents["jobs"]:
        identity(job)
        require(job.get("conclusion") in CONCLUSIONS)
    receipts, invalid_receipts = [], 0
    for raw in documents["receipts"]:
        try:
            receipts.append(validate_receipt(raw))
        except (ValueError, TypeError):
            invalid_receipts += 1
    pauses = []
    for pause in documents["pauses"]:
        require(isinstance(pause, dict) and pause.get("workflow") in WORKFLOWS)
        require(pause.get("provenance") == "operator-record")
        paused_from, paused_until = utc_time(pause["paused_from"]), utc_time(pause["paused_until"])
        require(paused_from < paused_until and paused_until - paused_from <= timedelta(days=31))
        pauses.append((pause["workflow"], paused_from, paused_until))
    bindings, conflicts = trigger_bindings(documents["trigger_evidence"], inputs, runs, expressions, as_of)
    unassigned, observed = [], []
    slot_runs = {}
    for run in runs:
        workflow, run_id, attempt = identity(run)
        created = utc_time(run["created_at"])
        candidates = [item["scheduled_for"] for item in expected if item["workflow"] == workflow
                      and run["event"] == "schedule"
                      and utc_time(item["scheduled_for"]) <= created <= utc_time(item["scheduled_for"]) + delay]
        outcome, data_reason, counters = data_status(run, documents["jobs"], receipts, as_of)
        projection = {"workflow": workflow, "run_id": run_id, "run_attempt": attempt,
                           "head_sha": run["head_sha"], "event": run["event"], "status": run["status"],
                           "conclusion": run["conclusion"], "data_outcome": outcome}
        slot = bindings.get((workflow, run_id))
        if slot:
            projection.update(scheduled_for=slot[1], data_reason=data_reason, counters=counters)
            observed.append(projection)
            slot_runs.setdefault(slot, set()).add(run_id)
        else:
            reason = "trigger_evidence_unavailable"
            if run["event"] != "schedule":
                reason = "not_a_schedule_event"
            elif (workflow, run_id) in conflicts:
                reason = "trigger_evidence_conflict"
            elif len(candidates) > 1:
                reason = "multiple_candidate_occurrences"
            projection.update(reason=reason, candidate_scheduled_for=candidates)
            unassigned.append(projection)
    for item in expected:
        when = utc_time(item["scheduled_for"])
        covered = observed_start <= when and observed_end >= when + delay
        run_ids = sorted(slot_runs.get((item["workflow"], item["scheduled_for"]), set()))
        paused = any(workflow == item["workflow"] and first <= when < last for workflow, first, last in pauses)
        item.update(status="not_yet_due" if as_of < when + delay else "not_observed_after_window",
                    reason="delay_window_open" if as_of < when + delay else "no_occurrence_proof",
                    coverage="covered" if covered else "incomplete", run_ids=run_ids)
        if as_of >= when + delay and not covered:
            item.update(status="ambiguous", reason="observation_coverage_incomplete")
        if paused:
            item.update(status="known_paused", reason="explicit_pause_window")
        if run_ids:
            item.update(status="observed", reason="explicit_trigger_evidence")
            if paused or len(run_ids) > 1:
                item.update(status="ambiguous", reason="pause_observation_conflict" if paused else "multiple_runs_for_occurrence")
    return {"schema_version": 1, "kind": "actions-reconcile-report", "as_of": iso(as_of),
            "day_start": start_date.isoformat(), "day_end": end_date.isoformat(),
            "observation_start": iso(observed_start), "observation_end": iso(observed_end),
            "occurrences": expected, "unsupported_schedules": unsupported,
            "expected_complete": not unsupported, "unassigned_runs": unassigned,
            "observed_runs": observed, "invalid_receipt_count": invalid_receipts,
            "limitations": ["supplied_observations_only", "absence_does_not_prove_github_dropped",
                            "configured_schedule_is_not_trigger_evidence"]}


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError("Invalid reconciliation arguments")


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        parser.add_argument("--input", required=True)
        parser.add_argument("--output", required=True)
        args = parser.parse_args(argv)
        manifest_path = Path(args.input)
        inputs = Inputs(manifest_path)
        manifest = inputs.json(inputs.read(manifest_path, MAX_MANIFEST_BYTES))
        value = reconcile(manifest, inputs)
        output = Path(args.output)
        require(output.resolve() not in inputs.paths)
        data = json.dumps(value, sort_keys=True, indent=2) + "\n"
        require(len(data.encode("utf-8")) <= MAX_TOTAL_BYTES)
        with output.open("x", encoding="utf-8") as handle:
            handle.write(data)
        return 0
    except Exception:
        sys.stderr.write("actions_reconcile: invalid input or output\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
