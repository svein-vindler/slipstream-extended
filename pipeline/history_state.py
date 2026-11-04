"""Private local, hash-chained progress. No provider/storage imports or network."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


class Stop(BaseException):
    def __init__(self, category="failed", not_before=None):
        self.category = category
        self.not_before = not_before
        super().__init__(category)


class DurabilityError(BaseException):
    pass


def digest(body):
    return hashlib.sha256(body).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode()


def write(path, value):
    body = value if isinstance(value, bytes) else encoded(value)
    try:
        with Path(path).open("xb") as handle:
            if handle.write(body) != len(body):
                raise OSError("short write")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        raise DurabilityError() from None
    return digest(body)


def read(path, maximum=16 * 1024 * 1024):
    with Path(path).open("rb") as handle:
        body = handle.read(maximum + 1)
    if len(body) > maximum:
        raise Stop("local_input_bound")
    return body


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise Stop("timezone_required")
    return parsed.astimezone(timezone.utc)


class Job:
    def __init__(self, folder):
        self.folder = Path(folder).absolute()
        self.plan = json.loads(read(self.folder / "plan.json"))
        if self.plan["state_dir"] != str(self.folder) or self.plan["schema"] != 1:
            raise Stop("state_binding_changed")
        self.plan_sha = digest(read(self.folder / "plan.json"))

    @classmethod
    def create(cls, folder, start, end, streams, provider, account):
        from .sources.provider_config import validate_account

        validate_account(account)
        a, b = date.fromisoformat(start), date.fromisoformat(end)
        if a > b or (b - a).days > 36524 or provider not in {"legacy", "personal"}:
            raise Stop("invalid_plan")
        if not streams or set(streams) - {"hr", "hrv"} or len(set(streams)) != len(streams):
            raise Stop("invalid_streams")
        folder = Path(folder).absolute()
        if folder.exists():
            raise Stop("plan_already_exists")
        folder.mkdir(mode=0o700, parents=True, exist_ok=False)
        targets = [
            {"date": (b - timedelta(days=i)).isoformat(), "stream": stream}
            for i in range((b - a).days + 1)
            for stream in sorted(streams)
        ]
        write(
            folder / "plan.json",
            {
                "schema": 1,
                "state_dir": str(folder),
                "provider": provider,
                "account_sha256": digest(account.encode()),
                "targets": targets,
            },
        )
        write(folder / "events.jsonl", b"")
        return cls(folder)

    def events(self):
        data = read(self.folder / "events.jsonl")
        if data and not data.endswith(b"\n"):
            raise Stop("torn_progress_hold")
        frames, previous = [], None
        for number, line in enumerate(data.splitlines(keepends=True)):
            frame = json.loads(line)
            if (
                frame["sequence"] != number
                or frame["previous"] != previous
                or frame["plan"] != self.plan_sha
            ):
                raise Stop("progress_binding_changed")
            previous = digest(line)
            frames.append(frame)
        return frames, previous

    def append(self, event):
        frames, previous = self.events()
        body = encoded(
            {"sequence": len(frames), "previous": previous, "plan": self.plan_sha, "event": event}
        )
        try:
            with (self.folder / "events.jsonl").open("ab") as handle:
                if handle.write(body) != len(body):
                    raise OSError("short append")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError:
            raise DurabilityError() from None

    def snapshot(self):
        rows = {
            t["date"] + ":" + t["stream"]: {**t, "status": "pending"} for t in self.plan["targets"]
        }
        hold, not_before, active, last_status = None, None, None, None
        baseline_refresh = {"status": "not_run"}
        last_run_timings = None
        for f in self.events()[0]:
            e = f["event"]
            if e["type"] == "begin":
                if active is not None:
                    raise Stop("unclosed_run_hold")
                active = e["run"]
            elif e["type"] == "end":
                if active != e["run"]:
                    raise Stop("run_binding_changed")
                active = None
                last_status = e["status"]
                last_run_timings = e.get("timings")
            elif e["type"] == "target":
                key = e["date"] + ":" + e["stream"]
                if key not in rows:
                    raise Stop("target_outside_plan")
                rows[key] = {**rows[key], **e}
            elif e["type"] == "pause":
                hold, not_before = e["reason"], e["not_before"]
            elif e["type"] == "reference_refresh":
                baseline_refresh = e["report"]
            elif e["type"] == "resume":
                hold, not_before = None, None
        return {
            "rows": list(rows.values()),
            "baseline_refresh": baseline_refresh,
            "pause": hold,
            "not_before": not_before,
            "active_run": active,
            "last_run_status": last_status,
            "last_run_timings": last_run_timings,
            "locked": (self.folder / "OWNER.lock").exists(),
        }

    def admission(self, expected_plan, resume=False, now=None, *, source=True, owner=None):
        if expected_plan != self.plan_sha:
            raise Stop("exact_plan_required")
        state = self.snapshot()
        if owner is not None and (
            not state["locked"] or json.loads(read(self.folder / "OWNER.lock")) != owner
        ):
            raise Stop("owner_binding_changed")
        if (state["locked"] and owner is None) or state["active_run"] is not None:
            raise Stop("unknown_owner_hold_no_takeover")
        if source and state["pause"] is not None:
            if not resume or (
                state["not_before"]
                and (now or datetime.now(timezone.utc)) < utc(state["not_before"])
            ):
                raise Stop("persistent_pause", state["not_before"])
        return state

    def pause(self, until, reason):
        requested = utc(until)
        if reason not in {"operator_quota", "no_more_observed"}:
            raise Stop("invalid_pause_reason")
        self.admission(self.plan_sha, source=False)
        owner = {"run": uuid.uuid4().hex, "phase": "pause", "plan": self.plan_sha}
        write(self.folder / "OWNER.lock", owner)
        state = self.admission(self.plan_sha, source=False, owner=owner)
        current = state["not_before"]
        not_before = max(requested, utc(current)) if current else requested
        self.append({"type": "pause", "reason": reason, "not_before": not_before.isoformat()})
        (self.folder / "OWNER.lock").unlink()
        return self.status()

    def status(self):
        state = self.snapshot()
        counts = Counter(r["status"] for r in state.pop("rows"))
        return {
            "plan_sha256": self.plan_sha,
            "targets": len(self.plan["targets"]),
            "pending": counts.get("pending", 0),
            "counts": dict(counts),
            **state,
        }


def rolling_usage(job, ledger_paths=(), now=None):
    """Read bound old/manual journals plus this job; unknown reservations never refund."""
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise Stop("timezone_required")
    paths = [str(Path(path).absolute()) for path in ledger_paths]
    if len(paths) > 32 or len(set(paths)) != len(paths):
        raise Stop("quota_journals_bound")
    binding = job.folder / "quota-journals.json"
    if binding.exists():
        bound = json.loads(read(binding))
        if paths and paths != bound["paths"]:
            raise Stop("quota_journals_changed")
        paths = bound["paths"]
    elif paths:
        write(binding, {"schema": 1, "paths": paths})
    reservations, provenance = {}, []
    observed = {}
    for frame in job.events()[0]:
        event = frame["event"]
        if event["type"] == "quota_journal":
            observed[event["index"]] = event
    for index, path in enumerate(paths):
        data = read(path)
        prior = observed.get(index)
        if prior and (
            len(data) < prior["bytes"] or digest(data[: prior["bytes"]]) != prior["sha256"]
        ):
            raise Stop("quota_journal_changed")
        if data and not data.endswith(b"\n"):
            raise Stop("quota_journal_torn")
        previous = None
        events = []
        for number, line in enumerate(data.splitlines(keepends=True)):
            frame = json.loads(line)
            if (
                type(frame.get("sequence")) is not int
                or frame["sequence"] != number
                or frame.get("previous") != previous
                or not isinstance(frame.get("event"), dict)
            ):
                raise Stop("quota_journal_changed")
            previous = digest(line)
            events.append(frame["event"])
        provenance.append({"sha256": digest(data), "bytes": len(data)})
        _quota_events(events, reservations)
        if prior is None or len(data) != prior["bytes"]:
            job.append(
                {
                    "type": "quota_journal",
                    "index": index,
                    "bytes": len(data),
                    "sha256": digest(data),
                }
            )
    _quota_events([frame["event"] for frame in job.events()[0]], reservations)
    cutoff = instant - timedelta(hours=24)
    http = reloads = unknown = 0
    for entry in reservations.values():
        closed = entry.get("closed")
        at = utc(closed["UTC"] if closed else entry["UTC"])
        if at > instant:
            raise Stop("quota_clock_reversed")
        if closed is None or at >= cutoff:
            http += closed["physicalHTTP"] if closed else entry["HTTP_reserved"]
            reloads += closed["reload_intent"] if closed else entry.get("reload_reserved", 1)
            unknown += closed is None
    return {
        "HTTP": http,
        "reloads": reloads,
        "unknown_reservations": unknown,
        "journals": provenance,
        "window_hours": 24,
    }


def _quota_events(events, reservations):
    for event in events:
        kind = event.get("type")
        if kind not in {"date_reserved", "date_closed"}:
            continue  # Parent completion is never inferred from dated evidence.
        identifier = event.get("id")
        if not isinstance(identifier, str) or not 1 <= len(identifier) <= 200:
            raise Stop("quota_entry_unverified")
        utc(event["UTC"])
        if kind == "date_reserved":
            if (
                identifier in reservations
                or type(event.get("HTTP_reserved")) is not int
                or not 1 <= event["HTTP_reserved"] <= 8
                or type(event.get("reload_reserved", 1)) is not int
                or event.get("reload_reserved", 1) not in (0, 1)
            ):
                raise Stop("quota_entry_unverified")
            reservations[identifier] = dict(event)
        else:
            before = reservations.get(identifier)
            if (
                before is None
                or "closed" in before
                or type(event.get("physicalHTTP")) is not int
                or not 0 <= event["physicalHTTP"] <= before["HTTP_reserved"]
                or any(
                    type(event.get(field)) is not int or event[field] not in (0, 1)
                    for field in ("reload_ACK", "reload_intent")
                )
                or event["reload_ACK"] > event["reload_intent"]
                or event["reload_intent"] > before.get("reload_reserved", 1)
                or utc(event["UTC"]) < utc(before["UTC"])
            ):
                raise Stop("quota_entry_unverified")
            before["closed"] = event
