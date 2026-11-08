"""Finite stored-only span checkpoints; publication stays in the existing engine."""
from __future__ import annotations

import json
import os
import re
from datetime import timedelta
from pathlib import Path

from .history_state import Job, Stop, digest, encoded, read, write
from .native_night import zone
from .night_products import calendar_day

MAX_CHUNKS = 128
MAX_SPANS = 8
_LIVE_USAGE = object()


def source_epoch_key(totals):
    return digest(encoded({field: totals.get(field) for field in
        ("round", "round_baseline", "reference_successor_sha256", "reference_round_sha256")}))


def _source_epoch(job):
    source = getattr(job, "source_job", job)
    return source_epoch_key(source.snapshot()["baseline_refresh"].get("execution_totals", {}))


def _sha(value):
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise Stop("rolling_span_sha_required")
    return value


def _root(job):
    return getattr(job, "source_job", job).folder / "rolling-reference-spans"


class StoredReferenceJob(Job):
    """One permanent publisher journal; never a source collector or source job."""
    def __init__(self, source_job, target):
        source_job.admission(source_job.plan_sha, source=False)
        self.source_job = source_job
        self.plan, self.plan_sha = source_job.plan, source_job.plan_sha
        self.folder = source_job.folder / "rolling-reference-execution"
        binding = {"schema": 1, "kind": "stored-reference-execution",
                   "source_job": str(source_job.folder), "source_plan": _ref(source_job.folder / "plan.json"),
                   "job_plan_sha256": self.plan_sha, "storage_target_sha256": target}
        if self.folder.exists():
            existing = json.loads(read(self.folder / "EXECUTION.json"))
            if {key: item for key, item in existing.items() if key != "source_epoch_key"} != binding:
                raise Stop("rolling_span_execution_changed")
        else:
            self.folder.mkdir()
            write(self.folder / "EXECUTION.json", {**binding, "source_epoch_key": _source_epoch(source_job)})
            write(self.folder / "events.jsonl", b"")
        target_path = self.folder / "storage-target.json"
        if target_path.exists():
            if json.loads(read(target_path)) != {"target_sha256": target}:
                raise Stop("storage_target_changed")
        else:
            write(target_path, {"target_sha256": target})

    def snapshot(self):
        active, last, reference, timings = None, None, {"status": "not_run"}, None
        for frame in self.events()[0]:
            event = frame["event"]
            if event["type"] == "begin" and active is None:
                active = event["run"]
            elif event["type"] == "end" and active == event["run"]:
                active, last, timings = None, event["status"], event.get("timings")
            elif event["type"] == "reference_refresh":
                reference = event["report"]
            else:
                raise Stop("rolling_span_execution_event_unverified")
        return {"rows": [], "baseline_refresh": reference, "pause": None, "not_before": None,
                "active_run": active, "last_run_status": last, "last_run_timings": timings,
                "locked": (self.folder / "OWNER.lock").exists()}

    def append(self, event):
        if event.get("type") not in {"begin", "reference_refresh", "end"}:
            raise Stop("rolling_span_source_actor_forbidden")
        if event["type"] == "begin":
            value = getattr(self, "publication_input", None)
            if not isinstance(value, dict):
                raise Stop("rolling_span_publication_input_required")
            write(self.folder / "runs" / event["run"] / "SPAN-INPUT.json", value)
        return super().append(event)

    def admission(self, expected_plan, resume=False, now=None, *, source=True, owner=None):
        if source:
            raise Stop("rolling_span_source_actor_forbidden")
        self.source_job.admission(expected_plan, source=False)
        return super().admission(expected_plan, resume, now, source=False, owner=owner)

    def status(self):
        state = self.snapshot()
        state.pop("rows")
        return {"plan_sha256": self.plan_sha, "targets": 0, "pending": 0,
                "counts": {}, "source_job": str(self.source_job.folder), **state}


def _folder(job, span_sha):
    return _root(job) / _sha(span_sha)


def _ref(path):
    body = read(path)
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def _pin(reference, *, prefix=False):
    if (not isinstance(reference, dict) or set(reference) != {"path", "bytes", "sha256"}
            or type(reference["bytes"]) is not int or not 0 < reference["bytes"] <= 16777216):
        raise Stop("rolling_span_proof_unverified")
    _sha(reference["sha256"])
    with Path(reference["path"]).open("rb") as handle:
        body = handle.read(reference["bytes"] + (not prefix))
    if len(body) != reference["bytes"] or digest(body) != reference["sha256"]:
        raise Stop("rolling_span_proof_changed")
    return body


def _json(reference):
    return json.loads(_pin(reference))


def _manifest(job, span_sha, target=None):
    path = _folder(job, span_sha) / "MANIFEST.json"
    body = read(path, 262144)
    value = json.loads(body)
    if digest(body) != span_sha or value.get("job_plan_sha256") != job.plan_sha:
        raise Stop("rolling_span_manifest_changed")
    if target is not None and value.get("storage_target_sha256") != target:
        raise Stop("storage_target_changed")
    from .rolling_span_source_proof import qualify_raw_floor

    source_job = getattr(job, "source_job", job)
    qualify_raw_floor(source_job, source_job.plan_sha, value["raw_floor_proof"], manifest=value)
    return value


def _chunks(manifest):
    upper, lower = map(calendar_day, (manifest["upper_endpoint"], manifest["lower_endpoint"]))
    days = [(upper - timedelta(days=i)).isoformat() for i in range((upper - lower).days + 1)]
    width = manifest.get("chunk_size", 7)
    return [sorted(days[offset:offset + width]) for offset in range(0, len(days), width)]


def _read_state(job, span_sha):
    path = _folder(job, span_sha) / "CHECKPOINTS.jsonl"
    if not path.exists():
        return None, None, 0
    body = read(path)
    if not body.endswith(b"\n"):
        raise Stop("rolling_span_checkpoint_torn")
    tip, state, count = None, None, 0
    for line in body.splitlines(keepends=True):
        frame = json.loads(line)
        if (frame.get("sequence") != count or frame.get("previous") != tip
                or frame.get("span_sha256") != span_sha or count >= MAX_CHUNKS * 100):
            raise Stop("rolling_span_checkpoint_changed")
        reference = frame.get("state_ref")
        if (frame.get("schema") != 1 or not isinstance(reference, dict)
                or Path(reference.get("path", "")).resolve().parent != (_folder(job, span_sha) / "checkpoints").resolve()):
            raise Stop("rolling_span_checkpoint_changed")
        state = reference
        tip, count = digest(line), count + 1
    return _json(state), tip, count


def _checkpoint(job, span_sha, state, *, expected_tip):
    _, tip, sequence = _read_state(job, span_sha)
    if tip != expected_tip:
        raise Stop("rolling_span_checkpoint_changed")
    snapshots = _folder(job, span_sha) / "checkpoints"
    snapshots.mkdir(exist_ok=True)
    state_body = encoded(state)
    state_path = snapshots / f"{sequence:05d}-{digest(state_body)}.json"
    if state_path.exists():
        raise Stop("rolling_span_checkpoint_unknown")
    write(state_path, state_body)
    body = encoded({"schema": 1, "sequence": sequence, "previous": tip,
                    "span_sha256": span_sha, "state_ref": _ref(state_path)})
    with (_folder(job, span_sha) / "CHECKPOINTS.jsonl").open("ab") as handle:
        if handle.write(body) != len(body):
            raise Stop("rolling_span_checkpoint_short_write")
        handle.flush()
        os.fsync(handle.fileno())


def register_span(job, plan_sha, manifest_ref, *, ordinary_admission_ref=None):
    """Register a finite scope once. No credentials, remote I/O or source actor."""
    job.admission(plan_sha, source=False)
    body, value = _pin(manifest_ref), _json(manifest_ref)
    fields = {"schema", "kind", "job_plan_sha256", "storage_target_sha256", "today", "timezone",
              "upper_endpoint", "lower_endpoint", "scope_status", "raw_floor_proof", "parent_span_sha256"}
    if (set(value) - {"chunk_size"} != fields or value.get("schema") != 1
            or value.get("kind") != "rolling-reference-span" or value.get("job_plan_sha256") != plan_sha
            or value.get("scope_status") not in {"provisional", "final"} or zone(value.get("timezone")) is None):
        raise Stop("rolling_span_manifest_unverified")
    if type(value.get("chunk_size", 7)) is not int or not 1 <= value.get("chunk_size", 7) <= 7:
        raise Stop("rolling_span_bound")
    _sha(value["storage_target_sha256"])
    upper, lower, today = map(calendar_day, (value["upper_endpoint"], value["lower_endpoint"], value["today"]))
    calendar_day((lower - timedelta(days=89)).isoformat())
    if lower > upper or upper > today or not 1 <= len(_chunks(value)) <= MAX_CHUNKS:
        raise Stop("rolling_span_bound")
    from .rolling_span_source_proof import qualify_ordinary_admission, qualify_raw_floor

    sha = digest(body)
    folder = _folder(job, sha)
    registered = folder.exists()
    if registered and read(folder / "MANIFEST.json") != body:
        raise Stop("rolling_span_manifest_changed")
    qualify_raw_floor(job, plan_sha, value["raw_floor_proof"], manifest=value,
                      fresh=not registered and ordinary_admission_ref is None)
    if ordinary_admission_ref is not None:
        qualify_ordinary_admission(job, plan_sha, manifest_ref, ordinary_admission_ref)
    path = job.folder / "storage-target.json"
    if path.exists() and json.loads(read(path)) != {"target_sha256": value["storage_target_sha256"]}:
        raise Stop("storage_target_changed")
    parent = value["parent_span_sha256"]
    if parent is not None:
        old = _manifest(job, parent, value["storage_target_sha256"])
        if (old["job_plan_sha256"] != plan_sha or old["timezone"] != value["timezone"]
                or upper != calendar_day(old["lower_endpoint"]) - timedelta(days=1)):
            raise Stop("rolling_span_extension_changed")
    root = _root(job)
    root.mkdir(exist_ok=True)
    existing = [p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"[a-f0-9]{64}", p.name)]
    if not registered:
        if len(existing) >= MAX_SPANS:
            raise Stop("rolling_span_manifest_bound")
        folder.mkdir()
        write(folder / "MANIFEST.json", body)
        _checkpoint(job, sha, {"chunk_index": 0, "status": "registered", "plan_sha256": None,
                              "previous_plan_sha256": None, "verified_chunks": 0,
                              "publication_cursor": 0, "verified_cursor": 0,
                              "generated_targets": [], "read_receipt_ref": None,
                              "partial_endpoints": [],
                              "gap_endpoints": [], "execution_totals": None}, expected_tip=None)
    return span_status(job, plan_sha, sha)


def span_status(job, plan_sha, span_sha):
    if plan_sha != job.plan_sha:
        raise Stop("exact_plan_required")
    manifest = _manifest(job, span_sha)
    state, tip, _ = _read_state(job, span_sha)
    chunks = _chunks(manifest)
    if not isinstance(state, dict) or not 0 <= state.get("chunk_index", -1) <= len(chunks):
        raise Stop("rolling_span_checkpoint_changed")
    return {"span_sha256": span_sha, "manifest": manifest, "chunks": len(chunks),
            "endpoints": sum(map(len, chunks)), "checkpoint_sha256": tip, **state,
            "reference_round_sha256": (state.get("execution_totals") or {}).get("reference_round_sha256"),
            "execution_namespace": str(getattr(job, "source_job", job).folder / "rolling-reference-execution"),
            "remaining_chunks": len(chunks) - state["chunk_index"],
            "next_endpoints": chunks[state["chunk_index"]] if state["chunk_index"] < len(chunks) else []}


def merge_prior(job, target, context):
    from .rolling_reference_cli import _merge

    if not isinstance(job, StoredReferenceJob):
        return
    root = _root(job)
    folders = [p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"[a-f0-9]{64}", p.name)] if root.exists() else []
    if len(folders) > MAX_SPANS:
        raise Stop("rolling_span_manifest_bound")
    for folder in folders:
        _manifest(job, folder.name, target)
        state, _, _ = _read_state(job, folder.name)
        if state["execution_totals"] is not None:
            _merge(context, state["execution_totals"])


def _verified_source_ledger(source, target):
    """Read the known Source receipts and return their actual cumulative watermark."""
    from .history_publish import _completed_context
    from .rolling_reference_cli import _prior

    state = source.admission(source.plan_sha, source=False)
    source_context = {"retain_reference_ledger": True}
    totals = state["baseline_refresh"].get("execution_totals", {})
    if totals.get("rolling_plan_sha256"):
        source_context["rolling_plan"] = {"reference_plan_sha256": totals["rolling_plan_sha256"]}
    if totals.get("continuation_sha256"):
        source_context["continuation_sha256"] = totals["continuation_sha256"]
    _completed_context(source, state, target, source_context, None)
    _prior(source, target, source_context)
    counts = source_context.get("counts", dict.fromkeys(("get", "list", "put"), 0))
    body_bytes = source_context.get("body_bytes", 0)
    source_phases = sum(frame["event"].get("type") == "end" and frame["event"].get("status") == "published"
                        for frame in source.events()[0])
    body = read(source.folder / "events.jsonl")
    watermark = {"counts": dict(counts), "body_bytes": body_bytes, "publication_phases": source_phases,
        "journal": {"path": str(source.folder / "events.jsonl"), "bytes": len(body), "sha256": digest(body)}}
    return source_context, watermark


def inherit_source_ledger(job, target, context):
    """Charge source interleaving exactly once, without changing its journal."""
    if not isinstance(job, StoredReferenceJob):
        return
    source_context, watermark = _verified_source_ledger(job.source_job, target)
    counts, body_bytes = watermark["counts"], watermark["body_bytes"]
    source_phases = watermark["publication_phases"]
    ends = [frame["event"] for frame in job.events()[0] if frame["event"].get("type") == "end"]
    if ends:
        prior = json.loads(read(job.folder / "runs" / ends[-1]["run"] / "SPAN-INPUT.json"))
        prior_watermark = prior["source_watermark"]
        current = context.get("source_watermark")
        if current is None or all(prior_watermark["counts"][kind] >= current["counts"][kind] for kind in counts):
            context["source_watermark"] = prior_watermark
    previous = context.get("source_watermark")
    if previous is None:
        # A new permanent namespace inherits the standing epoch, not a fresh pool.
        for field in ("counts", "body_bytes", "round_baseline", "phases", "publication_get_limit", "independent_gets"):
            if field in source_context:
                context[field] = source_context[field]
    else:
        for kind in counts:
            if counts[kind] < previous["counts"][kind]:
                raise Stop("rolling_span_source_ledger_regressed")
            context["counts"][kind] += counts[kind] - previous["counts"][kind]
        if body_bytes < previous["body_bytes"]:
            raise Stop("rolling_span_source_ledger_regressed")
        context["body_bytes"] += body_bytes - previous["body_bytes"]
        if source_phases < previous.get("publication_phases", source_phases):
            raise Stop("rolling_span_source_ledger_regressed")
        context["phases"] = min(14, context.get("phases", 0)
                                 + source_phases - previous.get("publication_phases", source_phases))
    context["publication_get_limit"] = min(context.get("publication_get_limit", 6000),
                                            source_context.get("publication_get_limit", 6000))
    context["source_watermark"] = watermark


def plan_folder(job, plan_sha, target):
    if not isinstance(job, StoredReferenceJob):
        raise Stop("rolling_span_execution_required")
    item = _root(job) / "plan-index" / (_sha(plan_sha) + ".json")
    value = json.loads(read(item, 65536))
    manifest = _manifest(job, value["span_sha256"], target)
    index = value["chunk_index"]
    if type(index) is not int or not 0 <= index < len(_chunks(manifest)):
        raise Stop("rolling_span_plan_changed")
    folder = _folder(job, value["span_sha256"]) / "chunks" / f"{index:04d}"
    if digest(read(folder / "PLAN.json")) != plan_sha:
        raise Stop("rolling_span_plan_changed")
    return folder


def prepare_plan(job, binding, target, endpoints, today, timezone, cache):
    from .rolling_reference_cli import _load_cache, _payload

    sha, index = binding["span_sha256"], binding["chunk_index"]
    manifest = _manifest(job, sha, target)
    if manifest["parent_span_sha256"] is not None:
        parent = span_status(job.source_job, job.plan_sha, manifest["parent_span_sha256"])
        if parent["remaining_chunks"]:
            raise Stop("rolling_span_parent_pending")
    state, tip, _ = _read_state(job, sha)
    if (set(binding) != {"span_sha256", "chunk_index", "checkpoint_sha256"}
            or tip != binding["checkpoint_sha256"] or index != state["chunk_index"]
            or state["plan_sha256"] is not None or endpoints != _chunks(manifest)[index]
            or today != manifest["today"] or timezone != manifest["timezone"]):
        raise Stop("rolling_span_plan_changed")
    previous = state.get("previous_plan_sha256")
    if previous is not None:
        payload, folder = _payload(job, previous, target)
        cache.update(_load_cache(job, payload, folder))
    for key in list(cache):
        match = re.search(r"/(\d{4}-\d{2}-\d{2})(?:[/.])", key)
        signal = key.startswith("health/signals/")
        first = calendar_day(endpoints[0]) - timedelta(days=93 if signal else 89)
        last = calendar_day(endpoints[-1]) + timedelta(days=2 if signal else 0)
        if not match or not first <= calendar_day(match[1]) <= last:
            del cache[key]
    return _folder(job, sha) / "chunks" / f"{index:04d}"


def cache_evicted(job, sha):
    from .rolling_reference_cli import _base

    path = _base(job) / "cache-evictions" / (_sha(sha) + ".json")
    if not path.exists():
        return False
    value = json.loads(read(path, 65536))
    if value.get("kind") != "bounded-span-cache-eviction" or value.get("sha256") != sha:
        raise Stop("rolling_reference_cache_bytes_changed")
    return True


def prune_cache(job, binding, index):
    """Evict only transient span cache blobs; immutable plans/proofs stay put."""
    from .rolling_reference_cli import _base

    if not isinstance(job, StoredReferenceJob):
        raise Stop("rolling_span_execution_required")
    root = _base(job)
    objects = root / "objects"
    wanted = {row["sha256"] for row in index}
    paths = list(objects.iterdir()) if objects.exists() else []
    if len(paths) > 512:
        raise Stop("rolling_reference_cache_bound")
    for path in paths:
        if not path.is_file() or re.fullmatch(r"[a-f0-9]{64}\.bin", path.name) is None:
            raise Stop("rolling_reference_cache_bound")
        if path.stem in wanted:
            continue
        if digest(read(path, 524288)) != path.stem:
            raise Stop("rolling_reference_cache_bytes_changed")
        retired = root / "cache-evictions" / (path.stem + ".json")
        retired.parent.mkdir(exist_ok=True)
        if not retired.exists():
            write(retired, {"kind": "bounded-span-cache-eviction", "sha256": path.stem,
                            "span_sha256": binding["span_sha256"], "chunk_index": binding["chunk_index"]})
        path.unlink()


def record_plan(job, binding, plan_sha, payload, folder):
    sha = binding["span_sha256"]
    state, tip, _ = _read_state(job, sha)
    if tip != binding["checkpoint_sha256"] or state["plan_sha256"] is not None:
        raise Stop("rolling_span_checkpoint_changed")
    root = _root(job) / "plan-index"
    root.mkdir(exist_ok=True)
    write(root / (plan_sha + ".json"), {"span_sha256": sha, "chunk_index": binding["chunk_index"]})
    _record_usage(job, _ref(folder / "READ-RECEIPT.json"), publication=False)
    observation = payload["observation"]
    unknown = any(item["reason"] != "sleep_not_stored" for item in observation["blocked"])
    unknown |= any(item["status"] != "ready" for item in observation["targets"])
    empty = not observation["eligible_endpoints"] and not unknown
    status = "blocked_unknown" if unknown else "verification_pending" if empty else "planned"
    _checkpoint(job, sha, {**state, "plan_sha256": plan_sha, "status": status,
                          "publication_cursor": binding["chunk_index"] + int(empty),
                          "read_receipt_ref": _ref(folder / "READ-RECEIPT.json"),
                          "producer_receipt": _ref(folder / "READ-RECEIPT.json"), "remote_ref": None,
                          "plan_complete": empty, "acknowledged_targets": {},
                          "producer_receipts": [_ref(folder / "READ-RECEIPT.json")],
                          "execution_totals": payload["read_execution_totals"]}, expected_tip=tip)


def next_plan(job, plan_sha, span_sha, *, store=None, reference_round_sha=None):
    from .rolling_reference_cli import _context, _descriptor, _payload, _round_capacity, create_plan

    current = span_status(job, plan_sha, span_sha)
    if not current["remaining_chunks"]:
        return current
    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    reference_round_sha = _current_epoch(execution, current, reference_round_sha)
    if current["plan_sha256"] is not None:
        payload, folder = _payload(execution, current["plan_sha256"], current["manifest"]["storage_target_sha256"])
        return {**current, "plan": payload["observation"], "reference_plan_ref": _ref(folder / "PLAN.json"),
                "producer_receipt": current["producer_receipt"]}
    context = {"reference_round_sha256": reference_round_sha} if reference_round_sha else {}
    _, context, _ = _context(execution, plan_sha, store if store is not None else _descriptor(), context)
    if _round_capacity(context):
        return {**current, "status": "epoch_required", "execution_totals": context_totals(context)}
    created = create_plan(execution, plan_sha, store=store, endpoints=current["next_endpoints"],
                          today=current["manifest"]["today"], timezone=current["manifest"]["timezone"],
                          execution_context=context, span={"span_sha256": span_sha,
                          "chunk_index": current["chunk_index"],
                          "checkpoint_sha256": current["checkpoint_sha256"]})
    status = span_status(job, plan_sha, span_sha)
    return {**status, "plan": created["plan"], "reference_plan_ref": _ref(Path(created["path"])),
            "producer_receipt": status["read_receipt_ref"]}


def context_totals(context):
    from .rolling_reference_cli import _totals

    return json.loads(json.dumps(_totals(context)))


def _current_epoch(execution, current, requested):
    if requested is not None:
        return requested
    if current.get("reference_round_sha256") is not None:
        return current["reference_round_sha256"]
    if execution.events()[0]:
        receipt, _, _, _ = _publication(execution)
        return receipt["baseline_refresh"]["execution_totals"].get("reference_round_sha256")
    return None


def _publication(job):
    frames, tip = job.events()
    ends = [frame["event"] for frame in frames if frame["event"].get("type") == "end"]
    if (not ends or ends[-1].get("status") != "published"
            or isinstance(job, StoredReferenceJob) and frames[-1]["event"].get("type") != "end"):
        raise Stop("rolling_span_publication_not_terminal")
    run = ends[-1]["run"]
    folder = job.folder / "runs" / run
    receipt, remote = _ref(folder / "RECEIPT.json"), _ref(folder / "REMOTE-VERIFIED.json")
    value, other = _json(receipt), _json(remote)
    if (value.get("status") != "complete" or other.get("status") != "remote_verified_cleanup_pending"
            or {key: val for key, val in value.items() if key != "status"}
            != {key: val for key, val in other.items() if key != "status"}
            or value.get("source_calls") != 0 or value.get("plan_sha256") != job.plan_sha):
        raise Stop("rolling_span_publication_unverified")
    return value, receipt, remote, tip


class _CapturedStore:
    """Capture existing guarded readbacks, without issuing additional requests."""
    def __init__(self, store, folder, targets):
        self.store, self.folder = store, folder
        self.targets = {row["storage_key"]: row for row in targets}
        self.written, self.observed, self.history = {}, {}, []
        self.queue_rows, self.completed_keys = {}, set()
        folder.mkdir(parents=True)

    def __getattr__(self, name):
        return getattr(self.store, name)

    def _written(self, name, data, receipt):
        if receipt.accepted and receipt.revision is not None:
            self.written[name] = {"revision": receipt.revision, "sha256": digest(data)}
        return receipt

    def create_with_receipt(self, name, data, *args, **kwargs):
        return self._written(name, data, self.store.create_with_receipt(name, data, *args, **kwargs))

    def put_if_revision(self, name, data, *args, **kwargs):
        return self._written(name, data, self.store.put_if_revision(name, data, *args, **kwargs))

    def get_snapshot(self, name, **kwargs):
        snapshot = self.store.get_snapshot(name, **kwargs)
        proof = {"key": name, "revision": snapshot.revision, "sha256": digest(snapshot.data)}
        from .night_baseline_refresh import QUEUE_KEY

        if name == QUEUE_KEY:
            rows = json.loads(snapshot.data)["pending"]
            for identifier in self.queue_rows.keys() - rows.keys():
                row = self.queue_rows[identifier]
                self.completed_keys.update(key for key, planned in self.targets.items()
                    if row.get("rolling_window") == planned["window_days"]
                    and row.get("end_date") == planned["endpoint"] and row.get("channel") == planned["channel"])
            self.queue_rows = rows
        if name in self.targets:
            product = json.loads(snapshot.data)
            planned = self.targets[name]
            written = self.written.get(name)
            if product.get("args") == planned["args"]:
                path = self.folder / (digest(name.encode()) + ".json")
                value = {**proof, "args": product["args"], "source_identity": product["source_identity"],
                         "endpoint": planned["endpoint"], "window_days": planned["window_days"],
                         "channel": planned["channel"], "written": bool(written)}
                # A key can be read as a preimage and again after its CAS; only
                # immutable hash-addressed observation artifacts are retained.
                artifact = self.folder / (proof["sha256"] + "-" + path.name)
                if not artifact.exists():
                    write(artifact, value)
                self.observed[name] = {**value, "producer_observation": _ref(artifact)}
        elif "/night-products/v1/history/" in name and name in self.written:
            path = self.folder / (digest(name.encode()) + ".history.json")
            if not path.exists():
                write(path, proof)
            self.history.append(_ref(path))
        return snapshot


def refresh_span(job, plan_sha, span_sha, *, store=None, confirm=False, reference_round_sha=None):
    from .rolling_reference_cli import (
        _cleanup,
        _descriptor,
        _open_store,
        _payload,
        _round_capacity,
        load_plan,
        refresh_plan,
    )

    if not confirm:
        raise Stop("explicit_storage_phase_required")
    current = span_status(job, plan_sha, span_sha)
    if current["status"] == "blocked_unknown" or current["plan_sha256"] is None:
        raise Stop("rolling_span_known_plan_required")
    if current["status"] == "verification_pending":
        return current
    if current["status"] == "metadata_required":
        return _metadata_phase(job, plan_sha, span_sha, current, store, reference_round_sha)
    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    execution.admission(plan_sha, source=False)
    reference_round_sha = _current_epoch(execution, current, reference_round_sha)
    _, _, sequence = _read_state(job, span_sha)
    if execution.events()[0]:
        _, prior_ref, _, _ = _publication(execution)
        before = _json(_ref(Path(prior_ref["path"]).parent / "SPAN-INPUT.json"))
        if before.get("checkpoint_sha256") == current["checkpoint_sha256"]:
            folder = plan_folder(execution, current["plan_sha256"], current["manifest"]["storage_target_sha256"])
            terminal_path = folder / "publications" / f"{sequence:04d}" / "TERMINAL.json"
            if not terminal_path.is_file():
                raise Stop("rolling_span_publication_checkpoint_unknown")
            terminal = _json(_ref(terminal_path))
            if terminal.get("owned_sdk_closed") is not True:
                raise Stop("rolling_span_publication_checkpoint_unknown")
            _record_terminal(job, plan_sha, span_sha, terminal)
            return span_status(job, plan_sha, span_sha)
    binding, _, context, observation = load_plan(execution, plan_sha, current["plan_sha256"],
        store=store if store is not None else _descriptor(), reference_round_sha=reference_round_sha)
    if _round_capacity(context):
        return {**current, "status": "epoch_required", "execution_totals": context_totals(context)}
    payload, plan_folder_path = _payload(execution, current["plan_sha256"], binding["storage_target_sha256"])
    capture_root = plan_folder_path / "publications" / f"{_read_state(job, span_sha)[2]:04d}"
    owned, close = (store, None) if store is not None else _open_store()
    capture = _CapturedStore(owned, capture_root, observation["targets"])
    execution.publication_input = {"kind": "rolling-span-publication-input", "span_sha256": span_sha,
        "reference_plan_sha256": current["plan_sha256"], "checkpoint_sha256": current["checkpoint_sha256"],
        "source_watermark": context["source_watermark"], "reference_round_sha256": reference_round_sha}
    primary = False
    try:
        result = refresh_plan(execution, plan_sha, current["plan_sha256"], store=capture,
                              confirm=True, reference_round_sha=reference_round_sha)
        if close is not None:
            close()
        receipt, receipt_ref, remote_ref, _ = _publication(execution)
        if receipt["baseline_refresh"].get("rolling_plan_sha256") != current["plan_sha256"]:
            raise Stop("rolling_span_publication_changed")
        targets = [{**item, "producer_receipt": receipt_ref, "history_receipts": capture.history}
                   for key, item in capture.observed.items() if key in capture.completed_keys or item["written"]]
        state, tip, _ = _read_state(job, span_sha)
        if tip != current["checkpoint_sha256"]:
            raise Stop("rolling_span_checkpoint_changed")
        complete = receipt["baseline_refresh"].get("rolling_plan_complete") is True
        terminal = {"kind": "rolling-span-known-publication-terminal", "span_sha256": span_sha,
            "checkpoint_sha256": tip, "reference_plan_sha256": current["plan_sha256"],
            "plan_complete": complete, "generated_targets": targets,
            "producer_receipt": receipt_ref, "remote_ref": remote_ref,
            "blocked_unknown": bool(receipt["baseline_refresh"].get("held_periods")
                                     or receipt["baseline_refresh"].get("blocked")),
            "execution_totals": result["execution_totals"], "owned_sdk_closed": close is not None}
        write(capture_root / "TERMINAL.json", terminal)
        _record_terminal(job, plan_sha, span_sha, terminal)
        return {**span_status(job, plan_sha, span_sha), "reference_plan_ref": _ref(plan_folder_path / "PLAN.json")}
    except BaseException:
        primary = True
        raise
    finally:
        if close is not None:
            _cleanup(close, primary=primary)


def _record_terminal(job, plan_sha, span_sha, terminal):
    current = span_status(job, plan_sha, span_sha)
    if (terminal.get("kind") != "rolling-span-known-publication-terminal"
            or terminal.get("span_sha256") != span_sha
            or terminal.get("checkpoint_sha256") != current["checkpoint_sha256"]
            or terminal.get("reference_plan_sha256") != current["plan_sha256"]):
        raise Stop("rolling_span_publication_checkpoint_unknown")
    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    _, actual, remote, _ = _publication(execution)
    if actual != terminal["producer_receipt"] or remote != terminal["remote_ref"]:
        raise Stop("rolling_span_publication_checkpoint_unknown")
    _record_usage(execution, actual, publication=True)
    for target in terminal["generated_targets"]:
        _pin(target["producer_observation"])
        for reference in target["history_receipts"]:
            _pin(reference)
    state, tip, _ = _read_state(job, span_sha)
    _checkpoint(job, span_sha, {**state, "status": "blocked_unknown" if terminal.get("blocked_unknown") else "verification_pending",
        "plan_complete": terminal["plan_complete"],
        "publication_cursor": state["chunk_index"] + int(terminal["plan_complete"]),
        "generated_targets": terminal["generated_targets"], "producer_receipt": actual, "remote_ref": remote,
        "producer_receipts": [actual], "execution_totals": terminal["execution_totals"]}, expected_tip=tip)


def _metadata_phase(job, plan_sha, span_sha, current, store, reference_round_sha):
    from .history_publish import StorageGuard
    from .rolling_reference_cli import (
        _cleanup,
        _descriptor,
        _open_store,
        _payload,
        _round_capacity,
        load_plan,
    )

    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    reference_round_sha = _current_epoch(execution, current, reference_round_sha)
    binding, _, context, observation = load_plan(execution, plan_sha, current["plan_sha256"],
        store=store if store is not None else _descriptor(), reference_round_sha=reference_round_sha)
    if _round_capacity(context):
        return {**current, "status": "epoch_required", "execution_totals": context_totals(context)}
    _, plan_path = _payload(execution, current["plan_sha256"], binding["storage_target_sha256"])
    missing = [target for target in observation["targets"]
               if target["storage_key"] not in current["acknowledged_targets"]]
    folder = plan_path / "metadata" / f"{_read_state(job, span_sha)[2]:04d}"
    folder.mkdir(parents=True)
    owner = {"run": digest(str(folder).encode())[:32], "plan": plan_sha, "phase": "references-metadata"}
    execution.admission(plan_sha, source=False)
    write(execution.folder / "OWNER.lock", owner)
    owned, close = (store, None) if store is not None else _open_store()
    guard, primary = None, False
    initial = context_totals(context)
    targets = []
    try:
        guard = StorageGuard(owned, {row["storage_key"] for row in missing}, folder, context)
        for target in missing:
            snapshot = owned.get_snapshot(target["storage_key"], max_bytes=262144)
            product = json.loads(snapshot.data)
            if (snapshot.content_type != "application/json" or snapshot.encoding is not None
                    or product.get("kind") != "night-baseline" or product.get("method_version") != "night-baseline-v1"
                    or product.get("args") != target["args"]):
                raise Stop("rolling_span_existing_product_unverified")
            artifact = folder / (digest(target["storage_key"].encode()) + ".json")
            value = {"key": target["storage_key"], "args": target["args"], "revision": snapshot.revision,
                "sha256": digest(snapshot.data), "source_identity": product["source_identity"],
                "endpoint": target["endpoint"], "window_days": target["window_days"],
                "channel": target["channel"], "written": False}
            write(artifact, value)
            targets.append({**value, "producer_observation": _ref(artifact), "history_receipts": []})
        guard.close()
        guard = None
        if close is not None:
            close()
        receipt_path = folder / "READ-RECEIPT.json"
        write(receipt_path, {"schema": 1, "kind": "rolling-reference-span-read-receipt",
            "read_kind": "verification_metadata", "status": "complete", "source_calls": 0,
            "job_plan_sha256": plan_sha, "storage_target_sha256": binding["storage_target_sha256"],
            "reference_plan_sha256": current["plan_sha256"],
            "span": {"span_sha256": span_sha, "chunk_index": current["chunk_index"]},
            "plan": _ref(plan_path / "PLAN.json"), "parent_producer_receipt": current["producer_receipt"],
            "counts": {kind: context["counts"][kind] - initial["counts"][kind] for kind in initial["counts"]},
            "body_bytes": context["body_bytes"] - initial["body_bytes"], "execution_totals": context_totals(context),
            "journal_tip": execution.events()[1], "guard_closed": True, "owned_sdk_closed": close is not None})
        reference = _ref(receipt_path)
        _record_usage(execution, reference, publication=False)
        targets = [{**target, "producer_receipt": reference} for target in targets]
        state, tip, _ = _read_state(job, span_sha)
        if tip != current["checkpoint_sha256"]:
            raise Stop("rolling_span_checkpoint_changed")
        _checkpoint(job, span_sha, {**state, "status": "verification_pending", "generated_targets": targets,
            "producer_receipt": reference, "remote_ref": None, "producer_receipts": [reference],
            "execution_totals": context_totals(context)}, expected_tip=tip)
        if json.loads(read(execution.folder / "OWNER.lock")) != owner:
            raise Stop("owner_binding_changed")
        (execution.folder / "OWNER.lock").unlink()
        return {**span_status(job, plan_sha, span_sha), "reference_plan_ref": _ref(plan_path / "PLAN.json")}
    except BaseException:
        primary = True
        raise
    finally:
        if guard is not None:
            _cleanup(guard.close, primary=primary)
        if close is not None:
            _cleanup(close, primary=primary)


def _target_summary(targets):
    return [{field: item[field] for field in ("key", "args", "revision", "sha256")}
            for item in targets]


def _qualified_postcheck(ack, targets, target_sha):
    postcheck = _json(ack["postcheck"])
    if (postcheck.get("kind") != "ACTUAL_METERED_SPAN_POSTCHECK_V1"
            or any(postcheck.get(field) != ack[field] for field in (
                "span_sha256", "chunk_index", "plan_sha256", "checkpoint_sha256", "producer_receipts"))
            or postcheck.get("generated_targets") != _target_summary(targets)
            or postcheck.get("storage_target_sha256") != target_sha
            or postcheck.get("componentCleanupConfirmed") is not True):
        raise Stop("rolling_span_postcheck_unqualified")
    qualification = _json(postcheck["qualification_ref"])
    if (qualification.get("kind") != "ACTUAL_CURRENT_PRODUCT_PORT"
            or qualification.get("outcome") != "allowed"
            or qualification.get("phase") not in {"postcheck", "reader-result"}
            or qualification.get("bindingSHA256") != postcheck.get("bindingSHA256")
            or type(qualification.get("callerPID")) is not int):
        raise Stop("rolling_span_postcheck_unqualified")
    if targets:
        readbacks = postcheck.get("readbacks")
        readers = postcheck.get("readerObservations")
        if not isinstance(readbacks, list) or not isinstance(readers, list) or not readers:
            raise Stop("rolling_span_independent_ack_required")
        for target in targets:
            candidates = [row for row in readbacks if row.get("keySHA256") == digest(target["key"].encode())]
            if len(candidates) != 1:
                raise Stop("rolling_span_independent_ack_required")
            body = _pin(candidates[0]["body"])
            metadata = digest(encoded({"revision": target["revision"], "content_type": "application/json", "encoding": None}))
            if (digest(body) != target["sha256"] or json.loads(body).get("args") != target["args"]
                    or candidates[0].get("metadataSHA256") != metadata):
                raise Stop("rolling_span_independent_ack_changed")
        covered = set()
        for reference in readers:
            reader = _json(reference)
            path = Path(reference["path"])
            if reader.get("status") != "matched" or not path.name.endswith(".observation.json"):
                raise Stop("rolling_span_reader_ack_required")
            stem = path.name.removesuffix(".observation.json")
            intent_body = read(path.with_name(stem + ".intent.json"))
            response = read(path.with_name(stem + ".response"))
            receipt = read(path.with_name(stem + ".execution-receipt.json"))
            claim = json.loads(read(path.with_name(stem + ".claim.json")))
            if (digest(intent_body) != reader.get("intent_sha256")
                    or digest(response) != reader.get("response_sha256")
                    or len(response) != reader.get("response_bytes")
                    or digest(receipt) != reader.get("receipt_sha256")
                    or claim != {key: reader[key] for key in ("intent_sha256", "receipt_ref", "receipt_sha256")}):
                raise Stop("rolling_span_reader_ack_changed")
            intent = json.loads(intent_body)
            if intent.get("binding", {}).get("target_sha256") != target_sha:
                raise Stop("rolling_span_reader_ack_changed")
            args = {key: value for key, value in intent["arguments"].items() if key not in {"period", "compare"}}
            for index, sha in enumerate(intent["sdk_product_sha256"]):
                selected_args = {**args, **intent["arguments"]["compare"]} if index else args
                covered.update(item["key"] for item in targets
                               if item["sha256"] == sha and item["args"] == selected_args)
        if covered != {item["key"] for item in targets}:
            raise Stop("rolling_span_reader_ack_required")
    elif postcheck.get("classification") not in {"known_source_gaps", "planning_only"}:
        raise Stop("rolling_span_empty_ack_unqualified")
    return postcheck


def acknowledge_span(job, plan_sha, span_sha, ack_ref):
    """Only caller-supplied independent observations can advance verification."""
    current = span_status(job, plan_sha, span_sha)
    if current.get("last_ack_ref") == ack_ref:
        _pin(ack_ref)
        return current
    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    execution.admission(plan_sha, source=False)
    ack = _json(ack_ref)
    if (ack.get("schema") != 1 or ack.get("kind") != "VERIFIED_STORED_SPAN_PHASE_V1"
            or current["status"] == "blocked_unknown" or current["plan_sha256"] is None
            or ack.get("span_sha256") != span_sha or ack.get("chunk_index") != current["chunk_index"]
            or ack.get("plan_sha256") != current["plan_sha256"]
            or ack.get("checkpoint_sha256") != current["checkpoint_sha256"]
            or ack.get("producer_receipts") != current["producer_receipts"]
            or ack.get("generated_targets") != _target_summary(current["generated_targets"])):
        raise Stop("rolling_span_ack_binding_changed")
    for reference in ack["producer_receipts"]:
        _pin(reference)
    postcheck = _qualified_postcheck(ack, current["generated_targets"], current["manifest"]["storage_target_sha256"])
    from .rolling_reference_cli import _payload

    payload, _ = _payload(execution, current["plan_sha256"], current["manifest"]["storage_target_sha256"])
    observed = payload["observation"]
    if (not current["generated_targets"] and postcheck.get("classification") == "known_source_gaps"
            and observed["eligible_endpoints"]):
        raise Stop("rolling_span_empty_ack_unqualified")
    acknowledged = dict(current["acknowledged_targets"])
    acknowledged.update({item["key"]: {"revision": item["revision"], "sha256": item["sha256"],
                                      "ack_ref": ack_ref} for item in current["generated_targets"]})
    requested = {target["storage_key"] for target in observed["targets"]}
    complete = current["plan_complete"] and requested <= acknowledged.keys()
    state, tip, _ = _read_state(job, span_sha)
    updated = {**state, "acknowledged_targets": acknowledged, "generated_targets": [],
               "last_ack_ref": ack_ref, "status": "metadata_required" if current["plan_complete"] else "planned"}
    if "meterJournal" in postcheck:
        _pin(postcheck["meterJournal"], prefix=True)
        lifetime, round_used = postcheck.get("cumulativeIndependentGETs"), postcheck.get("roundIndependentGETs")
        prior_used = (state["execution_totals"] or {}).get("independent_lifetime_gets", 0)
        if type(lifetime) is not int or lifetime < prior_used or type(round_used) is not int or not 0 <= round_used <= 256:
            raise Stop("rolling_span_independent_ledger_changed")
        updated["execution_totals"] = {**state["execution_totals"], "independent_lifetime_gets": lifetime,
            "independent_round_used": round_used, "independent_meter_prefix": postcheck["meterJournal"]}
    if not requested and postcheck.get("classification") != "known_source_gaps":
        complete = False
    if complete:
        gaps = sorted(set(state["gap_endpoints"]) | {row["endpoint"] for row in observed["blocked"]})
        partial = sorted(set(state["partial_endpoints"]) | {
            row["endpoint"] for row in observed["targets"] if row["coverage"] is None or row["coverage"] < 1})
        index = state["chunk_index"] + 1
        final = index == len(_chunks(current["manifest"]))
        updated.update(chunk_index=index, verified_cursor=index,
            publication_cursor=index, verified_chunks=state["verified_chunks"] + 1,
            previous_plan_sha256=current["plan_sha256"], plan_sha256=None, gap_endpoints=gaps,
            partial_endpoints=partial,
            status=("complete_with_gaps" if gaps or partial else "complete") if final else "registered")
    _checkpoint(job, span_sha, updated, expected_tip=tip)
    return span_status(job, plan_sha, span_sha)


def epoch_path(job, sha):
    return _root(job) / "epochs" / (_sha(sha) + ".json")


def _record_usage(job, reference, *, publication):
    path = job.folder / "SPAN-USAGE.jsonl"
    body = read(path) if path.exists() else b""
    previous, count = None, 0
    for line in body.splitlines(keepends=True):
        frame = json.loads(line)
        if frame["sequence"] != count or frame["previous"] != previous:
            raise Stop("rolling_span_usage_changed")
        if frame["receipt"] == reference:
            return
        previous, count = digest(line), count + 1
    receipt = _json(reference)
    totals = receipt["baseline_refresh"]["execution_totals"] if publication else receipt["execution_totals"]
    binding = json.loads(read(job.folder / "EXECUTION.json"))
    entry = {"schema": 1, "sequence": count, "previous": previous, "receipt": reference,
             "publication": publication, "source_epoch_key": binding["source_epoch_key"],
             "reference_round_sha256": totals.get("reference_round_sha256")}
    with path.open("ab") as handle:
        chunk = encoded(entry)
        if handle.write(chunk) != len(chunk):
            raise Stop("rolling_span_usage_short_write")
        handle.flush()
        os.fsync(handle.fileno())


def _cost():
    return {"counts": dict.fromkeys(("get", "list", "put"), 0), "body_bytes": 0, "publication_phases": 0}


def shared_span_usage(job, source_epoch_key, *, expected=None):
    """Read-only attributed costs; historical annotations validate their prefix."""
    folder = job.folder / "rolling-reference-execution"
    if not folder.exists():
        if expected is not None:
            raise Stop("rolling_span_usage_changed")
        return None
    execution_ref = _ref(folder / "EXECUTION.json")
    binding = _json(execution_ref)
    if (binding.get("source_job") != str(job.folder) or binding.get("job_plan_sha256") != job.plan_sha
            or binding.get("source_plan") != _ref(job.folder / "plan.json")):
        raise Stop("rolling_span_execution_changed")
    usage = folder / "SPAN-USAGE.jsonl"
    if expected is None:
        if (folder / "OWNER.lock").exists():
            raise Stop("unknown_owner_hold_no_takeover")
        data = read(usage) if usage.exists() else b""
        prefix = {"path": str(usage.resolve()), "bytes": len(data), "sha256": digest(data)}
    else:
        prefix = expected["usage_journal"]
        if Path(prefix["path"]).resolve() != usage.resolve():
            raise Stop("rolling_span_usage_changed")
        if prefix["bytes"] == 0:
            data = b""
            if prefix["sha256"] != digest(data):
                raise Stop("rolling_span_usage_changed")
        else:
            data = _pin(prefix, prefix=True)
    if data and not data.endswith(b"\n"):
        raise Stop("rolling_span_usage_changed")
    costs = {name: _cost() for name in ("source_pool", "separate_epochs", "lifetime")}
    previous, seen = None, set()
    for index, line in enumerate(data.splitlines(keepends=True)):
        frame = json.loads(line)
        if frame.get("schema") != 1 or frame.get("sequence") != index or frame.get("previous") != previous:
            raise Stop("rolling_span_usage_changed")
        reference = frame["receipt"]
        if reference["sha256"] in seen:
            raise Stop("rolling_span_usage_changed")
        seen.add(reference["sha256"])
        receipt = _json(reference)
        receipt_root = (folder if frame["publication"] else
                        _root(job) / _sha(receipt.get("span_sha256", receipt.get("span", {}).get("span_sha256"))))
        if (frame.get("source_epoch_key") != binding["source_epoch_key"]
                or not Path(reference["path"]).resolve().is_relative_to(receipt_root.resolve())):
            raise Stop("rolling_span_usage_changed")
        if receipt.get("source_calls") != 0:
            raise Stop("rolling_span_source_actor_forbidden")
        if frame["publication"]:
            if receipt.get("status") != "complete":
                raise Stop("rolling_span_usage_changed")
            totals = receipt["baseline_refresh"]["execution_totals"]
            remote = json.loads(read(Path(reference["path"]).parent / "REMOTE-VERIFIED.json"))
            if (remote.get("status") != "remote_verified_cleanup_pending"
                    or {key: val for key, val in remote.items() if key != "status"}
                    != {key: val for key, val in receipt.items() if key != "status"}):
                raise Stop("rolling_span_usage_changed")
        else:
            totals = receipt["execution_totals"]
            if receipt.get("status") != "complete" or receipt.get("guard_closed") is not True:
                raise Stop("rolling_span_usage_changed")
        epoch = totals.get("reference_round_sha256")
        if epoch != frame["reference_round_sha256"]:
            raise Stop("rolling_span_usage_changed")
        if epoch is not None:
            epoch_ref = _ref(epoch_path(job, epoch))
            if epoch_ref["sha256"] != epoch:
                raise Stop("rolling_span_usage_epoch_unqualified")
            proof = _json(epoch_ref)
            if proof.get("purpose") != "rolling_reference_span_only" or proof.get("job_plan_sha256") != job.plan_sha:
                raise Stop("rolling_span_usage_epoch_unqualified")
            _pin(proof["qualified_parent_ref"])
        group = "source_pool" if epoch is None and frame["source_epoch_key"] == source_epoch_key else "separate_epochs"
        counts, body_bytes = receipt["counts"], receipt["body_bytes"]
        if (set(counts) != {"get", "list", "put"} or any(type(item) is not int or item < 0 for item in counts.values())
                or type(body_bytes) is not int or body_bytes < 0):
            raise Stop("rolling_span_usage_changed")
        for name in (group, "lifetime"):
            for kind in counts:
                costs[name]["counts"][kind] += counts[kind]
            costs[name]["body_bytes"] += body_bytes
            costs[name]["publication_phases"] += int(frame["publication"])
        previous = digest(line)
    annotation = {"schema": 1, "kind": "attributed-span-account-usage", "job_plan_sha256": job.plan_sha,
                  "source_epoch_key": source_epoch_key, "execution_binding": execution_ref,
                  "usage_journal": prefix, **costs}
    if expected is not None and annotation != expected:
        raise Stop("rolling_span_usage_changed")
    return annotation


def validate_span_account_usage(job, annotation, source_epoch_key):
    observed = shared_span_usage(job, source_epoch_key, expected=annotation)
    return {name: observed[name] for name in ("source_pool", "separate_epochs", "lifetime")}


def effective_source_totals(job, totals, *, annotation=_LIVE_USAGE):
    """Budget view only; original receipt counters and baseline stay unchanged."""
    key = source_epoch_key(totals)
    if annotation is _LIVE_USAGE:
        annotation = shared_span_usage(job, key)
    elif annotation is not None:
        annotation = shared_span_usage(job, key, expected=annotation)
    result = json.loads(json.dumps(totals))
    if annotation is not None:
        source = annotation["source_pool"]
        for kind in source["counts"]:
            result["counts"][kind] += source["counts"][kind]
        result["body_bytes"] += source["body_bytes"]
        result["phases"] += source["publication_phases"]
    return result


def _meter_link(reference):
    value = _json(reference)
    if value.get("kind") != "ACTUAL_SPAN_METER_LINK_V1":
        raise Stop("rolling_span_meter_link_unqualified")
    body = _pin(value["meter_prefix"], prefix=True)
    if any(json.loads(line).get("kind") == "source_link" for line in body.splitlines()):
        from .rolling_span_meter_proof import validate_source_linked_meter

        return validate_source_linked_meter(reference)
    previous, count, baseline, initial, round_id, pending = None, None, 0, None, None, None
    for sequence, line in enumerate(body.splitlines(keepends=True)):
        frame = json.loads(line)
        if frame.get("sequence") != sequence or frame.get("previous") != previous:
            raise Stop("rolling_span_meter_link_changed")
        kind = frame.get("kind")
        if sequence == 0:
            if kind != "initial":
                raise Stop("rolling_span_meter_link_changed")
            initial = frame["count"]
        elif kind == "round":
            if pending is not None:
                raise Stop("rolling_span_meter_link_unknown")
            baseline, round_id = frame["baseline"], frame["proof"]["round_id"]
        elif kind == "attempt" and pending is None and frame["count"] == count + 1:
            pending = "sdk"
        elif kind == "known" and pending == "sdk" and frame["count"] == count:
            pending = None
        elif kind == "night_baseline_intent" and pending is None:
            if frame["count"] != count + frame["intent"]["reserved_gets"]:
                raise Stop("rolling_span_meter_link_changed")
            pending = frame["intent_sha256"]
        elif kind == "night_baseline_ack" and pending == frame["intent_sha256"] and frame["count"] == count:
            pending = None
        elif kind == "release_transition" and pending is None and frame["count"] == count:
            _pin(frame["transition_ref"])
            _pin(frame["admission_ref"])
        else:
            raise Stop("rolling_span_meter_link_unknown")
        count, previous = frame["count"], digest(line)
    if (not body.endswith(b"\n") or pending is not None or type(count) is not int
            or value.get("used") != count or value.get("round_baseline") != baseline
            or value.get("initial") != initial or value.get("current_round_id") != round_id
            or not 0 <= count - baseline <= 256):
        raise Stop("rolling_span_meter_link_changed")
    return value


def _known_anchor(source_job, current):
    reference = current.get("producer_receipt") or current.get("read_receipt_ref")
    if reference is None:
        value, reference, remote, _ = _publication(source_job)
        return value, reference, remote, True
    value = _json(reference)
    if value.get("kind") == "rolling-reference-span-read-receipt":
        if (value.get("status") != "complete" or value.get("guard_closed") is not True
                or value.get("owned_sdk_closed") is not True or value.get("source_calls") != 0):
            raise Stop("rolling_span_read_parent_unqualified")
        _pin(value["plan"])
        return value, reference, None, False
    execution = StoredReferenceJob(source_job, current["manifest"]["storage_target_sha256"])
    value, actual, remote, _ = _publication(execution)
    if actual != reference:
        raise Stop("rolling_span_epoch_parent_changed")
    return value, reference, remote, False


def admit_span_epoch(job, plan_sha, span_sha, qualified_parent_ref, *, confirm=False):
    """Explicit known-capacity admission; never a per-chunk quota reset."""
    from .rolling_reference_cli import _context, _descriptor, _round_capacity

    if not confirm:
        raise Stop("explicit_storage_phase_required")
    current = span_status(job, plan_sha, span_sha)
    if not current["remaining_chunks"] or current["status"] in {"verification_pending", "blocked_unknown"}:
        raise Stop("rolling_span_verified_parent_required")
    execution = StoredReferenceJob(job, current["manifest"]["storage_target_sha256"])
    admission_context = ({"rolling_plan": {"reference_plan_sha256": current["plan_sha256"]}}
                         if current["plan_sha256"] is not None else {})
    target, context, _ = _context(execution, plan_sha, _descriptor(), admission_context)
    parent = _json(qualified_parent_ref)
    anchor, anchor_ref, remote, initial = _known_anchor(job, current)
    if (parent.get("kind") != "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1" or parent.get("schema") != 1
            or parent.get("span_sha256") != span_sha or parent.get("chunk_index") != current["chunk_index"]
            or parent.get("checkpoint_sha256") != current["checkpoint_sha256"]
            or parent.get("terminal_receipt_ref") != anchor_ref
            or parent.get("ack_ref") != current.get("last_ack_ref")
            or initial and (current["chunk_index"] != 0 or current["plan_sha256"] is not None)):
        raise Stop("rolling_span_epoch_parent_changed")
    if parent.get("ack_ref") is not None:
        ack = _json(parent["ack_ref"])
        _pin(ack["postcheck"])
    elif not initial:
        raise Stop("rolling_span_verified_parent_required")
    native = _json(parent["native_terminal"])
    permission = _json(parent["qualification_ref"])
    if (native.get("allNativeDescendantsTerminal") is not True
            or native.get("actualNative") not in (0, 2)
            or permission.get("kind") != "ACTUAL_CURRENT_PRODUCT_PORT"
            or permission.get("phase") != "data" or permission.get("outcome") != "allowed"):
        raise Stop("rolling_span_epoch_parent_unqualified")
    meter = _meter_link(parent["source_meter_link"])
    context["independent_lifetime_gets"] = meter["used"]
    context["independent_round_used"] = meter["used"] - meter["round_baseline"]
    context["independent_meter_prefix"] = meter["meter_prefix"]
    reserve = meter.get("next_readback_reserve")
    maximum_reserve = 12 * len(current["next_endpoints"])
    if current["plan_sha256"]:
        from .rolling_reference_cli import _payload

        payload, _ = _payload(execution, current["plan_sha256"], target)
        maximum_reserve = 2 * len(payload["observation"]["targets"])
    if type(reserve) is not int or not 0 <= reserve <= maximum_reserve:
        raise Stop("rolling_span_meter_reserve_unverified")
    independent_capacity = meter["used"] - meter["round_baseline"] + reserve > 256
    if not _round_capacity(context) and not independent_capacity:
        raise Stop("reference_round_existing_capacity_remains")
    if initial:
        source_postcheck = _json(meter["source_postcheck_ref"])
        if source_postcheck.get("componentCleanupConfirmed") is not True:
            raise Stop("rolling_span_source_carry_unqualified")
    # The explicit caller proves account headroom and actual meter epochs. An
    # initial publication carry does not allocate a new independent GET pool.
    proof = {"schema": 1, "purpose": "rolling_reference_span_only", "span_sha256": span_sha,
        "chunk_index": current["chunk_index"], "checkpoint_sha256": current["checkpoint_sha256"],
        "job_plan_sha256": plan_sha, "storage_target_sha256": target,
        "qualified_parent_ref": qualified_parent_ref, "terminal_receipt_ref": anchor_ref,
        "remote_ref": remote, "source_carry": initial, "independent_pool_transition": not initial,
        "prior_totals": context_totals(context), "source_watermark": context["source_watermark"],
        "new_round_baseline": {"counts": dict(context["counts"]), "body_bytes": context["body_bytes"]},
        "prior_namespace_journal_tip": execution.events()[1],
        "limits": {"phases": 14, "get": 6000, "total_get": 6256, "independent_get": 256,
                   "class_a": 1000, "body_bytes": 10485760}}
    body = encoded(proof)
    sha = digest(body)
    path = epoch_path(job, sha)
    path.parent.mkdir(exist_ok=True)
    if path.exists():
        if read(path) != body:
            raise Stop("rolling_span_epoch_changed")
    else:
        write(path, body)
    return {"reference_round_sha256": sha, "path": str(path), "proof": proof}


def apply_epoch(job, sha, *, target_sha, execution_context, latest_receipt, journal_tip):
    from .rolling_reference_cli import _merge

    if not isinstance(job, StoredReferenceJob):
        raise Stop("rolling_span_execution_required")
    body = read(epoch_path(job, sha), 262144)
    proof = json.loads(body)
    if (digest(body) != sha or proof.get("purpose") != "rolling_reference_span_only"
            or proof.get("job_plan_sha256") != job.plan_sha
            or proof.get("storage_target_sha256") != target_sha):
        raise Stop("rolling_span_epoch_changed")
    parent = _json(proof["qualified_parent_ref"])
    _pin(proof["terminal_receipt_ref"])
    if proof["remote_ref"] is not None:
        _pin(proof["remote_ref"])
    _meter_link(parent["source_meter_link"])
    current = span_status(job.source_job, job.plan_sha, proof["span_sha256"])
    latest = current.get("execution_totals") or {}
    if latest.get("reference_round_sha256") == sha:
        baseline = proof["new_round_baseline"]
        if (latest.get("round_baseline") != baseline
                or any(latest.get("counts", {}).get(kind, -1) < baseline["counts"][kind]
                       for kind in ("get", "list", "put"))
                or latest.get("body_bytes", -1) < baseline["body_bytes"]):
            raise Stop("rolling_span_epoch_changed")
        restored = execution_context.get("round_baseline")
        if restored is not None and restored != baseline:
            prior = proof["prior_totals"]
            publication = (latest_receipt or {}).get("baseline_refresh", {}).get("execution_totals", {})
            watermark = execution_context.get("source_watermark")
            source_phase_delta = 0
            allowed_counts, allowed_body = latest["counts"], latest["body_bytes"]
            if watermark is not None and watermark != latest.get("source_watermark"):
                _, verified = _verified_source_ledger(job.source_job, target_sha)
                old = latest["source_watermark"]
                if (watermark != verified
                        or any(verified["counts"][kind] < old["counts"][kind] for kind in old["counts"])
                        or verified["body_bytes"] < old["body_bytes"]
                        or verified["publication_phases"] < old["publication_phases"]):
                    raise Stop("rolling_span_source_ledger_regressed")
                allowed_counts = {kind: latest["counts"][kind] + verified["counts"][kind] - old["counts"][kind]
                                  for kind in old["counts"]}
                allowed_body += verified["body_bytes"] - old["body_bytes"]
                source_phase_delta = verified["publication_phases"] - old["publication_phases"]
                if execution_context["counts"] != allowed_counts or execution_context["body_bytes"] != allowed_body:
                    raise Stop("rolling_span_epoch_parent_changed")
            if (restored != prior["round_baseline"]
                    or publication.get("round_baseline") != prior["round_baseline"]
                    or any(publication.get("counts", {}).get(kind, -1) < 0
                           or publication["counts"][kind] > prior["counts"][kind]
                           for kind in ("get", "list", "put"))
                    or publication.get("body_bytes", -1) < 0
                    or publication["body_bytes"] > prior["body_bytes"]
                    or any(execution_context["counts"][kind] > allowed_counts[kind]
                           for kind in ("get", "list", "put"))
                    or execution_context["body_bytes"] > allowed_body):
                raise Stop("rolling_span_epoch_parent_changed")
            # The last PUB may precede this epoch while its later READs are
            # already durable. Only that pinned prior epoch loses its phases.
            execution_context["round_baseline"] = baseline
            execution_context["phases"] = latest.get("phases", 0) + source_phase_delta
        _merge(execution_context, latest)
        execution_context["reference_round_sha256"] = sha
        return
    if (current["checkpoint_sha256"] != proof["checkpoint_sha256"]
            or journal_tip != proof["prior_namespace_journal_tip"]):
        raise Stop("rolling_span_epoch_parent_changed")
    # Restore only the checked prior lifetime usage; a new baseline is applied
    # once. Replaying a used admission restores its latest cumulative totals.
    for field, value in proof["prior_totals"].items():
        execution_context[field] = json.loads(json.dumps(value))
    execution_context["round_baseline"] = proof["new_round_baseline"]
    execution_context["phases"] = 0
    execution_context["reference_round_sha256"] = sha
