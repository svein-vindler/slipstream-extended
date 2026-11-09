"""Private immutable planning receipts for the existing stored-only publisher."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import timedelta
from itertools import islice
from types import SimpleNamespace

from .history_publish import StorageGuard, _cleanup, _completed_context
from .history_state import Stop, digest, encoded, read, write
from .native_night import zone
from .night_products import calendar_day
from .night_reference_sources import rolling_source_prefixes
from .r2_store import R2ObjectSnapshot, R2Store, _opaque_revision
from .rolling_reference_plan import plan_references

MAX_PLANS = 32


def _descriptor():
    account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    if re.fullmatch(r"[a-f0-9]{32}", account) is None:
        raise Stop("storage_account")
    return SimpleNamespace(client=SimpleNamespace(meta=SimpleNamespace(
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com")),
        bucket=os.environ.get("R2_BUCKET", "slipstream-data"))


def _open_store():
    import boto3
    from botocore.config import Config

    descriptor = _descriptor()
    client = boto3.client(
        "s3", endpoint_url=descriptor.client.meta.endpoint_url,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
        config=Config(request_checksum_calculation="when_required", connect_timeout=5,
                      read_timeout=20, retries={"total_max_attempts": 1},
                      s3={"addressing_style": "path"}),
    )
    closed = False

    def close():
        nonlocal closed
        if not closed:
            closed = True
            client.close()

    return R2Store(client=client, bucket=descriptor.bucket), close


def _target(store):
    return digest((store.client.meta.endpoint_url + "\0" + store.bucket).encode())


def _base(job):
    return job.folder / "rolling-reference-plans"


def _totals(context):
    return {name: context[name] for name in (
        "counts", "body_bytes", "publication_get_limit", "independent_gets", "phases", "round_baseline",
        "reference_round_sha256", "source_watermark", "independent_lifetime_gets",
        "independent_round_used", "independent_meter_prefix",
    ) if name in context}


def _merge(context, totals):
    counts = totals.get("counts")
    if (not isinstance(counts, dict) or set(counts) != {"get", "list", "put"}
            or any(type(value) is not int or value < 0 for value in counts.values())
            or type(totals.get("body_bytes")) is not int or totals["body_bytes"] < 0
            or type(totals.get("publication_get_limit")) is not int
            or not 0 <= totals["publication_get_limit"] <= 6000
            or type(totals.get("independent_gets")) is not int
            or not 0 <= totals["independent_gets"] <= 256):
        raise Stop("rolling_plan_read_ledger_unverified")
    current = context.setdefault("counts", {"get": 0, "list": 0, "put": 0})
    for kind in current:
        current[kind] = max(current[kind], counts[kind])
    for name in ("body_bytes", "independent_gets"):
        context[name] = max(context.get(name, 0), totals.get(name, 0))
    context["publication_get_limit"] = min(context.get("publication_get_limit", 6000),
                                            totals["publication_get_limit"])
    prior = context.setdefault("round_baseline", totals.get("round_baseline", {
        "counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0}))
    older_epoch = (prior != totals.get("round_baseline", prior)
                   and all(counts[kind] <= prior["counts"][kind] for kind in counts)
                   and totals["body_bytes"] <= prior["body_bytes"])
    if prior != totals.get("round_baseline", prior) and not older_epoch:
        raise Stop("rolling_plan_read_ledger_round_changed")
    if not older_epoch:
        context["phases"] = max(context.get("phases", 0), totals.get("phases", 0))
    watermark = totals.get("source_watermark")
    if watermark is not None:
        current_watermark = context.get("source_watermark")
        if current_watermark is None or all(watermark["counts"][kind] >= current_watermark["counts"][kind]
                                            for kind in counts):
            context["source_watermark"] = watermark
    if totals.get("independent_lifetime_gets", -1) >= context.get("independent_lifetime_gets", 0):
        for name in ("independent_lifetime_gets", "independent_round_used", "independent_meter_prefix"):
            if name in totals:
                context[name] = totals[name]


def _payload(job, sha, target):
    if not isinstance(sha, str) or re.fullmatch(r"[a-f0-9]{64}", sha) is None:
        raise Stop("rolling_reference_plan_sha_required")
    folder = _base(job) / "plans" / sha
    if not (folder / "PLAN.json").exists():
        from .rolling_reference_span import plan_folder

        try:
            folder = plan_folder(job, sha, target)
        except (OSError, ValueError):
            raise Stop("rolling_reference_plan_binding_changed") from None
    try:
        body = read(folder / "PLAN.json", 2097152)
        value = json.loads(body)
    except (OSError, ValueError):
        raise Stop("rolling_reference_plan_binding_changed") from None
    if (digest(body) != sha or value.get("schema") != 1
            or value.get("kind") != "job-rolling-night-reference-plan"
            or value.get("job_plan_sha256") != job.plan_sha
            or value.get("storage_target_sha256") != target):
        raise Stop("rolling_reference_plan_binding_changed")
    return value, folder


def _prior(job, target, context):
    root = _base(job) / "plans"
    folders = list(islice(root.iterdir(), MAX_PLANS + 1)) if root.exists() else []
    if len(folders) > MAX_PLANS:
        raise Stop("rolling_reference_plan_capacity")
    for folder in sorted(folders):
        if not folder.is_dir() or re.fullmatch(r"[a-f0-9]{64}", folder.name) is None:
            raise Stop("rolling_reference_plan_local_shape")
        value, _ = _payload(job, folder.name, target)
        _merge(context, value["read_execution_totals"])
    from .rolling_reference_span import merge_prior

    merge_prior(job, target, context)
    return folders


def _context(job, plan_sha, store, execution_context=None):
    state = job.admission(plan_sha, source=False)
    target = _target(store)
    path = job.folder / "storage-target.json"
    if path.exists() and json.loads(read(path)) != {"target_sha256": target}:
        raise Stop("storage_target_changed")
    context = execution_context if execution_context is not None else {}
    context["retain_reference_ledger"] = True
    _completed_context(job, state, target, context, None)
    folders = _prior(job, target, context)
    from .rolling_reference_span import inherit_source_ledger

    inherit_source_ledger(job, target, context)
    context.setdefault("counts", {"get": 0, "list": 0, "put": 0})
    context.setdefault("body_bytes", 0)
    context.setdefault("publication_get_limit", 6000)
    context.setdefault("independent_gets", 0)
    context.setdefault("phases", 0)
    context.setdefault("round_baseline", {"counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0})
    return target, context, folders


def _cache_index(cache):
    if len(cache) > 512 or sum(len(snapshot.data) for snapshot in cache.values()) > 16777216:
        raise Stop("rolling_reference_cache_bound")
    index = []
    for key, snapshot in sorted(cache.items()):
        if (not isinstance(key, str) or re.fullmatch(
                r"health/(?:(?:sleep/v1|hrv|signals/v1)/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}\.json(?:\.gz)?"
                r"|night-context/v1/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}/\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json)", key) is None
                or not isinstance(snapshot.data, bytes) or len(snapshot.data) > 524288
                or _opaque_revision(snapshot.revision) is None
                or any(value is not None and (not isinstance(value, str) or len(value) > 128)
                       for value in (snapshot.content_type, snapshot.encoding))):
            raise Stop("rolling_reference_cache_unverified")
        index.append({"key": key, "sha256": digest(snapshot.data), "bytes": len(snapshot.data),
                      "revision": snapshot.revision, "content_type": snapshot.content_type,
                      "encoding": snapshot.encoding})
    return index


def _admit_objects(objects, index):
    paths = list(islice(objects.iterdir(), 513)) if objects.exists() else []
    if (len(paths) > 512 or any(not path.is_file()
            or re.fullmatch(r"[a-f0-9]{64}\.bin", path.name) is None for path in paths)):
        raise Stop("rolling_reference_cache_bound")
    sizes = {path.stem: path.stat().st_size for path in paths}
    for row in index:
        sizes.setdefault(row["sha256"], row["bytes"])
    if len(sizes) > 512 or sum(sizes.values()) > 16777216:
        raise Stop("rolling_reference_cache_bound")


def _load_cache(job, payload, folder):
    raw = read(folder / "CACHE.json", 262144)
    if digest(raw) != payload["cache_index_sha256"]:
        raise Stop("rolling_reference_cache_index_changed")
    index = json.loads(raw)
    if not isinstance(index, list) or len(index) > 512:
        raise Stop("rolling_reference_cache_bound")
    cache, total = {}, 0
    for row in index:
        if (not isinstance(row, dict) or re.fullmatch(r"[a-f0-9]{64}", row.get("sha256", "")) is None
                or not isinstance(row.get("key"), str) or row["key"] in cache):
            raise Stop("rolling_reference_cache_unverified")
        objects = _base(job) / "objects"
        try:
            body = read(objects / (row["sha256"] + ".bin"), 524288)
        except FileNotFoundError:
            from .rolling_reference_span import cache_evicted

            if payload.get("span") is not None and cache_evicted(job, row["sha256"]):
                continue
            raise Stop("rolling_reference_cache_bytes_changed") from None
        total += len(body)
        if digest(body) != row["sha256"] or len(body) != row["bytes"] or total > 16777216:
            raise Stop("rolling_reference_cache_bytes_changed")
        cache[row["key"]] = R2ObjectSnapshot(body, row["revision"], row["content_type"], row["encoding"])
    if _cache_index(cache) != [row for row in index if row["key"] in cache]:
        raise Stop("rolling_reference_cache_unverified")
    return cache


class _ReadOnlySources:
    """Exact selected source dates; no product, queue, inventory or write method."""

    def __init__(self, store, guard, endpoints, today):
        self.store, self.guard = store, guard
        safe = sorted(day for day in endpoints if day <= today)
        self.dates, self.signal_dates, self.prefixes = set(), set(), {}
        if safe:
            first, last = calendar_day(safe[0]) - timedelta(days=89), calendar_day(safe[-1])
            days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
            self.dates = set(days)
            self.signal_dates = {(calendar_day(day) + timedelta(days=offset)).isoformat()
                                 for day in days for offset in range(-4, 3)}
            for offset in range(0, len(days), 90):
                self.prefixes.update(rolling_source_prefixes(days[offset:offset + 90]))
        self.guard.prefixes.update(self.prefixes)

    @property
    def client(self):
        return self.store.client

    @property
    def operations(self):
        return self.store.operations

    def physical_counts(self):
        return dict(self.guard.counts)

    def get_snapshot(self, key, **kwargs):
        source = re.fullmatch(r"health/(sleep/v1|hrv|signals/v1)/(\d{4}/\d{2})/(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?", key)
        context = re.fullmatch(r"health/night-context/v1/(\d{4}/\d{2})/(\d{4}-\d{2}-\d{2})/\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json", key)
        valid = bool(source and key.rsplit("/", 1)[0] + "/" in self.prefixes
                     and source[3] in (self.signal_dates if source[1] == "signals/v1" else self.dates)
                     and source[2] == source[3][:4] + "/" + source[3][5:7]
                     or context and context[2] in self.dates
                     and context[1] == context[2][:4] + "/" + context[2][5:7])
        if not valid:
            raise Stop("rolling_plan_source_outside_selection")
        self.guard.keys.add(key)
        return self.store.get_snapshot(key, **kwargs)

    def list_revisions_bounded(self, prefix, *, max_keys):
        if prefix not in self.prefixes or max_keys != self.prefixes[prefix]:
            raise Stop("rolling_plan_prefix_outside_selection")
        return self.store.list_revisions_bounded(prefix, max_keys=max_keys)


def _validate_input(endpoints, today, timezone):
    if (not isinstance(endpoints, (list, tuple)) or not 1 <= len(endpoints) <= 7
            or any(not isinstance(day, str) for day in endpoints)):
        raise Stop("rolling_reference_endpoint_bound")
    selected = sorted(set(endpoints))
    dates = [calendar_day(day) for day in selected]
    calendar_day((dates[0] - timedelta(days=89)).isoformat())
    calendar_day(today)
    if (dates[-1] - dates[0]).days > 6 or zone(timezone) is None:
        raise Stop("rolling_reference_endpoint_bound")
    return selected


def _create_plan(job, plan_sha, *, store, endpoints, today, timezone,
                 execution_context=None, close_owned=None, span=None):
    selected = _validate_input(endpoints, today, timezone)
    target, context, prior = _context(job, plan_sha, store, execution_context)
    if span is None and len(prior) >= MAX_PLANS:
        raise Stop("rolling_reference_plan_capacity")
    cache = context.setdefault("snapshot_cache", {})
    started_totals = json.loads(json.dumps(_totals(context)))
    if span is not None:
        from .rolling_reference_span import prepare_plan

        span_folder = prepare_plan(job, span, target, selected, today, timezone, cache)
    elif prior:
        # The latest cumulative read ledger owns the newest bounded snapshot cache.
        latest = max(prior, key=lambda p: sum(_payload(job, p.name, target)[0]["read_execution_totals"]["counts"].values()))
        previous, folder = _payload(job, latest.name, target)
        cache.update(_load_cache(job, previous, folder))
    run = uuid.uuid4().hex
    owner = {"run": run, "plan": plan_sha, "phase": "references-plan"}
    evidence = _base(job) / "reads" / run
    evidence.mkdir(parents=True)
    write(job.folder / "OWNER.lock", owner)
    guard = None
    try:
        job.admission(plan_sha, source=False, owner=owner)
        journal_tip = job.events()[1]
        guard = StorageGuard(store, set(), evidence, context)
        observed = plan_references(_ReadOnlySources(store, guard, selected, today), selected,
                                   today=today, timezone=timezone, snapshot_cache=cache)
        if guard.failed:
            raise Stop("rolling_plan_storage_outcome_unknown")
        guard.close()
        guard = None
        if close_owned is not None:
            close_owned()
        if job.events()[1] != journal_tip:
            raise Stop("rolling_plan_journal_changed")
        index = _cache_index(cache)
        index_bytes = encoded(index)
        objects = _base(job) / "objects"
        if span is not None:
            from .rolling_reference_span import prune_cache

            prune_cache(job, span, index)
        _admit_objects(objects, index)
        objects.mkdir(parents=True, exist_ok=True)
        for row in index:
            path = objects / (row["sha256"] + ".bin")
            if path.exists():
                if digest(read(path, 524288)) != row["sha256"]:
                    raise Stop("rolling_reference_cache_bytes_changed")
            else:
                write(path, cache[row["key"]].data)
        value = {"schema": 1, "kind": "job-rolling-night-reference-plan",
                 "job_plan_sha256": plan_sha, "storage_target_sha256": target,
                 "journal_tip": journal_tip, "today": today, "timezone": timezone,
                 "endpoints": selected, "observation": observed,
                 "read_execution_totals": _totals(context), "cache_index_sha256": digest(index_bytes)}
        if span is not None:
            value["span"] = span
        body = encoded(value)
        sha = digest(body)
        folder = span_folder if span is not None else _base(job) / "plans" / sha
        if (folder / "PLAN.json").exists():
            if read(folder / "PLAN.json", 2097152) != body or read(folder / "CACHE.json", 262144) != index_bytes:
                raise Stop("rolling_reference_plan_binding_changed")
        else:
            folder.mkdir(parents=True, exist_ok=True)
            write(folder / "CACHE.json", index_bytes)
            write(folder / "PLAN.json", body)
        if span is not None:
            from .rolling_reference_span import record_plan

            write(folder / "READ-RECEIPT.json", {
                "schema": 1, "kind": "rolling-reference-span-read-receipt", "status": "complete",
                "job_plan_sha256": plan_sha, "storage_target_sha256": target,
                "reference_plan_sha256": sha, "span": span, "source_calls": 0,
                "journal_tip": journal_tip, "plan": {"path": str((folder / "PLAN.json").resolve()),
                    "bytes": len(body), "sha256": sha},
                "counts": {kind: context["counts"][kind] - started_totals["counts"][kind]
                           for kind in context["counts"]},
                "body_bytes": context["body_bytes"] - started_totals["body_bytes"],
                "execution_totals": _totals(context), "guard_closed": True,
                "owned_sdk_closed": close_owned is not None,
            })
            record_plan(job, span, sha, value, folder)
        if json.loads(read(job.folder / "OWNER.lock")) != owner:
            raise Stop("owner_binding_changed")
        (job.folder / "OWNER.lock").unlink()
        return {"plan_sha256": sha, "path": str(folder / "PLAN.json"), "plan": observed}
    finally:
        if guard is not None:
            guard.close()


def create_plan(job, plan_sha, *, store=None, endpoints, today, timezone, execution_context=None,
                span=None):
    """Persist one stored-only plan and charged reads; no Job journal mutation."""
    _validate_input(endpoints, today, timezone)
    job.admission(plan_sha, source=False)
    if store is not None:
        return _create_plan(job, plan_sha, store=store, endpoints=endpoints, today=today,
                            timezone=timezone, execution_context=execution_context, span=span)
    owned, close = _open_store()
    primary = False
    try:
        return _create_plan(job, plan_sha, store=owned, endpoints=endpoints, today=today,
                            timezone=timezone, execution_context=execution_context, close_owned=close,
                            span=span)
    except BaseException:
        primary = True
        raise
    finally:
        _cleanup(close, primary=primary)


def load_plan(job, plan_sha, reference_plan_sha, *, store, reference_round_sha=None):
    """Validate private artifact, target and snapshots before publisher admission."""
    target, context, _ = _context(job, plan_sha, store, {
        "rolling_plan": {"reference_plan_sha256": reference_plan_sha},
        **({"reference_round_sha256": reference_round_sha} if reference_round_sha is not None else {})})
    payload, folder = _payload(job, reference_plan_sha, target)
    cache = _load_cache(job, payload, folder)
    _merge(context, payload["read_execution_totals"])
    plan = payload["observation"]
    binding = {"reference_plan_sha256": reference_plan_sha, "job_plan_sha256": plan_sha,
               "storage_target_sha256": target, "today": payload["today"],
               "timezone": payload["timezone"], "endpoints": plan["eligible_endpoints"],
               "verified_sleep_sources": plan["verified_sleep_sources"]}
    return binding, cache, context, plan


def _terminal_receipt(job):
    frames, tip = job.events()
    if not frames or frames[-1]["event"].get("type") != "end" or frames[-1]["event"].get("status") != "published":
        raise Stop("reference_round_requires_terminal_publication")
    run = frames[-1]["event"].get("run")
    if not isinstance(run, str) or re.fullmatch(r"[a-f0-9]{32}", run) is None:
        raise Stop("reference_round_prior_run_unverified")
    body = read(job.folder / "runs" / run / "RECEIPT.json", 2097152)
    return json.loads(body), digest(body), tip


def _round_totals(totals):
    scratch = {}
    _merge(scratch, totals)
    baseline = totals.get("round_baseline")
    if (type(totals.get("phases")) is not int or not 0 <= totals["phases"] <= 14
            or not isinstance(baseline, dict) or set(baseline) != {"counts", "body_bytes"}
            or not isinstance(baseline.get("counts"), dict)
            or set(baseline["counts"]) != {"get", "list", "put"}
            or any(type(value) is not int or not 0 <= value <= totals["counts"][kind]
                   for kind, value in baseline["counts"].items())
            or type(baseline.get("body_bytes")) is not int
            or not 0 <= baseline["body_bytes"] <= totals["body_bytes"]):
        raise Stop("reference_round_prior_usage_unverified")


def _round_path(job, plan_sha, round_sha):
    if (re.fullmatch(r"[a-f0-9]{64}", round_sha or "") is None
            or re.fullmatch(r"[a-f0-9]{64}", plan_sha or "") is None):
        raise Stop("reference_round_sha_required")
    return _base(job) / "plans" / plan_sha / "rounds" / (round_sha + ".json")


def _round_capacity(totals):
    baseline = totals["round_baseline"]
    used_get = totals["counts"]["get"] - baseline["counts"]["get"]
    used_a = sum(totals["counts"][kind] - baseline["counts"][kind] for kind in ("list", "put"))
    used_bytes = totals["body_bytes"] - baseline["body_bytes"]
    return (totals["phases"] >= 14 or used_get + 400 > totals["publication_get_limit"]
            or used_a + 200 > 1000 or used_bytes + 2097152 > 10485760)


def _round_prior(receipt, plan_sha, target_sha, reference_plan_sha):
    reference = receipt.get("baseline_refresh", {})
    totals = reference.get("execution_totals", {})
    _round_totals(totals)
    previous_plan = totals.get("rolling_plan_sha256")
    complete = reference.get("rolling_plan_complete") is True
    permitted_plan = (previous_plan is None or previous_plan == reference_plan_sha and not complete
                      or previous_plan not in {None, reference_plan_sha} and complete)
    if (receipt.get("status") != "complete" or receipt.get("plan_sha256") != plan_sha
            or receipt.get("target_sha256") != target_sha
            or type(receipt.get("source_calls")) is not int or receipt["source_calls"] != 0
            or not permitted_plan or reference.get("held_periods") or reference.get("blocked")
            or reference.get("cohort_held_periods") or reference.get("cohort_blocked")):
        raise Stop("reference_round_prior_publication_unverified")
    if not _round_capacity(totals):
        raise Stop("reference_round_existing_capacity_remains")


def admit_reference_round(job, plan_sha, reference_plan_sha, *, store=None, confirm=False):
    """Explicit local admission of another finite reference-only budget epoch.

    Cumulative counts remain unchanged. No rollover occurs inside refresh_plan.
    A new epoch is available only after known pending work exhausts admission.
    """
    if not confirm:
        raise Stop("explicit_storage_phase_required")
    descriptor = store if store is not None else _descriptor()
    binding, _cache, context, _plan = load_plan(job, plan_sha, reference_plan_sha, store=descriptor)
    receipt, receipt_sha, tip = _terminal_receipt(job)
    reference = receipt.get("baseline_refresh", {})
    totals = reference.get("execution_totals", {})
    _round_prior(receipt, plan_sha, binding["storage_target_sha256"], reference_plan_sha)
    proof = {"schema": 1, "purpose": "rolling_references_only",
             "job_plan_sha256": plan_sha, "reference_plan_sha256": reference_plan_sha,
             "storage_target_sha256": binding["storage_target_sha256"],
             "prior_run": receipt["run"], "prior_receipt_sha256": receipt_sha,
             "prior_journal_tip": tip, "prior_totals": totals,
             "previous_reference_round_sha256": totals.get("reference_round_sha256"),
             "new_round_baseline": {"counts": dict(context["counts"]), "body_bytes": context["body_bytes"]},
             "limits": {"phases": 14, "publication_get": 6000, "total_get": 6256,
                        "class_a": 1000, "body_bytes": 10485760, "independent_get": 256}}
    body = encoded(proof)
    sha = digest(body)
    path = _round_path(job, reference_plan_sha, sha)
    path.parent.mkdir(exist_ok=True)
    if path.exists():
        if read(path, 65536) != body:
            raise Stop("reference_round_binding_changed")
    else:
        write(path, body)
    return {"reference_round_sha256": sha, "path": str(path)}


def apply_reference_round(job, plan_sha, reference_plan_sha, reference_round_sha, *,
                           target_sha, execution_context, latest_receipt, journal_tip):
    """Publisher hook: apply a fresh explicit epoch once, or restore its usage."""
    from .rolling_reference_span import apply_epoch, epoch_path

    if epoch_path(job, reference_round_sha).is_file():
        return apply_epoch(job, reference_round_sha, target_sha=target_sha,
                           execution_context=execution_context, latest_receipt=latest_receipt,
                           journal_tip=journal_tip)
    path = _round_path(job, reference_plan_sha, reference_round_sha)
    try:
        body = read(path, 65536)
        proof = json.loads(body)
    except (OSError, ValueError):
        raise Stop("reference_round_binding_changed") from None
    if (digest(body) != reference_round_sha or proof.get("schema") != 1
            or proof.get("purpose") != "rolling_references_only"
            or proof.get("job_plan_sha256") != plan_sha or plan_sha != job.plan_sha
            or proof.get("reference_plan_sha256") != reference_plan_sha
            or proof.get("storage_target_sha256") != target_sha
            or proof.get("limits") != {"phases": 14, "publication_get": 6000, "total_get": 6256,
                                      "class_a": 1000, "body_bytes": 10485760, "independent_get": 256}
            or not isinstance(latest_receipt, dict)):
        raise Stop("reference_round_binding_changed")
    prior = proof["prior_totals"]
    _round_totals(prior)
    latest = latest_receipt.get("baseline_refresh", {}).get("execution_totals", {})
    _round_totals(latest)
    baseline = proof["new_round_baseline"]
    _prior(job, target_sha, execution_context)
    if (set(baseline) != {"counts", "body_bytes"}
            or set(baseline["counts"]) != {"get", "list", "put"}
            or any(type(baseline["counts"][kind]) is not int
                   or baseline["counts"][kind] < prior["counts"][kind] for kind in prior["counts"])
            or type(baseline["body_bytes"]) is not int or baseline["body_bytes"] < prior["body_bytes"]):
        raise Stop("reference_round_baseline_unverified")
    if latest.get("reference_round_sha256") == reference_round_sha:
        if latest["round_baseline"] != baseline:
            raise Stop("reference_round_replay_changed")
        # Existing receipt restoration already carried all phases/counts. Never reset.
        execution_context["reference_round_sha256"] = reference_round_sha
        return
    _round_prior(latest_receipt, plan_sha, target_sha, reference_plan_sha)
    if (digest(encoded(latest_receipt)) != proof["prior_receipt_sha256"]
            or latest_receipt.get("run") != proof["prior_run"]
            or latest != prior or journal_tip != proof["prior_journal_tip"]
            or latest.get("reference_round_sha256") != proof["previous_reference_round_sha256"]
            or any(execution_context["counts"][kind] != baseline["counts"][kind]
                   for kind in baseline["counts"])
            or execution_context["body_bytes"] != baseline["body_bytes"]):
        raise Stop("reference_round_prior_checkpoint_changed")
    execution_context["round_baseline"] = baseline
    execution_context["phases"] = 0
    execution_context["publication_get_limit"] = 6000
    execution_context["reference_round_sha256"] = reference_round_sha


def refresh_plan(job, plan_sha, reference_plan_sha, *, store=None, confirm=False, max_phases=1,
                 reference_round_sha=None, reference_max_periods=2):
    """Delegate bounded reference-only progress to the same owner/CAS publisher."""
    if not confirm:
        raise Stop("explicit_storage_phase_required")
    if type(max_phases) is not int or max_phases != 1:
        raise Stop("storage_execution_phase_budget")
    binding, cache, context, plan = load_plan(job, plan_sha, reference_plan_sha,
                                            store=store if store is not None else _descriptor(),
                                            reference_round_sha=reference_round_sha)
    context["rolling_plan"] = binding
    context["snapshot_cache"] = cache
    reports = []
    from .history_publish import publish

    if not binding["endpoints"]:
        return {"reference_plan_sha256": reference_plan_sha, "plan": plan,
                "publication_phases": 0, "reports": [], "status": "no_eligible_endpoints",
                "execution_totals": _totals(context)}
    for _ in range(min(max_phases, 14 - context["phases"])):
        result = publish(job, plan_sha, confirm=True, store=store, target_dates=[],
                         execution_context=context, finalize_dates=None,
                         reference_max_periods=reference_max_periods)
        report = result["baseline_refresh"]
        reports.append(report)
        if (report.get("rolling_plan_complete") is True
                or report.get("held_periods") or report.get("blocked")
                or not (report.get("periods_completed") or report.get("products_written"))):
            break
    return {"reference_plan_sha256": reference_plan_sha, "plan": plan,
            "publication_phases": len(reports), "reports": reports,
            "status": "complete" if reports and reports[-1].get("rolling_plan_complete") is True
            else "bounded_pending", "execution_totals": _totals(context)}
