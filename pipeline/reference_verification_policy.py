"""Explicit full-SDK verification with a finite deterministic reader canary.

This local metadata policy grants no permission and changes no allowance.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .history_state import Stop, digest, encoded, read, write
from .night_products import normalize_args, storage_key

MODE = "ALLSDK_EXACT_CONNECTED_READER_CANARY_V1"
ALL_MODE = "ALLSDK_EXACT_CONNECTED_READER_ALL_V1"
ALGORITHM = "SIX_GROUP_ENDPOINTS_MONTH_YEAR_CHANNEL_DST_V1"
MAX_CANARIES = 34
CHANNELS = ("heart_rate", "hrv")
WINDOWS = (28, 30, 90)
FIELDS = {"schema", "kind", "algorithm", "new_invocation", "scope_sha256", "job_plan_sha256",
          "storage_target_sha256", "scope_ref", "timezone", "deadline_utc", "source_seal", "build_seal",
          "prior_checkpoint", "prior_verified_count", "prior_verified_keys_sha256", "prior_independent_gets",
          "pending_target_count", "pending_targets_sha256", "selection", "selection_sha256"}


def _need(condition, category="reference_verification_policy_invalid"):
    if not condition:
        raise Stop(category)


def _ref(path):
    body = read(path)
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def _pin(reference, *, prefix=False):
    _need(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}
          and type(reference["path"]) is str and Path(reference["path"]).is_absolute()
          and type(reference["bytes"]) is int and 0 < reference["bytes"] <= 16777216
          and re.fullmatch(r"[a-f0-9]{64}", reference["sha256"] or "") is not None)
    body = read(reference["path"])
    if prefix:
        _need(len(body) >= reference["bytes"])
        body = body[:reference["bytes"]]
    _need(len(body) == reference["bytes"] and digest(body) == reference["sha256"],
          "reference_verification_policy_pin_changed")
    return body


def _identities(targets):
    _need(type(targets) is list and 0 < len(targets) <= 2436)
    rows = []
    for target in targets:
        _need(type(target) is dict and type(target.get("key")) is str and type(target.get("args")) is dict)
        args = normalize_args(period="custom", **target["args"])
        _need(args == target["args"] and storage_key(args) == target["key"])
        width = (date.fromisoformat(args["end_date"]) - date.fromisoformat(args["start_date"])).days + 1
        _need(args["channel"] in CHANNELS and width in WINDOWS)
        rows.append({"key": target["key"], "args": args})
    _need(len({row["key"] for row in rows}) == len(rows), "reference_verification_policy_duplicate_target")
    return sorted(rows, key=lambda row: row["key"])


def select_canaries(targets, timezone):
    """At most34 actual targets; no source reads or manufactured coverage.

    Per group: first, lower-middle, last, earliest month/year boundary.
    Per channel: earliest forward/backward DST boundary, preferring90/30/28
    only when multiple windows contain the same earliest boundary date.
    Calendar boundaries use the declared scope zone, not an inferred travel
    zone; none of the exact product arguments or UTC/context values change.
    """
    rows = _identities(targets)
    try:
        zone = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        raise Stop("reference_verification_policy_timezone") from None
    grouped = {(channel, width): [] for channel in CHANNELS for width in WINDOWS}
    for row in rows:
        args = row["args"]
        end = date.fromisoformat(args["end_date"])
        width = (end - date.fromisoformat(args["start_date"])).days + 1
        grouped[(args["channel"], width)].append((end, row["key"]))
    selected, groups, missing = set(), [], []
    for (channel, width), items in grouped.items():
        items.sort()
        record = {"channel": channel, "window_days": width, "available_targets": len(items),
                  "first": None, "middle": None, "last": None,
                  "month_boundary": None, "year_boundary": None}
        if not items:
            missing.append({"channel": channel, "window_days": width})
        else:
            record.update(first=items[0][1], middle=items[(len(items) - 1) // 2][1], last=items[-1][1])
            # Boundary endpoints must actually be declared; a missing first of
            # month/year is reported as absent rather than using another day.
            record["month_boundary"] = next((key for day, key in items if day.day == 1), None)
            record["year_boundary"] = next((key for day, key in items if day.month == day.day == 1), None)
            selected.update(value for name, value in record.items()
                            if name in {"first", "middle", "last", "month_boundary", "year_boundary"}
                            and value is not None)
        groups.append(record)
    dst = []
    for channel in CHANNELS:
        candidates = {"forward": [], "backward": []}
        for rank, width in enumerate((90, 30, 28)):
            for day, key in grouped[(channel, width)]:
                previous = datetime.combine(day - timedelta(days=1), time(12), zone).dst()
                current = datetime.combine(day, time(12), zone).dst()
                change = (current or timedelta()) - (previous or timedelta())
                if change:
                    candidates["forward" if change > timedelta() else "backward"].append((day, rank, key))
        record = {"channel": channel}
        for direction, values in candidates.items():
            record[direction] = min(values)[2] if values else None
            if values:
                selected.add(record[direction])
        dst.append(record)
    _need(0 < len(selected) <= MAX_CANARIES)
    return {"algorithm": ALGORITHM, "timezone": timezone, "max_canaries": MAX_CANARIES,
            "selected_keys": sorted(selected), "groups": groups, "missing_groups": missing,
            "dst": dst, "dst_zone_semantics": "declared_scope_calendar_not_inferred_night_timezone"}


def _prior_verified(workset, reference):
    """Replay only pinned prefix metadata; the caller validates full ACK state.

    No _publication_state recursion: ACK/replay already validates the complete
    chain, including every earlier proof, before accepting this policy proof.
    """
    if reference is None:
        return set(), 0
    _need(Path(reference["path"]).resolve() == workset.folder / "PUBLICATION-CHECKPOINTS.jsonl")
    body = _pin(reference, prefix=True)
    _need(body.endswith(b"\n"))
    previous, published, verified, final, independent = None, set(), set(), None, 0
    for sequence, line in enumerate(body.splitlines(keepends=True)):
        frame = json.loads(line)
        _need(frame.get("schema") == 1 and frame.get("scope_sha256") == workset.scope_sha256
              and frame.get("sequence") == sequence and frame.get("previous") == previous)
        value = json.loads(_pin(frame["reference"]))
        keys = [target["key"] for target in value["generated_targets"]]
        _need(len(keys) == len(set(keys)))
        if frame["event"] == "published":
            _need(value.get("kind") == "reference-bulk-stage-terminal" and value.get("status") != "hold"
                  and not published.intersection(keys))
            published.update(keys)
        else:
            _need(frame["event"] == "verified" and value.get("kind") == "ACTUAL_METERED_BULK_POSTCHECK_V1"
                  and set(keys) <= published and not verified.intersection(keys))
            verified.update(keys)
            charged = value["account_snapshot"]["context"]["independent_gets"]
            _need(type(charged) is int and charged >= independent)
            independent = charged
        previous, final = digest(line), frame["event"]
    _need(final == "verified" and verified == published, "reference_verification_policy_unverified_prior")
    return verified, independent


def _expected(workset, new_invocation, prior_checkpoint):
    seal = workset.assert_sealed()
    built = json.loads(_pin(seal))
    old, independent = _prior_verified(workset, prior_checkpoint)
    _need(old <= set(built["products"]))
    pending = [target for target in workset.targets if target["key"] in built["products"] and target["key"] not in old]
    identities = _identities(pending)
    request = json.loads(read(workset.folder / "REQUEST.json"))
    selection = select_canaries(pending, workset.binding["timezone"])
    return {"schema": 1, "kind": MODE, "algorithm": ALGORITHM, "new_invocation": new_invocation,
        "scope_sha256": workset.scope_sha256, "job_plan_sha256": workset.binding["job_plan_sha256"],
        "storage_target_sha256": workset.binding["storage_target_sha256"], "scope_ref": request["scope"],
        "timezone": workset.binding["timezone"], "deadline_utc": request["deadline_utc"],
        "source_seal": _ref(workset.folder / "SOURCE-SEAL.json"), "build_seal": seal,
        "prior_checkpoint": prior_checkpoint, "prior_verified_count": len(old),
        "prior_verified_keys_sha256": digest(encoded(sorted(old))),
        "prior_independent_gets": independent,
        "pending_target_count": len(pending), "pending_targets_sha256": digest(encoded(identities)),
        "selection": selection, "selection_sha256": digest(encoded(selection))}


def validate_policy_binding(binding):
    """Validate an explicit local activation pin; never activate a default."""
    if "bulkVerificationPolicy" not in binding:
        return None
    reference = binding["bulkVerificationPolicy"]
    policy = json.loads(_pin(reference))
    _need(binding.get("operation") == "rolling_bulk" and type(policy) is dict and set(policy) == FIELDS
          and policy.get("schema") == 1 and type(policy["schema"]) is int and policy.get("kind") == MODE
          and policy.get("algorithm") == ALGORITHM and policy.get("new_invocation") == binding.get("invocation")
          and policy.get("scope_ref") == binding.get("bulkScope")
          and policy.get("job_plan_sha256") == binding.get("plan", {}).get("sha256")
          and policy.get("storage_target_sha256") == binding.get("targetSHA256"),
          "reference_verification_policy_binding")
    scope = json.loads(_pin(binding["bulkScope"]))
    _need(policy["deadline_utc"] == scope.get("deadline_utc")
          and policy["timezone"] == scope.get("timezone"), "reference_verification_policy_binding")
    return policy


def validate_policy(workset, binding, targets):
    policy = validate_policy_binding(binding)
    if policy is None:
        return {"policy_ref": None, "mode": ALL_MODE,
                "selected_keys": [target["key"] for target in targets], "coverage": None, "get_counts": None}
    expected = _expected(workset, binding["invocation"], policy["prior_checkpoint"])
    _need(encoded(policy) == encoded(expected), "reference_verification_policy_changed")
    _need(digest(encoded(_identities(targets))) == policy["pending_targets_sha256"]
          and len(targets) == policy["pending_target_count"], "reference_verification_policy_pending_scope")
    keys = policy["selection"]["selected_keys"]
    coverage = {"mode": MODE, "sdk_targets": len(targets), "reader_targets": len(keys),
                "reader_get_attempts": len(keys), "reader_unchecked_targets": len(targets) - len(keys),
                "selection_sha256": policy["selection_sha256"]}
    get_counts = {"sdk": len(targets), "connected_reader": len(keys),
                  "independent": len(targets) + len(keys), "reader_observations": len(keys)}
    return {"policy_ref": binding["bulkVerificationPolicy"], "mode": MODE,
            "selected_keys": keys, "coverage": coverage, "get_counts": get_counts,
            "prior_independent_gets": policy["prior_independent_gets"]}


def create_canary_policy(job, plan_sha, scope_ref, folder, *, new_invocation, output, completion_ref=None):
    """Create one local policy before publication, without account or service I/O."""
    from .reference_bulk import _publication_state, load_bulk_workset

    _need(type(new_invocation) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,80}", new_invocation) is not None)
    workset, _ = load_bulk_workset(job, plan_sha, scope_ref, folder, completion_ref=completion_ref)
    state = _publication_state(workset)
    _need(set(state["published"]) == set(state["verified"]), "reference_verification_policy_unverified_prior")
    path = workset.folder / "PUBLICATION-CHECKPOINTS.jsonl"
    prior = _ref(path) if path.exists() else None
    value = _expected(workset, new_invocation, prior)
    output = Path(output)
    _need(output.is_absolute(), "reference_verification_policy_output")
    write(output, value)
    return _ref(output)
