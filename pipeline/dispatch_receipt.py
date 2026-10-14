"""Bounded create-only startup evidence for an explicitly correlated workflow."""
from __future__ import annotations

import re
import uuid

from .granular import json_bytes

CONTROL_BYTES = 64 * 1024


def valid_request_id(value):
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return str(parsed) == value and parsed.variant == uuid.RFC_4122 and parsed.version in range(1, 9)


def workflow_receipt(request, context):
    if context is None or request.request_id is None:
        return None
    if not valid_request_id(request.request_id):
        raise ValueError("Invalid request_id")
    if request.scheduled or request.backfill:
        # Preserve existing non-normal jobs without claiming bounded MCP coverage.
        return None
    repository = context.get("GITHUB_REPOSITORY", "")
    ref = context.get("GITHUB_REF", "")
    workflow_ref = context.get("GITHUB_WORKFLOW_REF", "")
    sha = context.get("GITHUB_SHA", "")
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
            or len(repository) > 256 or not re.fullmatch(r"refs/heads/[A-Za-z0-9/._-]{1,240}", ref)
            or not re.fullmatch(r"(?:[a-f0-9]{40}|[a-f0-9]{64})", sha)
            or context.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or context.get("GITHUB_RUN_ID") != request.run_id
            or not re.fullmatch(r"[1-9][0-9]{0,15}", request.run_id)
            or int(request.run_id) > 9007199254740991):
        raise ValueError("Unverified workflow source")
    prefix, suffix = repository + "/", "@" + ref
    if not workflow_ref.startswith(prefix) or not workflow_ref.endswith(suffix):
        raise ValueError("Unverified workflow source")
    path = workflow_ref[len(prefix):-len(suffix)]
    if not re.fullmatch(r"\.github/workflows/[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\.ya?ml", path):
        raise ValueError("Unverified workflow source")
    mode = request.mode
    activity_id = request.activity_id if mode == "activity" else None
    day = request.wake_date if mode == "night" else request.expected_date if mode == "activity" else None
    new_expected = request.new_activity_expected if mode != "night" else False
    scope = (f"night/{day}" if mode == "night" else
             f"activity/{day or 'latest'}/{activity_id or 'latest'}/{'new' if new_expected else 'known'}"
             if mode == "activity" else None)
    components = (["sleep", "hrv"] if mode == "night" else
                  ["activity_files", "coach_input", "context"] if mode == "activity" else
                  ["summary", "health", "body"] + (["activity_files", "coach_input"] if request.include_granular else []))
    return {"schema_version": 1, "kind": "refresh-dispatch-receipt",
            "request_id": request.request_id, "run_id": int(request.run_id),
            "source": {"repository": repository, "workflow_path": path, "event": "workflow_dispatch",
                       "ref": ref, "source_sha": sha},
            "request": {"mode": mode, "scope": scope, "date": day, "activity_id": activity_id,
                        "new_expected": new_expected, "repair_only": request.repair_only,
                        "include_granular": request.include_granular, "components": components}}


def publish_start(store, evidence):
    if not isinstance(evidence, dict) or not valid_request_id(evidence.get("request_id")):
        raise ValueError("Invalid request_id")
    publish_matching(store, f"refresh/requests/{evidence['request_id']}.json", json_bytes(evidence))


def publish_matching(store, key, data):
    if len(data) > CONTROL_BYTES:
        raise ValueError("Dispatch receipt exceeds control limit")
    if store.put_if_absent(key, data, "application/json"):
        return
    if store.get_bounded(key, CONTROL_BYTES) != data:
        raise ValueError("Dispatch receipt conflicts with existing evidence")
