"""Bounded, resumable coach-input generation using R2 data only."""

from __future__ import annotations

import csv
import gzip
import io
import json
import re
from datetime import datetime, timezone
from typing import Any

from .coach import (
    ANALYZER_VERSION,
    GCT_SIDES_ANALYZER_VERSION,
    STORED_CONTEXT_ANALYZER_VERSION,
    TRAINING_EFFECT_ANALYZER_VERSION,
    build_coach_input,
    select_profile,
)
from .coach_context_sources import assemble_stored_context, valid_source_proof
from .granular import gzip_json, sha256
from .measurements import measured_component
from .output_privacy import SafeArgumentParser, log_report, safe_cli
from .r2_store import R2Store
from .schema import activity_calendar_years, activity_local_date, to_utc

PLAN_KEY = "backfill/coach-input/v1/plan.json"
PROFILE_PREFIX = "coach/profiles/v1/"
MAX_ACTIVITIES_PER_RUN = 50
LEGACY_ANALYZER_VERSION = "1.0.0"
SUPPORTED_ANALYZER_VERSIONS = (
    LEGACY_ANALYZER_VERSION, "1.0.1", "1.1.0", ANALYZER_VERSION, STORED_CONTEXT_ANALYZER_VERSION,
    TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION,
)
BULK_REUSE_VERSION = 1


def _decode(data: bytes) -> bytes:
    return gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data


def _json(store, key: str) -> dict[str, Any]:
    value = json.loads(_decode(store.get(key)))
    if not isinstance(value, dict):
        raise ValueError(f"R2 object {key} is not a JSON object")
    return value


def _activities(data: bytes, *, clocks: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    text = _decode(data).decode("utf-8-sig")
    rows = []
    for row in csv.DictReader(io.StringIO(text)):
        activity_id = str(row.get("Activity ID") or "")
        if activity_id.startswith("garmin-"):
            raw_id = activity_id.removeprefix("garmin-")
        else:
            raw_id = activity_id
        activity_type = str(row.get("Activity Type") or "")
        if not raw_id.isdigit() or "run" not in activity_type.lower():
            continue
        raw_date = str(row.get("Activity Date") or "")
        local = activity_local_date(row.get("Activity Local Date"))
        if clocks is not None:
            clocks[raw_id] = {"local": local, "utc": raw_date}
        rows.append({
            "id": raw_id,
            "date": local or raw_date[:10],
            "name": row.get("Activity Name"),
            "type": activity_type,
            "moving_seconds": _float(row.get("Moving Time")),
        })
    return sorted(rows, key=lambda item: (item["date"], item["id"]), reverse=True)


def _float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _profiles(store, *, keys: set[str] | None = None) -> list[dict[str, Any]]:
    profiles = []
    for key in sorted(store.list_keys(PROFILE_PREFIX) if keys is None else keys):
        try:
            profiles.append(_json(store, key))
        except (ValueError, json.JSONDecodeError, KeyError):
            continue
    return profiles


def _latest_context_keys(all_keys: set[str]) -> dict[str, str]:
    """Find the newest context key per activity from one shared R2 listing."""
    latest: dict[str, str] = {}
    marker = "/context/v1/"
    for key in all_keys:
        if marker not in key:
            continue
        prefix = key.split(marker, 1)[0]
        if key > latest.get(prefix, ""):
            latest[prefix] = key
    return latest


def _activity_context_index(keys: set[str]) -> dict[str, str]:
    """Keep each exact context namespace's newest direct JSON child."""
    latest: dict[str, str] = {}
    for key in keys:
        parts = key.split("/", 5)
        if (len(parts) != 6 or parts[0] != "activities" or parts[3:5] != ["context", "v1"]
                or "/" in parts[5] or not parts[5].endswith(".json")):
            continue
        prefix = "/".join(parts[:5]) + "/"
        previous = latest.get(prefix)
        if previous is None or parts[5] > previous.rsplit("/", 1)[-1]:
            latest[prefix] = key
    # Cross-year ambiguity belongs to the queried activity, not index creation.
    return latest


def _activity_context_key(
    keys: set[str], activity: dict[str, Any], *, context_index: dict[str, str] | None = None,
) -> str | None:
    prefixes = [f"activities/{year}/{activity['id']}/context/v1/"
                for year in activity_calendar_years(activity["date"])]
    if context_index is None:
        candidates = [key for key in keys if any(key.startswith(prefix) and "/" not in key[len(prefix):]
                                               for prefix in prefixes) and key.endswith(".json")]
    else:
        candidates = [context_index[prefix] for prefix in prefixes if prefix in context_index]
    # Compare append-only timestamp basenames, not namespace years.
    candidates.sort(key=lambda key: key.rsplit("/", 1)[-1], reverse=True)
    if len(candidates) > 1 and candidates[0].rsplit("/", 1)[-1] == candidates[1].rsplit("/", 1)[-1]:
        raise ValueError("Activity context timestamp is ambiguous across calendar years")
    return candidates[0] if candidates else None


def _resolve_legacy_calendar(store, activity, utc_clock, keys):
    """Read at most two known canonical year candidates for one pending row."""
    fallback = None
    selected = None
    for year in activity_calendar_years(activity["date"]):
        key = f"activities/{year}/{activity['id']}/activity.v1.json"
        if key not in keys:
            continue
        decoded = _json(store, key)
        version, source_kind = decoded.get("schema_version"), decoded.get("source")
        supported_version = type(version) in (int, float) and version == 1
        if ((version is not None and not supported_version)
                or (source_kind is not None and source_kind != "garmin-fit")):
            raise ValueError("Canonical activity calendar schema or source is unsupported")
        source = decoded.get("activity", {})
        local = activity_local_date(source.get("start_time_local")) if isinstance(source, dict) else None
        if (not isinstance(source, dict) or source.get("id") is not None and str(source["id"]) != activity["id"]
                or local and str(source.get("id")) != activity["id"]):
            raise ValueError("Canonical activity calendar does not match its identity")
        if not local:
            if year == activity["date"][:4]:
                fallback = (activity["date"], key, decoded, "utc_fallback")
            continue
        if not supported_version or source_kind != "garmin-fit":
            # A legacy object without a declared contract cannot establish local
            # authority. Keep only the CSV UTC fallback in its original year.
            if year == activity["date"][:4]:
                fallback = (activity["date"], key, decoded, "utc_fallback")
                continue
            raise ValueError("Canonical activity calendar schema or source is unverified")
        if local[:4] != year:
            raise ValueError("Canonical activity calendar does not match its namespace")
        canonical_utc = source.get("start_time_gmt")
        if canonical_utc in (None, ""):
            # Older decoded exports can have only a local clock. Do not claim
            # association with the summary's UTC instant; keep a visible fallback.
            if year == activity["date"][:4]:
                fallback = (activity["date"], key, decoded, "utc_fallback")
                continue
            raise ValueError("Canonical activity UTC clock is unknown")
        if not activity_local_date(canonical_utc):
            raise ValueError("Canonical activity UTC clock is invalid")
        try:
            expected = datetime.fromisoformat(utc_clock.replace("Z", "+00:00"))
            actual = datetime.fromisoformat(canonical_utc.replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Canonical activity UTC clock is unknown") from exc
        if (to_utc(actual) != to_utc(expected)
                or abs((datetime.fromisoformat(local).date() - expected.date()).days) > 1):
            raise ValueError("Canonical activity calendar does not match its UTC clock")
        candidate = (local, key, decoded, "garmin_local")
        if selected is not None:
            raise ValueError("Canonical activity calendar is ambiguous across years")
        selected = candidate
    return selected or fallback


def _manifest_revisions(
    store,
    all_keys: set[str],
    object_revisions: dict[str, str],
    running_ids: set[str],
) -> dict[str, str]:
    """Read change revisions from listing metadata, with a test-store fallback."""
    revisions: dict[str, str] = {}
    suffix = "/source-manifest.v1.json"
    for key in sorted(item for item in all_keys if item.endswith(suffix)):
        parts = key.split("/")
        if len(parts) < 4 or parts[0] != "activities" or parts[2] not in running_ids:
            continue
        revision = object_revisions.get(key)
        revisions[parts[2]] = revision if revision is not None else sha256(store.get(key))
    return revisions


def _source_signature(
    activity: dict[str, Any],
    profile: dict[str, Any],
    *,
    context_key: str | None,
    manifest_revision: str | None,
    analyzer_version: str | None = None,
) -> str:
    """Identify inputs without relabeling a retained historical engine."""
    value = {
        "analyzer_version": analyzer_version or ANALYZER_VERSION,
        "activity": activity,
        "profile": profile,
        "context_key": context_key,
        "manifest_revision": manifest_revision,
    }
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _single_input_signature(
    activity: dict[str, Any], profile: dict[str, Any],
    required: list[str], revisions: dict[str, str], context_key: str | None,
    *, analyzer_version: str | None = None, input_source_revisions: dict | None = None,
) -> str | None:
    """Reuse only when every selected input has a revision, including context."""
    selected = [*required, *([context_key] if context_key else [])]
    if any(not isinstance(revisions.get(key), str) or not revisions[key] for key in selected):
        return None
    metadata = ({**activity, "moving_seconds": _float(activity["moving_seconds"])}
                if "moving_seconds" in activity else activity)
    value = {
        "signature_version": 2,
        "analyzer_version": analyzer_version or ANALYZER_VERSION,
        "activity": metadata,
        "profile": profile,
        "revisions": {key: revisions[key] for key in selected},
    }
    if ((analyzer_version or ANALYZER_VERSION) == STORED_CONTEXT_ANALYZER_VERSION
            or analyzer_version in (TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION)
            and input_source_revisions is not None):
        if not valid_source_proof(input_source_revisions, activity["date"]):
            return None
        value["input_source_revisions"] = input_source_revisions
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _bulk_cached_proof(proof, activity, profile, required, revisions, keys, context_key):
    """Reuse only the exact input, canonical and selection revisions inspected before."""
    if not isinstance(proof, dict) or not profile:
        return False
    engine = proof.get("analyzer_version")
    if engine not in SUPPORTED_ANALYZER_VERSIONS:
        return False
    key = proof.get("analysis_key")
    canonical = f"activities/{activity['date'][:4]}/{activity['id']}/coach-input/v1/canonical/"
    if (not isinstance(key, str) or not key.startswith(canonical)
            or len(key[len(canonical):]) != 29 or not key.endswith(".json")
            or any(c not in "0123456789abcdef" for c in key[len(canonical):-5])):
        return False
    pointer = f"activities/{activity['date'][:4]}/{activity['id']}/coach-input/v1/latest-ready.json"
    signature = _single_input_signature(
        activity, profile, required, revisions, context_key, analyzer_version=engine,
        input_source_revisions=proof.get("input_source_revisions"),
    )
    return bool(signature and signature == proof.get("input_signature")
                and isinstance(key, str) and key in keys
                and proof.get("canonical_revision") and revisions.get(key) == proof["canonical_revision"]
                and proof.get("pointer_revision") and revisions.get(pointer) == proof["pointer_revision"])


def _bulk_selection_proof(store, activity, profile, required, revisions, keys,
                          context_key, saved_pointer_revision, profiles):
    """Bounded inspection: retain known engines, prove input change, or defer unknown."""
    prefix = f"activities/{activity['date'][:4]}/{activity['id']}"
    pointer_key = f"{prefix}/coach-input/v1/latest-ready.json"
    if pointer_key not in keys:
        return "unknown", None
    try:
        pointer = _json(store, pointer_key)
        key = pointer.get("analysis_key")
        canonical = f"{prefix}/coach-input/v1/canonical/"
        if (not isinstance(key, str) or not key.startswith(canonical) or key not in keys
                or len(key[len(canonical):]) != 29 or not key.endswith(".json")
                or any(c not in "0123456789abcdef" for c in key[len(canonical):-5])):
            return "unknown", None
        candidate = _json(store, key)
        engine = candidate.get("analyzer_version")
        stored = pointer.get("input_signature")
        current_engine = engine in (ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION)
        if engine is None and pointer.get("analyzer_version") in (ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION):
            return "conflict", None
        marked = type(pointer.get("selection_version")) is int and pointer["selection_version"] == 1
        unmarked = "selection_version" not in pointer
        valid_schema = type(pointer.get("schema_version")) is int and pointer["schema_version"] == 1
        repair_marker = current_engine and (not valid_schema or not marked and not unmarked)
        if not repair_marker and (not valid_schema or not marked and not unmarked):
            return "unknown", None
        if (engine not in SUPPORTED_ANALYZER_VERSIONS
                or type(candidate.get("schema_version")) is not int or candidate["schema_version"] != 1
                or candidate.get("analysis_id") != key[len(canonical):-5]
                or str(candidate.get("activity_id")) != activity["id"]
                ):
            return "unknown", None
        # Validate the C3 content identity, restoring its builder seed before hashing.
        source, effective = candidate["source"], candidate["profile"]
        context = candidate.get("user_context")
        seed = {
            "activity_id": candidate["activity_id"], "analyzer_version": engine,
            "fit_sha256": source["fit_sha256"], "tcx_sha256": source["tcx_sha256"],
            "profile_id": str(effective["profile_id"]),
            "context_revision": str(context.get("context_id")) if context else None,
        }
        context_sources = pointer.get("input_source_revisions")
        has_context = (engine == STORED_CONTEXT_ANALYZER_VERSION
                       or engine in (TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION)
                       and "stored_context_snapshot" in candidate)
        if has_context:
            snapshot = candidate.get("stored_context_snapshot")
            if (pointer.get("analyzer_version") != engine or not isinstance(snapshot, dict)
                    or not valid_source_proof(context_sources, activity["date"])):
                return "unknown", None
            seed["stored_context_snapshot"] = snapshot
        elif engine in (TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION) and context_sources is not None:
            return "unknown", None
        if engine in (TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION):
            if pointer.get("analyzer_version") != engine or not isinstance(candidate.get("training_effect"), dict):
                return "unknown", None
            seed["training_effect"] = candidate["training_effect"]
        if engine == GCT_SIDES_ANALYZER_VERSION:
            if not isinstance(candidate.get("ground_contact_sides"), dict):
                return "unknown", None
            seed["ground_contact_sides"] = candidate["ground_contact_sides"]
        seed_id = sha256(json.dumps(seed, sort_keys=True, separators=(",", ":")).encode())[:24]
        checked = {**candidate, "summary": dict(candidate["summary"]), "analysis_id": seed_id}
        _identify_coach_input(checked)
        if checked["analysis_id"] != candidate["analysis_id"]:
            if engine in (TRAINING_EFFECT_ANALYZER_VERSION, GCT_SIDES_ANALYZER_VERSION) and has_context:
                # As with1.3, damaged retrospective snapshots are unknown. They
                # cannot authorize repair, historical regeneration or writes.
                return "unknown", None
            if current_engine:
                return ("repair", None) if candidate["analysis_id"] == seed_id else ("conflict", None)
            return "unknown", None
        if repair_marker:
            return "repair", None
        if (not isinstance(stored, str) or len(stored) != 64
                or any(c not in "0123456789abcdef" for c in stored)):
            return "unknown", None
        expected = _single_input_signature(
            activity, profile, required, revisions, context_key, analyzer_version=engine,
            input_source_revisions=context_sources,
        )
        if expected is None or not revisions.get(key) or not revisions.get(pointer_key):
            return "unknown", None
        if stored != expected:
            # The previous C3 checkpoint attests an unchanged legacy pointer. New
            # pointers also retain the context revision needed to verify old inputs.
            trusted_previous = bool(saved_pointer_revision
                                    and saved_pointer_revision == revisions[pointer_key])
            old_context_key = pointer.get("input_context_key")
            old_revisions = dict(pointer.get("source_revisions", {}))
            if old_context_key:
                allowed_prefixes = [
                    f"activities/{year}/{activity['id']}/context/v1/"
                    for year in activity_calendar_years(activity["date"])
                ]
                matched_prefix = next((item for item in allowed_prefixes
                                       if isinstance(old_context_key, str)
                                       and old_context_key.startswith(item)), None)
                if (matched_prefix is None
                        or not re.fullmatch(r"[A-Za-z0-9_-]+\.json", old_context_key[len(matched_prefix):])
                        or old_context_key not in keys
                        or not isinstance(pointer.get("input_context_revision"), str)
                        or revisions.get(old_context_key) != pointer["input_context_revision"]):
                    return "unknown", None
                old_revisions[old_context_key] = pointer.get("input_context_revision")
            old_activity = {"id": activity["id"], **candidate["activity"],
                            "moving_seconds": candidate["summary"]["moving_seconds"]}
            old_profiles = [select_profile([raw], candidate["activity"]["date"]) for raw in profiles]
            old_signatures = {
                _single_input_signature(
                    old_activity, normalized, required, old_revisions, old_context_key,
                    analyzer_version=engine,
                    input_source_revisions=context_sources,
                ) for normalized in old_profiles if normalized
                and {name: normalized.get(name) for name in (
                    "profile_id", "name", "effective_from", "zones", "references")} == effective
            } if not context or "input_context_key" in pointer else set()
            return ("changed", None) if trusted_previous or stored in old_signatures else ("unknown", None)
        if (candidate["activity"] != {"date": activity["date"], "name": activity["name"], "type": activity["type"]}
                or candidate["summary"]["moving_seconds"] != _float(activity["moving_seconds"])
                or effective != {name: profile.get(name) for name in (
                    "profile_id", "name", "effective_from", "zones", "references")}
                or pointer.get("profile_id") != effective["profile_id"]
                or pointer.get("context_id") != (context.get("context_id") if context else None)
                or pointer.get("source_revisions") != {key: revisions[key] for key in required}):
            return "unknown", None
        return "retained", {
            "analyzer_version": engine, "input_signature": expected, "analysis_key": key,
            "canonical_revision": revisions[key], "pointer_revision": revisions[pointer_key],
            "selection_kind": "unmarked" if unmarked else "marked",
            **({"input_source_revisions": context_sources} if has_context else {}),
        }
    except (KeyError, TypeError, ValueError, AttributeError):
        return "unknown", None


def _identify_coach_input(coach: dict[str, Any]) -> None:
    """Use the targeted content-hash scheme for both generators."""
    # Keep the builder's source identity in the hash for compatibility with
    # existing targeted versions. The deterministic result also covers metadata,
    # effective profile contents, context and derived endurance measurements.
    # CSV metadata supplies floats; targeted callers can supply equivalent ints.
    # Normalize only this caller-supplied field before identifying/publishing.
    coach["summary"]["moving_seconds"] = _float(coach["summary"]["moving_seconds"])
    coach["analysis_id"] = sha256(json.dumps(
        coach, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode())[:24]


def _publish_coach_input(store, key: str, coach: dict[str, Any], keys: set[str]) -> None:
    """Keep canonical bytes immutable, including after a stale LIST or lost reply."""
    payload = gzip_json(coach)
    if key not in keys and store.put_if_absent(
        key, payload, "application/json", encoding="gzip",
    ):
        keys.add(key)
        return
    # Compare decoded JSON rather than gzip bytes so older encodings/key orders
    # remain compatible. Even an identical result is never rewritten.
    # JSON comparison also distinguishes booleans from numbers (False != 0),
    # unlike Python object equality. Compare the actual serialized projection.
    expected = json.loads(_decode(payload))
    if json.dumps(_json(store, key), sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise ValueError("Coach analysis identity conflict")


def _publish_current_analysis(
    store, *, prefix: str, key: str, profile: dict[str, Any],
    coach: dict[str, Any], revisions: dict[str, str], keys: set[str],
    previous_pointer: dict[str, Any] | None = None,
    input_signature: str | None = None,
    context_key: str | None = None,
    input_source_revisions: dict | None = None,
) -> str | None:
    """Commit current selection after immutable content, before bulk progress."""
    pointer_key = f"{prefix}/coach-input/v1/latest-ready.json"
    if previous_pointer is None:
        previous_pointer = _json(store, pointer_key) if pointer_key in keys else {}
    required = [f"{prefix}/{name}" for name in (
        "activity.v1.json", "activity.endurance.v1.json", "activity.tcx",
    )]
    pointer = {
        "schema_version": 1, "selection_version": 1, "analysis_key": key,
        "source_revisions": {source: revisions[source] for source in required if revisions.get(source)},
        "profile_id": profile["profile_id"],
        "context_id": coach.get("user_context", {}).get("context_id") if coach.get("user_context") else None,
        "analyzer_version": coach["analyzer_version"],
        "input_context_key": context_key,
        "input_context_revision": revisions.get(context_key) if context_key else None,
    }
    signature = input_signature
    if signature is not None:
        pointer["input_signature"] = signature
    if input_source_revisions is not None:
        pointer["input_source_revisions"] = input_source_revisions
    if json.dumps(previous_pointer, sort_keys=True) != json.dumps(pointer, sort_keys=True):
        store.put(pointer_key, json.dumps(pointer, separators=(",", ":")).encode(), "application/json")
        keys.add(pointer_key)
        return getattr(store, "last_write_revision", None)
    return revisions.get(pointer_key)


def _selection_current(activity, revisions, keys, selections, legacy_selections) -> bool:
    pointer_key = f"activities/{activity['date'][:4]}/{activity['id']}/coach-input/v1/latest-ready.json"
    activity_id = activity["id"]
    if activity_id not in selections and activity_id not in legacy_selections:
        return pointer_key not in keys
    revision = selections.get(activity_id, legacy_selections.get(activity_id))
    return bool(isinstance(revision, str) and revision and revisions.get(pointer_key) == revision)


def _legacy_selection_matches(store, item, pointer, revisions, keys) -> bool:
    """Inspect only legacy metadata; never regenerate matching historical input."""
    if "selection_version" in pointer or type(pointer.get("schema_version")) is not int or pointer["schema_version"] != 1:
        return False
    activity, profile, prefix = item["activity"], item["profile"], item["prefix"]
    key = pointer.get("analysis_key")
    canonical = f"{prefix}/coach-input/v1/canonical/"
    if (not isinstance(key, str) or not key.startswith(canonical) or key not in keys
            or len(key[len(canonical):]) != 29 or not key.endswith(".json")
            or any(char not in "0123456789abcdef" for char in key[len(canonical):-5])):
        return False
    required = [item["fit_key"], item["endurance_key"], item["tcx_key"]]
    if any(not revisions.get(source) or pointer.get("source_revisions", {}).get(source) != revisions[source]
           for source in required):
        return False
    candidate = _json(store, key)
    metadata = candidate.get("activity", {})
    moving = candidate.get("summary", {}).get("moving_seconds")
    return bool(type(candidate.get("schema_version")) is int and candidate["schema_version"] == 1
                and candidate.get("analysis_id") == key[len(canonical):-5]
                and str(candidate.get("activity_id")) == activity["id"]
                and metadata.get("date") == activity["date"]
                and metadata.get("name") == activity["name"] and metadata.get("type") == activity["type"]
                and not isinstance(moving, bool) and moving == activity["moving_seconds"]
                and candidate.get("profile", {}).get("profile_id") == profile["profile_id"])


@measured_component("coach_input")
def run_one(*, activity: dict[str, Any], store: R2Store, force: bool = False) -> dict[str, Any]:
    """Analyze one prefix, reusing unchanged inputs unless recovery is forced."""
    activity_id = str(activity["id"])
    if not activity_id.isdigit() or len(activity_id) > 20:
        raise ValueError("Invalid activity ID")
    prefix = f"activities/{activity['date'][:4]}/{activity_id}"
    revisions = (store.list_object_revisions(f"{prefix}/")
                 if hasattr(store, "list_object_revisions") else {})
    keys = set(revisions) if revisions else store.list_keys(f"{prefix}/")
    profile = select_profile(_profiles(store), activity["date"])
    blocked = None
    if not profile:
        blocked = "no_effective_profile"
    required = [f"{prefix}/{name}" for name in (
        "activity.v1.json", "activity.endurance.v1.json", "activity.tcx",
    )]
    if not blocked and any(key not in keys for key in required):
        blocked = "missing_artifacts"
    if blocked:
        return {"processed_sources": {}, "blocked_activities": [
            {"activity_id": activity_id, "reason": blocked},
        ]}
    for year in activity_calendar_years(activity["date"]):
        if year == activity["date"][:4]:
            continue
        alias = f"activities/{year}/{activity_id}/context/v1/"
        extra = store.list_object_revisions(alias) if hasattr(store, "list_object_revisions") else {}
        revisions.update(extra)
        keys.update(extra if extra else store.list_keys(alias))
    context_key = _activity_context_key(keys, activity)
    pointer_key = f"{prefix}/coach-input/v1/latest-ready.json"
    previous_pointer = _json(store, pointer_key) if pointer_key in keys else {}
    endurance = _json(store, required[1])
    if endurance.get("available") is False:
        return {"processed_sources": {}, "blocked_activities": [
            {"activity_id": activity_id, "reason": "endurance_data_unavailable"},
        ]}
    decoded = _json(store, required[0])
    stored_context, context_sources = assemble_stored_context(store, activity, decoded, endurance)
    # Fingerprint the caller's metadata before an R2-only repair restores a known
    # moving time. Repeated calls with the same metadata reuse that successful result.
    signature = _single_input_signature(
        activity, profile, required, revisions, context_key,
        analyzer_version=GCT_SIDES_ANALYZER_VERSION,
        input_source_revisions=context_sources,
    )
    previous_key = previous_pointer.get("analysis_key")
    canonical_prefix = f"{prefix}/coach-input/v1/canonical/"
    if (not force and signature is not None
            and type(previous_pointer.get("schema_version")) is int
            and previous_pointer.get("schema_version") == 1
            and type(previous_pointer.get("selection_version")) is int
            and previous_pointer.get("selection_version") == 1
            and previous_pointer.get("input_signature") == signature
            and isinstance(previous_key, str) and previous_key.startswith(canonical_prefix)
            and previous_key.endswith(".json") and previous_key in keys):
        identifier = previous_key[len(canonical_prefix):-5]
        if len(identifier) == 24 and all(char in "0123456789abcdef" for char in identifier):
            print(json.dumps({"event": "targeted_coach_input", "analysis_reused": True}))
            return {"processed_sources": {activity_id: identifier}, "blocked_activities": [],
                    "skipped_this_run": [], "reused_sources": {activity_id: identifier}}
    tcx_hash = sha256(store.get(required[2]))
    # An R2-only repair has no Garmin summary. Preserve an already known moving
    # time only when both source files are unchanged; never infer it from elapsed time.
    if activity.get("moving_seconds") is None and pointer_key in keys:
        if isinstance(previous_key, str) and previous_key.startswith(canonical_prefix) and previous_key in keys:
            previous = _json(store, previous_key)
            source = previous.get("source", {})
            if (source.get("fit_sha256") == decoded.get("source_fit_sha256")
                    and source.get("tcx_sha256") == tcx_hash):
                activity = {**activity, "moving_seconds": previous.get("summary", {}).get("moving_seconds")}
    coach = build_coach_input(
        activity=activity, decoded_fit=decoded,
        endurance=endurance, profile=profile,
        tcx_sha256=tcx_hash,
        context=_json(store, context_key) if context_key else None,
        stored_context_snapshot=stored_context,
    )
    _identify_coach_input(coach)
    key = f"{prefix}/coach-input/v1/canonical/{coach['analysis_id']}.json"
    _publish_coach_input(store, key, coach, keys)
    _publish_current_analysis(
        store, prefix=prefix, key=key, profile=profile, coach=coach,
        revisions=revisions, keys=keys, previous_pointer=previous_pointer,
        input_signature=signature, context_key=context_key,
        input_source_revisions=context_sources,
    )
    print(json.dumps({"event": "targeted_coach_input", "analysis_reused": False}))
    return {"processed_sources": {activity_id: coach["analysis_id"]},
            "blocked_activities": [], "skipped_this_run": [], "reused_sources": {}}


def run(
    *,
    max_activities: int = 50,
    store: R2Store | None = None,
    priority_activity_ids: set[str] | None = None,
) -> dict[str, Any]:
    if not 1 <= max_activities <= MAX_ACTIVITIES_PER_RUN:
        raise ValueError(f"max_activities must be between 1 and {MAX_ACTIVITIES_PER_RUN}")
    store = store or R2Store()
    summary_bytes = store.get("summary/activities.csv")
    # One complete invocation-local keyset feeds both profile loading and the
    # fingerprint, including keys whose bodies cannot be parsed as profiles.
    profile_keys = store.list_keys(PROFILE_PREFIX)
    profiles = _profiles(store, keys=profile_keys)
    clocks: dict[str, dict[str, Any]] = {}
    activities = _activities(summary_bytes, clocks=clocks)
    if hasattr(store, "list_object_revisions"):
        object_revisions = store.list_object_revisions("activities/")
        all_keys = set(object_revisions)
    else:
        object_revisions = {}
        all_keys = store.list_keys("activities/")
    context_index = _activity_context_index(all_keys)
    running_ids = {activity["id"] for activity in activities}
    manifest_revisions = _manifest_revisions(
        store, all_keys, object_revisions, running_ids
    )
    fingerprint_parts = [
        f"analyzer:{ANALYZER_VERSION}",
        sha256(summary_bytes),
        sha256(json.dumps(profiles, sort_keys=True, separators=(",", ":")).encode()),
        *sorted(profile_keys),
        *sorted(
            key
            for key in all_keys
            if any(
                marker in key
                for marker in (
                    "/activity.v1.json",
                    "/activity.endurance.v1.json",
                    "/activity.tcx",
                    "/context/v1/",
                )
            )
        ),
    ]
    fingerprint_parts.extend(
        f"input:{key}:{object_revisions.get(key)}"
        for key in sorted(all_keys) if any(marker in key for marker in (
            "/activity.v1.json", "/activity.endurance.v1.json", "/activity.tcx", "/context/v1/",
        ))
    )
    # Activity refreshes replace canonical objects under the same keys. Their
    # small manifests make those replacements visible without downloading the
    # FIT, TCX and normalized JSON for every previously processed activity.
    fingerprint_parts.extend(
        f"manifest:{activity_id}:{revision}"
        for activity_id, revision in sorted(manifest_revisions.items())
    )
    source_fingerprint = sha256("\n".join(fingerprint_parts).encode())
    previous: dict[str, Any] = {}
    if PLAN_KEY in store.list_keys("backfill/coach-input/v1/"):
        previous = _json(store, PLAN_KEY)
    saved_clocks = previous.get("activity_clock_revisions", {})
    saved_clocks = saved_clocks if isinstance(saved_clocks, dict) else {}
    clock_revisions = {}
    for activity in activities:
        cached = saved_clocks.get(activity["id"])
        if not isinstance(cached, dict) or clocks[activity["id"]]["local"]:
            continue
        source_key = cached.get("source_key")
        day = activity_local_date(cached.get("date"))
        expected = {f"activities/{year}/{activity['id']}/activity.v1.json"
                    for year in activity_calendar_years(activity["date"])}
        if (day and cached.get("summary_utc") == clocks[activity["id"]]["utc"]
                and source_key in expected and cached.get("source_revision")
                and cached["source_revision"] == object_revisions.get(source_key)
                and day[:4] == source_key.split("/")[1]
                and cached.get("source") in {"garmin_local", "utc_fallback"}
                and (cached["source"] != "garmin_local"
                     or type(cached.get("calendar_contract_version")) is int and cached["calendar_contract_version"] == 1)
                and abs((datetime.fromisoformat(day).date() - datetime.fromisoformat(activity["date"]).date()).days) <= 1
                and (cached["source"] != "utc_fallback" or day == activity["date"])
                and cached.get("candidate_revisions") == {
                    key: object_revisions.get(key) for key in expected if key in all_keys
                }):
            activity["date"] = day
            clock_revisions[activity["id"]] = cached
    invalidated_clocks = {activity_id for activity_id in saved_clocks
                          if activity_id in clocks and not clocks[activity_id]["local"]
                          and activity_id not in clock_revisions}
    if previous:
        selections = previous.get("selection_revisions", {})
        selections = selections if isinstance(selections, dict) else {}
        legacy_selections = previous.get("legacy_selection_revisions", {})
        legacy_selections = legacy_selections if isinstance(legacy_selections, dict) else {}
        processed = previous.get("processed_sources", {})
        processed = processed if isinstance(processed, dict) else {}
        selections_current = all(
            _selection_current(activity, object_revisions, all_keys, selections, legacy_selections)
            for activity in activities if activity["id"] in processed
        )
        proofs = previous.get("bulk_reuse_proofs", {})
        proofs = proofs if isinstance(proofs, dict) else {}
        proofs_current = all(
            _bulk_cached_proof(
                proofs.get(activity["id"]), activity, select_profile(profiles, activity["date"]),
                [f"activities/{activity['date'][:4]}/{activity['id']}/{name}" for name in (
                    "activity.v1.json", "activity.endurance.v1.json", "activity.tcx")],
                object_revisions, all_keys, _activity_context_key(all_keys, activity, context_index=context_index),
            ) for activity in activities if activity["id"] in processed
        )
        if (previous.get("bulk_reuse_version") == BULK_REUSE_VERSION and proofs_current
                and not previous.get("deferred_legacy_activity_count")
                and selections_current and len(clock_revisions) == len(saved_clocks)
                and previous.get("source_fingerprint") == source_fingerprint and previous.get("status") in {
            "complete",
            "complete_with_blocked",
            "waiting_for_profile",
        }):
            return previous
    processed_sources = previous.get("processed_sources")
    if not isinstance(processed_sources, dict):
        processed_sources = {}
    processed_sources = {
        str(activity_id): str(signature)
        for activity_id, signature in processed_sources.items()
        if str(activity_id) in running_ids and isinstance(signature, str)
    }

    saved_selections = previous.get("selection_revisions", {})
    selection_revisions = {
        activity_id: revision for activity_id, revision in saved_selections.items()
        if activity_id in running_ids
    } if isinstance(saved_selections, dict) else {}

    saved_legacy = previous.get("legacy_selection_revisions", {})
    legacy_selection_revisions = {
        activity_id: revision for activity_id, revision in saved_legacy.items()
        if activity_id in running_ids
    } if isinstance(saved_legacy, dict) else {}

    saved_proofs = previous.get("bulk_reuse_proofs", {})
    reuse_proofs = dict(saved_proofs) if isinstance(saved_proofs, dict) else {}
    pending = []
    blocked = []
    already_complete = 0
    reuse_inspections = 0
    deferred_legacy = 0
    selection_adoptions = 0
    ordered = sorted(activities, key=lambda activity: activity["id"] not in priority_activity_ids) if priority_activity_ids else activities
    for activity in ordered:
        profile = select_profile(profiles, activity["date"])
        legacy_clock = not clocks[activity["id"]]["local"] and activity["id"] not in clock_revisions
        if not profile and (not legacy_clock or not profiles):
            blocked.append({"activity_id": activity["id"], "reason": "no_effective_profile"})
            processed_sources.pop(activity["id"], None)
            continue
        prefix = f"activities/{activity['date'][:4]}/{activity['id']}"
        if legacy_clock and f"{prefix}/activity.v1.json" not in all_keys:
            prefix = next((f"activities/{year}/{activity['id']}" for year in activity_calendar_years(activity["date"])
                           if f"activities/{year}/{activity['id']}/activity.v1.json" in all_keys), prefix)
        fit_key = f"{prefix}/activity.v1.json"
        endurance_key = f"{prefix}/activity.endurance.v1.json"
        tcx_key = f"{prefix}/activity.tcx"
        missing = [key for key in (fit_key, endurance_key, tcx_key) if key not in all_keys]
        if missing and not (legacy_clock and any(
            f"activities/{year}/{activity['id']}/activity.v1.json" in all_keys
            for year in activity_calendar_years(activity["date"])
        )):
            blocked.append(
                {"activity_id": activity["id"], "reason": "missing_artifacts", "keys": missing}
            )
            processed_sources.pop(activity["id"], None)
            continue
        context_key = _activity_context_key(all_keys, activity, context_index=context_index)
        required = [fit_key, endurance_key, tcx_key]
        cached_proof = reuse_proofs.get(activity["id"])
        cached_engine = cached_proof.get("analyzer_version") if isinstance(cached_proof, dict) else None
        cached_source_matches = cached_engine and processed_sources.get(activity["id"]) == _source_signature(
            activity, profile, context_key=context_key,
            manifest_revision=manifest_revisions.get(activity["id"]), analyzer_version=cached_engine,
        )
        if (cached_source_matches and not legacy_clock and activity["id"] not in invalidated_clocks
                and _bulk_cached_proof(cached_proof, activity, profile, required,
                                       object_revisions, all_keys, context_key)):
            already_complete += 1
            continue
        has_prior = activity["id"] in processed_sources or any(
            key.startswith(prefix + "/coach-input/v1/") for key in all_keys)
        if has_prior:
            if reuse_inspections + len(pending) >= max_activities:
                deferred_legacy += 1
                blocked.append({"activity_id": activity["id"], "reason": "historical_analysis_unverified"})
                continue
            reuse_inspections += 1
        if legacy_clock and has_prior:
            resolved = _resolve_legacy_calendar(store, activity, clocks[activity["id"]]["utc"], all_keys)
            if resolved:
                day, fit_key, _, date_source = resolved
                activity = {**activity, "date": day}
                prefix = fit_key.rsplit("/", 1)[0]
                endurance_key = fit_key.replace("activity.v1.json", "activity.endurance.v1.json")
                tcx_key = fit_key.replace("activity.v1.json", "activity.tcx")
                profile = select_profile(profiles, day)
                if not profile or any(key not in all_keys for key in (fit_key, endurance_key, tcx_key)):
                    deferred_legacy += 1
                    blocked.append({"activity_id": activity["id"], "reason": "historical_analysis_unverified"})
                    continue
                context_key = _activity_context_key(all_keys, activity, context_index=context_index)
                clock_revisions[activity["id"]] = {
                    "date": day, "source": date_source, "summary_utc": clocks[activity["id"]]["utc"],
                    "source_key": fit_key, "source_revision": object_revisions.get(fit_key),
                    "candidate_revisions": {
                        key: object_revisions.get(key) for year in activity_calendar_years(clocks[activity["id"]]["utc"][:10])
                        if (key := f"activities/{year}/{activity['id']}/activity.v1.json") in all_keys
                    },
                }
                if date_source == "garmin_local":
                    clock_revisions[activity["id"]]["calendar_contract_version"] = 1
                legacy_clock = False
                invalidated_clocks.discard(activity["id"])
        signature = _source_signature(
            activity,
            profile or {},
            context_key=context_key,
            manifest_revision=manifest_revisions.get(activity["id"]),
        )

        required = [fit_key, endurance_key, tcx_key]
        pointer_key = f"{prefix}/coach-input/v1/latest-ready.json"
        has_history = (activity["id"] in processed_sources or pointer_key in all_keys
                       or any(key.startswith(prefix + "/coach-input/v1/canonical/") for key in all_keys))
        if has_history:
            if legacy_clock or activity["id"] in invalidated_clocks:
                outcome, proof = "unknown", None
            else:
                outcome, proof = _bulk_selection_proof(
                    store, activity, profile, required, object_revisions, all_keys, context_key,
                    selection_revisions.get(activity["id"]), profiles,
                )
            if outcome == "conflict":
                raise ValueError("Coach analysis identity conflict")
            if outcome == "repair":
                outcome = "changed"
            if outcome == "retained" and activity["id"] in processed_sources:
                previous_signature = processed_sources[activity["id"]]
                # Only after the complete retained proof above: an explicit
                # same-input targeted upgrade can leave an older bulk checkpoint.
                # Reconstruct each known engine's original signature, not its
                # analysis content or the new builder output.
                supported_signatures = {
                    _source_signature(
                        activity, profile, context_key=context_key,
                        manifest_revision=manifest_revisions.get(activity["id"]),
                        analyzer_version=engine,
                    ) for engine in SUPPORTED_ANALYZER_VERSIONS
                }
                if previous_signature not in supported_signatures:
                    outcome = "changed"
            if outcome == "retained":
                reuse_proofs[activity["id"]] = proof
                if proof["selection_kind"] == "unmarked":
                    legacy_selection_revisions[activity["id"]] = proof["pointer_revision"]
                    selection_revisions.pop(activity["id"], None)
                    selection_adoptions += 1
                else:
                    selection_revisions[activity["id"]] = proof["pointer_revision"]
                processed_sources[activity["id"]] = _source_signature(
                    activity, profile, context_key=context_key,
                    manifest_revision=manifest_revisions.get(activity["id"]),
                    analyzer_version=proof["analyzer_version"],
                )
                already_complete += 1
                continue
            if outcome == "unknown":
                deferred_legacy += 1
                reuse_proofs.pop(activity["id"], None)
                blocked.append({"activity_id": activity["id"], "reason": "historical_analysis_unverified"})
                continue
            # A verified change consumes this activity's generation slot, not a
            # second inspection slot; retained/unknown inspections remain charged.
            reuse_inspections -= 1
        pending.append(
            {
                "activity": activity,
                "profile": profile,
                "prefix": prefix,
                "fit_key": fit_key,
                "endurance_key": endurance_key,
                "tcx_key": tcx_key,
                "context_key": context_key,
                "source_signature": signature,
                "inspect_selection": processed_sources.get(activity["id"]) == signature,
                "resolve_clock": legacy_clock,
            }
        )

    if priority_activity_ids:
        pending.sort(key=lambda item: item["activity"]["id"] not in priority_activity_ids)

    completed = []
    skipped = []
    context_source_cache = {}
    inspected = selection_adoptions
    clock_blocked = 0
    # One canonical attempt + one current-selection attempt per activity, and
    # one final plan PUT. Do not spend that checkpoint reservation on analysis.
    capacity = getattr(store, "remaining_write_attempts", None)
    available = max(0, max_activities - reuse_inspections)
    batch_limit = min(available, max(0, (capacity - 1) // 2)) if capacity is not None else available
    for item in pending[:batch_limit]:
        activity = item["activity"]
        decoded_fit = None
        if item["resolve_clock"]:
            resolved = _resolve_legacy_calendar(store, activity, clocks[activity["id"]]["utc"], all_keys)
            if not resolved:
                blocked.append({"activity_id": activity["id"], "reason": "activity_date_unknown"})
                processed_sources.pop(activity["id"], None)
                clock_blocked += 1
                continue
            day, fit_key, decoded_fit, date_source = resolved
            activity = {**activity, "date": day}
            item.update(activity=activity, prefix=fit_key.rsplit("/", 1)[0], fit_key=fit_key,
                        endurance_key=fit_key.replace("activity.v1.json", "activity.endurance.v1.json"),
                        tcx_key=fit_key.replace("activity.v1.json", "activity.tcx"))
            missing = [item[field] for field in ("fit_key", "endurance_key", "tcx_key") if item[field] not in all_keys]
            if missing:
                blocked.append({"activity_id": activity["id"], "reason": "missing_artifacts", "keys": missing})
                processed_sources.pop(activity["id"], None)
                clock_blocked += 1
                continue
            item["profile"] = select_profile(profiles, activity["date"])
            if not item["profile"]:
                blocked.append({"activity_id": activity["id"], "reason": "no_effective_profile"})
                processed_sources.pop(activity["id"], None)
                clock_blocked += 1
                continue
            item["context_key"] = _activity_context_key(all_keys, activity, context_index=context_index)
            item["source_signature"] = _source_signature(
                activity, item["profile"], context_key=item["context_key"],
                manifest_revision=manifest_revisions.get(activity["id"]),
            )
            clock_revisions[activity["id"]] = {
                "date": activity["date"], "source": date_source,
                "summary_utc": clocks[activity["id"]]["utc"], "source_key": item["fit_key"],
                "source_revision": object_revisions.get(item["fit_key"]),
                "candidate_revisions": {
                    key: object_revisions.get(key)
                    for year in activity_calendar_years(clocks[activity["id"]]["utc"][:10])
                    if (key := f"activities/{year}/{activity['id']}/activity.v1.json") in all_keys
                },
            }
            if date_source == "garmin_local":
                clock_revisions[activity["id"]]["calendar_contract_version"] = 1
        if item["inspect_selection"]:
            pointer_key = f"{item['prefix']}/coach-input/v1/latest-ready.json"
            if pointer_key in all_keys:
                pointer = _json(store, pointer_key)
                if _legacy_selection_matches(store, item, pointer, object_revisions, all_keys):
                    legacy_selection_revisions[activity["id"]] = object_revisions.get(pointer_key)
                    inspected += 1
                    continue
        endurance = _json(store, item["endurance_key"])
        if endurance.get("available") is False:
            processed_sources[activity["id"]] = item["source_signature"]
            skipped.append({
                "activity_id": activity["id"],
                "reason": str(endurance.get("reason") or "endurance_data_unavailable"),
            })
            continue
        decoded_fit = decoded_fit if decoded_fit is not None else _json(store, item["fit_key"])
        tcx = store.get(item["tcx_key"])
        context = _json(store, item["context_key"]) if item["context_key"] else None
        stored_context, context_sources = assemble_stored_context(
            store, activity, decoded_fit, endurance, cache=context_source_cache,
        )
        coach = build_coach_input(
            activity=activity,
            decoded_fit=decoded_fit,
            endurance=endurance,
            profile=item["profile"],
            tcx_sha256=sha256(tcx),
            context=context,
            stored_context_snapshot=stored_context,
        )
        _identify_coach_input(coach)
        key = f"{item['prefix']}/coach-input/v1/canonical/{coach['analysis_id']}.json"
        existed = key in all_keys
        _publish_coach_input(store, key, coach, all_keys)
        canonical_revision = object_revisions.get(key) if existed else getattr(store, "last_write_revision", None)
        required = [item["fit_key"], item["endurance_key"], item["tcx_key"]]
        selection_revision = _publish_current_analysis(
            store, prefix=item["prefix"], key=key, profile=item["profile"],
            coach=coach, revisions=object_revisions, keys=all_keys,
            context_key=item["context_key"],
            input_signature=_single_input_signature(
                activity, item["profile"], required, object_revisions, item["context_key"],
                analyzer_version=coach["analyzer_version"], input_source_revisions=context_sources,
            ),
            input_source_revisions=context_sources,
        )
        reuse_proofs[activity["id"]] = {
            "analyzer_version": coach["analyzer_version"],
            "input_signature": _single_input_signature(
                activity, item["profile"], required, object_revisions, item["context_key"],
                analyzer_version=coach["analyzer_version"], input_source_revisions=context_sources,
            ),
            "analysis_key": key, "canonical_revision": canonical_revision,
            "pointer_revision": selection_revision,
            **({"input_source_revisions": context_sources} if context_sources is not None else {}),
        }
        selection_revisions[activity["id"]] = selection_revision
        legacy_selection_revisions.pop(activity["id"], None)
        processed_sources[activity["id"]] = _source_signature(
            activity, item["profile"], context_key=item["context_key"],
            manifest_revision=manifest_revisions.get(activity["id"]),
            analyzer_version=coach["analyzer_version"],
        )
        completed.append(
            {
                "activity_id": coach["activity_id"],
                "analysis_id": coach["analysis_id"],
                "key": key,
            }
        )

    # Missing source artifacts remain visible but do not keep this R2-only job
    # spinning. Pending work is now derived from per-activity source revisions,
    # so later runs do not reread artifacts that were already analyzed.
    processed_this_run = len(completed) + len(skipped) + inspected + clock_blocked
    # Selection adoptions were never pending generation items. They consume the
    # inspection allowance, but cannot complete unrelated pending activities.
    pending_processed = processed_this_run - selection_adoptions
    remaining = max(0, len(pending) - pending_processed)
    status = (
        "complete_with_blocked"
        if remaining == 0 and blocked
        else "complete"
        if remaining == 0
        else "active"
    )
    if not profiles:
        status = "waiting_for_profile"
    now = datetime.now(timezone.utc).isoformat()
    plan = {
        "schema_version": 1,
        "status": status,
        "updated_at": now,
        "source_fingerprint": source_fingerprint,
        "bulk_reuse_version": BULK_REUSE_VERSION,
        "bulk_reuse_proofs": reuse_proofs,
        "deferred_legacy_activity_count": deferred_legacy,
        "processed_sources": processed_sources,
        "selection_revisions": selection_revisions,
        "legacy_selection_revisions": legacy_selection_revisions,
        "activity_clock_revisions": clock_revisions,
        "selection_inspections_this_run": inspected,
        "processed_source_count": len(processed_sources),
        "running_activities": len(activities),
        "already_current_activities": already_complete,
        "attempted_this_run": processed_this_run,
        "generated_this_run": len(completed),
        "completed_this_run": completed,
        "skipped_this_run": skipped,
        "remaining_activities": remaining,
        "blocked_activity_count": len(blocked),
        "blocked_activities": blocked[:100],
        "profile_count": len(profiles),
    }
    if status in {"complete", "complete_with_blocked"}:
        plan["completed_at"] = now
    # An engine-only transition/unknown legacy is not a new historical write job.
    # Persist verified reuse metadata with the next real bounded checkpoint only.
    if not previous or pending or completed or skipped or inspected or clock_blocked:
        store.put(PLAN_KEY, json.dumps(plan, separators=(",", ":")).encode(), "application/json")
    log_report("coach_backfill", plan)
    return plan


@safe_cli
def main():
    parser = SafeArgumentParser(description="Generate coach inputs from existing R2 artifacts.")
    parser.add_argument("--max-activities", type=int, default=50)
    args = parser.parse_args()
    try:
        run(max_activities=args.max_activities)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
