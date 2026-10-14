"""Bounded, resumable coach-input generation using R2 data only."""

from __future__ import annotations

import csv
import gzip
import io
import json
from datetime import datetime, timezone
from typing import Any

from .coach import ANALYZER_VERSION, build_coach_input, select_profile
from .granular import gzip_json, sha256
from .measurements import measured_component
from .output_privacy import SafeArgumentParser, log_report, safe_cli
from .r2_store import R2Store
from .schema import activity_calendar_years, activity_local_date, to_utc

PLAN_KEY = "backfill/coach-input/v1/plan.json"
PROFILE_PREFIX = "coach/profiles/v1/"
MAX_ACTIVITIES_PER_RUN = 50


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


def _profiles(store) -> list[dict[str, Any]]:
    profiles = []
    for key in sorted(store.list_keys(PROFILE_PREFIX)):
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


def _activity_context_key(keys: set[str], activity: dict[str, Any]) -> str | None:
    prefixes = [f"activities/{year}/{activity['id']}/context/v1/"
                for year in activity_calendar_years(activity["date"])]
    candidates = [key for key in keys if any(key.startswith(prefix) and "/" not in key[len(prefix):]
                                           for prefix in prefixes) and key.endswith(".json")]
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
) -> str:
    """Identify the inputs that decide whether one coach analysis is stale."""
    value = {
        "analyzer_version": ANALYZER_VERSION,
        "activity": activity,
        "profile": profile,
        "context_key": context_key,
        "manifest_revision": manifest_revision,
    }
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _single_input_signature(
    activity: dict[str, Any], profile: dict[str, Any],
    required: list[str], revisions: dict[str, str], context_key: str | None,
) -> str | None:
    """Reuse only when every selected input has a revision, including context."""
    selected = [*required, *([context_key] if context_key else [])]
    if any(not revisions.get(key) for key in selected):
        return None
    metadata = ({**activity, "moving_seconds": _float(activity["moving_seconds"])}
                if "moving_seconds" in activity else activity)
    return sha256(json.dumps({
        "signature_version": 2,
        "analyzer_version": ANALYZER_VERSION,
        "activity": metadata,
        "profile": profile,
        "revisions": {key: revisions[key] for key in selected},
    }, sort_keys=True, separators=(",", ":")).encode())


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
    }
    signature = input_signature
    if signature is not None:
        pointer["input_signature"] = signature
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
    # Fingerprint the caller's metadata before an R2-only repair restores a known
    # moving time. Repeated calls with the same metadata reuse that successful result.
    signature = _single_input_signature(activity, profile, required, revisions, context_key)
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
    endurance = _json(store, required[1])
    if endurance.get("available") is False:
        return {"processed_sources": {}, "blocked_activities": [
            {"activity_id": activity_id, "reason": "endurance_data_unavailable"},
        ]}
    decoded = _json(store, required[0])
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
    )
    _identify_coach_input(coach)
    key = f"{prefix}/coach-input/v1/canonical/{coach['analysis_id']}.json"
    _publish_coach_input(store, key, coach, keys)
    _publish_current_analysis(
        store, prefix=prefix, key=key, profile=profile, coach=coach,
        revisions=revisions, keys=keys, previous_pointer=previous_pointer,
        input_signature=signature,
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
    profiles = _profiles(store)
    clocks: dict[str, dict[str, Any]] = {}
    activities = _activities(summary_bytes, clocks=clocks)
    if hasattr(store, "list_object_revisions"):
        object_revisions = store.list_object_revisions("activities/")
        all_keys = set(object_revisions)
    else:
        object_revisions = {}
        all_keys = store.list_keys("activities/")
    running_ids = {activity["id"] for activity in activities}
    manifest_revisions = _manifest_revisions(
        store, all_keys, object_revisions, running_ids
    )
    fingerprint_parts = [
        f"analyzer:{ANALYZER_VERSION}",
        sha256(summary_bytes),
        *sorted(store.list_keys(PROFILE_PREFIX)),
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
        if (selections_current and len(clock_revisions) == len(saved_clocks)
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

    pending = []
    blocked = []
    already_complete = 0
    for activity in activities:
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
        context_key = _activity_context_key(all_keys, activity)
        signature = _source_signature(
            activity,
            profile or {},
            context_key=context_key,
            manifest_revision=manifest_revisions.get(activity["id"]),
        )
        selection_current = _selection_current(
            activity, object_revisions, all_keys, selection_revisions, legacy_selection_revisions,
        )
        if (processed_sources.get(activity["id"]) == signature and selection_current
                and activity["id"] not in invalidated_clocks):
            already_complete += 1
            continue
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
    inspected = 0
    clock_blocked = 0
    # One canonical attempt + one current-selection attempt per activity, and
    # one final plan PUT. Do not spend that checkpoint reservation on analysis.
    capacity = getattr(store, "remaining_write_attempts", None)
    batch_limit = min(max_activities, max(0, (capacity - 1) // 2)) if capacity is not None else max_activities
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
            item["context_key"] = _activity_context_key(all_keys, activity)
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
        coach = build_coach_input(
            activity=activity,
            decoded_fit=decoded_fit,
            endurance=endurance,
            profile=item["profile"],
            tcx_sha256=sha256(tcx),
            context=context,
        )
        _identify_coach_input(coach)
        key = f"{item['prefix']}/coach-input/v1/canonical/{coach['analysis_id']}.json"
        _publish_coach_input(store, key, coach, all_keys)
        required = [item["fit_key"], item["endurance_key"], item["tcx_key"]]
        selection_revision = _publish_current_analysis(
            store, prefix=item["prefix"], key=key, profile=item["profile"],
            coach=coach, revisions=object_revisions, keys=all_keys,
            input_signature=_single_input_signature(
                activity, item["profile"], required, object_revisions, item["context_key"],
            ),
        )
        selection_revisions[activity["id"]] = selection_revision
        legacy_selection_revisions.pop(activity["id"], None)
        processed_sources[activity["id"]] = item["source_signature"]
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
    remaining = max(0, len(pending) - processed_this_run)
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
