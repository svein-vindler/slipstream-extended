"""Explicit bounded, read-only observations for the existing reference publisher."""

from __future__ import annotations

from datetime import timedelta

from .native_night import NativeInputError, NativeNightReader, zone
from .night_baseline_refresh import _IO, BaselineRefreshStop
from .night_products import baseline, calendar_day, canonical, normalize_args, storage_key
from .night_reference_sources import rolling_fence_list_bound
from .rolling_night_references import CHANNELS, window_selection


def _dates(first, last):
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def _estimate(targets, proofs, reads):
    ready = [target for target in targets if target["status"] == "ready"]
    phases = (len(ready) + 1) // 2
    # No prior product/queue/inventory reads are performed by this planner.
    # Existing history and queue rewrites must therefore be priced at their caps.
    per_target_put = 11
    per_target_bytes = 3 * 262144 + 2048 + 12 * 262144
    known_fences = sum(2 * rolling_fence_list_bound(proofs[(target["endpoint"], target["window_days"])],
        **({"window_days": 365} if target["window_days"] == 365 else {}))
                       for target in ready)
    candidate_count = min(len(ready), 2 * min(14, 10485760 // 2097152, 1000 // 200))
    return {
        "basis": "conservative_upper_bounds_prior_products_and_queue_unknown",
        "actual_consumer_admission_required": True,
        "targets_requested": len(targets), "targets_observed_ready": len(ready),
        "requested_refresh_phases_at_most_two_targets": phases,
        "fresh_fence_list_pages_upper_bound": known_fences,
        "fresh_fence_method": "two_new_bounded_revision_LIST_passes_per_target",
        "put_control_history_upper_bound": per_target_put * len(ready),
        "write_bytes_upper_bound": per_target_bytes * len(ready),
        "get_upper_bound": phases * 400,
        "list_upper_bound": phases * 100,
        "class_a_upper_bound": phases * 200,
        "plan_plus_publication_get_upper_bound": reads["get"] + phases * 400,
        "shared_get_ledger_binding_required": True,
        "plan_exceeds_independent_get_pool_if_charged_there": reads["get"] > 256,
        "all_ready_targets_fit_upper_bounds": (phases <= 14 and phases * 200 <= 1000
                                                and reads["get"] + phases * 400 <= 6256
                                                and per_target_bytes * len(ready) <= 10485760),
        "limits": {"phases": 14, "publication_get": 6000, "independent_get": 256,
                   "class_a": 1000, "write_bytes": 10485760,
                   "phase_get": 400, "phase_list": 100, "phase_put": 100,
                   "phase_write_bytes": 2097152},
        "fully_charged_phase_capacity_upper_bound": 5,
        "bounded_first_candidates": [target["storage_key"] for target in ready[:candidate_count]],
        "remainder": [target["storage_key"] for target in ready[candidate_count:]],
        "guaranteed_admitted_targets": 0,
        "deferred_reason": "existing_product_queue_and_shared_ledgers_require_publication_admission",
    }


def plan_references(store, endpoints, *, today, timezone, snapshot_cache=None, windows=None):
    """Observe1..7 explicit wake endpoints; never write, inventory, or register.

    The returned JSON contains provenance and coverage metadata only. The caller's
    bounded verified snapshot mapping is filled for the existing publication path;
    its contents are observations, never a replacement for fresh source fences.
    """
    selected_windows = window_selection(windows)
    maximum = max(selected_windows)
    if (not isinstance(endpoints, (list, tuple)) or not 1 <= len(endpoints) <= 7
            or any(not isinstance(day, str) for day in endpoints)):
        raise ValueError("Provide1 through7 explicit endpoint dates")
    requested = sorted(set(endpoints))
    values = [calendar_day(day) for day in requested]
    calendar_day((values[0] - timedelta(days=maximum - 1)).isoformat())
    current = calendar_day(today)
    if (values[-1] - values[0]).days > 6 or zone(timezone) is None:
        raise ValueError("Endpoint batch requires one seven-day span and IANA timezone")
    cache = snapshot_cache if snapshot_cache is not None else {}
    if (not isinstance(cache, dict) or len(cache) > 512
            or any(not isinstance(getattr(snap, "data", None), bytes) for snap in cache.values())
            or sum(len(snap.data) for snap in cache.values()) > 16777216):
        raise ValueError("Verified source cache exceeds bounded contract")
    initial_cache = len(cache)
    safe = [day for day in requested if day <= current.isoformat()]
    deferred = [{"endpoint": day, "reason": "future_wake_date"}
                for day in requested if day > current.isoformat()]
    result = {"schema_version": 1, "kind": "rolling-night-reference-plan",
              "requested_endpoints": requested, "today": current.isoformat(), "timezone": timezone,
              "windows": list(selected_windows),
              "eligible_endpoints": [], "verified_sleep_sources": [], "targets": [],
              "deferred": deferred, "blocked": [], "status": "observed",
              "observation_only": True, "fresh_publication_fences_required": True}
    nights, dependencies, unknown, sleep_proofs = {}, {}, set(), {}
    io = _IO(store)
    started = io.counts()
    reader = NativeNightReader(io, timezone=timezone, snapshot_cache=cache)
    try:
        union = (_dates(calendar_day(safe[0]) - timedelta(days=maximum - 1), calendar_day(safe[-1]))
                 if safe else [])
        for offset in range(0, len(union), 90):
            batch = union[offset:offset + 90]
            try:
                reader.prime_rolling_sources(batch)
            except (NativeInputError, BaselineRefreshStop):
                unknown.update(union[offset:])
                result["status"] = "blocked"
                break
            for index, day in enumerate(batch):
                try:
                    nights[day] = reader.night(day)
                    dependencies[day] = reader.dependencies[day]
                    if day in safe and nights[day]["status"] in {"available", "partial"}:
                        root = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
                        selected = next((key for key in (root, root + ".gz")
                                         if reader.proof.get(key, {}).get("status") == "present"), None)
                        if selected:
                            binding = reader.proof[selected]
                            sleep_proofs[day] = {"key": selected, "revision": binding["revision"],
                                                 "sha256": binding["sha256"]}
                except NativeInputError:
                    unknown.add(day)
                    if (reader.read_bytes > 16777216 or reader.decoded_bytes > 33554432
                            or reader.input_points > 200000):
                        unknown.update(union[offset + index:])
                        result["status"] = "blocked"
                        break
                except BaselineRefreshStop:
                    unknown.update(union[offset + index:])
                    result["status"] = "blocked"
                    break
            if result["status"] == "blocked":
                break
        result["eligible_endpoints"] = sorted(sleep_proofs)
        result["verified_sleep_sources"] = [sleep_proofs[day] for day in sorted(sleep_proofs)]
        endpoint_proofs = {}
        for endpoint in safe:
            if endpoint not in sleep_proofs:
                result["blocked"].append({"endpoint": endpoint,
                                          "reason": "native_input_unknown" if endpoint in unknown
                                          else "sleep_not_stored"})
                continue
            for width in selected_windows:
                first = calendar_day(endpoint) - timedelta(days=width - 1)
                days = _dates(first, calendar_day(endpoint))
                endpoint_proofs[(endpoint, width)] = {
                    name: {key: value for day in days if day in dependencies
                           for key, value in dependencies[day][name].items()}
                    for name in ("objects", "lists")}
                observed = [nights[day] for day in days if day in nights]
                input_bytes = len(canonical(observed).encode())
                points = sum(len(channel["points"]) for night in observed
                             for channel in night["channels"].values())
                for channel in CHANNELS:
                    args = normalize_args(start_date=first.isoformat(), end_date=endpoint,
                                          period="custom" if width == 30 else str(width), channel=channel)
                    bad = sorted(day for day in days if day in unknown or day in nights and any(
                        nights[day]["channels"][channel]["quality"][field]
                        for field in ("invalid", "ambiguous_time", "conflicting_timestamps")))
                    if input_bytes > 16777216 or points > 200000:
                        bad = days
                    product = baseline(observed, args, input_bytes=input_bytes,
                                       input_points=points)["product"] if not bad else None
                    result["targets"].append({
                        "endpoint": endpoint, "window_days": width, "channel": channel,
                        "args": args, "storage_key": storage_key(args), "expected_nights": width,
                        "contributing_nights": product["contributing_nights"] if product else None,
                        "coverage": product["contributing_nights"] / width if product else None,
                        "coverage_basis": "contributing_nights_over_expected_wake_dates",
                        "gaps": product["excluded"] if product else None,
                        "status": "ready" if product else "blocked_unknown",
                        "unknown_wake_dates": bad,
                        "source_identity": product["source_identity"] if product else None,
                    })
        counts = io.counts()
        operations = {name: counts[name] - started[name] for name in counts}
        result["reads"] = {"operations": operations, "local_snapshot_reuse_lookups": reader.local_snapshot_hits,
                           "source_bytes": reader.read_bytes, "source_points": reader.input_points,
                           "wake_dates_observed": len(nights)}
        result["source_cache"] = {"initial_objects": initial_cache, "objects": len(cache),
                                  "stored_bytes": sum(len(snapshot.data) for snapshot in cache.values()),
                                  "maximum_objects": 512, "maximum_bytes": 16777216,
                                  "freshness_proof": False,
                                  "observed_objects": [
                                      {"key": key, "revision": binding["revision"],
                                       "sha256": binding["sha256"], "size": binding["size"]}
                                      for key, binding in sorted({
                                          key: binding for proof in dependencies.values()
                                          for key, binding in proof["objects"].items()
                                          if binding["status"] == "present"}.items())]}
        result["publication_estimate"] = _estimate(result["targets"], endpoint_proofs, operations)
        if result["blocked"] or any(target["status"] != "ready" for target in result["targets"]):
            result["status"] = "blocked" if not result["targets"] else "partial"
        return result
    finally:
        io.close()
