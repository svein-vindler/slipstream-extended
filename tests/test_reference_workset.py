"""Durable finite source collection and prepared-product barriers with actual SDK reads."""
import json
from collections import Counter
from datetime import date, timedelta

import pytest
from botocore.exceptions import ClientError
from test_rolling_reference_sources import sdk_store, seeded

from pipeline.history_state import Stop, digest, encoded
from pipeline.native_night import NativeNightReader
from pipeline.night_baseline_refresh import _IO, prepare_reference
from pipeline.night_products import canonical, normalize_args, storage_key
from pipeline.reference_workset import ReferenceWorkset


def ref(path):
    body = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}


def declaration(endpoints):
    binding = {"job_plan_sha256": "a" * 64, "storage_target_sha256": "b" * 64,
        "today": max(endpoints), "timezone": "UTC", "endpoints": sorted(endpoints)}
    targets = []
    for endpoint in sorted(endpoints):
        for width in (28, 30, 90):
            for channel in ("heart_rate", "hrv"):
                args = normalize_args(start_date=(date.fromisoformat(endpoint) - timedelta(days=width - 1)).isoformat(),
                    end_date=endpoint, period="custom" if width == 30 else str(width), channel=channel)
                key = storage_key(args)
                targets.append({"target_id": key, "key": key, "args": args,
                    "endpoint": endpoint, "window_days": width, "channel": channel})
    return binding, targets


def inventory(workset, store, folder):
    for index, request in enumerate(workset.inventory_requests()):
        rows = store.list_revisions_bounded(request["prefix"], max_keys=request["max_keys"])
        proof = folder / f"inventory-{index}.json"
        proof.write_bytes(encoded({"prefix": request["prefix"], "rows": rows, "native_wire_verified": True}))
        workset.record_inventory(request["prefix"], rows, ref(proof))


def test_collected_versions_survive_reopen_and_native_build_without_remote_refetch(tmp_path):
    server, _ = seeded("2026-10-23", 3)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    try:
        inventory(workset, store, tmp_path)
        collected = workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    finally:
        store.close()
    # Three Sleep, three HRV and four UTC signal-date objects (the preceding day too).
    assert collected["fetched"] == 10 and collected["remaining"] == 0
    workset.seal_sources()
    reopened = ReferenceWorkset.open(workset.folder, binding)
    calls = len(server.calls)
    reader = NativeNightReader(reopened.reader_store(), timezone="UTC")
    days = ["2026-10-23", "2026-10-24", "2026-10-25"]
    reader.prime_rolling_sources(days)
    observed = [reader.night(day) for day in days]
    assert all(night["status"] == "available" for night in observed)
    assert observed[-1]["channels"]["heart_rate"]["points"][0]["value"] == 55
    assert len(server.calls) == calls
    assert reopened.fetch_missing(None, max_items=400, admit_fetch=lambda row: True)["fetched"] == 0
    assert not server.puts


def test_verified_existing_local_sources_avoid_all_raw_body_gets(tmp_path):
    server, source = seeded("2026-10-23", 3)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    provenance = tmp_path / "verified-local.json"
    provenance.write_bytes(encoded({"kind": "EXTERNAL_VERIFIED_LOCAL_SOURCE_SNAPSHOTS",
        "job_plan_sha256": binding["job_plan_sha256"], "storage_target_sha256": binding["storage_target_sha256"],
        "objects": [{"key": key, "revision": snapshot.revision, "sha256": digest(snapshot.data)}
                    for key, snapshot in source.objects.items()]}))
    try:
        inventory(workset, store, tmp_path)
        seeded_count = workset.seed_verified_local(source.objects, ref(provenance))
        before = len(server.calls)
        result = workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    finally:
        store.close()
    assert seeded_count == 10 and result["fetched"] == result["remaining"] == 0
    assert len(server.calls) == before
    workset.seal_sources()
    reopened = ReferenceWorkset.open(workset.folder, binding)
    assert reopened.inventory_summary()["source_byte_bound"] > 0
    provenance.write_bytes(b"changed proof")
    with pytest.raises(Stop):
        ReferenceWorkset.open(workset.folder, binding)


def test_all_declared_products_must_be_staged_before_any_publish_access(tmp_path):
    server, _ = seeded("2026-10-23", 3)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    try:
        inventory(workset, store, tmp_path)
        workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    finally:
        store.close()
    workset.seal_sources()
    reader = NativeNightReader(workset.reader_store(), timezone="UTC")
    days = [(date(2026, 7, 28) + timedelta(days=index)).isoformat() for index in range(90)]
    reader.prime_rolling_sources(days)
    nights = {day: reader.night(day) for day in days}
    first_artifact = None
    before = len(server.calls)
    for index, target in enumerate(targets):
        selected = [day for day in days if target["args"]["start_date"] <= day]
        proof = {kind: {key: value for day in selected for key, value in reader.dependencies[day][kind].items()}
                 for kind in ("objects", "lists")}
        artifact = prepare_reference([nights[day] for day in selected], target["args"], proof)
        workset.stage_product(target["target_id"], artifact)
        assert len(workset.pending_targets()) == 5 - index
        first_artifact = first_artifact or artifact
        if index == 0:
            with pytest.raises(Stop):
                workset.seal_build()
            with pytest.raises(Stop):
                workset.product_for(target["key"])
    seal = workset.seal_build()
    reopened = ReferenceWorkset.open(workset.folder, binding)
    assert reopened.assert_sealed() == seal
    assert reopened.product_for(targets[0]["key"]) == first_artifact
    assert first_artifact["prior_inputs"][-1]["channel_status"] == "available"
    assert reopened.build_summary()["product_count"] == 6
    assert reopened.build_summary()["product_bytes"] > 0
    assert len(server.calls) == before and not server.puts


def test_more_than_512_sources_and_16mib_survive_bounded_collection_and_overlap_resume(tmp_path):
    server, _ = seeded("2026-01-01", 171)
    for index, (key, (body, _)) in enumerate(list(server.objects.items())):
        value = json.loads(body)
        value["synthetic_padding"] = "x" * 35000
        server.objects[key] = (canonical(value).encode(), f'"bulk-{index}"')
    endpoints = [(date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(171)]
    binding, targets = declaration(endpoints)
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    first = _IO(sdk_store(server))
    try:
        inventory(workset, first, tmp_path)
        part = workset.fetch_missing(first, max_items=400, admit_fetch=lambda row: True)
    finally:
        first.close()
    assert part == {"fetched": 400, "remaining": 114, "deferred": True}
    workset = ReferenceWorkset.open(workset.folder, binding)
    second = _IO(sdk_store(server))
    try:
        finished = workset.fetch_missing(second, max_items=400, admit_fetch=lambda row: True)
    finally:
        second.close()
    assert finished["fetched"] == 114 and finished["remaining"] == 0
    assert workset.inventory_summary()["source_object_bound"] == 514
    assert workset.inventory_summary()["source_byte_bound"] > 16777216
    workset.seal_sources()
    workset = ReferenceWorkset.open(workset.folder, binding)
    gets = Counter(key for method, key, query in server.calls if method == "GET" and "list-type" not in query)
    assert len(gets) == 514 and set(gets.values()) == {1}
    before = len(server.calls)
    for offset in (80, 81):
        days = endpoints[offset:offset + 90]
        reader = NativeNightReader(workset.reader_store(), timezone="UTC")
        reader.prime_rolling_sources(days)
        assert all(reader.night(day)["status"] == "available" for day in days)
        assert reader.read_bytes <= 16777216
    assert len(server.calls) == before and not server.puts


def test_budget_deferral_before_fetch_is_resumable_but_unknown_send_stays_held(tmp_path):
    server, _ = seeded("2026-10-25", 1)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    try:
        inventory(workset, store, tmp_path)
        before = len(server.calls)
        deferred = workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: False)
        assert deferred == {"fetched": 0, "remaining": 4, "deferred": True}
        assert len(server.calls) == before and workset.status()["hold"] is None
    finally:
        store.close()
    workset = ReferenceWorkset.open(workset.folder, binding)
    selected = workset.required_sources()[0]["key"]
    del server.objects[selected]
    store = _IO(sdk_store(server))
    try:
        with pytest.raises(ClientError):
            workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    finally:
        store.close()
    workset = ReferenceWorkset.open(workset.folder, binding)
    assert workset.status()["hold"]["reason"] == "source_get_unverifiable"
    before = len(server.calls)
    with pytest.raises(Stop):
        workset.fetch_missing(None, max_items=400, admit_fetch=lambda row: True)
    with pytest.raises(Stop):
        workset.seal_sources()
    assert len(server.calls) == before


@pytest.mark.parametrize("changed", ["canonical_revision", "selected_context"])
def test_frozen_sources_hold_on_new_revision_or_context_without_refetch(tmp_path, changed):
    server, _ = seeded("2026-10-25", 1)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    try:
        inventory(workset, store, tmp_path)
        workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
        workset.seal_sources()
        if changed == "canonical_revision":
            key = "health/hrv/2026/10/2026-10-25.json"
            server.objects[key] = (server.objects[key][0], '"corrected"')
            prefix, bound = "health/hrv/2026/10/", 63
        else:
            prefix, bound = "health/night-context/v1/2026/10/", 3101
            server.objects[prefix + "2026-10-25/20261025T123456789-nctx-" + "c" * 24 + ".json"] = (b"{}", '"new-context"')
        rows = store.list_revisions_bounded(prefix, max_keys=bound)
        proof = tmp_path / "new-observation.json"
        proof.write_bytes(encoded(rows))
        before = len(server.calls)
        with pytest.raises(Stop, match="reference_workset_source_changed"):
            workset.record_inventory(prefix, rows, ref(proof))
    finally:
        store.close()
    workset = ReferenceWorkset.open(workset.folder, binding)
    with pytest.raises(Stop):
        workset.reader_store()
    with pytest.raises(Stop):
        workset.fetch_missing(None, max_items=400, admit_fetch=lambda row: True)
    assert len(server.calls) == before and not server.puts


def test_stage_cleanup_receipt_blocks_seals_and_preserves_original_failure(tmp_path):
    server, _ = seeded("2026-10-25", 1)
    binding, targets = declaration(["2026-10-25"])
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets)
    store = _IO(sdk_store(server))
    try:
        inventory(workset, store, tmp_path)
        workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    finally:
        store.close()
    plan = tmp_path / "resource.json"
    plan.write_bytes(encoded({"kind": "finite-reference-bulk-resource-plan",
        "phase": "collection", "workset_sha256": workset.scope_sha256}))
    terminal = tmp_path / "terminal.json"
    terminal.write_bytes(encoded({"schema": 1, "kind": "reference-bulk-stage-terminal",
        "scope_sha256": workset.scope_sha256, "phase": "collection", "resource_plan_ref": ref(plan),
        "status": "hold", "counts": {"get": 4, "list": 17, "put": 0}, "body_bytes": 0,
        "componentCleanupConfirmed": False, "ownedSDKClosed": False,
        "primaryFailureCategory": "original_native_failure", "cleanupFailureCategory": "sdk_cleanup_unknown"}))
    workset.record_step(ref(terminal))
    workset.mark_hold("later_cleanup_failure")
    workset = ReferenceWorkset.open(workset.folder, binding)
    assert workset.status()["hold"]["reason"] == "original_native_failure"
    assert len(workset.status()["holds"]) == 2
    with pytest.raises(Stop):
        workset.seal_sources()
    assert not server.puts


def test_known_absence_is_distinct_from_unverifiable_target_and_all_targets_are_accounted(tmp_path):
    server, _ = seeded("2026-10-25", 0)
    server.objects.clear()
    binding, targets = declaration(["2026-10-25"])
    unknown = ReferenceWorkset.create(tmp_path / "unknown", binding, targets)
    store = _IO(sdk_store(server))
    outcome = tmp_path / "outcome.json"
    outcome.write_bytes(encoded({"scope_sha256": unknown.scope_sha256, "known": "inventory-absence"}))
    try:
        inventory(unknown, store, tmp_path)
        unknown.seal_sources()
        for index, target in enumerate(targets):
            unknown.classify_target(target["target_id"], "sleep_not_stored" if index else "native_unknown",
                                    ref(outcome), known=index != 0)
        with pytest.raises(Stop, match="reference_workset_build_incomplete"):
            unknown.seal_build()
        known = ReferenceWorkset.create(tmp_path / "known", binding, targets)
        for request in known.inventory_requests():
            known.record_inventory(request["prefix"], {}, ref(outcome))
        known.seal_sources()
        for target in targets:
            known.classify_target(target["target_id"], "sleep_not_stored", ref(outcome))
        known.seal_build()
    finally:
        store.close()
    summary = known.build_summary()
    assert summary["product_count"] == summary["product_bytes"] == 0
    assert summary["known_gap_count"] == 6
    assert not server.puts
