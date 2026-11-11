"""Explicit rolling declarations stay immutable through the bulk public boundary."""
import json

import pytest
from test_reference_bulk_core import file_ref, make_scope

from pipeline.history_state import Job, Stop, encoded, read
from pipeline.reference_bulk import load_bulk_workset, prepare_bulk


def declared_scope(tmp_path, windows):
    job = Job.create(tmp_path / "job", "2026-10-01", "2026-10-01", ["hrv"], "legacy", "synthetic")
    reference = make_scope(tmp_path, job, "2026-11-08", "2026-11-08")
    scope = json.loads(read(reference["path"]))
    scope["windows"] = windows
    path = tmp_path / "explicit-scope.json"
    path.write_bytes(encoded(scope))
    return job, file_ref(path)


def test_explicit_seven_and_year_scope_preserves_only_declared_targets(tmp_path):
    job, reference = declared_scope(tmp_path, [7, 365])
    folder = tmp_path / "workset"
    status = prepare_bulk(job, job.plan_sha, reference, folder)
    workset, scope = load_bulk_workset(job, job.plan_sha, reference, folder)
    assert status["target_count"] == 4
    assert workset.binding["windows"] == scope["windows"] == [7, 365]
    assert [(row["window_days"], row["args"]["start_date"]) for row in workset.targets] == [
        (7, "2026-11-02"), (7, "2026-11-02"),
        (365, "2025-11-09"), (365, "2025-11-09")]
    assert min(workset.wake_dates) == "2025-11-09"
    assert len(workset.wake_dates) == 365
    assert status["source_calls"] == status["product_puts"] == 0
    value = json.loads(read(reference["path"]))
    value["windows"] = [7]
    changed = tmp_path / "changed-scope.json"
    changed.write_bytes(encoded(value))
    with pytest.raises(Stop):
        load_bulk_workset(job, job.plan_sha, file_ref(changed), folder)


def test_explicit_ten_group_canary_stays_within_existing_reader_cap():
    from datetime import date, timedelta

    from pipeline.night_products import normalize_args, storage_key
    from pipeline.reference_verification_policy import select_canaries

    targets = []
    for offset in range(406):
        endpoint = date(2025, 9, 29) + timedelta(days=offset)
        for width in [7, 28, 30, 90, 365]:
            for channel in ["heart_rate", "hrv"]:
                args = normalize_args(period="custom", start_date=(endpoint - timedelta(days=width - 1)).isoformat(),
                                      end_date=endpoint.isoformat(), channel=channel)
                targets.append({"key": storage_key(args), "args": args})
    selection = select_canaries(targets, "Europe/Oslo", windows=[7, 28, 30, 90, 365])
    assert selection == select_canaries(list(reversed(targets)), "Europe/Oslo", windows=[7, 28, 30, 90, 365])
    assert selection["algorithm"] == "DECLARED_GROUP_ENDPOINTS_CHANNEL_DST_V2"
    assert selection["windows"] == [7, 28, 30, 90, 365]
    assert len(selection["groups"]) == 10 and selection["missing_groups"] == []
    keys = set(selection["selected_keys"])
    assert len(keys) <= 34
    assert all(row["first"] in keys and row["middle"] in keys and row["last"] in keys
               for row in selection["groups"])
    assert all(row[direction] in keys for row in selection["dst"] for direction in ["forward", "backward"])
    assert all(row[field] is None or row[field] in keys for row in selection["groups"]
               for field in ["month_boundary", "year_boundary"])


@pytest.mark.parametrize("windows", [None, [], [365, 7], [7, 7], [7.0], [True], [8], "7"])
def test_malformed_explicit_declaration_cannot_silently_widen_scope(tmp_path, windows):
    job, reference = declared_scope(tmp_path, windows)
    folder = tmp_path / "workset"
    with pytest.raises(Stop):
        prepare_bulk(job, job.plan_sha, reference, folder)
    assert not folder.exists()


@pytest.mark.parametrize("windows,last,count", [([7], "2025-10-07", 14), ([365], "2026-09-30", 730)])
def test_daily_delivery_carries_declared_windows_into_affected_provenance(tmp_path, windows, last, count):
    from test_reference_bulk_core import daily_policy_fixture

    from pipeline.reference_bulk import create_affected_scope, create_daily_endpoint_scope

    job, delivery, policy_ref, _, _ = daily_policy_fixture(tmp_path, ["2025-10-01"])
    path = tmp_path / "explicit-policy.json"
    policy = json.loads(read(policy_ref["path"]))
    path.write_bytes(encoded({**policy, "windows": windows}))
    universe_ref = create_daily_endpoint_scope(job, job.plan_sha, delivery, file_ref(path),
                                               output=tmp_path / "universe.json")
    universe = json.loads(read(universe_ref["path"]))
    assert universe["windows"] == windows
    assert universe["endpoints"][0] == "2025-10-01" and universe["endpoints"][-1] == last
    affected_ref = create_affected_scope(job, job.plan_sha, delivery, universe_ref,
        today=policy["today"], timezone=policy["timezone"], deadline_utc=policy["deadline_utc"],
        output=tmp_path / "affected.json")
    affected = json.loads(read(affected_ref["path"]))
    assert affected["windows"] == windows
    assert {row["window_days"] for row in affected["targets"]} == set(windows)
    assert affected["source_range_proofs"][0]["ranges"] == [
        {"window_days": windows[0], "first": "2025-10-01", "last": last}]
    folder = tmp_path / "workset"
    assert prepare_bulk(job, job.plan_sha, affected_ref, folder)["target_count"] == count
    assert load_bulk_workset(job, job.plan_sha, affected_ref, folder)[0].binding["windows"] == windows


@pytest.mark.parametrize("windows,target_count,independent", [([7], 2, 4), ([7, 365], 4, 8)])
def test_new_policy_binds_selected_workset_without_changing_full_sdk_accounting(
        tmp_path, windows, target_count, independent):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.reference_bulk import build_bulk, collect_bulk, inventory_bulk
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan
    from pipeline.reference_verification_policy import create_canary_policy, validate_policy

    job, scope = declared_scope(tmp_path, windows)
    folder, carried = tmp_path / "workset", {"independent_lifetime_gets": 680}
    prepare_bulk(job, job.plan_sha, scope, folder)
    workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
    server, _ = seeded("2026-11-06", 3)
    inventory_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=inventory_plan(workset, carried),
                   carried=carried, store_factory=lambda: sdk_store(server))
    workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
    collection = collect_bulk(job, job.plan_sha, scope, folder,
        resource_plan_ref=collection_plan(workset, carried=carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    assert collection["status"] == "sources_complete"
    result = build_bulk(job, job.plan_sha, scope, folder, carried=carried)
    assert result["status"] == "build_complete" and result["product_count"] == target_count
    assert not server.puts
    workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
    local = workset.reader_store()
    with pytest.raises(Stop):
        local.list_revisions_bounded("health/sleep/v1/1970/", max_keys=800)
    with pytest.raises(Stop):
        local.list_revisions_bounded("health/sleep/v1/2026/", max_keys=801)
    if 365 in windows:
        assert set(local.list_revisions_bounded("health/sleep/v1/2026/", max_keys=800)) == {
            "health/sleep/v1/2026/11/2026-11-06.json",
            "health/sleep/v1/2026/11/2026-11-07.json",
            "health/sleep/v1/2026/11/2026-11-08.json"}
    else:
        with pytest.raises(Stop):
            local.list_revisions_bounded("health/sleep/v1/2026/", max_keys=800)
    reference = create_canary_policy(job, job.plan_sha, scope, folder,
                                    new_invocation="explicit-local", output=tmp_path / "canary.json")
    policy = json.loads(read(reference["path"]))
    assert policy["windows"] == windows
    binding = {"operation": "rolling_bulk", "invocation": "explicit-local", "bulkVerificationPolicy": reference,
               "bulkScope": scope, "plan": {"sha256": job.plan_sha},
               "targetSHA256": workset.binding["storage_target_sha256"]}
    check = validate_policy(workset, binding, workset.targets)
    assert check["get_counts"] == {"sdk": target_count, "connected_reader": target_count,
                                   "independent": independent, "reader_observations": target_count}
    assert check["coverage"]["reader_unchecked_targets"] == 0
    all_reader = validate_policy(workset, {key: value for key, value in binding.items()
                                         if key != "bulkVerificationPolicy"}, workset.targets)
    assert set(all_reader["selected_keys"]) == {row["key"] for row in workset.targets}
    for changed in [None, [7], [28, 30, 90], [7, 365, 365]]:
        if changed == windows:
            continue
        altered = {**policy, "windows": changed}
        path = tmp_path / "altered-policy.json"
        path.write_bytes(encoded(altered))
        with pytest.raises(Stop):
            validate_policy(workset, {**binding, "bulkVerificationPolicy": file_ref(path)}, workset.targets)


def test_missing_declaration_keeps_legacy_scope_and_workset_bytes(tmp_path):
    from test_reference_workset import declaration

    job = Job.create(tmp_path / "job", "2026-10-01", "2026-10-01", ["hrv"], "legacy", "synthetic")
    reference = make_scope(tmp_path, job, "2026-11-08", "2026-11-08")
    original = read(reference["path"])
    folder = tmp_path / "workset"
    assert prepare_bulk(job, job.plan_sha, reference, folder)["target_count"] == 6
    workset, _ = load_bulk_workset(job, job.plan_sha, reference, folder)
    binding, targets = declaration(["2026-11-08"])
    binding.update(job_plan_sha256=job.plan_sha, storage_target_sha256=workset.binding["storage_target_sha256"],
                   timezone="Europe/Oslo")
    assert read(folder / "WORKSET.json") == encoded(
        {"schema": 1, "kind": "reference-workset", "binding": binding, "targets": targets})
    assert read(reference["path"]) == original


def test_five_window_geometry_is_finite_without_increasing_build_or_publication_limits(tmp_path):
    from pipeline.reference_bulk import build_bulk, publish_bulk

    job, reference = declared_scope(tmp_path, [7, 28, 30, 90, 365])
    value = json.loads(read(reference["path"]))
    value["lower_endpoint"] = "2025-09-29"
    path = tmp_path / "full-shape.json"
    path.write_bytes(encoded(value))
    folder = tmp_path / "workset"
    prepared = prepare_bulk(job, job.plan_sha, file_ref(path), folder)
    assert prepared["endpoint_count"] == 406 and prepared["target_count"] == 4060
    with pytest.raises(Stop, match="build_bound"):
        build_bulk(job, job.plan_sha, file_ref(path), folder, carried={}, max_targets=4060)
    with pytest.raises(Stop, match="publication_bound"):
        publish_bulk(job, job.plan_sha, file_ref(path), folder,
                     resource_plan_ref=None, carried={}, max_batches=4060)
    path = tmp_path / "overflow.json"
    value["lower_endpoint"] = "2025-09-28"
    path.write_bytes(encoded(value))
    with pytest.raises(Stop, match="scope_bound"):
        prepare_bulk(job, job.plan_sha, file_ref(path), tmp_path / "overflow-workset")


def test_bulk_all_five_publication_matches_legacy_sources_with_real_sdk_readback(tmp_path):
    from functools import partial

    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.history_state import digest
    from pipeline.reference_bulk import build_bulk, collect_bulk, inventory_bulk, publish_bulk
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan, publication_plan

    comparison, common = [], None
    for label, windows, count in [("legacy3", None, 6), ("explicit5", [7, 28, 30, 90, 365], 10)]:
        case = tmp_path / label
        case.mkdir()
        job = Job.create(case / "job", "2026-11-06", "2026-11-08", ["hr", "hrv"], "legacy", "synthetic")
        scope = make_scope(case, job, "2026-11-08", "2026-11-08")
        if windows is not None:
            value = json.loads(read(scope["path"]))
            path = case / "selected.json"
            path.write_bytes(encoded({**value, "windows": windows}))
            scope = file_ref(path)
        folder, carried = case / "workset", {"independent_lifetime_gets": 680}
        prepare_bulk(job, job.plan_sha, scope, folder)
        workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
        server, _ = seeded("2026-11-06", 3)
        factory = partial(sdk_store, server)
        originals = dict(server.objects)
        inventory_bulk(job, job.plan_sha, scope, folder,
            resource_plan_ref=inventory_plan(workset, carried), carried=carried,
            store_factory=factory)
        workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
        plan = collection_plan(workset, carried=carried)
        assert collect_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan,
            carried=carried, store_factory=factory)["status"] == "sources_complete"
        calls = len(server.calls)
        assert collect_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=plan,
            carried=carried, store_factory=factory)["fetched_sources"] == 0
        assert len(server.calls) == calls
        build = build_bulk(job, job.plan_sha, scope, folder, carried=carried)
        assert build["status"] == "build_complete" and build["product_count"] == count
        assert len(server.calls) == calls
        workset, _ = load_bulk_workset(job, job.plan_sha, scope, folder)
        publication = publication_plan(workset, carried=carried)
        result = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=publication,
            carried=carried, store_factory=factory)
        assert result["status"] == "verification_pending"
        assert result["publication_complete"] is True and result["published_products"] == count
        assert result["verified_products"] == 0
        calls = len(server.calls)
        retry = publish_bulk(job, job.plan_sha, scope, folder, resource_plan_ref=publication,
            carried=carried, store_factory=factory)
        assert retry == result and len(server.calls) == calls
        stages = []
        for terminal in [*folder.glob("stages/*/TERMINAL.json"), *folder.glob("publications/*/TERMINAL.json")]:
            value = json.loads(read(terminal))
            assert value["status"] == "complete" and value["componentCleanupConfirmed"] is True
            assert value.get("ownedSDKClosed", True) is True
            stages.append({"phase": value["phase"], "counts": value["counts"],
                           "metered_write_bytes": value["body_bytes"]})
        physical = {"get": 0, "list": 0, "put": 0}
        for method, _, query in server.calls:
            if method in ["GET", "HEAD"]:
                physical["list" if "list-type" in query else "get"] += 1
            elif method == "PUT":
                physical["put"] += 1
        assert physical == {name: sum(row["counts"][name] for row in stages) for name in physical}
        reader = sdk_store(server)
        snapshots = {}
        before_reads = len(server.calls)
        try:
            for target in result["generated_targets"]:
                snapshot = reader.get_snapshot(target["key"], max_bytes=262144)
                assert snapshot.revision == target["revision"]
                assert digest(snapshot.data) == target["sha256"]
                assert json.loads(snapshot.data)["args"] == target["args"]
                snapshots[target["key"]] = snapshot.data
        finally:
            reader.client.close()
        assert len(server.calls) - before_reads == count
        assert all(server.objects[key] == value for key, value in originals.items())
        legacy_products = {target["key"]: snapshots[target["key"]] for target in workset.targets
                           if target["window_days"] in [28, 30, 90]}
        if common is None:
            common = legacy_products
        else:
            assert legacy_products == common
        comparison.append({"case": label, "targets": count, "stages": stages,
            "physical_before_independent_readback": physical,
            "separate_sdk_product_gets": count, "publication_status": result["status"],
            "verified_products": result["verified_products"],
            "source_identity_sha256": digest(encoded({key: [digest(body), revision]
                for key, (body, revision) in originals.items()})),
            "retained_source_objects": len(originals), "cache_resume_http": 0, "publication_retry_http": 0})
        (tmp_path / "comparison.json").write_bytes(encoded(comparison))
    assert comparison[0]["source_identity_sha256"] == comparison[1]["source_identity_sha256"]
