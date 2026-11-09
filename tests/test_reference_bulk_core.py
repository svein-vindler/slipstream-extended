"""Portable bulk source/cache/build/publish checks using only external synthetic boundaries."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from history_storage_fixture import ACCOUNT, BUCKET

from pipeline.history_backfill import main
from pipeline.history_state import Job, digest, encoded, read


def make_scope(tmp_path, job, lower, upper):
    scope = tmp_path / "scope.json"
    target = digest(("https://" + ACCOUNT + ".r2.cloudflarestorage.com\0" + BUCKET).encode())
    scope.write_bytes(encoded({"schema": 1, "kind": "reference-bulk-scope",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": target,
        "today": "2026-11-08", "timezone": "Europe/Oslo",
        "lower_endpoint": lower, "upper_endpoint": upper,
        "deadline_utc": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "historical_refs": [], "source_delivery_refs": []}))
    return {"path": str(scope.resolve()), "bytes": len(read(scope)), "sha256": digest(read(scope))}


def file_ref(path):
    body = read(path)
    return {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}


def observe_daily_raw_boundary(job, folder, server, *, status, binding=None, actual_native=2, terminal_ref=None):
    """Synthetic Root observer over actual Source leaves, publisher receipts and separate SDK reads."""
    import inspect

    from test_history_baseline_integration import sdk_store

    from pipeline.history_canonical import key as raw_key

    folder.mkdir(parents=True)
    contract = Path(inspect.getfile(raw_key)).resolve()
    if binding is None:
        catalog = folder / "runtime.json"
        catalog.write_bytes(encoded({"files": [file_ref(contract)]}))
        binding = {"job": str(job.folder), "plan": file_ref(job.folder / "plan.json"),
            "accountSHA256": job.plan["account_sha256"],
            "targetSHA256": digest(("https://" + ACCOUNT + ".r2.cloudflarestorage.com\0" + BUCKET).encode()),
            "invocation": "synthetic-raw-observer", "cwd": str(contract.parents[1]),
            "runtimeManifest": file_ref(catalog)}
    binding_path = folder / "binding.json"
    binding_path.write_bytes(encoded(binding))
    binding_sha = digest(encoded(binding))
    events = [frame["event"] for frame in job.events()[0]]
    selection = next(event for event in reversed(events) if event["type"] == "daily_round_started")
    producers, refs, checks, readbacks = [], [file_ref(job.folder / "plan.json"), file_ref(job.folder / "events.jsonl")], [], []
    for event in events:
        if event["type"] == "target" and event.get("folder") and event.get("source_verified"):
            refs.extend(file_ref(Path(event["folder"]) / name) for name in ("INTENT.json", "RECEIPT.json", "RESULT.json"))
    sdk = sdk_store(server)
    try:
        for path in sorted((job.folder / "runs").glob("*/RECEIPT.json")):
            receipt = json.loads(read(path))
            if not receipt.get("destinations") or receipt.get("baseline_refresh", {}).get("execution_totals", {}).get("round") != selection["round"]:
                continue
            producers.append(file_ref(path))
            refs.append(file_ref(path))
            for row in receipt["destinations"]:
                snapshot = sdk.get_snapshot(row["key"], max_bytes=262144)
                body_path = folder / f"sdk-{len(readbacks):03d}.bin"
                body_path.write_bytes(snapshot.data)
                checks.append({"key": row["key"], "revision": snapshot.revision, "sha256": digest(snapshot.data),
                    "contentType": snapshot.content_type, "encoding": snapshot.encoding, "observedReference": False})
                readbacks.append({"keySHA256": digest(row["key"].encode()), "body": file_ref(body_path),
                    "metadataSHA256": digest(encoded({"revision": snapshot.revision,
                        "content_type": snapshot.content_type, "encoding": snapshot.encoding}))})
    finally:
        sdk.client.close()
    if terminal_ref is None:
        terminal_path = folder / "source-native.json"
        terminal_path.write_bytes(encoded({"actualNative": actual_native, "allNativeDescendantsTerminal": True,
            "bindingSHA256": binding_sha, "syntheticRootObservation": True}))
        terminal_ref = file_ref(terminal_path)
    context = {"round": selection["round"], "selection": selection, "producerReceipts": producers,
        "actualNative": actual_native, "sourceFailure": False}
    scope_path = folder / "raw-scope.json"
    scope_path.write_bytes(encoded({"kind": "CURRENT_DAILY_POSTCHECK", "bindingSHA256": binding_sha,
        "invocation": binding["invocation"], "dailyContext": context, "outcomeRefs": refs,
        "terminalEvidence": terminal_ref, "checks": checks}))
    postcheck_path = folder / "raw-postcheck.json"
    postcheck_path.write_bytes(encoded({"kind": "ACTUAL_METERED_CURRENT_DAILY_POSTCHECK", "bindingSHA256": binding_sha,
        "verificationPlan": file_ref(scope_path), "dailyContext": context, "outcomeRefs": refs,
        "rawVerified": True, "publishedDestinationsVerified": True, "componentCleanupConfirmed": True,
        "wholeDataVerified": False, "dataVerified": False, "readbacks": readbacks,
        "cumulativeIndependentGETs": len(readbacks), "newGETs": len(readbacks), "originalSourceStatus": status}))
    return {"binding": file_ref(binding_path), "postcheck": file_ref(postcheck_path), "terminal": terminal_ref}


def test_affected_scope_uses_verified_delivery_ranges_and_exact_subset(tmp_path):
    from test_rolling_span_source_proof import raw_floor_fixture

    from pipeline import reference_bulk

    job = Job.create(tmp_path / "job", "2026-10-01", "2026-10-01", ["hrv"], "legacy", "synthetic")
    universe = make_scope(tmp_path, job, "2026-11-05", "2026-11-08")
    data = json.loads(read(universe["path"]))
    floor = raw_floor_fixture(job, tmp_path / "source", {**data, "lower_endpoint": "2026-10-01",
        "scope_status": "final", "parent_span_sha256": None},
        delivered=[("2026-10-01", "hrv")], new_dates=["2026-10-01"])
    source = json.loads(read(floor["path"]))
    pairs = [{"binding": source["binding"], "postcheck": source["postcheck"]}]
    result = reference_bulk.create_affected_scope(job, job.plan_sha, pairs, universe,
        today=data["today"], timezone=data["timezone"], deadline_utc=data["deadline_utc"],
        output=tmp_path / "affected.json")
    affected = json.loads(read(result["path"]))
    assert affected["kind"] == "reference-bulk-affected-scope" and affected["schema"] == 2
    assert len(affected["targets"]) == 8
    assert {target["window_days"] for target in affected["targets"]} == {90}
    assert affected["source_delivery_refs"] == pairs
    prepared = reference_bulk.prepare_bulk(job, job.plan_sha, result, tmp_path / "workset")
    workset, _ = reference_bulk.load_bulk_workset(job, job.plan_sha, result, tmp_path / "workset")
    assert prepared["target_count"] == len(workset.targets) == 8
    assert len(workset.binding["endpoints"]) == 4


def daily_policy_fixture(tmp_path, dates, *, latest="2026-11-08"):
    """Only synthetic proof metadata; no thirty-date Source or SDK run is claimed."""
    from test_rolling_span_source_proof import raw_floor_fixture

    job = Job.create(tmp_path / "job", min(dates), max(dates), ["hrv"], "legacy", "synthetic")
    oneoff = make_scope(tmp_path, job, "2025-09-29", "2026-11-08")
    original = read(oneoff["path"])
    scope = json.loads(original)
    floor = raw_floor_fixture(job, tmp_path / "source", {**scope, "lower_endpoint": min(dates),
        "scope_status": "final", "parent_span_sha256": None},
        delivered=[(day, "hrv") for day in dates], new_dates=dates)
    raw = json.loads(read(floor["path"]))
    delivery = [{"binding": raw["binding"], "postcheck": raw["postcheck"]}]
    policy = {name: scope[name] for name in ("job_plan_sha256", "storage_target_sha256", "today",
        "timezone", "deadline_utc")}
    policy.update(schema=1, kind="DAILY_REFERENCE_ENDPOINT_POLICY_V1", latest_allowed_wake_date=latest,
                  max_source_dates=30, max_endpoints=406, historical_refs=[oneoff])
    path = tmp_path / "daily-policy.json"
    path.write_bytes(encoded(policy))
    return job, delivery, file_ref(path), oneoff, original


def test_daily_universe_includes_thirty_source_dates_before_oneoff_and_later_dirty_targets(tmp_path):
    from pipeline.reference_bulk import (
        create_affected_scope,
        create_daily_endpoint_scope,
        prepare_bulk,
    )

    dates = [(datetime(2025, 8, 1) + timedelta(days=index)).date().isoformat() for index in range(30)]
    job, delivery, policy_ref, oneoff, original = daily_policy_fixture(tmp_path, dates, latest="2025-10-01")
    before = read(job.folder / "events.jsonl")
    universe_ref = create_daily_endpoint_scope(job, job.plan_sha, delivery, policy_ref,
                                               output=tmp_path / "daily-universe.json")
    universe = json.loads(read(universe_ref["path"]))
    assert universe["source_dates"] == dates and universe["endpoints"][0] == "2025-08-01"
    assert universe["endpoints"][-1] == "2025-10-01" and len(universe["endpoints"]) == 62
    assert universe["policy_ref"] == policy_ref and set(dates) <= set(universe["endpoints"])
    scope_ref = create_affected_scope(job, job.plan_sha, delivery, universe_ref,
        today=universe["today"], timezone=universe["timezone"], deadline_utc=universe["deadline_utc"],
        output=tmp_path / "daily-affected.json")
    scope = json.loads(read(scope_ref["path"]))
    assert set(dates) <= {target["endpoint"] for target in scope["targets"]}
    assert any(target["endpoint"] == "2025-09-29" and target["window_days"] == 90 for target in scope["targets"])
    assert all(target["endpoint"] <= "2025-10-01" for target in scope["targets"])
    assert prepare_bulk(job, job.plan_sha, scope_ref, tmp_path / "workset")["endpoint_count"] == 62
    assert read(oneoff["path"]) == original and read(job.folder / "events.jsonl") == before


@pytest.mark.parametrize("fault", ["scattered_overflow", "more_than_thirty"])
def test_daily_universe_holds_without_truncating_an_overflowing_or_oversized_cohort(tmp_path, fault):
    from pipeline.history_state import Stop
    from pipeline.reference_bulk import create_daily_endpoint_scope

    step, count = (100, 30) if fault == "scattered_overflow" else (1, 31)
    dates = [(datetime(2017, 1, 1) + timedelta(days=index * step)).date().isoformat() for index in range(count)]
    job, delivery, policy, oneoff, original = daily_policy_fixture(tmp_path, dates)
    before = read(job.folder / "events.jsonl")
    output = tmp_path / "rejected-universe.json"
    with pytest.raises(Stop):
        create_daily_endpoint_scope(job, job.plan_sha, delivery, policy, output=output)
    assert not output.exists() and read(oneoff["path"]) == original
    assert read(job.folder / "events.jsonl") == before


def test_daily_bulk_opt_in_keeps_raw_native_result_and_omits_legacy_drain(tmp_path, monkeypatch, capsys):
    import boto3
    from test_rolling_reference_sources import RevisionServer

    from pipeline import history_collect

    job = Job.create(tmp_path / "job", "2026-04-29", "2026-04-29", ["hrv"], "legacy", "synthetic")
    server = RevisionServer()
    original_client, original_run = boto3.client, history_collect.subprocess.run

    def client_factory(*args, **kwargs):
        client = original_client(*args, **kwargs)
        client._endpoint.http_session.send = server.send
        return client

    def process(args, **kwargs):
        return original_run([args[0], "-B", str(Path(__file__).with_name("history_native_fixture.py")),
            "normal", str(job.folder), args[args.index("--run") + 1], args[args.index("--date") + 1]],
            input=kwargs["input"], capture_output=True, check=False)

    monkeypatch.setattr(boto3, "client", client_factory)
    monkeypatch.setattr(history_collect.subprocess, "run", process)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_BUCKET": BUCKET,
        "R2_ACCESS_KEY_ID": "synthetic", "R2_SECRET_ACCESS_KEY": "synthetic", "HEALTH_TIMEZONE": "UTC"}.items():
        monkeypatch.setenv(name, value)
    native = main(["--state", str(job.folder), "daily", "--plan-sha", job.plan_sha,
        "--confirm-cloud-jobs-paused", "--confirm-r2-write", "--max-dates", "1",
        "--defer-reference-build-to-bulk"])
    output = capsys.readouterr()
    assert output.out, output.err
    result = json.loads(output.out)
    assert native == 2 and result["daily_status"] == "baselines_pending"
    assert result["daily_new_dates"] == 1 and result["counts"] == {"published": 1}
    assert result["daily_publication_phases"] == 1
    assert result["baseline_refresh"]["reference_work_deferred"] is True
    assert not any(frame["event"]["type"] == "daily_round_finished" for frame in job.events()[0])
    assert not any(method == "PUT" and key.startswith("derived/night-baseline/") for method, key, *_ in server.calls)
    delivery = observe_daily_raw_boundary(job, tmp_path / "source-proof", server, status=result)
    from test_history_baseline_integration import sdk_store

    from pipeline.reference_bulk import (
        collect_bulk,
        inventory_bulk,
        load_bulk_workset,
        prepare_bulk,
    )
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan
    from pipeline.rolling_span_source_proof import observed_raw_delivery

    observed = observed_raw_delivery(job, delivery["binding"], delivery["postcheck"],
        target_sha=json.loads(read(delivery["binding"]["path"]))["targetSHA256"])
    assert observed["stage_facts"][0]["source_episode_native"] == 2
    scope_ref = make_scope(tmp_path, job, "2026-04-29", "2026-04-29")
    scope = json.loads(read(scope_ref["path"]))
    scope["source_delivery_refs"] = [{key: delivery[key] for key in ("binding", "postcheck")}]
    Path(scope_ref["path"]).write_bytes(encoded(scope))
    scope_ref = file_ref(Path(scope_ref["path"]))
    folder, carried = tmp_path / "workset", {"independent_lifetime_gets": 680}
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    inventory_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=inventory_plan(workset, carried),
        carried=carried, store_factory=lambda: sdk_store(server))
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    raw_keys = {row["key"] for row in observed["destinations"]}
    before = sum(method == "GET" and key in raw_keys for method, key, *_ in server.calls)
    collect_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=collection_plan(workset, carried=carried),
        carried=carried, store_factory=lambda: sdk_store(server))
    assert sum(method == "GET" and key in raw_keys for method, key, *_ in server.calls) == before
    assert json.loads(read(folder / "SOURCE-DELIVERY-SEED.json"))["stage_facts"][0]["whole_data_verified"] is False


def published_scope(tmp_path, days=1, *, publish=True):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.reference_bulk import (
        build_bulk,
        collect_bulk,
        inventory_bulk,
        load_bulk_workset,
        prepare_bulk,
        publish_bulk,
    )
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan, publication_plan

    lower = (datetime(2026, 10, 25) - timedelta(days=days - 1)).date().isoformat()
    job = Job.create(tmp_path / "job", lower, "2026-10-25", ["hr", "hrv"], "legacy", "synthetic")
    scope_ref = make_scope(tmp_path, job, lower, "2026-10-25")
    folder, carried = tmp_path / "workset", {"independent_lifetime_gets": 680}
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    server, _ = seeded(lower, days)
    inventory_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=inventory_plan(workset, carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    plan = collection_plan(workset, carried=carried)
    for _ in range(days * 3 + 1):
        result = collect_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                              carried=carried, store_factory=lambda: sdk_store(server))
        if result["status"] == "sources_complete":
            break
    assert result["status"] == "sources_complete"
    build_bulk(job, job.plan_sha, scope_ref, folder, carried=carried)
    workset, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    plan = publication_plan(workset, carried=carried)
    result = (publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                          carried=carried, store_factory=lambda: sdk_store(server)) if publish else None)
    return job, scope_ref, folder, carried, workset, server, plan, result


@pytest.mark.parametrize("fault", ["stale_source", "unknown_put_and_close", "unknown_close"])
def test_bulk_known_stale_and_unknown_effects_never_advance_ack(tmp_path, fault):
    from test_history_baseline_integration import sdk_store

    from pipeline.history_state import Stop
    from pipeline.r2_store import R2BudgetError
    from pipeline.reference_bulk import load_bulk_workset, publish_bulk
    from pipeline.reference_bulk_budget import BulkAccount

    job, scope_ref, folder, carried, workset, server, plan, _ = published_scope(tmp_path, publish=False)
    targets = {target["key"] for target in workset.targets}
    if fault == "stale_source":
        key = next(row["key"] for row in workset.required_sources() if row["key"].startswith("health/sleep/"))
        server.objects[key] = (server.objects[key][0], '"new-source-revision"')

    def factory():
        from urllib.parse import unquote, urlsplit

        store = sdk_store(server)
        original_send, original_close = store.client._endpoint.http_session.send, store.client.close

        def send(request):
            key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
            if fault == "unknown_put_and_close" and request.method == "PUT" and key in targets:
                raise TimeoutError("synthetic unknown PUT transport")
            return original_send(request)

        def close():
            original_close()
            if fault in {"unknown_close", "unknown_put_and_close"}:
                raise OSError("synthetic cleanup acknowledgement lost")

        store.client._endpoint.http_session.send = send
        store.client.close = close
        return store

    with pytest.raises((Stop, R2BudgetError, OSError)):
        publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
                     carried=carried, store_factory=factory)
    reopened, _ = load_bulk_workset(job, job.plan_sha, scope_ref, folder)
    assert reopened.status()["hold"] is not None
    assert (folder / "PUBLICATION-INTENT.json").exists()
    assert not (folder / "PUBLICATION-CHECKPOINTS.jsonl").exists()
    terminal = json.loads(read(sorted((folder / "publications").glob("*/TERMINAL.json"))[0]))
    assert terminal["status"] == "hold"
    if fault != "unknown_close":
        assert terminal["primaryFailureCategory"] is not None
    if fault in {"unknown_close", "unknown_put_and_close"}:
        assert terminal["cleanupFailureCategory"] is not None and terminal["ownedSDKClosed"] is False
    if fault == "stale_source":
        assert not any(method == "PUT" and key in targets for method, key, *_ in server.calls)
    with pytest.raises(Stop, match="bulk_resource_account_held"):
        BulkAccount.open(folder / "account", workset.scope_sha256, carried)


def test_prepare_whole_scope_is_offline_and_keeps_all_2436_possible_targets(tmp_path, capsys):
    job = Job.create(tmp_path / "job", "2025-09-29", "2026-11-08", ["hr", "hrv"],
                     "legacy", "synthetic")
    target = digest(("https://" + ACCOUNT + ".r2.cloudflarestorage.com\0" + BUCKET).encode())
    scope = tmp_path / "scope.json"
    scope.write_bytes(encoded({"schema": 1, "kind": "reference-bulk-scope",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": target,
        "today": "2026-11-08", "timezone": "Europe/Oslo",
        "lower_endpoint": "2025-09-29", "upper_endpoint": "2026-11-08",
        "deadline_utc": "2026-11-09T23:00:00+00:00",
        "historical_refs": [], "source_delivery_refs": []}))
    before = job.events()

    native = main(["--state", str(job.folder), "rolling-bulk", "--phase", "prepare",
        "--plan-sha", job.plan_sha, "--scope", str(scope), "--scope-sha", digest(read(scope)),
        "--workset", str(tmp_path / "workset")])

    assert native == 0
    result = json.loads(capsys.readouterr().out)
    assert result["endpoint_count"] == 406 and result["target_count"] == 2436
    assert result["status"] == "prepared" and result["product_puts"] == 0
    assert result["source_calls"] == 0
    assert job.events() == before and not job.status()["locked"]


def test_sdk_collection_reopens_one_account_and_never_fetches_a_completed_source_twice(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.native_night import NativeNightReader
    from pipeline.reference_bulk import collect_bulk, inventory_bulk, prepare_bulk
    from pipeline.reference_bulk_budget import BulkAccount, collection_plan, inventory_plan
    from pipeline.reference_workset import ReferenceWorkset

    job = Job.create(tmp_path / "job", "2026-10-17", "2026-10-25", ["hr", "hrv"],
                     "legacy", "synthetic")
    scope_ref = make_scope(tmp_path, job, "2026-10-17", "2026-10-25")
    folder = tmp_path / "workset"
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    scope = json.loads(read(scope_ref["path"]))
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = [f"2026-10-{day}" for day in range(17, 26)]
    workset = ReferenceWorkset.open(folder, binding)
    server, _ = seeded("2026-10-17", 9)
    carried = {"independent_lifetime_gets": 680}
    initial_plan = inventory_plan(workset, carried)

    listed = inventory_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=initial_plan,
        carried=carried, store_factory=lambda: sdk_store(server))
    assert listed["status"] == "inventory_complete" and server.puts == []
    workset = ReferenceWorkset.open(folder, binding)
    selected_plan = collection_plan(workset, carried=carried)
    first = collect_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=selected_plan,
        carried=carried, store_factory=lambda: sdk_store(server), max_items=5)
    assert first["status"] == "bounded_pending" and server.puts == []
    second = collect_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=selected_plan,
        carried=carried, store_factory=lambda: sdk_store(server), max_items=400)
    if second["status"] == "bounded_pending":
        second = collect_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=selected_plan,
            carried=carried, store_factory=lambda: sdk_store(server), max_items=400)
    assert second["status"] == "sources_complete"
    assert len([call for call in server.calls if call[0] == "GET" and "list-type" not in call[2]]) == 28
    assert server.puts == [] and job.events()[0] == []
    account = BulkAccount.open(folder / "account", workset.scope_sha256, carried)
    assert account.snapshot()["usage"]["get"] == 28
    assert account.snapshot()["independent_lifetime_gets"] == 680
    assert account.snapshot()["deadline_utc"] == scope["deadline_utc"]
    before = len(server.calls)
    local = ReferenceWorkset.open(folder, binding).reader_store()
    reader = NativeNightReader(local, timezone="Europe/Oslo")
    reader.prime_rolling_sources(["2026-10-24", "2026-10-25"])
    assert reader.night("2026-10-25")["status"] == "available"
    assert len(server.calls) == before


def test_local_build_resumes_and_cannot_admit_publication_before_all_54_targets_are_sealed(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.history_state import Stop
    from pipeline.reference_bulk import build_bulk, collect_bulk, inventory_bulk, prepare_bulk
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan, publication_plan
    from pipeline.reference_workset import ReferenceWorkset

    job = Job.create(tmp_path / "job", "2026-10-17", "2026-10-25", ["hr", "hrv"],
                     "legacy", "synthetic")
    scope_ref = make_scope(tmp_path, job, "2026-10-17", "2026-10-25")
    scope = json.loads(read(scope_ref["path"]))
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = [f"2026-10-{day}" for day in range(17, 26)]
    folder = tmp_path / "workset"
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset = ReferenceWorkset.open(folder, binding)
    server, _ = seeded("2026-10-17", 9)
    carried = {"independent_lifetime_gets": 680}
    inventory_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=inventory_plan(workset, carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    workset = ReferenceWorkset.open(folder, binding)
    selected_plan = collection_plan(workset, carried=carried)
    for _ in range(3):
        collected = collect_bulk(job, job.plan_sha, scope_ref, folder,
            resource_plan_ref=selected_plan, carried=carried, store_factory=lambda: sdk_store(server))
        if collected["status"] == "sources_complete":
            break
    assert collected["status"] == "sources_complete"
    before = len(server.calls)

    first = build_bulk(job, job.plan_sha, scope_ref, folder, carried=carried, max_targets=6)
    assert first["status"] == "bounded_pending" and first["built_targets"] == 6
    workset = ReferenceWorkset.open(folder, binding)
    assert len(workset.pending_targets()) == 48
    with pytest.raises(Stop):
        publication_plan(workset, carried=carried)
    second = build_bulk(job, job.plan_sha, scope_ref, folder, carried=carried, max_targets=48)
    assert second["status"] == "build_complete" and second["product_count"] == 54
    workset = ReferenceWorkset.open(folder, binding)
    assert workset.pending_targets() == [] and workset.assert_sealed() == second["build_seal"]
    assert len(server.calls) == before and server.puts == [] and job.events()[0] == []


def test_publish_uses_sealed_products_and_one_closed_client_without_refetching_sources(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.reference_bulk import (
        build_bulk,
        collect_bulk,
        inventory_bulk,
        prepare_bulk,
        publish_bulk,
    )
    from pipeline.reference_bulk_budget import collection_plan, inventory_plan, publication_plan
    from pipeline.reference_workset import ReferenceWorkset

    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hr", "hrv"],
                     "legacy", "synthetic")
    scope_ref = make_scope(tmp_path, job, "2026-10-25", "2026-10-25")
    scope = json.loads(read(scope_ref["path"]))
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = ["2026-10-25"]
    folder = tmp_path / "workset"
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset = ReferenceWorkset.open(folder, binding)
    server, _ = seeded("2026-10-25", 1)
    carried = {"independent_lifetime_gets": 680}
    inventory_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=inventory_plan(workset, carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    workset = ReferenceWorkset.open(folder, binding)
    collect_bulk(job, job.plan_sha, scope_ref, folder,
        resource_plan_ref=collection_plan(workset, carried=carried), carried=carried,
        store_factory=lambda: sdk_store(server))
    build_bulk(job, job.plan_sha, scope_ref, folder, carried=carried)
    workset = ReferenceWorkset.open(folder, binding)
    plan = publication_plan(workset, carried=carried)
    source_gets_before = len([call for call in server.calls if call[0] == "GET"
        and "list-type" not in call[2] and not call[1].startswith("health/night-products/")])
    opened, closed = [], []

    def factory():
        store = sdk_store(server)
        opened.append(store.client)
        actual_close = store.client.close

        def close():
            actual_close()
            closed.append(True)

        store.client.close = close
        return store

    result = publish_bulk(job, job.plan_sha, scope_ref, folder, resource_plan_ref=plan,
        carried=carried, store_factory=factory)

    assert result["status"] == "verification_pending" and len(result["generated_targets"]) == 6
    assert len(opened) == len(closed) == 1 and result["publication_receipts"]
    assert len([key for key in server.puts if "/baseline/" in key]) == 6
    source_gets_after = len([call for call in server.calls if call[0] == "GET"
        and "list-type" not in call[2] and not call[1].startswith("health/night-products/")])
    assert source_gets_after == source_gets_before == 4
    for target in result["generated_targets"]:
        body, revision = server.objects[target["key"]]
        assert digest(body) == target["sha256"] and revision == target["revision"]
        assert json.loads(body)["args"] == target["args"]
        assert target["producer_receipt"] in result["publication_receipts"]
    assert job.events()[0] == []


def test_inventory_rejects_parser_invented_revision_before_it_can_qualify_a_workset(tmp_path):
    from test_history_baseline_integration import sdk_store
    from test_rolling_reference_sources import seeded

    from pipeline.night_baseline_refresh import BaselineRefreshStop
    from pipeline.reference_bulk import inventory_bulk, prepare_bulk
    from pipeline.reference_bulk_budget import inventory_plan
    from pipeline.reference_workset import ReferenceWorkset

    job = Job.create(tmp_path / "job", "2026-10-25", "2026-10-25", ["hrv"], "legacy", "synthetic")
    scope_ref = make_scope(tmp_path, job, "2026-10-25", "2026-10-25")
    scope = json.loads(read(scope_ref["path"]))
    binding = {key: scope[key] for key in
               ("job_plan_sha256", "storage_target_sha256", "today", "timezone")}
    binding["endpoints"] = ["2026-10-25"]
    folder = tmp_path / "workset"
    prepare_bulk(job, job.plan_sha, scope_ref, folder)
    workset = ReferenceWorkset.open(folder, binding)
    server, _ = seeded("2026-10-25", 1)

    def factory():
        store = sdk_store(server)

        def alter(parsed, **_):
            for row in parsed.get("Contents", []):
                row["ETag"] = '"invented"'

        store.client.meta.events.register_first("after-call.s3.ListObjectsV2", alter)
        return store

    with pytest.raises(BaselineRefreshStop, match="raw revision disagreement"):
        inventory_bulk(job, job.plan_sha, scope_ref, folder,
            resource_plan_ref=inventory_plan(workset, {}), carried={}, store_factory=factory)
    assert ReferenceWorkset.open(folder, binding).status()["hold"] is not None
    assert server.puts == []
