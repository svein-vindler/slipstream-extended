"""Shared preparation and publication over actual SDK, synthetic HTTP only."""
import copy
import hashlib
import json
from datetime import date, timedelta

import pytest
from test_history_baseline_integration import sdk_store
from test_reference_workset import declaration, inventory
from test_rolling_reference_sources import seeded

from pipeline import night_baseline_refresh as engine
from pipeline.history_state import Stop
from pipeline.native_night import NativeNightReader
from pipeline.night_products import normalize_args, storage_key
from pipeline.reference_workset import ReferenceWorkset


def bounded_store(server):
    return sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152)


def assembled(store, endpoint="2026-10-27", width=28):
    end = date.fromisoformat(endpoint)
    days = [(end - timedelta(days=offset)).isoformat() for offset in range(width - 1, -1, -1)]
    reader = NativeNightReader(store, timezone="UTC")
    reader.prime_rolling_sources(days)
    values = [reader.night(day) for day in days]
    proof = {"objects": {}, "lists": {}}
    for day in days:
        for kind in proof:
            proof[kind].update(copy.deepcopy(reader.dependencies[day][kind]))
    return days, values, proof


def test_prepares_both_channels_locally_with_partial_coverage_and_no_product_write():
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        days, values, proof = assembled(store)
        reads = list(server.calls)
        artifacts = [engine.prepare_reference(values, normalize_args(period="custom",
            start_date=days[0], end_date=days[-1], channel=channel), proof)
            for channel in ("heart_rate", "hrv")]
        products = [json.loads(item["body"]) for item in artifacts]
        assert [item["contributing_nights"] for item in products] == [3, 3]
        assert [item["nights_requested"] for item in products] == [28, 28]
        assert [item["unit"] for item in products] == ["bpm", "ms"]
        assert [next(row["p50"] for row in item["bins"] if row["nights"]) for item in products] == [55, 45]
        assert all(item["prior_inputs"] and item["source_proof"] == proof for item in artifacts)
        assert server.calls == reads and not server.puts
    finally:
        store.client.close()


def test_publication_requires_a_whole_build_seal_before_any_storage_operation():
    server, _ = seeded("2026-10-25", 1)
    store = bounded_store(server)
    before = list(server.calls)
    try:
        with pytest.raises(engine.BaselineRefreshStop, match="Sealed reference workset required"):
            engine.publish_prepared_rows(store, {"pending": {}}, None, [], report={},
                                         prepared_manifest=None)
        assert server.calls == before and not server.puts
    finally:
        store.client.close()


def staged_workset(tmp_path, store, endpoint="2026-10-27", *, target_subset=False, windows=None):
    binding, targets = declaration([endpoint])
    if windows is not None:
        binding["windows"] = list(windows)
        targets = []
        for width in windows:
            for channel in ("heart_rate", "hrv"):
                args = normalize_args(end_date=endpoint, period=str(width), channel=channel)
                key = storage_key(args)
                targets.append({"target_id": key, "key": key, "args": args,
                    "endpoint": endpoint, "window_days": width, "channel": channel})
    if target_subset:
        targets = targets[:1]  # Explicit HR28 only; all other slots remain outside this build.
    workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets,
                                     target_subset=target_subset)
    inventory(workset, store, tmp_path)
    workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
    workset.seal_sources()
    end = date.fromisoformat(endpoint)
    width = max(target["window_days"] for target in targets)
    days = [(end - timedelta(days=offset)).isoformat() for offset in range(width - 1, -1, -1)]
    reader = NativeNightReader(workset.reader_store(), timezone="UTC")
    reader.prime_rolling_sources(days)
    nights = {day: reader.night(day) for day in days}
    for target in targets:
        selected = [day for day in days if target["args"]["start_date"] <= day]
        proof = {kind: {key: value for day in selected
            for key, value in reader.dependencies[day][kind].items()} for kind in ("objects", "lists")}
        workset.stage_product(target["key"], engine.prepare_reference(
            [nights[day] for day in selected], target["args"], proof))
    return workset, targets


def register_endpoint(store, server, endpoint="2026-10-27", *, prepared_manifest=None):
    key = "health/sleep/v1/" + endpoint[:4] + "/" + endpoint[5:7] + "/" + endpoint + ".json"
    body, revision = server.objects[key]
    engine.register_rolling_endpoints(store, [{"key": key, "revision": revision,
        "sha256": hashlib.sha256(body).hexdigest()}], today=endpoint,
        prepared_manifest=prepared_manifest)


def test_heterogeneous_sealed_targets_admit_only_declared_endpoint_windows_after_source_change(tmp_path):
    from pipeline import rolling_night_references as rolling

    first, peer = "2026-10-27", "2026-10-28"
    server, _ = seeded("2026-10-26", 3)
    store = bounded_store(server)
    try:
        binding, _ = declaration([first, peer])
        binding["windows"] = [7, 365]
        targets = []
        for endpoint, widths in ((first, (7, 365)), (peer, (365,))):
            for width in widths:
                for channel in rolling.CHANNELS:
                    args = normalize_args(period="custom", channel=channel, end_date=endpoint,
                        start_date=(date.fromisoformat(endpoint) - timedelta(days=width - 1)).isoformat())
                    key = storage_key(args)
                    targets.append({"target_id": key, "key": key, "args": args,
                        "endpoint": endpoint, "window_days": width, "channel": channel})
        workset = ReferenceWorkset.create(tmp_path / "workset", binding, targets, target_subset=True)
        descriptor = (workset.folder / "WORKSET.json").read_bytes()
        inventory(workset, store, tmp_path)
        workset.fetch_missing(store, max_items=400, admit_fetch=lambda row: True)
        workset.seal_sources()
        for target in targets:
            end = date.fromisoformat(target["endpoint"])
            days = [(end - timedelta(days=offset)).isoformat()
                    for offset in range(target["window_days"] - 1, -1, -1)]
            reader = NativeNightReader(workset.reader_store(), timezone="UTC")
            reader.prime_rolling_sources(days, window_days=365 if target["window_days"] == 365 else None)
            nights = [reader.night(day) for day in days]
            proof = {kind: {key: value for day in days
                for key, value in reader.dependencies[day][kind].items()} for kind in ("objects", "lists")}
            workset.stage_product(target["key"], engine.prepare_reference(nights, target["args"], proof))
        workset.seal_build()
        sources = []
        for day in (first, peer):
            key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
            body, revision = server.objects[key]
            sources.append({"key": key, "revision": revision, "sha256": hashlib.sha256(body).hexdigest()})
        begin = len(server.calls)
        engine.register_rolling_endpoints(store.new_stage(), sources, today=peer, prepared_manifest=workset)
        registered = len(server.calls)
        key = "health/hrv/2026/10/2026-10-27.json"
        source = json.loads(server.objects[key][0])
        source["readings"][0]["hrv_ms"] = 75
        assert engine.publish_source(store.new_stage(), key, json.dumps(source).encode(), "application/json")
        queue = json.loads(server.objects[engine.QUEUE_KEY][0])
        def counts(calls):
            return {kind: sum(method == "PUT" if kind == "put" else
                method == "GET" and bool(query.get("list-type")) if kind == "list" else
                method == "HEAD" or method == "GET" and not query.get("list-type")
                for method, _, query in calls) for kind in ("get", "list", "put")}
        (tmp_path / "ADMISSION-COST.json").write_text(json.dumps({
            "synthetic": True, "registration": counts(server.calls[begin:registered]),
            "source_change": counts(server.calls[registered:]), "live_calls": 0}), encoding="utf-8")
        status = rolling.status(queue, windows=rolling.WINDOWS)
        for channel in rolling.CHANNELS:
            assert status["7"][channel]["ranges"] == [[first, first]]
            assert status["365"][channel]["ranges"] == [[first, peer]]
            assert all(status[str(n)][channel]["count"] == 0 for n in rolling.LEGACY_WINDOWS)
        assert queue["rolling"]["window_eligible"]["7"] == [[first, first]]
        assert queue["rolling"]["window_eligible"]["365"] == [[first, peer]]
        assert (workset.folder / "WORKSET.json").read_bytes() == descriptor
        assert workset.assert_sealed()
        rolling.validate(queue)
    finally:
        store.client.close()


def test_sealed_explicit_week_on_existing_legacy_endpoint_publishes_only_new_slots(tmp_path):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        register_endpoint(store, server)
        before = json.loads(server.objects[engine.QUEUE_KEY][0])
        workset, targets = staged_workset(tmp_path, store.new_stage(), windows=(7,))
        workset.seal_build()
        register_endpoint(store.new_stage(), server, prepared_manifest=workset)
        report = engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
            max_periods=8, prepared_manifest=workset)
        assert report["products_written"] == 2
        assert report["closed_pending"] == 0
        assert all(target["key"] in server.objects for target in targets)
        assert {key for key in server.puts if "/baseline/" in key} == {target["key"] for target in targets}
        after = json.loads(server.objects[engine.QUEUE_KEY][0])
        assert after["pending"] == before["pending"]
        assert after["rolling"]["eligible"] == before["rolling"]["eligible"]
        for width in ("28", "30", "90"):
            assert after["rolling"]["dirty"][width] == before["rolling"]["dirty"][width]
    finally:
        store.client.close()


@pytest.mark.parametrize("existing_pending", [False, True, "compact"])
def test_sealed_target_subset_only_registers_and_publishes_declared_hr28(tmp_path, existing_pending):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        if existing_pending:
            register_endpoint(store, server, "2026-10-26")
            register_endpoint(store.new_stage(), server)
            key = "health/hrv/2026/10/2026-10-25.json"
            body, revision = server.objects[key]
            engine.enqueue_verified_sources(store.new_stage(), [{"key": key,
                "revision": revision, "sha256": hashlib.sha256(body).hexdigest()}])
            if existing_pending is True:
                io = engine._IO(store.new_stage())
                try:
                    queue, revision = engine._read_queue(io)
                    for _ in range(2):
                        engine.rolling.materialize(queue, today="2026-10-27", limit=8,
                                                   max_pending=128)
                    engine._save_queue(io, queue, revision)
                finally:
                    io.close()
        workset, targets = staged_workset(tmp_path, store.new_stage(), target_subset=True)
        workset.seal_build()
        before_queue = json.loads(server.objects[engine.QUEUE_KEY][0]) if existing_pending else None
        start = len(server.calls)
        register_endpoint(store.new_stage(), server, prepared_manifest=workset)
        report = engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
            max_periods=8, prepared_manifest=workset)
        assert report["products_written"] == 1
        assert targets[0]["key"] in server.objects
        operations = server.calls[start:]
        product_puts = [key for method, key, _ in operations if method == "PUT" and "/baseline/" in key]
        assert product_puts == [targets[0]["key"]]
        assert not [key for method, key, _ in operations if method == "GET"
                    and "/baseline/" in key and key != targets[0]["key"]]
        assert not [key for method, key, query in operations if method == "GET"
            and not query.get("list-type") and any(prefix in key for prefix in
                ("health/sleep/v1/", "health/hrv/", "health/signals/v1/"))]
        queue = json.loads(server.objects[engine.QUEUE_KEY][0])
        selected_name = "rolling/28/2026-10-27/heart_rate"
        if existing_pending:
            expected = copy.deepcopy(before_queue)
            if existing_pending is True:
                del expected["pending"][selected_name]
            else:
                dirty = expected["rolling"]["dirty"]["28"]
                dirty["heart_rate"] = engine.rolling.remove_day(dirty["heart_rate"], "2026-10-27")
            assert queue == expected  # Siblings, older endpoints and calendar rows are untouched.
            assert report["closed_pending"] > 0  # Global remainder remains honest.
        else:
            assert not queue["pending"]
            assert not any(item["count"] for channels in engine.rolling.status(queue).values()
                           for item in channels.values())
    finally:
        store.client.close()


def test_sealed_workset_publishes_both_channels_with_shared_fences_and_no_source_refetch(tmp_path):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        workset, targets = staged_workset(tmp_path, store)
        before = list(server.calls)
        with pytest.raises(Stop):
            register_endpoint(store, server, prepared_manifest=workset)
        with pytest.raises(Stop):
            engine.refresh_pending(store, today="2026-10-27", timezone="UTC", max_periods=8,
                period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
        assert server.calls == before
        workset.seal_build()
        start = len(server.calls)
        register_endpoint(store.new_stage(), server, prepared_manifest=workset)
        reports = []
        for _ in range(3):
            report = engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
            reports.append(report)
            if report["closed_pending"] == 0:
                break
        assert report["closed_pending"] == 0, [{key: item.get(key) for key in (
            "status", "budget_deferred", "component_operations", "held_periods", "blocked")}
            for item in reports]
        assert sum(item["products_written"] for item in reports) == 6
        assert all(target["key"] in server.objects for target in targets)
        assert [json.loads(server.objects[target["key"]][0])["contributing_nights"] for target in targets] == [3] * 6
        assert not [key for method, key, query in server.calls[start:]
            if method == "GET" and not query.get("list-type") and any(root in key for root in
                ("health/sleep/v1/", "health/hrv/", "health/signals/v1/"))]
        assert all(item["component_operations"]["list"] <= 100 for item in reports)
        (tmp_path / "SDK-COST.json").write_text(json.dumps({
            "synthetic": True, "targets": 6, "publication_batches": len(reports),
            "collection_canonical_body_GETs": sum(method == "GET" and not query.get("list-type")
                and any(prefix in key for prefix in ("health/sleep/v1/", "health/hrv/", "health/signals/v1/"))
                for method, key, query in server.calls[:start]),
            "canonical_body_GETs_after_build": 0,
            "publication_operations": [item["component_operations"] for item in reports],
            "writes": sum(item["products_written"] for item in reports),
            "source_seal": workset.assert_sealed(), "all_products_prepared_before_first_product_PUT": True,
            "live_calls": 0}), encoding="utf-8")
    finally:
        store.client.close()


def test_stale_prepared_source_holds_before_any_current_product_put(tmp_path):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        workset, targets = staged_workset(tmp_path, store)
        workset.seal_build()
        register_endpoint(store, server)
        key = "health/hrv/2026/10/2026-10-27.json"
        body, _ = server.objects[key]
        server.objects[key] = body, '"corrected-after-build"'
        with pytest.raises(engine.BaselineRefreshStop):
            engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
        assert not any(target["key"] in server.objects for target in targets)
        assert workset.assert_sealed()
    finally:
        store.client.close()


def test_fresh_post_fence_is_required_before_any_batch_row_is_completed(tmp_path):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        workset, _ = staged_workset(tmp_path, store)
        workset.seal_build()
        register_endpoint(store, server)
        changed = []

        def send(request):
            response = server.send(request)
            if request.method == "PUT" and "/baseline/" in request.url and not changed:
                changed.append(True)
                key = "health/hrv/2026/10/2026-10-27.json"
                server.objects[key] = server.objects[key][0], '"changed-during-publish"'
            return response

        store.client._endpoint.http_session.send = send
        with pytest.raises(engine.BaselineRefreshStop):
            engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
        queue = json.loads(server.objects[engine.QUEUE_KEY][0])
        assert changed and len(queue["pending"]) == 6
        assert any(row.get("completed") for row in queue["pending"].values())
        assert not any(row.get("validated_progress") for row in queue["pending"].values())
    finally:
        store.client.close()


@pytest.mark.parametrize("fault", ["unknown_put", "cas_conflict"])
def test_prepared_publication_retains_inflight_and_does_not_retry_unknown_or_conflict(tmp_path, fault):
    from urllib.parse import unquote, urlsplit

    from botocore.awsrequest import AWSResponse
    from botocore.exceptions import ConnectionClosedError
    from history_storage_fixture import BUCKET, Raw

    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)
    try:
        workset, targets = staged_workset(tmp_path, store)
        workset.seal_build()
        register_endpoint(store, server)
        sends, injected = [], []

        def send(request):
            key = unquote(urlsplit(request.url).path).removeprefix("/" + BUCKET + "/")
            sends.append((request.method, key))
            if request.method == "PUT" and "/baseline/" in key and not injected:
                injected.append(key)
                if fault == "cas_conflict":
                    server.objects[key] = (b"external concurrent value", '"external"')
                    body = b"<Error><Code>PreconditionFailed</Code></Error>"
                    return AWSResponse(request.url, 412, {"content-length": str(len(body))}, Raw(body))
                server.send(request)
                raise ConnectionClosedError(endpoint_url=request.url)
            return server.send(request)

        store.client._endpoint.http_session.send = send
        with pytest.raises(engine.BaselineRefreshStop):
            engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
        queue = json.loads(server.objects[engine.QUEUE_KEY][0])
        assert len(injected) == 1
        assert any(row.get("inflight", {}).get("key") == injected[0] for row in queue["pending"].values())
        if fault == "cas_conflict":
            assert server.objects[injected[0]][0] == b"external concurrent value"
        before = list(sends)
        with pytest.raises(engine.BaselineRefreshStop):
            engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
        assert sends == before
        assert sum(target["key"] in server.objects for target in targets) == 1
    finally:
        store.client.close()


@pytest.mark.parametrize("correction", ["changed", "empty"])
def test_prepared_correction_preserves_history_and_good_other_channel(tmp_path, correction):
    server, _ = seeded("2026-10-25", 3)
    store = bounded_store(server)

    def publish(workset):
        observed = []
        for _ in range(8):
            report = engine.refresh_pending(store.new_stage(), today="2026-10-27", timezone="UTC",
                max_periods=8, period_names=set(), rolling_endpoints=["2026-10-27"],
                prepared_manifest=workset)
            observed.append(report)
            if report["rolling_pending"] == 0 or report["blocked"]:
                break
        return observed

    try:
        old_folder, new_folder = tmp_path / "old", tmp_path / "new"
        old_folder.mkdir()
        new_folder.mkdir()
        old_workset, targets = staged_workset(old_folder, store)
        old_workset.seal_build()
        register_endpoint(store, server)
        assert publish(old_workset)[-1]["closed_pending"] == 0
        original = {target["key"]: server.objects[target["key"]][0] for target in targets}
        for day in ("2026-10-25", "2026-10-26", "2026-10-27"):
            key = "health/hrv/2026/10/" + day + ".json"
            value = json.loads(server.objects[key][0])
            if correction == "changed":
                value["readings"][0]["hrv_ms"] = 68
            else:
                value["readings"] = []
            body = json.dumps(value).encode()
            server.objects[key] = body, '"' + hashlib.sha256(body).hexdigest() + '"'
        new_workset, _ = staged_workset(new_folder, store.new_stage())
        new_workset.seal_build()
        # The real dirty-source seam, after a verified concurrent canonical update.
        sources = [{"key": key, "revision": server.objects[key][1],
            "sha256": hashlib.sha256(server.objects[key][0]).hexdigest()}
            for key in server.objects if key.startswith("health/hrv/")]
        engine.enqueue_verified_sources(store.new_stage(), sources)
        reports = publish(new_workset)
        history = [body for key, (body, _) in server.objects.items()
                   if key.startswith("health/night-products/v1/history/") and key.endswith("/product.json")]
        if correction == "changed":
            assert reports[-1]["rolling_pending"] == 0
            assert sum(item["products_written"] for item in reports) == 6
            assert all(body in history for body in original.values())
            for target in targets:
                product = json.loads(server.objects[target["key"]][0])
                expected = 68 if target["channel"] == "hrv" else 55
                assert next(row["p50"] for row in product["bins"] if row["nights"]) == expected
        else:
            assert reports[-1]["blocked"]
            assert all(server.objects[target["key"]][0] == original[target["key"]]
                       for target in targets if target["channel"] == "hrv")
            assert all(json.loads(server.objects[target["key"]][0])["contributing_nights"] == 3
                       for target in targets if target["channel"] == "heart_rate")
    finally:
        store.client.close()
