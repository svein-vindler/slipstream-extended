"""Verified local calculation inputs retain both physical storage fences."""

import json

from test_history_baseline_integration import sdk_store
from test_night_baseline_month_capacity import seeded

from pipeline.night_baseline_refresh import refresh_pending


def drain(server, **kwargs):
    store = sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152)
    try:
        return refresh_pending(store, timezone="Europe/Oslo", today="2026-11-02", **kwargs)
    finally:
        store.client.close()


def test_verified_local_canonical_snapshots_save_initial_reads_but_keep_both_fences():
    remote, _ = seeded("2026-10-26", 7, "week/2026-W44")
    remote_report = drain(remote)
    local, initial = seeded("2026-10-26", 7, "week/2026-W44")
    cache = {key: snap for key, snap in initial.objects.items() if key.startswith("health/hrv/")}
    local_report = drain(local, snapshot_cache=cache)
    assert remote_report["periods_completed"] == local_report["periods_completed"] == 1
    assert local_report["local_snapshot_hits"] == 7
    assert remote_report["component_operations"]["get"] - local_report["component_operations"]["get"] == 7
    for key in cache:
        assert len([call for call in local.calls if call[0] == "GET" and call[1] == key]) == 2
    remote_products = {key: json.loads(remote.objects[key][0]) for key in remote.puts if "/baseline/" in key}
    local_products = {key: json.loads(local.objects[key][0]) for key in local.puts if "/baseline/" in key}
    assert remote_products == local_products
    print({"remote": remote_report["component_operations"], "local": local_report["component_operations"]})


def test_stale_cached_dirty_source_cannot_publish_a_current_baseline():
    import pytest

    from pipeline.night_baseline_refresh import BaselineRefreshStop
    from pipeline.r2_store import R2ObjectSnapshot

    server, initial = seeded("2026-10-26", 7, "week/2026-W44")
    key = next(key for key in initial.objects if key.startswith("health/hrv/"))
    snap = initial.objects[key]
    cache = {key: R2ObjectSnapshot(snap.data, '"stale"', snap.content_type, snap.encoding)}
    with pytest.raises(BaselineRefreshStop, match="Dirty canonical source proof changed"):
        drain(server, snapshot_cache=cache)
    assert not server.puts


def test_cached_source_changed_on_remote_is_caught_by_fresh_prepublication_fence():
    import pytest

    from pipeline.night_baseline_refresh import BaselineRefreshStop

    server, initial = seeded("2026-10-26", 7, "week/2026-W44")
    cache = {key: snap for key, snap in initial.objects.items() if key.startswith("health/hrv/")}
    key = next(iter(cache))
    body, _ = server.objects[key]
    server.objects[key] = (body, '"changed-after-local-verification"')
    with pytest.raises(BaselineRefreshStop, match="Night source revision changed"):
        drain(server, snapshot_cache=cache)
    assert not any("/baseline/" in key for key in server.puts)


def test_cached_source_changed_after_product_keeps_period_pending():
    import pytest

    from pipeline.night_baseline_refresh import QUEUE_KEY, BaselineRefreshStop

    server, initial = seeded("2026-10-26", 7, "week/2026-W44")
    cache = {key: snap for key, snap in initial.objects.items() if key.startswith("health/hrv/")}
    changed_key = next(iter(cache))
    send = server.send
    changed = False

    def mutate_after_product(request):
        nonlocal changed
        response = send(request)
        if request.method == "PUT" and "/baseline/" in request.url and not changed:
            changed = True
            body, _ = server.objects[changed_key]
            server.objects[changed_key] = (body, '"changed-after-product"')
        return response

    server.send = mutate_after_product
    with pytest.raises(BaselineRefreshStop, match="Night source revision changed"):
        drain(server, snapshot_cache=cache)
    assert any("/baseline/" in key for key in server.puts)
    queue = json.loads(server.objects[QUEUE_KEY][0])
    assert "week/2026-W44" in queue["pending"]


def test_local_snapshot_keeps_stored_source_bound_before_any_product():
    import pytest

    from pipeline.native_night import NativeInputError
    from pipeline.night_baseline_refresh import BaselineRefreshStop
    from pipeline.r2_store import R2ObjectSnapshot

    server, initial = seeded("2026-10-26", 7, "week/2026-W44")
    key = next(key for key in initial.objects if key.startswith("health/hrv/"))
    snap = initial.objects[key]
    cache = {key: R2ObjectSnapshot(b"x" * 524289, snap.revision, snap.content_type, snap.encoding)}
    with pytest.raises(BaselineRefreshStop) as stopped:
        drain(server, snapshot_cache=cache)
    assert isinstance(stopped.value.__cause__, NativeInputError)
    assert str(stopped.value.__cause__) == "Stored source exceeds requested bound"
    assert not server.puts


def test_period_filter_never_completes_unselected_closed_period():
    server, _ = seeded("2026-10-26", 7, "week/2026-W44")
    result = drain(server, period_names={"month/2026-10"})
    assert result["periods_completed"] == 0
    assert result["closed_pending"] == result["pending"] == 1
    assert result["status"] == "bounded_pending"
    assert not server.puts


def test_direct_source_reads_share_remaining_guard_budget_with_later_baseline_drain(monkeypatch):
    from test_history_baseline_integration import memory_job

    from pipeline.history_publish import StorageGuard, _ReferenceStore

    job, _ = memory_job(monkeypatch, [])
    server, initial = seeded("2026-10-26", 7, "week/2026-W44")
    store = sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152)
    source = next(key for key in initial.objects if key.startswith("health/hrv/"))
    guard = StorageGuard(store, {source}, job.folder / "budget")
    reference = _ReferenceStore(store, guard)
    try:
        for _ in range(398):
            store.get_snapshot(source, max_bytes=524288)
        result = refresh_pending(reference, timezone="Europe/Oslo", today="2026-11-02")
        assert result["status"] == "bounded_pending"
        assert result["component_operations"]["get"] == guard.counts["get"] == 400
        assert result["periods_completed"] == 0
        assert not server.puts
        assert not guard.failed
    finally:
        guard.close()
        store.client.close()
