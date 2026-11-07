"""Thirty NEW dates finish alongside verified stored backlog within measured bounds."""

import json
import time
from collections import Counter
from datetime import date, timedelta
from types import SimpleNamespace

from daily_autonomy_fixture import daily_system, transport_counts

from pipeline.history_backfill import daily
from pipeline.history_canonical import decode
from pipeline.history_collect import collect
from pipeline.history_state import Job
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.r2_store import R2Store


def test_daily_publishes_thirty_new_dates_and_reports_rolling_rest_within_original_caps(monkeypatch):
    system = daily_system(monkeypatch)
    job, server, files = system.job, system.server, system.files
    # A real prior source receipt supplies the newest stored-only date. It must
    # remain reusable, and cannot consume any of the next thirty NEW date slots.
    cached = collect(job, job.plan_sha, confirm=True, max_dates=1,
                     target_dates=["2026-01-26"])
    assert cached["counts"] == {"ready": 2, "pending": 60}
    stored_proofs = dict(files)
    previous_http = len(system.source_calls)
    assert previous_http == 4
    args = SimpleNamespace(confirm_cloud_jobs_paused=True, confirm_r2_write=True,
                           max_dates=30, max_http=240, plan_sha=job.plan_sha,
                           resume=False, archive_reload=True, ledger=[])
    started = time.monotonic()
    result = daily(job, args)
    elapsed = time.monotonic() - started
    reports = result["daily_baseline_reports"]
    wire = transport_counts(server.calls)
    totals = reports[-1]["execution_totals"]
    source_calls = system.source_calls[previous_http:]
    publication_response_bytes = server.response_bytes
    verification_started = len(server.calls)
    # A new SDK client independently reads the actual destination bytes and
    # metadata; the publisher's own cache/readback is not this verification.
    verification = R2Store()
    snapshots = {}
    try:
        for key in sorted(set(server.puts)):
            if (key == QUEUE_KEY or "/baseline/" in key
                    or key.startswith(("health/signals/", "health/hrv/", "health/indexes/hrv/"))):
                snapshots[key] = verification.get_snapshot(key, max_bytes=524288)
    finally:
        verification.client.close()
    verification_wire = transport_counts(server.calls[verification_started:])
    assert verification_wire["HEAD"] == verification_wire["LIST"] == verification_wire["PUT"] == 0
    independent_gets = verification_wire["GET"]
    measurements = {
        "synthetic": True, "native_process_launched": False,
        "daily_status": result["daily_status"], "counts": result["counts"],
        "source_HTTP": len(source_calls), "stored_source_HTTP": previous_http,
        "source_new_dates": len({day for day, _, _ in source_calls}),
        "source_reload_POST": sum(method == "POST" for _, method, _ in source_calls),
        "physical_R2_publisher": dict(wire), "R2_response_bytes_publisher": publication_response_bytes,
        "independent_GET": independent_gets,
        "R2_response_bytes_verification": server.response_bytes - publication_response_bytes,
        "R2_attempted_write_bytes": server.attempted_write_bytes,
        "publication_phases": len(reports), "wall_seconds": elapsed,
        "phase_reports": reports,
        "publication_phase_timings": [frame["event"]["timings"] for frame in job.events()[0]
                                      if frame["event"]["type"] == "end"
                                      and frame["event"].get("status") == "published"],
    }
    print(json.dumps(measurements, sort_keys=True))
    assert result["daily_status"] == "baselines_pending", measurements
    assert reports[-1]["cohort_complete"] is False
    assert reports[-1]["cohort_rolling_remaining"]["90"]["hrv"]["count"] == 31
    assert result["counts"] == {"published": 62}
    expected_new_dates = {(date(2025, 12, 27) + timedelta(days=offset)).isoformat()
                          for offset in range(30)}
    assert {day for day, _, _ in source_calls} == expected_new_dates
    assert len(source_calls) == result["daily_HTTP"] == 210
    assert sum(frame["event"]["physicalHTTP"] for frame in job.events()[0]
               if frame["event"]["type"] == "date_closed") == 214
    assert sum(method == "POST" for _, method, _ in source_calls) == 30
    assert system.native_results == [0] * 31
    for name, body in stored_proofs.items():
        if "/2026-01-26/" in name:
            assert files[name] == body
    raw_writes = Counter(key for key in server.puts
                         if key.startswith(("health/signals/", "health/hrv/")))
    assert len(raw_writes) == 62 and set(raw_writes.values()) == {1}
    first_reference = next(i for i, key in enumerate(server.puts) if "/baseline/" in key)
    assert first_reference > max(i for i, key in enumerate(server.puts)
                                 if key in raw_writes or key.startswith("health/indexes/hrv/"))
    for key in raw_writes:
        snapshot = snapshots[key]
        assert snapshot.data == server.objects[key][0]
        assert snapshot.revision == server.objects[key][1]
        assert snapshot.content_type == "application/json" and snapshot.encoding == "gzip"
        value = decode(snapshot.data)
        if key.startswith("health/signals/"):
            channel = value["channels"]["heart_rate"]
            assert channel["missing_value_count"] == 1
            assert [p["value"] for p in channel["readings"]] == [55]
        else:
            assert value["archive_hrv_quality_v1"]["missing_readings"][0]["hrvValue"] is None
    queue = json.loads(snapshots[QUEUE_KEY].data)
    assert all(row.get("rolling_window") for row in queue["pending"].values())
    assert queue["rolling"]["eligible"] == [["2025-12-27", "2026-01-26"]]
    indexes = {key: decode(snapshot.data) for key, snapshot in snapshots.items()
               if key.startswith("health/indexes/hrv/")}
    assert set(indexes) == {"health/indexes/hrv/v1/2025/12.json", "health/indexes/hrv/v1/2026/01.json"}
    assert Counter(row["status"] for row in indexes["health/indexes/hrv/v1/2025/12.json"]["days"]) == {
        "available": 5}
    assert Counter(row["status"] for row in indexes["health/indexes/hrv/v1/2026/01.json"]["days"]) == {
        "available": 26}
    products = [json.loads(snapshot.data) for key, snapshot in snapshots.items() if "/baseline/" in key]
    # Independent worked ISO ranges: all Dec27-Jan26 dates make five December nights,
    # two nights in 2025-W52, and 26 January nights. Empty bins retain null values.
    expected = {("2025-12-01", "2025-12-31"): 5, ("2026-01-01", "2026-01-31"): 26,
                ("2025-12-22", "2025-12-28"): 2, ("2025-12-29", "2026-01-04"): 7,
                ("2026-01-05", "2026-01-11"): 7, ("2026-01-12", "2026-01-18"): 7,
                ("2026-01-19", "2026-01-25"): 7, ("2026-01-26", "2026-02-01"): 1}
    calendar_products = [p for p in products
                         if (p["args"]["start_date"], p["args"]["end_date"]) in expected]
    assert Counter((p["args"]["start_date"], p["args"]["end_date"]) for p in calendar_products) == Counter(
        {period: 2 for period in expected})
    for product in calendar_products:
        assert product["contributing_nights"] == expected[
            product["args"]["start_date"], product["args"]["end_date"]]
        assert {b["p50"] for b in product["bins"] if b["nights"]} == {
            55 if product["args"]["channel"] == "heart_rate" else 45}
        assert all(b["p50"] is None for b in product["bins"] if not b["nights"])
    assert result["daily_publication_phases"] == len(reports) <= 14
    for report in reports:
        assert report["publication_counts"]["get"] <= 400
        assert report["publication_counts"]["list"] <= 100
        assert report["publication_counts"]["put"] <= 100
        assert report["publication_body_bytes"] <= 2 * 1024 * 1024
    assert totals["counts"] == {"get": wire["GET"] + wire["HEAD"],
                                "list": wire["LIST"], "put": wire["PUT"]}
    assert totals["counts"]["get"] <= 6000
    assert independent_gets <= 256
    assert totals["counts"]["get"] + independent_gets <= 6256
    assert totals["counts"]["list"] + totals["counts"]["put"] <= 1000
    assert totals["body_bytes"] == server.attempted_write_bytes <= 10 * 1024 * 1024
    # Reopening the real completed journal verifies destination state without
    # spending source calls or rewriting already acknowledged canonical objects.
    before_http, before_wire, before_puts = len(system.source_calls), len(server.calls), list(server.puts)
    repeated = daily(Job(job.folder), args)
    assert repeated["daily_status"] == "baselines_pending"
    assert repeated["daily_publication_phases"] == 14
    assert len(system.source_calls) == before_http
    assert len(server.calls) > before_wire
    assert len(server.puts) > len(before_puts)
    assert repeated["baseline_refresh"]["cohort_rolling_remaining"] != reports[-1]["cohort_rolling_remaining"]
