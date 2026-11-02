"""Real SDK/helper boundary with paginated synthetic storage, no temp or network."""

import hashlib
import json
import re
from datetime import date, timedelta
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import BUCKET, Raw
from test_history_baseline_integration import BaselineServer, sdk_store
from test_night_baseline_corrections import calendar_store, queued_period

from pipeline.night_baseline_refresh import QUEUE_KEY, BaselineRefreshStop, refresh_pending


class PagedServer(BaselineServer):
    def send(self, request):
        query = parse_qs(urlsplit(request.url).query)
        if request.method != "GET" or query.get("list-type") != ["2"]:
            return super().send(request)
        prefix = query.get("prefix", [""])[0]
        self.calls.append(("GET", "inventory" if not prefix else prefix, query))
        if re.fullmatch(r"health/night-context/v1/\d{4}/\d{2}/", prefix):
            self.month_calls = getattr(self, "month_calls", 0) + 1
            if getattr(self, "month_failure", None) == "unknown":
                raise TimeoutError("synthetic transport")
            if getattr(self, "month_failure", None) == "denied":
                body = b"<Error><Code>AccessDenied</Code></Error>"
                return AWSResponse(request.url, 403, {"content-length": str(len(body))}, Raw(body))
            if getattr(self, "month_failure", None) == "malformed":
                return AWSResponse(request.url, 200, {"content-length": "7"}, Raw(b"invalid"))
            if getattr(self, "change_context", False):
                self.change_context = False
                add_history(self, 1)
            if getattr(self, "change_on_month", None) == self.month_calls:
                add_history(self, 1)
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        offset = int(query.get("continuation-token", ["0"])[0])
        size = int(query.get("max-keys", ["1000"])[0])
        if re.fullmatch(r"health/night-context/v1/\d{4}/\d{2}/", prefix):
            size = min(size, getattr(self, "month_page_size", size))
        assert 1 <= size <= 1000
        page = keys[offset : offset + size]
        truncated = offset + size < len(keys)
        rows = "".join(
            "<Contents><Key>"
            + escape(key)
            + "</Key><Size>"
            + str(len(self.objects[key][0]))
            + "</Size></Contents>"
            for key in page
        )
        token = (
            "<NextContinuationToken>" + str(offset + size) + "</NextContinuationToken>"
            if truncated
            else ""
        )
        body = (
            "<ListBucketResult><Name>"
            + BUCKET
            + "</Name><IsTruncated>"
            + str(truncated).lower()
            + "</IsTruncated><KeyCount>"
            + str(len(page))
            + "</KeyCount>"
            + rows
            + token
            + "</ListBucketResult>"
        ).encode()
        return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))


def seeded(first="2026-10-01", days=31, name=None):
    initial = calendar_store(first=date.fromisoformat(first), days=days)
    last = (date.fromisoformat(first) + timedelta(days=days - 1)).isoformat()
    queued_period(
        initial,
        first,
        last,
        [key for key in initial.objects if key.startswith("health/hrv/")],
        name=name,
    )
    server = PagedServer()
    for key, snap in initial.objects.items():
        server.objects[key] = (snap.data, snap.revision)
        server.metadata[key] = {"content-type": snap.content_type}
        if snap.encoding:
            server.metadata[key]["content-encoding"] = snap.encoding
    for i in range(20180 - len(server.objects)):
        server.objects[f"synthetic/fill/{i:05d}"] = (b"x", '"filler"')
    return server, initial


def add_history(server, count):
    for key in list(server.objects):
        if not key.startswith("health/night-context/v1/"):
            continue
        value = json.loads(server.objects[key][0])
        prefix = key.rsplit("/", 1)[0] + "/"
        for i in range(count):
            row = {k: v for k, v in value.items() if k != "context_id"}
            row["created_at"] = row["wake_date"] + f"T00:00:00.{i:03d}Z"
            core = json.dumps(
                dict(sorted(row.items())),
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
            row["context_id"] = "nctx-" + hashlib.sha256(core).hexdigest()[:24]
            name = (
                prefix
                + re.sub(r"[-:.Z]", "", row["created_at"])
                + "-"
                + row["context_id"]
                + ".json"
            )
            server.objects[name] = (json.dumps(row).encode(), '"history"')
            server.metadata[name] = {"content-type": "application/json"}


def run(server):
    return refresh_pending(
        sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152),
        timezone="Europe/Oslo",
        today="2026-11-02",
        max_periods=2,
    )


def test_31_night_month_finishes_with_21_inventory_pages_inside_original_caps():
    server, _initial = seeded()
    result = run(server)
    assert result["status"] == "complete"
    assert result["periods_completed"] == 1 and result["products_written"] == 2
    assert len([c for c in server.calls if c[1] == "inventory"]) == 21
    assert result["component_operations"]["get"] <= 400
    assert result["component_operations"]["list"] <= 100
    assert result["component_operations"]["put"] <= 100
    products = [json.loads(server.objects[key][0]) for key in server.puts if "/baseline/" in key]
    assert len(products) == 2 and all(p["contributing_nights"] == 31 for p in products)
    print({"full_month_20180_objects": result["component_operations"]})


def test_month_with_100_context_events_per_day_pages_both_fresh_fences():
    server, _initial = seeded()
    add_history(server, 99)
    for key in sorted(k for k in server.objects if k.startswith("synthetic/fill/"))[
        : len(server.objects) - 20180
    ]:
        del server.objects[key]
    result = run(server)
    assert result["status"] == "complete" and result["products_written"] == 2
    month = "health/night-context/v1/2026/10/"
    assert len([c for c in server.calls if c[1] == month]) == 8
    assert result["component_operations"]["list"] == 60
    print({"month_3100_context_events": result["component_operations"]})


@pytest.mark.parametrize(
    "first,name", [("2025-12-29", "week/2026-W01"), ("2026-10-26", "week/2026-W44")]
)
def test_iso_week_crossing_month_keeps_daily_semantics_and_full_dates(first, name):
    from pipeline.native_night import NativeNightReader
    from pipeline.night_products import baseline, canonical, normalize_args

    server, initial = seeded(first, 7, name)
    result = run(server)
    assert result["status"] == "complete" and result["products_written"] == 2
    dates = [(date.fromisoformat(first) + timedelta(days=i)).isoformat() for i in range(7)]
    reader = NativeNightReader(initial, timezone="Europe/Oslo")
    nights = [reader.night(day) for day in dates]
    for key in server.puts:
        if "/baseline/" not in key:
            continue
        product = json.loads(server.objects[key][0])
        args = normalize_args(
            start_date=dates[0],
            end_date=dates[-1],
            period="custom",
            channel=product["args"]["channel"],
        )
        expected = baseline(
            nights,
            args,
            input_bytes=len(canonical(nights).encode()),
            input_points=sum(len(ch["points"]) for n in nights for ch in n["channels"].values()),
        )["product"]
        assert {k: v for k, v in product.items() if k != "resources"} == {
            k: v for k, v in expected.items() if k != "resources"
        }
    assert result["component_operations"]["list"] == 32


@pytest.mark.parametrize("mode", ["denied", "unknown", "malformed", "changed"])
def test_fresh_context_fence_failure_stops_without_any_product_write(mode):
    server, _initial = seeded()
    if mode == "changed":
        server.change_context = True
    else:
        server.month_failure = mode
    with pytest.raises(BaselineRefreshStop):
        run(server)
    assert server.puts == [QUEUE_KEY]
    assert re.fullmatch(r"health/night-context/v1/\d{4}/\d{2}/", server.calls[-1][1])
    assert server.month_calls == 1
    assert (
        json.loads(server.objects[QUEUE_KEY][0])["pending"]["month/2026-10"]["phase"]
        == "publishing"
    )


def test_101_context_events_per_day_remains_unverified_and_never_creates_a_curve():
    server, _initial = seeded()
    add_history(server, 100)
    result = run(server)
    assert result["products_written"] == 0 and result["blocked"] == ["month/2026-10"]
    assert not any("/baseline/" in key for key in server.puts)


def test_context_post_fence_is_fresh_and_does_not_finalize_changed_sources():
    server, _initial = seeded()
    server.change_on_month = 2
    with pytest.raises(BaselineRefreshStop):
        run(server)
    row = json.loads(server.objects[QUEUE_KEY][0])["pending"]["month/2026-10"]
    assert row["phase"] == "publishing" and len(row["completed"]) == 2
    assert "validated_progress" not in row
    assert server.month_calls == 2


def test_short_pages_cannot_exceed_the_reserved_four_page_fence_before_products():
    server, _initial = seeded()
    server.month_page_size = 1
    with pytest.raises(BaselineRefreshStop):
        run(server)
    assert server.puts == [QUEUE_KEY]
    assert server.month_calls == 4


def test_larger_prefix_budget_is_rejected_before_sdk_io():
    server = PagedServer()
    with pytest.raises(ValueError):
        sdk_store(server).list_prefix_bounded("health/night-context/v1/2026/10/", max_keys=3102)
    assert server.calls == []


def test_public_history_publisher_drains_month_through_its_existing_storage_guard(monkeypatch):
    from test_history_baseline_integration import memory_job

    from pipeline.history_publish import publish

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _files = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server, _initial = seeded()
    result = publish(
        job,
        job.plan_sha,
        confirm=True,
        store=sdk_store(server, max_writes_per_run=100, max_write_bytes_per_run=2097152),
    )
    assert result["baseline_refresh"]["products_written"] == 2
    assert result["counts"] == {"published": 1}


def test_shared_health_client_pages_real_prefix_and_inventory_responses():
    from pipeline.r2_store import R2Store
    from scripts.benchmark_health_reuse import HealthClient

    client = HealthClient()
    prefix = "health/night-context/v1/2026/10/"
    client.objects = {
        prefix + f"synthetic-{i:04d}.json": {"Body": b"{}", "ContentType": "application/json"}
        for i in range(1001)
    }
    store = R2Store(client=client, bucket="synthetic")
    pages = list(client.get_paginator("list_objects_v2").paginate(Prefix=prefix, MaxKeys=1000))
    assert [page["KeyCount"] for page in pages] == [1000, 1]
    assert [page["IsTruncated"] for page in pages] == [True, False]
    assert pages[0]["NextContinuationToken"] == "1000"
    assert "NextContinuationToken" not in pages[1]
    assert store.list_prefix_bounded(prefix, max_keys=3101) == sorted(client.objects)
    assert store.inventory() == {"objects": 1001, "bytes": 2002}
    assert store.operations["list_pages"] == 4
    assert client.counts["list"] == 6
