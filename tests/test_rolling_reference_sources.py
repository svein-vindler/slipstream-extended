"""Rolling source reads through the actual bounded SDK/native-reader seams."""

import hashlib
import json
import re
from datetime import date, timedelta
from urllib.parse import parse_qs, quote, urlsplit
from xml.sax.saxutils import escape

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import BUCKET, Raw
from test_history_baseline_integration import BaselineServer, sdk_store
from test_night_baseline_corrections import calendar_store

from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.night_baseline_refresh import _IO, BaselineRefreshStop
from pipeline.night_reference_sources import validate_rolling_sources
from pipeline.r2_store import R2ReadBoundaryError


class RevisionServer(BaselineServer):
    def send(self, request):
        query = parse_qs(urlsplit(request.url).query)
        if request.method != "GET" or query.get("list-type") != ["2"]:
            return super().send(request)
        prefix = query.get("prefix", [""])[0]
        self.calls.append(("GET", prefix, query))
        if getattr(self, "deny_lists", False):
            body = b"<Error><Code>AccessDenied</Code></Error>"
            return AWSResponse(request.url, 403, {"content-length": str(len(body))}, Raw(body))
        keys = sorted(key for key in self.objects if key.startswith(prefix))
        offset = int(query.get("continuation-token", ["0"])[0])
        size = min(int(query.get("max-keys", ["1000"])[0]), 1000)
        page = keys[offset:offset + size]
        truncated = offset + size < len(keys)
        rows = "".join(
            "<Contents><Key>" + escape(quote(key, safe="")
                                      if getattr(self, "url_encoded", False) else key) + "</Key><Size>"
            + str(len(self.objects[key][0])) + "</Size>"
            + ("<ETag>" + escape(self.objects[key][1]) + "</ETag>"
               if not getattr(self, "omit_etag", False) else "")
            + "</Contents>" for key in page
        )
        token = ("<NextContinuationToken>" + str(offset + size)
                 + "</NextContinuationToken>" if truncated else "")
        encoding = "<EncodingType>url</EncodingType>" if getattr(self, "url_encoded", False) else ""
        body = ("<ListBucketResult><Name>" + BUCKET + "</Name>" + encoding + "<IsTruncated>"
                + str(truncated).lower() + "</IsTruncated><KeyCount>" + str(len(page))
                + "</KeyCount>" + rows + token + "</ListBucketResult>").encode()
        return AWSResponse(request.url, 200, {"content-length": str(len(body))}, Raw(body))


def seeded(first="2026-08-04", days=90, contexts=False):
    source = calendar_store(date.fromisoformat(first), days, contexts=contexts)
    server = RevisionServer()
    for key, snapshot in source.objects.items():
        server.objects[key] = (snapshot.data, snapshot.revision)
        server.metadata[key] = {"content-type": snapshot.content_type}
    return server, source


def test_bounded_revision_listing_preserves_exact_opaque_etag_and_size():
    server = RevisionServer()
    key = "health/hrv/2026/10/2026-10-25.json"
    server.objects[key] = (b"{}", '"opaque-multipart-3"')
    assert sdk_store(server).list_revisions_bounded("health/hrv/2026/10/", max_keys=63) == {
        key: {"revision": '"opaque-multipart-3"', "size": 2}
    }


def test_bounded_revision_listing_has_no_lastmodified_or_size_fallback():
    server = RevisionServer()
    server.objects["health/hrv/2026/10/2026-10-25.json"] = (b"{}", '"opaque"')
    server.omit_etag = True
    with pytest.raises(R2ReadBoundaryError):
        sdk_store(server).list_revisions_bounded("health/hrv/2026/10/", max_keys=63)


def test_native_revision_inventory_decodes_encoded_keys_exactly_once():
    server = RevisionServer()
    key = "health/night-context/v1/2026/10/literal%20space.json"
    server.objects[key] = (b"{}", '"opaque"')
    server.url_encoded = True
    io = _IO(sdk_store(server))
    try:
        assert io.list_revisions_bounded("health/night-context/v1/2026/10/", max_keys=3101) == {
            key: {"revision": '"opaque"', "size": 2}
        }
    finally:
        io.close()


def test_90_nights_reuse_verified_snapshots_across_overlapping_windows():
    server, source = seeded(days=91)
    store = sdk_store(server)
    days = [(date(2026, 8, 4) + timedelta(days=i)).isoformat() for i in range(90)]
    snapshots = {}
    reader = NativeNightReader(store, timezone="Europe/Oslo", snapshot_cache=snapshots)
    reader.prime_rolling_sources(days)
    nights = [reader.night(day) for day in days]
    assert len(nights) == 90 and all(n["status"] == "available" for n in nights)
    assert store.operations["get"] == 271
    before = store.operations["get"]
    for width in (28, 30, 90):
        reader = NativeNightReader(store, timezone="Europe/Oslo", snapshot_cache=snapshots)
        reader.prime_rolling_sources(days[-width:])
        assert [reader.night(day) for day in days[-width:]] == nights[-width:]
    assert store.operations["get"] == before
    assert store.operations["list_pages"] <= 64
    legacy = NativeNightReader(source, timezone="Europe/Oslo")
    assert nights[-1] == legacy.night(days[-1])
    shifted = days[1:] + ["2026-11-02"]
    reader = NativeNightReader(store, timezone="Europe/Oslo", snapshot_cache=snapshots)
    reader.prime_rolling_sources(shifted)
    assert all(reader.night(day)["status"] == "available" for day in shifted)
    assert store.operations["get"] == before + 3


def proof_for(reader, days):
    return {"objects": {key: binding for day in days
                        for key, binding in reader.dependencies[day]["objects"].items()},
            "lists": {key: binding for day in days
                      for key, binding in reader.dependencies[day]["lists"].items()}}


def test_native_wire_revision_fences_catch_mutation_even_with_verified_cache():
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)
    io = _IO(store)
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo", snapshot_cache={})
        reader.prime_rolling_sources(["2026-10-25"])
        reader.night("2026-10-25")
        proof = proof_for(reader, ["2026-10-25"])
        reads = store.operations["get"]
        validate_rolling_sources(io, proof)
        assert store.operations["get"] == reads
        key = "health/hrv/2026/10/2026-10-25.json"
        body, _revision = server.objects[key]
        server.objects[key] = (body, '"corrected"')
        with pytest.raises(NativeInputError, match="revision changed"):
            validate_rolling_sources(io, proof)
    finally:
        io.close()


def test_fresh_prime_replaces_stale_snapshot_and_detects_inventory_read_race():
    server, _ = seeded("2026-10-25", 1)
    store, cache = sdk_store(server), {}
    reader = NativeNightReader(store, snapshot_cache=cache)
    reader.prime_rolling_sources(["2026-10-25"])
    reader.night("2026-10-25")
    key = "health/hrv/2026/10/2026-10-25.json"
    body, _revision = server.objects[key]
    value = json.loads(body)
    value["readings"][0]["hrv_ms"] = 65
    server.objects[key] = (json.dumps(value).encode(), '"corrected"')
    before = store.operations["get"]
    reader = NativeNightReader(store, snapshot_cache=cache)
    reader.prime_rolling_sources(["2026-10-25"])
    assert reader.night("2026-10-25")["channels"]["hrv"]["points"][0]["value"] == 65
    assert store.operations["get"] == before + 1
    reader = NativeNightReader(store)
    reader.prime_rolling_sources(["2026-10-25"])
    server.objects[key] = (json.dumps(value).encode(), '"changed-again"')
    with pytest.raises(NativeInputError, match="after inventory"):
        reader.night("2026-10-25")


def test_missing_is_inventory_proven_and_new_context_is_never_hidden_by_cache():
    server, _ = seeded("2026-10-25", 1, contexts=True)
    store, cache = sdk_store(server), {}
    days = ["2026-10-25", "2026-10-26"]
    reader = NativeNightReader(store, timezone="Europe/Oslo", snapshot_cache=cache)
    reader.prime_rolling_sources(days)
    old = reader.night(days[0])
    assert reader.night(days[1])["status"] == "not_stored"
    oldproof = proof_for(reader, days)
    context = json.loads(next(body for key, (body, _) in server.objects.items()
                              if key.startswith("health/night-context/")))
    context["created_at"] = "2026-10-27T12:00:00.000Z"
    context["actual_night_timezone"] = "America/New_York"
    context["declared_fields"] = sorted(set(context["declared_fields"] + ["actual_night_timezone"]))
    context["changed_fields"] = ["actual_night_timezone"]
    core = {key: value for key, value in context.items() if key != "context_id"}
    context["context_id"] = "nctx-" + hashlib.sha256(json.dumps(
        dict(sorted(core.items())), ensure_ascii=False, separators=(",", ":")
    ).encode()).hexdigest()[:24]
    key = ("health/night-context/v1/2026/10/2026-10-25/"
           + re.sub(r"[-:.Z]", "", context["created_at"]) + "-" + context["context_id"] + ".json")
    server.objects[key] = (json.dumps(context).encode(), '"new-context"')
    server.metadata[key] = {"content-type": "application/json"}
    with pytest.raises(NativeInputError, match="context revision changed"):
        validate_rolling_sources(store, oldproof)
    reader = NativeNightReader(store, timezone="Europe/Oslo", snapshot_cache=cache)
    reader.prime_rolling_sources(days)
    fresh = reader.night(days[0])
    assert old["actual_night_timezone"] is None
    assert fresh["actual_night_timezone"] == "America/New_York"


def test_native_guard_rejects_parser_invented_revision():
    server, _ = seeded("2026-10-25", 1)
    store = sdk_store(server)
    io = _IO(store)

    def alter(parsed, **_):
        for row in parsed.get("Contents", []):
            row["ETag"] = '"invented"'

    store.client.meta.events.register_first("after-call.s3.ListObjectsV2", alter)
    try:
        with pytest.raises(BaselineRefreshStop, match="raw revision disagreement"):
            io.list_revisions_bounded("health/hrv/2026/10/", max_keys=63)
    finally:
        io.close()


@pytest.mark.parametrize("day", ["2026-03-29", "2026-10-25", "2027-01-01"])
def test_rolling_reader_preserves_legacy_local_dst_and_year_boundary_projection(day):
    server, source = seeded(day, 1, contexts=True)
    reader = NativeNightReader(sdk_store(server), timezone="Europe/Oslo")
    reader.prime_rolling_sources([day])
    assert reader.night(day) == NativeNightReader(source, timezone="Europe/Oslo").night(day)


def test_full_90_nights_and_both_fresh_fences_fit_original_native_budgets():
    server, _ = seeded(contexts=True)
    store = sdk_store(server)
    io = _IO(store)
    days = [(date(2026, 8, 4) + timedelta(days=i)).isoformat() for i in range(90)]
    try:
        reader = NativeNightReader(io, timezone="Europe/Oslo", snapshot_cache={})
        reader.prime_rolling_sources(days)
        nights = [reader.night(day) for day in days]
        assert all(night["status"] == "available" for night in nights)
        proof = proof_for(reader, days)
        validate_rolling_sources(io, proof)
        validate_rolling_sources(io, proof)
        assert io.counts() == {"get": 361, "list": 49, "put": 0}
        assert reader.read_bytes < 16777216 and reader.input_points <= 200000
        print({"synthetic_90_nights_two_fences": io.counts(),
               "source_bytes": reader.read_bytes, "source_points": reader.input_points})
    finally:
        io.close()


@pytest.mark.parametrize("mode", ["denied", "missing_etag"])
def test_revision_inventory_unknown_or_denied_is_never_absence(mode):
    server, _ = seeded("2026-10-25", 1)
    server.deny_lists = mode == "denied"
    server.omit_etag = mode == "missing_etag"
    io = _IO(sdk_store(server))
    try:
        with pytest.raises((NativeInputError, BaselineRefreshStop)):
            NativeNightReader(io).prime_rolling_sources(["2026-10-25"])
        assert io.counts()["get"] == 0
    finally:
        io.close()


@pytest.mark.parametrize("count,accepted", [(3100, True), (3102, False)])
def test_revision_month_inventory_is_complete_or_stops_within_four_pages(count, accepted):
    server = RevisionServer()
    prefix = "health/night-context/v1/2026/10/"
    for index in range(count):
        server.objects[prefix + f"synthetic-{index:04d}.json"] = (b"{}", '"opaque"')
    io = _IO(sdk_store(server))
    try:
        if accepted:
            assert len(io.list_revisions_bounded(prefix, max_keys=3101)) == count
        else:
            with pytest.raises(R2ReadBoundaryError):
                io.list_revisions_bounded(prefix, max_keys=3101)
        assert io.counts()["list"] == 4
    finally:
        io.close()
