"""Real SDK URL-encoded LIST contract; only physical HTTP and durable IO synthetic."""

from urllib.parse import parse_qs, quote, urlsplit
from xml.etree import ElementTree

import pytest
from botocore.awsrequest import AWSResponse
from history_storage_fixture import Raw
from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_publish as history


class EncodedServer(BaselineServer):
    def __init__(self, wire_key=None, encoding="url", keys=None):
        super().__init__()
        self.wire_key = wire_key
        self.encoding = encoding
        for key in keys if keys is not None else ["synthetic/literal%2F.json"]:
            self.objects[key] = (b"x", '"synthetic-existing"')

    def send(self, request):
        response = super().send(request)
        if parse_qs(urlsplit(request.url).query).get("list-type") != ["2"]:
            return response
        body = response.raw.read()
        response.raw.close()
        root = ElementTree.fromstring(body)
        if self.encoding is not None:
            ElementTree.SubElement(root, "EncodingType").text = self.encoding
        query = parse_qs(urlsplit(request.url).query, keep_blank_values=True)
        if "prefix" in query:
            ElementTree.SubElement(root, "Prefix").text = (
                quote(query["prefix"][0], safe="/")
                if self.encoding == "url"
                else query["prefix"][0]
            )
        for node in root.findall("Contents/Key"):
            node.text = (
                self.wire_key
                if self.wire_key is not None
                else quote(node.text, safe="/")
                if self.encoding == "url"
                else node.text
            )
        wire = ElementTree.tostring(root, encoding="utf-8")
        return AWSResponse(request.url, 200, {"content-length": str(len(wire))}, Raw(wire))


def test_history_inventory_accepts_sdk_once_decoded_url_keys(monkeypatch):
    job, _ = memory_job(monkeypatch, [])
    server = EncodedServer()
    store = sdk_store(server)
    guard = history.StorageGuard(store, set(), job.folder / "encoding")
    try:
        assert store.can_write_group(attempts=0, byte_count=0)
        assert store.write_budget_initialized
        assert guard.counts == {"get": 0, "list": 1, "put": 0}
        assert server.puts == []
    finally:
        history._cleanup(guard.close, store.client.close)


def test_public_source_queue_caller_accepts_same_auto_encoded_inventory(monkeypatch):
    from pipeline.granular import gzip_json
    from pipeline.night_baseline_refresh import publish_source

    memory_job(monkeypatch, [])
    server = EncodedServer()
    store = sdk_store(server)
    body = gzip_json(
        history.prepare(
            "hr",
            "2026-04-29",
            {"calendarDate": "2026-04-29", "heartRateValues": [[1777420800000, 61]]},
        )
    )
    try:
        assert publish_source(
            store, history.key("hr", "2026-04-29"), body, "application/json", encoding="gzip"
        )
        assert store.write_budget_initialized
        assert len(server.puts) == 3
    finally:
        store.client.close()


def test_invalid_percent_escape_cannot_initialize_write_inventory(monkeypatch):
    from pipeline.night_baseline_refresh import BaselineRefreshStop

    job, _ = memory_job(monkeypatch, [])
    server = EncodedServer(wire_key="synthetic/bad%GG.json")
    store = sdk_store(server)
    guard = history.StorageGuard(store, set(), job.folder / "encoding")
    try:
        with pytest.raises(BaselineRefreshStop):
            store.can_write_group(attempts=0, byte_count=0)
        assert not store.write_budget_initialized and server.puts == []
        assert guard.counts == {"get": 0, "list": 1, "put": 0}
    finally:
        history._cleanup(guard.close, store.client.close)


@pytest.mark.parametrize("automatic", [True, False])
def test_explicit_and_auto_encoding_keep_sdk_representation_and_logical_prefix(
    monkeypatch, automatic
):
    job, _ = memory_job(monkeypatch, [])
    server = EncodedServer()
    prefix = "synthetic/a b/"
    server.objects[prefix + "literal%2F.json"] = (b"x", '"prefixed"')
    store = sdk_store(server)
    guard = history.StorageGuard(store, set(), job.folder / "encoding")
    guard.prefixes = {prefix}
    try:
        options = {} if automatic else {"EncodingType": "url"}
        result = store.client.list_objects_v2(Bucket=store.bucket, Prefix=prefix, **options)
        assert result["Prefix"] == (prefix if automatic else "synthetic/a%20b/")
        assert [r["Key"] for r in result["Contents"]] == (
            ["synthetic/a b/literal%2F.json"]
            if automatic
            else ["synthetic/a%20b/literal%252F.json"]
        )
        assert guard.counts == {"get": 0, "list": 1, "put": 0} and server.puts == []
    finally:
        history._cleanup(guard.close, store.client.close)


@pytest.mark.parametrize("encoding", [None, "url"])
@pytest.mark.parametrize(
    "key",
    [
        "synthetic/a b.json",
        "synthetic/a+b.json",
        "synthetic/literal%252F.json",
        "synthetic/na\u00efve-\u00f8.json",
        "synthetic/" + "\u00f8" * 507,
    ],
    ids=["space", "plus", "literal_percent", "unicode", "max_utf8"],
)
def test_plain_and_encoded_keys_keep_one_logical_identity(monkeypatch, encoding, key):
    job, _ = memory_job(monkeypatch, [])
    server = EncodedServer(encoding=encoding, keys=[key])
    store = sdk_store(server)
    guard = history.StorageGuard(store, set(), job.folder / "encoding")
    guard.prefixes = {"synthetic/"}
    try:
        assert store.list_keys("synthetic/") == {key}
        assert guard.counts == {"get": 0, "list": 1, "put": 0} and server.puts == []
    finally:
        history._cleanup(guard.close, store.client.close)


@pytest.mark.parametrize("caller", ["history", "source"])
@pytest.mark.parametrize(
    "wire,encoding",
    [
        ("synthetic/bad%", "url"),
        ("synthetic/bad%GG", "url"),
        ("synthetic/bad%C3%28", "url"),
        ("synthetic/bad%00", "url"),
        ("synthetic/bad%0A", "url"),
        ("synthetic/plain", "unsupported"),
        ("x" * 1025, "url"),
    ],
    ids=["dangling", "bad_hex", "bad_utf8", "nul", "newline", "encoding", "key_bound"],
)
def test_invalid_wire_latches_before_write_or_any_subsequent_send(
    monkeypatch, caller, wire, encoding
):
    from pipeline.granular import gzip_json
    from pipeline.night_baseline_refresh import BaselineRefreshStop, publish_source

    job, _ = memory_job(monkeypatch, [])
    server = EncodedServer(wire_key=wire, encoding=encoding)
    store = sdk_store(server)
    guard = (
        history.StorageGuard(store, set(), job.folder / "encoding") if caller == "history" else None
    )
    body = gzip_json(
        history.prepare(
            "hr",
            "2026-04-29",
            {
                "calendarDate": "2026-04-29",
                "heartRateValues": [[1777420800000, 61]],
            },
        )
    )

    def call():
        return (
            store.can_write_group(attempts=0, byte_count=0)
            if caller == "history"
            else publish_source(
                store,
                history.key("hr", "2026-04-29"),
                body,
                "application/json",
                encoding="gzip",
            )
        )

    try:
        with pytest.raises((history.Stop, BaselineRefreshStop)):
            call()
        assert not store.write_budget_initialized and server.puts == []
        calls_after_failure = len(server.calls)
        with pytest.raises((history.Stop, BaselineRefreshStop)):
            call()
        assert len(server.calls) == calls_after_failure
        assert sum(q.get("list-type") == ["2"] for _, _, q in server.calls) == 1
    finally:
        history._cleanup(*([guard.close] if guard else []), store.client.close)


def test_encoded_pagination_keeps_continuation_token_opaque(monkeypatch):
    class Pages(EncodedServer):
        token = "opaque%GG+%252F"
        page = 0

        def send(self, request):
            response = super().send(request)
            query = parse_qs(urlsplit(request.url).query)
            if query.get("list-type") != ["2"]:
                return response
            self.page += 1
            assert self.page <= 2
            assert query.get("continuation-token") == (None if self.page == 1 else [self.token])
            body = response.raw.read()
            response.raw.close()
            root = ElementTree.fromstring(body)
            nodes = root.findall("Contents")
            for node in nodes:
                root.remove(node)
            root.append(nodes[self.page - 1])
            root.find("KeyCount").text = "1"
            root.find("IsTruncated").text = "true" if self.page == 1 else "false"
            if self.page == 1:
                ElementTree.SubElement(root, "NextContinuationToken").text = self.token
            else:
                ElementTree.SubElement(root, "ContinuationToken").text = self.token
            wire = ElementTree.tostring(root, encoding="utf-8")
            return AWSResponse(request.url, 200, {"content-length": str(len(wire))}, Raw(wire))

    job, _ = memory_job(monkeypatch, [])
    server = Pages(keys=["synthetic/a%2F.json", "synthetic/b+\u00f8.json"])
    store = sdk_store(server)
    guard = history.StorageGuard(store, set(), job.folder / "encoding")
    try:
        assert store.inventory() == {"objects": 2, "bytes": 2}
        assert server.page == 2 and guard.counts == {"get": 0, "list": 2, "put": 0}
        assert server.puts == []
    finally:
        history._cleanup(guard.close, store.client.close)
