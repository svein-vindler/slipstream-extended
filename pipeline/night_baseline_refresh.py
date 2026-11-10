"""Incremental closed night reference refresh; no provider calls."""

from __future__ import annotations

import calendar
import copy
import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlsplit

from botocore.exceptions import ClientError

from . import rolling_night_references as rolling
from .native_night import NativeInputError, NativeNightReader
from .night_products import baseline, canonical, normalize_args, storage_key
from .r2_store import R2BudgetError, missing_object
from .rolling_discovery import discovery_status, drain_discovery


def affected_periods(stream: str, day: str) -> dict[str, tuple[str, str]]:
    if stream not in {"sleep", "hrv", "signals"} or date.fromisoformat(day).isoformat() != day:
        raise ValueError("Invalid night source calendar")
    source = date.fromisoformat(day)
    result = {}
    # 36h sleep plus14h wall-clock margins: never identify HR's source day as its wake day.
    for offset in range(-3, 4) if stream == "signals" else (0,):
        wake = source + timedelta(days=offset)
        monday = wake - timedelta(days=wake.weekday())
        year, week, _ = monday.isocalendar()
        result[f"week/{year}-W{week:02d}"] = (
            monday.isoformat(),
            (monday + timedelta(days=6)).isoformat(),
        )
        first = wake.replace(day=1)
        last = wake.replace(day=calendar.monthrange(wake.year, wake.month)[1])
        result[f"month/{wake:%Y-%m}"] = (first.isoformat(), last.isoformat())
    return result


def is_closed(period: tuple[str, str], today: str) -> bool:
    return date.fromisoformat(period[1]) < date.fromisoformat(today)


QUEUE_KEY = "health/night-products/v1/refresh-queue.json"
MAX_PENDING = 128
MAX_QUEUE_BYTES = 262144
MAX_PERIODS = 2
MAX_LOCAL_PERIODS = 8


class BaselineRefreshStop(R2BudgetError):
    """A durable intent cannot become success after denial/unknown or interrupted publication."""


class ComponentBudgetDeferred(BaselineRefreshStop):
    """Known not-attempted component work; not a transport denial/unknown."""


class RecordedSourceHold(BaselineRefreshStop):
    """An already recorded unknown canonical operation, without new transport."""


def _source_maps(row):
    return (row["sources"], row.get("following_sources", {}))


def _held(row):
    return bool(
        row.get("inflight")
        or any(
            binding.get("state") == "intent"
            for entries in _source_maps(row)
            for binding in entries.values()
        )
        or row["phase"] == "publishing"
        and row.get("following_sources")
    )


def require(ok, category):
    if not ok:
        raise BaselineRefreshStop(category)


class _BodyBound:
    def __init__(self, raw, owner, cap, length):
        self.raw, self.owner, self.cap, self.length = raw, owner, cap, length
        self.bytes = 0
        self.complete = length == 0
        self.closed = False

    def read(self, amount=None, **_):
        if self.closed and self.complete:
            return b""
        require(not self.closed, "Baseline response already closed")
        requested = (
            self.cap - self.bytes + 1
            if amount is None or amount < 0
            else min(amount, self.cap - self.bytes + 1)
        )
        try:
            data = self.raw.read(requested)
        except Exception as error:
            self.owner.state["failed"] = True
            raise BaselineRefreshStop("Baseline response read failed") from error
        if not isinstance(data, bytes):
            self.owner.state["failed"] = True
            raise BaselineRefreshStop("Baseline response bytes unknown")
        self.bytes += len(data)
        self.owner.state["read_bytes"] += len(data)
        if self.bytes > self.cap or self.owner.state["read_bytes"] > 33554432:
            self.owner.state["failed"] = True
            raise BaselineRefreshStop("Baseline response byte bound")
        if self.length is not None and self.bytes == self.length:
            self.complete = True
        if not data:
            self.complete = True
            require(
                self.length is None or self.bytes == self.length,
                "Baseline response length mismatch",
            )
        if self.complete:
            # Complete/close before the SDK returns an ACK/body to a caller that
            # may persist a ready/completed checkpoint.
            self.close()
        return data

    def stream(self, amt=1024, **kwargs):
        while True:
            data = self.read(amt, **kwargs)
            if not data:
                return
            yield data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def close(self):
        if self.closed:
            return
        primary = sys.exception()
        secondary = None
        try:
            self.raw.close()
        except Exception as error:
            secondary = error
            self.owner.state["failed"] = True
        finally:
            self.closed = True
        if not self.complete:
            self.owner.state["failed"] = True
            if secondary is None:
                secondary = BaselineRefreshStop("Baseline body close unverified")
        if secondary is not None and primary is None:
            raise secondary

    def __getattr__(self, name):
        return getattr(self.raw, name)


def _list_text(value, encoding):
    """Validate one logical S3 field; percent decoding is strict and once-only."""
    from urllib.parse import unquote_to_bytes

    require(isinstance(value, str), "Baseline LIST text unverified")
    require(encoding in (None, "url"), "Baseline LIST encoding unsupported")
    try:
        require(
            len(value.encode("utf-8")) <= (3072 if encoding == "url" else 1024),
            "Baseline LIST text bound",
        )
        if encoding == "url":
            require(
                re.search(r"%(?![0-9a-fA-F]{2})", value) is None, "Baseline LIST escape unverified"
            )
            value = unquote_to_bytes(value).decode("utf-8", errors="strict")
        require(len(value.encode("utf-8")) <= 1024, "Baseline LIST logical text bound")
    except UnicodeError:
        raise BaselineRefreshStop("Baseline LIST UTF8 unverified") from None
    require(not any(ord(c) < 32 or ord(c) == 127 for c in value), "Baseline LIST text control")
    return value


def _raw_inventory(body):
    from xml.etree import ElementTree

    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise BaselineRefreshStop("Baseline LIST encoding unsupported") from error
    require("\x00" not in text, "Baseline LIST encoding unsupported")
    declaration = re.match(r"\s*<\?xml\b([^?]*?)\?>", text)
    if declaration:
        encoding = re.search(r"\bencoding\s*=\s*(['\"])(.*?)\1", declaration[1], re.IGNORECASE)
        require(
            encoding is None or encoding[2].lower() in {"utf-8", "utf8", "us-ascii", "ascii"},
            "Baseline LIST encoding unsupported",
        )
        if encoding and encoding[2].lower() in {"us-ascii", "ascii"}:
            require(text.isascii(), "Baseline LIST encoding disagreement")
    require(
        "<!DOCTYPE" not in text.upper() and "<!ENTITY" not in text.upper(),
        "Baseline LIST XML declarations forbidden",
    )
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError as error:
        raise BaselineRefreshStop("Baseline LIST XML unverified") from error

    def name(node):
        return node.tag.rsplit("}", 1)[-1]

    require(name(root) == "ListBucketResult", "Baseline LIST root unverified")

    def single(parent, key):
        rows = [node for node in parent if name(node) == key]
        require(len(rows) == 1 and not list(rows[0]), "Baseline LIST field unverified")
        return rows[0].text or ""

    def optional(key):
        nodes = [node for node in root if name(node) == key]
        require(len(nodes) <= 1, "Baseline LIST field unverified")
        return single(root, key) if nodes else None

    encoding = optional("EncodingType")
    require(encoding in (None, "url"), "Baseline LIST encoding unsupported")
    prefix = optional("Prefix")
    if prefix is not None:
        _list_text(prefix, encoding)
    truncated = single(root, "IsTruncated").strip()
    require(truncated in {"true", "false"}, "Baseline LIST truncation unverified")
    rows = [node for node in root if name(node) == "Contents"]
    require(len(rows) <= 1000, "Baseline LIST row bound")
    result = []
    for row in rows:
        key, size = single(row, "Key"), single(row, "Size").strip()
        logical = _list_text(key, encoding)
        require(
            0 < len(logical.encode()) <= 1024
            and "\x00" not in logical
            and re.fullmatch(r"[0-9]{1,20}", size),
            "Baseline LIST key/size unverified",
        )
        result.append({"Key": key, "Size": int(size)})
    token = single(root, "NextContinuationToken") if truncated == "true" else None
    require(token is None or 0 < len(token) <= 4096, "Baseline LIST continuation unverified")
    return {
        "IsTruncated": truncated == "true",
        "Contents": result,
        "NextContinuationToken": token,
        "EncodingType": encoding,
        "Prefix": prefix,
    }


def _list_sdk_text(value, encoding, context):
    """Validate logical names without decoding the SDK's auto-decoded text twice."""
    effective = encoding if not (context or {}).get("encoding_type_auto_set") else None
    return _list_text(value, effective)


def _inventory_for_sdk(raw, encoding, context):
    """Match the SDK's auto decode while retaining opaque tokens and raw evidence."""
    if raw is None:
        return None
    require(encoding == raw["EncodingType"], "Baseline LIST parsed/raw encoding disagreement")
    if encoding == "url" and (context or {}).get("encoding_type_auto_set") is True:
        raw = {
            **raw,
            "Contents": [
                {**row, "Key": _list_text(row["Key"], encoding)} for row in raw["Contents"]
            ],
        }
        if raw["Prefix"] is not None:
            raw["Prefix"] = _list_text(raw["Prefix"], encoding)
    return raw


class _WireBudget:
    """Scoped hooks; counters span only this component on the shared client."""

    def __init__(self, client, *, budget_owner=None):
        require(
            not getattr(client, "_night_baseline_observer_active", False),
            "Nested baseline observer",
        )
        self.client = client
        # A fresh serial stage gets fresh component limits. The client retains
        # the previous failure latch so new_stage cannot clear an unknown send.
        previous = getattr(client, "_night_baseline_budget_state", None)
        require(previous is None or not previous["failed"], "Prior component transport failure")
        owner = client if budget_owner is None else budget_owner
        self.state = getattr(owner, "_night_baseline_budget_state", None)
        if self.state is None:
            self.state = {
                "counts": {"get": 0, "list": 0, "put": 0},
                "failed": False,
                "read_bytes": 0,
            }
            owner._night_baseline_budget_state = self.state
        client._night_baseline_budget_state = self.state
        self.deadline = time.monotonic() + 900
        self.bodies = []
        self.binding = None
        self.operation = None
        self.strict_revision_listing = False
        self.raw_revision_inventory = None
        session = client._endpoint.http_session
        original = session.send

        def send(request, **kwargs):
            old = request.stream_output
            try:
                request.stream_output = True
                response = original(request)
            except Exception as error:
                self.state["failed"] = True
                raise BaselineRefreshStop("Baseline transport outcome unknown") from error
            finally:
                request.stream_output = old
            # Botocore evaluates retries before after-call. Latch the observed
            # failure at the native response boundary, including injected clients.
            if response.status_code not in {200, 404}:
                self.state["failed"] = True
            try:
                require(
                    response.__dict__.get("_content") is None,
                    "Baseline body preloaded outside bound",
                )
                cap = (
                    65536
                    if response.status_code >= 300 or self.operation == "put"
                    else 1048576
                    if self.operation == "list"
                    else 524288
                )
                raw_length = response.headers.get("content-length")
                try:
                    length = int(raw_length) if raw_length is not None else None
                except (ValueError, TypeError) as error:
                    raise BaselineRefreshStop("Baseline declared length invalid") from error
                if request.method == "HEAD":
                    length = 0
                require(length is None or 0 <= length <= cap, "Baseline declared body bound")
                bounded = _BodyBound(response.raw, self, cap, length)
                self.bodies.append(bounded)
                response.raw = bounded
                if length == 0:
                    bounded.close()
                if self.operation == "list" and response.status_code == 200:
                    wire = bounded.read()
                    if not bounded.complete:
                        require(bounded.read(1) == b"", "Baseline LIST body bound")
                    self.raw_inventory = _raw_inventory(wire)
                    if self.strict_revision_listing:
                        from .night_reference_sources import parse_revision_inventory

                        self.raw_revision_inventory = parse_revision_inventory(wire)
                    # This cache is assigned only after the bounded original wire
                    # is fully read/closed/validated, before the SDK parser sees it.
                    response._content = wire
                return response
            except Exception:
                self.state["failed"] = True
                try:
                    response.raw.close()
                except Exception:
                    pass  # Preserve the first header/preload/bound failure.
                raise

        session.send = send
        self.binding = (session, original, send)
        client.meta.events.register_first(
            "before-send.s3", self.before, unique_id="night-baseline-budget-before-v1"
        )
        client.meta.events.register_first(
            "after-call.s3", self.after, unique_id="night-baseline-budget-after-v1"
        )
        client._night_baseline_observer_active = True

    def before(self, request, **_):
        require(
            not self.state["failed"] and time.monotonic() < self.deadline,
            "Night baseline prior failure/deadline",
        )
        kind = (
            "list"
            if "list-type=" in urlsplit(request.url).query
            else "get"
            if request.method in {"GET", "HEAD"}
            else "put"
            if request.method == "PUT"
            else "unknown"
        )
        require(kind in self.state["counts"], "Night baseline unexpected operation")
        if self.state["counts"][kind] >= {"get": 400, "list": 100, "put": 250}[kind]:
            raise ComponentBudgetDeferred("Night baseline component operation budget")
        self.state["counts"][kind] += 1
        self.operation = kind
        self.raw_inventory = None
        self.raw_revision_inventory = None

    def after(self, http_response, parsed, context=None, **_):
        code = parsed.get("Error", {}).get("Code")
        if http_response.status_code == 200 and code is None:
            if self.operation == "list":
                raw = _inventory_for_sdk(self.raw_inventory, parsed.get("EncodingType"), context)
                require(
                    raw is not None
                    and parsed.get("IsTruncated") is raw["IsTruncated"]
                    and [
                        {"Key": item.get("Key"), "Size": item.get("Size")}
                        for item in parsed.get("Contents", [])
                        if isinstance(item, dict)
                    ]
                    == raw["Contents"]
                    and parsed.get("NextContinuationToken") == raw["NextContinuationToken"]
                    and parsed.get("EncodingType") == raw["EncodingType"]
                    and parsed.get("Prefix") == raw["Prefix"],
                    "Baseline LIST parsed/raw disagreement",
                )
                if self.strict_revision_listing:
                    rows = parsed.get("Contents", [])
                    revisions = {
                        _list_sdk_text(item.get("Key"), parsed.get("EncodingType"), context): {
                            "revision": item.get("ETag"), "size": item.get("Size")
                        } for item in rows
                    }
                    require(self.raw_revision_inventory is not None
                            and len(revisions) == len(rows)
                            and revisions == self.raw_revision_inventory,
                            "Rolling LIST raw revision disagreement")
                rows = parsed.get("Contents", [])
                token = parsed.get("NextContinuationToken")
                valid = (
                    type(parsed.get("IsTruncated")) is bool
                    and (
                        not parsed["IsTruncated"]
                        or isinstance(token, str)
                        and 0 < len(token) <= 4096
                    )
                    and isinstance(rows, list)
                    and len(rows) <= 1000
                    and all(
                        isinstance(row, dict)
                        and isinstance(row.get("Key"), str)
                        and 0
                        < len(
                            _list_sdk_text(row["Key"], parsed.get("EncodingType"), context).encode()
                        )
                        <= 1024
                        and "\x00" not in row["Key"]
                        and type(row.get("Size")) is int
                        and row["Size"] >= 0
                        for row in rows
                    )
                )
                if not valid:
                    self.state["failed"] = True
                    raise BaselineRefreshStop("Baseline LIST inventory unverified")
            return
        if http_response.status_code == 404 and code in {"404", "NoSuchKey", "NotFound"}:
            return
        self.state["failed"] = True
        raise BaselineRefreshStop("Night baseline storage denied or unknown")

    def close(self):
        primary = None
        for body in self.bodies:
            try:
                body.close()
            except Exception as error:
                self.state["failed"] = True
                if primary is None:
                    primary = error
        if self.binding:
            session, original, wrapped = self.binding
            if session.send is not wrapped:
                self.state["failed"] = True
                if primary is None:
                    primary = BaselineRefreshStop("Baseline sender ownership changed")
            else:
                session.send = original
        self.client.meta.events.unregister(
            "before-send.s3", unique_id="night-baseline-budget-before-v1"
        )
        self.client.meta.events.unregister(
            "after-call.s3", unique_id="night-baseline-budget-after-v1"
        )
        self.client._night_baseline_observer_active = False
        if primary is not None:
            raise primary


class _IO:
    LIMITS = {"get": 400, "list": 100, "put": 250}

    def __init__(self, store):
        self.store, self.bytes, self.closed = store, 0, False
        client = getattr(store, "client", None)
        self.observer = (
            _WireBudget(client, budget_owner=store)
            if client is not None and getattr(client, "meta", None) is not None
            else None
        )
        self.manual = dict.fromkeys(self.LIMITS, 0)
        self.started = dict(getattr(store, "operations", {}))
        if self.observer is None:
            owner = client if client is not None else store
            self.state = getattr(owner, "_night_baseline_logical_budget", None)
            if self.state is None:
                self.state = {"counts": dict.fromkeys(self.LIMITS, 0), "read_bytes": 0}
                owner._night_baseline_logical_budget = self.state
            self.base = dict(self.state["counts"])

    def counts(self):
        if self.observer is not None:
            observed = dict(self.observer.state["counts"])
            physical_counts = getattr(self.store, "physical_counts", None)
            if callable(physical_counts):
                physical = physical_counts()
                # The enclosing publisher also performs source/index I/O while
                # this observer is detached. Both counters cover the same sends.
                return {kind: max(observed[kind], physical[kind]) for kind in self.LIMITS}
            return observed
        operations = getattr(self.store, "operations", {})
        delta = {
            name: operations.get(name, 0) - self.started.get(name, 0)
            for name in ("get", "head", "list_pages", "put")
        }
        require(
            all(value >= 0 for value in delta.values()), "Component accounting reset during helper"
        )
        observed = {
            "get": delta["get"] + delta["head"],
            "list": delta["list_pages"],
            "put": delta["put"],
        }
        return {
            kind: self.base[kind] + max(self.manual[kind], observed[kind]) for kind in self.LIMITS
        }

    def require_capacity(self, kind, amount=1):
        if self.observer is not None:
            require(not self.observer.state["failed"], "Prior component transport failure")
        counts = self.counts()
        if (
            any(counts[name] > maximum for name, maximum in self.LIMITS.items())
            or counts[kind] + amount > self.LIMITS[kind]
        ):
            raise ComponentBudgetDeferred("Known remaining component budget")

    def before(self, kind):
        self.require_capacity(kind)
        if self.observer is None:
            self.manual[kind] += 1

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.observer is not None:
            self.observer.close()
        else:
            self.state["counts"] = self.counts()
            self.state["read_bytes"] += self.bytes

    def __getattr__(self, name):
        return getattr(self.store, name)

    def get_snapshot(self, key, *, max_bytes):
        self.before("get")
        value = self.store.get_snapshot(key, max_bytes=max_bytes)
        self.bytes += len(value.data)
        prior = 0 if self.observer else self.state["read_bytes"]
        require(prior + self.bytes <= 33554432, "Night baseline stored byte budget")
        return value

    def matches_object(self, *args, **kwargs):
        method = getattr(self.store, "matches_object", None)
        if not callable(method):
            return False
        self.before("get")
        return method(*args, **kwargs)

    def list_prefix_bounded(self, prefix, *, max_keys):
        self.before("list")
        return self.store.list_prefix_bounded(prefix, max_keys=max_keys)

    def list_revisions_bounded(self, prefix, *, max_keys):
        self.before("list")
        if self.observer is not None:
            self.observer.strict_revision_listing = True
        try:
            return self.store.list_revisions_bounded(prefix, max_keys=max_keys)
        finally:
            if self.observer is not None:
                self.observer.strict_revision_listing = False

    def create_with_receipt(self, *args, **kwargs):
        self.before("put")
        return self.store.create_with_receipt(*args, **kwargs)

    def put_if_revision(self, *args, **kwargs):
        self.before("put")
        return self.store.put_if_revision(*args, **kwargs)


def _snapshot(store, key, max_bytes=262144):
    try:
        return store.get_snapshot(key, max_bytes=max_bytes)
    except ClientError as error:
        if missing_object(error):
            return None
        raise


def _decode(body, limit=262144):
    import gzip
    import io

    if body[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as handle:
            body = handle.read(limit + 1)
    require(len(body) <= limit, "Night baseline decoded byte budget")
    value = json.loads(
        body,
        parse_constant=lambda _: (_ for _ in ()).throw(
            BaselineRefreshStop("Nonfinite baseline data")
        ),
    )
    require(isinstance(value, dict), "Unverified baseline object")
    return value


def decode_product_snapshot(snapshot):
    """Decode bounded product bytes without changing the observed wire metadata."""
    import gzip
    import io

    require(type(snapshot.data) is bytes and len(snapshot.data) <= 262144
            and snapshot.content_type == "application/json" and snapshot.encoding in (None, "gzip")
            and isinstance(snapshot.revision, str) and bool(snapshot.revision),
            "Stored reference metadata unverified")
    if snapshot.encoding == "gzip":
        require(snapshot.data[:2] == b"\x1f\x8b", "Stored reference encoding unverified")
        with gzip.GzipFile(fileobj=io.BytesIO(snapshot.data)) as stream:
            body = stream.read(262145)
    else:
        require(snapshot.data[:2] != b"\x1f\x8b", "Stored reference encoding unverified")
        body = snapshot.data
    _decode(body)
    return body


def _read_queue(store):
    before = _snapshot(store, QUEUE_KEY)
    queue = (
        _decode(before.data)
        if before
        else {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {}}
    )
    require(
        queue.get("schema_version") == 1
        and queue.get("kind") == "night-baseline-refresh-queue"
        and isinstance(queue.get("pending"), dict)
        and len(queue["pending"]) <= MAX_PENDING,
        "Unverified night baseline queue",
    )
    try:
        rolling.validate(queue)
    except (TypeError, ValueError, OverflowError) as error:
        raise BaselineRefreshStop("Unverified rolling endpoint registry") from error
    for name, item in queue["pending"].items():
        require(
            isinstance(item, dict)
            and item.get("phase") in {"dirty", "publishing"}
            and isinstance(item.get("sources"), dict),
            "Unverified queued period",
        )
        require(
            isinstance(item.get("following_sources", {}), dict),
            "Unverified following source generation",
        )
        for key, binding in list(item["sources"].items()) + list(
            item.get("following_sources", {}).items()
        ):
            kind, source_day = _source(key)
            require(
                (name in affected_periods(kind, source_day)
                 or "rolling_window" in item and rolling.row_identity(name, item)
                 and rolling.source_range(kind, source_day, item["rolling_window"])[0]
                 <= item["end_date"]
                 <= rolling.source_range(kind, source_day, item["rolling_window"])[1])
                and isinstance(binding, dict)
                and binding.get("state") in {"intent", "ready"}
                and isinstance(binding.get("sha256"), str)
                and re.fullmatch(r"[a-f0-9]{64}", binding["sha256"]),
                "Unverified queued source",
            )
            if binding["state"] == "ready":
                require(
                    isinstance(binding.get("revision"), str)
                    and 2 < len(binding["revision"]) <= 1024
                    and binding["revision"].startswith('"')
                    and binding["revision"].endswith('"'),
                    "Queued source revision missing",
                )
        start, end = item.get("start_date"), item.get("end_date")
        require(
            ("rolling_window" in item and rolling.row_identity(name, item)
             and bool(rolling.intersect(queue.get("rolling", {}).get("eligible", []), end, end)))
            or (name in affected_periods("hrv", start)
                and affected_periods("hrv", start)[name] == (start, end)),
            "Unverified queued calendar",
        )
    return queue, before


def _cas(store, key, data, before, *, encoding=None):
    if before is None:
        receipt = store.create_with_receipt(key, data, "application/json", encoding=encoding)
    else:
        receipt = store.put_if_revision(
            key, data, "application/json", revision=before.revision, encoding=encoding
        )
    require(
        receipt.accepted and receipt.revision is not None,
        "Conditional write missing acknowledgement",
    )
    after = store.get_snapshot(key, max_bytes=max(len(data), 262144))
    require(
        after.data == data
        and after.revision == receipt.revision
        and after.content_type == "application/json"
        and after.encoding == encoding,
        "Conditional write exact readback failed",
    )
    return after


def _save_queue(store, queue, before):
    rolling.validate(queue)
    body = canonical(queue).encode()
    require(
        len(body) <= MAX_QUEUE_BYTES and len(queue["pending"]) <= MAX_PENDING,
        "Night baseline pending capacity",
    )
    return _cas(store, QUEUE_KEY, body, before)


def _source(key):
    match = re.fullmatch(
        r"health/(sleep/v1|hrv|signals/v1)/(\d{4})/(\d{2})/(\d{4}-\d{2}-\d{2})\.json", key
    )
    require(match is not None, "Only canonical night sources may invalidate references")
    kind, year, month, day = match.groups()
    require(
        day[:4] == year and day[5:7] == month and date.fromisoformat(day).isoformat() == day,
        "Source calendar mismatch",
    )
    return {"sleep/v1": "sleep", "hrv": "hrv", "signals/v1": "signals"}[kind], day


def _validate_source(kind, day, value):
    from .native_night import instant

    if kind == "sleep":
        first, last = instant(value.get("sleep_start_gmt")), instant(value.get("sleep_end_gmt"))
        return bool(
            first
            and last
            and 0 < (last - first).total_seconds() <= 129600
            and abs((last.date() - date.fromisoformat(day)).days) <= 1
        )
    channel = value if kind == "hrv" else value.get("channels", {}).get("heart_rate")
    if channel is None:
        return None
    require(
        isinstance(channel, dict) and isinstance(channel.get("readings"), list),
        "Unverified native readings",
    )
    if kind == "signals":
        require(
            channel.get("status") in {"available", "no_data", "invalid_response"}
            and channel.get("unit") == "bpm"
            and not (channel["status"] == "no_data" and channel["readings"]),
            "Unverified HR channel",
        )
    usable = False
    for point in channel["readings"]:
        require(isinstance(point, dict), "Malformed native point")
        number = point.get("hrv_ms" if kind == "hrv" else "value")
        require(
            type(number) in (int, float) and math.isfinite(number) and number > 0,
            "Malformed native value",
        )
        clock = instant(point.get("timestamp"), point.get("timestamp_unit"))
        documented = (
            point.get("time_basis") == "utc"
            or point.get("time_basis") is None
            and isinstance(point.get("raw"), dict)
            and clock
            and instant(point["raw"].get("readingTimeGMT")) == clock
        )
        usable = usable or bool(clock and documented)
    return usable


def source_capacity(store, requested, *, checkpoint_puts=6):
    """Admit bounded source work before fetch, retaining caller checkpoints."""
    if not requested or store.remaining_write_attempts <= checkpoint_puts:
        return 0

    def remaining_periods():
        io = _IO(store)
        try:
            queue, _ = _read_queue(io)
            return MAX_PENDING - len(queue["pending"]), max(0, 400 - io.counts()["get"])
        except ComponentBudgetDeferred:
            return 0, 0
        finally:
            primary = sys.exception()
            try:
                io.close()
            except Exception:
                if primary is None:
                    raise

    room, reads = remaining_periods()
    if room < 2 and reads:
        refresh_pending(store, reserve_writes=checkpoint_puts)
        room, reads = remaining_periods()
    # Sleep/HRV can introduce a week and month for each newly selected day.
    return min(
        requested,
        max(0, (store.remaining_write_attempts - checkpoint_puts) // 3),
        room // 2,
        reads // 6,
    )


def publish_source(store, key, data, content_type, *, encoding=None):
    """Public importer hook: dirty intent BEFORE conditional canonical write/ACK/readback.

    Reuse this for NEW bounded archive publishers. It never reads a provider or
    changes old external operators. The caller supplies already normalized bytes.
    """
    require(
        content_type == "application/json" and encoding in (None, "gzip"),
        "Canonical source metadata",
    )
    kind, day = _source(key)
    io = _IO(store)
    try:
        incoming = _decode(data, 1048576 if kind == "signals" else 2097152)
        require(
            incoming.get("schema_version") == 1
            and incoming.get("source") == "garmin"
            and incoming.get("date") == day,
            "Unverified canonical source",
        )
        relevant = _validate_source(kind, day, incoming)
        queue, revision = _read_queue(io)
        if any(
            entries.get(key, {}).get("state") == "intent"
            for row in queue["pending"].values()
            for entries in _source_maps(row)
        ):
            raise RecordedSourceHold("Recorded unknown canonical source remains HOLD")
        matches = getattr(io, "matches_object", None)
        if callable(matches) and matches(key, data, content_type, encoding=encoding):
            return False
        before = _snapshot(io, key, 524288)
        if (
            before is not None
            and before.data == data
            and before.content_type == content_type
            and before.encoding == encoding
        ):
            return False
        previous = _decode(before.data, 2097152) if before else None
        # A known empty refresh is never permission to erase a good source curve.
        if previous and _validate_source(kind, day, previous) and not relevant:
            return False
        if relevant is None:
            _cas(io, key, data, before, encoding=encoding)
            return True
        targets = affected_periods(kind, day)
        digest = hashlib.sha256(data).hexdigest()
        destinations = {}
        for name, (first, last) in targets.items():
            row = queue["pending"].setdefault(
                name,
                {
                    "start_date": first,
                    "end_date": last,
                    "phase": "dirty",
                    "sources": {},
                    "completed": {},
                },
            )
            following = _held(row) or row["phase"] == "publishing"
            field = "following_sources" if following else "sources"
            destinations[name] = field
            row.setdefault(field, {})[key] = {"sha256": digest, "state": "intent"}
            if not following:
                row.pop("blocked", None)
                row["completed"] = {}
                row.pop("validated_progress", None)
        committed = copy.deepcopy(queue)
        if kind == "sleep" and relevant:
            rolling.register(committed, [day])
        rolling.invalidate(committed, kind, day)
        rolling.validate(committed)
        committed_bytes = len(canonical(committed).encode())
        require(
            store.can_write_group(
                attempts=3,
                byte_count=len(data)
                + len(canonical(queue).encode()) + committed_bytes
                + len(targets) * 2200
                + 4096,
            ),
            "Canonical source group budget deferred before intent",
        )
        require(
            committed_bytes + len(targets) * 2200 + 4096 <= MAX_QUEUE_BYTES,
            "Committed source queue capacity before intent",
        )
        io.require_capacity("get", 3)
        revision = _save_queue(io, queue, revision)
        after = _cas(io, key, data, before, encoding=encoding)
        for name in targets:
            queue["pending"][name][destinations[name]][key] = {
                "sha256": digest,
                "state": "ready",
                "revision": after.revision,
            }
        if kind == "sleep" and relevant:
            rolling.register(queue, [day])
        rolling.invalidate(queue, kind, day)
        _save_queue(io, queue, revision)
        return True
    except BaselineRefreshStop:
        raise
    except Exception as error:
        raise BaselineRefreshStop("Canonical night publication stopped") from error
    finally:
        primary = sys.exception()
        try:
            io.close()
        except Exception as error:
            if primary is None:
                raise BaselineRefreshStop("Canonical observer cleanup failed") from error


def enqueue_verified_sources(store, sources):
    """Explicit bounded activation/reconciliation, requiring fresh exact stored proofs.

    No source write, provider call, old-job adoption or historical enumeration.
    Each item contains only key, quoted revision and raw body sha256.
    """
    require(
        isinstance(sources, list) and 1 <= len(sources) <= 14, "Choose1-14 verified source proofs"
    )
    io = _IO(store)
    try:
        selected = []
        for item in sources:
            require(
                isinstance(item, dict) and set(item) == {"key", "revision", "sha256"},
                "Exact source proof required",
            )
            kind, day = _source(item["key"])
            snapshot = _snapshot(io, item["key"], 524288)
            require(
                snapshot is not None
                and snapshot.revision == item["revision"]
                and hashlib.sha256(snapshot.data).hexdigest() == item["sha256"],
                "Existing source proof changed",
            )
            value = _decode(snapshot.data, 2097152)
            require(
                value.get("schema_version") == 1
                and value.get("source") == "garmin"
                and value.get("date") == day,
                "Existing canonical source unverified",
            )
            _validate_source(kind, day, value)
            selected.append((item, affected_periods(kind, day), kind, day,
                             _validate_source(kind, day, value)))
        queue, before = _read_queue(io)
        for item, periods, kind, day, relevant in selected:
            for name, (first, last) in periods.items():
                row = queue["pending"].setdefault(
                    name,
                    {
                        "start_date": first,
                        "end_date": last,
                        "phase": "dirty",
                        "sources": {},
                        "completed": {},
                    },
                )
                require(
                    row["phase"] == "dirty"
                    and not row.get("inflight")
                    and all(value.get("state") == "ready" for value in row["sources"].values()),
                    "Unresolved old intent cannot be adopted",
                )
                row["sources"][item["key"]] = {
                    "sha256": item["sha256"],
                    "revision": item["revision"],
                    "state": "ready",
                }
                row.pop("blocked", None)
                row["completed"] = {}
                row.pop("validated_progress", None)
            if kind == "sleep" and relevant:
                rolling.register(queue, [day])
            rolling.invalidate(queue, kind, day)
        require(
            store.can_write_group(attempts=1, byte_count=len(canonical(queue).encode())),
            "Verified enqueue budget deferred",
        )
        _save_queue(io, queue, before)
        return {"queued_sources": len(selected), "pending_periods": len(queue["pending"])}
    finally:
        primary = sys.exception()
        try:
            io.close()
        except Exception:
            if primary is None:
                raise


def register_rolling_endpoints(store, sources, *, today=None, prepared_manifest=None,
                               prepared_target_keys=None):
    """Activate only endpoints backed by fresh exact, usable stored sleep proofs.

    Ordinary re-registration is idempotent. A sealed bulk caller can request
    exact still-unpublished slots even on previously clean known endpoints.
    Source corrections retain the ordinary hooks for later affected endpoints.
    """
    require(isinstance(sources, list) and 1 <= len(sources) <= 14,
            "Choose1-14 verified endpoint proofs")
    if today is not None:
        require(date.fromisoformat(today).isoformat() == today, "Exact endpoint day required")
    if prepared_manifest is not None:
        _sealed_preparation(prepared_manifest)
        require(len(sources) <= 7, "Choose at most seven prepared endpoints")
    selected_targets = _selected_prepared_targets(prepared_manifest, prepared_target_keys)
    local = prepared_manifest.reader_store() if prepared_manifest is not None else None
    io = _IO(store)
    try:
        queue, before = _read_queue(io)
        days = []
        prepared_proofs = []
        for item in sources:
            require(isinstance(item, dict) and set(item) == {"key", "revision", "sha256"},
                    "Exact endpoint proof required")
            kind, day = _source(item["key"])
            require(kind == "sleep" and (today is None or day <= today),
                    "Only available sleep endpoints may be registered")
            require(not any(entries.get(item["key"], {}).get("state") == "intent"
                            for row in queue["pending"].values() for entries in _source_maps(row)),
                    "Unknown sleep source cannot activate endpoints")
            snapshot = (_snapshot(io, item["key"], 524288) if local is None
                        else local.get_snapshot(item["key"], max_bytes=524288))
            require(snapshot is not None and snapshot.revision == item["revision"]
                    and hashlib.sha256(snapshot.data).hexdigest() == item["sha256"],
                    "Endpoint source proof changed")
            value = _decode(snapshot.data, 2097152)
            require(value.get("schema_version") == 1 and value.get("source") == "garmin"
                    and value.get("date") == day and _validate_source(kind, day, value),
                    "Endpoint sleep unavailable")
            if prepared_manifest is not None:
                target = max((target for target in prepared_manifest.targets
                    if target["endpoint"] == day), key=lambda target: target["window_days"], default=None)
                require(target is not None, "Prepared endpoint outside sealed scope")
                artifact = prepared_manifest.product_for(target["key"])
                _, _, proof = _prepared_value(artifact, {"start_date": target["args"]["start_date"],
                    "end_date": day, "rolling_window": target["window_days"],
                    "channel": target["channel"]})
                binding = proof["objects"].get(item["key"], {})
                require(binding.get("status") == "present" and binding.get("revision") == snapshot.revision
                        and binding.get("size") == len(snapshot.data)
                        and binding.get("sha256") == item["sha256"], "Prepared endpoint proof changed")
                prepared_proofs.append({"proof": proof})
            days.append(day)
        if prepared_proofs:
            from .night_reference_sources import validate_rolling_sources

            validate_rolling_sources(io, _combined_reference_proof(prepared_proofs))
        if prepared_manifest is None:
            rolling.register(queue, days)
        else:
            require(prepared_target_keys is None or all(
                target["endpoint"] in days for target in selected_targets),
                "Prepared targets outside registered endpoints")
            _register_prepared_endpoints(queue, days, selected_targets,
                                         include_existing=prepared_target_keys is not None)
        require(store.can_write_group(attempts=1, byte_count=len(canonical(queue).encode())),
                "Endpoint registration budget deferred")
        if before is None or before.data != canonical(queue).encode():
            _save_queue(io, queue, before)
        return {"registered_endpoints": len(set(days)),
                "rolling_remaining": rolling.status(queue, today=today),
                "pending_periods": len(queue["pending"])}
    finally:
        io.close()


def refresh_recent_night_references(store, *, wake_dates, timezone=None, snapshot_cache=None):
    """Finish a bounded current-night cohort through the ordinary local producer.

    Component counters and the store's write/inventory budget remain shared.
    Unrelated calendar work retains its ordinary caller's separate tail drain.
    """
    require(isinstance(wake_dates, (list, tuple)) and len(wake_dates) <= 3
            and all(isinstance(day, str) and date.fromisoformat(day).isoformat() == day
                    for day in wake_dates), "Choose at most three exact wake dates")
    cache = {} if snapshot_cache is None else snapshot_cache
    if not wake_dates:
        report = refresh_pending(store, timezone=timezone, snapshot_cache=cache)
        return {**report, "selected_remaining": None, "selected_status": "not_requested"}
    from .native_night import zone

    timezone = timezone or os.environ.get("HEALTH_TIMEZONE")
    if zone(timezone) is None:
        return {"status": "calendar_timezone_required", "selected_remaining": 6 * len(set(wake_dates))}
    today = _today(timezone)
    dates = sorted(set(wake_dates))
    require(all(day <= today for day in dates), "Wake endpoint is not yet available")
    sources, available = [], []
    io = _IO(store)
    try:
        for day in dates:
            key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
            snapshot = _snapshot(io, key, 524288)
            if snapshot is None:
                continue
            value = _decode(snapshot.data, 2097152)
            require(value.get("schema_version") == 1 and value.get("source") == "garmin"
                    and value.get("date") == day and _validate_source("sleep", day, value),
                    "Recent endpoint sleep is unverified")
            sources.append({"key": key, "revision": snapshot.revision,
                            "sha256": hashlib.sha256(snapshot.data).hexdigest()})
            available.append(day)
            if key in cache or (len(cache) < 512 and sum(len(row.data) for row in cache.values())
                                + len(snapshot.data) <= 16777216):
                cache[key] = snapshot
    finally:
        io.close()
    missing = 6 * (len(dates) - len(available))
    if not available:
        return {"status": "not_ready", "selected_remaining": missing, "missing_endpoints": len(dates)}
    registered = register_rolling_endpoints(store, sources, today=today)

    def remaining(report):
        return sum(any(first <= day <= last for first, last in item["ranges"])
                   for channels in report["rolling_remaining"].values()
                   for item in channels.values() for day in available)

    current = registered
    reports = []
    for day in reversed(available):
        if not any(any(first <= day <= last for first, last in item["ranges"])
                   for channels in current["rolling_remaining"].values() for item in channels.values()):
            continue
        previous = remaining(current)
        report = refresh_pending(store, today=today, timezone=timezone, max_periods=8,
            period_names=set(), rolling_endpoints=[day], snapshot_cache=cache,
            batch_local_preparation=True)
        reports.append(report)
        current = report
        if remaining(current) >= previous or report.get("budget_deferred"):
            break
    report = current
    selected = remaining(current) + missing
    status = ("bounded_pending" if report.get("blocked") else "not_ready" if missing else
              "complete" if selected == 0 else "held" if report.get("status") == "held" else "bounded_pending")
    return {**report, "status": status, "selected_remaining": selected,
            "selected_endpoints": len(dates), "missing_endpoints": len(dates) - len(available),
            "global_held_periods": report.get("held_periods"), "cohort_phases": reports,
            **{name: sum(row.get(name, 0) for row in reports)
               for name in ("products_written", "products_unchanged", "periods_completed", "local_snapshot_hits")}}


def _selected_prepared_targets(prepared_manifest, prepared_target_keys):
    if prepared_target_keys is None:
        return prepared_manifest.targets if prepared_manifest is not None else None
    require(prepared_manifest is not None
            and isinstance(prepared_target_keys, (set, frozenset))
            and 1 <= len(prepared_target_keys) <= 42
            and all(isinstance(key, str) for key in prepared_target_keys),
            "Exact prepared target keys required")
    targets = [target for target in prepared_manifest.targets if target["key"] in prepared_target_keys]
    require({target["key"] for target in targets} == prepared_target_keys,
            "Prepared targets outside sealed scope")
    return targets


def _register_prepared_endpoints(queue, days, targets, *, include_existing=False):
    """Admit declared slots, preserving prior dirty ranges and pending ownership."""
    previous = copy.deepcopy(queue.get("rolling"))
    rolling.register(queue, days)
    value = queue["rolling"]
    new_days = {day for day in days if previous is None
                or not rolling.intersect(previous["eligible"], day, day)}
    pending_targets = {(row.get("rolling_window"), row.get("channel"), row["end_date"])
                       for row in queue["pending"].values()}
    for n in rolling.WINDOWS:
        for channel in rolling.CHANNELS:
            prior = previous["dirty"][str(n)][channel] if previous else []
            added = [[target["endpoint"], target["endpoint"]] for target in targets
                     if target["endpoint"] in (set(days) if include_existing else new_days)
                     and target["window_days"] == n
                     and target["channel"] == channel
                     and (not include_existing
                          or (n, channel, target["endpoint"]) not in pending_targets)]
            value["dirty"][str(n)][channel] = rolling.merge([*prior, *added])


def _materialize_prepared(queue, targets, **kwargs):
    """Run the common scheduler on declared dirty slots, preserving other work."""
    view = copy.deepcopy(queue)
    if "rolling" not in view:
        return False
    for n in rolling.WINDOWS:
        for channel in rolling.CHANNELS:
            ranges = view["rolling"]["dirty"][str(n)][channel]
            view["rolling"]["dirty"][str(n)][channel] = rolling.merge([
                interval for target in targets
                if target["window_days"] == n and target["channel"] == channel
                for interval in rolling.intersect(ranges, target["endpoint"], target["endpoint"])
            ])
    if not rolling.materialize(view, **kwargs):
        return False
    for name, row in view["pending"].items():
        if name not in queue["pending"]:
            queue["pending"][name] = row
            channels = queue["rolling"]["dirty"][str(row["rolling_window"])]
            channels[row["channel"]] = rolling.remove_day(channels[row["channel"]], row["end_date"])
    return True


def rolling_cohort_status(queue, sources=None, *, today=None):
    """Public compact remaining report for a bounded local source cohort.

    Accept canonical keys, exact proof dictionaries, or (kind, date) pairs.
    No source list means all admitted endpoint work, including passive drains.
    """
    selected = None if sources is None else [
        _source(item["key"] if isinstance(item, dict) else item) if isinstance(item, (str, dict))
        else tuple(item) for item in sources
    ]
    return rolling.status(queue, today=today, sources=selected)


def _proof(reader, requested):
    result = {"objects": {}, "lists": {}}
    for day in requested:
        for kind in result:
            result[kind].update(copy.deepcopy(reader.dependencies[day][kind]))
    return result


def _context_months(prefixes):
    """Group physical checks; logical per-night dependency lists stay unchanged."""
    months = {}
    for prefix in prefixes:
        match = re.fullmatch(r"(health/night-context/v1/(\d{4})/(\d{2})/)(\d{4}-\d{2}-\d{2})/", prefix)
        require(match is not None, "Night context prefix unverified")
        month, year, number, day = match.groups()
        require(date.fromisoformat(day).isoformat() == day and day[:7] == year + "-" + number,
                "Night context calendar unverified")
        months.setdefault(month, []).append(prefix)
    return months


def _context_fence_lists(requested):
    prefixes = [f"health/night-context/v1/{day[:4]}/{day[5:7]}/{day}/" for day in requested]
    # 31 days * 100 events, plus an overflow sentinel: at most four physical pages.
    return 4 * len(_context_months(prefixes))


def _validate_sources(store, proof, *, check_lists=True):
    for key, binding in proof["objects"].items():
        now = _snapshot(store, key, 524288)
        if binding["status"] == "absent":
            require(now is None, "Night source appeared during calculation")
        else:
            require(
                now is not None
                and now.revision == binding["revision"]
                and hashlib.sha256(now.data).hexdigest() == binding["sha256"],
                "Night source revision changed",
            )
    for month, prefixes in _context_months(proof["lists"] if check_lists else ()).items():
        current = store.list_prefix_bounded(month, max_keys=3101)
        for prefix in prefixes:
            keys = sorted(key for key in current if key.startswith(prefix))
            require(len(keys) <= 100 and keys == proof["lists"][prefix],
                    "Night context revision changed")


def _today(timezone):
    from .native_night import zone

    configured = zone(timezone)
    require(configured is not None, "HEALTH_TIMEZONE required for automatic calendar closure")
    return datetime.now(UTC).astimezone(configured).date().isoformat()


def _preserve_reference(store, queue, queue_revision, row, key, before):
    old = _decode(before.data)
    identity = old.get("source_identity")
    args_hash = old.get("args_sha256")
    require(
        isinstance(identity, str)
        and re.fullmatch(r"[a-f0-9]{64}", identity)
        and isinstance(args_hash, str)
        and re.fullmatch(r"[a-f0-9]{64}", args_hash),
        "Previous reference provenance unverified",
    )
    digest = hashlib.sha256(before.data).hexdigest()
    prefix = f"health/night-products/v1/history/{args_hash}/{identity}/{digest}/{hashlib.sha256(before.revision.encode()).hexdigest()}/"
    provenance = {
        "schema_version": 1,
        "kind": "night-baseline-history",
        "previous_key": key,
        "previous_revision": before.revision,
        "sha256": digest,
        "source_identity": identity,
        "args_sha256": args_hash,
        "content_type": before.content_type,
        "encoding": before.encoding,
    }
    for suffix, data, encoding in (
        ("product.json", before.data, before.encoding),
        ("provenance.json", canonical(provenance).encode(), None),
    ):
        target = prefix + suffix
        existing = _snapshot(store, target)
        if existing is not None:
            require(
                existing.data == data
                and existing.content_type == "application/json"
                and existing.encoding == encoding,
                "Immutable reference history conflict",
            )
            continue
        row["inflight"] = {
            "key": target,
            "sha256": hashlib.sha256(data).hexdigest(),
            "revision": None,
        }
        queue_revision = _save_queue(store, queue, queue_revision)
        after = _cas(store, target, data, None, encoding=encoding)
        row.setdefault("history", {})[target] = {
            "revision": after.revision,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        row.pop("inflight")
        queue_revision = _save_queue(store, queue, queue_revision)
    return queue_revision


def prepare_reference(values, args, proof):
    """Build the same bounded baseline without any storage or queue operation."""
    require(type(values) is list and 1 <= len(values) <= 90, "Native reference window bound")
    args = normalize_args(period="custom", **args)
    for night in values:
        observed = night["channels"][args["channel"]]
        if any(observed["quality"][field] for field in
               ("invalid", "ambiguous_time", "conflicting_timestamps")):
            raise NativeInputError("Unverified native observations")
    input_bytes = len(canonical(values).encode())
    input_points = sum(len(channel["points"]) for night in values
                       for channel in night["channels"].values())
    require(input_bytes <= 16777216, "Native reference input byte limit")
    require(input_points <= 200000, "Native reference point limit")
    value = baseline(values, args, input_bytes=input_bytes, input_points=input_points)["product"]
    body = canonical(value)
    require(len(body.encode()) <= 262144, "Reference product bound")
    require(type(proof) is dict and set(proof) == {"objects", "lists"}
            and all(type(proof[kind]) is dict for kind in proof), "Native reference proof bound")
    return {"schema": 1, "kind": "prepared-night-baseline", "method_version": "night-baseline-v1",
        "args": args, "key": storage_key(args), "body": body,
        "sha256": hashlib.sha256(body.encode()).hexdigest(),
        "source_identity": value["source_identity"], "source_proof": copy.deepcopy(proof),
        "source_proof_sha256": hashlib.sha256(canonical(proof).encode()).hexdigest(),
        "prior_inputs": [{"wake_date": night["wake_date"], "status": night["status"],
            "channel_status": night["channels"][args["channel"]]["status"],
            "source_dates": copy.deepcopy(night["channels"][args["channel"]]["source_dates"]),
            "source_revisions": copy.deepcopy(night["channels"][args["channel"]]["source_revisions"])}
            for night in values]}


_ORDINARY_PREPARATION = object()
_ORDINARY_COHORT_PREPARATION = object()


def _sealed_preparation(workset):
    require(workset is not None, "Sealed reference workset required")
    from .reference_workset import ReferenceWorkset

    require(type(workset) is ReferenceWorkset, "Sealed reference workset required")
    return workset.assert_sealed()


def _prepared_value(artifact, row):
    require(type(artifact) is dict and artifact.get("schema") == 1
            and artifact.get("kind") == "prepared-night-baseline"
            and artifact.get("method_version") == "night-baseline-v1",
            "Prepared reference schema unverified")
    args = artifact["args"]
    require(args == normalize_args(period="custom", **args)
            and args["start_date"] == row["start_date"] and args["end_date"] == row["end_date"]
            and (not row.get("rolling_window") or args["channel"] == row["channel"])
            and artifact["key"] == storage_key(args), "Prepared reference arguments changed")
    body = artifact["body"]
    require(type(body) is str and len(body.encode()) <= 262144
            and hashlib.sha256(body.encode()).hexdigest() == artifact["sha256"],
            "Prepared reference bytes changed")
    value = _decode(body.encode())
    require(value.get("kind") == "night-baseline" and value.get("method_version") == "night-baseline-v1"
            and value.get("args") == args
            and value.get("args_sha256") == hashlib.sha256(canonical(args).encode()).hexdigest()
            and isinstance(value.get("source_manifest"), list)
            and value.get("source_identity") == artifact["source_identity"]
            == hashlib.sha256(canonical(value["source_manifest"]).encode()).hexdigest()
            and canonical(value) == body, "Prepared reference provenance changed")
    proof = artifact["source_proof"]
    require(type(proof) is dict and set(proof) == {"objects", "lists"}
            and all(type(proof[kind]) is dict for kind in proof)
            and hashlib.sha256(canonical(proof).encode()).hexdigest() == artifact["source_proof_sha256"],
            "Prepared reference proof changed")
    inputs = artifact["prior_inputs"]
    require(type(inputs) is list and 1 <= len(inputs) <= 90
            and all(type(item) is dict and set(item) == {
                "wake_date", "status", "channel_status", "source_dates", "source_revisions"}
                and type(item["source_dates"]) is list and type(item["source_revisions"]) is list
                for item in inputs), "Prepared reference availability changed")
    return value, body.encode(), proof


def _prior_prepared_input_unavailable(old, inputs):
    skipped = {item["wake_date"] for item in old["excluded"]}
    prior = {item["wake_date"]: item for item in old["source_manifest"]
             if item["wake_date"] not in skipped}
    require(len(prior) == old["contributing_nights"], "Previous contributor evidence unverified")
    current = {item["wake_date"]: item for item in inputs}
    for day, proof in prior.items():
        night = current.get(day)
        if night is None or night["status"] not in {"available", "partial"}:
            return True
        if night["channel_status"] not in {"available", "no_data"}:
            return True
        previous = {item["date"] for item in proof["source_revisions"]}
        available = {item["date"] for item in night["source_revisions"]}
        if (previous & set(night["source_dates"])) - available:
            return True
    return False


def _combined_reference_proof(records):
    proof = {"objects": {}, "lists": {}}
    for record in records:
        for kind in proof:
            for key, value in record["proof"][kind].items():
                require(key not in proof[kind] or proof[kind][key] == value,
                        "Reference batch source proof conflict")
                proof[kind][key] = value
    return proof


def publish_prepared_rows(io, queue, revision, rows, *, report, reserve_writes=0,
                          prepared_manifest=None, _ordinary_gate=None):
    """Publish a bounded sealed group with the established intent/CAS protocol.

    The ordinary refresh supplies one freshly assembled row through its private
    gate. Bulk publication always requires the whole-workset seal, not a list of
    the products that happened to finish preparation first.
    """
    if _ordinary_gate is not _ORDINARY_PREPARATION and _ordinary_gate is not _ORDINARY_COHORT_PREPARATION:
        _sealed_preparation(prepared_manifest)
    elif _ordinary_gate is _ORDINARY_PREPARATION:
        require(len(rows) == 1, "Ordinary reference row bound")
    require(type(rows) is list and 1 <= len(rows) <= MAX_LOCAL_PERIODS,
            "Choose one to eight prepared rows")
    records = []
    for name, artifacts in rows:
        row = queue["pending"][name]
        require(not row.get("inflight") and not _held(row)
                and all(item.get("state") == "ready" for item in row["sources"].values()),
                "Unresolved previous source/product write; no automatic adoption")
        require(type(artifacts) is list and len(artifacts) == (1 if row.get("rolling_window") else 2),
                "Prepared reference channel group changed")
        expected = {row["channel"]} if row.get("rolling_window") else {"heart_rate", "hrv"}
        require({item["args"]["channel"] for item in artifacts} == expected,
                "Prepared reference channel group changed")
        proof = artifacts[0]["source_proof"]
        proof_hash = artifacts[0]["source_proof_sha256"]
        pending = []
        for artifact in artifacts:
            value, data, checked_proof = _prepared_value(artifact, row)
            require(checked_proof == proof and artifact["source_proof_sha256"] == proof_hash,
                    "Prepared reference row proof conflict")
            for key, binding in row["sources"].items():
                observed = proof["objects"].get(key, {})
                require(observed.get("status") == "present"
                        and observed.get("revision") == binding["revision"]
                        and observed.get("sha256") == binding["sha256"], "Dirty canonical source proof changed")
            key = artifact["key"]
            before = _snapshot(io, key)
            old = _decode(before.data) if before else None
            if old is not None:
                require(old.get("kind") == "night-baseline" and old.get("method_version") == "night-baseline-v1"
                        and old.get("args") == artifact["args"]
                        and old.get("args_sha256") == hashlib.sha256(canonical(artifact["args"]).encode()).hexdigest()
                        and isinstance(old.get("source_manifest"), list)
                        and old.get("source_identity") == hashlib.sha256(canonical(old["source_manifest"]).encode()).hexdigest()
                        and old.get("unit") == value["unit"] and type(old.get("contributing_nights")) is int
                        and 0 <= old["contributing_nights"] <= old.get("nights_requested", -1),
                        "Existing reference unverified")
                require(isinstance(old.get("excluded"), list) and all(
                    isinstance(item, dict) and isinstance(item.get("wake_date"), str)
                    for item in old["excluded"]), "Previous reference exclusions unverified")
                if _prior_prepared_input_unavailable(old, artifact["prior_inputs"]):
                    row["blocked"] = "prior_contributing_input_unavailable"
                elif old["contributing_nights"] > 0 and value["contributing_nights"] == 0:
                    row["blocked"] = "empty_does_not_replace_good"
                if row.get("blocked"):
                    report["blocked"].append(name)
                    break
            value_sem, old_sem = dict(value), dict(old) if old else None
            value_sem.pop("resources", None)
            if old_sem is not None:
                old_sem.pop("resources", None)
            unchanged = old_sem == value_sem
            complete = row.get("completed", {}).get(key)
            if complete:
                require(before is not None and before.revision == complete["revision"]
                        and hashlib.sha256(before.data).hexdigest() == complete["sha256"]
                        and (unchanged or artifact["sha256"] == complete["sha256"]),
                        "Known completed reference changed")
            if complete or unchanged:
                report["products_unchanged"] += 1
            else:
                pending.append((key, data, before, False))
        if row.get("blocked"):
            revision = _save_queue(io, queue, revision)
            continue
        if row["phase"] == "publishing":
            require(row.get("source_proof_sha256") == proof_hash, "Interrupted reference source binding changed")
        if row.get("validated_progress"):
            require(row["validated_progress"] == proof_hash, "Known bounded progress source binding changed")
        records.append({"name": name, "row": row, "proof": proof, "proof_hash": proof_hash,
                        "pending": pending, "chosen": pending})
    chosen = []

    def fits(group):
        if any(not item["row"].get("rolling_window") for item in group) and len(group) != 1:
            return False
        proof = _combined_reference_proof(group)
        products = [product for item in group for product in item["chosen"]]
        rolling_group = bool(group[0]["row"].get("rolling_window"))
        reserve = 2 * len(group) + sum(9 if before else 3 for _, _, before, _ in products)
        reads = (0 if rolling_group else 2 * len(proof["objects"])) + 2 * len(group)
        reads += sum(11 if before else 3 for _, _, before, _ in products)
        if io.counts()["get"] + reads > 400:
            return False
        estimated = sum(len(data) + (2 * len(before.data) + 2048 if before else 0)
                        for _, data, before, _ in products)
        estimated += (reserve + 1) * min(MAX_QUEUE_BYTES, len(canonical(queue).encode()) + 65536)
        if not io.can_write_group(attempts=reserve + reserve_writes, byte_count=estimated):
            return False
        from .night_reference_sources import rolling_fence_list_bound

        lists = rolling_fence_list_bound(proof) if rolling_group else 4 * len(_context_months(proof["lists"]))
        return io.counts()["list"] + 2 * lists <= 100

    for record in records:
        accepted = False
        for count in range(len(record["pending"]), -1, -1):
            if record["pending"] and count == 0:
                continue
            record["chosen"] = record["pending"][:count]
            if fits(chosen + [record]):
                chosen.append(record)
                accepted = True
                break
        if not accepted or len(record["chosen"]) < len(record["pending"]):
            break
    if not chosen:
        if records:
            report.setdefault("budget_deferred", []).append(records[0]["name"])
        return {"queue_revision": revision, "completed": [], "deferred": bool(records)}
    proof = _combined_reference_proof(chosen)
    rolling_group = bool(chosen[0]["row"].get("rolling_window"))
    for record in chosen:
        record["row"]["phase"] = "publishing"
        record["row"]["source_proof_sha256"] = record["proof_hash"]
        record["row"].pop("validated_progress", None)
    revision = _save_queue(io, queue, revision)
    from .night_reference_sources import validate_rolling_sources

    validate = validate_rolling_sources if rolling_group else _validate_sources
    validate(io, proof)
    for record in chosen:
        row = record["row"]
        for key, data, before, _ in record["chosen"]:
            if before is not None:
                revision = _preserve_reference(io, queue, revision, row, key, before)
            row["inflight"] = {"key": key, "sha256": hashlib.sha256(data).hexdigest(),
                               "revision": before.revision if before else None}
            revision = _save_queue(io, queue, revision)
            after = _cas(io, key, data, before)
            row.setdefault("completed", {})[key] = {"revision": after.revision,
                "sha256": hashlib.sha256(data).hexdigest()}
            row.pop("inflight")
            revision = _save_queue(io, queue, revision)
            report["products_written"] += 1
    validate(io, proof)
    completed = []
    for record in chosen:
        if len(record["chosen"]) < len(record["pending"]):
            record["row"]["phase"] = "dirty"
            record["row"]["validated_progress"] = record["proof_hash"]
            report.setdefault("budget_deferred", []).append(record["name"])
        else:
            del queue["pending"][record["name"]]
            completed.append(record["name"])
            report["periods_completed"] += 1
    revision = _save_queue(io, queue, revision)
    deferred = len(chosen) < len(records) or any(
        len(item["chosen"]) < len(item["pending"]) for item in chosen)
    if len(chosen) < len(records):
        report.setdefault("budget_deferred", []).append(records[len(chosen)]["name"])
    return {"queue_revision": revision, "completed": completed, "deferred": deferred}


def refresh_pending(
    store, *, today=None, timezone=None, max_periods=MAX_PERIODS, reserve_writes=0,
    snapshot_cache=None, period_names=None, rolling_sources=None, rolling_endpoints=None,
    prepared_manifest=None, prepared_target_keys=None, batch_local_preparation=False,
):
    """Bounded stored-only drain with optional verified local calculation inputs.

    Local snapshots never satisfy the physical pre/post publication fences.
    A period filter limits work; pending/open counts still describe the full queue.
    """
    require(
        type(max_periods) is int and 1 <= max_periods <= MAX_LOCAL_PERIODS,
        "Choose one to eight pending periods",
    )
    require(
        type(reserve_writes) is int and 0 <= reserve_writes <= 250,
        "Bounded caller checkpoint reservation",
    )
    require(type(batch_local_preparation) is bool and not (batch_local_preparation and prepared_manifest is not None),
            "Choose one local preparation contract")
    require(rolling_endpoints is None or isinstance(rolling_endpoints, (list, tuple))
            and len(rolling_endpoints) <= 7 and all(
                isinstance(endpoint, str) and date.fromisoformat(endpoint).isoformat() == endpoint
                for endpoint in rolling_endpoints), "Choose at most seven exact rolling endpoints")
    timezone = timezone or os.environ.get("HEALTH_TIMEZONE")
    day = today
    seal = _sealed_preparation(prepared_manifest) if prepared_manifest is not None else None
    prepared_targets = _selected_prepared_targets(prepared_manifest, prepared_target_keys)
    require(prepared_target_keys is None or rolling_endpoints is not None and all(
        target["endpoint"] in rolling_endpoints for target in prepared_targets),
        "Prepared targets outside refresh endpoints")
    prepared_keys = {target["key"] for target in prepared_targets} if prepared_targets is not None else None
    io = _IO(store)
    report = {
        "periods_completed": 0,
        "local_snapshot_hits": 0,
        "products_written": 0,
        "products_unchanged": 0,
        "blocked": [],
        "held_periods": 0,
        "pending": 0,
    }
    queue = None
    if seal is not None:
        report["prepared_build_seal"] = seal
    try:
        queue, revision = _read_queue(io)
        if rolling_sources != []:
            from .rolling_context_refresh import reconcile_saved_contexts

            report["saved_contexts"], changed = reconcile_saved_contexts(
                io, queue, reserve_writes=reserve_writes,
            )
            if changed:
                revision = _save_queue(io, queue, revision)
        report["held_periods"] = (sum(_held(row) for row in queue["pending"].values())
                                  + sum(binding["state"] == "intent" for binding in
                                        queue.get("rolling_context_intents", {}).values()))
        report["rolling_remaining"] = rolling.status(queue)
        report["discovery_remaining"] = discovery_status(queue)
        report["pending"] = (
            sum(not row.get("rolling_window") for row in queue["pending"].values())
            + sum(item["count"] for channels in report["rolling_remaining"].values()
                  for item in channels.values())
            + report["discovery_remaining"]["count"]
        )
        if not queue["pending"] and not queue.get("rolling_context_intents") and not report["discovery_remaining"]["count"] and not any(
            item["count"] for channels in report["rolling_remaining"].values()
            for item in channels.values()
        ):
            return {
                **report,
                "status": "complete",
                "deferred_open": 0,
                "closed_pending": 0,
                "component_operations": io.counts(),
            }
        if day is None:
            from .native_night import zone

            if zone(timezone) is None:
                return {
                    **report,
                    "pending_materialized": len(queue["pending"]),
                    "status": "calendar_timezone_required",
                    "closed_pending": None,
                    "deferred_open": None,
                }
            day = _today(timezone)
        require(date.fromisoformat(day).isoformat() == day, "Exact closure day required")
        report["closure_day"] = day
        allowed = None if rolling_sources is None else [
            _source(item["key"] if isinstance(item, dict) else item)
            if isinstance(item, (str, dict)) else tuple(item) for item in rolling_sources
        ]
        if prepared_targets is None and rolling_endpoints is None and rolling_sources != []:
            discovery_before = copy.deepcopy(queue)
            snapshot_cache = {} if snapshot_cache is None else snapshot_cache
            discovery = drain_discovery(io, queue, today=day, snapshot_cache=snapshot_cache,
                                        allowed_sources=allowed)
            report["discovery"] = discovery
            if discovery["changed"]:
                if not store.can_write_group(attempts=1 + reserve_writes,
                                             byte_count=len(canonical(queue).encode())):
                    queue = discovery_before
                    raise ComponentBudgetDeferred("Discovery checkpoint budget deferred")
                io.require_capacity("get", 1)
                io.require_capacity("put", 1)
                revision = _save_queue(io, queue, revision)
        before_materialization = copy.deepcopy(queue)
        calendar_waiting = prepared_targets is None and any(
            not row.get("rolling_window") and (period_names is None or name in period_names)
            and is_closed((row["start_date"], row["end_date"]), day)
            and not row.get("blocked") and not _held(row)
            for name, row in queue["pending"].items()
        )
        materialize_args = {"today": day, "limit": max_periods, "max_pending": MAX_PENDING,
                            "allowed_sources": allowed, "allowed_endpoints": rolling_endpoints}
        if not calendar_waiting and (
            rolling.materialize(queue, **materialize_args) if prepared_targets is None
            else _materialize_prepared(queue, prepared_targets, **materialize_args)
        ):
            if not store.can_write_group(attempts=1 + reserve_writes,
                                         byte_count=len(canonical(queue).encode())):
                queue = before_materialization
                raise ComponentBudgetDeferred("Rolling materialization group deferred")
            io.require_capacity("get", 1)
            io.require_capacity("put", 1)
            revision = _save_queue(io, queue, revision)
        reader = NativeNightReader(io, timezone=timezone, snapshot_cache=snapshot_cache)
        nights = {}
        eligible = [
            name
            for name, row in sorted(queue["pending"].items())
            if (row.get("rolling_window") and (rolling_endpoints is None or row["end_date"] in rolling_endpoints)
                and (allowed is None or any(
                a <= row["end_date"] <= b for kind, source_day in allowed
                for a, b in [rolling.source_range(kind, source_day, row["rolling_window"])]
            )) or not row.get("rolling_window") and (period_names is None or name in period_names))
            and (row["end_date"] <= day if row.get("rolling_window")
                 else is_closed((row["start_date"], row["end_date"]), day))
            and not row.get("blocked")
            and not _held(row)
            and (prepared_keys is None or row.get("rolling_window") and storage_key(normalize_args(
                start_date=row["start_date"], end_date=row["end_date"], period="custom",
                channel=row["channel"])) in prepared_keys)
        ]
        # Keep the established closed calendar cohort together. Rolling work
        # remains durably pending for the next bounded phase when calendars use
        # this phase; daily source consumers can continue their existing drain.
        calendars = [name for name in eligible if not queue["pending"][name].get("rolling_window")]
        selected = (calendars or eligible)[:max_periods]
        staged_rows = []
        if batch_local_preparation and selected and not calendars:
            first = min(queue["pending"][name]["start_date"] for name in selected)
            last = max(queue["pending"][name]["end_date"] for name in selected)
            days = [(date.fromisoformat(first) + timedelta(days=index)).isoformat()
                    for index in range((date.fromisoformat(last) - date.fromisoformat(first)).days + 1)]
            from .night_reference_sources import prime_rolling_list_bound

            if io.counts()["list"] + prime_rolling_list_bound(days) > 100:
                raise ComponentBudgetDeferred("Local cohort inventory budget deferred")
            reader.prime_rolling_sources(days)
        for name in selected:
            row = queue["pending"][name]
            is_rolling = bool(row.get("rolling_window"))
            if is_rolling and (any(
                binding.get("state") == "intent" and
                rolling.source_range(*_source(key), row["rolling_window"])[0] <= row["end_date"]
                <= rolling.source_range(*_source(key), row["rolling_window"])[1]
                for pending_row in queue["pending"].values()
                for entries in _source_maps(pending_row) for key, binding in entries.items()
            ) or any(
                binding["wake_date"] <= row["end_date"] <=
                rolling.source_range("sleep", binding["wake_date"], row["rolling_window"])[1]
                for binding in queue.get("rolling_context_intents", {}).values()
            )):
                report["held_periods"] += 1
                continue
            require(
                not row.get("inflight")
                and all(source.get("state") == "ready" for source in row["sources"].values()),
                "Unresolved previous source/product write; no automatic adoption",
            )
            if prepared_manifest is not None:
                artifacts = [prepared_manifest.product_for(storage_key(normalize_args(
                    start_date=row["start_date"], end_date=row["end_date"], period="custom",
                    channel=channel)))
                    for channel in ((row["channel"],) if is_rolling else ("heart_rate", "hrv"))]
                staged_rows.append((name, artifacts))
                continue
            for key, binding in row["sources"].items():
                # Capture once through the same reader used for native assembly.
                # Pre/post publication fences below still perform fresh storage reads.
                source_kind, _ = _source(key)
                reader.read(
                    key,
                    262144 if source_kind == "signals" else 524288,
                    1048576 if source_kind == "signals" else 2097152,
                )
                observed = reader.proof[key]
                require(
                    observed.get("status") == "present"
                    and observed["revision"] == binding["revision"]
                    and observed["sha256"] == binding["sha256"],
                    "Dirty canonical source proof changed",
                )
            first, last = date.fromisoformat(row["start_date"]), date.fromisoformat(row["end_date"])
            requested = [
                (first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)
            ]
            lists_used = io.counts()["list"]
            if is_rolling:
                from .night_reference_sources import (
                    prime_rolling_list_bound,
                )

                assembly_lists = prime_rolling_list_bound(requested)
            else:
                assembly_lists = sum(wake not in nights for wake in requested)
            if (not batch_local_preparation
                    and lists_used + assembly_lists + 2 * _context_fence_lists(requested) > 100):
                report.setdefault("budget_deferred", []).append(name)
                break
            try:
                if is_rolling and not batch_local_preparation:
                    reader.prime_rolling_sources(requested)
                    nights = {}
                for wake in requested:
                    if wake not in nights:
                        nights[wake] = reader.night(wake)
                values = [nights[wake] for wake in requested]
                # A partial but verified night is usable; conflicting/ambiguous or
                # malformed observations are not permission to replace a reference.
                if any(
                    channel["quality"][field]
                    for night in values
                    for channel in ([night["channels"][row["channel"]]] if is_rolling
                                    else night["channels"].values())
                    for field in ("invalid", "ambiguous_time", "conflicting_timestamps")
                ):
                    raise NativeInputError("Unverified native observations")
            except NativeInputError:
                row["blocked"] = "native_input_unverified"
                report["blocked"].append(name)
                revision = _save_queue(io, queue, revision)
                continue
            require(
                len(canonical(values).encode()) <= 16777216, "Native reference input byte limit"
            )
            require(
                sum(len(ch["points"]) for night in values for ch in night["channels"].values())
                <= 200000,
                "Native reference point limit",
            )
            proof = _proof(reader, requested)
            for key in row["sources"]:
                proof["objects"][key] = copy.deepcopy(reader.proof[key])
            artifacts = [prepare_reference(values, normalize_args(
                start_date=row["start_date"], end_date=row["end_date"], period="custom",
                channel=channel), proof)
                for channel in ((row["channel"],) if is_rolling else ("heart_rate", "hrv"))]
            if batch_local_preparation:
                staged_rows.append((name, artifacts))
                continue
            published = publish_prepared_rows(io, queue, revision, [(name, artifacts)],
                report=report, reserve_writes=reserve_writes, _ordinary_gate=_ORDINARY_PREPARATION)
            revision = published["queue_revision"]
            if published["deferred"]:
                break
        if staged_rows:
            published = publish_prepared_rows(io, queue, revision, staged_rows,
                report=report, reserve_writes=reserve_writes, prepared_manifest=prepared_manifest,
                _ordinary_gate=_ORDINARY_COHORT_PREPARATION if batch_local_preparation else None)
            revision = published["queue_revision"]
        report["local_snapshot_hits"] = reader.local_snapshot_hits
        report["pending_materialized"] = len(queue["pending"])
        report["rolling_remaining"] = rolling.status(queue)
        report["rolling_eligible_remaining"] = rolling.status(queue, today=day)
        report["discovery_remaining"] = discovery_status(queue)
        report["discovery_eligible_remaining"] = discovery_status(queue, today=day)
        report["rolling_pending"] = sum(
            item["count"] for channels in report["rolling_remaining"].values()
            for item in channels.values()
        )
        compact_remaining = sum(
            item["count"] for channels in report["rolling_eligible_remaining"].values()
            for item in channels.values()
        )
        legacy = [row for row in queue["pending"].values() if not row.get("rolling_window")]
        report["pending"] = (len(legacy) + report["rolling_pending"]
                             + report["discovery_remaining"]["count"])
        report["deferred_open"] = (
            sum(not is_closed((row["start_date"], row["end_date"]), day) for row in legacy)
            + report["rolling_pending"] - compact_remaining
            + report["discovery_remaining"]["count"] - report["discovery_eligible_remaining"]["count"]
        )
        report["closed_pending"] = report["pending"] - report["deferred_open"]
        report["status"] = (
            "complete"
            if not report["pending"] and not report["rolling_pending"] and not queue.get("rolling_context_intents")
            else "held"
            if report["held_periods"]
            else "bounded_pending"
            if queue.get("rolling_context_intents")
            else "waiting_until_close"
            if not report["closed_pending"] and not compact_remaining
            else "bounded_pending"
        )
        report["component_operations"] = io.counts()
        return report
    except ComponentBudgetDeferred:
        if queue is not None and any(row.get("inflight") for row in queue["pending"].values()):
            raise
        report.update(
            status="bounded_pending",
            pending=len(queue["pending"]) if queue is not None else None,
            held_periods=report["held_periods"] if queue is not None else None,
            deferred_open=None,
            closed_pending=None,
            component_operations=io.counts(),
        )
        if queue is not None:
            report["rolling_remaining"] = rolling.status(queue)
            report["discovery_remaining"] = discovery_status(queue)
            report["pending_materialized"] = len(queue["pending"])
            report["pending"] = (
                sum(not row.get("rolling_window") for row in queue["pending"].values())
                + sum(item["count"] for channels in report["rolling_remaining"].values()
                      for item in channels.values())
                + report["discovery_remaining"]["count"]
            )
        report.setdefault("budget_deferred", []).append("component_capacity")
        return report
    except BaselineRefreshStop:
        raise
    except Exception as error:
        raise BaselineRefreshStop("Stored night reference refresh stopped") from error
    finally:
        primary = sys.exception()
        try:
            io.close()
        except Exception as error:
            if primary is None:
                raise BaselineRefreshStop("Reference observer cleanup failed") from error
