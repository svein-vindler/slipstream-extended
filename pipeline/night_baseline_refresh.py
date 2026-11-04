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

from .native_night import NativeInputError, NativeNightReader
from .night_products import baseline, canonical, normalize_args, storage_key
from .r2_store import R2BudgetError, missing_object


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

    def __init__(self, client):
        require(
            not getattr(client, "_night_baseline_observer_active", False),
            "Nested baseline observer",
        )
        self.client = client
        self.state = getattr(client, "_night_baseline_budget_state", None)
        if self.state is None:
            self.state = {
                "counts": {"get": 0, "list": 0, "put": 0},
                "failed": False,
                "read_bytes": 0,
            }
            client._night_baseline_budget_state = self.state
        self.deadline = time.monotonic() + 900
        self.bodies = []
        self.binding = None
        self.operation = None
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
            _WireBudget(client)
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
                name in affected_periods(kind, source_day)
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
            name in affected_periods("hrv", start)
            and affected_periods("hrv", start)[name] == (start, end),
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
        require(
            store.can_write_group(
                attempts=3,
                byte_count=len(data)
                + 2 * len(canonical(queue).encode())
                + len(targets) * 2200
                + 4096,
            ),
            "Canonical source group budget deferred before intent",
        )
        require(
            len(canonical(queue).encode()) + len(targets) * 2200 <= MAX_QUEUE_BYTES,
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
            selected.append((item, affected_periods(kind, day)))
        queue, before = _read_queue(io)
        for item, periods in selected:
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


def _prior_input_unavailable(old, values, channel):
    excluded = old.get("excluded")
    require(
        isinstance(excluded, list)
        and all(
            isinstance(row, dict) and isinstance(row.get("wake_date"), str) for row in excluded
        ),
        "Previous reference exclusions unverified",
    )
    skipped = {row["wake_date"] for row in excluded}
    prior = {
        row["wake_date"]: row for row in old["source_manifest"] if row["wake_date"] not in skipped
    }
    require(len(prior) == old["contributing_nights"], "Previous contributor evidence unverified")
    current = {row["wake_date"]: row for row in values}
    for day, proof in prior.items():
        night = current.get(day)
        if night is None or night.get("status") not in {"available", "partial"}:
            return True
        observed = night["channels"][channel]
        if observed["status"] not in {"available", "no_data"}:
            return True
        previous_dates = {row["date"] for row in proof["source_revisions"]}
        available_dates = {row["date"] for row in observed["source_revisions"]}
        still_needed = set(observed["source_dates"])
        if (previous_dates & still_needed) - available_dates:
            return True
    return False


def refresh_pending(
    store, *, today=None, timezone=None, max_periods=MAX_PERIODS, reserve_writes=0,
    snapshot_cache=None, period_names=None,
):
    """Bounded stored-only drain with optional verified local calculation inputs.

    Local snapshots never satisfy the physical pre/post publication fences.
    A period filter limits work; pending/open counts still describe the full queue.
    """
    require(
        type(max_periods) is int and 1 <= max_periods <= MAX_PERIODS,
        "Choose one or two pending periods",
    )
    require(
        type(reserve_writes) is int and 0 <= reserve_writes <= 250,
        "Bounded caller checkpoint reservation",
    )
    timezone = timezone or os.environ.get("HEALTH_TIMEZONE")
    day = today
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
    queue = active_row = None
    try:
        queue, revision = _read_queue(io)
        report["held_periods"] = sum(_held(row) for row in queue["pending"].values())
        if not queue["pending"]:
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
                    "pending": len(queue["pending"]),
                    "status": "calendar_timezone_required",
                    "closed_pending": None,
                    "deferred_open": None,
                }
            day = _today(timezone)
        require(date.fromisoformat(day).isoformat() == day, "Exact closure day required")
        reader = NativeNightReader(io, timezone=timezone, snapshot_cache=snapshot_cache)
        nights = {}
        selected = [
            name
            for name, row in sorted(queue["pending"].items())
            if (period_names is None or name in period_names)
            and is_closed((row["start_date"], row["end_date"]), day)
            and not row.get("blocked")
            and not _held(row)
        ][:max_periods]
        for name in selected:
            row = queue["pending"][name]
            active_row = row
            require(
                not row.get("inflight")
                and all(source.get("state") == "ready" for source in row["sources"].values()),
                "Unresolved previous source/product write; no automatic adoption",
            )
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
            if (
                lists_used + sum(wake not in nights for wake in requested) + 2 * _context_fence_lists(requested)
                > 100
            ):
                report.setdefault("budget_deferred", []).append(name)
                break
            try:
                for wake in requested:
                    if wake not in nights:
                        nights[wake] = reader.night(wake)
                values = [nights[wake] for wake in requested]
                # A partial but verified night is usable; conflicting/ambiguous or
                # malformed observations are not permission to replace a reference.
                if any(
                    channel["quality"][field]
                    for night in values
                    for channel in night["channels"].values()
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
            products = []
            for channel in ("heart_rate", "hrv"):
                args = normalize_args(
                    start_date=row["start_date"],
                    end_date=row["end_date"],
                    period="custom",
                    channel=channel,
                )
                value = baseline(
                    values,
                    args,
                    input_bytes=len(canonical(values).encode()),
                    input_points=sum(
                        len(ch["points"]) for night in values for ch in night["channels"].values()
                    ),
                )["product"]
                key = storage_key(args)
                before = _snapshot(io, key)
                old = _decode(before.data) if before else None
                if old is not None:
                    require(
                        old.get("kind") == "night-baseline"
                        and old.get("method_version") == "night-baseline-v1"
                        and old.get("args") == args
                        and old.get("args_sha256")
                        == hashlib.sha256(canonical(args).encode()).hexdigest()
                        and isinstance(old.get("source_manifest"), list)
                        and old.get("source_identity")
                        == hashlib.sha256(canonical(old["source_manifest"]).encode()).hexdigest()
                        and old.get("unit") == value["unit"]
                        and type(old.get("contributing_nights")) is int
                        and 0 <= old["contributing_nights"] <= old.get("nights_requested", -1),
                        "Existing reference unverified",
                    )
                    if _prior_input_unavailable(old, values, channel):
                        row["blocked"] = "prior_contributing_input_unavailable"
                        report["blocked"].append(name)
                        break
                    if old.get("contributing_nights", 0) > 0 and value["contributing_nights"] == 0:
                        row["blocked"] = "empty_does_not_replace_good"
                        report["blocked"].append(name)
                        break
                value_sem = dict(value)
                value_sem.pop("resources", None)
                old_sem = dict(old) if old else None
                if old_sem is not None:
                    old_sem.pop("resources", None)
                data = canonical(value).encode()
                require(len(data) <= 262144, "Reference product bound")
                products.append((key, data, before, old_sem == value_sem))
            if row.get("blocked"):
                revision = _save_queue(io, queue, revision)
                continue
            proof = _proof(reader, requested)
            for key in row["sources"]:
                proof["objects"][key] = copy.deepcopy(reader.proof[key])
            proof_hash = hashlib.sha256(canonical(proof).encode()).hexdigest()
            if row["phase"] == "publishing":
                require(
                    row.get("source_proof_sha256") == proof_hash,
                    "Interrupted reference source binding changed",
                )
            if row.get("validated_progress"):
                require(
                    row["validated_progress"] == proof_hash,
                    "Known bounded progress source binding changed",
                )
            pending = []
            for product in products:
                key, data, before, unchanged = product
                complete = row.get("completed", {}).get(key)
                if complete:
                    require(
                        before is not None
                        and before.revision == complete["revision"]
                        and hashlib.sha256(before.data).hexdigest() == complete["sha256"]
                        and (unchanged or hashlib.sha256(data).hexdigest() == complete["sha256"]),
                        "Known completed reference changed",
                    )
                    report["products_unchanged"] += 1
                elif unchanged:
                    report["products_unchanged"] += 1
                else:
                    pending.append(product)
            chosen = []
            # Both fresh fences and all history/control/current readbacks must fit
            # before entering publication. Smaller known groups retain progress.
            for count in range(len(pending), -1, -1):
                if pending and count == 0:
                    continue
                group = pending[:count]
                reserve = 2 + sum(9 if before is not None else 3 for _, _, before, _ in group)
                reads = (
                    2 * len(proof["objects"])
                    + 2
                    + sum(11 if before is not None else 3 for _, _, before, _ in group)
                )
                if io.counts()["get"] + reads > 400:
                    continue
                estimated = sum(
                    len(data) + (2 * len(before.data) + 2048 if before else 0)
                    for _, data, before, _ in group
                ) + (reserve + 1) * min(MAX_QUEUE_BYTES, len(canonical(queue).encode()) + 65536)
                if not store.can_write_group(
                    attempts=reserve + reserve_writes, byte_count=estimated
                ):
                    continue
                # Admission may have inventoried the bucket inside this observer.
                # Both fresh context fences must still fit before any intent.
                if io.counts()["list"] + 2 * _context_fence_lists(requested) > 100:
                    continue
                chosen = group
                break
            else:
                report.setdefault("budget_deferred", []).append(name)
                break
            row["phase"] = "publishing"
            row["source_proof_sha256"] = proof_hash
            row.pop("validated_progress", None)
            revision = _save_queue(io, queue, revision)
            _validate_sources(io, proof)
            for key, data, before, _ in chosen:
                if before is not None:
                    revision = _preserve_reference(io, queue, revision, row, key, before)
                row["inflight"] = {
                    "key": key,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "revision": before.revision if before else None,
                }
                revision = _save_queue(io, queue, revision)
                after = _cas(io, key, data, before)
                row.setdefault("completed", {})[key] = {
                    "revision": after.revision,
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
                row.pop("inflight")
                revision = _save_queue(io, queue, revision)
                report["products_written"] += 1
            _validate_sources(io, proof)
            if len(chosen) < len(pending):
                # This is a known ACK/readback + fresh-post-fence checkpoint, not
                # an unknown publication. New sources can supersede it normally.
                row["phase"] = "dirty"
                row["validated_progress"] = proof_hash
                revision = _save_queue(io, queue, revision)
                report.setdefault("budget_deferred", []).append(name)
                break
            del queue["pending"][name]
            revision = _save_queue(io, queue, revision)
            report["periods_completed"] += 1
        report["local_snapshot_hits"] = reader.local_snapshot_hits
        report["pending"] = len(queue["pending"])
        report["deferred_open"] = sum(
            not is_closed((row["start_date"], row["end_date"]), day)
            for row in queue["pending"].values()
        )
        report["closed_pending"] = report["pending"] - report["deferred_open"]
        report["status"] = (
            "complete"
            if not report["pending"]
            else "held"
            if report["held_periods"]
            else "waiting_until_close"
            if not report["closed_pending"]
            else "bounded_pending"
        )
        report["component_operations"] = io.counts()
        return report
    except ComponentBudgetDeferred:
        if active_row is not None and active_row.get("inflight"):
            raise
        report.update(
            status="bounded_pending",
            pending=len(queue["pending"]) if queue is not None else None,
            held_periods=report["held_periods"] if queue is not None else None,
            deferred_open=None,
            closed_pending=None,
            component_operations=io.counts(),
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
