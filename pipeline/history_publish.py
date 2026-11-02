"""Explicit R2-only phase: source-proof verification, conditional writes and readback."""

from __future__ import annotations

import calendar
import json
import re
import sys
import time
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from .history_canonical import decode, has_curve, key, prepare
from .history_state import Stop, digest, read, write


def _cleanup(*closers):
    """Attempt every close; preserve an active primary or the first close error."""
    primary = sys.exception()
    first = None
    for close in closers:
        try:
            close()
        except BaseException as error:
            if first is None:
                first = error
    if primary is None and first is not None:
        raise first


def _inventory_projection(body):
    """Preserve bounded raw fields until the physical SDK page is verified."""
    from xml.etree import ElementTree

    from .night_baseline_refresh import _raw_inventory

    projection = _raw_inventory(body)  # Includes encoding/DTD/entity/shape bounds.
    root = ElementTree.fromstring(body.decode("utf-8-sig"))
    for field in ("Name", "KeyCount", "Prefix", "ContinuationToken", "MaxKeys", "EncodingType"):
        nodes = [node for node in root if node.tag.rsplit("}", 1)[-1] == field]
        if len(nodes) > 1 or any(list(node) for node in nodes):
            raise Stop("storage_list_envelope_unverified")
        value = (nodes[0].text or "") if nodes else None
        if field in {"Name", "KeyCount"} and value is None:
            raise Stop("storage_list_envelope_unverified")
        if field in {"KeyCount", "MaxKeys"} and value is not None:
            if not re.fullmatch(r"[0-9]{1,20}", value.strip()):
                raise Stop("storage_list_envelope_unverified")
            value = int(value)
            if not 0 <= value <= 1000:
                raise Stop("storage_list_envelope_unverified")
        projection[field] = value
    return projection


class StorageGuard:
    def __init__(self, store, keys, folder):
        self.store, self.keys, self.folder = store, keys, folder
        self.counts = {"get": 0, "list": 0, "put": 0}
        self.expected = {}
        self.failed = False
        self.acks = []
        self.body_bytes = 0
        self.raw_bytes = 0
        self.decoded_bytes = 0
        self.started = time.monotonic()
        self.prefixes = set()
        self.host = urlsplit(store.client.meta.endpoint_url).netloc
        self.original_send = store.client._endpoint.http_session.send

        def bounded_send(request):
            import time

            from botocore.awsrequest import AWSResponse

            try:
                response = self.original_send(request)
            except BaseException:
                self.failed = True
                raise Stop("unknown_storage_transport") from None
            started = time.monotonic()
            body = bytearray()
            query = parse_qs(urlsplit(request.url).query)
            kind = "list" if "list-type" in query else "put" if request.method == "PUT" else "get"
            cap = (
                65536
                if response.status_code >= 300 or kind == "put"
                else 1048576
                if kind == "list"
                else 524288
            )
            try:
                for part in response.raw.stream(8192, decode_content=False):
                    body.extend(part)
                    self.raw_bytes += len(part)
                    if (
                        len(body) > cap
                        or self.raw_bytes > 16777216
                        or time.monotonic() - started > 30
                        or time.monotonic() - self.started > 900
                    ):
                        self.failed = True
                        raise Stop("storage_response_bound")

                if kind == "list" and response.status_code == 200:
                    self.raw_inventory = _inventory_projection(bytes(body))
                if response.status_code not in {200, 201, 204, 404, 412}:
                    self.failed = True
                    raise Stop("storage_denied_or_unknown")
                import gzip
                import io

                encoding = response.headers.get("content-encoding", "identity")
                if encoding == "gzip":
                    with gzip.GzipFile(fileobj=io.BytesIO(body)) as handle:
                        decoded = handle.read(1048577)
                    if len(decoded) > 1048576:
                        raise Stop("storage_decoded_object_bound")
                elif encoding == "identity":
                    decoded = body
                else:
                    raise Stop("storage_encoding")
                self.decoded_bytes += len(decoded)
                if self.decoded_bytes > 33554432:
                    raise Stop("storage_decoded_total_bound")

                class Raw:
                    def __init__(self):
                        import io

                        self.buffer = io.BytesIO(bytes(body))

                    def read(self, amount=None):
                        return self.buffer.read(amount)

                    def stream(self, amount=8192, **kwargs):
                        while part := self.read(amount):
                            yield part

                    def close(self):
                        self.buffer.close()

                return AWSResponse(response.url, response.status_code, response.headers, Raw())
            except BaseException:
                self.failed = True
                raise
            finally:
                _cleanup(response.raw.close)

        store.client._endpoint.http_session.send = bounded_send
        store.client.meta.events.register_first("before-send.s3", self.before)
        store.client.meta.events.register("after-call.s3", self.after)

    def before(self, request, **kwargs):
        if self.failed or time.monotonic() - self.started > 900:
            raise Stop("storage_already_stopped_or_deadline")
        request.stream_output = True
        self.raw_inventory = None
        target = urlsplit(request.url)
        object_key = unquote(target.path).removeprefix("/" + self.store.bucket + "/")
        query = parse_qs(target.query)
        kind = (
            "list"
            if query.get("list-type") == ["2"]
            else "get"
            if request.method in {"GET", "HEAD"}
            else "put"
            if request.method == "PUT"
            else None
        )
        if (
            target.scheme != "https"
            or not re.fullmatch(r"[a-z0-9]+\.r2\.cloudflarestorage\.com", target.netloc)
            or target.netloc != self.host
            or target.fragment
            or kind is None
        ):
            self.failed = True
            raise Stop("storage_route")
        if kind != "list" and object_key not in self.keys:
            self.failed = True
            raise Stop("storage_key_outside_plan")
        if kind == "list":
            self.list_query = query
            if (
                request.method != "GET"
                or target.path.rstrip("/") != "/" + self.store.bucket
                or not set(query)
                <= {"list-type", "prefix", "continuation-token", "encoding-type", "max-keys"}
            ):
                raise Stop("storage_list_route")
            if query.get("prefix", [""])[0] not in self.prefixes | {""}:
                raise Stop("storage_list_prefix")
        if self.counts[kind] >= {"get": 400, "list": 100, "put": 100}[kind]:
            self.failed = True
            raise Stop("storage_budget")
        if kind == "put":
            expected = self.expected.get(object_key)
            headers = {
                str(k).lower(): v.decode() if isinstance(v, bytes) else v
                for k, v in request.headers.items()
            }
            if (
                expected is None
                or headers.get(expected[0]) != expected[1]
                or ("if-match" in headers) == ("if-none-match" in headers)
            ):
                self.failed = True
                raise Stop("storage_condition")
            body = request.body
            if isinstance(body, bytes):
                length = len(body)
            elif hasattr(body, "seek") and hasattr(body, "tell"):
                position = body.tell()
                body.seek(0, 2)
                length = body.tell()
                body.seek(position)
            else:
                length = int(headers.get("content-length", "-1"))
            if length < 0 or self.body_bytes + length > 2097152:
                raise Stop("storage_bytes")
            self.body_bytes += length
        self.counts[kind] += 1
        write(
            self.folder / f"wire-{sum(self.counts.values()):04d}.json",
            {"kind": kind, "key_sha256": digest(object_key.encode()), "counts": self.counts},
        )

    def after(self, http_response, parsed, model, context=None, **kwargs):
        status = http_response.status_code
        operation = model.name
        if parsed.get("ResponseMetadata", {}).get("HTTPStatusCode") != status:
            self.failed = True
            raise Stop("storage_status_unknown")
        if (
            operation in {"GetObject", "HeadObject"}
            and status == 404
            and parsed.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}
        ):
            return
        if status not in {200, 201, 204} or (operation == "PutObject" and not parsed.get("ETag")):
            self.failed = True
            raise Stop("unknown_write" if operation == "PutObject" else "storage_denied_or_unknown")
        if operation == "ListObjectsV2":
            from .night_baseline_refresh import _inventory_for_sdk, _list_sdk_text

            contents = parsed.get("Contents")
            count = parsed.get("KeyCount")
            truncated = parsed.get("IsTruncated")
            if contents is None and count == 0 and type(count) is int:
                contents = []  # Explicitly confirmed empty S3 pages omit Contents.
            if (
                type(truncated) is not bool
                or not isinstance(contents, list)
                or len(contents) > 1000
                or type(count) is not int
                or count != len(contents)
                or parsed.get("Name") != self.store.bucket
            ):
                self.failed = True
                raise Stop("storage_list_unverified")
            prefix = self.list_query.get("prefix", [""])[0]
            seen = set()
            for row in contents:
                name = row.get("Key") if isinstance(row, dict) else None
                size = row.get("Size") if isinstance(row, dict) else None
                logical_name = (
                    _list_sdk_text(name, parsed.get("EncodingType"), context)
                    if isinstance(name, str)
                    else None
                )
                if (
                    not isinstance(name, str)
                    or not logical_name
                    or len(logical_name.encode("utf-8")) > 1024
                    or any(ord(c) < 32 or ord(c) == 127 for c in logical_name)
                    or not logical_name.startswith(prefix)
                    or logical_name in seen
                    or type(size) is not int
                    or not 0 <= size <= 2**63 - 1
                ):
                    self.failed = True
                    raise Stop("storage_list_entry_unverified")
                seen.add(logical_name)
            if truncated:
                token = parsed.get("NextContinuationToken")
                if (
                    not isinstance(token, str)
                    or not 1 <= len(token.encode("utf-8")) <= 4096
                    or any(ord(c) < 32 or ord(c) == 127 for c in token)
                    or token == self.list_query.get("continuation-token", [None])[0]
                    or not contents
                ):
                    self.failed = True
                    raise Stop("storage_list_continuation_unverified")
            raw = _inventory_for_sdk(self.raw_inventory, parsed.get("EncodingType"), context)
            if (
                raw is None
                or parsed.get("IsTruncated") is not raw["IsTruncated"]
                or [{"Key": row["Key"], "Size": row["Size"]} for row in contents] != raw["Contents"]
                or parsed.get("NextContinuationToken") != raw["NextContinuationToken"]
                or any(
                    parsed.get(field) != raw[field]
                    for field in (
                        "Name",
                        "KeyCount",
                        "Prefix",
                        "ContinuationToken",
                        "MaxKeys",
                        "EncodingType",
                    )
                )
                or raw["Prefix"] is not None
                and _list_sdk_text(raw["Prefix"], parsed.get("EncodingType"), context) != prefix
                or raw["ContinuationToken"] is not None
                and raw["ContinuationToken"] != self.list_query.get("continuation-token", [None])[0]
            ):
                self.failed = True
                raise Stop("storage_list_projection_mismatch")
            self.raw_inventory = None
            parsed["Contents"] = contents
        if operation == "PutObject":
            self.acks.append(parsed["ETag"].strip('"'))

    def close(self):
        self.store.client._endpoint.http_session.send = self.original_send
        self.store.client.meta.events.unregister("before-send.s3", self.before)
        self.store.client.meta.events.unregister("after-call.s3", self.after)


class _ReferenceStore:
    """Keep merged importer/reference operations inside this invocation's guard.

    The existing helpers choose exact revisions and verify ACK/readback. This
    adapter adds their fixed namespaces to the outer physical budget, never an
    unconditional write API or a fresh budget/stage.
    """

    def __init__(self, store, guard):
        self.store, self.guard = store, guard
        self.source_keys = set()
        self.source_attempted = set()
        self.index_reserve = 0
        self.index_bytes = 0
        self.draining = True
        self.current_source = None
        self.queue_pending = set()
        self.queue_bytes = 0

    def __getattr__(self, name):
        return getattr(self.store, name)

    def _read_key(self, name):
        from .night_baseline_refresh import QUEUE_KEY

        allowed = name == QUEUE_KEY or re.fullmatch(
            r"health/(?:(?:sleep/v1|hrv|signals/v1)/[0-9]{4}/[0-9]{2}/[0-9-]{10}\.json(?:\.gz)?"
            r"|night-context/v1/[0-9]{4}/[0-9]{2}/[0-9-]{10}/[0-9]{8}T[0-9]{9}-nctx-[a-f0-9]{24}\.json"
            r"|night-products/v1/baseline/[0-9]{4}/[0-9]{2}/[0-9]{2}/[a-f0-9]{64}\.json"
            r"|night-products/v1/history/(?:[a-f0-9]{64}/){4}(?:product|provenance)\.json)",
            name,
        )
        if not allowed:
            raise Stop("reference_key_outside_contract")
        self.guard.keys.add(name)

    def get_snapshot(self, name, **kwargs):
        self._read_key(name)
        from botocore.exceptions import ClientError

        from .r2_store import missing_object

        expected = (
            self.guard.expected.get(name)
            if name in self.source_keys - self.source_attempted
            else None
        )
        try:
            snapshot = self.store.get_snapshot(name, **kwargs)
        except ClientError as error:
            if expected and expected[0] == "if-match" and missing_object(error):
                raise Stop("source_preimage_changed") from None
            raise
        if expected and (expected[0] != "if-match" or snapshot.revision != expected[1]):
            raise Stop("source_preimage_changed")
        from .night_baseline_refresh import QUEUE_KEY

        if name == QUEUE_KEY:
            self.queue_pending = set(decode(snapshot.data).get("pending", {}))
            self.queue_bytes = len(snapshot.data)
        return snapshot

    def matches_object(self, name, *args, **kwargs):
        self._read_key(name)
        # Staged canonical targets must revalidate the exact captured preimage,
        # even if a concurrent writer has installed byte-identical incoming data.
        if name in self.source_keys:
            return False
        return self.store.matches_object(name, *args, **kwargs)

    def list_prefix_bounded(self, prefix, **kwargs):
        daily = re.fullmatch(r"health/night-context/v1/[0-9]{4}/[0-9]{2}/[0-9-]{10}/", prefix)
        monthly = (
            re.fullmatch(r"health/night-context/v1/[0-9]{4}/[0-9]{2}/", prefix)
            and type(kwargs.get("max_keys")) is int and kwargs["max_keys"] == 3101
        )
        if not (daily or monthly):
            raise Stop("reference_prefix_outside_contract")
        self.guard.prefixes.add(prefix)
        return self.store.list_prefix_bounded(prefix, **kwargs)

    def can_write_group(self, *, attempts, byte_count):
        from .night_baseline_refresh import ComponentBudgetDeferred, affected_periods

        if not self.draining:
            stream, day = self.current_source
            targets = affected_periods("signals" if stream == "hr" else "hrv", day)
            if (
                len(self.queue_pending | targets.keys()) > 128
                or self.queue_bytes + len(targets) * 3000 > 262144
            ):
                raise ComponentBudgetDeferred("Known queue capacity before source intent")
        fits = self.store.can_write_group(
            attempts=attempts + self.index_reserve,
            byte_count=byte_count + self.index_bytes,
        )
        if not fits and not self.draining:
            # Raised before the helper has saved ANY source intent.
            raise ComponentBudgetDeferred("Canonical group deferred before intent")
        return fits

    def _condition(self, name, revision):
        from .night_baseline_refresh import QUEUE_KEY

        product = bool(
            re.fullmatch(
                r"health/night-products/v1/(?:baseline/[0-9]{4}/[0-9]{2}/[0-9]{2}/[a-f0-9]{64}\.json"
                r"|history/(?:[a-f0-9]{64}/){4}(?:product|provenance)\.json)",
                name,
            )
        )
        expected = ("if-match", revision) if revision else ("if-none-match", "*")
        if name in self.source_keys:
            if self.guard.expected.get(name) != expected:
                raise Stop("source_preimage_changed")
            self.source_attempted.add(name)
        elif name != QUEUE_KEY and not (self.draining and product):
            raise Stop("reference_write_outside_contract")
        self.guard.keys.add(name)
        self.guard.expected[name] = expected

    def create_with_receipt(self, name, *args, **kwargs):
        self._condition(name, None)
        return self.store.create_with_receipt(name, *args, **kwargs)

    def put_if_revision(self, name, *args, revision, **kwargs):
        self._condition(name, revision)
        return self.store.put_if_revision(name, *args, revision=revision, **kwargs)


def _month_compatibility(revisions, month):
    for name in revisions:
        if not name.endswith(".json.gz"):
            continue
        day = name.rsplit("/", 1)[-1].removesuffix(".json.gz")
        try:
            valid = date.fromisoformat(day).isoformat() == day and day[:7] == month
        except ValueError:
            valid = False
        if valid and name.removesuffix(".gz") not in revisions:
            raise Stop("legacy_index_source_requires_review")


def _snapshot(store, object_key):
    from botocore.exceptions import ClientError

    from .r2_store import missing_object

    try:
        snapshot = store.get_snapshot(object_key, max_bytes=262144)
    except ClientError as error:
        if missing_object(error):
            return None
        raise
    if (
        not snapshot.revision
        or snapshot.content_type != "application/json"
        or snapshot.encoding != "gzip"
    ):
        raise Stop("stored_metadata")
    decode(snapshot.data)
    return snapshot


def publish(job, plan_sha, *, confirm=False, store=None):
    if not confirm:
        raise Stop("explicit_storage_phase_required")
    state = job.admission(plan_sha, source=False)
    ready = [r for r in state["rows"] if r["status"] == "ready"]
    days = list(dict.fromkeys(r["date"] for r in ready))[:7]
    ready = [r for r in ready if r["date"] in days]
    selected_count = len(ready)
    # Validate every native source/result/raw proof before importing storage or credentials.
    for row in ready:
        folder = Path(row["folder"])
        if not folder.is_relative_to(job.folder / "runs"):
            raise Stop("source_folder_binding")
        result = json.loads(read(folder / "RESULT.json"))
        receipt_bytes = read(folder / "RECEIPT.json")
        receipt = json.loads(receipt_bytes)
        if (
            type(result["native"]) is not int
            or result["native"] != 0
            or row["native"] != 0
            or receipt["status"] != "complete"
            or result["receipt_sha256"] != digest(receipt_bytes)
            or row["receipt_sha256"] != digest(receipt_bytes)
        ):
            raise Stop("native_source_unverified")
        for field in ("wire", "view"):
            if digest(read(folder / row[field], 5 * 1024 * 1024)) != row[field + "_sha256"]:
                raise Stop("source_bytes_changed")
    owned = store is None
    if store is None:
        import os

        import boto3
        from botocore.config import Config

        from .r2_store import R2Store

        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        if not re.fullmatch(r"[0-9a-f]{32}", account):
            raise Stop("storage_account")
        client = boto3.client(
            "s3",
            endpoint_url="https://" + account + ".r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region_name="auto",
            config=Config(
                request_checksum_calculation="when_required",
                connect_timeout=5,
                read_timeout=20,
                retries={"total_max_attempts": 1},
                s3={"addressing_style": "path"},
            ),
        )
        store = R2Store(client=client)
    run = uuid.uuid4().hex
    folder = job.folder / "runs" / run
    folder.mkdir()
    owner = {"run": run, "plan": plan_sha, "phase": "publish"}
    write(job.folder / "OWNER.lock", owner)
    current = job.admission(plan_sha, source=False, owner=owner)
    if current["rows"] != state["rows"]:
        raise Stop("source_progress_changed_before_owner")
    job.append({"type": "begin", "run": run})
    target_sha = digest((store.client.meta.endpoint_url + "\0" + store.bucket).encode())
    target_file = job.folder / "storage-target.json"
    if target_file.exists():
        if json.loads(read(target_file)) != {"target_sha256": target_sha}:
            raise Stop("storage_target_changed")
    else:
        write(target_file, {"target_sha256": target_sha})
    store.max_bucket_bytes = min(store.max_bucket_bytes, 5368709120)
    store.max_bucket_objects = min(store.max_bucket_objects, 100000)
    store.max_writes_per_run = min(store.max_writes_per_run, 100)
    store.max_write_bytes_per_run = min(store.max_write_bytes_per_run, 2097152)
    months = {r["date"][:7] for r in ready if r["stream"] == "hrv"}
    keys = {key(r["stream"], r["date"]) for r in ready}
    for month in months:
        keys.add(f"health/indexes/hrv/v1/{month[:4]}/{month[5:]}.json")
        keys.update(
            key("hrv", month + f"-{day:02d}")
            for day in range(1, calendar.monthrange(int(month[:4]), int(month[5:]))[1] + 1)
        )
    guard = StorageGuard(store, keys, folder)
    guard.prefixes = {f"health/hrv/{m[:4]}/{m[5:]}/" for m in months}
    staged, done, changed_months = [], [], set()
    reference_store = _ReferenceStore(store, guard)
    try:
        from .night_baseline_refresh import ComponentBudgetDeferred, publish_source, refresh_pending

        # Drain BEFORE source/index reads. A full month needs most of GET400;
        # it must not inherit a hidden extra allowance after source publication.
        baseline_report = refresh_pending(reference_store)
        reference_store.draining = False
        index_reads = sum(calendar.monthrange(int(m[:4]), int(m[5:]))[1] + 2 for m in months)
        # Known not-attempted source targets stay ready; a later explicit publish
        # can proceed after bounded earlier reference work has checkpointed.
        # Inventory is shared, and may itself spend the remaining LIST budget.
        # Recheck before source/index intent rather than discovering this after
        # a canonical source has already been written.
        inventory_fits = not ready or store.can_write_group(attempts=0, byte_count=0)
        source_deferred = bool(ready) and (
            not inventory_fits
            or guard.counts["get"] + 8 * len(ready) + index_reads + 2 > 400
            or guard.counts["list"] + 2 * len(months) > 100
        )
        if source_deferred:
            ready = []
        for row in ready:
            object_key = key(row["stream"], row["date"])
            before = _snapshot(store, object_key)
            previous = decode(before.data) if before else None
            if before:
                write(
                    folder / ("before-" + row["stream"] + "-" + row["date"] + ".bin"), before.data
                )
                if has_curve(row["stream"], row["date"], previous):
                    done.append((row, object_key, before.revision, digest(before.data)))
                    continue
                if row["stream"] == "hr":
                    raise Stop("existing_hr_not_replaced")
            raw_folder = Path(row["folder"])
            value = prepare(
                row["stream"],
                row["date"],
                json.loads(read(raw_folder / row["view"])),
                previous,
                wire=read(raw_folder / row["wire"]),
            )
            if value is None:
                raise Stop("positive_source_changed")
            from .granular import gzip_json

            data = gzip_json(value)
            if len(data) > 262144:
                raise Stop("canonical_body_bound")
            staged.append((row, object_key, before, data))
            guard.expected[object_key] = (
                ("if-match", before.revision) if before else ("if-none-match", "*")
            )
            if row["stream"] == "hrv":
                changed_months.add(row["date"][:7])
        # Aggregate byte capacity is known before any source intent. Admit a
        # fitting newest-first prefix, reserving every affected index, so an
        # explicit later publish can continue the untouched ready suffix.
        admitted, admitted_months, source_bytes = [], set(), 0
        for entry in staged:
            row, _, _, data = entry
            next_months = admitted_months | ({row["date"][:7]} if row["stream"] == "hrv" else set())
            planned_bytes = source_bytes + len(data) + len(next_months) * 262144
            if guard.body_bytes + planned_bytes > 2097152 or not store.can_write_group(
                attempts=0, byte_count=planned_bytes
            ):
                source_deferred = True
                break
            admitted.append(entry)
            admitted_months = next_months
            source_bytes += len(data)
        staged, changed_months = admitted, admitted_months
        index_before = {}
        for month in changed_months:
            index_key = f"health/indexes/hrv/v1/{month[:4]}/{month[5:]}.json"
            index_before[month] = _snapshot(store, index_key)
            old = index_before[month]
            guard.expected[index_key] = (
                ("if-match", old.revision) if old else ("if-none-match", "*")
            )
            # Resolve known legacy-only membership before ANY curve PUT. A
            # canonical source wins over its old shadow; neither is deleted.
            prefix = f"health/hrv/{month[:4]}/{month[5:]}/"
            _month_compatibility(store.list_object_revisions(prefix), month)

        def put_verify(object_key, data, before):
            write(
                folder / f"put-intent-{len(guard.acks):03d}.json",
                {
                    "key": object_key,
                    "before_revision": before.revision if before else None,
                    "sha256": digest(data),
                },
            )
            ack = (
                store.put_if_revision(
                    object_key, data, "application/json", revision=before.revision, encoding="gzip"
                )
                if before
                else store.create_with_receipt(
                    object_key, data, "application/json", encoding="gzip"
                )
            )
            if not ack.accepted or not ack.revision:
                raise Stop("conditional_or_unknown_write")
            after = _snapshot(store, object_key)
            if after is None or after.revision != ack.revision or after.data != data:
                raise Stop("readback_unverified")
            write(folder / f"postimage-{len(guard.acks):03d}.bin", data)
            return after

        reference_store.source_keys = {entry[1] for entry in staged}
        reference_store.index_reserve = len(changed_months)
        reference_store.index_bytes = len(changed_months) * 262144
        actual_months = set()
        queued = 0
        for row, object_key, _before, data in staged:
            reference_store.current_source = (row["stream"], row["date"])
            try:
                publish_source(
                    reference_store, object_key, data, "application/json", encoding="gzip"
                )
            except ComponentBudgetDeferred:
                source_deferred = True
                break
            queued += 1
            after = _snapshot(store, object_key)
            if after is None or after.data != data:
                raise Stop("canonical_source_readback_changed")
            write(folder / f"source-postimage-{len(done):03d}.bin", after.data)
            done.append((row, object_key, after.revision, digest(data)))
            if row["stream"] == "hrv":
                actual_months.add(row["date"][:7])
        changed_months = actual_months

        for month in sorted(changed_months):
            from .granular import gzip_json
            from .health_history_index import build_month_index

            prefix = f"health/hrv/{month[:4]}/{month[5:]}/"
            revisions = store.list_object_revisions(prefix)
            _month_compatibility(revisions, month)
            sources = {}
            for name, revision in revisions.items():
                if name in keys and name.endswith(".json"):
                    sources[name.rsplit("/", 1)[-1][:-5]] = (name, revision)
            before = index_before[month]
            old = decode(before.data) if before else None
            if old and not {r["date"] for r in old.get("days", [])} <= set(sources):
                raise Stop("index_source_missing")

            class VerifiedSources:
                def __init__(self, pinned):
                    self.pinned = pinned

                def get(self, name):
                    snap = _snapshot(store, name)
                    if snap is None or snap.revision.strip('"') != self.pinned[name]:
                        raise Stop("index_revision_changed")
                    has_curve("hrv", name.rsplit("/", 1)[-1][:-5], decode(snap.data))
                    return snap.data

            index = build_month_index(
                "hrv", month, sources, VerifiedSources(revisions), previous=old
            )
            if any(r["status"] not in {"available", "no_data"} for r in index["days"]):
                raise Stop("index_unverified")
            put_verify(
                f"health/indexes/hrv/v1/{month[:4]}/{month[5:]}.json", gzip_json(index), before
            )
        evidence = {
            "status": "remote_verified_cleanup_pending",
            "baseline_refresh": {
                **baseline_report,
                "status": "bounded_pending" if queued else baseline_report["status"],
                "sources_queued": queued,
                "source_targets_deferred": selected_count - len(done),
            },
            "source_deferred": source_deferred,
            "source_calls": 0,
            "counts": guard.counts,
            "body_bytes": guard.body_bytes,
            "acknowledged_writes": len(guard.acks),
            "destinations": [
                {"date": r["date"], "stream": r["stream"], "key": k, "revision": rev, "sha256": sha}
                for r, k, rev, sha in done
            ],
        }
        # Genuine remote acknowledgements/readbacks remain durable even when
        # mandatory outer cleanup subsequently fails. This is not completion.
        write(folder / "REMOTE-VERIFIED.json", evidence)
    finally:
        _cleanup(guard.close, *([store.client.close] if owned else []))
    # Only after each closer succeeded may this run publish its terminal state.
    if json.loads(read(job.folder / "OWNER.lock")) != owner:
        raise Stop("owner_binding_changed")
    write(folder / "RECEIPT.json", {**evidence, "status": "complete"})
    for row, object_key, revision, sha in done:
        job.append(
            {
                "type": "target",
                "date": row["date"],
                "stream": row["stream"],
                "status": "published",
                "destination_key": object_key,
                "revision": revision,
                "sha256": sha,
            }
        )
    job.append({"type": "reference_refresh", "report": evidence["baseline_refresh"]})
    job.append({"type": "end", "run": run, "status": "published"})
    (job.folder / "OWNER.lock").unlink()
    return job.status()
