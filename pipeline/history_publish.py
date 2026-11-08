"""Explicit R2-only phase: source-proof verification, conditional writes and readback."""

from __future__ import annotations

import calendar
import json
import re
import time
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from .history_canonical import decode, has_curve, key, prepare
from .history_state import Stop, digest, read, write


def _cleanup(*closers, primary=False):
    """Attempt every close; preserve an active primary or the first close error."""
    first = None
    for close in closers:
        try:
            close()
        except BaseException as error:
            if first is None:
                first = error
    if not primary and first is not None:
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
    def __init__(self, store, keys, folder, execution_context=None):
        self.store, self.keys, self.folder = store, keys, folder
        self.execution_context = execution_context if execution_context is not None else {}
        self.execution_context.setdefault("counts", {"get": 0, "list": 0, "put": 0})
        self.execution_context.setdefault("body_bytes", 0)
        limit = self.execution_context.setdefault("publication_get_limit", 6000)
        if type(limit) is not int or not 0 <= limit <= 6000:
            raise Stop("storage_execution_budget")
        self.baseline = self.execution_context.setdefault("round_baseline", {
            "counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0,
        })
        attribution = self.execution_context.get("span_account_usage")
        self.shared_cost = (attribution["source_pool"] if attribution is not None else {
            "counts": dict.fromkeys(("get", "list", "put"), 0), "body_bytes": 0, "publication_phases": 0})
        independent = self.execution_context.setdefault("independent_gets", 0)
        if (type(independent) is not int or not 0 <= independent <= 256
                or any(type(self.execution_context["counts"].get(k)) is not int
                       or type(self.baseline.get("counts", {}).get(k)) is not int
                       or not 0 <= self.baseline["counts"][k] <= self.execution_context["counts"][k]
                       for k in ("get", "list", "put"))
                or type(self.execution_context["body_bytes"]) is not int
                or type(self.baseline.get("body_bytes")) is not int
                or not 0 <= self.baseline["body_bytes"] <= self.execution_context["body_bytes"]):
            raise Stop("storage_execution_budget")
        self.counts = {"get": 0, "list": 0, "put": 0}
        self.expected = {}
        self.failed = False
        self.acks = []
        self.body_bytes = 0
        self.raw_bytes = 0
        self.decoded_bytes = 0
        self.started = time.monotonic()
        self.network_seconds = 0.0
        self.prefixes = set()
        self.host = urlsplit(store.client.meta.endpoint_url).netloc
        self.original_send = store.client._endpoint.http_session.send

        def bounded_send(request):
            import time

            from botocore.awsrequest import AWSResponse

            network_started = time.monotonic()
            try:
                response = self.original_send(request)
            except BaseException:
                self.failed = True
                raise Stop("unknown_storage_transport") from None
            finally:
                self.network_seconds += time.monotonic() - network_started
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
            primary = False
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
                primary = True
                self.failed = True
                raise
            finally:
                self.network_seconds += time.monotonic() - started
                _cleanup(response.raw.close, primary=primary)

        store.client._endpoint.http_session.send = bounded_send
        try:
            store.client.meta.events.register_first("before-send.s3", self.before)
            store.client.meta.events.register("after-call.s3", self.after)
        except BaseException:
            _cleanup(self.close, primary=True)
            raise

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
        totals = self.execution_context["counts"]
        if (
            totals["get"] - self.baseline["counts"]["get"] + self.shared_cost["counts"]["get"] + (kind == "get") > self.execution_context["publication_get_limit"]
            or totals["get"] - self.baseline["counts"]["get"] + self.shared_cost["counts"]["get"] + (kind == "get") + self.execution_context["independent_gets"] > 6256
            or totals["list"] + totals["put"] - self.baseline["counts"]["list"] - self.baseline["counts"]["put"] + sum(self.shared_cost["counts"][item] for item in ("list", "put")) + (kind in {"list", "put"}) > 1000
        ):
            self.failed = True
            raise Stop("storage_execution_budget")
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
            if (
                length < 0
                or self.body_bytes + length > 2097152
                or self.execution_context["body_bytes"] - self.baseline["body_bytes"] + self.shared_cost["body_bytes"] + length > 10485760
            ):
                raise Stop("storage_bytes")
            self.body_bytes += length
            self.execution_context["body_bytes"] += length
        self.counts[kind] += 1
        totals[kind] += 1
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
        _cleanup(
            lambda: self.store.client.meta.events.unregister("before-send.s3", self.before),
            lambda: self.store.client.meta.events.unregister("after-call.s3", self.after),
        )


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
        self.queue_rows = None
        self.queue_snapshot = None
        self.queue_bytes = 0

    def __getattr__(self, name):
        return getattr(self.store, name)

    def physical_counts(self):
        return dict(self.guard.counts)

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

        from .night_baseline_refresh import QUEUE_KEY
        from .r2_store import missing_object

        expected = (
            self.guard.expected.get(name)
            if name in self.source_keys - self.source_attempted
            else None
        )
        try:
            snapshot = self.store.get_snapshot(name, **kwargs)
        except ClientError as error:
            if name == QUEUE_KEY and missing_object(error):
                self.queue_rows = {}
                self.queue_snapshot = {"pending": {}}
            if expected and expected[0] == "if-match" and missing_object(error):
                raise Stop("source_preimage_changed") from None
            raise
        if expected and (expected[0] != "if-match" or snapshot.revision != expected[1]):
            raise Stop("source_preimage_changed")
        from .night_baseline_refresh import QUEUE_KEY

        if name == QUEUE_KEY:
            self.queue_snapshot = decode(snapshot.data)
            self.queue_rows = self.queue_snapshot.get("pending", {})
            self.queue_pending = set(self.queue_rows)
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
            and type(kwargs.get("max_keys")) is int
            and kwargs["max_keys"] == 3101
        )
        if not (daily or monthly):
            raise Stop("reference_prefix_outside_contract")
        self.guard.prefixes.add(prefix)
        return self.store.list_prefix_bounded(prefix, **kwargs)

    def list_revisions_bounded(self, prefix, *, max_keys):
        # The consumer chooses only per-window canonical/context months. The
        # physical guard still charges every page and verifies the raw envelope.
        canonical = re.fullmatch(r"health/(?:sleep/v1|hrv|signals/v1)/[0-9]{4}/[0-9]{2}/", prefix)
        context = re.fullmatch(r"health/night-context/v1/[0-9]{4}/[0-9]{2}/", prefix)
        if not (canonical and max_keys == 63 or context and max_keys == 3101):
            raise Stop("reference_prefix_outside_contract")
        self.guard.prefixes.add(prefix)
        return self.store.list_revisions_bounded(prefix, max_keys=max_keys)

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


def _reference_complete(reference):
    if "rolling_plan_complete" in reference:
        return reference["rolling_plan_complete"] is True
    if "cohort_complete" in reference:
        return reference["cohort_complete"] is True
    if "continuation_complete" in reference:
        # A legacy selected-period receipt does not prove its original cohort.
        return reference.get("original_cohort_complete") is True
    return (
        reference.get("status") in {"complete", "waiting_until_close"}
        and reference.get("closed_pending") == 0
        and not reference.get("held_periods") and not reference.get("blocked")
    )


def _completed_context(job, state, target_sha, execution_context, finalize_dates):
    """Reuse only this journal's terminal publications, never an unknown parent.

    Local bytes accelerate initial assembly; the reference publisher still reads
    both source/context fences directly from storage. Receipt totals are carried
    only while the preceding round has unfinished closed reference work.
    """
    from .r2_store import R2ObjectSnapshot

    cache = execution_context.setdefault("snapshot_cache", {})
    published = {
        (row["date"], row["stream"]): row for row in state["rows"]
        if row["status"] == "published"
    }
    active, targets, references, latest = None, [], [], None
    local_snapshots = {}
    cohort = set(finalize_dates) if finalize_dates is not None else None
    frames, journal_tip = job.events()
    requested_round = execution_context.get("round")
    round_start = None
    if requested_round is not None:
        starts = [frame for frame in frames if frame["event"].get("type") == "daily_round_started"
                  and frame["event"].get("round") == requested_round]
        if not re.fullmatch(r"[a-f0-9]{32}", requested_round) or len(starts) != 1:
            raise Stop("publication_round_unverified")
        round_start = starts[0]
        event = round_start["event"]
        if (not isinstance(event.get("dates"), list) or not isinstance(event.get("finalize_dates"), list)
                or not set(event["dates"]) <= {r["date"] for r in state["rows"]}
                or not set(event["finalize_dates"]) <= {r["date"] for r in state["rows"]}):
            raise Stop("publication_round_unverified")
    latest_end_sequence = None
    legacy_phases = 0
    for frame in frames:
        event = frame["event"]
        if event["type"] == "begin":
            active, targets, references = event["run"], [], []
        elif event["type"] == "target" and active is not None:
            targets.append(event)
        elif event["type"] == "reference_refresh" and active is not None:
            references.append(event.get("report"))
        elif event["type"] == "end":
            run, active = active, None
            if event["status"] != "published":
                continue
            if not isinstance(run, str) or not re.fullmatch(r"[a-f0-9]{32}", run):
                raise Stop("publication_checkpoint_unverified")
            folder = job.folder / "runs" / run
            try:
                receipt = json.loads(read(folder / "RECEIPT.json"))
                remote = json.loads(read(folder / "REMOTE-VERIFIED.json"))
            except (FileNotFoundError, KeyError, ValueError):
                raise Stop("publication_checkpoint_unverified") from None
            if not isinstance(receipt, dict) or not isinstance(remote, dict):
                raise Stop("publication_checkpoint_unverified")
            if (
                receipt.get("status") != "complete"
                or remote.get("status") != "remote_verified_cleanup_pending"
                or {k: v for k, v in receipt.items() if k != "status"}
                != {k: v for k, v in remote.items() if k != "status"}
                or len(references) != 1
                or receipt.get("baseline_refresh") != references[0]
                or receipt.get("run", run) != run
                or receipt.get("plan_sha256", job.plan_sha) != job.plan_sha
                or receipt.get("target_sha256", target_sha) != target_sha
                or not isinstance(receipt.get("destinations"), list)
                or not all(isinstance(row, dict) for row in receipt["destinations"])
            ):
                raise Stop("publication_checkpoint_unverified")
            destinations = receipt["destinations"]
            if len(destinations) != len(targets) or any(
                target.get("status") != "published" or any(
                    target.get(field) != destination.get(other)
                    for field, other in (
                        ("date", "date"), ("stream", "stream"),
                        ("destination_key", "key"), ("revision", "revision"), ("sha256", "sha256"),
                    )
                ) for target, destination in zip(targets, destinations, strict=True)
            ):
                raise Stop("publication_checkpoint_unverified")
            legacy_phases += 1
            latest = receipt
            latest_end_sequence = frame["sequence"]
            reference = receipt.get("baseline_refresh", {})
            if not isinstance(reference, dict):
                raise Stop("publication_checkpoint_unverified")
            complete = _reference_complete(reference)
            same_cohort = cohort is not None and reference.get("finalize_dates") == sorted(cohort)
            if complete and not same_cohort:
                legacy_phases = 0
                local_snapshots.clear()
                continue
            for ordinal, destination in enumerate(destinations):
                day, stream = destination.get("date"), destination.get("stream")
                row = published.get((day, stream))
                if (
                    row is None or stream not in {"hr", "hrv"}
                    or (cohort is not None and day not in cohort)
                    or destination.get("key") != key(stream, day)
                    or any(row.get(field) != destination.get(other) for field, other in (
                        ("destination_key", "key"), ("revision", "revision"), ("sha256", "sha256"),
                    ))
                    or not isinstance(destination.get("revision"), str)
                    or not re.fullmatch(r'"[^"\r\n]+"', destination["revision"])
                ):
                    continue
                # Older complete receipts used _snapshot's fixed JSON/gzip
                # metadata invariant and these two bounded filenames.
                names = (f"source-postimage-{ordinal:03d}.bin", f"before-{stream}-{day}.bin")
                metadata = destination.get("snapshot")
                if metadata is not None:
                    if (
                        not isinstance(metadata, dict) or metadata.get("file") not in names
                        or metadata.get("content_type") != "application/json"
                        or metadata.get("encoding") != "gzip"
                    ):
                        continue
                    names = (metadata["file"],)
                for name in names:
                    try:
                        body = read(folder / name, 262144)
                    except (FileNotFoundError, KeyError):
                        continue
                    if digest(body) != destination.get("sha256") or (
                        metadata is not None and metadata.get("bytes") != len(body)
                    ):
                        continue
                    decode(body)
                    local_snapshots[destination["key"]] = R2ObjectSnapshot(
                        data=body, revision=destination["revision"],
                        content_type="application/json", encoding="gzip",
                    )
                    break
    cache.update(local_snapshots)
    if latest is not None:
        reference = latest.get("baseline_refresh", {})
        complete = _reference_complete(reference) and not any(
            row["status"] in {"ready", "pending"}
            and row["date"] in reference.get("finalize_dates", []) for row in state["rows"]
        )
        if (not complete or requested_round is not None
                or execution_context.get("continuation_sha256") is not None
                or execution_context.get("rolling_plan") is not None
                or execution_context.get("retain_reference_ledger")):
            totals = reference.get("execution_totals", {})
            prior_rolling = totals.get("rolling_plan_sha256")
            current_rolling = execution_context.get("rolling_plan", {}).get("reference_plan_sha256")
            if not complete and prior_rolling is not None and prior_rolling != current_rolling:
                raise Stop("unfinished_rolling_plan_requires_same_plan")
            counts = totals.get("counts", {})
            if not (all(type(counts.get(kind)) is int and counts[kind] >= 0
                        for kind in ("get", "list", "put"))
                    and type(totals.get("body_bytes")) is int and totals["body_bytes"] >= 0):
                raise Stop("publication_checkpoint_unverified")
            limit = totals.get("publication_get_limit", 2256)
            if type(limit) is not int or not 0 <= limit <= 6000:
                raise Stop("publication_checkpoint_unverified")
            new_round = requested_round is not None and totals.get("round") != requested_round
            joining_round = new_round and not complete and totals.get("round") is None and limit == 6000
            if new_round and ((not complete and not joining_round) or round_start["sequence"] <= latest_end_sequence):
                raise Stop("unfinished_publication_round_requires_continuation")
            if joining_round and not set(reference.get("finalize_dates", [])) <= set(finalize_dates or []):
                raise Stop("unfinished_publication_cohort_changed")
            prior_continuation = totals.get("continuation_sha256")
            if (not new_round and prior_continuation
                    and execution_context.get("continuation_sha256") != prior_continuation
                    and not (execution_context.get("rolling_plan") is not None
                             and reference.get("continuation_complete") is True)):
                raise Stop("explicit_continuation_required")
            if new_round and not joining_round:
                execution_context["round_baseline"] = {
                    "counts": dict(counts), "body_bytes": totals["body_bytes"],
                }
                execution_context["phases"] = 0
                execution_context.setdefault("independent_gets", 0)
            else:
                execution_context["publication_get_limit"] = min(
                    execution_context.get("publication_get_limit", 6000), limit
                )
                if "round_baseline" in totals:
                    execution_context["round_baseline"] = totals["round_baseline"]
                execution_context["phases"] = max(execution_context.get("phases", 0), totals.get("phases", legacy_phases))
                execution_context["independent_gets"] = max(
                    execution_context.get("independent_gets", 0), totals.get("independent_gets", 0)
                )
            current = execution_context.setdefault("counts", {"get": 0, "list": 0, "put": 0})
            for kind in ("get", "list", "put"):
                current[kind] = max(current[kind], counts[kind])
            execution_context["body_bytes"] = max(execution_context.get("body_bytes", 0), totals["body_bytes"])
    reference_round = execution_context.get("reference_round_sha256")
    if reference_round is not None:
        from .rolling_reference_cli import apply_reference_round

        apply_reference_round(
            job, job.plan_sha,
            execution_context.get("rolling_plan", {}).get("reference_plan_sha256"),
            reference_round, target_sha=target_sha, execution_context=execution_context,
            latest_receipt=latest, journal_tip=journal_tip,
        )
    successor = execution_context.get("reference_successor_sha256")
    if successor is not None:
        from .history_reference_successor import apply

        apply(job, successor, target_sha=target_sha, context=execution_context,
              latest=latest, journal_tip=journal_tip, cohort=finalize_dates)
    from .rolling_reference_span import StoredReferenceJob, shared_span_usage, source_epoch_key

    if not isinstance(job, StoredReferenceJob):
        annotation = shared_span_usage(job, source_epoch_key(execution_context))
        if annotation is not None:
            execution_context["span_account_usage"] = annotation
    return journal_tip


def admit_continuation(
    job, plan_sha, *, confirm=False, prior_receipt_sha256, independent_gets, period_names,
):
    """Freeze an explicit R2-only extension of one known terminal legacy round."""
    from .night_baseline_refresh import affected_periods, is_closed

    if not confirm:
        raise Stop("explicit_storage_phase_required")
    state = job.admission(plan_sha, source=False)
    periods = sorted(set(period_names))
    if (
        not 1 <= len(periods) <= 3 or len(periods) != len(period_names)
        or any(not re.fullmatch(r"(?:week/[0-9]{4}-W[0-9]{2}|month/[0-9]{4}-[0-9]{2})", name)
               for name in periods)
        or type(independent_gets) is not int or not 0 <= independent_gets <= 256
        or not re.fullmatch(r"[a-f0-9]{64}", prior_receipt_sha256)
    ):
        raise Stop("continuation_binding_unverified")
    target_sha = json.loads(read(job.folder / "storage-target.json"))["target_sha256"]
    context = {}
    tip = _completed_context(job, state, target_sha, context, None)
    frames, _ = job.events()
    runs = [frame["event"]["run"] for frame in frames
            if frame["event"]["type"] == "end" and frame["event"]["status"] == "published"]
    if not runs:
        raise Stop("continuation_binding_unverified")
    receipt_bytes = read(job.folder / "runs" / runs[-1] / "RECEIPT.json")
    receipt = json.loads(receipt_bytes)
    if (
        digest(receipt_bytes) != prior_receipt_sha256
        or context.get("publication_get_limit") != 2000
        or _reference_complete(receipt["baseline_refresh"])
        or any(frame["event"]["type"] == "publication_continuation" for frame in frames)
    ):
        raise Stop("continuation_binding_unverified")
    reference = receipt["baseline_refresh"]
    cohort, closure_day = reference.get("finalize_dates"), reference.get("closure_day")
    if (not isinstance(cohort, list) or not cohort
            or any(not isinstance(day, str) for day in cohort)
            or len(set(cohort)) != len(cohort)
            or not isinstance(closure_day, str)):
        raise Stop("continuation_cohort_unverified")
    # The exact receipt cohort can include reused dates without source-plan rows.
    # Only published plan rows below authorize the requested reference periods.
    try:
        if any(date.fromisoformat(day).isoformat() != day for day in [*cohort, closure_day]):
            raise ValueError()
    except ValueError:
        raise Stop("continuation_cohort_unverified") from None
    allowed = set()
    for row in state["rows"]:
        if row["status"] == "published" and row["date"] in cohort:
            allowed.update(name for name, period in affected_periods(
                "signals" if row["stream"] == "hr" else "hrv", row["date"]
            ).items() if is_closed(period, closure_day))
    if not set(periods) <= allowed:
        raise Stop("continuation_periods_outside_closed_cohort")
    admission = {
        "schema": 1, "purpose": "pending_references_only", "job": str(job.folder),
        "plan_sha256": plan_sha, "target_sha256": target_sha, "journal_tip": tip,
        "prior_receipt_sha256": prior_receipt_sha256, "prior_run": runs[-1],
        "prior_usage": {"counts": dict(context["counts"]), "body_bytes": context["body_bytes"],
                        "publication_get_limit": 2000, "phases": context.get("phases", 0)},
        "independent_gets": independent_gets, "period_names": periods,
        "finalize_dates": sorted(cohort), "closure_day": closure_day,
        "publication_get_limit": 6000, "total_get_limit": 6256,
        "class_a_limit": 1000, "body_bytes_limit": 10485760, "phase_limit": 14,
    }
    from .history_state import encoded

    sha = digest(encoded(admission))
    write(job.folder / ("publication-continuation-" + sha + ".json"), admission)
    job.append({"type": "publication_continuation", "sha256": sha})
    return sha


def _continuation(job, sha):
    if not isinstance(sha, str) or not re.fullmatch(r"[a-f0-9]{64}", sha):
        raise Stop("continuation_binding_unverified")
    body = read(job.folder / ("publication-continuation-" + sha + ".json"))
    admission = json.loads(body)
    frames, _ = job.events()
    matches = [frame for frame in frames if frame["event"] == {
        "type": "publication_continuation", "sha256": sha,
    }]
    if (
        digest(body) != sha or len(matches) != 1
        or matches[0]["previous"] != admission.get("journal_tip")
        or admission.get("job") != str(job.folder) or admission.get("plan_sha256") != job.plan_sha
        or admission.get("purpose") != "pending_references_only"
        or admission.get("publication_get_limit") != 6000
        or admission.get("total_get_limit") != 6256 or admission.get("phase_limit") != 14
        or admission.get("class_a_limit") != 1000 or admission.get("body_bytes_limit") != 10485760
        or digest(read(job.folder / "runs" / admission["prior_run"] / "RECEIPT.json"))
        != admission.get("prior_receipt_sha256")
    ):
        raise Stop("continuation_binding_unverified")
    # Older immutable admissions omitted this binding. Recover it only from the
    # exact original receipt already pinned above, without rewriting old bytes.
    prior = json.loads(read(job.folder / "runs" / admission["prior_run"] / "RECEIPT.json"))
    original = prior["baseline_refresh"]
    for field in ("finalize_dates", "closure_day"):
        if field in admission and admission[field] != original.get(field):
            raise Stop("continuation_cohort_unverified")
        admission[field] = original.get(field)
    return admission

def source_free_binding(job, plan_sha, proof_path, proof_sha, *, owner=None):
    """Pin one existing stopped daily round; this is never source admission."""
    state = job.admission(plan_sha, source=False, owner=owner)
    body = read(proof_path)
    if not isinstance(proof_sha, str) or not re.fullmatch(r"[a-f0-9]{64}", proof_sha) or digest(body) != proof_sha:
        raise Stop("source_free_proof_changed")
    proof = json.loads(body)
    if (
        not isinstance(proof, dict) or proof.get("schema") != 1
        or proof.get("operation") != "source-free-publication"
        or proof.get("plan_sha256") != plan_sha
        or any(not isinstance(proof.get(field), str) or not re.fullmatch(r"[a-f0-9]{64}", proof[field])
               for field in ("storage_target_sha256", "source_stop_proof_sha256"))
        or type(proof.get("independent_gets")) is not int or not 0 <= proof["independent_gets"] <= 256
        or not isinstance(proof.get("source_dates"), list)
    ):
        raise Stop("source_free_binding_changed")
    frames, tip = job.events()
    prefix = proof.get("journal", {})
    count = prefix.get("frames") if isinstance(prefix, dict) else None
    if (type(count) is not int or not 1 <= count <= len(frames)
            or prefix.get("tip") != (frames[count]["previous"] if count < len(frames) else tip)):
        raise Stop("source_free_journal_changed")
    active, start = None, None
    for index, frame in enumerate(frames):
        event = frame["event"]
        if event["type"] == "daily_round_started":
            if active is not None:
                raise Stop("daily_round_unclosed")
            active, start = event, index
        elif event["type"] == "daily_round_finished":
            if active is None or active["round"] != event["round"]:
                raise Stop("daily_round_binding_changed")
            active = None
    planned = {row["date"] for row in job.plan["targets"]}
    if (
        active is None or start >= count or active.get("round") != proof.get("daily_round")
        or not isinstance(active.get("round"), str) or not re.fullmatch(r"[a-f0-9]{32}", active["round"])
        or any(not isinstance(active.get(field), list)
               or any(not isinstance(day, str) or day not in planned for day in active[field])
               or len(set(active[field])) != len(active[field])
               for field in ("dates", "new_dates", "stored_dates", "finalize_dates"))
        or set(active["dates"]) != set(active["new_dates"]) | set(active["stored_dates"])
        or set(active["new_dates"]) & set(active["stored_dates"])
        or not set(active["dates"]) <= set(active["finalize_dates"])
        or type(active.get("max_dates")) is not int or not 1 <= active["max_dates"] <= 30
        or len(active["new_dates"]) > active["max_dates"]
        or type(active.get("max_http")) is not int or not 1 <= active["max_http"] <= 240
    ):
        raise Stop("source_free_round_changed")
    wanted = set()
    for frame in frames[start + 1:count]:
        event = frame["event"]
        if event["type"] == "date_closed":
            run, day = event["id"].split(":")
            if day not in active["dates"]:
                raise Stop("source_free_selection_changed")
            wanted.add((run, day))
    for row in state["rows"]:
        if row["date"] in active["dates"] and row["status"] in {"ready", "published"}:
            folder = Path(row["folder"])
            if folder.parent.parent != job.folder / "runs" or folder.name != row["date"]:
                raise Stop("source_folder_binding")
            wanted.add((folder.parent.name, folder.name))
    supplied = set()
    for source in proof["source_dates"]:
        if (not isinstance(source, dict) or not isinstance(source.get("run"), str)
                or not re.fullmatch(r"[a-f0-9]{32}", source["run"])
                or not isinstance(source.get("date"), str) or source["date"] not in active["dates"]):
            raise Stop("source_free_source_changed")
        identity = source["run"], source["date"]
        if identity in supplied or identity not in wanted:
            raise Stop("source_free_source_changed")
        supplied.add(identity)
        folder = job.folder / "runs" / source["run"] / source["date"]
        values = {}
        for name in ("intent", "result", "receipt"):
            data = read(folder / (name.upper() + ".json"))
            if source.get(name + "_sha256") != digest(data):
                raise Stop("source_free_source_changed")
            values[name] = json.loads(data)
        intent, result, receipt = (values[name] for name in ("intent", "result", "receipt"))
        if (
            intent.get("plan") != plan_sha or intent.get("run") != source["run"]
            or intent.get("date") != source["date"]
            or type(result.get("native")) is not int or result["native"] not in (0, 2)
            or (result["native"] == 0) != (receipt.get("status") == "complete")
            or receipt.get("status") not in {"complete", "stopped"}
            or receipt.get("cleanup_status") != "complete"
            or result.get("receipt_sha256") != source["receipt_sha256"]
            or receipt.get("intent_sha256") != source["intent_sha256"]
            or not isinstance(receipt.get("targets"), list)
        ):
            raise Stop("source_free_native_unverified")
        for target in receipt["targets"]:
            if target.get("date") != source["date"] or target.get("stream") not in intent["streams"]:
                raise Stop("source_free_source_changed")
            if target.get("status") != "ready":
                continue
            if target.get("source_verified") is not True:
                raise Stop("native_source_unverified")
            for field in ("wire", "view"):
                name = target.get(field)
                if not isinstance(name, str) or Path(name).name != name:
                    raise Stop("source_folder_binding")
                if digest(read(folder / name, 5 * 1024 * 1024)) != target.get(field + "_sha256"):
                    raise Stop("source_bytes_changed")
            current = next(row for row in state["rows"]
                           if (row["date"], row["stream"]) == (target["date"], target["stream"]))
            if (current["status"] not in {"ready", "published"}
                    or current.get("folder") != str(folder)
                    or current.get("native") != result["native"]
                    or current.get("receipt_sha256") != source["receipt_sha256"]
                    or any(current.get(field) != target.get(field)
                           for field in ("wire", "view", "wire_sha256", "view_sha256"))):
                raise Stop("source_free_target_changed")
    if not supplied or supplied != wanted:
        raise Stop("source_free_source_changed")
    # Only this admission's ordinary, fully closed publications may extend its prefix.
    current_run, reports = None, []
    for frame in frames[count:]:
        event = frame["event"]
        kind = event["type"]
        if kind == "begin" and current_run is None:
            current_run, reports = event["run"], []
        elif kind == "target" and current_run is not None and event.get("status") == "published":
            if event.get("date") not in active["dates"]:
                raise Stop("source_free_selection_changed")
        elif kind == "reference_refresh" and current_run is not None:
            reports.append(event["report"])
        elif kind == "end" and current_run == event.get("run") and event.get("status") == "published":
            folder = job.folder / "runs" / current_run
            receipt = json.loads(read(folder / "RECEIPT.json"))
            remote = json.loads(read(folder / "REMOTE-VERIFIED.json"))
            reference = receipt.get("baseline_refresh", {})
            totals = reference.get("execution_totals", {})
            if (receipt.get("status") != "complete" or remote.get("status") != "remote_verified_cleanup_pending"
                    or {k: v for k, v in receipt.items() if k != "status"}
                    != {k: v for k, v in remote.items() if k != "status"}
                    or receipt.get("run") != current_run or receipt.get("plan_sha256") != plan_sha
                    or receipt.get("target_sha256") != proof["storage_target_sha256"]
                    or receipt.get("source_calls") != 0 or reports != [reference]
                    or reference.get("finalize_dates") != sorted(active["finalize_dates"])
                    or totals.get("round") != active["round"]
                    or totals.get("source_free_proof_sha256") != proof_sha):
                raise Stop("source_free_publication_changed")
            current_run = None
        else:
            raise Stop("source_free_journal_changed")
    if current_run is not None:
        raise Stop("source_free_journal_changed")
    return proof, active


def publish(
    job, plan_sha, *, confirm=False, store=None, target_dates=None, execution_context=None,
    finalize_dates=None, defer_references=False, continuation_sha256=None, source_free_proof=None,
):
    if not confirm:
        raise Stop("explicit_storage_phase_required")
    if execution_context and execution_context.get("reference_successor_sha256") is not None and (
        target_dates != [] or not finalize_dates or defer_references
        or continuation_sha256 is not None or source_free_proof is not None
        or execution_context.get("rolling_plan") is not None
        or execution_context.get("reference_round_sha256") is not None
    ):
        raise Stop("daily_successor_references_only")
    if execution_context and execution_context.get("rolling_plan") is not None and (
        target_dates != [] or finalize_dates is not None or defer_references
        or continuation_sha256 is not None or source_free_proof is not None
    ):
        raise Stop("rolling_plan_references_only")
    state = job.admission(plan_sha, source=False)
    if source_free_proof is not None:
        proof, round_binding = source_free_binding(job, plan_sha, *source_free_proof)
        if (continuation_sha256 is not None or target_dates not in (round_binding["dates"], [])
                or finalize_dates != round_binding["finalize_dates"]
                or execution_context is None or execution_context.get("round") != round_binding["round"]
                or execution_context.get("source_free_proof_sha256") != source_free_proof[1]
                or execution_context.get("independent_gets", 0) < proof["independent_gets"]):
            raise Stop("source_free_binding_changed")
    continuation = _continuation(job, continuation_sha256) if continuation_sha256 else None
    if continuation is not None:
        if target_dates not in (None, []) or finalize_dates is not None or defer_references:
            raise Stop("continuation_references_only")
        target_dates = []
        execution_context = execution_context if execution_context is not None else {}
        execution_context["continuation_sha256"] = continuation_sha256
    ready = [
        r
        for r in state["rows"]
        if r["status"] == "ready" and (target_dates is None or r["date"] in target_dates)
    ]
    days = list(dict.fromkeys(r["date"] for r in ready))[:7]
    ready = [r for r in ready if r["date"] in days]
    # Validate every native source/result/raw proof before importing storage or credentials.
    for row in ready:
        folder = Path(row["folder"])
        if not folder.is_relative_to(job.folder / "runs"):
            raise Stop("source_folder_binding")
        result = json.loads(read(folder / "RESULT.json"))
        receipt_bytes = read(folder / "RECEIPT.json")
        receipt = json.loads(receipt_bytes)
        complete = result.get("native") == 0 and receipt.get("status") == "complete"
        target_proof = any(
            target.get("status") == "ready"
            and target.get("source_verified") is True
            and all(
                row.get(field) == target.get(field)
                for field in ("date", "stream", "wire", "view", "wire_sha256", "view_sha256")
            )
            for target in receipt.get("targets", [])
        )
        stopped = result.get("native") == 2 and receipt.get("status") == "stopped" and target_proof
        if (
            type(result["native"]) is not int
            or row["native"] != result["native"]
            or not (complete or stopped)
            or result["receipt_sha256"] != digest(receipt_bytes)
            or row["receipt_sha256"] != digest(receipt_bytes)
            or receipt["intent_sha256"] != digest(read(folder / "INTENT.json"))
        ):
            raise Stop("native_source_unverified")
        for field in ("wire", "view"):
            if digest(read(folder / row[field], 5 * 1024 * 1024)) != row[field + "_sha256"]:
                raise Stop("source_bytes_changed")
    close_owned = None
    primary = False
    try:
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
            closed = False

            def close_owned():
                # Close before terminal proof; never retry a failed close.
                nonlocal closed
                if not closed:
                    closed = True
                    client.close()

            store = R2Store(client=client)
        return _publish_store(
            job, plan_sha, state, ready, store=store,
            execution_context=execution_context, finalize_dates=finalize_dates,
            defer_references=defer_references, close_owned=close_owned,
            source_free_proof=source_free_proof,
        )
    except BaseException:
        primary = True
        raise
    finally:
        if close_owned is not None:
            _cleanup(close_owned, primary=primary)


def _publish_store(
    job, plan_sha, state, ready, *, store, execution_context,
    finalize_dates, defer_references, close_owned, source_free_proof=None,
):
    selected_count = len(ready)
    target_sha = digest((store.client.meta.endpoint_url + "\0" + store.bucket).encode())
    if source_free_proof is not None:
        proof, _ = source_free_binding(job, plan_sha, *source_free_proof)
        if target_sha != proof["storage_target_sha256"]:
            raise Stop("storage_target_changed")
    target_file = job.folder / "storage-target.json"
    if target_file.exists() and json.loads(read(target_file)) != {"target_sha256": target_sha}:
        raise Stop("storage_target_changed")
    if execution_context is None:
        execution_context = {}
    rolling_plan = execution_context.get("rolling_plan")
    if rolling_plan is not None:
        from .rolling_reference_cli import load_plan

        binding, verified_cache, planned_context, _ = load_plan(
            job, plan_sha, rolling_plan.get("reference_plan_sha256"), store=store,
            reference_round_sha=execution_context.get("reference_round_sha256"),
        )
        if rolling_plan != binding:
            raise Stop("rolling_plan_binding_changed")
        cache = execution_context.setdefault("snapshot_cache", {})
        for name, snapshot in verified_cache.items():
            cache.setdefault(name, snapshot)
        for kind in ("get", "list", "put"):
            counts = execution_context.setdefault("counts", {"get": 0, "list": 0, "put": 0})
            counts[kind] = max(counts[kind], planned_context["counts"][kind])
        execution_context["body_bytes"] = max(
            execution_context.get("body_bytes", 0), planned_context["body_bytes"],
        )
    context_tip = _completed_context(job, state, target_sha, execution_context, finalize_dates)
    continuation_sha = execution_context.get("continuation_sha256")
    continuation = _continuation(job, continuation_sha) if continuation_sha else None
    if continuation is not None:
        if continuation["target_sha256"] != target_sha:
            raise Stop("continuation_binding_unverified")
        finalize_dates = continuation["finalize_dates"]
        execution_context["publication_get_limit"] = 6000
        execution_context["independent_gets"] = continuation["independent_gets"]
        for kind in ("get", "list", "put"):
            execution_context["counts"][kind] = max(
                execution_context["counts"][kind], continuation["prior_usage"]["counts"][kind]
            )
        execution_context["body_bytes"] = max(
            execution_context["body_bytes"], continuation["prior_usage"]["body_bytes"]
        )
    phases = execution_context.get("phases", 0)
    shared_phases = execution_context.get("span_account_usage", {}).get("source_pool", {}).get("publication_phases", 0)
    if type(phases) is not int or not 0 <= phases or phases + shared_phases >= 14:
        raise Stop("storage_execution_phase_budget")
    execution_context["phases"] = phases + 1
    run = uuid.uuid4().hex
    folder = job.folder / "runs" / run
    owner = {"run": run, "plan": plan_sha, "phase": "publish"}
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
    guard = StorageGuard(store, keys, folder, execution_context)
    guard.prefixes = {f"health/hrv/{m[:4]}/{m[5:]}/" for m in months}
    staged, done, changed_months = [], [], set()
    snapshot_files = {}
    snapshot_cache = guard.execution_context.setdefault("snapshot_cache", {})
    reference_store = _ReferenceStore(store, guard)
    primary = False
    owner_created = begin_attempted = False
    try:
        (job.folder / "runs").mkdir(exist_ok=True)
        folder.mkdir()
        if not target_file.exists():
            write(target_file, {"target_sha256": target_sha})
        write(job.folder / "OWNER.lock", owner)
        owner_created = True
        current = job.admission(plan_sha, source=False, owner=owner)
        if current["rows"] != state["rows"]:
            raise Stop("source_progress_changed_before_owner")
        if job.events()[1] != context_tip:
            raise Stop("publication_progress_changed_before_owner")
        if source_free_proof is not None:
            source_free_binding(job, plan_sha, *source_free_proof, owner=owner)
        begin_attempted = True
        job.append({"type": "begin", "run": run})
        from .night_baseline_refresh import (
            ComponentBudgetDeferred,
            _held,
            affected_periods,
            is_closed,
            publish_source,
            refresh_pending,
        )

        # Finalize only AFTER this phase's canonical source/index ACKs and readbacks.
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
                    snapshot_cache[object_key] = before
                    snapshot_files[object_key] = "before-" + row["stream"] + "-" + row["date"] + ".bin"
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
            snapshot_cache[object_key] = after
            snapshot_files[object_key] = f"source-postimage-{len(done):03d}.bin"
            write(folder / snapshot_files[object_key], after.data)
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
        reference_store.draining = True
        reference_store.index_reserve = reference_store.index_bytes = 0
        if rolling_plan is not None and rolling_plan["verified_sleep_sources"]:
            from .night_baseline_refresh import register_rolling_endpoints

            register_rolling_endpoints(reference_store, rolling_plan["verified_sleep_sources"],
                                       today=rolling_plan["today"])
        period_names = None
        if finalize_dates is not None:
            period_names = set()
            delivered = {(row["stream"], row["date"]) for row, *_ in done}
            for row in state["rows"]:
                if row["date"] in finalize_dates and (
                    row["status"] == "published" or (row["stream"], row["date"]) in delivered
                ):
                    period_names.update(affected_periods(
                        "signals" if row["stream"] == "hr" else "hrv", row["date"]
                    ))
            for row in state["rows"]:
                if (
                    row["date"] in finalize_dates
                    and row["status"] in {"ready", "pending"}
                    and (row["stream"], row["date"]) not in delivered
                ):
                    period_names.difference_update(affected_periods(
                        "signals" if row["stream"] == "hr" else "hrv", row["date"]
                    ))
        if continuation is not None:
            period_names = set(continuation["period_names"])
        raw_destinations = [
            {
                "date": r["date"], "stream": r["stream"], "key": k, "revision": rev, "sha256": sha,
                "snapshot": {
                    "file": snapshot_files[k], "bytes": len(snapshot_cache[k].data),
                    "content_type": snapshot_cache[k].content_type, "encoding": snapshot_cache[k].encoding,
                },
            }
            for r, k, rev, sha in done
        ]
        # A later baseline denial/unknown must not erase known raw delivery.
        # This receipt does not release the owner or declare phase completion.
        write(folder / "RAW-DESTINATIONS-VERIFIED.json", {
            "status": "raw_verified_baselines_pending",
            "destinations": raw_destinations,
            "counts": dict(guard.counts),
            "body_bytes": guard.body_bytes,
        })
        baseline_report = refresh_pending(
            reference_store, snapshot_cache=snapshot_cache,
            today=rolling_plan["today"] if rolling_plan else None,
            timezone=rolling_plan["timezone"] if rolling_plan else None,
            period_names=set() if defer_references or rolling_plan is not None else period_names,
            rolling_endpoints=rolling_plan["endpoints"] if rolling_plan is not None else None,
            rolling_sources=[] if defer_references else (
                [("signals" if row["stream"] == "hr" else "hrv", row["date"])
                 for row in state["rows"] if row["date"] in finalize_dates]
                if finalize_dates is not None else None
            ),
        )
        if continuation is not None:
            rows = reference_store.queue_rows
            pending_names = sorted(set(rows or {}) & set(continuation["period_names"]))
            baseline_report.update(
                continuation_periods=continuation["period_names"], continuation_pending=pending_names,
                continuation_complete=bool(rows is not None and not pending_names),
            )
        if defer_references:
            baseline_report["reference_work_deferred"] = True
        if rolling_plan is not None:
            from . import rolling_night_references as rolling

            current_queue = reference_store.queue_snapshot
            if current_queue is None:
                raise Stop("rolling_queue_unverified")
            remaining = rolling.status(current_queue, today=rolling_plan["today"])
            count = 0
            for channels in remaining.values():
                for item in channels.values():
                    item["ranges"] = rolling.merge([
                        [day, day] for day in rolling_plan["endpoints"]
                        if any(first <= day <= last for first, last in item["ranges"])
                    ])
                    item["count"] = sum((date.fromisoformat(b) - date.fromisoformat(a)).days + 1
                                        for a, b in item["ranges"])
                    count += item["count"]
            baseline_report.update(
                rolling_plan_sha256=rolling_plan["reference_plan_sha256"],
                rolling_plan_remaining=remaining,
                rolling_plan_complete=count == 0,
            )
        if finalize_dates is not None:
            baseline_report["finalize_dates"] = sorted(set(finalize_dates))
            cohort_names = set()
            raw_pending = False
            for row in state["rows"]:
                if row["date"] in finalize_dates:
                    cohort_names.update(affected_periods(
                        "signals" if row["stream"] == "hr" else "hrv", row["date"]
                    ))
                    raw_pending |= row["status"] in {"ready", "pending"} and (
                        row["stream"], row["date"]
                    ) not in delivered
            rows = reference_store.queue_rows
            closure_day = (continuation["closure_day"] if continuation is not None
                           else baseline_report.get("closure_day"))
            known = rows is not None and type(baseline_report.get("pending")) is int
            pending = {name: row for name, row in (rows or {}).items() if name in cohort_names}
            known = known and (not pending or closure_day is not None)
            closed = sum(
                is_closed((row["start_date"], row["end_date"]), closure_day)
                for row in pending.values()
            ) if known and pending else 0 if known else None
            held = sum(_held(row) for row in pending.values()) if known else None
            blocked = [name for name, row in pending.items() if row.get("blocked")] if known else None
            legacy_closed = closed
            if known:
                from . import rolling_discovery
                from . import rolling_night_references as rolling

                sources = [("signals" if row["stream"] == "hr" else "hrv", row["date"])
                           for row in state["rows"] if row["date"] in finalize_dates]
                queue = reference_store.queue_snapshot
                remaining = rolling.status(queue, today=closure_day, sources=sources)
                discovery = rolling_discovery.discovery_status(
                    queue, today=closure_day, sources=sources,
                )
                rolling_count = sum(item["count"] for channels in remaining.values()
                                    for item in channels.values())
                closed += rolling_count + discovery["count"]
                for name, row in rows.items():
                    if not row.get("rolling_window") or not any(
                        first <= row["end_date"] <= last
                        for first, last in remaining[str(row["rolling_window"])][row["channel"]]["ranges"]
                    ):
                        continue
                    held += bool(_held(row))
                    if row.get("blocked"):
                        blocked.append(name)
                # Unknown context writes remain holds only for dependent windows.
                held += sum(any(
                    first <= rolling.source_range("sleep", binding["wake_date"], int(n))[1]
                    and last >= binding["wake_date"]
                    for n, channels in remaining.items() for item in channels.values()
                    for first, last in item["ranges"]
                ) for binding in queue.get("rolling_context_intents", {}).values())
                baseline_report.update(cohort_rolling_remaining=remaining,
                                       cohort_discovery_remaining=discovery)
            baseline_report.update(
                cohort_closed_pending=closed,
                cohort_deferred_open=len(pending) - legacy_closed if known else None,
                cohort_held_periods=held,
                cohort_blocked=blocked,
                cohort_complete=bool(known and not raw_pending and closed == 0 and not held and not blocked),
            )
            if continuation is not None:
                baseline_report.update(
                    original_cohort_complete=baseline_report["cohort_complete"],
                    original_cohort_closure_day=closure_day,
                )
        evidence = {
            "status": "remote_verified_cleanup_pending",
            "run": run, "plan_sha256": plan_sha, "target_sha256": target_sha,
            "baseline_refresh": {
                **baseline_report,
                "publication_counts": dict(guard.counts),
                "publication_body_bytes": guard.body_bytes,
                "execution_totals": {
                    "counts": dict(guard.execution_context["counts"]),
                    "body_bytes": guard.execution_context["body_bytes"],
                    "publication_get_limit": guard.execution_context["publication_get_limit"],
                    "phases": guard.execution_context["phases"],
                    "round_baseline": guard.execution_context["round_baseline"],
                    **({"round": guard.execution_context["round"]} if "round" in guard.execution_context else {}),
                    "independent_gets": guard.execution_context.get("independent_gets", 0),
                    **({"source_free_proof_sha256": guard.execution_context["source_free_proof_sha256"]}
                       if "source_free_proof_sha256" in guard.execution_context else {}),
                    **({"continuation_sha256": continuation_sha} if continuation_sha else {}),
                    **({"rolling_plan_sha256": rolling_plan["reference_plan_sha256"]}
                       if rolling_plan is not None else {}),
                    **({"reference_round_sha256": execution_context["reference_round_sha256"]}
                       if execution_context.get("reference_round_sha256") else {}),
                    **({"reference_successor_sha256": execution_context["reference_successor_sha256"]}
                       if execution_context.get("reference_successor_sha256") else {}),
                    **({"span_account_usage": execution_context["span_account_usage"]}
                       if execution_context.get("span_account_usage") is not None else {}),
                },
                "sources_queued": queued,
                "source_targets_deferred": selected_count - len(done),
            },
            "source_deferred": source_deferred,
            "source_calls": 0,
            "counts": guard.counts,
            "body_bytes": guard.body_bytes,
            "acknowledged_writes": len(guard.acks),
            "destinations": raw_destinations,
        }
        # Genuine remote acknowledgements/readbacks remain durable even when
        # mandatory outer cleanup subsequently fails. This is not completion.
        write(folder / "REMOTE-VERIFIED.json", evidence)
    except BaseException:
        primary = True
        if owner_created and not begin_attempted:
            def release_not_started():
                if (
                    json.loads(read(job.folder / "OWNER.lock")) == owner
                    and job.snapshot()["active_run"] is None
                ):
                    (job.folder / "OWNER.lock").unlink()

            _cleanup(release_not_started, primary=True)
        raise
    finally:
        _cleanup(guard.close, *([close_owned] if close_owned is not None else []), primary=primary)
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
    job.append(
        {
            "type": "end",
            "run": run,
            "status": "published",
            "timings": {
                "source_wait_seconds": None,
                "network_seconds": None,
                "local_compute_seconds": max(
                    0.0, time.monotonic() - guard.started - guard.network_seconds
                ),
                "r2_seconds": guard.network_seconds,
                "control_seconds": None,
            },
        }
    )
    (job.folder / "OWNER.lock").unlink()
    return job.status()
