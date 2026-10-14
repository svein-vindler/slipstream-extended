"""Small, budget-limited S3-compatible client for the private R2 bucket."""

from __future__ import annotations

import hashlib
import os
import sys
from dataclasses import dataclass

from botocore.exceptions import ClientError

from .measurements import timed

GIB = 1024**3
MIB = 1024**2
DEFAULT_MAX_BUCKET_BYTES = 5 * GIB
DEFAULT_MAX_BUCKET_OBJECTS = 100_000
DEFAULT_MAX_WRITES_PER_RUN = 250
DEFAULT_MAX_WRITE_BYTES_PER_RUN = 512 * MIB


def missing_object(error: ClientError) -> bool:
    """Only a missing object is recoverable; authorization/service errors are not."""
    return str(error.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}


class R2BudgetError(RuntimeError):
    """Raised before a write would exceed a configured free-tier safety limit."""


class R2ReadBoundaryError(ValueError):
    """A bounded object could not be consumed safely; contains no key/body."""

    def __init__(self, status):
        self.status = status
        super().__init__("Bounded storage read rejected")


def _opaque_revision(value) -> str | None:
    if (isinstance(value, str) and 2 < len(value) <= 1024
            and value.startswith('"') and value.endswith('"')
            and all(32 <= ord(char) < 127 for char in value)
            and '"' not in value[1:-1]):
        return value
    return None


@dataclass(frozen=True)
class R2ObjectSnapshot:
    """Stored bytes and opaque revision from one SDK response."""
    data: bytes
    revision: str | None
    content_type: str | None = None
    encoding: str | None = None


@dataclass(frozen=True)
class R2WriteReceipt:
    accepted: bool
    revision: str | None


@dataclass
class _BucketBudget:
    """Conservative accounting owned by one serial refresh, never persisted."""

    inventory: dict[str, int] | None = None
    writes: int = 0
    write_bytes: int = 0


def _positive_limit(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _limit(value: int | None, env_name: str, default: int) -> int:
    resolved = _positive_limit(env_name, default) if value is None else value
    if resolved <= 0:
        raise ValueError(f"{env_name} must be a positive integer")
    return resolved


class R2Store:
    def __init__(
        self,
        *,
        client=None,
        bucket: str | None = None,
        max_bucket_bytes: int | None = None,
        max_bucket_objects: int | None = None,
        max_writes_per_run: int | None = None,
        max_write_bytes_per_run: int | None = None,
    ):
        self.bucket = bucket or os.environ.get("R2_BUCKET", "slipstream-data")
        if client is None:
            account_id = os.environ["CLOUDFLARE_ACCOUNT_ID"]
            access_key = os.environ["R2_ACCESS_KEY_ID"]
            secret_key = os.environ["R2_SECRET_ACCESS_KEY"]

            import boto3

            client = boto3.client(
                "s3",
                endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="auto",
            )
        self.client = client
        self.max_bucket_bytes = _limit(
            max_bucket_bytes, "R2_MAX_BUCKET_BYTES", DEFAULT_MAX_BUCKET_BYTES
        )
        self.max_bucket_objects = _limit(
            max_bucket_objects, "R2_MAX_BUCKET_OBJECTS", DEFAULT_MAX_BUCKET_OBJECTS
        )
        self.max_writes_per_run = _limit(
            max_writes_per_run, "R2_MAX_WRITES_PER_RUN", DEFAULT_MAX_WRITES_PER_RUN
        )
        self.max_write_bytes_per_run = _limit(
            max_write_bytes_per_run,
            "R2_MAX_WRITE_BYTES_PER_RUN",
            DEFAULT_MAX_WRITE_BYTES_PER_RUN,
        )
        self._bucket_budget = _BucketBudget()
        self._writes = 0
        self.last_write_revision: str | None = None
        self._write_bytes = 0
        # Successful LIST pages; GET/HEAD/PUT SDK calls, excluding internal retries.
        # Inventory pages used by write guards are included.
        self.operations = {"get": 0, "head": 0, "list_pages": 0, "listed_objects": 0, "put": 0}
        self.timings_ms = {"get": 0.0, "head": 0.0, "list": 0.0, "put": 0.0, "inventory": 0.0}
        self.bounded_read_bytes = 0  # Application-consumed bytes, including overflow checks.

    @property
    def remaining_write_attempts(self) -> int:
        return max(0, self.max_writes_per_run - self._writes)

    @property
    def write_budget_initialized(self) -> bool:
        """Whether an existing write established the run's guarded inventory."""
        return self._initial_inventory is not None

    @property
    def _initial_inventory(self):
        return self._bucket_budget.inventory

    def new_stage(self) -> R2Store:
        """Fresh stage limits/counters, sharing this serial run's bucket budget.

        Only use while other pipeline writers are idle/serialized. A new run
        must construct a new root store, so it inventories current storage.
        Overwrites and uncertain failed PUTs remain charged in full.
        """
        store = R2Store(
            client=self.client, bucket=self.bucket,
            max_bucket_bytes=self.max_bucket_bytes,
            max_bucket_objects=self.max_bucket_objects,
            max_writes_per_run=self.max_writes_per_run,
            max_write_bytes_per_run=self.max_write_bytes_per_run,
        )
        store._bucket_budget = self._bucket_budget
        return store

    def _pages(self, **kwargs):
        with timed(self.timings_ms, "list"):
            paginator = self.client.get_paginator("list_objects_v2")
            pages = iter(paginator.paginate(Bucket=self.bucket, **kwargs))
        while True:
            try:
                with timed(self.timings_ms, "list"):
                    page = next(pages)
            except StopIteration:
                return
            yield page

    def inventory(self) -> dict[str, int]:
        objects = 0
        total_bytes = 0
        # Full inventory overlaps LIST timing and includes metadata processing.
        with timed(self.timings_ms, "inventory"):
            for page in self._pages():
                self.operations["list_pages"] += 1
                self.operations["listed_objects"] += len(page.get("Contents", []))
                for item in page.get("Contents", []):
                    objects += 1
                    total_bytes += int(item.get("Size", 0))
        return {"objects": objects, "bytes": total_bytes}

    def list_keys(self, prefix: str = "") -> set[str]:
        keys: set[str] = set()
        for page in self._pages(Prefix=prefix):
            self.operations["list_pages"] += 1
            self.operations["listed_objects"] += len(page.get("Contents", []))
            for item in page.get("Contents", []):
                key = item.get("Key")
                if isinstance(key, str):
                    keys.add(key)
        return keys

    def list_object_revisions(self, prefix: str = "") -> dict[str, str]:
        """List object keys with stable revisions without downloading bodies."""
        revisions: dict[str, str] = {}
        for page in self._pages(Prefix=prefix):
            self.operations["list_pages"] += 1
            self.operations["listed_objects"] += len(page.get("Contents", []))
            for item in page.get("Contents", []):
                key = item.get("Key")
                if not isinstance(key, str):
                    continue
                etag = item.get("ETag")
                if isinstance(etag, str) and etag:
                    revisions[key] = etag.strip('"')
                    continue
                modified = item.get("LastModified")
                revisions[key] = f"{modified!s}:{int(item.get('Size', 0))}"
        return revisions

    def _check_write_budget(self, data: bytes):
        if self._initial_inventory is None:
            self._bucket_budget.inventory = self.inventory()
            print(
                "[r2-budget] "
                f"objects={self._initial_inventory['objects']}/{self.max_bucket_objects} "
                f"bytes={self._initial_inventory['bytes']}/{self.max_bucket_bytes}",
                file=sys.stderr,
            )

        if self._writes + 1 > self.max_writes_per_run:
            raise R2BudgetError(
                f"R2 write limit reached ({self.max_writes_per_run} per run)"
            )
        if self._write_bytes + len(data) > self.max_write_bytes_per_run:
            raise R2BudgetError(
                "R2 per-run byte limit would be exceeded "
                f"({self.max_write_bytes_per_run} bytes)"
            )
        if self._initial_inventory["objects"] + self._bucket_budget.writes + 1 > self.max_bucket_objects:
            raise R2BudgetError(
                f"R2 object safety limit would be exceeded ({self.max_bucket_objects})"
            )
        projected_bytes = self._initial_inventory["bytes"] + self._bucket_budget.write_bytes + len(data)
        if projected_bytes > self.max_bucket_bytes:
            raise R2BudgetError(
                f"R2 storage safety limit would be exceeded ({self.max_bucket_bytes} bytes)"
            )

    def get(self, key: str) -> bytes:
        """Read legacy bytes unchanged; close the body without masking read failures."""
        self.operations["get"] += 1
        with timed(self.timings_ms, "get"):
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"]
            try:
                data = body.read()
            except BaseException:
                try:
                    body.close()
                except BaseException:
                    pass  # Preserve the original read failure, including cancellation.
                raise
            else:
                body.close()
                return data

    def get_snapshot(self, key: str, *, max_bytes: int | None = None) -> R2ObjectSnapshot:
        """Capture bytes/ETag together; bounds are optional for existing CSV sizes."""
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0):
            raise ValueError("Invalid storage read limit")
        self.operations["get"] += 1
        with timed(self.timings_ms, "get"):
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"]
            try:
                length = response.get("ContentLength")
                if length is not None and (type(length) is not int or length < 0):
                    raise R2ReadBoundaryError("schema_unverified")
                if max_bytes is not None and length is not None and length > max_bytes:
                    raise R2ReadBoundaryError("oversize")
                data = body.read() if max_bytes is None else body.read(max_bytes + 1)
                if not isinstance(data, bytes):
                    raise R2ReadBoundaryError("schema_unverified")
                if max_bytes is not None and len(data) > max_bytes:
                    raise R2ReadBoundaryError("oversize")
                if length is not None and len(data) != length:
                    raise R2ReadBoundaryError("schema_unverified")
                return R2ObjectSnapshot(data, _opaque_revision(response.get("ETag")),
                                        response.get("ContentType"), response.get("ContentEncoding"))
            finally:
                body.close()

    def get_bounded(self, key: str, max_bytes: int) -> bytes:
        """One counted GET; identity bytes only, limit before parsing, always close.

        Additive reader: existing unbounded consumers are unchanged. Underlying
        SDK retries are outside these application-operation counters.
        """
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("Invalid storage read limit")
        self.operations["get"] += 1
        with timed(self.timings_ms, "get"):
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"]
            try:
                length = response.get("ContentLength")
                if length is not None and (type(length) is not int or length < 0):
                    raise R2ReadBoundaryError("schema_unverified")
                if length is not None and length > max_bytes:
                    raise R2ReadBoundaryError("oversize")
                if response.get("ContentEncoding") not in {None, "", "identity"}:
                    raise R2ReadBoundaryError("schema_unverified")
                data = bytearray()
                while True:
                    chunk = body.read(min(4096, max_bytes + 1 - len(data)))
                    self.bounded_read_bytes += len(chunk)
                    if not chunk:
                        break
                    data.extend(chunk)
                    if len(data) > max_bytes:
                        raise R2ReadBoundaryError("oversize")
                if length is not None and len(data) != length:
                    raise R2ReadBoundaryError("schema_unverified")
                return bytes(data)
            finally:
                body.close()

    def put_if_changed(self, key: str, data: bytes, content_type: str, *, encoding: str | None = None) -> bool:
        """Skip identical single-PUT objects, including their serving metadata."""
        try:
            self.operations["head"] += 1
            with timed(self.timings_ms, "head"):
                previous = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if not missing_object(exc):
                raise
            previous = {}
        digest = hashlib.md5(data, usedforsecurity=False).hexdigest()
        etag = previous.get("ETag")
        if (isinstance(etag, str) and etag.strip('"') == digest
                and previous.get("ContentLength") == len(data)
                and previous.get("ContentType") == content_type
                and (previous.get("ContentEncoding") or None) == (encoding or None)
                and not previous.get("SSECustomerAlgorithm")
                and not previous.get("ServerSideEncryption")):
            return False
        # Unknown/multipart ETags conservatively write through the existing guards.
        self.put(key, data, content_type, encoding=encoding)
        return True

    def put(self, key: str, data: bytes, content_type: str, *, encoding: str | None = None):
        self._put(key, data, content_type, encoding=encoding)

    def put_if_absent(
        self, key: str, data: bytes, content_type: str, *, encoding: str | None = None,
    ) -> bool:
        """Create only: a concurrent/existing object is never overwritten.

        A rejected attempt remains budgeted, just like uncertain ordinary PUTs.
        Only a failed precondition means an object exists; other failures escape.
        """
        return self._put(key, data, content_type, encoding=encoding, if_absent=True)

    def list_prefix_bounded(self, prefix: str, *, max_keys: int) -> list[str]:
        """One explicitly bounded namespace check, never an inventory fallback."""
        if type(max_keys) is not int or not 1 <= max_keys <= 1000:
            raise ValueError("Invalid storage listing limit")
        self.operations["list_pages"] += 1
        with timed(self.timings_ms, "list"):
            result = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=max_keys)
        entries = result.get("Contents", [])
        if (not isinstance(entries, list) or len(entries) > max_keys
                or result.get("IsTruncated") is not False
                or any(not isinstance(item, dict) or not isinstance(item.get("Key"), str)
                       or not item["Key"].startswith(prefix) for item in entries)):
            raise R2ReadBoundaryError("schema_unverified")
        self.operations["listed_objects"] += len(entries)
        return [item["Key"] for item in entries]

    def create_with_receipt(
        self, key: str, data: bytes, content_type: str, *, encoding: str | None = None,
    ) -> R2WriteReceipt:
        return self._put(key, data, content_type, encoding=encoding,
                         if_absent=True, return_receipt=True)

    def put_if_revision(
        self, key: str, data: bytes, content_type: str, *, revision: str,
        encoding: str | None = None,
    ) -> R2WriteReceipt:
        """Conditional replace; returns the actual SDK acknowledgement revision."""
        if _opaque_revision(revision) is None:
            raise ValueError("Invalid conditional revision")
        return self._put(key, data, content_type, encoding=encoding,
                         if_match=revision, return_receipt=True)

    def _put(
        self, key: str, data: bytes, content_type: str, *,
        encoding: str | None = None, if_absent: bool = False,
        if_match: str | None = None, return_receipt: bool = False,
    ) -> bool | R2WriteReceipt:
        self._check_write_budget(data)
        # Reserve before transmission: a timeout may follow a committed PUT.
        # Neither a later stage nor a caught retry can reclaim that capacity.
        self._writes += 1
        self._write_bytes += len(data)
        self._bucket_budget.writes += 1
        self._bucket_budget.write_bytes += len(data)
        args = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
        }
        if encoding:
            args["ContentEncoding"] = encoding
        self.operations["put"] += 1
        if if_absent:
            args["IfNoneMatch"] = "*"
        if if_match is not None:
            args["IfMatch"] = if_match
        try:
            with timed(self.timings_ms, "put"):
                self.last_write_revision = None
                response = self.client.put_object(**args)
                etag = response.get("ETag") if isinstance(response, dict) else None
                self.last_write_revision = etag.strip('"') if isinstance(etag, str) and etag else None
        except ClientError as exc:
            if (if_absent or if_match is not None) and str(exc.response.get("Error", {}).get("Code")) in {
                "PreconditionFailed", "412",
            }:
                return R2WriteReceipt(False, None) if return_receipt else False
            raise
        return R2WriteReceipt(True, _opaque_revision(etag)) if return_receipt else True
