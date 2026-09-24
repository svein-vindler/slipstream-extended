"""Small, budget-limited S3-compatible client for the private R2 bucket."""

from __future__ import annotations

import os
import sys

GIB = 1024**3
MIB = 1024**2
DEFAULT_MAX_BUCKET_BYTES = 5 * GIB
DEFAULT_MAX_BUCKET_OBJECTS = 100_000
DEFAULT_MAX_WRITES_PER_RUN = 250
DEFAULT_MAX_WRITE_BYTES_PER_RUN = 512 * MIB


class R2BudgetError(RuntimeError):
    """Raised before a write would exceed a configured free-tier safety limit."""


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
        self._initial_inventory: dict[str, int] | None = None
        self._writes = 0
        self._write_bytes = 0

    def inventory(self) -> dict[str, int]:
        objects = 0
        total_bytes = 0
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket):
            for item in page.get("Contents", []):
                objects += 1
                total_bytes += int(item.get("Size", 0))
        return {"objects": objects, "bytes": total_bytes}

    def list_keys(self, prefix: str = "") -> set[str]:
        keys: set[str] = set()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item.get("Key")
                if isinstance(key, str):
                    keys.add(key)
        return keys

    def list_object_revisions(self, prefix: str = "") -> dict[str, str]:
        """List object keys with stable revisions without downloading bodies."""
        revisions: dict[str, str] = {}
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
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
            self._initial_inventory = self.inventory()
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
        if self._initial_inventory["objects"] + self._writes + 1 > self.max_bucket_objects:
            raise R2BudgetError(
                f"R2 object safety limit would be exceeded ({self.max_bucket_objects})"
            )
        projected_bytes = self._initial_inventory["bytes"] + self._write_bytes + len(data)
        if projected_bytes > self.max_bucket_bytes:
            raise R2BudgetError(
                f"R2 storage safety limit would be exceeded ({self.max_bucket_bytes} bytes)"
            )

    def get(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def put(self, key: str, data: bytes, content_type: str, *, encoding: str | None = None):
        self._check_write_budget(data)
        args = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": data,
            "ContentType": content_type,
        }
        if encoding:
            args["ContentEncoding"] = encoding
        self.client.put_object(**args)
        self._writes += 1
        self._write_bytes += len(data)
