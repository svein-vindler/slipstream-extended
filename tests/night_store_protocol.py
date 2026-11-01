"""Synthetic storage service protocol for older in-memory caller fixtures.

Production uses R2Store. These service-side revisions/CAS operations have no HTTP,
credentials, filesystem or transport retry; native HTTP is tested separately.
"""

import hashlib
from types import SimpleNamespace

from botocore.exceptions import ClientError


class NightStoreProtocol:
    def _night_raw(self, key):
        objects = getattr(self, "objects", None)
        if objects is not None:
            if key not in objects:
                raise KeyError(key)
            return objects[key]
        extra = getattr(self, "_night_extra_objects", {})
        if key in extra:
            return extra[key]
        if key.startswith("health/hrv/"):
            body = getattr(self, "body", None)
            if body is not None:
                return body
        raise KeyError(key)

    def get_snapshot(self, key, *, max_bytes):
        try:
            data = self._night_raw(key)
        except KeyError:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "GetObject",
            ) from None
        if len(data) > max_bytes:
            raise ValueError("Synthetic response bound")
        meta = getattr(self, "_night_metadata", {}).get(
            key, ("application/json", "gzip" if data[:2] == b"\x1f\x8b" else None)
        )
        return SimpleNamespace(
            data=data,
            revision='"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"',
            content_type=meta[0],
            encoding=meta[1],
        )

    @property
    def remaining_write_attempts(self):
        calls = getattr(self, "calls", [])
        known = sum(isinstance(row, tuple) and row[0] == "PUT" for row in calls)
        return max(
            0,
            250
            - max(known, len(getattr(self, "puts", [])), getattr(self, "_night_write_count", 0)),
        )

    def can_write_group(self, *, attempts, byte_count):
        return attempts <= self.remaining_write_attempts and byte_count <= 512 * 1024 * 1024

    def _night_write(self, key, data, kind, encoding):
        objects = getattr(self, "objects", None)
        if objects is not None:
            self.put(key, data, kind, encoding=encoding)
        elif key.startswith("health/hrv/"):
            self.put_if_changed(key, data, kind, encoding=encoding)
        else:
            if not hasattr(self, "_night_extra_objects"):
                self._night_extra_objects = {}
            self._night_extra_objects[key] = data
        if not hasattr(self, "_night_metadata"):
            self._night_metadata = {}
        self._night_metadata[key] = (kind, encoding)
        self._night_write_count = getattr(self, "_night_write_count", 0) + 1
        return SimpleNamespace(
            accepted=True, revision='"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
        )

    def create_with_receipt(self, key, data, kind, *, encoding=None):
        try:
            self._night_raw(key)
        except KeyError:
            return self._night_write(key, data, kind, encoding)
        return SimpleNamespace(accepted=False, revision=None)

    def put_if_revision(self, key, data, kind, *, revision, encoding=None):
        try:
            current = self.get_snapshot(key, max_bytes=4 * 1024 * 1024)
        except ClientError:
            return SimpleNamespace(accepted=False, revision=None)
        if current.revision != revision:
            return SimpleNamespace(accepted=False, revision=None)
        return self._night_write(key, data, kind, encoding)

    def list_prefix_bounded(self, prefix, *, max_keys):
        objects = getattr(self, "objects", None)
        keys = (
            list(objects)
            if objects is not None
            else list(getattr(self, "_night_extra_objects", {}))
        )
        result = sorted(key for key in keys if key.startswith(prefix))
        if len(result) > max_keys:
            raise ValueError("Synthetic namespace bound")
        return result
