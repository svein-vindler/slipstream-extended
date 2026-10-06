"""Read-only, scoped backup sources. No credential discovery or Garmin imports."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class BackupError(ValueError):
    """Public errors contain only fixed codes, never provider messages or keys."""


class SourceChanged(BackupError):
    pass


MIB = 1024**2


@dataclass(frozen=True)
class Limits:
    objects: int = 1000
    activities: int = 100
    stored_object: int = MIB
    stored_total: int = 8 * MIB
    decoded_object: int = 2 * MIB
    decoded_total: int = 16 * MIB
    document: int = 12 * MIB
    archive: int = 17 * MIB
    operations: int = 10000
    json_nodes: int = 100000

    def __post_init__(self):
        # Callers may lower limits, never silently raise the delivery's budgets.
        for name, maximum in type(self).__dataclass_fields__.items():
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum.default:
                raise BackupError("invalid_limits")


DEFAULT_LIMITS = Limits()


@dataclass(frozen=True, order=True)
class ObjectVersion:
    key: str
    revision: str
    size: int


@dataclass(frozen=True)
class Scope:
    activities: tuple[tuple[str, str], ...]

    def validate(self, limits: Limits):
        if not isinstance(self.activities, tuple) or len(self.activities) > limits.activities:
            raise BackupError("invalid_scope")
        seen = set()
        ids = set()
        for item in self.activities:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not all(isinstance(part, str) for part in item)
                or not re.fullmatch(r"[12][0-9]{3}", item[0])
                or not re.fullmatch(r"[0-9]{1,20}", item[1])
                or item in seen
                or item[1] in ids
            ):
                raise BackupError("invalid_scope")
            seen.add(item)
            ids.add(item[1])

    def prefixes(self) -> tuple[str, ...]:
        return (
            "coach/profiles/v1/",
            *(
                prefix
                for year, identifier in self.activities
                for prefix in (
                    f"activities/{year}/{identifier}/context/v1/",
                    f"activities/{year}/{identifier}/coach-input/v1/canonical/",
                )
            ),
        )


_IDENTIFIER = r"[A-Za-z0-9_-]{1,80}"
_PROFILE = re.compile(rf"coach/profiles/v1/(\d{{4}}-\d{{2}}-\d{{2}})/({_IDENTIFIER})\.json")
_CONTEXT = re.compile(
    rf"activities/([12]\d{{3}})/(\d{{1,20}})/context/v1/"
    rf"(\d{{8}}T\d{{9}})-({_IDENTIFIER})\.json"
)
_ANALYSIS = re.compile(
    r"activities/([12]\d{3})/(\d{1,20})/coach-input/v1/canonical/"
    r"([a-f0-9]{24})\.json"
)


def key_info(key: str) -> tuple[str, tuple[str, ...]]:
    if not isinstance(key, str) or len(key) > 240:
        raise BackupError("invalid_object_name")
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
    if any(part.upper().split(".")[0] in reserved for part in key.split("/")):
        raise BackupError("invalid_object_name")
    for kind, pattern in (("profile", _PROFILE), ("context", _CONTEXT), ("analysis", _ANALYSIS)):
        match = pattern.fullmatch(key)
        if match:
            return kind, match.groups()
    # Full-match grammars exclude traversal, drives, backslashes, ADS and pointers.
    raise BackupError("object_type_not_allowed")


def allowed_key(key: str, scope: Scope) -> str:
    kind, parts = key_info(key)
    if kind != "profile" and (parts[0], parts[1]) not in scope.activities:
        raise BackupError("object_outside_scope")
    return kind


def safe_path(path: Path):
    # Reject symlinks and Windows junctions in every existing path component.
    absolute = path.absolute()
    for part in (absolute, *absolute.parents):
        if part.is_symlink() or (hasattr(part, "is_junction") and part.is_junction()):
            raise BackupError("linked_path_not_allowed")
    return absolute


def safe_file(path: Path) -> Path:
    path = safe_path(path)
    if not stat.S_ISREG(path.stat().st_mode):
        raise BackupError("regular_file_required")
    return path


class ReadOnlySource(Protocol):
    def inventory(self, scope: Scope) -> Iterator[ObjectVersion]: ...
    def read(self, version: ObjectVersion, maximum: int) -> bytes: ...


class LocalSource:
    """Explicit local object tree for synthetic fixtures or an approved offline copy."""

    def __init__(self, root: Path):
        self.root = safe_path(root)
        self.operations = {"get": 0, "list_pages": 0, "listed_objects": 0, "put": 0}
        self.read_bytes = 0
        self._scope = None
        if not self.root.is_dir():
            raise BackupError("source_directory_required")

    def inventory(self, scope: Scope) -> Iterator[ObjectVersion]:
        scope.validate(DEFAULT_LIMITS)
        self._scope = scope
        pending = []
        for prefix in scope.prefixes():
            directory = safe_path(self.root / prefix)
            if not directory.exists():
                continue
            if not directory.is_dir():
                raise BackupError("source_directory_required")
            pending.append(directory)
        directories = 0
        while pending:
            directory = pending.pop()
            directories += 1
            # Stream directory entries; os.walk would first load every filename.
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        path = safe_path(Path(entry.path))
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(path)
                            if directories + len(pending) > 4 * DEFAULT_LIMITS.objects:
                                raise BackupError("source_directory_limit")
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            raise BackupError("regular_file_required")
                        key = path.relative_to(self.root).as_posix()
                        allowed_key(key, scope)
                        size = path.stat().st_size
                        if size > DEFAULT_LIMITS.stored_object:
                            raise BackupError("stored_object_limit")
                        self.operations["listed_objects"] += 1
                        data = self._read(path, DEFAULT_LIMITS.stored_object)
                        yield ObjectVersion(key, hashlib.sha256(data).hexdigest(), len(data))
            except FileNotFoundError:
                raise SourceChanged("source_changed") from None
            except OSError:
                raise BackupError("source_read_failed") from None

    def _read(self, path: Path, maximum: int) -> bytes:
        path = safe_file(path)
        if self.operations["get"] >= DEFAULT_LIMITS.operations:
            raise BackupError("operation_limit")
        self.operations["get"] += 1
        with path.open("rb") as stream:
            data = stream.read(maximum + 1)
        self.read_bytes += len(data)
        if len(data) > maximum:
            raise BackupError("stored_object_limit")
        return data

    def read(self, version: ObjectVersion, maximum: int) -> bytes:
        if self._scope is None:
            raise BackupError("source_inventory_required")
        if type(maximum) is not int or not 1 <= maximum <= DEFAULT_LIMITS.stored_object:
            raise BackupError("stored_object_limit")
        allowed_key(version.key, self._scope)
        try:
            data = self._read(safe_path(self.root / version.key), maximum)
        except FileNotFoundError:
            raise SourceChanged("source_changed") from None
        if len(data) != version.size or hashlib.sha256(data).hexdigest() != version.revision:
            raise SourceChanged("source_changed")
        return data


class R2ReadOnlySource:
    """Inject an approved read-only S3 client; no live CLI switch or environment lookup.

    Only bounded prefix LIST and conditional GET are exposed. The client must
    use read-only credentials, finite timeouts and at most one SDK retry.
    """

    def __init__(self, client, bucket: str, limits: Limits = DEFAULT_LIMITS):
        self._client = client
        self._bucket = bucket
        self.limits = limits
        self.operations = {"get": 0, "list_pages": 0, "listed_objects": 0, "put": 0}
        self.read_bytes = 0
        self._scope = None
        config = getattr(getattr(client, "meta", None), "config", None)
        if config is not None:
            attempts = (config.retries or {}).get("total_max_attempts")
            if (
                type(attempts) is not int
                or not 1 <= attempts <= 2
                or not 0 < config.connect_timeout <= 30
                or not 0 < config.read_timeout <= 60
            ):
                raise BackupError("bounded_client_configuration_required")

    def _operation(self, kind: str):
        if self.operations["get"] + self.operations["list_pages"] >= self.limits.operations:
            raise BackupError("operation_limit")
        self.operations[kind] += 1

    def inventory(self, scope: Scope) -> Iterator[ObjectVersion]:
        scope.validate(self.limits)
        self._scope = scope
        for prefix in scope.prefixes():
            token = None
            seen_tokens = set()
            while True:
                args = {"Bucket": self._bucket, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    args["ContinuationToken"] = token
                self._operation("list_pages")
                try:
                    page = self._client.list_objects_v2(**args)
                except Exception:
                    raise BackupError("source_read_failed") from None
                for item in page.get("Contents", []):
                    self.operations["listed_objects"] += 1
                    if self.operations["listed_objects"] > 4 * self.limits.objects:
                        raise BackupError("listed_object_limit")
                    key = item.get("Key")
                    allowed_key(key, scope)
                    if not key.startswith(prefix):
                        raise BackupError("object_outside_scope")
                    revision = item.get("ETag")
                    if not isinstance(revision, str) or not revision or len(revision) > 256:
                        raise BackupError("source_revision_required")
                    yield ObjectVersion(key, revision, item.get("Size"))
                if not page.get("IsTruncated"):
                    break
                token = page.get("NextContinuationToken")
                if not isinstance(token, str) or not token or token in seen_tokens:
                    raise BackupError("invalid_source_pagination")
                seen_tokens.add(token)

    def read(self, version: ObjectVersion, maximum: int) -> bytes:
        if self._scope is None:
            raise BackupError("source_inventory_required")
        if type(maximum) is not int or not 1 <= maximum <= self.limits.stored_object:
            raise BackupError("stored_object_limit")
        allowed_key(version.key, self._scope)
        self._operation("get")
        body = None
        try:
            response = self._client.get_object(
                Bucket=self._bucket, Key=version.key, IfMatch=version.revision
            )
            body = response["Body"]
            if response.get("ETag") != version.revision:
                raise SourceChanged("source_changed")
            if response.get("ContentLength") != version.size:
                raise SourceChanged("source_changed")
            data = body.read(maximum + 1)
            self.read_bytes += len(data)
            if len(data) > maximum:
                raise BackupError("stored_object_limit")
            if len(data) != version.size:
                raise SourceChanged("source_changed")
            return data
        except BackupError:
            raise
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound", "412", "PreconditionFailed"}:
                raise SourceChanged("source_changed") from None
            raise BackupError("source_read_failed") from None
        finally:
            if body is not None:
                try:
                    body.close()
                except Exception:
                    raise BackupError("source_close_failed") from None
