"""One summary publication/restore contract; fixed keys, immutable paired data."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from botocore.exceptions import ClientError

from .granular import gzip_bytes, json_bytes, sha256
from .r2_store import R2ObjectSnapshot, missing_object
from .summary_restore import decode_summary

MANIFEST_KEY = "summary/manifest.json"
FILES = (("activities.csv", "Activity ID,"), ("health_daily.csv", "Date,"))
MAX_MANIFEST_BYTES = 64 * 1024


class SummaryUnverified(ValueError):
    """Fixed guidance; never embeds keys, source bytes or SDK exceptions."""
    def __init__(self):
        super().__init__("Stored summary publication cannot be verified")


class SummaryConflict(RuntimeError):
    def __init__(self):
        super().__init__("Summary publication base changed; start a new normal run")


@dataclass(frozen=True, repr=False)
class RestoredBaseProof:
    storage_scope: tuple[int, str]
    flow_id: str
    local_root: str
    manifest_snapshot: R2ObjectSnapshot
    manifest_sha256: str
    dataset_hashes: tuple[str, str]
    decoded_pair: tuple[bytes, bytes]
    stored_pair: tuple[bytes, bytes]
    selection: str
    snapshot_day: str
    generated_at_literal: str


@dataclass(frozen=True, repr=False)
class VerifiedEmptyDerivedBaseProof:
    storage_scope: tuple[int, str]
    flow_id: str
    local_root: str
    seed_hashes: tuple[str | None, str | None]


@dataclass(frozen=True, repr=False)
class FullExportIntent:
    storage_scope: tuple[int, str]
    flow_id: str
    local_root: str
    decoded_pair: tuple[bytes, bytes]


@dataclass(frozen=True, repr=False)
class FullExportTarget:
    storage_scope: tuple[int, str]
    flow_id: str
    local_root: str
    state: str
    manifest_snapshot: R2ObjectSnapshot | None
    manifest: dict | None
    decoded_pair: tuple[bytes, bytes] | None
    stored_pair: tuple[bytes, bytes] | None


@dataclass(frozen=True, repr=False)
class RestoredSummaries:
    report: dict
    base: RestoredBaseProof


def _scope(store):
    try:
        return id(store.client), store.bucket
    except AttributeError:
        raise SummaryUnverified() from None


def _flow(value):
    if not isinstance(value, str) or not value or len(value) > 128:
        raise SummaryUnverified()
    return value


def _root(data_dir):
    return str(Path(data_dir).resolve())


def _optional(store, key, *, max_bytes=None):
    try:
        return store.get_snapshot(key, max_bytes=max_bytes)
    except ClientError as exc:
        if not missing_object(exc):
            raise
        return None


def _local_pair(data_dir):
    result = {}
    for name, header in FILES:
        raw = (Path(data_dir) / name).read_bytes()
        try:
            result[name] = decode_summary(raw, header)
        except ValueError:
            raise SummaryUnverified() from None
    return result


def _generation(pair, day):
    seed = {"schema_version": 2, "snapshot_date": day,
            "files": [{"name": name, "sha256": sha256(pair[name])} for name, _ in FILES]}
    return sha256(json_bytes(seed))


def _build(pair, created, *, snapshot_day=None, stored_pair=None):
    if created.tzinfo is None:
        raise SummaryUnverified()
    created = created.astimezone(timezone.utc)
    day = snapshot_day if snapshot_day is not None else created.date().isoformat()
    try:
        if date.fromisoformat(day).isoformat() != day:
            raise SummaryUnverified()
    except (TypeError, ValueError):
        raise SummaryUnverified() from None
    generation = _generation(pair, day)
    prefix = f"summary/generations/v2/{day.replace('-', '/')}/{generation}"
    items, files = [], []
    for name, _ in FILES:
        raw, key = pair[name], f"{prefix}/{name}"
        encoded = stored_pair[name] if stored_pair is not None else gzip_bytes(raw)
        items.append((key, encoded, "text/csv; charset=utf-8", "gzip" if encoded.startswith(b"\x1f\x8b") else None))
        files.append({"name": name, "current_key": f"summary/{name}", "snapshot_key": key,
                      "sha256": sha256(raw), "rows": max(len(raw.splitlines()) - 1, 0),
                      "uncompressed_bytes": len(raw), "compressed_bytes": len(encoded)})
    manifest = {"schema_version": 1, "publication_version": 2, "generation_id": generation,
                "snapshot_date": day, "generated_at": created.isoformat(), "files": files}
    archive = {"schema_version": 2, "generation_id": generation, "snapshot_date": day, "files": files}
    items.append((f"{prefix}/manifest.json", json_bytes(archive), "application/json", None))
    return items, manifest


def _seal(store, items):
    for key, data, kind, encoding in items:
        previous = _optional(store, key)
        if previous is None:
            receipt = store.create_with_receipt(key, data, kind, encoding=encoding)
            if receipt.accepted:
                if receipt.revision is None:
                    raise SummaryUnverified()
                continue
            previous = store.get_snapshot(key)
        if previous.data != data:
            raise SummaryUnverified()


def _parse_manifest(snapshot):
    try:
        manifest = json.loads(snapshot.data, object_pairs_hook=_unique_fields)
        if (not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int
                or manifest["schema_version"] != 1
                or ("publication_version" in manifest and
                    (type(manifest["publication_version"]) is not int or manifest["publication_version"] != 2))
                or not isinstance(manifest.get("files"), list) or len(manifest["files"]) != 2):
            raise SummaryUnverified()
        day = manifest["snapshot_date"]
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise SummaryUnverified()
        stamp = datetime.fromisoformat(manifest["generated_at"])
        if stamp.tzinfo is None or snapshot.revision is None:
            raise SummaryUnverified()
        by_name = {item["name"]: item for item in manifest["files"]}
        if set(by_name) != {name for name, _ in FILES}:
            raise SummaryUnverified()
        seed = {"schema_version": 2, "snapshot_date": day,
                "files": [{"name": name, "sha256": by_name[name]["sha256"]} for name, _ in FILES]}
        generation = sha256(json_bytes(seed))
        marked = manifest.get("publication_version") == 2
        if marked and manifest.get("generation_id") != generation:
            raise SummaryUnverified()
        for name, _ in FILES:
            item = by_name[name]
            if (not isinstance(item["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
                    or item["current_key"] != f"summary/{name}"
                    or item["snapshot_key"] != (f"summary/generations/v2/{day.replace('-', '/')}/{generation}/{name}"
                                                if marked else f"summary/snapshots/{day.replace('-', '/')}/{name}")
                    or any(type(item.get(field)) is not int or item[field] < 0
                           for field in ("rows", "uncompressed_bytes", "compressed_bytes"))):
                raise SummaryUnverified()
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        raise SummaryUnverified() from None
    return manifest


class _PairMismatch(Exception):
    pass


def _read_marked(store, snapshot, *, field="snapshot_key"):
    manifest = _parse_manifest(snapshot)
    raw_pair, stored_pair = {}, {}
    by_name = {item["name"]: item for item in manifest["files"]}
    for name, header in FILES:
        item = by_name[name]
        stored = _optional(store, item[field])
        if stored is None:
            raise _PairMismatch()
        try:
            raw = decode_summary(stored.data, header)
        except ValueError:
            raise _PairMismatch() from None
        if (sha256(raw) != item["sha256"] or len(raw) != item["uncompressed_bytes"]
                or len(stored.data) != item["compressed_bytes"]
                or max(len(raw.splitlines()) - 1, 0) != item["rows"]):
            raise _PairMismatch()
        raw_pair[name], stored_pair[name] = raw, stored.data
    return manifest, raw_pair, stored_pair


def _read_selected(store, snapshot, *, with_selection=False):
    manifest = _parse_manifest(snapshot)
    if manifest.get("publication_version") == 2:
        try:
            result = _read_marked(store, snapshot)
            selection = "generation"
        except _PairMismatch:
            raise SummaryUnverified() from None
    else:
        try:
            result = _read_marked(store, snapshot, field="current_key")
            selection = "legacy_current"
        except _PairMismatch:
            try:
                result = _read_marked(store, snapshot)
                selection = "legacy_snapshot"
            except _PairMismatch:
                raise SummaryUnverified() from None
    return (*result, selection) if with_selection else result


def _compatibility_copy(store, key, data):
    encoding = "gzip" if data.startswith(b"\x1f\x8b") else None
    kind = "text/csv; charset=utf-8"
    existing = _optional(store, key)
    if (existing is not None and existing.data == data and existing.content_type == kind
            and (existing.encoding or None) == encoding):
        return False
    if existing is None:
        receipt = store.create_with_receipt(key, data, kind, encoding=encoding)
    else:
        if existing.revision is None:
            raise SummaryUnverified()
        receipt = store.put_if_revision(key, data, kind, revision=existing.revision, encoding=encoding)
    if not receipt.accepted:
        raise SummaryConflict()
    if receipt.revision is None:
        raise SummaryUnverified()
    return True


def _assert_same_parent(store, expected):
    current = _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES)
    if current is None or current.revision != expected.revision or current.data != expected.data:
        raise SummaryConflict()


def _promote_legacy(store, snapshot, manifest, pair, stored_pair):
    if manifest.get("publication_version") == 2:
        return snapshot, manifest
    _assert_same_parent(store, snapshot)
    items, promoted = _build(pair, datetime.fromisoformat(manifest["generated_at"]),
                             snapshot_day=manifest["snapshot_date"], stored_pair=stored_pair)
    promoted["generated_at"] = manifest["generated_at"]
    _seal(store, items)
    prefix = items[-1][0].removesuffix("/manifest.json")
    origin = f"{prefix}/origins/{sha256(snapshot.data)}.json"
    _seal(store, [(origin, snapshot.data, "application/json", None)])
    raw = json_bytes(promoted)
    receipt = store.put_if_revision(MANIFEST_KEY, raw, "application/json", revision=snapshot.revision)
    if not receipt.accepted:
        raise SummaryConflict()
    if receipt.revision is None:
        raise SummaryUnverified()
    return R2ObjectSnapshot(raw, receipt.revision, "application/json", None), promoted


def _publish_existing(pair, *, store, previous, manifest, old_pair, stored_pair, created):
    previous, manifest = _promote_legacy(store, previous, manifest, old_pair, stored_pair)
    _assert_same_parent(store, previous)
    if created.astimezone(timezone.utc).date().isoformat() == manifest["snapshot_date"] and pair == old_pair:
        for name, _ in FILES:
            _compatibility_copy(store, f"summary/{name}", stored_pair[name])
        _assert_same_parent(store, previous)
        return manifest
    items, changed = _seal_generation(store, pair, created)
    for item, metadata in zip(items[:2], changed["files"], strict=True):
        _compatibility_copy(store, metadata["current_key"], item[1])
    receipt = store.put_if_revision(MANIFEST_KEY, json_bytes(changed), "application/json",
                                    revision=previous.revision)
    if not receipt.accepted:
        raise SummaryConflict()
    if receipt.revision is None:
        raise SummaryUnverified()
    return changed


def export_full(data_dir, *, store, generated_at=None):
    """Explicit full local pair; normal producers use a separate proof interface."""
    pair = _local_pair(data_dir)
    scope, flow_id, local_root = _scope(store), uuid4().hex, _root(data_dir)
    intent = FullExportIntent(scope, flow_id, local_root, tuple(pair[name] for name, _ in FILES))
    previous = _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES)
    if previous is not None:
        manifest, old_pair, stored_pair = _read_selected(store, previous)
        target = FullExportTarget(scope, flow_id, local_root, "existing", previous, manifest,
                                  tuple(old_pair[name] for name, _ in FILES),
                                  tuple(stored_pair[name] for name, _ in FILES))
    else:
        state = _verify_absence(store, matching_pair=pair)
        target = FullExportTarget(scope, flow_id, local_root, state, None, None, None, None)
    return _publish_full_intent(intent, target, store=store, data_dir=data_dir,
                                created=generated_at or datetime.now(timezone.utc))


def _publish_full_intent(intent, target, *, store, data_dir, created):
    if (not isinstance(intent, FullExportIntent) or not isinstance(target, FullExportTarget)
            or intent.storage_scope != target.storage_scope or target.storage_scope != _scope(store)
            or intent.flow_id != target.flow_id or intent.local_root != target.local_root
            or target.local_root != _root(data_dir)):
        raise SummaryUnverified()
    pair = dict(zip((name for name, _ in FILES), intent.decoded_pair, strict=True))
    if target.state == "existing" and target.manifest_snapshot is not None:
        return _publish_existing(pair, store=store, previous=target.manifest_snapshot,
                                 manifest=target.manifest,
                                 old_pair=dict(zip((name for name, _ in FILES), target.decoded_pair, strict=True)),
                                 stored_pair=dict(zip((name for name, _ in FILES), target.stored_pair, strict=True)),
                                 created=created)
    if target.state not in {"verified_empty", "verified_matching_stage"} or target.manifest_snapshot is not None:
        raise SummaryUnverified()
    return _publish_initial(pair, store=store, created=created)


def _verify_absence(store, *, matching_pair=None):
    if _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES) is not None:
        raise SummaryConflict()
    for name, _ in FILES:
        if _optional(store, f"summary/{name}") is not None:
            raise SummaryUnverified()
    listing = store.list_prefix_bounded("summary/", max_keys=7)
    if listing:
        if matching_pair is None:
            raise SummaryUnverified()
        _verify_matching_stage(store, listing, matching_pair)
        return "verified_matching_stage"
    return "verified_empty"


def _publish_initial(pair, *, store, created):
    items, manifest = _build(pair, created)
    _seal(store, items)
    receipt = store.create_with_receipt(MANIFEST_KEY, json_bytes(manifest), "application/json")
    if not receipt.accepted:
        raise SummaryConflict()
    if receipt.revision is None:
        raise SummaryUnverified()
    for item, metadata in zip(items[:2], manifest["files"], strict=True):
        _, data, kind, encoding = item
        key = metadata["current_key"]
        receipt = store.create_with_receipt(key, data, kind, encoding=encoding)
        if not receipt.accepted:
            existing = store.get_snapshot(key)
            if existing.data != data or existing.content_type != kind or existing.encoding != encoding:
                raise SummaryConflict()
        elif receipt.revision is None:
            raise SummaryUnverified()
    return manifest


def restore_with_base(data_dir, store, *, flow_id):
    """Proof exists only after the entire verified local pair is materialized."""
    flow_id = _flow(flow_id)
    snapshot = _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES)
    if snapshot is None:
        raise SummaryUnverified()
    manifest, pair, stored, selection = _read_selected(store, snapshot, with_selection=True)
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    staged = []
    try:
        for name, _ in FILES:
            temporary = root / f".{name}.{uuid4().hex}.tmp"
            temporary.write_bytes(pair[name])
            staged.append((temporary, root / name))
        for temporary, destination in staged:
            temporary.replace(destination)
        if _local_pair(data_dir) != pair:
            raise SummaryUnverified()
    finally:
        for temporary, _ in staged:
            temporary.unlink(missing_ok=True)
    report = {"generated_at": manifest["generated_at"],
              "files": [{"name": name, "rows": max(len(pair[name].splitlines()) - 1, 0)} for name, _ in FILES]}
    proof = _proof(data_dir, store, flow_id, snapshot, manifest, pair, stored, selection)
    return RestoredSummaries(report, proof)


def restore_report(data_dir, store):
    return restore_with_base(data_dir, store, flow_id=uuid4().hex).report


def publish_derived(data_dir, *, store, base: RestoredBaseProof | VerifiedEmptyDerivedBaseProof,
                    flow_id: str, generated_at=None):
    """A normal update cannot reread/rebind a newer target as its input base."""
    if isinstance(base, VerifiedEmptyDerivedBaseProof):
        _validate_empty(data_dir, store, base, flow_id)
        _verify_absence(store)
        return _publish_initial(_local_pair(data_dir), store=store,
                                created=generated_at or datetime.now(timezone.utc))
    _validate_proof(data_dir, store, base, flow_id)
    _assert_same_parent(store, base.manifest_snapshot)
    manifest, old_pair, stored_pair = _read_selected(store, base.manifest_snapshot)
    if (tuple(old_pair[name] for name, _ in FILES) != base.decoded_pair
            or tuple(stored_pair[name] for name, _ in FILES) != base.stored_pair):
        raise SummaryUnverified()
    return _publish_existing(_local_pair(data_dir), store=store, previous=base.manifest_snapshot,
                             manifest=manifest, old_pair=old_pair, stored_pair=stored_pair,
                             created=generated_at or datetime.now(timezone.utc))


def prepare_normal_base(data_dir, *, store, flow_id, base=None, allow_direct_initial=False):
    """Acquire once before source fetching; never adopt divergent local data."""
    flow_id = _flow(flow_id)
    if isinstance(base, VerifiedEmptyDerivedBaseProof):
        if not allow_direct_initial:
            raise SummaryUnverified()
        _validate_empty(data_dir, store, base, flow_id)
        if _seed_hashes(data_dir) != base.seed_hashes:
            raise SummaryUnverified()
        _verify_absence(store)
        return base
    if base is not None:
        _validate_proof(data_dir, store, base, flow_id)
        if tuple(_local_pair(data_dir)[name] for name, _ in FILES) != base.decoded_pair:
            raise SummaryUnverified()
        _assert_same_parent(store, base.manifest_snapshot)
        return base
    if allow_direct_initial and _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES) is None:
        _verify_absence(store)
        return VerifiedEmptyDerivedBaseProof(_scope(store), flow_id, _root(data_dir), _seed_hashes(data_dir))
    if any(not (Path(data_dir) / name).is_file() for name, _ in FILES):
        return restore_with_base(data_dir, store, flow_id=flow_id).base
    snapshot = _optional(store, MANIFEST_KEY, max_bytes=MAX_MANIFEST_BYTES)
    if snapshot is None:
        raise SummaryUnverified()
    manifest, pair, stored, selection = _read_selected(store, snapshot, with_selection=True)
    if _local_pair(data_dir) != pair:
        raise SummaryUnverified()
    return _proof(data_dir, store, flow_id, snapshot, manifest, pair, stored, selection)


def _proof(data_dir, store, flow_id, snapshot, manifest, pair, stored, selection):
    return RestoredBaseProof(_scope(store), flow_id, _root(data_dir), snapshot, sha256(snapshot.data),
                             tuple(sha256(pair[name]) for name, _ in FILES),
                             tuple(pair[name] for name, _ in FILES), tuple(stored[name] for name, _ in FILES),
                             selection, manifest["snapshot_date"], manifest["generated_at"])


def _validate_proof(data_dir, store, base, flow_id):
    if (not isinstance(base, RestoredBaseProof) or base.storage_scope != _scope(store)
            or base.flow_id != _flow(flow_id) or base.local_root != _root(data_dir)
            or base.manifest_sha256 != sha256(base.manifest_snapshot.data)
            or base.dataset_hashes != tuple(sha256(raw) for raw in base.decoded_pair)):
        raise SummaryUnverified()


def _seed_hashes(data_dir):
    hashes = []
    for name, header in FILES:
        path = Path(data_dir) / name
        if not path.is_file():
            hashes.append(None)
            continue
        try:
            hashes.append(sha256(decode_summary(path.read_bytes(), header)))
        except ValueError:
            raise SummaryUnverified() from None
    return tuple(hashes)


def _validate_empty(data_dir, store, base, flow_id):
    if (base.storage_scope != _scope(store) or base.flow_id != _flow(flow_id)
            or base.local_root != _root(data_dir)):
        raise SummaryUnverified()


def _verify_matching_stage(store, listing, pair):
    """Only bounded, incoming-pair-matching initial archives may resume."""
    if len(listing) != len(set(listing)) or len(listing) > 6:
        raise SummaryUnverified()
    groups = set()
    for key in listing:
        match = re.fullmatch(r"summary/generations/v2/(\d{4}/\d{2}/\d{2})/([0-9a-f]{64})/(activities.csv|health_daily.csv|manifest.json)", key)
        if match is None:
            raise SummaryUnverified()
        day = match[1].replace("/", "-")
        try:
            created = datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=timezone.utc)
        except ValueError:
            raise SummaryUnverified() from None
        items, manifest = _build(pair, created)
        if match[2] != manifest["generation_id"]:
            raise SummaryUnverified()
        groups.add((day, match[2]))
        expected = {item[0]: item[1] for item in items}
        stored = _optional(store, key, max_bytes=MAX_MANIFEST_BYTES if key.endswith("/manifest.json") else None)
        if stored is None or stored.data != expected[key]:
            raise SummaryUnverified()
    if len(groups) > 2:
        raise SummaryUnverified()


def _unique_fields(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise SummaryUnverified()
        result[name] = value
    return result


def _seal_generation(store, pair, created):
    """Reuse a prior generation's verified encoding; never replace its bytes."""
    items, _ = _build(pair, created)
    stored_pair = {}
    for (name, header), (key, data, kind, encoding) in zip(FILES, items[:2], strict=True):
        previous = _optional(store, key)
        if previous is None:
            receipt = store.create_with_receipt(key, data, kind, encoding=encoding)
            if receipt.accepted:
                if receipt.revision is None:
                    raise SummaryUnverified()
                stored_pair[name] = data
                continue
            previous = store.get_snapshot(key)
        try:
            decoded = decode_summary(previous.data, header)
        except ValueError:
            raise SummaryUnverified() from None
        if decoded != pair[name]:
            raise SummaryUnverified()
        stored_pair[name] = previous.data
    items, manifest = _build(pair, created, stored_pair=stored_pair)
    _seal(store, items[2:])
    return items, manifest
