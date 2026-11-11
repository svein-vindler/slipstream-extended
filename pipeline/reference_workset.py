"""Finite durable canonical sources and prepared products; no provider or write client."""
from __future__ import annotations

import copy
import json
import os
import re
from datetime import timedelta
from pathlib import Path

from .history_state import Stop, digest, encoded, read, write
from .native_night import zone
from .night_products import (
    calendar_day,
    canonical,
    normalize_args,
    reference_resources_valid,
    storage_key,
)
from .r2_store import R2ObjectSnapshot, _opaque_revision
from .rolling_night_references import LEGACY_WINDOWS, window_selection

ROOTS = ("health/sleep/v1/", "health/hrv/", "health/signals/v1/")
CONTEXT = "health/night-context/v1/"
TARGET_FIELDS = {"target_id", "key", "args", "endpoint", "window_days", "channel"}


def _require(condition, reason="reference_workset_changed"):
    if not condition:
        raise Stop(reason)


def _ref(path):
    body = read(path)
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def _pin(reference):
    _require(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}
             and type(reference["bytes"]) is int and 0 < reference["bytes"] <= 16777216)
    body = read(reference["path"])
    _require(len(body) == reference["bytes"] and digest(body) == reference["sha256"])
    return body


def _immutable(path, value):
    body = value if isinstance(value, bytes) else encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        _require(read(path) == body)
    else:
        write(path, body)
    return _ref(path)


def _days(first, last):
    return [(first + timedelta(days=index)).isoformat() for index in range((last - first).days + 1)]


def _validate_scope(binding, targets, target_subset):
    _require(type(target_subset) is bool, "reference_workset_targets")
    fields = {"job_plan_sha256", "storage_target_sha256", "today", "timezone", "endpoints"}
    _require(type(binding) is dict and set(binding) in (fields, fields | {"windows"}),
        "reference_workset_binding")
    _require("windows" not in binding or type(binding["windows"]) is list and binding["windows"],
             "reference_workset_binding")
    try:
        windows = window_selection(binding["windows"] if "windows" in binding else LEGACY_WINDOWS)
    except ValueError:
        raise Stop("reference_workset_binding") from None
    _require(all(type(binding[name]) is str and re.fullmatch(r"[a-f0-9]{64}", binding[name])
                 for name in ("job_plan_sha256", "storage_target_sha256")),
             "reference_workset_binding")
    endpoints = binding["endpoints"]
    _require(type(endpoints) is list and 1 <= len(endpoints) <= 406
             and endpoints == sorted(set(endpoints)) and zone(binding["timezone"]) is not None
             and all(calendar_day(day).isoformat() == day and day <= binding["today"] for day in endpoints)
             and calendar_day(binding["today"]).isoformat() == binding["today"]
             and (calendar_day(endpoints[-1]) - calendar_day(endpoints[0])).days <= 405,
             "reference_workset_scope")
    expected = set()
    target_count = 2 * len(windows) * len(endpoints)
    _require(type(targets) is list and (1 <= len(targets) <= target_count if target_subset else len(targets) == target_count), "reference_workset_targets")
    for target in targets:
        _require(type(target) is dict and set(target) == TARGET_FIELDS
                 and target["endpoint"] in endpoints and type(target["window_days"]) is int
                 and target["window_days"] in windows
                 and target["channel"] in ("heart_rate", "hrv"), "reference_workset_targets")
        args = normalize_args(period="custom", **target["args"])
        width = (calendar_day(args["end_date"]) - calendar_day(args["start_date"])).days + 1
        identity = (target["endpoint"], target["window_days"], target["channel"])
        _require(args == target["args"] and args["end_date"] == target["endpoint"]
                 and width == target["window_days"] and args["channel"] == target["channel"]
                 and target["key"] == storage_key(args) and target["target_id"] == target["key"]
                 and identity not in expected, "reference_workset_targets")
        expected.add(identity)
    _require({target["endpoint"] for target in targets} == set(endpoints), "reference_workset_targets")


class ReferenceWorkset:
    """One immutable declared scope, one append-only local progress journal."""

    @classmethod
    def create(cls, folder, binding, targets, *, target_subset=False):
        _validate_scope(binding, targets, target_subset)
        descriptor = {"schema": 2 if target_subset else 1,
                      "kind": "reference-workset-target-subset" if target_subset else "reference-workset",
                      "binding": binding, "targets": targets}
        folder = Path(folder).resolve()
        folder.mkdir(parents=True, exist_ok=True)
        _immutable(folder / "WORKSET.json", descriptor)
        journal = folder / "PROGRESS.jsonl"
        if not journal.exists():
            write(journal, b"")
        return cls.open(folder, binding)

    @classmethod
    def open(cls, folder, expected_binding):
        instance = cls()
        instance.folder = Path(folder).resolve()
        descriptor_body = read(instance.folder / "WORKSET.json")
        descriptor = json.loads(descriptor_body)
        subset = descriptor.get("schema") == 2 and descriptor.get("kind") == "reference-workset-target-subset"
        _require((subset or descriptor.get("schema") == 1 and descriptor.get("kind") == "reference-workset")
                 and set(descriptor) == {"schema", "kind", "binding", "targets"}
                 and descriptor.get("binding") == expected_binding, "reference_workset_binding")
        _validate_scope(descriptor["binding"], descriptor["targets"], subset)
        instance._binding, instance._targets = descriptor["binding"], descriptor["targets"]
        instance.scope_sha256 = digest(descriptor_body)
        instance._target_map = {target["key"]: target for target in instance._targets}
        instance.wake_dates = set()
        for target in instance._targets:
            args = target["args"]
            instance.wake_dates.update(_days(calendar_day(args["start_date"]), calendar_day(args["end_date"])))
        instance.signal_dates = {(calendar_day(day) + timedelta(days=offset)).isoformat()
                                 for day in instance.wake_dates for offset in range(-4, 3)}
        instance.prefixes = {root + day[:4] + "/" + day[5:7] + "/": 63
            for root in ROOTS for day in (instance.signal_dates if root == ROOTS[2] else instance.wake_dates)}
        instance.prefixes.update({CONTEXT + day[:4] + "/" + day[5:7] + "/": 3101 for day in instance.wake_dates})
        instance._inventories, instance._sources, instance._products = {}, {}, {}
        instance._inventory_refs, instance._rows = {}, {}
        instance._outcomes, instance._steps, instance._plans = {}, [], {}
        instance._source_seal = instance._build_seal = instance._hold = None
        instance._holds = []
        instance._completion_read = None
        instance._sequence, instance._previous, instance._journal_bytes = 0, None, 0
        body = read(instance.folder / "PROGRESS.jsonl")
        _require(not body or body.endswith(b"\n"))
        for line in body.splitlines(keepends=True):
            frame = json.loads(line)
            _require(frame.get("sequence") == instance._sequence and frame.get("previous") == instance._previous)
            instance._apply(frame["operation"], frame["value"])
            instance._previous, instance._sequence = digest(line), instance._sequence + 1
        instance._journal_bytes = len(body)
        return instance

    @property
    def scope_sha(self):
        return self.scope_sha256

    @property
    def binding(self):
        return copy.deepcopy(self._binding)

    @property
    def targets(self):
        return copy.deepcopy(self._targets)

    def _append(self, operation, value):
        path = self.folder / "PROGRESS.jsonl"
        _require(path.stat().st_size == self._journal_bytes, "reference_workset_concurrent_change")
        frame = {"sequence": self._sequence, "previous": self._previous,
                 "operation": operation, "value": value}
        body = encoded(frame)
        with path.open("ab") as handle:
            _require(handle.write(body) == len(body), "reference_workset_short_write")
            handle.flush()
            os.fsync(handle.fileno())
        self._apply(operation, value)
        self._sequence, self._previous = self._sequence + 1, digest(body)
        self._journal_bytes += len(body)

    def _apply(self, operation, value):
        if operation == "inventory":
            inventory = json.loads(_pin(value["reference"]))
            _pin(inventory["proof_ref"])
            self._inventories[value["prefix"]] = inventory
            self._inventory_refs[value["prefix"]] = value["reference"]
            self._rows.update(inventory["rows"])
        elif operation in {"fetch_begin", "source"}:
            if operation == "source":
                body = _pin(value["body_ref"])
                _require(digest(body) == value["sha256"] and len(body) == value["size"])
                if value.get("provenance_ref") is not None:
                    _pin(value["provenance_ref"])
            self._sources[value["key"]] = value
        elif operation == "hold":
            if value.get("proof_ref") is not None:
                _pin(value["proof_ref"])
            self._holds.append(value)
            if self._hold is None:
                self._hold = value
            if value.get("key") in self._sources:
                self._sources[value["key"]]["status"] = "unverifiable"
        elif operation == "source_seal":
            _require(json.loads(_pin(value))["scope_sha256"] == self.scope_sha256)
            self._source_seal = value
        elif operation == "product":
            self._load_artifact(value["reference"], value["key"])
            self._products[value["key"]] = value["reference"]
        elif operation == "outcome":
            _pin(value["proof_ref"])
            self._outcomes[value["key"]] = value
        elif operation == "build_seal":
            _require(json.loads(_pin(value))["scope_sha256"] == self.scope_sha256)
            self._build_seal = value
        elif operation == "resource_plan":
            plan = json.loads(_pin(value["reference"]))
            _require(plan.get("workset_sha256") == self.scope_sha256 and plan.get("phase") == value["phase"])
            self._plans[value["phase"]] = value["reference"]
        elif operation == "step":
            receipt = json.loads(_pin(value))
            _require(receipt.get("scope_sha256") == self.scope_sha256)
            self._steps.append(value)
            if (receipt.get("status") == "hold" or receipt.get("componentCleanupConfirmed") is False
                    or receipt.get("ownedSDKClosed") is False):
                self._apply("hold", {"reason": receipt.get("primaryFailureCategory")
                    or receipt.get("cleanupFailureCategory") or "stage_cleanup_unknown", "proof_ref": value})
        else:
            raise Stop("reference_workset_event_unknown")

    def inventory_requests(self):
        return [{"prefix": prefix, "max_keys": bound} for prefix, bound in sorted(self.prefixes.items())
                if prefix not in self._inventories]

    def record_inventory(self, prefix, rows, proof_ref):
        _require(prefix in self.prefixes and type(rows) is dict and len(rows) <= self.prefixes[prefix],
                 "reference_workset_inventory")
        _pin(proof_ref)
        filtered = {}
        for key, record in rows.items():
            _require(type(key) is str and key.startswith(prefix) and type(record) is dict
                     and set(record) == {"revision", "size"} and _opaque_revision(record["revision"]) is not None
                     and type(record["size"]) is int and record["size"] >= 0, "reference_workset_inventory")
            match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?", key[len(prefix):])
            context = re.fullmatch(r"(\d{4}-\d{2}-\d{2})/\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json", key[len(prefix):])
            relevant = (context and context[1] in self.wake_dates if prefix.startswith(CONTEXT) else
                        match and match[1] in (self.signal_dates if prefix.startswith(ROOTS[2]) else self.wake_dates))
            if relevant:
                filtered[key] = copy.deepcopy(record)
        if prefix in self._inventories:
            if self._inventories[prefix]["rows"] != filtered:
                self._append("hold", {"reason": "source_inventory_changed"})
                raise Stop("reference_workset_source_changed")
            return
        _require(self._source_seal is None, "reference_workset_sources_sealed")
        value = {"prefix": prefix, "rows": filtered, "proof_ref": proof_ref}
        reference = _immutable(self.folder / "inventories" / (digest(prefix.encode()) + ".json"), value)
        self._append("inventory", {"prefix": prefix, "reference": reference})

    def required_sources(self):
        _require(not self.inventory_requests(), "reference_workset_inventory_incomplete")
        rows = self._rows
        selected = {}
        for root in ROOTS:
            for day in sorted(self.signal_dates if root == ROOTS[2] else self.wake_dates):
                key = f"{root}{day[:4]}/{day[5:7]}/{day}.json"
                key = key if key in rows else key + ".gz"
                if key in rows:
                    selected[key] = 262144 if root == ROOTS[2] else 524288
        for day in sorted(self.wake_dates):
            prefix = f"{CONTEXT}{day[:4]}/{day[5:7]}/{day}/"
            keys = sorted(key for key in rows if key.startswith(prefix))
            _require(len(keys) <= 100, "reference_workset_context_bound")
            if keys:
                selected[keys[-1]] = 16384
        result = []
        for key, maximum in sorted(selected.items()):
            record = rows[key]
            _require(record["size"] <= maximum, "reference_workset_source_bound")
            result.append({"key": key, **record, "max_bytes": maximum,
                           "status": self._sources.get(key, {}).get("status", "missing")})
        return result

    def fetch_missing(self, store, *, max_items, admit_fetch):
        _require(type(max_items) is int and max_items >= 0 and callable(admit_fetch))
        _require(self._hold is None and all(value["status"] != "pending" for value in self._sources.values()),
                 "reference_workset_source_unverifiable")
        fetched, deferred = 0, False
        for row in self.required_sources():
            if row["status"] == "present":
                continue
            if fetched >= max_items or not admit_fetch(copy.deepcopy(row)):
                deferred = True
                break
            self._append("fetch_begin", {**row, "status": "pending"})
            try:
                snapshot = store.get_snapshot(row["key"], max_bytes=row["max_bytes"])
                _require(type(snapshot.data) is bytes and len(snapshot.data) == row["size"]
                         and snapshot.revision == row["revision"]
                         and snapshot.content_type in (None, "application/json")
                         and snapshot.encoding in (None, "gzip"), "reference_workset_source_changed")
                body_ref = _immutable(self.folder / "sources" / (digest(snapshot.data) + ".bin"), snapshot.data)
                self._append("source", {**row, "status": "present", "sha256": digest(snapshot.data),
                    "body_ref": body_ref, "content_type": snapshot.content_type, "encoding": snapshot.encoding})
                fetched += 1
            except BaseException:
                self._append("hold", {"reason": "source_get_unverifiable", "key": row["key"]})
                raise
        remaining = sum(row["status"] != "present" for row in self.required_sources())
        return {"fetched": fetched, "remaining": remaining, "deferred": deferred}

    def seed_verified_local(self, snapshots, provenance_ref):
        """Adopt the caller's already verified receipt projection, never loose blobs."""
        _require(self._hold is None and all(row.get("status") != "pending" for row in self._sources.values()),
                 "reference_workset_source_unverifiable")
        provenance = json.loads(_pin(provenance_ref))
        _require(provenance.get("job_plan_sha256") == self._binding["job_plan_sha256"]
                 and provenance.get("storage_target_sha256") == self._binding["storage_target_sha256"],
                 "reference_workset_seed_binding")
        proofs = {(row["key"], row["revision"], row["sha256"])
                  for row in provenance.get("objects", provenance.get("destinations", []))}
        selected = {row["key"]: row for row in self.required_sources()}
        adopted = 0
        for key, snapshot in snapshots.items():
            if key not in selected or selected[key]["status"] == "present":
                continue
            row = selected[key]
            if snapshot.revision != row["revision"] or len(snapshot.data) != row["size"]:
                continue
            _require((key, snapshot.revision, digest(snapshot.data)) in proofs
                     and snapshot.content_type in (None, "application/json")
                     and snapshot.encoding in (None, "gzip"), "reference_workset_seed_unverified")
            body_ref = _immutable(self.folder / "sources" / (digest(snapshot.data) + ".bin"), snapshot.data)
            self._append("source", {**row, "status": "present", "sha256": digest(snapshot.data),
                "body_ref": body_ref, "content_type": snapshot.content_type, "encoding": snapshot.encoding,
                "provenance_ref": provenance_ref})
            adopted += 1
        return adopted

    def seal_sources(self):
        _require(self._hold is None and all(row["status"] == "present" for row in self.required_sources()),
                 "reference_workset_sources_incomplete")
        value = {"schema": 1, "kind": "reference-workset-source-seal", "scope_sha256": self.scope_sha256,
                 "inventories": self._inventory_refs, "sources": self._sources}
        reference = _immutable(self.folder / "SOURCE-SEAL.json", value)
        if self._source_seal is None:
            self._append("source_seal", reference)
        else:
            _require(self._source_seal == reference)
        return reference

    def reader_store(self):
        _require(self._source_seal is not None and self._sealed_read_allowed(), "reference_workset_sources_incomplete")
        _pin(self._source_seal)
        workset = self
        annual = {}
        if ("windows" in self._binding and 365 in self._binding["windows"]
                and any(target["window_days"] == 365 for target in self._targets)):
            # This is a selected-date view of sealed local months. Independent
            # fresh fences enumerate the full physical year namespace later.
            annual = {prefix.rstrip("/").rsplit("/", 1)[0] + "/":
                      3101 if prefix.startswith(CONTEXT) else 800 for prefix in self.prefixes}

        class LocalSources:
            def list_revisions_bounded(self, prefix, *, max_keys):
                if prefix in workset._inventories:
                    rows = workset._inventories[prefix]["rows"]
                else:
                    _require(prefix in annual and max_keys == annual[prefix],
                             "reference_workset_source_outside_scope")
                    rows = {key: value for month, inventory in workset._inventories.items()
                            if month.startswith(prefix) for key, value in inventory["rows"].items()}
                _require(len(rows) <= max_keys, "reference_workset_inventory")
                return copy.deepcopy(rows)

            def list_prefix_bounded(self, prefix, *, max_keys):
                rows = {key for inventory in workset._inventories.values() for key in inventory["rows"]}
                keys = sorted(key for key in rows if key.startswith(prefix))
                _require(len(keys) <= max_keys, "reference_workset_inventory")
                return keys

            def get_snapshot(self, key, *, max_bytes):
                record = workset._sources.get(key)
                _require(record is not None and record["status"] == "present",
                         "reference_workset_source_not_local")
                body = _pin(record["body_ref"])
                _require(len(body) <= max_bytes, "reference_workset_source_bound")
                return R2ObjectSnapshot(body, record["revision"], record["content_type"], record["encoding"])

        return LocalSources()

    def _verify_source_proof(self, proof):
        _require(type(proof) is dict and set(proof) == {"objects", "lists"},
                 "reference_workset_product_proof")
        rows = self._rows
        for key, binding in proof["objects"].items():
            _require(any(key.startswith(prefix) for prefix in self.prefixes), "reference_workset_source_outside_scope")
            if binding.get("status") == "absent":
                _require(key not in rows, "reference_workset_product_proof")
            else:
                current = self._sources.get(key, {})
                _require(binding.get("status") == current.get("status") == "present"
                         and binding.get("revision") == current["revision"]
                         and binding.get("sha256") == current["sha256"]
                         and binding.get("size", current["size"]) == current["size"],
                         "reference_workset_product_proof")
        for prefix, keys in proof["lists"].items():
            expected = sorted(key for key in rows if key.startswith(prefix))
            _require(prefix.startswith(CONTEXT) and keys == expected, "reference_workset_product_proof")

    def _load_artifact(self, reference, key):
        metadata = json.loads(_pin(reference))
        body = _pin(metadata.pop("body_ref")).decode("utf-8")
        artifact = {**metadata, "body": body}
        target = self._target_map.get(key)
        _require(target is not None and artifact.get("key") == key and artifact.get("args") == target["args"]
                 and digest(body.encode()) == artifact.get("sha256"), "reference_workset_product_changed")
        _require(reference_resources_valid(json.loads(body), artifact.get("prior_inputs")),
                 "reference_workset_product_resources")
        self._verify_source_proof(artifact["source_proof"])
        return artifact

    def stage_product(self, target_id, artifact):
        _require(self._source_seal is not None and self._hold is None, "reference_workset_sources_incomplete")
        _require(self._build_seal is None and target_id in self._target_map, "reference_workset_product_target")
        expected = {"schema", "kind", "method_version", "args", "key", "body", "sha256",
                    "source_identity", "source_proof", "source_proof_sha256", "prior_inputs"}
        target = self._target_map[target_id]
        _require(type(artifact) is dict and set(artifact) == expected and artifact["schema"] == 1
                 and artifact["kind"] == "prepared-night-baseline"
                 and artifact["method_version"] == "night-baseline-v1" and artifact["key"] == target_id
                 and artifact["args"] == target["args"] and type(artifact["body"]) is str,
                 "reference_workset_product_target")
        body = artifact["body"].encode("utf-8")
        value = json.loads(body)
        _require(len(body) <= 262144 and digest(body) == artifact["sha256"]
                 and value.get("args") == target["args"]
                 and value.get("source_identity") == artifact["source_identity"]
                 and digest(canonical(artifact["source_proof"]).encode()) == artifact["source_proof_sha256"]
                 and type(artifact["prior_inputs"]) is list
                 and len(artifact["prior_inputs"]) <= target["window_days"], "reference_workset_product_changed")
        _require(reference_resources_valid(value, artifact["prior_inputs"]),
                 "reference_workset_product_resources")
        self._verify_source_proof(artifact["source_proof"])
        body_ref = _immutable(self.folder / "products" / (artifact["sha256"] + ".bin"), body)
        metadata = {key: item for key, item in artifact.items() if key != "body"}
        metadata["body_ref"] = body_ref
        reference = _immutable(self.folder / "artifacts" / (digest(target_id.encode()) + ".json"), metadata)
        if target_id in self._products:
            _require(self._products[target_id] == reference)
        else:
            _require(target_id not in self._outcomes, "reference_workset_target_classified")
            self._append("product", {"key": target_id, "reference": reference})
        return reference

    def classify_target(self, target_id, reason, proof_ref, known=True):
        _require(self._source_seal is not None and target_id in self._target_map
                 and self._build_seal is None and target_id not in self._products,
                 "reference_workset_product_target")
        _require(type(known) is bool and type(reason) is str and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", reason),
                 "reference_workset_outcome")
        _pin(proof_ref)
        if known:
            day = self._target_map[target_id]["endpoint"]
            key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
            rows = {key for inventory in self._inventories.values() for key in inventory["rows"]}
            _require(reason == "sleep_not_stored" and key not in rows and key + ".gz" not in rows,
                     "reference_workset_unknown_not_absent")
        value = {"key": target_id, "reason": reason, "known": known, "proof_ref": proof_ref}
        if target_id in self._outcomes:
            _require(self._outcomes[target_id] == value)
        else:
            self._append("outcome", value)

    def seal_build(self):
        _require(self._source_seal is not None and self._hold is None, "reference_workset_sources_incomplete")
        _require(set(self._products) | set(self._outcomes) == set(self._target_map)
                 and all(value["known"] for value in self._outcomes.values()), "reference_workset_build_incomplete")
        for key, reference in self._products.items():
            self._load_artifact(reference, key)
        value = {"schema": 1, "kind": "reference-workset-build-seal", "scope_sha256": self.scope_sha256,
                 "source_seal": self._source_seal, "products": self._products, "outcomes": self._outcomes}
        reference = _immutable(self.folder / "BUILD-SEAL.json", value)
        if self._build_seal is None:
            self._append("build_seal", reference)
        else:
            _require(self._build_seal == reference)
        return reference

    def assert_sealed(self):
        _require(self._build_seal is not None and self._sealed_read_allowed(), "reference_workset_build_incomplete")
        value = json.loads(_pin(self._build_seal))
        _require(value["scope_sha256"] == self.scope_sha256 and value["source_seal"] == self._source_seal
                 and value["products"] == self._products and value["outcomes"] == self._outcomes)
        _pin(self._source_seal)
        return copy.deepcopy(self._build_seal)

    def qualify_completion(self, reference):
        from .reference_completion import qualify_workset

        self._completion_read = qualify_workset(self, reference)
        self.assert_sealed()

    def _sealed_read_allowed(self):
        if self._hold is None:
            return True
        from .reference_completion import SealedCompletionRead

        return (type(self._completion_read) is SealedCompletionRead
                and self._completion_read.revalidate(self))

    def product_for(self, key):
        self.assert_sealed()
        _require(key in self._products, "reference_workset_product_not_ready")
        return self._load_artifact(self._products[key], key)

    def pending_targets(self):
        return [copy.deepcopy(target) for target in self._targets
                if target["key"] not in self._products and target["key"] not in self._outcomes]

    def build_summary(self):
        from .night_reference_sources import rolling_fence_list_bound

        seal = self.assert_sealed()
        product_bytes = fence_pages = 0
        for key, reference in self._products.items():
            product = self._load_artifact(reference, key)
            product_bytes += len(product["body"].encode())
            fence_pages += 2 * rolling_fence_list_bound(product["source_proof"],
                **({"window_days": 365} if self._target_map[key]["window_days"] == 365 else {}))
        return {"product_count": len(self._products), "product_bytes": product_bytes,
                "fresh_fence_list_pages": fence_pages,
                "known_gap_count": len(self._outcomes), "build_seal": seal}

    def mark_hold(self, category, proof_ref=None):
        _require(type(category) is str and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", category),
                 "reference_workset_hold_category")
        if proof_ref is not None:
            _pin(proof_ref)
        self._append("hold", {"reason": category, "proof_ref": proof_ref})

    def bind_resource_plan(self, phase, reference):
        plan = json.loads(_pin(reference))
        _require(phase in {"inventory", "collection", "publication"}
                 and plan.get("kind") == "finite-reference-bulk-resource-plan"
                 and plan.get("phase") == phase and plan.get("workset_sha256") == self.scope_sha256,
                 "reference_workset_resource_binding")
        if phase in self._plans:
            _require(self._plans[phase] == reference, "reference_workset_resource_changed")
            return copy.deepcopy(reference)
        if phase == "collection":
            _require(not self.inventory_requests(), "reference_workset_inventory_incomplete")
        if phase == "publication":
            _require(plan.get("build_seal") == self.assert_sealed(), "reference_workset_resource_binding")
        self._append("resource_plan", {"phase": phase, "reference": reference})
        return copy.deepcopy(reference)

    def record_step(self, receipt_ref):
        receipt = json.loads(_pin(receipt_ref))
        _require(receipt.get("schema") == 1 and receipt.get("kind") == "reference-bulk-stage-terminal"
                 and receipt.get("scope_sha256") == self.scope_sha256
                 and receipt.get("phase") in {"inventory", "collection", "build", "publication"}
                 and receipt.get("status") in {"complete", "bounded_pending", "hold"}
                 and type(receipt.get("counts")) is dict
                 and set(receipt["counts"]) == {"get", "list", "put"}
                 and all(type(value) is int and value >= 0 for value in receipt["counts"].values())
                 and type(receipt.get("body_bytes")) is int and receipt["body_bytes"] >= 0
                 and type(receipt.get("componentCleanupConfirmed")) is bool
                 and (receipt["phase"] == "build" or type(receipt.get("ownedSDKClosed")) is bool),
                 "reference_workset_step_unverified")
        resource_phase = "collection" if receipt["phase"] == "build" else receipt["phase"]
        self.bind_resource_plan(resource_phase, receipt["resource_plan_ref"])
        if receipt_ref in self._steps:
            return copy.deepcopy(receipt_ref)
        self._append("step", receipt_ref)
        return copy.deepcopy(receipt_ref)

    def inventory_summary(self):
        complete = not self.inventory_requests()
        required = self.required_sources() if complete else []
        return {"inventory_requests": [{"prefix": prefix, "max_keys": bound,
            "status": "present" if prefix in self._inventories else "missing",
            "proof_ref": self._inventories.get(prefix, {}).get("proof_ref")}
            for prefix, bound in sorted(self.prefixes.items())],
            "required_sources": required, "inventory_complete": complete,
            "source_object_bound": len(required), "source_byte_bound": sum(row["size"] for row in required)}

    def status(self):
        summary = self.inventory_summary()
        pending = [key for key, value in self._sources.items() if value["status"] == "pending"]
        return {"scope_sha256": self.scope_sha256, "targets": len(self._targets),
            "sources_complete": self._source_seal is not None, "build_complete": self._build_seal is not None,
            "hold": copy.deepcopy(self._hold) or ({"reason": "source_fetch_pending"} if pending else None),
            "holds": copy.deepcopy(self._holds), "steps": copy.deepcopy(self._steps),
            "resource_plans": copy.deepcopy(self._plans), **summary}
