"""Finite, create-only allowances and one durable account for a bulk workset.

These are local software bounds, not provider quotas or execution permission.
Legacy source/span pools remain separate immutable carried evidence.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .history_state import Stop, digest, encoded

PHASES = ("inventory", "collection", "publication")


def _need(value, category):
    if not value:
        raise Stop(category)


def _integer(value, maximum=10**15):
    return type(value) is int and 0 <= value <= maximum


def _ref(path):
    body = Path(path).read_bytes()
    return {"path": str(Path(path).resolve()), "bytes": len(body), "sha256": digest(body)}


def _pin(reference):
    body = Path(reference["path"]).read_bytes()
    _need(len(body) == reference["bytes"] and digest(body) == reference["sha256"],
          "bulk_resource_plan_changed")
    return json.loads(body)


def _deadline(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _need(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0,
              "bulk_resource_deadline")
        return parsed
    except (ValueError, TypeError, AttributeError):
        raise Stop("bulk_resource_deadline") from None


def make_plan(folder, *, phase, workset_sha256, target_count, inventory_requests,
              source_count, source_bytes, product_count=0, product_bytes=0,
              build_seal=None, carried, queue_bytes=262144, old_product_bytes=None, fence_list_pages=None, _value_only=False):
    """Price a declared stage from finite scope and actual source/build metadata.

    The fixed20% margin covers bounded metadata/control overhead. No stage can
    refill itself; activation preserves every prior charge and the same deadline.
    """
    _need(phase in PHASES and re.fullmatch(r"[a-f0-9]{64}", workset_sha256 or ""),
          "bulk_resource_scope")
    _need(all(_integer(value) for value in (target_count, inventory_requests, source_count,
                source_bytes, product_count, product_bytes, queue_bytes))
          and 1 <= target_count <= 2436 and product_count <= target_count
          and 0 < queue_bytes <= 262144 and isinstance(carried, dict)
          and inventory_requests <= 160 and source_count <= 1986
          and source_bytes <= source_count * 524288
          and product_bytes <= product_count * 262144, "bulk_resource_basis")
    _need(phase == "publication" or product_count == product_bytes == 0,
          "bulk_resource_build_required")
    _need(phase != "publication" or isinstance(build_seal, dict), "bulk_resource_build_required")
    if old_product_bytes is None:
        old_product_bytes = 262144 * product_count
    _need(_integer(old_product_bytes) and old_product_bytes <= 262144 * product_count,
          "bulk_resource_basis")
    batches = (product_count + 7) // 8
    if fence_list_pages is None:
        fence_list_pages = 2 * inventory_requests * product_count
    _need(_integer(fence_list_pages)
          and fence_list_pages <= 2 * inventory_requests * product_count,
          "bulk_resource_basis")
    # Inventory100 pages is the existing100000-object bucket safety ceiling.
    lists = inventory_requests + (100 + fence_list_pages + inventory_requests * product_count
                                  if phase == "publication" else 0)
    gets = source_count if phase != "inventory" else 0
    puts = independent = write_bytes = 0
    if phase == "publication":
        gets += 20 * product_count + 4 * batches
        puts = 16 * product_count + 4 * batches
        independent = 2 * product_count
        write_bytes = 3 * product_bytes + 2 * old_product_bytes + 12 * product_count * queue_bytes + 65536 * batches
    def margin(value):
        return (value * 6 + 4) // 5
    limits = {"publication_get": margin(gets), "class_a": margin(lists + puts),
              "write_bytes": margin(write_bytes), "independent_get": independent,
              "total_get": margin(gets) + independent,
              "stage_count": margin(source_count + inventory_requests + product_count + 16),
              "retained_growth_bytes": margin(product_bytes + old_product_bytes
                  + 2048 * product_count + queue_bytes) if phase == "publication" else 0,
              "retained_object_growth": margin(3 * product_count + 1) if phase == "publication" else 0}
    basis = {"target_count": target_count, "inventory_requests": inventory_requests,
             "source_count": source_count, "source_bytes": source_bytes,
             "product_count": product_count, "product_bytes": product_bytes, "queue_bytes": queue_bytes, "old_product_bytes": old_product_bytes, "fence_list_pages": fence_list_pages}
    value = {"schema": 1, "kind": "finite-reference-bulk-resource-plan", "phase": phase,
             "workset_sha256": workset_sha256, "basis": basis, "limits": limits,
             "build_seal": build_seal, "carried_sha256": digest(encoded(carried)),
             "margin": {"numerator": 6, "denominator": 5},
             "technical_batch_limits": {"get": 400, "list": 100, "put": 100,
                                         "write_bytes": 2097152}}
    if _value_only:
        return value
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    body = encoded(value)
    path = folder / (digest(body) + ".json")
    if path.exists():
        _need(path.read_bytes() == body, "bulk_resource_plan_changed")
    else:
        with path.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
    return _ref(path)


def _source_basis(workset):
    summary = workset.inventory_summary()
    requests = sum((row["max_keys"] + 999) // 1000 for row in summary["inventory_requests"])
    return summary, requests


def inventory_plan(workset, carried):
    summary, requests = _source_basis(workset)
    return make_plan(workset.folder / "resource-plans", phase="inventory",
        workset_sha256=workset.scope_sha256, target_count=len(workset.targets),
        inventory_requests=requests, source_count=summary["source_object_bound"],
        source_bytes=summary["source_byte_bound"], carried=carried)


def collection_plan(workset, inventories=None, *, carried):
    summary, requests = _source_basis(workset)
    _need(summary["inventory_complete"], "bulk_resource_inventory_required")
    required = summary["required_sources"]
    return make_plan(workset.folder / "resource-plans", phase="collection",
        workset_sha256=workset.scope_sha256, target_count=len(workset.targets),
        inventory_requests=requests, source_count=len(required),
        source_bytes=sum(row["size"] for row in required), carried=carried)


def publication_plan(workset, *, carried, queue_bytes=262144, old_product_bytes=None):
    summary, requests = _source_basis(workset)
    built = workset.build_summary()
    return make_plan(workset.folder / "resource-plans", phase="publication",
        workset_sha256=workset.scope_sha256, target_count=len(workset.targets),
        inventory_requests=requests, source_count=len(summary["required_sources"]),
        source_bytes=sum(row["size"] for row in summary["required_sources"]),
        product_count=built["product_count"], product_bytes=built["product_bytes"],
        build_seal=built["build_seal"], carried=carried, queue_bytes=queue_bytes,
        old_product_bytes=old_product_bytes, fence_list_pages=built["fresh_fence_list_pages"])


class BulkAccount:
    """One CAS-checked local ledger; new views never renew time or allowance."""

    @classmethod
    def open(cls, folder, scope_sha, carried, *, deadline_utc=None, completion_ref=None):
        self = cls()
        self.folder = Path(folder)
        self.path = self.folder / "ACCOUNT.json"
        self.scope_sha = scope_sha
        _need(completion_ref is None or self.path.is_file(), "bulk_completion_account_missing")
        self.folder.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            self._body = self.path.read_bytes()
            self.state = json.loads(self._body)
            _need(self.state["scope_sha256"] == scope_sha and self.state["carried"] == carried,
                  "bulk_resource_account_binding")
            _need(deadline_utc is None or deadline_utc == self.state["deadline_utc"],
                  "bulk_resource_deadline_changed")
        else:
            _need(deadline_utc is not None, "bulk_resource_deadline_required")
            _need(_deadline(deadline_utc) > datetime.now(timezone.utc), "bulk_resource_deadline")
            self.state = {"schema": 1, "kind": "reference-bulk-account", "scope_sha256": scope_sha,
                "carried": carried, "deadline_utc": deadline_utc, "active_plan": None,
                "plans": {}, "failed": None, "pending_wire": None,
                "context": {"counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0,
                    "independent_gets": 0, "bulk_stages": 0,
                    "round_baseline": {"counts": {"get": 0, "list": 0, "put": 0}, "body_bytes": 0}}}
            self._body = encoded(self.state)
            with self.path.open("xb") as stream:
                stream.write(self._body)
                stream.flush()
                os.fsync(stream.fileno())
        self.execution_context = self.state["context"]
        self.plan = _pin(self.state["active_plan"]) if self.state["active_plan"] is not None else None
        self.completion_ref = completion_ref
        if completion_ref is not None:
            from .reference_completion import attach_account

            attach_account(self, completion_ref)
        else:
            _need(self.state["failed"] is None and self.state["pending_wire"] is None
                  and "completion" not in self.state, "bulk_resource_account_held")
        self.check_deadline()
        return self

    def check_deadline(self):
        if getattr(self, "completion_ref", None) is not None:
            from .reference_completion import check_account

            check_account(self)
            return
        _need(datetime.now(timezone.utc) < _deadline(self.state["deadline_utc"]),
              "bulk_resource_deadline")
        _need(self.state["failed"] is None, "bulk_resource_account_held")

    def checkpoint(self):
        _need(self.path.read_bytes() == self._body, "bulk_resource_account_changed")
        body = encoded(self.state)
        temporary = self.folder / ("account-" + uuid.uuid4().hex + ".tmp")
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        _need(self.path.read_bytes() == self._body, "bulk_resource_account_changed")
        os.replace(temporary, self.path)
        self._body = body

    def activate(self, reference):
        self.check_deadline()
        value = _pin(reference)
        _need(value.get("kind") == "finite-reference-bulk-resource-plan"
              and value.get("workset_sha256") == self.scope_sha
              and value.get("carried_sha256") == digest(encoded(self.state["carried"])),
              "bulk_resource_plan_binding")
        try:
            expected = make_plan(self.folder, phase=value["phase"], workset_sha256=self.scope_sha,
                carried=self.state["carried"], build_seal=value["build_seal"],
                _value_only=True, **value["basis"])
            _need(encoded(expected) == encoded(value), "bulk_resource_plan_derivation")
        except (KeyError, TypeError, ValueError):
            raise Stop("bulk_resource_plan_derivation") from None
        phase = value["phase"]
        _need(phase in PHASES and (phase not in self.state["plans"]
              or self.state["plans"][phase] == reference), "bulk_resource_plan_changed")
        if self.plan is not None:
            _need(PHASES.index(phase) >= PHASES.index(self.plan["phase"]), "bulk_resource_plan_regression")
        self.state["plans"][phase] = reference
        self.state["active_plan"] = reference
        self.plan = value
        self.execution_context["publication_get_limit"] = value["limits"]["publication_get"]
        self.checkpoint()

    def admit(self, kind, *, byte_count=0):
        self.check_deadline()
        _need(self.plan is not None and kind in {"get", "list", "put"}
              and _integer(byte_count), "bulk_resource_plan_required")
        counts, limits = self.execution_context["counts"], self.plan["limits"]
        _need(counts["get"] + (kind == "get") <= limits["publication_get"]
              and counts["list"] + counts["put"] + (kind in {"list", "put"}) <= limits["class_a"]
              and counts["get"] + (kind == "get") + self.execution_context["independent_gets"] <= limits["total_get"]
              and self.execution_context["body_bytes"] + byte_count <= limits["write_bytes"]
              and (kind != "put" or self.plan["phase"] == "publication"), "bulk_resource_budget_deferred")

    def start_stage(self):
        self.check_deadline()
        _need(self.plan is not None, "bulk_resource_plan_required")
        _need(self.execution_context["bulk_stages"] + 1 <= self.plan["limits"]["stage_count"],
              "bulk_resource_budget_deferred")
        self.execution_context["bulk_stages"] += 1
        self.checkpoint()

    def remaining(self):
        self.check_deadline()
        _need(self.plan is not None, "bulk_resource_plan_required")
        limits, context = self.plan["limits"], self.execution_context
        return {"get": max(0, limits["publication_get"] - context["counts"]["get"]),
                "class_a": max(0, limits["class_a"] - context["counts"]["list"] - context["counts"]["put"]),
                "write_bytes": max(0, limits["write_bytes"] - context["body_bytes"]),
                "independent_get": max(0, limits["independent_get"] - context["independent_gets"])}

    def can_fetch(self, row=None):
        try:
            self.admit("get")
            return True
        except Stop as error:
            if error.category == "bulk_resource_budget_deferred":
                return False
            raise

    def start_wire(self, kind, byte_count=0):
        self.check_deadline()
        _need(self.plan is not None and self.execution_context["counts"]["get"] <= self.plan["limits"]["publication_get"]
              and self.execution_context["counts"]["list"] + self.execution_context["counts"]["put"] <= self.plan["limits"]["class_a"]
              and self.execution_context["body_bytes"] <= self.plan["limits"]["write_bytes"], "bulk_resource_budget_deferred")
        _need(self.state["pending_wire"] is None, "bulk_resource_account_held")
        self.state["pending_wire"] = {"kind": kind, "body_bytes": byte_count}
        # StorageGuard already charged its shared context before this checkpoint.
        self.checkpoint()

    def finish_wire(self):
        _need(self.state["pending_wire"] is not None, "bulk_resource_wire_binding")
        self.state["pending_wire"] = None
        self.checkpoint()

    def charge_independent(self, kind, key_sha256, **_):
        self.check_deadline()
        _need(kind in {"sdk", "reader"} and re.fullmatch(r"[a-f0-9]{64}", key_sha256 or ""),
              "bulk_resource_reader_binding")
        _need(self.plan is not None and self.plan["phase"] == "publication", "bulk_resource_plan_required")
        limits = self.plan["limits"]
        count = self.execution_context["independent_gets"]
        _need(count + 1 <= limits["independent_get"]
              and count + 1 + self.execution_context["counts"]["get"] <= limits["total_get"],
              "bulk_resource_budget_deferred")
        self.execution_context["independent_gets"] += 1
        self.checkpoint()

    def mark_failed(self, category):
        if getattr(self, "completion_ref", None) is not None:
            self.state["completion"]["failed"] = category
            self.checkpoint()
            return
        self.state["failed"] = category if category in {
            "unknown_storage_transport", "storage_denied_or_unknown", "unknown_write", "reader_unknown"
        } else "bulk_storage_unknown"
        self.checkpoint()

    def snapshot(self):
        result = json.loads(encoded(self.state))
        context = result["context"]
        result["usage"] = {**context["counts"], "body_bytes": context["body_bytes"],
                           "independent_gets": context["independent_gets"]}
        result["independent_lifetime_gets"] = self.state["carried"].get("independent_lifetime_gets", 0) + context["independent_gets"]
        return result
