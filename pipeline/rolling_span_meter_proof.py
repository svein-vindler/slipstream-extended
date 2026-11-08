"""Read-only replay of the public source-linked independent-read proof codec."""
import json
from pathlib import Path

from .history_state import Stop, digest, encoded


def _need(condition):
    if not condition:
        raise Stop("rolling_span_meter_link_changed")


def _pin(reference, *, prefix=False):
    _need(isinstance(reference, dict) and set(reference) == {"path", "bytes", "sha256"}
          and type(reference["bytes"]) is int and 0 < reference["bytes"] <= 16777216)
    with Path(reference["path"]).open("rb") as handle:
        body = handle.read(reference["bytes"] + (not prefix))
    _need(len(body) == reference["bytes"] and digest(body) == reference["sha256"])
    return body


def _json(reference):
    return json.loads(_pin(reference))


class _Replay:
    def __init__(self):
        self.memo, self.active = {}, set()

    def source_link(self, reference):
        value = _json(reference)
        _need(value.get("kind") == "ACTUAL_SPAN_SOURCE_LINK_V1")
        binding, postcheck = _json(value["source_binding_ref"]), _json(value["source_postcheck_ref"])
        source_path = Path(value["source_meter_path"]).resolve()
        _need(source_path == Path(binding["job"]).resolve() / "operator-enclosure-20261106/independent-gets.jsonl"
              and Path(value["source_prefix"]["path"]).resolve() == source_path
              and Path(value["source_journal_prefix"]["path"]).resolve() == Path(binding["job"]).resolve() / "events.jsonl")
        journal = value["source_journal_prefix"]
        if journal["bytes"]:
            _pin(journal, prefix=True)
        else:
            _need(journal["sha256"] == digest(b""))
        permission = _json(value["qualification_ref"])
        _need(postcheck.get("componentCleanupConfirmed") is True
              and postcheck.get("bindingSHA256") == digest(encoded(binding))
              and postcheck.get("cumulativeIndependentGETs") == value["source_used"]
              and postcheck.get("meterJournal") == value["source_prefix"]
              and permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT"
              and permission.get("outcome") == "allowed"
              and permission.get("phase") in {"data", "postcheck"}
              and type(permission.get("callerPID")) is int)
        source = self.scan(value["source_prefix"], span=False)
        _need(source["count"] == value["source_used"] and source["baseline"] == value["source_baseline"]
              and source["initial"] == value["source_initial"] and source["round"] == value["source_round"]
              and source["identity"] == value["source_identity"])
        return value, binding

    def round_proof(self, proof, previous=None):
        _need(isinstance(proof, dict) and proof.get("schema") == 3
              and proof.get("kind") == "SOURCE_LINKED_SPAN_READ_ROUND_V1")
        source, binding = self.source_link(proof["source_link_ref"])
        initial_round = (source["source_round"]["round_id"] if source["source_round"] else
            "span-source-" + digest(encoded({"path": source["source_meter_path"],
                "plan": binding["plan"], "target": binding["targetSHA256"]})))
        _need(proof["plan_sha256"] == binding["plan"]["sha256"]
              and proof["target_sha256"] == binding["targetSHA256"]
              and proof["initial_carried"] == source["source_initial"])
        if proof["epoch_ref"] is None:
            _need(proof["round_id"] == initial_round and proof["previous_round_id"] is None)
        else:
            epoch = _json(proof["epoch_ref"])
            parent = _json(epoch["qualified_parent_ref"])
            _need(epoch.get("schema") == 1 and epoch.get("purpose") == "rolling_reference_span_only"
                  and epoch.get("source_carry") is False and epoch.get("independent_pool_transition") is True
                  and epoch["job_plan_sha256"] == proof["plan_sha256"]
                  and epoch["storage_target_sha256"] == proof["target_sha256"]
                  and parent.get("kind") == "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1"
                  and parent.get("schema") == 1 and parent["span_sha256"] == epoch["span_sha256"]
                  and parent.get("ack_ref") is not None
                  and proof["round_id"] == "span-epoch-" + proof["epoch_ref"]["sha256"])
            prior = self.link(parent["source_meter_link"])
            _need(proof["previous_round_id"] == prior["current_round_id"]
                  and (previous is None or proof["previous_round_id"] == previous["round_id"]))
            _pin(epoch["terminal_receipt_ref"])
            if epoch.get("remote_ref") is not None:
                _pin(epoch["remote_ref"])
            ack = _json(parent["ack_ref"])
            _pin(ack["postcheck"])
            native, permission = _json(parent["native_terminal"]), _json(parent["qualification_ref"])
            _need(native.get("allNativeDescendantsTerminal") is True and native.get("actualNative") in (0, 2)
                  and permission.get("kind") == "ACTUAL_CURRENT_PRODUCT_PORT"
                  and permission.get("phase") == "data" and permission.get("outcome") == "allowed")
        return source

    def scan(self, reference, *, span):
        key = (reference["path"], reference["bytes"], reference["sha256"], span)
        if key in self.memo:
            return self.memo[key]
        _need(key not in self.active and len(self.active) < 128 and len(self.memo) < 4096)
        self.active.add(key)
        body = _pin(reference, prefix=True)
        _need(body.endswith(b"\n"))
        state = {"count": 0, "initial": None, "baseline": 0, "round": None, "identity": None,
                 "span_gets": 0, "source_pool_gets": 0, "separate_epoch_gets": 0}
        previous, pending, source_ref, linked, excluded = None, None, None, False, 0
        imports, external_ids, sessions = {}, set(), {}
        for sequence, line in enumerate(body.splitlines(keepends=True)):
            _need(sequence < 16384)
            frame = json.loads(line)
            kind, count, before = frame["kind"], frame["count"], state["count"]
            _need(frame["sequence"] == sequence and frame["previous"] == previous
                  and type(count) is int and count >= 0)
            identity = frame.get("identity")
            if sequence == 0:
                state["identity"] = identity
            elif kind != "release_transition":
                _need(identity == state["identity"])
            if kind == "initial":
                _need(sequence == 0)
                state["initial"] = count
            elif kind == "source_link" and span:
                source, _ = self.source_link(frame["link_ref"])
                snapshot = self.scan(frame["snapshot_ref"], span=False)
                _need(pending is None and count == snapshot["count"] and count >= before
                      and Path(frame["snapshot_ref"]["path"]).resolve() == Path(source["source_meter_path"]).resolve()
                      and source["source_initial"] == state["initial"])
                if linked:
                    _need(frame["proof"] == state["round"] and frame["baseline"] == state["baseline"])
                else:
                    _need(frame["baseline"] == source["source_baseline"])
                self.round_proof(frame["proof"])
                state["round"], state["baseline"] = frame["proof"], frame["baseline"]
                source_ref, linked = frame["link_ref"], True
            elif kind == "round":
                _need(pending is None and count == before and type(frame["baseline"]) is int)
                if span:
                    _need(linked and frame["baseline"] == count and frame["proof"]["epoch_ref"] is not None)
                    self.round_proof(frame["proof"], state["round"])
                else:
                    _need(frame["baseline"] == (0 if frame["proof"].get("initial_round") else count))
                    if state["round"] is not None:
                        _need(frame["proof"]["previous_round_id"] == state["round"]["round_id"])
                    excluded = 0
                state["round"], state["baseline"] = frame["proof"], frame["baseline"]
            elif kind == "attempt":
                _need(pending is None and count == before + 1)
                pending = ("sdk",)
            elif kind == "known":
                _need(pending == ("sdk",) and count == before)
                pending = None
            elif kind == "night_baseline_intent":
                intent = frame["intent"]
                _need(pending is None and state["round"] is not None
                      and intent["round_id"] == state["round"]["round_id"]
                      and type(intent["reserved_gets"]) is int and 1 <= intent["reserved_gets"] <= 2
                      and count == before + intent["reserved_gets"] and intent["intent_id"] not in external_ids)
                external_ids.add(intent["intent_id"])
                pending = ("reader", intent, frame["intent_sha256"])
            elif kind == "night_baseline_ack":
                _need(pending == ("reader", frame["intent"], frame["intent_sha256"]) and count == before
                      and type(frame["reported_gets"]) is int and 0 <= frame["reported_gets"] <= frame["intent"]["reserved_gets"])
                _pin(frame["observation_ref"])
                pending = None
            elif kind == "span_usage" and not span:
                observed = self.scan(frame["span_meter_prefix"], span=True)
                path = str(Path(frame["span_meter_prefix"]["path"]).resolve())
                _need(Path(path) == Path(reference["path"]).resolve().parent / "span-independent-gets.jsonl")
                old = imports.get(path, dict.fromkeys(("span_gets", "source_pool_gets", "separate_epoch_gets"), 0))
                for field, delta in (("span_gets", "delta"), ("source_pool_gets", "source_pool_delta"),
                                     ("separate_epoch_gets", "separate_epoch_delta")):
                    _need(frame[field] == observed[field] and frame[delta] == observed[field] - old[field] >= 0)
                _need(count == before + frame["delta"] and frame["delta"] == frame["source_pool_delta"] + frame["separate_epoch_delta"]
                      and frame["span_identity"] == observed["identity"] and frame["source_link_ref"] == observed["source_ref"])
                linked_source, _ = self.source_link(frame["source_link_ref"])
                _need(linked_source["source_identity"] == identity)
                imports[path], excluded = frame, excluded + frame["separate_epoch_delta"]
            elif kind == "span_session_intent" and not span:
                session = _json(frame["session_ref"])
                claim = _json(session["root_claim_ref"]).get("currentLiveExecutor", {})
                _need(count == before and pending is None and session.get("kind") == "ACTUAL_SPAN_SOURCE_SESSION_INTENT_V1"
                      and claim.get("running") is True and claim.get("terminalObserved") is False
                      and claim.get("bindingSHA256") == digest(encoded(session["binding"]))
                      and claim.get("callerPID") == session["callerPID"] and session["session_id"] not in sessions)
                sessions[session["session_id"]] = frame["session_ref"]
            elif kind == "span_session_closed" and not span:
                closure, session = _json(frame["closure_ref"]), _json(frame["session_ref"])
                prepared = _json(closure["prepared_ref"])
                _need(count == before and pending is None and sessions.get(session["session_id"]) == frame["session_ref"]
                      and closure.get("kind") == "KNOWN_SPAN_SOURCE_SESSION_CLOSED_V1" and closure.get("leaseClosed") is True
                      and closure["session_ref"] == frame["session_ref"] and prepared["session_ref"] == frame["session_ref"])
                for field in ("root_terminal_ref", "restore_ref"):
                    _pin(prepared[field])
                for field in ("native_terminal_refs", "postchecks"):
                    for item in prepared[field]:
                        _pin(item)
                self.scan(prepared["meter_prefix"], span=True)
                del sessions[session["session_id"]]
            elif kind == "release_transition" and not span:
                _need(pending is None and count == before)
                _pin(frame["transition_ref"])
                _pin(frame["admission_ref"])
                state["identity"] = identity
            else:
                raise Stop("rolling_span_meter_link_unknown")
            if span:
                if kind in {"attempt", "night_baseline_intent"}:
                    _need(linked)
                    delta = count - before
                    state["span_gets"] += delta
                    field = "source_pool_gets" if state["round"]["epoch_ref"] is None else "separate_epoch_gets"
                    state[field] += delta
                for field in ("span_gets", "source_pool_gets", "separate_epoch_gets"):
                    _need(frame[field] == state[field])
            _need(0 <= count - state["baseline"] - (0 if span else excluded) <= 256)
            state["count"], previous = count, digest(line)
        _need(pending is None and (not span or linked))
        state["source_ref"] = source_ref
        self.active.remove(key)
        self.memo[key] = state
        return state

    def link(self, reference):
        value = _json(reference)
        _need(value.get("kind") == "ACTUAL_SPAN_METER_LINK_V1")
        state = self.scan(value["meter_prefix"], span=True)
        _need(value["used"] == state["count"] and value["round_baseline"] == state["baseline"]
              and value["initial"] == state["initial"] and value["current_round_id"] == state["round"]["round_id"])
        return value


def validate_source_linked_meter(reference):
    """Validate pinned historical bytes only; current Root permission is separate."""
    return _Replay().link(reference)
