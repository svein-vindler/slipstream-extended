"""Portable source-linked meter codec; external Root proof envelopes are synthetic."""
import json

import pytest

from pipeline.history_state import Stop, digest, encoded
from pipeline.rolling_reference_span import _meter_link


def _ref(path):
    body = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}


def codec_fixture(folder, *, qualified=True):
    folder.mkdir()
    def value(name, body):
        path = folder / name
        path.write_bytes(encoded(body))
        return _ref(path)

    job = folder / "job"
    meters = job / "operator-enclosure-20261106"
    meters.mkdir(parents=True)
    source_path = meters / "independent-gets.jsonl"
    span_path = meters / "span-independent-gets.jsonl"
    events = job / "events.jsonl"
    events.write_bytes(b"")
    plan = value("plan.json", {"syntheticSourcePlan": True})
    binding = value("binding.json", {"job": str(job), "plan": plan, "targetSHA256": "a" * 64})
    permission = value("permission.json", {"kind": "ACTUAL_CURRENT_PRODUCT_PORT",
        "phase": "data", "outcome": "allowed", "callerPID": 123})
    frames = {source_path: [], span_path: []}

    def append(path, kind, count, **fields):
        rows = frames[path]
        body = encoded({"sequence": len(rows), "previous": digest(rows[-1]) if rows else None,
            "identity": "s" * 64 if path == source_path else "t" * 64,
            "invocation": "synthetic", "kind": kind, "count": count, **fields})
        rows.append(body)
        path.write_bytes(b"".join(rows))
        return _ref(path)

    append(source_path, "initial", 0)
    for count in range(1, 174):
        append(source_path, "attempt", count)
        append(source_path, "known", count)

    def source_link(name, used):
        postcheck = value(name + "-postcheck.json", {"componentCleanupConfirmed": True,
            "bindingSHA256": digest(encoded(json.loads(open(binding["path"], encoding="utf-8").read()))),
            "cumulativeIndependentGETs": used, "meterJournal": _ref(source_path)})
        return value(name + "-source-link.json", {"kind": "ACTUAL_SPAN_SOURCE_LINK_V1",
            "source_meter_path": str(source_path), "source_identity": "s" * 64,
            "source_initial": 0, "source_used": used, "source_baseline": 0, "source_round": None,
            "source_prefix": _ref(source_path), "source_postcheck_ref": postcheck,
            "source_binding_ref": binding, "source_journal_prefix": _ref(events),
            "qualification_ref": permission})

    link = source_link("initial", 173)
    proof = {"schema": 3, "kind": "SOURCE_LINKED_SPAN_READ_ROUND_V1",
        "round_id": "span-source-" + digest(encoded({"path": str(source_path), "plan": plan, "target": "a" * 64})),
        "plan_sha256": plan["sha256"], "target_sha256": "a" * 64, "initial_carried": 0,
        "source_link_ref": link, "epoch_ref": None, "previous_round_id": None}
    zero = dict.fromkeys(("span_gets", "source_pool_gets", "separate_epoch_gets"), 0)
    append(span_path, "initial", 0, **zero)
    append(span_path, "source_link", 173, link_ref=link, snapshot_ref=_ref(source_path),
        proof=proof, baseline=0, **zero)
    prior = value("prior-meter.json", {"kind": "ACTUAL_SPAN_METER_LINK_V1", "meter_prefix": _ref(span_path),
        "used": 173, "round_baseline": 0, "initial": 0, "current_round_id": proof["round_id"],
        "next_readback_reserve": 84})
    postcheck = value("postcheck.json", {"kind": "ACTUAL_METERED_SPAN_POSTCHECK_V1", "componentCleanupConfirmed": True})
    ack = value("ack.json", {"postcheck": postcheck})
    terminal = value("terminal.json", {"actualNative": 0, "allNativeDescendantsTerminal": True})
    receipt = value("receipt.json", {"status": "complete", "source_calls": 0})
    parent = value("parent.json", {"schema": 1, "kind": "QUALIFIED_STORED_SPAN_EPOCH_PARENT_V1",
        "span_sha256": "b" * 64, "ack_ref": ack, "source_meter_link": prior,
        "native_terminal": terminal, "qualification_ref": permission})
    epoch = value("epoch.json", {"schema": 1, "purpose": "rolling_reference_span_only", "source_carry": False,
        "independent_pool_transition": qualified, "job_plan_sha256": plan["sha256"],
        "storage_target_sha256": "a" * 64, "span_sha256": "b" * 64, "qualified_parent_ref": parent,
        "terminal_receipt_ref": receipt, "remote_ref": None})
    new_proof = {**proof, "round_id": "span-epoch-" + epoch["sha256"], "epoch_ref": epoch,
                 "previous_round_id": proof["round_id"]}
    append(span_path, "round", 173, proof=new_proof, baseline=173, **zero)
    for number in range(1, 91):
        counters = {"span_gets": number, "source_pool_gets": 0, "separate_epoch_gets": number}
        append(span_path, "attempt", 173 + number, **counters)
        append(span_path, "known", 173 + number, **counters)
    append(source_path, "span_usage", 263, span_identity="t" * 64, span_meter_prefix=_ref(span_path),
        span_gets=90, source_pool_gets=0, separate_epoch_gets=90, delta=90,
        source_pool_delta=0, separate_epoch_delta=90, source_link_ref=link)
    append(source_path, "attempt", 264)
    append(source_path, "known", 264)
    fresh_link = source_link("resumed", 264)
    counters = {"span_gets": 90, "source_pool_gets": 0, "separate_epoch_gets": 90}
    append(span_path, "source_link", 264, link_ref=fresh_link, snapshot_ref=_ref(source_path),
        proof=new_proof, baseline=173, **counters)

    def meter():
        return value("meter.json", {"kind": "ACTUAL_SPAN_METER_LINK_V1", "meter_prefix": _ref(span_path),
            "used": json.loads(frames[span_path][-1])["count"], "round_baseline": 173,
            "initial": 0, "current_round_id": new_proof["round_id"], "next_readback_reserve": 12})
    return meter, span_path, append, counters


def test_source_linked_codec_qualified_epoch_and_positive_source_interleave(tmp_path):
    meter, _, _, _ = codec_fixture(tmp_path / "proofs")
    result = _meter_link(meter())
    assert result["used"] == 264 and result["round_baseline"] == 173
    assert result["used"] - result["round_baseline"] == 91


@pytest.mark.parametrize("fault", ["unqualified_epoch", "attribution", "pending", "source_carry"])
def test_source_linked_codec_unknown_changed_or_unqualified_hold(tmp_path, fault):
    meter, span_path, append, counters = codec_fixture(tmp_path / "proofs", qualified=fault != "unqualified_epoch")
    if fault == "attribution":
        rows = span_path.read_bytes().splitlines(keepends=True)
        frame = json.loads(rows[-1])
        frame["separate_epoch_gets"] = 0
        rows[-1] = encoded(frame)
        span_path.write_bytes(b"".join(rows))
    elif fault == "pending":
        append(span_path, "attempt", 265, span_gets=91, source_pool_gets=0, separate_epoch_gets=91)
    elif fault == "source_carry":
        rows = span_path.read_bytes().splitlines(keepends=True)
        frame = json.loads(rows[-1])
        frame["count"] = 263
        rows[-1] = encoded(frame)
        span_path.write_bytes(b"".join(rows))
    with pytest.raises(Stop):
        _meter_link(meter())
