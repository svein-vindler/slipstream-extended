"""Known source2 retains its unresolved channel while verified positives reach bulk."""
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_rolling_span_source_proof import raw_floor_fixture, ref

from pipeline.history_state import Job, Stop, digest, encoded, read
from pipeline.rolling_span_source_proof import observed_raw_delivery


def write(path, value):
    body = value if isinstance(value, bytes) else encoded(value)
    Path(path).write_bytes(body)
    return digest(body)


def stopped_delivery(tmp_path):
    dates = [(datetime(2026, 9, 1) + timedelta(days=index)).date().isoformat() for index in range(30)]
    job = Job.create(tmp_path / "job", dates[0], dates[-1], ["hr", "hrv"], "legacy", "synthetic")
    manifest = {"storage_target_sha256": "a" * 64, "lower_endpoint": dates[0],
        "job_plan_sha256": job.plan_sha, "today": "2026-11-11", "timezone": "UTC",
        "upper_endpoint": dates[-1], "scope_status": "final", "parent_span_sha256": None}
    delivered = [(day, stream) for day in dates[:15] for stream in ("hr", "hrv")
                 if (day, stream) != (dates[14], "hrv")]
    wrapper = raw_floor_fixture(job, tmp_path / "proof", manifest, delivered=delivered, new_dates=dates)
    floor = json.loads(read(wrapper["path"]))
    binding = json.loads(read(floor["binding"]["path"]))
    scope_path, post_path = tmp_path / "proof" / "scope.json", tmp_path / "proof" / "postcheck.json"
    scope, post = json.loads(read(scope_path)), json.loads(read(post_path))
    events = [frame["event"] for frame in job.events()[0]]
    selection = events[0]
    last = next(event for event in events if event["type"] == "target"
                and event.get("source_verified") and event["date"] == dates[14])
    leaf = Path(last["folder"])
    receipt = json.loads(read(leaf / "RECEIPT.json"))
    empty = {"date": dates[14], "stream": "hrv", "status": "empty_unresolved", "source_verified": False}
    receipt.update(status="stopped", targets=[receipt["targets"][0], empty])
    receipt_sha = write(leaf / "RECEIPT.json", receipt)
    write(leaf / "RESULT.json", {"native": 2, "receipt_sha256": receipt_sha})
    last.update(native=2, receipt_sha256=receipt_sha)
    empty.update(folder=str(leaf), native=2, receipt_sha256=receipt_sha)
    # Build a source-prefix with real reserve/close pairs before the existing publisher events.
    write(job.folder / "events.jsonl", b"")
    job.append(selection)
    for day in dates[:15]:
        identifier = "synthetic:" + day
        job.append({"type": "date_reserved", "id": identifier, "UTC": datetime.now(UTC).isoformat(),
                    "HTTP_reserved": 8, "reload_reserved": 1})
        for event in events:
            if event["type"] == "target" and event.get("source_verified") and event["date"] == day:
                job.append(event)
        if day == dates[14]:
            job.append({"type": "target", **empty})
        job.append({"type": "date_closed", "id": identifier, "UTC": datetime.now(UTC).isoformat(),
                    "physicalHTTP": 7, "reload_intent": 1, "reload_ACK": 1})
    run = "d" * 32
    job.append({"type": "begin", "run": run})
    job.append({"type": "pause", "reason": "no_more_observed", "not_before": None})
    job.append({"type": "end", "run": run, "status": "empty_unresolved"})
    for event in events:
        if event["type"] != "daily_round_started" and not (event["type"] == "target" and event.get("source_verified")):
            job.append(event)
    refs = []
    for item in scope["outcomeRefs"]:
        refs.append(ref(Path(item["path"])))
    status = {**job.status(), "daily_status": "source_stopped", "daily_source_stop": "empty_unresolved",
              "daily_round": selection["round"], "daily_new_dates": 15}
    terminal = tmp_path / "proof" / "terminal.json"
    write(terminal, {"actualNative": 2, "allNativeDescendantsTerminal": True,
                     "bindingSHA256": digest(encoded(binding))})
    outcome = tmp_path / "proof" / "outcome.json"
    write(outcome, {"bindingSHA256": digest(encoded(binding)), "actualNative": 2, "sourceFailed": True,
                    "normalCLIStatus": status, "journalReceipts": refs, "successClaimed": False})
    context = {**scope["dailyContext"], "actualNative": 2, "sourceFailure": True,
        "knownFirstempty": {"schema": 1, "kind": "DAILY_KNOWN_FIRSTEMPTY_RAW_V1", "sourceStatus": status,
                           "sourceNativeTerminal": ref(terminal), "sourceOutcome": ref(outcome)}}
    scope.update(dailyContext=context, outcomeRefs=refs, terminalEvidence=ref(terminal))
    write(scope_path, scope)
    post.update(dailyContext=context, outcomeRefs=refs, verificationPlan=ref(scope_path))
    write(post_path, post)
    return job, {"binding": floor["binding"], "postcheck": ref(post_path)}, manifest


def test_source2_positive_stopped_leaf_is_verified_without_sourcefree_or_full_selection(tmp_path):
    job, delivery, manifest = stopped_delivery(tmp_path)
    observed = observed_raw_delivery(job, **{name + "_ref": value for name, value in delivery.items()},
                                    target_sha=manifest["storage_target_sha256"])
    partial = observed["known_firstempty_partial"]
    assert len(observed["destinations"]) == 29
    assert len(partial["selected_dates"]) == 30 and len(partial["attempted_dates"]) == 15
    assert len(partial["unattempted_dates"]) == 15
    assert partial["unresolved_targets"] == [{"date": "2026-09-15", "stream": "hrv", "status": "empty_unresolved"}]
    assert partial["source_actual_native"] == 2 and partial["source_failure"] is True
    assert observed["stage_facts"][0]["source_failure"] is True
    assert any(row["source_native"] == 2 for row in observed["destinations"])
    assert job.status()["counts"] == {"published": 29, "empty_unresolved": 1, "pending": 30}


def partial_scope(tmp_path):
    from pipeline.reference_bulk import create_affected_scope, create_daily_endpoint_scope

    job, delivery, manifest = stopped_delivery(tmp_path)
    policy_path = tmp_path / "policy.json"
    write(policy_path, {"schema": 1, "kind": "DAILY_REFERENCE_ENDPOINT_POLICY_V1",
        "job_plan_sha256": job.plan_sha, "storage_target_sha256": manifest["storage_target_sha256"],
        "today": manifest["today"], "timezone": "UTC", "deadline_utc": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "latest_allowed_wake_date": manifest["today"], "max_source_dates": 30, "max_endpoints": 406,
        "historical_refs": [], "windows": [7, 28, 30, 90, 365]})
    universe = create_daily_endpoint_scope(job, job.plan_sha, [delivery], ref(policy_path), output=tmp_path / "universe.json")
    policy = json.loads(read(policy_path))
    scope = create_affected_scope(job, job.plan_sha, [delivery], universe, today=policy["today"],
        timezone=policy["timezone"], deadline_utc=policy["deadline_utc"], output=tmp_path / "affected.json")
    return job, delivery, universe, scope


def test_partial_scope_binds_delivered_dates_and_all_five_existing_windows(tmp_path):
    job, delivery, universe_ref, scope_ref = partial_scope(tmp_path)
    universe, scope = json.loads(read(universe_ref["path"])), json.loads(read(scope_ref["path"]))
    assert universe["source_dates"] == [f"2026-09-{day:02d}" for day in range(1, 16)]
    assert len(universe["known_firstempty_partial"]["selected_dates"]) == 30
    assert {target["window_days"] for target in scope["targets"]} == {7, 28, 30, 90, 365}
    assert len(scope["source_range_proofs"]) == 29
    assert job.status()["pause"] == "no_more_observed"


@pytest.mark.parametrize("fault", ["unknown_native", "nonterminal", "pending_raw", "unverified_receipt",
    "missing_intent_ref", "unknown_source_outcome", "changed_selection", "owner_lock"])
def test_unknown_or_incomplete_source_evidence_never_gains_partial_admission(tmp_path, fault):
    job, delivery, manifest = stopped_delivery(tmp_path)
    post_path = Path(delivery["postcheck"]["path"])
    post = json.loads(read(post_path))
    scope_path = Path(post["verificationPlan"]["path"])
    scope = json.loads(read(scope_path))
    context = scope["dailyContext"]
    known = context["knownFirstempty"]
    terminal_path, outcome_path = Path(known["sourceNativeTerminal"]["path"]), Path(known["sourceOutcome"]["path"])
    terminal, outcome = json.loads(read(terminal_path)), json.loads(read(outcome_path))
    if fault == "unknown_native":
        context["actualNative"] = terminal["actualNative"] = outcome["actualNative"] = None
    elif fault == "nonterminal":
        terminal["allNativeDescendantsTerminal"] = False
    elif fault == "pending_raw":
        job.append({"type": "target", "date": "2026-09-01", "stream": "hr", "status": "ready"})
    elif fault == "unverified_receipt":
        path = next(Path(row["path"]) for row in scope["outcomeRefs"] if row["path"].endswith("RECEIPT.json")
                    and "2026-09-01" in row["path"])
        receipt = json.loads(read(path))
        receipt["cleanup_status"] = "unknown"
        write(path, receipt)
    elif fault == "missing_intent_ref":
        scope["outcomeRefs"] = [row for row in scope["outcomeRefs"]
            if not ("2026-09-01" in row["path"] and row["path"].endswith("INTENT.json"))]
    elif fault == "unknown_source_outcome":
        outcome["sourceFailed"] = False
    elif fault == "changed_selection":
        context["selection"]["new_dates"].reverse()
    elif fault == "owner_lock":
        write(job.folder / "OWNER.lock", {"owner": "unknown"})
    scope["outcomeRefs"] = [ref(Path(row["path"])) for row in scope["outcomeRefs"]]
    outcome["journalReceipts"] = scope["outcomeRefs"]
    write(outcome_path, outcome)
    write(terminal_path, terminal)
    known["sourceOutcome"], known["sourceNativeTerminal"] = ref(outcome_path), ref(terminal_path)
    scope["terminalEvidence"] = ref(terminal_path)
    write(scope_path, scope)
    post.update(dailyContext=context, outcomeRefs=scope["outcomeRefs"], verificationPlan=ref(scope_path))
    write(post_path, post)
    before = read(job.folder / "events.jsonl")
    with pytest.raises(Stop):
        observed_raw_delivery(job, delivery["binding"], ref(post_path), target_sha=manifest["storage_target_sha256"])
    assert read(job.folder / "events.jsonl") == before
    if fault == "owner_lock":
        assert (job.folder / "OWNER.lock").is_file()
