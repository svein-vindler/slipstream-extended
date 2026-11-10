"""Explicit, deterministic and finite full-SDK/connected-reader canary scope."""
from datetime import date, timedelta

import pytest

from pipeline.history_state import DurabilityError, Stop
from pipeline.reference_bulk import _targets


def targets(first, count):
    lower = date.fromisoformat(first)
    return _targets([(lower + timedelta(days=index)).isoformat() for index in range(count)])


def test_canary_selection_is_finite_deterministic_and_covers_actual_groups():
    from pipeline.reference_verification_policy import select_canaries

    declared = targets("2025-09-29", 406)
    answer = select_canaries(declared, "Europe/Oslo")
    assert answer == select_canaries(list(reversed(declared)), "Europe/Oslo")
    assert 18 <= len(answer["selected_keys"]) <= 34
    assert answer["missing_groups"] == []
    assert len(answer["groups"]) == 6
    assert all(row["first"] and row["middle"] and row["last"]
               and row["month_boundary"] and row["year_boundary"] for row in answer["groups"])
    assert all(row["forward"] and row["backward"] for row in answer["dst"])
    assert set(answer["selected_keys"]) <= {row["key"] for row in declared}


def test_small_and_absent_groups_are_explicit_and_never_invented():
    from pipeline.reference_verification_policy import select_canaries

    declared = targets("2026-10-18", 8)
    answer = select_canaries(declared, "UTC")
    assert len(answer["selected_keys"]) == 18
    assert all(row["month_boundary"] is None and row["year_boundary"] is None for row in answer["groups"])
    assert all(row["forward"] is None and row["backward"] is None for row in answer["dst"])
    one = declared[:1]
    tiny = select_canaries(one, "UTC")
    assert tiny["selected_keys"] == [one[0]["key"]]
    assert len(tiny["missing_groups"]) == 5
    with pytest.raises(Stop):
        select_canaries([], "UTC")
    with pytest.raises(Stop):
        select_canaries([*one, *one], "UTC")


def test_create_only_policy_binds_full_pending_scope_and_rejects_tampering(tmp_path):
    import json

    from test_reference_bulk_core import published_scope

    from pipeline.history_state import encoded, read
    from pipeline.reference_verification_policy import create_canary_policy, validate_policy

    job, scope, folder, _, workset, _, _, _ = published_scope(tmp_path, days=8, publish=False)
    path = tmp_path / "policy.json"
    policy = create_canary_policy(job, job.plan_sha, scope, folder, new_invocation="synthetic-new", output=path)
    binding = {"operation": "rolling_bulk", "invocation": "synthetic-new", "bulkVerificationPolicy": policy,
               "bulkScope": scope, "plan": {"sha256": job.plan_sha},
               "targetSHA256": workset.binding["storage_target_sha256"]}
    answer = validate_policy(workset, binding, workset.targets)
    assert len(answer["selected_keys"]) == 18
    assert answer["coverage"]["sdk_targets"] == 48
    assert answer["coverage"]["reader_targets"] == 18
    assert answer["coverage"]["reader_unchecked_targets"] == 30
    with pytest.raises(DurabilityError):
        create_canary_policy(job, job.plan_sha, scope, folder, new_invocation="synthetic-new", output=path)
    with pytest.raises(Stop):
        validate_policy(workset, binding, workset.targets[:-1])
    original = json.loads(read(path))
    for fault in ("selected", "count", "timezone", "mode"):
        value = json.loads(encoded(original))
        if fault == "selected":
            value["selection"]["selected_keys"].pop()
        elif fault == "count":
            value["pending_target_count"] -= 1
        elif fault == "timezone":
            value["timezone"] = "UTC"
        else:
            value["kind"] = "arbitrary-policy"
        body = encoded(value)
        path.write_bytes(body)
        from pipeline.history_state import digest
        altered = {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}
        with pytest.raises(Stop):
            validate_policy(workset, {**binding, "bulkVerificationPolicy": altered}, workset.targets)
    path.write_bytes(encoded(original))


def test_legacy_no_policy_remains_full_reader_scope():
    from pipeline.reference_verification_policy import validate_policy

    declared = targets("2026-10-25", 1)
    answer = validate_policy(None, {"operation": "rolling_bulk"}, declared)
    assert answer["policy_ref"] is None and answer["coverage"] is None
    assert answer["selected_keys"] == [row["key"] for row in declared]
