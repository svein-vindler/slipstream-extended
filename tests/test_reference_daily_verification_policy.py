"""Daily admission cannot borrow rolling policy or bypass real source evidence."""
from types import SimpleNamespace

import pytest

from pipeline.history_state import Stop, digest, encoded
from pipeline.reference_verification_policy import (
    MODE,
    validate_daily_after_source_policy,
    validate_policy,
    validate_policy_binding,
)


def reference(tmp_path, name, value):
    path = tmp_path / name
    body = encoded(value)
    path.write_bytes(body)
    return {"path": str(path.resolve()), "bytes": len(body), "sha256": digest(body)}


def test_default_daily_keeps_every_reader_and_has_no_policy():
    targets = [{"key": "synthetic-a"}, {"key": "synthetic-b"}]
    result = validate_policy(None, {"operation": "daily"}, targets)
    assert result["selected_keys"] == ["synthetic-a", "synthetic-b"]
    assert result["policy_ref"] is result["coverage"] is None


@pytest.mark.parametrize("change", ["absent", "wrong-mode", "handpicked-field", "rolling", "spoof-rolling-policy"])
def test_daily_explicit_gate_precedes_and_does_not_weaken_rolling_contract(tmp_path, change):
    request = {"endpoint_scope_ref": {}, "today": "2026-11-14", "timezone": "Europe/Oslo",
               "deadline_utc": "2026-11-14T12:00:00+00:00", "workset_root": str(tmp_path),
               "carried_ref": {}, "verification_policy": MODE}
    binding = {"operation": "daily", "deferReferenceBuildToBulk": True}
    if change == "absent":
        del request["verification_policy"]
    elif change == "wrong-mode":
        request["verification_policy"] = "handpicked"
    elif change == "handpicked-field":
        request["selected_keys"] = ["synthetic-a"]
    elif change == "rolling":
        binding["operation"] = "rolling_bulk"
    else:
        binding["bulkVerificationPolicy"] = {}
    binding["bulkAfterSource"] = reference(tmp_path, "request.json", request)
    ref = reference(tmp_path, "binding.json", binding)
    with pytest.raises(Stop, match="daily_verification_policy_"):
        validate_daily_after_source_policy(None, ref, {}, {}, [])


def test_daily_cannot_disguise_a_rolling_policy_as_phase_policy(tmp_path):
    ref = reference(tmp_path, "rolling-policy.json", {"kind": MODE})
    with pytest.raises(Stop, match="reference_verification_policy_binding"):
        validate_policy_binding({"operation": "daily", "bulkVerificationPolicy": ref})


def test_explicit_daily_still_requires_genuine_after_source_context(tmp_path):
    request = {"endpoint_scope_ref": {}, "today": "2026-11-14", "timezone": "Europe/Oslo",
               "deadline_utc": "2026-11-14T12:00:00+00:00", "workset_root": str(tmp_path),
               "carried_ref": {}, "verification_policy": MODE}
    binding = {"operation": "daily", "deferReferenceBuildToBulk": True,
               "bulkAfterSource": reference(tmp_path, "daily-request.json", request)}
    (tmp_path / "REQUEST.json").write_bytes(encoded({"scope": {}}))
    ref = reference(tmp_path, "daily-binding.json", binding)
    forged = reference(tmp_path, "forged-after-source.json", {"schema": 1, "kind": "arbitrary"})
    with pytest.raises(Stop, match="reference_bulk_after_source_unqualified"):
        validate_daily_after_source_policy(SimpleNamespace(folder=tmp_path), ref, forged, {}, [])
