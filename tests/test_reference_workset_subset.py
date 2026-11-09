"""Daily affected worksets bind only explicitly selected normalized targets."""
import json

import pytest
from test_reference_workset import declaration

from pipeline.history_state import Stop, encoded
from pipeline.reference_workset import ReferenceWorkset


def test_explicit_daily_subset_remains_exact_across_reopen(tmp_path):
    binding, targets = declaration(["2026-10-25"])
    with pytest.raises(Stop, match="targets"):
        ReferenceWorkset.create(tmp_path / "full", binding, targets[:1])
    workset = ReferenceWorkset.create(tmp_path / "affected", binding, targets[:1], target_subset=True)
    assert workset.targets == targets[:1]
    assert len(workset.wake_dates) == 28
    before = workset.scope_sha256
    reopened = ReferenceWorkset.open(workset.folder, binding)
    assert reopened.scope_sha256 == before and reopened.targets == targets[:1]
    assert reopened.status()["targets"] == 1
    descriptor = json.loads((workset.folder / "WORKSET.json").read_bytes())
    descriptor["schema"] = 1
    descriptor["kind"] = "reference-workset"
    (workset.folder / "WORKSET.json").write_bytes(encoded(descriptor))
    with pytest.raises(Stop, match="targets"):
        ReferenceWorkset.open(workset.folder, binding)
