import json

import pytest

from pipeline.granular import gzip_json
from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.weight_index import ROOT, compact, index_key, sync_dates
from scripts.benchmark_health_reuse import HealthClient
from scripts.build_weight_contract import build

DAY = "2026-06-12"
KEY = f"{ROOT}/2026/06/{DAY}.json"


def payload(weight=80):
    return {"date": DAY, "measurements": [
        {"weight_kg": weight, "timestamp_local": f"{DAY}T07:30:00", "measurement_id": "synthetic",
         "bmi": 999, "private_provider": "never-index"},
        {"weight_kg": 81, "timestamp_local": f"{DAY}T19:30:00"},
        {"weight_kg": 999, "is_daily_average": True}, None]}


def test_index_keeps_all_weights_clocks_and_average_flags_without_other_metrics():
    rows = compact(DAY, gzip_json(payload()))
    assert len(rows) == 4 and rows[-1] is None
    assert [row["weight_kg"] for row in rows[:-1]] == [80, 81, 999]
    assert rows[0]["timestamp_local"] == f"{DAY}T07:30:00"
    assert rows[2]["is_daily_average"] is True
    assert "bmi" not in rows[0] and "private_provider" not in rows[0]


def test_repeated_index_build_reuses_revisions_and_source_edit_reads_only_changed_object():
    client = HealthClient()
    def store():
        return R2Store(client=client, bucket="synthetic-weight")
    other = KEY.replace(DAY, "2026-06-13")
    store().put(KEY, gzip_json(payload()), "application/json")
    value = payload()
    value["date"] = "2026-06-13"
    store().put(other, gzip_json(value), "application/json")
    assert sync_dates(store(), [DAY])["months_written"] == ["2026-06"]
    before = client.objects[index_key("2026-06")]["Body"]
    client.reset()
    assert sync_dates(store(), [DAY])["months_unchanged"] == ["2026-06"]
    assert client.reads == [index_key("2026-06")] and client.writes == []
    store().put(KEY, gzip_json(payload(75)), "application/json")
    client.reset()
    sync_dates(store(), [DAY])
    assert client.reads == [index_key("2026-06"), KEY]
    assert client.objects[index_key("2026-06")]["Body"] != before
    del client.objects[KEY]
    sync_dates(store(), [DAY])
    index = json.loads(client.objects[index_key("2026-06")]["Body"])
    assert KEY not in index["objects"] and other in index["objects"]


def test_builder_budget_failure_preserves_source_and_recovery_is_idempotent():
    client = HealthClient()
    good = R2Store(client=client, bucket="synthetic-weight")
    good.put(KEY, gzip_json(payload()), "application/json")
    budget = R2Store(client=client, bucket="synthetic-weight", max_writes_per_run=1)
    budget.put("synthetic-budget.json", b"{}", "application/json")
    before = client.objects[KEY]["Body"]
    with pytest.raises(R2BudgetError):
        sync_dates(budget, [DAY])
    assert client.objects[KEY]["Body"] == before
    assert index_key("2026-06") not in client.objects
    sync_dates(R2Store(client=client, bucket="synthetic-weight"), [DAY])
    client.reset()
    sync_dates(R2Store(client=client, bucket="synthetic-weight"), [DAY])
    assert client.writes == []


@pytest.mark.parametrize("raw", [b"invalid", b"\x1f\x8bbad", b"x" * (256 * 1024 + 1),
                                   gzip_json({"date": DAY, "measurements": ["x" * (512 * 1024)]})],
                         ids=["json", "gzip", "stored-limit", "decoded-limit"])
def test_invalid_or_oversized_canonical_cannot_enter_index(raw):
    with pytest.raises((ValueError, OSError, EOFError)):
        compact(DAY, raw)


def test_shared_fixture_is_deterministic_and_indexes_exclude_extra_provider_fields():
    import base64
    first = build()
    assert first == build()
    for key, value in first["states"]["initial"].items():
        if key.startswith("health/indexes/"):
            assert b"must-not-enter-index" not in base64.b64decode(value["body"])
