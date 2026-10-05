"""One fixture supplies both Python storage and actual Worker transport tests."""
import base64
import gzip
import json

import pytest

from pipeline.health_history_index import index_key
from scripts.build_health_contract import FIXTURE, build_contract

CASES = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]


@pytest.fixture(scope="module")
def contract():
    return build_contract()


def document(state, key):
    body = base64.b64decode(state[key]["body"])
    return json.loads(gzip.decompress(body) if state[key]["contentEncoding"] == "gzip" else body)


def canonical(stream, day):
    root = "health/sleep/v1" if stream == "sleep" else "health/hrv"
    return f"{root}/{day[:4]}/{day[5:7]}/{day}.json"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_imported_and_indexed_values_match_independent_expectations(contract, case):
    state, day, expected = contract["states"]["initial"], case["date"], case["expected"]
    sleep = document(state, canonical("sleep", day))
    assert sleep["date"] == day
    assert sleep["summary"]["sleep_seconds"] == expected["sleep_seconds"]
    assert sleep["summary"]["sleep_score"] == expected["sleep_score"]
    assert sleep["sleep_start_gmt"] == case["sleep"]["dailySleepDTO"]["sleepStartTimestampGMT"]
    sleep_row = document(state, index_key("sleep", day[:7]))["days"][0]
    hrv_index = document(state, index_key("hrv", day[:7]))
    assert sleep_row["summary"]["sleep_seconds"] == expected["sleep_seconds"]
    assert hrv_index["days"][0]["derived"]["mean_ms"] == expected["hrv_mean_ms"]
    key = canonical("hrv", day)
    assert hrv_index["source_revisions"][key] == state[key]["etag"]


def test_repeat_checks_source_without_redownloading_or_rewriting_stored_history(contract):
    operation = contract["operations"]["repeated"]
    assert len(operation["source_calls"]) == 2 * len(CASES)
    assert not any(key.startswith("health/") for key in operation["writes"])
    assert not any(key.startswith(("health/hrv/", "health/sleep/v1/")) for key in operation["reads"])
    before, after = (contract["states"][name] for name in ("initial", "repeated"))
    assert {k: v for k, v in before.items() if k.startswith("health/")} == {
        k: v for k, v in after.items() if k.startswith("health/")}
    key = "refresh/checks/v1/night/2026-09-30.json"
    assert document(after, key)["checked_at"] > document(before, key)["checked_at"]


@pytest.mark.parametrize("stream", ["sleep", "hrv"])
def test_index_failure_retains_success_receipts_and_repair_needs_no_source_fetch(contract, stream):
    states, day = contract["states"], CASES[0]["date"]
    prior = states["repeated" if stream == "sleep" else "repaired_sleep"]
    broken, repaired = states[f"interrupted_{stream}"], states[f"repaired_{stream}"]
    key = canonical(stream, day)
    idx = index_key(stream, day[:7])
    assert broken[key] != prior[key]
    assert broken[idx] == prior[idx]
    for receipt in (f"refresh/checks/v1/health/{stream}/{day}.json",
                    f"refresh/checks/v1/night/{day}.json"):
        assert broken[receipt] == repaired[receipt] == prior[receipt]
    assert repaired[key] == broken[key]
    assert document(repaired, idx)["source_revisions"][key] == repaired[key]["etag"]
    assert contract["operations"][f"repaired_{stream}"]["source_calls"] == []


@pytest.mark.parametrize("name", ["wrong_date", "incomplete"])
def test_unusable_provider_responses_preserve_good_data_and_positive_receipts(contract, name):
    assert contract["states"][name] == contract["states"]["completed"]
    assert contract["operations"][name]["writes"] == []


@pytest.mark.parametrize("name", ["missing", "corrupt"])
def test_recovery_rebuilds_only_indexes_without_source_download(contract, name):
    recovered = contract["states"][f"recovered_{name}"]
    completed = contract["states"]["completed"]
    assert recovered.keys() == completed.keys()
    for key in recovered:
        if key.startswith("health/indexes/"):
            before, after = document(completed, key), document(recovered, key)
            assert after.pop("generated_at") >= before.pop("generated_at")
            assert before == after
        else:
            assert recovered[key] == completed[key]
    operation = contract["operations"][f"recovered_{name}"]
    assert operation["source_calls"] == []
    assert len(operation["writes"]) == 2
    assert all(key.startswith("health/indexes/") for key in operation["writes"])


def test_contract_generation_is_deterministic(contract):
    assert build_contract() == contract
