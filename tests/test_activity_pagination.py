"""Synthetic sources and private in-memory storage only; never log in."""
import copy
import json

import pytest
from botocore.exceptions import ClientError

from pipeline import activity_backfill as backfill
from pipeline.activity_backfill_scheduler import PLAN_KEY
from pipeline.activity_backfill_scheduler import run as schedule
from pipeline.activity_pagination import encode_progress, load_cursor, range_status
from pipeline.granular_export import activity_artifact_keys
from pipeline.r2_store import R2BudgetError
from pipeline.sources.activity_page import MetadataBudget, page_params

START, END = "2024-01-01", "2024-12-31"


def activity(aid, kind="running"):
    return {"activityId": aid, "startTimeLocal": "2024-01-01 00:30:00",
            "startTimeGMT": "2023-12-31 23:30:00", "activityType": {"typeKey": kind}}


class Store:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.fail_progress = False

    def list_keys(self, prefix):
        return {k for k in self.objects if k.startswith(prefix)}

    def get(self, key):
        return self.objects[key]

    def put(self, key, data, content_type, **kwargs):
        if self.fail_progress and key.startswith("backfill/"):
            raise OSError("synthetic-private-error")
        self.objects[key] = data
        self.puts.append(key)

    def progress(self):
        data = json.loads(self.objects[backfill.progress_key(START, END)])
        if "pagination" in data:
            data["pagination"] = load_cursor(data, START, END)
        return data


class Source:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.error_at = None

    def get_activity_page(self, start_date, end_date, *, offset=0):
        page_params(start_date, end_date, offset)
        self.calls.append(offset)
        if offset == self.error_at:
            raise TimeoutError("synthetic-private-error")
        return copy.deepcopy(self.rows[offset:offset + 20])


@pytest.fixture
def exporter(monkeypatch):
    calls = []

    def export(row, garmin, store, output, *, existing_keys, force=False):
        calls.append((row["activityId"], force))
        for key in activity_artifact_keys(row):
            if force or key not in existing_keys:
                store.put(key, b"synthetic", "application/octet-stream")
        return {"id": str(row["activityId"]), "status": "updated"}

    monkeypatch.setattr(backfill, "export_activity", export)
    return calls


def run(tmp_path, source, store, **kwargs):
    return backfill.run(start_date=START, end_date=END, store=store, garmin=source,
                        output_dir=str(tmp_path), **kwargs)


def finish(tmp_path, source, store, **kwargs):
    for _ in range(100):
        result = run(tmp_path, source, store, **kwargs)
        if result["status"] in {"complete", "blocked"}:
            return result
    pytest.fail("Synthetic backfill failed to terminate")


@pytest.mark.parametrize("count", [0, 1, 20, 40, 73])
def test_source_end_requires_empty_page_and_matching_passes(tmp_path, exporter, count):
    source, store = Source([activity(i) for i in range(1, count + 1)]), Store()
    result = finish(tmp_path, source, store, max_activities=7)
    assert result["pagination"]["source_exhausted"]
    assert result["remaining_activities"] == 0
    assert result["complete_activities"] == count
    assert len(exporter) == count
    assert source.calls.count(((count + 19) // 20) * 20) >= 2
    before = len(source.calls), len(store.puts)
    run(tmp_path, source, store)
    assert before == (len(source.calls), len(store.puts))


def test_resume_pending_moves_past_already_stored_and_unsupported(tmp_path, exporter):
    rows = [activity(i) for i in range(1, 146)]
    rows[1] = activity(2, "yoga")
    source, store = Source(rows), Store()
    for row in rows[:100]:
        for key in activity_artifact_keys(row):
            store.objects[key] = b"synthetic-existing"
    result = finish(tmp_path, source, store, max_activities=3)
    assert result["complete_activities"] == 144
    assert {aid for aid, _ in exporter} == set(range(101, 146))
    assert result["activities_found"] == 145


def test_batch_limit_and_call_limit_are_not_source_end(tmp_path, exporter):
    source, store = Source([activity(i) for i in range(1, 61)]), Store()
    first = run(tmp_path, source, store, max_activities=1)
    assert first["stop_reason"] == "batch_limit" and first["status"] == "active"
    assert len(first["pagination"]["pending"]) == 19
    assert first["pagination"]["offset"] == 20
    # All observed objects exist, but there are still unseen pages.
    source2, store2 = Source([activity(i, "yoga") for i in range(1, 241)]), Store()
    limit = run(tmp_path, source2, store2)
    assert limit["known_remaining_activities"] == 0 and limit["status"] == "active"
    assert limit["remaining_activities"] > limit["blocked_after_three_failures"]
    assert not limit["remaining_count_is_exact"]
    assert limit["metadata_calls_this_run"] == 10 and len(source2.calls) == 10


@pytest.mark.parametrize("mutation", ["insert", "reorder", "change", "middle_reorder"])
def test_mutation_before_checkpoint_does_not_hide_items(tmp_path, exporter, mutation):
    source, store = Source([activity(i) for i in range(1, 81)]), Store()
    first = run(tmp_path, source, store, max_activities=35)
    assert first["pagination"]["offset"] == 40
    if mutation == "insert":
        source.rows.insert(0, activity(999))
    elif mutation == "change":
        source.rows[0]["activityName"] = "synthetic changed"
    elif mutation == "reorder":
        source.rows[0], source.rows[60] = source.rows[60], source.rows[0]
    else:
        # Outside head/anchor; the verification pass must find the skipped row.
        source.rows[20], source.rows[60] = source.rows[60], source.rows[20]
    result = finish(tmp_path, source, store, max_activities=35)
    expected = {row["activityId"] for row in source.rows}
    assert {aid for aid, _ in exporter} == expected
    assert result["complete_activities"] == len(expected)
    if mutation == "change":
        assert (1, True) in exporter


def test_duplicate_ids_are_imported_once(tmp_path, exporter):
    rows = [activity(i) for i in range(1, 42)]
    rows.insert(20, activity(20))
    result = finish(tmp_path, Source(rows), Store())
    assert len(exporter) == result["activities_found"] == 41


def test_source_error_retains_cursor_and_pending_without_tombstones(tmp_path, exporter, capsys):
    source, store = Source([activity(i) for i in range(1, 42)]), Store()
    run(tmp_path, source, store)
    source.error_at = 20
    with pytest.raises(RuntimeError, match="progress retained"):
        run(tmp_path, source, store)
    error = store.progress()
    assert error["stop_reason"] == "temporary_error" and error["status"] == "active"
    assert error["pagination"]["offset"] == 20
    assert not error["failures"]
    assert "synthetic-private-error" not in capsys.readouterr().out
    source.error_at = None
    assert finish(tmp_path, source, store)["complete_activities"] == 41


@pytest.mark.parametrize("payload", [None, {}, [activity(1), {}],
                                     [dict(activity(1), startTimeLocal=None)],
                                     [dict(activity(1), startTimeLocal="2025-01-01 00:00:00")]])
def test_unknown_or_partial_source_is_never_empty(tmp_path, payload):
    source, store = Source([]), Store()
    source.get_activity_page = lambda *args, **kwargs: payload
    with pytest.raises(RuntimeError):
        run(tmp_path, source, store)
    progress = store.progress()
    assert not progress["pagination"]["source_exhausted"]
    assert progress["pagination"]["offset"] == 0 and progress["status"] == "active"


def test_partial_import_failure_retry_and_blocked_recovery(tmp_path, monkeypatch, exporter):
    source, store = Source([activity(1), activity(2)]), Store()
    original = backfill.export_activity

    def partial(row, *args, **kwargs):
        if row["activityId"] == 1:
            store.put(activity_artifact_keys(row)[0], b"synthetic-fit", "application/octet-stream")
            raise ValueError("synthetic-private-error")
        return original(row, *args, **kwargs)

    monkeypatch.setattr(backfill, "export_activity", partial)
    for _ in range(3):
        result = run(tmp_path, source, store)
    assert result["status"] == "blocked"
    assert result["pagination"]["pending"]["1"]
    assert result["failures"]["1"]["last_error"] == "activity_import_failed"
    monkeypatch.setattr(backfill, "export_activity", original)
    recovered = run(tmp_path, source, store, retry_failures=True)
    assert recovered["status"] == "complete" and not recovered["failures"]
    assert exporter == [(2, False), (1, False)]


@pytest.mark.parametrize("failure", [R2BudgetError("synthetic"), OSError("synthetic"),
    ClientError({"Error": {"Code": "ServiceUnavailable"},
                 "ResponseMetadata": {"HTTPStatusCode": 503}}, "PutObject")])
def test_budget_and_write_stops_do_not_lose_pending(tmp_path, monkeypatch, exporter, failure):
    source, store = Source([activity(i) for i in range(1, 24)]), Store()
    run(tmp_path, source, store, max_activities=1)
    original = backfill.export_activity

    def partial(row, *args, **kwargs):
        store.put(activity_artifact_keys(row)[0], b"synthetic", "application/octet-stream")
        raise failure

    monkeypatch.setattr(backfill, "export_activity", partial)
    with pytest.raises((R2BudgetError, RuntimeError)):
        run(tmp_path, source, store)
    assert "2" in store.progress()["pagination"]["pending"]
    assert not store.progress()["failures"]
    monkeypatch.setattr(backfill, "export_activity", original)
    assert finish(tmp_path, source, store)["complete_activities"] == 23


def test_checkpoint_put_failure_replays_successful_files(tmp_path, exporter):
    source, store = Source([activity(i) for i in range(1, 24)]), Store()
    run(tmp_path, source, store, max_activities=1)
    store.fail_progress = True
    with pytest.raises(OSError):
        run(tmp_path, source, store)
    assert store.progress()["pagination"]["offset"] == 20
    store.fail_progress = False
    assert finish(tmp_path, source, store)["complete_activities"] == 23
    assert len(exporter) == 23


def test_explicit_reconciliation_finds_late_upload_and_old_changes(tmp_path, exporter):
    source, store = Source([activity(1)]), Store()
    finish(tmp_path, source, store)
    source.rows[0]["distance"] = 1000
    source.rows.insert(0, activity(2))
    result = run(tmp_path, source, store, reconcile=True)
    assert result["status"] == "complete"
    assert exporter == [(1, False), (2, False), (1, True)]


def test_old_active_progress_is_additive_and_completed_plan_is_read_only(tmp_path, exporter):
    store, source = Store(), Source([activity(1)])
    key = backfill.progress_key(START, END)
    store.objects[key] = json.dumps({"schema_version": 2, "remaining_activities": 1,
        "blocked_after_three_failures": 0, "failures": {"1": {"attempts": 1}}}).encode()
    result = finish(tmp_path, source, store)
    assert result["status"] == "complete" and key in store.objects
    store.objects[PLAN_KEY] = json.dumps({"schema_version": 2, "status": "complete", "ranges": []}).encode()
    source.calls.clear()
    puts = len(store.puts)
    assert schedule(store=store, garmin=source)["status"] == "complete"
    assert not source.calls and len(store.puts) == puts


def test_scheduler_obeys_shared_metadata_budget_and_does_not_skip_unknown_pages(tmp_path, monkeypatch):
    store, source = Store(), Source([activity(i, "yoga") for i in range(1, 241)])
    store.objects[PLAN_KEY] = json.dumps({"schema_version": 2, "status": "active", "ranges": [
        {"start_date": START, "end_date": END}, {"start_date": "2023-01-01", "end_date": "2023-12-31"}]}).encode()
    monkeypatch.chdir(tmp_path)
    result = schedule(store=store, garmin=source)
    assert result["status"] == "active" and result["ranges"][0]["status"] == "active"
    assert len(source.calls) == 10


@pytest.mark.parametrize("value", [True, -1, 21, 1.5, "20"])
def test_page_size_is_strictly_bounded(value):
    with pytest.raises(ValueError):
        page_params(START, END, 0, value)


@pytest.mark.parametrize("offset", [True, -1, 100001, 1.5, "20"])
def test_offset_is_strictly_bounded(offset):
    with pytest.raises(ValueError):
        page_params(START, END, offset)


def test_invalid_dates_schema_and_retry_configuration_fail_closed():
    for day in ("20240101", "2024-02-30", START + "?method=POST"):
        with pytest.raises(ValueError):
            page_params(day, END, 0)
    for limit in (0, 11, True):
        with pytest.raises(ValueError):
            MetadataBudget(limit)
    with pytest.raises(ValueError):
        load_cursor({"pagination": {"version": 99}}, START, END)
    with pytest.raises(ValueError):
        range_status({"schema_version": 99})
    with pytest.raises(ValueError):
        range_status({"remaining_activities": 0})


def test_interruption_before_progress_write_is_replayable(tmp_path, monkeypatch, exporter):
    source, store = Source([activity(1), activity(2)]), Store()
    original = backfill.export_activity
    interrupted = False

    def interrupt(row, *args, **kwargs):
        nonlocal interrupted
        if row["activityId"] == 2 and not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        return original(row, *args, **kwargs)

    monkeypatch.setattr(backfill, "export_activity", interrupt)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, source, store)
    result = finish(tmp_path, source, store)
    assert result["status"] == "complete" and exporter == [(1, False), (2, False)]


def test_real_exporter_recovers_partial_put_without_redownloading_fit(tmp_path, monkeypatch):
    from pipeline.granular_export import export_activity
    source, store = Source([activity(1)]), Store()
    downloads = []
    source.download_activity = lambda aid, dl_fmt: downloads.append(dl_fmt.name) or b"synthetic-file"
    monkeypatch.setattr(backfill, "export_activity", export_activity)
    monkeypatch.setattr("pipeline.granular_export.extract_fit", lambda data: data)
    monkeypatch.setattr("pipeline.granular_export.decode_fit", lambda *a, **kw: {
        "message_counts": {}, "normalized_strength_sets": []})
    monkeypatch.setattr("pipeline.granular_export.normalize_endurance_session", lambda *a: {
        "summary": {"trackpoint_count": 0}})
    real_put = store.put

    def partial_put(key, *args, **kwargs):
        if key.endswith("activity.v1.json"):
            raise OSError("synthetic storage interruption")
        return real_put(key, *args, **kwargs)

    store.put = partial_put
    with pytest.raises(RuntimeError):
        run(tmp_path, source, store)
    assert "1" in store.progress()["pagination"]["pending"]
    store.put = real_put
    assert finish(tmp_path, source, store)["status"] == "complete"
    assert downloads == ["ORIGINAL", "TCX"]


def test_failed_forced_replacement_stays_pending_when_all_names_exist(tmp_path, monkeypatch, exporter):
    source, store = Source([activity(1)]), Store()
    finish(tmp_path, source, store)
    original = backfill.export_activity
    source.rows[0]["activityName"] = "synthetic changed"
    monkeypatch.setattr(backfill, "export_activity", lambda *a, **kw: (_ for _ in ()).throw(OSError("synthetic")))
    with pytest.raises(RuntimeError):
        run(tmp_path, source, store, reconcile=True)
    assert store.progress()["pagination"]["pending"]["1"]["force"]
    monkeypatch.setattr(backfill, "export_activity", original)
    assert finish(tmp_path, source, store)["status"] == "complete"
    assert exporter == [(1, False), (1, True)]


def test_missing_stored_artifact_reopens_completed_range(tmp_path, exporter):
    source, store = Source([activity(1)]), Store()
    finish(tmp_path, source, store)
    del store.objects[activity_artifact_keys(source.rows[0])[1]]
    assert finish(tmp_path, source, store)["status"] == "complete"
    assert exporter == [(1, False), (1, False)]


def test_repeated_item_failures_do_not_block_older_missing_files(tmp_path, monkeypatch, exporter):
    source, store = Source([activity(i) for i in range(1, 81)]), Store()
    original = backfill.export_activity

    def broken_first(row, *args, **kwargs):
        if row["activityId"] == 1:
            raise ValueError("synthetic broken FIT")
        return original(row, *args, **kwargs)

    monkeypatch.setattr(backfill, "export_activity", broken_first)
    result = finish(tmp_path, source, store, max_activities=3)
    assert result["status"] == "blocked" and result["blocked_after_three_failures"] == 1
    assert {aid for aid, _ in exporter} == set(range(2, 81))


def test_checkpoint_limits_stop_before_cursor_advance(tmp_path, monkeypatch, exporter):
    monkeypatch.setattr("pipeline.activity_pagination.MAX_RECORDS", 20)
    source, store = Source([activity(i) for i in range(1, 42)]), Store()
    run(tmp_path, source, store)
    with pytest.raises(RuntimeError):
        run(tmp_path, source, store)
    assert store.progress()["pagination"]["offset"] == 20
    assert len(store.progress()["pagination"]["records"]) == 20
    assert store.progress()["status"] == "active"
    assert store.progress()["stop_reason"] == "checkpoint_limit"


@pytest.mark.parametrize("damage", ["schema", "offset", "keys", "pending", "end", "scope", "digest"])
def test_corrupt_checkpoint_fails_before_source_access(tmp_path, exporter, damage):
    source, store = Source([activity(i) for i in range(1, 42)]), Store()
    run(tmp_path, source, store, max_activities=1)
    data = store.progress()
    c = data["pagination"]
    if damage == "schema":
        c["version"] = 99
    elif damage == "offset":
        c["offset"] = 21
    elif damage == "keys":
        c["records"]["2"]["keys"] = ["arbitrary/private/object"]
    elif damage == "pending":
        c["pending"]["2"]["activity"]["activityId"] = 999
    elif damage == "end":
        c["source_exhausted"] = True
    elif damage == "scope":
        data["start_date"] = "2023-01-01"
    else:
        c["digest"] = "unknown"
    store.objects[backfill.progress_key(START, END)] = json.dumps(data).encode()
    source.calls.clear()
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls


def test_scheduler_shares_ten_calls_across_empty_ranges(tmp_path, monkeypatch):
    store, source = Store(), Source([])
    ranges = [{"start_date": f"{year}-01-01", "end_date": f"{year}-12-31"}
              for year in range(2024, 2014, -1)]
    store.objects[PLAN_KEY] = json.dumps({"schema_version": 2, "status": "active", "ranges": ranges}).encode()
    monkeypatch.chdir(tmp_path)
    result = schedule(store=store, garmin=source)
    assert len(source.calls) == 10 and result["status"] == "active"
    assert len(result["last_run_ranges"]) == 5


@pytest.mark.parametrize("attempts", [4, 7])
def test_legacy_accumulated_failure_stays_blocked_without_hiding_other_items(tmp_path, exporter, attempts):
    store, source = Store(), Source([activity(1), activity(2)])
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "schema_version": 2, "remaining_activities": 2, "complete_activities": 0,
        "blocked_after_three_failures": 1, "failures": {"1": {"attempts": attempts}},
    }).encode()
    result = finish(tmp_path, source, store)
    assert result["status"] == "blocked" and result["complete_activities"] == 1
    assert result["failures"]["1"]["attempts"] == attempts
    assert exporter == [(2, False)]
    run(tmp_path, source, store)
    assert store.progress()["failures"]["1"]["attempts"] == attempts
    assert exporter == [(2, False)]


@pytest.mark.parametrize("attempts", [4, 7, backfill.MAX_HISTORICAL_FAILURE_ATTEMPTS])
def test_explicit_legacy_retry_is_once_per_item_and_preserves_total(tmp_path, monkeypatch, attempts):
    source, store = Source([activity(1)] * 41), Store()
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "schema_version": 2, "remaining_activities": 1, "blocked_after_three_failures": 1,
        "failures": {"1": {"attempts": attempts}},
    }).encode()
    calls = []

    def broken(row, *args, **kwargs):
        calls.append(row["activityId"])
        raise ValueError("synthetic broken file")

    monkeypatch.setattr(backfill, "export_activity", broken)
    result = run(tmp_path, source, store, retry_failures=True)
    assert calls == [1] and result["status"] == "blocked"
    failure = store.progress()["failures"]["1"]
    assert failure["attempts"] == min(attempts + 1, backfill.MAX_HISTORICAL_FAILURE_ATTEMPTS)
    assert failure.get("attempts_saturated", False) == (attempts == backfill.MAX_HISTORICAL_FAILURE_ATTEMPTS)
    run(tmp_path, source, store)
    assert calls == [1] and store.progress()["failures"]["1"] == failure
    run(tmp_path, source, store, retry_failures=True)
    assert calls == [1, 1]
    assert store.progress()["failures"]["1"]["attempts"] == min(attempts + 2, backfill.MAX_HISTORICAL_FAILURE_ATTEMPTS)


@pytest.mark.parametrize("attempts", [4, 7])
def test_successful_explicit_legacy_retry_clears_failure(tmp_path, exporter, attempts):
    source, store = Source([activity(1)]), Store()
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "remaining_activities": 1, "blocked_after_three_failures": 1,
        "failures": {"1": {"attempts": attempts}},
    }).encode()
    result = run(tmp_path, source, store, retry_failures=True)
    assert result["status"] == "complete" and not store.progress()["failures"]
    assert exporter == [(1, False)]


@pytest.mark.parametrize("attempts", [-1, True, "4", 4.5, None, 2**31])
def test_invalid_legacy_failure_counter_stops_before_source(tmp_path, attempts):
    store, source = Store(), Source([])
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "remaining_activities": 0, "blocked_after_three_failures": 0,
        "failures": {"1": {"attempts": attempts}},
    }).encode()
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls and not store.puts


@pytest.mark.parametrize("schema", [True, 99, "2"])
def test_invalid_legacy_schema_stops_before_source(tmp_path, schema):
    store, source = Store(), Source([])
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "schema_version": schema, "remaining_activities": 0, "blocked_after_three_failures": 0,
    }).encode()
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls and not store.puts


def test_invalid_saturation_state_stops_before_source(tmp_path):
    store, source = Store(), Source([])
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "remaining_activities": 1, "blocked_after_three_failures": 1,
        "failures": {"1": {"attempts": 7, "attempts_saturated": "true"}},
    }).encode()
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls and not store.puts


@pytest.mark.parametrize("schema", [1, 2])
def test_completed_legacy_range_and_plan_with_old_counters_remain_passive(tmp_path, schema):
    store, source = Store(), Source([])
    store.objects[backfill.progress_key(START, END)] = json.dumps({
        "schema_version": schema, "remaining_activities": 0, "blocked_after_three_failures": 0,
        "failures": {"1": {"attempts": 7}},
    }).encode()
    before = dict(store.objects)
    result = run(tmp_path, source, store)
    assert result["attempted_this_run"] == 0 and result["failures"]["1"]["attempts"] == 7
    store.objects[PLAN_KEY] = json.dumps({"schema_version": schema, "status": "complete", "ranges": [
        {"start_date": START, "end_date": END}]}).encode()
    assert schedule(store=store, garmin=source)["status"] == "complete"
    assert not source.calls and not store.puts
    assert store.objects[backfill.progress_key(START, END)] == before[backfill.progress_key(START, END)]


def test_v1_checkpoint_resumes_to_v2_with_forced_pending_and_legacy_counter(tmp_path, exporter):
    source, store = Source([activity(1), activity(2)]), Store()
    run(tmp_path, source, store, max_activities=1)
    old = store.progress()
    old["pagination"]["pending"]["2"]["force"] = True
    old["failures"] = {"2": {"attempts": 7}}
    for key in activity_artifact_keys(activity(2)):
        store.objects[key] = b"synthetic-old-revision"
    store.objects[backfill.progress_key(START, END)] = json.dumps(old).encode()
    blocked = finish(tmp_path, source, store)
    assert blocked["status"] == "blocked"
    raw = json.loads(store.objects[backfill.progress_key(START, END)])
    assert raw["pagination"]["version"] == 2
    assert raw["pagination"]["records"]["2"] == [old["pagination"]["records"]["2"]["fingerprint"], "2024", 4]
    assert raw["pagination"]["pending"]["2"]["force"] and raw["failures"]["2"]["attempts"] == 7
    assert run(tmp_path, source, store, retry_failures=True)["status"] == "complete"
    assert exporter == [(1, False), (2, True)]


def test_compact_records_roundtrip_supported_and_unsupported_types(tmp_path, exporter):
    source = Source([activity(1), activity(2, "strength_training"), activity(3, "yoga")])
    store = Store()
    result = finish(tmp_path, source, store)
    raw = json.loads(store.objects[backfill.progress_key(START, END)])
    assert [raw["pagination"]["records"][str(i)][1:] for i in range(1, 4)] == [
        ["2024", 4], ["2024", 2], [None, 0]]
    assert load_cursor(raw, START, END) == result["pagination"]
    # v1 completed progress is read as-is, without an automatic write/migration.
    store.objects[backfill.progress_key(START, END)] = json.dumps(result).encode()
    before = dict(store.objects), len(source.calls), len(store.puts)
    assert run(tmp_path, source, store)["pagination"] == result["pagination"]
    assert before == (store.objects, len(source.calls), len(store.puts))


@pytest.mark.parametrize("packed", [None, {}, [], ["f" * 64, "2024"],
    ["f" * 64, "2024", 3], ["f" * 64, "2024", True], ["f" * 64, 2024, 4],
    ["f" * 64, "0000", 4], ["f" * 64, "2024", 0], ["unknown", "2024", 4]])
def test_corrupt_compact_record_stops_before_source(tmp_path, exporter, packed):
    source, store = Source([activity(1), activity(2)]), Store()
    run(tmp_path, source, store, max_activities=1)
    data = json.loads(store.objects[backfill.progress_key(START, END)])
    data["pagination"]["records"]["2"] = packed
    store.objects[backfill.progress_key(START, END)] = json.dumps(data).encode()
    source.calls.clear()
    puts = len(store.puts)
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls and len(store.puts) == puts


def test_compact_pending_reference_is_checked_after_expansion(tmp_path, exporter):
    source, store = Source([activity(1), activity(2)]), Store()
    run(tmp_path, source, store, max_activities=1)
    data = json.loads(store.objects[backfill.progress_key(START, END)])
    data["pagination"]["records"]["2"][1] = "2023"
    store.objects[backfill.progress_key(START, END)] = json.dumps(data).encode()
    source.calls.clear()
    with pytest.raises(ValueError, match="pending activity artifacts"):
        run(tmp_path, source, store)
    assert not source.calls


def test_stored_and_expanded_payload_limits_are_independent(tmp_path, exporter, monkeypatch):
    import pipeline.activity_pagination as pagination
    from pipeline.granular import json_bytes

    source, store = Source([activity(i) for i in range(1, 21)]), Store()
    result = finish(tmp_path, source, store)
    wire = store.objects[backfill.progress_key(START, END)]
    assert len(wire) < len(json_bytes(result))
    monkeypatch.setattr(pagination, "MAX_CHECKPOINT_BYTES", len(wire) + 1)
    with pytest.raises(ValueError, match="Decoded.*payload limit"):
        load_cursor(json.loads(wire), START, END)
    with pytest.raises(ValueError, match="Decoded.*payload limit"):
        encode_progress(result)
    store.objects[backfill.progress_key(START, END)] = b" " * (backfill.MAX_CHECKPOINT_BYTES + 1)
    source.calls.clear()
    with pytest.raises(ValueError, match="payload limit"):
        run(tmp_path, source, store)
    assert not source.calls


@pytest.mark.parametrize("damage", ["boolean_version", "missing_records", "invalid_id", "truncated_json",
                                    "mixed_v1_years", "nonstring_v1_fingerprint"])
def test_damaged_v1_v2_shape_cannot_resume_or_write(tmp_path, exporter, damage):
    source, store = Source([activity(1), activity(2)]), Store()
    run(tmp_path, source, store, max_activities=1)
    data = json.loads(store.objects[backfill.progress_key(START, END)])
    if damage == "boolean_version":
        data["pagination"]["version"] = True
    elif damage == "missing_records":
        del data["pagination"]["records"]
    elif damage == "invalid_id":
        data["pagination"]["records"]["arbitrary"] = data["pagination"]["records"].pop("1")
    elif damage.startswith("mixed"):
        data = store.progress()
        data["pagination"]["records"]["1"]["keys"][0] = "activities/2023/1/activity.fit"
    elif damage.startswith("nonstring"):
        data = store.progress()
        data["pagination"]["records"]["1"]["fingerprint"] = int("1" * 64)
    raw = json.dumps(data).encode()
    store.objects[backfill.progress_key(START, END)] = raw[:-10] if damage == "truncated_json" else raw
    source.calls.clear()
    puts = len(store.puts)
    with pytest.raises(ValueError):
        run(tmp_path, source, store)
    assert not source.calls and len(store.puts) == puts
