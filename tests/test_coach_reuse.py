"""Input invalidation and recovery for incremental single-activity analysis."""

import gzip
import json

import pytest
from test_latest_activity import _activity

from pipeline import coach, coach_backfill
from pipeline.coach_backfill import run_one
from pipeline.latest_activity import _coach_status
from scripts.benchmark_coach_reuse import CountingStore

PREFIX = "activities/2026/1"
POINTER = f"{PREFIX}/coach-input/v1/latest-ready.json"
PROFILE = "coach/profiles/v1/2026-01-01/test.json"
CONTEXT = f"{PREFIX}/context/v1/2026-09-30T12-00-00.json"
ACTIVITY = {"id": "1", "date": "2026-09-30", "name": "Synthetic run", "moving_seconds": 1800}


@pytest.fixture
def store():
    return CountingStore()


@pytest.fixture
def builds(monkeypatch):
    calls = []
    original = coach_backfill.build_coach_input

    def observed(**kwargs):
        calls.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(coach_backfill, "build_coach_input", observed)
    return calls


def current(store):
    pointer = json.loads(store.objects[POINTER])
    return json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))


def test_unchanged_inputs_skip_artifact_downloads_analysis_and_writes(store, builds, capsys):
    first = run_one(activity=ACTIVITY, store=store)
    store.reset_counts()
    second = run_one(activity=ACTIVITY, store=store)
    assert second["processed_sources"] == first["processed_sources"]
    assert second["reused_sources"] == first["processed_sources"]
    assert second["skipped_this_run"] == []
    assert len(builds) == 1
    assert (store.counts["get"], store.counts["list"], store.counts["put"]) == (2, 2, 0)
    assert store.counts["download_bytes"] < 1024
    assert _coach_status(store, _activity("1", 9), second) == "ready"
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [
        {"event": "targeted_coach_input", "analysis_reused": False},
        {"event": "targeted_coach_input", "analysis_reused": True},
    ]


@pytest.mark.parametrize("changed", [
    "fit", "endurance", "tcx", "profile", "context", "new_context", "removed_context",
    "name", "date", "type", "moving_seconds", "analyzer",
])
def test_changed_input_rebuilds_and_preserves_the_previous_version(store, builds, monkeypatch, changed):
    store.objects[CONTEXT] = json.dumps({"context_id": "synthetic-context", "note": "before"}).encode()
    run_one(activity=ACTIVITY, store=store)
    old_key = json.loads(store.objects[POINTER])["analysis_key"]
    old_bytes = store.objects[old_key]
    activity = ACTIVITY.copy()
    if changed == "fit":
        value = json.loads(store.objects[f"{PREFIX}/activity.v1.json"])
        value["source_fit_sha256"] = "changed-synthetic-fit"
        store.objects[f"{PREFIX}/activity.v1.json"] = json.dumps(value).encode()
    elif changed == "endurance":
        store.objects[f"{PREFIX}/activity.endurance.v1.json"] = json.dumps({
            "available": True, "summary": {"distance_m": 6000},
        }).encode()
    elif changed == "tcx":
        store.objects[f"{PREFIX}/activity.tcx"] = b"changed synthetic tcx"
    elif changed == "profile":
        value = json.loads(store.objects[PROFILE])
        value["zones"][0]["max_bpm"] = 160
        store.objects[PROFILE] = json.dumps(value).encode()
    elif changed in {"context", "new_context"}:
        key = CONTEXT if changed == "context" else CONTEXT.replace("12-00-00", "13-00-00")
        store.objects[key] = json.dumps({"context_id": "synthetic-context", "note": "after"}).encode()
    elif changed == "removed_context":
        del store.objects[CONTEXT]
    elif changed in {"name", "date", "type", "moving_seconds"}:
        activity[changed] = {"name": "Changed name", "date": "2026-09-29",
                             "type": "trail_running", "moving_seconds": 1900}[changed]
    else:
        monkeypatch.setattr(coach_backfill, "ANALYZER_VERSION", "synthetic-next")
        monkeypatch.setattr(coach, "ANALYZER_VERSION", "synthetic-next")
    result = run_one(activity=activity, store=store)
    assert result["skipped_this_run"] == []
    assert result["reused_sources"] == {}
    assert len(builds) == 2
    assert json.loads(store.objects[POINTER])["analysis_key"] != old_key
    assert store.objects[old_key] == old_bytes
    run_one(activity=activity, store=store)
    assert len(builds) == 2


def test_profile_selection_remains_historically_effective(store, builds):
    run_one(activity=ACTIVITY, store=store)
    value = json.loads(store.objects[PROFILE])
    value.update(profile_id="future-profile", effective_from="2026-10-01")
    store.objects["coach/profiles/v1/2026-10-01/future.json"] = json.dumps(value).encode()
    run_one(activity=ACTIVITY, store=store)
    assert len(builds) == 1
    value.update(profile_id="effective-profile", effective_from="2026-09-01")
    store.objects["coach/profiles/v1/2026-09-01/effective.json"] = json.dumps(value).encode()
    run_one(activity=ACTIVITY, store=store)
    assert len(builds) == 2
    assert current(store)["profile"]["profile_id"] == "effective-profile"


@pytest.mark.parametrize("recovery", ["legacy", "missing_analysis", "force"])
def test_recovery_rebuilds_existing_pointer_safely(store, builds, recovery):
    run_one(activity=ACTIVITY, store=store)
    pointer = json.loads(store.objects[POINTER])
    if recovery == "legacy":
        del pointer["input_signature"]
        store.objects[POINTER] = json.dumps(pointer).encode()
    elif recovery == "missing_analysis":
        del store.objects[pointer["analysis_key"]]
    result = run_one(activity=ACTIVITY, store=store, force=recovery == "force")
    assert result["skipped_this_run"] == [] and len(builds) == 2
    assert json.loads(store.objects[POINTER])["input_signature"]
    assert current(store)["summary"]["moving_seconds"] == 1800


def test_missing_artifact_or_profile_blocks_even_with_matching_pointer(store, builds):
    run_one(activity=ACTIVITY, store=store)
    tcx = store.objects.pop(f"{PREFIX}/activity.tcx")
    result = run_one(activity=ACTIVITY, store=store)
    assert result["blocked_activities"][0]["reason"] == "missing_artifacts"
    store.objects[f"{PREFIX}/activity.tcx"] = tcx
    del store.objects[PROFILE]
    result = run_one(activity=ACTIVITY, store=store)
    assert result["blocked_activities"][0]["reason"] == "no_effective_profile"
    assert len(builds) == 1


def test_unavailable_endurance_cannot_reuse_previous_ready_analysis(store, builds):
    run_one(activity=ACTIVITY, store=store)
    store.objects[f"{PREFIX}/activity.endurance.v1.json"] = b'{"available":false}'
    result = run_one(activity=ACTIVITY, store=store)
    assert result["blocked_activities"][0]["reason"] == "endurance_data_unavailable"
    assert len(builds) == 1


def test_repeated_r2_repairs_preserve_known_moving_time(store, builds):
    run_one(activity=ACTIVITY, store=store)
    repair = {key: value for key, value in ACTIVITY.items() if key != "moving_seconds"}
    run_one(activity=repair, store=store)
    assert current(store)["summary"]["moving_seconds"] == 1800
    run_one(activity=repair, store=store)
    assert len(builds) == 2
    store.objects[f"{PREFIX}/activity.tcx"] = b"changed source"
    run_one(activity=repair, store=store)
    assert len(builds) == 3
    assert current(store)["summary"]["moving_seconds"] is None


def test_failed_rebuild_does_not_advance_pointer_or_signature(store, monkeypatch):
    run_one(activity=ACTIVITY, store=store)
    old_pointer = store.objects[POINTER]
    store.objects[f"{PREFIX}/activity.tcx"] = b"changed source"

    def failure(**kwargs):
        raise ValueError("synthetic analyzer failure")

    monkeypatch.setattr(coach_backfill, "build_coach_input", failure)
    with pytest.raises(ValueError, match="synthetic analyzer failure"):
        run_one(activity=ACTIVITY, store=store)
    assert store.objects[POINTER] == old_pointer


def test_store_without_revision_metadata_rebuilds_conservatively(store, builds, monkeypatch):
    monkeypatch.setattr(store, "list_object_revisions", lambda prefix: {})
    run_one(activity=ACTIVITY, store=store)
    run_one(activity=ACTIVITY, store=store)
    assert len(builds) == 2
    assert POINTER not in store.objects


def test_missing_context_revision_disables_reuse(store, builds, monkeypatch):
    store.objects[CONTEXT] = b'{"context_id":"synthetic-context"}'
    original = store.list_object_revisions

    def incomplete(prefix):
        return {key: revision if key != CONTEXT else ""
                for key, revision in original(prefix).items()}

    monkeypatch.setattr(store, "list_object_revisions", incomplete)
    run_one(activity=ACTIVITY, store=store)
    run_one(activity=ACTIVITY, store=store)
    assert len(builds) == 2
    assert "input_signature" not in json.loads(store.objects[POINTER])


def test_failed_analysis_write_leaves_the_previous_pointer_intact(store, monkeypatch):
    run_one(activity=ACTIVITY, store=store)
    old_pointer = store.objects[POINTER]
    store.objects[f"{PREFIX}/activity.tcx"] = b"changed source"

    def failure(*args, **kwargs):
        raise RuntimeError("synthetic storage failure")

    monkeypatch.setattr(store, "put", failure)
    with pytest.raises(RuntimeError, match="synthetic storage failure"):
        run_one(activity=ACTIVITY, store=store)
    assert store.objects[POINTER] == old_pointer
