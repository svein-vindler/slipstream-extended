"""Activity/source-bound side declarations through genuine FIT and Coach storage."""
import base64
import gzip
import json
from pathlib import Path

import pytest

from pipeline import coach_backfill
from pipeline.granular import gzip_json
from scripts.benchmark_coach_reuse import CountingStore
from scripts.build_gct_sides_contract import (
    ACTIVITY,
    CONTEXT_KEY,
    PREFIX,
    declare,
    selected,
    source_store,
)


def test_source_bound_session_and_actual_lap_sides(tmp_path, record_property):
    store = source_store(tmp_path)
    context = declare(store)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, analysis = selected(store)
    record_property("genuine_before_objects", json.dumps({
        key: base64.b64encode(body).decode() for key, body in store.objects.items()
    }, sort_keys=True))
    sides = analysis["ground_contact_sides"]
    assert analysis["analyzer_version"] == "1.6.0"
    assert sides["session"] == {
        "status": "available", "left_percent": 49.25, "right_percent": 50.75,
        "recorded_side": "left", "calculated_side": "right", "method": "recorded_and_complement",
    }
    assert [(lap["left_percent"], lap["right_percent"]) for lap in sides["laps"]["items"]] == [
        (49.25, 50.75), (51.25, 48.75),
    ]
    assert sides["declaration"] == {**context["gct_balance_side"], "context_id": "ctx-sides"}
    assert analysis["user_context"] == context
    assert analysis["recorded_running_metrics"]["average_ground_contact_balance"]["value"] == 49.25
    assert analysis["recorded_running_metrics"]["average_ground_contact_balance"]["side_semantics"] == "unknown"
    before = dict(store.objects)
    store.reset_counts()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    assert store.objects == before
    assert store.counts["put"] == 0
    assert selected(store)[0] == pointer


@pytest.mark.parametrize(("raw", "expected", "status"), [
    (0, (100, 0), "available"), (100, (0, 100), "available"),
    (49.25, (50.75, 49.25), "available"), (None, (None, None), "missing"),
    (49.125, (50.875, 49.125), "available"),
    (True, (None, None), "invalid"), (-1, (None, None), "invalid"),
    (101, (None, None), "invalid"), (float("nan"), (None, None), "missing"),
])
def test_raw_right_side_zero_and_invalid_values(tmp_path, raw, expected, status):
    store = source_store(tmp_path)
    declare(store, "right")
    key = PREFIX + "activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["messages"]["session_mesgs"][0]["avg_stance_time_balance"] = raw
    store.objects[key] = gzip_json(decoded)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    _, analysis = selected(store)
    value = analysis["ground_contact_sides"]["session"]
    assert (value["left_percent"], value["right_percent"]) == expected
    assert value["status"] == status


@pytest.mark.parametrize("mode", ["absent", "stale", "wrong_activity", "ambiguous", "unknown_side",
                                 "wrong_date", "wrong_provenance"])
def test_unverified_declaration_never_confirms_sides(tmp_path, mode):
    store = source_store(tmp_path)
    if mode != "absent":
        context = declare(store)
        if mode == "stale":
            context["gct_balance_side"]["source_fit_sha256"] = "f" * 64
        elif mode == "wrong_activity":
            context["activity_id"] = "43"
        elif mode == "unknown_side":
            context["gct_balance_side"]["raw_side"] = "device_guess"
        elif mode == "wrong_date":
            context["activity_date"] = "2026-10-09"
        elif mode == "wrong_provenance":
            context["source"] = "provider_guess"
        elif mode == "ambiguous":
            key = PREFIX + "activity.v1.json"
            decoded = json.loads(gzip.decompress(store.objects[key]))
            decoded["messages"]["session_mesgs"] *= 2
            store.objects[key] = gzip_json(decoded)
        store.objects[CONTEXT_KEY] = json.dumps(context).encode()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    _, analysis = selected(store)
    block = analysis["ground_contact_sides"]
    assert block["status"] in ("unverified", "unavailable")
    assert block["declaration"] is None
    for item in [block["session"], *block["laps"]["items"]]:
        assert item["left_percent"] is item["right_percent"] is None
        assert item["recorded_side"] is item["calculated_side"] is None


def test_genuine140_checkpoint_upgrade_retains_canonical_bytes(tmp_path, record_property):
    legacy = json.loads((Path(__file__).parent / "fixtures/gct_engine_140.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in legacy["objects_base64"].items()}
    old_pointer, old = selected(store)
    assert old["analyzer_version"] == "1.4.0"
    assert "ground_contact_sides" not in old
    before = dict(store.objects)
    coach_backfill.run(store=store)
    store.reset_counts()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert selected(store)[1] == old
    record_property("legacy_noop_counts", json.dumps(store.counts))
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    new_pointer, new = selected(store)
    assert new["analyzer_version"] == "1.6.0"
    assert new_pointer["analysis_key"] != old_pointer["analysis_key"]
    assert store.objects[old_pointer["analysis_key"]] == before[old_pointer["analysis_key"]]
    store.reset_counts()
    result = coach_backfill.run(store=store)
    assert result["generated_this_run"] == result["remaining_activities"] == 0
    assert selected(store)[1] == new
    record_property("after_targeted_upgrade_counts", json.dumps(store.counts))


def archive_document(store):
    from test_coach_context_backup import document

    from pipeline.coach import validate_profile

    pointer, analysis = selected(store)
    profile = validate_profile({"profile_id": "synthetic-segments", "effective_from": "2026-01-01",
                                "created_at": "2026-01-01T00:00:00.000Z",
                                "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}]})
    objects = {pointer["analysis_key"]: store.objects[pointer["analysis_key"]],
               "coach/profiles/v1/2026-01-01/synthetic-segments.json": json.dumps(profile).encode(),
               CONTEXT_KEY: store.objects[CONTEXT_KEY]}
    archive = document(objects)
    for entry in archive["objects"]:
        if entry["key"] == CONTEXT_KEY:
            entry["type"] = "context"
    archive["references"][0]["context"] = CONTEXT_KEY
    return objects, archive


def test_encrypted_backup_preserves_new_context_and_sides(tmp_path):
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup as backup

    store = source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, analysis = selected(store)
    objects, archive = archive_document(store)
    verified = backup.verify_backup(_write_document(tmp_path, archive), PASSWORD)
    assert verified.objects == objects
    assert json.loads(gzip.decompress(verified.objects[pointer["analysis_key"]])) == analysis


@pytest.mark.parametrize("mode", ["complement", "boolean", "extra", "relabel_old_engine", "declaration"])
def test_encrypted_backup_rejects_unbound_or_unversioned_sides(tmp_path, mode):
    from test_private_backup import PASSWORD, _write_document

    from pipeline import private_backup as backup
    from pipeline.backup_source import BackupError

    store = source_store(tmp_path)
    declare(store)
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    pointer, analysis = selected(store)
    if mode == "complement":
        analysis["ground_contact_sides"]["session"]["right_percent"] = 99
    elif mode == "boolean":
        analysis["ground_contact_sides"]["session"]["left_percent"] = False
    elif mode == "extra":
        analysis["ground_contact_sides"]["declaration"]["device_private_extra"] = "synthetic"
    elif mode == "relabel_old_engine":
        analysis["analyzer_version"] = "1.4.0"
    elif mode == "declaration":
        analysis["ground_contact_sides"]["declaration"]["source_fit_sha256"] = "f" * 64
    store.objects[pointer["analysis_key"]] = gzip_json(analysis)
    _, archive = archive_document(store)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, archive), PASSWORD)


def test_interrupted_pointer_retry_retains_new_canonical_bytes(tmp_path, monkeypatch, record_property):
    store = source_store(tmp_path)
    declare(store)
    put = store.put

    def interrupted(key, *args, **kwargs):
        if key == PREFIX + "coach-input/v1/latest-ready.json":
            raise RuntimeError("synthetic interruption before pointer")
        return put(key, *args, **kwargs)

    monkeypatch.setattr(store, "put", interrupted)
    with pytest.raises(RuntimeError, match="synthetic interruption"):
        coach_backfill.run_one(activity=ACTIVITY, store=store)
    canonical = {key: body for key, body in store.objects.items() if "/canonical/" in key}
    assert len(canonical) == 1
    monkeypatch.setattr(store, "put", put)
    store.reset_counts()
    coach_backfill.run_one(activity=ACTIVITY, store=store)
    assert all(store.objects[key] == body for key, body in canonical.items())
    assert selected(store)[0]["analysis_key"] in canonical
    assert store.counts["put"] == 1
    record_property("interrupted_pointer_retry_counts", json.dumps(store.counts))
