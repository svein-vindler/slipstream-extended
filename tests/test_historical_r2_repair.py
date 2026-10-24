"""Historical exact repair through the public pipeline and real Coach producer."""
import base64
import gzip
import json
from datetime import date
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from pipeline import latest_activity, refresh
from scripts.benchmark_coach_reuse import CountingStore
from scripts.build_gct_sides_contract import PREFIX, selected

DAY = "2026-10-08"
RUN_ID = "201"


@pytest.fixture(autouse=True)
def fixed_current_day(monkeypatch):
    class Clock(date):
        @classmethod
        def today(cls):
            return cls(2026, 10, 20)

    for module in ("pipeline.freshness", "pipeline.latest_activity", "pipeline.refresh"):
        monkeypatch.setattr(f"{module}.date", Clock)


@pytest.fixture
def stored_source(monkeypatch):
    fixture = json.loads((Path(__file__).parent / "fixtures/gct_engine_140.json").read_text())
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in fixture["objects_base64"].items()}
    store.objects.pop("summary/activities.csv", None)
    provider_calls = []

    def forbidden_provider(*args, **kwargs):
        provider_calls.append(True)
        pytest.fail("Exact R2-only repair must not contact Garmin")

    monkeypatch.setattr(latest_activity.garmin_source, "_login", forbidden_provider)
    monkeypatch.setattr(latest_activity.garmin_source, "fetch", forbidden_provider)
    store.reset_counts()
    return store, provider_calls, forbidden_provider


def invoke(ingress, stored_source, tmp_path, **changes):
    store, _, login = stored_source
    options = {"repair_only": True, "expected_date": DAY, "activity_id": "42"}
    options.update(changes)
    if ingress == "refresh":
        return refresh.run(refresh.RefreshRequest(mode="activity", run_id=RUN_ID, **options),
                           data_dir=str(tmp_path), store_factory=lambda: store, login=login)
    options["requested_activity_id"] = options.pop("activity_id")
    return latest_activity.run(run_id=RUN_ID, data_dir=str(tmp_path), store=store, **options)


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
def test_historical_exact_repair_uses_real_coach_and_preserves_old_bytes(
    ingress, stored_source, tmp_path, record_property,
):
    store, provider_calls, _ = stored_source
    old_pointer, old = selected(store)
    assert old["analyzer_version"] == "1.4.0"
    before = dict(store.objects)
    result = invoke(ingress, stored_source, tmp_path)
    first_counts = dict(store.counts)
    pointer, analysis = selected(store)
    persisted = json.loads(store.objects[f"refresh/reports/{RUN_ID}.json"])
    assert persisted["status"] == persisted["coach_status"] == "ready"
    assert persisted["activity_id"] == "garmin-42" and persisted["activity_date"] == DAY
    assert persisted["source_checked"] is False
    assert not any(key.startswith("refresh/checks/") for key in store.objects)
    assert analysis["analyzer_version"] == "1.9.0"
    assert analysis["activity_id"] == "42" and analysis["activity"]["date"] == DAY
    assert pointer["analysis_key"] != old_pointer["analysis_key"]
    assert set(store.objects) - set(before) == {
        pointer["analysis_key"], f"refresh/reports/{RUN_ID}.json",
    }
    assert all(store.objects[key] == body for key, body in before.items()
               if key != PREFIX + "coach-input/v1/latest-ready.json")
    if ingress == "refresh":
        assert result["status"] == "complete"
        assert result["source_checks"][0]["checked"] is False
        assert result["source_checks"][0]["outcome"] == "not_checked"
    canonical = {key: body for key, body in store.objects.items() if "/canonical/" in key}
    store.reset_counts()
    invoke(ingress, stored_source, tmp_path)
    assert selected(store)[0]["analysis_key"] == pointer["analysis_key"]
    assert {key: body for key, body in store.objects.items() if "/canonical/" in key} == canonical
    assert provider_calls == []
    record_property("first_repair_counts", json.dumps(first_counts))
    record_property("repeat_repair_counts", json.dumps(store.counts))


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
@pytest.mark.parametrize("version", [99, None, True])
def test_historical_repair_rejects_unknown_stored_schema(ingress, version, stored_source, tmp_path):
    store, provider_calls, _ = stored_source
    key = PREFIX + "activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    decoded["schema_version"] = version
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    before = dict(store.objects)
    with pytest.raises(ValueError, match="schema"):
        invoke(ingress, stored_source, tmp_path)
    assert store.objects == before
    assert provider_calls == []


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
@pytest.mark.parametrize("fault", [
    "wrong_id", "wrong_local_date", "missing_local_date", "missing_activity",
    "missing_canonical", "corrupt_json", "corrupt_gzip", "unknown_shape", "auth",
])
def test_historical_repair_does_not_publish_on_unverified_source(
    ingress, fault, stored_source, tmp_path, monkeypatch,
):
    store, provider_calls, _ = stored_source
    key = PREFIX + "activity.v1.json"
    decoded = json.loads(gzip.decompress(store.objects[key]))
    if fault == "wrong_id":
        decoded["activity"]["id"] = "43"
    elif fault == "wrong_local_date":
        decoded["activity"]["start_time_local"] = "2026-10-09 10:00:00"
    elif fault == "missing_local_date":
        decoded["activity"].pop("start_time_local")
    elif fault == "missing_activity":
        decoded.pop("activity")
    store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
    if fault == "missing_canonical":
        del store.objects[key]
    elif fault == "corrupt_json":
        store.objects[key] = b"not JSON"
    elif fault == "corrupt_gzip":
        store.objects[key] = b"\x1f\x8btruncated"
    elif fault == "unknown_shape":
        store.objects[key] = b"[]"
    elif fault == "auth":
        original_get = store.get

        def denied(requested):
            if requested == key:
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
            return original_get(requested)

        monkeypatch.setattr(store, "get", denied)
    before = dict(store.objects)
    with pytest.raises((ValueError, KeyError, OSError, EOFError, ClientError)):
        invoke(ingress, stored_source, tmp_path)
    assert store.objects == before
    assert provider_calls == []


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
@pytest.mark.parametrize("missing,reason", [
    (PREFIX + "activity.endurance.v1.json", "missing_artifacts"),
    (PREFIX + "activity.tcx", "missing_artifacts"),
    ("profile", "no_effective_profile"),
])
def test_historical_incomplete_source_is_pending_without_replacing_history(
    ingress, missing, reason, stored_source, tmp_path,
):
    store, provider_calls, _ = stored_source
    if missing == "profile":
        for key in list(store.objects):
            if key.startswith("coach/profiles/"):
                del store.objects[key]
    else:
        del store.objects[missing]
    before = dict(store.objects)
    result = invoke(ingress, stored_source, tmp_path)
    report = json.loads(store.objects[f"refresh/reports/{RUN_ID}.json"])
    assert report["status"] == "coach_pending" and report["coach_status"] == reason
    assert report["source_checked"] is False
    assert all(store.objects[key] == body for key, body in before.items())
    assert not any(key.startswith("refresh/checks/") for key in store.objects)
    if ingress == "refresh":
        assert result["status"] == "partial"
        assert result["source_checks"][0]["checked"] is False
    assert provider_calls == []


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
@pytest.mark.parametrize("changes", [
    {"activity_id": None}, {"activity_id": ""}, {"activity_id": "-1"},
    {"activity_id": "garmin-42"}, {"activity_id": "1" * 21}, {"activity_id": "43"},
    {"expected_date": None}, {"expected_date": ""}, {"expected_date": "20261008"},
    {"expected_date": "2026-02-30"}, {"expected_date": "2026-10-8"},
    {"expected_date": "2026-10-09"}, {"expected_date": "2025-10-08"},
])
def test_historical_repair_requires_exact_id_and_strict_matching_local_date(
    ingress, changes, stored_source, tmp_path,
):
    store, provider_calls, _ = stored_source
    before = dict(store.objects)
    with pytest.raises((ValueError, KeyError)):
        invoke(ingress, stored_source, tmp_path, **changes)
    assert store.objects == before
    assert provider_calls == []


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
@pytest.mark.parametrize("day", ["2026-10-12", "2026-10-22"])
def test_provider_fetching_activity_keeps_recent_window(ingress, day, stored_source, tmp_path):
    store, provider_calls, _ = stored_source
    before = dict(store.objects)
    with pytest.raises(ValueError, match="recent"):
        invoke(ingress, stored_source, tmp_path, repair_only=False, expected_date=day)
    assert store.objects == before
    assert provider_calls == []


@pytest.mark.parametrize("ingress", ["refresh", "latest"])
def test_r2_only_keeps_future_limit_before_storage(ingress, stored_source, tmp_path):
    store, provider_calls, _ = stored_source
    before = dict(store.objects)
    with pytest.raises(ValueError, match="after tomorrow|recent"):
        invoke(ingress, stored_source, tmp_path, expected_date="2026-10-22")
    assert store.objects == before
    assert store.counts["get"] == 0
    assert provider_calls == []


@pytest.mark.parametrize("day", ["2026-10-08", "2026-10-21"])
def test_exact_repair_request_accepts_history_and_existing_upper_boundary(day):
    refresh.RefreshRequest(mode="activity", repair_only=True, activity_id="42",
                           expected_date=day).validate()


@pytest.mark.parametrize("mode", ["activity", "night"])
@pytest.mark.parametrize("day,allowed", [
    ("2026-10-12", False), ("2026-10-13", True),
    ("2026-10-21", True), ("2026-10-22", False),
])
def test_provider_request_date_boundaries_are_unchanged(mode, day, allowed):
    options = {"expected_date" if mode == "activity" else "wake_date": day}
    request = refresh.RefreshRequest(mode=mode, **options)
    if allowed:
        request.validate()
    else:
        with pytest.raises(ValueError, match="recent"):
            request.validate()
