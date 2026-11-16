"""Activity CLI imports and analyzes new runs without waiting for the summary CSV."""

import gzip
import json
import sys

import pytest
from botocore.awsrequest import AWSResponse
from test_activity_refresh_runtime import Raw
from test_activity_refresh_runtime import runtime as runtime

from pipeline import activity_refresh


def add_profile(store):
    store.put("coach/profiles/v1/2026-01-01/synthetic.json", json.dumps({
        "profile_id": "synthetic-profile", "effective_from": "2026-01-01",
        "zones": [{"label": "synthetic", "min_bpm": 100, "max_bpm": 150}],
    }).encode(), "application/json")


@pytest.fixture
def cli(runtime, monkeypatch, tmp_path):
    detail, source, store, calls, _ = runtime
    for name in (
        "GARMIN_PROVIDER_FILE", "GARMIN_ACCOUNT", "GARMINTOKENS", "GARMINTOKENS_FILE",
        "GARMIN_PERSONAL_API_KEY", "GARMIN_PERSONAL_API_KEY_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GARMIN_PROVIDER", "legacy")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setattr(activity_refresh, "R2Store", lambda: store)
    class RecentSDK:
        def get_activities_by_date(self, start, end, *, sortorder):
            assert sortorder == "desc"
            return [detail]

        def __getattr__(self, name):
            return getattr(source, name)

    monkeypatch.setattr(activity_refresh, "_login", lambda: RecentSDK())

    def invoke(*args, automatic=False):
        selector = [] if automatic else ["--activity-id", "123"]
        monkeypatch.setattr(sys, "argv", ["activity-refresh", *selector, *args])
        return activity_refresh.main()

    return detail, store, calls, invoke


def analysis(store):
    pointer = json.loads(store.get("activities/2027/123/coach-input/v1/latest-ready.json"))
    return json.loads(gzip.decompress(store.get(pointer["analysis_key"])))


@pytest.mark.parametrize("event", ["schedule", "workflow_dispatch"])
def test_cli_new_activity_gets_coach_without_waiting_for_summary_csv(cli, monkeypatch, event):
    detail, store, _, invoke = cli
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    detail["summaryDTO"]["movingDuration"] = 1700
    add_profile(store)
    report = invoke(automatic=True)
    assert "summary/activities.csv" not in store.list_keys("summary/")
    assert report["activities"][0]["coach_status"] == "ready"
    result = analysis(store)
    assert result["activity"]["date"] == "2027-01-01"
    assert result["summary"]["moving_seconds"] == 1700


def test_pending_profile_resumes_and_ready_coach_reuses_without_more_downloads(cli):
    _, store, calls, invoke = cli
    pending = invoke()
    assert pending["activities"][0]["coach_status"] == "no_effective_profile"
    first_downloads = sum("download-service" in url for _, url in calls)
    add_profile(store)
    resumed = invoke()
    assert resumed["by_status"] == {"unchanged": 1}
    assert resumed["activities"][0]["coach_status"] == "ready"
    before = store.list_keys("activities/2027/123/coach-input/v1/canonical/")
    writes = store.operations["put"]
    repeated = invoke()
    assert repeated["activities"][0]["coach_reused"] is True
    assert store.operations["put"] == writes
    assert store.list_keys("activities/2027/123/coach-input/v1/canonical/") == before
    assert len(before) == 1
    assert sum("download-service" in url for _, url in calls) == first_downloads == 2


@pytest.mark.parametrize("moving,expected", [
    (0, 0), (1672.5, 1672.5), (None, None), (True, None), (-1, None), ("1700", None),
])
def test_only_observed_valid_moving_duration_enters_coach(cli, moving, expected):
    detail, store, _, invoke = cli
    detail["movingDuration"] = moving
    detail["duration"] = 1800
    add_profile(store)
    assert invoke()["activities"][0]["coach_status"] == "ready"
    assert analysis(store)["summary"]["moving_seconds"] == expected


def test_changed_source_regenerates_then_reuses_its_targeted_analysis(cli):
    detail, store, _, invoke = cli
    add_profile(store)
    invoke()
    original = analysis(store)["analysis_id"]
    detail["movingDuration"] = 1600
    report = invoke()
    assert report["by_status"] == {"refreshed": 1}
    assert report["activities"][0]["coach_reused"] is False
    assert analysis(store)["analysis_id"] != original
    assert analysis(store)["summary"]["moving_seconds"] == 1600
    assert invoke()["activities"][0]["coach_reused"] is True


def test_missing_local_clock_is_pending_without_inventing_a_local_date(cli):
    detail, store, _, invoke = cli
    detail["summaryDTO"].pop("startTimeLocal")
    add_profile(store)
    report = invoke()
    assert report["activities"][0]["coach_status"] == "activity_date_unknown"
    assert not any("coach-input" in key for key in store.list_keys("activities/"))


@pytest.mark.parametrize("status", [403, 503])
def test_coach_storage_auth_or_service_failure_is_a_nonzero_cli_exit(cli, monkeypatch, status):
    _, store, _, invoke = cli
    invoke()  # Successful source import, legitimately waiting for its profile.
    add_profile(store)
    send = store.client._endpoint.http_session.send

    def fail_profile(request):
        if request.method == "GET" and "/coach/profiles/" in request.url:
            return AWSResponse(request.url, status, {}, Raw(b"<Error><Code>AccessDenied</Code></Error>"))
        return send(request)

    monkeypatch.setattr(store.client._endpoint.http_session, "send", fail_profile)
    with pytest.raises(SystemExit) as error:
        invoke()
    assert error.value.code != 0
    assert not store.list_keys("activities/2027/123/coach-input/")


def test_corrupt_coach_source_schema_is_a_nonzero_cli_exit(cli):
    _, store, _, invoke = cli
    invoke()
    add_profile(store)
    store.put("activities/2027/123/activity.v1.json", b"[]", "application/json")
    with pytest.raises(SystemExit) as error:
        invoke()
    assert error.value.code != 0
    assert not store.list_keys("activities/2027/123/coach-input/")


def test_interrupted_pointer_publication_resumes_without_duplicate_analysis_or_downloads(cli, monkeypatch):
    _, store, calls, invoke = cli
    add_profile(store)
    send = store.client._endpoint.http_session.send
    interrupted = False

    def stop_before_pointer_send(request):
        nonlocal interrupted
        if request.method == "PUT" and request.url.endswith("/latest-ready.json") and not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        return send(request)

    monkeypatch.setattr(store.client._endpoint.http_session, "send", stop_before_pointer_send)
    with pytest.raises(SystemExit) as error:
        invoke()
    assert error.value.code == 130
    canonical = store.list_keys("activities/2027/123/coach-input/v1/canonical/")
    assert len(canonical) == 1
    downloads = sum("download-service" in url for _, url in calls)
    report = invoke()
    assert report["by_status"] == {"unchanged": 1}
    assert report["activities"][0]["coach_status"] == "ready"
    assert store.list_keys("activities/2027/123/coach-input/v1/canonical/") == canonical
    assert sum("download-service" in url for _, url in calls) == downloads == 2
    assert invoke()["activities"][0]["coach_reused"] is True


def test_unsupported_activity_and_empty_recent_selection_do_not_create_coach(cli):
    detail, store, calls, invoke = cli
    detail["activityTypeDTO"] = {"typeKey": "meditation"}
    assert invoke()["by_status"] == {"unsupported": 1}
    assert invoke(automatic=True)["activities_checked"] == 0
    assert not store.list_keys("activities/")
    assert sum("download-service" in url for _, url in calls) == 0


def test_supported_nonrunning_activity_imports_without_coach(cli):
    detail, store, _, invoke = cli
    detail["activityTypeDTO"] = {"typeKey": "cycling"}
    add_profile(store)
    report = invoke()
    assert report["by_status"] == {"refreshed": 1}
    assert "coach_status" not in report["activities"][0]
    assert not store.list_keys("activities/2027/123/coach-input/")


def test_personal_cli_stays_unsupported_without_login_storage_or_fallback(cli, monkeypatch):
    _, store, calls, invoke = cli
    monkeypatch.setenv("GARMIN_PROVIDER", "personal")
    monkeypatch.setenv("GARMIN_ACCOUNT", "synthetic-owner")
    report = invoke()
    assert report["status"] == "unsupported"
    assert report["garmin_provider"] == "personal"
    assert report["unsupported_features"] == ["activity_detail"]
    assert calls == []
    assert not any(store.operations.values())
