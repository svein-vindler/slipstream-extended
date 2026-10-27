"""Partial provider coverage must not replace complete canonical activity artifacts."""

from pathlib import Path

import pytest
from test_garmin_personal import HTTP


def test_personal_forced_refresh_preserves_complete_existing_activity(tmp_path):
    from pipeline.granular_export import activity_artifact_keys, export_activity
    from pipeline.sources.garmin_personal import PersonalGarmin

    activity = {
        "activityId": 123,
        "activityType": {"typeKey": "running"},
        "startTimeLocal": "2026-10-25 02:30:00",
    }
    existing = set(activity_artifact_keys(activity))

    class Store:
        def put(self, *args, **kwargs):
            raise AssertionError("A partial source must not replace existing complete artifacts")

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    result = export_activity(
        activity, client, Store(), Path(tmp_path), existing_keys=existing, force=True
    )
    assert result["status"] == "unsupported"
    assert result["files"] == []
    assert http.sent == []


@pytest.mark.parametrize(
    "module_name,kwargs",
    [
        ("activity_refresh", {}),
        ("activity_backfill", {"start_date": "2026-10-25", "end_date": "2026-10-25"}),
        ("activity_backfill_scheduler", {}),
        ("health_detail_backfill", {}),
        (
            "training_sync",
            {"config": {"status": "enabled", "date": "2026-10-25", "timezone": "UTC"}},
        ),
    ],
)
def test_unsupported_jobs_stop_before_storage_and_provider_io(module_name, kwargs):
    import importlib

    from pipeline.sources.garmin_personal import PersonalGarmin

    module = importlib.import_module("pipeline." + module_name)

    class Store:
        def __getattr__(self, name):
            raise AssertionError("An unsupported job must not access storage")

    http = HTTP([])
    client = PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http)
    report = module.run(garmin=client, store=Store(), **kwargs)
    assert report["status"] == "unsupported"
    assert http.sent == []
