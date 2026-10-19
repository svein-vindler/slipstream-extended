"""Actual recent-health results feed the existing refresh diagnostic assembler."""

import json
from datetime import date, datetime, timezone
from time import perf_counter
from types import SimpleNamespace

import pytest

from pipeline import recent_health, refresh
from pipeline.r2_store import R2Store
from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

DAY = "2026-09-30"


@pytest.fixture
def entry(monkeypatch, tmp_path, record_property):
    class Clock(date):
        @classmethod
        def today(cls):
            return cls.fromisoformat(DAY)

    monkeypatch.setattr(recent_health, "date", Clock)
    monkeypatch.setattr("pipeline.freshness.date", Clock)

    class Instant(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("pipeline.health_sync.datetime", Instant)
    monkeypatch.setattr(refresh, "datetime", Instant)
    client, source = HealthClient(), SyntheticGarmin()
    # Neighboring stages are outside this slice. The recent-health producer,
    # normalizers, guarded store and diagnostic assembler remain actual code.
    monkeypatch.setattr(
        refresh.summary_restore, "restore_with_base", lambda *a, **k: SimpleNamespace(base=None)
    )

    def summary(*a, **kwargs):
        (tmp_path / "health_daily.csv").write_text("Date,Weight KG\n")
        return {"status": "complete"}

    monkeypatch.setattr(refresh.refresh_summaries, "run", summary)
    monkeypatch.setattr(refresh.recent_body, "run", lambda **k: {"status": "complete"})
    phases = []
    path = tmp_path / "diagnostic.json"

    def invoke(*, run_id="0"):
        started = perf_counter()
        try:
            return refresh.run(
                refresh.RefreshRequest(run_id=run_id),
                data_dir=str(tmp_path),
                store_factory=lambda: R2Store(client=client, bucket="synthetic-health"),
                login=lambda: source,
                diagnostics_file=path,
            )
        finally:
            report = json.loads(path.read_text())
            phases.append(
                {
                    "source_checks": report["source_checks"],
                    "status": report["status"],
                    "source_calls": list(source.calls),
                    "r2_counts": client.counts.copy(),
                    "r2_reads": client.reads.copy(),
                    "r2_writes": client.writes.copy(),
                    "local_ms": (perf_counter() - started) * 1000,
                }
            )
            record_property("C10_phases", json.dumps(phases))

    invoke.diagnostics_file = path
    return client, source, invoke


def test_wrong_date_does_not_confirm_a_new_successful_health_source_check(entry):
    client, source, invoke = entry
    invoke()
    keys = [
        f"health/hrv/2026/09/{DAY}.json",
        f"refresh/checks/v1/health/hrv/{DAY}.json",
        f"refresh/checks/v1/night/{DAY}.json",
    ]
    old = {key: client.objects[key].copy() for key in keys}
    source.responses["hrv", DAY] = {"hrvSummary": {"calendarDate": "2026-09-29"}, "hrvReadings": []}
    source.calls.clear()
    client.reset()
    report = invoke()
    hrv = next(item for item in report["source_checks"] if item["scope"] == "hrv")
    assert hrv == {"scope": "hrv", "checked": False, "checked_at": None, "outcome": "failed"}
    assert len(source.calls) == 6
    assert all(client.objects[key] == value for key, value in old.items())
    assert not any(key in client.writes for key in keys)
    assert report["status"] == "partial"
    assert report["health_validation_rejections"] == [
        {"stream": "hrv", "date": DAY, "category": "wrong_date"},
    ]


def test_private_refresh_diagnostics_identify_rejected_day_without_hosted_log_details(entry, capsys):
    client, source, invoke = entry
    invoke()
    keys = [
        f"health/hrv/2026/09/{DAY}.json",
        f"refresh/checks/v1/health/hrv/{DAY}.json",
        f"refresh/checks/v1/night/{DAY}.json",
    ]
    before = {key: client.objects[key].copy() for key in keys}
    source.responses["hrv", DAY] = {
        "hrvSummary": {"calendarDate": "2026-09-29", "lastNightAvg": "PRIVATE_RESPONSE_MARKER"},
        "hrvReadings": [],
    }
    source.calls.clear()
    client.reset()
    capsys.readouterr()

    report = invoke(run_id="19")

    expected = [{"stream": "hrv", "date": DAY, "category": "wrong_date"}]
    assert report.get("health_validation_rejections") == expected
    assert json.loads(invoke.diagnostics_file.read_text()) == report
    saved = json.loads(client.objects["refresh/diagnostics/v1/19.json"]["Body"])
    assert saved["health_validation_rejections"] == expected
    assert report["status"] == "partial" and len(source.calls) == 6
    assert all(client.objects[key] == value for key, value in before.items())
    assert not any(key in client.writes for key in keys)
    assert "PRIVATE_RESPONSE_MARKER" not in json.dumps(report)
    hosted = capsys.readouterr().out
    assert "recent-health-sync" in hosted and "refresh-diagnostics" in hosted
    assert all(value not in hosted for value in (
        DAY, "2026-09-29", "wrong_date", "health_validation_rejections", "PRIVATE_RESPONSE_MARKER",
    ))


def test_unknown_sleep_shape_is_not_a_verified_empty_response(entry):
    _, source, invoke = entry
    source.responses["sleep", DAY] = {"unexpected": "synthetic private marker"}
    report = invoke()
    sleep = next(item for item in report["source_checks"] if item["scope"] == "sleep")
    assert sleep["checked"] is False and sleep["checked_at"] is None
    assert report["health_validation_rejections"] == [
        {"stream": "sleep", "date": DAY, "category": "invalid_response"},
    ]


@pytest.mark.parametrize(
    "stream,case",
    [
        ("sleep", "unknown"),
        ("sleep", "partial"),
        ("sleep", "wrong_date"),
        ("sleep", "conflicting_date"),
        ("sleep", "invalid_date"),
        ("sleep", "compound_qualifier_object"),
        ("sleep", "compound_qualifier_list"),
        ("hrv", "unknown"),
        ("hrv", "partial"),
        ("hrv", "wrong_date"),
        ("hrv", "summary_without_date"),
    ],
)
def test_unverified_source_shapes_preserve_good_data_and_check_times(entry, stream, case):
    client, source, invoke = entry
    invoke()
    root = "health/sleep/v1" if stream == "sleep" else "health/hrv"
    keys = [
        f"{root}/2026/09/{DAY}.json",
        f"refresh/checks/v1/health/{stream}/{DAY}.json",
        f"refresh/checks/v1/night/{DAY}.json",
    ]
    before = {key: client.objects[key].copy() for key in keys}
    if case == "unknown":
        raw = {}
    elif case == "partial":
        raw = source._fetch(stream, DAY)
        if stream == "sleep":
            del raw["dailySleepDTO"]["sleepEndTimestampGMT"]
        else:
            del raw["hrvReadings"][0]["readingTimeGMT"]
    elif case == "summary_without_date":
        raw = {"hrvSummary": {"lastNightAvg": 50}}
    elif case == "conflicting_date":
        raw = source._fetch(stream, DAY)
        raw["calendarDate"] = "2026-09-29"
    elif case == "invalid_date":
        raw = source._fetch(stream, DAY)
        raw["dailySleepDTO"]["calendarDate"] = ""
    elif case.startswith("compound_qualifier_"):
        raw = source._fetch(stream, DAY)
        qualifier = {"private": "D4_SYNTHETIC_MARKER"} if case.endswith("object") else []
        raw["dailySleepDTO"]["sleepScores"] = {"future_score": {"value": 0, "qualifierKey": qualifier}}
    else:
        raw = source._fetch(stream, DAY)
        raw["dailySleepDTO" if stream == "sleep" else "hrvSummary"]["calendarDate"] = "2026-09-29"
    source.responses[stream, DAY] = raw
    source.calls.clear()
    client.reset()
    report = invoke()
    checked = next(v for v in report["source_checks"] if v["scope"] == stream)
    assert checked == {"scope": stream, "checked": False, "checked_at": None, "outcome": "failed"}
    assert len(source.calls) == 6 and report["status"] == "partial"
    assert all(client.objects[k] == v for k, v in before.items())
    assert all(k not in client.writes for k in keys)
    category = "wrong_date" if case in {"wrong_date", "conflicting_date", "invalid_date"} else "invalid_response"
    assert report["health_validation_rejections"] == [
        {"stream": stream, "date": DAY, "category": category},
    ]


@pytest.mark.parametrize(
    "stream,case", [("sleep", "empty"), ("sleep", "unconfirmed"), ("hrv", "empty")]
)
def test_validated_pending_response_is_a_successful_check_without_replacing_good_history(
    entry, stream, case
):
    client, source, invoke = entry
    invoke()
    root = "health/sleep/v1" if stream == "sleep" else "health/hrv"
    keys = [
        f"{root}/2026/09/{DAY}.json",
        f"refresh/checks/v1/health/{stream}/{DAY}.json",
        f"refresh/checks/v1/night/{DAY}.json",
    ]
    before = {k: client.objects[k].copy() for k in keys}
    if case == "unconfirmed":
        raw = source._fetch(stream, DAY)
        raw["dailySleepDTO"]["sleepWindowConfirmed"] = False
    elif stream == "sleep":
        raw = {"dailySleepDTO": {"calendarDate": DAY, "sleepTimeSeconds": 0}}
    else:
        raw = {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []}
    source.responses[stream, DAY] = raw
    source.calls.clear()
    client.reset()
    report = invoke()
    checked = next(v for v in report["source_checks"] if v["scope"] == stream)
    assert (
        checked["checked"] is True
        and checked["checked_at"] is not None
        and checked["outcome"] == "not_ready"
    )
    assert report["status"] == "partial" and len(source.calls) == 6
    assert all(client.objects[k] == v for k, v in before.items())
    assert report["health_validation_rejections"] == [
        {"stream": stream, "date": DAY, "category": "garmin_not_ready"},
    ]


def test_valid_unchanged_source_still_confirms_checks_and_preserves_canonical_bytes(entry):
    client, source, invoke = entry
    invoke()
    old = {k: v["Body"] for k, v in client.objects.items() if k.startswith("health/")}
    source.calls.clear()
    client.reset()
    report = invoke()
    assert report["status"] == "complete" and len(source.calls) == 6
    assert all(v["checked"] is True and v["outcome"] == "checked" for v in report["source_checks"])
    assert all(client.objects[k]["Body"] == v for k, v in old.items())
    assert all(k.startswith("refresh/checks/") for k in client.writes)
    assert report["health_validation_rejections"] == []


@pytest.mark.parametrize("kind", ["auth", "transport", "other"])
def test_source_failures_never_confirm_a_new_check_or_advance_failed_scope(entry, kind):
    client, source, invoke = entry
    invoke()
    keys = [
        f"health/sleep/v1/2026/09/{DAY}.json",
        f"refresh/checks/v1/health/sleep/{DAY}.json",
        f"refresh/checks/v1/night/{DAY}.json",
    ]
    before = {k: client.objects[k].copy() for k in keys}
    error = (
        TimeoutError("synthetic secret") if kind == "transport" else
        ValueError("wrong_date garmin_not_ready invalid_response synthetic secret")
    )
    if kind == "auth":
        error.response = SimpleNamespace(status_code=401)
    source.responses["sleep", DAY] = error
    source.calls.clear()
    client.reset()
    if kind == "other":
        report = invoke()
        assert next(v for v in report["source_checks"] if v["scope"] == "sleep")["checked"] is False
    else:
        with pytest.raises(RuntimeError):
            invoke()
        report = json.loads(invoke.diagnostics_file.read_text())
        assert all(
            v["checked"] is False and v["checked_at"] is None for v in report["source_checks"]
        )
    assert "synthetic secret" not in json.dumps(report)
    assert all(client.objects[k] == v for k, v in before.items())
    assert report.get("health_validation_rejections", []) == []


def test_validation_rejections_are_bounded_by_existing_overlap_and_streams():
    days = ["2026-09-30", "2026-09-29", "2026-09-28", "2026-09-27", "2026-09-26",
            "2026-09-25", "2026-09-24", "2026-09-23", "2026-09-22", "2026-09-21",
            "2026-09-20", "2026-09-19", "2026-09-18", "2026-09-17"]
    client, source = HealthClient(), SyntheticGarmin()
    for stream in ("sleep", "hrv"):
        for day in days:
            source.responses[stream, day] = {"unexpected": "PRIVATE_RESPONSE_MARKER"}

    report = recent_health.run(
        days=14, streams=("sleep", "hrv", "sleep"), today=date(2026, 9, 30),
        store=R2Store(client=client, bucket="synthetic-health"), garmin=source,
    )

    assert report["health_validation_rejections"] == [
        {"stream": stream, "date": day, "category": "invalid_response"}
        for stream in ("sleep", "hrv") for day in days
    ]
    assert len(report["health_validation_rejections"]) == len(source.calls) == 28
    assert all(value == 0 for value in client.counts.values())
    assert "PRIVATE_RESPONSE_MARKER" not in json.dumps(report)


def test_actual_r2_only_activity_repair_never_confirms_a_garmin_check(
    entry, tmp_path, record_property
):
    client, source, _ = entry
    client.objects["activities/2026/1/activity.v1.json"] = {
        "Body": json.dumps(
            {
                "activity": {"id": "1", "type": "running", "start_time_local": f"{DAY} 10:00:00"},
            }
        ).encode()
    }
    report = refresh.run(
        refresh.RefreshRequest(
            mode="activity", repair_only=True, activity_id="1", expected_date=DAY
        ),
        data_dir=str(tmp_path),
        store_factory=lambda: R2Store(client=client, bucket="synthetic-health"),
        login=lambda: pytest.fail("R2-only repair cannot login"),
    )
    assert source.calls == []
    assert [v["stage"] for v in report["stages"]] == ["latest_activity"]
    assert report["source_checks"] == [
        {"scope": "activity", "checked": False, "checked_at": None, "outcome": "not_checked"}
    ]
    assert not any(key.startswith("refresh/checks/") for key in client.writes)
    record_property(
        "C10_repair",
        json.dumps(
            {
                "source_checks": report["source_checks"],
                "source_calls": source.calls,
                "r2_counts": client.counts,
            }
        ),
    )
