"""Summary scope/cadence tests with actual adapters, writer, export and R2 guards."""
import csv
import json
from datetime import timedelta
from io import StringIO

import pytest
from botocore.exceptions import ClientError

from pipeline.r2_store import R2BudgetError, R2Store
from pipeline.refresh_summaries import checkpoint_key, run, select_windows
from scripts.benchmark_summary_windows import NOW, HealthClient, SummaryGarmin


@pytest.fixture
def environment(tmp_path):
    storage, sdk = HealthClient(), SummaryGarmin()
    def refresh(**kwargs):
        return run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-summary"),
                   client=sdk, now=kwargs.pop("now", NOW), request_pause=0, **kwargs)
    return storage, sdk, tmp_path, refresh


def check(storage, stream, wide=True):
    return json.loads(storage.objects[checkpoint_key(stream, wide)]["Body"])


def test_first_reconciles_repeat_is_recent_and_retains_original_summary_bytes(environment):
    storage, sdk, root, refresh = environment
    first = refresh()
    assert first["windows"]["health"]["calendar_days"] == 14
    assert first["windows"]["activities"]["calendar_days"] == 31
    assert first["health_diagnostics"]["sdk_calls"] == 61
    assert first["provider_diagnostics"]["garmin_api_calls"] == 66  # Five activity pages incl empty final page.
    originals = {p.name: p.read_bytes() for p in root.glob("*.csv")}
    old_checks = {s: check(storage, s) for s in ("health", "activities")}
    storage.reset()
    second = refresh(now=NOW + timedelta(minutes=1))
    assert second["status"] == "complete" and second["checkpoints_written"] == 2
    assert second["health_diagnostics"]["sdk_calls"] == 17
    assert second["provider_diagnostics"]["garmin_api_calls"] == 19
    assert second["windows"]["activities"]["calendar_days"] == 8
    assert second["windows"]["health"]["calendar_days"] == 3
    assert storage.counts["get"] == 3 and storage.counts["head"] == 5 and storage.counts["list"] == 1
    assert storage.writes == [checkpoint_key(s, False) for s in ("activities", "health")]
    assert all(p.read_bytes() == originals[p.name] for p in root.glob("*.csv"))
    assert all(check(storage, s) == old_checks[s] for s in old_checks)


@pytest.mark.parametrize("hours,mode", [(23.99, "recent"), (24, "reconciliation"), (48, "reconciliation")])
def test_cadence_and_utc_year_boundary(environment, hours, mode):
    storage, _, root, refresh = environment
    refresh(now=NOW.replace(month=12, day=31))
    windows = select_windows(R2Store(client=storage, bucket="synthetic-summary"), root,
                             NOW.replace(month=12, day=31) + timedelta(hours=hours))
    assert all(w["mode"] == mode for w in windows.values())


@pytest.mark.parametrize("defect", ["missing", "json", "kind", "schema", "stream", "status", "scope",
                                    "future", "naive", "hash", "start", "end", "short_window", "old_end"])
def test_invalid_reconciliation_checkpoint_is_due_only_for_affected_scope(environment, defect):
    storage, _, _, refresh = environment
    refresh()
    key = checkpoint_key("health")
    payload = check(storage, "health")
    if defect == "missing":
        del storage.objects[key]
    elif defect == "json":
        storage.objects[key]["Body"] = b"{broken"
    else:
        field, value = {"kind": ("kind", "wrong"), "schema": ("schema_version", 2),
            "stream": ("stream", "activities"), "status": ("status", "partial"), "scope": ("scope", "recent"),
            "future": ("checked_at", (NOW + timedelta(hours=1)).isoformat()),
            "naive": ("checked_at", NOW.replace(tzinfo=None).isoformat()), "hash": ("summary_sha256", None),
            "start": ("start_date", "invalid"), "end": ("end_date", "invalid"),
            "short_window": ("start_date", NOW.date().isoformat()),
            "old_end": ("end_date", (NOW - timedelta(days=3)).date().isoformat())}[defect]
        payload[field] = value
        storage.objects[key]["Body"] = json.dumps(payload).encode()
    result = refresh(now=NOW + timedelta(minutes=1))
    assert result["windows"]["health"]["mode"] == "reconciliation"
    assert result["windows"]["activities"]["mode"] == "recent"


@pytest.mark.parametrize("filename", ["activities.csv", "health_daily.csv"])
def test_missing_local_dataset_forces_only_its_wide_window(environment, filename):
    _, _, root, refresh = environment
    refresh()
    (root / filename).unlink()
    result = refresh()
    assert result["windows"]["activities" if filename == "activities.csv" else "health"]["mode"] == "reconciliation"


def test_force_recovery_and_older_edits_are_picked_up_only_by_due_reconciliation(environment):
    _, sdk, root, refresh = environment
    refresh()
    sdk.edits[(NOW - timedelta(days=10)).date().isoformat()] = 1234
    refresh(now=NOW + timedelta(minutes=1))
    def value():
        rows = list(csv.DictReader(StringIO((root / "health_daily.csv").read_text())))
        return next(row["Steps"] for row in rows if row["Date"] == (NOW - timedelta(days=10)).date().isoformat())
    assert value() == "1000"
    result = refresh(now=NOW + timedelta(minutes=2), force_reconcile=True)
    assert result["windows"]["health"]["mode"] == "reconciliation" and value() == "1234"


@pytest.mark.parametrize("failure", [ValueError("PRIVATE_MESSAGE"), None, "bad shape"])
def test_partial_health_preserves_wide_check_and_values_then_retries(environment, failure, capsys):
    storage, sdk, root, refresh = environment
    refresh()
    before = check(storage, "health")
    original = list(csv.DictReader(StringIO((root / "health_daily.csv").read_text())))
    sdk.failures["stats"] = failure
    capsys.readouterr()
    result = refresh(now=NOW + timedelta(hours=24))
    assert result["status"] == "partial" and result["windows"]["health"]["source_checked"] is False
    assert check(storage, "health") == before
    after = {row["Date"]: row for row in csv.DictReader(StringIO((root / "health_daily.csv").read_text()))}
    assert all(after[row["Date"]] == row for row in original)
    assert "PRIVATE_MESSAGE" not in capsys.readouterr().err
    sdk.failures.clear()
    assert refresh(now=NOW + timedelta(hours=24, minutes=1))["windows"]["health"]["mode"] == "reconciliation"


def test_missing_health_day_preserves_positive_check(environment):
    storage, sdk, _, refresh = environment
    refresh()
    before = check(storage, "health")
    sdk.empty_day = NOW.date().isoformat()
    assert refresh(now=NOW + timedelta(hours=24))["status"] == "partial"
    assert check(storage, "health") == before


def test_activity_request_failure_publishes_no_receipts(environment):
    storage, sdk, _, refresh = environment
    refresh()
    before = {k: v["Body"] for k, v in storage.objects.items() if k.startswith("refresh/")}
    sdk.failures[sdk.garmin_connect_activities] = TimeoutError("PRIVATE_MESSAGE")
    with pytest.raises(TimeoutError):
        refresh(now=NOW + timedelta(hours=24))
    assert all(storage.objects[k]["Body"] == value for k, value in before.items())


@pytest.mark.parametrize("phase", ["get", "head", "checkpoint"])
def test_storage_errors_never_advance_failed_receipts(environment, monkeypatch, phase):
    storage, _, _, refresh = environment
    refresh()
    before = {k: v["Body"] for k, v in storage.objects.items() if k.startswith("refresh/")}
    method = {"get": "get_object", "head": "head_object", "checkpoint": "put_object"}[phase]
    original = getattr(storage, method)
    def fail(**kwargs):
        if phase != "checkpoint" or kwargs["Key"].startswith("refresh/"):
            raise ClientError({"Error": {"Code": "403"}}, "Synthetic storage")
        return original(**kwargs)
    monkeypatch.setattr(storage, method, fail)
    with pytest.raises(ClientError):
        refresh(now=NOW + timedelta(hours=24))
    assert all(storage.objects[k]["Body"] == value for k, value in before.items())


def test_budget_and_corrupt_local_summary_fail_without_new_checks(environment, monkeypatch):
    storage, sdk, root, refresh = environment
    refresh()
    before = {k: v["Body"] for k, v in storage.objects.items() if k.startswith("refresh/")}
    monkeypatch.setenv("R2_MAX_WRITES_PER_RUN", "1")
    with pytest.raises(R2BudgetError):
        refresh(now=NOW + timedelta(minutes=1))
    # The successful activity scope may have advanced before a later budget
    # failure; the failed health scope must retain its previous positive check.
    assert storage.objects[checkpoint_key("health", False)]["Body"] == before[checkpoint_key("health", False)]
    sdk.calls.clear()
    (root / "health_daily.csv").write_text("Invalid,header\n")
    with pytest.raises(ValueError, match="header"):
        refresh()
    assert sdk.calls == []


def test_no_plan_reads_or_historical_writes(environment):
    storage, _, _, refresh = environment
    storage.put_object(Key="backfill/private-plan.json", Body=b"preserve")
    refresh()
    assert storage.objects["backfill/private-plan.json"]["Body"] == b"preserve"
    assert all(not key.startswith("backfill/") for key in storage.reads)
    assert storage.prefixes == [""]  # Required bucket-budget inventory only.


def test_due_reconciliation_detects_edit_outside_recent_window(environment):
    _, sdk, root, refresh = environment
    refresh()
    older_day = (NOW - timedelta(days=10)).date().isoformat()
    sdk.edits[older_day] = 1200
    result = refresh(now=NOW + timedelta(hours=24))
    assert result["windows"]["health"]["mode"] == "reconciliation"
    rows = {row["Date"]: row for row in csv.DictReader(StringIO((root / "health_daily.csv").read_text()))}
    assert rows[older_day]["Steps"] == "1200"


def test_missing_local_file_restores_all_old_r2_rows_before_reconciliation(environment):
    storage, _, root, refresh = environment
    refresh()
    # An old row beyond either ordinary window must survive local file loss.
    path = root / "health_daily.csv"
    with path.open("a", newline="") as handle:
        csv.writer(handle).writerow(["2020-01-01", "100"] + [""] * 24 + ["garmin"])
    from pipeline.summary_export import run as export
    export(str(root), store=R2Store(client=storage, bucket="synthetic-summary"))
    path.unlink()
    refresh()
    assert "2020-01-01,100," in path.read_text()


def test_existing_unmanifested_r2_history_is_never_overwritten(environment):
    storage, sdk, _, refresh = environment
    storage.put_object(Key="summary/health_daily.csv", Body=b"Date,Source\n2020-01-01,garmin\n")
    before = storage.objects["summary/health_daily.csv"]["Body"]
    with pytest.raises(ValueError, match="manifest"):
        refresh()
    assert sdk.calls == [] and storage.objects["summary/health_daily.csv"]["Body"] == before


@pytest.mark.parametrize("shape", [None, "invalid", []])
def test_corrupt_health_source_shape_does_not_advance_scope(environment, shape):
    storage, sdk, _, refresh = environment
    refresh()
    before = check(storage, "health", False)
    sdk.failures["weight-range"] = shape
    result = refresh(now=NOW + timedelta(minutes=1))
    assert result["status"] == "partial"
    assert result["health_diagnostics"]["source_errors"] == 1
    assert check(storage, "health", False) == before


def test_storage_export_failure_leaves_all_old_checks_and_repeat_repairs(environment, monkeypatch):
    storage, sdk, _, refresh = environment
    refresh()
    before = {k: v["Body"] for k, v in storage.objects.items() if k.startswith("refresh/")}
    sdk.edits[NOW.date().isoformat()] = 1234
    original = storage.put_object
    def fail(**kwargs):
        if kwargs["Key"] == "summary/manifest.json":
            raise ClientError({"Error": {"Code": "SlowDown"}}, "PutObject")
        return original(**kwargs)
    monkeypatch.setattr(storage, "put_object", fail)
    with pytest.raises(ClientError):
        refresh(now=NOW + timedelta(minutes=1))
    assert all(storage.objects[k]["Body"] == value for k, value in before.items())
    monkeypatch.setattr(storage, "put_object", original)
    assert refresh(now=NOW + timedelta(minutes=2))["status"] == "complete"


def test_malformed_activity_does_not_advance_check_or_replace_existing_row(environment, monkeypatch):
    storage, sdk, root, refresh = environment
    refresh()
    before = check(storage, "activities", False)
    original = (root / "activities.csv").read_bytes()
    monkeypatch.setattr(sdk, "get_activities_by_date", lambda *args: [{"activityId": None, "startTimeGMT": "invalid"}])
    assert refresh(now=NOW + timedelta(minutes=1))["status"] == "partial"
    assert check(storage, "activities", False) == before
    assert (root / "activities.csv").read_bytes() == original


def test_valid_empty_activity_window_is_checked_without_deleting_history(environment, monkeypatch):
    storage, sdk, root, refresh = environment
    refresh()
    original = (root / "activities.csv").read_bytes()
    monkeypatch.setattr(sdk, "get_activities_by_date", lambda *args: [])
    result = refresh(now=NOW + timedelta(minutes=1))
    assert result["windows"]["activities"]["source_checked"] is True
    assert (root / "activities.csv").read_bytes() == original
    assert check(storage, "activities", False)["scope"] == "recent"


def test_wrong_date_health_rows_are_excluded_and_not_checkpointed(environment, monkeypatch):
    storage, sdk, _, refresh = environment
    refresh()
    before = check(storage, "health", False)
    monkeypatch.setattr(sdk, "get_sleep_daily", lambda *args: [{"calendarDate": "2020-01-01", "sleepTimeSeconds": 1}])
    assert refresh(now=NOW + timedelta(minutes=1))["status"] == "partial"
    assert check(storage, "health", False) == before


def test_rate_limit_retry_counts_attempts_but_final_success_is_checked(environment, monkeypatch):
    _, sdk, _, refresh = environment
    original = sdk.get_weigh_ins
    attempts = 0
    def limited(*args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("429 PRIVATE_MESSAGE")
        return original(*args)
    monkeypatch.setattr(sdk, "get_weigh_ins", limited)
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda *args: None)
    result = refresh()
    assert result["status"] == "complete" and result["health_diagnostics"]["sdk_calls"] == 62


def test_diagnostics_restore_connectapi_after_failure(environment):
    _, sdk, _, refresh = environment
    original = sdk.connectapi
    sdk.failures[sdk.garmin_connect_activities] = TimeoutError()
    with pytest.raises(TimeoutError):
        refresh()
    assert sdk.connectapi == original


@pytest.mark.parametrize("identifier", [None, True, 0, -1, "invalid"])
def test_invalid_activity_id_does_not_enter_summary_or_advance_check(environment, monkeypatch, identifier):
    storage, sdk, root, refresh = environment
    refresh()
    original = (root / "activities.csv").read_bytes()
    before = check(storage, "activities", False)
    monkeypatch.setattr(sdk, "get_activities_by_date", lambda *args: [{"activityId": identifier,
                        "startTimeGMT": f"{NOW.date()} 10:00:00"}])
    assert refresh(now=NOW + timedelta(minutes=1))["status"] == "partial"
    assert (root / "activities.csv").read_bytes() == original
    assert check(storage, "activities", False) == before


def test_wrong_requested_daily_date_preserves_metrics_and_receipts(environment, monkeypatch):
    storage, sdk, root, refresh = environment
    refresh()
    original = {row["Date"]: row for row in csv.DictReader(StringIO((root / "health_daily.csv").read_text()))}
    before = check(storage, "health", False)
    monkeypatch.setattr(sdk, "get_stats", lambda *args: {"calendarDate": "2026-09-29", "totalSteps": 1})
    result = refresh(now=NOW + timedelta(minutes=1))
    assert result["status"] == "partial" and result["health_diagnostics"]["invalid_dates"] == 2
    after = {row["Date"]: row for row in csv.DictReader(StringIO((root / "health_daily.csv").read_text()))}
    assert all(after[day] == row for day, row in original.items() if day != "2026-09-29")
    assert after["2026-09-29"]["Steps"] == "1"  # The one correctly dated source update is valid.
    assert check(storage, "health", False) == before
