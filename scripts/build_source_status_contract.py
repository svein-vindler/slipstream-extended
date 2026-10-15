"""Actual synthetic health producer/diagnostics for MCP source-status contracts."""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import sys
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import freshness, health_sync, recent_health, refresh  # noqa: E402
from pipeline.granular import json_bytes  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from scripts.benchmark_body_reuse import SyntheticBodyGarmin  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin  # noqa: E402
from scripts.benchmark_summary_windows import SummaryGarmin  # noqa: E402
from scripts.build_dispatch_contract import SOURCE  # noqa: E402

DAY = "2026-10-14"


class Clock(date):
    @classmethod
    def today(cls):
        return cls.fromisoformat(DAY)


class Instant(datetime):
    hour, minute = 11, 50

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 14, cls.hour, cls.minute, tzinfo=timezone.utc)


def build_case(mode, outcome, number):
    client, garmin = HealthClient(), SyntheticGarmin()
    night_source = garmin
    summary_case = outcome.startswith("summary_") or outcome in {"full_valid", "fatal_activity_transport"}
    if summary_case:
        night_source, garmin = garmin, SummaryGarmin()
        garmin.failures["synthetic-activities"] = []
        garmin.get_sleep_data = night_source.get_sleep_data
        garmin.get_hrv_data = night_source.get_hrv_data
    body_enabled = outcome.startswith("body_") or outcome in {"both_errors", "mixed_pending", "fatal_body_transport", "full_valid"}
    fatal = outcome.startswith("fatal_") or outcome == "summary_transport"
    body_source = SyntheticBodyGarmin()
    if outcome == "body_valid":
        raw = body_source.get_daily_weigh_ins(DAY)
        raw["dateWeightList"][0]["bodyFat"] = 0
        raw["dateWeightList"][1].pop("bodyFat")
        body_source.responses[DAY] = raw
    garmin.get_daily_weigh_ins = body_source.get_daily_weigh_ins
    actual_body = refresh.recent_body.run
    actual_summary = refresh.refresh_summaries.run
    actual_manual = refresh.manual_activity_refresh.run
    actual_snapshot = refresh.manual_activity_refresh.snapshot
    if summary_case:
        garmin.get_weigh_ins = lambda *_: body_source.get_daily_weigh_ins(DAY)
    run_id = str(number)
    request_id = "12345678-1234-4234-8234-1234567890" + run_id
    active_run = "0"
    with TemporaryDirectory(prefix="synthetic-source-status-") as directory, ExitStack() as stack:
        root = Path(directory)
        (root / "activities.csv").write_text("Activity ID,Activity Date,Activity Name,Activity Type\n", encoding="utf-8")
        stack.enter_context(redirect_stdout(io.StringIO()))
        stack.enter_context(redirect_stderr(io.StringIO()))
        for module in (freshness, health_sync, refresh, refresh.refresh_summaries):
            stack.enter_context(patch.object(module, "datetime", Instant))
        for module in (freshness, recent_health):
            stack.enter_context(patch.object(module, "date", Clock))
        stack.enter_context(patch.object(refresh.summary_restore, "restore_with_base",
                                        return_value=SimpleNamespace(base=None)))

        def summaries(*args, **kwargs):
            if summary_case:
                return actual_summary(*args, **kwargs, now=Instant.now(timezone.utc), request_pause=0)
            if outcome == "fatal_manual_transport" and active_run != "0":
                (root / "activities.csv").write_text("Activity ID,Activity Date,Activity Name,Activity Type\n"
                    + f"garmin-1,{DAY} 10:00:00,Synthetic,Running\n", encoding="utf-8")
            (root / "health_daily.csv").write_text("Date,Weight KG\n" + (DAY + ",80\n" if body_enabled else ""), encoding="utf-8")
            return {"status": "complete"}

        def activity_report(*args, **kwargs):
            report = {"schema_version": 1, "updated_at": Instant.now().isoformat(),
                      "new_activity_count": 0, "checked_activity_count": 0,
                      "remaining_candidate_count": 0, "activities": []}
            if active_run != "0":
                kwargs["store"].put(f"refresh/reports/{active_run}.json", json_bytes(report), "application/json")
            return report

        stack.enter_context(patch.object(refresh.refresh_summaries, "run", summaries))
        stack.enter_context(patch.object(refresh.recent_body, "run", side_effect=lambda **kwargs:
            actual_body(**kwargs) if active_run != "0" and body_enabled else {"status": "complete"}))
        stack.enter_context(patch.object(refresh.manual_activity_refresh, "snapshot", side_effect=lambda *args:
            actual_snapshot(*args) if outcome in {"fatal_manual_transport", "full_valid"} else {}))
        stack.enter_context(patch.object(refresh.manual_activity_refresh, "run", side_effect=lambda **kwargs:
            actual_manual(**kwargs) if active_run != "0" and outcome in {"fatal_manual_transport", "full_valid"} else activity_report(**kwargs)))
        Instant.hour, Instant.minute = 11, 50
        refresh.run(refresh.RefreshRequest(include_granular=True), data_dir=directory,
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=lambda: garmin)
        if body_enabled:
            seed = R2Store(client=client, bucket="synthetic")
            record = health_sync.store_day(seed, "body_composition", DAY, body_source.get_daily_weigh_ins(DAY))
            health_sync.finalize_days(seed, "body_composition", [record])
        originals = {k: v["Body"] for k, v in client.objects.items()
                     if k.startswith("health/") or k.startswith("refresh/checks/")}
        if outcome == "wrong_date":
            garmin.responses["hrv", DAY] = {"hrvSummary": {"calendarDate": "2026-10-13"}, "hrvReadings": []}
        elif outcome == "invalid_response":
            garmin.responses["sleep", DAY] = {"unexpected": "SYNTHETIC_PROVIDER_TEXT"}
        elif outcome == "source_error":
            garmin.responses["sleep", DAY] = ValueError("SYNTHETIC_PROVIDER_TEXT")
        elif outcome == "pending":
            garmin.responses["hrv", DAY] = {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []}
        if outcome in {"body_wrong_date", "both_errors"}:
            body_source.responses[DAY] = {"calendarDate": "2026-10-13", "dateWeightList": []}
        elif outcome in {"body_invalid_response", "body_unknown"}:
            body_source.responses[DAY] = {"unexpected": "SYNTHETIC_PROVIDER_TEXT"}
        elif outcome in {"body_missing_weight", "body_missing_time"}:
            sample = body_source.get_daily_weigh_ins(DAY)
            for row in sample["dateWeightList"]:
                for key in (["weight"] if outcome == "body_missing_weight" else ["timestampGMT", "timestampLocal"]):
                    row.pop(key)
            body_source.responses[DAY] = sample
        elif outcome in {"body_pending", "mixed_pending"}:
            body_source.responses[DAY] = {"calendarDate": DAY, "dateWeightList": []}
        if outcome == "both_errors":
            garmin.responses["sleep", DAY] = {"unexpected": "SYNTHETIC_PROVIDER_TEXT"}
        if outcome == "mixed_pending":
            garmin.responses["hrv", DAY] = {"hrvSummary": {"calendarDate": DAY}, "hrvReadings": []}
        error = TimeoutError("SYNTHETIC_PROVIDER_TEXT")
        if outcome in {"fatal_health_auth", "fatal_night_auth", "fatal_login"}:
            error = ValueError("SYNTHETIC_PROVIDER_TEXT")
            error.response = SimpleNamespace(status_code=401)
        if outcome in {"fatal_health_auth", "fatal_night_auth", "fatal_night_transport"}:
            garmin.responses["sleep", DAY] = error
        if outcome == "fatal_body_transport":
            body_source.responses[DAY] = error
        if outcome == "fatal_manual_transport":
            def get_activity(_):
                raise error
            garmin.get_activity = get_activity
        if outcome == "fatal_activity_transport":
            def get_activities_by_date(*_args, **_kwargs):
                raise error
            garmin.get_activities_by_date = get_activities_by_date
        if outcome == "summary_schema":
            garmin.failures["stats"] = "SYNTHETIC_PROVIDER_TEXT"
        if outcome == "summary_transport":
            garmin.failures["synthetic-activities"] = error
        def login():
            if outcome == "fatal_login":
                raise error
            return garmin
        active_run = run_id
        body_source.calls.clear()
        Instant.hour, Instant.minute = 12, 0
        garmin.calls.clear()
        night_source.calls.clear()
        client.reset()
        request = refresh.RefreshRequest(mode=mode, run_id=run_id, request_id=request_id,
            include_granular=mode == "general", new_activity_expected=False,
            wake_date=DAY if mode == "night" else None,
            expected_date=DAY if mode == "activity" else None, activity_id="1" if mode == "activity" else None)
        began = perf_counter()
        diagnostic_path = root / "diagnostics.json"
        raised = False
        try:
            diagnostic = refresh.run(request, data_dir=directory,
                store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=login,
                diagnostics_file=diagnostic_path, workflow_context={**SOURCE, "GITHUB_RUN_ID": run_id})
        except Exception as exc:
            if not fatal:
                raise
            assert exc is error or exc.__cause__ is error
            raised = True
            diagnostic = json.loads(diagnostic_path.read_text(encoding="utf-8"))
        assert raised == fatal
        elapsed_ms = (perf_counter() - began) * 1000
    canonical = {k: b for k, b in originals.items() if k.startswith("health/")}
    assert all(client.objects[k]["Body"] == b for k, b in canonical.items())
    # Export synthetic JSON only. Exact original stored bytes are attested separately.
    objects = {}
    for key, value in client.objects.items():
        if key.endswith(f"/{DAY}.json") or key in (
            f"refresh/requests/{request_id}.json", f"refresh/reports/{run_id}.json",
            f"refresh/diagnostics/v1/{run_id}.json"):
            body = value["Body"]
            objects[key] = json.loads(gzip.decompress(body) if body.startswith(b"\x1f\x8b") else body)
    return {"mode": mode, "outcome": outcome, "fatal": fatal, "job_conclusion": "failure" if fatal else "success", "run_id": number, "request_id": request_id,
            "diagnostic": diagnostic, "objects": objects, "garmin_calls": len(garmin.calls) + len(body_source.calls) + (len(night_source.calls) if summary_case else 0),
            "r2_operations": client.counts, "elapsed_ms": elapsed_ms, "canonical_bytes_preserved": True,
            "checkpoint_bytes_preserved": {k: client.objects[k]["Body"] == b
                                         for k, b in originals.items() if k.startswith("refresh/checks/")},
            "canonical_sha256": {k: hashlib.sha256(b).hexdigest() for k, b in canonical.items()
                                 if not k.startswith("health/indexes/")},
            "index_sha256": {k: hashlib.sha256(b).hexdigest() for k, b in canonical.items()
                             if k.startswith("health/indexes/")}}


def main():
    cases = [build_case("general", kind, 55 + i) for i, kind in enumerate(
        ["wrong_date", "invalid_response", "source_error", "pending", "valid"])]
    cases += [build_case("night", kind, 60 + i) for i, kind in enumerate(
        ["invalid_response", "pending", "valid"])]
    cases += [build_case("general", kind, 63 + i) for i, kind in enumerate(
        ["body_wrong_date", "body_invalid_response", "body_unknown", "body_missing_weight", "body_missing_time", "body_pending", "body_valid"])]
    cases += [build_case("general", kind, 70 + i) for i, kind in enumerate(
        ["fatal_login", "fatal_health_auth", "fatal_body_transport", "fatal_manual_transport",
         "summary_transport", "summary_schema", "full_valid", "both_errors", "mixed_pending"])]
    cases += [build_case(mode, kind, 79 + i) for i, (mode, kind) in enumerate(
        [("night", "fatal_night_transport"), ("night", "fatal_night_auth"), ("activity", "fatal_activity_transport")])]
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"producer_sha256": {module.__name__: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        for module in (refresh, health_sync, recent_health, refresh.recent_body, refresh.latest_night,
                       refresh.latest_activity, refresh.refresh_summaries, refresh.manual_activity_refresh)}, "cases": cases}, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
