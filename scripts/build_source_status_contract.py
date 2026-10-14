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
from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin  # noqa: E402
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
    run_id = str(number)
    request_id = "12345678-1234-4234-8234-1234567890" + run_id
    active_run = "0"
    with TemporaryDirectory(prefix="synthetic-source-status-") as directory, ExitStack() as stack:
        root = Path(directory)
        stack.enter_context(redirect_stdout(io.StringIO()))
        stack.enter_context(redirect_stderr(io.StringIO()))
        for module in (freshness, health_sync, refresh):
            stack.enter_context(patch.object(module, "datetime", Instant))
        for module in (freshness, recent_health):
            stack.enter_context(patch.object(module, "date", Clock))
        stack.enter_context(patch.object(refresh.summary_restore, "restore_with_base",
                                        return_value=SimpleNamespace(base=None)))

        def summaries(*args, **kwargs):
            (root / "health_daily.csv").write_text("Date,Weight KG\n", encoding="utf-8")
            return {"status": "complete"}

        def activity_report(*args, **kwargs):
            report = {"schema_version": 1, "updated_at": Instant.now().isoformat(),
                      "new_activity_count": 0, "checked_activity_count": 0,
                      "remaining_candidate_count": 0, "activities": []}
            if active_run != "0":
                kwargs["store"].put(f"refresh/reports/{active_run}.json", json_bytes(report), "application/json")
            return report

        stack.enter_context(patch.object(refresh.refresh_summaries, "run", summaries))
        stack.enter_context(patch.object(refresh.recent_body, "run", return_value={"status": "complete"}))
        stack.enter_context(patch.object(refresh.manual_activity_refresh, "snapshot", return_value={}))
        stack.enter_context(patch.object(refresh.manual_activity_refresh, "run", activity_report))
        Instant.hour, Instant.minute = 11, 50
        refresh.run(refresh.RefreshRequest(include_granular=True), data_dir=directory,
                    store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=lambda: garmin)
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
        active_run = run_id
        Instant.hour, Instant.minute = 12, 0
        garmin.calls.clear()
        client.reset()
        request = refresh.RefreshRequest(mode=mode, run_id=run_id, request_id=request_id,
            include_granular=mode == "general", new_activity_expected=False,
            wake_date=DAY if mode == "night" else None)
        began = perf_counter()
        diagnostic = refresh.run(request, data_dir=directory,
            store_factory=lambda: R2Store(client=client, bucket="synthetic"), login=lambda: garmin,
            workflow_context={**SOURCE, "GITHUB_RUN_ID": run_id})
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
    return {"mode": mode, "outcome": outcome, "run_id": number, "request_id": request_id,
            "diagnostic": diagnostic, "objects": objects, "garmin_calls": len(garmin.calls),
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
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"producer_sha256": {module.__name__: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        for module in (refresh, health_sync, recent_health)}, "cases": cases}, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
