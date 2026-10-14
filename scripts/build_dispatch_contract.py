"""Synthetic actual startup producer fixture; no external services."""
from __future__ import annotations

import hashlib
import io
import json
import sys
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import freshness, refresh  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from scripts.benchmark_summary_reuse import SummaryClient  # noqa: E402

REQUEST_ID = "12345678-1234-4234-8234-123456789012"
SOURCE = {"GITHUB_REPOSITORY": "synthetic/slipstream",
          "GITHUB_WORKFLOW_REF": "synthetic/slipstream/.github/workflows/refresh.yml@refs/heads/main",
          "GITHUB_REF": "refs/heads/main", "GITHUB_SHA": "a" * 40,
          "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_RUN_ID": "55"}


def build_general():
    client = SummaryClient()
    client.objects["synthetic/history-marker.json"] = {"Body": b"original history"}
    store = R2Store(client=client, bucket="synthetic")
    request = refresh.RefreshRequest(run_id="55", request_id=REQUEST_ID,
                                     include_granular=True, new_activity_expected=False)
    with TemporaryDirectory(prefix="synthetic-dispatch-") as directory, ExitStack() as stack:
        root = Path(directory)
        (root / "health_daily.csv").write_text("Date,Source\n")
        stack.enter_context(redirect_stdout(io.StringIO()))
        stack.enter_context(redirect_stderr(io.StringIO()))
        stack.enter_context(patch.object(refresh.summary_restore, "restore_with_base",
                                        return_value=SimpleNamespace(base=None)))
        for module, method in [(refresh.refresh_summaries, "run"), (refresh.recent_health, "run"),
                               (refresh.recent_body, "run"), (refresh.manual_activity_refresh, "run"),
                               (refresh.manual_activity_refresh, "snapshot")]:
            stack.enter_context(patch.object(module, method, return_value={}))
        diagnostics = refresh.run(request, data_dir=str(root), store_factory=lambda: store,
                                  login=lambda: SimpleNamespace(), workflow_context=SOURCE)
        stored = client.objects.get(f"refresh/requests/{REQUEST_ID}.json")
        receipt = json.loads(stored["Body"]) if stored is not None else None
    assert client.objects["synthetic/history-marker.json"]["Body"] == b"original history"
    return {"request_id": REQUEST_ID, "receipt": receipt, "diagnostics": diagnostics,
            "r2_operations": client.counts, "garmin_calls": 0, "original_bytes_preserved": True,
            "original_sha256": {"synthetic/history-marker.json": hashlib.sha256(b"original history").hexdigest()}}


class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2026, 10, 14)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 10, 14, 12, tzinfo=timezone.utc)
        return value if tz is None else value.astimezone(tz)


class EmptyGarmin:
    def __init__(self):
        self.calls = []

    def get_sleep_data(self, day):
        self.calls.append("sleep")
        return {"dailySleepDTO": {"calendarDate": day, "sleepTimeSeconds": 0}}

    def get_hrv_data(self, day):
        self.calls.append("hrv")
        return {"hrvSummary": {"calendarDate": day}, "hrvReadings": []}


def build_targeted(kind, *, repair=False):
    run_id = "56" if kind == "night" else "58" if repair else "57"
    request_id = "12345678-1234-4234-8234-1234567890" + run_id
    client, garmin = SummaryClient(), EmptyGarmin()
    # Unrelated history and original canonical bytes must survive metadata work.
    client.objects["synthetic/history-marker.json"] = {"Body": b"original history"}
    client.objects["activities/2026/1/coach-input/v1/canonical/original.json"] = {
        "Body": b'{"schema_version":1,"analysis_id":"synthetic-original","activity_id":"1"}'}
    client.objects["refresh/requests/22345678-1234-4234-8234-123456789012.json"] = {
        "Body": b'{"run_id":1,"scope":"night/2000-01-01"}'}
    if repair:
        client.objects["activities/2026/1/activity.v1.json"] = {"Body": json.dumps({
            "activity": {"id": "1", "start_time_local": "2026-10-14 10:00:00", "type": "running"}}).encode()}
    originals = {key: value["Body"] for key, value in client.objects.items()}
    store = R2Store(client=client, bucket="synthetic")
    request = refresh.RefreshRequest(mode=kind, repair_only=repair, run_id=run_id, request_id=request_id,
        wake_date="2026-10-14" if kind == "night" else None,
        expected_date="2026-10-14" if kind == "activity" else None,
        activity_id="1" if kind == "activity" else None, new_activity_expected=False)
    with TemporaryDirectory(prefix="synthetic-dispatch-") as directory, ExitStack() as stack:
        stack.enter_context(redirect_stdout(io.StringIO()))
        stack.enter_context(redirect_stderr(io.StringIO()))
        stack.enter_context(patch.object(freshness, "date", FixedDate))
        stack.enter_context(patch.object(freshness, "datetime", FixedDateTime))
        stack.enter_context(patch.object(refresh, "datetime", FixedDateTime))
        stack.enter_context(patch.object(refresh.summary_restore, "restore_with_base", return_value=SimpleNamespace(base=None)))
        stack.enter_context(patch.object(refresh.latest_activity, "prepare_normal_base", return_value=None))
        fetcher = stack.enter_context(patch.object(refresh.latest_activity.garmin_source, "fetch", return_value=[]))
        stack.enter_context(patch.object(refresh.latest_activity, "run_coach_one", return_value={}))
        diagnostics = refresh.run(request, data_dir=directory, store_factory=lambda: store,
            login=lambda: garmin, workflow_context={**SOURCE, "GITHUB_RUN_ID": run_id})
    assert all(client.objects[key]["Body"] == body for key, body in originals.items())
    return {"request_id": request_id, "run_id": int(run_id),
            "receipt": json.loads(client.objects[f"refresh/requests/{request_id}.json"]["Body"]),
            "report": json.loads(client.objects[f"refresh/reports/{run_id}.json"]["Body"]),
            "diagnostics": diagnostics, "r2_operations": client.counts, "garmin_calls": len(garmin.calls) + fetcher.call_count,
            "original_bytes_preserved": True,
            "original_sha256": {key: hashlib.sha256(body).hexdigest() for key, body in originals.items()}}


def main():
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"general": build_general(), "night": build_targeted("night"),
                                  "activity": build_targeted("activity"), "repair": build_targeted("activity", repair=True)}, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
