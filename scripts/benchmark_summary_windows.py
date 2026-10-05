"""Synthetic source-to-summary measurement using the real SDK pagination routine."""
from __future__ import annotations

import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garminconnect import Garmin  # noqa: E402

from pipeline.fetch import run as legacy  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.refresh_summaries import provider_diagnostics, run  # noqa: E402
from pipeline.sources import garmin, garmin_health  # noqa: E402
from pipeline.summary_export import run as export  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient  # noqa: E402

NOW = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)


class SummaryGarmin:
    get_activities_by_date = Garmin.get_activities_by_date
    garmin_connect_activities = "synthetic-activities"

    def __init__(self):
        self.edits = {}
        self.failures = {}
        self.empty_day = None
        self.calls = []

    def connectapi(self, path, params=None):
        self.calls.append((path, params))
        if path in self.failures:
            failure = self.failures[path]
            if isinstance(failure, Exception):
                raise failure
            return failure
        if path == self.garmin_connect_activities:
            first = datetime.fromisoformat(params["startDate"]).date()
            last = datetime.fromisoformat(params["endDate"]).date()
            rows = []
            for offset in range((last - first).days + 1):
                day = (last - timedelta(days=offset)).isoformat()
                for index in range(2):
                    rows.append({"activityId": index + int(day.replace("-", "")) * 10,
                                 "startTimeGMT": f"{day} 10:00:00", "startTimeLocal": f"{day} 12:00:00",
                                 "activityType": {"typeKey": "running"}, "distance": 5000,
                                 "duration": 1800, "activityName": self.edits.get(day, "Synthetic run")})
            start = int(params["start"])
            return rows[start:start + int(params["limit"])]
        day = params["day"]
        if day == self.empty_day:
            return {}
        if path == "stats":
            return {"totalSteps": self.edits.get(day, 1000), "restingHeartRate": 50}
        if path == "sleep-detail":
            return {"dailySleepDTO": {"calendarDate": day, "sleepTimeSeconds": 25200}}
        if path == "heart":
            return {"restingHeartRate": 50}
        if path == "respiration":
            return {"averageRespirationValue": 15}
        if path in {"steps-range", "sleep-range", "battery-range"}:
            return []
        return {}

    def get_daily_steps(self, first, last):
        return self.connectapi("steps-range", {"day": first})

    def get_sleep_daily(self, first, last):
        return self.connectapi("sleep-range", {"day": first})

    def get_hrv_data_range(self, first, last):
        return self.connectapi("hrv-range", {"day": first})

    def get_body_battery(self, first, last):
        return self.connectapi("battery-range", {"day": first})

    def get_weigh_ins(self, first, last):
        return self.connectapi("weight-range", {"day": first})

    def get_stats(self, day):
        return self.connectapi("stats", {"day": day})

    def get_sleep_data(self, day):
        return self.connectapi("sleep-detail", {"day": day})

    def get_heart_rates(self, day):
        return self.connectapi("heart", {"day": day})

    def get_respiration_data(self, day):
        return self.connectapi("respiration", {"day": day})


def main():
    results = {}
    with TemporaryDirectory() as directory, redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        sdk, storage = SummaryGarmin(), HealthClient()
        store = R2Store(client=storage, bucket="synthetic-summary")
        initial = run(directory, store=store, client=sdk, now=NOW, request_pause=0)
        storage.reset()
        recent = run(directory, store=R2Store(client=storage, bucket="synthetic-summary"),
                     client=sdk, now=NOW + timedelta(minutes=1), request_pause=0)
        results["new_initial"] = initial
        results["new_recent_repeat"] = {"source": recent, "r2": storage.counts.copy()}
        storage.reset()
        with patch.object(garmin, "_login", return_value=sdk), patch.object(garmin_health, "_login", return_value=sdk), patch.object(garmin_health.time, "sleep"):
            # Anchor the existing orchestrator/adapter's default date for comparison.
            class Today:
                today = staticmethod(lambda: NOW.date())
            with patch.object(garmin, "date", Today), provider_diagnostics(sdk) as calls:
                legacy(data_dir=directory, health_end=NOW.date())
            export(directory, store=R2Store(client=storage, bucket="synthetic-summary"))
        results["previous_repeat"] = {"source": calls, "r2": storage.counts.copy()}
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
