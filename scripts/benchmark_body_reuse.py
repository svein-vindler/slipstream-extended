"""Synthetic old/new body refresh counts using the real legacy dayview routine."""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.health_detail_backfill import run as legacy  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.recent_body import run  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient  # noqa: E402

TODAY = date(2026, 9, 30)
SUMMARY = b"Date,Sleep Seconds,Weight KG\n2026-09-30,,80\n2026-09-22,,80\n2026-09-10,,80\n2020-01-01,,80\n"
PREFIX = "health/body-composition/v1/"


class SyntheticBodyGarmin:
    def __init__(self):
        self.calls = []
        self.responses = {}

    def get_daily_weigh_ins(self, day):
        self.calls.append(day)
        if day in self.responses:
            response = self.responses[day]
            if isinstance(response, Exception):
                raise response
            return response
        return {"dateWeightList": [{"calendarDate": day, "samplePk": index + 1,
                                   "timestampGMT": f"{day}T{hour:02}:00:00Z",
                                   "timestampLocal": f"{day}T{hour + 2:02}:00:00",
                                   "weight": 80000 + index * 100, "bodyFat": 18 + index,
                                   "muscleMass": 60100, "sourceType": "MANUAL"}
                                  for index, hour in enumerate((5, 12, 19))],
                "totalAverage": {"weight": 80100}}


def measure(client, garmin, old=False):
    client.reset()
    garmin.calls = []
    store = R2Store(client=client, bucket="synthetic-body")
    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        if old:
            legacy(skip_sleep=True, max_body_days=3, refresh_recent_days=3, store=store, garmin=garmin)
        else:
            run(health_csv=SUMMARY, store=store, garmin=garmin, today=TODAY)
    return {**client.counts, "provider_calls": len(garmin.calls),
            "canonical_put": sum(key.startswith(PREFIX) for key in client.writes),
            "canonical_get": sum(key.startswith(PREFIX) for key in client.reads),
            "plan_put": sum(key.startswith("backfill/") for key in client.writes),
            "checkpoint_put": sum(key.startswith("refresh/checks/") for key in client.writes)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    client, garmin = HealthClient(), SyntheticBodyGarmin()
    client.put_object(Key="summary/health_daily.csv", Body=SUMMARY, ContentType="text/csv")
    first = measure(client, garmin, args.baseline)
    repeat = measure(client, garmin, args.baseline)
    print(json.dumps({"mode": "baseline" if args.baseline else "incremental",
                      "initial": first, "unchanged": repeat}, indent=2))


if __name__ == "__main__":
    main()
