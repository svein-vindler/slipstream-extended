"""Synthetic ordinary collection -> canonical storage fixture for MCP runtime tests.

No account, network or installation is used. Expected literals are an independently
worked six-point/two-night-date example; raw fixture bytes come from real writers.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garminconnect import Garmin  # noqa: E402

from pipeline.health_sync import finalize_days, store_day  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.refresh_summaries import run as refresh  # noqa: E402
from pipeline.sources.garmin_readonly import ReadOnlyGarmin  # noqa: E402
from scripts.benchmark_summary_windows import HealthClient, SummaryGarmin  # noqa: E402

WAKE_DATE = "2026-10-25"
SIGNAL_DAYS = ("2026-10-24", WAKE_DATE)
START = "2026-10-24T21:58:00Z"
END = "2026-10-25T06:48:00Z"
# Literal instants, independent of the source/reader timestamp conversion routines.
HR = {
    "2026-10-24": [[1792879080000, 80], [1792879200000, 48], [1792879320000, 46],
                   [1792879440000, 44], [1792879560000, 42]],
    WAKE_DATE: [[1792886400000, 60], [1792910880000, 99]],
}


class SyntheticSignals(SummaryGarmin):
    def get_heart_rates(self, day):
        super().get_heart_rates(day)
        return {"calendarDate": day, "heartRateValues": HR.get(day, [])}

    def get_body_battery(self, first, last):
        super().get_body_battery(first, last)
        return [{"date": day, "bodyBatteryValuesArray": [[HR[day][0][0], level]]}
                for day, level in zip(SIGNAL_DAYS, (55, 65), strict=True)]


class SyntheticStressResponse:
    """The bounded reader consumes this local byte stream, never a socket."""

    def __init__(self, data: bytes):
        self.raw = self
        self.stream = BytesIO(data)
        self.status_code = 200
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(data))}
        self.closed = False

    def read1(self, amount, *, decode_content):
        if decode_content is not False:
            raise AssertionError("Synthetic source cannot decompress a provider response")
        return self.stream.read(amount)

    def close(self):
        self.closed = True

    def json(self):
        raise AssertionError("Synthetic source must use the bounded JSON parser")


class SyntheticAdditionalSignals(SyntheticSignals):
    def __init__(self):
        super().__init__()
        self.responses = []
        sdk = Garmin()
        sdk.client.di_token = "synthetic-night-stress-token"

        def forbidden(*args, **kwargs):
            raise AssertionError("Synthetic collection cannot authenticate or refresh")

        def send(method, url, **kwargs):
            day = url.rsplit("/", 1)[-1]
            if method != "GET" or day not in SIGNAL_DAYS or url != (
                "https://connectapi.garmin.com/wellness-service/wellness/dailyStress/" + day
            ):
                raise AssertionError("Synthetic collection attempted an unexpected transport")
            points = (
                [[1792879080000, 0], [1792879200000, -1], [1792879320000, -2], [1792879440000, 35]]
                if day == "2026-10-24" else [[1792886400000, 45], [1792886520000, 50]]
            )
            response = SyntheticStressResponse(json.dumps({
                "calendarDate": day, "stressValuesArray": points,
            }, separators=(",", ":")).encode())
            self.responses.append(response)
            return response

        sdk.client._api_session.request = send
        for name in ("_refresh_session", "_refresh_di_token", "login"):
            setattr(sdk.client, name, forbidden)
        sdk.client.cs.request = forbidden
        self.stress_reader = ReadOnlyGarmin(sdk)

    def get_stress_data(self, day):
        return self.stress_reader.get_stress_data(day)


def build_case(*, additional_signals=False) -> dict:
    """Return real stored bytes and independent expected values, without side effects outside temp."""
    storage = HealthClient()
    source = SyntheticAdditionalSignals() if additional_signals else SyntheticSignals()
    store = R2Store(client=storage, bucket="synthetic-night-signals")
    captured = StringIO()
    with TemporaryDirectory(prefix="night-collection-") as directory, redirect_stdout(captured), redirect_stderr(captured):
        result = refresh(directory, store=store, client=source,
                         now=datetime(2026, 10, 25, 12, tzinfo=timezone.utc), request_pause=0)
        sleep = store_day(store, "sleep", WAKE_DATE, {
            "dailySleepDTO": {"calendarDate": WAKE_DATE, "sleepTimeSeconds": 31800,
                              "sleepStartTimestampGMT": START, "sleepEndTimestampGMT": END,
                              "sleepStartTimestampLocal": "2026-10-24T22:58:00",
                              "sleepEndTimestampLocal": "2026-10-25T07:48:00",
                              "sleepWindowConfirmed": True},
            "sleepLevels": [{"startGMT": START, "endGMT": END, "activityLevel": "light"}],
        })
        hrv = store_day(store, "hrv", WAKE_DATE, {
            "hrvSummary": {"calendarDate": WAKE_DATE, "lastNightAvg": 46},
            "sleepStartTimestampGMT": START, "sleepEndTimestampGMT": END,
            "sleepStartTimestampLocal": "2026-10-24T22:58:00",
            "sleepEndTimestampLocal": "2026-10-25T07:48:00",
            "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 42},
                            {"readingTimeGMT": "2026-10-25T00:00:00Z", "hrvValue": 50},
                            {"readingTimeLocal": "2026-10-25T01:00:00", "hrvValue": 999}],
        })
        finalize_days(store, "sleep", [sleep])
        finalize_days(store, "hrv", [hrv])
    objects = {}
    for key, value in storage.objects.items():
        if not key.startswith("health/"):
            continue
        if key.startswith("health/signals/") and not any(day in key for day in SIGNAL_DAYS):
            continue
        objects[key] = {"base64": base64.b64encode(value["Body"]).decode("ascii"),
                        "content_type": value.get("ContentType", "application/json"),
                        "content_encoding": value.get("ContentEncoding")}
    payload = {
        "schema_version": 1, "synthetic": True, "wake_date": WAKE_DATE,
        "timezone": "Europe/Oslo", "objects": objects,
        "expected": {"sleep_start_utc": START, "sleep_end_utc": END,
                     "sleep_start_iana": "2026-10-24T23:58:00+02:00",
                     "sleep_end_iana": "2026-10-25T07:48:00+01:00",
                     "heart_rate_values": [80, 48, 46, 44, 42, 60],
                     "hrv_values": [42, 50], "hrv_ambiguous_points": 1,
                     "heart_rate_minimum_10m_median": 45,
                     "first_10m_window_median": 46,
                     "body_battery_values": [55, 65],
                     "outside_hr_value": 99},
        "collection": {"health_sdk_calls": result["health_diagnostics"]["sdk_calls"],
                       "provider_api_calls": result["provider_diagnostics"]["garmin_api_calls"],
                       "signals": result["signals"]},
    }
    if additional_signals:
        payload["expected"].update(stress_values=[0, 35, 45, 50], stress_unit="points",
                                   stress_missing_value_count=2, measurement_kind="garmin_derived")
        payload["collection"]["stress"] = result["health_diagnostics"]["stress"]
        payload["collection"]["provider_counter_scope"] = (
            "Existing SDK connectapi calls only; direct bounded stress fitness attempts reported separately"
        )
        payload["collection"]["stress_responses_closed"] = all(response.closed for response in source.responses)
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--additional-signals", action="store_true")
    args = parser.parse_args()
    filename = "night-additional-signals-contract.json" if args.additional_signals else "night-signals-contract.json"
    target = args.output or Path(__file__).resolve().parents[1] / "worker/test-runtime/generated" / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = build_case(additional_signals=args.additional_signals)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"generated": str(target), "objects": len(payload["objects"]),
                      "synthetic": True}))


if __name__ == "__main__":
    main()