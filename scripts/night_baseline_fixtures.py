"""Deterministic synthetic native-night inputs for Python and mandatory Worker CI."""

import hashlib
from types import SimpleNamespace

from botocore.exceptions import ClientError

from pipeline.night_products import canonical


class Snapshots:
    def __init__(self):
        self.objects = {}
        self.reads = []

    def add(self, key, value):
        body = canonical(value).encode()
        self.objects[key] = SimpleNamespace(
            data=body,
            revision='"' + hashlib.md5(body, usedforsecurity=False).hexdigest() + '"',
            content_type="application/json",
            encoding=None,
        )

    def get_snapshot(self, key, **_):
        self.reads.append(key)
        if key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "GetObject",
            )
        return self.objects[key]

    def list_prefix_bounded(self, prefix, **_):
        return sorted(key for key in self.objects if key.startswith(prefix))


def native_fixture(day="2026-10-25"):
    store = Snapshots()
    sleep = {
        "schema_version": 1,
        "source": "garmin",
        "date": day,
        "sleep_start_gmt": "2026-10-24T22:00:00Z",
        "sleep_end_gmt": "2026-10-25T07:00:00Z",
        "sleep_start_garmin_local": "2026-10-25T00:00:00",
        "sleep_end_garmin_local": "2026-10-25T08:00:00",
    }
    store.add("health/sleep/v1/2026/10/" + day + ".json", sleep)
    for source_day in ("2026-10-24", "2026-10-25"):
        readings = [
            {
                "timestamp": f"2026-10-25T00:{i:02d}:00Z",
                "value": 50 + i,
                "time_basis": "utc",
                "timestamp_unit": "iso8601",
            }
            for i in range(12)
        ]
        store.add(
            "health/signals/v1/2026/10/" + source_day + ".json",
            {
                "schema_version": 1,
                "source": "garmin",
                "date": source_day,
                "channels": {
                    "heart_rate": {"status": "available", "unit": "bpm", "readings": readings}
                },
            },
        )
    store.add(
        "health/hrv/2026/10/" + day + ".json",
        {
            "schema_version": 1,
            "source": "garmin",
            "date": day,
            "readings": [{"timestamp": "2026-10-25T00:05:00Z", "hrv_ms": 45, "time_basis": "utc"}],
        },
    )
    return store


def contract_cases():
    cases = [
        {
            "name": "DST fall / repeated daily source points",
            "day": "2026-10-25",
            "timezone": "Europe/Oslo",
            "store": native_fixture(),
        }
    ]
    for name, day, start, end, wall_start, wall_end, actual, dates in [
        (
            "DST spring",
            "2026-03-29",
            "2026-03-28T22:00:00Z",
            "2026-03-29T06:00:00Z",
            "2026-03-28T23:00:00",
            "2026-03-29T08:00:00",
            "Europe/Oslo",
            ["2026-03-28", "2026-03-29"],
        ),
        (
            "36hour travel / plus14 UTC and source-calendar boundaries",
            "2026-04-01",
            "2026-03-29T23:00:00Z",
            "2026-03-31T11:00:00Z",
            "2026-03-30T13:00:00",
            "2026-04-01T01:00:00",
            "Pacific/Kiritimati",
            ["2026-03-29", "2026-03-30", "2026-03-31", "2026-04-01"],
        ),
        (
            "ISO-year boundary",
            "2021-01-01",
            "2020-12-31T22:00:00Z",
            "2021-01-01T07:00:00Z",
            "2020-12-31T23:00:00",
            "2021-01-01T08:00:00",
            None,
            ["2020-12-31", "2021-01-01"],
        ),
    ]:
        store = Snapshots()
        stamp = start[:16] + ":00Z"
        sleep = {
            "schema_version": 1,
            "source": "garmin",
            "date": day,
            "sleep_start_gmt": start,
            "sleep_end_gmt": end,
            "sleep_start_garmin_local": wall_start,
            "sleep_end_garmin_local": wall_end,
            "actual_night_timezone": actual,
        }
        store.add(f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json", sleep)
        for source_day in dates:
            store.add(
                f"health/signals/v1/{source_day[:4]}/{source_day[5:7]}/{source_day}.json",
                {
                    "schema_version": 1,
                    "source": "garmin",
                    "date": source_day,
                    "channels": {
                        "heart_rate": {
                            "status": "available",
                            "unit": "bpm",
                            "readings": [
                                {
                                    "timestamp": stamp,
                                    "value": 55,
                                    "time_basis": "utc",
                                    "timestamp_unit": "iso8601",
                                }
                            ],
                        }
                    },
                },
            )
        store.add(
            f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json",
            {
                "schema_version": 1,
                "source": "garmin",
                "date": day,
                "readings": [{"timestamp": stamp, "hrv_ms": 45, "raw": {"readingTimeGMT": stamp}}],
            },
        )
        cases.append({"name": name, "day": day, "timezone": "Europe/Oslo", "store": store})
    for clear in (False, True):
        store = native_fixture()
        day = "2026-10-25"
        user = {
            "schema_version": 1,
            "wake_date": day,
            "previous_context_id": None,
            "created_at": "2026-10-26T12:00:00.000Z",
            "source": "user",
            "alcohol": None,
            "late_bedtime": None,
            "travel": True,
            "actual_night_timezone": None if clear else "Asia/Tokyo",
            "illness": None,
            "sleep_quality": None,
            "morning_condition": None,
            "note": None,
            "declared_fields": ["actual_night_timezone", "travel"],
            "changed_fields": ["actual_night_timezone", "travel"],
        }
        import json

        identity = (
            "nctx-"
            + hashlib.sha256(
                json.dumps(
                    dict(sorted(user.items())),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest()[:24]
        )
        user["context_id"] = identity
        store.add(f"health/night-context/v1/2026/10/{day}/20261026T120000000-{identity}.json", user)
        cases.append(
            {
                "name": "Explicit user zone " + ("cleared" if clear else "overrides"),
                "day": day,
                "timezone": "UTC",
                "store": store,
            }
        )
    store = native_fixture()
    for key in list(store.objects):
        if "/signals/" in key:
            import json

            value = json.loads(store.objects[key].data)
            channel = value["channels"]["heart_rate"]
            channel["quality_flags"] = [
                "missing_values",
                "retained_previous_observations",
                "source_completeness_unknown",
            ]
            channel["missing_value_count"] = 2
            channel["source_composition"] = {
                "method_version": "preserve-good-observations-v1",
                "completeness": "unknown",
                "incoming_valid_points": 1,
                "retained_valid_points": 11,
                "new_valid_points": 1,
                "replaced_valid_points": 0,
                "source_revisions": ["a" * 64],
                "source_revision_count": 1,
                "source_revisions_truncated": False,
            }
            store.add(key, value)
    cases.append(
        {
            "name": "Missing and retained source provenance",
            "day": "2026-10-25",
            "timezone": "Europe/Oslo",
            "store": store,
        }
    )
    return cases
