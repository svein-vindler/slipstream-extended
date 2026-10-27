"""Provider changes preserve already collected health data through the real writer."""

import gzip
import json

import pytest


def test_partial_personal_hrv_keeps_previous_points_and_non_null_summary():
    from pipeline.health_sync import store_day

    day = "2026-10-25"
    key = "health/hrv/2026/10/" + day + ".json"
    old = {
        "schema_version": 1,
        "source": "garmin",
        "date": day,
        "summary": {"calendarDate": day, "lastNightAvg": 46, "weeklyAvg": 49},
        "reading_count": 2,
        "readings": [
            {
                "timestamp": "2026-10-24T22:00:00",
                "hrv_ms": 45,
                "raw": {"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 45},
            },
            {
                "timestamp": "2026-10-24T22:05:00",
                "hrv_ms": 48,
                "raw": {"readingTimeGMT": "2026-10-24T22:05:00", "hrvValue": 48},
            },
        ],
    }

    class Store:
        def __init__(self):
            self.body = gzip.compress(json.dumps(old).encode())

        def get_bounded(self, requested, maximum):
            assert requested == key
            assert len(self.body) <= maximum
            return self.body

        def put_if_changed(self, requested, body, content_type, **kwargs):
            assert requested == key
            self.body = body
            return True

    store = Store()
    raw = {
        "hrvSummary": {"calendarDate": day, "lastNightAvg": None},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 47}],
        "_slipstream_source": {"provider": "personal", "adapter_version": 1},
    }
    store_day(store, "hrv", day, raw)
    result = json.loads(gzip.decompress(store.body))
    assert result["reading_count"] == 2
    assert [row["hrv_ms"] for row in result["readings"]] == [47, 48]
    assert result["summary"]["lastNightAvg"] == 46
    assert result["summary"]["weeklyAvg"] == 49
    assert result["source_provider"] == "personal"


def test_provider_provenance_does_not_reuse_the_consumers_different_composition_schema():
    from botocore.exceptions import ClientError

    from pipeline.health_sync import store_day

    class Store:
        body = None

        def get_bounded(self, key, maximum):
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        def put_if_changed(self, key, body, content_type, **kwargs):
            self.body = body
            return True

    store = Store()
    raw = {
        "hrvSummary": {"calendarDate": "2026-10-25"},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 47}],
        "_slipstream_source": {"provider": "personal", "adapter_version": 1},
    }
    store_day(store, "hrv", "2026-10-25", raw)
    result = json.loads(gzip.decompress(store.body))
    assert "source_composition" not in result
    assert result["provider_composition"]["completeness"] == "unknown"
    assert "source_completeness_unknown" in result["quality_flags"]


def test_returning_to_legacy_also_keeps_prior_personal_observations():
    from pipeline.health_sync import store_day

    day = "2026-10-25"
    old = {
        "schema_version": 1,
        "source": "garmin",
        "date": day,
        "source_provider": "personal",
        "summary": {"calendarDate": day, "lastNightAvg": 46},
        "reading_count": 2,
        "readings": [
            {
                "timestamp": "2026-10-24T22:00:00",
                "hrv_ms": 45,
                "source_provider": "personal",
                "raw": {"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 45},
            },
            {
                "timestamp": "2026-10-24T22:05:00",
                "hrv_ms": 48,
                "source_provider": "personal",
                "raw": {"readingTimeGMT": "2026-10-24T22:05:00", "hrvValue": 48},
            },
        ],
    }

    class Store:
        body = gzip.compress(json.dumps(old).encode())

        def get_bounded(self, key, maximum):
            return self.body

        def put_if_changed(self, key, body, content_type, **kwargs):
            self.body = body
            return True

    store = Store()
    raw = {
        "hrvSummary": {"calendarDate": day, "lastNightAvg": 47},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 47}],
        "_slipstream_source": {"provider": "legacy", "adapter_version": 1},
    }
    store_day(store, "hrv", day, raw)
    result = json.loads(gzip.decompress(store.body))
    assert result["reading_count"] == 2
    assert [row["hrv_ms"] for row in result["readings"]] == [47, 48]
    assert result["source_provider"] == "legacy"


def test_repeated_partial_hrv_is_byte_stable_and_does_not_keep_stale_consumer_counts():
    from botocore.exceptions import ClientError

    from pipeline.health_sync import store_day

    class Store:
        body = None

        def get_bounded(self, key, maximum):
            if self.body is None:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return self.body

        def put_if_changed(self, key, body, content_type, **kwargs):
            changed = body != self.body
            self.body = body
            return changed

    store = Store()
    raw = {
        "hrvSummary": {"calendarDate": "2026-10-25"},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 47}],
        "_slipstream_source": {"provider": "personal"},
    }
    store_day(store, "hrv", "2026-10-25", raw)
    first = store.body
    store_day(store, "hrv", "2026-10-25", raw)
    assert store.body == first
    prior = json.loads(gzip.decompress(store.body))
    prior["source_composition"] = {
        "method_version": "old-composition",
        "retained_valid_points": 300,
    }
    store.body = gzip.compress(json.dumps(prior).encode())
    store_day(store, "hrv", "2026-10-25", raw)
    assert "source_composition" not in json.loads(gzip.decompress(store.body))


@pytest.mark.parametrize(
    "body", [b"not-json", b"{" + b" " * (512 * 1024)], ids=["corrupt", "oversize"]
)
def test_unverifiable_prior_hrv_fails_before_any_write(body):
    from pipeline.health_sync import store_day

    class Store:
        def get_bounded(self, *args):
            return body

        def put_if_changed(self, *args, **kwargs):
            raise AssertionError("Must preserve prior bytes")

    raw = {
        "hrvSummary": {"calendarDate": "2026-10-25"},
        "hrvReadings": [{"readingTimeGMT": "2026-10-24T22:00:00", "hrvValue": 47}],
        "_slipstream_source": {"provider": "personal"},
    }
    import pytest

    with pytest.raises(ValueError):
        store_day(Store(), "hrv", "2026-10-25", raw)
