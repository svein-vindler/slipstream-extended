"""Public ordinary collection retains good source observations on partial replies."""
import gzip
import json
from datetime import timedelta

import pytest

from pipeline.r2_store import R2Store
from pipeline.refresh_summaries import run
from scripts.benchmark_summary_windows import NOW, HealthClient, SummaryGarmin

DAY = "2026-09-30"
KEY = "health/signals/v1/2026/09/2026-09-30.json"
T1, T2, T3 = 1790726400000, 1790726520000, 1790726640000


class PartialSource(SummaryGarmin):
    def __init__(self):
        super().__init__()
        self.points = [[T1, 80], [T2, 48]]

    def get_heart_rates(self, day):
        super().get_heart_rates(day)
        return {"calendarDate": day, "heartRateValues": self.points if day == DAY else []}


def document(storage):
    return json.loads(gzip.decompress(storage.objects[KEY]["Body"]))["channels"]["heart_rate"]


@pytest.mark.parametrize("points,expected,retained,incoming,new", [
    ([[T1, 80], [T2, None]], [(T1, 80), (T2, 48)], 1, 1, 0),
    ([[T1, 80], [T2, 0]], [(T1, 80), (T2, 48)], 1, 1, 0),
    ([[T1, 80]], [(T1, 80), (T2, 48)], 1, 1, 0),
    ([[T1, 82], [T2, None]], [(T1, 82), (T2, 48)], 1, 1, 0),
    ([[T1, 80], [T2, None], [T3, 50]], [(T1, 80), (T2, 48), (T3, 50)], 1, 2, 1),
    ([[T1 // 1000, 80], [T2, None]], [(T1 // 1000, 80), (T2, 48)], 1, 1, 0),
    ([[T1, 80], [T1, 82], [T2, None]], [(T1, 80), (T2, 48)], 2, 0, 0),
    ([[T1, 80], [T1, 80], [T2, None]], [(T1, 80), (T2, 48)], 1, 1, 0),
], ids=["null", "hr-zero", "omitted", "replacement", "new-point", "seconds", "conflict", "duplicate"])
def test_repeated_collection_preserves_good_points_and_stable_revision_lineage(
    tmp_path, points, expected, retained, incoming, new,
):
    storage, source = HealthClient(), PartialSource()
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
        client=source, now=NOW, request_pause=0)
    old_revision = document(storage)["source_sha256"]
    source.points = points
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
        client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    channel = document(storage)
    assert [(point["timestamp"], point["value"]) for point in channel["readings"]] == expected
    assert channel["source_composition"]["retained_valid_points"] == retained
    assert channel["source_composition"]["incoming_valid_points"] == incoming
    assert channel["source_composition"]["new_valid_points"] == new
    assert channel["source_composition"]["completeness"] == "unknown"
    assert len(channel["source_composition"]["source_revisions"]) <= 16
    assert "retained_previous_observations" in channel["quality_flags"]
    assert "source_completeness_unknown" in channel["quality_flags"]
    for point in channel["readings"]:
        assert point["source_revision"] == (old_revision if point["observation_origin"] == "retained"
                                             else channel["source_sha256"])
    before = storage.objects[KEY]["Body"]
    storage.reset()
    result = run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
                 client=source, now=NOW + timedelta(minutes=2), request_pause=0)
    assert storage.objects[KEY]["Body"] == before and KEY not in storage.writes
    assert result["signals"]["objects_written"] == 0

def test_body_battery_zero_replaces_a_positive_value_while_hr_zero_remains_missing(tmp_path):
    class Source(PartialSource):
        battery = 65

        def get_body_battery(self, first, last):
            super().get_body_battery(first, last)
            return [{"date": DAY, "bodyBatteryValuesArray": [[T1, self.battery]]}]

    storage, source = HealthClient(), Source()
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
        client=source, now=NOW, request_pause=0)
    source.battery = 0
    source.points = [[T1, 0], [T2, None]]
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
        client=source, now=NOW + timedelta(minutes=1), request_pause=0)
    channels = json.loads(gzip.decompress(storage.objects[KEY]["Body"]))["channels"]
    assert [point["value"] for point in channels["heart_rate"]["readings"]] == [80, 48]
    assert [point["value"] for point in channels["body_battery"]["readings"]] == [0]
    assert channels["body_battery"]["source_composition"]["retained_valid_points"] == 0


def test_repeated_partial_sources_keep_lineage_bounded_and_each_point_revision_flat(tmp_path):
    storage, source = HealthClient(), PartialSource()
    run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
        client=source, now=NOW, request_pause=0)
    for offset in range(20):
        source.points = [[T3 + offset * 120000, 60]]
        run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
            client=source, now=NOW + timedelta(minutes=offset + 1), request_pause=0)
    channel = document(storage)
    assert len(channel["readings"]) == 22
    assert len(channel["source_composition"]["source_revisions"]) == 16
    assert channel["source_composition"]["source_revision_count"] == 21
    assert channel["source_composition"]["source_revisions_truncated"] is True
    assert all(len(point["source_revision"]) == 64 for point in channel["readings"])
    assert all(not any(isinstance(value, list) for value in point.values()) for point in channel["readings"])
    assert len(gzip.decompress(storage.objects[KEY]["Body"])) < 1024 * 1024
    assert len(storage.objects[KEY]["Body"]) < 256 * 1024


def test_merged_point_limit_preserves_previous_source_bytes(tmp_path):
    from pipeline.granular import gzip_json, json_bytes
    from pipeline.r2_store import R2ReadBoundaryError

    storage, source = HealthClient(), PartialSource()
    old = {"schema_version": 1, "source": "garmin", "date": DAY, "channels": {
        "heart_rate": {"status": "available", "unit": "bpm", "source_sha256": "f" * 64,
                       "readings": [{"timestamp": T1 // 1000 + offset, "value": 50, "time_basis": "utc",
                                     "timestamp_unit": "epoch_seconds"} for offset in range(10000)]},
    }}
    assert len(json_bytes(old)) < 1024 * 1024
    before = gzip_json(old)
    storage.put_object(Key=KEY, Body=before, ContentType="application/json", ContentEncoding="gzip")
    source.points = [[T1 + 20000000, 60]]
    with pytest.raises(R2ReadBoundaryError) as error:
        run(str(tmp_path), store=R2Store(client=storage, bucket="synthetic-retention"),
            client=source, now=NOW, request_pause=0)
    assert error.value.status == "point_limit"
    assert storage.objects[KEY]["Body"] == before

def test_distinct_partial_responses_keep_unknown_retained_revision_unknown(tmp_path):
    from pipeline.granular import gzip_json

    storage, source = HealthClient(), PartialSource()
    legacy = {"schema_version": 1, "source": "garmin", "date": DAY, "channels": {
        "heart_rate": {"status": "available", "unit": "bpm", "readings": [
            {"timestamp": T2, "value": 48, "time_basis": "utc",
             "timestamp_unit": "epoch_milliseconds"}]}}}
    store = R2Store(client=storage, bucket="synthetic-unknown-retention")
    store.put(KEY, gzip_json(legacy), "application/json", encoding="gzip")
    revisions = []
    for offset, value in enumerate((80, 82)):
        source.points = [[T1, value], [T2, None]]
        result = run(str(tmp_path), store=store, client=source,
                     now=NOW + timedelta(minutes=offset), request_pause=0)
        channel = document(storage)
        revisions.append(channel["source_sha256"])
        assert [(point["timestamp"], point["value"]) for point in channel["readings"]] == [(T1, value), (T2, 48)]
        assert channel["readings"][1]["source_revision"] is None
        assert channel["readings"][1]["observation_origin"] == "retained"
        assert "retained_revision_unknown" in channel["quality_flags"]
        assert channel["source_composition"]["retained_valid_points"] == 1
        assert result["signals"]["objects_written"] >= 1
    assert revisions[0] != revisions[1]
    before = storage.objects[KEY]["Body"]
    storage.reset()
    result = run(str(tmp_path), store=store, client=source,
                 now=NOW + timedelta(minutes=2), request_pause=0)
    assert storage.objects[KEY]["Body"] == before and KEY not in storage.writes
    assert result["signals"]["put_attempts"] == 0


@pytest.mark.parametrize("point_fields,channel_fields,expected", [
    ({}, {}, "f" * 64),
    ({"source_revision": None}, {}, None),
    ({"source_revision": ""}, {}, None),
    ({"observation_origin": "retained"}, {"source_composition": {}}, None),
], ids=["direct-legacy-hash", "explicit-null", "invalid-explicit", "composed-missing"])
def test_retained_revision_fallback_requires_direct_legacy_origin(
    tmp_path, point_fields, channel_fields, expected,
):
    from pipeline.granular import gzip_json

    storage, source = HealthClient(), PartialSource()
    point = {"timestamp": T2, "value": 48, "time_basis": "utc",
             "timestamp_unit": "epoch_milliseconds", **point_fields}
    legacy = {"schema_version": 1, "source": "garmin", "date": DAY, "channels": {
        "heart_rate": {"status": "available", "unit": "bpm", "source_sha256": "f" * 64,
                       "readings": [point], **channel_fields}}}
    store = R2Store(client=storage, bucket="synthetic-retention-origin")
    store.put(KEY, gzip_json(legacy), "application/json", encoding="gzip")
    source.points = [[T1, 80], [T2, None]]
    run(str(tmp_path), store=store, client=source, now=NOW, request_pause=0)
    channel = document(storage)
    assert channel["readings"][1]["source_revision"] == expected
    assert ("retained_revision_unknown" in channel["quality_flags"]) == (expected is None)
