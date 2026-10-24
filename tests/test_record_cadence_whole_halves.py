"""Eligible whole-half cadence through real FIT decoding and stored publication."""
import base64
import gzip
import json
from datetime import datetime, timedelta, timezone

from garmin_fit_sdk import Encoder

from pipeline import coach_backfill
from pipeline.granular import decode_fit, gzip_json, json_bytes, normalize_endurance_session
from scripts.benchmark_coach_reuse import CountingStore


def test_published_whole_halves_use_left_record_cadence_not_native_or_terminal_values(tmp_path):
    activity = {
        "id": "42", "date": "2026-10-08", "name": "Synthetic record cadence",
        "type": "Run", "moving_seconds": 20.0,
    }
    dto = {
        "activityId": 42, "activityName": activity["name"],
        "activityType": {"typeKey": "running"}, "startTimeGMT": "2026-10-08 07:00:00",
        "startTimeLocal": "2026-10-08 09:00:00",
    }
    start = datetime(2026, 10, 8, 7, tzinfo=timezone.utc)
    encoder = Encoder()
    for seconds, distance, base, fraction, power in (
        (0, 0, 80, 0.5, 150), (10, 10, 82, 0, 300), (20, 20, 120, 0.5, 450),
    ):
        encoder.write_mesg({
            "mesg_num": 20, "timestamp": start + timedelta(seconds=seconds),
            "distance": distance, "cadence": base, "fractional_cadence": fraction,
            "power": power, "step_length": 1000, "stance_time": 200,
            "vertical_oscillation": 70, "vertical_ratio": 7, "stance_time_balance": 48,
        })
    for mesg_num, base, power in ((19, 91, 300), (18, 90, 250)):
        encoder.write_mesg({
            "mesg_num": mesg_num, "message_index": 0, "sport": "running",
            "start_time": start, "timestamp": start + timedelta(seconds=20),
            "total_elapsed_time": 20, "total_timer_time": 20, "total_distance": 20,
            "avg_cadence": base, "avg_fractional_cadence": 0,
            "max_cadence": 125, "max_fractional_cadence": 0, "avg_power": power,
            "avg_step_length": 1200, "avg_stance_time": 250, "avg_vertical_oscillation": 80,
            "avg_vertical_ratio": 6, "avg_stance_time_balance": 49,
        })
    fit = bytes(encoder.close())
    decoded = json.loads(json_bytes(decode_fit(fit, activity=dto)))
    assert decoded["decode_errors"] == []
    records = decoded["messages"]["record_mesgs"]
    assert [row["cadence"] for row in records] == [80, 82, 120]
    assert [row["fractional_cadence"] for row in records] == [0.5, 0, 0.5]
    assert [row["distance"] for row in records] == [0, 10, 20]
    assert [row["timestamp"] for row in records] == [
        "2026-10-08T07:00:00+00:00", "2026-10-08T07:00:10+00:00", "2026-10-08T07:00:20+00:00",
    ]
    tcx = (
        b'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
        b'<Id>2026-10-08T07:00:00Z</Id><Lap StartTime="2026-10-08T07:00:00Z">'
        b'<TotalTimeSeconds>20</TotalTimeSeconds><DistanceMeters>20</DistanceMeters>'
        b'<Track><Trackpoint><Time>2026-10-08T07:00:00Z</Time><DistanceMeters>0</DistanceMeters>'
        b'</Trackpoint><Trackpoint><Time>2026-10-08T07:00:10Z</Time><DistanceMeters>10</DistanceMeters>'
        b'</Trackpoint><Trackpoint><Time>2026-10-08T07:00:20Z</Time><DistanceMeters>20</DistanceMeters>'
        b'</Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>'
    )
    store = CountingStore()
    store.objects = {key: body for key, body in store.objects.items()
                     if not key.startswith("activities/")}
    store.objects.update({
        "activities/2026/42/activity.v1.json": gzip_json(decoded),
        "activities/2026/42/activity.endurance.v1.json": gzip_json(normalize_endurance_session(dto, tcx)),
        "activities/2026/42/activity.tcx": tcx,
    })
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=activity, store=store)
    counts = dict(store.counts)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    after = {key: store.objects.get(key) for key in before}
    evidence = tmp_path / "record-cadence-tracer"
    evidence.mkdir()
    (evidence / "synthetic.fit").write_bytes(fit)
    (evidence / "decoded.actual.json").write_bytes(json_bytes(decoded))
    (evidence / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "preserved-blocks.actual.json").write_bytes(json_bytes({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")
    }))
    for label, objects in (("before", before), ("after", after)):
        (evidence / f"sources.{label}.json").write_text(json.dumps({
            key: base64.b64encode(body).decode("ascii") if body is not None else None
            for key, body in objects.items()
        }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "counts": counts, "result": result, "sources_preserved": before == after,
        "analyzer_version": analysis["analyzer_version"],
    }, sort_keys=True), encoding="utf-8")
    assert before == after
    native = analysis["recorded_running_metrics"]
    assert native["schema_version"] == 2 and native["association"] == "matched"
    assert native["average_power"]["value"] == 250
    assert native["laps"]["items"][0]["average_power"]["value"] == 300
    old_whole = analysis["segment_running_metrics"]["whole_distance_halves"]
    assert old_whole["status"] == "available" and old_whole["reason"] is None
    assert [row["segment"] for row in old_whole["items"]] == ["first", "second"]
    assert [row["average_power"]["value"] for row in old_whole["items"]] == [150, 300]
    coverage = {
        "valid_seconds": 10, "eligible_seconds": 10, "excluded_gap_seconds": 0,
        "contributing_intervals": 1, "status": "complete", "boundary_excluded_seconds": 0,
    }
    old_metrics = (
        "average_power", "average_step_length", "average_ground_contact_time",
        "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance",
    )
    for row in old_whole["items"]:
        for name in old_metrics:
            assert row[name]["coverage"] == coverage
    cadence = analysis["recorded_cadence"]
    assert cadence["session"]["average"]["value"] == 180
    assert cadence["laps"]["items"][0]["average"]["value"] == 182

    # First new-contract assertion follows real source/geometry/publication controls.
    whole = cadence["record_aggregates"]["whole_distance_halves"]
    assert cadence["schema_version"] == 2
    assert whole["status"] == "available" and whole["reason"] is None
    assert [row["segment"] for row in whole["items"]] == ["first", "second"]
    for row, expected in zip(whole["items"], (161, 164), strict=True):
        value = row["average_cadence"]
        assert value["status"] == "available"
        assert type(value["value"]) in (int, float) and value["value"] == expected
        assert value["unit"] == "steps_per_minute"
        assert value["coverage"] == coverage
