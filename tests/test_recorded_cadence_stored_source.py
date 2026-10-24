"""Running cadence through the real FIT decoder and stored Coach publication.

Only the outer object store is fake. No provider/export helper or internal
producer/decoder result is substituted. All measurements below are invented.
"""
import base64
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone

from garmin_fit_sdk import Decoder, Encoder, Stream

from pipeline import coach_backfill
from pipeline.granular import decode_fit, gzip_json, json_bytes, normalize_endurance_session
from scripts.benchmark_coach_reuse import CountingStore


def test_real_decoded_fraction_reaches_published_running_cadence(tmp_path):
    activity = {
        "id": "42", "date": "2026-10-08", "name": "Synthetic cadence run",
        "type": "Run", "moving_seconds": 60.0,
    }
    dto = {
        "activityId": 42, "activityName": activity["name"],
        "activityType": {"typeKey": "running"},
        "startTimeGMT": "2026-10-08 07:00:00", "startTimeLocal": "2026-10-08 09:00:00",
    }
    start = datetime(2026, 10, 8, 7, tzinfo=timezone.utc)
    encoder = Encoder()
    for seconds, distance in ((0, 0), (30, 100), (60, 200)):
        encoder.write_mesg({
            "mesg_num": 20, "timestamp": start + timedelta(seconds=seconds),
            "distance": distance, "cadence": 80, "fractional_cadence": 0.5,
            "power": 150, "step_length": 1000, "stance_time": 200,
            "vertical_oscillation": 70, "vertical_ratio": 7, "stance_time_balance": 48,
        })
    encoder.write_mesg({
        "mesg_num": 19, "message_index": 0, "sport": "running",
        "start_time": start, "timestamp": start + timedelta(seconds=60),
        "total_elapsed_time": 60, "total_timer_time": 60, "total_distance": 200,
        "avg_cadence": 80, "avg_fractional_cadence": 0.5,
        "max_cadence": 82, "max_fractional_cadence": 0,
        "avg_power": 300, "avg_step_length": 1250, "avg_stance_time": 255,
        "avg_vertical_oscillation": 85, "avg_vertical_ratio": 6.5,
        "avg_stance_time_balance": 48.5,
    })
    encoder.write_mesg({
        "mesg_num": 18, "message_index": 0, "sport": "running",
        "start_time": start, "timestamp": start + timedelta(seconds=60),
        "total_elapsed_time": 60, "total_timer_time": 60, "total_distance": 200,
        "avg_cadence": 80, "avg_fractional_cadence": 0.5,
        "max_cadence": 82, "max_fractional_cadence": 0,
        "avg_power": 250, "avg_step_length": 1200, "avg_stance_time": 250,
        "avg_vertical_oscillation": 80, "avg_vertical_ratio": 6,
        "avg_stance_time_balance": 49,
    })
    # Encoder takes scaled values: its 0.5 must become raw uint8 64, not 8192.
    fit = bytes(encoder.close())
    raw, errors = Decoder(Stream.from_byte_array(bytearray(fit))).read(
        apply_scale_and_offset=False, merge_heart_rates=False,
    )
    decoded = decode_fit(fit, activity=dto)
    stored_decoded = json.loads(json_bytes(decoded))
    evidence = tmp_path / "cadence-tracer"
    evidence.mkdir()
    (evidence / "synthetic.fit").write_bytes(fit)
    (evidence / "decoder.actual.json").write_bytes(json_bytes({
        "raw": raw, "raw_errors": [str(error) for error in errors],
        "decoded": stored_decoded,
    }))
    assert errors == [] and stored_decoded["decode_errors"] == []
    assert stored_decoded["source_fit_sha256"] == hashlib.sha256(fit).hexdigest()
    assert len(raw["session_mesgs"]) == len(raw["lap_mesgs"]) == 1
    assert len(raw["record_mesgs"]) == 3
    for kind in ("session_mesgs", "lap_mesgs"):
        assert raw[kind][0]["avg_fractional_cadence"] == 64
        assert raw[kind][0]["max_fractional_cadence"] == 0
        row = stored_decoded["messages"][kind][0]
        assert row["sport"] == "running"
        assert row["avg_cadence"] == row["avg_running_cadence"] == 80
        assert row["avg_fractional_cadence"] == 0.5
        assert row["max_cadence"] == row["max_running_cadence"] == 82
        assert row["max_fractional_cadence"] == 0
    assert [row["cadence"] for row in raw["record_mesgs"]] == [80, 80, 80]
    assert [row["fractional_cadence"] for row in raw["record_mesgs"]] == [64, 64, 64]
    assert [row["cadence"]
            for row in stored_decoded["messages"]["record_mesgs"]] == [80, 80, 80]
    assert [row["fractional_cadence"]
            for row in stored_decoded["messages"]["record_mesgs"]] == [0.5, 0.5, 0.5]

    tcx = (
        b'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
        b'<Id>2026-10-08T07:00:00Z</Id><Lap StartTime="2026-10-08T07:00:00Z">'
        b'<TotalTimeSeconds>60</TotalTimeSeconds><DistanceMeters>200</DistanceMeters>'
        b'<Track><Trackpoint><Time>2026-10-08T07:00:00Z</Time><DistanceMeters>0</DistanceMeters>'
        b'</Trackpoint><Trackpoint><Time>2026-10-08T07:00:30Z</Time><DistanceMeters>100</DistanceMeters>'
        b'</Trackpoint><Trackpoint><Time>2026-10-08T07:01:00Z</Time><DistanceMeters>200</DistanceMeters>'
        b'</Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>'
    )
    store = CountingStore()
    store.objects = {key: body for key, body in store.objects.items()
                     if not key.startswith("activities/")}
    store.objects.update({
        "activities/2026/42/activity.v1.json": gzip_json(stored_decoded),
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
    preserved = {name: analysis[name] for name in
                 ("recorded_running_metrics", "segment_running_metrics")}
    (evidence / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (evidence / "canonical.actual.json.gz").write_bytes(canonical)
    (evidence / "preserved-blocks.actual.json").write_bytes(json_bytes(preserved))
    for label, objects in (("before", before), ("after", after)):
        (evidence / f"sources.{label}.json").write_text(json.dumps({
            key: base64.b64encode(body).decode("ascii") if body is not None else None
            for key, body in objects.items()
        }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "result": result, "counts": counts, "sources_preserved": before == after,
        "analyzer_version": analysis["analyzer_version"],
        "preserved_blocks_sha256": hashlib.sha256(json_bytes(preserved)).hexdigest(),
        "canonical_gzip_bytes": len(canonical),
    }, sort_keys=True), encoding="utf-8")
    assert before == after
    native = analysis["recorded_running_metrics"]
    assert native["schema_version"] == 2 and native["association"] == "matched"
    names = (
        "average_power", "average_step_length", "average_ground_contact_time",
        "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance",
    )
    assert [native[name]["value"] for name in names] == [250, 1200, 250, 80, 6, 49]
    assert [native["laps"]["items"][0][name]["value"] for name in names] == [
        300, 1250, 255, 85, 6.5, 48.5,
    ]

    # The first new-contract assertion is after real decoding/publication/evidence.
    cadence = analysis["recorded_cadence"]
    assert cadence["schema_version"] == 2
    assert cadence["method"] == "fit_running_native_cadence_conversion"
    assert cadence["activity_id"] == "42"
    assert cadence["source_fit_sha256"] == stored_decoded["source_fit_sha256"]
    assert cadence["association"] == "matched"
    assert [row["lap"] for row in cadence["laps"]["items"]] == [1]
    for scope in (cadence["session"], cadence["laps"]["items"][0]):
        for name, value, fields in (
            ("average", 161, (
                ["avg_cadence", "avg_fractional_cadence"],
                ["avg_running_cadence", "avg_fractional_cadence"],
            )),
            ("maximum", 164, (
                ["max_cadence", "max_fractional_cadence"],
                ["max_running_cadence", "max_fractional_cadence"],
            )),
        ):
            metric = scope[name]
            assert {key: metric[key] for key in ("status", "value", "unit", "precision")} == {
                "status": "available", "value": value, "unit": "steps_per_minute",
                "precision": "recorded_base_and_fraction",
            }
            assert metric["source_fields"] in fields
