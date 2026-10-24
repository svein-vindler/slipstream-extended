"""Typed FIT workout values keep their decoded units and custom zero sentinel."""
import gzip
import json
from datetime import datetime, timedelta, timezone

from garmin_fit_sdk import Encoder

from pipeline import coach_backfill
from pipeline.granular import decode_fit, gzip_json, json_bytes, normalize_endurance_session
from scripts.benchmark_coach_reuse import CountingStore


def test_published_targets_use_active_decoded_time_distance_speed_fields_and_keep_zero(tmp_path):
    start = datetime(2026, 10, 8, 7, tzinfo=timezone.utc)
    activity = {"id": "42", "date": "2026-10-08", "name": "Synthetic targets", "type": "Run", "moving_seconds": 60.0}
    dto = {"activityId": 42, "activityName": "Synthetic targets", "activityType": {"typeKey": "running"},
           "startTimeGMT": "2026-10-08 07:00:00", "startTimeLocal": "2026-10-08 09:00:00"}
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 27, "message_index": 0, "intensity": "active", "duration_type": "time",
                        "duration_value": 60000, "target_type": "speed", "target_value": 0,
                        "custom_target_value_low": 3000, "custom_target_value_high": 4000})
    encoder.write_mesg({"mesg_num": 27, "message_index": 1, "intensity": "recovery", "duration_type": "distance",
                        "duration_value": 40000, "target_type": "open", "target_value": 0})
    for mesg_num in (19, 18):
        encoder.write_mesg({"mesg_num": mesg_num, "message_index": 0, "sport": "running", "wkt_step_index": 0,
                            "intensity": "active", "start_time": start, "timestamp": start + timedelta(seconds=60),
                            "total_elapsed_time": 60, "total_timer_time": 60, "total_distance": 200})
    fit = bytes(encoder.close())
    decoded = json.loads(json_bytes(decode_fit(fit, activity=dto)))
    assert decoded["decode_errors"] == []
    steps = decoded["messages"]["workout_step_mesgs"]
    assert [row["message_index"] for row in steps] == [0, 1]
    assert steps[0]["duration_time"] == 60 and steps[0]["duration_value"] == 60000
    assert steps[0]["target_speed_zone"] == steps[0]["target_value"] == 0
    assert [steps[0][name] for name in ("custom_target_speed_low", "custom_target_speed_high")] == [3, 4]
    assert steps[1]["duration_distance"] == 400 and steps[1]["duration_value"] == 40000
    tcx = (b'<TrainingCenterDatabase><Activities><Activity Sport="Running"><Id>2026-10-08T07:00:00Z</Id>'
           b'<Lap StartTime="2026-10-08T07:00:00Z"><TotalTimeSeconds>60</TotalTimeSeconds><DistanceMeters>200</DistanceMeters>'
           b'<Track><Trackpoint><Time>2026-10-08T07:00:00Z</Time><DistanceMeters>0</DistanceMeters></Trackpoint>'
           b'<Trackpoint><Time>2026-10-08T07:01:00Z</Time><DistanceMeters>200</DistanceMeters></Trackpoint>'
           b'</Track></Lap></Activity></Activities></TrainingCenterDatabase>')
    store = CountingStore()
    store.objects = {key: body for key, body in store.objects.items() if not key.startswith("activities/")}
    store.objects.update({"activities/2026/42/activity.v1.json": gzip_json(decoded),
                          "activities/2026/42/activity.endurance.v1.json": gzip_json(normalize_endurance_session(dto, tcx)),
                          "activities/2026/42/activity.tcx": tcx})
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=activity, store=store)
    assert result["blocked_activities"] == []
    key = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])["analysis_key"]
    analysis = json.loads(gzip.decompress(store.objects[key]))
    (tmp_path / "decoded.actual.json").write_bytes(json_bytes(decoded))
    (tmp_path / "analysis.actual.json").write_bytes(gzip.decompress(store.objects[key]))
    (tmp_path / "preserved-blocks.actual.json").write_bytes(json_bytes({name: analysis[name] for name in
                                                                   ("recorded_running_metrics", "segment_running_metrics", "workout_structure")}))
    assert all(store.objects[name] == body for name, body in before.items())
    assert analysis["workout_structure"]["planned_steps"][0]["duration_value"] == 60000
    assert analysis["workout_structure"]["planned_steps"][0]["target_value"] == 0
    targets = analysis["workout_targets"]
    assert targets["schema_version"] == 1 and targets["activity_id"] == "42"
    assert targets["source_fit_sha256"] == decoded["source_fit_sha256"]
    assert [row["step_index"] for row in targets["items"]] == [0, 1]
    assert targets["items"][0]["duration"] == {
        "status": "available", "type": "time", "value": 60, "unit": "s", "source_field": "duration_time", "reason": None,
    }
    assert targets["items"][1]["duration"] == {
        "status": "available", "type": "distance", "value": 400, "unit": "m", "source_field": "duration_distance", "reason": None,
    }
    assert targets["items"][0]["target"] == {"type": "speed", "mode": "custom", "raw_value": 0,
        "range": {"status": "available", "low": 3, "high": 4, "unit": "m/s",
                  "source_fields": ["custom_target_speed_low", "custom_target_speed_high"], "reason": None}}
