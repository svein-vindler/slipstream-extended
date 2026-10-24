"""Native cadence guards through real decoding and stored Coach publication."""
import base64
import gzip
import json
from datetime import datetime, timedelta, timezone

import pytest
from garmin_fit_sdk import Encoder

from pipeline import coach_backfill
from pipeline.granular import decode_fit, gzip_json, json_bytes, normalize_endurance_session
from scripts.benchmark_coach_reuse import CountingStore


def _stored_case(case):
    sport = "cycling" if case == "cycling" else "running"
    activity = {
        "id": "42", "date": "2026-10-08", "name": "Synthetic cadence boundary",
        "type": "Cycling" if case == "cycling" else "Run", "moving_seconds": 60.0,
    }
    dto = {
        "activityId": 42, "activityName": activity["name"],
        "activityType": {"typeKey": sport}, "startTimeGMT": "2026-10-08 07:00:00",
        "startTimeLocal": "2026-10-08 09:00:00",
    }
    start = datetime(2026, 10, 8, 7, tzinfo=timezone.utc)
    encoder = Encoder()
    for mesg_num, power in ((19, 300), (18, 250)):
        row = {
            "mesg_num": mesg_num, "message_index": 0, "sport": sport,
            "start_time": start, "timestamp": start + timedelta(seconds=60),
            "total_elapsed_time": 60, "total_timer_time": 60, "total_distance": 200,
            "avg_cadence": 0 if case == "zero" else 80,
            "max_cadence": 82, "max_fractional_cadence": 0, "avg_power": power,
        }
        if case != "missing_fraction":
            row["avg_fractional_cadence"] = 0 if case == "zero" else 0.5
        encoder.write_mesg(row)
    decoded = json.loads(json_bytes(decode_fit(bytes(encoder.close()), activity=dto)))
    assert decoded["decode_errors"] == []
    for kind in ("session_mesgs", "lap_mesgs"):
        row = decoded["messages"][kind][0]
        assert row["sport"] == sport
        if case == "missing_fraction":
            assert "avg_fractional_cadence" not in row
        else:
            assert row["avg_fractional_cadence"] == (0 if case == "zero" else 0.5)
        if sport == "running":
            assert row["avg_running_cadence"] == row["avg_cadence"]
        # Deliberate stored-JSON corruption, never claimed as SDK output.
        if case == "invalid_fraction":
            row["avg_fractional_cadence"] = False
        elif case == "alias_conflict":
            assert row["avg_cadence"] == row["avg_running_cadence"] == 80
            row["avg_running_cadence"] = 81
    tcx = (
        b'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
        b'<Id>2026-10-08T07:00:00Z</Id><Lap StartTime="2026-10-08T07:00:00Z">'
        b'<TotalTimeSeconds>60</TotalTimeSeconds><DistanceMeters>200</DistanceMeters>'
        b'<Track><Trackpoint><Time>2026-10-08T07:00:00Z</Time><DistanceMeters>0</DistanceMeters>'
        b'</Trackpoint><Trackpoint><Time>2026-10-08T07:01:00Z</Time><DistanceMeters>200</DistanceMeters>'
        b'</Trackpoint></Track></Lap></Activity></Activities></TrainingCenterDatabase>'
    )
    if case == "cycling":
        tcx = tcx.replace(b'Sport="Running"', b'Sport="Biking"')
    store = CountingStore()
    store.objects = {key: body for key, body in store.objects.items()
                     if not key.startswith("activities/")}
    store.objects.update({
        "activities/2026/42/activity.v1.json": gzip_json(decoded),
        "activities/2026/42/activity.endurance.v1.json": gzip_json(normalize_endurance_session(dto, tcx)),
        "activities/2026/42/activity.tcx": tcx,
    })
    return activity, store, decoded


@pytest.mark.parametrize(("case", "value", "precision", "reason"), [
    ("missing_fraction", None, "fraction_unavailable", "fraction_missing_or_invalid"),
    ("invalid_fraction", None, "fraction_invalid", "fraction_invalid"),
    ("zero", 0, "recorded_base_and_fraction", None),
    ("cycling", None, "unverified", "source_unmatched"),
    ("alias_conflict", None, "conflicting_base", "base_alias_conflict"),
])
def test_native_cadence_boundaries_survive_stored_publication(tmp_path, case, value, precision, reason):
    activity, store, decoded = _stored_case(case)
    before = dict(store.objects)
    store.reset_counts()
    result = coach_backfill.run_one(activity=activity, store=store)
    counts = dict(store.counts)
    assert result["blocked_activities"] == []
    pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
    canonical = store.objects[pointer["analysis_key"]]
    analysis = json.loads(gzip.decompress(canonical))
    after = {key: store.objects.get(key) for key in before}
    evidence = tmp_path / "cadence-boundary"
    evidence.mkdir()
    (evidence / "decoded-stored.actual.json").write_bytes(json_bytes(decoded))
    (evidence / "analysis.actual.json").write_bytes(gzip.decompress(canonical))
    (evidence / "preserved-blocks.actual.json").write_bytes(json_bytes({
        name: analysis[name] for name in ("recorded_running_metrics", "segment_running_metrics")
    }))
    for label, objects in (("before", before), ("after", after)):
        (evidence / f"sources.{label}.json").write_text(json.dumps({
            key: base64.b64encode(body).decode("ascii") if body is not None else None
            for key, body in objects.items()
        }, sort_keys=True), encoding="utf-8")
    (evidence / "resources.actual.json").write_text(json.dumps({
        "case": case, "counts": counts, "result": result, "sources_preserved": before == after,
        "stored_corruption": case in ("invalid_fraction", "alias_conflict"),
    }, sort_keys=True), encoding="utf-8")
    assert before == after
    native = analysis["recorded_running_metrics"]
    assert native["schema_version"] == 2 and "recorded_cadence" not in native
    cadence = analysis["recorded_cadence"]
    assert cadence["schema_version"] == 2
    assert cadence["method"] == "fit_running_native_cadence_conversion"
    assert cadence["activity_id"] == "42"
    if case == "cycling":
        assert native["association"] == "unsupported_sport"
        assert cadence["association"] != "matched"
        assert cadence["laps"]["items"] == []
        scopes = [cadence["session"]]
    else:
        assert cadence["association"] == "matched"
        assert cadence["source_fit_sha256"] == decoded["source_fit_sha256"]
        assert [row["lap"] for row in cadence["laps"]["items"]] == [1]
        assert native["average_power"]["value"] == 250
        assert native["laps"]["items"][0]["average_power"]["value"] == 300
        scopes = [cadence["session"], cadence["laps"]["items"][0]]
    for scope in scopes:
        average = scope["average"]
        assert average["status"] == ("unavailable" if value is None else "available")
        assert average["value"] == value
        if value is not None:
            assert type(average["value"]) in (int, float)
        assert average["unit"] == "steps_per_minute"
        assert average["precision"] == precision
        assert average["reason"] == reason
        if case == "missing_fraction":
            assert "avg_fractional_cadence" not in average["source_fields"]
        if case == "cycling":
            assert scope["maximum"]["status"] == "unavailable"
            assert scope["maximum"]["value"] is None
        else:
            assert scope["maximum"]["status"] == "available"
            assert scope["maximum"]["value"] == 164
            assert scope["maximum"]["precision"] == "recorded_base_and_fraction"
