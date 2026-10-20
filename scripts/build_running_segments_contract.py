"""Invented sensor combinations through real export and immutable Coach publication."""
from __future__ import annotations

import base64
import gzip
import io
import json
import sys
from contextlib import redirect_stdout
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garmin_fit_sdk import Encoder  # noqa: E402

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular_export import export_activity  # noqa: E402
from pipeline.recorded_running_metrics import expanded_recorded_running_metrics  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402
from scripts.build_training_context_contract import DTO, START  # noqa: E402

ACTIVITY = {"id": "42", "date": "2026-10-08", "name": "Synthetic run",
            "type": "Run", "moving_seconds": 40.0}
SEGMENT_ACTIVITY = {**ACTIVITY, "moving_seconds": 60.0}


def structured_source_store(output, *, elapsed_shift=0.0, tcx_distance_scale=1.0, summary_timestamp_shift=0):
    """Actual encoded sources for a split active step and a later repeated step."""
    class StructuredGarmin:
        def download_activity(self, activity_id, *, dl_fmt):
            from garminconnect import Garmin

            assert activity_id == 42
            if dl_fmt != Garmin.ActivityDownloadFormat.ORIGINAL:
                points = "".join(
                    f"<Trackpoint><Time>{(START + timedelta(seconds=second)).isoformat()}</Time>"
                    f"<DistanceMeters>{second * 50 * tcx_distance_scale}</DistanceMeters>"
                    "<HeartRateBpm><Value>140</Value></HeartRateBpm></Trackpoint>"
                    for second in range(0, 61, 10)
                )
                return (f'<TrainingCenterDatabase><Activities><Activity Sport="Running">'
                        f'<Id>{START.isoformat()}</Id><Lap StartTime="{START.isoformat()}">'
                        f'<TotalTimeSeconds>60</TotalTimeSeconds><DistanceMeters>{3000 * tcx_distance_scale}</DistanceMeters>'
                        f'<Track>{points}</Track></Lap></Activity></Activities></TrainingCenterDatabase>').encode()
            encoder = Encoder()
            powers = [900, 100, 200, 0, 400, 900, 999]
            lengths = [9000, 1000, 1100, None, 1300, 9000, 9999]
            contacts = [999, 200, 220, 240, 260, 999, 999]
            oscillations = [999, 60, 80, 100, 120, 999, 999]
            ratios = [99, 6, 8, 10, 12, 99, 99]
            balances = [50, 101, 103, 105, 107, 50, 0]
            for index in range(7):
                row = {"mesg_num": 20, "timestamp": START + timedelta(seconds=index * 10),
                       "distance": index * 500, "power": powers[index],
                       "stance_time": contacts[index], "vertical_oscillation": oscillations[index],
                       "vertical_ratio": ratios[index], "stance_time_balance": balances[index]}
                if lengths[index] is not None:
                    row["step_length"] = lengths[index]
                encoder.write_mesg(row)
            for index, intensity in enumerate(("warmup", "active", "recovery", "cooldown")):
                encoder.write_mesg({"mesg_num": 27, "message_index": index, "intensity": intensity})
            for index, step in enumerate((0, 1, 1, 2, 1, 3)):
                encoder.write_mesg({"mesg_num": 19, "message_index": index, "wkt_step_index": step,
                                   "start_time": START + timedelta(seconds=index * 10),
                                   "timestamp": START + timedelta(seconds=(index + 1) * 10 + summary_timestamp_shift),
                                   "total_elapsed_time": 10 + (elapsed_shift if index == 1 else 0),
                                   "total_timer_time": 10,
                                   "total_distance": 500, "avg_power": 777})
            encoder.write_mesg({"mesg_num": 18, "start_time": START,
                               "timestamp": START + timedelta(seconds=60 + summary_timestamp_shift), "sport": "running",
                               "total_distance": 3000, "total_elapsed_time": 60,
                               "total_timer_time": 60, "avg_power": 999})
            return bytes(encoder.close())

    store = CountingStore()
    store.objects = {
        "summary/activities.csv": (
            b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
            b"garmin-42,2026-10-07 23:30:00,Synthetic run,Run,60,2026-10-08\n"),
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-segments", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode(),
    }
    export_activity(DTO, StructuredGarmin(), store, output)
    return store


def synthetic_fit(*, sparse=False):
    encoder = Encoder()
    for seconds, distance, power, length, contact, oscillation, ratio in (
        (0, 0, 100, 1000, 200, 60, 6),
        (10, 500, 200, 1100, 210, 70, 7),
        (20, 1000, 300, None, 230, 90, 9),
        (40, 2000, 400, 1300, 240, 100, 10),
    ):
        row = {"mesg_num": 20, "timestamp": START + timedelta(seconds=seconds),
               "distance": distance, "power": 0 if sparse else power}
        if not sparse:
            row.update(stance_time=contact, vertical_oscillation=oscillation,
                       vertical_ratio=ratio, stance_time_balance=49)
            if length is not None:
                row["step_length"] = length
        encoder.write_mesg(row)
    for index, (start, end, power) in enumerate(((0, 20, 145), (20, 40, 300))):
        row = {"mesg_num": 19, "message_index": index,
               "start_time": START + timedelta(seconds=start),
               "timestamp": START + timedelta(seconds=end), "total_distance": 1000,
               "avg_power": 0 if sparse else power}
        if not sparse:
            row.update(avg_stance_time=230 + 20 * index, avg_vertical_oscillation=70 + 10 * index,
                       avg_vertical_ratio=7 + index, avg_stance_time_balance=49.25 + index * 2)
            if index == 0:
                row["avg_step_length"] = 1000
        encoder.write_mesg(row)
    session = {"mesg_num": 18, "start_time": START,
               "timestamp": START + timedelta(seconds=40), "sport": "running",
               "total_elapsed_time": 40, "total_timer_time": 40, "total_distance": 2000,
               "avg_power": 0 if sparse else 225, "total_training_effect": 3.2,
               "total_anaerobic_training_effect": 0}
    if not sparse:
        session.update(avg_step_length=1100, avg_stance_time=245, avg_vertical_oscillation=80,
                       avg_vertical_ratio=7.5, avg_stance_time_balance=49.25)
    encoder.write_mesg(session)
    return bytes(encoder.close())


def synthetic_tcx():
    points = "".join(
        f"<Trackpoint><Time>{(START + timedelta(seconds=second)).isoformat()}</Time>"
        f"<DistanceMeters>{distance}</DistanceMeters>"
        f"<HeartRateBpm><Value>{heart}</Value></HeartRateBpm></Trackpoint>"
        for second, distance, heart in ((0, 0, 130), (10, 500, 140), (20, 1000, 150), (40, 2000, 160))
    )
    return (f'<TrainingCenterDatabase><Activities><Activity Sport="Running"><Id>{START.isoformat()}</Id>'
            f'<Lap StartTime="{START.isoformat()}"><TotalTimeSeconds>40</TotalTimeSeconds>'
            f'<DistanceMeters>2000</DistanceMeters><Track>{points}</Track></Lap>'
            '</Activity></Activities></TrainingCenterDatabase>').encode()


class SyntheticGarmin:
    def __init__(self, sparse):
        self.sparse = sparse

    def download_activity(self, activity_id, *, dl_fmt):
        from garminconnect import Garmin
        assert activity_id == 42
        return synthetic_fit(sparse=self.sparse) if dl_fmt == Garmin.ActivityDownloadFormat.ORIGINAL else synthetic_tcx()


def build_case(*, sparse=False, source_error=False, invalid_halves=False):
    store = CountingStore()
    store.objects = {
        "summary/activities.csv": (
            b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
            b"garmin-42,2026-10-07 23:30:00,Synthetic run,Run,40,2026-10-08\n"),
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-segments", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode(),
    }
    with TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
        export_activity(DTO, SyntheticGarmin(sparse), store, Path(temporary))
        if source_error:
            # Inject a source failure at the outer stored-input boundary. Coach
            # publication itself still uses the real producer and selection.
            key = "activities/2026/42/activity.v1.json"
            decoded = json.loads(gzip.decompress(store.objects[key]))
            decoded["decode_errors"] = ["synthetic_record_decode_error"]
            store.objects[key] = gzip.compress(json.dumps(decoded).encode(), mtime=0)
        if invalid_halves:
            # Corrupt only stored partition metadata, then use the real optional
            # producer again; raw FIT geometry and the rest of TCX stay intact.
            decoded = json.loads(gzip.decompress(store.objects["activities/2026/42/activity.v1.json"]))
            key = "activities/2026/42/activity.endurance.v1.json"
            endurance = json.loads(gzip.decompress(store.objects[key]))
            endurance["distance_halves"]["first"]["distance_m"] = 500
            endurance["distance_halves"]["second"]["distance_m"] = 1500
            endurance["recorded_running_metrics_v2"] = expanded_recorded_running_metrics(
                ACTIVITY, decoded, endurance,
            )
            store.objects[key] = gzip.compress(json.dumps(endurance).encode(), mtime=0)
        result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    return {"analysis_id": result["processed_sources"]["42"],
            "objects_base64": {key: base64.b64encode(body).decode()
                               for key, body in sorted(store.objects.items())
                               if key.endswith((".json", ".csv"))}}


def build():
    return {**build_case(), "sparse": build_case(sparse=True),
            "source_error": build_case(source_error=True),
            "invalid_halves": build_case(invalid_halves=True)}


def build_segment_contract(work):
    """Same actual encoded-source helper as Python tests, exported for native MCP."""
    result = {}
    for name, options in (("complete", {}), ("official_clock", {"summary_timestamp_shift": -1}),
                          ("unaligned_whole", {"tcx_distance_scale": 1.1})):
        output = work / (name + "-" + uuid4().hex)
        output.mkdir(parents=True)
        store = structured_source_store(output, **options)
        store.reset_counts()
        coach_backfill.run_one(activity=SEGMENT_ACTIVITY, store=store)
        pointer = json.loads(store.objects["activities/2026/42/coach-input/v1/latest-ready.json"])
        body = store.objects[pointer["analysis_key"]]
        analysis = json.loads(gzip.decompress(body))
        result[name] = {
            "analysis_id": analysis["analysis_id"],
            "analysis": analysis, "counts": dict(store.counts),
            "objects_base64": {key: base64.b64encode(value).decode() for key, value in store.objects.items()},
        }
    return result


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    produced = (build_segment_contract(Path(sys.argv[2])) if len(sys.argv) == 3 else build())
    target.write_text(json.dumps(produced, separators=(",", ":")), encoding="utf-8")
