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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garmin_fit_sdk import Encoder  # noqa: E402

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular_export import export_activity  # noqa: E402
from pipeline.recorded_running_metrics import expanded_recorded_running_metrics  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402
from scripts.build_training_context_contract import DTO, START  # noqa: E402

ACTIVITY = {"id": "42", "date": "2026-10-08", "name": "Synthetic run",
            "type": "Run", "moving_seconds": 40.0}


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


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build(), separators=(",", ":")), encoding="utf-8")
