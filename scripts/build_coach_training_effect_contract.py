"""Synthetic structured FIT through actual export and immutable Coach publication."""
from __future__ import annotations

import base64
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

PRODUCER = (Path(sys.argv[1]).resolve() if __name__ == "__main__" and len(sys.argv) > 2
            else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(PRODUCER))

from garmin_fit_sdk import Encoder  # noqa: E402

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular_export import export_activity  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402
from scripts.build_running_segments_contract import ACTIVITY, SyntheticGarmin, synthetic_tcx  # noqa: E402
from scripts.build_training_context_contract import DTO, START  # noqa: E402


def synthetic_fit(*, laps=None, aerobic=3.4, anaerobic=0):
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 27, "message_index": 0, "intensity": "active"})
    for index, (elapsed, distance, heart_rate) in enumerate(laps if laps is not None else [(40, 2000, 150)]):
        row = {"mesg_num": 19, "message_index": index, "wkt_step_index": 0,
               "start_time": START, "timestamp": START}
        row.update({key: value for key, value in (("total_elapsed_time", elapsed),
                    ("total_distance", distance), ("avg_heart_rate", heart_rate)) if value is not None})
        encoder.write_mesg(row)
    session = {"mesg_num": 18, "start_time": START, "timestamp": START, "sport": "running"}
    session.update({key: value for key, value in (("total_training_effect", aerobic),
                    ("total_anaerobic_training_effect", anaerobic)) if value is not None})
    encoder.write_mesg(session)
    return bytes(encoder.close())


class StructuredGarmin(SyntheticGarmin):
    def __init__(self, laps=None, aerobic=3.4, anaerobic=0):
        self.laps = laps
        self.aerobic, self.anaerobic = aerobic, anaerobic

    def download_activity(self, activity_id, *, dl_fmt):
        from garminconnect import Garmin
        assert activity_id == 42
        return synthetic_fit(laps=self.laps, aerobic=self.aerobic, anaerobic=self.anaerobic) \
            if dl_fmt == Garmin.ActivityDownloadFormat.ORIGINAL else synthetic_tcx()


def source_store(*, laps=None, aerobic=3.4, anaerobic=0):
    store = CountingStore()
    store.objects = {
        "summary/activities.csv": (
            b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
            b"garmin-42,2026-10-07 23:30:00,Synthetic run,Run,40,2026-10-08\n"),
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-structured", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode(),
    }
    with TemporaryDirectory() as temporary, redirect_stdout(io.StringIO()):
        export_activity(DTO, StructuredGarmin(laps, aerobic, anaerobic), store, Path(temporary))
    return store


def build_case(*, laps=None):
    store = source_store(laps=laps)
    with redirect_stdout(io.StringIO()):
        result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    return {"analysis_id": result["processed_sources"]["42"],
            "objects_base64": {key: base64.b64encode(body).decode()
                               for key, body in sorted(store.objects.items())
                               if key.endswith((".json", ".csv"))}}


if __name__ == "__main__":
    output = Path(sys.argv[2] if len(sys.argv) > 2 else sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_case(), separators=(",", ":")), encoding="utf-8")
