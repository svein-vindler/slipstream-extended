"""Invented FIT/TCX through real Coach publication, for JSON/SSE reader tests."""
from __future__ import annotations

import base64
import io
import json
import sys
from contextlib import redirect_stdout
from datetime import timedelta
from pathlib import Path

from garmin_fit_sdk import Encoder

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline import coach_backfill  # noqa: E402
from pipeline.granular import decode_fit, gzip_json, normalize_endurance_session  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402
from scripts.build_training_context_contract import DTO, START, TCX  # noqa: E402

ACTIVITY = {"id": "42", "date": "2026-10-08", "name": "Synthetic run",
            "type": "Run", "moving_seconds": 1800.0}


def synthetic_fit():
    encoder = Encoder()
    encoder.write_mesg({"mesg_num": 18, "message_index": 0, "start_time": START,
                        "timestamp": START + timedelta(minutes=30), "sport": "running",
                        "avg_power": 250, "avg_step_length": 1234.5, "avg_stance_time": 241.2,
                        "avg_vertical_oscillation": 86.4, "avg_vertical_ratio": 6.78})
    return bytes(encoder.close())


def build():
    prefix = "activities/2026/42"
    store = CountingStore()
    store.objects = {
        "summary/activities.csv": (
            b"Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
            b"garmin-42,2026-10-07 23:30:00,Synthetic run,Run,1800,2026-10-08\n"),
        prefix + "/activity.v1.json": gzip_json(decode_fit(synthetic_fit(), activity=DTO)),
        prefix + "/activity.endurance.v1.json": gzip_json(normalize_endurance_session(DTO, TCX)),
        prefix + "/activity.tcx": TCX,
        "coach/profiles/v1/2026-01-01/synthetic.json": json.dumps({
            "profile_id": "synthetic-recorded", "effective_from": "2026-01-01",
            "zones": [{"label": "Easy", "min_bpm": 100, "max_bpm": 150}],
        }).encode(),
    }
    with redirect_stdout(io.StringIO()):
        result = coach_backfill.run_one(activity=ACTIVITY, store=store)
    return {
        "activity": ACTIVITY,
        "analysis_id": result["processed_sources"]["42"],
        "objects_base64": {key: base64.b64encode(body).decode()
                           for key, body in sorted(store.objects.items()) if not key.endswith(".tcx")},
    }


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build(), separators=(",", ":")), encoding="utf-8")
