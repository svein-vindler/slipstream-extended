"""Generate actual producer outputs for the MCP contract; no test execution."""
import base64
import gzip
import json
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular_export import export_activity  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402
from scripts.build_running_segments_contract import ACTIVITY, SyntheticGarmin  # noqa: E402
from scripts.build_training_context_contract import DTO  # noqa: E402

PREFIX = "activities/2026/42/"
CONTEXT_KEY = PREFIX + "context/v1/20261008T120000000-ctx-sides.json"


def source_store(output):
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
    export_activity(DTO, SyntheticGarmin(False), store, output)
    return store


def declare(store, side="left", **changes):
    decoded = json.loads(gzip.decompress(store.objects[PREFIX + "activity.v1.json"]))
    context = {
        "schema_version": 2, "activity_id": "42", "context_id": "ctx-sides",
        "activity_date": "2026-10-08", "date_source": "garmin_local",
        "rpe": 7, "conditions": "Synthetic dry", "note": "Synthetic note",
        "workout_correction": None, "source": "user", "created_at": "2026-10-08T12:00:00.000Z",
        "gct_balance_side": {"source_fit_sha256": decoded["source_fit_sha256"],
                             "raw_side": side, "source": "user_activity_declaration"},
    }
    context["gct_balance_side"].update(changes)
    store.objects[CONTEXT_KEY] = json.dumps(context).encode()
    return context


def selected(store):
    pointer = json.loads(store.objects[PREFIX + "coach-input/v1/latest-ready.json"])
    return pointer, json.loads(gzip.decompress(store.objects[pointer["analysis_key"]]))


def build(work):
    result = {}
    for mode in ("left", "right", "unknown", "stale"):
        output = work / (mode + "-" + uuid4().hex)
        output.mkdir(parents=True)
        store = source_store(output)
        if mode != "unknown":
            declare(store, "right" if mode == "right" else "left",
                    **({"source_fit_sha256": "f" * 64} if mode == "stale" else {}))
        coach_backfill.run_one(activity=ACTIVITY, store=store)
        _, analysis = selected(store)
        result[mode] = {"analysis_id": analysis["analysis_id"], "counts": dict(store.counts),
                        "objects_base64": {key: base64.b64encode(body).decode()
                                           for key, body in store.objects.items()}}
    return result


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build(Path(sys.argv[2])), sort_keys=True), encoding="utf-8")
