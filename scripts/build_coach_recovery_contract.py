"""Actual synthetic FIT/TCX publication for current recovery reader contracts."""
import base64
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import coach_backfill  # noqa: E402
from scripts.build_gct_sides_contract import declare, selected  # noqa: E402
from scripts.build_running_segments_contract import SEGMENT_ACTIVITY, structured_source_store  # noqa: E402


def build():
    with TemporaryDirectory() as temporary:
        store = structured_source_store(Path(temporary), balance_values=[50, 48, 49, 48.24, 52, 50, 0])
        declare(store, "right")
        result = coach_backfill.run_one(activity=dict(SEGMENT_ACTIVITY), store=store)
        if result["blocked_activities"]:
            raise RuntimeError("Synthetic recovery publication failed")
        _, analysis = selected(store)
        return {"analysis_id": analysis["analysis_id"], "objects_base64": {
            key: base64.b64encode(body).decode("ascii") for key, body in store.objects.items()}}


if __name__ == "__main__":
    Path(sys.argv[1]).write_text(json.dumps(build(), sort_keys=True) + "\n", encoding="utf-8")
