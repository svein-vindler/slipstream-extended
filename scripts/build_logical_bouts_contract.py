"""Build current synthetic Coach through the ordinary stored-source producer."""
from __future__ import annotations

import base64
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import coach_backfill  # noqa: E402
from scripts.benchmark_coach_reuse import CountingStore  # noqa: E402


def build(*, cadence=False):
    fixtures = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
    name = "logical_cadence_stored_source.json" if cadence else "logical_bouts_stored_source.json"
    sources = json.loads((fixtures / name).read_text(encoding="utf-8"))
    store = CountingStore()
    store.objects = {key: base64.b64decode(body) for key, body in sources.items()}
    with redirect_stdout(io.StringIO()):
        result = coach_backfill.run_one(activity={
            "id": "42", "date": "2026-10-08", "name": "Synthetic run", "type": "Run", "moving_seconds": 60.0,
        }, store=store)
    if result["blocked_activities"]:
        raise RuntimeError("Synthetic stored Coach publication failed")
    return {
        key: base64.b64encode(body).decode("ascii") for key, body in store.objects.items()
    }


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3) or len(sys.argv) == 3 and sys.argv[2] != "cadence":
        raise ValueError("Expected output path and optional cadence case")
    Path(sys.argv[1]).write_text(json.dumps(build(cadence=len(sys.argv) == 3), sort_keys=True) + "\n", encoding="utf-8")
