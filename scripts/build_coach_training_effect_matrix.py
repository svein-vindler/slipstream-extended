"""Named stored-source variants through the real export/Coach producer seam."""
from __future__ import annotations

import base64
import gzip
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import coach_backfill  # noqa: E402
from pipeline.granular import gzip_json  # noqa: E402
from scripts.build_coach_training_effect_contract import ACTIVITY, source_store  # noqa: E402


def build_matrix():
    cases = {}
    variants = {"zero": {"aerobic": 0}, "missing": {"aerobic": None, "anaerobic": None},
                "partial_totals": {"laps": [(None, 500, 150), (40, None, 150)]},
                "unknown_totals": {"laps": [(None, None, 150)]},
                "invalid_estimate": {}, "unverified_source": {}, "no_clock": {}}
    with redirect_stdout(io.StringIO()):
        for name, options in variants.items():
            store = source_store(**options)
            key = "activities/2026/42/activity.v1.json"
            decoded = json.loads(gzip.decompress(store.objects[key]))
            if name == "invalid_estimate":
                decoded["messages"]["session_mesgs"][0]["total_training_effect"] = "synthetic-invalid"
            elif name == "unverified_source":
                decoded["source"] = "unverified-third-party"
            elif name == "no_clock":
                decoded["activity"]["start_time_gmt"] = None
            store.objects[key] = gzip_json(decoded)
            store.reset_counts()
            result = coach_backfill.run_one(activity=ACTIVITY, store=store)
            cases[name] = {"analysis_id": result["processed_sources"]["42"],
                           "objects_base64": {key: base64.b64encode(body).decode()
                                              for key, body in sorted(store.objects.items())
                                              if key.endswith((".json", ".csv"))},
                           "publication_operations": dict(store.counts)}
        # This existing real source builder mutates its synthetic clock module;
        # run it last so the earlier Oct8 variants keep their independent clocks.
        from scripts.build_coach_stored_context_contract import build_case
        cases["stored_context"] = build_case("complete")
    return {"schema_version": 1, "cases": cases}


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build_matrix(), separators=(",", ":")), encoding="utf-8")
