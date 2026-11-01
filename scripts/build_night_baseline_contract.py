"""Emit only deterministic synthetic parity inputs; no services or file outputs."""

import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.native_night import NativeNightReader
from pipeline.night_products import baseline, normalize_args
from scripts.night_baseline_fixtures import contract_cases


def main():
    result = []
    for case in contract_cases():
        store = case["store"]
        night = NativeNightReader(store, timezone=case["timezone"]).night(case["day"])
        args = normalize_args(end_date=case["day"], period="7", channel="heart_rate")
        product = baseline([night], args, input_bytes=0, input_points=1000)
        result.append(
            {
                "name": case["name"],
                "day": case["day"],
                "timezone": case["timezone"],
                "objects": {
                    key: {"body": base64.b64encode(v.data).decode(), "revision": v.revision}
                    for key, v in store.objects.items()
                },
                "night": night,
                "baseline": product,
            }
        )
    print(json.dumps(result, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
