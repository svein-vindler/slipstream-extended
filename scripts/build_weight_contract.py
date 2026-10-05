"""Generate invented canonical/index bytes through the real guarded builder."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.granular import gzip_json, json_bytes  # noqa: E402
from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.weight_index import ROOT, index_key, sync_dates  # noqa: E402
from scripts.benchmark_health_reuse import HealthClient  # noqa: E402


def build():
    client = HealthClient()
    def store():
        return R2Store(client=client, bucket="synthetic-weight")
    fixtures = [
        {"date": "2026-06-12", "local": "2026-06-12T07:30:00+09:00", "gmt": "2026-06-11T22:30:00Z", "weight": 80},
        {"date": "2026-10-25", "local": None, "gmt": "2026-10-25T06:30:00Z", "weight": 79},
        {"date": "2027-01-01", "local": "2027-01-01T07:30:00", "gmt": "2027-01-01T06:30:00Z", "weight": 78},
    ]
    keys = {}
    for item in fixtures:
        day = item["date"]
        key = f"{ROOT}/{day[:4]}/{day[5:7]}/{day}.json"
        keys[day] = key
        payload = {"date": day, "measurements": [
            {"weight_kg": item["weight"], "timestamp_local": item["local"], "timestamp_gmt": item["gmt"],
             "measurement_id": "synthetic-first", "source_type": "synthetic", "bmi": 999,
             "provider_private_field": "must-not-enter-index"},
            {"weight_kg": item["weight"] + 1, "timestamp_local": f"{day}T09:30:00",
             "timestamp_gmt": f"{day}T{'00' if item['weight'] == 80 else '08'}:30:00Z",
             "measurement_id": "synthetic-later"},
            {"weight_kg": 999, "timestamp_local": f"{day}T05:00:00", "is_daily_average": True},
        ]}
        store().put(key, gzip_json(payload), "application/json", encoding="gzip")
    sync_dates(store(), keys)
    def capture():
        return {key: {"body": base64.b64encode(value["Body"]).decode(),
                      "etag": hashlib.md5(value["Body"], usedforsecurity=False).hexdigest()}
                for key, value in client.objects.items()}
    states = {"initial": capture()}
    first = keys[fixtures[0]["date"]]
    import gzip
    edited = json.loads(gzip.decompress(client.objects[first]["Body"]))
    edited["measurements"][0]["weight_kg"] = 75
    store().put(first, gzip_json(edited), "application/json", encoding="gzip")
    states["changed"] = capture()
    sync_dates(store(), [fixtures[0]["date"]])
    states["repaired"] = capture()
    del client.objects[first]
    states["deleted"] = capture()
    states["no_indexes"] = {key: value for key, value in states["initial"].items() if not key.startswith("health/indexes/")}
    states["corrupt_indexes"] = deepcopy(states["initial"])
    for key, value in states["corrupt_indexes"].items():
        if key.startswith("health/indexes/"):
            value["body"] = base64.b64encode(b"invalid-index").decode()
            value["etag"] = hashlib.md5(b"invalid-index", usedforsecurity=False).hexdigest()
    states["bad_entry"] = deepcopy(states["initial"])
    key = index_key(fixtures[0]["date"][:7])
    index = json.loads(base64.b64decode(states["bad_entry"][key]["body"]))
    index["objects"][first]["measurements"][0]["unexpected"] = {"private": True}
    body = json_bytes(index)
    states["bad_entry"][key] = {"body": base64.b64encode(body).decode(),
                                "etag": hashlib.md5(body, usedforsecurity=False).hexdigest()}
    return {"fixtures": fixtures, "states": states}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(build(), sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print("Built synthetic Python-to-Worker weight contract")


if __name__ == "__main__":
    main()
