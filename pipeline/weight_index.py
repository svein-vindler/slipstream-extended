"""Optional revision-verified monthly indexes of every real weight measurement."""
from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import re
from datetime import date, timedelta

from botocore.exceptions import ClientError

from .granular import json_bytes
from .r2_store import R2Store, missing_object

ROOT = "health/body-composition/v1"
INDEX_ROOT = "health/indexes/body-composition/v1"
FIELDS = ("timestamp_gmt", "timestamp_local", "weight_kg", "is_daily_average", "measurement_id", "source_type")
STORED_LIMIT = 256 * 1024
DECODED_LIMIT = 512 * 1024
INDEX_LIMIT = 512 * 1024


def valid_measurements(rows):
    return isinstance(rows, list) and all(item is None or isinstance(item, dict)
        and all(key in FIELDS and (value is None or isinstance(value, (str, int, float, bool)))
                and (not isinstance(value, float) or math.isfinite(value))
                for key, value in item.items()) for item in rows)


def index_key(month):
    date.fromisoformat(month + "-01")
    return f"{INDEX_ROOT}/{month}.json"


def compact(day, raw):
    if len(raw) > STORED_LIMIT:
        raise ValueError("Body object exceeds stored limit")
    if raw[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as handle:
            raw = handle.read(DECODED_LIMIT + 1)
    if len(raw) > DECODED_LIMIT:
        raise ValueError("Body object exceeds decoded limit")
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("date") != day or not isinstance(value.get("measurements"), list):
        raise ValueError("Unsupported body schema")
    # Retain count/order and every clock, average flag, identity and actual
    # weight. Other body metrics are still available from the canonical tool.
    return [{key: item[key] for key in FIELDS if key in item} if isinstance(item, dict) else None
            for item in value["measurements"]]


def sync_dates(store, dates):
    result = {"months_written": [], "months_unchanged": [], "months_skipped": []}
    months = sorted({date.fromisoformat(day).isoformat()[:7] for day in dates})
    for month in months:
        prefix = f"{ROOT}/{month[:4]}/{month[5:7]}/"
        sources = {}
        for key, etag in store.list_object_revisions(prefix).items():
            match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?", key.removeprefix(prefix))
            if not match or not match[1].startswith(month):
                continue
            try:
                date.fromisoformat(match[1])
            except ValueError:
                continue
            sources[key] = (match[1], etag)
        current = None
        try:
            raw = store.get(index_key(month))
            if len(raw) <= INDEX_LIMIT:
                current = json.loads(raw)
        except KeyError:
            pass
        except ClientError as error:
            if not missing_object(error):
                raise
        except (ValueError, UnicodeError):
            pass
        cached = current.get("objects", {}) if (isinstance(current, dict)
            and current.get("schema_version") == 1 and current.get("kind") == "weight-month-index"
            and current.get("month") == month and isinstance(current.get("objects"), dict)) else {}
        objects = {}
        for key, (day, etag) in sorted(sources.items()):
            entry = cached.get(key)
            if (isinstance(entry, dict) and entry.get("etag") == etag and entry.get("date") == day
                    and valid_measurements(entry.get("measurements"))):
                objects[key] = entry
                continue
            try:
                measurements = compact(day, store.get(key))
            except (OSError, EOFError, ValueError, TypeError):
                continue  # Invalid objects remain canonical read-through cases.
            if not valid_measurements(measurements):
                continue
            objects[key] = {"date": day, "etag": etag, "measurements": measurements}
        value = {"schema_version": 1, "kind": "weight-month-index", "month": month, "objects": objects}
        body = json_bytes(value)
        if len(body) > INDEX_LIMIT:
            result["months_skipped"].append(month)
        elif value == current:
            result["months_unchanged"].append(month)
        else:
            store.put(index_key(month), body, "application/json")
            result["months_written"].append(month)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    span = (args.end_date - args.start_date).days
    if not 0 <= span < 366:
        parser.error("Choose an ordered range of at most 366 days")
    result = sync_dates(R2Store(), [(args.start_date + timedelta(days=n)).isoformat() for n in range(span + 1)])
    print(json.dumps(result))


if __name__ == "__main__":
    main()
