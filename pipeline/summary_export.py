"""Upload compact summary datasets and daily recovery snapshots to private R2."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from .granular import gzip_bytes, json_bytes, sha256
from .r2_store import missing_object

SUMMARY_FILES = (
    ("activities.csv", "summary/activities.csv"),
    ("health_daily.csv", "summary/health_daily.csv"),
)


def build_summary_exports(
    data_dir: str,
    *,
    snapshot_day: date | None = None,
    generated_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = Path(data_dir)
    created = generated_at or datetime.now(timezone.utc)
    day = snapshot_day or created.astimezone(timezone.utc).date()
    snapshot_prefix = f"summary/snapshots/{day:%Y/%m/%d}"
    objects: list[dict[str, Any]] = []
    manifest_files: list[dict[str, Any]] = []

    for filename, current_key in SUMMARY_FILES:
        source = root / filename
        raw = source.read_bytes()
        compressed = gzip_bytes(raw)
        snapshot_key = f"{snapshot_prefix}/{filename}"
        for key in (current_key, snapshot_key):
            objects.append({
                "key": key,
                "data": compressed,
                "content_type": "text/csv; charset=utf-8",
                "encoding": "gzip",
            })
        manifest_files.append({
            "name": filename,
            "current_key": current_key,
            "snapshot_key": snapshot_key,
            "rows": max(len(raw.decode("utf-8").splitlines()) - 1, 0),
            "uncompressed_bytes": len(raw),
            "compressed_bytes": len(compressed),
            "sha256": sha256(raw),
        })

    manifest = {
        "schema_version": 1,
        "generated_at": created.astimezone(timezone.utc).isoformat(),
        "snapshot_date": day.isoformat(),
        "files": manifest_files,
    }
    objects.append({
        "key": "summary/manifest.json",
        "data": json_bytes(manifest),
        "content_type": "application/json",
        "encoding": None,
    })
    return objects, manifest


def run(data_dir: str = "data", *, store=None) -> dict[str, Any]:
    from .r2_store import R2Store

    objects, manifest = build_summary_exports(data_dir)
    store = store or R2Store()
    try:
        previous_bytes = store.get("summary/manifest.json")
    except KeyError:  # In-memory adapters use KeyError for a missing object.
        previous_bytes = None
    except ClientError as exc:
        if not missing_object(exc):
            raise
        previous_bytes = None
    try:
        previous = json.loads(previous_bytes) if previous_bytes is not None else None
    except (ValueError, UnicodeDecodeError):
        previous = None
    try:
        previous_time = datetime.fromisoformat(previous["generated_at"]) if isinstance(previous, dict) else None
    except (KeyError, TypeError, ValueError):
        previous_time = None
    if (isinstance(previous, dict) and previous.get("schema_version") == 1
            and previous.get("snapshot_date") == manifest["snapshot_date"]
            and previous.get("files") == manifest["files"]
            and previous_time is not None and previous_time.tzinfo is not None):
        manifest = previous
        objects[-1]["data"] = previous_bytes
    written = 0
    write = getattr(store, "put_if_changed", None)
    for item in objects:
        if callable(write):
            written += bool(write(item["key"], item["data"], item["content_type"], encoding=item["encoding"]))
        else:
            store.put(item["key"], item["data"], item["content_type"], encoding=item["encoding"])
            written += 1
    print(json.dumps({"event": "summary_export", "objects_written": written,
                      "objects_unchanged": len(objects) - written}))
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Upload Slipstream summaries to private R2.")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    run(args.data_dir)


if __name__ == "__main__":
    main()
