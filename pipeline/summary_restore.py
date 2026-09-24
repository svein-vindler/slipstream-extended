"""Restore the current summary datasets from private R2 before refreshing them."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any

from .granular import sha256

SUMMARY_FILES = (
    ("activities.csv", "summary/activities.csv", "Activity ID,"),
    ("health_daily.csv", "summary/health_daily.csv", "Date,"),
)


def decode_summary(data: bytes, expected_header: str) -> bytes:
    """Decode a possibly gzipped CSV and reject malformed summary files."""
    if data.startswith(b"\x1f\x8b"):
        try:
            data = gzip.decompress(data)
        except (EOFError, OSError) as exc:
            raise ValueError("R2 summary contains invalid gzip data") from exc

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("R2 summary is not valid UTF-8") from exc

    first_line = text.splitlines()[0] if text.splitlines() else ""
    if not first_line.startswith(expected_header):
        raise ValueError(
            f"R2 summary has an unexpected CSV header: {first_line!r}"
        )
    return data


def _manifest_files(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("files"), list):
        raise ValueError("R2 summary manifest has an unsupported format")
    return {
        item["name"]: item
        for item in manifest["files"]
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }


def restore_summaries(data_dir: str, store: Any) -> dict[str, Any]:
    """Validate both R2 summaries, then replace the local copies atomically."""
    try:
        manifest = json.loads(store.get("summary/manifest.json"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("R2 summary manifest is not valid JSON") from exc

    manifest_files = _manifest_files(manifest)
    restored: list[tuple[Path, bytes]] = []

    for filename, key, expected_header in SUMMARY_FILES:
        metadata = manifest_files.get(filename)
        if not metadata or metadata.get("current_key") != key:
            raise ValueError(f"R2 manifest is missing metadata for {filename}")

        raw = decode_summary(store.get(key), expected_header)
        if sha256(raw) != metadata.get("sha256"):
            raise ValueError(f"R2 checksum mismatch for {filename}")
        restored.append((Path(data_dir) / filename, raw))

    Path(data_dir).mkdir(parents=True, exist_ok=True)
    for destination, raw in restored:
        temporary = destination.with_name(f".{destination.name}.tmp")
        temporary.write_bytes(raw)
        temporary.replace(destination)

    result = {
        "generated_at": manifest.get("generated_at"),
        "files": [
            {"name": path.name, "rows": max(len(raw.splitlines()) - 1, 0)}
            for path, raw in restored
        ],
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def run(data_dir: str = "data") -> dict[str, Any]:
    from .r2_store import R2Store

    return restore_summaries(data_dir, R2Store())


def main():
    parser = argparse.ArgumentParser(description="Restore Slipstream summaries from private R2.")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    run(args.data_dir)


if __name__ == "__main__":
    main()
