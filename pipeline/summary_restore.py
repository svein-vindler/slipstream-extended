"""Restore the current summary datasets from private R2 before refreshing them."""

from __future__ import annotations

import gzip
from typing import Any

from .output_privacy import SafeArgumentParser, log_report, safe_cli

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


def restore_summaries(data_dir: str, store: Any) -> dict[str, Any]:
    """Restore the exact verified commit; the payload remains compatible."""
    from .summary_publication import restore_report

    result = restore_report(data_dir, store)
    log_report("summary_restore", result)
    return result


def restore_with_base(data_dir: str, store: Any, *, flow_id: str, allow_initial=False):
    from .summary_publication import restore_with_base as capture

    result = capture(data_dir, store, flow_id=flow_id, allow_initial=allow_initial)
    log_report("summary_restore", result.report)
    return result


def run(data_dir: str = "data") -> dict[str, Any]:
    from .r2_store import R2Store

    return restore_summaries(data_dir, R2Store())


@safe_cli
def main():
    parser = SafeArgumentParser(description="Restore Slipstream summaries from private R2.")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    run(args.data_dir)


if __name__ == "__main__":
    main()
