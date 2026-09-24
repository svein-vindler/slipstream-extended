"""Import existing raw Garmin exports into the canonical private R2 layout.

The importer is intentionally local-only.  It accepts raw JSON produced by
Garmin export tools, normalizes each day with the same helpers as live
backfills, skips existing objects, and relies on :class:`R2Store` for budget
enforcement.  Re-running the command is the resume mechanism.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .granular import gzip_json, normalize_hrv
from .health_detail import normalize_sleep_detail
from .health_detail_backfill import STREAMS
from .hrv_backfill import hrv_key
from .local_bootstrap import (
    load_env_file,
    local_process_lock,
    validate_environment,
)
from .r2_store import DEFAULT_MAX_WRITES_PER_RUN, R2BudgetError, R2Store

DEFAULT_STATUS_FILE = Path(".granular/local-import/status.json")
DEFAULT_LOCK_FILE = Path(".granular/local-bootstrap.lock")


@dataclass(frozen=True)
class ImportSpec:
    name: str
    metric_names: tuple[str, ...]
    prefix: str
    key: Callable[[str], str]
    day: Callable[[dict[str, Any]], str | None]
    normalize: Callable[[str, Any], dict[str, Any]]


def _iso_day(value: Any) -> str | None:
    raw = str(value or "")[:10]
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _hrv_day(raw: dict[str, Any]) -> str | None:
    summary = raw.get("hrvSummary")
    if isinstance(summary, dict):
        day = _iso_day(summary.get("calendarDate"))
        if day:
            return day
    return _iso_day(raw.get("calendarDate") or raw.get("startTimestampLocal"))


def _sleep_day(raw: dict[str, Any]) -> str | None:
    dto = raw.get("dailySleepDTO")
    if isinstance(dto, dict):
        day = _iso_day(dto.get("calendarDate"))
        if day:
            return day
    return _iso_day(raw.get("calendarDate"))


def _normalize_hrv_import(day: str, raw: Any) -> dict[str, Any]:
    payload = normalize_hrv(day, raw)
    payload["available"] = bool(payload["reading_count"])
    if not payload["available"]:
        payload["unavailable_reason"] = "garmin_returned_no_detailed_readings"
        payload["checked_at"] = datetime.now(timezone.utc).isoformat()
    return payload


SPECS = {
    "hrv": ImportSpec(
        name="hrv",
        metric_names=("hrv",),
        prefix="health/hrv/",
        key=hrv_key,
        day=_hrv_day,
        normalize=_normalize_hrv_import,
    ),
    "sleep": ImportSpec(
        name="sleep",
        metric_names=("sleep",),
        prefix="health/sleep/v1/",
        key=STREAMS["sleep"].object_key,
        day=_sleep_day,
        normalize=normalize_sleep_detail,
    ),
}


def read_export(path: Path, spec: ImportSpec) -> list[dict[str, Any]]:
    """Read either a raw list or a combined ``metric/data`` export."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read Garmin JSON export {path.name}: {exc}") from exc

    records: list[Any]
    if isinstance(value, list) and value and all(
        isinstance(item, dict) and "metric" in item and "data" in item
        for item in value
    ):
        records = []
        for wrapper in value:
            metric = str(wrapper.get("metric") or "").lower()
            if metric not in spec.metric_names:
                continue
            data = wrapper.get("data")
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)
    elif isinstance(value, list):
        records = value
    else:
        raise ValueError(
            f"Garmin JSON export {path.name} must contain a list of records"
        )
    return [item for item in records if isinstance(item, dict)]


def indexed_records(
    path: Path, spec: ImportSpec
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    """Index valid records by date without leaking source paths to R2."""
    indexed: dict[str, dict[str, Any]] = {}
    stats = {
        "source_records": 0,
        "source_errors": 0,
        "missing_dates": 0,
        "duplicate_dates": 0,
    }
    for raw in read_export(path, spec):
        stats["source_records"] += 1
        if raw.get("error"):
            stats["source_errors"] += 1
            continue
        day = spec.day(raw)
        if day is None:
            stats["missing_dates"] += 1
            continue
        if day in indexed:
            stats["duplicate_dates"] += 1
        indexed[day] = raw
    return indexed, stats


def import_file(
    path: Path,
    spec: ImportSpec,
    *,
    store: R2Store,
    limit: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    records, stats = indexed_records(path, spec)
    existing = store.list_keys(spec.prefix)
    missing = [
        day for day in sorted(records, reverse=True) if spec.key(day) not in existing
    ]
    selected = missing[:limit]
    completed = []
    failed = []
    for day in selected:
        try:
            payload = spec.normalize(day, records[day])
            data = gzip_json(payload)
            if not dry_run:
                store.put(spec.key(day), data, "application/json", encoding="gzip")
            completed.append({
                "date": day,
                "key": spec.key(day),
                "bytes": len(data),
            })
        except R2BudgetError:
            raise
        except Exception as exc:
            failed.append({
                "date": day,
                "error": str(exc),
            })

    imported = 0 if dry_run else len(completed)
    remaining = len(missing) - imported - len(failed)
    if dry_run:
        status = "dry_run"
    elif remaining == 0 and failed:
        status = "complete_with_errors"
    elif remaining == 0:
        status = "complete"
    else:
        status = "active"
    return {
        "stream": spec.name,
        "source_file": path.name,
        **stats,
        "dated_records": len(records),
        "already_present": len(records) - len(missing),
        "attempted_this_run": len(selected),
        "imported_this_run": imported,
        "would_import": len(completed) if dry_run else 0,
        "failed_this_run": failed,
        "remaining_records": remaining,
        "status": status,
    }


def run_import_cycle(
    *,
    files: dict[str, Path],
    batch_size: int = 200,
    dry_run: bool = False,
    store: R2Store | None = None,
) -> dict[str, dict[str, Any]]:
    if not 1 <= batch_size <= DEFAULT_MAX_WRITES_PER_RUN:
        raise ValueError(
            f"batch_size must be between 1 and {DEFAULT_MAX_WRITES_PER_RUN}"
        )
    store = store or R2Store()
    remaining_budget = batch_size
    results: dict[str, dict[str, Any]] = {}
    for name in ("hrv", "sleep"):
        path = files.get(name)
        if path is None:
            continue
        limit = batch_size if dry_run else remaining_budget
        result = import_file(
            path,
            SPECS[name],
            store=store,
            limit=limit,
            dry_run=dry_run,
        )
        results[name] = result
        if not dry_run:
            remaining_budget -= int(result["imported_this_run"])
            if remaining_budget <= 0:
                break
    return results


def _write_status(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def run_local_import(
    *,
    files: dict[str, Path],
    max_hours: float = 8,
    pause_seconds: float = 60,
    max_cycles: int = 0,
    batch_size: int = 200,
    dry_run: bool = False,
    status_file: Path = DEFAULT_STATUS_FILE,
    lock_file: Path = DEFAULT_LOCK_FILE,
    store_factory: Callable[[], R2Store] = R2Store,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if not files:
        raise ValueError("At least one Garmin export file is required")
    if max_hours <= 0:
        raise ValueError("max_hours must be greater than zero")
    if pause_seconds < 0:
        raise ValueError("pause_seconds cannot be negative")
    if max_cycles < 0:
        raise ValueError("max_cycles cannot be negative")
    if not 1 <= batch_size <= DEFAULT_MAX_WRITES_PER_RUN:
        raise ValueError(
            f"batch_size must be between 1 and {DEFAULT_MAX_WRITES_PER_RUN}"
        )

    deadline = monotonic() + max_hours * 3600
    cycles = 0
    latest: dict[str, dict[str, Any]] = {}
    reason = "dry_run" if dry_run else "time_limit"

    def status_payload(status: str, error: BaseException | None = None):
        payload: dict[str, Any] = {
            "schema_version": 1,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "cycles": cycles,
            "results": latest,
        }
        if error is not None:
            payload["error"] = {
                "type": type(error).__name__,
                "message": str(error),
            }
        _write_status(status_file, payload)
        return payload

    try:
        with local_process_lock(lock_file):
            while monotonic() < deadline:
                cycles += 1
                latest = run_import_cycle(
                    files=files,
                    batch_size=batch_size,
                    dry_run=dry_run,
                    store=store_factory(),
                )
                print(json.dumps(latest, indent=2, ensure_ascii=False), flush=True)
                if dry_run:
                    break
                if set(latest) == set(files) and all(
                    result.get("status") in {"complete", "complete_with_errors"}
                    for result in latest.values()
                ):
                    reason = "complete"
                    break
                if max_cycles and cycles >= max_cycles:
                    reason = "cycle_limit"
                    break
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                sleep(min(pause_seconds, remaining))
    except KeyboardInterrupt as exc:
        status_payload("interrupted", exc)
        raise
    except Exception as exc:
        status_payload("stopped_error", exc)
        raise
    return status_payload(reason)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import raw Garmin HRV/sleep JSON into canonical private R2 objects."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--hrv-file", type=Path)
    parser.add_argument("--sleep-file", type=Path)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-hours", type=float, default=8)
    parser.add_argument("--pause-seconds", type=float, default=60)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--confirm-cloud-jobs-paused",
        action="store_true",
        help="Confirm scheduled Garmin and coach workflows are disabled or idle.",
    )
    args = parser.parse_args()
    if not args.confirm_cloud_jobs_paused:
        parser.error(
            "pause the scheduled Garmin/coach workflows first, then pass "
            "--confirm-cloud-jobs-paused"
        )
    files = {
        name: path
        for name, path in (("hrv", args.hrv_file), ("sleep", args.sleep_file))
        if path is not None
    }
    try:
        load_env_file(args.env_file)
        validate_environment(())
        result = run_local_import(
            files=files,
            batch_size=args.batch_size,
            max_hours=args.max_hours,
            pause_seconds=args.pause_seconds,
            max_cycles=args.max_cycles,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        print("\n[local-import] stopped safely; rerun to resume.")
        return
    except (R2BudgetError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"Local import stopped safely: {exc}\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
