"""Safely re-fetch selected existing sleep/HRV nights after local-time changes.

Date ranges belong in an ignored local file, never in the repository. A dry run
shows the exact existing R2 objects that still lack Garmin local timestamps.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .activity_backfill import is_job_stopping_error
from .granular import gzip_json, normalize_hrv
from .health_detail import normalize_sleep_detail
from .health_detail_backfill import STREAMS
from .health_history_index import sync_dates
from .hrv_backfill import hrv_key
from .r2_store import R2Store
from .sources.garmin import _login

MAX_TARGET_DAYS = 250
MAX_APPLY_OBJECTS = 40
STREAMS_TO_REPAIR = ("sleep", "hrv")


def dates_from_file(path: Path, *, margin_days: int = 1) -> list[str]:
    """Accept one ISO wake-date or inclusive START..END range per line."""
    if not 0 <= margin_days <= 2:
        raise ValueError("margin_days must be between 0 and 2")
    result: set[str] = set()
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        bounds = line.split("..")
        if len(bounds) not in (1, 2):
            raise ValueError(f"invalid date range on line {number}")
        try:
            start = date.fromisoformat(bounds[0].strip())
            end = date.fromisoformat(bounds[-1].strip())
        except ValueError as exc:
            raise ValueError(f"invalid ISO date on line {number}") from exc
        if end < start:
            raise ValueError(f"reversed date range on line {number}")
        if (end - start).days + 1 > MAX_TARGET_DAYS:
            raise ValueError(f"date range too large on line {number}")
        current = start - timedelta(days=margin_days)
        through = end + timedelta(days=margin_days)
        while current <= through:
            result.add(current.isoformat())
            current += timedelta(days=1)
        if len(result) > MAX_TARGET_DAYS:
            raise ValueError(f"more than {MAX_TARGET_DAYS} target days")
    if not result:
        raise ValueError("date file contains no dates")
    return sorted(result)


def object_key(stream: str, day: str) -> str:
    if stream == "sleep":
        return STREAMS["sleep"].object_key(day)
    if stream == "hrv":
        return hrv_key(day)
    raise ValueError(f"unsupported stream: {stream}")


def _json_object(data: bytes) -> dict[str, Any]:
    decoded = gzip.decompress(data) if data.startswith(b"\x1f\x8b") else data
    value = json.loads(decoded)
    if not isinstance(value, dict):
        raise ValueError("R2 night object is not a JSON object")
    return value


def has_local_window(payload: dict[str, Any]) -> bool:
    return all(payload.get(field) is not None for field in (
        "sleep_start_gmt", "sleep_end_gmt",
        "sleep_start_garmin_local", "sleep_end_garmin_local",
    ))


def _load_skips(path: Path) -> dict[tuple[str, str], str]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError("local skipped-night state must contain a list")
    skips = {}
    for item in value:
        if not isinstance(item, dict) or item.get("stream") not in STREAMS_TO_REPAIR:
            raise ValueError("local skipped-night state is invalid")
        day = item.get("date")
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
            raise ValueError("local skipped-night date is invalid")
        skips[(item["stream"], day)] = str(item.get("reason") or "unavailable")
    return skips


def _save_skips(path: Path, skips: dict[tuple[str, str], str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"stream": stream, "date": day, "reason": reason}
            for (stream, day), reason in sorted(skips.items())]
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def plan(
    store: R2Store, days: list[str], *,
    skipped: dict[tuple[str, str], str] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, int]]:
    skipped = skipped or {}
    existing = {
        "sleep": store.list_keys("health/sleep/v1/"),
        "hrv": store.list_keys("health/hrv/"),
    }
    counts = {"target_days": len(days), "existing_sleep": 0, "existing_hrv": 0,
              "already_local": 0, "unavailable_from_garmin": 0,
              "missing_objects": 0}
    pending: list[tuple[str, str]] = []
    for day in days:
        for stream in STREAMS_TO_REPAIR:
            key = object_key(stream, day)
            if key not in existing[stream]:
                counts["missing_objects"] += 1
                continue
            counts[f"existing_{stream}"] += 1
            payload = _json_object(store.get(key))
            if has_local_window(payload):
                counts["already_local"] += 1
            elif (stream, day) in skipped:
                counts["unavailable_from_garmin"] += 1
            else:
                pending.append((stream, day))
    return pending, counts


def _instant(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        if value.isdigit():
            return _instant(int(value))
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            return None
    return None


def valid_local_window(payload: dict[str, Any]) -> bool:
    """Reject absent or implausible Garmin wall-clock offsets before overwrite."""
    if not has_local_window(payload):
        return False
    for end in ("start", "end"):
        gmt = _instant(payload[f"sleep_{end}_gmt"])
        local = _instant(payload[f"sleep_{end}_garmin_local"])
        if gmt is None or local is None:
            return False
        if abs((local - gmt).total_seconds()) > 14 * 3600:
            return False
    return True


def _normalized(stream: str, day: str, raw: Any) -> dict[str, Any]:
    if stream == "sleep":
        source = raw if isinstance(raw, dict) else {}
        dto = source.get("dailySleepDTO")
        if isinstance(dto, dict) and dto.get("calendarDate") not in (None, day):
            raise ValueError("Garmin sleep wake-date differs from requested date")
        return normalize_sleep_detail(day, raw)
    source = raw if isinstance(raw, dict) else {}
    summary = source.get("hrvSummary")
    if isinstance(summary, dict) and summary.get("calendarDate") not in (None, day):
        raise ValueError("Garmin HRV wake-date differs from requested date")
    payload = normalize_hrv(day, raw)
    if not payload["reading_count"]:
        raise ValueError("Garmin returned no detailed HRV readings")
    return payload


def apply(
    store: R2Store,
    garmin: Any,
    pending: list[tuple[str, str]],
    *,
    backup_dir: Path,
    max_objects: int = 20,
    request_pause: float = 0.25,
    skipped_state: dict[tuple[str, str], str] | None = None,
) -> dict[str, Any]:
    if not 1 <= max_objects <= MAX_APPLY_OBJECTS:
        raise ValueError(f"max_objects must be between 1 and {MAX_APPLY_OBJECTS}")
    if not 0 <= request_pause <= 10:
        raise ValueError("request_pause must be between 0 and 10")
    backup_dir.mkdir(parents=True, exist_ok=True)
    state_path = backup_dir / "skipped.json"
    skipped_state = skipped_state if skipped_state is not None else _load_skips(state_path)
    completed: dict[str, list[str]] = {"sleep": [], "hrv": []}
    skipped: list[dict[str, str]] = []
    try:
        for stream, day in pending[:max_objects]:
            key = object_key(stream, day)
            try:
                raw = garmin.get_sleep_data(day) if stream == "sleep" else garmin.get_hrv_data(day)
                payload = _normalized(stream, day, raw)
                if not valid_local_window(payload):
                    raise ValueError("Garmin local sleep window is absent or invalid")
            except Exception as exc:
                if is_job_stopping_error(exc):
                    raise RuntimeError("Garmin service or authentication error; batch stopped") from exc
                skipped.append({"stream": stream, "date": day, "reason": str(exc)})
                skipped_state[(stream, day)] = str(exc)
                _save_skips(state_path, skipped_state)
                if request_pause:
                    time.sleep(request_pause)
                continue
            previous = store.get(key)
            backup_path = backup_dir / key
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            if not backup_path.exists():
                with backup_path.open("xb") as file:
                    file.write(previous)
            store.put(key, gzip_json(payload), "application/json", encoding="gzip")
            completed[stream].append(day)
            if (stream, day) in skipped_state:
                skipped_state.pop((stream, day))
                _save_skips(state_path, skipped_state)
            if request_pause:
                time.sleep(request_pause)
    finally:
        for stream, dates in completed.items():
            if dates:
                sync_dates(store, stream, dates)
    return {"completed": completed, "skipped": skipped}


def main() -> None:
    from .local_bootstrap import load_env_file, prepare_environment

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dates-file", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env.local-bootstrap"))
    parser.add_argument("--garmin-token-file", type=Path)
    parser.add_argument("--margin-days", type=int, default=1)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--sync-index", action="store_true",
                        help="Reconcile months after an interrupted run without refetching")
    parser.add_argument("--retry-skipped", action="store_true",
                        help="Retry dates previously lacking valid Garmin local times")
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--max-objects", type=int, default=20)
    parser.add_argument("--request-pause", type=float, default=0.25)
    args = parser.parse_args()
    if not args.dates_file.name.endswith(".local"):
        parser.error("date lists must use an ignored *.local filename")
    if args.apply and args.sync_index:
        parser.error("choose --apply or --sync-index, not both")
    if args.apply and args.backup_dir is None:
        parser.error("--backup-dir is required with --apply")
    if args.apply and ".granular" not in args.backup_dir.parts:
        parser.error("--backup-dir must be inside the ignored .granular directory")
    if not 1 <= args.max_objects <= MAX_APPLY_OBJECTS:
        parser.error(f"--max-objects must be between 1 and {MAX_APPLY_OBJECTS}")
    if args.apply:
        prepare_environment(args.env_file, args.garmin_token_file)
    else:
        load_env_file(args.env_file)
    days = dates_from_file(args.dates_file, margin_days=args.margin_days)
    store = R2Store()
    skipped_state = _load_skips(args.backup_dir / "skipped.json") if args.backup_dir else {}
    pending, counts = plan(store, days, skipped={} if args.retry_skipped else skipped_state)
    mode = "apply" if args.apply else "sync_index" if args.sync_index else "dry_run"
    print(json.dumps({"mode": mode,
                      "bucket": store.bucket, "counts": counts,
                      "pending": len(pending), "selected": pending[:args.max_objects]},
                     indent=2))
    if args.apply and pending:
        result = apply(store, _login(), pending, backup_dir=args.backup_dir,
                       max_objects=args.max_objects, request_pause=args.request_pause,
                       skipped_state=skipped_state)
        print(json.dumps(result, indent=2))
    elif args.sync_index:
        synced = {}
        for stream in STREAMS_TO_REPAIR:
            prefix = "health/sleep/v1/" if stream == "sleep" else "health/hrv/"
            keys = store.list_keys(prefix)
            stored_days = [day for day in days if object_key(stream, day) in keys]
            if stored_days:
                synced[stream] = sync_dates(store, stream, stored_days)
        print(json.dumps({"synced": synced}, indent=2))


if __name__ == "__main__":
    main()
