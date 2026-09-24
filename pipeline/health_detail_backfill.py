"""Resumable Garmin sleep and body-composition backfills into private R2."""

from __future__ import annotations

import argparse
import csv
import io
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from .activity_backfill import is_job_stopping_error
from .granular import gzip_json, json_bytes
from .health_detail import normalize_body_composition, normalize_sleep_detail
from .health_history_index import sync_dates
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login
from .summary_restore import decode_summary

MAX_DAYS_PER_STREAM = 100
MAX_RECENT_REFRESH_DAYS = 14
MAX_FAILURE_ATTEMPTS = 3


@dataclass(frozen=True)
class DetailStream:
    name: str
    summary_column: str
    plan_key: str
    prefix: str
    fetch: Callable[[Any, str], Any]
    normalize: Callable[[str, Any], dict[str, Any]]
    range_fetch: Callable[[Any, str, str], Any] | None = None

    def object_key(self, day: str) -> str:
        return f"{self.prefix}/{day[:4]}/{day[5:7]}/{day}.json"


STREAMS = {
    "sleep": DetailStream(
        name="sleep",
        summary_column="Sleep Seconds",
        plan_key="backfill/sleep/v1/plan.json",
        prefix="health/sleep/v1",
        fetch=lambda garmin, day: garmin.get_sleep_data(day),
        normalize=normalize_sleep_detail,
    ),
    "body_composition": DetailStream(
        name="body_composition",
        summary_column="Weight KG",
        plan_key="backfill/body-composition/v1/plan.json",
        prefix="health/body-composition/v1",
        fetch=lambda garmin, day: garmin.get_body_composition(day),
        normalize=normalize_body_composition,
        range_fetch=lambda garmin, start, end: garmin.get_body_composition(start, end),
    ),
}


def date_windows(days: list[str], *, max_span_days: int = 31) -> list[tuple[str, str, list[str]]]:
    """Group sparse target dates into newest-first bounded calendar windows."""
    if max_span_days <= 0:
        raise ValueError("max_span_days must be greater than zero")
    parsed = sorted({date.fromisoformat(day) for day in days}, reverse=True)
    windows: list[tuple[str, str, list[str]]] = []
    current: list[date] = []
    newest: date | None = None
    for day in parsed:
        if newest is None or (newest - day).days < max_span_days:
            current.append(day)
            newest = newest or day
            continue
        windows.append((current[-1].isoformat(), current[0].isoformat(), [item.isoformat() for item in current]))
        current = [day]
        newest = day
    if current:
        windows.append((current[-1].isoformat(), current[0].isoformat(), [item.isoformat() for item in current]))
    return windows


def history_dates(health_csv: bytes, column: str) -> list[str]:
    raw = decode_summary(health_csv, "Date,")
    rows = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    dates = set()
    for row in rows:
        value = str(row.get("Date") or "")[:10]
        if not str(row.get(column) or "").strip():
            continue
        try:
            dates.add(date.fromisoformat(value).isoformat())
        except ValueError:
            continue
    return sorted(dates, reverse=True)


def new_plan(stream: DetailStream, health_csv: bytes) -> dict[str, Any]:
    dates = history_dates(health_csv, stream.summary_column)
    now = datetime.now(timezone.utc).isoformat()
    plan: dict[str, Any] = {
        "schema_version": 1,
        "kind": f"{stream.name}-backfill-plan",
        "created_at": now,
        "status": "active" if dates else "complete",
        "target_dates": dates,
        "total_days": len(dates),
        "complete_days": 0,
        "remaining_days": len(dates),
        "failures": {},
    }
    if dates:
        plan.update({"history_start": dates[-1], "history_end": dates[0]})
    else:
        plan["completed_at"] = now
    return plan


def _load_json(store: R2Store, key: str) -> dict[str, Any]:
    try:
        value = json.loads(store.get(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"R2 JSON object is invalid: {key}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"R2 JSON object must contain an object: {key}")
    return value


def _refresh_plan_dates(
    stream: DetailStream,
    plan: dict[str, Any],
    health_csv: bytes,
) -> bool:
    current = history_dates(health_csv, stream.summary_column)
    stored = plan.get("target_dates")
    if not isinstance(stored, list) or not all(isinstance(day, str) for day in stored):
        raise ValueError(f"R2 {stream.name} backfill plan has invalid target dates")
    merged = sorted(set(stored).union(current), reverse=True)
    if merged == stored:
        return False
    plan.update({
        "status": "active",
        "target_dates": merged,
        "total_days": len(merged),
        "remaining_days": len(merged),
    })
    if merged:
        plan.update({"history_start": merged[-1], "history_end": merged[0]})
    plan.pop("completed_at", None)
    return True


def select_batch(
    stream: DetailStream,
    target_dates: list[str],
    existing_keys: set[str],
    failures: dict[str, dict[str, Any]],
    *,
    limit: int,
    retry_failures: bool = False,
    refresh_dates: frozenset[str] = frozenset(),
) -> list[str]:
    selected = []
    for day in target_dates:
        if stream.object_key(day) in existing_keys and day not in refresh_dates:
            continue
        attempts = int(failures.get(day, {}).get("attempts", 0))
        if attempts >= MAX_FAILURE_ATTEMPTS and not retry_failures:
            continue
        selected.append(day)
        if len(selected) == limit:
            break
    return selected


def _counts(
    stream: DetailStream,
    target_dates: list[str],
    existing_keys: set[str],
    failures: dict[str, dict[str, Any]],
) -> tuple[int, int, int]:
    complete = sum(stream.object_key(day) in existing_keys for day in target_dates)
    blocked = sum(
        stream.object_key(day) not in existing_keys
        and int(failures.get(day, {}).get("attempts", 0)) >= MAX_FAILURE_ATTEMPTS
        for day in target_dates
    )
    return len(target_dates), complete, blocked


def run_stream(
    stream: DetailStream,
    *,
    health_csv: bytes,
    max_days: int,
    store: R2Store,
    get_garmin: Callable[[], Any],
    retry_failures: bool = False,
    refresh_recent_days: int = 0,
    request_pause: float = 0.15,
) -> dict[str, Any]:
    if not 1 <= max_days <= MAX_DAYS_PER_STREAM:
        raise ValueError(
            f"max_days must be between 1 and {MAX_DAYS_PER_STREAM} for {stream.name}"
        )
    if not 0 <= refresh_recent_days <= MAX_RECENT_REFRESH_DAYS:
        raise ValueError(
            "refresh_recent_days must be between 0 and "
            f"{MAX_RECENT_REFRESH_DAYS} for {stream.name}"
        )

    progress_keys = store.list_keys(stream.plan_key.rsplit("/", 1)[0] + "/")
    if stream.plan_key in progress_keys:
        plan = _load_json(store, stream.plan_key)
        previous_status = plan.get("status")
        changed = _refresh_plan_dates(stream, plan, health_csv)
        if previous_status in {"complete", "complete_with_blocked"}:
            retrying = previous_status == "complete_with_blocked" and retry_failures
            if not changed and not retrying and refresh_recent_days == 0:
                passive = dict(plan)
                passive.update({
                    "attempted_this_run": 0,
                    "completed_this_run": [],
                    "failed_this_run": [],
                })
                return passive
            plan["status"] = "active"
            plan.pop("completed_at", None)
    else:
        plan = new_plan(stream, health_csv)
        if plan["status"] == "complete":
            store.put(stream.plan_key, json_bytes(plan), "application/json")
            return plan

    target_dates = plan.get("target_dates")
    if not isinstance(target_dates, list) or not all(
        isinstance(day, str) for day in target_dates
    ):
        raise ValueError(f"R2 {stream.name} backfill plan has invalid target dates")
    failures = plan.get("failures")
    if not isinstance(failures, dict):
        failures = {}
    refresh_dates = frozenset(target_dates[:refresh_recent_days])

    existing_keys = store.list_keys(stream.prefix + "/")
    batch = select_batch(
        stream,
        target_dates,
        existing_keys,
        failures,
        limit=max_days,
        retry_failures=retry_failures,
        refresh_dates=refresh_dates,
    )
    completed = []
    failed = []
    refreshed_existing = 0

    def record_failure(day: str, exc: Exception) -> None:
        attempts = int(failures.get(day, {}).get("attempts", 0)) + 1
        failure = {
            "attempts": attempts,
            "last_error": str(exc),
            "last_attempt": datetime.now(timezone.utc).isoformat(),
        }
        failures[day] = failure
        failed.append({"date": day, **failure})

    def store_day(day: str, raw: Any) -> None:
        nonlocal refreshed_existing
        payload = stream.normalize(day, raw)
        data = gzip_json(payload)
        key = stream.object_key(day)
        replacing = key in existing_keys
        store.put(key, data, "application/json", encoding="gzip")
        existing_keys.add(key)
        if replacing:
            refreshed_existing += 1
        failures.pop(day, None)
        completed.append({
            "date": day,
            "bytes": len(data),
            "items": payload.get("stage_count")
            if stream.name == "sleep"
            else payload.get("measurement_count"),
        })

    if batch:
        garmin = get_garmin()
        if stream.range_fetch is not None:
            windows = date_windows(batch)
            for index, (start, end, days) in enumerate(windows):
                try:
                    raw = stream.range_fetch(garmin, start, end)
                except Exception as exc:
                    if is_job_stopping_error(exc):
                        raise RuntimeError(
                            "Garmin service or authentication error; stopping this "
                            f"{stream.name} batch without counting a date failure"
                        ) from exc
                    for day in days:
                        record_failure(day, exc)
                    continue
                for day in days:
                    try:
                        store_day(day, raw)
                    except R2BudgetError:
                        raise
                    except Exception as exc:
                        record_failure(day, exc)
                if request_pause and index + 1 < len(windows):
                    time.sleep(request_pause)
        else:
            for index, day in enumerate(batch):
                try:
                    store_day(day, stream.fetch(garmin, day))
                    if request_pause and index + 1 < len(batch):
                        time.sleep(request_pause)
                except R2BudgetError:
                    raise
                except Exception as exc:
                    if is_job_stopping_error(exc):
                        raise RuntimeError(
                            "Garmin service or authentication error; stopping this "
                            f"{stream.name} batch without counting a date failure"
                        ) from exc
                    record_failure(day, exc)

    total, complete, blocked = _counts(stream, target_dates, existing_keys, failures)
    if (
        stream.name == "sleep"
        and completed
        and hasattr(store, "list_object_revisions")
    ):
        sync_dates(store, "sleep", (item["date"] for item in completed))
    remaining = total - complete
    status = (
        "complete"
        if remaining == 0
        else "complete_with_blocked"
        if remaining <= blocked
        else "active"
    )
    now = datetime.now(timezone.utc).isoformat()
    plan.update({
        "status": status,
        "updated_at": now,
        "total_days": total,
        "complete_days": complete,
        "remaining_days": remaining,
        "blocked_after_three_failures": blocked,
        "attempted_this_run": len(batch),
        "completed_this_run": completed,
        "refreshed_existing_this_run": refreshed_existing,
        "failed_this_run": failed,
        "failures": failures,
    })
    if status in {"complete", "complete_with_blocked"}:
        plan["completed_at"] = now
    store.put(stream.plan_key, json_bytes(plan), "application/json")
    return plan


def run(
    *,
    max_sleep_days: int = 100,
    max_body_days: int = 100,
    retry_failures: bool = False,
    refresh_recent_days: int = 0,
    store: R2Store | None = None,
    garmin=None,
) -> dict[str, Any]:
    store = store or R2Store()
    health_csv = store.get("summary/health_daily.csv")
    shared = garmin

    def get_garmin():
        nonlocal shared
        if shared is None:
            shared = _login()
        return shared

    result = {
        "schema_version": 1,
        "sleep": run_stream(
            STREAMS["sleep"],
            health_csv=health_csv,
            max_days=max_sleep_days,
            store=store,
            get_garmin=get_garmin,
            retry_failures=retry_failures,
            refresh_recent_days=refresh_recent_days,
        ),
        "body_composition": run_stream(
            STREAMS["body_composition"],
            health_csv=health_csv,
            max_days=max_body_days,
            store=store,
            get_garmin=get_garmin,
            retry_failures=retry_failures,
            refresh_recent_days=refresh_recent_days,
        ),
    }
    report = {
        name: {
            key: value
            for key, value in plan.items()
            if key != "target_dates"
        }
        for name, plan in result.items()
        if isinstance(plan, dict)
    }
    report["schema_version"] = result["schema_version"]
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Advance detailed Garmin sleep and body-composition backfills."
    )
    parser.add_argument("--max-sleep-days", type=int, default=100)
    parser.add_argument("--max-body-days", type=int, default=100)
    parser.add_argument("--retry-failures", action="store_true")
    parser.add_argument("--refresh-recent-days", type=int, default=0)
    args = parser.parse_args()
    try:
        run(
            max_sleep_days=args.max_sleep_days,
            max_body_days=args.max_body_days,
            retry_failures=args.retry_failures,
            refresh_recent_days=args.refresh_recent_days,
        )
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
