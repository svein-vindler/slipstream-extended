"""Small, scope-specific receipts; never advance a check after source failure."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

from .granular import json_bytes


def recent_day(value: str) -> str:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value or not date.today() - timedelta(days=7) <= parsed <= date.today() + timedelta(days=1):
        raise ValueError("Requested local day must be within the recent eight-day window")
    return value


def activity_scope(activity_id: str | None, expected_date: str | None, new_expected: bool) -> str:
    return f"activity/{expected_date or 'latest'}/{activity_id or 'latest'}/{'new' if new_expected else 'known'}"


def receipt(store, run_id: str, request_id: str | None, scope: str) -> None:
    if not run_id.isdigit():
        raise ValueError("run_id must contain digits only")
    if request_id is None:
        return
    if not re.fullmatch(r"[a-f0-9-]{36}", request_id):
        raise ValueError("Invalid request_id")
    store.put(f"refresh/requests/{request_id}.json", json_bytes({
        "run_id": int(run_id), "scope": scope,
    }), "application/json")


def publish(store, run_id: str, scope: str, report: dict, *, source_checked: bool = True) -> dict:
    report["scope"] = scope
    report["source_checked"] = source_checked
    report["checked_at"] = datetime.now(timezone.utc).isoformat()
    store.put(f"refresh/reports/{run_id}.json", json_bytes(report), "application/json")
    if source_checked:
        store.put(f"refresh/checks/v1/{scope}.json", json_bytes(report), "application/json")
    return report
