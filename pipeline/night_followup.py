"""Reconcile the current local night inside an existing scheduled Garmin job."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

from . import recent_health
from .granular import sha256
from .health_sync import publish_completed_nights
from .r2_store import missing_object


def _stored_current_night(store, day: str, now: datetime) -> bool:
    """Reuse only source-check receipts bound to the exact canonical bytes."""
    records: dict[str, list[dict[str, Any]]] = {}
    for stream, root in (("sleep", "health/sleep/v1"), ("hrv", "health/hrv")):
        key = f"{root}/{day[:4]}/{day[5:7]}/{day}.json"
        try:
            proof = json.loads(store.get_bounded(
                f"refresh/checks/v1/health/{stream}/{day}.json", 64 * 1024,
            ))
        except ClientError as exc:
            if not missing_object(exc):
                raise
            return False
        if (not isinstance(proof, dict)
                or proof.get("schema_version") != 1
                or proof.get("kind") != "health-source-check"
                or proof.get("status") != "ready"
                or proof.get("stream") != stream or proof.get("date") != day
                or proof.get("scope") != f"health/{stream}/{day}"
                or proof.get("source_key") != key
                or not isinstance(proof.get("source_sha256"), str)
                or re.fullmatch(r"[a-f0-9]{64}", proof["source_sha256"]) is None):
            raise ValueError("Stored night source check is invalid")
        checked = datetime.fromisoformat(proof["checked_at"].replace("Z", "+00:00"))
        if checked.tzinfo is None or checked.utcoffset() is None or checked > now:
            raise ValueError("Stored night source-check time is invalid")
        try:
            data = store.get_snapshot(key, max_bytes=4 * 1024 * 1024).data
        except ClientError as exc:
            if not missing_object(exc):
                raise
            return False
        if sha256(data) != proof["source_sha256"]:
            return False
        records[stream] = [proof]
    # Finish an interrupted final publication from verified components, retaining
    # their original source-check time. This never claims a new Garmin check.
    try:
        night = json.loads(store.get_bounded(
            f"refresh/checks/v1/night/{day}.json", 64 * 1024,
        ))
    except ClientError as exc:
        if not missing_object(exc):
            raise
        publish_completed_nights(store, records)
        return True
    if (not isinstance(night, dict) or night.get("schema_version") != 1
            or night.get("kind") != "latest-night" or night.get("wake_date") != day
            or night.get("scope") != f"night/{day}"
            or night.get("source_checked") is not True):
        raise ValueError("Stored completed night check is invalid")
    checked = datetime.fromisoformat(night["checked_at"].replace("Z", "+00:00"))
    if checked.tzinfo is None or checked.utcoffset() is None or checked > now:
        raise ValueError("Stored completed night-check time is invalid")
    states = (night.get("sleep_status"), night.get("hrv_status"))
    if (night.get("status") == "pending" and states == ("stored", "stored")
            and night.get("heart_rate_status") == "garmin_not_ready"):
        # Reuse the verified sleep/HRV pair; only HR still needs a genuine check.
        return True
    if (night.get("status") == "pending"
            and all(state in ("stored", "garmin_not_ready") for state in states)
            and "garmin_not_ready" in states):
        # A later genuine source check can be pending while good old component
        # bytes remain. Reconcile again; never promote old proofs over that check.
        return False
    if night.get("status") != "stored" or states != ("stored", "stored"):
        raise ValueError("Stored completed night state is invalid")
    return True


def run_command(
    operation: Callable[..., Any], *, store_factory, login: Callable[[], Any],
    now: datetime | None = None, **kwargs,
):
    if os.environ.get("GITHUB_EVENT_NAME") != "schedule":
        return operation(**kwargs)
    store = store_factory()
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Scheduled night checks need an aware clock")
    local_day = instant.astimezone(ZoneInfo(os.environ.get("HEALTH_TIMEZONE") or "UTC")).date()
    garmin = None
    def source():
        nonlocal garmin
        garmin = garmin or login()
        return garmin

    if not _stored_current_night(store, local_day.isoformat(), instant):
        night = recent_health.run(days=1, store=store, garmin=source(), today=local_day,
                                  night_references=True)
    else:
        from .latest_night import collect_heart_rate, refresh_references

        hr = collect_heart_rate(store, garmin, [local_day.isoformat()],
            timezone=os.environ.get("HEALTH_TIMEZONE"), reuse_checks=True, now=instant, login=source)
        night = {"heart_rate": hr, "baseline_refresh": (
            {"status": "not_run_source_failure"} if hr["status"] == "import_error"
            else refresh_references(store, [local_day.isoformat()]))}
    result = operation(store=store, garmin=garmin, **kwargs)
    if isinstance(result, dict):
        result["night_followup"] = night
        result["baseline_refresh"] = night["baseline_refresh"]
    return result
