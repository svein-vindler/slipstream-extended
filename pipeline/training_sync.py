"""Optional single-day collection. Shared login is supplied, never initiated here."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from .r2_store import R2BudgetError, R2ReadBoundaryError, missing_object
from .sources.garmin_readonly import GarminReadOnlyViolation
from .sources.training_readiness import ReadinessRead
from .training_day import (
    MAX_DAY_BYTES,
    configuration,
    day_key,
    decode_day,
    encode_day,
    merge_day,
    normalize,
)


def run(*, store=None, garmin=None, config=None, env=None, now=None):
    config = config or configuration(os.environ if env is None else env, now=now)
    result = {"kind": "training-collection", "status": config["status"],
              "fitness_http_attempts": 0, "component_auth_http_attempts": 0,
              "provider_body_bytes": 0, "day_read_bytes": 0, "day_write_bytes": 0}
    if config["status"] != "enabled":
        return result
    key = day_key(config["date"])
    previous = None
    read_before = getattr(store, "bounded_read_bytes", None)
    try:
        body = store.get_bounded(key, MAX_DAY_BYTES)
        result["day_read_bytes"] = len(body)
        previous = decode_day(body, config["date"])
    except ClientError as error:
        if not missing_object(error):
            result["status"] = "source_error"
            return result
    except R2ReadBoundaryError as error:
        result["status"] = error.status
        return result
    except (ValueError, RecursionError):
        result["status"] = "schema_unverified"
        return result
    except R2BudgetError:
        raise
    except Exception:
        result["status"] = "source_error"
        return result
    finally:
        result["day_read_bytes"] = store.bounded_read_bytes - read_before if read_before is not None else None
    # A same-day zone change must not relabel retained clocks/provenance. Keep
    # the canonical object intact; no provider fetch or automatic migration.
    if previous is not None and previous["date_resolution"]["timezone"] != config["timezone"]:
        result["status"] = "configuration_required"
        return result
    # Never use _login/provider.get as a fallback: reuse already shared auth.
    read = garmin.get_training_readiness_bounded(config["date"]) if garmin is not None else ReadinessRead("auth_required")
    result.update(fitness_http_attempts=read.metrics["fitness_http_attempts"],
                  component_auth_http_attempts=read.metrics["component_auth_http_attempts"],
                  provider_body_bytes=read.metrics["body_bytes"], provider_elapsed_ms=read.metrics["elapsed_ms"])
    status, devices = read.status, []
    checked = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    if status == "available":
        try:
            devices, status = normalize(read.payload, config, checked)
        except ValueError as error:
            status = "wrong_date" if str(error) == "Wrong training date" else "schema_unverified"
    try:
        day = merge_day(previous, devices, status, config, checked)
        body = encode_day(day)
    except ValueError:
        result["status"] = "schema_unverified"
        return result
    result["status"] = status
    put_before = getattr(store, "operations", {}).get("put")
    try:
        changed = store.put_if_changed(key, body, "application/json")
        result["day_write_bytes"] = len(body) if changed else 0
    except (R2BudgetError, GarminReadOnlyViolation):
        raise
    except Exception:
        result["status"] = "publication_unknown"
    finally:
        if put_before is not None:
            result["day_write_bytes"] = (store.operations["put"] - put_before) * len(body)
        elif result["status"] == "publication_unknown":
            result["day_write_bytes"] = None
    return result
