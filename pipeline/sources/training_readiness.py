"""Bounded parsing only; the private sender is installed by the read-only guard.

No login, token refresh, retries, SDK error translation or logging occurs here.
Timeouts are cooperative I/O limits, not a hard wall-time guarantee.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

import requests
from urllib3.exceptions import HTTPError

from .garmin_policy import GarminReadOnlyViolation

MAX_BODY_BYTES = 256 * 1024
WORK_SECONDS = 30


@dataclass
class ReadinessRead:
    status: str
    payload: object = None
    metrics: dict = field(default_factory=lambda: {
        "fitness_http_attempts": 0, "component_auth_http_attempts": 0,
        "body_bytes": 0, "elapsed_ms": 0.0, "response_closed": False,
    })


def strict_json(data: bytes):
    def invalid_constant(_):
        raise ValueError("Invalid JSON constant")
    return json.loads(data.decode("utf-8"), parse_constant=invalid_constant)


def bounded_read(send, headers, *, clock=time.monotonic) -> ReadinessRead:
    """One guarded send, close before status/parse, never consume error bodies."""
    result = ReadinessRead("source_error")
    began = clock()
    response = None
    body = bytearray()
    status = None
    try:
        remaining = WORK_SECONDS - (clock() - began)
        if remaining <= 0:
            result.status = "timeout"
            return result
        response = send(headers, (min(3, remaining), min(5, remaining)), result.metrics)
        status = response.status_code
        if status == 200:
            content_type = response.headers.get("Content-Type", "").lower().replace(" ", "")
            encoding = response.headers.get("Content-Encoding", "identity").lower()
            length = response.headers.get("Content-Length")
            if (content_type not in {"application/json", "application/json;charset=utf-8"}
                    or encoding not in {"", "identity"}):
                return result
            if length is not None:
                if not length.isdecimal():
                    return result
                if int(length) > MAX_BODY_BYTES:
                    result.status = "oversize"
                    return result
            while True:
                if clock() - began >= WORK_SECONDS:
                    result.status = "timeout"
                    return result
                # read1 avoids waiting to fill a whole block on a trickling
                # stream. Socket idle timeout remains 5s; no hard wall promise.
                chunk = response.raw.read1(min(4096, MAX_BODY_BYTES + 1 - len(body)),
                                          decode_content=False)
                result.metrics["body_bytes"] += len(chunk)
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > MAX_BODY_BYTES:
                    result.status = "oversize"
                    return result
            if length is not None and len(body) != int(length):
                return result
            if clock() - began >= WORK_SECONDS:
                result.status = "timeout"
                return result
    except GarminReadOnlyViolation:
        raise  # PermissionError is an OSError; policy must never become source_error.
    except (requests.RequestException, HTTPError, OSError):
        result.status = "source_error"
        return result
    finally:
        if response is not None:
            try:
                response.close()
                result.metrics["response_closed"] = True
            except GarminReadOnlyViolation:
                raise
            except Exception:
                result.status = "source_error"
            finally:
                result.metrics["elapsed_ms"] = (clock() - began) * 1000
        else:
            result.metrics["elapsed_ms"] = (clock() - began) * 1000
    # No status is interpreted and no JSON is parsed while a response is open.
    if status is not None and 300 <= status < 400:
        result.status = "redirect"  # Policy violation even if close failed.
    elif not result.metrics["response_closed"]:
        return result
    elif status == 401:
        result.status = "auth_required"
    elif status == 200:
        try:
            result.payload = strict_json(bytes(body))
            result.status = "available"
        except (ValueError, RecursionError):
            result.status = "schema_unverified"
    result.metrics["elapsed_ms"] = (clock() - began) * 1000
    return result
