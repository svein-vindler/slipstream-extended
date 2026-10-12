"""Offline component delta, same invented DTO/day, disabled versus enabled.

Warm Python + real pinned guard/parser/R2Store with in-memory transports. Shared
login, core pipeline, startup, network, SDK retries and queueing are excluded.
"""
from __future__ import annotations

import io
import json
import socket
import statistics
import sys
from contextlib import redirect_stderr
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import requests
from garminconnect import Garmin
from urllib3.response import HTTPResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.r2_store import R2Store  # noqa: E402
from pipeline.sources.garmin_readonly import ReadOnlyGarmin  # noqa: E402
from pipeline.training_sync import run  # noqa: E402
from scripts.benchmark_refresh_budget import BudgetClient  # noqa: E402
from scripts.build_training_day_contract import synthetic_day  # noqa: E402


def compare(samples=100):
    # Fail even if a future refactor accidentally leaves the mocked transport.
    socket.socket.connect = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("External network forbidden"))
    dto = {"deviceId": 900000, "calendarDate": "2026-10-08", "timestampLocal": "2026-10-08T07:30:00",
           "timestamp": "2026-10-08T05:30:00Z", "score": 80, "recoveryTime": 60,
           "recoveryTimeChangePhrase": "REACHED_ZERO", "inputContext": "AFTER_WAKEUP_RESET"}
    payload = json.dumps([dto], separators=(",", ":")).encode()
    observations = []
    for enabled, stored, status, ready_inventory in ((False, True, 200, True), (True, False, 200, True),
            (True, True, 200, True), (True, True, 401, True), (True, True, 200, False)):
        times = []
        last = None
        for _ in range(samples):
            client = BudgetClient(0)
            previous = synthetic_day()
            if stored:
                client.objects["health/training-context/v1/2026/10/2026-10-08.json"] = {
                    "Body": json.dumps(previous, sort_keys=True, separators=(",", ":")).encode(), "ContentType": "application/json"}
            store = R2Store(client=client, bucket="synthetic")
            # Reuse the general run's already established inventory; no own LIST.
            if ready_inventory:
                store._bucket_budget.inventory = {"objects": len(client.objects), "bytes": sum(len(v["Body"]) for v in client.objects.values())}
            sdk = Garmin(retry_attempts=0)
            sdk.client.di_token = "synthetic-di"
            sdk.client._refresh_session = lambda: (_ for _ in ()).throw(RuntimeError("Component auth forbidden"))
            def send(*a, status=status, **kw):
                response = requests.Response()
                response.status_code = status
                response.headers = {"Content-Type": "application/json"}
                response.raw = HTTPResponse(body=io.BytesIO(payload), preload_content=False, status=status)
                return response
            sdk.client._api_session.request = send
            source = ReadOnlyGarmin(sdk)
            started = perf_counter()
            with redirect_stderr(io.StringIO()):
                last = run(store=store, garmin=source, env={"TRAINING_READINESS_ENABLED": "true" if enabled else "false",
                    "HEALTH_TIMEZONE": "Europe/Oslo"}, now=datetime(2026, 10, 8, 8, tzinfo=timezone.utc))
            times.append((perf_counter() - started) * 1000)
            sdk.client._api_session.close()
        observations.append({"enabled": enabled, "prior_day": stored, "fitness_status": status, "shared_inventory_ready": ready_inventory,
            "operations": {**store.operations}, "fitness_attempts": last["fitness_http_attempts"],
            "component_auth_attempts": last["component_auth_http_attempts"],
            "body_bytes": last["provider_body_bytes"], "day_read_bytes": last["day_read_bytes"],
            "day_write_bytes": last["day_write_bytes"], "status": last["status"],
            "wall_ms_mean": round(statistics.mean(times), 4), "wall_ms_median": round(statistics.median(times), 4)})
    return {"synthetic": True, "samples_per_case": samples, "scope": "optional_component_only",
            "queue_startup_shared_login_network_sdk_retries_included": False, "cases": observations}


if __name__ == "__main__":
    print(json.dumps(compare(), indent=2))
