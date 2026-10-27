"""Run the existing resumable backfills locally in conservative batches.

This optional bootstrap accelerator reuses the hosted backfill implementations
and their R2 progress objects instead of introducing a second storage format.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .activity_backfill_scheduler import run as run_activity_backfill
from .coach_backfill import run as run_coach_backfill
from .health_detail_backfill import run as run_health_detail_backfill
from .hrv_backfill import run as run_hrv_backfill
from .r2_store import R2BudgetError, R2Store
from .sources.garmin import _login

PHASES = ("activity", "hrv", "health", "coach")
DEFAULT_PHASES = ("activity", "hrv", "health")
GARMIN_PHASES = frozenset({"activity", "hrv", "health"})
TERMINAL_STATUSES = frozenset({"complete", "complete_with_blocked", "unsupported"})


class LazyGarmin:
    """Log in once, and only if an incomplete phase actually needs Garmin."""

    def __init__(self, login: Callable[[], Any] = _login):
        if login is _login:
            from .sources.provider_config import selection
            chosen = selection(os.environ)
            self._login = lambda: login(chosen=chosen)
            self._provider = chosen.provider
        else:
            self._login = login
            self._provider = "legacy"
        self._client = None

    @property
    def provider(self):
        return self._provider

    def __getattr__(self, name: str):
        if self._client is None:
            self._client = self._login()
        return getattr(self._client, name)


def load_env_file(path: Path) -> None:
    """Load a small KEY=VALUE file without overriding the current environment."""
    if not path.exists():
        return
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid environment line {number} in {path}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Empty environment key on line {number} in {path}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def prepare_environment(env_file: Path, token_file: Path | None = None) -> None:
    from .sources.provider_config import selection

    load_env_file(env_file)
    if os.environ.get("GARMIN_PROVIDER_FILE"):
        settings_path = Path(os.environ["GARMIN_PROVIDER_FILE"])
        if not settings_path.is_absolute():
            os.environ["GARMIN_PROVIDER_FILE"] = str(env_file.parent / settings_path)
    chosen = selection(os.environ)
    if token_file is not None and chosen.provider != "legacy":
        raise ValueError("--token-file is only for the legacy Garmin provider.")
    name = chosen.credential_name
    configured = token_file or (Path(os.environ[name + "_FILE"])
                                if os.environ.get(name + "_FILE") else None)
    if configured and not configured.is_absolute():
        configured = env_file.parent / configured
    if configured and not os.environ.get(name):
        os.environ[name + "_FILE"] = str(configured)
        os.environ[name] = selection(os.environ).credential()
        # Process-local normalization: no persistent file or global setting changes.
        os.environ.pop(name + "_FILE", None)


def validate_environment(phases: tuple[str, ...]) -> None:
    required = {"CLOUDFLARE_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"}
    if GARMIN_PHASES.intersection(phases):
        from .sources.provider_config import selection
        chosen = selection(os.environ)
        if not os.environ.get(chosen.credential_name + "_FILE"):
            required.add(chosen.credential_name)
    missing = sorted(name for name in required if not os.environ.get(name))
    if missing:
        raise ValueError("Missing required local secrets: " + ", ".join(missing))


def phase_is_terminal(name: str, result: dict[str, Any]) -> bool:
    if name == "health":
        return all(
            isinstance(result.get(stream), dict)
            and result[stream].get("status") in TERMINAL_STATUSES
            for stream in ("sleep", "body_composition")
        )
    if name == "coach" and result.get("status") == "waiting_for_profile":
        return True
    return result.get("status") in TERMINAL_STATUSES


def all_phases_terminal(
    phases: tuple[str, ...], results: dict[str, dict[str, Any]]
) -> bool:
    return all(
        name in results and phase_is_terminal(name, results[name]) for name in phases
    )


def compact_result(name: str, result: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "status",
        "garmin_provider",
        "unsupported_features",
        "total_days",
        "complete_days",
        "remaining_days",
        "blocked_after_three_failures",
        "attempted_this_run",
        "running_activities",
        "remaining_activities",
        "profile_count",
        "last_range",
        "last_run_ranges",
        "duration_seconds",
    )
    if name == "health":
        compact = {
            stream: compact_result(stream, value)
            for stream, value in result.items()
            if stream in {"sleep", "body_composition"} and isinstance(value, dict)
        }
        if "duration_seconds" in result:
            compact["duration_seconds"] = result["duration_seconds"]
        return compact
    compact = {field: result[field] for field in fields if field in result}
    blocked = result.get("blocked_activities")
    if isinstance(blocked, list):
        total_blocked = result.get("blocked_activity_count")
        compact["blocked_activities"] = (
            total_blocked if isinstance(total_blocked, int) else len(blocked)
        )
    return compact


def run_cycle(
    *,
    phases: tuple[str, ...],
    garmin: Any,
    retry_failures: bool = False,
    activity_batch: int = 50,
    hrv_batch: int = 100,
    sleep_batch: int = 100,
    body_batch: int = 100,
    coach_batch: int = 50,
    store_factory: Callable[[], R2Store] = R2Store,
) -> dict[str, dict[str, Any]]:
    """Run one bounded pass; each phase retains its normal per-run R2 budget."""
    results: dict[str, dict[str, Any]] = {}

    def timed(call: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        started = time.perf_counter()
        result = dict(call())
        result["duration_seconds"] = round(time.perf_counter() - started, 3)
        return result

    if "activity" in phases:
        results["activity"] = timed(
            lambda: run_activity_backfill(
                max_activities=activity_batch,
                store=store_factory(),
                garmin=garmin,
            )
        )
    if "hrv" in phases:
        results["hrv"] = timed(
            lambda: run_hrv_backfill(
                max_days=hrv_batch,
                retry_failures=retry_failures,
                store=store_factory(),
                garmin=garmin,
            )
        )
    if "health" in phases:
        results["health"] = timed(
            lambda: run_health_detail_backfill(
                max_sleep_days=sleep_batch,
                max_body_days=body_batch,
                retry_failures=retry_failures,
                store=store_factory(),
                garmin=garmin,
            )
        )
    if "coach" in phases:
        results["coach"] = timed(
            lambda: run_coach_backfill(
                max_activities=coach_batch,
                store=store_factory(),
            )
        )
    return results


def _write_status(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


@contextmanager
def local_process_lock(path: Path):
    """Prevent two local bootstrap processes from running at the same time."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError("Another local bootstrap process is already running") from exc
    try:
        yield
    finally:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def run_local_bootstrap(
    *,
    phases: tuple[str, ...] = DEFAULT_PHASES,
    max_hours: float = 8,
    pause_seconds: float = 180,
    max_cycles: int = 0,
    retry_failures: bool = False,
    status_file: Path = Path(".granular/local-bootstrap/status.json"),
    lock_file: Path = Path(".granular/local-bootstrap.lock"),
    garmin: Any | None = None,
    cycle_runner: Callable[..., dict[str, dict[str, Any]]] = run_cycle,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    **batch_sizes: int,
) -> dict[str, Any]:
    if max_hours <= 0:
        raise ValueError("max_hours must be greater than zero")
    if pause_seconds < 0:
        raise ValueError("pause_seconds cannot be negative")
    if max_cycles < 0:
        raise ValueError("max_cycles cannot be negative")
    unknown = sorted(set(phases) - set(PHASES))
    if not phases or unknown:
        raise ValueError(f"Unknown or empty phases: {', '.join(unknown) or 'none'}")

    deadline = monotonic() + max_hours * 3600
    cycles = 0
    latest: dict[str, dict[str, Any]] = {}
    lazy_garmin = garmin or LazyGarmin()
    reason = "time_limit"

    def final_status(status: str, *, error: BaseException | None = None):
        payload: dict[str, Any] = {
            "schema_version": 1,
            "status": status,
            "updated_at": datetime.now(UTC).isoformat(),
            "cycles": cycles,
            "phases": list(phases),
            "results": {
                name: compact_result(name, result) for name, result in latest.items()
            },
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
                print(f"\n[local-bootstrap] starting cycle {cycles}", flush=True)
                latest = cycle_runner(
                    phases=phases,
                    garmin=lazy_garmin,
                    retry_failures=retry_failures,
                    **batch_sizes,
                )
                payload = final_status("running")
                print(json.dumps(payload, indent=2, ensure_ascii=False), flush=True)

                if all_phases_terminal(phases, latest):
                    unsupported = any(
                        result.get("status") == "unsupported"
                        or any(isinstance(value, dict) and value.get("status") == "unsupported"
                               for value in result.values())
                        for result in latest.values()
                    )
                    reason = "complete_with_unsupported" if unsupported else "complete"
                    break
                if max_cycles and cycles >= max_cycles:
                    reason = "cycle_limit"
                    break
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                sleep(min(pause_seconds, remaining))
    except KeyboardInterrupt as exc:
        final_status("interrupted", error=exc)
        raise
    except Exception as exc:
        final_status("stopped_error", error=exc)
        raise
    return final_status(reason)


def _parse_phases(value: str) -> tuple[str, ...]:
    phases = tuple(
        dict.fromkeys(item.strip() for item in value.split(",") if item.strip())
    )
    unknown = sorted(set(phases) - set(PHASES))
    if not phases or unknown:
        raise argparse.ArgumentTypeError(
            "phases must contain one or more of: " + ", ".join(PHASES)
        )
    return phases


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Safely accelerate the existing R2 backfills on this computer."
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--garmin-token-file", type=Path)
    parser.add_argument("--phases", type=_parse_phases, default=DEFAULT_PHASES)
    parser.add_argument("--max-hours", type=float, default=8)
    parser.add_argument("--pause-seconds", type=float, default=180)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--retry-failures", action="store_true")
    parser.add_argument("--activity-batch", type=int, default=50)
    parser.add_argument("--hrv-batch", type=int, default=100)
    parser.add_argument("--sleep-batch", type=int, default=100)
    parser.add_argument("--body-batch", type=int, default=100)
    parser.add_argument("--coach-batch", type=int, default=50)
    parser.add_argument(
        "--status-file",
        type=Path,
        default=Path(".granular/local-bootstrap/status.json"),
    )
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=Path(".granular/local-bootstrap.lock"),
    )
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
    try:
        prepare_environment(args.env_file, args.garmin_token_file)
        validate_environment(args.phases)
        result = run_local_bootstrap(
            phases=args.phases,
            max_hours=args.max_hours,
            pause_seconds=args.pause_seconds,
            max_cycles=args.max_cycles,
            retry_failures=args.retry_failures,
            activity_batch=args.activity_batch,
            hrv_batch=args.hrv_batch,
            sleep_batch=args.sleep_batch,
            body_batch=args.body_batch,
            coach_batch=args.coach_batch,
            status_file=args.status_file,
            lock_file=args.lock_file,
        )
    except KeyboardInterrupt:
        print("\n[local-bootstrap] stopped safely; rerun to resume.")
        return
    except (R2BudgetError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"Local bootstrap stopped safely: {exc}\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
