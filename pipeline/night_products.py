"""Bounded offline products from explicitly exported native night signals.

This module never contacts a provider, scans R2 or uploads its output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

METHOD = "night-baseline-v1"
MAX_INPUT_BYTES = 16 * 1024 * 1024
MAX_POINTS = 200000
MAX_PRODUCT_BYTES = 1024 * 1024
UNITS = {"heart_rate": "bpm", "hrv": "ms", "body_battery": "points", "stress": "points"}


def canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def calendar_day(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value or parsed.year < 1970:
        raise ValueError("Invalid calendar date")
    return parsed


def normalize_args(
    *,
    end_date: str,
    period: str,
    start_date: str | None = None,
    alignment: str = "sleep_start",
    bin_minutes: int = 30,
    channel: str = "hrv",
    exclude_dates: list[str] | None = None,
    context_filter: dict | None = None,
) -> dict:
    end = calendar_day(end_date)
    if period == "custom":
        if start_date is None:
            raise ValueError("Custom periods require start_date")
        start = calendar_day(start_date)
    elif period in {"7", "28", "90", "365"}:
        start = end - timedelta(days=int(period) - 1)
        if start_date is not None and start_date != start.isoformat():
            raise ValueError("Preset period conflicts with start_date")
    else:
        raise ValueError("Unsupported period")
    if not 1 <= (end - start).days + 1 <= 366:
        raise ValueError("Reference periods permit1 through366 days")
    if (
        alignment not in {"sleep_start", "wake", "clock"}
        or bin_minutes not in {15, 30}
        or channel not in UNITS
    ):
        raise ValueError("Unsupported reference method")
    excluded = sorted(set(exclude_dates or []))
    if len(excluded) > 366:
        raise ValueError("Too many excluded dates")
    for value in excluded:
        calendar_day(value)
    context_filter = context_filter or {}
    if not isinstance(context_filter, dict):
        raise ValueError("Unsupported context filter")
    for field, value in context_filter.items():
        if field in {"alcohol", "late_bedtime", "travel", "illness"}:
            valid = isinstance(value, bool)
        elif field in {"sleep_quality", "morning_condition"}:
            valid = isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 5
        elif field == "actual_night_timezone":
            valid = isinstance(value, str) and 1 <= len(value) <= 128
            if valid:
                try:
                    ZoneInfo(value)
                except (ZoneInfoNotFoundError, ValueError):
                    valid = False
        else:
            valid = False
        if not valid:
            raise ValueError("Unsupported context filter")
    return {
        "alignment": alignment,
        "bin_minutes": bin_minutes,
        "channel": channel,
        "context_filter": context_filter,
        "end_date": end.isoformat(),
        "exclude_dates": excluded,
        "start_date": start.isoformat(),
    }


def storage_key(args: dict) -> str:
    end = args["end_date"]
    return f"health/night-products/v1/baseline/{end[:4]}/{end[5:7]}/{end[8:10]}/{digest(args)}.json"


def utc_instant(value: Any) -> datetime | None:
    if not isinstance(value, str) or not (
        value.endswith("Z") or "+" in value[10:] or "-" in value[10:]
    ):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except ValueError:
        return None


def quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * probability
    lower = math.floor(rank)
    upper = math.ceil(rank)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def median(values: list[float]) -> float | None:
    return quantile(values, 0.5)


def read_exports(paths: list[str]) -> tuple[list[dict], int, int]:
    if not 1 <= len(paths) <= 64:
        raise ValueError("Provide1 through64 bounded export files")
    nights: list[dict] = []
    total_bytes = 0
    total_points = 0
    for name in paths:
        with Path(name).open("rb") as handle:
            raw = handle.read(MAX_INPUT_BYTES - total_bytes + 1)
        total_bytes += len(raw)
        if total_bytes > MAX_INPUT_BYTES:
            raise ValueError("Input exports exceed16MiB")
        payload = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("Invalid JSON number")),
        )
        if isinstance(payload, dict) and isinstance(payload.get("structuredContent"), dict):
            payload = payload["structuredContent"]
        batch = (
            payload.get("nights")
            if isinstance(payload, dict) and "nights" in payload
            else [payload]
        )
        if not isinstance(batch, list) or len(batch) > 366:
            raise ValueError("Unsupported export shape")
        for night in batch:
            if not isinstance(night, dict):
                raise ValueError("Unsupported night shape")
            channels = night.get("channels") or {}
            if not isinstance(channels, dict):
                raise ValueError("Unsupported channel shape")
            for channel in channels.values():
                if isinstance(channel, dict) and isinstance(channel.get("points"), list):
                    total_points += len(channel["points"])
            if total_points > MAX_POINTS:
                raise ValueError("Input exports exceed200000 points")
            nights.append(night)
    return nights, total_bytes, total_points


def baseline(nights: list[dict], args: dict, *, input_bytes: int, input_points: int) -> dict:
    start_day, end_day = calendar_day(args["start_date"]), calendar_day(args["end_date"])
    requested = [
        (start_day + timedelta(days=offset)).isoformat()
        for offset in range((end_day - start_day).days + 1)
    ]
    indexed: dict[str, dict] = {}
    variants: dict[str, dict[str, dict]] = {}
    for night in nights:
        day = night.get("wake_date")
        if isinstance(day, str) and day in requested:
            snapshot = {
                key: value for key, value in night.items() if key not in {"resources", "view"}
            }
            variants.setdefault(day, {})[digest(snapshot)] = night
    for day, group in variants.items():
        indexed[day] = group[min(group)]
    bins: dict[int, list[tuple[float, int]]] = {}
    source_manifest = []
    excluded = []
    contributing = 0
    invalid_points = 0
    quality_notes = []
    for day in requested:
        night = indexed.get(day)
        if night is None:
            excluded.append({"wake_date": day, "reason": "not_in_export"})
            continue
        channel = (night.get("channels") or {}).get(args["channel"])
        context = night.get("user_context") if isinstance(night.get("user_context"), dict) else {}
        revisions = channel.get("source_revisions", []) if isinstance(channel, dict) else []
        if not isinstance(revisions, list) or len(revisions) > 16:
            raise ValueError("Unsupported source revisions")
        safe_revisions = []
        for revision in revisions:
            if (
                not isinstance(revision, dict)
                or not isinstance(revision.get("date"), str)
                or not isinstance(revision.get("revision"), str)
                or len(revision["revision"]) > 256
            ):
                raise ValueError("Unsupported source revision")
            calendar_day(revision["date"])
            safe_revisions.append({"date": revision["date"], "revision": revision["revision"]})
        revisions = sorted(
            {canonical(value): value for value in safe_revisions}.values(), key=canonical
        )
        snapshot = {key: value for key, value in night.items() if key not in {"resources", "view"}}
        snapshot_hash = (
            digest(sorted(variants[day])) if len(variants[day]) > 1 else digest(snapshot)
        )
        source_manifest.append(
            {
                "wake_date": day,
                "snapshot_sha256": snapshot_hash,
                "source_revisions": revisions,
                "context_id": context.get("context_id")
                if isinstance(context.get("context_id"), str)
                and 1 <= len(context["context_id"]) <= 128
                else None,
            }
        )
        if day in args["exclude_dates"]:
            excluded.append({"wake_date": day, "reason": "explicit_date_exclusion"})
            continue
        if len(variants[day]) > 1:
            excluded.append({"wake_date": day, "reason": "duplicate_conflict"})
            continue
        if args["context_filter"]:
            if (
                context.get("source") != "user"
                or context.get("wake_date") != day
                or context.get("schema_version") != 1
                or not isinstance(context.get("context_id"), str)
                or not 1 <= len(context["context_id"]) <= 128
            ):
                excluded.append({"wake_date": day, "reason": "context_source_unverified"})
                continue
            problem = None
            for field, expected in sorted(args["context_filter"].items()):
                actual = context.get(field)
                if actual is None or type(actual) is not type(expected):
                    problem = {"wake_date": day, "reason": "context_field_unknown", "field": field}
                    break
                if actual != expected:
                    problem = {
                        "wake_date": day,
                        "reason": "context_filter_mismatch",
                        "field": field,
                    }
                    break
            if problem:
                excluded.append(problem)
                continue
        if night.get("method_version") != "night-signals-v1":
            excluded.append({"wake_date": day, "reason": "unsupported_source_method"})
            continue
        if night.get("status") not in {"available", "partial"} or night.get("date") != day:
            excluded.append({"wake_date": day, "reason": "channel_unavailable"})
            continue
        if night.get("resolution") != "native":
            excluded.append({"wake_date": day, "reason": "non_native_input"})
            continue
        start, end = (
            utc_instant(night.get("sleep_start_utc")),
            utc_instant(night.get("sleep_end_utc")),
        )
        if start is None or end is None or not 0 < (end - start).total_seconds() <= 36 * 3600:
            excluded.append({"wake_date": day, "reason": "invalid_sleep_interval"})
            continue
        if (
            not isinstance(channel, dict)
            or channel.get("status") not in {"available", "no_data"}
            or channel.get("unit") != UNITS[args["channel"]]
            or not isinstance(channel.get("points"), list)
        ):
            excluded.append({"wake_date": day, "reason": "channel_unavailable"})
            continue
        if channel["status"] == "no_data" and channel["points"]:
            excluded.append({"wake_date": day, "reason": "channel_unavailable"})
            continue
        zone = None
        if args["alignment"] == "clock":
            actual_zone = night.get("actual_night_timezone")
            if (
                not isinstance(actual_zone, str)
                and context.get("source") == "user"
                and context.get("wake_date") == day
            ):
                actual_zone = context.get("actual_night_timezone")
            try:
                zone = ZoneInfo(actual_zone) if isinstance(actual_zone, str) else None
            except (ZoneInfoNotFoundError, ValueError):
                zone = None
            if zone is None:
                excluded.append({"wake_date": day, "reason": "unknown_actual_timezone"})
                continue
        nightly: dict[int, list[float]] = {}
        clock_offsets: dict[int, set[float]] = {}
        observed: dict[datetime, list[float]] = {}
        for point in channel["points"]:
            if not isinstance(point, dict):
                invalid_points += 1
                continue
            timestamp = utc_instant(point.get("timestamp_utc"))
            value = point.get("value")
            if (
                point.get("time_basis") != "utc"
                or timestamp is None
                or isinstance(value, bool)
                or not isinstance(value, (float, int))
                or not math.isfinite(value)
            ):
                invalid_points += 1
                continue
            if (args["channel"] in {"heart_rate", "hrv"} and value <= 0) or (
                args["channel"] in {"body_battery", "stress"} and not 0 <= value <= 100
            ):
                invalid_points += 1
                continue
            if not start <= timestamp < end:
                invalid_points += 1
                continue
            observed.setdefault(timestamp, []).append(value)
        for timestamp, values in sorted(observed.items()):
            if len(set(values)) > 1:
                invalid_points += len(values)
                continue
            invalid_points += len(values) - 1
            value = values[0]
            if args["alignment"] == "clock":
                local = timestamp.astimezone(zone)
                offset = math.floor(
                    (local.hour * 60 + local.minute + local.second / 60) / args["bin_minutes"]
                )
                clock_offsets.setdefault(offset, set()).add(local.utcoffset().total_seconds())
            else:
                origin = end if args["alignment"] == "wake" else start
                offset = math.floor(
                    (timestamp - origin).total_seconds() / (args["bin_minutes"] * 60)
                )
            nightly.setdefault(offset, []).append(value)
        if not nightly:
            excluded.append({"wake_date": day, "reason": "no_valid_points"})
            continue
        contributing += 1
        count = math.ceil((end - start).total_seconds() / (args["bin_minutes"] * 60))
        offsets = (
            range(1440 // args["bin_minutes"])
            if args["alignment"] == "clock"
            else range(-count, 0)
            if args["alignment"] == "wake"
            else range(count)
        )
        flags = []
        if any(len(values) > 1 for values in clock_offsets.values()):
            flags.append("dst_fold_combined")
        if night.get("status") == "partial":
            flags.append("partial_source")
        if flags:
            quality_notes.append({"wake_date": day, "flags": flags})
        for offset in offsets:
            bins.setdefault(offset, [])
        for offset, values in nightly.items():
            bins[offset].append((median(values), len(values)))
    reference = []
    for offset, contributions in sorted(bins.items()):
        values = [value for value, _count in contributions]
        reference.append(
            {
                "bin_index": offset,
                "offset_minutes": offset * args["bin_minutes"],
                **{f"p{int(p * 100)}": quantile(values, p) for p in (0.1, 0.25, 0.5, 0.75, 0.9)},
                "nights": len(contributions),
                "valid_points": sum(count for _value, count in contributions),
            }
        )
    product = {
        "schema_version": 1,
        "kind": "night-baseline",
        "method_version": METHOD,
        "args": args,
        "args_sha256": digest(args),
        "source_identity": digest(source_manifest),
        "source_manifest": source_manifest,
        "nights_requested": len(requested),
        "source_nights": len(indexed),
        "contributing_nights": contributing,
        "unit": UNITS[args["channel"]],
        "excluded": excluded,
        "method": {
            "within_night": "median",
            "quantiles": "linear-(n-1)p",
            "one_contribution_per_night_bin": True,
        },
        "bins": reference,
        "quality_notes": quality_notes,
        "resources": {
            "input_bytes": input_bytes,
            "input_points": input_points,
            "invalid_points": invalid_points,
        },
    }
    if len(canonical(product).encode("utf-8")) > MAX_PRODUCT_BYTES:
        raise ValueError("Product exceeds1MiB")
    return {"storage_key": storage_key(args), "product": product}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    command = subcommands.add_parser("baseline")
    command.add_argument("--input", action="append", required=True)
    command.add_argument("--end-date", required=True)
    command.add_argument("--period", choices=["7", "28", "90", "365", "custom"], default="28")
    command.add_argument("--start-date")
    command.add_argument(
        "--alignment", choices=["sleep_start", "wake", "clock"], default="sleep_start"
    )
    command.add_argument("--bin-minutes", type=int, choices=[15, 30], default=30)
    command.add_argument("--channel", choices=list(UNITS), default="hrv")
    command.add_argument("--exclude-date", action="append", default=[])
    command.add_argument("--context-filter", default="{}")
    options = parser.parse_args(argv)
    try:
        args = normalize_args(
            end_date=options.end_date,
            period=options.period,
            start_date=options.start_date,
            alignment=options.alignment,
            bin_minutes=options.bin_minutes,
            channel=options.channel,
            exclude_dates=options.exclude_date,
            context_filter=json.loads(options.context_filter),
        )
        nights, input_bytes, input_points = read_exports(options.input)
        output = baseline(nights, args, input_bytes=input_bytes, input_points=input_points)
    except (ValueError, OSError, TypeError, OverflowError):
        parser.exit(2, "Cannot build reference product: invalid or oversized input/arguments.\n")
    print(canonical(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
