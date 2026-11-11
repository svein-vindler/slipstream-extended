"""Stored native HR/HRV night projection for the existing baseline producer.

Parity target: Worker night_signals(native, sleep_start, heart_rate+hrv).
Unknown or malformed stored input stops automatic publication; no provider calls.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import re
from datetime import UTC, date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from botocore.exceptions import ClientError

from .night_products import median
from .r2_store import R2BudgetError, missing_object


class NativeInputError(R2BudgetError):
    pass


class _RawInputPointLimit(NativeInputError):
    category = "native_raw_input_point_limit"


class _DecodedInputByteLimit(NativeInputError):
    category = "native_decoded_input_byte_limit"


def require(value, reason="Unverified stored night input"):
    if not value:
        raise NativeInputError(reason)


def instant(value, unit=None):
    try:
        if isinstance(value, bool):
            return None
        if (
            isinstance(value, (int, float))
            or isinstance(value, str)
            and re.fullmatch(r"\d{9,13}", value.strip())
        ):
            number = float(value)
            millis = (
                number * 1000
                if unit == "epoch_seconds" or unit != "epoch_milliseconds" and number < 10000000000
                else number
            )
            result = datetime.fromtimestamp(millis / 1000, UTC)
        elif isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}T", value):
            result = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if result.tzinfo is None:
                result = result.replace(tzinfo=UTC)
            result = result.astimezone(UTC)
        else:
            return None
        return result if 1970 <= result.year < 2200 else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def iso(value):
    return (
        value.astimezone(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
        .replace(".000Z", "Z")
    )


def zone(value):
    try:
        return (
            ZoneInfo(value.strip())
            if isinstance(value, str) and 0 < len(value.strip()) <= 128
            else None
        )
    except (ZoneInfoNotFoundError, ValueError):
        return None


def _source_clock(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value if instant(value) else None
    if not isinstance(value, str) or len(value) > 64:
        return None
    if re.fullmatch(r"\d{10,13}", value.strip()) and instant(value):
        return value
    return (
        value
        if re.fullmatch(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,12})?(?:Z|[+-]\d{2}:?\d{2})?",
            value.strip(),
        )
        else None
    )


def _wall(utc, raw):
    if utc is None:
        return None
    if isinstance(raw, str) and not re.fullmatch(r"\d{10,13}", raw.strip()):
        naive = instant(re.sub(r"(?:Z|[+-]\d{2}:?\d{2})$", "", raw.replace(" ", "T")))
    else:
        naive = instant(raw)
    if naive is None:
        return None
    minutes = (naive - utc).total_seconds() / 60
    rounded = math.floor(minutes + 0.5)
    if abs(rounded) > 840 or abs(minutes - rounded) > 0.5:
        return None
    return naive.replace(tzinfo=timezone(timedelta(minutes=rounded)))


def _local(utc, tz):
    return utc.astimezone(tz) if utc and tz else None


def _stamp(value):
    return value.isoformat(timespec="seconds") if value else None


def night_context(day, sleep, configured, actual, *, declared=False):
    start, end = instant(sleep.get("sleep_start_gmt")), instant(sleep.get("sleep_end_gmt"))
    garmin_start = _wall(start, sleep.get("sleep_start_garmin_local"))
    garmin_end = _wall(end, sleep.get("sleep_end_garmin_local"))
    configured_start, configured_end = _local(start, configured), _local(end, configured)
    actual_zone = zone(actual)
    actual_start, actual_end = _local(start, actual_zone), _local(end, actual_zone)
    local_start = actual_start or garmin_start or configured_start
    local_end = actual_end if actual_zone else garmin_end if garmin_start else configured_end
    valid = start and end and 0 < (end - start).total_seconds() <= 129600
    midpoint = start + (end - start) / 2 if valid else None
    matches = (
        garmin_start
        and garmin_end
        and _stamp(garmin_start) == _stamp(configured_start)
        and _stamp(garmin_end) == _stamp(configured_end)
    )
    local_mid = (
        _local(midpoint, actual_zone)
        if actual_zone
        else (midpoint + garmin_start.utcoffset()).replace(tzinfo=garmin_start.tzinfo)
        if midpoint
        and garmin_start
        and garmin_end
        and garmin_start.utcoffset() == garmin_end.utcoffset()
        else _local(midpoint, configured)
        if not garmin_start or matches
        else None
    )
    flags = [] if actual_zone else ["actual_timezone_unknown"]
    if not valid:
        flags.append("invalid_sleep_interval")
    if any(
        value is not None
        and (
            _source_clock(value) is None
            or instant(value.replace(" ", "T") if isinstance(value, str) else value) is None
        )
        for value in (
            sleep.get("sleep_start_gmt"),
            sleep.get("sleep_end_gmt"),
            sleep.get("sleep_start_garmin_local"),
            sleep.get("sleep_end_garmin_local"),
        )
    ):
        flags.append("invalid_source_clock")
    if actual is not None and actual_zone is None:
        flags.append("invalid_actual_night_timezone")
    if any(
        a and b and _stamp(a) != _stamp(b)
        for a, b in ((garmin_start, actual_start), (garmin_end, actual_end))
    ):
        flags.append("source_local_conflicts_actual_timezone")
    if any(
        a and b and _stamp(a) != _stamp(b)
        for a, b in ((garmin_start, configured_start), (garmin_end, configured_end))
    ):
        flags.append("source_local_conflicts_configured_timezone")
    if declared and actual is None:
        flags.append("user_cleared_actual_timezone")

    def clock_day(value):
        return value.date().isoformat() if value else None

    return {
        "wake_date": day,
        "night_of": clock_day(local_start),
        "sleep_start_local": _stamp(local_start),
        "sleep_end_local": _stamp(local_end),
        "sleep_midpoint_local": _stamp(local_mid),
        "sleep_start_date_local": clock_day(local_start),
        "sleep_end_date_local": clock_day(local_end),
        "sleep_start_weekday_local": local_start.strftime("%A") if local_start else None,
        "sleep_end_weekday_local": local_end.strftime("%A") if local_end else None,
        "timezone": actual_zone.key
        if actual_zone
        else None
        if garmin_start
        else configured.key
        if configured
        else None,
        "local_time_source": "actual_night_timezone"
        if actual_start
        else "garmin_local"
        if garmin_start
        else "configured_timezone"
        if configured_start
        else "unavailable",
        "actual_night_timezone": actual_zone.key if actual_zone else None,
        "configured_timezone": configured.key if configured else None,
        "configured_projection": {
            "timezone": configured.key,
            "night_of": clock_day(configured_start),
            "sleep_start_local": _stamp(configured_start),
            "sleep_end_local": _stamp(configured_end),
            "sleep_midpoint_local": _stamp(_local(midpoint, configured)),
        }
        if configured
        else None,
        "source_time": {
            key: _source_clock(sleep.get(key))
            for key in (
                "sleep_start_gmt",
                "sleep_end_gmt",
                "sleep_start_garmin_local",
                "sleep_end_garmin_local",
            )
        },
        "time_quality_flags": flags,
    }


def json_numbers(value):
    """Match JSON.parse/JSON.stringify integral-number representation before source hashing."""
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: json_numbers(child) for key, child in value.items()}
    if isinstance(value, list):
        return [json_numbers(child) for child in value]
    return value


def empty_channel(unit, status="not_stored"):
    return {
        "status": status,
        "unit": unit,
        "valid_points": 0,
        "observed_cadence_seconds": None,
        "coverage": None,
        "measurement_window_seconds": None,
        "measurement_window_basis": "not_documented_in_stored_source",
        "coverage_basis": "occupied-observed-cadence-slots-v1",
        "largest_gap_seconds": None,
        "quality": dict.fromkeys(
            (
                "invalid",
                "ambiguous_time",
                "outside_interval",
                "duplicates",
                "conflicting_timestamps",
                "gaps",
            ),
            0,
        ),
        "quality_flags": [],
        "source_dates": [],
        "source_revisions": [],
        "points": [],
        "bins": [],
        "minimum": None,
    }


def add_readings(channel, readings, hrv, start, end):
    require(isinstance(readings, list) and len(readings) <= 100000)
    for raw in readings:
        if not isinstance(raw, dict):
            channel["quality"]["invalid"] += 1
            continue
        value = raw.get("hrv_ms" if hrv else "value")
        try:
            number = (
                float(value)
                if not isinstance(value, bool) and value is not None and value != ""
                else math.nan
            )
        except (ValueError, TypeError):
            number = math.nan
        if not math.isfinite(number) or number <= 0:
            channel["quality"]["invalid"] += 1
            continue
        number = int(number) if number.is_integer() else number
        source, unit, basis, field = (
            raw.get("timestamp"),
            raw.get("timestamp_unit"),
            raw.get("time_basis"),
            raw.get("timestamp_field"),
        )
        original = raw.get("raw")
        if (
            basis is None
            and isinstance(original, dict)
            and instant(original.get("readingTimeGMT"))
            and instant(original.get("readingTimeGMT")) == instant(source, unit)
        ):
            basis, field = "utc", "readingTimeGMT"
        if basis != "utc":
            channel["quality"]["ambiguous_time"] += 1
            continue
        clock = instant(source, unit)
        if clock is None:
            channel["quality"]["invalid"] += 1
            continue
        if not start <= clock < end:
            channel["quality"]["outside_interval"] += 1
            continue
        point = {
            "timestamp_utc": iso(clock),
            "value": number,
            "source_timestamp": source[:80] if isinstance(source, str) else source,
            "time_basis": "utc",
            "timestamp_field": field[:128] if isinstance(field, str) else "legacy_documented_gmt",
            "timestamp_unit": unit[:32]
            if isinstance(unit, str)
            else "epoch_seconds"
            if isinstance(source, (int, float)) and source < 10000000000
            else "epoch_milliseconds"
            if isinstance(source, (int, float))
            else "iso8601",
        }
        for key, pattern in (
            ("source_revision", r"[a-f0-9]{64}"),
            ("observation_origin", r"incoming|retained"),
        ):
            if key in raw:
                valid = (
                    raw[key] is None
                    or isinstance(raw[key], str)
                    and re.fullmatch(pattern, raw[key])
                )
                point[key] = raw[key] if valid else None
                if not valid and "source_quality_metadata_invalid" not in channel["quality_flags"]:
                    channel["quality_flags"].append("source_quality_metadata_invalid")
        if (
            point.get("observation_origin") == "retained"
            and not point.get("source_revision")
            and "source_retained_revision_unknown" not in channel["quality_flags"]
        ):
            channel["quality_flags"].append("source_retained_revision_unknown")
        channel["points"].append(point)


def finish(channel, start, end, hr):
    groups = {}
    for point in channel["points"]:
        groups.setdefault(point["timestamp_utc"], []).append(point)
    points = []
    for group in groups.values():
        if len({p["value"] for p in group}) > 1:
            channel["quality"]["conflicting_timestamps"] += 1
            continue
        channel["quality"]["duplicates"] += len(group) - 1
        points.append(group[0])
    points.sort(key=lambda p: p["timestamp_utc"])
    channel["points"], channel["valid_points"] = points, len(points)
    times = [(instant(p["timestamp_utc"]) - start).total_seconds() * 1000 for p in points]
    diffs = [b - a for a, b in zip(times, times[1:], strict=False)]
    cadence = median(diffs)
    channel["observed_cadence_seconds"] = cadence / 1000 if cadence is not None else None
    channel["largest_gap_seconds"] = max(diffs) / 1000 if diffs else None
    duration = (end - start).total_seconds() * 1000
    if cadence and cadence > 0:
        channel["quality"]["gaps"] = sum(d > cadence * 2 for d in diffs)
        channel["coverage"] = min(
            1, len({math.floor(t / cadence) for t in times}) / math.ceil(duration / cadence)
        )
        if hr and cadence <= 150000 and math.ceil(600000 / cadence) * 0.8 <= len(points):
            first = max(0, math.ceil((times[0] - 2 * cadence) / cadence)) * cadence
            while first + 600000 <= duration and first <= times[-1]:
                window = [
                    p for p, t in zip(points, times, strict=True) if first <= t < first + 600000
                ]
                clocks = [t for t in times if first <= t < first + 600000]
                coverage = len({math.floor((t - first) / cadence) for t in clocks}) / math.ceil(
                    600000 / cadence
                )
                gaps = (
                    [
                        clocks[0] - first,
                        *[b - a for a, b in zip(clocks, clocks[1:], strict=False)],
                        first + 600000 - clocks[-1],
                    ]
                    if clocks
                    else [math.inf]
                )
                if len(window) >= 4 and coverage >= 0.8 and max(gaps) <= cadence * 2:
                    value = median([p["value"] for p in window])
                    if channel["minimum"] is None or value < channel["minimum"]["value"]:
                        channel["minimum"] = {
                            "value": value,
                            "method": "lowest-10-minute-median-v1",
                            "window_start_utc": iso(start + timedelta(milliseconds=first)),
                            "window_end_utc": iso(start + timedelta(milliseconds=first + 600000)),
                            "coverage": coverage,
                            "valid_points": len(window),
                        }
                first += cadence
    if points:
        channel["status"] = "available"
    for field, flag in (
        ("invalid", "invalid_observations"),
        ("ambiguous_time", "ambiguous_time_excluded"),
        ("conflicting_timestamps", "conflicting_instants_excluded"),
        ("gaps", "observed_gaps"),
    ):
        if channel["quality"][field]:
            channel["quality_flags"].append(flag)
    if channel["coverage"] is None:
        channel["quality_flags"].append("coverage_unknown")
    if hr and channel["minimum"] is None:
        channel["quality_flags"].append("minimum_window_not_covered")


class NativeNightReader:
    def __init__(self, store, *, timezone=None, snapshot_cache=None):
        self.store, self.configured = store, zone(timezone)
        self.snapshot_cache = snapshot_cache if snapshot_cache is not None else {}
        self.local_snapshot_hits = 0
        self.cache, self.proof, self.lists, self.dependencies = {}, {}, {}, {}
        self.cache_sizes = {}
        self.active_keys, self.active_prefixes = set(), set()
        self.read_bytes = self.decoded_bytes = self.input_points = 0
        self.rolling_inventory = None
        self.annual_input_days = frozenset()
        self._annual_read = False

    def prime_rolling_sources(self, days, *, window_days=None):
        """Fresh bounded inventory, preserving each night's logical proof.

        Verified shared snapshots save body GETs only while the newly observed
        canonical revision and size match. Both publication fences remain fresh.
        """
        from .night_reference_sources import (
            CONTEXT,
            _inventory_for,
            _read_inventories,
            rolling_source_prefixes,
        )

        # A new or failed prime cannot retain an earlier annual admission.
        self.annual_input_days = frozenset()
        days = tuple(days)
        prefixes = rolling_source_prefixes(days, window_days=window_days)
        inventory = _read_inventories(self.store, prefixes, window_days=window_days)
        self.rolling_inventory = inventory
        # A new prime is a new observation epoch, including absence/context.
        self.cache, self.proof, self.lists, self.dependencies = {}, {}, {}, {}
        self.cache_sizes = {}
        for day in days:
            prefix = f"{CONTEXT}{day[:4]}/{day[5:7]}/{day}/"
            keys = sorted(key for key in _inventory_for(prefix, inventory) if key.startswith(prefix))
            require(len(keys) <= 100)
            self.lists[prefix] = keys
        if window_days == 365:
            self.annual_input_days = frozenset(days)

    def _rolling_binding(self, key):
        if self.rolling_inventory is None:
            return False, None
        for prefix, rows in self.rolling_inventory.items():
            if key.startswith(prefix):
                return True, rows.get(key)
        # Rare UTC/local boundary dates outside the primed calendar still use
        # the ordinary bounded GET path and acquire their own fence dependency.
        return False, None

    def _retain_snapshot(self, key, snapshot):
        if self.rolling_inventory is None or key in self.snapshot_cache:
            return
        if len(self.snapshot_cache) < 512:
            used = sum(len(value.data) for value in self.snapshot_cache.values())
            if used + len(snapshot.data) <= 16777216:
                self.snapshot_cache[key] = snapshot

    def read(self, key, stored=524288, decoded=2097152):
        self.active_keys.add(key)
        if key in self.cache:
            if self.cache[key] is not None:
                stored_size, decoded_size = self.cache_sizes[key]
                require(
                    stored_size <= stored and decoded_size <= decoded,
                    "Cached stored source exceeds requested bounds",
                )
            return self.cache[key]
        try:
            inventoried, binding = self._rolling_binding(key)
            if inventoried and binding is None:
                self.proof[key] = {"status": "absent"}
                self.cache[key] = None
                return None
            cached = self.snapshot_cache.get(key)
            if (cached is not None and (self.rolling_inventory is None or inventoried and
                    cached.revision == binding["revision"] and len(cached.data) == binding["size"])):
                snapshot = self.snapshot_cache[key]
                self.local_snapshot_hits += 1
            else:
                if self.rolling_inventory is not None:
                    self.snapshot_cache.pop(key, None)
                snapshot = self.store.get_snapshot(key, max_bytes=stored)
        except ClientError as error:
            if not missing_object(error):
                raise
            require(not inventoried, "Rolling source disappeared after inventory")
            self.proof[key] = {"status": "absent"}
            self.cache[key] = None
            return None
        require(snapshot.revision is not None)
        if inventoried:
            require(snapshot.revision == binding["revision"] and len(snapshot.data) == binding["size"],
                    "Rolling source revision changed after inventory")
        data = snapshot.data
        stored_size = len(data)
        require(stored_size <= stored, "Stored source exceeds requested bound")
        self.read_bytes += len(data)
        require(self.read_bytes <= 16777216)
        self.proof[key] = {
            "status": "present",
            "revision": snapshot.revision,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        if self.rolling_inventory is not None:
            self.proof[key]["size"] = stored_size
        if data[:2] == b"\x1f\x8b":
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as handle:
                data = handle.read(decoded + 1)
        require(len(data) <= decoded)
        self.decoded_bytes += len(data)
        # ceil(32MiB *365/90), only while assembling an admitted annual night.
        # Direct/foreign reads and new or failed primes retain the ordinary cap.
        byte_limit = 136081864 if self._annual_read and inventoried else 33554432
        if self.decoded_bytes > byte_limit:
            raise _DecodedInputByteLimit("Stored native decoded input byte limit")
        try:
            value = json.loads(
                data,
                parse_constant=lambda _: (_ for _ in ()).throw(
                    NativeInputError("Nonfinite source")
                ),
            )
        except (ValueError, UnicodeError) as error:
            raise NativeInputError("Malformed stored source JSON") from error
        require(isinstance(value, dict))
        self.cache_sizes[key] = (stored_size, len(data))
        self.cache[key] = (value, snapshot.revision[1:-1])
        self._retain_snapshot(key, snapshot)
        return self.cache[key]

    def source(self, stream, day):
        root = {"sleep": "health/sleep/v1", "hrv": "health/hrv", "signals": "health/signals/v1"}[
            stream
        ]
        key = f"{root}/{day[:4]}/{day[5:7]}/{day}.json"
        result = self.read(
            key,
            262144 if stream == "signals" else 524288,
            1048576 if stream == "signals" else 2097152,
        )
        if result is None:
            result = self.read(
                key + ".gz",
                262144 if stream == "signals" else 524288,
                1048576 if stream == "signals" else 2097152,
            )
        if result is not None:
            require(
                result[0].get("schema_version") == 1
                and result[0].get("source") == "garmin"
                and result[0].get("date") == day
            )
        return result

    def context(self, day):
        prefix = f"health/night-context/v1/{day[:4]}/{day[5:7]}/{day}/"
        self.active_prefixes.add(prefix)
        keys = (self.lists[prefix] if self.rolling_inventory is not None and prefix in self.lists
                else self.store.list_prefix_bounded(prefix, max_keys=101))
        require(
            len(keys) <= 100
            and all(
                re.fullmatch(r"\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json", key[len(prefix) :])
                for key in keys
            )
        )
        self.lists[prefix] = sorted(keys)
        if not keys:
            return None
        selected = sorted(keys)[-1]
        row = self.read(selected, 16384, 32768)
        require(row is not None)
        user = dict(row[0])
        for field in ("actual_night_timezone", "note"):
            if isinstance(user.get(field), str):
                user[field] = user[field].strip()
        core = {k: v for k, v in user.items() if k != "context_id"}
        expected = (
            "nctx-"
            + hashlib.sha256(
                json.dumps(
                    dict(sorted(core.items())),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest()[:24]
        )
        require(
            user.get("schema_version") == 1
            and user.get("source") == "user"
            and user.get("wake_date") == day
            and user.get("context_id") == expected
            and selected
            == prefix + re.sub(r"[-:.Z]", "", user.get("created_at", "")) + "-" + expected + ".json"
        )
        fields = {
            "alcohol",
            "late_bedtime",
            "travel",
            "actual_night_timezone",
            "illness",
            "sleep_quality",
            "morning_condition",
            "note",
        }
        require(
            set(user)
            == fields
            | {
                "schema_version",
                "wake_date",
                "context_id",
                "previous_context_id",
                "created_at",
                "source",
                "declared_fields",
                "changed_fields",
            }
        )
        require(
            isinstance(user["declared_fields"], list)
            and user["declared_fields"] == sorted(set(user["declared_fields"]))
            and set(user["declared_fields"]) <= fields
            and len(user["declared_fields"]) > 0
        )
        require(
            isinstance(user["changed_fields"], list)
            and len(set(user["changed_fields"])) == len(user["changed_fields"])
            and 0 < len(user["changed_fields"]) <= 8
            and set(user["changed_fields"]) <= fields
        )
        for name in fields:
            value = user[name]
            require(
                value is None
                or (
                    isinstance(value, bool)
                    if name in {"alcohol", "late_bedtime", "travel", "illness"}
                    else type(value) is int and 1 <= value <= 5
                    if name in {"sleep_quality", "morning_condition"}
                    else isinstance(value, str)
                    and 0 < len(value) <= 1000
                    and value == value.strip()
                    if name == "note"
                    else zone(value) is not None and len(value) <= 100
                )
            )
        return user

    def _finish_night(self, day, output):
        self.dependencies[day] = {
            "objects": {key: self.proof[key] for key in self.active_keys},
            "lists": {key: self.lists[key] for key in self.active_prefixes},
        }
        return json_numbers(output)

    def night(self, day):
        self._annual_read = day in self.annual_input_days
        try:
            return self._night(day)
        finally:
            self._annual_read = False

    def _night(self, day):
        self.active_keys, self.active_prefixes = set(), set()
        require(date.fromisoformat(day).isoformat() == day)
        user = self.context(day)
        channels = {
            name: empty_channel(unit) for name, unit in (("heart_rate", "bpm"), ("hrv", "ms"))
        }
        output = {
            "date": day,
            "wake_date": day,
            "night_of": None,
            "sleep_start_utc": None,
            "sleep_end_utc": None,
            "timezone": None,
            "configured_timezone": self.configured.key if self.configured else None,
            "actual_night_timezone": None,
            "night_context": None,
            "status": "not_stored",
            "resolution": "native",
            "alignment": "sleep_start",
            "bin_origin_utc": None,
            "method_version": "night-signals-v1",
            "channels": channels,
            "quality_flags": [],
            "user_context": user,
        }
        selected = self.source("sleep", day)
        if selected is None:
            return self._finish_night(day, output)
        sleep = selected[0]
        start, end = instant(sleep.get("sleep_start_gmt")), instant(sleep.get("sleep_end_gmt"))
        require(start and end and 0 < (end - start).total_seconds() <= 129600)
        # Documented calendar windows are bounded independently of any configured home zone.
        require(
            abs((end.date() - date.fromisoformat(day)).days) <= 1,
            "Sleep wake-date binding is unverified",
        )
        actual = (
            sleep.get("actual_night_timezone")
            if isinstance(sleep.get("actual_night_timezone"), str)
            else None
        )
        original_context = night_context(day, sleep, self.configured, actual)
        declared = user is not None and "actual_night_timezone" in user["declared_fields"]
        if user and (declared or isinstance(user.get("actual_night_timezone"), str)):
            actual = user["actual_night_timezone"]
        context = night_context(day, sleep, self.configured, actual, declared=declared)
        replaced = {
            "actual_timezone_unknown",
            "source_local_conflicts_actual_timezone",
            "user_context_unavailable",
            "user_cleared_actual_timezone",
        }
        if declared:
            replaced.add("invalid_actual_night_timezone")
        prior_flags = [
            flag for flag in original_context["time_quality_flags"] if flag not in replaced
        ]
        context["time_quality_flags"] = list(
            dict.fromkeys(prior_flags + context["time_quality_flags"])
        )
        output.update(
            night_context=context,
            night_of=context["night_of"],
            timezone=context["timezone"],
            actual_night_timezone=context["actual_night_timezone"],
            sleep_start_utc=iso(start),
            sleep_end_utc=iso(end),
            bin_origin_utc=iso(start),
        )
        output["quality_flags"] = list(context["time_quality_flags"])
        dates = {day}
        cursor = start.date()
        while datetime.combine(cursor, datetime.min.time(), UTC) < end:
            dates.add(cursor.isoformat())
            cursor += timedelta(days=1)
        for key in ("sleep_start_date_local", "sleep_end_date_local"):
            if context[key]:
                dates.add(context[key])
        for key, utc in (("sleep_start_garmin_local", start), ("sleep_end_garmin_local", end)):
            wall = _wall(utc, sleep.get(key))
            if wall:
                dates.add(wall.date().isoformat())
        channel = channels["heart_rate"]
        for source_day in sorted(dates):
            source = self.source("signals", source_day)
            channel["source_dates"].append(source_day)
            if source is None:
                channel["status"] = "not_stored"
                channel["quality_flags"].append("source_not_stored")
                continue
            channel["source_revisions"].append({"date": source_day, "revision": source[1]})
            raw = source[0].get("channels", {}).get("heart_rate")
            require(
                isinstance(raw, dict)
                and raw.get("status") in {"available", "no_data"}
                and raw.get("unit") == "bpm"
                and isinstance(raw.get("readings"), list)
                and not (raw["status"] == "no_data" and raw["readings"])
            )
            flags = raw.get("quality_flags")
            allowed_flags = {
                "missing_values",
                "partial_source",
                "retained_previous_observations",
                "source_completeness_unknown",
                "retained_revision_unknown",
                "invalid_source_shape",
                "missing_or_conflicting_source_date",
                "invalid_or_oversize_curve",
                "invalid_point",
                "invalid_timestamp",
                "invalid_timestamp_unit_or_range",
                "invalid_value",
                "conflicting_source_observations",
                "duplicate_source_observations",
                "unverified_source_observations",
            }
            if flags is not None:
                require(
                    isinstance(flags, list)
                    and len(flags) <= 16
                    and all(isinstance(flag, str) and flag in allowed_flags for flag in flags)
                )
                channel["source_quality_flags"] = sorted(
                    set(channel.get("source_quality_flags", []) + flags)
                )
            if "missing_value_count" in raw:
                require(
                    type(raw["missing_value_count"]) is int
                    and 0 <= raw["missing_value_count"] <= 10000
                )
                channel["source_missing_value_count"] = (
                    channel.get("source_missing_value_count", 0) + raw["missing_value_count"]
                )
            if "source_composition" in raw:
                composition = raw["source_composition"]
                counts = {
                    "incoming_valid_points",
                    "retained_valid_points",
                    "new_valid_points",
                    "replaced_valid_points",
                }
                optional = {"conflicting_source_timestamps", "duplicate_source_points"}
                required = counts | {
                    "method_version",
                    "completeness",
                    "source_revisions",
                    "source_revision_count",
                    "source_revisions_truncated",
                }
                require(
                    isinstance(composition, dict)
                    and required <= set(composition) <= required | optional
                )
                require(
                    composition["method_version"] == "preserve-good-observations-v1"
                    and composition["completeness"] == "unknown"
                )
                require(
                    all(
                        type(composition[key]) is int and 0 <= composition[key] <= 10000
                        for key in (counts | optional) & set(composition)
                    )
                )
                revisions = composition["source_revisions"]
                require(
                    isinstance(revisions, list)
                    and len(revisions) <= 16
                    and all(
                        isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value)
                        for value in revisions
                    )
                    and len(set(revisions)) == len(revisions)
                )
                require(
                    type(composition["source_revisions_truncated"]) is bool
                    and type(composition["source_revision_count"]) is int
                    and len(revisions) <= composition["source_revision_count"] <= 100000
                )
                require(
                    composition["source_revisions_truncated"]
                    or composition["source_revision_count"] == len(revisions)
                )
                require(
                    composition["incoming_valid_points"]
                    == composition["new_valid_points"] + composition["replaced_valid_points"]
                )
                channel.setdefault("source_compositions", []).append(
                    {"date": source_day, **composition}
                )
            if channel["status"] == "not_stored":
                channel["status"] = "no_data" if raw["status"] == "no_data" else "available"
            self.input_points += len(raw["readings"])
            add_readings(channel, raw["readings"], False, start, end)
        channel = channels["hrv"]
        source = self.source("hrv", day)
        channel["source_dates"].append(day)
        if source:
            channel["source_revisions"].append({"date": day, "revision": source[1]})
            readings = source[0].get("readings")
            require(isinstance(readings, list))
            channel["status"] = "available" if readings else "no_data"
            self.input_points += len(readings)
            add_readings(channel, readings, True, start, end)
        # ceil(200000 * 365 / 90): finite raw work for the explicit annual scope.
        # Normalized prepare_reference points and byte bounds stay separate.
        point_limit = 811112 if day in self.annual_input_days else 200000
        if self.input_points > point_limit:
            raise _RawInputPointLimit("Stored native raw input point limit")
        for name, channel in channels.items():
            finish(channel, start, end, name == "heart_rate")
            channel["quality_flags"] = list(dict.fromkeys(channel["quality_flags"]))
        require(sum(ch["valid_points"] for ch in channels.values()) <= 4096)
        complete = all(
            ch["status"] in {"available", "no_data"}
            and not any(flag.startswith("source_") for flag in ch["quality_flags"])
            and not any(
                ch["quality"][k] for k in ("invalid", "ambiguous_time", "conflicting_timestamps")
            )
            and not ch.get("source_missing_value_count")
            and not ch.get("source_quality_flags")
            and not any(c["completeness"] == "unknown" for c in ch.get("source_compositions", []))
            for ch in channels.values()
        )
        output["status"] = "available" if complete else "partial"
        output["quality_flags"] = list(
            dict.fromkeys(output["quality_flags"] + ["coverage_is_nominal_observed_cadence"])
        )
        return self._finish_night(day, output)
