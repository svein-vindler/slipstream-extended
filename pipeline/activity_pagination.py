"""Private progress inside the existing range manifest, not a new storage system.

Offsets are observations, not snapshots. Check head/overlap on resume and require
two matching whole-range passes (including an empty page) before source end.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re

from .granular import json_bytes
from .granular_export import activity_artifact_keys
from .sources.activity_page import MAX_OFFSET, PAGE_SIZE, bounded_int, validate_page

MAX_RECORDS = 10_000
MAX_PENDING = 1_000
MAX_CHECKPOINT_BYTES = 4 * 1024 * 1024
ZERO_DIGEST = "0" * 64
ARTIFACT_PATH = re.compile(
    r"activities/[0-9]{4}/([1-9][0-9]{0,19})/activity\.(?:fit|tcx|v1\.json|endurance\.v1\.json)"
)
ARTIFACT_NAMES = ("activity.fit", "activity.v1.json", "activity.tcx", "activity.endurance.v1.json")


class PaginationLimitError(ValueError):
    """A capacity boundary needs a smaller range, not automatic retries."""


def fingerprint(activity):
    # Local import avoids the existing refresh -> backfill import cycle. Keep
    # the established source-change definition rather than inventing another.
    from .activity_refresh import activity_fingerprint
    return activity_fingerprint(activity)


def signatures(rows):
    return [f"{row['activityId']}:{fingerprint(row)}" for row in rows]


def reset_scan(cursor):
    cursor.update(offset=0, anchor=[], head=[], digest=ZERO_DIGEST,
                  previous_digest=None, source_exhausted=False)


def encode_progress(progress):
    """Wire v2 derives fixed keys from ID/year/count; pending source stays intact.

    Keep the expanded v1 working model and its existing decoded payload limit.
    This is ordinary compact JSON, not an unbounded compression/decode layer.
    """
    if len(json_bytes(progress)) > MAX_CHECKPOINT_BYTES:
        raise ValueError("Decoded activity backfill progress exceeds payload limit")
    cursor = progress["pagination"]
    records = {}
    for aid, row in cursor["records"].items():
        keys = row["keys"]
        records[aid] = [row["fingerprint"], keys[0].split("/")[1] if keys else None, len(keys)]
    wire = dict(progress, pagination=dict(cursor, version=2, records=records))
    encoded = json_bytes(wire)
    if len(encoded) > MAX_CHECKPOINT_BYTES:
        raise ValueError("Stored activity backfill progress exceeds payload limit")
    return encoded


def _expand_records(records):
    if not isinstance(records, dict) or len(records) > MAX_RECORDS:
        raise ValueError("Activity pagination record limit exceeded")
    expanded = {}
    for aid, packed in records.items():
        if (not isinstance(aid, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", aid)
                or not isinstance(packed, list) or len(packed) != 3):
            raise ValueError("Invalid compact activity pagination record")
        fp, year, count = packed
        if (not isinstance(fp, str) or not re.fullmatch(r"[a-f0-9]{64}", fp)
                or type(count) is not int or count not in {0, 2, 4}):
            raise ValueError("Invalid compact activity pagination record")
        if count == 0:
            if year is not None:
                raise ValueError("Invalid compact unsupported activity")
            keys = []
        else:
            if (not isinstance(year, str) or not re.fullmatch(r"[0-9]{4}", year)
                    or year == "0000"):
                raise ValueError("Invalid compact activity year")
            keys = [f"activities/{year}/{aid}/{name}" for name in ARTIFACT_NAMES[:count]]
        expanded[aid] = {"fingerprint": fp, "keys": keys}
    return expanded


def load_cursor(previous, start_date, end_date, *, reconcile=False):
    value = previous.get("pagination")
    if value is None:
        cursor = {"version": 1, "records": {}, "pending": {}}
        reset_scan(cursor)
        return cursor
    if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] not in {1, 2}:
        raise ValueError("Unsupported activity pagination checkpoint")
    if (previous.get("start_date") != start_date or previous.get("end_date") != end_date):
        raise ValueError("Activity pagination checkpoint scope mismatch")
    cursor = copy.deepcopy(value)
    if cursor["version"] == 2:
        cursor["records"] = _expand_records(cursor.get("records"))
        cursor["version"] = 1  # Same validated working model for both wire versions.
    if len(json_bytes(dict(previous, pagination=cursor))) > MAX_CHECKPOINT_BYTES:
        raise ValueError("Decoded activity backfill progress exceeds payload limit")
    bounded_int(cursor.get("offset"), 0, MAX_OFFSET)
    if cursor["offset"] % PAGE_SIZE:
        raise ValueError("Invalid activity pagination offset")
    if type(cursor.get("source_exhausted")) is not bool:
        raise ValueError("Invalid activity pagination source state")
    for key in ("digest", "previous_digest"):
        digest = cursor.get(key)
        if key == "previous_digest" and digest is None:
            continue
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Invalid activity pagination digest")
    for key in ("head", "anchor"):
        if (not isinstance(cursor.get(key), list) or len(cursor[key]) > PAGE_SIZE
                or any(not isinstance(s, str) or len(s) > 85 for s in cursor[key])):
            raise ValueError("Invalid activity pagination boundary")
    if cursor["offset"] and (not cursor["head"] or not cursor["anchor"]):
        raise ValueError("Missing activity pagination boundary")
    records, pending = cursor.get("records"), cursor.get("pending")
    if not isinstance(records, dict) or len(records) > MAX_RECORDS:
        raise ValueError("Activity pagination record limit exceeded")
    if not isinstance(pending, dict) or len(pending) > MAX_PENDING:
        raise ValueError("Activity pagination pending limit exceeded")
    for aid, record in records.items():
        if (not re.fullmatch(r"[1-9][0-9]{0,19}", aid)
                or not isinstance(record, dict)
                or not isinstance(record.get("fingerprint"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", record["fingerprint"])
                or not isinstance(record.get("keys"), list) or len(record["keys"]) not in {0, 2, 4}):
            raise ValueError("Invalid activity pagination record")
        # Stored keys must remain tied to the same fixed canonical activity ID.
        for key in record["keys"]:
            match = ARTIFACT_PATH.fullmatch(key) if isinstance(key, str) else None
            if match is None or match[1] != aid or key.split("/")[1] == "0000":
                raise ValueError("Invalid activity pagination artifact reference")
        if (len(set(record["keys"])) != len(record["keys"])
                or len({k.split("/")[1] for k in record["keys"]}) > 1
                or (record["keys"] and {k.rsplit("/", 1)[-1] for k in record["keys"]} not in (
                    {"activity.fit", "activity.v1.json"},
                    {"activity.fit", "activity.v1.json", "activity.tcx", "activity.endurance.v1.json"}))):
            raise ValueError("Invalid activity pagination required artifacts")
    for aid, item in pending.items():
        if not isinstance(item, dict) or type(item.get("force")) is not bool:
            raise ValueError("Invalid pending activity state")
        validate_page([item.get("activity")], start_date, end_date)
        if str(item["activity"]["activityId"]) != aid or aid not in records:
            raise ValueError("Invalid pending activity reference")
        if records[aid]["keys"] != list(activity_artifact_keys(item["activity"])):
            raise ValueError("Invalid pending activity artifacts")
        if records[aid]["fingerprint"] != fingerprint(item["activity"]):
            raise ValueError("Invalid pending activity fingerprint")
    if cursor["source_exhausted"] and cursor["previous_digest"] != cursor["digest"]:
        raise ValueError("Unconfirmed activity pagination source end")
    if reconcile:
        reset_scan(cursor)  # Preserve incomplete files, failures and fingerprints.
    return cursor


class ActivityPager:
    def __init__(self, cursor, *, garmin, budget, start_date, end_date):
        self.cursor, self.garmin, self.budget = cursor, garmin, budget
        self.start_date, self.end_date = start_date, end_date
        self.first_page = None
        self.restarted = False

    def fetch(self, offset):
        return self.budget.fetch(self.garmin, self.start_date, self.end_date, offset)

    def prepare(self):
        """At most one restart per run; cache the probe if it becomes page zero."""
        c = self.cursor
        if not c["offset"] or c["source_exhausted"]:
            return
        head = self.fetch(0)
        self.first_page = head
        if signatures(head) == c["head"]:
            if c["offset"] == PAGE_SIZE:
                anchor = head
            elif self.budget.remaining:
                anchor = self.fetch(c["offset"] - PAGE_SIZE)
            else:
                return
            if signatures(anchor) == c["anchor"]:
                self.first_page = None
                return
        reset_scan(c)
        self.restarted = True

    def next_page(self, existing_keys, supported):
        c = self.cursor
        rows = self.first_page if self.first_page is not None and c["offset"] == 0 else self.fetch(c["offset"])
        self.first_page = None
        if not rows:
            # A full or short page alone never proves source end. Confirm the
            # same ordered metadata twice; arbitrary changes still need an
            # explicit future reconciliation because Garmin has no snapshot.
            if c["previous_digest"] == c["digest"]:
                c["source_exhausted"] = True
            else:
                c.update(previous_digest=c["digest"], offset=0, anchor=[], head=[], digest=ZERO_DIGEST)
            return
        records = dict(c["records"])
        pending = dict(c["pending"])
        for row in rows:
            aid, fp = str(row["activityId"]), fingerprint(row)
            old = records.get(aid)
            keys = list(activity_artifact_keys(row)) if supported(row) else []
            changed = bool(old and old["fingerprint"] != fp)
            records[aid] = {"fingerprint": fp, "keys": keys}
            if not keys:
                pending.pop(aid, None)  # Explicit, validated unsupported type.
            elif changed or not all(k in existing_keys for k in keys):
                pending[aid] = {"activity": row, "force": changed or pending.get(aid, {}).get("force", False)}
            # A previous failed forced import stays pending even if all filenames
            # exist: some may still contain the old revision.
        if (len(records) > MAX_RECORDS or len(pending) > MAX_PENDING
                or c["offset"] + PAGE_SIZE > MAX_OFFSET):
            raise PaginationLimitError("Activity pagination checkpoint limit reached")
        candidate = dict(c, records=records, pending=pending)
        if len(json.dumps(candidate).encode("utf-8")) > MAX_CHECKPOINT_BYTES - 256 * 1024:
            raise PaginationLimitError("Activity pagination checkpoint payload limit reached")
        sig = signatures(rows)
        if c["offset"] == 0:
            c["head"] = sig
        c.update(records=records, pending=pending, anchor=sig,
                 offset=c["offset"] + PAGE_SIZE,
                 digest=hashlib.sha256((c["digest"] + json.dumps(sig)).encode()).hexdigest())


def observed_status(remaining, blocked, source_exhausted):
    """Status of validated observations; no checkpoint decoding or copying."""
    if type(remaining) is not int or type(blocked) is not int or not 0 <= blocked <= remaining:
        raise ValueError("Invalid activity backfill progress counts")
    if type(source_exhausted) is not bool:
        raise ValueError("Invalid activity pagination source state")
    if not source_exhausted:
        return "active"
    return "complete" if remaining == 0 else "blocked" if remaining <= blocked else "active"


def range_status(progress):
    """Keep legacy completed ranges readable, reject unknown future schemas."""
    schema = progress.get("schema_version", 2)
    if type(schema) is not int or schema not in {1, 2}:
        raise ValueError("Unsupported activity backfill progress schema")
    paging = progress.get("pagination")
    if paging is not None:
        load_cursor(progress, progress.get("start_date"), progress.get("end_date"))
    return observed_status(progress.get("remaining_activities"),
                           progress.get("blocked_after_three_failures"),
                           paging is None or paging["source_exhausted"])
