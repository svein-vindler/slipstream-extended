"""One reviewed date-filtered page; no unbounded SDK pagination or retries."""
from __future__ import annotations

import re
from datetime import date, datetime

PAGE_SIZE = 20  # Same as the pinned SDK's date-range paginator.
MAX_OFFSET = 100_000
MAX_METADATA_CALLS = 10
MAX_PAGE_BYTES = 1024 * 1024


def bounded_int(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("Activity pagination integer is outside its allowed bounds")
    return value


def page_params(start_date, end_date, offset, limit=PAGE_SIZE):
    for value in (start_date, end_date):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError("Dates must use YYYY-MM-DD")
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date cannot be after end_date")
    if (end - start).days + 1 > 366:
        raise ValueError("Activity pagination range must contain 1 to 366 days")
    bounded_int(offset, 0, MAX_OFFSET)
    bounded_int(limit, 1, PAGE_SIZE)
    return {"startDate": start_date, "endDate": end_date,
            "start": str(offset), "limit": str(limit), "sortOrder": "desc"}


def validate_page(rows, start_date, end_date, limit=PAGE_SIZE):
    # None, objects, partial/unknown rows and out-of-range responses are errors,
    # never evidence that the source is exhausted. Prefer Garmin's local day.
    if not isinstance(rows, list) or len(rows) > limit:
        raise ValueError("Invalid activity metadata page")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Invalid activity metadata row")
        aid = row.get("activityId")
        if isinstance(aid, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(aid)):
            raise ValueError("Invalid activity metadata identifier")
        kind = row.get("activityType")
        local = row.get("startTimeLocal")
        if (not isinstance(kind, dict) or not isinstance(kind.get("typeKey"), str)
                or not kind["typeKey"] or not isinstance(local, str)):
            raise ValueError("Incomplete activity metadata")
        try:
            day = datetime.fromisoformat(local).date().isoformat()
        except ValueError:
            raise ValueError("Invalid local activity time") from None
        if not start_date <= day <= end_date:
            raise ValueError("Activity metadata is outside the requested local dates")
    return rows


class MetadataBudget:
    """Shared across scheduler ranges; a failed page also consumes a call."""
    def __init__(self, limit=MAX_METADATA_CALLS):
        self.limit = bounded_int(limit, 1, MAX_METADATA_CALLS)
        self.calls = 0

    @property
    def remaining(self):
        return self.limit - self.calls

    def fetch(self, garmin, start_date, end_date, offset):
        page_params(start_date, end_date, offset)
        if not self.remaining:
            raise RuntimeError("Activity metadata call budget exhausted")
        self.calls += 1
        rows = garmin.get_activity_page(start_date, end_date, offset=offset)
        validate_page(rows, start_date, end_date)
        # Protect checkpoint size before accepting an unexpectedly large page.
        import json
        if len(json.dumps(rows).encode("utf-8")) > MAX_PAGE_BYTES:
            raise ValueError("Activity metadata page exceeds the payload limit")
        return rows
