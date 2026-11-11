"""Bounded rolling source inventory and independent publication fences."""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from urllib.parse import unquote_to_bytes
from xml.etree import ElementTree

from .native_night import require
from .r2_store import _opaque_revision

ROOTS = ("health/sleep/v1/", "health/hrv/", "health/signals/v1/")
CONTEXT = "health/night-context/v1/"
ANNUAL_RAW_KEYS = 800
ANNUAL_CONTEXT_KEYS = 3101
ANNUAL_INVENTORY_BYTES = 2097152


def parse_revision_inventory(body):
    """Read exact logical Key/Size/quoted ETag triples from bounded native XML."""
    require(isinstance(body, bytes) and len(body) <= 1048576, "Revision LIST body bound")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeError:
        require(False, "Revision LIST encoding unverified")
    require("\x00" not in text and "<!DOCTYPE" not in text.upper()
            and "<!ENTITY" not in text.upper(), "Revision LIST XML declarations forbidden")
    declaration = re.match(r"\s*<\?xml\b([^?]*?)\?>", text)
    if declaration:
        encoding = re.search(r"\bencoding\s*=\s*(['\"])(.*?)\1", declaration[1], re.I)
        require(encoding is None or encoding[2].lower() in {"utf-8", "utf8", "us-ascii", "ascii"},
                "Revision LIST encoding unsupported")
        require(encoding is None or encoding[2].lower() not in {"us-ascii", "ascii"}
                or text.isascii(), "Revision LIST encoding disagreement")
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        require(False, "Revision LIST XML unverified")

    def name(node):
        return node.tag.rsplit("}", 1)[-1]

    def single(parent, field, optional=False):
        found = [node for node in parent if name(node) == field]
        require(len(found) == 1 or optional and not found, "Revision LIST field unverified")
        if not found:
            return None
        require(not list(found[0]), "Revision LIST nested field")
        return found[0].text or ""

    require(name(root) == "ListBucketResult", "Revision LIST root unverified")
    encoding = single(root, "EncodingType", True)
    require(encoding in {None, "url"}, "Revision LIST encoding unsupported")
    rows = [row for row in root if name(row) == "Contents"]
    require(len(rows) <= 1000, "Revision LIST row bound")
    result = {}
    for row in rows:
        key, size, revision = single(row, "Key"), single(row, "Size"), single(row, "ETag")
        require(len(key.encode()) <= (3072 if encoding == "url" else 1024),
                "Revision LIST key bound")
        if encoding == "url":
            require(re.search(r"%(?![0-9a-fA-F]{2})", key) is None,
                    "Revision LIST escape unverified")
            try:
                key = unquote_to_bytes(key).decode("utf-8", errors="strict")
            except UnicodeError:
                require(False, "Revision LIST UTF8 unverified")
        require(0 < len(key.encode()) <= 1024 and key not in result
                and not any(ord(c) < 32 or ord(c) == 127 for c in key)
                and re.fullmatch(r"[0-9]{1,20}", size.strip())
                and _opaque_revision(revision) is not None, "Revision LIST row unverified")
        result[key] = {"revision": revision, "size": int(size)}
    return result


def _annual_dates(days, *, maximum=365):
    values = sorted(set(days))
    require(365 <= len(values) <= maximum, "Annual source window bound")
    dates = [date.fromisoformat(day) for day in values]
    require(all(day.isoformat() == text for day, text in zip(dates, values, strict=True))
            and (dates[-1] - dates[0]).days == len(dates) - 1,
            "Annual source calendar bound")
    return dates


def _annual_prefixes(dates):
    years = {day.strftime("%Y/") for day in dates}
    signal_years = {(day + timedelta(days=offset)).strftime("%Y/")
                    for day in dates for offset in range(-4, 3)}
    # These limits cover each ENTIRE physical year, including unselected months.
    return {root + year: ANNUAL_RAW_KEYS for root in ROOTS for year in
            (signal_years if root == ROOTS[2] else years)} | {
                CONTEXT + year: ANNUAL_CONTEXT_KEYS for year in years}


def rolling_source_prefixes(days, *, window_days=None):
    require(window_days is None or type(window_days) is int and window_days == 365,
            "Choose an explicit annual source window")
    if window_days == 365:
        return _annual_prefixes(_annual_dates(days))
    values = sorted(set(days))
    require(0 < len(values) <= 90, "Rolling source window bound")
    dates = [date.fromisoformat(day) for day in values]
    require(all(day.isoformat() == text for day, text in zip(dates, values, strict=True))
            and (dates[-1] - dates[0]).days <= 89, "Rolling source calendar bound")
    months = {day.strftime("%Y/%m/") for day in dates}
    # Sleep lasts at most36h and wake binding permits one UTC day difference;
    # observed Garmin/actual wall clocks can differ by up to14h.
    signal_months = {(day + timedelta(days=offset)).strftime("%Y/%m/")
                     for day in dates for offset in range(-4, 3)}
    return {root + month: 63 for root in ROOTS for month in
            (signal_months if root == ROOTS[2] else months)} | {
                CONTEXT + month: 3101 for month in months}


def prime_rolling_list_bound(days, *, window_days=None):
    return sum((bound + 999) // 1000
               for bound in rolling_source_prefixes(days, window_days=window_days).values())


def _month(key):
    match = re.fullmatch(
        r"(health/(?:sleep/v1|hrv|signals/v1)/\d{4}/\d{2}/)\d{4}-\d{2}-\d{2}\.json(?:\.gz)?",
        key,
    )
    if match:
        return match[1], 63
    match = re.fullmatch(r"(health/night-context/v1/\d{4}/\d{2}/)\d{4}-\d{2}-\d{2}/.+", key)
    require(match is not None, "Rolling proof object namespace unverified")
    return match[1], 3101


def _annual_object(key):
    match = re.fullmatch(
        r"(health/(?:sleep/v1|hrv|signals/v1)/)(\d{4})/(\d{2})/(\d{4}-\d{2}-\d{2})"
        r"\.json(?:\.gz)?", key)
    if match is None:
        match = re.fullmatch(
            r"(health/night-context/v1/)(\d{4})/(\d{2})/(\d{4}-\d{2}-\d{2})/"
            r"\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json", key)
    require(match is not None, "Annual source namespace unverified")
    root, year, month, text = match.groups()
    require(date.fromisoformat(text).isoformat() == text and text[:7] == year + "-" + month,
            "Annual source calendar unverified")
    return root, text, root + year + "/"


def _annual_proof_prefixes(proof):
    # A sealed <=7-endpoint cohort may combine overlapping annual dependencies.
    # Missing sleep still contributes exact absent object dates; its context
    # list need not be retained just to declare an empty night's annual extent.
    dates = set()
    for key in proof["objects"]:
        root, day, _ = _annual_object(key)
        if root == ROOTS[0]:
            dates.add(day)
    for prefix in proof["lists"]:
        match = re.fullmatch(r"health/night-context/v1/(\d{4})/(\d{2})/(\d{4}-\d{2}-\d{2})/", prefix)
        require(match is not None and match[3][:7] == match[1] + "-" + match[2],
                "Annual context proof unverified")
        dates.add(match[3])
    parsed = _annual_dates(dates, maximum=371)
    prefixes = _annual_prefixes(parsed)
    first, last = parsed[0].isoformat(), parsed[-1].isoformat()
    for key in proof["objects"]:
        root, day, physical = _annual_object(key)
        margin_first = (parsed[0] - timedelta(days=4)).isoformat() if root == ROOTS[2] else first
        margin_last = (parsed[-1] + timedelta(days=2)).isoformat() if root == ROOTS[2] else last
        require(physical in prefixes and margin_first <= day <= margin_last,
                "Annual object outside declared source calendar")
    return prefixes


def _proof_prefixes(proof, *, window_days=None):
    require(window_days is None or type(window_days) is int and window_days == 365,
            "Choose an explicit annual source window")
    if window_days == 365:
        return _annual_proof_prefixes(proof)
    prefixes = dict(_month(key) for key in proof["objects"])
    for prefix in proof["lists"]:
        require(re.fullmatch(r"health/night-context/v1/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}/", prefix)
                is not None, "Rolling context proof unverified")
        prefixes[prefix.rsplit("/", 2)[0] + "/"] = 3101
    require(len(prefixes) <= 24, "Rolling proof month bound")
    return prefixes


def rolling_fence_list_bound(proof, *, window_days=None):
    return sum((bound + 999) // 1000
               for bound in _proof_prefixes(proof, window_days=window_days).values())


def _read_inventories(store, prefixes, *, window_days=None):
    result = {}
    metadata_bytes = 0
    observer = getattr(store, "observer", None)
    wire_before = observer.state["read_bytes"] if observer is not None else None
    for prefix, bound in sorted(prefixes.items()):
        rows = store.list_revisions_bounded(prefix, max_keys=bound)
        require(len(rows) <= bound, "Rolling inventory key bound")
        for key, binding in rows.items():
            require(isinstance(key, str) and key.startswith(prefix)
                    and isinstance(binding, dict)
                    and isinstance(binding.get("revision"), str)
                    and type(binding.get("size")) is int and binding["size"] >= 0,
                    "Rolling source inventory unverified")
            if window_days == 365:
                _, _, physical = _annual_object(key)
                require(physical == prefix and _opaque_revision(binding["revision"]) is not None,
                        "Annual inventory revision unverified")
        if window_days == 365:
            # Count all returned names, including legitimate dates outside the
            # selected window. The native observer separately charges every
            # physical XML byte/page, including XML overhead and padding.
            metadata_bytes += len(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode())
            require(metadata_bytes <= ANNUAL_INVENTORY_BYTES, "Annual inventory metadata byte bound")
            if wire_before is not None:
                require(observer.state["read_bytes"] - wire_before <= ANNUAL_INVENTORY_BYTES,
                        "Annual inventory physical byte bound")
        result[prefix] = rows
    return result


def _inventory_for(key, inventories):
    matches = [rows for prefix, rows in inventories.items() if key.startswith(prefix)]
    require(len(matches) == 1, "Rolling proof inventory missing")
    return matches[0]


def validate_rolling_sources(store, proof, *, window_days=None):
    """Fresh native LIST evidence at each fence; local caches never establish freshness."""
    inventories = _read_inventories(store, _proof_prefixes(proof, window_days=window_days),
                                   window_days=window_days)
    for key, binding in proof["objects"].items():
        current = _inventory_for(key, inventories).get(key)
        if binding["status"] == "absent":
            require(current is None, "Night source appeared during calculation")
        else:
            require(binding["status"] == "present" and current is not None
                    and current["revision"] == binding["revision"]
                    and ("size" not in binding or current["size"] == binding["size"]),
                    "Night source revision changed")
    for prefix, expected in proof["lists"].items():
        inventory = _inventory_for(prefix, inventories)
        keys = sorted(key for key in inventory if key.startswith(prefix))
        require(len(keys) <= 100 and keys == expected, "Night context revision changed")
