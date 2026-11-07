"""Bounded rolling source inventory and independent publication fences."""

from __future__ import annotations

import re
from datetime import date, timedelta
from urllib.parse import unquote_to_bytes
from xml.etree import ElementTree

from .native_night import require
from .r2_store import _opaque_revision

ROOTS = ("health/sleep/v1/", "health/hrv/", "health/signals/v1/")
CONTEXT = "health/night-context/v1/"


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


def rolling_source_prefixes(days):
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


def prime_rolling_list_bound(days):
    return sum((bound + 999) // 1000 for bound in rolling_source_prefixes(days).values())


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


def _proof_prefixes(proof):
    prefixes = dict(_month(key) for key in proof["objects"])
    for prefix in proof["lists"]:
        require(re.fullmatch(r"health/night-context/v1/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}/", prefix)
                is not None, "Rolling context proof unverified")
        prefixes[prefix.rsplit("/", 2)[0] + "/"] = 3101
    require(len(prefixes) <= 24, "Rolling proof month bound")
    return prefixes


def rolling_fence_list_bound(proof):
    return sum((bound + 999) // 1000 for bound in _proof_prefixes(proof).values())


def validate_rolling_sources(store, proof):
    """Fresh native LIST evidence at each fence; local caches never establish freshness."""
    inventories = {prefix: store.list_revisions_bounded(prefix, max_keys=bound)
                   for prefix, bound in sorted(_proof_prefixes(proof).items())}
    for key, binding in proof["objects"].items():
        current = inventories[_month(key)[0]].get(key)
        if binding["status"] == "absent":
            require(current is None, "Night source appeared during calculation")
        else:
            require(binding["status"] == "present" and current is not None
                    and current["revision"] == binding["revision"]
                    and ("size" not in binding or current["size"] == binding["size"]),
                    "Night source revision changed")
    for prefix, expected in proof["lists"].items():
        inventory = inventories[prefix.rsplit("/", 2)[0] + "/"]
        keys = sorted(key for key in inventory if key.startswith(prefix))
        require(len(keys) <= 100 and keys == expected, "Night context revision changed")
