"""Closed HTTP method, route and query policy for the Personal collector."""

import re
from datetime import date

HOST = "https://personal.connectapi.garmin.com"
TOKEN_PATH = "/personalapitoken-service/personal-api/personal-api-key/token"
DAY = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
ID = r"[1-9][0-9]{0,19}"


def _day(value):
    if not isinstance(value, str) or not re.fullmatch(DAY, value):
        raise ValueError("Invalid source date")
    return date.fromisoformat(value)


def _range(start, end, limit):
    if not 1 <= (_day(end) - _day(start)).days + 1 <= limit:
        raise ValueError("Invalid source range")


def validate_request(path, params=None, *, token=False, binary=False):
    if not isinstance(path, str) or type(token) is not bool or type(binary) is not bool:
        raise ValueError("Invalid Personal request")
    params = {} if params is None else params
    if not isinstance(params, dict):
        raise ValueError("Invalid Personal parameters")
    if token:
        if path != TOKEN_PATH or params or binary:
            raise ValueError("Unreviewed token operation")
        return "POST"
    if path == "/userprofile-service/userprofile/settings" and not params and not binary:
        return "GET"
    if re.fullmatch(r"/download-service/files/activity/" + ID, path) and not params and binary:
        return "GET"
    if binary:
        raise ValueError("Unreviewed binary operation")
    if path == "/sleep-service/sleep/dailySleepSummariesByDate":
        if set(params) != {"startDate", "endDate"}:
            raise ValueError("Unreviewed query")
        _range(params["startDate"], params["endDate"], 31)
        return "GET"
    if path == "/wellness-service/wellness/bodyBattery/reports/daily":
        if (
            set(params) != {"startDate", "endDate", "chunks"}
            or type(params["chunks"]) is not int
            or params["chunks"] != 288
        ):
            raise ValueError("Unreviewed query")
        _range(params["startDate"], params["endDate"], 31)
        return "GET"
    if path in {
        "/wellness-service/wellness/dailyHeartRate",
        "/usersummary-service/usersummary/daily",
    }:
        key = "date" if path.endswith("dailyHeartRate") else "calendarDate"
        if set(params) != {key}:
            raise ValueError("Unreviewed query")
        _day(params[key])
        return "GET"
    if path == "/activitylist-service/activities/search/activities":
        if (
            set(params) != {"startDate", "endDate", "start", "limit", "sortOrder"}
            or params["sortOrder"] != "desc"
        ):
            raise ValueError("Unreviewed query")
        _range(params["startDate"], params["endDate"], 366)
        for key, low, high in [("start", 0, 100000), ("limit", 1, 20)]:
            value = params[key]
            if (
                not isinstance(value, str)
                or not re.fullmatch(r"[0-9]{1,6}", value)
                or not low <= int(value) <= high
            ):
                raise ValueError("Unreviewed pagination")
        return "GET"
    if params:
        raise ValueError("Unreviewed query")
    match = re.fullmatch(r"/hrv-service/hrv/(" + DAY + r")", path)
    if match:
        _day(match[1])
        return "GET"
    for prefix, limit in [
        ("/hrv-service/hrv/daily/", 31),
        ("/usersummary-service/stats/steps/daily/", 28),
        ("/weight-service/weight/all/", 31),
    ]:
        match = re.fullmatch(re.escape(prefix) + "(" + DAY + ")/(" + DAY + ")", path)
        if match:
            _range(match[1], match[2], limit)
            return "GET"
    if re.fullmatch(r"/activity-service/activity/" + ID, path):
        return "GET"
    raise ValueError("Unreviewed Personal request")
