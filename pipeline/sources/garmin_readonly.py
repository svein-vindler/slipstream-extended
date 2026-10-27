"""Fail-closed Garmin data access, independent of the session's write privileges.

This is an application boundary, not a sandbox for arbitrary Python code. Auth
uses the SDK's separate SSO session; fitness API traffic is GET-only and limited
to reviewed endpoints. Never expose the underlying SDK client through MCP.
"""
from __future__ import annotations

import inspect
import re
from datetime import date
from functools import wraps
from importlib.metadata import version
from urllib.parse import unquote, urlsplit

import requests
from garminconnect.client import Client, GarminConnectAuthenticationError
from requests.adapters import HTTPAdapter

from ..garmin_sdk import CURRENT_SDK_VERSION
from .activity_page import page_params
from .garmin_policy import GarminReadOnlyViolation
from .training_readiness import ReadinessRead, bounded_read

_READINESS_ROUTE = r"/metrics-service/metrics/trainingreadiness/([0-9]{4}-[0-9]{2}-[0-9]{2})"
_STRESS_ROUTE = r"/wellness-service/wellness/dailyStress/([0-9]{4}-[0-9]{2}-[0-9]{2})"
_REVIEWED_HEADERS = Client.get_api_headers
_REVIEWED_ADAPTER_SEND = HTTPAdapter.send
_REVIEWED_SESSION_SEND = requests.Session.send

READ_METHODS = frozenset({
    "get_training_readiness_bounded", "get_stress_data",
    "get_activity_page",
    "get_activities_by_date", "get_activity", "get_activity_exercise_sets",
    "download_activity", "get_daily_steps", "get_sleep_daily", "get_hrv_data_range",
    "get_body_battery", "get_weigh_ins", "get_stats", "get_sleep_data",
    "get_hrv_data", "get_daily_weigh_ins", "get_heart_rates", "get_respiration_data",
})
_DAY = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
_ID = r"[1-9][0-9]{0,19}"
_USER = r"[^/\\?#;%\x00-\x20]{1,128}"
_ROUTES = (
    (r"/userprofile-service/socialProfile", frozenset()),
    (r"/userprofile-service/userprofile/user-settings", frozenset()),
    (r"/activitylist-service/activities/search/activities",
     frozenset({"startDate", "endDate", "start", "limit", "activityType", "sortOrder"})),
    (rf"/activity-service/activity/{_ID}(?:/exerciseSets)?", frozenset()),
    (rf"/download-service/(?:files/activity|export/(?:tcx|gpx)/activity)/{_ID}", frozenset()),
    (rf"/usersummary-service/usersummary/daily/{_USER}", frozenset({"calendarDate"})),
    (rf"/usersummary-service/stats/steps/daily/{_DAY}/{_DAY}", frozenset()),
    (rf"/sleep-service/stats/sleep/daily/{_DAY}/{_DAY}", frozenset()),
    (rf"/wellness-service/wellness/dailySleepData/{_USER}", frozenset({"date", "nonSleepBufferMinutes"})),
    (rf"/wellness-service/wellness/dailyHeartRate/{_USER}", frozenset({"date"})),
    (r"/wellness-service/wellness/bodyBattery/reports/daily", frozenset({"startDate", "endDate"})),
    (rf"/wellness-service/wellness/daily/respiration/{_DAY}", frozenset()),
    (rf"/hrv-service/hrv/(?:{_DAY}|daily/{_DAY}/{_DAY})", frozenset()),
    (rf"/weight-service/weight/(?:dayview/{_DAY}|range/{_DAY}/{_DAY})", frozenset({"includeAll"})),
)


def _deny():
    raise GarminReadOnlyViolation("Garmin read-only policy rejected this operation.")


def _valid_day(value):
    if not isinstance(value, str) or not re.fullmatch(_DAY, value):
        _deny()
    try:
        date.fromisoformat(value)
    except ValueError:
        _deny()


def _validate_path(path, params=None):
    if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
        _deny()
    decoded = unquote(path)
    if (any(character in decoded for character in "\\?#%;")
            or any(ord(character) < 32 or ord(character) == 127 for character in decoded)
            or any(segment in {".", ".."} for segment in decoded.split("/"))):
        _deny()
    for pattern, allowed_keys in _ROUTES:
        if re.fullmatch(pattern, decoded):
            keys = allowed_keys
            break
    else:
        _deny()
    if params is None:
        return
    if not isinstance(params, dict) or not params.keys() <= keys:
        _deny()
    for key, value in params.items():
        if key in {"date", "calendarDate", "startDate", "endDate"}:
            _valid_day(value)
        elif key in {"start", "limit", "nonSleepBufferMinutes"}:
            if isinstance(value, bool) or not re.fullmatch(r"[0-9]{1,10}", str(value)):
                _deny()
        elif key == "includeAll":
            if value is not True:
                _deny()
        elif key == "sortOrder":
            if value not in {"asc", "desc"}:
                _deny()
        elif key == "activityType":
            if value not in {"cycling", "running", "swimming", "multi_sport", "fitness_equipment",
                             "hiking", "walking", "other"}:
                _deny()


def _validate_options(path, kwargs):
    if not kwargs.keys() <= {"params", "headers", "timeout", "allow_redirects"}:
        _deny()
    _validate_path(path, kwargs.get("params"))
    headers = kwargs.get("headers", {})
    if (not isinstance(headers, dict) or any(not isinstance(key, str) or key.lower() != "accept"
                                           for key in headers)
            or any(not isinstance(value, str) or "\r" in value or "\n" in value
                   for value in headers.values())):
        _deny()  # In particular, deny HTTP method-override headers.
    if kwargs.get("allow_redirects") not in {None, False}:
        _deny()


def _install_transport_policy(sdk):
    """Check before token refresh and again at the SDK fitness HTTP session."""
    try:
        client = sdk.client
        original = client._run_request
        session = client._api_session
        send = session.request
        origin = urlsplit(client._connectapi)
        api_read = sdk.connectapi
        download = sdk.download
    except AttributeError:
        _deny()  # SDK transport changes require review; never silently bypass.
    if (origin.scheme != "https" or origin.netloc not in {"connectapi.garmin.com", "connectapi.garmin.cn"}
            or origin.path or origin.query or origin.fragment):
        _deny()
    readiness_capability = object()
    stress_capability = object()

    def readiness_path(path):
        match = re.fullmatch(_READINESS_ROUTE, path)
        if not match:
            _deny()
        _valid_day(match[1])

    def stress_path(path):
        match = re.fullmatch(_STRESS_ROUTE, path)
        if not match:
            _deny()
        _valid_day(match[1])
    def readiness_transport_valid():
        # Pin the reviewed SDK/session/header surface. No saved, unguarded send
        # is exposed to the application; the lower guard is always traversed.
        if (version("garminconnect") != CURRENT_SDK_VERSION or type(client) is not Client
                or type(session) is not requests.Session
                or sdk.client is not client or client._run_request is not read_request
                or client._api_session is not session or session.request is not send_read
                or client._connectapi != f"https://{origin.netloc}"
                or getattr(client.get_api_headers, "__func__", None) is not _REVIEWED_HEADERS
                or getattr(session.send, "__func__", None) is not _REVIEWED_SESSION_SEND
                or session.verify is not True or session.auth is not None or session.params
                or any(session.hooks.values())
                or any(str(key).lower() in {"x-http-method-override", "x-http-method", "x-method-override"}
                       for key in session.headers)):
            _deny()
        adapter = session.get_adapter(f"https://{origin.netloc}/")
        if (type(adapter) is not HTTPAdapter or adapter.max_retries.total != 0
                or adapter.max_retries.connect not in {None, 0}
                or adapter.max_retries.read not in {None, 0}
                or getattr(adapter.send, "__func__", None) is not _REVIEWED_ADAPTER_SEND):
            _deny()

    @wraps(original)
    def read_request(method, path, **kwargs):
        if method != "GET":
            _deny()
        _validate_options(path, kwargs)
        kwargs["allow_redirects"] = False
        return original(method, path, **kwargs)

    @wraps(send)
    def send_read(method, url, **kwargs):
        context = kwargs.pop("_readiness_context", None)
        stress_context = kwargs.pop("_stress_context", None)
        readiness = isinstance(context, tuple) and len(context) == 2 and context[0] is readiness_capability
        stress = (isinstance(stress_context, tuple) and len(stress_context) == 2
                  and stress_context[0] is stress_capability)
        if (context is not None and not readiness or stress_context is not None and not stress
                or readiness and stress):
            _deny()
        bounded = readiness or stress
        if stress:
            context = stress_context
        if (method != "GET" or not isinstance(url, str)
                or not kwargs.keys() <= ({"headers", "timeout", "allow_redirects", "stream", "verify"}
                                        if bounded else {"params", "headers", "timeout", "allow_redirects"})):
            _deny()
        target = urlsplit(url)
        if (target.scheme != "https" or target.netloc != origin.netloc
                or target.query or target.fragment or any(key in kwargs for key in ("data", "json", "files"))):
            _deny()
        if bounded:
            readiness_transport_valid()
            if stress:
                stress_path(target.path)
            else:
                readiness_path(target.path)
            if (kwargs.get("stream") is not True or kwargs.get("verify") is not True
                    or kwargs.get("allow_redirects") is not False):
                _deny()
        else:
            _validate_path(target.path, kwargs.get("params"))
        headers = kwargs.get("headers", {})
        if not isinstance(headers, dict) or any(not isinstance(key, str) or key.lower() in {
                "x-http-method-override", "x-http-method", "x-method-override"} for key in headers):
            _deny()
        if bounded and any(not isinstance(value, str) or "\r" in value or "\n" in value for value in headers.values()):
            _deny()
        if kwargs.get("allow_redirects") not in {None, False}:
            _deny()
        kwargs["allow_redirects"] = False
        if bounded:
            context[1]["fitness_http_attempts"] += 1
        response = send(method, url, **kwargs)
        if bounded:
            return response  # The bounded reader closes even 401/3xx before handling.
        if 300 <= response.status_code < 400:
            _deny()  # Never forward an authenticated request to a redirect.
        return response

    client._run_request = read_request
    session.request = send_read

    # Reject application-supplied paths before SDK decorators can log their
    # contents or translate a policy failure into a retryable network error.
    @wraps(api_read)
    def checked_api(path, **kwargs):
        _validate_options(path, kwargs)
        return api_read(path, **kwargs)

    @wraps(download)
    def checked_download(path, **kwargs):
        _validate_options(path, kwargs)
        return download(path, **kwargs)

    sdk.connectapi = checked_api
    sdk.download = checked_download

    def read_readiness(cdate):
        _valid_day(cdate)  # Upper guard, before headers or transport.
        path = f"/metrics-service/metrics/trainingreadiness/{cdate}"
        readiness_path(path)
        readiness_transport_valid()
        try:
            headers = client.get_api_headers()  # DI or JWT_WEB; does not mutate tokens.
        except GarminConnectAuthenticationError:
            return ReadinessRead("auth_required")
        headers = {**headers, "Accept": "application/json", "Accept-Encoding": "identity"}

        def fixed_send(auth_headers, timeout, metrics):
            return session.request("GET", f"https://{origin.netloc}{path}", headers=auth_headers,
                timeout=timeout, stream=True, verify=True, allow_redirects=False,
                _readiness_context=(readiness_capability, metrics))

        result = bounded_read(fixed_send, headers)
        if result.status == "redirect":
            _deny()
        return result

    def read_stress(cdate):
        _valid_day(cdate)
        path = f"/wellness-service/wellness/dailyStress/{cdate}"
        stress_path(path)
        readiness_transport_valid()
        try:
            headers = client.get_api_headers()
        except GarminConnectAuthenticationError:
            return ReadinessRead("auth_required")
        headers = {**headers, "Accept": "application/json", "Accept-Encoding": "identity"}

        def fixed_send(auth_headers, timeout, metrics):
            return session.request("GET", f"https://{origin.netloc}{path}", headers=auth_headers,
                timeout=timeout, stream=True, verify=True, allow_redirects=False,
                _stress_context=(stress_capability, metrics))

        result = bounded_read(fixed_send, headers)
        if result.status == "redirect":
            _deny()
        return result

    return read_readiness, read_stress


class ReadOnlyGarmin:
    """Expose only explicitly reviewed reads, with no runtime opt-out flag."""
    __slots__ = ("__readiness", "__sdk", "__stress", "__track_provider")

    def __init__(self, sdk, *, track_provider=False):
        self.__readiness, self.__stress = _install_transport_policy(sdk)
        self.__sdk = sdk
        self.__track_provider = bool(track_provider)


    @property
    def provider(self):
        return "legacy"

    @property
    def account_identity(self):
        value = self.__sdk.display_name
        if not isinstance(value, str) or not value:
            raise ValueError("Garmin account identity is unavailable.")
        return value

    def get_hrv_data(self, cdate):
        _valid_day(cdate)
        value = self.__sdk.get_hrv_data(cdate)
        if self.__track_provider and isinstance(value, dict):
            return {**value, "_slipstream_source": {"provider": "legacy", "adapter_version": 1,
                                                   "operation": "hrv_day"}}
        return value

    def get_training_readiness_bounded(self, cdate):
        """Fixed bounded GET; <=1 fitness send, zero auth/refresh/replay."""
        return self.__readiness(cdate)

    def get_stress_data(self, cdate):
        """Exact bounded stress GET; one fitness send, zero auth/refresh/retry."""
        return self.__stress(cdate)
    def get_activity_page(self, start_date, end_date, *, offset=0, limit=20):
        """Fixed GET route and query, zero SDK/network retries (auth replay <=1).

        Use the reviewed native client directly to avoid the outer SDK's
        retry/logging decorator. Both installed transport guards still apply.
        No URL, method, headers or arbitrary query options enter this method.
        """
        params = page_params(start_date, end_date, offset, limit)
        return self.__sdk.client.connectapi(
            "/activitylist-service/activities/search/activities", params=params
        )

    @property
    def connectapi(self):
        # Internal diagnostic decorators count SDK pagination here; the lower
        # transport policy remains installed even while this hook is replaced.
        return self.__sdk.connectapi

    @connectapi.setter
    def connectapi(self, callback):
        if not callable(callback):
            _deny()
        self.__sdk.connectapi = callback

    def __getattr__(self, name):
        if name not in READ_METHODS:
            raise AttributeError("Garmin read-only client does not expose this operation.")
        method = getattr(self.__sdk, name)

        @wraps(method)
        def read(*args, **kwargs):
            bound = inspect.signature(method).bind(*args, **kwargs)
            for key, value in bound.arguments.items():
                if key in {"cdate", "start", "end", "startdate", "enddate"} and value is not None:
                    _valid_day(value)
                elif key == "activity_id":
                    if isinstance(value, bool) or not re.fullmatch(_ID, str(value)):
                        _deny()
            return method(*args, **kwargs)

        return read
