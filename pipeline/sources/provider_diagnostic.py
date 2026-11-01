"""One explicit identity diagnostic; no SDK replay after a physical failure.

The instance owns its two sessions. The ordinary Garmin login never installs
this policy. Token refresh before the first refusal is still permitted.
"""

from __future__ import annotations

import json
import re
import sys
import time
from importlib.metadata import version
from urllib.parse import urlsplit

import requests

from ..garmin_sdk import CURRENT_SDK_VERSION

CATEGORIES = frozenset(
    {
        "garmin_authentication_required",
        "garmin_access_rejected",
        "garmin_rate_limited",
        "garmin_connection_failed",
        "garmin_response_unverified",
        "garmin_response_limit",
        "garmin_policy_rejected",
    }
)
PHASES = frozenset({"preflight", "auth_refresh", "identity_profile", "identity_settings"})


class ProviderDiagnosticError(RuntimeError):
    """Fixed, bounded evidence; never retain a response, request or provider text."""

    def __init__(self, category, attempts, phase, http_status=None):
        super().__init__("Selected Garmin diagnostic stopped.")
        self.category = category
        self.attempts = attempts
        self.phase = phase
        self.http_status = http_status

    def evidence(self):
        if (
            not isinstance(self.category, str)
            or self.category not in CATEGORIES
            or not isinstance(self.phase, str)
            or self.phase not in PHASES
            or type(self.attempts) is not int
            or not 0 <= self.attempts <= 8
            or self.http_status is not None
            and (type(self.http_status) is not int or not 100 <= self.http_status <= 599)
        ):
            return {"kind": "garmin-provider-check", "status": "unverified"}
        return {
            "kind": "garmin-provider-check",
            "status": "stopped",
            "category": self.category,
            "physical_http_attempts": self.attempts,
            "phase": self.phase,
            "http_status": self.http_status,
        }


class _Abort(BaseException):
    # Bypass SDK catch/refresh/retry loops; translated at this explicit scope's exit.
    pass


class LegacyDiagnostic:
    """Bound the real requests and SDK CFFI auth boundaries, not Garmin.login."""

    def __init__(self, sdk):
        self.sdk = sdk
        self.failure = None
        self.in_scope = False
        self.closed = False
        self.attempts = 0
        self.phase = "preflight"
        self.status = None
        self.started = time.monotonic()
        self.last = None
        self.body_bytes = 0
        self.sessions = (sdk.client._api_session, sdk.client.cs)

    def _fail(self, category):
        if self.failure is None:
            self.failure = ProviderDiagnosticError(category, self.attempts, self.phase, self.status)
        if self.in_scope:
            raise _Abort()
        raise self.failure

    def _check(self):
        if self.closed:
            self._fail("garmin_policy_rejected")
        if self.failure is not None:
            raise _Abort()
        if self.attempts >= 8 or time.monotonic() - self.started >= 120:
            self._fail("garmin_response_limit")

    def _exchange(self, send):
        self._check()
        if self.last is not None:
            time.sleep(max(0, 1.5 - (time.monotonic() - self.last)))
        self._check()
        self.last = time.monotonic()
        self.attempts += 1
        self.status = None
        response = None
        try:
            response = send()
            status = response.status_code
            if type(status) is not int or not 100 <= status <= 599:
                self._fail("garmin_response_unverified")
            self.status = status
            if status != 200:
                self._fail(
                    {
                        401: "garmin_authentication_required",
                        403: "garmin_access_rejected",
                        429: "garmin_rate_limited",
                    }.get(status, "garmin_connection_failed")
                )
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                self._fail("garmin_response_unverified")
            length = response.headers.get("Content-Length")
            if length is not None and (
                not re.fullmatch(r"[0-9]{1,10}", length) or int(length) > 5 * 1024 * 1024
            ):
                self._fail("garmin_response_limit")
            body = bytearray()
            parts = (
                response.iter_content(chunk_size=8192)
                if isinstance(response, requests.Response)
                else response.iter_content()
            )
            for part in parts:
                if not isinstance(part, bytes):
                    self._fail("garmin_response_unverified")
                self.body_bytes += len(part)
                if (
                    self.body_bytes > 5 * 1024 * 1024
                    or time.monotonic() - self.last > 30
                    or time.monotonic() - self.started >= 120
                ):
                    self._fail("garmin_response_limit")
                body.extend(part)
            if length is not None and len(body) != int(length):
                self._fail("garmin_response_unverified")
            try:
                value = json.loads(body)
            except (ValueError, UnicodeError, RecursionError):
                self._fail("garmin_response_unverified")
            if not isinstance(value, dict):
                self._fail("garmin_response_unverified")
            if self.phase == "identity_profile" and not (
                isinstance(value.get("displayName"), str) and value["displayName"].strip()
            ):
                self._fail("garmin_response_unverified")
            if self.phase == "identity_settings" and not isinstance(value.get("userData"), dict):
                self._fail("garmin_response_unverified")
            if self.phase == "auth_refresh" and not (
                isinstance(value.get("access_token"), str) and value["access_token"]
            ):
                self._fail("garmin_response_unverified")
            # SDK only needs JSON/status here. The source response is already
            # bounded; this also supports CFFI without caching unbounded content.
            bounded = requests.Response()
            bounded.status_code = status
            bounded.headers.update(response.headers)
            bounded._content = bytes(body)
            bounded._content_consumed = True
            return bounded
        except _Abort:
            raise
        except Exception:
            self._fail("garmin_connection_failed")
        finally:
            active = sys.exc_info()[0] is not None
            if response is not None:
                try:
                    response.close()
                except Exception:
                    if not active:
                        self._fail("garmin_connection_failed")

    def __enter__(self):
        client = self.sdk.client
        if version("garminconnect") != CURRENT_SDK_VERSION:
            self._fail("garmin_policy_rejected")
        origin = urlsplit(client._connectapi)
        if origin.geturl() not in {"https://connectapi.garmin.com", "https://connectapi.garmin.cn"}:
            self._fail("garmin_policy_rejected")
        for session in self.sessions:
            session.trust_env = False
            original = session.send

            def guarded(request, _send=original, **kwargs):
                self._check()
                target = urlsplit(request.url)
                routes = {
                    "/userprofile-service/socialProfile": "identity_profile",
                    "/userprofile-service/userprofile/user-settings": "identity_settings",
                }
                if (
                    target.scheme != "https"
                    or target.netloc != origin.netloc
                    or target.query
                    or target.fragment
                    or request.method != "GET"
                    or request.body not in (None, b"", "")
                    or target.path not in routes
                ):
                    self._fail("garmin_policy_rejected")
                self.phase = routes[target.path]
                request.headers["Accept-Encoding"] = "identity"
                kwargs.update(stream=True, allow_redirects=False, timeout=(5, 20), verify=True)
                return self._exchange(lambda: _send(request, **kwargs))

            session.send = guarded
        original_post = client._http_post

        def guarded_post(url, **kwargs):
            self._check()
            self.phase = "auth_refresh"
            if url != client._di_token_url or url not in {
                "https://diauth.garmin.com/di-oauth2-service/oauth/token",
                "https://diauth.garmin.cn/di-oauth2-service/oauth/token",
            }:
                self._fail("garmin_policy_rejected")
            data = kwargs.get("data")
            if (
                not isinstance(data, dict)
                or set(data) != {"grant_type", "client_id", "refresh_token"}
                or data.get("grant_type") != "refresh_token"
            ):
                self._fail("garmin_policy_rejected")
            kwargs["headers"] = {**kwargs.get("headers", {}), "Accept-Encoding": "identity"}
            kwargs.update(stream=True, allow_redirects=False, timeout=(5, 20), verify=True)
            return self._exchange(lambda: original_post(url, **kwargs))

        client._http_post = guarded_post
        self.in_scope = True
        return self

    def __exit__(self, kind, error, traceback):
        self.in_scope = False
        self.closed = True
        cleanup_error = False
        for session in self.sessions:
            try:
                session.close()
            except Exception:
                cleanup_error = True
        if self.failure is not None:
            raise self.failure from None
        if kind is None and cleanup_error:
            raise ProviderDiagnosticError(
                "garmin_connection_failed", self.attempts, self.phase, self.status
            )
        return False
