"""Separate opt-in dated archive port. The general read-only facade is unchanged."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from importlib.metadata import version
from urllib.parse import parse_qs, urlsplit

from .history_canonical import prepare
from .history_state import Stop, digest, write
from .sources.provider_config import selection


class Clock:
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)

    @staticmethod
    def now():
        return datetime.now(timezone.utc)


class ReloadAck(BaseException):
    pass


class HistorySource:
    def __init__(self, plan, intent, folder, *, chosen=None, clock=None):
        self.plan, self.intent, self.folder = plan, intent, folder
        self.clock = clock or Clock()
        self.chosen = chosen or selection(os.environ)
        if (
            self.chosen.provider != plan["provider"]
            or not self.chosen.account
            or digest(self.chosen.account.encode()) != plan["account_sha256"]
        ):
            raise Stop("account_binding")
        if intent["archive_reload"] and self.chosen.provider != "legacy":
            raise Stop("unsupported")
        self.source_wait_seconds = 0.0
        self.network_seconds = 0.0
        self.http_count = 0
        self.body_bytes = 0
        self.last = None
        self.started = self.clock.monotonic()
        self.client = None
        self.sessions = []
        self.current = None
        self.captured = {}
        self.reload_open = False
        self.reload_used = False
        self.reload_ack = False
        self.reads = {}
        self.identity = None
        self.failed = False
        self.failure = None
        self.seen_identity = set()

    def _sleep(self, seconds):
        started = self.clock.monotonic()
        try:
            self.clock.sleep(seconds)
        finally:
            self.source_wait_seconds += self.clock.monotonic() - started

    def timings(self):
        return {
            "source_wait_seconds": self.source_wait_seconds,
            "network_seconds": self.network_seconds,
            "local_compute_seconds": max(
                0.0,
                self.clock.monotonic()
                - self.started
                - self.source_wait_seconds
                - self.network_seconds,
            ),
            "r2_seconds": None,
            "control_seconds": None,
        }

    def checkpoint(self):
        if (
            self.folder.parent / "CANCEL"
        ).exists() or self.clock.monotonic() - self.started >= self.intent.get("max_seconds", 899):
            raise Stop("cancelled_or_deadline")

    def _session(self):
        import requests
        from requests.adapters import HTTPAdapter

        session = requests.Session()
        session.trust_env = False
        session.mount("https://", HTTPAdapter(max_retries=0))
        self._install(session)
        return session

    def _install_auth(self, client):
        # SDK0.3.20 uses a separate CFFI (or requests) POST, outside its sessions.
        # Route it through the same physical guard before the SDK can see it.
        import requests

        original = client._http_post
        session = requests.Session()

        def physical(request, **kwargs):
            fields = parse_qs(request.body, strict_parsing=True, max_num_fields=3)
            return original(
                request.url,
                data={key: values[0] for key, values in fields.items()},
                headers=dict(request.headers),
                **kwargs,
            )

        session.send = physical
        self._install(session)

        def guarded_post(url, **kwargs):
            if self.failed:
                raise self.failure
            data = kwargs.get("data")
            if (
                set(kwargs) - {"data", "headers", "timeout"}
                or not isinstance(data, dict)
                or set(data) != {"grant_type", "client_id", "refresh_token"}
                or data.get("grant_type") != "refresh_token"
                or any(
                    not isinstance(value, str) or not 1 <= len(value) <= 8192
                    for value in data.values()
                )
            ):
                self.failed = True
                self.failure = Stop("request_rejected")
                raise self.failure
            try:
                request = requests.Request(
                    "POST", url, data=data, headers=kwargs.get("headers")
                ).prepare()
            except Exception:
                self.failed = True
                self.failure = Stop("request_rejected")
                raise self.failure from None
            return session.send(request)

        client._http_post = guarded_post

    def _install(self, session):
        original = session.send
        session.trust_env = False

        def bounded(request, **kwargs):
            self.checkpoint()
            target = urlsplit(request.url)
            day = self.intent["date"]
            personal = self.chosen.provider == "personal"
            auth = False
            if (
                target.scheme != "https"
                or target.fragment
                or any(
                    str(k).lower()
                    in {"x-http-method-override", "x-http-method", "x-method-override"}
                    for k in request.headers
                )
            ):
                raise Stop("request_rejected")
            if personal:
                from .sources.personal_policy import HOST, TOKEN_PATH

                allowed = {
                    "/userprofile-service/userprofile/settings": "GET",
                    TOKEN_PATH: "POST",
                    "/wellness-service/wellness/dailyHeartRate": "GET",
                    "/hrv-service/hrv/" + day: "GET",
                }
                if (
                    target.netloc != urlsplit(HOST).netloc
                    or allowed.get(target.path) != request.method
                ):
                    raise Stop("request_rejected")
                expected = {"date": [day]} if "dailyHeartRate" in target.path else {}
                if parse_qs(target.query) != expected:
                    raise Stop("wrong_date_request")
                fitness = "dailyHeartRate" in target.path or target.path.startswith(
                    "/hrv-service/hrv/"
                )
                is_reload = False
            else:
                auth = (
                    target.netloc == "diauth.garmin.com"
                    and target.path == "/di-oauth2-service/oauth/token"
                    and request.method == "POST"
                )
                identity = target.path in {
                    "/userprofile-service/socialProfile",
                    "/userprofile-service/userprofile/user-settings",
                }
                hr = (
                    target.path
                    == "/wellness-service/wellness/dailyHeartRate/" + self.chosen.account
                )
                hrv = target.path == "/hrv-service/hrv/" + day
                is_reload = target.path == "/wellness-service/wellness/epoch/request/" + day
                fitness = hr or hrv
                if auth:
                    if (
                        target.query
                        or not isinstance(request.body, str)
                        or len(request.body) > 32768
                    ):
                        raise Stop("request_rejected")
                    fields = parse_qs(request.body, strict_parsing=True, max_num_fields=3)
                    if (
                        set(fields) != {"grant_type", "client_id", "refresh_token"}
                        or fields["grant_type"] != ["refresh_token"]
                        or any(len(values) != 1 or not values[0] for values in fields.values())
                    ):
                        raise Stop("request_rejected")
                if not auth:
                    if target.netloc != "connectapi.garmin.com" or not (
                        identity or hr or hrv or is_reload
                    ):
                        raise Stop("request_rejected")
                    if request.method != ("POST" if is_reload else "GET") or request.body not in (
                        None,
                        b"",
                        "",
                    ):
                        raise Stop("request_rejected")
                    if parse_qs(target.query) != ({"date": [day]} if hr else {}):
                        raise Stop("wrong_date_request")
                if is_reload and not self.reload_open:
                    raise Stop("reload_not_admitted")
            if not fitness and not is_reload:
                identity_slot = target.netloc + target.path
                if identity_slot in self.seen_identity:
                    raise Stop("duplicate_identity_auth")
                self.seen_identity.add(identity_slot)
            if fitness and self.current not in {"hr", "hrv"}:
                raise Stop("source_phase")
            if self.http_count >= self.intent["max_http"]:
                raise Stop("http_budget")
            if self.last is not None:
                self._sleep(max(0, 1.5 - (self.clock.monotonic() - self.last)))
            self.checkpoint()
            if self.clock.monotonic() - self.started + 30 >= self.intent.get("max_seconds", 899):
                raise Stop("date_deadline")
            self.last = self.clock.monotonic()
            self.http_count += 1
            write(
                self.folder / f"http-{self.http_count:02d}.json",
                {
                    "attempt": self.http_count,
                    "kind": self.current if fitness or is_reload else "identity_auth",
                    "at_monotonic": self.last,
                },
            )
            kwargs.update(stream=True, allow_redirects=False, timeout=(5, 20), verify=True)
            # Session.send has no allow_redirects parameter handling of its own;
            # false prevents its redirect recursion and the next boundary is guarded.
            request.headers["Accept-Encoding"] = "identity"
            network_started = self.clock.monotonic()
            try:
                response = original(request, **kwargs)
            except Exception as error:
                # Fixed event/class only: never persist provider text, URL or auth material.
                write(
                    self.folder / f"http-result-{self.http_count:02d}.json",
                    {
                        "attempt": self.http_count,
                        "http_status": None,
                        "exception_class": type(error).__name__[:64],
                    },
                )
                raise Stop("transport_failed") from None
            finally:
                self.network_seconds += self.clock.monotonic() - network_started
            primary = False
            try:
                status = response.status_code
                if type(status) is not int or not 100 <= status <= 599:
                    raise Stop("response_unverified")
                write(
                    self.folder / f"http-result-{self.http_count:02d}.json",
                    {"attempt": self.http_count, "http_status": status},
                )
                if status == 429:
                    delay = 86400
                    retry = response.headers.get("Retry-After")
                    try:
                        delay = (
                            max(delay, int(retry))
                            if retry and retry.isdigit()
                            else max(
                                delay,
                                (parsedate_to_datetime(retry) - self.clock.now()).total_seconds(),
                            )
                            if retry
                            else delay
                        )
                    except (TypeError, ValueError, OverflowError):
                        pass
                    raise Stop(
                        "rate_limited", (self.clock.now() + timedelta(seconds=delay)).isoformat()
                    )
                if status not in (
                    {200, 202, 204} if is_reload else {200, 204} if fitness else {200}
                ):
                    raise Stop("denied" if status in (401, 403) else "failed")
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise Stop("body_encoding")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdigit() or int(length) > 5 * 1024 * 1024):
                    raise Stop("body_bound")
                body = bytearray()
                import requests

                parts = (
                    response.iter_content(8192)
                    if isinstance(response, requests.Response)
                    else response.iter_content()
                )
                body_started = self.clock.monotonic()
                try:
                    for part in parts:
                        if not isinstance(part, bytes):
                            raise Stop("response_unverified")
                        self.body_bytes += len(part)
                        self.checkpoint()
                        if (
                            self.body_bytes > 5 * 1024 * 1024
                            or self.clock.monotonic() - self.last > 30
                        ):
                            raise Stop("body_bound")
                        body.extend(part)
                except Exception as error:
                    write(
                        self.folder / f"http-error-{self.http_count:02d}.json",
                        {
                            "attempt": self.http_count,
                            "http_status": status,
                            "exception_class": type(error).__name__[:64],
                        },
                    )
                    raise Stop("transport_failed") from None
                finally:
                    self.network_seconds += self.clock.monotonic() - body_started
                if length is not None and len(body) != int(length):
                    raise Stop("body_length")
                if status == 204 and body:
                    raise Stop("unexpected_no_content_body")
                if is_reload:
                    raise ReloadAck()
                if (
                    fitness
                    and status == 200
                    and response.headers.get("Content-Type", "").split(";")[0].strip().lower()
                    != "application/json"
                ):
                    raise Stop("body_type")
                if fitness:
                    name = f"{self.current}-{self.http_count:02d}.wire"
                    write(self.folder / name, bytes(body))
                    self.captured[self.current] = (name, bytes(body), status)
                if not personal and hr and status == 200:
                    # A whole-body null/array cannot declare a bound source day.
                    # Keep the bounded raw wire, but stop before SDK handling/replay.
                    try:
                        value = json.loads(body)
                    except (ValueError, UnicodeError, RecursionError):
                        raise Stop("source_shape") from None
                    if not isinstance(value, dict):
                        raise Stop("source_shape")
                if not personal and target.path.endswith("socialProfile"):
                    try:
                        identity = json.loads(body)
                    except (ValueError, UnicodeError, RecursionError):
                        raise Stop("account_binding") from None
                    if (
                        not isinstance(identity, dict)
                        or identity.get("displayName") != self.chosen.account
                        or type(identity.get("profileId")) is not int
                    ):
                        raise Stop("account_binding")
                    self.identity = identity["profileId"]
                if not personal and target.path.endswith("user-settings"):
                    try:
                        settings = json.loads(body)
                    except (ValueError, UnicodeError, RecursionError):
                        raise Stop("settings_unverified") from None
                    if not isinstance(settings, dict) or not isinstance(
                        settings.get("userData"), dict
                    ):
                        raise Stop("settings_unverified")
                if auth:
                    try:
                        value = json.loads(body)
                    except (ValueError, UnicodeError, RecursionError):
                        raise Stop("response_unverified") from None
                    if (
                        not isinstance(value, dict)
                        or not isinstance(value.get("access_token"), str)
                        or not value["access_token"]
                    ):
                        raise Stop("response_unverified")
                # Return only the bounded body, including for streamed CFFI responses.
                bounded_response = requests.Response()
                bounded_response.status_code = status
                bounded_response.headers.update(response.headers)
                bounded_response._content = bytes(body)
                bounded_response._content_consumed = True
                return bounded_response
            except BaseException:
                primary = True
                raise
            finally:
                try:
                    response.close()
                except Exception:
                    if not primary:
                        raise Stop("transport_failed") from None

        def guarded(request, **kwargs):
            if self.failed:
                raise self.failure
            try:
                return bounded(request, **kwargs)
            except ReloadAck:
                raise
            except BaseException as error:
                self.failed = True
                self.failure = error
                raise

        session.send = guarded
        self.sessions.append(session)

    def _ensure(self):
        if self.failed:
            raise self.failure
        if self.client is not None:
            return
        if self.chosen.provider == "personal":
            from .sources.garmin_personal import PersonalGarmin

            self.client = PersonalGarmin(
                self.chosen.credential(),
                expected_account=self.chosen.account,
                session=self._session(),
            )
        else:
            from .sources.garmin import _history_login

            if version("garminconnect") != "0.3.20":
                raise Stop("sdk_version")
            self.client = _history_login(self.chosen, self._install, self._install_auth)

    def read(self, stream):
        if (
            stream not in self.intent.get("streams", ["hr", "hrv"])
            or self.reads.get(stream, 0) >= 2
        ):
            raise Stop("source_slot")
        self._ensure()
        self.current = stream
        self.reads[stream] = self.reads.get(stream, 0) + 1
        value = (
            self.client.get_heart_rates(self.intent["date"])
            if stream == "hr"
            else self.client.get_hrv_data(self.intent["date"])
        )
        name, body, status = self.captured[stream]
        if status == 204:
            value = None
        if value is not None and self.chosen.provider == "legacy":
            profiles = [value[k] for k in ("userProfilePK", "userProfilePk") if k in value]
            if not profiles or any(p != self.identity or type(p) is not int for p in profiles):
                raise Stop("profile_binding")
        prepared = (
            prepare(stream, self.intent["date"], value, wire=body) if value is not None else None
        )
        view = name + ".view.json"
        view_sha = write(self.folder / view, value)
        self.current = None
        return {
            "date": self.intent["date"],
            "stream": stream,
            "status": "ready" if prepared is not None else "empty_unresolved",
            "wire": name,
            "wire_sha256": digest(body),
            "view": view,
            "view_sha256": view_sha,
        }

    def reload(self):
        if (
            not self.intent["archive_reload"]
            or self.chosen.provider != "legacy"
            or self.reload_used
        ):
            raise Stop("reload_not_admitted")
        self.reload_used = self.reload_open = True
        self.current = "reload"
        try:
            self.client.request_reload(self.intent["date"])
        except ReloadAck:
            self.reload_ack = True
        else:
            raise Stop("reload_ack_unknown")
        finally:
            self.reload_open = False
            self.current = None
        self.reload_at = self.clock.monotonic()

    def wait_archive(self):
        while self.clock.monotonic() - self.reload_at < 120:
            self.checkpoint()
            self._sleep(min(1, 120 - (self.clock.monotonic() - self.reload_at)))

    def close(self):
        first = None
        for session in self.sessions:
            try:
                session.close()
            except BaseException as error:
                if first is None:
                    first = error
        if first is not None:
            raise first
