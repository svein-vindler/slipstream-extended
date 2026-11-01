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
        self.reads = {}
        self.identity = None
        self.failed = False
        self.seen_identity = set()

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

    def _install(self, session):
        original = session.send
        session.trust_env = False

        def bounded(request, **kwargs):
            self.checkpoint()
            target = urlsplit(request.url)
            day = self.intent["date"]
            personal = self.chosen.provider == "personal"
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
                if auth and target.query:
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
                self.clock.sleep(max(0, 1.5 - (self.clock.monotonic() - self.last)))
            self.checkpoint()
            if self.clock.monotonic() - self.started + 30 >= self.intent.get("max_seconds", 899):
                raise Stop("date_deadline")
            self.last = self.clock.monotonic()
            self.http_count += 1
            write(
                self.folder / f"http-{self.http_count:02d}.json",
                {
                    "attempt": self.http_count,
                    "kind": self.current or "identity_auth",
                    "at_monotonic": self.last,
                },
            )
            kwargs.update(stream=True, allow_redirects=False, timeout=(5, 20), verify=True)
            # Session.send has no allow_redirects parameter handling of its own;
            # false prevents its redirect recursion and the next boundary is guarded.
            request.headers["Accept-Encoding"] = "identity"
            response = original(request, **kwargs)
            try:
                status = response.status_code
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
                for part in response.iter_content(8192):
                    self.body_bytes += len(part)
                    if self.body_bytes > 5 * 1024 * 1024 or self.clock.monotonic() - self.last > 30:
                        raise Stop("body_bound")
                    body.extend(part)
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
                if not personal and target.path.endswith("socialProfile"):
                    identity = json.loads(body)
                    if (
                        identity.get("displayName") != self.chosen.account
                        or type(identity.get("profileId")) is not int
                    ):
                        raise Stop("account_binding")
                    self.identity = identity["profileId"]
                if not personal and target.path.endswith("user-settings"):
                    settings = json.loads(body)
                    if not isinstance(settings, dict) or not isinstance(
                        settings.get("userData"), dict
                    ):
                        raise Stop("settings_unverified")
                response._content = bytes(body)
                response._content_consumed = True
                return response
            finally:
                response.close()

        def guarded(request, **kwargs):
            if self.failed:
                raise Stop("source_already_stopped")
            try:
                return bounded(request, **kwargs)
            except ReloadAck:
                raise
            except BaseException:
                self.failed = True
                raise

        session.send = guarded
        self.sessions.append(session)

    def _ensure(self):
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
            self.client = _history_login(self.chosen, self._install)

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
            pass
        else:
            raise Stop("reload_ack_unknown")
        finally:
            self.reload_open = False
            self.current = None
        self.reload_at = self.clock.monotonic()

    def wait_archive(self):
        while self.clock.monotonic() - self.reload_at < 120:
            self.checkpoint()
            self.clock.sleep(min(1, 120 - (self.clock.monotonic() - self.reload_at)))

    def close(self):
        for session in self.sessions:
            session.close()
