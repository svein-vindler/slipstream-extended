"""Bounded, GET-only Garmin Personal collector. No SDK cache or arbitrary URLs."""

from __future__ import annotations

import json
import re
import time
from datetime import UTC, date

import requests

from .personal_policy import HOST, TOKEN_PATH, validate_request

MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_HTTP_ATTEMPTS = 128


class PersonalSourceError(RuntimeError):
    def __init__(self, status="source_error", http_status=None):
        super().__init__("Garmin Personal source: " + status)
        self.status = status
        self.status_code = http_status


class PersonalUnsupported(ValueError):
    status = "unsupported"

    def __init__(self):
        super().__init__("This feature is not defined by the Garmin Personal contract.")


def calendar_day(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Use a valid Garmin calendar date.")
    try:
        date.fromisoformat(value)
    except ValueError:
        raise ValueError("Use a valid Garmin calendar date.") from None
    return value


def _original_xml_root(content):
    """Validate XML syntax/root without retaining a tree or allowing any DTD."""
    from xml.etree import ElementTree

    class RootOnly:
        def __init__(self):
            self.root = None
            self.depth = 0
            self.nodes = 0

        def start(self, tag, attributes):
            self.depth += 1
            self.nodes += 1
            if self.depth > 128 or self.nodes > 100000 or len(attributes) > 256:
                raise ValueError("Original XML exceeds its resource bounds")
            if self.root is None:
                self.root = tag.rsplit("}", 1)[-1]

        def end(self, tag):
            self.depth -= 1

        def data(self, text):
            pass

        def doctype(self, name, public_id, system_id):
            # Parser-level rejection also covers UTF-16 and split declarations.
            raise ValueError("Original XML DTD is unsupported")

        def close(self):
            return self.root

    parser = ElementTree.XMLParser(target=RootOnly())
    for offset in range(0, len(content), 8192):
        parser.feed(content[offset : offset + 8192])
    return parser.close()


class PersonalGarmin:
    """One job, one account, one token, bounded HTTP, no provider fallback."""

    def __init__(self, key, *, expected_account, session=None):
        from .provider_config import validate_account, validate_personal_key

        validate_personal_key(key)
        validate_account(expected_account)
        self._key = key
        self._account = expected_account
        self._token = None
        self._ready = False
        self._daily_cache = {}
        self._auth_attempted = False
        self._failure = None
        self._http = session
        self._last_original_validation = None
        self.metrics = {
            "fitness_http_attempts": 0,
            "auth_http_attempts": 0,
            "identity_http_attempts": 0,
            "body_bytes": 0,
        }

    @property
    def account_identity(self):
        return self._account if self._ready else None

    def _fail(self, status="source_error", http_status=None):
        self._failure = (status, http_status)
        raise PersonalSourceError(status, http_status) from None

    def _send(self, path, *, params=None, token=False, binary=False):
        if self._failure is not None:
            self._fail(*self._failure)
        if (
            sum(
                self.metrics[key]
                for key in ("fitness_http_attempts", "auth_http_attempts", "identity_http_attempts")
            )
            >= MAX_HTTP_ATTEMPTS
        ):
            self._fail("budget_exhausted")
        if self._http is None:
            self._http = requests.Session()
            self._http.trust_env = False
        try:
            method = validate_request(path, params, token=token, binary=binary)
        except ValueError:
            self._fail("request_rejected")
        if not isinstance(self._key if token else self._token, str):
            self._fail("auth_required")
        header = "ApiKey " + self._key if token else "Bearer " + self._token
        headers = {
            "Authorization": header,
            "Accept-Encoding": "identity",
            "Accept": "text/plain"
            if token
            else "application/x-zip-compressed, application/zip, application/octet-stream"
            if binary
            else "application/json",
        }
        counter = (
            "auth_http_attempts"
            if token
            else "identity_http_attempts"
            if path == "/userprofile-service/userprofile/settings"
            else "fitness_http_attempts"
        )
        self.metrics[counter] += 1
        response = None
        started = time.monotonic()
        limit = 16384 if token else MAX_FILE_BYTES if binary else MAX_JSON_BYTES
        try:
            response = self._http.request(
                method,
                HOST + path,
                headers=headers,
                params=params,
                allow_redirects=False,
                stream=True,
                timeout=(5, 20),
            )
            status = response.status_code
            if status == 204 and not token and not binary:
                return None
            if status != 200:
                category = (
                    "auth_required"
                    if status in {401, 403}
                    else "rate_limited"
                    if status == 429
                    else "redirect_rejected"
                    if 300 <= status < 400
                    else "source_error"
                )
                self._fail(category, status)
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                self._fail("schema_unverified")
            length = response.headers.get("Content-Length")
            if length is not None and (not re.fullmatch(r"[0-9]+", length) or int(length) > limit):
                self._fail("payload_limit")
            body = bytearray()
            for part in response.iter_content(chunk_size=8192):
                self.metrics["body_bytes"] += len(part)
                if len(body) + len(part) > limit or time.monotonic() - started > 30:
                    self._fail("payload_limit")
                body.extend(part)
            if length is not None and len(body) != int(length):
                self._fail("schema_unverified")
            media = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
            allowed = (
                {
                    "application/x-zip-compressed",
                    "application/zip",
                    "application/octet-stream",
                }
                if binary
                else {"text/plain", "application/octet-stream"}
                if token
                else {"application/json"}
            )
            if media not in allowed:
                self._fail("schema_unverified")
            return bytes(body)
        except PersonalSourceError:
            raise
        except Exception:
            self._failure = ("source_error", None)
            raise PersonalSourceError() from None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    if self._failure is None:
                        self._fail("source_error")

    def _json(self, path, params=None):
        import math

        body = self._send(path, params=params)
        if body is None:
            return None
        try:

            def invalid_constant(_):
                raise ValueError("Non-finite JSON")

            def finite_float(value):
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("Non-finite JSON")
                return number

            def finite_integer(value):
                number = int(value)
                try:
                    finite = math.isfinite(number)
                except OverflowError:
                    finite = False
                if not finite:
                    raise ValueError("JSON number exceeds finite source range")
                return number

            return json.loads(
                body.decode("utf-8"),
                parse_constant=invalid_constant,
                parse_float=finite_float,
                parse_int=finite_integer,
            )
        except (UnicodeError, ValueError, RecursionError):
            self._fail("schema_unverified")

    def _authenticate_identity(self):
        if self._failure is not None:
            self._fail(*self._failure)
        if self._auth_attempted:
            self._fail("auth_required")
        self._auth_attempted = True
        raw = self._send(TOKEN_PATH, token=True)
        try:
            token = raw.decode("ascii").strip()
        except UnicodeError:
            self._fail("auth_required")
        if not re.fullmatch(r"[A-Za-z0-9_.~-]{1,16384}", token):
            self._fail("auth_required")
        self._token = token
        self._key = None
        settings = self._json("/userprofile-service/userprofile/settings")
        account = settings.get("displayName") if isinstance(settings, dict) else None
        if not isinstance(account, str) or not account:
            self._fail("schema_unverified")
        from .provider_config import validate_account

        try:
            validate_account(account)
        except ValueError:
            self._fail("schema_unverified")
        return account

    def _ensure(self):
        if self._ready:
            return
        account = self._authenticate_identity()
        if account != self._account:
            self._fail("account_mismatch")
        self._ready = True

    @classmethod
    def identify_account(cls, key, *, session=None):
        """Explicit new-install setup: return identity only, no fitness data/client."""
        reader = cls(key, expected_account="identity-only", session=session)
        return reader._authenticate_identity()

    def get_hrv_data(self, day):
        calendar_day(day)
        self._ensure()
        result = self._json("/hrv-service/hrv/" + day)
        if result is None:
            return None
        if not isinstance(result, dict) or not isinstance(result.get("hrvSummary"), dict):
            self._fail("schema_unverified")
        for declared in (
            result.get("calendarDate"),
            result["hrvSummary"].get("calendarDate"),
        ):
            if declared is not None and declared != day:
                self._fail("wrong_date")
        return {
            **result,
            "_slipstream_source": {
                "provider": "personal",
                "adapter_version": 1,
                "operation": "hrv_day",
            },
        }

    def _range(self, start, end, maximum):
        calendar_day(start)
        calendar_day(end)
        count = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        if not 1 <= count <= maximum:
            raise ValueError("Garmin date range exceeds the supported request limit.")
        return {"startDate": start, "endDate": end}

    def _rows(self, result, start, end, *, maximum=None):
        if result is None:
            return []
        limit = (
            maximum
            if maximum is not None
            else (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        )
        if not isinstance(result, list) or len(result) > limit:
            self._fail("schema_unverified")
        for item in result:
            if not isinstance(item, dict):
                self._fail("schema_unverified")
            day = item.get("calendarDate", item.get("date"))
            if not isinstance(day, str) or not start <= day <= end:
                self._fail("wrong_date")
            try:
                calendar_day(day)
            except ValueError:
                self._fail("schema_unverified")
        return result

    def get_sleep_daily(self, start, end):
        params = self._range(start, end, 31)
        self._ensure()
        return self._rows(
            self._json("/sleep-service/sleep/dailySleepSummariesByDate", params),
            start,
            end,
        )

    def get_daily_steps(self, start, end):
        self._range(start, end, 28)
        self._ensure()
        return self._rows(
            self._json("/usersummary-service/stats/steps/daily/" + start + "/" + end),
            start,
            end,
        )

    def get_body_battery(self, start, end):
        params = {**self._range(start, end, 31), "chunks": 288}
        self._ensure()
        rows = self._rows(
            self._json("/wellness-service/wellness/bodyBattery/reports/daily", params),
            start,
            end,
        )
        result = []
        for raw in rows:
            descriptors = raw.get("bodyBatteryValueDescriptorDTOList")
            indices = {"timestamp": 0, "bodyBatteryLevel": 1}
            width = 2
            if descriptors is not None:
                if not isinstance(descriptors, list) or not 2 <= len(descriptors) <= 4:
                    self._fail("schema_unverified")
                indices = {}
                for descriptor in descriptors:
                    if (
                        not isinstance(descriptor, dict)
                        or type(descriptor.get("bodyBatteryValueDescriptorIndex")) is not int
                        or descriptor.get("bodyBatteryValueDescriptorKey")
                        not in {
                            "timestamp",
                            "bodyBatteryLevel",
                            "bodyBatteryStatus",
                            "bodyBatteryVersion",
                        }
                    ):
                        self._fail("schema_unverified")
                    key = descriptor["bodyBatteryValueDescriptorKey"]
                    if key in indices:
                        self._fail("schema_unverified")
                    indices[key] = descriptor["bodyBatteryValueDescriptorIndex"]
                if not {"timestamp", "bodyBatteryLevel"} <= indices.keys() or set(
                    indices.values()
                ) != set(range(len(indices))):
                    self._fail("schema_unverified")
                width = len(indices)
            values = raw.get("bodyBatteryValuesArray")
            if not isinstance(values, list) or len(values) > 10000:
                self._fail("schema_unverified")
            normalized = []
            for point in values:
                if not isinstance(point, list) or len(point) != width:
                    self._fail("schema_unverified")
                normalized.append([point[indices["timestamp"]], point[indices["bodyBatteryLevel"]]])
            result.append(
                {
                    **raw,
                    "bodyBatteryValuesArray": normalized,
                    "bodyBatteryValueDescriptorDTOList": [
                        {
                            "bodyBatteryValueDescriptorIndex": 0,
                            "bodyBatteryValueDescriptorKey": "timestamp",
                        },
                        {
                            "bodyBatteryValueDescriptorIndex": 1,
                            "bodyBatteryValueDescriptorKey": "bodyBatteryLevel",
                        },
                    ],
                    "_slipstream_source": {
                        "provider": "personal",
                        "adapter_version": 1,
                        "sampling": "interval_extrema",
                        "chunks": 288,
                        "value_descriptors": descriptors,
                    },
                }
            )
        return result

    def get_hrv_data_range(self, start, end):
        self._range(start, end, 31)
        self._ensure()
        value = self._json("/hrv-service/hrv/daily/" + start + "/" + end)
        if value is None:
            return {"hrvSummaries": []}
        if not isinstance(value, dict):
            self._fail("schema_unverified")
        self._rows(value.get("hrvSummaries"), start, end)
        return value

    def get_stats(self, day):
        from copy import deepcopy

        calendar_day(day)
        if day in self._daily_cache:
            return deepcopy(self._daily_cache[day])
        self._ensure()
        value = self._json("/usersummary-service/usersummary/daily", {"calendarDate": day})
        if not isinstance(value, dict) or value.get("calendarDate") != day:
            self._fail("wrong_date" if isinstance(value, dict) else "schema_unverified")
        self._daily_cache[day] = value
        return deepcopy(value)

    def get_activity_page(self, start, end, *, offset=0, limit=20):
        from .activity_page import page_params, validate_page

        params = page_params(start, end, offset, limit)
        self._ensure()
        result = self._json("/activitylist-service/activities/search/activities", params)
        try:
            return validate_page(result, start, end, limit)
        except ValueError:
            self._fail("schema_unverified")

    @staticmethod
    def _activity_id(value):
        if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(value)):
            raise ValueError("Use a valid Garmin activity identifier.")
        return str(value)

    def get_activity(self, activity_id):
        activity_id = self._activity_id(activity_id)
        self._ensure()
        wire = self._json("/activity-service/activity/" + activity_id)
        if not isinstance(wire, dict) or str(wire.get("activityId")) != activity_id:
            self._fail("schema_unverified")
        result = dict(wire)
        summary = wire.get("summaryDTO")
        if not isinstance(summary, dict):
            self._fail("schema_unverified")
        result.setdefault("activityType", wire.get("activityTypeDTO"))
        for name, value in summary.items():
            result.setdefault(name, value)
        return result

    def download_activity(self, activity_id, dl_fmt=None):
        import hashlib
        import io
        import stat
        import zipfile

        from garmin_fit_sdk import Decoder, Stream

        activity_id = self._activity_id(activity_id)
        name = getattr(dl_fmt, "name", dl_fmt)
        if name not in {None, "ORIGINAL", "original"}:
            raise PersonalUnsupported()
        self._last_original_validation = None
        self._ensure()
        body = self._send("/download-service/files/activity/" + activity_id, binary=True)
        members = []
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                entries = archive.infolist()
                if (
                    not 1 <= len(entries) <= 16
                    or sum(item.file_size for item in entries) > MAX_FILE_BYTES
                ):
                    self._fail("payload_limit")
                found = False
                for item in entries:
                    if (
                        item.flag_bits & 1
                        or item.is_dir()
                        or any(part in item.filename for part in ["/", "\\", ":"])
                        or item.filename.startswith(".")
                        or stat.S_IFMT(item.external_attr >> 16) == stat.S_IFLNK
                    ):
                        self._fail("schema_unverified")
                    with archive.open(item) as member:
                        content = member.read(MAX_FILE_BYTES + 1)
                    if len(content) != item.file_size or len(content) > MAX_FILE_BYTES:
                        self._fail("payload_limit")
                    extension = item.filename.rsplit(".", 1)[-1].lower()
                    integrity = "zip_crc_verified_only"
                    if extension == "fit":
                        if (
                            len(content) < 14
                            or content[0] not in {12, 14}
                            or content[8:12] != b".FIT"
                            or int.from_bytes(content[4:8], "little") + content[0] + 2
                            != len(content)
                        ):
                            self._fail("schema_unverified")
                        decoder = Decoder(Stream.from_byte_array(bytearray(content)))
                        if not decoder.check_integrity():
                            self._fail("schema_unverified")
                        integrity = "fit_crc_verified"
                        found = True
                    elif extension in {"gpx", "tcx"}:
                        expected = "gpx" if extension == "gpx" else "TrainingCenterDatabase"
                        if _original_xml_root(content) != expected:
                            self._fail("schema_unverified")
                        integrity = "xml_root_verified"
                        found = True
                    members.append(
                        {
                            "format": extension
                            if extension in {"fit", "tcx", "gpx"}
                            else "unverified",
                            "sha256": hashlib.sha256(content).hexdigest(),
                            "integrity": integrity,
                        }
                    )
                if not found:
                    self._fail("unsupported_original_format")
        except PersonalSourceError:
            raise
        except Exception:
            self._fail("schema_unverified")
        # The download route binds the request; FIT has no universal Garmin ID.
        # These bytes must not be promoted to associated canonical activity data.
        self._last_original_validation = {
            "provider": "personal",
            "requested_activity_id": activity_id,
            "archive_sha256": hashlib.sha256(body).hexdigest(),
            "members": members,
            "association": "request_route_only",
            "activity_association_verified": False,
        }
        return body

    @property
    def original_validation(self):
        from copy import deepcopy

        return deepcopy(self._last_original_validation)

    def get_sleep_data(self, day):
        calendar_day(day)
        # The existing detailed-sleep contract requires stage intervals.
        raise PersonalUnsupported()

    def get_activity_exercise_sets(self, activity_id):
        self._activity_id(activity_id)
        raise PersonalUnsupported()

    def get_daily_weigh_ins(self, day):
        calendar_day(day)
        raise PersonalUnsupported()

    provider = "personal"

    def capabilities(self):
        from .provider_capabilities import capabilities

        return capabilities(self.provider)

    def get_respiration_data(self, day):
        return self.get_stats(day)

    def get_weigh_ins(self, start, end):
        import math

        self._range(start, end, 31)
        self._ensure()
        rows = self._rows(
            self._json("/weight-service/weight/all/" + start + "/" + end), start, end, maximum=5000
        )
        for row in rows:
            value = row.get("weight")
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                self._fail("schema_unverified")
        return {
            "dateWeightList": rows,
            "_slipstream_source": {"provider": "personal", "weight_unit": "kg"},
        }

    def get_heart_rates(self, day):
        calendar_day(day)
        self._ensure()
        raw = self._json("/wellness-service/wellness/dailyHeartRate", {"date": day})
        wire_day = raw.get("calendarDate") if isinstance(raw, dict) else None
        local_day = wire_day.get("localDate") if isinstance(wire_day, dict) else wire_day
        if not isinstance(raw, dict) or local_day != day:
            self._fail("wrong_date" if isinstance(raw, dict) else "schema_unverified")
        descriptors = raw.get("heartRateValueDescriptors")
        indices = {"timestamp": 0, "heartrate": 1}
        if descriptors is not None:
            if not isinstance(descriptors, list):
                self._fail("schema_unverified")
            indices = {}
            for descriptor in descriptors:
                if (
                    not isinstance(descriptor, dict)
                    or type(descriptor.get("index")) is not int
                    or descriptor["index"] < 0
                    or descriptor.get("key") not in {"timestamp", "heartrate"}
                    or descriptor["key"] in indices
                ):
                    self._fail("schema_unverified")
                indices[descriptor["key"]] = descriptor["index"]
            if set(indices) != {"timestamp", "heartrate"} or len(set(indices.values())) != 2:
                self._fail("schema_unverified")
        values = raw.get("heartRateValues")
        if not isinstance(values, list) or len(values) > 10000:
            self._fail("schema_unverified")
        normalized = []
        for point in values:
            if not isinstance(point, list) or len(point) <= max(indices.values()):
                self._fail("schema_unverified")
            instant = point[indices["timestamp"]]
            if isinstance(instant, dict):
                from datetime import datetime

                value = instant.get("date")
                if not isinstance(value, str) or "T" not in value:
                    self._fail("schema_unverified")
                try:
                    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    # ConnectTimestamp.date is explicitly UTC in Garmin's schema.
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=UTC)
                    instant = int(parsed.timestamp() * 1000)
                except (ValueError, OverflowError, OSError):
                    self._fail("schema_unverified")
            normalized.append([instant, point[indices["heartrate"]]])
        return {
            **raw,
            "calendarDate": day,
            "heartRateValues": normalized,
            "heartRateValueDescriptors": [
                {"index": 0, "key": "timestamp"},
                {"index": 1, "key": "heartrate"},
            ],
            "_slipstream_source": {
                "provider": "personal",
                "sampling": "resolution_unverified",
                "calendar_date": wire_day,
                "value_descriptors": descriptors,
            },
        }

    def get_training_readiness_bounded(self, day):
        from .training_readiness import ReadinessRead

        calendar_day(day)
        return ReadinessRead("unsupported")

    def get_stress_data(self, day):
        from .training_readiness import ReadinessRead

        calendar_day(day)
        return ReadinessRead("unsupported")

    def get_activities_by_date(self, start, end, activitytype=None, sortorder="desc"):
        from .activity_page import MAX_METADATA_CALLS, PAGE_SIZE

        self._range(start, end, 366)
        if sortorder not in {"asc", "desc"}:
            raise ValueError("Activity sort order must be asc or desc.")
        if activitytype is not None and (
            not isinstance(activitytype, str) or not re.fullmatch(r"[a-z_]{1,64}", activitytype)
        ):
            raise ValueError("Invalid activity type.")
        rows = []
        for page in range(MAX_METADATA_CALLS):
            current = self.get_activity_page(start, end, offset=page * PAGE_SIZE)
            rows.extend(current)
            if len(current) < PAGE_SIZE:
                if activitytype is not None:
                    rows = [row for row in rows if row["activityType"]["typeKey"] == activitytype]
                return rows if sortorder == "desc" else list(reversed(rows))
        self._fail("budget_exhausted")

    def verify_account(self):
        """Validate the selected account without requesting fitness payloads."""
        self._ensure()
        return self.account_identity
