import hashlib
import json
import os
import unittest
from types import SimpleNamespace

from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import canonical, normalize_args
from scripts.night_baseline_fixtures import Snapshots, native_fixture


class CalendarTest(unittest.TestCase):
    def test_source_calendar_margin_and_iso_year_are_not_wake_day_shortcuts(self):
        hrv = refresh.affected_periods("hrv", "2021-01-01")
        self.assertEqual(hrv["week/2020-W53"], ("2020-12-28", "2021-01-03"))
        self.assertEqual(hrv["month/2021-01"], ("2021-01-01", "2021-01-31"))
        hr = refresh.affected_periods("signals", "2026-03-31")
        self.assertIn("month/2026-03", hr)
        self.assertIn("month/2026-04", hr)
        self.assertIn("week/2026-W13", hr)
        self.assertIn("week/2026-W14", hr)
        self.assertFalse(refresh.is_closed(("2026-10-26", "2026-11-01"), "2026-10-31"))
        self.assertTrue(refresh.is_closed(("2026-10-19", "2026-10-25"), "2026-10-26"))


class WritableSnapshots(Snapshots):
    def __init__(self):
        super().__init__()
        self.writes = []
        self.operations = {"get": 0, "list_pages": 0, "put": 0}

    @property
    def remaining_write_attempts(self):
        return 250 - self.operations["put"]

    def can_write_group(self, *, attempts, byte_count):
        return attempts <= self.remaining_write_attempts

    def get_snapshot(self, key, **kwargs):
        self.operations["get"] += 1
        return super().get_snapshot(key, **kwargs)

    def list_prefix_bounded(self, prefix, **kwargs):
        self.operations["list_pages"] += 1
        return super().list_prefix_bounded(prefix, **kwargs)

    def create_with_receipt(self, key, data, kind, **kwargs):
        if key in self.objects:
            return SimpleNamespace(accepted=False, revision=None)
        return self._write(key, data, kind, kwargs.get("encoding"))

    def put_if_revision(self, key, data, kind, *, revision, **kwargs):
        if key not in self.objects or self.objects[key].revision != revision:
            return SimpleNamespace(accepted=False, revision=None)
        return self._write(key, data, kind, kwargs.get("encoding"))

    def put(self, key, data, kind, **kwargs):
        self._write(key, data, kind, kwargs.get("encoding"))

    def list_keys(self, prefix=""):
        return {key for key in self.objects if key.startswith(prefix)}

    def _write(self, key, data, kind, encoding):
        revision = '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
        self.objects[key] = SimpleNamespace(
            data=data, revision=revision, content_type=kind, encoding=encoding
        )
        self.writes.append(key)
        self.operations["put"] += 1
        return SimpleNamespace(accepted=True, revision=revision)


class IncrementalTest(unittest.TestCase):
    def test_correction_and_later_unchanged_ingestion_refresh_closed_week_then_month(self):
        initial = native_fixture()
        store = WritableSnapshots()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 65
        self.assertTrue(
            refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        )
        first = refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        self.assertEqual((first["periods_completed"], first["products_written"]), (1, 2))
        week_args = normalize_args(
            start_date="2026-10-19", end_date="2026-10-25", period="7", channel="hrv"
        )
        from pipeline.night_products import storage_key

        week = json.loads(store.objects[storage_key(week_args)].data)
        self.assertEqual(week["bins"][4]["p50"], 65)
        self.assertFalse(
            refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        )
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        store = reopened
        second = refresh.refresh_pending(store, today="2026-11-01", timezone="Europe/Oslo")
        self.assertEqual((second["periods_completed"], second["products_written"]), (1, 2))
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        store = reopened
        self.assertEqual(refresh.refresh_pending(store, today="2026-11-02")["products_written"], 0)
        source["readings"][0]["hrv_ms"] = 75
        self.assertTrue(
            refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        )
        corrected = refresh.refresh_pending(store, today="2026-11-02", timezone="Europe/Oslo")
        self.assertEqual(corrected["products_written"], 2)
        self.assertEqual(corrected["status"], "bounded_pending")
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        remainder = refresh.refresh_pending(reopened, today="2026-11-02", timezone="Europe/Oslo")
        self.assertEqual(corrected["products_written"] + remainder["products_written"], 4)
        self.assertEqual(
            json.loads(store.objects[storage_key(week_args)].data)["bins"][4]["p50"], 75
        )


class HistoryTest(unittest.TestCase):
    def test_previous_current_reference_body_and_revision_are_preserved_before_replace(self):
        initial = native_fixture()
        store = WritableSnapshots()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 65
        refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        old = {key: value for key, value in store.objects.items() if "/baseline/" in key}
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        store = reopened
        source["readings"][0]["hrv_ms"] = 75
        refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        history = [
            (key, value)
            for key, value in store.objects.items()
            if "/history/" in key and key.endswith("/product.json")
        ]
        self.assertEqual(len(history), 2)
        for old_key, before in old.items():
            saved = [(key, value) for key, value in history if value.data == before.data]
            self.assertEqual(len(saved), 1)
            proof = json.loads(
                store.objects[saved[0][0].removesuffix("product.json") + "provenance.json"].data
            )
            self.assertEqual(
                (proof["previous_key"], proof["previous_revision"], proof["sha256"]),
                (old_key, before.revision, hashlib.sha256(before.data).hexdigest()),
            )
            self.assertLess(store.writes.index(saved[0][0]), store.writes.index(old_key))


class ImportBoundaryTest(unittest.TestCase):
    def test_real_local_import_reserves_control_writes_before_opening_source_intent(self):
        from pipeline.local_import import SPECS, import_file

        class Export:
            name = "synthetic.json"

            def read_text(self, **_):
                return json.dumps(
                    [
                        {
                            "hrvSummary": {"calendarDate": "2026-10-25"},
                            "hrvReadings": [
                                {"readingTimeGMT": "2026-10-25T00:05:00Z", "hrvValue": 45}
                            ],
                        },
                        {"hrvSummary": {"calendarDate": "2026-10-24"}, "hrvReadings": []},
                    ]
                )

        class Limited(WritableSnapshots):
            @property
            def remaining_write_attempts(self):
                return 4 - self.operations["put"]

        store = Limited()
        result = import_file(Export(), SPECS["hrv"], store=store, limit=10)
        self.assertEqual(result["attempted_this_run"], 1)
        self.assertEqual(result["imported_this_run"], 1)
        self.assertEqual(result["status"], "active")
        self.assertEqual(len([key for key in store.writes if key.startswith("health/hrv/")]), 1)
        queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
        self.assertTrue(
            all(
                source["state"] == "ready"
                for row in queue["pending"].values()
                for source in row["sources"].values()
            )
        )


class NormalReportTest(unittest.TestCase):
    def test_current_open_periods_do_not_make_a_successful_source_refresh_partial(self):
        from datetime import date
        from unittest.mock import patch

        from pipeline.r2_store import R2Store
        from pipeline.recent_health import run

        clock = patch("pipeline.night_baseline_refresh._today", return_value="2026-10-31")
        environment = patch.dict(os.environ, {"HEALTH_TIMEZONE": "Europe/Oslo"})
        clock.start()
        environment.start()
        self.addCleanup(clock.stop)
        self.addCleanup(environment.stop)
        from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

        client = HealthClient()
        store = R2Store(client=client, bucket="synthetic")
        first = run(days=1, store=store, garmin=SyntheticGarmin(), today=date(2026, 10, 31))
        self.assertEqual(first["status"], "complete")
        self.assertEqual(first["baseline_refresh"]["status"], "waiting_until_close")
        self.assertEqual(first["baseline_refresh"]["products_written"], 0)
        first_counts = dict(store.operations)
        second = run(days=1, store=store, garmin=SyntheticGarmin(), today=date(2026, 10, 31))
        self.assertEqual(second["status"], "complete")
        self.assertTrue(
            all(stream["objects_written"] == 0 for stream in second["streams"].values())
        )
        self.assertEqual(second["baseline_refresh"]["status"], "waiting_until_close")
        self.assertEqual(len([key for key in client.objects if "/baseline/" in key]), 0)
        print(
            json.dumps(
                {
                    "synthetic_first_pair": first_counts,
                    "synthetic_unchanged_pair": {
                        key: value - first_counts[key] for key, value in store.operations.items()
                    },
                }
            )
        )


class UnknownTest(unittest.TestCase):
    def test_unknown_product_write_keeps_intent_and_restart_never_adopts_or_retries(self):
        class Unknown(WritableSnapshots):
            def _write(self, key, data, kind, encoding):
                result = super()._write(key, data, kind, encoding)
                if "/baseline/" in key:
                    raise OSError("synthetic unknown after remote commit")
                return result

        initial = native_fixture()
        store = Unknown()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 65
        refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        with self.assertRaises(refresh.BaselineRefreshStop):
            refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        self.assertEqual(len([key for key in store.writes if "/baseline/" in key]), 1)
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        result = refresh.refresh_pending(reopened, today="2026-10-26", timezone="Europe/Oslo")
        self.assertEqual(result["status"], "held")
        self.assertEqual(result["held_periods"], 1)
        self.assertEqual(reopened.writes, [])
        self.assertEqual(reopened.reads, [refresh.QUEUE_KEY])

    def test_empty_or_malformed_source_cannot_replace_good_native_curve(self):
        initial = native_fixture()
        store = WritableSnapshots()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        before = store.objects[key]
        source = json.loads(before.data)
        source["readings"] = []
        self.assertFalse(
            refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        )
        source["readings"] = [
            {"timestamp": "2026-10-25T00:05:00Z", "hrv_ms": None, "time_basis": "utc"}
        ]
        with self.assertRaises(refresh.BaselineRefreshStop):
            refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        self.assertEqual(store.objects[key], before)
        self.assertEqual(store.writes, [])


class SDKBoundaryTest(unittest.TestCase):
    def test_normal_sdk_constructor_no_put_retry_and_helpers_release_shared_client_hooks(self):
        import io
        import socket
        import xml.sax.saxutils
        from contextlib import ExitStack
        from unittest.mock import patch
        from urllib.parse import parse_qs, unquote, urlsplit

        import boto3
        import botocore.session
        from urllib3.connectionpool import HTTPSConnectionPool
        from urllib3.response import HTTPResponse

        from pipeline.r2_store import R2Store

        prior_body = b'{"synthetic":true}'
        objects = {"synthetic-prior": (prior_body, '"synthetic-prior"', "application/json", None)}
        requests = []
        fail = [False]
        list_override = [None]
        returned_bodies = []
        close_failure = [False]
        transport_unknown = [None]

        def peer(connection, method, url, **kwargs):
            parsed = urlsplit(url)
            key = unquote(parsed.path).removeprefix("/synthetic/")
            q = parse_qs(parsed.query)
            requests.append((method, key))
            if method == "PUT" and transport_unknown[0] is not None:
                raise transport_unknown[0]
            headers = {
                str(k).lower(): v.decode() if isinstance(v, bytes) else v
                for k, v in kwargs["headers"].items()
            }
            response = {}
            status = 200
            declared_length = None
            if "list-type" in q:
                prefix = q.get("prefix", [""])[0]
                rows = "".join(
                    "<Contents><Key>"
                    + xml.sax.saxutils.escape(name)
                    + "</Key><Size>"
                    + str(len(item[0]))
                    + '</Size><ETag>"synthetic-etag"</ETag></Contents>'
                    for name, item in objects.items()
                    if name.startswith(prefix)
                )
                data = (
                    '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>'
                    + rows
                    + "</ListBucketResult>"
                ).encode("utf-8-sig")
                if list_override[0] is not None:
                    data = list_override[0]
                response["content-type"] = "application/xml"
            elif method == "HEAD":
                data = b""
                if key in objects:
                    stored, revision, kind, encoding = objects[key]
                    declared_length = len(stored)
                    response.update(etag=revision, **{"content-type": kind})
                    if encoding:
                        response["content-encoding"] = encoding
                else:
                    status = 404
            elif method == "GET":
                if key in objects:
                    data, revision, kind, encoding = objects[key]
                    response.update(etag=revision, **{"content-type": kind})
                    if encoding:
                        response["content-encoding"] = encoding
                else:
                    status = 404
                    data = b"<Error><Code>NoSuchKey</Code></Error>"
                    response["content-type"] = "application/xml"
            else:
                self.assertEqual(method, "PUT")
                if fail[0]:
                    status = 503
                    data = b"<Error><Code>ServiceUnavailable</Code></Error>"
                else:
                    self.assertTrue(headers.get("if-none-match") == "*" or headers.get("if-match"))
                    body = kwargs["body"]
                    data = body if isinstance(body, bytes) else body.read()
                    if "aws-chunked" in headers.get("content-encoding", ""):
                        decoded = b""
                        offset = 0
                        while True:
                            end = data.index(b"\r\n", offset)
                            size = int(data[offset:end].split(b";")[0], 16)
                            offset = end + 2
                            if not size:
                                break
                            decoded += data[offset : offset + size]
                            offset += size + 2
                        data = decoded
                    revision = '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
                    objects[key] = (
                        data,
                        revision,
                        headers["content-type"],
                        ",".join(
                            token.strip()
                            for token in headers.get("content-encoding", "").split(",")
                            if token.strip() and token.strip() != "aws-chunked"
                        )
                        or None,
                    )
                    response["etag"] = revision
                    data = b""
            response["content-length"] = str(
                len(data) if declared_length is None else declared_length
            )

            class CloseFails(io.BytesIO):
                def close(self):
                    super().close()
                    raise OSError("synthetic close failure")

            body = (
                CloseFails(data)
                if close_failure[0] and method == "PUT" and key.startswith("health/hrv/")
                else io.BytesIO(data)
            )
            returned_bodies.append(body)
            return HTTPResponse(
                body=body,
                status=status,
                request_method=method,
                headers=response,
                preload_content=False,
                decode_content=False,
            )

        native = botocore.session.Session()
        native._config = {"profiles": {}}
        native._profile_map = {}
        session = boto3.session.Session(botocore_session=native)

        def no_socket(*_, **__):
            raise AssertionError("No socket/discovery")

        with ExitStack() as stack:
            stack.enter_context(patch.object(HTTPSConnectionPool, "urlopen", peer))
            stack.enter_context(patch.object(socket, "create_connection", no_socket))
            stack.enter_context(patch.object(socket, "getaddrinfo", no_socket))
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {
                        "CLOUDFLARE_ACCOUNT_ID": "0" * 32,
                        "R2_ACCESS_KEY_ID": "synthetic",
                        "R2_SECRET_ACCESS_KEY": "synthetic",
                    },
                )
            )
            stack.enter_context(patch.object(boto3, "client", session.client))
            store = R2Store(bucket="synthetic")
            self.assertEqual(store.client.meta.config.retries["total_max_attempts"], 1)
            self.assertEqual(
                (store.client.meta.config.connect_timeout, store.client.meta.config.read_timeout),
                (5, 20),
            )
            original = store.client._endpoint.http_session.send
            for _ in range(20):
                store.get_snapshot("synthetic-prior", max_bytes=1024)
            source = {"schema_version": 1, "source": "garmin", "date": "2026-10-25", "readings": []}
            self.assertTrue(
                refresh.publish_source(
                    store,
                    "health/hrv/2026/10/2026-10-25.json",
                    canonical(source).encode(),
                    "application/json",
                )
            )
            self.assertEqual(store.client._endpoint.http_session.send, original)
            scoped = refresh.refresh_pending(store, today="2026-10-25", timezone="UTC")
            self.assertEqual(
                store.operations["get"]
                + store.operations["head"]
                - scoped["component_operations"]["get"],
                20,
            )
            stage = store.new_stage()
            self.assertIs(stage._bucket_budget, store._bucket_budget)
            with patch("pipeline.night_baseline_refresh.time.monotonic", return_value=10**12):
                self.assertEqual(
                    stage.get_snapshot("health/hrv/2026/10/2026-10-25.json", max_bytes=1024).data,
                    canonical(source).encode(),
                )
            for invalid_xml in (
                b"<ListBucketResult/>",
                (
                    '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE ListBucketResult [<!ENTITY done "false">]><ListBucketResult><IsTruncated>&done;</IsTruncated></ListBucketResult>'
                ).encode("utf-16"),
                (
                    '<?xml version="1.0" encoding="UTF-16LE"?><!DOCTYPE ListBucketResult [<!ENTITY done "false">]><ListBucketResult><IsTruncated>&done;</IsTruncated></ListBucketResult>'
                ).encode("utf-16le"),
                (
                    '<?xml version="1.0" encoding="UTF-16BE"?><!DOCTYPE ListBucketResult [<!ENTITY done "false">]><ListBucketResult><IsTruncated>&done;</IsTruncated></ListBucketResult>'
                ).encode("utf-16be"),
                b"<ListBucketResult><IsTruncated>unverified</IsTruncated></ListBucketResult>",
                b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>x</Key><Size>unverified</Size></Contents></ListBucketResult>",
                b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>x</Key></Contents></ListBucketResult>",
                b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>x</Key><Size>-1</Size></Contents></ListBucketResult>",
                b"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>",
            ):
                list_override[0] = invalid_xml
                malformed_store = R2Store(bucket="synthetic")
                prior_puts = len([method for method, _ in requests if method == "PUT"])
                with self.assertRaisesRegex(refresh.BaselineRefreshStop, "LIST"):
                    refresh.publish_source(
                        malformed_store,
                        "health/hrv/2026/10/2026-10-24.json",
                        canonical({**source, "date": "2026-10-24"}).encode(),
                        "application/json",
                    )
                self.assertEqual(
                    len([method for method, _ in requests if method == "PUT"]), prior_puts
                )
                self.assertTrue(all(body.closed for body in returned_bodies))
                malformed_store.client.close()
            list_override[0] = None
            fail[0] = True
            prior = len([method for method, _ in requests if method == "PUT"])
            source["date"] = "2026-10-24"
            with self.assertRaises(refresh.BaselineRefreshStop):
                refresh.publish_source(
                    stage,
                    "health/hrv/2026/10/2026-10-24.json",
                    canonical(source).encode(),
                    "application/json",
                )
            self.assertEqual(len([method for method, _ in requests if method == "PUT"]) - prior, 1)
            self.assertEqual(store.client._endpoint.http_session.send, original)
            store.client.close()
            # Explicit injected configuration stays intact; the scoped wire latch
            # still bars a second physical write after the first denied response.
            from botocore.config import Config

            injected = session.client(
                "s3",
                endpoint_url="https://" + "0" * 32 + ".r2.cloudflarestorage.com",
                aws_access_key_id="synthetic",
                aws_secret_access_key="synthetic",
                region_name="auto",
                config=Config(retries={"total_max_attempts": 3}),
            )
            injected_store = R2Store(client=injected, bucket="synthetic")
            prior = len([method for method, _ in requests if method == "PUT"])
            with patch("botocore.endpoint.time.sleep", lambda *_: None):
                with self.assertRaises(refresh.BaselineRefreshStop):
                    refresh.publish_source(
                        injected_store,
                        "health/hrv/2026/10/2026-10-24.json",
                        canonical(source).encode(),
                        "application/json",
                    )
            self.assertEqual(injected.meta.config.retries["total_max_attempts"], 3)
            self.assertEqual(len([method for method, _ in requests if method == "PUT"]) - prior, 1)
            injected.close()
            from urllib3.exceptions import ProtocolError, ReadTimeoutError

            for unknown in (
                ReadTimeoutError(None, "synthetic", "synthetic timeout"),
                ProtocolError("synthetic disconnected"),
                OSError("synthetic transport"),
            ):
                transport_unknown[0] = unknown
                unknown_client = session.client(
                    "s3",
                    endpoint_url="https://" + "0" * 32 + ".r2.cloudflarestorage.com",
                    aws_access_key_id="synthetic",
                    aws_secret_access_key="synthetic",
                    region_name="auto",
                    config=Config(retries={"total_max_attempts": 3}),
                )
                unknown_store = R2Store(client=unknown_client, bucket="synthetic")
                prior = len([method for method, _ in requests if method == "PUT"])
                with patch("botocore.endpoint.time.sleep", lambda *_: None):
                    with self.assertRaisesRegex(
                        refresh.BaselineRefreshStop, "transport outcome unknown"
                    ):
                        refresh.publish_source(
                            unknown_store,
                            "health/hrv/2026/10/2026-10-24.json",
                            canonical(source).encode(),
                            "application/json",
                        )
                self.assertEqual(unknown_client.meta.config.retries["total_max_attempts"], 3)
                self.assertEqual(
                    len([method for method, _ in requests if method == "PUT"]) - prior, 1
                )
                unknown_client.close()
            transport_unknown[0] = None
            fail[0] = False
            close_failure[0] = True
            objects.clear()  # A separate synthetic job; do not adopt the prior failed intent.
            close_store = R2Store(bucket="synthetic")
            with self.assertRaises(refresh.BaselineRefreshStop):
                refresh.publish_source(
                    close_store,
                    "health/hrv/2026/10/2026-10-26.json",
                    canonical({**source, "date": "2026-10-26"}).encode(),
                    "application/json",
                )
            pending = json.loads(objects[refresh.QUEUE_KEY][0])["pending"]
            self.assertTrue(
                any(
                    "health/hrv/2026/10/2026-10-26.json" in row["sources"]
                    for row in pending.values()
                )
            )
            self.assertTrue(
                all(
                    row["sources"]["health/hrv/2026/10/2026-10-26.json"]["state"] == "intent"
                    for row in pending.values()
                    if "health/hrv/2026/10/2026-10-26.json" in row["sources"]
                )
            )
            close_store.client.close()


class PreservationGateTest(unittest.TestCase):
    def test_conflicting_stored_native_points_block_reference_replacement_until_new_source(self):
        initial = native_fixture()
        store = WritableSnapshots()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 65
        refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        old = {name: value.data for name, value in store.objects.items() if "/baseline/" in name}
        hr_key = "health/signals/v1/2026/10/2026-10-25.json"
        broken = json.loads(store.objects[hr_key].data)
        broken["channels"]["heart_rate"]["readings"].append(
            {**broken["channels"]["heart_rate"]["readings"][0], "value": 199}
        )
        refresh.publish_source(store, hr_key, canonical(broken).encode(), "application/json")
        result = refresh.refresh_pending(store, today="2026-10-26", timezone="Europe/Oslo")
        self.assertIn("week/2026-W43", result["blocked"])
        self.assertEqual(result["products_written"], 0)
        self.assertTrue(all(store.objects[name].data == body for name, body in old.items()))
        reopened = WritableSnapshots()
        reopened.objects = store.objects
        refresh.refresh_pending(reopened, today="2026-10-26", timezone="Europe/Oslo")
        self.assertFalse(any("/baseline/" in name for name in reopened.writes))

    def test_explicit_activation_requires_exact_stored_proof_and_never_rewrites_source(self):
        initial = native_fixture()
        store = WritableSnapshots()
        store.objects = initial.objects.copy()
        key = "health/hrv/2026/10/2026-10-25.json"
        before = store.objects[key]
        proof = {
            "key": key,
            "revision": before.revision,
            "sha256": hashlib.sha256(before.data).hexdigest(),
        }
        with self.assertRaises(refresh.BaselineRefreshStop):
            refresh.enqueue_verified_sources(store, [{**proof, "revision": '"foreign"'}])
        self.assertEqual(store.writes, [])
        result = refresh.enqueue_verified_sources(store, [proof])
        self.assertEqual(result, {"queued_sources": 1, "pending_periods": 2})
        self.assertEqual(store.writes, [refresh.QUEUE_KEY])
        self.assertEqual(store.objects[key], before)


class ReportSafetyTest(unittest.TestCase):
    def test_untrusted_nested_status_is_safely_projected_without_private_marker(self):
        import io
        from contextlib import redirect_stdout

        from pipeline.output_privacy import log_report

        output = io.StringIO()
        with redirect_stdout(output):
            for value in (["PRIVATE_STATUS_SENTINEL"], {"PRIVATE_STATUS_SENTINEL": True}):
                log_report(
                    "recent_health",
                    {
                        "status": "complete",
                        "baseline_refresh": {
                            "status": value,
                            "pending": 1,
                            "private": "PRIVATE_STATUS_SENTINEL",
                        },
                    },
                )
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertTrue(
            all(row["baseline_refresh"] == {"status": "unknown", "pending": 1} for row in rows)
        )
        self.assertNotIn("PRIVATE_STATUS_SENTINEL", output.getvalue())
