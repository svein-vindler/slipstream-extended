"""Bounded memory/native regressions for the consolidated night-reference review."""

import hashlib
import io
import json
import unittest
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from test_night_baseline_refresh import WritableSnapshots

from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import canonical
from pipeline.r2_store import R2Store


class PrimaryFailureTest(unittest.TestCase):
    def test_original_read_failure_survives_incomplete_and_failing_cleanup(self):
        for close_fails in (False, True):
            closed = []
            original = OSError("synthetic primary read")

            class Raw:
                def __init__(self, error, log, failed_close):
                    self.error, self.log, self.failed_close = error, log, failed_close

                def read(self, amount):
                    raise self.error

                def close(self):
                    self.log.append(True)
                    if self.failed_close:
                        raise OSError("synthetic secondary close")

            owner = SimpleNamespace(state={"failed": False, "read_bytes": 0})
            body = refresh._BodyBound(Raw(original, closed, close_fails), owner, 1024, 20)

            class Client:
                def __init__(self, response):
                    self.response = response

                def get_object(self, **_):
                    return {
                        "Body": self.response,
                        "ETag": '"synthetic"',
                        "ContentType": "application/json",
                        "ContentLength": 20,
                    }

            store = R2Store(client=Client(body), bucket="synthetic")
            with self.assertRaisesRegex(
                refresh.BaselineRefreshStop, "response read failed"
            ) as caught:
                store.get_snapshot("synthetic", max_bytes=1024)
            self.assertIs(caught.exception.__cause__, original)
            self.assertEqual(closed, [True])
            self.assertTrue(owner.state["failed"])
            self.assertTrue(body.closed)

    def test_standalone_incomplete_close_still_fails_and_latches(self):
        owner = SimpleNamespace(state={"failed": False, "read_bytes": 0})
        body = refresh._BodyBound(io.BytesIO(b"synthetic"), owner, 1024, 20)
        with self.assertRaisesRegex(refresh.BaselineRefreshStop, "close unverified"):
            body.close()
        self.assertTrue(owner.state["failed"])
        self.assertTrue(body.closed)


def calendar_store(first=date(2026, 10, 1), days=31, contexts=True):
    store = WritableSnapshots()
    for offset in range(-1, days):
        day = first + timedelta(days=offset)
        text = day.isoformat()
        store.add(
            f"health/signals/v1/{day:%Y/%m}/{text}.json",
            {
                "schema_version": 1,
                "source": "garmin",
                "date": text,
                "channels": {
                    "heart_rate": {
                        "status": "available",
                        "unit": "bpm",
                        "readings": [
                            {"timestamp": text + "T01:00:00Z", "value": 55, "time_basis": "utc"}
                        ],
                    }
                },
            },
        )
        if offset < 0:
            continue
        store.add(
            f"health/sleep/v1/{day:%Y/%m}/{text}.json",
            {
                "schema_version": 1,
                "source": "garmin",
                "date": text,
                "sleep_start_gmt": (day - timedelta(days=1)).isoformat() + "T22:00:00Z",
                "sleep_end_gmt": text + "T06:00:00Z",
            },
        )
        store.add(
            f"health/hrv/{day:%Y/%m}/{text}.json",
            {
                "schema_version": 1,
                "source": "garmin",
                "date": text,
                "readings": [{"timestamp": text + "T02:00:00Z", "hrv_ms": 45, "time_basis": "utc"}],
            },
        )
        if contexts:
            user = {
                "schema_version": 1,
                "wake_date": text,
                "previous_context_id": None,
                "created_at": (day + timedelta(days=1)).isoformat() + "T12:00:00.000Z",
                "source": "user",
                "alcohol": None,
                "late_bedtime": None,
                "travel": False,
                "actual_night_timezone": None,
                "illness": None,
                "sleep_quality": None,
                "morning_condition": None,
                "note": None,
                "declared_fields": ["travel"],
                "changed_fields": ["travel"],
            }
            identity = (
                "nctx-"
                + hashlib.sha256(
                    json.dumps(
                        dict(sorted(user.items())), ensure_ascii=False, separators=(",", ":")
                    ).encode()
                ).hexdigest()[:24]
            )
            user["context_id"] = identity
            stamp = (day + timedelta(days=1)).strftime("%Y%m%dT120000000")
            store.add(f"health/night-context/v1/{day:%Y/%m}/{text}/{stamp}-{identity}.json", user)
    return store


def queued_period(store, first, last, sources=None, name=None):
    name = name or "month/" + first[:7]
    queue = {
        "schema_version": 1,
        "kind": "night-baseline-refresh-queue",
        "pending": {
            name: {
                "start_date": first,
                "end_date": last,
                "phase": "dirty",
                "sources": {},
                "completed": {},
            }
        },
    }
    for key in sources or []:
        snap = store.objects[key]
        queue["pending"][name]["sources"][key] = {
            "state": "ready",
            "revision": snap.revision,
            "sha256": hashlib.sha256(snap.data).hexdigest(),
        }
    store.add(refresh.QUEUE_KEY, queue)
    return queue


class FullMonthTest(unittest.TestCase):
    def test_31_ready_nights_and_contexts_finish_below_400_get_without_reusing_fresh_fences(self):
        store = calendar_store()
        keys = [key for key in store.objects if key.startswith("health/hrv/")]
        queued_period(store, "2026-10-01", "2026-10-31", keys)
        result = refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["products_written"], 2)
        products = [
            json.loads(value.data) for key, value in store.objects.items() if "/baseline/" in key
        ]
        self.assertEqual(len(products), 2)
        self.assertTrue(all(value["contributing_nights"] == 31 for value in products))
        self.assertLessEqual(store.operations["get"], 400)
        self.assertLessEqual(store.operations["list_pages"], 100)
        print(json.dumps({"full_month_operations": store.operations}))

    def test_actual_source_revision_changed_after_product_stops_without_queue_completion(self):
        base = calendar_store(days=1)
        key = "health/hrv/2026/10/2026-10-01.json"

        class Changed(WritableSnapshots):
            def _write(self, target, data, kind, encoding):
                receipt = super()._write(target, data, kind, encoding)
                if "/baseline/" in target:
                    value = json.loads(self.objects[key].data)
                    value["readings"][0]["hrv_ms"] += 1
                    self.add(key, value)
                return receipt

        store = Changed()
        store.objects = base.objects
        queued_period(store, "2026-10-01", "2026-10-31", [key])
        with self.assertRaisesRegex(refresh.BaselineRefreshStop, "source revision changed"):
            refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(len(json.loads(store.objects[refresh.QUEUE_KEY].data)["pending"]), 1)


class CalendarAdmissionTest(unittest.TestCase):
    def test_recent_health_uses_configured_calendar_not_host_source_window(self):
        import os
        from unittest.mock import patch

        from pipeline.recent_health import run
        from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

        class Clock(datetime):
            current = datetime(2026, 11, 1, 0, 30, tzinfo=UTC)

            @classmethod
            def now(cls, tz=None):
                return cls.current

        for period, wake, instant, first, last, name in (
            (
                "month",
                date(2026, 10, 31),
                datetime(2026, 11, 1, 0, 30, tzinfo=UTC),
                "2026-10-01",
                "2026-10-31",
                "month/2026-10",
            ),
            (
                "week",
                date(2026, 11, 1),
                datetime(2026, 11, 2, 0, 30, tzinfo=UTC),
                "2026-10-26",
                "2026-11-01",
                "week/2026-W44",
            ),
        ):
            Clock.current = instant
            for zone_name in ("America/Los_Angeles", None):
                initial = calendar_store(first=wake, days=1, contexts=False)
                queued_period(initial, first, last, name=name)
                client = HealthClient()
                for key, snap in initial.objects.items():
                    client.objects[key] = {"Body": snap.data, "ContentType": "application/json"}
                store = R2Store(client=client, bucket="synthetic")
                with (
                    patch.dict(
                        os.environ, {"HEALTH_TIMEZONE": zone_name} if zone_name else {}, clear=True
                    ),
                    patch.object(refresh, "datetime", Clock),
                ):
                    result = run(
                        days=1,
                        streams=("hrv",),
                        today=instant.date(),
                        store=store,
                        garmin=SyntheticGarmin(),
                    )
                self.assertEqual(result["status"], "complete", period)
                self.assertEqual(
                    result["baseline_refresh"]["status"],
                    "waiting_until_close" if zone_name else "calendar_timezone_required",
                    period,
                )
                self.assertEqual(result["baseline_refresh"]["products_written"], 0, period)
                self.assertFalse(any("/baseline/" in key for key in client.objects), period)


class RecordedHoldTest(unittest.TestCase):
    def test_old_reference_hold_does_not_abort_recent_body_or_manual_activity(self):
        import os
        from unittest.mock import patch

        from pipeline.manual_activity_refresh import run as activity_run
        from pipeline.recent_body import run as body_run
        from pipeline.recent_health import run
        from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

        initial = calendar_store(days=1, contexts=False)
        queue = queued_period(initial, "2026-10-01", "2026-10-31")
        row = queue["pending"]["month/2026-10"]
        row.update(
            phase="publishing",
            inflight={
                "key": "synthetic-unresolved-reference",
                "sha256": "a" * 64,
                "revision": None,
            },
        )
        initial.add(refresh.QUEUE_KEY, queue)
        old_scope = dict(row)
        client = HealthClient()
        for key, snap in initial.objects.items():
            client.objects[key] = {"Body": snap.data, "ContentType": "application/json"}
        store = R2Store(client=client, bucket="synthetic")

        class MemoryFile:
            def __init__(self, text):
                self.text = text

            def open(self, **_):
                return io.StringIO(self.text)

            def read_text(self, **_):
                return self.text

        with (
            patch.dict(os.environ, {"HEALTH_TIMEZONE": "UTC"}),
            patch.object(refresh, "_today", return_value="2026-11-02"),
        ):
            result = run(
                days=1,
                streams=("hrv",),
                today=date(2026, 11, 2),
                store=store,
                garmin=SyntheticGarmin(),
            )
            body = body_run(
                health_csv=b"Date,Weight KG\n",
                store=store,
                garmin=SyntheticGarmin(),
                today=date(2026, 11, 2),
            )
            activity = activity_run(
                baseline=MemoryFile("[]"),
                summary=MemoryFile("Activity ID\n"),
                store=store,
                garmin=SyntheticGarmin(),
                today=date(2026, 11, 2),
            )
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["baseline_refresh"]["held_periods"], 1)
        self.assertEqual(body["status"], "idle")
        self.assertEqual(activity["checked_activity_count"], 0)
        retained = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])["pending"]["month/2026-10"]
        self.assertEqual(retained, old_scope)

    def test_other_source_in_same_period_is_independent_but_exact_unknown_key_stays_held(self):
        import copy
        import os
        from unittest.mock import patch

        from pipeline.recent_health import run
        from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

        old_key = "health/hrv/2026/11/2026-11-01.json"
        new_key = "health/hrv/2026/11/2026-11-02.json"
        for reference_inflight in (False, True):
            initial = calendar_store(first=date(2026, 11, 1), days=1, contexts=False)
            queue = queued_period(initial, "2026-11-01", "2026-11-30", [old_key])
            row = queue["pending"]["month/2026-11"]
            if reference_inflight:
                row.update(
                    phase="publishing",
                    inflight={"key": "synthetic-reference", "sha256": "b" * 64, "revision": None},
                )
            else:
                row["sources"][old_key] = {"state": "intent", "sha256": "a" * 64}
            original = copy.deepcopy(row)
            initial.add(refresh.QUEUE_KEY, queue)
            client = HealthClient()
            for key, snap in initial.objects.items():
                client.objects[key] = {"Body": snap.data, "ContentType": "application/json"}
            store = R2Store(client=client, bucket="synthetic")
            with (
                patch.dict(os.environ, {"HEALTH_TIMEZONE": "UTC"}),
                patch.object(refresh, "_today", return_value="2026-11-02"),
            ):
                result = run(
                    days=1,
                    streams=("hrv",),
                    today=date(2026, 11, 2),
                    store=store,
                    garmin=SyntheticGarmin(),
                )
            self.assertEqual(result["status"], "complete")
            self.assertIn(new_key, client.objects)
            retained = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])["pending"][
                "month/2026-11"
            ]
            self.assertTrue(all(retained[k] == v for k, v in original.items()))
            self.assertEqual(retained["following_sources"][new_key]["state"], "ready")
            if not reference_inflight:
                before = store.operations["put"]
                with self.assertRaises(refresh.BaselineRefreshStop):
                    refresh.publish_source(
                        store, old_key, initial.objects[old_key].data, "application/json"
                    )
                self.assertEqual(store.operations["put"], before)


class PriorContributionTest(unittest.TestCase):
    def test_unavailable_old_contributor_is_not_hidden_by_another_new_night(self):

        for old_days in (7, 6):
            store = calendar_store(first=date(2026, 10, 19), days=old_days, contexts=False)
            queued_period(store, "2026-10-19", "2026-10-25", name="week/2026-W43")
            refresh.refresh_pending(store, today="2026-10-26", timezone="UTC", max_periods=1)
            old = {
                name: value.data for name, value in store.objects.items() if "/baseline/" in name
            }
            lost = "health/hrv/2026/10/2026-10-20.json"
            del store.objects[lost]
            if old_days == 6:
                extra = calendar_store(first=date(2026, 10, 25), days=1, contexts=False)
                store.objects.update(extra.objects)
            changed = "health/hrv/2026/10/2026-10-21.json"
            value = json.loads(store.objects[changed].data)
            value["readings"][0]["hrv_ms"] = 68
            refresh.publish_source(store, changed, canonical(value).encode(), "application/json")
            result = refresh.refresh_pending(
                store, today="2026-10-26", timezone="UTC", max_periods=1
            )
            self.assertEqual(result["products_written"], 0)
            self.assertIn("week/2026-W43", result["blocked"])
            self.assertTrue(all(store.objects[name].data == body for name, body in old.items()))

    def test_verified_partial_improvement_and_correction_still_publish(self):
        store = calendar_store(first=date(2026, 10, 19), days=6, contexts=False)
        queued_period(store, "2026-10-19", "2026-10-25", name="week/2026-W43")
        refresh.refresh_pending(store, today="2026-10-26", timezone="UTC", max_periods=1)
        extra = calendar_store(first=date(2026, 10, 25), days=1, contexts=False)
        store.objects.update(extra.objects)
        changed = "health/hrv/2026/10/2026-10-21.json"
        value = json.loads(store.objects[changed].data)
        value["readings"][0]["hrv_ms"] = 68
        refresh.publish_source(store, changed, canonical(value).encode(), "application/json")
        result = refresh.refresh_pending(store, today="2026-10-26", timezone="UTC", max_periods=1)
        self.assertEqual(result["products_written"], 2)
        self.assertEqual(result["blocked"], [])
        products = [
            json.loads(value.data) for key, value in store.objects.items() if "/baseline/" in key
        ]
        self.assertTrue(all(value["contributing_nights"] == 7 for value in products))
        hrv = next(value for value in products if value["args"]["channel"] == "hrv")
        self.assertGreater(max(row["p90"] for row in hrv["bins"] if row["p90"] is not None), 45)


class FullQueueAdmissionTest(unittest.TestCase):
    def test_128_ready_periods_make_progress_before_new_backfill_source_call(self):
        import calendar
        import os
        from unittest.mock import patch

        from pipeline.hrv_backfill import PLAN_KEY, run
        from scripts.benchmark_health_reuse import HealthClient, SyntheticGarmin

        initial = WritableSnapshots()
        queue = {"schema_version": 1, "kind": "night-baseline-refresh-queue", "pending": {}}
        for offset in range(128):
            year = 2010 + offset // 12
            month = offset % 12 + 1
            first = date(year, month, 1)
            last = date(year, month, calendar.monthrange(year, month)[1])
            day = first.isoformat()
            key = f"health/hrv/{year}/{month:02}/{day}.json"
            initial.add(key, {"schema_version": 1, "source": "garmin", "date": day, "readings": []})
            snap = initial.objects[key]
            queue["pending"][f"month/{year}-{month:02}"] = {
                "start_date": day,
                "end_date": last.isoformat(),
                "phase": "dirty",
                "sources": {
                    key: {
                        "state": "ready",
                        "revision": snap.revision,
                        "sha256": hashlib.sha256(snap.data).hexdigest(),
                    }
                },
                "completed": {},
            }
        initial.add(refresh.QUEUE_KEY, queue)
        client = HealthClient()
        for key, snap in initial.objects.items():
            client.objects[key] = {"Body": snap.data, "ContentType": "application/json"}
        client.objects["summary/health_daily.csv"] = {
            "Body": b"Date,HRV Last Night Average\n2026-11-02,45\n",
            "ContentType": "text/csv",
        }
        store = R2Store(client=client, bucket="synthetic")

        class ObservedGarmin(SyntheticGarmin):
            def _fetch(self, stream, day):
                self.pending_at_source = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])[
                    "pending"
                ]
                return super()._fetch(stream, day)

        garmin = ObservedGarmin()
        with (
            patch.dict(os.environ, {"HEALTH_TIMEZONE": "UTC"}),
            patch.object(refresh, "_today", return_value="2026-11-02"),
        ):
            result = run(max_days=1, store=store, garmin=garmin)
        retained = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])
        self.assertEqual(len(garmin.pending_at_source), 126)
        self.assertNotIn("month/2010-01", garmin.pending_at_source)
        self.assertNotIn("month/2010-02", garmin.pending_at_source)
        self.assertEqual(len(retained["pending"]), 128)  # two drained, then week + month added
        self.assertEqual(garmin.calls, [("hrv", "2026-11-02")])
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["attempted_this_run"], 1)
        self.assertEqual(result["failures"], {})
        self.assertIn("health/hrv/2026/11/2026-11-02.json", client.objects)
        self.assertIn(PLAN_KEY, client.objects)
        # Month-fence savings admit the source in this ordinary budget. A later
        # invocation admits the logical date again using its stored canonical bytes.
        self.assertLessEqual(result["baseline_refresh"]["component_operations"]["get"], 400)
        self.assertLessEqual(result["baseline_refresh"]["component_operations"]["list"], 100)
        print(
            json.dumps(
                {
                    "full_queue_admission": result["baseline_refresh"],
                    "store_operations": store.operations,
                    "pending_at_source": len(garmin.pending_at_source),
                }
            )
        )
        import copy

        next_client = HealthClient()
        next_client.objects = copy.deepcopy(client.objects)
        next_store = R2Store(client=next_client, bucket="synthetic")
        next_garmin = SyntheticGarmin()
        with (
            patch.dict(os.environ, {"HEALTH_TIMEZONE": "UTC"}),
            patch.object(refresh, "_today", return_value="2026-11-02"),
        ):
            following = run(max_days=1, store=next_store, garmin=next_garmin)
        self.assertEqual(following["attempted_this_run"], 1)
        self.assertEqual(next_garmin.calls, [])
        self.assertIn("health/hrv/2026/11/2026-11-02.json", next_client.objects)


class ContextFenceAndCheckpointTest(unittest.TestCase):
    def test_new_context_after_product_is_found_by_fresh_postpublication_listing(self):
        class Changed(WritableSnapshots):
            changed = False

            def _write(self, key, data, kind, encoding):
                receipt = super()._write(key, data, kind, encoding)
                if "/baseline/" in key and not self.changed:
                    self.changed = True
                    original = next(name for name in self.objects if "/night-context/" in name)
                    row = json.loads(self.objects[original].data)
                    row.pop("context_id")
                    row["created_at"] = "2026-10-02T13:00:00.000Z"
                    row["travel"] = True
                    identity = (
                        "nctx-"
                        + hashlib.sha256(
                            json.dumps(
                                dict(sorted(row.items())), ensure_ascii=False, separators=(",", ":")
                            ).encode()
                        ).hexdigest()[:24]
                    )
                    row["context_id"] = identity
                    self.add(
                        "health/night-context/v1/2026/10/2026-10-01/20261002T130000000-"
                        + identity
                        + ".json",
                        row,
                    )
                return receipt

        base = calendar_store(days=1)
        store = Changed()
        store.objects = base.objects
        queued_period(store, "2026-10-01", "2026-10-31")
        with self.assertRaisesRegex(refresh.BaselineRefreshStop, "context revision changed"):
            refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertIn("month/2026-10", json.loads(store.objects[refresh.QUEUE_KEY].data)["pending"])

    def test_known_completed_product_checkpoints_resume_without_repeating_product_writes(self):
        class Recorded(WritableSnapshots):
            def __init__(self):
                super().__init__()
                self.checkpoints = []

            def _write(self, key, data, kind, encoding):
                result = super()._write(key, data, kind, encoding)
                if key == refresh.QUEUE_KEY:
                    self.checkpoints.append(data)
                return result

        base = calendar_store()
        store = Recorded()
        store.objects = base.objects
        queued_period(
            store,
            "2026-10-01",
            "2026-10-31",
            [key for key in store.objects if key.startswith("health/hrv/")],
        )
        refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        acknowledged = next(
            body
            for body in reversed(store.checkpoints)
            if json.loads(body)["pending"]
            and len(json.loads(body)["pending"]["month/2026-10"]["completed"]) == 2
        )
        resumed = WritableSnapshots()
        resumed.objects = store.objects.copy()
        resumed.add(refresh.QUEUE_KEY, json.loads(acknowledged))
        result = refresh.refresh_pending(resumed, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["products_written"], 0)
        self.assertEqual(result["products_unchanged"], 2)
        self.assertFalse(any("/baseline/" in key for key in resumed.writes))
        self.assertLessEqual(resumed.operations["get"], 400)
