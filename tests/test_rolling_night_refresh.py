"""Stored source admission and bounded rolling consumer contracts."""

import hashlib
import json
import unittest
from datetime import date, timedelta

from test_night_baseline_refresh import WritableSnapshots
from test_rolling_reference_sources import sdk_store, seeded

from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import normalize_args, storage_key
from scripts.night_baseline_fixtures import contract_cases, native_fixture


class RollingStore(WritableSnapshots):
    def list_revisions_bounded(self, prefix, **_):
        self.operations["list_pages"] += 1
        return {key: {"revision": row.revision, "size": len(row.data)}
                for key, row in self.objects.items() if key.startswith(prefix)}


class RollingRefreshTest(unittest.TestCase):
    def store(self):
        store = RollingStore()
        store.objects = native_fixture().objects.copy()
        return store

    def proof(self, store, day="2026-10-25"):
        key = f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json"
        value = store.objects[key]
        return {"key": key, "revision": value.revision,
                "sha256": hashlib.sha256(value.data).hexdigest()}

    def add_sleep(self, store, day):
        store.add(f"health/sleep/v1/{day[:4]}/{day[5:7]}/{day}.json", {
            "schema_version": 1, "source": "garmin", "date": day,
            "sleep_start_gmt": day + "T00:00:00Z", "sleep_end_gmt": day + "T07:00:00Z",
        })

    def reopen(self, store):
        fresh = RollingStore()
        fresh.objects = store.objects
        return fresh

    def drain(self, store, today, *, sources=None, limit=30, snapshot_cache=None):
        reports = []
        for _ in range(limit):
            store = self.reopen(store)
            result = refresh.refresh_pending(store, today=today, timezone="Europe/Oslo",
                                             period_names=set(), rolling_sources=sources,
                                             snapshot_cache=snapshot_cache)
            reports.append(result)
            if not any(row["count"] for channels in result["rolling_remaining"].values()
                       for row in channels.values()):
                break
        return store, reports

    def test_verified_sleep_registers_compact_six_channel_windows_and_deduplicates(self):
        store = self.store()
        result = refresh.register_rolling_endpoints(store, [self.proof(store)])
        self.assertEqual(result["rolling_remaining"]["28"]["heart_rate"]["count"], 1)
        self.assertEqual(result["rolling_remaining"]["90"]["hrv"]["ranges"],
                         [["2026-10-25", "2026-10-25"]])
        again = refresh.register_rolling_endpoints(store, [self.proof(store)])
        self.assertEqual(again["rolling_remaining"], result["rolling_remaining"])
        queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
        self.assertEqual(len(queue["pending"]), 0)
        self.assertLess(len(store.objects[refresh.QUEUE_KEY].data), 2048)

    def test_passive_consumer_publishes_inclusive_28_day_channels_on_endpoint_day(self):
        store = self.store()
        refresh.register_rolling_endpoints(store, [self.proof(store)])
        result = refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo")
        self.assertEqual(result["products_written"], 2)
        for channel in ("heart_rate", "hrv"):
            args = normalize_args(start_date="2026-09-28", end_date="2026-10-25",
                                  period="28", channel=channel)
            product = json.loads(store.objects[storage_key(args)].data)
            self.assertEqual(product["nights_requested"], 28)
            self.assertEqual(product["contributing_nights"], 1)
            self.assertEqual(product["bins"][4]["p50"], 45 if channel == "hrv" else 55.5)
        self.assertEqual(result["rolling_remaining"]["28"]["heart_rate"]["count"], 0)
        self.assertEqual(result["rolling_remaining"]["90"]["hrv"]["count"], 1)
        self.assertEqual(result["status"], "bounded_pending")

    def test_corrected_old_source_revises_every_known_later_endpoint_without_fanout(self):
        store = self.store()
        for day in ("2026-10-26", "2026-10-27"):
            self.add_sleep(store, day)
        refresh.register_rolling_endpoints(store, [self.proof(store, day) for day in
                                                   ("2026-10-25", "2026-10-26", "2026-10-27")])
        store, _ = self.drain(store, "2026-10-27")
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 75
        refresh.publish_source(store, key, json.dumps(source).encode(), "application/json")
        queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
        remaining = refresh.rolling_cohort_status(queue, [key], today="2026-10-27")
        self.assertEqual(remaining["90"]["hrv"],
                         {"count": 3, "ranges": [["2026-10-25", "2026-10-27"]]})
        self.assertEqual(len(queue["pending"]), 2)  # legacy week/month only
        store, reports = self.drain(store, "2026-10-27", sources=[key])
        for days, first in ((28, "2026-09-28"), (30, "2026-09-26"), (90, "2026-07-28")):
            for offset in range(3):
                start = (date.fromisoformat(first) + timedelta(days=offset)).isoformat()
                end = (date(2026, 10, 25) + timedelta(days=offset)).isoformat()
                args = normalize_args(start_date=start, end_date=end, period="custom", channel="hrv")
                product = json.loads(store.objects[storage_key(args)].data)
                self.assertEqual(product["nights_requested"], days)
                self.assertEqual(product["bins"][4]["p50"], 75)
        self.assertTrue(any("/history/" in key for key in store.objects))
        self.assertEqual(sum(row["count"] for channels in reports[-1]["rolling_remaining"].values()
                             for row in channels.values()), 0)

    def test_unverified_hr_does_not_block_independent_hrv_checkpoint(self):
        store = self.store()
        key = "health/signals/v1/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["channels"]["heart_rate"]["readings"][0]["value"] = "invalid"
        store.add(key, source)
        refresh.register_rolling_endpoints(store, [self.proof(store)])
        result = refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo")
        self.assertEqual(result["products_written"], 1)
        self.assertEqual(result["rolling_remaining"]["28"]["hrv"]["count"], 0)
        self.assertEqual(result["rolling_remaining"]["28"]["heart_rate"]["count"], 1)
        args = normalize_args(start_date="2026-09-28", end_date="2026-10-25",
                              period="28", channel="hrv")
        self.assertEqual(json.loads(store.objects[storage_key(args)].data)["contributing_nights"], 1)

    def test_30_custom_and_90_share_canonical_engine_across_year_and_dst(self):
        boundaries = {
            "2021-01-01": ("2020-12-05", "2020-12-03", "2020-10-04"),
            "2026-03-29": ("2026-03-02", "2026-02-28", "2025-12-30"),
        }
        for case in contract_cases():
            day = case["day"]
            if day not in boundaries:
                continue
            with self.subTest(day=day):
                store = RollingStore()
                store.objects = case["store"].objects.copy()
                refresh.register_rolling_endpoints(store, [self.proof(store, day)])
                store, reports = self.drain(store, day, snapshot_cache={})
                self.assertEqual(sum(row["products_written"] for row in reports), 6)
                for days, start in zip((28, 30, 90), boundaries[day], strict=True):
                    args = normalize_args(start_date=start, end_date=day, period="custom", channel="hrv")
                    product = json.loads(store.objects[storage_key(args)].data)
                    self.assertEqual(product["nights_requested"], days)
                    self.assertEqual(product["contributing_nights"], 1)
                    self.assertEqual(product["method_version"], "night-baseline-v1")
                self.assertEqual(reports[-1]["status"], "complete")

    def test_dense_90_nights_finish_with_shared_snapshots_inside_component_bounds(self):
        store = RollingStore()
        for offset in range(90):
            day = (date(2026, 3, 29) - timedelta(days=offset)).isoformat()
            self.add_sleep(store, day)
            store.add(f"health/hrv/{day[:4]}/{day[5:7]}/{day}.json", {
                "schema_version": 1, "source": "garmin", "date": day,
                "readings": [{"timestamp": day + "T00:05:00Z", "hrv_ms": 45,
                              "time_basis": "utc"}],
            })
            store.add(f"health/signals/v1/{day[:4]}/{day[5:7]}/{day}.json", {
                "schema_version": 1, "source": "garmin", "date": day,
                "channels": {"heart_rate": {"status": "available", "unit": "bpm",
                    "readings": [{"timestamp": day + "T00:05:00Z", "value": 55,
                                  "time_basis": "utc"}]}},
            })
        refresh.register_rolling_endpoints(store, [self.proof(store, "2026-03-29")])
        store, reports = self.drain(store, "2026-03-29", snapshot_cache={})
        self.assertLessEqual(len(reports), 14)
        self.assertEqual(sum(row["products_written"] for row in reports), 6)
        for report in reports:
            self.assertLessEqual(report["component_operations"]["get"], 400)
            self.assertLessEqual(report["component_operations"]["list"], 100)
            self.assertLessEqual(report["component_operations"]["put"], 100)
        args = normalize_args(start_date="2025-12-30", end_date="2026-03-29",
                              period="90", channel="hrv")
        self.assertEqual(json.loads(store.objects[storage_key(args)].data)["contributing_nights"], 90)
        print({"rolling_90_dense_phases": [row["component_operations"] for row in reports]})

    def test_90_known_endpoints_stay_compact_and_one_call_materializes_at_most_two(self):
        store = RollingStore()
        days = [(date(2026, 1, 1) + timedelta(days=i)).isoformat() for i in range(90)]
        for day in days:
            self.add_sleep(store, day)
        for start in range(0, 90, 14):
            refresh.register_rolling_endpoints(store, [self.proof(store, day)
                                                     for day in days[start:start + 14]])
        queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
        self.assertEqual(len(queue["pending"]), 0)
        self.assertEqual(queue["rolling"]["eligible"], [["2026-01-01", "2026-03-31"]])
        self.assertLess(len(store.objects[refresh.QUEUE_KEY].data), 2048)
        result = refresh.refresh_pending(store, today="2026-03-31", timezone="Europe/Oslo")
        self.assertLessEqual(result["periods_completed"], 2)
        self.assertGreater(result["closed_pending"], 500)
        self.assertEqual(result["status"], "bounded_pending")

    def test_unknown_product_and_unknown_source_remain_hold_after_restart(self):
        class UnknownProduct(RollingStore):
            def _write(self, key, data, kind, encoding):
                value = super()._write(key, data, kind, encoding)
                if "/baseline/" in key:
                    raise OSError("synthetic unknown")
                return value

        store = UnknownProduct()
        store.objects = native_fixture().objects.copy()
        refresh.register_rolling_endpoints(store, [self.proof(store)])
        with self.assertRaises(refresh.BaselineRefreshStop):
            refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo")
        restarted = self.reopen(store)
        result = refresh.refresh_pending(restarted, today="2026-10-25", timezone="Europe/Oslo")
        queue = json.loads(restarted.objects[refresh.QUEUE_KEY].data)
        held = [row for row in queue["pending"].values() if row.get("inflight")]
        self.assertEqual(len(held), 1)
        self.assertEqual(result["status"], "held")
        self.assertEqual(result["rolling_remaining"]["28"]["heart_rate"]["count"], 1)

        class UnknownSource(RollingStore):
            def _write(self, key, data, kind, encoding):
                value = super()._write(key, data, kind, encoding)
                if key.startswith("health/hrv/"):
                    raise OSError("synthetic source unknown")
                return value

        source_store = UnknownSource()
        source_store.objects = self.store().objects.copy()
        refresh.register_rolling_endpoints(source_store, [self.proof(source_store)])
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(source_store.objects[key].data)
        source["readings"][0]["hrv_ms"] = 75
        with self.assertRaises(refresh.BaselineRefreshStop):
            refresh.publish_source(source_store, key, json.dumps(source).encode(), "application/json")
        fresh = self.reopen(source_store)
        result = refresh.refresh_pending(fresh, today="2026-10-25", timezone="Europe/Oslo")
        self.assertEqual(result["products_written"], 0)
        self.assertEqual(result["status"], "held")
        self.assertEqual(result["rolling_remaining"]["90"]["hrv"]["count"], 1)

    def test_context_intent_holds_dependent_ranges_before_materialization(self):
        store = self.store()
        refresh.register_rolling_endpoints(store, [self.proof(store)])
        queue = json.loads(store.objects[refresh.QUEUE_KEY].data)
        identity = "nctx-" + "a" * 24
        queue["rolling_context_intents"] = {identity: {
            "wake_date": "2026-10-25", "state": "intent", "sha256": "b" * 64,
            "key": "health/night-context/v1/2026/10/2026-10-25/20261025T120000000-"
                   + identity + ".json",
        }}
        store.add(refresh.QUEUE_KEY, queue)
        result = refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo")
        self.assertEqual(result["products_written"], 0)
        self.assertEqual(result["pending_materialized"], 0)
        self.assertEqual(result["status"], "held")
        self.assertEqual(result["rolling_remaining"]["28"]["hrv"]["count"], 1)

    def test_actual_sdk_publication_and_passive_consumer_keep_exact_fences_and_cas(self):
        server, fixture = seeded("2026-10-25", 1, contexts=True)
        proof = self.proof(fixture)
        refresh.register_rolling_endpoints(sdk_store(server), [proof], today="2026-10-25")
        cache = {}
        reports = []
        for _ in range(6):
            store = sdk_store(server)
            result = refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo",
                                             snapshot_cache=cache)
            reports.append(result)
            if result["status"] == "complete":
                break
        self.assertEqual(sum(row["products_written"] for row in reports), 6)
        self.assertEqual(reports[-1]["status"], "complete")
        key = "health/hrv/2026/10/2026-10-25.json"
        source = json.loads(server.objects[key][0])
        source["readings"][0]["hrv_ms"] = 75
        self.assertTrue(refresh.publish_source(sdk_store(server), key,
                                              json.dumps(source).encode(), "application/json"))
        row = refresh.refresh_pending(sdk_store(server), today="2026-10-25", timezone="Europe/Oslo",
                                      period_names=set(), snapshot_cache=cache)
        self.assertEqual(row["products_written"], 2)
        self.assertTrue(any("/history/" in key for key in server.objects))
        for report in [*reports, row]:
            self.assertLessEqual(report["component_operations"]["get"], 400)
            self.assertLessEqual(report["component_operations"]["list"], 100)
            self.assertLessEqual(report["component_operations"]["put"], 100)
        self.assertTrue(all("if-match" in headers or headers.get("if-none-match") == "*"
                            for method, _, headers in server.headers if method == "PUT"))
        print({"rolling_sdk_phase_costs": [r["component_operations"] for r in [*reports, row]]})

    def test_channel_progress_is_not_starved_by_multiple_invalid_hr_endpoints(self):
        store = self.store()
        for day in ("2026-10-26", "2026-10-27"):
            self.add_sleep(store, day)
        key = "health/signals/v1/2026/10/2026-10-25.json"
        source = json.loads(store.objects[key].data)
        source["channels"]["heart_rate"]["readings"][0]["value"] = "invalid"
        store.add(key, source)
        refresh.register_rolling_endpoints(store, [self.proof(store, day) for day in
                                                   ("2026-10-25", "2026-10-26", "2026-10-27")])
        result = refresh.refresh_pending(store, today="2026-10-27", timezone="Europe/Oslo")
        self.assertEqual(result["products_written"], 1)
        self.assertEqual(result["rolling_remaining"]["28"]["hrv"]["count"], 2)

    def test_unavailable_hr_keeps_good_curve_while_corrected_hrv_revises(self):
        store = self.store()
        refresh.register_rolling_endpoints(store, [self.proof(store)])
        store, _ = self.drain(store, "2026-10-25")
        args = normalize_args(start_date="2026-09-28", end_date="2026-10-25",
                              period="28", channel="heart_rate")
        old_hr = store.objects[storage_key(args)]
        # Service loses native HR; this is unknown input, never a zero curve.
        for key in list(store.objects):
            if key.startswith("health/signals/"):
                del store.objects[key]
        key = "health/hrv/2026/10/2026-10-25.json"
        value = json.loads(store.objects[key].data)
        value["readings"][0]["hrv_ms"] = 75
        refresh.publish_source(store, key, json.dumps(value).encode(), "application/json")
        store = self.reopen(store)
        result = refresh.refresh_pending(store, today="2026-10-25", timezone="Europe/Oslo",
                                         period_names=set())
        self.assertEqual(result["products_written"], 1, result)
        self.assertEqual(store.objects[storage_key(args)].data, old_hr.data)
        self.assertEqual(result["rolling_remaining"]["28"]["heart_rate"]["count"], 1)
        self.assertEqual(result["rolling_remaining"]["28"]["hrv"]["count"], 0)
