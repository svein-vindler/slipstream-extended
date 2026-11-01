"""Focused second correction round; all data and service boundaries are synthetic."""

import gzip
import json
import unittest
from datetime import date

from test_night_baseline_corrections import calendar_store, queued_period

from pipeline import night_baseline_refresh as refresh
from pipeline.native_night import NativeInputError, NativeNightReader
from pipeline.night_products import canonical
from pipeline.r2_store import R2Store
from scripts.benchmark_health_reuse import HealthClient


def oversized_hr(compressed):
    initial = calendar_store(first=date(2026, 10, 25), days=1, contexts=False)
    key = "health/signals/v1/2026/10/2026-10-25.json"
    value = json.loads(initial.objects[key].data)
    value["padding"] = "x" * (1050000 if compressed else 270000)
    raw = canonical(value).encode()
    body = gzip.compress(raw, mtime=0) if compressed else raw
    client = HealthClient()
    for name, snap in initial.objects.items():
        client.objects[name] = {"Body": snap.data, "ContentType": "application/json"}
    client.objects[key] = {
        "Body": body,
        "ContentType": "application/json",
        **({"ContentEncoding": "gzip"} if compressed else {}),
    }
    return initial, key, client, body


class CachedSourceBoundsTest(unittest.TestCase):
    def test_cached_and_fresh_reads_apply_the_same_stored_and_decoded_limits_without_extra_get(
        self,
    ):
        for compressed in (False, True):
            _, key, client, _ = oversized_hr(compressed)
            reader = NativeNightReader(R2Store(client=client, bucket="synthetic"), timezone="UTC")
            reader.read(key, 524288, 2097152)
            with self.assertRaises(NativeInputError):
                reader.source("signals", "2026-10-25")
            self.assertEqual(client.reads.count(key), 1)

    def test_public_drain_never_accepts_oversize_hr_because_a_dirty_proof_primed_cache(self):
        for compressed in (False, True):
            for dirty in (False, True):
                initial, key, client, body = oversized_hr(compressed)
                initial.objects[key] = R2Store(client=client, bucket="synthetic").get_snapshot(
                    key, max_bytes=524288
                )
                queued_period(
                    initial,
                    "2026-10-19",
                    "2026-10-25",
                    [key] if dirty else [],
                    name="week/2026-W43",
                )
                client.objects[refresh.QUEUE_KEY] = {
                    "Body": initial.objects[refresh.QUEUE_KEY].data,
                    "ContentType": "application/json",
                }
                client.reads.clear()
                store = R2Store(client=client, bucket="synthetic")
                try:
                    refresh.refresh_pending(
                        store, today="2026-10-26", timezone="UTC", max_periods=1
                    )
                except refresh.BaselineRefreshStop:
                    pass
                self.assertFalse(
                    any("/baseline/" in name for name in client.objects), (compressed, dirty)
                )
                self.assertEqual(client.reads.count(key), 1)


def memory_paths():
    import io
    from pathlib import PurePosixPath

    files = {"data/activities.csv": b"Activity ID\n", "data/health_daily.csv": b"Date,Weight KG\n"}

    class MemoryPath:
        def __init__(self, value):
            self.value = str(value)

        def __str__(self):
            return self.value

        @property
        def parent(self):
            return type(self)(PurePosixPath(self.value).parent)

        def __truediv__(self, part):
            return type(self)(PurePosixPath(self.value) / str(part))

        def resolve(self):
            return self

        def exists(self):
            return self.value in files

        def mkdir(self, **_):
            pass

        def read_bytes(self):
            return files[self.value]

        def read_text(self, **_):
            return self.read_bytes().decode()

        def write_text(self, text, **_):
            files[self.value] = text.encode()

        def open(self, **_):
            return io.StringIO(self.read_text())

    return MemoryPath


class SourceCheckAdmissionTest(unittest.TestCase):
    def test_normal_general_refresh_reports_zero_partial_and_full_source_admission_truthfully(self):
        import os
        from datetime import UTC, datetime
        from types import SimpleNamespace
        from unittest.mock import patch

        from pipeline import recent_health as health
        from pipeline import refresh as orchestration
        from scripts.benchmark_health_reuse import SyntheticGarmin

        class Day(date):
            @classmethod
            def today(cls):
                return cls(2026, 11, 2)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 11, 2, 12, tzinfo=UTC)

        for maximum, expected in [
            (1, {"sleep": 0, "hrv": 0}),
            (16, {"sleep": 1, "hrv": 0}),
            (250, {"sleep": 3, "hrv": 3}),
        ]:
            client = HealthClient()
            source = SyntheticGarmin()

            def factory(limit=maximum, adapter=client):
                return R2Store(client=adapter, bucket="synthetic", max_writes_per_run=limit)

            # Unrelated summary hydration/export are synthetic service boundaries;
            # actual recent_health, body, manual and source-check adapter execute.
            with (
                patch.dict(os.environ, {"HEALTH_TIMEZONE": "UTC"}, clear=True),
                patch.object(orchestration, "Path", memory_paths()),
                patch.object(
                    orchestration.summary_restore,
                    "restore_with_base",
                    return_value=SimpleNamespace(base=object()),
                ),
                patch.object(
                    orchestration.refresh_summaries,
                    "run",
                    return_value={
                        "kind": "summary-window-refresh",
                        "status": "complete",
                        "windows": {},
                    },
                ),
                patch.object(health, "date", Day),
                patch.object(orchestration, "datetime", Clock),
                patch.object(refresh, "_today", return_value="2026-11-02"),
            ):
                report = orchestration.run(
                    orchestration.RefreshRequest(mode="general", include_granular=True, run_id="0"),
                    store_factory=factory,
                    login=lambda selected=source: selected,
                )
            for stream, count in expected.items():
                self.assertEqual(sum(kind == stream for kind, _ in source.calls), count)
                check = next(item for item in report["source_checks"] if item["scope"] == stream)
                self.assertEqual(check["checked"], count > 0)
                self.assertEqual(
                    check["outcome"],
                    "not_checked" if count == 0 else "not_ready" if count < 3 else "checked",
                )
                self.assertEqual(check["checked_at"] is not None, count > 0)
            stages = {item["stage"]: item["status"] for item in report["stages"]}
            self.assertEqual(stages["recent_body"], "idle")
            self.assertEqual(stages["manual_activity"], "complete")
            self.assertEqual(report["status"], "complete" if maximum == 250 else "partial")


def full_month_service(objects=None):
    import copy

    initial = calendar_store()
    queued_period(
        initial,
        "2026-10-01",
        "2026-10-31",
        [key for key in initial.objects if key.startswith("health/hrv/")],
    )
    client = HealthClient()
    client.objects = (
        copy.deepcopy(objects)
        if objects is not None
        else {
            key: {"Body": snap.data, "ContentType": "application/json"}
            for key, snap in initial.objects.items()
        }
    )
    return client, R2Store(client=client, bucket="synthetic")


class ScopedReadBudgetTest(unittest.TestCase):
    def test_twenty_prior_ordinary_reads_do_not_become_component_reads_and_helpers_share_one_cap(
        self,
    ):
        client, store = full_month_service()
        key = "health/hrv/2026/10/2026-10-01.json"
        for _ in range(20):
            store.get_snapshot(key, max_bytes=524288)
        result = refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(store.operations["get"], 406)
        self.assertEqual(result["component_operations"]["get"], 386)
        stage = store.new_stage()
        for _ in range(14):
            self.assertEqual(
                refresh.refresh_pending(stage, today="2026-11-01", timezone="UTC")["status"],
                "complete",
            )
        deferred = refresh.refresh_pending(stage, today="2026-11-01", timezone="UTC")
        self.assertEqual(deferred["status"], "bounded_pending")
        self.assertEqual(deferred["component_operations"]["get"], 400)
        self.assertEqual(stage.operations["get"], 14)
        self.assertIsNone(deferred["pending"])

    def test_known_bounded_correction_progress_resumes_and_new_sources_do_not_create_a_hold(self):
        import copy

        client, store = full_month_service()
        refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        next_client, next_store = full_month_service(client.objects)
        key = "health/hrv/2026/10/2026-10-31.json"
        source = json.loads(next_client.objects[key]["Body"])
        source["readings"][0]["hrv_ms"] = 69
        refresh.publish_source(next_store, key, canonical(source).encode(), "application/json")
        first = refresh.refresh_pending(
            next_store, today="2026-11-01", timezone="UTC", max_periods=1
        )
        self.assertEqual(first["status"], "bounded_pending")
        self.assertEqual(first["products_written"], 1)
        row = json.loads(next_client.objects[refresh.QUEUE_KEY]["Body"])["pending"]["month/2026-10"]
        self.assertEqual(row["phase"], "dirty")
        self.assertNotIn("inflight", row)
        self.assertEqual(len(row["completed"]), 1)
        self.assertTrue(row["validated_progress"])
        frozen = copy.deepcopy(next_client.objects)
        resume_client, resume_store = full_month_service(frozen)
        resumed = refresh.refresh_pending(
            resume_store, today="2026-11-01", timezone="UTC", max_periods=1
        )
        self.assertEqual(resumed["products_written"], 1)
        self.assertEqual(resumed["products_unchanged"], 1)
        self.assertNotIn(
            "month/2026-10", json.loads(resume_client.objects[refresh.QUEUE_KEY]["Body"])["pending"]
        )
        changed_client, changed_store = full_month_service(frozen)
        source["readings"][0]["hrv_ms"] = 70
        refresh.publish_source(changed_store, key, canonical(source).encode(), "application/json")
        new_row = json.loads(changed_client.objects[refresh.QUEUE_KEY]["Body"])["pending"][
            "month/2026-10"
        ]
        self.assertEqual(new_row["phase"], "dirty")
        self.assertNotIn("following_sources", new_row)
        self.assertNotIn("inflight", new_row)
        self.assertEqual(new_row["completed"], {})
        partial = refresh.refresh_pending(
            changed_store, today="2026-11-01", timezone="UTC", max_periods=1
        )
        self.assertEqual(partial["held_periods"], 0)
        final_client, final_store = full_month_service(changed_client.objects)
        final = refresh.refresh_pending(
            final_store, today="2026-11-01", timezone="UTC", max_periods=1
        )
        self.assertEqual(final["held_periods"], 0)
        self.assertNotIn(
            "month/2026-10", json.loads(final_client.objects[refresh.QUEUE_KEY]["Body"])["pending"]
        )
        self.assertLessEqual(final["component_operations"]["get"], 400)
