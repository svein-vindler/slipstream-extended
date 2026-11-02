"""Two bounded closing regressions through public APIs and synthetic storage."""

import copy
import hashlib
import json
import unittest

from test_night_baseline_remaining import full_month_service

from pipeline import night_baseline_refresh as refresh
from pipeline.night_products import canonical
from pipeline.r2_store import R2Store
from scripts.benchmark_health_reuse import HealthClient


class ClosingBranchesTest(unittest.TestCase):
    def test_explicit_verified_enqueue_supersedes_only_known_partial_progress(self):
        client, store = full_month_service()
        refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        client, store = full_month_service(client.objects)
        key = "health/hrv/2026/10/2026-10-31.json"
        source = json.loads(client.objects[key]["Body"])
        source["readings"][0]["hrv_ms"] = 69
        refresh.publish_source(store, key, canonical(source).encode(), "application/json")
        first = refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(first["products_written"], 1)
        frozen = copy.deepcopy(client.objects)
        client, store = full_month_service(frozen)
        source["readings"][0]["hrv_ms"] = 70
        # A separate, already verified canonical publisher changed the source.
        client.objects[key]["Body"] = canonical(source).encode()
        snapshot = store.get_snapshot(key, max_bytes=524288)
        proof = {
            "key": key,
            "revision": snapshot.revision,
            "sha256": hashlib.sha256(snapshot.data).hexdigest(),
        }
        row = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])["pending"]["month/2026-10"]
        self.assertTrue(row["validated_progress"])
        self.assertEqual(len(row["completed"]), 1)
        refresh.enqueue_verified_sources(store, [proof])
        row = json.loads(client.objects[refresh.QUEUE_KEY]["Body"])["pending"]["month/2026-10"]
        self.assertNotIn("validated_progress", row)
        self.assertEqual(row["completed"], {})
        result = refresh.refresh_pending(store, today="2026-11-01", timezone="UTC", max_periods=1)
        self.assertEqual(result["status"], "bounded_pending")
        self.assertEqual(result["products_written"], 1)
        self.assertEqual(result["held_periods"], 0)
        for unknown in ("source", "inflight"):
            blocked_client, blocked_store = full_month_service(frozen)
            queue = json.loads(blocked_client.objects[refresh.QUEUE_KEY]["Body"])
            row = queue["pending"]["month/2026-10"]
            if unknown == "source":
                row["sources"][key] = {"state": "intent", "sha256": proof["sha256"]}
            else:
                row["inflight"] = {"key": "synthetic-unknown", "sha256": proof["sha256"]}
            blocked_client.objects[refresh.QUEUE_KEY]["Body"] = canonical(queue).encode()
            snapshot = blocked_store.get_snapshot(key, max_bytes=524288)
            current = {
                "key": key,
                "revision": snapshot.revision,
                "sha256": hashlib.sha256(snapshot.data).hexdigest(),
            }
            before = copy.deepcopy(blocked_client.objects)
            with self.assertRaisesRegex(refresh.BaselineRefreshStop, "old intent"):
                refresh.enqueue_verified_sources(blocked_store, [current])
            self.assertEqual(blocked_client.objects, before)
            self.assertEqual(blocked_store.operations["put"], 0)

    def test_inventory_lists_are_reserved_before_publication_intent(self):
        class PagedInventory(HealthClient):
            def __init__(self, pages):
                super().__init__()
                self.pages = pages

            def get_paginator(self, name):
                ordinary = super().get_paginator(name)
                client = self

                class Pages:
                    def paginate(self, **kwargs):
                        if kwargs.get("Prefix", ""):
                            yield from ordinary.paginate(**kwargs)
                            return
                        rows = [
                            {
                                "Key": key,
                                "Size": len(value["Body"]),
                                "ETag": client._etag(value["Body"]),
                            }
                            for key, value in sorted(client.objects.items())
                        ]
                        assert len(rows) >= client.pages
                        for index in range(client.pages):
                            page = (
                                rows[index : index + 1]
                                if index < client.pages - 1
                                else rows[index:]
                            )
                            truncated = index < client.pages - 1
                            client.counts["list"] += 1
                            client.prefixes.append("")
                            result = {
                                "Contents": page,
                                "IsTruncated": truncated,
                                "KeyCount": len(page),
                                "MaxKeys": 1000,
                                "Prefix": "",
                            }
                            if truncated:
                                result["NextContinuationToken"] = str(index + 1)
                            yield result

                return Pages()

        seed, _ = full_month_service()
        # Eight inventory pages now fit because two month fences replace 62
        # daily fences. A 62-page inventory still leaves too little reserve.
        for pages, status, products, lists in (
            (8, "complete", 2, 41),
            (62, "bounded_pending", 0, 93),
        ):
            with self.subTest(inventory_pages=pages):
                client = PagedInventory(pages)
                client.objects = copy.deepcopy(seed.objects)
                store = R2Store(client=client, bucket="synthetic")
                original_queue = client.objects[refresh.QUEUE_KEY]["Body"]
                result = refresh.refresh_pending(
                    store, today="2026-11-01", timezone="UTC", max_periods=1
                )
                self.assertEqual(result["status"], status)
                self.assertEqual(result["products_written"], products)
                self.assertEqual(result["component_operations"]["list"], lists)
                self.assertEqual(client.prefixes.count(""), pages)
                if not products:
                    self.assertEqual(store.operations["put"], 0)
                    self.assertEqual(client.objects[refresh.QUEUE_KEY]["Body"], original_queue)
                    row = json.loads(original_queue)["pending"]["month/2026-10"]
                    self.assertEqual(row["phase"], "dirty")
                    self.assertNotIn("inflight", row)
                print(
                    json.dumps(
                        {
                            "inventory_reservation": pages,
                            "component_operations": result["component_operations"],
                            "products": products,
                            "status": status,
                        }
                    )
                )
