"""Offline comparison of separate versus shared refresh inventory guards."""
from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.r2_store import R2Store  # noqa: E402
from scripts.benchmark_summary_reuse import SummaryClient  # noqa: E402


class BudgetClient(SummaryClient):
    """Synthetic S3 pagination; no credentials, network, clocks or private data."""

    def __init__(self, objects=2001):
        super().__init__()
        self.objects = {f"synthetic/history/{i:06}": {"Body": b"synthetic"}
                        for i in range(objects)}

    def get_paginator(self, name):
        assert name == "list_objects_v2"

        def pages(**kwargs):
            contents = [{"Key": key, "Size": len(value["Body"])}
                        for key, value in sorted(self.objects.items())
                        if key.startswith(kwargs.get("Prefix", ""))]
            for offset in range(0, max(1, len(contents)), 1000):
                self.counts["list"] += 1
                yield {"Contents": contents[offset:offset + 1000]}
        return SimpleNamespace(paginate=pages)


def compare(objects=15001):
    observations = []
    outputs = []
    for shared in (False, True):
        client = BudgetClient(objects)
        store = R2Store(client=client, bucket="synthetic-refresh")
        with redirect_stderr(io.StringIO()):
            # Three writing stages, followed by index reuse and diagnostics.
            for index in range(3):
                store = store.new_stage() if shared else R2Store(client=client, bucket=store.bucket)
                store.put(f"synthetic/stage/{index}", b"changed", "text/plain")
            store = store.new_stage() if shared else R2Store(client=client, bucket=store.bucket)
            store.put("synthetic/diagnostics", b"report", "text/plain")
        observations.append(dict(client.counts))
        outputs.append(client.objects)
    assert outputs[0] == outputs[1]
    return {"synthetic_objects": objects, "provider_calls": 0, "outputs_identical": True,
            "separate": observations[0], "shared": observations[1]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--objects", type=int, default=15001)
    args = parser.parse_args()
    if not 0 <= args.objects <= 100000:
        parser.error("Choose 0 to 100000 synthetic objects")
    print(json.dumps(compare(args.objects), indent=2))


if __name__ == "__main__":
    main()
