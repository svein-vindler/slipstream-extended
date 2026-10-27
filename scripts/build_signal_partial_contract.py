"""Portable synthetic repeated collection -> stored partial-source MCP fixture.

The generator uses only stdlib infrastructure and the repository's normal runtime
pipeline dependencies. No test modules, account, network or existing fixture is
needed. Optional source layers let reviewers bind an unassembled candidate exactly.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import importlib.abc
import importlib.util
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

DAY = "2026-10-25"
KEY = "health/signals/v1/2026/10/2026-10-25.json"
T1, T2 = 1792886400000, 1792886520000
OLD_REVISION = "f8cc675055a4edbdcf226f4f9311cfb37fcefbd1e49a7a7e11e80e75f9d8f377"
NEW_REVISION = "9b95d332cd61d002750a3cfcf91cef4830b700cb72c9d2aa919538f941c7fba4"


class SourceLayers(importlib.abc.MetaPathFinder):
    """Resolve project modules exclusively from explicit, ordered source roots."""

    def __init__(self, roots):
        self.roots = roots

    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] not in {"pipeline", "scripts"}:
            return None
        relative = Path(*fullname.split("."))
        for root in self.roots:
            module = root / relative.with_suffix(".py")
            package = root / relative / "__init__.py"
            if module.is_file():
                return importlib.util.spec_from_file_location(fullname, module)
            if package.is_file():
                return importlib.util.spec_from_file_location(
                    fullname, package, submodule_search_locations=[str(root / relative)])
        directories = [str(root / relative) for root in self.roots
                       if (root / relative).is_dir()]
        if directories:
            spec = importlib.util.spec_from_loader(fullname, loader=None, is_package=True)
            spec.submodule_search_locations = directories
            return spec
        raise ImportError("Module absent from selected repository sources: " + fullname)


def source_trace(roots):
    result = {}
    for name, module in sorted(sys.modules.items()):
        if name.split(".")[0] not in {"pipeline", "scripts"}:
            continue
        filename = getattr(module, "__file__", None)
        if filename is None:
            continue
        path = Path(filename).resolve()
        layer = next((index for index, root in enumerate(roots)
                      if path.is_relative_to(root)), None)
        if layer is None:
            raise RuntimeError("Project module escaped selected source roots")
        result[name] = {"layer": layer, "path": path.relative_to(roots[layer]).as_posix(),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    if "pytest" in sys.modules or any(name.startswith("tests.") for name in sys.modules):
        raise RuntimeError("Generator must not depend on test modules")
    return result


def build_case(*, unknown_revision=False):
    from pipeline import health_history_index
    from pipeline.granular import gzip_json
    from pipeline.r2_store import R2Store
    from pipeline.refresh_summaries import run
    from scripts.benchmark_summary_windows import HealthClient, SummaryGarmin
    from scripts.build_night_signals_contract import build_case as base_case

    class PartialSource(SummaryGarmin):
        partial = False
        value = 80

        def get_heart_rates(self, day):
            super().get_heart_rates(day)
            points = [[T1, self.value], [T2, None if self.partial else 48]] if day == DAY else []
            return {"calendarDate": day, "heartRateValues": points}

    # Generate the normal A contract from its real source/storage writers first.
    class SyntheticClock(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = cls(2026, 10, 25, 12, tzinfo=UTC)
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    # Only the clock is synthetic; indexes still use the actual storage writer.
    with patch.object(health_history_index, "datetime", SyntheticClock):
        fixture = base_case()
    storage, source = HealthClient(), PartialSource()
    store = R2Store(client=storage, bucket="synthetic-partial")
    now = datetime(2026, 10, 25, 12, tzinfo=UTC)
    retained_revision = None if unknown_revision else OLD_REVISION
    expected_value = 82 if unknown_revision else 80
    second_raw = {"date": DAY, "heartRateValues": [[T1, 82], [T2, None]]}
    second_revision = hashlib.sha256(json.dumps(second_raw, ensure_ascii=False,
                                                separators=(",", ":")).encode()).hexdigest()
    incoming_revision = second_revision if unknown_revision else NEW_REVISION
    if unknown_revision:
        legacy = {"schema_version": 1, "source": "garmin", "date": DAY, "channels": {
            "heart_rate": {"status": "available", "unit": "bpm", "readings": [
                {"timestamp": T2, "value": 48, "time_basis": "utc",
                 "timestamp_unit": "epoch_milliseconds"}]}}}
        store.put(KEY, gzip_json(legacy), "application/json", encoding="gzip")
        source.partial = True

    def channel():
        return json.loads(gzip.decompress(storage.objects[KEY]["Body"]))["channels"]["heart_rate"]

    with TemporaryDirectory(prefix="signal-partial-") as directory, \
            redirect_stdout(StringIO()), redirect_stderr(StringIO()):
        first = run(directory, store=store, client=source, now=now, request_pause=0)
        first_revision = channel()["source_sha256"]
        assert first_revision == (NEW_REVISION if unknown_revision else OLD_REVISION)
        if unknown_revision:
            assert channel()["readings"][1]["source_revision"] is None
            assert "retained_revision_unknown" in channel()["quality_flags"]
            source.value = 82
        source.partial = True
        storage.reset()
        second = run(directory, store=store, client=source,
                     now=now + timedelta(minutes=1), request_pause=0)
        partial = channel()
        assert [(point["timestamp"], point["value"]) for point in partial["readings"]] == [(T1, expected_value), (T2, 48)]
        assert partial["source_sha256"] == incoming_revision
        assert [(point["source_revision"], point["observation_origin"])
                for point in partial["readings"]] == [(incoming_revision, "incoming"), (retained_revision, "retained")]
        if unknown_revision:
            assert incoming_revision != first_revision
            assert "retained_revision_unknown" in partial["quality_flags"]
            assert partial["readings"][1]["source_revision"] is None
        assert partial["missing_value_count"] == 1
        assert partial["source_composition"]["incoming_valid_points"] == 1
        assert partial["source_composition"]["retained_valid_points"] == 1
        assert partial["source_composition"]["completeness"] == "unknown"
        assert {"missing_values", "partial_source", "retained_previous_observations",
                "source_completeness_unknown"}.issubset(partial["quality_flags"])
        previous = storage.objects[KEY]["Body"]
        storage.reset()
        third = run(directory, store=store, client=source,
                    now=now + timedelta(minutes=2), request_pause=0)
        assert storage.objects[KEY]["Body"] == previous and KEY not in storage.writes
        assert third["signals"]["put_attempts"] == 0

    fixture["objects"] = {key: value for key, value in fixture["objects"].items()
                          if not key.startswith("health/signals/")}
    for day in ("2026-10-24", DAY):
        key = f"health/signals/v1/2026/10/{day}.json"
        value = storage.objects[key]
        fixture["objects"][key] = {"base64": base64.b64encode(value["Body"]).decode("ascii"),
                                   "content_type": "application/json", "content_encoding": "gzip"}
    fixture["expected"].update(
        heart_rate_values=[expected_value, 48], heart_rate_minimum_10m_median=None,
        source_missing_value_count=1, retained_valid_points=1, incoming_valid_points=1,
        retained_source_revision=retained_revision, incoming_source_revision=incoming_revision,
        source_completeness="unknown")
    if unknown_revision:
        fixture["expected"].update(first_partial_source_revision=first_revision,
                                   retained_revision_unknown=True)
    for key in ("first_10m_window_median", "body_battery_values", "outside_hr_value"):
        fixture["expected"].pop(key, None)
    fixture["collection"] = {"first": first["health_diagnostics"],
                             "partial": second["health_diagnostics"], "repeat": third["signals"]}
    return fixture


def main():
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=repo)
    parser.add_argument("--source-overlay", type=Path, action="append", default=[],
                        help="Highest-priority overlay first; repeat as needed")
    parser.add_argument("--unknown-revision", action="store_true")
    parser.add_argument("--output", type=Path,
                        default=None)
    parser.add_argument("--proof", type=Path)
    args = parser.parse_args()
    roots = [root.resolve(strict=True) for root in [*args.source_overlay, args.source_root]]
    sys.meta_path.insert(0, SourceLayers(roots))
    if args.output is None:
        filename = "night-signals-unknown-revision.json" if args.unknown_revision else "night-signals-partial-null.json"
        args.output = repo / "worker/test-runtime/generated" / filename
    payload = build_case(unknown_revision=args.unknown_revision)
    trace = source_trace(roots)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
    args.output.write_bytes(data)
    proof = {"status": "PASS", "synthetic": True, "test_modules_loaded": False,
             "output_sha256": hashlib.sha256(data).hexdigest(), "output_bytes": len(data),
             "objects": len(payload["objects"]), "source_modules": trace,
             "collection": payload["collection"]}
    if args.proof:
        args.proof.parent.mkdir(parents=True, exist_ok=True)
        args.proof.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in proof.items() if key != "source_modules"}))


if __name__ == "__main__":
    main()
