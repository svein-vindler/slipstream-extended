"""Provisional C2-on-C5 proof/CSV seam; synthetic SDK boundaries only."""
import csv
import io
import json
from datetime import datetime, timezone

import pytest
from test_summary_publication import ConditionalClient
from test_summary_recovery_flows import Garmin

from pipeline import refresh, refresh_summaries, summary_export, summary_restore
from pipeline.r2_store import R2Store
from pipeline.summary_publication import SummaryUnverified

LEGACY = ["Activity ID", "Activity Date", "Activity Name", "Activity Type", "Raw Sport",
          "Distance", "Filename", "Moving Time", "Elapsed Time", "Max Heart Rate",
          "Average Heart Rate", "Elevation Gain", "Average Speed", "Average Watts", "Calories", "Source"]


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2027, 1, 1, 12, tzinfo=timezone.utc)


class CalendarGarmin(Garmin):
    def connectapi(self, path, params=None):
        if path == self.garmin_connect_activities:
            self.calls.append((path, params))
            return [{"activityId": 73, "startTimeGMT": "2026-12-31 23:30:00",
                     "startTimeLocal": "2027-01-01 00:30:00", "activityName": "Synthetic update",
                     "activityType": {"typeKey": "running"}, "duration": 2000, "distance": 5000}
                    ] if int(params["start"]) == 0 else []
        return super().connectapi(path, params)


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    monkeypatch.setenv("TRAINING_CONTEXT_ENABLED", "false")
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket="synthetic")
    return tmp_path, client, factory


def csv_pair(root, columns):
    root.mkdir()
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(LEGACY + (["Activity Local Date"] if columns == 17 else []))
    writer.writerow(["garmin-73", "2026-12-31 23:30:00", "Synthetic original", "Run", "running",
                     "5", "", "1800", "1900", "", "", "", "", "", "", "garmin"]
                    + (["2027-01-01"] if columns == 17 else []))
    (root / "activities.csv").write_bytes(output.getvalue().encode())
    (root / "health_daily.csv").write_bytes(b"Date,Source\n")


@pytest.mark.parametrize("columns", [16, 17])
@pytest.mark.parametrize("root_kind", ["legacy", "marked"])
def test_actual_normal_restore_merge_publish_preserves_both_csv_forms_and_local_calendar(scenario, columns, root_kind):
    root, client, factory = scenario
    seed = root / "seed"
    csv_pair(seed, columns)
    initial = (seed / "activities.csv").read_bytes()
    if root_kind == "legacy":
        objects, _ = summary_export.build_summary_exports(str(seed))
        for obj in objects:
            factory().put(obj["key"], obj["data"], obj["content_type"], encoding=obj["encoding"])
    else:
        summary_export.run(str(seed), store=factory())
    archived = {key: item["Body"] for key, item in client.objects.items()
                if "/snapshots/" in key or "/generations/" in key}
    baseline = root / "baseline"
    summary_restore.restore_summaries(str(baseline), factory())
    assert (baseline / "activities.csv").read_bytes() == initial
    target, sdk = root / "normal", CalendarGarmin()
    for _ in range(2):
        result = refresh.run(refresh.RefreshRequest(), data_dir=str(target), store_factory=factory, login=lambda: sdk)
        assert result["status"] != "failed"
        rows = list(csv.DictReader(io.StringIO((target / "activities.csv").read_text(encoding="utf-8"))))
        assert len(rows) == 1
        row = rows[0]
        assert (row["Activity ID"], row["Activity Date"], row["Activity Local Date"]) == (
            "garmin-73", "2026-12-31 23:30:00", "2027-01-01")
        assert (row["Moving Time"], row["Elapsed Time"]) == ("1800", "2000")
        assert len(row) == 17
        manifest = json.loads(client.objects["summary/manifest.json"]["Body"])
        assert manifest["publication_version"] == 2
        if _ == 0:
            generation = manifest["generation_id"]
        else:
            assert manifest["generation_id"] == generation
        reader = root / "reader"
        summary_restore.restore_summaries(str(reader), factory())
        assert (reader / "activities.csv").read_bytes() == (target / "activities.csv").read_bytes()
        assert all(client.objects[key]["Body"] == raw for key, raw in archived.items())


@pytest.mark.parametrize("columns", [16, 17])
def test_csv_compatibility_does_not_relax_unknown_manifest_schema(scenario, columns):
    root, client, factory = scenario
    seed = root / "seed"
    csv_pair(seed, columns)
    summary_export.run(str(seed), store=factory())
    manifest = json.loads(client.objects["summary/manifest.json"]["Body"])
    manifest["publication_version"] = 99
    bad_root = json.dumps(manifest).encode()
    client.objects["summary/manifest.json"]["Body"] = bad_root
    sdk = CalendarGarmin()
    puts = client.counts["put"]
    with pytest.raises(SummaryUnverified):
        refresh.run(refresh.RefreshRequest(), data_dir=str(root / "normal"), store_factory=factory, login=lambda: sdk)
    assert sdk.calls == []
    assert client.counts["put"] == puts
    assert client.objects["summary/manifest.json"]["Body"] == bad_root
