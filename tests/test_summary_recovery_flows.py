"""Normal pipeline flows against synthetic external Garmin and conditional S3."""
import json
from datetime import datetime, timezone

import pytest
from test_summary_publication import ConditionalClient, source

from pipeline import refresh, refresh_summaries, summary_export, summary_restore
from pipeline.r2_store import R2Store
from pipeline.summary_publication import SummaryConflict
from scripts.benchmark_summary_windows import SummaryGarmin


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 13, 12, tzinfo=timezone.utc)


class Garmin(SummaryGarmin):
    def __init__(self, hook=None):
        super().__init__()
        self.hook = hook

    def connectapi(self, path, params=None):
        if self.hook is not None:
            hook, self.hook = self.hook, None
            hook()
        return super().connectapi(path, params)

    def get_hrv_data(self, day):
        return self.connectapi("hrv-day", {"day": day})


@pytest.fixture
def scenario(tmp_path, monkeypatch):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    monkeypatch.setenv("TRAINING_CONTEXT_ENABLED", "false")
    monkeypatch.setattr("pipeline.sources.garmin_health.time.sleep", lambda seconds: None)
    data, other = tmp_path / "data", tmp_path / "other"
    source(data)
    source(other)
    client = ConditionalClient()
    def factory():
        return R2Store(client=client, bucket='synthetic')
    summary_export.run(str(data), store=factory())
    def competing_commit():
        (other / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic B\n")
        summary_export.run(str(other), store=factory())
    return data, other, client, factory, competing_commit


def test_direct_summary_carries_its_prefetch_base_and_rejects_competing_commit(scenario):
    data, other, client, factory, commit = scenario
    sdk = Garmin(commit)
    with pytest.raises(SummaryConflict):
        refresh_summaries.run(str(data), store=factory(), client=sdk, request_pause=0)
    reader = other / "verify"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (other / "activities.csv").read_bytes()
    assert not any(k.startswith("refresh/checks/") for k in client.objects)



@pytest.mark.parametrize("route", ["historical", "activity", "direct_activity", "general_login_race"])
def test_all_normal_callers_keep_the_captured_base_across_source_stages(scenario, tmp_path, route):
    from pipeline import latest_activity
    data, other, client, factory, commit = scenario
    sdk = Garmin(commit)
    if route == "direct_activity":
        def operation():
            return latest_activity.run(run_id='0', data_dir=str(data), store=factory(), garmin=sdk)
    else:
        request = (refresh.RefreshRequest(health_start="2026-10-12", health_end="2026-10-13")
                   if route == "historical" else refresh.RefreshRequest(mode="activity")
                   if route == "activity" else refresh.RefreshRequest())
        def login():
            if route == "general_login_race":
                sdk.hook = None
                commit()
            return sdk
        def operation():
            return refresh.run(request, data_dir=str(tmp_path / route), store_factory=factory, login=login)
    with pytest.raises(SummaryConflict):
        operation()
    target = other / "verify"
    summary_restore.restore_summaries(str(target), factory())
    assert (target / "activities.csv").read_bytes() == (other / "activities.csv").read_bytes()
    assert not any(k.startswith("refresh/checks/") for k in client.objects)


def test_direct_summary_keeps_its_existing_verified_empty_bootstrap(tmp_path, monkeypatch):
    monkeypatch.setattr(summary_export, "datetime", Clock)
    monkeypatch.setattr(refresh_summaries, "datetime", Clock)
    client, sdk = ConditionalClient(), Garmin()
    store = R2Store(client=client, bucket="synthetic")
    result = refresh_summaries.run(str(tmp_path / "empty"), store=store, client=sdk, request_pause=0)
    assert result["status"] == "complete"
    assert json.loads(client.objects["summary/manifest.json"]["Body"])["publication_version"] == 2
    reader = tmp_path / "reader"
    summary_restore.restore_summaries(str(reader), store)
    assert b"garmin-" in (reader / "activities.csv").read_bytes()


@pytest.mark.parametrize("cut", [None, "archive", "commit"])
def test_fresh_normal_runner_recovers_committed_pair_after_export_interruption(scenario, tmp_path, monkeypatch, cut):
    import csv
    data, _, client, factory, _ = scenario
    if cut:
        (data / "activities.csv").write_bytes(b"Activity ID,Activity Name\ngarmin-1,Synthetic changed\n")
        original = client.put_object
        def interrupt(**kwargs):
            key = kwargs["Key"]
            if ((cut == "archive" and key.startswith("summary/generations/") and key.endswith("/activities.csv"))
                    or cut == "commit" and key == "summary/manifest.json"):
                raise RuntimeError("Synthetic publication interrupted")
            return original(**kwargs)
        monkeypatch.setattr(client, "put_object", interrupt)
        with pytest.raises(RuntimeError, match="Synthetic publication interrupted"):
            summary_export.run(str(data), store=factory())
        monkeypatch.setattr(client, "put_object", original)
    sdk = Garmin()
    target = tmp_path / "cold-normal"
    result = refresh.run(refresh.RefreshRequest(), data_dir=str(target), store_factory=factory, login=lambda: sdk)
    assert result["status"] != "failed"
    assert result["stages"][1]["stage"] == "refresh_summaries"
    with (target / "activities.csv").open(encoding="utf-8", newline="") as handle:
        assert any(row["Activity ID"] == "garmin-1" for row in csv.DictReader(handle))
    summary_restore.restore_summaries(str(tmp_path / "verified"), factory())
    assert (tmp_path / "verified/activities.csv").read_bytes() == (target / "activities.csv").read_bytes()



@pytest.mark.parametrize("route", ["general", "scheduled", "historical", "activity", "direct_activity"])
def test_positive_normal_routes_publish_a_pair_readable_by_the_common_reader(scenario, tmp_path, route):
    from pipeline import latest_activity
    data, _, client, factory, _ = scenario
    sdk = Garmin()
    if route == "direct_activity":
        result = latest_activity.run(run_id="0", data_dir=str(data), store=factory(), garmin=sdk)
        assert result["summary_updated"] is True
        target = data
    else:
        target = tmp_path / route
        request = (refresh.RefreshRequest(scheduled=True) if route == "scheduled" else
                   refresh.RefreshRequest(health_start="2026-10-12", health_end="2026-10-13")
                   if route == "historical" else refresh.RefreshRequest(mode="activity")
                   if route == "activity" else refresh.RefreshRequest())
        result = refresh.run(request, data_dir=str(target), store_factory=factory, login=lambda: sdk)
        assert result["status"] != "failed"
    reader = tmp_path / "common-reader"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (target / "activities.csv").read_bytes()
    assert (reader / "health_daily.csv").read_bytes() == (target / "health_daily.csv").read_bytes()


@pytest.mark.parametrize("route", ["shared", "direct_activity"])
def test_missing_target_is_not_an_implicit_full_export_or_shared_bootstrap(tmp_path, route):
    from pipeline import latest_activity
    from pipeline.summary_publication import SummaryUnverified
    client, sdk = ConditionalClient(), Garmin()
    def factory():
        return R2Store(client=client, bucket="synthetic")
    with pytest.raises(SummaryUnverified):
        if route == "shared":
            refresh.run(refresh.RefreshRequest(), data_dir=str(tmp_path), store_factory=factory, login=lambda: sdk)
        else:
            latest_activity.run(run_id="0", data_dir=str(tmp_path), store=factory(), garmin=sdk)
    assert sdk.calls == []
    assert not any(key.startswith("summary/") for key in client.objects)


@pytest.mark.parametrize("cut", range(1, 5))
@pytest.mark.parametrize("after", [False, True])
def test_each_source_receipt_cut_retains_the_commit_and_next_cold_normal_run(scenario, tmp_path, monkeypatch, cut, after):
    data, _, client, factory, _ = scenario
    sdk, original, writes = Garmin(), client.put_object, []
    def interrupt(**kwargs):
        checkpoint = kwargs["Key"].startswith("refresh/checks/v1/summary/")
        if checkpoint:
            writes.append(kwargs["Key"])
        if checkpoint and len(writes) == cut and not after:
            raise TimeoutError("synthetic source receipt cut")
        result = original(**kwargs)
        if checkpoint and len(writes) == cut and after:
            raise TimeoutError("synthetic source receipt cut")
        return result
    monkeypatch.setattr(client, "put_object", interrupt)
    with pytest.raises(TimeoutError, match="synthetic source receipt cut"):
        refresh_summaries.run(str(data), store=factory(), client=sdk, request_pause=0)
    assert len(writes) == cut
    monkeypatch.setattr(client, "put_object", original)
    reader = tmp_path / "committed-before-retry"
    summary_restore.restore_summaries(str(reader), factory())
    assert (reader / "activities.csv").read_bytes() == (data / "activities.csv").read_bytes()
    cold = tmp_path / "cold-retry"
    result = refresh.run(refresh.RefreshRequest(), data_dir=str(cold), store_factory=factory, login=lambda: Garmin())
    assert result["status"] != "failed"
    summary_restore.restore_summaries(str(tmp_path / "final-reader"), factory())
    assert (tmp_path / "final-reader/activities.csv").read_bytes() == (cold / "activities.csv").read_bytes()
