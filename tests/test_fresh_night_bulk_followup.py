"""Fresh current night through the real SDK; no provider/network is used."""
import json
from datetime import UTC, datetime

from test_history_baseline_integration import sdk_store
from test_recent_night_production import Garmin, NightServer, clock
from test_rolling_reference_sources import seeded

from pipeline import latest_night, night_followup
from pipeline.fresh_night_references import refresh_current_night
from pipeline.night_baseline_refresh import QUEUE_KEY
from pipeline.night_products import normalize_args, storage_key


def qualify(store, day, now):
    # Use genuine ordinary source/check writers, never fabricated source ACKs.
    from test_recent_night_production import GeneralGarmin

    from pipeline.health_sync import finalize_days, publish_completed_nights, store_day
    garmin = GeneralGarmin()
    records = {}
    for stream in ("sleep", "hrv"):
        raw = getattr(garmin, "get_" + ("sleep_data" if stream == "sleep" else "hrv_data"))(day)
        records[stream] = [store_day(store, stream, day, raw)]
        finalize_days(store, stream, records[stream])
    publish_completed_nights(store, records)
    latest_night.collect_heart_rate(store, garmin, [day], timezone="Europe/Oslo", now=now)
    return garmin


def test_cold365_all_ten_same_pool_and_no_old_queue(tmp_path, monkeypatch):
    day = "2026-10-25"
    clock(monkeypatch, day)
    now = datetime(2026, 10, 25, 7, tzinfo=UTC)
    seeded_server, _ = seeded("2025-10-26", 365)
    server = NightServer()
    server.objects = seeded_server.objects
    server.metadata = seeded_server.metadata
    store = sdk_store(server)
    garmin = qualify(store, day, now)
    source_calls = list(garmin.calls)
    # A genuine older calendar row is left untouched by the selected producer.
    queue = json.loads(store.get(QUEUE_KEY))
    old = {key: value for key, value in queue["pending"].items()
           if not value.get("rolling_window")}
    before = dict(store.operations)
    result = refresh_current_night(store, day, now=now, folder=tmp_path / "fresh")
    assert result["status"] == "complete" and result["selected_remaining"] == 0
    assert garmin.calls == source_calls
    assert result["producer_publication_complete"] is True
    assert result["independent_bulk_verified"] is False and result["wholeDataVerified"] is False
    assert result["collection_phases"] > 1 and result["source_calls"] == 0
    for width in (7,28,30,90,365):
        from datetime import date, timedelta
        first = (date.fromisoformat(day)-timedelta(days=width-1)).isoformat()
        for channel in ("heart_rate", "hrv"):
            key = storage_key(normalize_args(period="custom", start_date=first,end_date=day,channel=channel))
            assert key in server.objects
            value = json.loads(store.get_snapshot(key,max_bytes=262144).data)
            assert value["nights_requested"] == width
    after = json.loads(store.get(QUEUE_KEY))
    assert all(after["pending"].get(key) == value for key,value in old.items())
    account = json.loads((tmp_path / "fresh/workset/account/ACCOUNT.json").read_bytes())
    assert all(account["carried"]["ordinary_before"][key] >= value for key,value in before.items())
    assert store._writes == store._bucket_budget.writes
    terminals = [json.loads(path.read_bytes()) for path in (tmp_path / "fresh/workset/stages").glob("*/TERMINAL.json")]
    assert all(row["counts"]["get"] <= 400 and row["ownedSDKClosed"] for row in terminals if row["phase"] != "build")
    pubs = list((tmp_path / "fresh/workset/publications").glob("*/batch-*/RESULT.json"))
    assert len(pubs) <= 2
    print(json.dumps({"producer_targets":10,"collection_phases":result["collection_phases"],
        "monotone_account":account["context"],"ordinary_store":store.operations}))


def test_not_ready_or_historical_never_opens_bulk(tmp_path, monkeypatch):
    clock(monkeypatch)
    store = sdk_store(NightServer())
    now = datetime(2026,9,30,7,tzinfo=UTC)
    assert refresh_current_night(store,"2026-09-29",now=now,folder=tmp_path/"old")["status"] == "not_run_not_current"
    assert refresh_current_night(store,"2026-09-30",now=now,folder=tmp_path/"pending")["status"] == "bounded_pending"
    assert not (tmp_path/"old").exists() and not (tmp_path/"pending").exists()


def test_scheduled_followup_runs_after_existing_operation(monkeypatch):
    day="2026-09-30"
    clock(monkeypatch,day)
    monkeypatch.setenv("GITHUB_EVENT_NAME","schedule")
    server,garmin=NightServer(),Garmin()
    store=sdk_store(server)
    events=[]
    # The seam/order is tested separately from the expensive real engine above.
    monkeypatch.setattr("pipeline.fresh_night_references.refresh_current_night",
        lambda *a,**kw: events.append("reference") or {"status":"complete","selected_remaining":0})
    def operation(**kwargs):
        assert kwargs["defer_references"] is True
        events.append("source-operation")
        return {"status":"complete"}
    night_followup.run_command(operation,defer_references=True,store_factory=lambda store=store:store,login=lambda garmin=garmin:garmin,
        now=datetime(2026,9,30,7,tzinfo=UTC))
    assert events == ["source-operation","reference"]


def test_legacy_defaults_are_unchanged():
    from pipeline.rolling_night_references import window_selection
    assert window_selection(None) == (28,30,90)


def test_targeted_and_general_current_night_reach_explicit_fresh_seam(monkeypatch):
    from test_recent_night_production import GeneralGarmin

    from pipeline import recent_health

    day="2026-09-30"
    clock(monkeypatch,day)
    endpoints=[]
    monkeypatch.setattr("pipeline.fresh_night_references.refresh_current_night",
        lambda store,wake_date,**kw: endpoints.append(wake_date) or {"status":"complete","selected_remaining":0})
    latest_night.run(run_id="101",wake_date=day,store=sdk_store(NightServer()),garmin=Garmin())
    recent_health.run(days=3,store=sdk_store(NightServer()),garmin=GeneralGarmin(),night_references=True)
    assert endpoints == [day,day]


def test_manual_backfill_keeps_existing_source_guard(monkeypatch):
    monkeypatch.setenv("GITHUB_EVENT_NAME","workflow_dispatch")
    def forbidden():
        raise AssertionError("Manual historical entry opened fresh sources")
    assert night_followup.run_command(lambda **kw: "existing",store_factory=forbidden,login=forbidden) == "existing"


def test_denied_source_check_stops_before_workset(tmp_path, monkeypatch):
    import pytest
    from botocore.exceptions import ClientError

    clock(monkeypatch)
    server=NightServer()
    server.denied_key="refresh/checks/v1/health/sleep/2026-09-30.json"
    server.denial="denied"
    with pytest.raises(ClientError):
        refresh_current_night(sdk_store(server),"2026-09-30",
            now=datetime(2026,9,30,7,tzinfo=UTC),folder=tmp_path/"denied")
    assert not (tmp_path/"denied").exists()


def test_scheduled_backfill_sources_have_only_one_reference_producer(monkeypatch):
    from pipeline import health_detail_backfill, hrv_backfill

    clock(monkeypatch)
    monkeypatch.setenv("GITHUB_EVENT_NAME","schedule")
    def forbidden(*args,**kwargs):
        raise AssertionError("Scheduled legacy producer ran before whole-workset seal")
    monkeypatch.setattr("pipeline.night_baseline_refresh.refresh_pending",forbidden)
    observed=[]
    monkeypatch.setattr("pipeline.fresh_night_references.refresh_current_night",
        lambda *a,**kw: observed.append(True) or {"status":"complete","selected_remaining":0})
    for operation in (health_detail_backfill.run,hrv_backfill.run):
        server,garmin=NightServer(),Garmin()
        store=sdk_store(server)
        store.put("summary/health_daily.csv",b"Date,Sleep Seconds,HRV Last Night Average,Weight KG\n2026-09-30,25200,44,\n","text/csv")
        night_followup.run_command(operation,defer_references=True,store_factory=lambda store=store:store,login=lambda garmin=garmin:garmin,
            now=datetime(2026,9,30,7,tzinfo=UTC))
    assert len(observed) == 2


def test_pending_reference_is_not_successful_no_work():
    from pipeline.actions_receipt import _reference_outcome
    assert _reference_outcome({"baseline_refresh":{"status":"bounded_pending","selected_remaining":10}},"no_work") == "partial"
