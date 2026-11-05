"""An explicit finite experiment keeps the ordinary source ceiling unchanged."""

from datetime import datetime, timezone

import pytest
from test_history_backfill import native_runner
from test_history_daily_robustness import journal

from pipeline.history_collect import collect
from pipeline.history_state import Job, Stop

NOW = datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc)


def previous_thirty(tmp_path, timestamp=None):
    paths = []
    for label, count in (("old", 17), ("manual", 13)):
        events = []
        for index in range(count):
            identifier = f"{label}:{index}"
            events.extend([
                {"type": "date_reserved", "id": identifier, "UTC": timestamp or "2026-04-29T23:00:00Z",
                 "HTTP_reserved": 8, "reload_reserved": 1},
                {"type": "date_closed", "id": identifier, "UTC": timestamp or "2026-04-29T23:01:00Z",
                 "physicalHTTP": 8, "reload_ACK": 1, "reload_intent": 1},
            ])
        paths.append(journal(tmp_path / f"{label}.jsonl", events))
    return paths


def test_explicit_plan_admits_real_sdk_reload31_while_ordinary_plan_still_stops(tmp_path):
    paths = previous_thirty(tmp_path)
    originals = [path.read_bytes() for path in paths]
    ordinary = Job.create(tmp_path / "ordinary", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic")
    with pytest.raises(Stop) as failure:
        collect(ordinary, ordinary.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8,
                now=NOW, ledger_paths=paths,
                date_runner=lambda *a: pytest.fail("ordinary30 ceiling reached source"))
    assert failure.value.category == "rolling_source_budget"
    experiment = Job.create(
        tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
        experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW,
    )
    result = collect(experiment, experiment.plan_sha, confirm=True, reload=True, max_dates=1,
                     max_http=8, now=NOW, date_runner=native_runner("reload_sibling"))
    assert result["counts"] == {"ready": 1, "pending": 6}
    assert result["last_run_status"] == "bounded_complete" and not result["locked"]
    from pipeline.history_state import rolling_usage

    usage = rolling_usage(experiment, now=NOW)
    assert usage["reloads"] == 31 and usage["HTTP"] == 245
    assert [path.read_bytes() for path in paths] == originals


def test_offline_cli_freezes_experiment_and_rejects_daily_before_credentials(tmp_path):
    import json

    from test_history_backfill import cli

    paths = previous_thirty(tmp_path, datetime.now(timezone.utc).isoformat())
    state = tmp_path / "experiment"
    result = cli(state, "plan", "--start", "2026-04-23", "--end", "2026-04-29", "--streams", "hr,hrv",
                 "--provider", "legacy", "--account", "synthetic", "--experiment-max-reloads", "37",
                 "--experiment-max-http", "296", "--ledger", str(paths[0]), "--ledger", str(paths[1]))
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["quota_experiment"]["rolling_limits"] == {"reloads": 37, "HTTP": 296}
    assert status["quota_experiment"]["baseline_usage"]["HTTP"] == 240
    rejected = cli(state, "daily", "--plan-sha", status["plan_sha256"],
                   "--confirm-cloud-jobs-paused", "--confirm-r2-write", "--archive-reload")
    assert rejected.returncode == 2
    assert not (state / "runs").exists()


@pytest.mark.parametrize("limits,provider,end", [
    ({"reloads": 38, "HTTP": 296}, "legacy", "2026-04-29"),
    ({"reloads": 37, "HTTP": 297}, "legacy", "2026-04-29"),
    ({"reloads": True, "HTTP": 296}, "legacy", "2026-04-29"),
    ({"reloads": 37, "HTTP": None}, "legacy", "2026-04-29"),
    ({"reloads": 37, "HTTP": 296}, "personal", "2026-04-29"),
    ({"reloads": 37, "HTTP": 296}, "legacy", "2026-04-30"),
])
def test_invalid_experiment_bounds_never_create_a_source_job(tmp_path, limits, provider, end):
    paths = previous_thirty(tmp_path)
    with pytest.raises(Stop):
        Job.create(tmp_path / "experiment", "2026-04-23", end, ["hr"], provider, "synthetic",
                   experiment_limits=limits, ledger_paths=paths, now=NOW)
    assert not (tmp_path / "experiment").exists()


def test_altered_baseline_cannot_turn_a_bound_plan_into_an_arbitrary_quota_override(tmp_path):
    import json

    paths = previous_thirty(tmp_path)
    job = Job.create(tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                     experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    # An untrusted edited JSON plan is not a new authorization or observed usage.
    plan = job.plan
    plan["quota_experiment"]["baseline_usage"]["HTTP"] = 10000
    plan["quota_experiment"]["rolling_limits"]["HTTP"] = 10056
    (job.folder / "plan.json").write_text(json.dumps(plan))
    invalid = Job(job.folder)
    with pytest.raises(Stop):
        collect(invalid, invalid.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8, now=NOW,
                date_runner=lambda *a: pytest.fail("edited baseline reached credentials"))
    assert not (job.folder / "runs").exists()


@pytest.mark.parametrize("mode,expected", [("empty", "empty_unresolved"), ("quota", "rate_limited"),
                                           ("sibling_denial_close", "denied"), ("unknown", "unknown")])
def test_first_empty_failure_or_unknown_never_retries_or_advances_an_experiment(tmp_path, mode, expected):
    paths = previous_thirty(tmp_path)
    end = "2026-04-28" if mode == "quota" else "2026-04-29"
    start = "2026-04-22" if mode == "quota" else "2026-04-23"
    job = Job.create(tmp_path / "experiment", start, end, ["hr", "hrv"], "legacy", "synthetic",
                     experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    options = {"confirm": True, "reload": True, "max_dates": 1, "max_http": 8, "now": NOW}
    if expected == "unknown":
        with pytest.raises(Stop):
            collect(job, job.plan_sha, date_runner=native_runner(mode), **options)
        assert job.status()["locked"]
    else:
        result = collect(job, job.plan_sha, date_runner=native_runner(mode), **options)
        assert expected in result["counts"]
        assert result["counts"]["pending"] == 12
    with pytest.raises(Stop):
        collect(job, job.plan_sha, date_runner=lambda *a: pytest.fail("advanced after experiment stop"), **options)
    assert len(list((job.folder / "runs").iterdir())) == 1


def test_exact_physical_intents_distinguish_oslo_midnight_without_relabelling_old_aggregates(tmp_path):
    import json

    paths = previous_thirty(tmp_path)
    job = Job.create(tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                     experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    result = collect(job, job.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8, now=NOW,
                     date_runner=native_runner("calendar_boundary"))
    observed = result["quota_observations"]
    assert observed["rolling24h_after_last_date"] == {"HTTP": 243, "reloads": 30}
    physical = observed["calendar_counts"]["physical_send"]
    assert physical["UTC"] == {"2026-04-29": {"HTTP": 3, "reload_intent": 0, "reload_ACK": 0}}
    assert physical["Europe/Oslo"] == {
        "2026-04-29": {"HTTP": 1, "reload_intent": 0, "reload_ACK": 0},
        "2026-04-30": {"HTTP": 2, "reload_intent": 0, "reload_ACK": 0},
    }
    legacy = observed["calendar_counts"]["date_close_aggregate"]
    assert legacy["UTC"]["2026-04-29"]["HTTP"] == 240
    assert legacy["Europe/Oslo"]["2026-04-30"]["reload_ACK"] == 30
    first = next((job.folder / "runs").glob("*/2026-04-29/http-01.json"))
    assert json.loads(first.read_bytes())["UTC"] == "2026-04-29T21:59:59+00:00"


def test_exact_new_http_can_cross_utc_midnight_without_a_rolling_counter_reset(tmp_path):
    paths = previous_thirty(tmp_path)
    job = Job.create(tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                     experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    result = collect(job, job.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8, now=NOW,
                     date_runner=native_runner("calendar_utc"))
    observed = result["quota_observations"]
    assert observed["rolling24h_after_last_date"] == {"HTTP": 243, "reloads": 30}
    physical = observed["calendar_counts"]["physical_send"]
    assert physical["UTC"] == {
        "2026-04-29": {"HTTP": 1, "reload_intent": 0, "reload_ACK": 0},
        "2026-04-30": {"HTTP": 2, "reload_intent": 0, "reload_ACK": 0},
    }
    assert physical["Europe/Oslo"] == {"2026-04-30": {"HTTP": 3, "reload_intent": 0, "reload_ACK": 0}}


def test_required_journals_cannot_be_omitted_changed_or_refunded(tmp_path):
    paths = previous_thirty(tmp_path)
    with pytest.raises(Stop):
        Job.create(tmp_path / "missing", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                   experiment_limits={"reloads": 37, "HTTP": 296}, now=NOW)
    job = Job.create(tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                     experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    options = {"confirm": True, "reload": True, "max_dates": 1, "max_http": 8, "now": NOW,
               "date_runner": lambda *a: pytest.fail("incomplete journal reached credentials")}
    with pytest.raises(Stop):
        collect(job, job.plan_sha, ledger_paths=paths[:1], **options)
    paths[1].write_bytes(b"")
    with pytest.raises(Stop):
        collect(job, job.plan_sha, **options)
    assert not (job.folder / "runs").exists()


def test_unclosed_prior_reservation_is_not_experimental_capacity(tmp_path):
    paths = previous_thirty(tmp_path)
    unknown = journal(tmp_path / "unknown.jsonl", [
        {"type": "date_reserved", "id": "unclosed", "UTC": "2026-03-01T00:00:00Z", "HTTP_reserved": 8},
    ])
    with pytest.raises(Stop) as failure:
        Job.create(tmp_path / "experiment", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                   experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=[*paths, unknown], now=NOW)
    assert failure.value.category == "unknown_source_reservation_hold"


def test_later_explicit_window_requires_every_predecessor_and_observes_appended_stop(tmp_path):
    paths = previous_thirty(tmp_path)
    first = Job.create(tmp_path / "first", "2026-04-23", "2026-04-29", ["hr"], "legacy", "synthetic",
                       experiment_limits={"reloads": 37, "HTTP": 296}, ledger_paths=paths, now=NOW)
    collect(first, first.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8,
            now=NOW, date_runner=native_runner("reload_sibling"))
    previous = first.folder / "events.jsonl"
    with pytest.raises(Stop) as failure:
        Job.create(tmp_path / "incomplete", "2026-04-16", "2026-04-22", ["hr"], "legacy", "synthetic",
                   experiment_limits={"reloads": 38, "HTTP": 301}, ledger_paths=[previous], now=NOW)
    assert failure.value.category == "quota_journals_incomplete"
    later = Job.create(tmp_path / "later", "2026-04-16", "2026-04-22", ["hr"], "legacy", "synthetic",
                       experiment_limits={"reloads": 38, "HTTP": 301}, ledger_paths=[*paths, previous], now=NOW)
    assert later.status()["quota_experiment"]["baseline_usage"]["reloads"] == 31
    first.append({"type": "quota_experiment_stop", "reason": "empty_unresolved", "UTC": NOW.isoformat()})
    with pytest.raises(Stop) as failure:
        collect(later, later.plan_sha, confirm=True, reload=True, max_dates=1, max_http=8, now=NOW,
                date_runner=lambda *a: pytest.fail("appended predecessor stop reached source"))
    assert failure.value.category == "quota_experiment_stopped"
    assert not (later.folder / "runs").exists()
