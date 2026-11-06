"""Explicit publication admission through the real guarded R2 SDK boundary."""

import json

import pytest
from test_history_baseline_integration import BaselineServer, memory_job, sdk_store

from pipeline import history_publish
from pipeline.history_state import Stop, digest


def test_bounded_new_round_can_use_6000_publication_get_allowance(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, _ = memory_job(monkeypatch, [{"date": "2026-04-29", "stream": "hr"}])
    server = BaselineServer()
    report = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        execution_context={"counts": {"get": 2290, "list": 0, "put": 0},
                           "body_bytes": 0, "publication_get_limit": 6000},
    )["baseline_refresh"]
    assert report["execution_totals"]["counts"]["get"] == 2298
    assert report["execution_totals"]["publication_get_limit"] == 6000
    assert len([c for c in server.calls if c[0] in {"GET", "HEAD"} and not c[2]]) == 8


@pytest.mark.parametrize("prior_body_bytes", [4111887, 4111889])
def test_authorized_old_2000_round_continues_only_three_bound_references(monkeypatch, prior_body_bytes):
    class BodyServer(BaselineServer):
        def __init__(self):
            super().__init__()
            self.write_body_sizes = []

        def send(self, request):
            if request.method == "PUT":
                body = request.body
                if isinstance(body, bytes):
                    size = len(body)
                else:
                    position = body.tell()
                    size = len(body.read())
                    body.seek(position)
                self.write_body_sizes.append(size)
            return super().send(request)

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [
        {"date": "2025-12-21", "stream": "hr"},
        {"date": "2025-12-28", "stream": "hr"},
    ])
    server = BodyServer()
    initial = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"counts": {"get": 1587, "list": 379, "put": 235},
                           "body_bytes": prior_body_bytes, "publication_get_limit": 2000, "phases": 9},
    )["baseline_refresh"]
    assert initial["execution_totals"]["counts"] == {"get": 1602, "list": 380, "put": 241}
    # Compressed sizes vary between zlib implementations. The independent
    # HTTP observer accounts for every attempted body on top of the prior seed.
    assert len(server.write_body_sizes) == 6
    initial_body_bytes = prior_body_bytes + sum(server.write_body_sizes)
    assert initial["execution_totals"]["body_bytes"] == initial_body_bytes
    receipts = {name: body for name, body in files.items() if name.endswith("/RECEIPT.json")}
    prior = next(body for name, body in receipts.items() if json.loads(body).get("status") == "complete"
                 and "baseline_refresh" in json.loads(body))
    periods = ["week/2025-W51", "week/2025-W52", "week/2026-W01"]
    admitted = history_publish.admit_continuation(
        job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(prior),
        independent_gets=95, period_names=periods,
    )
    for _ in range(4):
        result = history_publish.publish(
            job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
            continuation_sha256=admitted,
        )["baseline_refresh"]
        if result["continuation_complete"]:
            break
    assert result["continuation_complete"] is True
    totals = result["execution_totals"]
    assert totals["counts"]["get"] > 1602
    assert totals["independent_gets"] == 95 and totals["publication_get_limit"] == 6000
    physical_gets = sum(method in {"GET", "HEAD"} and not query for method, _, query in server.calls)
    assert totals["counts"]["get"] == 1587 + physical_gets
    assert totals["counts"]["list"] == 379 + sum(bool(query) for _, _, query in server.calls)
    assert totals["counts"]["put"] == 235 + len(server.write_body_sizes)
    assert totals["phases"] == 12
    assert totals["counts"]["list"] >= initial["execution_totals"]["counts"]["list"]
    assert totals["body_bytes"] > initial_body_bytes
    assert totals["body_bytes"] == prior_body_bytes + sum(server.write_body_sizes)
    assert all(files[name] == body for name, body in receipts.items())
    from pipeline.night_baseline_refresh import QUEUE_KEY

    pending = json.loads(server.objects[QUEUE_KEY][0])["pending"]
    assert not set(periods) & pending.keys()
    assert "month/2025-12" in pending
    raw = [name for name in server.puts if name.startswith("health/signals/")]
    assert len(raw) == len(set(raw)) == 2
    print(json.dumps({"old": initial["execution_totals"], "continued": totals}))
    repeated = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
        continuation_sha256=admitted,
    )["baseline_refresh"]["execution_totals"]
    assert repeated["counts"]["get"] == totals["counts"]["get"] + 1
    assert repeated["independent_gets"] == 95
    assert repeated["body_bytes"] == totals["body_bytes"]


def test_rolling_admission_reports_first_safe_24h_expiry_without_reset(monkeypatch):
    from datetime import datetime, timezone

    from pipeline.history_state import rolling_usage

    job, _ = memory_job(monkeypatch, [])
    for number in range(30):
        stamp = "2026-11-06T11:00:00Z" if number == 0 else "2026-11-06T11:30:00Z"
        job.append({"type": "date_reserved", "id": str(number), "UTC": stamp,
                    "HTTP_reserved": 8, "reload_reserved": 1})
        job.append({"type": "date_closed", "id": str(number), "UTC": stamp,
                    "physicalHTTP": 8, "reload_intent": 1, "reload_ACK": 1})
    usage = rolling_usage(job, now=datetime(2026, 11, 7, 10, 30, tzinfo=timezone.utc))
    assert usage["HTTP"] == 240 and usage["reloads"] == 30
    assert usage["next_date_not_before"] == "2026-11-07T11:00:00.000001+00:00"


def test_new_bound_daily_round_keeps_cumulative_usage_and_new_finite_allowance(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2026-04-15"
    job, _ = memory_job(monkeypatch, [{"date": day, "stream": "hr"}])
    server = BaselineServer()
    for round_id in ["1" * 32, "2" * 32]:
        job.append({"type": "daily_round_started", "round": round_id, "dates": [day],
                    "new_dates": [day], "stored_dates": [], "finalize_dates": [day],
                    "max_dates": 1, "max_http": 8})
        context = {"publication_get_limit": 6000, "round": round_id}
        for _ in range(4):
            result = history_publish.publish(
                job, job.plan_sha, confirm=True, store=sdk_store(server), finalize_dates=[day],
                execution_context=context,
            )["baseline_refresh"]
            if result["cohort_complete"]:
                break
        assert result["cohort_complete"] is True
        if round_id.startswith("1"):
            prior = result["execution_totals"]
            job.append({"type": "daily_round_finished", "round": round_id})
    totals = result["execution_totals"]
    assert totals["counts"]["get"] == prior["counts"]["get"] + 1
    assert totals["round_baseline"]["counts"] == prior["counts"]
    assert totals["body_bytes"] == prior["body_bytes"]
    assert totals["round"] == "2" * 32


@pytest.mark.parametrize("invalid", ["plan", "receipt", "unconfirmed", "unknown"])
def test_continuation_rejects_wrong_bindings_and_unknown_owners_before_transport(monkeypatch, invalid):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2025-12-21", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    receipt = next(body for name, body in files.items() if name.endswith("/RECEIPT.json")
                   and "baseline_refresh" in json.loads(body))
    if invalid == "unknown":
        job.append({"type": "begin", "run": "f" * 32})
    calls = len(server.calls)
    with pytest.raises(Stop):
        history_publish.admit_continuation(
            job, "b" * 64 if invalid == "plan" else job.plan_sha,
            confirm=invalid != "unconfirmed",
            prior_receipt_sha256="0" * 64 if invalid == "receipt" else digest(receipt),
            independent_gets=95, period_names=["week/2025-W51"],
        )
    assert len(server.calls) == calls
    assert not any("publication-continuation-" in name for name in files)


def test_true_new_limit_exhaustion_stops_before_another_physical_send(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2025-12-21", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    receipt = next(body for name, body in files.items() if name.endswith("/RECEIPT.json")
                   and "baseline_refresh" in json.loads(body))
    admission = history_publish.admit_continuation(
        job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
        independent_gets=95, period_names=["week/2025-W51"],
    )
    calls = len(server.calls)
    with pytest.raises(Stop) as stopped:
        history_publish.publish(
            job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
            continuation_sha256=admission,
            execution_context={"counts": {"get": 6000, "list": 0, "put": 0}, "body_bytes": 0},
        )
    assert stopped.value.category == "storage_execution_budget"
    assert len(server.calls) == calls and job.status()["locked"]


def test_rolling_unknown_never_gets_an_automatic_expiry(monkeypatch):
    from datetime import datetime, timezone

    from pipeline.history_state import rolling_usage

    job, _ = memory_job(monkeypatch, [])
    job.append({"type": "date_reserved", "id": "unknown", "UTC": "2026-11-01T11:00:00Z",
                "HTTP_reserved": 8, "reload_reserved": 1})
    usage = rolling_usage(job, now=datetime(2026, 11, 7, 10, 30, tzinfo=timezone.utc))
    assert usage["next_date_not_before"] is None
    assert usage["unknown_reservations"] == 1 and usage["HTTP"] == 8


def test_continuation_cannot_be_transplanted_to_another_job(monkeypatch):
    import copy

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2025-12-21", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    receipt = next(body for name, body in files.items() if name.endswith("/RECEIPT.json")
                   and "baseline_refresh" in json.loads(body))
    admission = history_publish.admit_continuation(
        job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
        independent_gets=95, period_names=["week/2025-W51"],
    )
    clone = copy.copy(job)
    clone.folder = type(job.folder)("other-job")
    files.update({"other-job/" + name.removeprefix("job/"): body
                  for name, body in list(files.items()) if name.startswith("job/")})
    calls = len(server.calls)
    with pytest.raises(Stop) as failure:
        history_publish.publish(clone, clone.plan_sha, confirm=True, store=sdk_store(server),
                                continuation_sha256=admission)
    assert failure.value.category == "continuation_binding_unverified"
    assert len(server.calls) == calls


def test_unfinished_legacy_round_cannot_rotate_to_a_fresh_daily_allowance(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2025-12-21"
    job, _ = memory_job(monkeypatch, [{"date": day, "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    job.append({"type": "daily_round_started", "round": "1" * 32, "dates": [day],
                "new_dates": [], "stored_dates": [day], "finalize_dates": [day],
                "max_dates": 1, "max_http": 8})
    calls = len(server.calls)
    with pytest.raises(Stop) as failure:
        history_publish.publish(
            job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
            execution_context={"publication_get_limit": 6000, "round": "1" * 32},
        )
    assert failure.value.category == "unfinished_publication_round_requires_continuation"
    assert len(server.calls) == calls


def test_unknown_continuation_write_keeps_owner_and_rejects_fresh_admission(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2025-12-21", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    receipt_name = next(name for name, body in files.items() if name.endswith("/RECEIPT.json")
                        and "baseline_refresh" in json.loads(body))
    receipt = files[receipt_name]
    admission = history_publish.admit_continuation(
        job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
        independent_gets=95, period_names=["week/2025-W51"],
    )
    server.mode = "lost_ack"
    with pytest.raises(Stop) as failure:
        history_publish.publish(job, job.plan_sha, confirm=True, store=sdk_store(server),
                                target_dates=[], continuation_sha256=admission)
    assert failure.value.category == "unknown_storage_transport"
    assert job.status()["locked"] and job.status()["active_run"] is not None
    calls = len(server.calls)
    with pytest.raises(Stop) as held:
        history_publish.admit_continuation(
            job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
            independent_gets=95, period_names=["week/2025-W51"],
        )
    assert held.value.category == "unknown_owner_hold_no_takeover"
    assert len(server.calls) == calls and files[receipt_name] == receipt


@pytest.mark.parametrize("prior_phases", [1, 10])
def test_legacy_receipts_without_phase_field_still_count_prior_terminal_phases(monkeypatch, prior_phases):
    from pipeline.history_state import encoded

    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2025-12-21", "stream": "hr"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[row["date"] for row in job.plan["targets"]],
        execution_context={"publication_get_limit": 2000},
    )
    for _ in range(prior_phases - 1):
        history_publish.publish(
            job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
            defer_references=True, finalize_dates=["2025-12-21"],
        )
    # Filesystem seam supplies an authentic older schema, whose journal report
    # and both receipt variants never had the additive phase field.
    for name, body in list(files.items()):
        if name.endswith(("/RECEIPT.json", "/REMOTE-VERIFIED.json")):
            value = json.loads(body)
            if "baseline_refresh" in value:
                value["baseline_refresh"]["execution_totals"].pop("phases")
                files[name] = encoded(value)
    chain, previous = bytearray(), None
    for frame in job.events()[0]:
        if frame["event"]["type"] == "reference_refresh":
            frame["event"]["report"]["execution_totals"].pop("phases")
        frame["previous"] = previous
        line = encoded(frame)
        chain.extend(line)
        previous = digest(line)
    files[str(job.folder / "events.jsonl")] = bytes(chain)
    latest_run = [frame["event"]["run"] for frame in job.events()[0]
                  if frame["event"]["type"] == "end" and frame["event"]["status"] == "published"][-1]
    receipt = files[str(job.folder / "runs" / latest_run / "RECEIPT.json")]
    admission = history_publish.admit_continuation(
        job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
        independent_gets=95, period_names=["week/2025-W51"],
    )
    result = history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), target_dates=[],
        continuation_sha256=admission,
    )["baseline_refresh"]
    assert result["execution_totals"]["phases"] == prior_phases + 1


@pytest.mark.parametrize("case", ["outside_old_cohort", "open_old_cohort"])
def test_continuation_admits_only_closed_periods_from_exact_old_receipt_cohort(monkeypatch, case):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    day = "2026-04-15" if case == "outside_old_cohort" else "2026-11-01"
    targets = [{"date": day, "stream": "hrv"}]
    if case == "outside_old_cohort":
        targets.append({"date": "2026-02-18", "stream": "hrv"})
    else:
        targets.append({"date": "2026-04-15", "stream": "hrv"})
    job, files = memory_job(monkeypatch, targets)
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        finalize_dates=[day] if case == "outside_old_cohort" else [row["date"] for row in targets],
        execution_context={"publication_get_limit": 2000},
    )
    assert job.status()["counts"] == {"published": len(targets)}
    receipt_name = next(name for name, body in files.items() if name.endswith("/RECEIPT.json")
                        and "baseline_refresh" in json.loads(body))
    receipt = files[receipt_name]
    calls, journal = len(server.calls), files[str(job.folder / "events.jsonl")]
    with pytest.raises(Stop) as failure:
        history_publish.admit_continuation(
            job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
            independent_gets=95,
            period_names=["week/2026-W08" if case == "outside_old_cohort" else "month/2026-11"],
        )
    assert failure.value.category == "continuation_periods_outside_closed_cohort"
    assert len(server.calls) == calls and files[receipt_name] == receipt
    assert files[str(job.folder / "events.jsonl")] == journal
    assert not any("publication-continuation-" in name for name in files)


def test_continuation_holds_legacy_receipt_without_bound_cohort(monkeypatch):
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    job, files = memory_job(monkeypatch, [{"date": "2026-04-15", "stream": "hrv"}])
    server = BaselineServer()
    history_publish.publish(
        job, job.plan_sha, confirm=True, store=sdk_store(server), defer_references=True,
        execution_context={"publication_get_limit": 2000},
    )
    receipt = next(body for name, body in files.items() if name.endswith("/RECEIPT.json")
                   and "baseline_refresh" in json.loads(body))
    calls, journal = len(server.calls), files[str(job.folder / "events.jsonl")]
    with pytest.raises(Stop) as failure:
        history_publish.admit_continuation(
            job, job.plan_sha, confirm=True, prior_receipt_sha256=digest(receipt),
            independent_gets=95, period_names=["week/2026-W16"],
        )
    assert failure.value.category == "continuation_cohort_unverified"
    assert len(server.calls) == calls and files[str(job.folder / "events.jsonl")] == journal
    assert not any("publication-continuation-" in name for name in files)
