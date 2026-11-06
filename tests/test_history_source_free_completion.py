"""Source-free CLI at the real Job/publisher/SDK physical HTTP boundary."""

import json
from collections import Counter
from datetime import datetime, timezone

import boto3
import pytest
from history_storage_fixture import ACCOUNT, BUCKET
from pipeline.history_backfill import main
from pipeline.history_state import Job, digest, encoded, read, write
from test_history_baseline_integration import BaselineServer

ROUND = "c" * 32
SELECTED = ["2026-04-29", "2026-04-28"]
TARGET_SHA = digest(("https://" + ACCOUNT + ".r2.cloudflarestorage.com\0" + BUCKET).encode())


def source_date(job, day, *, denied=False):
    """Persist synthetic native source artifacts in their actual date layout."""
    run = digest(day.encode())[:32]
    folder = job.folder / "runs" / run / day
    folder.mkdir(parents=True)
    intent_sha = write(folder / "INTENT.json", {
        "run": run, "date": day, "streams": ["hr", "hrv"], "plan": job.plan_sha,
        "max_http": 8, "archive_reload": True,
    })
    rows = []
    for stream in ("hr", "hrv"):
        if denied and stream == "hrv":
            rows.append({"date": day, "stream": stream, "status": "denied", "reason": "denied"})
            continue
        stamp = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
        raw = (
            {"calendarDate": day, "heartRateValues": [[stamp, 55]]}
            if stream == "hr" else {
                "calendarDate": day, "hrvSummary": {"calendarDate": day, "lastNightAvg": 44},
                "hrvReadings": [{"hrvValue": 44, "readingTimeGMT": day + "T01:00:00",
                                 "readingTimeLocal": day + "T03:00:00"}],
            }
        )
        body = encoded(raw)
        for field in ("wire", "view"):
            write(folder / (stream + "-" + field + ".json"), body)
        rows.append({
            "date": day, "stream": stream, "status": "ready", "source_verified": True,
            "wire": stream + "-wire.json", "view": stream + "-view.json",
            "wire_sha256": digest(body), "view_sha256": digest(body),
        })
    receipt_sha = write(folder / "RECEIPT.json", {
        "status": "stopped" if denied else "complete", "intent_sha256": intent_sha,
        "targets": rows, "cleanup_status": "complete", "HTTP": 8,
        "category": "denied" if denied else None,
    })
    result_sha = write(folder / "RESULT.json", {
        "native": 2 if denied else 0, "receipt_sha256": receipt_sha,
    })
    job.append({"type": "begin", "run": run})
    job.append({"type": "date_reserved", "id": run + ":" + day,
                "UTC": "2026-05-01T10:00:00+00:00", "HTTP_reserved": 8, "reload_reserved": 1})
    for row in rows:
        job.append({"type": "target", **row, "folder": str(folder),
                    "receipt_sha256": receipt_sha, "native": 2 if denied else 0})
    job.append({"type": "date_closed", "id": run + ":" + day,
                "UTC": "2026-05-01T10:00:01+00:00", "physicalHTTP": 8,
                "reload_intent": 1, "reload_ACK": 1})
    job.append({"type": "end", "run": run, "status": "denied" if denied else "bounded_complete",
                "HTTP": 8})
    return {"run": run, "date": day, "intent_sha256": intent_sha,
            "result_sha256": result_sha, "receipt_sha256": receipt_sha}


def stopped_job(tmp_path):
    job = Job.create(tmp_path / "job", "2026-04-25", "2026-04-29",
                     ["hr", "hrv"], "legacy", "synthetic")
    source_date(job, "2026-04-25")  # Foreign ready targets must stay outside the selected round.
    job.append({"type": "daily_round_started", "round": ROUND,
                "dates": SELECTED, "new_dates": SELECTED, "stored_dates": [],
                "finalize_dates": SELECTED, "max_dates": 2, "max_http": 240})
    sources = [source_date(job, SELECTED[0]), source_date(job, SELECTED[1], denied=True)]
    job.append({"type": "pause", "reason": "operator_quota", "not_before": None})
    return job, *admission(job, sources, tmp_path)


def admission(job, sources, tmp_path):
    frames, tip = job.events()
    proof = {
        "schema": 1, "operation": "source-free-publication", "plan_sha256": job.plan_sha,
        "daily_round": ROUND, "journal": {"frames": len(frames), "tip": tip},
        "storage_target_sha256": TARGET_SHA, "independent_gets": 7,
        "source_stop_proof_sha256": "d" * 64, "source_dates": sources,
    }
    proof_path = tmp_path / "source-proof.json"
    proof_sha = write(proof_path, proof)
    return proof_path, proof_sha


def physical_storage(monkeypatch, server):
    import requests

    real_client = boto3.client
    clients = []

    def client(*args, **kwargs):
        current = real_client(*args, **kwargs)
        current._endpoint.http_session.send = server.send
        clients.append(current)
        return current

    monkeypatch.setattr(boto3, "client", client)
    def no_source_send(*args, **kwargs):
        raise AssertionError("Source-free publication must not send Garmin requests")

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", no_source_send)
    for name, value in {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "R2_ACCESS_KEY_ID": "synthetic",
                        "R2_SECRET_ACCESS_KEY": "synthetic", "R2_BUCKET": BUCKET,
                        "HEALTH_TIMEZONE": "Europe/Oslo"}.items():
        monkeypatch.setenv(name, value)
    return clients


def invoke(job, proof_path, proof_sha):
    return main(["--state", str(job.folder), "publish-source-free", "--plan-sha", job.plan_sha,
                 "--confirm-r2-write", "--source-proof", str(proof_path),
                 "--source-proof-sha", proof_sha])


def test_source_free_cli_publishes_three_positive_curves_preserving_denied_and_selection(
    tmp_path, monkeypatch, capsys,
):
    job, proof_path, proof_sha = stopped_job(tmp_path)
    original_prefix = read(job.folder / "events.jsonl")
    server = BaselineServer()
    from pipeline.night_baseline_refresh import QUEUE_KEY

    unrelated = {"phase": "dirty", "sources": {}, "completed": {},
                 "start_date": "2026-04-13", "end_date": "2026-04-19"}
    server.objects[QUEUE_KEY] = (encoded({"schema_version": 1,
        "kind": "night-baseline-refresh-queue", "pending": {"week/2026-W16": unrelated}}), '"old-queue"')
    server.metadata[QUEUE_KEY] = {"content-type": "application/json"}
    physical_storage(monkeypatch, server)

    assert invoke(job, proof_path, proof_sha) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["source_free_status"] == "bounded_complete"
    assert result["daily_status"] == "source_stopped" and result["source_free_source_native"] == 2
    assert result["daily_round"] == result["source_free_round"] == ROUND
    assert result["counts"] == {"published": 3, "denied": 1, "ready": 2, "pending": 4}
    assert result["pause"] == "operator_quota" and result["locked"] is False
    assert result["baseline_refresh"]["execution_totals"]["independent_gets"] == 7
    assert read(job.folder / "events.jsonl").startswith(original_prefix)
    suffix = read(job.folder / "events.jsonl")[len(original_prefix):]
    assert b'date_reserved' not in suffix and b'daily_round_finished' not in suffix
    assert b'"type": "resume"' not in suffix
    assert {name for name in server.objects if name.startswith("health/signals/")} == {
        "health/signals/v1/2026/04/2026-04-29.json", "health/signals/v1/2026/04/2026-04-28.json",
    }
    assert "health/hrv/2026/04/2026-04-29.json" in server.objects
    assert "health/hrv/2026/04/2026-04-28.json" not in server.objects
    assert json.loads(server.objects[QUEUE_KEY][0])["pending"]["week/2026-W16"] == unrelated


@pytest.mark.parametrize("earlier_current", [False, True])
def test_source_free_cli_carries_older_complete_and_current_publication_receipts(
    tmp_path, monkeypatch, capsys, earlier_current,
):
    from pipeline.history_publish import publish

    server = BaselineServer()
    physical_storage(monkeypatch, server)
    job = Job.create(tmp_path / "job", "2026-04-25", "2026-04-29",
                     ["hr", "hrv"], "legacy", "synthetic")
    old_round = "b" * 32
    job.append({"type": "daily_round_started", "round": old_round,
                "dates": ["2026-04-25"], "new_dates": ["2026-04-25"], "stored_dates": [],
                "finalize_dates": ["2026-04-25"], "max_dates": 1, "max_http": 240})
    source_date(job, "2026-04-25")
    previous_context = {"round": old_round, "publication_get_limit": 6000}
    for _ in range(14):
        old = publish(job, job.plan_sha, confirm=True, target_dates=["2026-04-25"],
                      finalize_dates=["2026-04-25"], execution_context=previous_context)
        if old["baseline_refresh"].get("cohort_complete"):
            break
    assert old["baseline_refresh"]["cohort_complete"] is True
    old_totals = old["baseline_refresh"]["execution_totals"]
    job.append({"type": "daily_round_finished", "round": old_round})
    job.append({"type": "daily_round_started", "round": ROUND,
                "dates": SELECTED, "new_dates": SELECTED, "stored_dates": [],
                "finalize_dates": SELECTED, "max_dates": 2, "max_http": 240})
    sources = [source_date(job, SELECTED[0])]
    current_phases = 0
    if earlier_current:
        current = publish(job, job.plan_sha, confirm=True, target_dates=[SELECTED[0]],
                          finalize_dates=SELECTED, defer_references=True,
                          execution_context={"round": ROUND, "publication_get_limit": 6000})
        current_phases = current["baseline_refresh"]["execution_totals"]["phases"]
        assert current_phases == 1
    sources.append(source_date(job, SELECTED[1], denied=True))
    proof_path, proof_sha = admission(job, sources, tmp_path)

    assert invoke(job, proof_path, proof_sha) == 0
    result = json.loads(capsys.readouterr().out)
    totals = result["baseline_refresh"]["execution_totals"]
    physical = Counter("list" if query.get("list-type") else "put" if method == "PUT" else "get"
                       for method, _, query in server.calls)
    assert totals["counts"] == dict(physical)
    assert totals["round_baseline"] == {"counts": old_totals["counts"], "body_bytes": old_totals["body_bytes"]}
    assert totals["phases"] > current_phases
    assert totals["independent_gets"] == 7
    assert totals["source_free_proof_sha256"] == proof_sha
    assert result["source_free_status"] == "bounded_complete" and result["counts"]["denied"] == 1


@pytest.mark.parametrize("change", [
    "proof_hash", "round", "journal_tip", "missing_source", "extra_source", "duplicate_source",
    "target", "wire", "receipt", "owner", "source_suffix",
])
def test_source_free_cli_rejects_changed_binding_before_any_storage_send(
    tmp_path, monkeypatch, change,
):
    job, proof_path, proof_sha = stopped_job(tmp_path)
    proof = json.loads(read(proof_path))
    if change == "proof_hash":
        proof_sha = "0" * 64
    elif change == "round":
        proof["daily_round"] = "e" * 32
    elif change == "journal_tip":
        proof["journal"]["tip"] = "0" * 64
    elif change == "missing_source":
        proof["source_dates"].pop()
    elif change == "extra_source":
        proof["source_dates"].append({**proof["source_dates"][0], "date": "2026-04-25"})
    elif change == "duplicate_source":
        proof["source_dates"].append(proof["source_dates"][0])
    elif change == "target":
        proof["storage_target_sha256"] = "0" * 64
    elif change in {"wire", "receipt"}:
        source = proof["source_dates"][0]
        folder = job.folder / "runs" / source["run"] / source["date"]
        (folder / ("hr-wire.json" if change == "wire" else "RECEIPT.json")).write_bytes(b"{}")
    elif change == "owner":
        write(job.folder / "OWNER.lock", {"run": "f" * 32, "phase": "collect", "plan": job.plan_sha})
    elif change == "source_suffix":
        job.append({"type": "date_reserved", "id": "f" * 32 + ":2026-04-29"})
    if change in {"round", "journal_tip", "missing_source", "extra_source", "duplicate_source", "target"}:
        proof_path.write_bytes(encoded(proof))
        proof_sha = digest(read(proof_path))
    server = BaselineServer()
    physical_storage(monkeypatch, server)
    before = read(job.folder / "events.jsonl")
    assert invoke(job, proof_path, proof_sha) == 2
    assert server.calls == []
    assert read(job.folder / "events.jsonl") == before
    assert job.status()["counts"] == {"ready": 5, "denied": 1, "pending": 4}


def test_repeated_source_free_cli_reuses_only_its_completed_admission_without_more_requests(
    tmp_path, monkeypatch, capsys,
):
    job, proof_path, proof_sha = stopped_job(tmp_path)
    server = BaselineServer()
    physical_storage(monkeypatch, server)
    assert invoke(job, proof_path, proof_sha) == 0
    capsys.readouterr()
    calls = list(server.calls)
    journal = read(job.folder / "events.jsonl")
    assert invoke(job, proof_path, proof_sha) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["source_free_status"] == "bounded_complete"
    assert server.calls == calls and read(job.folder / "events.jsonl") == journal
