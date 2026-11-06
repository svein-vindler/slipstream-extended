"""First unresolved channel stops ordinary daily source work, preserving good data."""

import json
from urllib.parse import urlsplit

import pytest
import requests
from daily_autonomy_fixture import daily_system, transport_counts

from pipeline import history_backfill
from pipeline.history_canonical import decode, key
from pipeline.history_state import Job, digest, rolling_usage


@pytest.mark.parametrize("empty_stream", ["hr", "hrv"])
@pytest.mark.parametrize("finish_command", ["publish", "resume"])
def test_daily_first_empty_channel_stops_source_and_allows_verified_publication(
    monkeypatch, capsys, empty_stream, finish_command
):
    system = daily_system(monkeypatch)
    job = system.job
    day = "2026-01-26"
    positive_stream = "hrv" if empty_stream == "hr" else "hr"
    physical_send = requests.adapters.HTTPAdapter.send
    empty_reads = []

    def unresolved_channel(adapter, request, **kwargs):
        response = physical_send(adapter, request, **kwargs)
        path = urlsplit(request.url).path
        selected = "dailyHeartRate" in path if empty_stream == "hr" else "hrv-service" in path
        if system.source_calls[-1][0] == day and selected:
            value = json.loads(response.content)
            value["heartRateValues" if empty_stream == "hr" else "hrvReadings"] = []
            response._content = json.dumps(value).encode()
            response.headers["Content-Length"] = str(len(response._content))
            empty_reads.append(day)
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", unresolved_channel)
    argv = ["--state", str(job.folder), "daily", "--plan-sha", job.plan_sha,
            "--confirm-cloud-jobs-paused", "--confirm-r2-write", "--archive-reload",
            "--max-dates", "2", "--max-http", "16", "--max-wait-seconds", "0"]
    native = history_backfill.main(argv)
    report = json.loads(capsys.readouterr().out)
    attempted = list(dict.fromkeys(date for date, _, _ in system.source_calls))
    daily_metrics = {"case": empty_stream, "stage": "daily", "native": native,
                      "dates": attempted, "physical_HTTP": len(system.source_calls),
                      "reloads": sum(method == "POST" for _, method, _ in system.source_calls),
                      "child_native": system.native_results,
                      "daily_status": report["daily_status"]}
    with capsys.disabled():
        print(json.dumps(daily_metrics, sort_keys=True))
    assert attempted == [day], "Unresolved first channel must stop before the next source date"
    assert native == 2 and report["daily_status"] == "source_stopped"
    assert report["daily_source_stop"] == "empty_unresolved"
    assert report["pause"] == "no_more_observed" and report["not_before"] is None
    assert report["daily_HTTP"] == 7 and report["daily_new_dates"] == 1
    assert len(system.source_calls) == 7 and empty_reads == [day, day]
    assert system.native_results == [0]  # Actual date completion; never invent a failed native.
    assert not report["locked"]

    source_receipts = {
        name: body for name, body in system.files.items()
        if name.endswith("/RECEIPT.json") and "baseline_refresh" not in json.loads(body)
    }
    assert len(source_receipts) == 1
    receipt_name, receipt_bytes = next(iter(source_receipts.items()))
    receipt = json.loads(receipt_bytes)
    assert receipt["status"] == "complete" and receipt["HTTP"] == 7
    assert receipt["reload_intent"] == receipt["reload_ACK"] == 1
    assert {row["stream"]: row["status"] for row in receipt["targets"]} == {
        empty_stream: "empty_unresolved", positive_stream: "ready",
    }
    positive = next(row for row in receipt["targets"] if row["stream"] == positive_stream)
    assert positive["source_verified"] is True
    folder = receipt_name.rsplit("/", 1)[0]
    preserved = {receipt_name: receipt_bytes, folder + "/RESULT.json":
                 system.files[folder + "/RESULT.json"]}
    for field in ("wire", "view"):
        name = folder + "/" + positive[field]
        preserved[name] = system.files[name]
        assert digest(preserved[name]) == positive[field + "_sha256"]
    assert json.loads(preserved[folder + "/RESULT.json"]) == {
        "native": 0, "receipt_sha256": digest(receipt_bytes),
    }
    usage = rolling_usage(job)
    assert usage["HTTP"] == 7 and usage["reloads"] == 1
    assert not usage["unknown_reservations"]

    # Qualified stored-data finishing happens even though further source is stopped.
    first_rows = {(row["date"], row["stream"]): row for row in job.snapshot()["rows"]}
    assert first_rows[day, positive_stream]["status"] == "published"
    positive_key = key(positive_stream, day)
    raw_puts = system.server.puts.count(positive_key)
    assert raw_puts == 1
    assert report["daily_publication_phases"] >= 1

    # Reopening the durable job cannot silently advance past the unresolved day.
    reopened = Job(job.folder)
    assert reopened.status()["pause"] == "no_more_observed"
    calls_before = list(system.source_calls)
    assert history_backfill.main(argv) == 2
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["daily_status"] == "source_stopped"
    assert resumed["daily_round"] == report["daily_round"]
    assert system.source_calls == calls_before
    assert system.server.puts.count(positive_key) == raw_puts

    if finish_command == "resume":
        # Only an explicit operator resume admits the untouched next selected date.
        resumed_native = history_backfill.main([*argv, "--resume"])
        resumed_output = capsys.readouterr()
        assert resumed_output.out, resumed_output.err
        explicit = json.loads(resumed_output.out)
        assert explicit["daily_source_stop"] is None
        assert explicit["daily_status"] != "source_stopped"
        assert explicit["daily_round"] == report["daily_round"]
        assert explicit["daily_HTTP"] == 14 and explicit["daily_new_dates"] == 2
        assert list(dict.fromkeys(date for date, _, _ in system.source_calls)) == [day, "2026-01-25"]
        assert len(system.source_calls) == 14 and system.native_results == [0, 0]
        assert explicit["pause"] is None
        assert all(system.files[name] == body for name, body in preserved.items())
        assert system.server.puts.count(positive_key) == raw_puts
        print(json.dumps({"case": empty_stream, "stage": "explicit_resume",
                          "native": resumed_native, "physical_HTTP": len(system.source_calls),
                          "daily_status": explicit["daily_status"],
                          "child_native": system.native_results}, sort_keys=True))
        return

    # The ordinary explicit R2-only CLI remains usable while source is paused.
    assert history_backfill.main([
        "--state", str(job.folder), "publish", "--plan-sha", job.plan_sha,
        "--confirm-r2-write",
    ]) == 0
    published = json.loads(capsys.readouterr().out)
    rows = {(row["date"], row["stream"]): row for row in reopened.snapshot()["rows"]}
    assert rows[day, positive_stream]["status"] == "published"
    assert rows[day, empty_stream]["status"] == "empty_unresolved"
    assert rows["2026-01-25", "hr"]["status"] == "pending"
    assert rows["2026-01-25", "hrv"]["status"] == "pending"
    assert system.source_calls == calls_before and system.native_results == [0]
    assert all(system.files[name] == body for name, body in preserved.items())
    canonical = decode(system.server.objects[key(positive_stream, day)][0])
    assert canonical["date"] == day
    if positive_stream == "hr":
        channel = canonical["channels"]["heart_rate"]
        assert [row["value"] for row in channel["readings"]] == [55]
        assert channel["missing_value_count"] == 1
    else:
        assert [row["hrv_ms"] for row in canonical["readings"]] == [45]
        assert canonical["archive_hrv_quality_v1"]["missing_value_count"] == 1
    assert published["pause"] == "no_more_observed" and not published["locked"]
    with capsys.disabled():
        print(json.dumps({"case": empty_stream, "stage": "publish", "native": 0,
                          "physical_HTTP": len(system.source_calls),
                          "physical_R2": dict(transport_counts(system.server.calls)),
                          "baseline_status": published["baseline_refresh"]["status"],
                          "source_receipt_sha256": digest(receipt_bytes)}, sort_keys=True))

