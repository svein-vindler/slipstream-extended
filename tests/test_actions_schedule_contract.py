"""Offline contracts at the committed Actions schedule/entry boundary."""

import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
MANUAL_INPUTS = {
    "activity-backfill.yml": ["start_date", "end_date", "max_activities", "retry_failures"],
    "activity-refresh.yml": ["activity_id", "force", "lookback_days"],
    "check-garmin-connection.yml": [],
    "coach-input-backfill.yml": ["max_activities"],
    "granular-pilot.yml": ["hrv_days", "activity_days"],
    "health-history-index.yml": ["stream", "mode", "start_date", "end_date", "recent_months"],
    "refresh.yml": [
        "health_start", "health_end", "backfill_start_year", "backfill_end_year",
        "include_granular", "latest_activity_only", "latest_night_only", "latest_wake_date",
        "sync_request_id", "latest_r2_only", "latest_activity_id", "latest_new_activity_expected",
        "latest_expected_date",
    ],
    "scheduled-activity-backfill.yml": ["max_activities"],
    "scheduled-health-detail-backfill.yml": [
        "max_sleep_days", "max_body_days", "retry_failures", "repair_body_start", "repair_body_end",
    ],
    "scheduled-hrv-backfill.yml": ["max_days", "retry_failures"],
}


def workflow(name):
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def schedules(name):
    text = workflow(name)
    block = re.search(r"(?ms)^  schedule:\n(.*?)(?=^  \w|^\S|\Z)", text)
    if block is None:
        return []
    return re.findall(
        r'    - cron: "([^"]+)"[^\n]*\n(?:      timezone: "([^"]+)"\n)?',
        block.group(1),
    )


def nominal_occurrences(entries, local_date):
    """Evaluate only the fixed minute/hour/weekday forms used by these jobs."""
    result = []
    for cron, zone in entries:
        minute, hours, day, month, weekdays = cron.split()
        assert day == month == "*"
        local = datetime.fromisoformat(local_date).replace(tzinfo=ZoneInfo(zone or "UTC"))
        weekday = (local.weekday() + 1) % 7
        if weekdays == "*":
            matches = True
        elif "-" in weekdays:
            start, end = map(int, weekdays.split("-"))
            matches = start <= weekday <= end
        else:
            matches = weekday in map(int, weekdays.split(","))
        if matches:
            result.extend(
                local.replace(hour=int(hour), minute=int(minute)).astimezone(timezone.utc)
                for hour in hours.split(",")
            )
    return sorted(result)


@pytest.mark.parametrize("local_date,expected_utc", [
    ("2026-01-12", ["2026-01-12T02:15+00:00", "2026-01-12T07:00+00:00"]),
    ("2026-01-17", ["2026-01-17T02:15+00:00", "2026-01-17T09:00+00:00"]),
    ("2026-07-13", ["2026-07-13T01:15+00:00", "2026-07-13T06:00+00:00"]),
    ("2026-07-18", ["2026-07-18T01:15+00:00", "2026-07-18T08:00+00:00"]),
    ("2026-03-29", ["2026-03-29T01:15+00:00", "2026-03-29T08:00+00:00"]),
    ("2026-10-25", ["2026-10-25T02:15+00:00", "2026-10-25T09:00+00:00"]),
])
def test_refresh_has_one_broad_run_and_one_local_morning(local_date, expected_utc):
    entries = schedules("refresh.yml")
    assert all(zone == "Europe/Oslo" for _, zone in entries)
    assert nominal_occurrences(entries, local_date) == [
        datetime.fromisoformat(value) for value in expected_utc
    ]


@pytest.mark.parametrize("local_date,expected_utc", [
    ("2026-01-12", ["2026-01-12T11:00+00:00", "2026-01-12T17:00+00:00"]),
    ("2026-01-17", ["2026-01-17T11:00+00:00", "2026-01-17T17:00+00:00"]),
    ("2026-07-13", ["2026-07-13T10:00+00:00", "2026-07-13T16:00+00:00"]),
    ("2026-07-18", ["2026-07-18T10:00+00:00", "2026-07-18T16:00+00:00"]),
    ("2026-03-29", ["2026-03-29T10:00+00:00", "2026-03-29T16:00+00:00"]),
    ("2026-10-25", ["2026-10-25T11:00+00:00", "2026-10-25T17:00+00:00"]),
])
def test_activity_checks_run_at_local_noon_and_evening(local_date, expected_utc):
    entries = schedules("activity-refresh.yml")
    assert all(zone == "Europe/Oslo" for _, zone in entries)
    assert nominal_occurrences(entries, local_date) == [
        datetime.fromisoformat(value) for value in expected_utc
    ]


def test_public_historical_schedules_and_manual_entries_remain_independent():
    for name, expected in (
        ("scheduled-activity-backfill.yml", [("30 2,8,14,20 * * *", "")]),
        ("scheduled-health-detail-backfill.yml", [
            ("0 8 * * 1-5", "Europe/Oslo"), ("0 10 * * 0,6", "Europe/Oslo"),
        ]),
        ("scheduled-hrv-backfill.yml", [
            ("0 8 * * 1-5", "Europe/Oslo"), ("0 10 * * 0,6", "Europe/Oslo"),
        ]),
        ("coach-input-backfill.yml", [("40 2,8,14,20 * * *", "")]),
    ):
        assert schedules(name) == expected, name
        assert "  workflow_dispatch:\n    inputs:\n" in workflow(name)


@pytest.mark.parametrize("name,inputs", MANUAL_INPUTS.items())
def test_manual_inputs_and_shared_writer_queue_are_preserved(name, inputs):
    text = workflow(name)
    assert "  workflow_dispatch:" in text
    assert re.findall(r"^      (\w+):$", text, re.MULTILINE) == inputs
    concurrency = re.search(r"(?ms)^concurrency:\n(.*?)(?=^\S)", text).group(1)
    assert re.findall(r"^  (group|cancel-in-progress|queue): (.+)$", concurrency, re.MULTILINE) == [
        ("group", "garmin-sync"), ("cancel-in-progress", "false"), ("queue", "max"),
    ]


def test_refresh_cron_event_and_manual_modes_reach_the_existing_receipted_entry():
    text = workflow("refresh.yml")
    assert "ACTIONS_CRON_EXPRESSION: ${{ github.event.schedule }}" in text
    assert "REFRESH_EVENT: ${{ github.event_name }}" in text
    for variable, name in (
        ("ACTIVITY_ONLY", "latest_activity_only"),
        ("NIGHT_ONLY", "latest_night_only"),
        ("INCLUDE_GRANULAR", "include_granular"),
    ):
        assert f"{variable}: ${{{{ inputs.{name} }}}}" in text
    assert (
        'run: python -m pipeline.actions_receipt --workflow refresh.yml '
        '--receipt "$RUNNER_TEMP/actions-run-receipt.json" -- --workflow'
    ) in text


@pytest.mark.parametrize("name", [
    "activity-refresh.yml", "scheduled-activity-backfill.yml",
    "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml", "coach-input-backfill.yml",
])
def test_historical_and_activity_entries_keep_the_receipt_identity(name):
    text = workflow(name)
    assert "ACTIONS_CRON_EXPRESSION: ${{ github.event.schedule }}" in text
    assert (
        f'python -m pipeline.actions_receipt --workflow {name} '
        '--receipt "$RUNNER_TEMP/actions-run-receipt.json" -- '
    ) in text
    assert "${{ inputs." not in "\n".join(
        line for line in text.splitlines() if line.startswith("          ")
    )


def test_coach_keeps_its_existing_completed_workflow_sources():
    text = workflow("coach-input-backfill.yml")
    sources = re.search(r"(?ms)^  workflow_run:\n(.*?)(?=^  \w)", text).group(1)
    assert re.findall(r"^      - (.+)$", sources, re.MULTILINE) == [
        "Refresh fitness data", "Refresh changed Garmin activities",
        "Scheduled Garmin activity backfill", "Backfill granular Garmin activities",
    ]
    assert "types: [completed]" in sources
    for name, expected in (
        ("refresh.yml", "Refresh fitness data"),
        ("activity-refresh.yml", "Refresh changed Garmin activities"),
        ("scheduled-activity-backfill.yml", "Scheduled Garmin activity backfill"),
        ("activity-backfill.yml", "Backfill granular Garmin activities"),
    ):
        assert workflow(name).splitlines()[0] == f"name: {expected}"
