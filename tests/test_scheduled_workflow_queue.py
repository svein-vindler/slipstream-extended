"""Actual workflow configuration at GitHub's documented concurrency boundary.

The small external-service model covers already-created runs only. It is not a
GitHub scheduler emulator and cannot prove delivery of a hosted cron event.
GitHub's contract: one running run; queue=single replaces pending work, while
queue=max retains up to 100 pending runs. All names/configuration come from the
repository files; expected completed work is a caller-visible literal.
"""

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
MORNING_WORKFLOWS = (
    "refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
)


def concurrency(workflow):
    """Read the literal concurrency mapping used by these workflow fixtures."""
    text = (WORKFLOWS / workflow).read_text(encoding="utf-8")
    block = re.search(r"(?ms)^concurrency:\n(.*?)(?=^\S|\Z)", text)
    assert block, f"Missing workflow concurrency in {workflow}"
    return dict(re.findall(r"^  ([a-z-]+): ([^\n#]+)", block.group(1), re.MULTILINE))


@dataclass
class CreatedRunQueue:
    """Documented GitHub boundary, with explicit waiting order in this fixture."""

    running: str | None = None
    pending: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)

    def arrive(self, run_id, settings):
        assert settings["group"].strip().lower() == "garmin-sync"
        assert settings["cancel-in-progress"].strip() == "false"
        policy = settings.get("queue", "single").strip()
        assert policy in {"single", "max"}
        if self.running is None:
            self.running = run_id
        elif policy == "single":
            self.cancelled.extend(self.pending)
            self.pending[:] = [run_id]
        elif len(self.pending) < 100:
            self.pending.append(run_id)
        else:
            self.cancelled.append(run_id)

    def finish_running(self):
        assert self.running is not None
        self.completed.append(self.running)
        self.running = self.pending.pop(0) if self.pending else None

    def drain(self):
        while self.running is not None:
            self.finish_running()


def test_three_created_morning_runs_all_complete_without_pending_replacement():
    queue = CreatedRunQueue()
    for workflow in MORNING_WORKFLOWS:
        queue.arrive(workflow, concurrency(workflow))
    queue.drain()

    assert queue.completed == [
        "refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
    ]
    assert queue.cancelled == []


def shared_group_members():
    return [path.name for path in sorted(WORKFLOWS.glob("*.yml"))
            if re.search(r"^  group: garmin-sync$", path.read_text(), re.MULTILINE)]


@pytest.mark.parametrize("arriving_workflow", shared_group_members())
def test_every_shared_group_member_retains_waiting_morning_work(arriving_workflow):
    queue = CreatedRunQueue()
    for workflow in MORNING_WORKFLOWS:
        queue.arrive(workflow, concurrency(workflow))
    queue.arrive("additional-created-run", concurrency(arriving_workflow))
    queue.drain()

    assert queue.completed == [
        "refresh.yml", "scheduled-health-detail-backfill.yml", "scheduled-hrv-backfill.yml",
        "additional-created-run",
    ]
    assert queue.cancelled == []


def test_full_documented_queue_rejects_new_run_without_displacing_waiting_work():
    queue = CreatedRunQueue()
    settings = concurrency("refresh.yml")
    queue.arrive("running", settings)
    for number in range(100):
        queue.arrive(f"waiting-{number}", settings)
    queue.arrive("over-capacity", settings)

    assert queue.running == "running"
    assert len(queue.pending) == 100
    assert queue.cancelled == ["over-capacity"]
    queue.drain()
    assert len(queue.completed) == 101
    assert queue.completed[1:4] == ["waiting-0", "waiting-1", "waiting-2"]
    assert queue.completed[-1] == "waiting-99"


def boolean_expression(node):
    """Closed adapter for the actual Coach condition; never evaluate Python code."""
    if isinstance(node, ast.Constant) and type(node.value) in {str, bool, type(None)}:
        return node.value
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(boolean_expression(value) for value in node.values)
        if isinstance(node.op, ast.Or):
            return any(boolean_expression(value) for value in node.values)
    if isinstance(node, ast.Compare) and len(node.ops) == 1:
        left = boolean_expression(node.left)
        right = boolean_expression(node.comparators[0])
        if isinstance(node.ops[0], ast.Eq):
            return left == right
        if isinstance(node.ops[0], ast.NotEq):
            return left != right
    raise AssertionError("Coach condition left the qualified expression subset")


def coach_accepts(event, upstream_name=None, upstream_event=None, conclusion=None):
    text = (WORKFLOWS / "coach-input-backfill.yml").read_text()
    if event == "workflow_run":
        trigger = re.search(r"(?ms)^  workflow_run:\n(.*?)(?=^  \S)", text).group(1)
        watched = re.findall(r"^      - ([^\n]+)$", trigger, re.MULTILINE)
        assert "    types: [completed]" in trigger
        if upstream_name not in watched:
            return False
    else:
        assert event in {"schedule", "workflow_dispatch"}
        assert f"  {event}:" in text

    block = re.search(r"(?ms)^    if: >-\n(.*?)(?=^    \S)", text).group(1)
    expression = " ".join(line.strip() for line in block.splitlines()).strip()
    assert expression.startswith("${{") and expression.endswith("}}")
    expression = expression[3:-2].strip()
    values = {
        "github.event_name": event,
        "github.event.workflow_run.conclusion": conclusion,
        "github.event.workflow_run.name": upstream_name,
        "github.event.workflow_run.event": upstream_event,
    }
    for field_name, value in values.items():
        expression = expression.replace(field_name, repr(value))
    expression = expression.replace("&&", " and ").replace("||", " or ")
    return boolean_expression(ast.parse(expression, mode="eval").body)


@pytest.mark.parametrize(("event", "upstream_name", "upstream_event", "conclusion", "expected"), [
    ("schedule", None, None, None, True),
    ("workflow_dispatch", None, None, None, True),
    ("workflow_run", "Refresh fitness data", "schedule", "success", True),
    ("workflow_run", "Refresh fitness data", "workflow_dispatch", "success", False),
    ("workflow_run", "Refresh changed Garmin activities", "schedule", "success", True),
    ("workflow_run", "Refresh changed Garmin activities", "workflow_dispatch", "success", True),
    ("workflow_run", "Scheduled Garmin activity backfill", "schedule", "success", True),
    ("workflow_run", "Backfill granular Garmin activities", "workflow_dispatch", "success", True),
    ("workflow_run", "Refresh changed Garmin activities", "schedule", "failure", False),
    ("workflow_run", "Refresh changed Garmin activities", "schedule", "cancelled", False),
    ("workflow_run", "Refresh changed Garmin activities", "schedule", "skipped", False),
    ("workflow_run", "Scheduled Garmin health detail backfill", "schedule", "success", False),
    ("workflow_run", "Unrelated workflow", "schedule", "success", False),
])
def test_actual_coach_event_condition_keeps_only_expected_consumers(
    event, upstream_name, upstream_event, conclusion, expected,
):
    assert coach_accepts(event, upstream_name, upstream_event, conclusion) is expected


def test_successful_created_activity_completion_retains_morning_jobs_and_coach():
    queue = CreatedRunQueue()
    queue.arrive("activity-refresh", concurrency("activity-refresh.yml"))
    for workflow in MORNING_WORKFLOWS:
        queue.arrive(workflow, concurrency(workflow))
    queue.finish_running()
    if coach_accepts("workflow_run", "Refresh changed Garmin activities", "schedule", "success"):
        queue.arrive("coach-consumer", concurrency("coach-input-backfill.yml"))
    queue.drain()

    assert queue.completed == [
        "activity-refresh", "refresh.yml", "scheduled-health-detail-backfill.yml",
        "scheduled-hrv-backfill.yml", "coach-consumer",
    ]
    assert queue.cancelled == []


@pytest.mark.parametrize(("day", "expected_utc"), [
    ("2026-11-11", "2026-11-11T07:00:00Z"),  # winter weekday 08:00 Oslo
    ("2026-11-14", "2026-11-14T09:00:00Z"),  # winter Saturday 10:00 Oslo
    ("2026-03-29", "2026-03-29T08:00:00Z"),  # DST starts; Sunday 10:00 Oslo
    ("2026-03-30", "2026-03-30T06:00:00Z"),  # summer weekday 08:00 Oslo
    ("2026-10-25", "2026-10-25T09:00:00Z"),  # DST ends; Sunday 10:00 Oslo
    ("2026-10-26", "2026-10-26T07:00:00Z"),  # winter weekday again
])
def test_actual_reconciliation_cli_keeps_nominal_oslo_mornings_distinct_from_delivery(
    tmp_path, day, expected_utc,
):
    refs = []
    for index, workflow in enumerate(MORNING_WORKFLOWS):
        text = (WORKFLOWS / workflow).read_text()
        schedules = [
            {"cron_expression": cron, "timezone": zone or "UTC"}
            for cron, zone in re.findall(
                r'    - cron: "([^"\n]+)"[^\n]*\n(?:      timezone: "([^"\n]+)"\n)?', text,
            )
        ]
        body = json.dumps({"workflow": workflow, "schedules": schedules}).encode()
        name = f"config-{index}.json"
        (tmp_path / name).write_bytes(body)
        refs.append({"path": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    manifest = {
        "schema_version": 1, "kind": "actions-reconcile-input", "day_start": day, "day_end": day,
        "as_of": "2026-11-16T00:00:00Z", "observation_start": day + "T00:00:00Z",
        "observation_end": day + "T23:59:59Z", "delay_window_minutes": 120,
        "workflow_configs": refs, "runs": [], "jobs": [], "receipts": [],
        "trigger_evidence": [], "pauses": [],
    }
    input_path, output_path = tmp_path / "input.json", tmp_path / "output.json"
    input_path.write_text(json.dumps(manifest), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-B", "-X", "utf8", "-m", "pipeline.actions_reconcile",
         "--input", str(input_path), "--output", str(output_path)],
        cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == result.stderr == ""
    report = json.loads(output_path.read_text())
    assert [(item["workflow"], item["scheduled_for"]) for item in report["occurrences"]] == [
        ("refresh.yml", expected_utc),
        ("scheduled-health-detail-backfill.yml", expected_utc),
        ("scheduled-hrv-backfill.yml", expected_utc),
    ]
    assert {item["status"] for item in report["occurrences"]} == {"not_observed_after_window"}
    assert "absence_does_not_prove_github_dropped" in report["limitations"]
