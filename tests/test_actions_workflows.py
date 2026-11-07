"""The actual six scheduled writers retain controls around the common entry."""

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("refresh.yml", "0 */6 * * *", "--workflow", None, "250", "536870912"),
    ("activity-refresh.yml", "15 23 * * *", '"${args[@]}"', 30, "250", "536870912"),
    ("scheduled-activity-backfill.yml", "30 2,8,14,20 * * *", '--max-activities "$MAX_ACTIVITIES"', 30, "250", "536870912"),
    ("scheduled-health-detail-backfill.yml", "15 1,7,13,19 * * *", '"${args[@]}"', 30, "250", "536870912"),
    ("scheduled-hrv-backfill.yml", "30 4,10,16,22 * * *", '"${args[@]}"', 30, "250", "536870912"),
    ("coach-input-backfill.yml", "40 2,8,14,20 * * *", '--max-activities "$MAX_ACTIVITIES"', 20, "60", "67108864"),
]


@pytest.mark.parametrize(("name", "cron", "arguments", "timeout", "writes", "byte_limit"), CASES)
def test_scheduled_writer_uses_common_receipt_with_original_arguments_and_controls(
    name, cron, arguments, timeout, writes, byte_limit,
):
    text = (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
    invocation = (
        f'python -m pipeline.actions_receipt --workflow {name} '
        f'--receipt "$RUNNER_TEMP/actions-run-receipt.json" -- {arguments}'
    )
    assert invocation in text
    assert text.count("python -m pipeline.actions_receipt") == 1
    assert 'ACTIONS_CRON_EXPRESSION: ${{ github.event.schedule }}' in text
    assert f'cron: "{cron}"' in text and "workflow_dispatch:" in text
    assert "permissions:\n  contents: read\n" in text
    assert "group: garmin-sync\n  cancel-in-progress: false" in text
    assert f'R2_MAX_WRITES_PER_RUN: "{writes}"' in text
    assert f'R2_MAX_WRITE_BYTES_PER_RUN: "{byte_limit}"' in text
    assert 'R2_MAX_BUCKET_BYTES: "5368709120"' in text
    assert 'R2_MAX_BUCKET_OBJECTS: "100000"' in text
    assert re.findall(r"uses: ([^\s]+)", text) == [
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/setup-python@6d4fd22b20465ccc0f7354476588e8df7c9ca0e0",
    ]
    assert (f"timeout-minutes: {timeout}" in text) if timeout else "timeout-minutes:" not in text
    assert "pip install -r requirements.txt" in text
    assert "workflow_run:" in text if name == "coach-input-backfill.yml" else "workflow_run:" not in text


def test_manual_shell_validation_and_conditional_coach_trigger_are_retained():
    activity = (ROOT / ".github/workflows/activity-refresh.yml").read_text()
    assert 'args=(--lookback-days "$LOOKBACK_DAYS" --max-activities 50)' in activity
    assert '[[ "$ACTIVITY_ID" =~ ^(garmin-)?[0-9]{1,20}$ ]]' in activity
    assert 'args+=(--activity-id "${ACTIVITY_ID#garmin-}")' in activity
    assert 'args+=(--force)' in activity and 'force requires activity_id.' in activity
    health = (ROOT / ".github/workflows/scheduled-health-detail-backfill.yml").read_text()
    assert 'args+=(--repair-body-start "$REPAIR_BODY_START" --repair-body-end "$REPAIR_BODY_END")' in health
    assert 'args+=(--retry-failures)' in health
    coach = (ROOT / ".github/workflows/coach-input-backfill.yml").read_text()
    assert "github.event.workflow_run.conclusion == 'success'" in coach
    assert "github.event.workflow_run.event != 'workflow_dispatch'" in coach
    assert "workflows:\n      - Refresh fitness data" in coach


def test_shared_receipt_validation_omits_untrusted_timestamp_from_failure():
    from pipeline.actions_receipt_schema import validate_receipt

    receipt = json.loads((ROOT / "tests/fixtures/actions_run/receipt.json").read_text())
    receipt["started_at"] = "SYNTHETIC_PRIVATE_TIMESTAMP_MARKER"
    with pytest.raises(ValueError) as caught:
        validate_receipt(receipt)
    assert str(caught.value) == "Invalid run receipt"
