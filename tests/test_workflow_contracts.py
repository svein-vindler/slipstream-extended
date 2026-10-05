from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_all_refresh_modes_use_one_core_with_existing_input_mapping():
    workflow = (ROOT / ".github/workflows/refresh.yml").read_text(encoding="utf-8")
    assert "python -m pipeline.refresh --workflow" in workflow
    assert "group: garmin-sync" in workflow
    assert 'cron: "0 */6 * * *"' in workflow
    for name in ("latest_activity_only", "latest_night_only", "include_granular",
                 "sync_request_id", "latest_wake_date", "latest_activity_id",
                 "latest_r2_only", "health_start", "health_end",
                 "backfill_start_year", "backfill_end_year"):
        assert "${{ inputs." + name + " }}" in workflow
    assert "${{ github.event_name }}" in workflow
    assert "pipeline.latest_night" not in workflow
    assert "pipeline.refresh_summaries" not in workflow


def test_upstream_check_is_notification_only_for_independent_history():
    workflow = (ROOT / ".github/workflows/upstream-sync.yml").read_text(encoding="utf-8")

    assert "git ls-remote" in workflow
    assert ".upstream-version" in workflow
    assert "issues: write" in workflow
    assert "git merge" not in workflow
    assert "git push" not in workflow


def test_upstream_issue_body_stays_inside_yaml_run_literal():
    workflow = (ROOT / ".github/workflows/upstream-sync.yml").read_text(encoding="utf-8")
    lines = workflow.splitlines()
    body_start = lines.index('          body="$(cat <<EOF')
    body_end = lines.index('          )"', body_start)

    assert all(
        not line.strip() or line.startswith("          ")
        for line in lines[body_start : body_end + 1]
    )


def test_manual_health_index_build_retains_full_default_and_can_exercise_scheduled_policy():
    workflow = (ROOT / ".github/workflows/health-history-index.yml").read_text(encoding="utf-8")
    assert "default: full" in workflow and "options: [full, scheduled]" in workflow
    assert "args+=(--scheduled)" in workflow
    assert '"$RECENT_MONTHS" != "0"' in workflow
    assert "group: garmin-sync" in workflow
