from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_on_demand_refresh_generates_coach_after_activity_artifacts():
    workflow = (ROOT / ".github/workflows/refresh.yml").read_text(encoding="utf-8")
    restore = workflow.index("- name: Restore current summaries from private R2")
    snapshot = workflow.index("- name: Remember activities already stored")
    fetch = workflow.index("- name: Fetch + write data/")
    import_workouts = workflow.index("- name: Import newly discovered workouts")

    assert restore < snapshot < fetch < import_workouts
    assert "python -m pipeline.manual_activity_refresh process" in workflow
    assert "github.event_name == 'workflow_dispatch' && inputs.include_granular" in workflow


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


def test_targeted_night_excludes_broad_fetch_export_and_backfill():
    workflow = (ROOT / ".github/workflows/refresh.yml").read_text(encoding="utf-8")
    broad_names = ["Fetch + write data/", "Upload summaries", "Refresh recent detailed sleep",
                   "Import newly discovered workouts",
                   "Refresh recent body composition"]
    steps = workflow.split("      - name: ")
    for name in broad_names:
        step = next(step for step in steps if step.startswith(name))
        condition = next(line for line in step.splitlines() if "if:" in line)
        assert "!inputs.latest_activity_only && !inputs.latest_night_only" in condition
    assert 'python -m pipeline.latest_night "${args[@]}"' in workflow
    assert "group: garmin-sync" in workflow
    assert 'cron: "0 */6 * * *"' in workflow
    reconciliation = next(step for step in steps if step.startswith("Reconcile health history"))
    assert "github.event_name == 'schedule'" in next(line for line in reconciliation.splitlines() if "if:" in line)
    assert "python -m pipeline.health_history_index --scheduled" in reconciliation


def test_manual_health_index_build_retains_full_default_and_can_exercise_scheduled_policy():
    workflow = (ROOT / ".github/workflows/health-history-index.yml").read_text(encoding="utf-8")
    assert "default: full" in workflow and "options: [full, scheduled]" in workflow
    assert "args+=(--scheduled)" in workflow
    assert '"$RECENT_MONTHS" != "0"' in workflow
    assert "group: garmin-sync" in workflow


def test_general_body_refresh_uses_bounded_dayviews_without_historical_backfill():
    workflow = (ROOT / ".github/workflows/refresh.yml").read_text(encoding="utf-8")
    body = next(step for step in workflow.split("      - name: ") if step.startswith("Refresh recent body composition"))
    assert "pipeline.recent_body --max-days 3 --summary data/health_daily.csv" in body
    assert "health_detail_backfill" not in workflow
    assert "github.event_name == 'schedule'" in body
    assert "!inputs.latest_activity_only && !inputs.latest_night_only" in body


def test_general_summary_refresh_exports_before_checkpointing_and_retains_explicit_backfill():
    workflow = (ROOT / ".github/workflows/refresh.yml").read_text(encoding="utf-8")
    assert "python -m pipeline.refresh_summaries --data-dir data" in workflow
    assert 'python -m pipeline.fetch --skip-activities' in workflow
    upload = next(step for step in workflow.split("      - name: ") if step.startswith("Upload summaries"))
    assert "inputs.health_start || inputs.health_end || inputs.backfill_start_year || inputs.backfill_end_year" in upload
    assert "!inputs.latest_activity_only && !inputs.latest_night_only" in upload
