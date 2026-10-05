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
                   "Refresh recent detailed HRV", "Import newly discovered workouts",
                   "Reconcile health history"]
    steps = workflow.split("      - name: ")
    for name in broad_names:
        step = next(step for step in steps if step.startswith(name))
        condition = next(line for line in step.splitlines() if "if:" in line)
        assert "!inputs.latest_activity_only && !inputs.latest_night_only" in condition
    assert 'python -m pipeline.latest_night "${args[@]}"' in workflow
    assert "group: garmin-sync" in workflow
    assert 'cron: "0 */6 * * *"' in workflow
