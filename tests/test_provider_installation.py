"""Installation accepts only selected credentials and keeps account bindings private."""

import json
from pathlib import Path

from scripts.check_installation import check


def test_personal_installation_check_uses_actual_repo_mode_without_legacy_secret(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("scripts.check_installation.shutil.which", lambda name: name)
    monkeypatch.setattr("scripts.check_installation.importlib.util.find_spec", lambda name: True)

    def execute(args, **kwargs):
        target = " ".join(args)
        if target.endswith("actions/variables"):
            return b'{"variables":[{"name":"GARMIN_PROVIDER","value":"personal"}]}'
        if "actions/secrets" in target:
            return json.dumps(
                [
                    "GARMIN_PERSONAL_API_KEY",
                    "GARMIN_ACCOUNT",
                    "CLOUDFLARE_ACCOUNT_ID",
                    "R2_ACCESS_KEY_ID",
                    "R2_SECRET_ACCESS_KEY",
                ]
            ).encode()
        if "object" in args:
            Path(args[-1]).write_bytes(b"synthetic")
            return b""
        if "whoami" in args:
            return b'{"accounts":[{"id":"synthetic"}]}'
        if "secret" in args:
            return b"[]"
        return b'{"state":"active"}'

    report = check(root=tmp_path, online=True, repo="synthetic/repo", execute=execute)
    assert report["garmin_provider"] == "personal"
    assert next(r for r in report["checks"] if r["step"] == "github_secrets")["status"] == "passed"
    assert report["garmin_capabilities"]["activity_detail"] is False


def test_all_source_workflows_select_only_the_configured_secret_and_private_account():
    root = Path(__file__).resolve().parents[1]
    names = [
        "activity-backfill",
        "activity-refresh",
        "granular-pilot",
        "refresh",
        "scheduled-activity-backfill",
        "scheduled-health-detail-backfill",
        "scheduled-hrv-backfill",
    ]
    for name in names:
        text = (root / ".github/workflows" / (name + ".yml")).read_text()
        assert "GARMIN_PROVIDER: ${{ vars.GARMIN_PROVIDER || 'legacy' }}" in text
        assert "GARMIN_ACCOUNT: ${{ secrets.GARMIN_ACCOUNT }}" in text
        assert "vars.GARMIN_ACCOUNT" not in text
        assert "vars.GARMIN_PROVIDER == 'personal' && secrets.GARMIN_PERSONAL_API_KEY" in text
        assert "vars.GARMIN_PROVIDER == 'legacy') && secrets.GARMINTOKENS" in text


def test_private_local_provider_metadata_is_ignored():
    root = Path(__file__).resolve().parents[1]
    text = (root / ".gitignore").read_text()
    assert ".garmin-provider.local.json*" in text
