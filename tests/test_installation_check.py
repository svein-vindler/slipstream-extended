import json
from pathlib import Path
from urllib.error import HTTPError

import pytest

from scripts.check_installation import check


def test_offline_check_never_runs_cloud_commands(tmp_path):
    report = check(root=tmp_path, execute=lambda *a, **kw: pytest.fail("Unexpected cloud command"))
    assert report["ready_for_authenticated_client_test"] is False
    assert any(row["step"] == "cloud_checks" and row["status"] == "not_checked" for row in report["checks"])


def test_online_checks_are_read_only_and_report_has_no_account_values(tmp_path, monkeypatch):
    (tmp_path / "worker/node_modules/wrangler/bin").mkdir(parents=True)
    (tmp_path / "worker/node_modules/wrangler/bin/wrangler.js").touch()
    monkeypatch.setattr("scripts.check_installation.shutil.which", lambda name: name)
    monkeypatch.setattr("scripts.check_installation.importlib.util.find_spec", lambda name: True)
    commands = []
    def execute(args, **kwargs):
        commands.append(args)
        if "whoami" in args:
            return b'{"accounts":[{"id":"private-account"}]}'
        if "secret" in args:
            return json.dumps([{"name": name} for name in ("ACCESS_AUD", "ACCESS_TEAM_DOMAIN", "MCP_HOSTNAME")]).encode()
        if "object" in args:
            Path(args[-1]).write_bytes(b"private-fitness-value")
            return b"private-account-command-output"
        if "--jq" in args:
            return b'["GARMINTOKENS","CLOUDFLARE_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"]'
        return b'{"state":"active"}'
    class Opener:
        def open(self, *args, **kwargs):
            raise HTTPError("https://synthetic.example/mcp", 401, "denied", {}, None)
    report = check(online=True, repo="synthetic/example", worker_url="https://synthetic.example",
                   root=tmp_path, execute=execute, opener=Opener())
    assert report["ready_for_authenticated_client_test"] is True
    assert all(not set(args) & {"put", "delete", "deploy", "create", "enable", "disable"} for args in commands)
    serialized = json.dumps(report)
    assert all(value not in serialized for value in ("private-account", "private-fitness-value", "synthetic/example", "synthetic.example"))
    assert len(report["client_checks"]) == 2


def test_provider_error_is_actionable_without_echoing_private_error(tmp_path):
    def fail(*a, **kw):
        raise RuntimeError("private-error-and-account")
    report = check(online=True, root=tmp_path, execute=fail)
    assert any(row["status"] == "needs_check" for row in report["checks"])
    assert "private-error-and-account" not in json.dumps(report)


@pytest.mark.parametrize("kwargs", [{"repo": "unsafe;command"}, {"worker_url": "http://synthetic.example"},
                                   {"worker_url": "https://secret@synthetic.example"}, {"bucket": "../unsafe"}])
def test_invalid_targets_rejected_before_commands(kwargs):
    with pytest.raises(ValueError):
        check(online=True, execute=lambda *a, **kw: pytest.fail("Unexpected IO"), **kwargs)
