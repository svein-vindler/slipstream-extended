"""Historical approved npm audit fixture and fail-closed policy regressions."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "worker_npm_audit", ROOT / "scripts/worker_npm_audit.py"
)
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


def fixture(platform="windows"):
    name = "npm-picomatch-audit-linux.json" if platform == "linux" else "npm-picomatch-audit.json"
    text = (ROOT / "tests/fixtures" / name).read_text(encoding="utf-8")
    lock_path = Path(os.environ.get(
        "NPM_AUDIT_TEST_LOCK", ROOT / "tests/fixtures/npm-picomatch-approved-lock.json"))
    return text, audit.strict_json(text), lock_path.read_bytes()


def test_historical_fixture_does_not_extend_acceptance_to_changed_current_lock():
    _text, report, historical_lock = fixture()
    assert hashlib.sha256(historical_lock).hexdigest() == audit.LOCK_SHA256
    current_lock = (ROOT / "worker/package-lock.json").read_bytes()
    assert current_lock != historical_lock
    with pytest.raises(ValueError, match="changed dependency lock"):
        audit.accepted_findings(report, current_lock, 1)


@pytest.mark.parametrize("platform,count", [("windows", 8), ("linux", 9)])
def test_actual_known_production_advisory_and_locked_chain_are_accepted(platform, count):
    _text, report, lock = fixture(platform)
    assert audit.strict_json(lock)["packages"]["node_modules/picomatch"].get("dev") is not True
    accepted = audit.accepted_findings(report, lock, 1)
    assert len(accepted) == count and accepted == sorted(report["vulnerabilities"])
    assert ("@vitest/mocker" in accepted) is (platform == "linux")


@pytest.mark.parametrize("fault", [
    "new-finding", "advisory", "advisory-source", "version", "lock-change", "node",
    "via", "severity", "error", "counts", "bool-count",
    "report-version", "fix-metadata", "dependency-metadata", "native",
])
def test_new_findings_versions_errors_and_malformed_reports_stay_fatal(fault):
    _text, report, lock = fixture()
    leaf = report["vulnerabilities"]["picomatch"]
    if fault == "new-finding":
        report["vulnerabilities"]["new-risk"] = copy.deepcopy(leaf)
        report["metadata"]["vulnerabilities"].update(high=9, total=9)
    elif fault == "advisory":
        leaf["via"][0]["url"] = "https://github.com/advisories/GHSA-new-risk"
    elif fault == "advisory-source":
        leaf["via"][0]["source"] = 1
    elif fault == "version":
        changed = audit.strict_json(lock)
        changed["packages"]["node_modules/picomatch"]["version"] = "4.0.8"
        lock = json.dumps(changed).encode()
    elif fault == "lock-change":
        lock += b"\n"
    elif fault == "node":
        leaf["nodes"] = ["node_modules/other/node_modules/picomatch"]
    elif fault == "via":
        report["vulnerabilities"]["agents"]["via"].append("vitest")
    elif fault == "severity":
        leaf["severity"] = "critical"
    elif fault == "error":
        report["error"] = {"code": "ENETWORK"}
    elif fault == "counts":
        report["metadata"]["vulnerabilities"]["total"] = 0
    elif fault == "bool-count":
        report["metadata"]["vulnerabilities"]["low"] = False
    elif fault == "report-version":
        report["auditReportVersion"] = 1
    elif fault == "fix-metadata":
        leaf["fixAvailable"] = {}
    elif fault == "dependency-metadata":
        report["metadata"]["dependencies"]["prod"] = "unknown"
    with pytest.raises((ValueError, TypeError, KeyError)):
        audit.accepted_findings(report, lock, 2 if fault == "native" else 1)


@pytest.mark.parametrize("text", ['{"x": 1, "x": 2}', '{"x": NaN}', "{bad"])
def test_malformed_duplicate_and_non_json_constants_fail(text):
    with pytest.raises(ValueError):
        audit.strict_json(text)


@pytest.mark.parametrize("platform", ["windows", "linux"])
def test_main_runs_full_audit_preserves_report_and_logs_honest_acceptance(monkeypatch, capsys, platform):
    text, _report, lock = fixture(platform)
    seen = []

    def run(command, **kwargs):
        seen.append((command, kwargs))
        return SimpleNamespace(returncode=1, stdout=text, stderr="")

    monkeypatch.setattr(audit.subprocess, "run", run)
    monkeypatch.setattr(audit.Path, "read_bytes", lambda *_args, **_kwargs: lock)
    assert audit.main() == 0
    assert seen[0][0] == ["npm", "audit", "--json"]
    assert seen[0][1]["check"] is False and seen[0][1]["capture_output"] is True
    captured = capsys.readouterr()
    assert captured.out == text
    assert "TEMPORARY CI RISK ACCEPTANCE" in captured.err and "production/transitive" in captured.err
    assert "Vulnerability is NOT fixed" in captured.err
    assert "no Worker deployment is authorized" in captured.err


def test_clean_audit_can_pass_without_using_exception(monkeypatch, capsys):
    _text, report, lock = fixture()
    report["vulnerabilities"] = {}
    report["metadata"]["vulnerabilities"] = dict.fromkeys(report["metadata"]["vulnerabilities"], 0)
    changed = audit.strict_json(lock)
    changed["packages"]["node_modules/picomatch"]["version"] = "4.0.8"
    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs:
                        SimpleNamespace(returncode=0, stdout=json.dumps(report), stderr=""))
    monkeypatch.setattr(audit.Path, "read_bytes", lambda *_args, **_kwargs: json.dumps(changed).encode())
    assert audit.main() == 0 and "ACCEPTANCE" not in capsys.readouterr().err


def test_command_error_stays_fatal(monkeypatch, capsys):
    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs:
                        SimpleNamespace(returncode=2, stdout='{"error":{"code":"ENETWORK"}}', stderr=""))
    assert audit.main() == 1 and "rejected" in capsys.readouterr().err


@pytest.mark.parametrize("fault", ["nested-advisory", "cycle", "unresolved"])
def test_nested_distinct_advisory_cycle_and_unresolved_graph_stay_fatal(fault):
    _text, report, lock = fixture("linux")
    if fault == "nested-advisory":
        leaf = copy.deepcopy(report["vulnerabilities"]["picomatch"]["via"][0])
        leaf["url"] = "https://github.com/advisories/GHSA-distinct-risk"
        report["vulnerabilities"]["agents"]["via"].append(leaf)
        reason = "advisory"
    elif fault == "cycle":
        report["vulnerabilities"]["agents"]["via"] = ["agents"]
        reason = "cycle"
    else:
        report["vulnerabilities"]["agents"]["via"] = ["not-in-report"]
        reason = "unresolved"
    with pytest.raises(ValueError, match=reason):
        audit.accepted_findings(report, lock, 1)
