"""Keep shared development locks installable on Windows after Linux regeneration."""

from pathlib import Path


def test_windows_pytest_dependency_is_preserved_in_shared_lock():
    root = Path(__file__).resolve().parents[1]
    marker = 'colorama==0.4.6 ; sys_platform == "win32"'
    assert marker in (root / "requirements-dev.in").read_text(encoding="utf-8")
    lock = (root / "requirements-dev.txt").read_text(encoding="utf-8")
    assert marker in lock
    assert "sha256:4f1d9991f5acc0ca119f9d443620b77f9d6b33703e51011c16baf57afb285fc6" in lock


def test_windows_garmin_timezone_dependency_is_preserved_in_both_shared_locks():
    root = Path(__file__).resolve().parents[1]
    marker = 'tzdata==2026.5 ; sys_platform == "win32"'
    assert marker in (root / "requirements.in").read_text(encoding="utf-8")
    for filename in ("requirements.txt", "requirements-dev.txt"):
        lock = (root / filename).read_text(encoding="utf-8")
        assert marker in lock
        assert "sha256:8cc73c0a0bfca7dbfa59235d60b2eff82231dee33f53d206db1acd9173cfc0a7" in lock
        assert "sha256:b683bd1b6659ddcd810ff02ad09ba821d4bf1065072805063eb35c49617905ac" in lock
