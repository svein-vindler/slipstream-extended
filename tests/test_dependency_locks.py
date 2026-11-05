"""Keep shared development locks installable on Windows after Linux regeneration."""

import re
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
    marker = 'tzdata==2026.6 ; sys_platform == "win32"'
    assert marker in (root / "requirements.in").read_text(encoding="utf-8")
    for filename in ("requirements.txt", "requirements-dev.txt"):
        lock = (root / filename).read_text(encoding="utf-8")
        assert marker in lock
        assert "sha256:0a10eec6e13ef16b627fabed9a5f2ba61a62b1c67f327305f4e7e412a70b4aec" in lock
        assert "sha256:85d2cd9c09f81f0bb29db27759fc83af4f1e620efb814f8c5bf8b4cc4feece6d" in lock


def test_updated_direct_dependencies_match_inputs_and_shared_locks():
    root = Path(__file__).resolve().parents[1]
    for dependency, input_name, lock_names in (
        ("boto3", "requirements.in", ("requirements.txt", "requirements-dev.txt")),
        ("pytest", "requirements-dev.in", ("requirements-dev.txt",)),
    ):
        inputs = (root / input_name).read_text(encoding="utf-8")
        pins = re.findall(rf"^{dependency}==[^\s;]+$", inputs, re.MULTILINE)
        assert len(pins) == 1, f"Expected one exact {dependency} pin in {input_name}"
        pin = pins[0]
        for filename in lock_names:
            lock = (root / filename).read_text(encoding="utf-8")
            assert re.search(
                rf"^{re.escape(pin)} \\\n[ \t]+--hash=sha256:[0-9a-f]{{64}}(?: \\)?$",
                lock,
                re.MULTILINE,
            ), f"{filename} must contain a hash-bearing {pin} block"
