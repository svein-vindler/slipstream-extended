import os
import stat
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import garmin_login
from scripts.public_release_check import audit_paths

TOKEN = '{"session": "synthetic-token-æ"}'
OLD_TOKEN = "synthetic-previous-token"


def posix_os(**overrides):
    """Exercise POSIX control flow without claiming Windows enforces POSIX modes."""
    values = {
        "name": "posix",
        "fdopen": os.fdopen,
        "fchmod": lambda fd, mode: None,
        "replace": os.replace,
        "close": os.close,
    }
    return SimpleNamespace(**(values | overrides))


def test_local_token_round_trip_and_replacement(tmp_path, capsys):
    path = tmp_path / "nested output" / "garmin_tokens.local.txt"
    garmin_login._write_local_token(path, OLD_TOKEN)
    garmin_login._write_local_token(path, TOKEN)

    assert path.read_text(encoding="utf-8") == TOKEN
    assert list(path.parent.iterdir()) == [path]
    assert capsys.readouterr().err.count("Wrote local Garmin token") == 2


def test_protection_failure_preserves_previous_token(monkeypatch, tmp_path, capsys):
    path = tmp_path / "garmin_tokens.local.txt"
    path.write_text(OLD_TOKEN, encoding="utf-8")

    def denied(*args, **kwargs):
        raise PermissionError("synthetic permission failure")

    monkeypatch.setattr(garmin_login, "os", posix_os(fchmod=denied))
    # Also reach the vulnerable writer's old post-write permission attempt.
    monkeypatch.setattr(Path, "chmod", denied)
    with pytest.raises(PermissionError, match="synthetic permission failure"):
        garmin_login._write_local_token(path, TOKEN)

    assert path.read_text(encoding="utf-8") == OLD_TOKEN
    assert list(tmp_path.iterdir()) == [path]
    assert "Wrote local Garmin token" not in capsys.readouterr().err


@pytest.mark.parametrize("existing", [False, True])
def test_content_is_private_before_write_and_published_complete(monkeypatch, tmp_path, existing):
    path = tmp_path / "token.txt"
    if existing:
        path.write_text(OLD_TOKEN, encoding="utf-8")
    events = []
    real_write_text = Path.write_text
    real_fdopen = os.fdopen

    def protect(fd, mode):
        assert mode == 0o600
        assert os.fstat(fd).st_size == 0
        events.append("protected")

    class ObservedStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, content):
            assert events == ["protected"]
            assert os.fstat(self.stream.fileno()).st_size == 0
            if existing:
                assert path.read_text(encoding="utf-8") == OLD_TOKEN
            else:
                assert not path.exists()
            events.append("write")
            return self.stream.write(content)

    def fdopen(*args, **kwargs):
        return ObservedStream(real_fdopen(*args, **kwargs))

    def replace(source, destination):
        assert events == ["protected", "write"]
        assert Path(source).read_text(encoding="utf-8") == TOKEN
        assert Path(source).parent == path.parent
        assert destination == path
        events.append("publish")
        os.replace(source, destination)

    def no_in_place_write(target, content, *args, **kwargs):
        assert content != TOKEN, "session bytes reached an unprotected pathname write"
        return real_write_text(target, content, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", no_in_place_write)
    monkeypatch.setattr(garmin_login, "os", posix_os(fchmod=protect, fdopen=fdopen, replace=replace))
    garmin_login._write_local_token(path, TOKEN)

    assert events == ["protected", "write", "publish"]
    assert path.read_text(encoding="utf-8") == TOKEN
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("phase", ["create", "protect", "fdopen", "write", "close", "replace"])
@pytest.mark.parametrize("existing", [False, True])
def test_failures_leave_no_new_token_or_open_descriptor(
    monkeypatch, tmp_path, capsys, phase, existing
):
    path = tmp_path / "token.txt"
    if existing:
        path.write_text(OLD_TOKEN, encoding="utf-8")
    descriptors = []
    real_create = garmin_login.tempfile.mkstemp

    def fail():
        raise OSError("synthetic storage failure")

    def create(*args, **kwargs):
        if phase == "create":
            fail()
        descriptor, temporary = real_create(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor, temporary

    class FailingStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            result = self.stream.__exit__(*args)
            if phase == "close":
                fail()
            return result

        def write(self, content):
            if phase == "write":
                self.stream.write(content[:5])
                fail()
            return self.stream.write(content)

    def fdopen(*args, **kwargs):
        if phase == "fdopen":
            fail()
        return FailingStream(os.fdopen(*args, **kwargs))

    def protect(*args):
        if phase == "protect":
            fail()

    def replace(*args):
        if phase == "replace":
            fail()
        os.replace(*args)

    monkeypatch.setattr(garmin_login.tempfile, "mkstemp", create)
    monkeypatch.setattr(garmin_login, "os", posix_os(fchmod=protect, fdopen=fdopen, replace=replace))
    with pytest.raises(OSError, match="synthetic storage failure"):
        garmin_login._write_local_token(path, TOKEN)

    if existing:
        assert path.read_text(encoding="utf-8") == OLD_TOKEN
    else:
        assert not path.exists()
    assert list(tmp_path.iterdir()) == ([path] if existing else [])
    assert "Wrote local Garmin token" not in capsys.readouterr().err
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_abrupt_exit_leaves_staged_token_covered_by_secret_exclusions(tmp_path):
    root = Path(__file__).resolve().parents[1]
    path = tmp_path / "token.txt"
    path.write_text(OLD_TOKEN, encoding="utf-8")
    code = """
import os
import sys
from pathlib import Path
from scripts import garmin_login
garmin_login.os.replace = lambda *args: os._exit(73)
garmin_login._write_local_token(Path(sys.argv[1]), sys.argv[2])
"""
    result = subprocess.run(
        [sys.executable, "-X", "utf8", "-B", "-c", code, str(path), TOKEN],
        cwd=root, capture_output=True, check=False,
    )
    assert result.returncode == 73, result.stderr
    assert path.read_text(encoding="utf-8") == OLD_TOKEN
    staged = [entry for entry in tmp_path.iterdir() if entry != path]
    assert len(staged) == 1
    assert staged[0].read_text(encoding="utf-8") == TOKEN
    ignored_token_patterns = [
        line for line in (root / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.startswith("garmin_tokens")
    ]
    assert any(fnmatch(staged[0].name, pattern) for pattern in ignored_token_patterns)
    assert audit_paths([staged[0].name])
    if os.name == "posix":
        assert stat.S_IMODE(staged[0].stat().st_mode) == 0o600


@pytest.mark.skipif(os.name != "posix", reason="Requires POSIX file-link semantics")
@pytest.mark.parametrize("link_type", ["hard", "symbolic"])
def test_replacement_does_not_write_through_a_link(tmp_path, link_type):
    alias = tmp_path / "readable-alias.txt"
    alias.write_text(OLD_TOKEN, encoding="utf-8")
    alias.chmod(0o666)
    path = tmp_path / "token.txt"
    if link_type == "hard":
        os.link(alias, path)
    else:
        path.symlink_to(alias)

    garmin_login._write_local_token(path, TOKEN)

    assert path.read_text(encoding="utf-8") == TOKEN
    assert alias.read_text(encoding="utf-8") == OLD_TOKEN
    assert not path.is_symlink()
    assert set(tmp_path.iterdir()) == {alias, path}


@pytest.mark.skipif(os.name != "posix", reason="Requires real POSIX file permissions")
@pytest.mark.parametrize("existing", [False, True])
def test_real_posix_mode_before_first_byte_under_permissive_umask(monkeypatch, tmp_path, existing):
    path = tmp_path / "token.txt"
    if existing:
        path.write_text(OLD_TOKEN, encoding="utf-8")
        path.chmod(0o666)
    tmp_path.chmod(0o755)
    writes = []

    class CheckedStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, content):
            info = os.fstat(self.stream.fileno())
            assert stat.S_IMODE(info.st_mode) == 0o600
            assert info.st_size == 0
            writes.append(True)
            return self.stream.write(content)

    def fdopen(*args, **kwargs):
        return CheckedStream(os.fdopen(*args, **kwargs))

    monkeypatch.setattr(garmin_login, "os", posix_os(fchmod=os.fchmod, fdopen=fdopen))
    previous_umask = os.umask(0)
    try:
        garmin_login._write_local_token(path, TOKEN)
    finally:
        os.umask(previous_umask)

    assert writes == [True]
    assert path.read_text(encoding="utf-8") == TOKEN
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o755
    assert list(tmp_path.iterdir()) == [path]


def fake_login(monkeypatch, tmp_path, arguments):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(garmin_login.sys, "argv", ["garmin_login.py", *arguments])
    monkeypatch.setattr("builtins.input", lambda prompt: "synthetic@example.invalid")
    monkeypatch.setattr(garmin_login.getpass, "getpass", lambda prompt: "synthetic-password")
    client = SimpleNamespace(dumps=lambda: TOKEN)
    monkeypatch.setattr(
        garmin_login, "Garmin", lambda *args, **kwargs: SimpleNamespace(login=lambda: None, client=client)
    )


@pytest.mark.parametrize("arguments,repo,upload_fails,output,uploads", [
    (["--local-only", "--local-token-file", "nested/token.txt"], None, False, "nested/token.txt", 0),
    (["--local-token-file", "custom.tokens"], "synthetic/repo", False, "custom.tokens", 1),
    (["--local-token-file", "custom.tokens"], "synthetic/repo", True, "custom.tokens", 1),
    (["--local-token-file", "custom.tokens"], None, False, "custom.tokens", 0),
    ([], "synthetic/repo", False, None, 1),
    ([], "synthetic/repo", True, "garmin_tokens.txt", 1),
    ([], None, False, "garmin_tokens.txt", 0),
])
def test_cli_output_routes_remain_compatible(
    monkeypatch, tmp_path, arguments, repo, upload_fails, output, uploads
):
    fake_login(monkeypatch, tmp_path, arguments)
    monkeypatch.setattr(garmin_login, "detect_repo", lambda: repo)
    calls = []

    def run(command, *, input, check):
        assert command == ["gh", "secret", "set", "GARMINTOKENS", "-R", "synthetic/repo"]
        assert input == TOKEN.encode("utf-8")
        assert check is True
        calls.append(command)
        if upload_fails:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(garmin_login.subprocess, "run", run)
    garmin_login.main()

    assert len(calls) == uploads
    files = [path for path in tmp_path.rglob("*") if path.is_file()]
    if output is None:
        assert not files
    else:
        assert files == [tmp_path / output]
        assert files[0].read_text(encoding="utf-8") == TOKEN


def test_cli_local_protection_failure_stops_before_github(monkeypatch, tmp_path, capsys):
    fake_login(monkeypatch, tmp_path, ["--local-token-file", "token.txt"])

    def denied(*args, **kwargs):
        raise PermissionError("synthetic permission failure")

    def unexpected(*args, **kwargs):
        pytest.fail("Local protection failure must stop before GitHub detection/upload")

    monkeypatch.setattr(garmin_login, "os", posix_os(fchmod=denied))
    monkeypatch.setattr(garmin_login, "detect_repo", unexpected)
    monkeypatch.setattr(garmin_login.subprocess, "run", unexpected)
    with pytest.raises(PermissionError, match="synthetic permission failure"):
        garmin_login.main()

    assert not list(tmp_path.iterdir())
    assert "Wrote local Garmin token" not in capsys.readouterr().err
