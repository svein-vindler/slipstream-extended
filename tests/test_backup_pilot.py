"""The live pilot is exercised only with an invented read-only source."""

import json

import pytest
from test_private_backup import PASSWORD, SCOPE, FakeR2

from pipeline import backup_pilot as pilot
from pipeline.backup_source import BackupError, R2ReadOnlySource


def test_pilot_round_trip_and_private_outputs(tmp_path):
    source = R2ReadOnlySource(FakeR2(), "synthetic-bucket", pilot.PILOT_LIMITS)
    result = pilot.run_pilot(source, SCOPE, tmp_path, PASSWORD)
    assert result["exact_bytes_and_references"]
    assert result["source_operations"]["get"] == 6
    assert result["source_operations"]["list_pages"] == 6
    assert result["remote_writes"] == result["garmin_calls"] == result["ready_analyses"] == 0
    (session,) = tmp_path.iterdir()
    assert (session / "PILOT-COMPLETE").exists()
    assert not (session / "PILOT-INCOMPLETE").exists()
    assert (session / "history.slbk").read_bytes() == (
        session / "history-verified-copy.slbk"
    ).read_bytes()
    output = (session / "result-anonymous.json").read_text()
    assert "900001" not in output
    assert "SYNTHETIC_PRIVATE_SENTINEL" not in output
    assert PASSWORD.decode() not in output


def test_pilot_refuses_git_root_before_reads(tmp_path):
    (tmp_path / ".git").mkdir()
    client = FakeR2()
    source = R2ReadOnlySource(client, "synthetic-bucket", pilot.PILOT_LIMITS)
    with pytest.raises(BackupError, match="private_directory_must_be_outside_git"):
        pilot.run_pilot(source, SCOPE, tmp_path, PASSWORD)
    assert not client.lists and not client.gets


def test_pilot_activity_without_covered_data_is_incomplete(tmp_path):
    client = FakeR2()
    client.objects = {
        key: value for key, value in client.objects.items() if key.startswith("coach/")
    }
    with pytest.raises(BackupError, match="pilot_activity_has_no_covered_objects"):
        pilot.run_pilot(
            R2ReadOnlySource(client, "synthetic-bucket", pilot.PILOT_LIMITS),
            SCOPE,
            tmp_path,
            PASSWORD,
        )
    (session,) = tmp_path.iterdir()
    assert (session / "PILOT-INCOMPLETE").exists()
    assert not (session / "PILOT-COMPLETE").exists()


def test_client_uses_explicit_credentials_and_zero_retries(monkeypatch):
    import boto3

    captured = {}

    def fake_client(service, **kwargs):
        captured.update(kwargs)
        assert service == "s3"
        return "synthetic client"

    monkeypatch.setattr(boto3, "client", fake_client)
    assert (
        pilot._client("a" * 32, "synthetic-access-key", "synthetic-secret-key")
        == "synthetic client"
    )
    assert captured["config"].retries["total_max_attempts"] == 1
    assert captured["config"].connect_timeout == 10
    assert captured["config"].read_timeout == 30
    assert captured["aws_secret_access_key"] == "synthetic-secret-key"
    assert captured["endpoint_url"] == "https://" + "a" * 32 + ".r2.cloudflarestorage.com"
    with pytest.raises(BackupError):
        pilot._client("https://arbitrary.example", "synthetic-access-key", "synthetic-secret-key")


def test_pilot_cli_sanitizes_provider_errors(tmp_path, monkeypatch, capsys):
    replies = iter(
        [
            "2026",
            "900001",
            "READONLY",
            "a" * 32,
            "synthetic-bucket",
            "synthetic-access-key",
            "SYNTHETIC_SECRET_SENTINEL",
            PASSWORD.decode(),
            PASSWORD.decode(),
        ]
    )
    monkeypatch.setattr(pilot, "_prompt", lambda _label: next(replies))

    def failure(*_args):
        raise RuntimeError("SYNTHETIC_SECRET_SENTINEL")

    monkeypatch.setattr(pilot, "_client", failure)
    assert pilot.main(["--private-root", str(tmp_path)]) == 2
    output = capsys.readouterr()
    assert "SYNTHETIC_SECRET_SENTINEL" not in output.out + output.err
    assert PASSWORD.decode() not in output.out + output.err
    assert not list(tmp_path.iterdir())


def test_pilot_cli_success_and_counts_only(tmp_path, monkeypatch, capsys):
    replies = iter(
        [
            "2026",
            "900001",
            " readonly ",
            "a" * 32,
            "synthetic-bucket",
            "synthetic-access-key",
            "synthetic-secret-key",
            PASSWORD.decode(),
            PASSWORD.decode(),
        ]
    )
    monkeypatch.setattr(pilot, "_prompt", lambda _label: next(replies))
    client = FakeR2()
    client.close = lambda: None
    monkeypatch.setattr(pilot, "_client", lambda *_args: client)
    assert pilot.main(["--private-root", str(tmp_path)]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "live_test_verified"
    assert "900001" not in output.out + output.err
    assert "synthetic-bucket" not in output.out + output.err
    assert PASSWORD.decode() not in output.out + output.err


def test_invalid_password_fails_before_source_access(tmp_path):
    client = FakeR2()
    source = R2ReadOnlySource(client, "synthetic-bucket", pilot.PILOT_LIMITS)
    with pytest.raises(BackupError, match="password_length_invalid"):
        pilot.run_pilot(source, SCOPE, tmp_path, b"short")
    assert not client.lists and not client.gets
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("password", ["short", "x" * 1025])
def test_cli_invalid_password_fails_before_client_creation(tmp_path, monkeypatch, capsys, password):
    replies = iter(
        [
            "2026",
            "900001",
            "READONLY",
            "a" * 32,
            "synthetic-bucket",
            "synthetic-access-key",
            "synthetic-secret-key",
            password,
        ]
    )
    monkeypatch.setattr(pilot, "_prompt", lambda _label: next(replies))

    def forbidden_client(*_args):
        pytest.fail("Invalid password must not create a network client")

    monkeypatch.setattr(pilot, "_client", forbidden_client)
    assert pilot.main(["--private-root", str(tmp_path)]) == 2
    assert capsys.readouterr().err.strip() == "backup_error: password_length_invalid"
    assert not list(tmp_path.iterdir())


def test_private_selection_file_preselects_activity_without_printing_it(
    tmp_path, monkeypatch, capsys
):
    selection = tmp_path / "scope.private.json"
    selection.write_text('[{"year":"2026","activity_id":"900001"}]', encoding="utf-8")
    replies = iter(
        [
            "READONLY",
            "a" * 32,
            "synthetic-bucket",
            "synthetic-access-key",
            "synthetic-secret-key",
            PASSWORD.decode(),
            PASSWORD.decode(),
        ]
    )
    monkeypatch.setattr(pilot, "_prompt", lambda _label: next(replies))
    client = FakeR2()
    client.close = lambda: None
    monkeypatch.setattr(pilot, "_client", lambda *_args: client)
    assert pilot.main(["--private-root", str(tmp_path), "--scope", str(selection)]) == 0
    output = capsys.readouterr()
    assert "900001" not in output.out + output.err
    assert json.loads(output.out)["exact_bytes_and_references"]
