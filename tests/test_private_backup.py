"""Synthetic-only backup and adversarial import tests; no external services."""

import copy
import gzip
import hashlib
import io
import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline import backup_source as sources
from pipeline import private_backup as backup
from pipeline.backup_source import (
    BackupError,
    Limits,
    LocalSource,
    ObjectVersion,
    R2ReadOnlySource,
    Scope,
    SourceChanged,
)
from pipeline.coach import build_coach_input, validate_profile

PASSWORD = b"synthetic-test-password-only"
SCOPE = Scope((("2026", "900001"),))
BASE = "activities/2026/900001/"


def synthetic_objects():
    objects = {}
    for revision in (1, 2):
        profile = validate_profile(
            {
                "profile_id": f"synthetic-profile-{revision}",
                "name": "Synthetic example",
                "effective_from": "2026-01-01",
                "created_at": f"2026-01-0{revision}T00:00:00.000Z",
                "zones": [{"label": "Z1", "min_bpm": 80, "max_bpm": 140 + revision}],
                "references": {"lt1_min_bpm": 130 + revision},
            }
        )
        context = {
            "schema_version": 1,
            "activity_id": "900001",
            "context_id": f"synthetic-context-{revision}",
            "rpe": revision,
            "conditions": "synthetic dry",
            "note": "SYNTHETIC_PRIVATE_SENTINEL",
            "workout_correction": None,
            "source": "user",
            "created_at": f"2026-09-0{revision}T12:00:00.000Z",
        }
        analysis = build_coach_input(
            activity={
                "id": "900001",
                "date": "2026-09-01",
                "name": "Synthetic run",
                "type": "running",
            },
            decoded_fit={"source_fit_sha256": "a" * 64},
            endurance={},
            profile=profile,
            tcx_sha256="b" * 64,
            context=context,
        )
        objects[f"coach/profiles/v1/2026-01-01/{profile['profile_id']}.json"] = backup._json_bytes(
            profile
        )
        objects[f"{BASE}context/v1/2026090{revision}T120000000-{context['context_id']}.json"] = (
            backup._json_bytes(context)
        )
        objects[f"{BASE}coach-input/v1/canonical/{analysis['analysis_id']}.json"] = gzip.compress(
            backup._json_bytes(analysis), mtime=0
        )
    return objects


class MemorySource:
    def __init__(self, objects=None):
        self.objects = objects if objects is not None else synthetic_objects()
        self.list_calls = self.get_calls = 0

    def inventory(self, scope):
        self.list_calls += 1
        for key, data in sorted(self.objects.items()):
            if any(key.startswith(prefix) for prefix in scope.prefixes()):
                yield ObjectVersion(key, hashlib.sha256(data).hexdigest(), len(data))

    def read(self, version, maximum):
        self.get_calls += 1
        data = self.objects.get(version.key)
        if data is None or hashlib.sha256(data).hexdigest() != version.revision:
            raise SourceChanged("source_changed")
        assert len(data) <= maximum
        return data


@pytest.fixture
def archive(tmp_path):
    path = tmp_path / "synthetic.slbk"
    backup.create_backup(MemorySource(), SCOPE, path, PASSWORD)
    return path


def test_round_trip_all_versions_exact_bytes_and_references(archive, tmp_path):
    verified = backup.verify_backup(archive, PASSWORD)
    assert verified.objects == synthetic_objects()
    assert len(verified.manifest["references"]) == 2
    assert verified.summary()["ready_analyses"] == 0
    target = tmp_path / "isolated"
    backup.restore_local(archive, PASSWORD, target)
    restored = {
        path.relative_to(target / "objects").as_posix(): path.read_bytes()
        for path in (target / "objects").rglob("*.json")
    }
    assert restored == synthetic_objects()
    manifest = json.loads((target / "manifest.private.json").read_bytes())
    assert manifest["references"] == verified.manifest["references"]
    assert not list(target.rglob("latest-ready.json"))
    assert not list(target.rglob("profiles-v1.json"))
    assert (target / "RESTORE-COMPLETE").exists()
    assert not (target / "RESTORE-INCOMPLETE").exists()


def test_plan_is_read_only(archive, tmp_path):
    before = sorted(str(path) for path in tmp_path.rglob("*"))
    result = backup.restore_plan(archive, PASSWORD, tmp_path / "preview")
    assert result["would_write_objects"] == 6
    assert result["derived_pointers_created"] == 0
    assert sorted(str(path) for path in tmp_path.rglob("*")) == before


@pytest.mark.parametrize("operation", ["create", "plan", "restore"])
def test_existing_destinations_never_overwritten(archive, tmp_path, operation):
    target = tmp_path / "existing"
    target.mkdir()
    sentinel = target / "keep"
    sentinel.write_bytes(b"existing")
    with pytest.raises((BackupError, FileExistsError)):
        if operation == "create":
            source = MemorySource()
            backup.create_backup(source, SCOPE, archive, PASSWORD)
            assert source.get_calls == 0
        elif operation == "plan":
            backup.restore_plan(archive, PASSWORD, target)
        else:
            backup.restore_local(archive, PASSWORD, target)
    assert sentinel.read_bytes() == b"existing"


@pytest.mark.parametrize("mutation", ["password", "byte", "truncate", "salt", "trailing", "header"])
def test_authentication_rejected_before_parsing_or_writing(
    archive, tmp_path, monkeypatch, mutation
):
    data = bytearray(archive.read_bytes())
    password = PASSWORD
    if mutation == "password":
        password = b"wrong-synthetic-password"
    elif mutation == "byte":
        data[-30] = ord("a") if data[-30] != ord("a") else ord("b")
    elif mutation == "truncate":
        del data[-20:]
    elif mutation == "salt":
        data[len(backup.MAGIC)] ^= 1
    elif mutation == "header":
        data[0] ^= 1
    else:
        data.extend(b"\nignored-garbage")
    damaged = tmp_path / "damaged.slbk"
    damaged.write_bytes(data)
    monkeypatch.setattr(
        backup, "_loads", lambda *_args: pytest.fail("parsed unauthenticated content")
    )
    with pytest.raises(BackupError):
        backup.restore_local(damaged, password, tmp_path / "must-not-exist")
    assert not (tmp_path / "must-not-exist").exists()


class ChangingSource(MemorySource):
    def __init__(self, mode):
        super().__init__()
        self.mode = mode

    def read(self, version, maximum):
        if self.get_calls == 0:
            if self.mode == "missing":
                self.objects.pop(version.key)
            elif self.mode == "added":
                self.objects[f"{BASE}context/v1/20260903T120000000-synthetic-context-3.json"] = (
                    b"{}"
                )
            else:
                data = self.objects[version.key]
                self.objects[version.key] = (
                    gzip.compress(gzip.decompress(data), mtime=1)
                    if data[:2] == b"\x1f\x8b"
                    else data + b" "
                )
        return super().read(version, maximum)


@pytest.mark.parametrize("mode", ["changed", "missing", "added"])
def test_source_races_refuse_complete_backup(tmp_path, mode):
    destination = tmp_path / "inconsistent.slbk"
    with pytest.raises(BackupError):
        backup.create_backup(ChangingSource(mode), SCOPE, destination, PASSWORD)
    assert not destination.exists()
    assert not list(tmp_path.glob(".slbk-part-*"))


def test_one_retry_restarts_entire_snapshot(tmp_path):
    source = ChangingSource("changed")
    report = backup.create_backup(source, SCOPE, tmp_path / "consistent.slbk", PASSWORD, retries=1)
    assert report["attempts"] == 2
    assert backup.verify_backup(tmp_path / "consistent.slbk", PASSWORD).objects == source.objects
    with pytest.raises(BackupError):
        backup.create_backup(source, SCOPE, tmp_path / "bad.slbk", PASSWORD, retries=2)


def _write_document(tmp_path, document):
    path = tmp_path / "crafted.slbk"
    path.write_bytes(backup._seal(document, PASSWORD, Limits()))
    return path


@pytest.mark.parametrize(
    "key",
    [
        "../outside.json",
        "/absolute.json",
        "C:/absolute.json",
        "coach/profiles/v1/../../bad.json",
        "a\\b.json",
        f"{BASE}activity.fit",
        f"{BASE}activity.tcx",
        "tokens.json",
        f"{BASE}coach-input/v1/latest-ready.json",
    ],
)
def test_authenticated_archive_rejects_names_and_excluded_types(archive, tmp_path, key):
    document = copy.deepcopy(backup.verify_backup(archive, PASSWORD).manifest)
    document["objects"][0]["key"] = key
    path = _write_document(tmp_path, document)
    with pytest.raises(BackupError):
        backup.restore_local(path, PASSWORD, tmp_path / "invalid")
    assert not (tmp_path / "invalid").exists()


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "type", "checksum", "size", "reference", "version", "complete", "scope"],
)
def test_authenticated_archive_rejects_manifest_errors(archive, tmp_path, mutation):
    document = copy.deepcopy(backup.verify_backup(archive, PASSWORD).manifest)
    if mutation == "duplicate":
        document["objects"].append(document["objects"][0])
    elif mutation == "type":
        document["objects"][0]["type"] = "credentials"
    elif mutation == "checksum":
        document["objects"][0]["sha256"] = "0" * 64
    elif mutation == "size":
        document["objects"][0]["size"] += 1
    elif mutation == "reference":
        document["references"][0]["ready"] = True
    elif mutation == "scope":
        document["scope"] = []
    elif mutation == "version":
        document["version"] = 2
    else:
        document["complete"] = False
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document), PASSWORD)


@pytest.mark.parametrize(
    "mutation",
    [
        "profile-missing",
        "context-missing",
        "profile-version",
        "context-version",
        "secret-field",
        "gps-field",
        "analysis-id",
        "schema",
        "rpe",
    ],
)
def test_export_rejects_invalid_content_and_links(tmp_path, mutation):
    objects = synthetic_objects()
    profile_key = next(key for key in objects if "profiles/" in key)
    context_key = next(key for key in objects if "context/" in key)
    analysis_key = next(key for key in objects if "canonical/" in key)
    if mutation == "profile-missing":
        del objects[profile_key]
    elif mutation == "context-missing":
        del objects[context_key]
    else:
        selected = (
            profile_key
            if mutation in {"profile-version", "secret-field", "schema"}
            else context_key
        )
        selected = analysis_key if mutation in {"analysis-id", "gps-field"} else selected
        raw = objects[selected]
        value = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        if mutation == "profile-version":
            value["zones"][0]["max_bpm"] += 1
        elif mutation == "context-version":
            value["note"] = "changed synthetic context"
        elif mutation == "secret-field":
            value["password"] = "SYNTHETIC_SECRET_SENTINEL"
        elif mutation == "gps-field":
            value["summary"]["gps"] = [0, 0]
        elif mutation == "analysis-id":
            value["analysis_id"] = "0" * 24
        elif mutation == "schema":
            value["schema_version"] = 2
        elif mutation == "rpe":
            value["rpe"] = 99
        objects[selected] = backup._json_bytes(value)
    with pytest.raises(BackupError):
        backup.create_backup(MemorySource(objects), SCOPE, tmp_path / "invalid.slbk", PASSWORD)
    assert not (tmp_path / "invalid.slbk").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("objects", 5),
        ("stored_object", 20),
        ("stored_total", 100),
        ("decoded_object", 30),
        ("decoded_total", 100),
        ("document", 100),
        ("archive", 100),
        ("json_nodes", 10),
    ],
)
def test_resource_limits_fail_closed(archive, field, value):
    with pytest.raises(BackupError):
        backup.verify_backup(archive, PASSWORD, limits=Limits(**{field: value}))


def test_compression_bomb_and_json_depth():
    with pytest.raises(BackupError, match="decoded_object_limit"):
        backup._decode(gzip.compress(b" " * 1000), Limits(decoded_object=20))
    with pytest.raises(BackupError, match="json_depth_limit"):
        backup._loads(b"[" * 40 + b"0" + b"]" * 40, 1000, Limits())
    with pytest.raises(BackupError):
        backup._loads(b'{"a":1,"a":2}', 1000, Limits())
    with pytest.raises(BackupError):
        backup._loads(b'{"a":1e999}', 1000, Limits())


def test_interrupted_publish_leaves_no_final_backup(tmp_path, monkeypatch):
    def interrupted(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(backup.os, "link", interrupted)
    with pytest.raises(KeyboardInterrupt):
        backup.create_backup(MemorySource(), SCOPE, tmp_path / "aborted.slbk", PASSWORD)
    assert not (tmp_path / "aborted.slbk").exists()
    assert not list(tmp_path.glob(".slbk-part-*"))


def test_publish_race_does_not_replace_existing_file(tmp_path, monkeypatch):
    real_link = backup.os.link

    def competing(source, destination):
        destination.write_bytes(b"other backup")
        real_link(source, destination)

    monkeypatch.setattr(backup.os, "link", competing)
    path = tmp_path / "race.slbk"
    with pytest.raises(FileExistsError):
        backup.create_backup(MemorySource(), SCOPE, path, PASSWORD)
    assert path.read_bytes() == b"other backup"


def test_cli_aggregates_and_errors_never_echo_private_values(
    archive, tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(backup.getpass, "getpass", lambda *_args: PASSWORD.decode())
    assert backup.main(["verify", "--backup", str(archive)]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["objects"] == 6
    assert "SYNTHETIC_PRIVATE_SENTINEL" not in output.out + output.err
    assert PASSWORD.decode() not in output.out + output.err
    assert "900001" not in output.out + output.err
    assert backup.main(["verify", "--backup", str(tmp_path / "SYNTHETIC_SECRET_SENTINEL")]) == 2
    assert "SYNTHETIC_SECRET_SENTINEL" not in capsys.readouterr().err
    with pytest.raises(SystemExit):
        backup.main(["verify", "--password", "SYNTHETIC_SECRET_SENTINEL"])
    assert "SYNTHETIC_SECRET_SENTINEL" not in capsys.readouterr().err
    assert b"SYNTHETIC_PRIVATE_SENTINEL" not in archive.read_bytes()
    assert b"coach/profiles/" not in archive.read_bytes()


def test_local_source_only_allowed_trees(tmp_path):
    objects = synthetic_objects()
    for key, data in objects.items():
        path = tmp_path / "source" / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    (tmp_path / "source" / "secret.token").write_bytes(b"never read")
    path = tmp_path / "local.slbk"
    backup.create_backup(LocalSource(tmp_path / "source"), SCOPE, path, PASSWORD)
    assert backup.verify_backup(path, PASSWORD).objects == objects


class FakeR2:
    def __init__(self):
        self.objects = synthetic_objects()
        self.lists = []
        self.gets = []
        self.bodies = []

    def list_objects_v2(self, **args):
        assert args["Prefix"] in SCOPE.prefixes()
        self.lists.append(args)
        return {
            "Contents": [
                {
                    "Key": key,
                    "ETag": '"' + hashlib.sha256(data).hexdigest() + '"',
                    "Size": len(data),
                }
                for key, data in self.objects.items()
                if key.startswith(args["Prefix"])
            ]
        }

    def get_object(self, **args):
        assert "IfMatch" in args
        self.gets.append(args)
        data = self.objects[args["Key"]]
        assert args["IfMatch"] == '"' + hashlib.sha256(data).hexdigest() + '"'
        body = io.BytesIO(data)
        self.bodies.append(body)
        return {"ETag": args["IfMatch"], "ContentLength": len(data), "Body": body}


def test_r2_adapter_read_only_prefixes_conditional_reads_and_operation_counts(tmp_path):
    client = FakeR2()
    source = R2ReadOnlySource(client, "synthetic-bucket")
    report = backup.create_backup(source, SCOPE, tmp_path / "r2.slbk", PASSWORD)
    assert report["storage_operations"] == {
        "get": 6,
        "list_pages": 6,
        "listed_objects": 12,
        "put": 0,
    }
    assert all(body.closed for body in client.bodies)
    assert not hasattr(source, "put")
    assert len(client.gets) == 6


def test_r2_adapter_operation_budget_and_provider_errors_are_sanitized(tmp_path):
    client = FakeR2()
    with pytest.raises(BackupError, match="operation_limit"):
        backup.create_backup(
            R2ReadOnlySource(client, "synthetic-bucket", Limits(operations=2)),
            SCOPE,
            tmp_path / "limited.slbk",
            PASSWORD,
        )

    def failure(**_args):
        raise RuntimeError("SYNTHETIC_SECRET_SENTINEL")

    client.list_objects_v2 = failure
    with pytest.raises(BackupError, match="^source_read_failed$"):
        backup.create_backup(
            R2ReadOnlySource(client, "synthetic-bucket"), SCOPE, tmp_path / "failed.slbk", PASSWORD
        )


def test_all_four_cli_commands_with_synthetic_local_source(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"
    for key, data in synthetic_objects().items():
        path = source / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    scope = tmp_path / "scope.json"
    scope.write_bytes(backup._json_bytes(backup._scope_data(SCOPE)))
    path = tmp_path / "cli.slbk"
    target = tmp_path / "cli-recovery"
    monkeypatch.setattr(backup.getpass, "getpass", lambda *_args: PASSWORD.decode())
    assert (
        backup.main(
            ["create", "--source", str(source), "--scope", str(scope), "--output", str(path)]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["storage_operations"]["get"] == 18
    assert report["source_read_bytes"] == 3 * sum(map(len, synthetic_objects().values()))
    assert backup.main(["verify", "--backup", str(path)]) == 0
    capsys.readouterr()
    assert backup.main(["plan", "--backup", str(path), "--target", str(target)]) == 0
    capsys.readouterr()
    assert not target.exists()
    assert backup.main(["restore-local", "--backup", str(path), "--target", str(target)]) == 0
    assert (target / "RESTORE-COMPLETE").exists()


def test_incomplete_restore_keeps_marker_and_refuses_reuse(archive, tmp_path, monkeypatch):
    original = Path.open

    def failed_open(path, *args, **kwargs):
        if "objects" in path.parts and path.suffix == ".json":
            raise OSError("synthetic interruption")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failed_open)
    target = tmp_path / "partial-restore"
    with pytest.raises(OSError):
        backup.restore_local(archive, PASSWORD, target)
    assert (target / "RESTORE-INCOMPLETE").exists()
    assert not (target / "RESTORE-COMPLETE").exists()
    with pytest.raises((BackupError, FileExistsError)):
        backup.restore_local(archive, PASSWORD, target)


def test_linked_source_and_target_are_rejected(archive, tmp_path):
    linked = tmp_path / "linked"
    try:
        linked.symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(linked), str(tmp_path)],
            capture_output=True,
            check=False,
        )
        if result.returncode:
            pytest.skip("Windows account cannot create symlinks or junctions")
    with pytest.raises(BackupError, match="linked_path_not_allowed"):
        backup.restore_plan(archive, PASSWORD, linked / "new")
    with pytest.raises(BackupError, match="linked_path_not_allowed"):
        LocalSource(linked)


def test_reserved_windows_names_case_duplicates_and_node_budget(archive, tmp_path):
    with pytest.raises(BackupError):
        backup.key_info("coach/profiles/v1/2026-01-01/CON.json")
    document = copy.deepcopy(backup.verify_backup(archive, PASSWORD).manifest)
    entry = copy.deepcopy(next(item for item in document["objects"] if item["type"] == "profile"))
    entry["key"] = entry["key"].replace("synthetic-profile", "SYNTHETIC-PROFILE")
    document["objects"].append(entry)
    with pytest.raises(BackupError):
        backup.verify_backup(_write_document(tmp_path, document), PASSWORD)
    budget = [20]
    backup._loads(b"[1,2,3,4,5,6,7,8]", 100, Limits(), budget)
    with pytest.raises(BackupError, match="json_node_limit"):
        backup._loads(b"[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]", 100, Limits(), budget)


@pytest.mark.parametrize(
    "kind", ["revision", "length", "pagination", "duplicate", "outside", "size"]
)
def test_r2_metadata_and_pagination_fail_closed(tmp_path, kind):
    client = FakeR2()
    original_list = client.list_objects_v2
    original_get = client.get_object

    def listing(**args):
        page = original_list(**args)
        if kind == "pagination":
            return {"IsTruncated": True, "NextContinuationToken": "same", "Contents": []}
        if page["Contents"]:
            if kind == "duplicate":
                page["Contents"].append(page["Contents"][0])
            elif kind == "outside":
                page["Contents"][0]["Key"] = "secrets/token.json"
            elif kind == "size":
                page["Contents"][0]["Size"] = Limits().stored_object + 1
        return page

    def getting(**args):
        result = original_get(**args)
        if kind == "revision":
            result["ETag"] = '"different"'
        elif kind == "length":
            result["ContentLength"] += 1
        return result

    client.list_objects_v2 = listing
    client.get_object = getting
    with pytest.raises(BackupError):
        backup.create_backup(
            R2ReadOnlySource(client, "synthetic-bucket"), SCOPE, tmp_path / "invalid.slbk", PASSWORD
        )
    assert not (tmp_path / "invalid.slbk").exists()
    assert all(body.closed for body in client.bodies)


def test_injected_sdk_requires_finite_timeouts_and_retry_limit():
    client = FakeR2()
    client.meta = SimpleNamespace(
        config=SimpleNamespace(
            retries={"total_max_attempts": 3}, connect_timeout=10, read_timeout=30
        )
    )
    with pytest.raises(BackupError, match="bounded_client_configuration_required"):
        R2ReadOnlySource(client, "synthetic-bucket")
    client.meta.config.retries = {"total_max_attempts": 2}
    assert R2ReadOnlySource(client, "synthetic-bucket")


def test_nested_unexpected_types_and_fields_rejected(tmp_path):
    for replacement in ("unexpected string", {"raw_route": [0, 0]}):
        objects = synthetic_objects()
        key = next(key for key in objects if "canonical" in key)
        value = json.loads(gzip.decompress(objects[key]))
        value["summary"] = replacement
        objects[key] = backup._json_bytes(value)
        with pytest.raises(BackupError):
            backup.create_backup(MemorySource(objects), SCOPE, tmp_path / "invalid.slbk", PASSWORD)


def test_create_rejects_invalid_password_before_source_reads(tmp_path):
    source = MemorySource()
    with pytest.raises(BackupError, match="^password_length_invalid$"):
        backup.create_backup(source, SCOPE, tmp_path / "invalid.slbk", b"short")
    assert source.list_calls == source.get_calls == 0
    assert not list(tmp_path.iterdir())


def test_verify_requires_a_regular_file_before_decryption(tmp_path, monkeypatch):
    def forbidden_decryption(*_args):
        pytest.fail("A non-file must be rejected before password derivation")

    monkeypatch.setattr(backup, "_fernet", forbidden_decryption)
    with pytest.raises(BackupError, match="^regular_file_required$"):
        backup.verify_backup(tmp_path, PASSWORD)


def test_local_source_refuses_nonregular_file_before_read(tmp_path, monkeypatch):
    key, data = next(iter(synthetic_objects().items()))
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    path.write_bytes(data)
    source = LocalSource(tmp_path)
    # Simulate a device/FIFO at the read boundary, without blocking any platform.
    monkeypatch.setattr(sources.stat, "S_ISREG", lambda _mode: False)
    with pytest.raises(BackupError, match="^regular_file_required$"):
        list(source.inventory(SCOPE))
    assert source.operations["get"] == source.read_bytes == 0


def test_local_source_refuses_file_in_place_of_namespace(tmp_path):
    namespace = tmp_path / "coach/profiles/v1"
    namespace.parent.mkdir(parents=True)
    namespace.write_bytes(b"unexpected file")
    source = LocalSource(tmp_path)
    with pytest.raises(BackupError, match="^source_directory_required$"):
        list(source.inventory(SCOPE))
    assert source.operations["get"] == 0


def test_local_source_inventory_permission_errors_fail_closed(tmp_path, monkeypatch):
    (tmp_path / "coach/profiles/v1").mkdir(parents=True)
    source = LocalSource(tmp_path)

    def denied(_path):
        raise PermissionError("SYNTHETIC_SECRET_SENTINEL")

    monkeypatch.setattr(sources.os, "scandir", denied)
    with pytest.raises(BackupError, match="^source_read_failed$"):
        list(source.inventory(SCOPE))
    assert source.operations["get"] == 0


def test_local_inventory_stops_streaming_at_directory_limit(tmp_path, monkeypatch):
    (tmp_path / "coach/profiles/v1").mkdir(parents=True)
    source = LocalSource(tmp_path)
    consumed = 0

    @contextmanager
    def huge_directory(directory):
        def entries():
            nonlocal consumed
            for index in range(100000):
                consumed += 1
                yield SimpleNamespace(
                    path=str(directory / f"synthetic-directory-{index}"),
                    is_dir=lambda **_kwargs: True,
                )

        yield entries()

    monkeypatch.setattr(sources, "DEFAULT_LIMITS", Limits(objects=2))
    monkeypatch.setattr(sources.os, "scandir", huge_directory)
    with pytest.raises(BackupError, match="^source_directory_limit$"):
        list(source.inventory(SCOPE))
    assert consumed <= 9
    assert source.operations["get"] == 0


def test_r2_body_close_failure_is_sanitized_and_prevents_publication(tmp_path):
    client = FakeR2()
    original_get = client.get_object

    class BrokenClose(io.BytesIO):
        def close(self):
            super().close()
            raise RuntimeError("SYNTHETIC_SECRET_SENTINEL")

    def getting(**kwargs):
        response = original_get(**kwargs)
        original_body = response["Body"]
        response["Body"] = BrokenClose(original_body.getvalue())
        original_body.close()
        return response

    client.get_object = getting
    path = tmp_path / "failed.slbk"
    with pytest.raises(BackupError, match="^source_close_failed$"):
        backup.create_backup(R2ReadOnlySource(client, "synthetic-bucket"), SCOPE, path, PASSWORD)
    assert not path.exists()


def test_restore_rechecks_grown_file_with_bounded_read(archive, tmp_path, monkeypatch):
    original_open = Path.open
    read_sizes = []

    class RecordedRead:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.stream.close()

        def read(self, maximum=-1):
            read_sizes.append(maximum)
            assert maximum == Limits().stored_object + 1
            return self.stream.read(maximum)

    def changed_file(path, mode="r", *args, **kwargs):
        if "objects" in path.parts and mode == "rb":
            with original_open(path, "ab") as stream:
                stream.write(b"x" * (Limits().stored_object + 2))
            return RecordedRead(original_open(path, mode, *args, **kwargs))
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", changed_file)
    target = tmp_path / "grown-restore"
    with pytest.raises(BackupError, match="^local_restore_checksum_failed$"):
        backup.restore_local(archive, PASSWORD, target)
    assert read_sizes == [Limits().stored_object + 1]
    assert (target / "RESTORE-INCOMPLETE").exists()
    assert not (target / "RESTORE-COMPLETE").exists()


def test_create_refuses_git_destination_before_source_access(tmp_path):
    repository = tmp_path / "synthetic-repo"
    repository.mkdir()
    (repository / ".git").write_bytes(b"synthetic worktree marker")
    source = MemorySource()
    with pytest.raises(BackupError, match="^private_directory_must_be_outside_git$"):
        backup.create_backup(source, SCOPE, repository / "history.slbk", PASSWORD)
    assert source.list_calls == source.get_calls == 0
    assert not (repository / "history.slbk").exists()


@pytest.mark.parametrize("operation", [backup.restore_plan, backup.restore_local])
def test_recovery_refuses_git_destination_without_writes(archive, tmp_path, operation):
    repository = tmp_path / "synthetic-repo"
    repository.mkdir()
    (repository / ".git").mkdir()
    target = repository / "private-recovery"
    with pytest.raises(BackupError, match="^private_directory_must_be_outside_git$"):
        operation(archive, PASSWORD, target)
    assert not target.exists()
