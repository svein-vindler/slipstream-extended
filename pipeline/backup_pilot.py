"""Manually authorized R2 read-only pilot; secrets enter through hidden local prompts."""

from __future__ import annotations

import getpass
import json
import logging
import re
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

from .backup_source import MIB, BackupError, Limits, R2ReadOnlySource, Scope, safe_file, safe_path
from .private_backup import (
    _json_bytes,
    _loads,
    _Parser,
    _publish,
    _scope,
    create_backup,
    restore_local,
    restore_plan,
    verify_backup,
)

PILOT_LIMITS = Limits(
    objects=100,
    activities=1,
    stored_total=MIB,
    decoded_total=2 * MIB,
    document=2 * MIB,
    archive=3 * MIB,
    operations=20,
)


def check_private_root(root: Path) -> Path:
    root = safe_path(root)
    if not root.is_dir():
        raise BackupError("private_parent_directory_required")
    for parent in (root, *root.parents):
        if (parent / ".git").exists():
            raise BackupError("private_directory_must_be_outside_git")
    # Permissions, encryption and exclusion from sync must be checked by the owner.
    return root


def run_pilot(source: R2ReadOnlySource, scope: Scope, private_root: Path, password: bytes) -> dict:
    """No retries, no remote writes. All plaintext remains in the chosen private area."""
    scope.validate(PILOT_LIMITS)
    if len(scope.activities) != 1 or source.limits != PILOT_LIMITS:
        raise BackupError("pilot_requires_one_activity_and_fixed_limits")
    if not isinstance(password, bytes) or not 12 <= len(password) <= 1024:
        raise BackupError("password_length_invalid")
    root = check_private_root(private_root)
    session = root / ("backup-pilot-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    session.mkdir(mode=0o700, exist_ok=False)
    marker = session / "PILOT-INCOMPLETE"
    marker.write_bytes(b"manual-read-only-pilot\n")
    archive = session / "history.slbk"
    target = session / "recovery-test"
    created = create_backup(source, scope, archive, password, limits=PILOT_LIMITS, retries=0)
    verified = verify_backup(archive, password, limits=PILOT_LIMITS)
    # Do not label an empty or profiles-only export a successful activity pilot.
    selected = scope.activities[0]
    activity_prefix = f"activities/{selected[0]}/{selected[1]}/"
    if not any(key.startswith(activity_prefix) for key in verified.objects):
        raise BackupError("pilot_activity_has_no_covered_objects")
    plan = restore_plan(archive, password, target, limits=PILOT_LIMITS)
    if target.exists():
        raise BackupError("pilot_preview_wrote_target")
    restored = restore_local(archive, password, target, limits=PILOT_LIMITS)
    for key, expected in verified.objects.items():
        path = safe_path(target / "objects" / key)
        with path.open("rb") as stream:
            actual = stream.read(PILOT_LIMITS.stored_object + 1)
        if actual != expected:
            raise BackupError("pilot_round_trip_mismatch")
    with (target / "manifest.private.json").open("rb") as stream:
        private_manifest = json.loads(stream.read(PILOT_LIMITS.document + 1))
    if private_manifest["references"] != verified.manifest["references"]:
        raise BackupError("pilot_reference_mismatch")
    # An additional local ciphertext copy proves independent re-verification;
    # choosing a separate device/location remains an explicit owner action.
    with archive.open("rb") as stream:
        encrypted = stream.read(PILOT_LIMITS.archive + 1)
    second = session / "history-verified-copy.slbk"
    _publish(second, encrypted)
    second_verified = verify_backup(second, password, limits=PILOT_LIMITS)
    if second_verified.objects != verified.objects:
        raise BackupError("pilot_copy_mismatch")
    result = {
        "status": "live_test_verified",
        "create": created,
        "verification": verified.summary(),
        "preview": plan,
        "restore": restored,
        "exact_bytes_and_references": True,
        "encrypted_copy_verified": True,
        "source_operations": dict(source.operations),
        "ready_analyses": 0,
        "remote_writes": 0,
        "garmin_calls": 0,
        "production_restore": False,
    }
    with (session / "result-anonymous.json").open("xb") as stream:
        stream.write(_json_bytes(result))
    with (session / "PILOT-COMPLETE").open("xb") as stream:
        stream.write(b"local-historical-copy-only\n")
    marker.unlink()
    return result


def _prompt(label: str) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        return getpass.getpass(label)


def _client(account: str, access_key: str, secret_key: str):
    if (
        not re.fullmatch(r"[a-fA-F0-9]{32}", account)
        or not 16 <= len(access_key) <= 256
        or not 16 <= len(secret_key) <= 1024
    ):
        raise BackupError("invalid_r2_configuration")
    import boto3
    from botocore.config import Config

    # Credentials passed explicitly in memory; no environment/file discovery.
    # No SDK retries or alternative arbitrary URLs, even under ambient AWS settings.
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            connect_timeout=10,
            read_timeout=30,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def main(argv=None) -> int:
    parser = _Parser(description="Manual read-only R2 pilot: one activity, 20 calls, 1 MiB.")
    parser.add_argument("--private-root", type=Path, required=True)
    parser.add_argument("--scope", type=Path, help="Optional private local activity selection file")
    args = parser.parse_args(argv)
    source = client = None
    previous_logging = logging.root.manager.disable
    try:
        check_private_root(args.private_root)
        if args.scope is None:
            year = _prompt("Activity year (hidden): ")
            activity_id = _prompt("Activity ID (hidden): ")
            scope = Scope(((year, activity_id),))
        else:
            with safe_file(args.scope).open("rb") as stream:
                scope = _scope(_loads(stream.read(4097), 4096, PILOT_LIMITS), PILOT_LIMITS)
        scope.validate(PILOT_LIMITS)
        if len(scope.activities) != 1:
            raise BackupError("pilot_requires_one_activity_and_fixed_limits")
        permission = (
            _prompt(
                "FIRST: type READONLY after checking the R2 token is Object Read only (hidden): "
            )
            .strip()
            .upper()
        )
        if permission != "READONLY":
            raise BackupError("read_only_credential_required")
        account = _prompt("Cloudflare account ID (hidden): ")
        bucket = _prompt("R2 bucket (hidden): ")
        access_key = _prompt("R2 read-only access key ID (hidden): ")
        secret_key = _prompt("R2 read-only secret key (hidden): ")
        password = _prompt("New backup password (12-1024 UTF-8 bytes, hidden): ").encode("utf-8")
        if not 12 <= len(password) <= 1024:
            raise BackupError("password_length_invalid")
        confirmation = _prompt("Repeat backup password (hidden): ").encode("utf-8")
        if password != confirmation:
            raise BackupError("password_confirmation_failed")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket):
            raise BackupError("invalid_r2_configuration")
        logging.disable(logging.CRITICAL)
        client = _client(account, access_key, secret_key)
        source = R2ReadOnlySource(client, bucket, PILOT_LIMITS)
        result = run_pilot(source, scope, args.private_root, password)
        print(json.dumps(result, sort_keys=True))
        return 0
    except BackupError as exc:
        print(f"backup_error: {exc}", file=sys.stderr)
    except (OSError, EOFError, getpass.GetPassWarning):
        print("backup_error: local_io_or_private_prompt_failed", file=sys.stderr)
    except KeyboardInterrupt:
        print("backup_error: interrupted", file=sys.stderr)
    except Exception:
        print("backup_error: operation_failed", file=sys.stderr)
    finally:
        if source is not None and any(source.operations.values()):
            print(
                json.dumps(
                    {"source_operations": source.operations, "source_read_bytes": source.read_bytes}
                ),
                file=sys.stderr,
            )
        if client is not None:
            try:
                client.close()
            except Exception:
                print("backup_error: source_close_failed", file=sys.stderr)
        logging.disable(previous_logging)
    return 2


if __name__ == "__main__":
    sys.exit(main())
