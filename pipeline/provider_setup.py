"""Prepare a source change only after the target proves the existing account."""

from __future__ import annotations

from .sources.provider_capabilities import capabilities
from .sources.provider_config import selection


def prepare_switch(provider, env, *, session=None, new_install=False):
    capabilities(provider)  # Validate the closed choice before credentials or HTTP.
    if new_install:
        if (
            provider != "personal"
            or env.get("GARMIN_ACCOUNT")
            or env.get("GARMIN_PROVIDER_FILE")
            or env.get("GARMINTOKENS")
            or env.get("GARMINTOKENS_FILE")
        ):
            raise ValueError("New-install setup cannot replace an existing Garmin account binding.")
        target = {
            **env,
            "GARMIN_PROVIDER": "personal",
            "GARMIN_ACCOUNT": "identity-only",
        }
        from .sources.garmin_personal import PersonalGarmin

        account = PersonalGarmin.identify_account(selection(target).credential(), session=session)
        return {"schema_version": 1, "provider": "personal", "account": account}
    current = selection(env)
    target = dict(env)
    target.pop("GARMIN_PROVIDER_FILE", None)
    target["GARMIN_PROVIDER"] = provider
    if current.account is not None:
        target["GARMIN_ACCOUNT"] = current.account
    else:
        target.pop("GARMIN_ACCOUNT", None)

    from .sources.garmin import _login

    if provider == "legacy":
        client = _login(env=target)
        account = client.account_identity
    else:
        account = current.account
        if account is None:
            legacy = {**target, "GARMIN_PROVIDER": "legacy"}
            client = _login(env=legacy)
            account = client.account_identity
        target["GARMIN_ACCOUNT"] = account
        key = selection(target).credential()
        from .sources.garmin_personal import PersonalGarmin

        client = PersonalGarmin(key, expected_account=account, session=session)
        account = client.verify_account()
    # Reuse the same strict account syntax; no account identifier is printed.
    selection({"GARMIN_PROVIDER": provider, "GARMIN_ACCOUNT": account})
    return {"schema_version": 1, "provider": provider, "account": account}


def configure(provider, path, env, *, session=None, new_install=False):
    """Serialize one operator; replace private metadata only after identity proof."""
    import json
    import os
    import uuid
    from pathlib import Path

    path = Path(path)
    if env.get("GARMIN_PROVIDER") and env["GARMIN_PROVIDER"] != provider:
        raise ValueError(
            "Clear the conflicting GARMIN_PROVIDER environment override before switching."
        )
    if path.name != ".garmin-provider.local.json" or path.is_symlink():
        raise ValueError("Use the private .garmin-provider.local.json settings file.")
    lock = path.with_name(path.name + ".lock")
    staging = path.with_name(path.name + ".new-" + uuid.uuid4().hex)
    # Exclusive creation fails closed; no lock adoption or retry.
    with lock.open("x", encoding="utf-8") as lock_handle:
        try:

            def bounded_settings():
                if not path.exists():
                    return None
                with path.open("rb") as stream:
                    return stream.read(4097)

            previous = bounded_settings()
            if previous is not None and len(previous) > 4096:
                raise ValueError("Provider settings exceed the size limit.")
            current = dict(env)
            if previous is not None:
                if new_install:
                    raise ValueError("New-install setup cannot replace existing settings.")
                current["GARMIN_PROVIDER_FILE"] = str(path)
            value = prepare_switch(provider, current, session=session, new_install=new_install)
            if bounded_settings() != previous:
                raise ValueError("Provider settings changed during account verification.")
            body = (json.dumps(value, indent=2) + "\n").encode("utf-8")
            descriptor = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            os.replace(staging, path)
            return {"provider": provider, "configured": True}
        finally:
            staging.unlink(missing_ok=True)
            lock_handle.close()
            lock.unlink(missing_ok=True)


def main(argv=None):
    import os
    from pathlib import Path

    from .output_privacy import SafeArgumentParser

    parser = SafeArgumentParser(
        description="Choose a Garmin connection or verify a later same-account switch."
    )
    parser.add_argument("--provider", choices=("legacy", "personal"))
    parser.add_argument("--config", type=Path, default=Path(".garmin-provider.local.json"))
    parser.add_argument(
        "--new-install",
        action="store_true",
        help="Personal-only setup for a new empty installation, without legacy credentials.",
    )
    parser.add_argument(
        "--capabilities",
        action="store_true",
        help="Show available features locally; no login or data requests.",
    )
    args = parser.parse_args(argv)
    if args.capabilities:
        from .sources.provider_capabilities import describe

        print(describe(args.provider))
        return
    if args.provider is None:
        parser.error("Choose --provider legacy or personal, or use --capabilities.")
    try:
        configure(args.provider, args.config, os.environ, new_install=args.new_install)
    except Exception:
        raise SystemExit(
            "Garmin connection setup failed. Check the private connection settings before starting another job."
        ) from None
    print(
        "Garmin connection verified and saved. Use GARMIN_PROVIDER_FILE for subsequent local jobs."
    )
    print(
        "Keep the private settings file out of Git. Finish running jobs before changing hosted settings."
    )


if __name__ == "__main__":
    main()
