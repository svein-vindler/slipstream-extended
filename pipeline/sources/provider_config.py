"""Immutable provider choice and bounded credential loading for one ingestion job."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

MAX_CREDENTIAL_BYTES = 65536


def validate_account(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[^\s/\\?#;%]{1,128}", value):
        raise ValueError("Invalid GARMIN_ACCOUNT binding.")
    return value


def validate_personal_key(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[\x21-\x7e]{1,4096}", value):
        raise ValueError("Invalid selected Garmin Personal key.")
    return value


def _settings_file(path):
    try:
        with Path(path).open("rb") as stream:
            body = stream.read(4097)
        if len(body) > 4096:
            raise ValueError("Provider settings exceed the size limit.")

        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("Duplicate provider setting.")
                result[key] = value
            return result

        value = json.loads(body.decode("utf-8-sig"), object_pairs_hook=unique)
        if (
            not isinstance(value, dict)
            or set(value) != {"schema_version", "provider", "account"}
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
        ):
            raise ValueError("Unknown provider settings schema.")
        return value
    except (OSError, UnicodeError, ValueError, RecursionError):
        raise ValueError("Cannot verify the private Garmin provider settings.") from None


@dataclass(frozen=True)
class ProviderSelection:
    provider: str
    account: str | None = field(default=None, repr=False)
    _value: str | None = field(default=None, repr=False)
    _file: str | None = field(default=None, repr=False)

    @property
    def credential_name(self) -> str:
        return "GARMINTOKENS" if self.provider == "legacy" else "GARMIN_PERSONAL_API_KEY"

    def credential(self) -> str:
        value = self._value
        if value and self._file:
            raise ValueError("Set only one selected Garmin credential source.")
        if not value and self._file:
            try:
                with Path(self._file).open("rb") as handle:
                    body = handle.read(MAX_CREDENTIAL_BYTES + 1)
                if len(body) > MAX_CREDENTIAL_BYTES:
                    raise ValueError("Selected Garmin credential exceeds its size limit.")
                value = body.decode("utf-8-sig").strip()
            except (OSError, UnicodeError):
                raise ValueError("Cannot read the selected Garmin credential file.") from None
        if not isinstance(value, str) or not value:
            raise ValueError("Missing selected Garmin credential: " + self.credential_name)
        if len(value.encode("utf-8")) > MAX_CREDENTIAL_BYTES or "\x00" in value:
            raise ValueError("Invalid selected Garmin credential.")
        # Legacy credentials are serialized JSON, including valid whitespace.
        # Only the Personal key is placed directly in an HTTP header.
        if self.provider == "personal":
            validate_personal_key(value)
        return value


def selection(env: Mapping[str, str]) -> ProviderSelection:
    settings = (
        _settings_file(env["GARMIN_PROVIDER_FILE"]) if env.get("GARMIN_PROVIDER_FILE") else {}
    )
    if "GARMIN_PROVIDER" in env and settings and env["GARMIN_PROVIDER"] != settings["provider"]:
        raise ValueError("Garmin provider settings conflict.")
    if env.get("GARMIN_ACCOUNT") and settings and env["GARMIN_ACCOUNT"] != settings["account"]:
        raise ValueError("Garmin account settings conflict.")
    provider = env.get("GARMIN_PROVIDER", settings.get("provider", "legacy"))
    if provider not in {"legacy", "personal"}:
        raise ValueError("GARMIN_PROVIDER must be legacy or personal.")
    account = env.get("GARMIN_ACCOUNT") or settings.get("account") or None
    if account is not None:
        validate_account(account)
    if provider == "personal" and not account:
        raise ValueError("GARMIN_ACCOUNT binding is required before using Personal API.")
    name = "GARMINTOKENS" if provider == "legacy" else "GARMIN_PERSONAL_API_KEY"
    return ProviderSelection(provider, account, env.get(name), env.get(name + "_FILE"))
