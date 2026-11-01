"""Explicit selected-provider identity check; no fitness data or R2 access."""

from __future__ import annotations

import json
import os
import sys

from .output_privacy import SafeArgumentParser, safe_cli
from .sources.garmin import _login
from .sources.provider_config import selection


def run(*, env=None):
    chosen = selection(os.environ if env is None else env)
    if chosen.provider == "personal":
        from .sources.garmin_personal import PersonalGarmin, PersonalSourceError

        account = PersonalGarmin.identify_account(chosen.credential())
        if account != chosen.account:
            raise PersonalSourceError("account_mismatch")
    else:
        client = _login(chosen=chosen, identity_diagnostic=True)
        # The shared login already verifies the configured account and loads
        # identity. Do not request an activity, health day or another profile.
        _ = client.account_identity
    return {
        "kind": "garmin-provider-check",
        "provider": chosen.provider,
        "status": "authenticated",
        "fitness_data_requested": False,
        "r2_operations": 0,
    }


@safe_cli
def main():
    parser = SafeArgumentParser(
        description="Check the selected Garmin connection without fetching fitness data or using R2."
    )
    parser.parse_args()
    from .sources.provider_diagnostic import ProviderDiagnosticError

    try:
        result = run()
    except ProviderDiagnosticError as error:
        print(json.dumps(error.evidence()), file=sys.stderr)
        raise
    print(json.dumps(result))


if __name__ == "__main__":
    main()
