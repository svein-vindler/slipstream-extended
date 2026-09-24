#!/usr/bin/env python3
"""One-time Garmin login -> mint a refreshable session token (no password stored).

Handles two-factor: if Garmin prompts, paste the code it sends. On success it
sets the GARMINTOKENS secret on your GitHub repo (so the scheduled Action can
fetch your data). The token auto-refreshes (~1 year), so this is the only login.

If you just hit HTTP 429 (rate limited), wait ~30 min OR switch your machine to
your phone's hotspot (a fresh IP) before retrying.
"""

import argparse
import getpass
import os
import subprocess
import sys
from pathlib import Path

from garminconnect import Garmin
from garminconnect.exceptions import (
    GarminConnectAuthenticationError,
    GarminConnectTooManyRequestsError,
)


def detect_repo() -> str | None:
    if os.environ.get("GH_REPO"):
        return os.environ["GH_REPO"]
    try:
        r = subprocess.run(
            ["gh", "repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"],
            capture_output=True, text=True, check=True,
        )
        return r.stdout.strip() or None
    except Exception:
        return None


def _write_local_token(path: Path, blob: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(blob, encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    print(f"✓ Wrote local Garmin token to {path} (gitignored).", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Mint a Garmin session token for GitHub and/or local bootstrap."
    )
    parser.add_argument(
        "--local-token-file",
        type=Path,
        help="Also save the token locally, for example garmin_tokens.local.txt.",
    )
    parser.add_argument(
        "--local-only",
        action="store_true",
        help="Do not upload the token to GitHub (requires --local-token-file).",
    )
    args = parser.parse_args()
    if args.local_only and not args.local_token_file:
        parser.error("--local-only requires --local-token-file")

    email = input("Garmin email: ").strip()
    password = getpass.getpass("Garmin password: ")

    print("Logging in… (if prompted, enter the MFA code Garmin sends)", file=sys.stderr)
    g = Garmin(email, password, prompt_mfa=lambda: input("Garmin MFA code: ").strip())

    try:
        g.login()
    except GarminConnectTooManyRequestsError:
        sys.exit("\n✗ Garmin rate-limited this IP (429).\n"
                 "  Fastest fix: connect to your phone's hotspot (a fresh IP) and rerun.\n"
                 "  Otherwise wait ~30 min and DON'T retry in between.")
    except GarminConnectAuthenticationError as e:
        sys.exit(f"\n✗ Garmin auth failed: {e}")

    blob = g.client.dumps()  # full session as a JSON string
    print(f"\n✓ Logged in. Session token = {len(blob)} chars.", file=sys.stderr)
    if args.local_token_file:
        _write_local_token(args.local_token_file, blob)
    if args.local_only:
        return

    repo = detect_repo()
    if repo:
        try:
            subprocess.run(["gh", "secret", "set", "GARMINTOKENS", "-R", repo],
                           input=blob.encode(), check=True)
            print(f"✓ Set GARMINTOKENS secret on {repo}. Garmin is done.", file=sys.stderr)
            return
        except Exception as exc:  # noqa: BLE001
            print(f"(couldn't auto-set the secret: {exc})", file=sys.stderr)

    if args.local_token_file:
        print(
            "GitHub secret was not updated; the requested local token is available.",
            file=sys.stderr,
        )
        return

    out = Path("garmin_tokens.txt")
    _write_local_token(out, blob)
    print(f"\nWrote {out} (gitignored). Set it manually with:\n"
          f"  gh secret set GARMINTOKENS -R <your-repo> < {out}\n"
          f"then delete {out}.", file=sys.stderr)


if __name__ == "__main__":
    main()
