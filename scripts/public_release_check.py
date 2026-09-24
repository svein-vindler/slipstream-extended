#!/usr/bin/env python3
"""Fail when the tracked tree contains private or installation-specific data."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

FORBIDDEN_EXACT_PATHS = {
    ".connector_url",
    ".dev.vars",
    ".env",
    ".mcp_secret",
    "setup-windows.sh",
}
FORBIDDEN_DIRECTORIES = {
    ".garminconnect",
    ".garmintokens",
    ".granular",
}
FORBIDDEN_SUFFIXES = {".fit", ".tcx"}

SECURITY_FILES = {
    "README.md",
    "docs/CONNECT.md",
    "docs/INSTALL.md",
    "docs/MANAGED_OAUTH.md",
    "setup.sh",
    "setup-windows.ps1",
}
LEGACY_AUTH_PATTERNS = {
    r"\bMCP_SECRET\b": "legacy shared MCP secret",
    r"\bDATA_REPO\b": "legacy GitHub data repository binding",
    r"wrangler\s+secret\s+put\s+GITHUB_TOKEN\b": "legacy broad GitHub token name",
    r"Authentication:\s*No authentication": "unauthenticated connector instruction",
    r"leave\s+(?:the\s+)?OAuth\s+blank": "instruction to bypass OAuth",
}

WRANGLER_FORBIDDEN_PATTERNS = {
    r'"account_id"\s*:': "Cloudflare account ID",
    r'"(?:ACCESS_TEAM_DOMAIN|ACCESS_AUD|MCP_HOSTNAME|GITHUB_REPOSITORY|GITHUB_ACTIONS_TOKEN)"\s*:': (
        "deployment-specific value stored in wrangler.jsonc instead of a secret"
    ),
    r"(?<![<\w.-])[a-z0-9-]+\.[a-z0-9-]+\.workers\.dev": (
        "installation-specific workers.dev hostname"
    ),
}

REQUIRED_PUBLIC_FILES = {
    ".upstream-version",
    "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "SECURITY.md",
    "docs/INSTALL.md",
    "docs/PUBLISHING.md",
}

PRIVATE_SLIPSTREAM_OWNER = "svein" + "-" + "vindler"
GITHUB_SLIPSTREAM_REFERENCE_PATTERNS = (
    re.compile(
        r"https://github\.com/(?P<owner>[a-z0-9_.-]+)/"
        r"(?P<repo>slipstream)(?:/|\.git\b|$)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"git@github\.com:(?P<owner>[a-z0-9_.-]+)/"
        r"(?P<repo>slipstream)(?:\.git)?(?=$|[\s\"'<>),;])",
        flags=re.IGNORECASE,
    ),
)

PUBLIC_TEXT_SUFFIXES = {
    ".cjs",
    ".example",
    ".in",
    ".js",
    ".json",
    ".jsonc",
    ".md",
    ".mjs",
    ".ps1",
    ".py",
    ".sh",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}


def _normalize(path: str) -> PurePosixPath:
    return PurePosixPath(path.replace("\\", "/"))


def _contains_private_slipstream_reference(text: str) -> bool:
    for pattern in GITHUB_SLIPSTREAM_REFERENCE_PATTERNS:
        for match in pattern.finditer(text):
            if match.group("owner").lower() == PRIVATE_SLIPSTREAM_OWNER:
                return True
    return False


def audit_paths(paths: list[str]) -> list[str]:
    """Return violations found in a list of repository-relative tracked paths."""
    violations: list[str] = []
    for raw_path in paths:
        path = _normalize(raw_path)
        parts = set(path.parts)
        lower_name = path.name.lower()

        if path.as_posix() in FORBIDDEN_EXACT_PATHS:
            violations.append(f"{path}: private local configuration must not be tracked")
        if parts & FORBIDDEN_DIRECTORIES:
            violations.append(f"{path}: private runtime directory must not be tracked")
        if path.suffix.lower() in FORBIDDEN_SUFFIXES:
            violations.append(f"{path}: raw fitness file must not be tracked")
        if path.parts[:1] == ("data",) and path.suffix.lower() == ".csv":
            violations.append(f"{path}: generated fitness CSV must not be tracked")
        if lower_name.startswith("garmin_tokens") and lower_name.endswith(".txt"):
            violations.append(f"{path}: Garmin session token must not be tracked")

    return violations


def audit_text(path: str, text: str) -> list[str]:
    """Return content violations for a reviewed public-facing file."""
    patterns: dict[str, str] = {}
    if path in SECURITY_FILES:
        patterns.update(LEGACY_AUTH_PATTERNS)
    if path == "worker/wrangler.jsonc":
        patterns.update(WRANGLER_FORBIDDEN_PATTERNS)

    violations: list[str] = []
    for pattern, description in patterns.items():
        if re.search(pattern, text, flags=re.IGNORECASE):
            violations.append(f"{path}: contains {description}")
    return violations


def tracked_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return [item.decode("utf-8") for item in result.stdout.split(b"\0") if item]


def audit_public_metadata(root: Path, paths: list[str]) -> list[str]:
    """Return violations in public-release identity and maintenance metadata."""
    tracked = set(paths)
    violations = [
        f"{path}: required public-release file is missing"
        for path in sorted(REQUIRED_PUBLIC_FILES - tracked)
    ]

    marker = root / ".upstream-version"
    if ".upstream-version" in tracked:
        value = marker.read_text(encoding="utf-8").strip()
        if not re.fullmatch(r"[0-9a-f]{40}", value):
            violations.append(".upstream-version: expected one full lowercase commit SHA")

    workflow_path = ".github/workflows/upstream-sync.yml"
    if workflow_path in tracked:
        workflow = (root / workflow_path).read_text(encoding="utf-8")
        for required in ("git ls-remote", ".upstream-version", "issues: write"):
            if required not in workflow:
                violations.append(f"{workflow_path}: missing safe upstream check: {required}")
        for forbidden in ("git merge", "git push"):
            if forbidden in workflow:
                violations.append(f"{workflow_path}: must not run {forbidden}")

    for relative_path in paths:
        path = Path(relative_path)
        if path.suffix.lower() not in PUBLIC_TEXT_SUFFIXES:
            continue
        text = (root / path).read_text(encoding="utf-8")
        if _contains_private_slipstream_reference(text):
            violations.append(
                f"{relative_path}: contains private Slipstream repository reference"
            )

    return violations


def audit_repository(root: Path) -> list[str]:
    paths = tracked_files(root)
    violations = audit_paths(paths)
    violations.extend(audit_public_metadata(root, paths))
    content_paths = SECURITY_FILES | {"worker/wrangler.jsonc"}

    for relative_path in sorted(content_paths.intersection(paths)):
        text = (root / relative_path).read_text(encoding="utf-8")
        violations.extend(audit_text(relative_path, text))
    return violations


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    violations = audit_repository(root)
    if violations:
        print("Public release check failed:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1

    print("Public release check passed: tracked tree contains no blocked private artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
