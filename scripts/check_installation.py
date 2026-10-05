#!/usr/bin/env python3
"""Read-only, resumable installation checks with actionable next steps."""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]
GUIDE = "docs/INSTALL.md"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def command(args, cwd=ROOT):
    # Commands are fixed argument arrays. Captured output is never printed or
    # copied into the report; provider errors may contain account details.
    result = subprocess.run(args, cwd=cwd, capture_output=True, timeout=60, check=False)
    if result.returncode:
        raise RuntimeError("Read-only command did not succeed")
    return result.stdout


def check(*, online=False, repo=None, worker_url=None, bucket="slipstream-data",
          root=ROOT, execute=command, opener=None):
    if repo and not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("Repository must use owner/name")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,62}", bucket):
        raise ValueError("Use the configured R2 bucket name")
    if worker_url:
        parsed = urlsplit(worker_url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path not in {"", "/", "/mcp"}):
            raise ValueError("Worker URL must be an HTTPS base URL or /mcp URL without credentials")
    rows = []

    def step(name, test, action, section):
        try:
            ok = test()
            status = "passed" if ok else "needs_setup"
        except (OSError, RuntimeError, ValueError, TypeError, KeyError, AttributeError, URLError, subprocess.TimeoutExpired):
            status = "needs_check"
        rows.append({"step": name, "status": status, "next_action": None if status == "passed" else action,
                     "guide": f"{GUIDE}#{section}"})

    step("local_tools", lambda: sys.version_info >= (3, 12)
         and all(shutil.which(tool) for tool in ("git", "node", "npm", "gh")),
         "Install the prerequisites, then rerun this check.", "1-prerequisites")
    step("python_dependencies", lambda: all(importlib.util.find_spec(name) for name in ("boto3", "garminconnect")),
         "Run the bootstrap or install requirements.txt in the current Python environment.", "1-prerequisites")
    wrangler = root / "worker/node_modules/wrangler/bin/wrangler.js"
    step("worker_dependencies", lambda: wrangler.is_file(),
         "Run npm ci in worker/ using the committed lockfile.", "1-prerequisites")
    if online:
        node = shutil.which("node") or "node"
        def wr(*args):
            return execute([node, str(wrangler), *args], cwd=root / "worker")
        step("cloudflare_login", lambda: bool(json.loads(wr("whoami", "--json")).get("accounts")),
             "Sign in with the project-local Wrangler and confirm the intended account.", "2-enable-and-create-cloudflare-r2")
        if repo:
            def github_secrets():
                value = json.loads(execute(["gh", "api", f"repos/{repo}/actions/secrets", "--jq", "[.secrets[].name]"], cwd=root))
                return {"GARMINTOKENS", "CLOUDFLARE_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"} <= set(value)
            step("github_secrets", github_secrets,
                 "Add only the documented restricted R2 credentials and Garmin session secret.", "3-create-restricted-r2-credentials-for-github-actions")
            step("refresh_workflow", lambda: json.loads(execute(
                ["gh", "api", f"repos/{repo}/actions/workflows/refresh.yml"], cwd=root))["state"] == "active",
                "Enable Actions and the refresh workflow in your own installation repository.", "5-seed-summaries-and-detailed-data")
        else:
            rows.append({"step": "github_target", "status": "not_checked",
                         "next_action": "Provide --repo owner/name to check GitHub secrets and refresh configuration.", "guide": GUIDE})
        def stored(key):
            with tempfile.TemporaryDirectory(prefix="slipstream-install-check-") as directory:
                output = Path(directory) / "object"
                wr("r2", "object", "get", f"{bucket}/{key}", "--remote", "--file", str(output))
                return output.is_file() and output.stat().st_size > 0
        step("stored_summaries", lambda: stored("summary/activities.csv") and stored("summary/health_daily.csv"),
             "Run the first bounded summary refresh and inspect its private Actions result.", "5-seed-summaries-and-detailed-data")
        def worker_secrets():
            names = {item["name"] for item in json.loads(wr("secret", "list", "--format", "json"))}
            return {"ACCESS_AUD", "ACCESS_TEAM_DOMAIN", "MCP_HOSTNAME"} <= names
        step("worker_oauth_configuration", worker_secrets,
             "Deploy the Worker and configure the exact Access application and hostname secrets.", "6-deploy-the-worker")
        if worker_url:
            def protected():
                request = Request(worker_url.rstrip("/").removesuffix("/mcp") + "/mcp", method="POST",
                                  headers={"content-type": "application/json"}, data=b'{}')
                try:
                    (opener or build_opener(NoRedirect())).open(request, timeout=20).close()
                except HTTPError as error:
                    return error.code in {401, 403}
                return False
            step("anonymous_mcp_protection", protected,
                 "Check Access/Worker configuration: anonymous MCP must be denied. Then verify an authenticated tool call.", "7-enable-cloudflare-zero-trust-free")
        else:
            rows.append({"step": "worker_target", "status": "not_checked",
                         "next_action": "Provide --worker-url to check anonymous endpoint protection.", "guide": GUIDE})
    else:
        rows.append({"step": "cloud_checks", "status": "not_checked",
                     "next_action": "Rerun with --online, --repo owner/name and --worker-url after local preparation.", "guide": GUIDE})
    pending = next((row for row in rows if row["status"] != "passed"), None)
    return {"schema_version": 1, "kind": "installation-check", "checks": rows,
            "next_step": pending["step"] if pending else "authenticated_client_validation",
            "client_checks": ["Connect through Managed OAuth and call data_status and health_status.",
                              "Call coach_profile to check effective profiles; create one if absent, then check coach_input."],
            "ready_for_authenticated_client_test": online and pending is None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--online", action="store_true", help="Read cloud metadata and stored objects; never write")
    parser.add_argument("--repo")
    parser.add_argument("--worker-url")
    parser.add_argument("--bucket", default="slipstream-data")
    parser.add_argument("--report", type=Path, default=ROOT / ".granular/installation-check.json")
    args = parser.parse_args()
    try:
        report = check(online=args.online, repo=args.repo, worker_url=args.worker_url, bucket=args.bucket)
    except ValueError as error:
        parser.error(str(error))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for row in report["checks"]:
        print(f"{row['step']}: {row['status']}")
        if row["next_action"]:
            print(f"  {row['next_action']} ({row['guide']})")
    print(f"Next step: {report['next_step']}")


if __name__ == "__main__":
    main()
