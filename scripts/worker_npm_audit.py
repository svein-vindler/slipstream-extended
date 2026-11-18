"""Full npm audit; temporary acceptance of one exact locked picomatch advisory."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ADVISORY = "https://github.com/advisories/GHSA-fc5q-39xc-vjpf"
LOCK_SHA256 = "a5b77cd9c2ff7e025a89f4d7db88c400427ffa48b71a0e60681a45c1a8d45d97"
CHAIN = {
    "@cloudflare/vitest-plugin": ["vitest"],
    "@rolldown/plugin-babel": ["picomatch", "vite"],
    "agents": ["@rolldown/plugin-babel"],
    "fdir": ["picomatch"],
    "picomatch": None,
    "tinyglobby": ["fdir", "picomatch"],
    "vite": ["picomatch", "tinyglobby"],
    "vitest": ["picomatch", "tinyglobby"],
}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def strict_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_value):
        raise ValueError("invalid JSON constant")
    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant)


def accepted_findings(report, lock_bytes, audit_exit):
    """Accept clean audit or precisely the approved current production chain."""
    require(type(report) is dict and set(report) == {
        "auditReportVersion", "vulnerabilities", "metadata"}, "audit report shape/error")
    require(type(report["auditReportVersion"]) is int and report["auditReportVersion"] == 2
            and type(report["vulnerabilities"]) is dict, "audit report version")
    lock = strict_json(lock_bytes)
    require(type(lock) is dict and lock.get("lockfileVersion") == 3
            and type(lock.get("packages")) is dict, "lockfile shape")
    findings = report["vulnerabilities"]
    metadata = report["metadata"]
    require(type(metadata) is dict and set(metadata) == {"vulnerabilities", "dependencies"}
            and type(metadata["vulnerabilities"]) is dict
            and type(metadata["dependencies"]) is dict
            and set(metadata["dependencies"]) == {
                "prod", "dev", "optional", "peer", "peerOptional", "total"}
            and all(type(v) is int and v >= 0 for v in metadata["dependencies"].values()),
            "audit metadata")
    counts = dict.fromkeys(("info", "low", "moderate", "high", "critical", "total"), 0)
    counts.update(high=len(findings), total=len(findings))
    require(metadata["vulnerabilities"] == counts
            and all(type(v) is int for v in metadata["vulnerabilities"].values()),
            "audit counts or other severity")
    require(type(audit_exit) is int and audit_exit == (1 if findings else 0),
            "npm audit command failed")
    if not findings:
        return []
    require(set(findings) == set(CHAIN) and hashlib.sha256(lock_bytes).hexdigest() == LOCK_SHA256,
            "new finding or changed dependency lock")
    require(lock["packages"]["node_modules/picomatch"]["version"] == "4.0.7",
            "picomatch version")
    for name, via in CHAIN.items():
        item = findings[name]
        require(type(item) is dict and set(item) == {
            "name", "severity", "isDirect", "via", "effects", "range", "nodes", "fixAvailable"}
            and item["name"] == name and item["severity"] == "high"
            and type(item["isDirect"]) is bool and item["nodes"] == ["node_modules/" + name]
            and type(item["via"]) is list and type(item["effects"]) is list
            and all(type(effect) is str and effect in CHAIN for effect in item["effects"])
            and type(item["range"]) is str and item["range"]
            and type(item["fixAvailable"]) in (bool, dict), "audit finding shape/node")
        fix = item["fixAvailable"]
        require(type(fix) is bool or (
            set(fix) == {"name", "version", "isSemVerMajor"}
            and type(fix["name"]) is str and fix["name"]
            and type(fix["version"]) is str and fix["version"]
            and type(fix["isSemVerMajor"]) is bool), "malformed fix metadata")
        if via is not None:
            require(all(type(edge) is str for edge in item["via"])
                    and sorted(item["via"]) == sorted(via), "new advisory/dependency chain")
        else:
            require(len(item["via"]) == 1 and type(item["via"][0]) is dict, "picomatch advisory")
            leaf = item["via"][0]
            require(leaf.get("url") == ADVISORY and leaf.get("source") == 1247710
                    and leaf.get("name") == leaf.get("dependency") == "picomatch"
                    and leaf.get("severity") == "high" and leaf.get("range") == "<=4.0.7",
                    "new picomatch advisory")
    return sorted(findings)


def main():
    try:
        # Preserve the full audit, without omitting production/dev dependencies.
        result = subprocess.run(["npm", "audit", "--json"], check=False,
                                capture_output=True, encoding="utf-8")
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        accepted = accepted_findings(strict_json(result.stdout),
                                     Path("package-lock.json").read_bytes(), result.returncode)
        if accepted:
            print("TEMPORARY CI RISK ACCEPTANCE: GHSA-fc5q-39xc-vjpf; picomatch4.0.7; "
                  "known locked production/transitive chain: " + ", ".join(accepted)
                  + ". Vulnerability is NOT fixed; no Worker deployment is authorized.",
                  file=sys.stderr)
        return 0
    except (OSError, ValueError, TypeError, KeyError):
        print("npm audit rejected: new finding, changed lock/version, error or malformed report.",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
