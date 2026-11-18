"""Full npm audit; temporary acceptance of one exact locked picomatch advisory."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ADVISORY = "https://github.com/advisories/GHSA-fc5q-39xc-vjpf"
LOCK_SHA256 = "a5b77cd9c2ff7e025a89f4d7db88c400427ffa48b71a0e60681a45c1a8d45d97"
NAMES = {
    "@cloudflare/vitest-plugin", "@rolldown/plugin-babel", "@vitest/mocker",
    "agents", "fdir", "picomatch", "tinyglobby", "vite", "vitest",
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
    require(hashlib.sha256(lock_bytes).hexdigest() == LOCK_SHA256,
            "changed dependency lock")
    require(lock["packages"]["node_modules/picomatch"]["version"] == "4.0.7",
            "picomatch version")
    packages = lock["packages"]
    visiting, done = set(), set()

    def visit(name):
        require(name not in visiting, "audit dependency cycle")
        if name in done:
            return
        require(name in NAMES and name in findings, "unknown/unresolved audit dependency")
        item = findings[name]
        require(type(item) is dict and set(item) == {
            "name", "severity", "isDirect", "via", "effects", "range", "nodes", "fixAvailable"}
            and item["name"] == name and item["severity"] == "high"
            and type(item["isDirect"]) is bool and item["nodes"] == ["node_modules/" + name]
            and type(item["via"]) is list and type(item["effects"]) is list
            and all(type(effect) is str and effect in NAMES and "node_modules/" + effect in packages
                    for effect in item["effects"])
            and type(item["range"]) is str
            and type(item["fixAvailable"]) in (bool, dict), "audit finding shape/node")
        entry = packages.get(item["nodes"][0])
        require(type(entry) is dict and type(entry.get("version")) is str,
                "audit node absent from authenticated lock")
        fix = item["fixAvailable"]
        require(type(fix) is bool or (
            set(fix) == {"name", "version", "isSemVerMajor"}
            and type(fix["name"]) is str and fix["name"]
            and type(fix["version"]) is str and fix["version"]
            and type(fix["isSemVerMajor"]) is bool), "malformed fix metadata")
        require(item["via"], "missing audit advisory path")
        visiting.add(name)
        for via in item["via"]:
            if type(via) is str:
                visit(via)
                edges = {**entry.get("dependencies", {}), **entry.get("peerDependencies", {}),
                         **entry.get("optionalDependencies", {})}
                require(via in edges, "audit dependency not in authenticated lock")
            else:
                require(type(via) is dict and via.get("url") == ADVISORY
                        and via.get("source") == 1247710
                        and via.get("name") == via.get("dependency") == "picomatch"
                        and via.get("severity") == "high" and via.get("range") == "<=4.0.7",
                        "new picomatch advisory")
        visiting.remove(name)
        done.add(name)

    for name in findings:
        visit(name)
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
