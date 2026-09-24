#!/usr/bin/env bash
# Slipstream local bootstrap. Account-specific cloud resources and OAuth policy
# are intentionally configured with the reviewed steps in docs/INSTALL.md.
set -euo pipefail

BOLD=$'\033[1m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RED=$'\033[31m'; RESET=$'\033[0m'
say() { printf '%s\n' "$*"; }
ok() { printf "${GREEN}✓${RESET} %s\n" "$*"; }
warn() { printf "${YELLOW}!${RESET} %s\n" "$*"; }
die() { printf "${RED}✗ %s${RESET}\n" "$*" >&2; exit 1; }

cd "$(dirname "$0")"

say "${BOLD}Slipstream local bootstrap${RESET}"
say "This prepares the repository. It does not create broad cloud credentials"
say "or weaken Access policy to automate security-sensitive dashboard steps."
say

missing=0
check() {
  command -v "$1" >/dev/null 2>&1 || { warn "Missing $1 ($2)"; missing=1; }
}
check python3 "install Python 3.12 or newer"
check git "install Git"
check node "install current Node.js"
check npm "installed with Node.js"
check gh "install GitHub CLI from https://cli.github.com"
[ "$missing" = 0 ] || die "Install the missing prerequisites and rerun ./setup.sh."

python3 - <<'PY'
import sys
if sys.version_info < (3, 12):
    raise SystemExit("Python 3.12 or newer is required.")
PY
ok "Prerequisites found"

if ! gh auth status >/dev/null 2>&1; then
  say "Signing in to GitHub..."
  gh auth login
fi
ok "GitHub CLI authenticated"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
PYTHON=./.venv/bin/python
"$PYTHON" -m pip install --quiet --upgrade pip
"$PYTHON" -m pip install --quiet -r requirements.txt
ok "Python dependencies installed"

(cd worker && npm ci)
ok "Worker dependencies installed from the lockfile"

if ! (cd worker && npx wrangler whoami >/dev/null 2>&1); then
  say "Signing in to Cloudflare..."
  (cd worker && npx wrangler login)
fi
ok "Cloudflare Wrangler authenticated"

cat <<'EOF'

Local preparation is complete.

Continue with docs/INSTALL.md, starting at section 2. The remaining steps create
a private R2 bucket, restricted GitHub secrets, the Garmin session, the Worker,
and the exact-email Cloudflare Access Managed OAuth policy.

Only create the secrets listed in docs/INSTALL.md; current Slipstream has no
secret-path or unauthenticated connector mode.
EOF
