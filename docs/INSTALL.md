# Complete Slipstream Extended self-hosted installation

This guide creates one personal Slipstream instance in accounts you control.
No fitness data or infrastructure is shared with the project maintainer.

## 1. Prerequisites

Install Git, Python 3.12+, Node.js, npm, the GitHub CLI (`gh`) and a current web
browser. Create accounts for Garmin, GitHub and Cloudflare.

Create your own repository from the public Slipstream Extended repository by
using **Use this template** (recommended) or **Fork**. It may be public because
generated fitness data is never committed; use a private repository if you
prefer. Clone it and enter the folder:

```bash
git clone https://github.com/<you>/slipstream-extended.git
cd slipstream-extended
```

Run the local bootstrap on macOS/Linux:

```bash
./setup.sh
```

On Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup-windows.ps1
```

Or install manually on macOS/Linux:

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cd worker
npm ci
npx wrangler login
cd ..
```

Manual Windows equivalent:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Set-Location worker
npm.cmd ci
npx.cmd wrangler login
Set-Location ..
```

## 2. Enable and create Cloudflare R2

In Cloudflare, open **Storage & databases → R2 Object Storage**. Enable R2 on the
account and select the free monthly usage option. Cloudflare may require a
payment method even when current usage is free.

Create the bucket:

```bash
cd worker
npx wrangler r2 bucket create slipstream-data
cd ..
```

If asked for a binding name, use `SLIPSTREAM_DATA`. Keep the bucket private and
on the **Standard** storage class. The R2 free tier does not apply to Infrequent
Access storage.

## 3. Create restricted R2 credentials for GitHub Actions

In **R2 → Manage R2 API tokens**, create an account token restricted to:

- Object Read & Write
- only the `slipstream-data` bucket

Copy the access key ID and secret access key once. Also copy the Cloudflare
account ID. Add them under **GitHub repository → Settings → Secrets and
variables → Actions → New repository secret**:

```text
CLOUDFLARE_ACCOUNT_ID
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
```

Do not use a full-account Cloudflare token or commit these values.

## 4. Create the Garmin session secret

Authenticate the GitHub CLI, then run the login helper from the repository root:

```bash
gh auth login
python scripts/garmin_login.py
```

Complete Garmin MFA if prompted. The helper stores the refreshable session as
the encrypted GitHub Actions secret `GARMINTOKENS`; the password is not stored.

## 5. Seed summaries and detailed data

Enable Actions for the repository if GitHub asks. Start and watch the first
summary refresh:

```bash
gh workflow run refresh.yml
gh run watch
```

Then run the bounded detailed pilot once:

```bash
gh workflow run granular-pilot.yml
```

Scheduled activity, HRV, sleep and body-composition backfills take over. They
are resumable and automatically become cheap read-only checks after eligible
history is complete.

For an initial installation with years of history, a trusted computer can
optionally advance the exact same plans overnight. Complete the normal setup
first, then follow [LOCAL_BOOTSTRAP.md](LOCAL_BOOTSTRAP.md). The cloud schedules
must be paused while local mode runs.

After detailed HRV and sleep objects exist, build the compact monthly history
indexes once:

```bash
gh workflow run health-history-index.yml -f stream=all -f recent_months=0
```

The job never contacts Garmin, is safe to rerun, and skips unchanged months.
Future HRV and sleep ingestion updates affected months automatically. See
[HEALTH_HISTORY.md](HEALTH_HISTORY.md) for the equivalent local command and the
query limits.

## 6. Deploy the Worker

From `worker/`:

```bash
npm run typecheck
npm test
npm run deploy
```

The first deploy creates the Worker, rate limiter and RefreshCoordinator Durable
Object and attaches R2. Copy the base URL; the MCP URL is that URL plus `/mcp`.
The endpoint intentionally returns an OAuth configuration error until the next
steps and secrets are complete.

## 7. Enable Cloudflare Zero Trust Free

Open **Zero Trust** and choose **Zero Trust Free**. If Cloudflare returns to the
welcome page, activation succeeded; click **Get started**. A generated team name
is acceptable. Cloudflare's built-in one-time PIN login is sufficient for a
personal installation; Google or another identity provider is optional.

## 8. Create the Access application

Go to **Access controls → Applications → Create new application**:

1. Choose **Self-hosted and private**.
2. Select the **Workers** destination type.
3. Select `slipstream-mcp` and protect production and preview URLs.
4. Leave browser-based RDP/SSH/VNC rendering off.
5. Create an **Allow** policy named `Slipstream owner`.
6. Under Include, select **Emails** and enter the exact allowed email.
7. Keep available identity providers enabled.
8. Name the application `Slipstream MCP` and set its session to **1 week**.

Protect the whole Worker, not only `/mcp`. Unauthenticated requests are then
rejected before they consume Worker or R2 operations.

## 9. Enable Managed OAuth

In **Additional settings**, expand **Managed OAuth (Beta)**:

- Enable Managed OAuth.
- Keep **Allow localhost clients** off.
- Keep **Allow loopback clients** off.
- Add the client redirect URIs. For ChatGPT add:

```text
https://chatgpt.com/connector_platform_oauth_redirect
https://chatgpt.com/connector/oauth/*
```

- Use the application session duration (1 week) for grants.
- Keep the access-token lifetime at the default (about 15 minutes).
- Leave CORS, App Launcher, browser rendering and Service Auth at defaults
  unless a client specifically requires them.

Create the application. Copy the **Application Audience (AUD) Tag**. Under Zero
Trust settings, copy the team domain, for example
`https://team-name.cloudflareaccess.com`.

## 10. Configure Worker assertion validation

Store the Access values and the exact public Worker hostname as Worker secrets:

```bash
cd worker
npx wrangler secret put ACCESS_TEAM_DOMAIN
npx wrangler secret put ACCESS_AUD
npx wrangler secret put MCP_HOSTNAME
npm run deploy
```

For `MCP_HOSTNAME`, enter only the hostname, for example
`slipstream-mcp.example.workers.dev`—no `https://`, path or port. Paste one
value at each prompt. Do not store installation-specific values in
`wrangler.jsonc`.

The MCP server is read-only by default. If you want ChatGPT to save immutable
coach profiles and append-only activity context, explicitly enable those tools:

```bash
npx wrangler secret put MCP_WRITES_ENABLED
```

Enter the exact value `true`, then redeploy. The Worker enforces a shared
cooldown plus an exact limit of 60 accepted MCP writes per authenticated
identity and UTC day. Leave the secret unset for the smallest attack surface.

## 11. Optional: allow chat-triggered refresh

Create a fine-grained GitHub token restricted to this repository with
**Actions: Read and write**. Store the repository and token:

```bash
npx wrangler secret put GITHUB_REPOSITORY
npx wrangler secret put GITHUB_ACTIONS_TOKEN
npm run deploy
```

Use `owner/repository` for the first value. The GitHub permission is
repository-wide, although Slipstream dispatches only `refresh.yml`. Omit both
secrets if you prefer to refresh only from GitHub.

## 12. Connect and verify

Follow [CONNECT.md](CONNECT.md), using:

```text
https://<worker>.<subdomain>.workers.dev/mcp
```

After OAuth login, verify `data_status`, `health_status`, `hrv_curve` for a
recent date and a known `strength_session` or `endurance_session`. Both status
tools should report `storage: "r2"`. Test `refresh_today` only if the optional
GitHub secrets are configured. A chat-triggered refresh now finishes recent
daily summaries, detailed sleep, HRV, activity artifacts and applicable coach
input before reporting `data_ready: true`.

## 13. Cost and security checks

Check **Cloudflare Workers → Metrics**, **R2 → Usage**, and **GitHub → Settings →
Billing → Actions**. The project guards R2 at 5 GiB and bounded writes per run,
but an account-level billing alert is still recommended.

Before making the repository public, confirm `git status` does not show token
files, `.dev.vars`, generated CSV files or Garmin sessions. Read
[SECURITY.md](../SECURITY.md) for the full model.
