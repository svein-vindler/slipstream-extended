# Complete Slipstream Extended self-hosted installation

This guide creates one personal Slipstream instance in accounts you control.
No fitness data or infrastructure is shared with the project maintainer.

You can stop and resume at any section. The bootstrap helpers run a read-only
local check and show the next setup step. Rerun it at any time using your project
Python environment:

```bash
python scripts/check_installation.py
python scripts/check_installation.py --online --repo <you>/<repo> --worker-url https://<your-worker-host>
```

Online checks read cloud metadata, secret **names**, stored summaries and the
anonymous MCP response. They create or modify nothing. The ignored report is
`.granular/installation-check.json`; it contains statuses and guidance without
account identifiers, URLs, credentials or fitness values. A passed cloud check
means ready for an authenticated client test, not proof that OAuth redirects or
coach setup are complete. Finish with `data_status`, `health_status`,
`coach_profile` and, once a profile exists, `coach_input` in your connected chat.

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
bash setup.sh
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

## 4. Choose the Garmin connection

Existing installations continue using `legacy` when `GARMIN_PROVIDER` is unset.
A new installation can choose `legacy` (the existing community Garmin Connect
connection) or `personal` (Garmin's official Personal API). See the current
[Garmin API documentation](https://developer.garmin.com/gc-developer-program/personal/personal-api/),
[FAQ](https://developer.garmin.com/gc-developer-program/personal/personal-faq/) and
[response schemas](https://developer.garmin.com/gc-developer-program/personal/swagger-docs/).
Garmin price, numerical quotas, Connect+ requirements and general history limits
were not established by those reviewed sources; check Garmin's current terms.

Compare features locally without logging in or requesting data:

```bash
python -m pipeline.provider_setup --capabilities
```

| Feature in this adapter | Legacy | Personal |
| --- | --- | --- |
| Activity and daily health summaries | Yes | Yes |
| HRV, heart rate and weight trend | Yes | Yes |
| Body Battery curve | Yes | Bounded interval extrema; full resolution unverified |
| Full sleep stage timeline and raw stress | Yes | Unsupported |
| Training readiness and full body composition detail | Yes | Unsupported |
| Full detailed activity import, performed strength sets, TCX/GPX conversion | Yes | Unsupported |
| Original activity archive through the collector | Yes | FIT/TCX/GPX only in this adapter; automatic detailed import still requires legacy |

Personal jobs skip unsupported detail work and report `unsupported`; local
bootstrap may finish as `complete_with_unsupported`. This is not full data parity.
Existing detailed artifacts remain available. No automatic provider fallback is
performed. The Worker API and stored canonical paths remain shared.

Original ZIPs have bounded members and decompression with ZIP CRC validation.
FIT members additionally use the pinned FIT SDK integrity/CRC check; GPX/TCX
members require safe XML and the expected root element. Other original formats
from Garmin are not validated by this adapter. `original_validation` reports
byte digests and the request-route association basis. This is not independent
proof that file contents belong to that activity; that association remains
unverified. No filename convention, embedded activity ID or UTC offset is
invented, and these bytes never enter the automatic detailed import in Personal mode.

### Existing connection (legacy)

```bash
gh auth login
python scripts/garmin_login.py
```

Complete Garmin MFA if prompted. The helper stores the refreshable session as
the encrypted GitHub Actions secret `GARMINTOKENS`; the password is not stored.
Leave `GARMIN_PROVIDER` unset or set the repository variable to `legacy`.
The session is not a Garmin-issued read-only scope; the application restricts
fitness requests. Authentication and renewal may use POST.

### Personal API and local account binding

Obtain your Personal API key through Garmin's own program and keep it in a private
UTF-8 text file outside the repository. Never paste the key into chat, a command
argument, a repository variable or an issue. Set `GARMIN_PERSONAL_API_KEY_FILE`
to that file's path in the process that runs setup. A private bootstrap environment
file is loaded only by `local_bootstrap`; it is not read automatically by setup.
The application exchanges the key for one access token per job; the documented
access token lasts 24 hours and has no refresh flow. Key/account errors stop the
job without trying the other connection.

For a **new, empty Personal-only installation**, run:

```bash
python -m pipeline.provider_setup --provider personal --new-install
```

For an existing installation, first set `GARMINTOKENS_FILE` to the private legacy
session file (or provide the existing process-local `GARMINTOKENS`). Then run:

```bash
python -m pipeline.provider_setup --provider personal
```

The existing-account path checks that the current and target connections identify
the same Garmin account before saving `.garmin-provider.local.json`. The new-install
option must never be used to rebind existing data. Once bound, a later Personal
switch does not need an unexpired legacy session. The private metadata contains
an account identifier, not the API key. It is ignored by Git. Set
`GARMIN_PROVIDER_FILE` to this metadata file for subsequent local jobs; relative
paths in a bootstrap environment file resolve beside that file.

For GitHub Actions, use your own installation repository's Settings:

1. Keep the existing restricted R2 secrets from section 3.
2. Save the API key as the encrypted Actions secret `GARMIN_PERSONAL_API_KEY`.
3. Save the verified `account` from the private metadata file as the encrypted
   Actions secret `GARMIN_ACCOUNT`. It must not be a repository variable.
4. Finish running data jobs before changing hosted settings. Upload secrets first;
   set the repository variable `GARMIN_PROVIDER` to `personal` last.
5. Run the read-only installer check with `--online --repo OWNER/REPO
   --garmin-provider personal` to verify the configured mode and secret names.
   It does not prove live Garmin coverage or run a sync.

Each workflow receives only the selected Garmin credential and freezes its source
for that job. Credentials already handed to a running job are not replaced by a
later configuration change. Hosted changes are explicit operator steps, not a
side effect of the local setup command.

### Switching back later

Keep the same private metadata/account binding and supply a valid legacy session:

```bash
python -m pipeline.provider_setup --provider legacy
```

This checks the legacy account and atomically replaces the local source choice.
A wrong account leaves the old choice unchanged. For hosted jobs, keep
`GARMIN_ACCOUNT`, ensure `GARMINTOKENS` is present, finish running jobs, then set
`GARMIN_PROVIDER` to `legacy`. Keep the account secret even on legacy after first
binding, so later jobs continue verifying it and preserving mixed-source HRV.

Switching never deletes or migrates history and never starts a backfill. Stable
Garmin activity identifiers remain shared. Missing incoming HRV observations do
not remove earlier good readings; incoming non-null values can update the same
known instant. Provider provenance and unverified time bases remain distinguishable.
Do not reuse one installation/bucket for a different Garmin account.

## 5. Seed summaries and detailed data

Enable Actions for the repository if GitHub asks. Start and watch the first
summary refresh:

For a recent-data-first setup, keep the three automatic historical jobs paused
in your own repository before enabling the normal refresh. If already active:

```bash
gh workflow disable scheduled-activity-backfill.yml
gh workflow disable scheduled-hrv-backfill.yml
gh workflow disable scheduled-health-detail-backfill.yml
```

Then start the bounded summary refresh:

The first ordinary manual or scheduled refresh can create summaries when both
local summary CSV files are absent and the summary namespace is verified empty.
Other stored data, such as Coach profiles, is preserved. A missing manifest alone
does not prove emptiness: partial summaries, unverified publications and storage
errors stop the refresh rather than replacing existing data. Targeted and
historical jobs do not use this first-summary bootstrap.

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

Full history is optional. You can finish the connector with recent summaries
and the bounded pilot first, then enable the historical backfill workflows in
your own repository when you want older data. Historical work uses the same
resumable plans and does not need to finish before the connector is usable.
Use `gh workflow enable` with those same three filenames when ready.

Activity backfill resumes bounded metadata pages and pending imports in the
existing range progress. Completion requires confirmed source end; completed
historical plans stay read-only. See [ACTIVITY_BACKFILL.md](ACTIVITY_BACKFILL.md)
for call limits, explicit reconciliation/retry and handling legacy plans.

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
npm run deploy -- --no-upload-source-maps
```

Wrangler 4.151 uploads source maps by default. The explicit flag preserves
non-upload without changing configuration. Use it on a preceding
`npm run deploy -- --dry-run --no-upload-source-maps` too; local `.map` files and
a successful dry-run do not prove remote upload behavior or resource readiness.
For an existing installation, retain its reviewed config, entrypoint and release
flags; see the [maintainer release boundary](MAINTAINER_WORKFLOW.md#source-maps-and-existing-installations).

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

Set your usual IANA timezone once (for example `Europe/Oslo`, not `UTC+1`)
as a fallback for older sleep/HRV records without Garmin local times:

```bash
npx wrangler secret put HEALTH_TIMEZONE
```

Enter your own timezone at the prompt. Wrangler deploys a new Worker version
when the secret is saved. This value is optional; new Garmin imports can
provide local `night_of`/weekday fields even without it. Older records without
Garmin local times remain `null` rather than guessing. The existing
`date` in sleep and nightly HRV responses always means the morning/wake-date.

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
