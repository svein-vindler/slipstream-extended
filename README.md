# Slipstream Extended

[![CI](https://github.com/svein-vindler/slipstream-extended/actions/workflows/ci.yml/badge.svg)](https://github.com/svein-vindler/slipstream-extended/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A private, self-hosted MCP server for exploring your Garmin history with an
AI assistant.**

Slipstream Extended imports data from Garmin Connect, stores it in your own
private Cloudflare R2 bucket, and exposes safe, analysis-oriented tools through
a Cloudflare Worker. Cloudflare Access Managed OAuth controls who may connect.
There is no shared Slipstream service: every user installs and pays for their
own infrastructure.

## What it includes

- Activity and daily-health summaries
- Minute-level overnight HRV curves
- Sleep stages, sleep score components, respiration, SpO2 and stress
- Bounded HRV and sleep history with automatic weekly summaries for long ranges
- Weight and body-composition measurements
- Strength exercises, sets, reps, weight, work time and rest time
- Endurance laps, kilometre splits, heart-rate distribution and drift
- Versioned coach profiles, append-only RPE/context and prepared coach-input JSON
- Resumable historical backfills that become read-only checks when complete
- Detection and replacement of changed Garmin activity files
- Optional chat-triggered refresh with completion polling

Raw FIT/TCX files remain private in R2. MCP tools never return GPS coordinates,
routes, arbitrary R2 objects or Garmin credentials.

## Architecture

```text
Garmin Connect
      │ read-only session token
      ▼
GitHub Actions ── restore / merge / upload ──► private Cloudflare R2
                                                      │ private binding
                                                      ▼
AI client ◄── MCP + Managed OAuth ── Cloudflare Access ── Worker
```

- GitHub Actions supplies scheduled compute; generated fitness data is not
  committed to Git.
- R2 is the source of truth for summaries, detailed exports and backfill state.
- The Worker validates Cloudflare Access assertions before reading R2.
- A rate limiter, payload bounds and a Durable Object refresh lease protect the
  installation from abuse and duplicate dispatches.

See [Architecture](docs/ARCHITECTURE.md) for the complete flow.

## Install

Each installation needs its own Garmin, GitHub and Cloudflare accounts, plus an
OAuth-capable MCP client. The documented and tested client flow uses ChatGPT.

Start with the [complete installation guide](docs/INSTALL.md). It covers R2,
GitHub secrets, Garmin login, Worker deployment, Cloudflare Access, Managed
OAuth, redirect URIs, testing and cost checks.

`./setup.sh` is a safe bootstrap helper for macOS/Linux, and
`setup-windows.ps1` provides the equivalent native PowerShell flow on Windows.
Both check prerequisites and install local dependencies, but deliberately leave
account-specific R2 credentials and Access policy decisions to the documented
dashboard steps.

## Connect and use

The canonical server URL is:

```text
https://<worker-name>.<workers-subdomain>.workers.dev/mcp
```

It is protected by Cloudflare Access Managed OAuth. There is no secret URL and
no unauthenticated fallback. See [Connecting an MCP client](docs/CONNECT.md).

Example questions:

- “How has my running volume changed over the last eight weeks?”
- “Show last night's HRV curve and compare it with my recent baseline.”
- “Summarize my latest strength session by exercise and category.”
- “Analyze the latest run for splits, heart-rate drift and time at each BPM.”
- “Save my confirmed running zones, then prepare coach input for my latest run.”
- “Refresh today's Garmin data, wait until it is ready, then summarize it.”

More examples are in [Prompt ideas](docs/PROMPTS.md).
See [HRV and sleep history](docs/HEALTH_HISTORY.md) for date-range behavior,
monthly R2 indexes and the one-time index build.

## Keeping data fresh

The summary workflow runs every six hours. Detailed health and activity jobs
are staggered and serialized so Garmin is never queried concurrently. Historical
backfills skip existing objects, persist progress, pause repeated item failures,
and quiesce automatically when complete.

Each six-hour summary refresh also refreshes the three most recent HRV and
detailed-sleep dates. A chat-triggered refresh performs the same recent-health
steps, checks recent activity artifacts and generates coach input for refreshed
running activities. Therefore `data_ready` means these related R2 outputs have
finished—not merely that the summary CSV was written. A configured coach profile
and usable endurance artifacts are still required for an analysis.

For a new installation with years of history, an optional
[local overnight bootstrap](docs/LOCAL_BOOTSTRAP.md) can advance the same R2
plans in consecutive bounded cycles. It is resumable, retains all free-tier
guards, and requires the scheduled cloud jobs to be paused while it runs. The
same local tooling can seed canonical HRV and sleep objects from existing raw
Garmin JSON exports, skipping anything already present in R2.

Coach generation runs as a separate R2-only consumer after activity-producing
workflows. It keeps a per-activity source index, so unchanged FIT/TCX artifacts
are not reread on every historical backfill cycle.

Manual refresh:

```bash
gh workflow run refresh.yml
```

Optional chat-triggered refresh requires a fine-grained, single-repository
GitHub token with **Actions: read and write**. Store both values as Worker
secrets, never in `wrangler.jsonc`:

```bash
cd worker
npx wrangler secret put GITHUB_REPOSITORY
npx wrangler secret put GITHUB_ACTIONS_TOKEN
npm run deploy
```

Use `owner/repository` for `GITHUB_REPOSITORY`. Without these optional secrets,
all read tools still work and only on-demand refresh is disabled.

## Free-tier design

The workflows use R2 Standard storage and fail closed before projected usage
exceeds conservative project limits: 5 GiB stored, 100,000 objects, 250 writes
per run and 512 MiB uploaded per run. These are below Cloudflare R2's published
free allowance of 10 GB-month, 1 million Class A operations and 10 million
Class B operations per month. Authenticated MCP calls are limited to 30 per
minute per identity, leaving substantial room below the Workers Free daily
request allowance for a personal installation.

The public template exposes only read tools by default. Installers may opt in
to immutable coach-profile and append-only activity-context writes; accepted
writes then have an exact 60-per-identity UTC-day ceiling in addition to short
cooldowns.

Coach analysis is deliberately outside the Worker CPU path. The scheduled job
uses existing R2 artifacts only, processes 50 activities per run, and is limited
to 60 writes and 64 MiB per run. Historical sleep and body-composition jobs use
up to 100 dates per stream while retaining the shared 250-write ceiling. The
activity scheduler keeps its 50-activity Garmin limit but carries unused capacity
across year boundaries. Once complete these jobs perform no
further writes until a relevant source, profile or user context changes.

Standard GitHub-hosted runners are free for public repositories. Private
repositories use the account's included Actions allowance. Cloudflare and
GitHub can change their terms, so verify the official pages:

- [Cloudflare R2 pricing](https://developers.cloudflare.com/r2/pricing/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions)

## Privacy and security

- Access is allow-listed by identity in the installer's own Zero Trust account.
- The Worker verifies signature, issuer, audience, expiry and human identity.
- The Worker accepts only the configured production hostname and is read-only
  unless the installer explicitly enables bounded append-only tools.
- Garmin and R2 credentials stay in encrypted GitHub/Cloudflare secrets.
- R2 is private and reached through a Worker binding.
- Generated datasets and tokens are ignored by Git.
- Responses use `no-store`; request and object sizes are bounded.
- Refresh requests are rate-limited, cooldown-protected and atomically leased.

Read [Security](SECURITY.md) before making an installation available outside a
personal account.

## Maintenance

Dependabot proposes weekly Python, npm, Wrangler and GitHub Actions updates.
Review and merge only after CI passes. A weekly upstream workflow opens or
updates an issue when the original project changes. It never merges or pushes
upstream code automatically; every change must be reviewed and adapted manually.

Useful documents:

- [Install](docs/INSTALL.md)
- [Connect](docs/CONNECT.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Optional local overnight bootstrap](docs/LOCAL_BOOTSTRAP.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)
- [FAQ](docs/FAQ.md)
- [Security](SECURITY.md)
- [Publishing safely](docs/PUBLISHING.md)
- [Maintainer promotion workflow](docs/MAINTAINER_WORKFLOW.md)
- [Contributing](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)

## Scope

Garmin does not provide an official personal API. Slipstream uses the community
`garminconnect` library and may need maintenance if Garmin changes private
interfaces. It is intentionally single-tenant and read-only with respect to
Garmin.

## License and non-affiliation

[MIT](LICENSE). Slipstream Extended is an independent derivative of
[the original Slipstream project](https://github.com/yhecht/slipstream) by
Yannique Hecht. The clean public history intentionally does not claim GitHub
fork ancestry. Slipstream Extended is not affiliated with or endorsed by Garmin,
Cloudflare, GitHub, Anthropic or OpenAI.
