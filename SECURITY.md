# Security policy

## Reporting a vulnerability

Do not open a public issue for a security problem. Use GitHub private
vulnerability reporting under **Security → Report a vulnerability**. Never
include real tokens, Garmin sessions, Access assertions, email addresses or
private fitness data in a report.

Maintainers should run the adversarial and operational checks in
[docs/SECURITY_TESTING.md](docs/SECURITY_TESTING.md) before a public release.

## Intended deployment

Slipstream Extended is a single-tenant, self-hosted application. Each installer owns the
GitHub repository, Cloudflare Worker, Access policy and private R2 bucket. It is
not a hosted multi-user service.

## Authentication and authorization

- Cloudflare Access protects the entire Worker before requests reach code.
- Managed OAuth supports discovery, dynamic registration, PKCE and token
  renewal.
- The Access Allow policy should contain only the intended exact email.
- The Worker validates the Access assertion's RS256 signature, issuer,
  application audience, expiry, subject and email.
- `MCP_HOSTNAME` pins transport acceptance to one exact production hostname.
- `/mcp` is the only application route; every other route returns 404.
- There is no secret URL or unauthenticated fallback.
- MCP write tools are absent unless the installer explicitly enables them.

The Worker secrets `ACCESS_TEAM_DOMAIN`, `ACCESS_AUD` and `MCP_HOSTNAME` bind
validation to one specific Access organization, application and public origin.
If the Access application or Worker hostname changes, update the applicable
values and redeploy.

## Data exposure

MCP exposes activity and health summaries, normalized HRV, sleep,
body-composition, strength and GPS-free endurance analysis. It does not expose:

- GPS coordinates or routes
- raw FIT/TCX files
- arbitrary R2 keys or objects
- Garmin session tokens or passwords
- Cloudflare or GitHub credentials

Garmin access is read-only. An explicit `refresh_today` call may dispatch the
fixed GitHub Actions refresh workflow, which updates the installer's private R2
bucket.

## Abuse and cost controls

- Cloudflare Access rejects unauthenticated traffic at the edge.
- Authenticated traffic is limited per Access subject.
- Request bodies, R2 objects and decompressed payloads have hard limits.
- Responses are `no-store`, `nosniff` and use a no-referrer policy.
- A SQLite Durable Object lease prevents simultaneous refresh dispatches.
- The same Durable Object atomically enforces write cooldowns and an exact
  per-identity UTC-day write budget, including under concurrent requests.
- Refresh has a cooldown and bounded status polling.
- GitHub jobs share a concurrency group, cap each batch and fail closed against
  project R2 storage/object/write budgets.
- Completed historical backfills perform only a cheap eligibility check until
  new data appears.

Cloud-account quotas still apply before or around application code. Configure
billing alerts and monitor Workers, R2 and GitHub Actions usage.

## Secrets

GitHub Actions secrets:

- `GARMINTOKENS`
- `CLOUDFLARE_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

Cloudflare Worker secrets:

- `ACCESS_TEAM_DOMAIN`
- `ACCESS_AUD`
- `MCP_HOSTNAME`
- optional `MCP_WRITES_ENABLED` (literal `true` enables bounded append-only tools)
- optional `GITHUB_REPOSITORY`
- optional `GITHUB_ACTIONS_TOKEN`

Use an R2 token restricted to Object Read & Write on only the Slipstream bucket.
For on-demand refresh, use a fine-grained GitHub token restricted to one
repository with Actions read/write. GitHub grants this at repository scope even
though Slipstream invokes only `refresh.yml`.

Never commit `.dev.vars`, Garmin token files, generated CSV files or local
secret files. Before publishing, inspect `git status` and the staged diff.

## Logging

Application logs contain bounded error categories, not tokens or object data.
Cloudflare invocation logs are disabled in the default configuration to reduce
noise and retention. Distributed traces use 1% head sampling for low-cost
incident diagnosis; the application adds no fitness payloads or credentials as
trace attributes. Temporarily use `wrangler tail` for diagnosis and do not paste
sensitive output into public issues.

## Supply chain

- Python and npm dependencies are transitively version- and hash-locked.
- Dependabot proposes weekly dependency and GitHub Actions updates.
- CI runs Python and npm vulnerability audits, Python lint/tests, TypeScript
  checks, unit tests and Workers-runtime Durable Object tests before merge.
- Garmin access relies on the community `garminconnect` package because Garmin
  has no official personal API; upstream changes can require maintenance.

## Incident response

If a credential or session leaks:

1. Revoke it at the issuing service immediately.
2. Create a least-privilege replacement.
3. Update the relevant GitHub or Worker secret.
4. Redeploy or rerun the affected workflow.
5. Review GitHub Actions, Access and Cloudflare logs for unexpected use.

If an identity should lose access, remove it from the Access Allow policy or
delete the Access application. If the application itself may be compromised,
recreate it, update `ACCESS_AUD`, and reconnect the MCP client.

## Privacy and removal

Slipstream Extended sends no project telemetry and has no shared backend. To remove an
installation, delete its MCP connector, Access application, Worker, R2 bucket,
GitHub Actions secrets and repository. Deleting the R2 bucket permanently
removes the stored fitness exports.

## Non-affiliation

Slipstream Extended is independent and is not affiliated with or endorsed by Garmin,
Cloudflare, GitHub, Anthropic or OpenAI.
