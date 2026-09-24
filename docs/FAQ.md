# FAQ

### What is Slipstream?

A self-hosted pipeline and OAuth-protected MCP server for asking an AI assistant
about your own Garmin activity, recovery, sleep and body-composition history.

### Is it a hosted service?

No. Every user creates their own GitHub repository, Cloudflare Worker, Access
application and R2 bucket. The maintainer neither hosts nor pays for other
users' data or traffic.

### Is it free?

It is designed to fit the free allowances of Cloudflare and GitHub for personal
use, but no software can guarantee a third party will never bill you. R2 must be
enabled and may require a payment method. Use Standard storage, keep the project
guardrails, configure billing alerts and check the current official pricing.
Public repositories receive free standard GitHub-hosted runner usage; private
repositories consume their account's included minutes.

### Where is the data stored?

Only in the installer's private Cloudflare R2 bucket. GitHub Actions uses local
working files during a job, then uploads to R2; generated datasets are not
committed to Git.

### Who can access it?

Only identities allowed by the installer's Cloudflare Access policy. The
recommended policy contains one exact email address. The Worker also validates
the signed Access assertion before reading R2.

### Does it expose my location?

No. Raw FIT/TCX files can contain track data but remain private in R2. MCP
returns summaries and normalized analysis without GPS coordinates or routes.

### What health detail is supported?

Daily summaries, overnight HRV curves, detailed sleep, weight and available
body-composition fields. Detailed backfills target only eligible dates and
reactivate when a new eligible date appears.

### What activity detail is supported?

Strength sets and exercise names, plus GPS-free endurance analysis including
laps, kilometre splits, heart-rate distribution, distance halves and drift.
Changed recent Garmin activities are detected and their stored artifacts can be
replaced. One explicit activity can also be force-refreshed.

### How far back does it go?

Summary history and detailed history are separate. The summary refresh preserves
existing history in R2. Scheduled resumable backfills fill detailed activity,
HRV, sleep and body-composition data within the available Garmin history.

### Do backfills run forever?

The schedules remain enabled so they can notice new eligible data. Once caught
up, each completed backfill exits before Garmin login and R2 writes. Activity
change detection remains a deliberate rolling check, not a historical backfill.

### Can I accelerate the first historical import on my own computer?

Yes. The optional [local overnight bootstrap](LOCAL_BOOTSTRAP.md) runs the same
bounded, resumable modules against the same private R2 bucket. It does not create
a second data format. Pause the hosted schedules first so local and GitHub jobs
cannot contact Garmin concurrently.

### Can it modify Garmin?

No. Garmin access is read-only. The optional `refresh_today` MCP tool only asks
GitHub Actions to re-read Garmin and update private R2.

### Why Cloudflare Access Managed OAuth?

It gives a normal login flow, short-lived tokens, exact-identity policy and edge
rejection without running a separate identity server. Slipstream has no secret
URL and no unauthenticated fallback.

### Is Google login required?

No. Cloudflare's built-in one-time PIN can authenticate the exact email in the
policy. Google or another identity provider can be added later.

### Can several people share one installation?

The code and policy can allow more identities, but data remains single-tenant:
everyone allowed into that deployment sees the same person's dataset. Separate
people should normally deploy separate instances.

### How do I update it?

Pull reviewed changes, run Python and Worker tests, deploy the Worker if Worker
code/config changed, and merge workflow changes only after CI succeeds.
Dependabot proposes dependency updates; the upstream sync workflow prepares a
review PR and never merges automatically.

### How do I remove it?

Remove the client connector, then delete the Access application, Worker, R2
bucket, GitHub secrets and repository. R2 deletion is the destructive step that
removes stored fitness data.
