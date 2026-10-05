# Shared refresh pipeline and measurements

Chat-triggered refresh and the six-hour schedule enter the same Python core:
`pipeline.refresh`. The GitHub workflow passes its existing inputs to
`python -m pipeline.refresh --workflow`; local execution calls that module too.
The existing importers, canonical keys, readiness checks, successful receipts,
source fingerprints and guarded R2 store remain the implementation of each stage.

General refresh restores summaries, optionally remembers activity IDs, refreshes
and exports bounded summaries, checks three recent sleep/HRV dates and three
recent measured body dates, then optionally imports recent activity artifacts
and Coach Input. Scheduled runs finish with recent/weekly health-index repair.
Targeted activity and night modes retain their bounded scope. R2-only activity
repair never logs into Garmin. Explicit historical date/year inputs remain
available; year imports run newest first. Historical work is never inferred from
a normal chat request.

Each stage keeps a separate R2 write budget, matching the former workflow steps.
The process logs into Garmin once and reuses that client. Source errors stop
later imports; scheduled derived-index repair still runs after ordinary failure,
but is skipped after interruption. Failed or partial checks retain the existing
receipt rules. A workflow success alone does not assert data readiness.

## Local use

Prepare ignored local credentials as described in [LOCAL_BOOTSTRAP.md](LOCAL_BOOTSTRAP.md).
Pause cloud Garmin jobs and wait for active runs before using the same core locally:

```bash
python -m pipeline.refresh --confirm-cloud-jobs-paused
python -m pipeline.refresh --confirm-cloud-jobs-paused --mode night --wake-date YYYY-MM-DD
python -m pipeline.refresh --confirm-cloud-jobs-paused --mode activity --include-granular
python -m pipeline.refresh --confirm-cloud-jobs-paused --mode activity --repair-only --activity-id NUMERIC_ID
```

Use a recent Garmin-local wake-date or expected workout date. Local invocation
loads `.env` without overriding existing environment variables, including an
optional ignored token file. Resume after failure by running the same request;
existing canonical data and checkpoints remain reusable. Full history is optional
and uses the established backfill entrypoints.

## Diagnostics

Each run writes ignored `.granular/refresh-diagnostics.json` atomically and emits
a compact `refresh-diagnostics` JSON line. It contains a random correlation ID,
mode, stage names/statuses, elapsed milliseconds, Garmin `connectapi` attempts
and errors, and per-stage R2 SDK operations. It excludes fitness values, dates,
activity IDs, object keys, request arguments, credentials and exception text.
The existing importer reports can contain operational details and belong only
in the private installation; they are separate from this sanitized report.

Garmin counts cover the SDK's `connectapi` calls, including failed attempts;
`garmin_connectapi_available` distinguishes missing instrumentation from zero
calls. R2 counts use the existing store counters, include write-guard inventory,
and exclude internal SDK/HTTP retries. LIST counts are returned pages; GET/HEAD/
PUT counts follow the store's SDK-call counters. Elapsed time includes waiting
for the provider and storage and does not measure client-side chat latency.
Compare matching scopes and readiness, rather than interpreting a partial run
as a speed improvement or extrapolating one run to a guaranteed cost.

Tests verify mode routing, stage order and bounds, independent budgets, one
login, failure/interrupt behavior, historical ordering, diagnostic redaction and
restored instrumentation. Existing importer and source/R2/MCP contracts remain
part of CI.
