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
and errors, and per-stage R2 SDK operations. It excludes fitness values, source dates,
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

## Latency breakdown

The shared runner now also publishes one sanitized object at
`refresh/diagnostics/v1/<run_id>.json`. It uses the last existing stage's guarded
store and remaining write budget: one extra small PUT, no extra inventory,
unchanged write/object/byte limits and schedules. Failed publication propagates
when there was no prior failure; it never replaces an existing source failure.
This object contains UTC start/finish and successful source-check receipt times,
not Garmin-local activity/sleep dates. Existing targeted reports and receipts
keep their keys and schemas. The final diagnostics PUT itself is outside the
stored stage counters/timings and pipeline duration.

`pipeline_diagnostics` exposes validated stages, source outcomes and operation
counts on completed-run responses. `latency` distinguishes:

| Measurement | Boundary and limits |
| --- | --- |
| `github_queue_ms` | Workflow `created_at` to `run_started_at`, when both exist; workflow-level queue proxy, not individual runner-job timing |
| `github_startup_ms` | Workflow start to Python pipeline start, including checkout/setup/install; unknown on old runs without instrumentation |
| `pipeline_ms` | Python orchestration, including source/storage waits, excluding GitHub queue/setup and final diagnostics publication |
| `garmin_fetch_ms` | Timed SDK `connectapi` calls, including failures; login is separately `login_ms`; transport calls outside `connectapi` are not included |
| `r2_read_ms`, `r2_write_ms` | Timed SDK GET including body read, HEAD, LIST page retrieval, and PUT; existing write-guard LIST is included in reads |
| `activity_file_import_ms` | Existing artifact refresh/import call, including reuse checks, downloads, decoding and export |
| `coach_input_ms` | Existing one-activity Coach Input operation, including reads, source/profile/context checks, generation or reuse and export |
| `request_elapsed_ms` | Original shared targeted request/reservation to this status observation |
| `request_to_ready_observed_ms` | Same boundary, only when the complete fresh canonical package is observed; an upper bound on availability, not the actual first-ready instant |

Every unknown/unavailable duration is JSON `null`, never zero. A measured zero
or an explicitly source-free stage can be zero. Component timers overlap source
and R2 timers; do not sum them into total latency. A component that was never
invoked stays unknown. General/manual run-only status has no original request
clock, so its request-wait fields stay null. Older shared jobs without the new
request timestamp also stay unknown. Compatible requests share the original
job timestamp; this measures shared sync wait, not each client's chat latency.

The existing single Worker diagnostics event adds timings for its canonical R2
reads/LISTs and GitHub HTTP calls. Summary/profile counters still count service
reads; their storage latency is not included in canonical read timing. No new
per-object logging or trace sampling changes are introduced. This delivery does
not measure Garmin's device-upload/finalization delay or time before a user
asks for data. Synthetic tests establish boundaries and operation counts, not
production performance.
