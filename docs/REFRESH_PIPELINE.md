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
Stages reuse one R2 client and a lazy bucket inventory owned by this run. The
first required PUT inventories the bucket; subsequent stages carry forward
all reserved object/byte growth, including overwrites. GET/HEAD/LIST reads and
unchanged objects do not trigger the inventory. Each new run starts without
an inventory and checks current storage again; nothing is cached between runs.
An uncertain failed PUT consumes both its stage allowance and the shared
bucket allowance, since a lost response may follow a committed write.

All pipeline R2 writers, including the source-free Coach Input reconciler,
share the workflow's `garmin-sync` concurrency group without cancellation.
Local execution requires those jobs to be paused and idle. These application
guards are conservative checks, not a bucket-wide transactional lock: keep
other bulk uploaders idle while refreshing, as with standalone R2 imports.
Standalone importer stores retain their own fresh inventory and limits.

The process logs into Garmin once and reuses that client. Source errors stop
later imports; scheduled derived-index repair still runs after ordinary failure,
but is skipped after interruption. Failed or partial checks retain the existing
receipt rules. A workflow success alone does not assert data readiness.

Incremental activity summaries merge fields for the same source and activity ID.
Missing, null, invalid or empty fields retain valid previous observations; finite
nonnegative numeric values, including zero, replace them. Missing sport does not
replace a known sport with `Other`. Elapsed-time fallback can fill a new activity's
moving time, but cannot replace an existing valid moving time. Average speed is
recomputed when its merged distance or moving time changes; zero moving time
leaves speed unknown. Obviously invalid old numeric fields are not carried into
the refreshed row. Unfetched historical observations stay unchanged.

The summary receipt records a successful source-window check and the merged CSV
hash. It does not assert that every metric was returned again: retained values
remain earlier Garmin observations, with no new per-field measurement timestamp.
The `Source` column still identifies Garmin; there is no historical regeneration
or migration. A later valid response can update each metric normally.
Targeted Coach Input takes moving time from the merged summary for the selected
source/activity ID, so an omitted moving field cannot become a new elapsed-time
estimate in an immutable analysis while the CSV retains the measured value.

## Activity calendar context

`Activity Date` remains the UTC start instant. The additive final CSV column
`Activity Local Date` carries Garmin's validated local calendar day; the original
columns keep their names and positions. Partial responses retain a valid known
local day. Invalid clocks and impossible dates do not establish local context.
The provider's calendar is used directly, including travel and DST, without
converting it through the configured health/home timezone.

For an unpinned newly expected activity, the existing request default still uses
today's configured `HEALTH_TIMEZONE` day as the requested search scope. This does
not establish the activity's Garmin-local date or discover the device timezone.
During travel, supply the explicit Garmin-local `expected_date`; an `activity_id`
can identify a stored activity without inferring its date from the home timezone.

The local day selects activity namespaces, summary date filters/month/year
groups, context placement and effective coach profiles. Existing rows without
local context remain readable. Summary-only tools use the UTC day and expose
optional `date_source: utc_fallback`; they never download FIT per historical row.
Single-activity readers can confirm legacy context from bounded stored canonical
objects in the UTC year and, only at a year boundary, one adjacent year. Identity,
UTC clock and namespace must agree before the local date is claimed.

Bulk coach resolves legacy dates only for its selected pending batch. Its plan
records `activity_clock_revisions`, including `garmin_local` or `utc_fallback`
and the summary/canonical revisions; unchanged work reuses that association.
The supported canonical contract is numeric schema version 1 (excluding booleans)
and source `garmin-fit`, matching the reader. An unsupported declared contract
stops before a local claim or any new coach/pointer/plan write. Older exports
without a declared contract can retain the CSV UTC fallback in the same year;
they cannot establish a local date or a new cross-year association.
Local cache claims also carry `calendar_contract_version: 1` from that successful
validation. Unmarked local claims are checked again only when selected for the
existing pending batch; they cannot bypass the canonical source/schema gate.
Old completed analyses are not swept or regenerated merely to populate dates.
Historical analyses lacking a confirmed local association remain immutable;
ordinary reads identify their summary date as a fallback rather than a newly
verified Garmin calendar. Date-only summary context can guide sparse detail
responses without fabricating a local start time.

Old context notes under a UTC-year namespace remain visible through bounded
adjacent-year context reads. The newest append-only timestamp basename wins;
an ambiguous timestamp across years is rejected. No objects are moved or deleted.
Summary generation/snapshot archive dates and receipt timestamps remain UTC.

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
a compact, separately projected `refresh-diagnostics` JSON line. The private
report includes a correlation ID; the log contains mode, stage names/statuses,
elapsed milliseconds, Garmin `connectapi` attempts
and errors, and per-stage R2 SDK operations. It excludes fitness values, source dates,
activity IDs, object keys, request arguments, credentials and exception text.
The private local and stored diagnostic reports can contain operational details
and must remain in the private installation; they are not the sanitized log.

After a completed recent-health stage, their additive
`health_validation_rejections` list identifies each rejected sleep/HRV request
using only `stream`, requested ISO `date`, and `category`. Categories come from
the existing validator: `invalid_response`, `wrong_date`, or
`garmin_not_ready`; provider exception text never selects a category. There are
at most two streams times the existing 14-day overlap limit (28 records; normally
six requests). A valid dated pending response remains a successful source check;
an invalid response still preserves the previous successful check and good data.
No list member contains measurements, provider messages, object keys or raw input.
The list is absent when the recent-health result was not available, not proof
that no rejection occurred. Hosted logs and the existing MCP diagnostic projection
exclude the list and its dates. This adds no fetch, retry, acceptance rule or PUT.

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

## Reproduce the storage optimization offline

```bash
python scripts/benchmark_refresh_budget.py --objects 15001
```

This uses only synthetic in-memory storage. For three writing stages followed
by read-only index reuse and diagnostic publication, separate inventories need
64 LIST pages; one shared run needs 16. Both produce identical objects with
four PUTs and the same upload bytes. The benchmark establishes operation
savings, not production latency. Regression tests also cover per-stage byte/
write limits, cumulative bucket limits, overwrite accounting, partial listing
failure, ambiguous committed writes and fresh inventories on the next run.

The six-hour schedule and successful/negative targeted freshness periods stay
unchanged. Removing redundant storage scans does not justify extending source
freshness or reducing the recent-day overlap that catches finalized Garmin data.

## Latency breakdown

The shared runner also publishes one sanitized object at
`refresh/diagnostics/v1/<run_id>.json`. It uses the last existing stage's guarded
store and remaining write budget: one extra small PUT sharing the run's bucket
accounting, with unchanged write/object/byte limits and schedules. An existing
write must already have established the run's shared guarded inventory; the
diagnostic publication never initializes an inventory solely for measurements.

If no stage established a write inventory, R2 publication is skipped and
`diagnostics_persistence_skipped` is true in the local/log report. The local
report and its sanitized JSON log are retained; completed status checks may
therefore have unknown pipeline timings. Publication is also skipped after
interruption. There is at most one extra small PUT, no extra inventory, and
unchanged write/object/byte limits and schedules. Failed publication propagates
when there was no prior failure; it never replaces an existing source failure.
This private object contains UTC start/finish and successful source-check receipt
times plus the bounded requested dates described above. Existing targeted reports
and receipts keep their keys and schemas. The final diagnostics PUT itself is outside the
stored stage counters/timings and pipeline duration.

One compact `refresh-diagnostics-publication` JSON event per cloud run records
publication status/reason, elapsed milliseconds and the publication's SDK
operation deltas. This includes skipped or failed publication without exception
text or object keys. It should show zero LIST pages and at most one PUT. These
final timings are available in the run log, not in the object whose own upload
they measure; updating that object again would require a second PUT. Compare
pipeline duration and publication overhead separately.

`pipeline_diagnostics` exposes validated stages, source outcomes and operation
counts on completed-run responses. Optional stage fields `r2_get_ms`,
`r2_head_ms`, and `r2_list_ms` split the existing `r2_read_ms`. `r2_inventory_ms`
measures full-bucket inventory, including LIST waits and metadata processing;
it overlaps LIST and must not be added to read time. Prefix LISTs are excluded
from inventory time. Older reports may omit these new fields, meaning unknown.
The existing top-level latency fields remain compatible. `latency` distinguishes:

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
