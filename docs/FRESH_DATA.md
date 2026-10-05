# Fresh data on request

`sync_latest_activity` and `sync_latest_night` are explicitly authorized sync
tools. They may contact Garmin and update private R2. Historical read tools
keep their read-only contracts. Use the fresh tools when the user asks to fetch
current data, and show the reported source-check time and missing components.

## Date and readiness contract

The activity tool accepts an exact activity ID and/or `expected_date`, a
Garmin-local workout date. A newly expected workout defaults to today's date
in `HEALTH_TIMEZONE`; without a valid zone, an explicit date is required.
Multiple matching sessions require an exact ID. UTC is never substituted for
an absent Garmin-local activity date.

The night tool accepts `wake_date`: the local calendar date on waking. Its
default is today's date in `HEALTH_TIMEZONE`. During travel, pass the actual
Garmin-local wake-date. This first delivery targets that wake-date; it does not
guess a previous night's date before the user has woken. Requests are limited
to seven days back and at most one day ahead of UTC to accommodate travel.
Existing Garmin wall-clock timestamps and IANA/DST fallback rules are reused.

Both tools read canonical R2 objects before considering dispatch. A successful
source check is valid for five minutes, independently of completeness. Object
upload times, CSV/index times and successful GitHub runs are not source checks.
The returned `freshness` contains scope, source-check time, source age,
completeness, missing components and the canonical package. `data_ready` means
that the requested package is complete and source-fresh.

An activity package includes canonical local start time, summary, detailed
analysis, running Coach Input and the newest explicitly supplied context for
the same ID. It verifies required files and the Coach Input's source revisions,
effective profile and context. Missing profiles and unavailable endurance
analysis are explicit blocked states. Strength sessions do not need running
Coach Input. Raw FIT/TCX and GPS are not returned.

A night package includes canonical sleep and associated HRV. Sleep must have
positive duration, an ordered timestamp window, actual stages within that window
and a matching local wake-date; a window
marked unconfirmed is incomplete. HRV requires actual detailed readings for
the same date with valid reading timestamps. Garmin can expose a partial night
before finalizing it. Empty or zero-duration source responses preserve existing
canonical sleep while recording a negative source check.

Canonical data can be served even when summaries or monthly indexes lag.
Missing Coach Input or an outdated source/context/profile pointer can be
repaired by an R2-only targeted job without Garmin login or file downloads.
Older installations without the pointer receive this bounded derived repair.

## Coordination and limits

The existing `RefreshCoordinator` persists the scope, correlation UUID, run ID,
state and polling count in an additive SQLite table. Compatible requests share
the same job. Incompatible IDs/dates are never attached to one another. Each
scope type retains a five-minute dispatch cooldown and a 12-job UTC-day budget.
R2-only coach repairs have a separate one-minute lease but consume the same
activity budget. Existing R2 write/storage guards still apply.

The targeted workflow shares `garmin-sync` concurrency with scheduled jobs.
Activity discovery stays within the existing seven-day window and details are
limited to the selected ID. Unchanged complete files use existing source
fingerprints instead of unconditional forced downloads. The night job calls
only `get_sleep_data(wake_date)` and `get_hrv_data(wake_date)`.

Use `refresh_status(request_id=...)` for follow-up. A run ID can also resolve a
known targeted job. There are at most three short polling windows per job,
with two GitHub checks and a four-second interval per window. Stop when
`should_continue_polling=false`, even if `terminal=false`, and report the
60-second retry guidance. Later status reads can still observe completion.
The two-hour observation deadline stops polling; it never authorizes a
duplicate dispatch while the existing job is unconfirmed or still active.

When GitHub returns no dispatch receipt, only the request UUID's private R2
receipt may resolve the run ID. The unrelated latest workflow is never used.
An uncertain POST retains its reservation. A definite dispatch rejection (for
example, invalid inputs or access denied) ends that job and retains the cooldown,
so corrected requests can retry without leaving the scope permanently active.
Failure to read run details after an accepted POST still retains correlation.
Failed or cancelled runs do not
confirm readiness, and partial/failed source retrieval does not advance a
successful source checkpoint. A successful empty source response gives a
two-minute negative-result cooldown; the five-minute dispatch floor can
additionally delay a retry. No broad refresh is a workaround for these limits.

Canonical activity discovery sorts indexed candidates by date before limiting
them to 20. Only a current checkpoint can pin the selected ID; older canonical
sessions outside the recent window do not block a new source check. When no
indexed/current-checkpoint ID exists, at most two year-prefix listings cover the
recent window across New Year, each bounded to 1,000 objects, with 20 candidates
in total. A
truncated listing fails explicitly; supply an exact ID. Per-activity listings
are also bounded. This avoids unbounded history scans in chat requests.

## Additive storage

- `refresh/checks/v1/<scope>.json`: successful source observation, including
  negative results. No advancement after failed/partial source retrieval.
- `refresh/requests/<uuid>.json`: request-to-run receipt for correlation.
- `refresh/reports/<run-id>.json`: per-job report, including partial results.
- `activities/<year>/<id>/coach-input/v1/latest-ready.json`: derived pointer to
  an immutable canonical analysis and its source object ETags.

Existing canonical source keys, historical coach analyses, profiles and the
historical coach backfill plan remain readable. The single-activity coach path
does not list the full activity inventory or replace the backfill plan. No data
migration or deletion is needed to publish this code.

## Incremental targeted Coach Input

The single-activity pipeline records an additive `input_signature` on the
ready pointer. It covers the supplied activity metadata, all three canonical
source revisions, the complete historically effective profile, the selected
context key and revision, and the analyzer version. Editing a profile or context
under the same ID still invalidates reuse. A future profile does not invalidate
an analysis for an earlier workout.

When those inputs match and the immutable analysis still exists in the activity
listing, the pipeline returns that ready analysis without downloading FIT JSON,
endurance JSON or TCX, running the analyzer or writing R2. It still lists only
the selected activity prefix and the profile prefix, and reads profiles and the
small ready pointer. Reuse does not constitute a new Garmin source check.

Legacy pointers are rebuilt once to acquire the signature. Missing source files
or profiles remain blocked, a missing analysis is regenerated, and stores with
incomplete revision metadata conservatively rebuild. `run_one(..., force=True)`
bypasses reuse for recovery. Existing immutable versions and the historical
backfill plan are preserved. A repair without Garmin summary metadata retains
known moving time only while the FIT and TCX source hashes are unchanged.

Run `python scripts/benchmark_coach_reuse.py` from the repository root for a
synthetic 1 MiB TCX fixture with no Garmin or external storage calls. The same
fixture run with the pre-change `coach_backfill.py` provides the baseline:

| Targeted call | GET before/after | LIST before/after | PUT before/after | Download bytes before/after |
| --- | --- | --- | --- | --- |
| Initial generation | 4 / 4 | 2 / 2 | 2 / 2 | 1,048,820 / 1,048,820 |
| Unchanged metadata | 5 / 2 | 2 / 2 | 0 / 0 | 1,049,320 / 714 |
| Repeated R2-only repair | 7 / 2 | 2 / 2 | 0 / 0 | 1,050,678 / 714 |

The benchmark also reports local elapsed time. One local run for unchanged
metadata measured 0.876 ms before and 0.465 ms after; these are in-memory Python
timings, not production network latency. This change reduces actual downloads
and analyzer work; it does not parallelize existing calls or reduce LIST counts.
PUTs were already skipped for unchanged targeted analyses. Scheduled source
windows and reconciliation require separate incremental improvements.

The pipeline logs `targeted_coach_input` with only the `analysis_reused` boolean
so a private canary can confirm the reuse branch. It logs no activity IDs or
fitness values. Run one known activity to upgrade a legacy pointer, then repeat
the same targeted request and check that reuse is true and the package is ready.

## Incremental general manual refresh

The older `refresh_today` tool still dispatches the broad refresh workflow, but
its selected new/recent running activities now use the same single-activity
Coach Input pipeline and input signature. It keeps the existing ten-activity
limit, file-fingerprint checks and source observations. Missing profiles or
artifacts remain explicit; failed analysis does not turn a successful
file import into a file failure. Missing Garmin-local dates may be recovered
from matching canonical metadata; the UTC summary date is never a local date.

This path no longer invokes the historical coach backfill or modifies its plan.
The initial activity inventory used to choose candidates remains unchanged.
The broad health steps and their recent reconciliation windows remain active;
the integration checks recent source data instead of assuming a modified-since
feed. Thus "incremental" means reusing unchanged artifacts and analyses, not
eliminating all Garmin observations.

Summary export, shared by general, scheduled and targeted activity updates,
now skips objects whose single-PUT content checksum, size, type and encoding
match. It uses HEAD requests and retains all write-budget guards for changed,
missing or unrecognized objects. A repeated unchanged export preserves the
manifest's `generated_at`: this records content generation, not source-check
freshness. Each new UTC day still gets its two recovery snapshots. The manifest
is written last, so an interrupted export can resume without rewriting completed
objects. Missing files are repaired even if the manifest claims unchanged data.

`python scripts/benchmark_summary_reuse.py --baseline` reproduces the original
always-write export; omit `--baseline` to measure the new path with synthetic
CSV files and no external services. For an unchanged repeat, the baseline uses
five PUTs and one inventory LIST; the new path uses one manifest GET, five HEADs,
zero PUTs and zero inventory LISTs. Initial generation still writes five objects
and adds metadata checks. A changed CSV writes its current object, today's
snapshot and the manifest; a new day writes two snapshots and the manifest.
These are local operation counts, not production latency or total-cost claims.

The metadata checks follow [R2's S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
and [S3's ETag semantics](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html).
Multipart, missing or unexpected ETags conservatively write through the guards.
Private-canary diagnostics log only summary objects written/unchanged and the
coach reuse boolean; no private fitness values are added to these diagnostics.

## Measurement and private-canary validation

Responses and structured diagnostics expose canonical JSON GETs, canonical
LISTs, summary/profile reads, GitHub requests and elapsed milliseconds. The
summary/profile counters count service calls; their internal storage requests
are not included in canonical GET/LIST totals. Logs contain control state and
correlation UUIDs, never fitness values or Garmin activity IDs.

The synthetic runtime fixture for a complete, fresh night uses three canonical
GETs (checkpoint, sleep, HRV), zero LISTs and zero GitHub requests. Fresh complete
activities also require zero GitHub requests. Previously the latest-activity
tool always dispatched and polled. These fixtures verify request volume, not
production latency or total cost. Real Garmin/GitHub delays remain external.

Before deploying the private canary, review this PR and its passing Python,
Worker, privacy and dry-run checks. Then, under separate deployment approval,
validate a fresh stored package, one missing workout, one partial night,
duplicate requests, travel/DST dates and a source edit. Check source age,
package ID, missing components, bounded requests and warm-worker reads. Observe
a representative run before considering promotion to the independent public
history. Do not enable the data-producing workflow in the public source repo.
