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
positive duration, timestamps, stages and a matching local wake-date; a window
marked unconfirmed is incomplete. HRV requires actual detailed readings for
the same date. Garmin can expose a partial night before finalizing it.

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
An uncertain POST retains its reservation. Failed or cancelled runs do not
confirm readiness, and partial/failed source retrieval does not advance a
successful source checkpoint. A successful empty source response gives a
two-minute negative-result cooldown; the five-minute dispatch floor can
additionally delay a retry. No broad refresh is a workaround for these limits.

Canonical activity discovery is bounded to 20 candidates and one year-prefix
listing of at most 1,000 objects when no indexed/checkpoint ID exists. A
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
