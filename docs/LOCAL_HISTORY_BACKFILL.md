# Optional local HR/HRV history

This opt-in terminal tool collects HR/HRV history locally, then publishes verified
curves to your existing private R2 bucket in a separate command. It runs from an
ordinary installed checkout on Windows, Linux or macOS. It does not use GitHub
Actions, start a service, or change schedules. Installation and setup do not run it.

Keep the computer online and the terminal open while collecting. Cloud Garmin
jobs and other local Garmin jobs must be paused and finished first. Use one job
folder and one source owner. Keep the whole job folder: its plan, immutable raw
responses, source clocks, checksums and progress are private health data. The
recommended `.granular/` location is ignored by Git. Do not copy a job to another
state directory to resume it, remove locks, or edit its journal.

## Plan and inspect offline

Use the same private account identity as `GARMIN_ACCOUNT` / provider setup. The
account argument is hashed in the private plan and is not printed. Use an
unshared terminal because shell history may retain typed arguments. Neither the
plan nor status/CSV/help reads credential files or contacts Garmin or R2.

```bash
python -m pipeline.history_backfill plan --start 2025-01-01 --end 2026-01-01 --streams hr,hrv --provider legacy --account YOUR_PRIVATE_ACCOUNT
python -m pipeline.history_backfill status
python -m pipeline.history_backfill missing > .granular/missing-history.csv
```

The output contains `plan_sha256`. Substitute that exact hash in subsequent
commands. The immutable newest-first plan has explicit calendar-date and stream
bounds. It never guesses a first HRV date from the presence of an HR curve.
Choose an appropriate HRV start date yourself, or use a separate dated HR-only
plan once the first job is complete. A different `--state` path selects a distinct
job, not a way to continue a stopped or unknown one. Do not run overlapping jobs
for the same account. Set `--state` before the subcommand when using an explicit
private state directory.

## Collect one conservative batch

Use the existing selected-provider environment conventions. For example,
`GARMIN_PROVIDER`, `GARMIN_ACCOUNT` and `GARMINTOKENS_FILE` select a legacy session;
`GARMIN_PERSONAL_API_KEY_FILE` selects the Personal key. Private
`GARMIN_PROVIDER_FILE` settings also work. Only the selected Garmin credential is
read during collection; R2 credentials are not read. The plan's provider and
account must match the selected connection. There is no provider fallback.

```bash
python -m pipeline.history_backfill collect --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused
```

For `collect` and `resume`, default and maximum per invocation: seven dates,
56 physical HTTP requests,
eight requests and 5 MiB received per date, one physical request at a time,
at least 1.5 seconds between sends, less than 900 seconds per date and 2400
seconds per batch. Authentication and identity sends count too. `--max-dates`
and `--max-http` may reduce those bounds; they cannot increase them. These are
local safety limits, not an official Garmin quota. More work requires another
explicit bounded `collect` or `resume` command. Ready and published targets in
this job are skipped without a new source call.

The dated legacy port guards both SDK sessions and the SDK's separate DI token
refresh POST before login, including when SDK 0.3.20 uses CFFI. Only the fixed
DI refresh endpoint and refresh-token form are admitted. Authentication shares
the date's HTTP/body/deadline/pacing limits, streams its bounded response before
SDK parsing and cannot follow redirects. The first HTTP refusal, unverified
response or transport failure stops the date before any SDK replay, fitness read
or reload. Normal token renewal before such a failure is still allowed; this
port does not persist refreshed credentials. Ordinary shared login and the
separate identity-only diagnostic retain their own policies.

Private date evidence records physical intents, validated HTTP statuses and,
for transport failures, only the exception class. Auth refresh is always
`identity_auth`, even during an HR/HRV read. Auth bodies, headers, token values,
URLs and provider exception text are never written to these journals. A missing
HTTP status remains unknown; it is not evidence of401 or429. Older journals
cannot reconstruct auth traffic that their transport did not observe.

For older Connect charts, legacy SDK 0.3.20 has a separate explicitly authorized,
**date-bound archive request**. Enable it only when you intend to consume Garmin
chart reload capacity:

```bash
python -m pipeline.history_backfill collect --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --archive-reload
```

For a date with missing detail this sends at most one fixed archive reload,
waits the full 120 seconds and reads each missing stream once more. It never
retries a reload. Positive data is established by after-read data, not the reload
acknowledgement. Waiting is serial in this initial portable version. Ordinary
Garmin/MCP access remains GET-only: no shared write switch is introduced.
Personal API archive reload is unverified and unsupported; requesting it records
`unsupported` without credentials or HTTP, and never switches to legacy.
Personal ordinary supported HR/HRV reads keep using the existing adapter.

## Explicit bounded quota experiment

Ordinary plans keep the rolling **30 reload / 240 physical HTTP** ceiling.
An explicitly authorized experiment uses a **new immutable plan**, not an edited
or reset consumed plan. The CLI and `Job.create` bind its absolute rolling
ceilings, at most seven new dates / 56 new HTTP, and the exact required input
journal paths with their byte-prefix hashes. Include every original, manual and
previous experiment journal; a missing, changed or truncated binding stops before
credentials or source access. There is no global quota setting or provider fallback.

For an initial seven-date window, with 30 reloads / 240 HTTP already observed,
the absolute experiment ceilings are 37 / 296. The newest-first window below is
29–23 April 2026, HR and HRV, legacy only (synthetic example dates):

```bash
python -m pipeline.history_backfill --state .granular/quota-experiment-01 plan --start 2026-04-23 --end 2026-04-29 --streams hr,hrv --provider legacy --account YOUR_PRIVATE_ACCOUNT --experiment-max-reloads 37 --experiment-max-http 296 --ledger PRIVATE_ORIGINAL_EVENTS.jsonl --ledger PRIVATE_MANUAL_EVENTS.jsonl --ledger PRIVATE_PREVIOUS_EXPERIMENT_EVENTS.jsonl
```

Use the real complete journal list; the example paths are placeholders, not a
request to invent a previous experiment. Preserve all earlier counters and actual
timestamps. Midnight does not refund the rolling allowance. A later deliberately
bounded stage can use a new plan with larger absolute ceilings derived from the
observed totals, without another code change, but must bind every prior journal
and retain the same seven-date / 56-new-HTTP window. This is not permission for
automatic successive plans or an unlimited until-stop loop.

Qualify the source window and remaining rolling budget before pausing competing
cloud jobs. Keep one actual source owner and finish competing jobs first. Execute
only one ordinary bounded date at a time using the new plan's exact SHA:

```bash
python -m pipeline.history_backfill --state .granular/quota-experiment-01 collect --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --archive-reload --max-dates 1 --max-http 8
```

Inspect the durable result before another separately authorized date. The first
`empty_unresolved`, failure, denial, real rate limit or unknown result stops the
experiment; a new plan must not retry that date or evade the stop. `daily` refuses
experimental plans. Publish verified results separately through the existing
R2-only `publish` command; do not replay already delivered source targets.

The existing account binding and selected cached auth remain in use; no new
credential or interactive login is required by this experiment option. All other
contracts remain: eight HTTP per date including auth/identity, serial 1.5-second
pacing, one date-bound reload, the full 120-second archive wait, bounded bodies
and deadlines, preserved primary/cleanup/native evidence, actual single-writer
control and restore, and unchanged R2 budgets, conditional writes and readback.

An application ceiling is a **local cap**, not an observed Garmin quota. A
native0 result with `empty_unresolved` establishes only missing readable detail;
it does not prove429, a quota reset, or permanent absence. Keep actual provider
HTTP outcomes separate from these local observations. New experiment evidence
records actual UTC intent and ACK timestamps so rolling24-hour usage and calendar
counts in both UTC and `Europe/Oslo` can be compared. Older journals with only
date-close aggregate timestamps remain labeled aggregate-only evidence; they
cannot supply invented per-send calendar timestamps or an unobserved ACK.

## One ordinary bounded local run

After the offline plan and your normal provider/R2 setup, use one command to
collect a bounded batch, publish its verified dates, and finish eligible closed references:

```bash
python -m pipeline.history_backfill daily --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --confirm-r2-write --archive-reload
```

The default remains seven **new source dates** and 56 source HTTP requests, in serial order.
Omit `--archive-reload` when you intend ordinary reads only. An explicitly ordered
larger daily batch may use `--max-dates 30 --max-http 240`; it still runs bounded
source/publication chunks of at most seven dates. Locally verified ready curves
and a prior publication backlog are reused without consuming that new-date
target. They still consume their actual publication allowance. It never walks an unbounded
queue, starts a schedule or increases the source limits. Collection and
publication remain separate phases: source reads only the selected Garmin
credential, and publication reads only R2 credentials. Existing `collect`,
`resume` and `publish` commands remain available.

When continuing an installation that used earlier dated controllers, include
**every** old and manual source journal on first use, for example:

```bash
python -m pipeline.history_backfill daily --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --confirm-r2-write --archive-reload --ledger PRIVATE_OLD_LEDGER.jsonl --ledger PRIVATE_MANUAL_LEDGER.jsonl
```

Source/publication chunks checkpoint the raw curves and indexes first. Reference
assembly waits until all selected raw chunks have checkpointed, then the same run drains up to
two eligible closed ISO weeks (Monday–Sunday) or calendar months per phase.
Each durable daily round has at most fourteen publication phases, including
reference-only finishing phases. It reserves bounded reference
completion while the serial source chunks remain unchanged; the shared request
and byte budgets still stop work earlier when needed.
It reuses exact verified canonical source snapshots for initial calculation,
fetching missing native inputs/context. A later ordinary R2-only continuation
can reuse exact local postimages from complete publication receipts for this
same job and storage target. Hashes alone are never treated as source bodies.
Both source/context fences remain fresh R2 reads; no current reference is
published from an unuploaded or stale source. Capacity, held work or a phase
without progress stays pending rather than starting a retry loop.

`daily_status=bounded_complete` means the selected raw work and its affected
closed references completed. The round durably binds its new-date selection,
stored backlog and affected cohort. An unfinished invocation reuses that selection
and its spent source/publication allowances after restart, rather than selecting
another 30 dates. A completed round can admit the next finite selection without
erasing cumulative counters or old receipts. Global queue
counts remain visible; unrelated older pending periods are not recalculated or
claimed complete by this round. An older journal without a bound cohort keeps
closed references pending; the explicit `publish` command remains available
within the operator's existing authorized resource envelope. `raw_pending` or `baselines_pending` returns exit 2, preserving
known progress and explicit deferred/held/blocked counts. Open periods appear in
`baseline_refresh.deferred_open`; they wait for local calendar closure and do not
force a partial reference. Each actual phase report remains in
`daily_baseline_reports`. A repeated all-published daily run can contact R2 to
check closure; it does not collect Garmin again.

With `--archive-reload`, the first HR or HRV channel still `empty_unresolved`
after the bounded reload stops source work before another date. The other
channel's verified bytes and actual date-native result are retained. Qualified
R2-only finishing can publish those positive curves while source stays paused.
The command returns exit 2 with `daily_status=source_stopped` and
`daily_source_stop=empty_unresolved`; `last_run_status` still describes the
latest actual phase, which may be publication. Reopening the same round does
not clear the source stop or retry reference work automatically. Explicit
`publish` remains available without source, and an explicitly admitted
`daily --resume` can continue untouched targets without reloading the empty one.
An empty channel is not evidence of Garmin quota exhaustion.

These private paths become bound to the job. Later commands may omit the flags
and reuse the bound list; an explicit replacement list must match it. Keep the
original journals. The tool verifies their complete physical-line hash chains
and counts old, manual and new source usage together over a rolling 24 hours.
Midnight does not reset the 30 reload/240 HTTP ceiling. Unknown reservations stay
charged and cannot age into permission. This is an application ceiling, not a
claim about Garmin's available quota or permission to start source work.

For an authorized 30-new-date run, use:

```bash
python -m pipeline.history_backfill daily --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --confirm-r2-write --archive-reload --max-dates 30 --max-http 240
```

There is no extra fixed 24-hour delay between round starts. If a previous run
started late, validated persistent rolling usage determines the exact safe
`daily_not_before`. The command can wait and resume untouched dates in the same
day, with a default total wait allowance of 3600 seconds. `--max-wait-seconds`
accepts 0..21600; zero returns known `source_budget_pending` immediately with
the not-before time and exit 2. Waiting performs no Garmin/R2/GitHub calls, uses
bounded local checks, and cannot reset source counters or retry a failed date.
Real provider/operator pauses, unknown reservations and changed evidence still
stop the run. `daily_new_date_target`, `daily_new_dates`,
`daily_stored_dates_reused`, `daily_round` and `daily_wait_seconds` distinguish
the admitted target from actual progress. Thirty is an operational target, not
an established Garmin quota or a guarantee that every date contains both curves.

Run the ordinary command once per admitted operator phase, using the existing
selected credentials. It does not query GitHub quota/workflow inventory or ask
for an interactive login per date. Do not add separate connection probes around
every date. Native source children still perform their required guarded SDK
authentication/account checks, counted in physical HTTP; their isolation and
provider rejection rules are unchanged. An external operator must establish
the exact cloud-workflow baseline once, stop overlapping jobs, and restore that
same baseline only after physical terminal evidence. This CLI does not activate
schedules or alter workflows. A scheduler's local trigger time is not a reset of
rolling source usage or permission to overlap another owner.

### Explicit reference-only continuation of an older cap

A prior immutable receipt with a smaller publication cap is not silently
upgraded by starting a fresh context or daily round. If known nonadmission left
at most three already affected closed references pending, explicitly authorize
their continuation from the same job and exact plan:

```bash
python -m pipeline.history_backfill --state PRIVATE_JOB daily --help
python -m pipeline.history_backfill --state PRIVATE_JOB continue-publication --plan-sha PLAN_SHA256 --confirm-r2-write --prior-receipt-sha OLD_RECEIPT_SHA256 --independent-gets PRIOR_INDEPENDENT_GET_COUNT --period week/2020-W02 --period week/2020-W03 --period week/2020-W04
```

The example periods are synthetic placeholders; select only the actual bound
pending references reported for that job. The admission binds purpose, job,
plan, exact old receipt and journal tip, previous cumulative usage, independently
metered GETs, target periods and the new finite allowance. It never reads Garmin,
replaces raw source or edits an old receipt/cap. `continuation_status` separates
`bounded_complete`, `references_pending` and `budget_pending`; incomplete results
return exit 2. Retain `publication_continuation_sha256` and resume that admission:

```bash
python -m pipeline.history_backfill --state PRIVATE_JOB continue-publication --plan-sha PLAN_SHA256 --confirm-r2-write --continuation-sha CONTINUATION_SHA256
```

Selecting one of several eligible periods only completes that selection:
`continuation_complete` and `continuation_status=bounded_complete` do not declare
the original cohort finished. Admission and later receipts retain its original
`finalize_dates` and closure evidence. `cohort_complete` and
`original_cohort_complete` distinguish completion of that qualified cohort.
The receipt cohort can include reused stored dates outside the source plan;
these dates do not authorize additional periods. Requested closed periods must
still be supported by published source-plan rows within that exact receipt cohort.
Unfinished original periods remain bound to the carried usage during a later
daily round; they cannot open a fresh resource baseline. Once the original
cohort completes, unrelated global queue entries do not block a new baseline.

Wrong job/plan/receipt, changed proof, an unresolved write/owner or a real new
limit exhaustion remains stopped. Do not remove locks, rewrite receipts, refund
usage or invoke lower-level publication helpers to continue. All old local
source/R2 journals remain part of the installation's actual history.

Already ready or published source targets are skipped without another Garmin
read. A failed date retains the verified targets it completed before the stop,
its original native failure and cleanup result; it does not become a successful
date or parent job. A later explicit R2-only publication may use only the exact
validated per-target receipt and raw proof. An unknown native/write result,
changed proof or retained owner still requires HOLD. The daily command stops at
its first failed phase and does not retry or take over a lock.

Offline status includes `last_run_timings`, separating source pacing/archive
wait, network, local compute, R2 and local control time. Network/R2 measurements
include bounded response-body reading and validation; R2 also includes its guard's
decoding work. They are not pure network latency. A phase that did not measure a
category reports `null`; it does not claim zero work or a completed external
check. Local control time covers this command's orchestration, not independent
cloud-job qualification. Known source denials keep their primary category even
when cleanup also fails; cleanup is recorded separately.

## Stop, pause and explicitly resume

In another terminal, use the same state folder:

```bash
python -m pipeline.history_backfill stop
python -m pipeline.history_backfill status
```

Stop finishes the current date, closes known progress and starts no next date.
Ctrl+C, an unknown native exit, torn durable progress, a persistence failure or
an uncertain storage write retains an owner HOLD. It is not a normal resumable
stop. There is no lock-steal/reset/adoption command. Preserve all evidence and
investigate that HOLD before further operation.

A real HTTP429 stores `rate_limited` and an absolute UTC `not_before`, honoring
Retry-After with a conservative minimum 24 hours. An operator can also persist an
explicit quota pause without any network:

```bash
python -m pipeline.history_backfill pause --reason operator_quota --until 2026-12-01T12:00:00Z
python -m pipeline.history_backfill resume --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused
```

Use `daily --resume` with the same explicit confirmations if you intend the
combined bounded source/publication command after a known pause. An early resume
sends zero HTTP and does not load credentials, including after a process restart. There is no hot polling, automatic next-day wake-up or automatic
retry. Without archive reload, two fully empty bounded batches record
`no_more_observed`. With archive reload, the first unresolved channel records
that source-only pause immediately, including when its sibling is positive. This is an
observation pause, not proof of429 or permanent absence. Explicit resume can
continue safe pending targets after that pause. Previously empty, failed,
unsupported or rate-limited targets remain in the CSV; this version does not
retry them implicitly or offer a retry/reset bypass. Do not create an overlapping
new job to evade the persisted pause or reload an already-attempted date.

Status and CSV distinguish `pending`, `ready` (local, not delivered),
`empty_unresolved`, `rate_limited`, `denied`, `failed`, `unsupported` and
`published`. Unknown or unclosed outcomes additionally hold the owner and are not
promoted. An empty result says only that the attempt returned no readable detail.

For legacy HR, one validated daily envelope may explicitly contain
`heartRateValues: null` and `heartRateValueDescriptors: null`, null min/max, the
known finite summary fields and valid source clocks. That means
`empty_unresolved`; summary values never become curve samples. The exact raw
response, profile binding, source UTC/local clocks and nulls are retained. A
whole HTTP200 body of `null`, a missing curve key, malformed points, wrong
date/profile or an unknown null-envelope schema stops before a reload. HTTP204
remains a valid empty response. With the separate archive opt-in, a verified
empty channel can use the existing one-reload/full120-second-wait path; a still
empty after-read remains unresolved. No normalizer fabricates a zero curve or
replaces a good stored curve from this absence.
Raw wires and their source clocks/nulls remain preserved even if a later phase
stops. Missing samples are never zero-filled. Canonical HR keeps existing UTC/unit
provenance and missing-value flags/counts; exact missing timestamps remain in raw
evidence. Canonical HRV keeps positive readings, while exact timed nulls are
retained in additive `archive_hrv_quality_v1` metadata. Existing MCP readers do
not currently surface that root metadata; the private source proof remains.

## Publish separately to existing R2

Finish/stop collection first. A persisted Garmin quota pause remains in force
but does not prevent this separate source-free R2 publication. Supply the normal R2 environment configuration for
your existing bucket (`CLOUDFLARE_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`). This command reads only R2 credentials; it does not load
Garmin tokens or issue Garmin requests:

```bash
python -m pipeline.history_backfill publish --plan-sha PLAN_SHA256 --confirm-r2-write
python -m pipeline.history_backfill status
```

Each publication handles at most seven verified ready dates. Canonical HR/HRV uses
`night_baseline_refresh.publish_source`: dirty intent, conditional source write
with exact ACK/readback, and ready checkpoint. A changed captured canonical
preimage holds before source intent; an old unknown intent is never adopted.
After exact canonical/index ACK/readback, the same phase drains at most two
closed reference periods through the existing stored-only helper. A private
`RAW-DESTINATIONS-VERIFIED.json` receipt preserves exact raw delivery even if later
reference work fails; it never releases a retained owner or declares completion. A full queue
can defer new raw targets safely and still make bounded reference progress.

Reference control/history/current, sources and indexes share the invocation's
budgets. No new stage or allowance is created. A full month can consume most of
GET400; source targets that cannot fit remain `ready`, with
`baseline_refresh.source_targets_deferred` in status. A full queue or low write
budget also defers before source intent. Aggregate staged bytes may admit only a
fitting newest-first prefix, with its affected indexes reserved; the untouched
suffix remains ready. Lower configured byte caps are respected as well.
The existing `publish` command is also the stored-only completion command:
repeat it only within the existing authorized phase/resource envelope to continue
safe work without new Garmin calls. It remains R2-only even when the plan still
has pending Garmin dates or an active Garmin quota pause. Check `status` after
each bounded call; stop on denial, unknown outcome, HOLD, or no safe progress. Even when all source targets are
published, `publish` still needs R2 admission to drain queued references.

New sources can leave references `bounded_pending` for the next explicit publish
or ordinary ingestion. `published` describes the canonical curve and its index,
not completion of every reference. Set Python `HEALTH_TIMEZONE` to an IANA zone;
this CLI does not discover an environment file or use the separate Worker secret.
Without it reference work reports `calendar_timezone_required`. Open periods wait
until closure. Missing native input is not zero-filled into a curve; replacing a
reference preserves immutable history and freshly checks source/context revisions.

### Finish a stopped daily round without the source

An explicitly admitted operator stage can finish the saved positive targets of
one stopped daily round through the same publisher:

```bash
python -m pipeline.history_backfill --state PRIVATE_JOB publish-source-free --plan-sha PLAN_SHA256 --confirm-r2-write --source-proof PRIVATE_PROOF.json --source-proof-sha PROOF_SHA256
```

This requires known physical source cleanup and a separately authorized R2-only
stage. It does not provide that authorization or resolve an unresolved source
claim. The operator retains the original native result, source stop, pause,
denied targets and HOLD. Only R2 credentials are used; no source request, date
reservation, archive reload, source resume, new plan or replacement job is made.

The immutable proof uses schema `1`, operation `source-free-publication`, the
exact `plan_sha256` and `daily_round`, and `journal: {frames, tip}` for the original
hash-chained prefix. `storage_target_sha256` always binds the intended endpoint
and bucket, including before the job's first publication. `independent_gets` is
the already used independent GET count for this current round, not a new
allowance or lifetime total. `source_stop_proof_sha256` identifies the operator's
original physical-cleanup/source-stop admission. `source_dates` contains exact
`run`, `date`, `intent_sha256`, `result_sha256` and `receipt_sha256` entries for
every date closed in this round plus the original source artifacts of selected
ready or published targets. Unrelated source dates are rejected.

The command derives the fixed selection and reference cohort from the existing
active daily-round event. It verifies source receipts, known date cleanup and
positive wire/view bytes before credentials and again under the publication
owner, and checks the intended R2 target before sending. It publishes only that
selection and finishes only its eligible references. Foreign ready targets and
the unrelated reference queue remain outside this stage. The ordinary
publisher's current source/context fences, conditional writes, ACK/readback,
cleanup and resource limits still apply.

Earlier complete and current-round publication receipts carry their cumulative
usage and round baseline; the current independent-read count is retained. Every
new ordinary receipt also records
`baseline_refresh.execution_totals.source_free_proof_sha256`. Only completed
publication phases bound to this same proof may extend its original journal
prefix. A new source event, altered proof/selection/bytes, unknown owner or
different target stops without automatic adoption or retry.

`source_free_status` reports `bounded_complete`, `raw_pending` or
`baselines_pending`; the latter two return exit `2`. Successful R2 completion can
return exit `0` while `daily_status=source_stopped` and
`source_free_source_native=2` preserve the original stage outcome.
`daily_round` and `source_free_round` identify the same round. It never appends a
daily-round completion event or declares the denied/incomplete source work
successful. Keep the original source evidence and perform the operator's
existing independently metered destination and normal-reader checks before
closing the stage.

Each source publication binds the first
configured R2 endpoint/bucket digest for this job and rejects a later target
change. Existing good curves are preserved. Missing HR objects use create-only
writes; existing empty/invalid HR objects HOLD rather than being overwritten.
Empty-detail HRV objects can be conditionally filled at the exact old revision;
all old summary/context fields are retained. Only actually changed HRV months
are rebuilt, preserving unrelated canonical days and revision references. A
legacy-only `.json.gz` monthly source requires review and causes HOLD.

New writes require a genuine parsed HTTP acknowledgement and revision, then exact
byte/revision/type/encoding readback. A conflict, denial, missing acknowledgement,
changed readback or unknown outcome stops without a second attempt. Published
means verified destination evidence, not merely a local curve or an accepted
reload. R2 limits remain GET400/LIST100/PUT100 and 2 MiB written per publication phase,
with the existing 5 GiB / 100,000 object bucket ceiling (or lower configured limits).
Responses remain bounded (64 KiB error/PUT,512 KiB GET,1 MiB LIST), with 16 MiB
raw / 32 MiB decoded totals, 1 MiB decoded objects and a 900 second job deadline.
One R2Store shares its cached inventory throughout each publication phase.
A daily round keeps a fourteen-phase ceiling, including reference-only
completion. All phases share monotonic physical execution counters across
restart, with a round allowance of at most 1000 class A operations (LIST+PUT),
6000 publication GET/HEAD plus a 256-GET independent verification reserve
(6256 combined class B), and 10 MiB attempted write bytes. Per-round baselines
do not erase cumulative usage; a new baseline requires a validated new round
after completed work. Independent checks remain explicitly metered operator
work, not an extra unbounded allowance or a claim about arbitrary clients.
Every phase keeps the original limits and freshly
inventories the bucket. Earlier live/controller consumption still belongs to the
operator's standing budget; starting this command does not reset that journal. The source's rolling 30/240 ceiling
is shared across all chunks; a new publication phase never resets it.
Valid S3 URL-encoded LIST keys and prefixes are compared with the SDK's
once-decoded representation only when the SDK selected that encoding automatically.
Explicitly requested URL encoding keeps the SDK's encoded response representation;
logical key/prefix validation still applies. Literal percent signs and plus signs
are preserved, and continuation tokens remain opaque. Malformed escapes, invalid
UTF-8, decoded control characters, unsupported encoding and raw/SDK disagreements
stop before inventory can authorize a write. The logical key limit remains 1024 UTF-8
bytes; percent-encoded wire text stays within the existing bounded LIST response.
 Publication
uses no Actions or deployments. Full historical completeness and reference completion are separate; neither is
inferred from successful canonical writes. A synthetic seven-date/two-month
publication required 44 conditional PUTs (14 intent/source/ready groups plus two
indexes), all within the unchanged cap. A known deferred group opens no intent.
