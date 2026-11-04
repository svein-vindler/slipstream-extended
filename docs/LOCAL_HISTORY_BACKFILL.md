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

## One ordinary bounded local run

After the offline plan and your normal provider/R2 setup, use one command to
collect a bounded batch, publish its verified dates, and finish eligible closed references:

```bash
python -m pipeline.history_backfill daily --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --confirm-r2-write --archive-reload
```

The default remains seven dates and 56 source HTTP requests, in serial order.
Omit `--archive-reload` when you intend ordinary reads only. An explicitly ordered
larger daily batch may use `--max-dates 30 --max-http 240`; it still runs bounded
source/publication chunks of at most seven dates. It never walks an unbounded
queue, starts a schedule or increases the source limits. Collection and
publication remain separate phases: source reads only the selected Garmin
credential, and publication reads only R2 credentials. Existing `collect`,
`resume` and `publish` commands remain available.

When continuing an installation that used earlier dated controllers, include
**every** old and manual source journal on first use, for example:

```bash
python -m pipeline.history_backfill daily --plan-sha PLAN_SHA256 --confirm-cloud-jobs-paused --confirm-r2-write --archive-reload --ledger PRIVATE_OLD_LEDGER.jsonl --ledger PRIVATE_MANUAL_LEDGER.jsonl
```

After each source/publication chunk, the same run drains up to two eligible
closed ISO weeks (Monday–Sunday) or calendar months. It reuses exact verified
canonical source snapshots from this run for initial calculation, fetching only
missing native inputs/context. It waits to finalize a period while another selected
source date can still change it. Both source/context fences remain fresh R2 reads;
no current reference is published from an unuploaded or stale source. Remaining
publication slots can finish closed references without another source request.
At most five publication phases run in total, including source publication.

`daily_status=bounded_complete` means the selected raw work and closed reference
queue completed. `raw_pending` or `baselines_pending` returns exit 2, preserving
known progress and explicit deferred/held/blocked counts. Open periods appear in
`baseline_refresh.deferred_open`; they wait for local calendar closure and do not
force a partial reference. Each actual phase report remains in
`daily_baseline_reports`. A repeated all-published daily run can contact R2 to
check closure; it does not collect Garmin again.

These private paths become bound to the job. Later commands may omit the flags
and reuse the bound list; an explicit replacement list must match it. Keep the
original journals. The tool verifies their complete physical-line hash chains
and counts old, manual and new source usage together over a rolling 24 hours.
Midnight does not reset the 30 reload/240 HTTP ceiling. Unknown reservations stay
charged and cannot age into permission. This is an application ceiling, not a
claim about Garmin's available quota or permission to start source work.

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
retry. Two fully empty bounded batches record `no_more_observed`; this is an
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
A `daily` run opens at most five phases in total, including reference-only
completion. All phases share one physical execution counter with at most
1000 class A operations (LIST+PUT), 2256 class B operations (GET/HEAD), and
10 MiB attempted write bytes. Every phase keeps the original limits and freshly
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
