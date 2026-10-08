# Bounded activity backfill and reconciliation

Activity backfill fills missing canonical files in private R2. Ordinary history
reads do not run it. Latest-activity sync, file status, Coach Input and recent
source-change detection keep their existing contracts. No MCP tool is added.

## Resume the existing range

The existing `backfill/activities/v2/<start>_<end>.json` object now also contains
`pagination.version = 2`: an offset, head/overlap fingerprints, pass digests,
unique observed records and explicit pending imports. The scheduler still uses
`backfill/activities/plan-v2.json`. There is no separate checkpoint store,
bucket, automatic job or budget implementation.

Wire version 2 stores each observed record as `[fingerprint, year, artifact_count]`
under its activity ID. The fixed canonical paths are derived on reading: count
2 means FIT/JSON, 4 adds TCX/endurance JSON, and 0 with a null year means an
unsupported type. Pending metadata and its force flag remain intact. Existing
pagination version 1 is validated and expanded to the same working model; an
active range writes version 2 at its next ordinary checkpoint. Completed ranges
are not rewritten. Both stored and expanded JSON remain limited to 4 MiB, with
the same record/pending limits and strict shape/reference validation. No gzip or
new object keys are involved. The Python run result keeps expanded records.

Each job uses pages of 20, at most 10 page requests shared across all scheduler
ranges, at most 50 attempted imports, and at most three automatic item failures.
The page adapter has zero SDK/network retries; the pinned native transport may
replay a GET once after renewing authentication. Thus 10 page requests mean at
most 20 fitness HTTP attempts, excluding separate auth traffic. Date ranges
contain 1–366 Garmin-local days; offset is bounded at 100,000. Checkpoints allow
at most 10,000 distinct records, 1,000 pending imports and 4 MiB; a response page
is capped at 1 MiB after JSON decoding. These are stop limits, not promises about
total account usage or pre-decode network memory. Existing R2 limits, schedules,
serial execution and polling intervals remain unchanged.

`status` distinguishes `active`, `complete` and `blocked`. `stop_reason`
distinguishes `batch_limit`, `metadata_call_limit`, `source_end`,
`temporary_error` and `checkpoint_limit`. Batch/call limits and short/full pages
never mean the requested period is complete. Counts cover unique *observed*
metadata; unseen pages are unknown. `known_remaining_activities` counts observed
incomplete imports. Until source end, `remaining_activities` additionally reserves
one outstanding scan task and `remaining_count_is_exact` is false. This keeps
older schedulers from treating zero known missing files as a completed range.
After source end, the two remaining counts agree.

Pending metadata survives moving the offset and includes a forced-replacement
flag when a previously observed source fingerprint changes. Existing unchanged
files and unsupported activities do not consume the import batch. Missing
derived files reuse stored FIT/TCX where possible. Failed forced replacements
remain pending even when all filenames exist. Account/service/storage failures
pause without an item tombstone and persist progress when the write guard permits.
If the checkpoint write or a budget/interruption stops the job, the last durable
checkpoint is replayed; successful canonical files are reused. Repeated item
failures remain explicitly blocked and never hide older missing activities.
Capacity boundaries require a smaller explicitly chosen range, not raised limits.

`failures[*].attempts` is the accumulated historical item-failure count, including
explicit retries. Older totals such as 4 or 7 are preserved and remain blocked
for automatic retry. Valid totals are strict integers from 0 through 2^31-1;
negative values, booleans, missing totals and larger values are rejected. Another
failed explicit retry increments the total; at the upper bound it stays saturated
and sets `attempts_saturated: true`, making the total a lower bound. A successful
import clears the item's failure as before. This does not reopen the three-error
automatic budget or permit more than one item attempt per run.

## Offset consistency and explicit reconciliation

The reviewed GET route is `/activitylist-service/activities/search/activities`
with fixed `startDate`, `endDate`, `start`, `limit` and descending `sortOrder`.
The native transport still enforces the existing host, path, GET and redirect
guards. No URL, SDK method, header override or arbitrary query enters the adapter.
Pinned `garminconnect` 0.3.17's date helper itself loops in 20-row pages until an
empty response; a method named `get` is not evidence of one call.

Garmin offsets are not a snapshot. On resume the first page and previous-page
overlap are checked; a mismatch causes at most one restart in that job. IDs are
deduplicated, and two identical ordered fingerprint passes, each ending with a
validated empty response, are required before `source_exhausted` is true. The
verification pass also catches activities moved behind a previously scanned
boundary. Coverage assumes the source eventually stays stable over those passes.
Arbitrary concurrent changes, ambiguous ordering of ties or changes after the
last observation cannot be ruled out without a server snapshot. No permanent
completeness claim follows from an offset, a job exit or two equal observations.

Completed scheduled plans stay read-only. Late uploads and historical edits
outside normal recent refresh require an explicitly approved reconciliation of
the affected range. Only the first call of a resumed reconciliation uses
`--reconcile`; later calls omit it, so the offset can advance:

```bash
python -m pipeline.activity_backfill --start-date 2024-01-01 --end-date 2024-12-31 --max-activities 20 --reconcile
# Continue the same range with the same command, omitting --reconcile.
```

After fixing an item failure, add `--retry-failures` to the affected range call.
This permits at most one attempt per item in that run; it does not increase the
batch, metadata or write limits. Do not run concurrent cloud/local writers.
These operational commands contact Garmin and write R2; they require the
installation owner's separate authorization. Development tests are synthetic;
the separate private release canary is summarized below without operational data.

Existing recent change detection remains responsible for its source manifests
and strength exercise-set changes. Backfill comparisons reuse its metadata
fingerprint definition; an activity with no earlier fingerprint is a baseline,
not proof that historical artifacts match its latest source. Backfill does not
replace the explicit activity-refresh/recovery flow. Source-manifest refresh
after a changed backfill import can conservatively repeat a download. An activity
removed at Garmin stays in private storage and observed counts; backfill does
not infer deletion or delete history.

## Existing plans and local verification

Active v2 range progress gains pagination on the next approved run, starting at
offset zero and preserving failures/artifacts. Completed v1/v2 plans and range
results are honored as legacy completion, without claiming a fresh source check.
No finished history is automatically migrated or rescanned. Unknown schemas,
invalid cursors, incomplete metadata, missing counts and scope mismatches fail
closed. Install the pagination, adapter, backfill and scheduler code together;
an older complete backfill implementation can discard additive cursor fields,
so review rollback and keep a private checkpoint copy before any later canary.
The first pagination reader accepts only wire version 1 and will fail closed on
version 2; rollback needs the preserved compatible checkpoint and a separately
reviewed recovery, rather than running the older reader on new progress. Future
canonical artifact layouts require an explicit format version change.

Synthetic verification includes exact full final pages, empty ranges, local/UTC
year boundaries, duplicates, changed order/metadata, late insertion, interrupted
imports/checkpoint writes, real export recovery after a partial PUT, blocked
retry, old schemas, malformed checkpoints, and shared call/write limits.

```bash
python -m pytest -q
```

For full local tests, choose a temporary directory **outside Git**: the private
backup tests deliberately refuse archives inside a repository. The private benchmark
exports the complete pipeline from each verified local Git reference and imports
each in a fresh interpreter, preventing current helpers from altering an older
version. The revised pipeline comes from the working tree, identified by a source
digest. It uses the installed
SDK's date loop with a synthetic source, identical simulated file exports, and
real R2Store guards over in-memory S3. It compares entire multi-job runs and
asserts identical expected artifact sets and completion. Counts must match across
three fresh runs per version; elapsed time is their median with all samples kept.
Measurement tooling and raw evidence remain private. The measurement includes JSON/local writes,
head/overlap checks and verification passes; excludes auth, network, queues,
startup and retries. Simulated FIT/TCX bytes do not measure decoder or production
performance. More/larger checkpoints and extra verification jobs can increase
R2 operations/bytes and local CPU even while metadata requests fall.

The revised checkpoint removes repeated derivable path strings. Status for a
newly constructed manifest checks its counts without decoding/copying the whole
cursor again; untrusted stored progress still undergoes full validation. Two
stable passes and head/overlap probes remain necessary within the current offset
model. Extra verification jobs and inventory LISTs across jobs remain: inventory
is reread for each job because another writer may have changed stored objects.

## Synthetic measurements and private release validation

The reviewed three-way fixture contains 1,200 synthetic metadata rows, 1,080
supported activities and 180 already complete activities, with an import batch
of 50. Each complete pipeline version ran in isolation against three fresh
in-memory stores. All produced the same 4,320 expected artifacts and completed.

| Measurement | Unbounded baseline | Initial pagination | Compact pagination |
| --- | ---: | ---: | ---: |
| Jobs | 18 | 27 | 27 |
| Metadata calls | 1,098 | 174 | 174 |
| Peak metadata calls per job | 61 | 10 | 10 |
| Metadata response bytes | 3,040,830 | 483,544 | 483,544 |
| Simulated file calls | 1,800 | 1,800 | 1,800 |
| Simulated file response bytes | 460,800 | 460,800 | 460,800 |
| R2 GET / HEAD / LIST pages / PUT | 17 / 0 / 122 / 3,618 | 26 / 0 / 213 / 3,627 | 26 / 0 / 213 / 3,627 |
| Total R2 operations | 3,757 | 3,866 | 3,866 |
| R2 GET body bytes | 33,518 | 5,395,359 | 1,977,725 |
| R2 PUT body bytes | 957,131 | 6,609,410 | 3,003,652 |
| Final checkpoint bytes | 2,013 | 292,451 | 104,327 |
| Median local simulated time, ms | 148.58 | 478.16 | 524.45 |

Metadata calls fell 84.15% from the baseline; total simulated Garmin calls fell
about 31.9% including unchanged file calls. R2 operation count rose 2.9%.
Compaction reduced GET bytes 63.34%, PUT bytes 54.55% and final checkpoint size
64.33% compared with initial pagination, while median local time rose 9.68%.
The timing includes JSON/local writes and real R2Store guards against in-memory
S3, but excludes network, authentication, retries, queueing, process/import
startup and real FIT/TCX decoding. LIST response bytes and protocol overhead
were not measured. These are synthetic tradeoffs, not a production speedup or
a guarantee of free operation or total account usage.

A separately authorized, narrow private canary verified existing-file reuse,
confirmed source exhaustion and passive resumption. Its durable wire-v2
checkpoint matched the local bytes. Other object revisions, including historical
Coach versions and other periods, were unchanged at verification. It did not
exercise broader imports, forced replacements or account-wide cost. A private
measurement-helper error after the successful PUT was reproduced and corrected
offline; the first invocation's total elapsed time remains unknown. The passive
resumption required no Garmin calls or R2 writes. This is functional evidence,
not a comparable before/after production performance measurement. No private
dates, identifiers, installation values, reports or source data are published.

The [external pagination reference](https://github.com/Taxuspt/garmin_mcp/blob/main/src/garmin_mcp/activity_management.py)
was inspected as an idea; its [MIT license](https://github.com/Taxuspt/garmin_mcp/blob/main/LICENSE)
was checked. No external code was copied and its writes/permissions were not adopted.

## Later approved canary

Before any later canary, review the diff, confirm current main and related
integration changes, and run normal CI. Separately authorize one quiet, narrow range and
the exact maximum batch on a serialized writer, preserving its existing private
progress first. Record page requests, HTTP/auth retries, file calls, R2
GET/HEAD/LIST/PUT, bytes, elapsed time and whether startup/queue time is included.
Check pending state and canonical artifacts after interruption/resume; require
confirmed source end and no actionable pending work before completion. Stop on
auth/rate/service/schema/budget errors. Broader backfill, schedule changes,
public promotion, push/PR/merge and deployment each retain their approval gates.
