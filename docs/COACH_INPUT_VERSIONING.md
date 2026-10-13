# Coach Input identity and immutable publication

Bulk `coach_backfill.run` and targeted `run_one` use one content-identity and
publication rule for newly generated analyses. This is R2-derived calculation;
it does not call Garmin or change profile selection, zones or user context.

## Identity

The identifier hashes the deterministic analysis JSON, including its existing
source/profile/context/analyzer seed identifier. The result covers relevant
activity metadata, derived endurance measurements, the effective profile and
explicit user context, in addition to source hashes and analyzer version.
Object member order does not affect the identity. CSV and targeted moving-time
values are normalized to the same numeric representation (`1800` and `1800.0`
produce the same new analysis). The canonical key remains
`activities/<year>/<id>/coach-input/v1/canonical/<24-hex-id>.json`.

An unchanged relevant result has the same identity. Changed relevant content
has a different identity and an additional canonical object. Source changes
remain discovered through the existing bounded batch, source manifests and
plan invalidation rules; this fix does not add a historical inventory scan.

## Publication and recovery

Canonical creation uses the S3 `If-None-Match: *` condition through the existing
R2 write guards. A listing is only an optimization, not permission to overwrite.
If the identity already exists, its decoded JSON must equal the intended result;
its original bytes and encoding are retained. A failed create precondition is
checked the same way, including when another writer won the race. Different
content is an identity conflict and stops publication without advancing the plan
or ready pointer. Authorization, provider conflicts and uncertain failures are
not permission for an unconditional write. The condition is documented in
[Cloudflare's R2 S3 compatibility reference](https://developers.cloudflare.com/r2/api/s3/api/).

The immutable object is committed before the bulk plan or targeted ready pointer.
A retry after a lost canonical reply or an interrupted checkpoint validates the
existing object and completes the checkpoint without rewriting it. Equal
concurrent inputs converge on one canonical object; different inputs retain
separate versions. Concurrent checkpoint writes retain the existing last-writer
behavior; this is not a transaction across analyses, source files and pointers.
Existing workflow serialization remains in force.

Conditional attempts retain the existing per-run and shared storage/write
accounting, even when rejected or uncertain. No limits, schedules or concurrency
settings change. The existing first-write budget inventory is reused; no extra
full-bucket inventory or activity listing is introduced.

## Current selection shared by producers and readers

Both generators publish `coach-input/v1/latest-ready.json` after the immutable
canonical object, before recording bulk progress. The pointer's additive
`selection_version: 1` marks this contract. The selected canonical key identifies
one exact analysis for the same activity; it can return to an older immutable
object in an A → B → A sequence without rewriting that object's bytes or upload
time. Ordinary `coach_input` and the freshness reader share that selection rule.

Bulk plans record additive `selection_revisions` using the confirmed opaque
ETag returned by the pointer PUT. The existing activity listing detects a pointer
changed by targeted generation even when a cached bulk plan's source fingerprint
still matches. A changed/unknown revision is reconciled only in the bounded batch.
No additional global LIST or inventory is needed. Missing SDK revision evidence
prevents fast reuse; the next bounded pass can confirm an unchanged pointer from
listing metadata.

Only an absent pointer permits ordinary legacy discovery by uploaded time in
the same activity's canonical prefix (at most 1000 objects, no truncated results).
A present but invalid, malformed, oversized, wrong-activity or dangling pointer
never falls back to another historical analysis. For unmarked old pointers,
internal SDK upload metadata also preserves a later old bulk upsert over an
older targeted pointer. Before replacing an unmarked pointer's selection, its
original key must be present in the same bounded canonical metadata list. A
missing original reference returns unverified without selecting other history;
this adds no GET, HEAD or LIST. All candidate upload clocks must be valid, and different
candidates must not share the newest time. This applies both without a pointer
and with an older unmarked pointer; an object key is never a time tie-breaker.
Missing/invalid/tied legacy clocks cannot select between different histories.
Marked new selections never depend on upload clocks. Provider failures propagate
to the existing privacy boundary. Marked pointers also bind the exact 24-hex ID to
the selected JSON. Older scoped targeted pointers remain readable.

The freshness reader does not accept a different legacy canonical object using
another object's old source-revision proof; it remains pending for bounded repair.
The freshness reader still requires the three canonical source ETags, Garmin-local
activity date, source FIT hash and effective profile/context association. It
never uses legacy uploaded-time fallback as freshness proof. Ordinary historical
reads do not infer Garmin-local dates from the CSV's UTC clock. No MCP read starts
sync or writes storage.

The shared reader distinguishes verified absence from a stored selection that
cannot be verified. Only an absent pointer with a successful empty bounded
canonical listing means no analysis is stored. Unknown schemas, dangling or
invalid selections and ambiguous legacy chronology return controlled unavailable
results with retry/maintainer compatibility guidance, never profile/backfill
advice or a claim that stored data is absent. The ordinary MCP payload retains
`available: false` and no analysis; HTTP 200 and a non-error tool result are
preserved for these controlled outcomes. Provider/parser/size/list-limit failures
retain their existing generic privacy-safe tool errors. Freshness remains pending
unless the exact selection and all existing source proofs pass.

The current pointer is the per-activity checkpoint. If a pointer fails before
commit, the previous selection remains; a committed pointer with a lost reply
is already observable and the retry verifies it. A later bulk-plan failure does
not undo a committed per-activity selection. This does not make the whole batch
or source inventory a distributed transaction.

Bulk reserves up to two write attempts per selected activity plus the final plan
PUT. With the existing 60-write limit, a new batch handles at most 29 activities
before checkpointing progress, still below the existing 50-activity request cap.
Limits and schedules do not increase. Byte/storage failures still stop safely,
with canonical bytes preserved and unfinished checkpoints retried normally.

## Compatibility

Existing v1 analyses, profiles, contexts and key versions remain readable and
are not migrated or deleted. Legacy source-only bulk identities remain stored.
Completed historical plans without pointers remain passive. Older pointers
are inspected in the bounded batch when revision evidence is absent or changed;
matching legacy analyses are checkpointed without regenerating or downloading
the historical source files. The plan's additive `legacy_selection_revisions`
records those metadata-only checks; a later new targeted selection cannot hide
behind the old bulk fingerprint. Older JSON member
order, whitespace and gzip metadata are accepted without rewriting equal data.

The targeted input-signature policy is versioned to avoid reusing a cached
result that bypasses numeric normalization. An addressed legacy pointer is
checked/rebuilt on its next ordinary targeted request; this is not a history
sweep. Most existing content-hash IDs remain identical. A legacy targeted
integer moving-time representation can acquire a new ID with normalized content;
its earlier bytes remain stored. Analyzer/schema versions and consumer payload
fields are unchanged.

## Offline verification

`tests/test_coach_versions.py` exercises the actual bulk and targeted entry
points through `R2Store` with a synthetic S3 boundary: stable identity, metadata
and source changes, preservation of historical bytes, cross-path parity,
interrupted publication, create races, collisions, provider failures and budget
rejection. Existing targeted reuse and authenticated MCP legacy-reading tests
remain applicable. No real Garmin/R2 data is used.

The permanent Worker regression imports Python-generated snapshots for bulk,
targeted and mixed A → B → A sequences, plus checkpoint interruptions/retries,
into actual authenticated MCP JSON/SSE calls. It verifies both current readers,
immutable bytes, legacy compatibility and negative selection failures. Additional
JSON/SSE regressions assert transport/tool status and guidance for unknown pointer
and analysis schemas, dangling selections, true absence, and tied/invalid/missing
legacy clocks with or without an older pointer; distinct valid chronology remains
readable. Fixtures
are built with the existing Worker pretest contract builder; no pytest package
is required to build them.

The following one-activity/one-profile, 1 MiB synthetic TCX comparison is against
the immutable-generation implementation before the shared selection fix. LIST
includes the existing write-budget inventory; PUT counts SDK attempts. All rows
use zero Garmin calls and zero HEAD calls. The retry row measures only the retry
call after the initial canonical/pointer commits and failed plan write.

| Scenario | GET before/after | LIST pages before/after | PUT before/after | Download bytes before/after |
| --- | --- | --- | --- | --- |
| Bulk initial | 5 / 5 | 5 / 5 | 2 / 3 | 1,048,915 / 1,048,915 |
| Bulk unchanged | 3 / 3 | 4 / 4 | 0 / 0 | 967 / 1,097 |
| Bulk moving-time change | 6 / 7 | 5 / 5 | 2 / 3 | 1,049,642 / 1,050,283 |
| Bulk return to A | 7 / 8 | 5 / 5 | 1 / 2 | 1,050,503 / 1,051,144 |
| Targeted initial | 4 / 4 | 3 / 3 | 2 / 2 | 1,048,804 / 1,048,804 |
| Targeted unchanged | 2 / 2 | 2 / 2 | 0 / 0 | 618 / 640 |
| Targeted moving-time change | 5 / 5 | 3 / 3 | 2 / 2 | 1,049,293 / 1,049,315 |
| Targeted return to A | 6 / 6 | 3 / 3 | 1 / 1 | 1,050,154 / 1,050,176 |
| Cached bulk A after targeted B | 3 / 9 | 4 / 5 | 0 / 2 | 967 / 1,051,655 |
| Bulk retry after interrupted plan | 6 / 7 | 5 / 5 | 1 / 1 | 1,049,776 / 1,050,287 |

The current ordinary MCP path reads summary, pointer and exact analysis (normally
3 GET, 1 HEAD, no LIST/PUT); the prior reader used summary, a canonical LIST and
one analysis (2 GET, 1 HEAD, 1 LIST, no PUT). Legacy fallback adds one pointer miss
GET to the prior read pattern. The freshness path retains its existing source
proof/read requirements. Returned analysis bytes/fields and consumer payload
schemas remain unchanged.

The bulk pointer adds 511 bytes in this fixture; new selection/progress fields
increase its one-activity plan by 130 bytes. Initial/moving-change upload attempts
are 1,588 → 2,229 bytes; returning to A is 727 → 1,368. Targeted pointer upload
attempts grow by 22 bytes for the selection marker, with no extra normal
operations. The mixed cached-plan row fixes a formerly incorrect zero-write
shortcut and therefore legitimately performs bounded analysis/selection work.

Storage still preserves both canonical versions. Matching old-pointer
reconciliation reads only the pointer/analysis metadata, never historical FIT,
endurance or TCX. Unknown revisions conservatively prevent cached reuse.
Body bytes and in-memory timings exclude network, scheduler/queue, provider
retries, large-inventory pagination and account cost; no performance or free-tier
guarantee is claimed. More bounded batches can increase whole-backfill inventory
and plan operations even though per-run LIST/inventory patterns are unchanged.
Live R2 behavior/concurrency is not exercised.

The status/legacy correction was also compared through synthetic actual MCP calls
against its frozen reader baseline. Valid marked selection remains 3 GET, 1 HEAD,
0 LIST/PUT; valid legacy selection remains 3 GET, 1 HEAD, 1 LIST, 0 PUT.
Tied or invalid legacy chronology is rejected before the canonical GET (2 GET,
1 HEAD, 1 LIST, 0 PUT). Unknown-schema/dangling checks add no operations. These
body-byte/operation measurements exclude LIST-body bytes, network, provider
retries and account costs; they do not establish a runtime performance guarantee.

The missing-original-reference JSON/SSE regression measures 2 GET, 1 HEAD,
1 LIST and no PUT/DELETE, versus 3 GET before rejection was fixed. Consumed
synthetic R2 bodies are 221 versus 1084 bytes; the eight positive legacy cases
retain their operation/body-byte counts. No new inventory or resource limit is
introduced; these are offline body measurements with the same exclusions above.
