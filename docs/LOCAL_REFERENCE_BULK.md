# Local bulk night references

The bulk workset separates stored-source collection, local computation and
publication. It uses the existing HR/HRV night-baseline motor for rolling
7/28/30/90/365 windows and the same product
schema. It does not collect Garmin data or introduce a schedule.

A full scope declares inclusive wake endpoints and exact channel/window
combinations. New five-window scopes explicitly declare
`"windows": [7, 28, 30, 90, 365]`, yielding ten possible products per endpoint.
Omitting `windows` preserves the legacy 28/30/90 selection and six combinations;
an invalid declaration is rejected. These are possible products, not a promise of complete source
coverage. A daily affected scope declares only the exact rolling targets
invalidated by its verified delivered sources. Calendar products are outside the
rolling build barrier and have a separate bounded daily phase; unrelated queue
entries remain pending.
Rolling seven-date products are distinct from completed ISO weeks. A rolling
365-date product requests exactly 365 nights, even across a leap year; it does
not mean a full calendar year of up to 366 dates.

## Durable phases

1. **Prepare:** bind the job, storage target, timezone, explicit targets, source
   evidence, window selection and one absolute UTC deadline in a create-only
   local workset. Legacy declarations keep their original meaning on resume.
2. **Inventory and collect:** record bounded native-verified canonical revisions;
   deduplicate the required source versions. Matching verified local publication
   snapshots are reused. Each missing required version is fetched once into the
   durable local cache. Complete inventory can establish absence; an unknown
   response cannot.
3. **Build:** compute the declared products from local bytes with the existing
   night reader and baseline function. Store canonical product bytes, input
   identity and provenance locally. Known missing coverage is explicit. Unknown
   or stale inputs cannot complete the build barrier.
4. **Plan publication:** derive a finite resource plan from the sealed products,
   their bytes, source fences, history/control/CAS and independent reader needs.
   The plan distinguishes gross attempted write bytes from retained object/byte
   growth. Qualification of that plan belongs to the operator before live use.
5. **Publish:** only a globally sealed build can enter this phase. Technical
   bounded batches share one command-owned client and bucket inventory. Current
   preimages, conditional writes, immutable previous-product history, fresh
   source/context fences and exact readback remain required.
6. **Verify and acknowledge:** aggregate independent SDK and normal-reader
   observations against the actual published or verified-unchanged postimages.
   Publication alone is not completed verification.

Identity and gzip postimages retain their actual raw bytes, hash, opaque
revision, content type and encoding. A bounded decoded representation is used
only for product validation and normal-reader comparison. New proof records bind
both raw and decoded hashes/references; identity can reuse the same byte proof.
Older identity-format proof records remain readable. An unchanged gzip product
is not rewritten merely to make its bytes look like identity JSON. A global
held count outside the selected scope does not reject otherwise verified work;
the normal selected-source, unknown-write and no-progress guards still apply.

No product PUT occurs during source collection or local building. A technical
one-PUT-per-object is still necessary; no separate human approval or job startup
is required for every curve. The per-window native memory/point limits remain
unchanged. The durable disk cache is bounded by the finite declared workset and
recorded source sizes; it does not evict an earlier needed source merely to fit
an arbitrary transient cache size.

## Resume and failure

Resume uses the same immutable scope, cache, product manifest, carried evidence,
resource account and deadline. Known bounded progress is recorded before
returning a remainder. No new process or technical batch refills the whole-plan
allowance or renews its deadline. A source revision change is an explicit stale
remainder or hold, never an instruction to silently refetch and rebuild.

Resume the explicit unfinished command phase. Restarting the whole operator
controller is not an automatic recovery/adoption path: existing prepare or
calendar artifacts can require a hold and separate qualification. Completed
publication and acknowledgement can be repeated without additional I/O.

A pending or unknown storage operation, conflicting immutable artifact or failed
cleanup stays held. The first operation failure and cleanup outcome are recorded
separately. Good old products are preserved when replacement inputs disappear;
a missing value is not converted to zero. Existing timestamps, local-night/UTC
handling, context selection, coverage flags and historical product readers use
the same contracts as ordinary reference generation.

A new explicit bulk allowance is separate from the existing smaller software
round defaults. All previous source and independent-read counters and their
prefix evidence remain carried and visible. Technical transfer caps, provider
settings, bucket safety limits and the daily Garmin source ceiling are unchanged.
There is no automatic retry, quota refill or permission extension.

## Daily integration

The ordinary operator may select the explicit bulk-after-source mode. The daily
source step then defers the old interleaved reference drain. After known physical
source closure and the ordinary raw-source verification, the same retained owner
builds the exact affected target workset and uses the same local build/publisher.
It does not restart collection, widen the source limit or drain unrelated history.
The compatible legacy daily mode remains available; selecting the new binding is
an operator adoption step, not a new schedule.

Affected selection includes existing partial curves as well as missing products.
When another verified good source day arrives, the normal source hook invalidates
every affected admitted channel/window. A later caller with that selected scope
rebuilds those exact targets locally; default 28/30/90 callers do not adopt
unrelated 7/365 work. No manual product selection is required. Coverage
improves only where the new source actually contributes; gaps, nulls and partial
coverage are not replaced with invented complete nights. The same dirty-source
contract applies to qualified complete ISO weeks and calendar months.

After rolling publication and its independent verification, daily mode uses the
existing bounded calendar publisher for affected completed ISO weeks and calendar
months. That phase does not drain the entire historical queue or widen the
rolling manifest. Its actual result and postcheck join the raw-delivery proof and
exact rolling acknowledgement before closing the source cohort. Known calendar
gaps or pending work remain explicit; an unknown operation stays held. Closing a
source cohort permits the next disjoint source selection, and does not assert
that every historical reference is complete. The deferred source command keeps
its actual native exit status and original proof.

Each daily handoff derives a new immutable endpoint universe from its actual
selected source cohort of at most 30 dates. It includes each new source date and
its following wake dates through the largest declared window minus one day,
clamped to the current
allowed latest wake date. It deduplicates this finite union, then the existing
affected compiler selects targets from the verified raw delivery. The historic
406-endpoint one-off set is not a permanent lower bound for daily products.

For legacy 28/30/90 scopes this is at most 89 later dates and 119 endpoints for
thirty consecutive source dates. With an explicit 365-day window it is at most
364 later dates and 394 endpoints for the same consecutive cohort. The existing
406-endpoint workset capacity remains an explicit admission bound; a more
scattered cohort whose union exceeds it stays held without truncating dates or
targets. `no_affected_targets` likewise does not mean that its calendar work ran
or its source cohort closed. Generated policy, scope and resource-plan bindings
remain immutable throughout the admitted technical batches and phase resume.
Window selection also binds source lookback and resource-plan identity. The
existing 2436-target, 1986-source and 160-inventory admission caps remain in
force: a five-window scope that exceeds one stays held for explicit scope
qualification rather than being truncated or receiving a larger allowance.
Local and GitHub ordinary ingestion use the same reference engine and affected
source contract. This explicit bulk mode changes neither their schedules nor
provider selection, and does not start a hidden full rebuild.

## Command contract

The portable entry is `python -B -m pipeline.history_backfill --state JOB
rolling-bulk`. It requires an explicit `--phase`, `--plan-sha`, `--scope`,
`--scope-sha` and `--workset`. The six one-off phases are `prepare`, `inventory`,
`collect`, `build`, `publication` and `ack`; they do not select Garmin sources.
The same workset path and immutable scope references must be retained on resume.

Every phase after prepare also requires the qualified `--resource-plan` and its
`--resource-plan-sha`, plus `--carried` and `--carried-sha`. Inventory, collection
and publication use distinct derived plans; build uses the collection plan.
Acknowledgement additionally requires the independently produced `--proof` and
`--proof-sha`. The operator constructs and qualifies these artifacts from actual
scope metadata and observed results. A caller-provided larger cap is not an
accepted resource plan.

Technical bounds are explicit: `--max-items` is at most 400 missing source
versions, `--max-targets` at most 2436 declared targets and `--max-batches` at most
2436 bounded publication batches. Each publisher batch retains its existing
maximum of eight product rows and lower transfer/memory/time caps. A known
bounded remainder is resumable; it is not successful whole-workset completion.
The existing daily command selects this mode only with the explicit
`--defer-reference-build-to-bulk` option and its qualified same-owner operator
binding. Running that source step alone cannot prove completed reference work.

## Private artifacts

Workset sources, product bodies, provenance and receipts contain private health
or installation information. Keep them in the ignored local job area. They are
not source files, public examples or CI artifacts. Source publication should
include only the portable code, synthetic tests and documentation.
