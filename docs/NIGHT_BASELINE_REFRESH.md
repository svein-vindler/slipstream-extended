# Incremental stored night references

The existing ingestion commands maintain personal HR/HRV `night-baseline-v1`
products from R2 sources, using the unchanged `pipeline.night_products` producer.
The arguments remain `sleep_start`, 30-minute bins and no filters/exclusions.
Full Monday-Sunday ISO weeks and full calendar months remain supported alongside
rolling 7/28/30/90/365-night references. Keys and Worker reader schema
remain compatible with previously generated references; a week crossing a month
or ISO-year boundary is not clipped or regrouped.

## Rolling windows and bounded historical discovery

For a verified local wake-date endpoint D, a window N includes exactly
D-(N-1)..D, including D. HR and HRV have independent queued products and coverage;
missing nights are gaps, never zeros. The same native engine preserves units,
alignment, nulls, actual local clocks and DST. Existing Worker `night_baseline`
reads stored products: use periods `7`, `28`, `30`, `90`, `365`, or an explicit
`custom` inclusive range. A rolling seven-date reference is distinct from a
completed ISO week. A rolling 365-date reference always requests 365 nights,
including across a leap year; a full calendar year can contain 366 dates. The
reader does not calculate missing references or start a sync.

The existing durable queue stores compact eligible/dirty date ranges. Explicit
7/365 admission retains its own endpoint ranges; a legacy registration does not
admit a new extended-window endpoint. Reads preserve older queue representations. A changed
sleep/HRV source day invalidates every eligible D through d+N-1, not only d.
HR calendar dependencies use a conservative four-day margin for rolling work;
the established week/month mapping is unchanged. Bounded discovery checks only
Sleep month prefixes inside the affected horizon, verifies usable stored Sleep
bytes and registers endpoints. Discovery candidates are explicitly distinct from
known eligible channel jobs. Historical absence never becomes a fabricated night.

At most two publication rows are materialized per phase, with the unchanged
128-row/262144-byte queue bounds. Discovery and channel remainders persist across
phases. Daily raw staging defers references until canonical ACK/readback; the
finishing phases include the original source cohort's later rolling endpoints.
A completed source stage is not proof of completed rolling references.

Overlapping windows share a bounded source cache. Fresh bounded monthly revision
inventories gate reuse; two fresh publication fences independently verify source
revision, size and per-day context membership. A cached body alone never proves
freshness. Canonical month inventories cap at 63 keys; context months at 3101.
No new full bucket enumeration is introduced for rolling discovery or planning.

Context appends first reserve a queue intent. Only the confirmed immutable append
with exact event readback can durably enqueue invalidation. During an overlapping
publisher, a `saved` marker retains the event revision and dirty work while the
publisher row stays intact. The append owner releases its day claim only after
that queue CAS and exact readback; this confirms queued work, not fresh products.
The consumer later verifies the immutable event and every recorded effect before
restarting a generation at a checkpoint with no inflight write or source intent.
Unknown append/queue outcomes and inflight writes retain HOLD. A confirmed append
remains `saved: true` even if subsequent invalidation fails: retry the read,
never append the event again. Stored reads do not perform this write-side repair.

## Existing entry points

Normal recent-night production retains the implicit HR/HRV 28/30/90 cohort,
prioritizing the newest selected wake-date. New job/workset scopes must explicitly
select 7/365 or all five windows; installation does not expand an existing scope.
The ordinary
local reader computes the bounded cohort from shared snapshots, then the same
publisher checks the combined source/context proof before and after conditional
publication. It keeps the existing GET/LIST/PUT, memory and shared write limits;
the standalone two-row refresh default remains unchanged. A larger recent cohort
can retain an exact `selected_remaining` and `bounded_pending` result. Later
invocations with the same selected scope drain that remainder without re-dirtying
unchanged accepted source data. Default consumers leave unrelated 7/365 work queued
and exclude it from their selected completion. Unrelated unknown intents are retained and do not make a
completed selected cohort incomplete; an unknown operation on a selected target
still prevents completion. The ordinary bounded calendar tail remains separate.
Existing legacy three-window queue state remains readable. Ordinary source
changes enqueue only affected eligible references; adding supported periods
does not silently enumerate or rebuild all historical endpoints. Explicit bulk
scopes and resource plans retain legacy 28/30/90 meaning when `windows` is absent;
new five-window scopes declare `[7, 28, 30, 90, 365]` explicitly. Their exact
sources, target count, bytes and independent verification remain bounded.

The hook covers `health_sync.store_day` (sleep/HRV),
`DailySignalCollector.publish` (HR), `recent_health`, `latest_night`,
`local_import.run_import_cycle`, the merged `hrv_backfill`, sleep
`health_detail_backfill`, `night_local_repair`, and HRV R2 `granular_export`.
Unsupported streams do not trigger storage reads. Local-file-only export stays
local. The opt-in `pipeline.history_backfill publish` also uses this source hook.
It drains at most two selected periods per bounded phase and never fetches Garmin
in its publication phase. The ordinary daily job first publishes canonical raw
chunks and then drains its cohort's calendar/rolling references. Its later
ordinary invocations can admit finite receipt-bound reference-only successors
after known terminal budget exhaustion, retaining cumulative usage, the original
source cohort and all source/per-phase/per-round limits. See
[local history](LOCAL_HISTORY_BACKFILL.md) for its separate bounded commands.

The queue is `health/night-products/v1/refresh-queue.json`. A changed canonical
source uses a conditional dirty intent, conditional source write plus exact
ACK/revision/body/serving-metadata readback, then a ready checkpoint. Unchanged
canonical bytes and serving metadata do not enqueue work. The queue is shared
by conditional revisions, so concurrent attempts conflict instead of silently
losing a source change. There is no recursive global R2 write hook.

HRV and sleep use the actual wake date. HR calendar objects conservatively dirty
wake dates within three days on either side, covering the bounded 36-hour sleep
window, UTC/Garmin-local clocks, travel and up to 14-hour wall-clock offsets.
Calculation then reads only the source dates actually needed by each night.
Python assembly is checked against actual Worker domain code for DST, travel,
ISO-year and explicit user-timezone cases. Stored UTC timestamps, local clocks,
null/absence and context IDs are preserved; no home-timezone inference fills an
unknown actual zone. Baseline sleep-start alignment needs the verified UTC sleep
window, not an invented clock alignment.

## Calendar timezone in each runtime

The Python queue drain needs an IANA calendar timezone such as `Europe/Oslo` to
determine when a week or month has closed. Set `HEALTH_TIMEZONE=Europe/Oslo` in the
local Python process environment, or in an existing local environment file that
the selected command explicitly loads. Programmatic callers may supply
`refresh_pending(store, timezone="Europe/Oslo")` instead. No environment file is
implicitly discovered by this helper.

For hosted Python, configure the existing GitHub repository Actions secret
`HEALTH_TIMEZONE` with the IANA value. Existing refresh and HRV/sleep-backfill jobs
pass that secret through their Python job environment. The Worker has its own
Cloudflare `HEALTH_TIMEZONE` secret; configuring it alone does not configure the
Python process or the GitHub repository. No credential, permission or actual
configuration change is made by installation of this code. An absent/invalid
Python setting leaves work at `calendar_timezone_required`, without a UTC
fallback. The calendar setting does not replace a source's unknown actual-night
zone or rewrite UTC/Garmin-local clocks.

## Pending work and safe restart

Each drain considers at most two closed periods and shares decoded night/source
cache across a week and month. It reads the single bounded queue and exact
source/context prefixes; it never scans the bucket to discover dirty work.
Open periods remain pending, then a later ordinary ingestion drains them even
when that later source is unchanged. The source's stored/complete/latest-night
meaning is separate from reference status:

- `complete`: no queued periods remain.
- `waiting_until_close`: only current, still-open periods remain.
- `bounded_pending`: closed work is capped, blocked or still pending.
- `calendar_timezone_required`: closure cannot be established safely.
- `held`: an older unresolved source/reference operation is retained and counted
  separately; unrelated new phases are still allowed.

Source admission also remains separate from Garmin-check evidence. Each supported
recent-health stream reports requested, admitted/checked and deferred day counts.
Zero admitted days yield `source_checks.checked=false`, `outcome=not_checked`
and no new `checked_at`; this is pending capacity, not a provider failure. A
successful partial admission can confirm an actual check while reporting
`not_ready` for the incomplete window. Independent body/activity phases continue;
a new provider/storage failure retains its existing fail-closed behavior.
If all supported streams fail without any accepted source day, reference tail
publication is skipped as `not_run_source_failure`; the bounded queue admission
before each stream remains necessary, and does not fetch historical sources.

Partial, verified nights improve references without requiring complete coverage.
Same-count corrected values change the source revision and are recomputed.
Malformed, ambiguous or conflicting native observations block a period before
reference writes; known empty results never replace a previously contributing
product. Blocking is retained until a new verified source generation clears it.
Missing values remain missing; there is no zero-filled positive curve.

Source SHA/revisions, context listing and exact product preimages are checked
before publication. Source object bindings and context prefixes are checked again afterward using
fresh storage reads/listings, without claiming an atomic multi-object R2
transaction. Initial queued-source capture is shared with native assembly;
publication fences never reuse that cache. Each fresh context fence groups the
requested daily prefixes by calendar month, lists each month afresh, then checks
the same exact per-day key sets. The logical limit stays at 100 events per day;
context selection, native inputs and product `source_identity` stay unchanged.
A month check has a 3,101-key overflow bound and at most four physical pages of
up to 1,000 keys. A truncated fourth page, duplicates, wrong prefix or malformed
page stops the check without accepting partial context evidence. Existing
prefix checks of 1,000 keys or fewer retain their single-page contract.

A fully populated 31-night month with 31 dirty HRV proofs and 31 contexts uses
386 GET and 54 LIST with a real 21-page synthetic bucket inventory, including both
fresh fences. The same case with 100 context events per day uses 60 LIST.
Admission reserves four physical pages per month per fence, including for
cross-month or cross-ISO-year weeks; it never resets the shared client counter
or skips the bucket inventory. Twenty ordinary reads before the
helper make the store total 406 GET while this component still uses 386; the
helper's scoped cap does not impose a new global limit on ordinary R2 callers.
A second period defers if the remaining component GET or LIST budget cannot
cover its fresh before/after fences and exact write readbacks. Each product
prefix that fits is checkpointed only after its accepted ACK, exact readback and
fresh post-publication source/context fences. Such known progress returns to
`dirty` with its validated source proof; a later invocation can finish the other
product without repeating the first. A new verified source can supersede that
known progress. An unknown in-flight write never follows this path and remains
HOLD.
Current products use exact revision CAS, accepted ACK and exact readback. A
previously contributing wake date or required channel-source date must not
become unavailable in its replacement, even when a newly added night would hide
the loss in the total count. Verified corrections and improved partial coverage
remain eligible.
Before replacement, the old product bytes and a provenance sidecar with its key,
opaque revision, SHA, source identity and arguments hash are saved under
`health/night-products/v1/history/`. These are immutable conditional creates.
No history is deleted or reset.

A durable in-flight record is written before each product/history write. A newly
observed unknown result stops the affected execution with its intent intact.
A later invocation reports/skips an already recorded old HOLD without adopting,
repeating or clearing it, so unrelated current sources/body/activity phases can
continue. A new, different canonical source in the same held period is recorded
additively under `following_sources`; original sources, proof, checkpoints and
in-flight evidence stay intact. The exact unknown canonical key remains HOLD,
even if remote bytes happen to match a new request. A restart cannot silently
adopt remote bytes or repeat that write. Only an already
checkpointed ACK/readback is reusable after revalidating its exact stored bytes,
revision and source proof. No rollback or automatic lock/intent reset exists.
Operators must resolve an unknown outcome explicitly, with its original evidence.

## Budgets and transport

All control, source, history and product writes consume the caller's existing
R2Store stage/shared bucket budget. No helper creates a fresh stage or allowance.
A source group reserves three writes (intent/source/ready) and caller checkpoint
space before starting. Source admission checks the bounded pending queue before
backfill fetches; a full ready queue drains bounded closed work before any new
source call. If capacity still cannot admit a day, the source plan remains active
with zero new attempts, and the next ordinary invocation may progress it. Caller
checkpoint space is retained and no stage allowance is added. Importers reduce
source selection to remaining capacity;
closed reference groups defer before publication intent if the remaining budget
cannot cover their control/history/current checkpoints. The existing 5 GiB,
100,000-object inventory and configured write/byte limits remain authoritative.

The scoped physical observer caps this component's total client activity at
400 GET/HEAD, 100 LIST, 250 PUT and 32 MiB read bytes, within those existing store
budgets; these are local safety bounds, not new R2 service allowances. Native
wire counting and the synthetic/logical fallback use the same component scope.
Counts persist across helpers and `new_stage` calls sharing one client, including
source, queue, history and product work; no helper resets them. Reads outside an
active helper do not consume its component counters, while the caller's existing
shared store limits remain authoritative. Exhausted read capacity returns known
pending work before a new publication intent, without a new allowance. It uses a
900-second active-helper deadline, at most 128 pending periods, 256 KiB queue and
product bodies, at most 200,000 decoded points and 4,096 native points per night.
Oversized/unknown data stays pending rather than becoming an empty reference.
Original bounded LIST XML is validated before the SDK can coerce boolean/integer
fields. The supported wire encodings are UTF-8 (including its BOM) and ASCII;
UTF-16/UTF-32 and other declared encodings are rejected before entity-capable
parsing. DTD/entity rejection runs on decoded, encoding-checked text rather than
an ASCII byte substring. Relevant fields remain strict: truncation must be literally true/false, with a valid continuation and
complete nonnegative object sizes. The parsed SDK projection must agree with
these original fields before any inventory can authorize PUT. No retries occur after a
storage denial or unknown write.

The native reader preserves its per-source stored/decoded limits on both fresh
and cached reads. Dirty HR capture uses the same 256 KiB stored/1 MiB decoded
bounds as the ordinary HR source path; sleep/HRV retain 512 KiB/2 MiB. A later
stricter cache request rechecks captured byte sizes without another GET. Initial
capture sharing never substitutes for fresh publication fences.

The observer wraps native response streams before preload and unregisters all
of its hooks/restores the sender on exit. It does not leave its deadline on the
shared SDK client or unrelated later refresh/coach stages. Ordinary R2Store
construction now uses a per-client 5-second connect timeout, 20-second read
I/O timeout and one total attempt. The 20-second read timeout is not a total
CPU/runtime guarantee. Explicitly injected clients retain their configuration.

## New bounded local archive publishers and activation

Use the public Python API with the existing bounded R2Store, after the new code
is actually merged/installed:

```python
from pipeline.night_baseline_refresh import publish_source, refresh_pending

# Already normalized, profile/date-verified canonical bytes; no provider call.
changed = publish_source(store, canonical_key, body, "application/json", encoding="gzip")
reference_report = refresh_pending(store, timezone=health_timezone)
```

Calling code still owns its source acquisition, destination preservation policy,
explicit period/budgets and lifecycle. The helper is not authorization to widen
an archive run. Frozen old one-off publishers outside the repository are not
rewritten or rerun. Writes made before activation have no automatic dirty marker.
For a specifically approved, bounded set of existing sources, use
`enqueue_verified_sources(store, proofs)` with 1-14 exact dictionaries containing
`key`, quoted `revision`, and raw-body `sha256`. It verifies every proof before
one conditional queue write; it does not modify source objects or fetch Garmin
history. An unresolved old intent is never adopted through this API. The Root
operator must supply current proofs and then call the normal bounded drain.
There is no implicit whole-history seed.

Worker-only user-context updates are included/revision-bound when a queued night
is calculated. A context-only write is not itself a new Python queue trigger;
explicit bounded enqueueing is available when recalculating an otherwise clean
period for such a change. No new Worker scheduling or data workflow is added.

## Validation and deployment limits

The existing Worker unit job installs runtime Python dependencies and Worker
pinned dependencies before its ordinary `npm test`. The new mandatory unit
contract emits deterministic Python fixtures and runs actual Worker night and
baseline readers against them; it does not skip when Node is unavailable in the
separate Python job. The regular Python suite tests importer/source/drain, safe
restart, history, bounds and the actual SDK/URLLib3 synthetic HTTP boundary.
All development inputs are synthetic. Local Windows workerd/CFFI and previously
blocked temporary-file tests are not claimed as passing; ordinary Linux head CI
and fresh main CI remain release gates. No live Garmin/R2 proof is implied by
local tests. Installation creates no new service, schedule, credential or bill.
