# Recoverable summary publication

Normal refreshes merge new source rows into a verified activity/health pair.
`summary/manifest.json` is the commit record for Python restore and publication.
It retains schema version 1 and adds `publication_version: 2`, `generation_id`
and immutable `snapshot_key` references. CSV columns and consumer payloads are
unchanged. This protocol does not change Coach Input canonical identity.

## Commit and history

A generation lives under `summary/generations/v2/YYYY/MM/DD/<generation_id>/`.
Its ID hashes the protocol version, UTC archive day and the two named decoded
CSV hashes. Its two CSV objects and deterministic generation manifest are
created conditionally. Existing different decoded data or archive metadata causes a controlled stop;
verified prior encodings (including plain legacy CSV) are retained without replacing their bytes. No generation is overwritten or deleted.
Repeat input on the same UTC day retains the commit timestamp and needs no
publication PUT when the compatibility copies are intact. A new day records
a new dated generation even if the dataset bytes are unchanged.

For an existing installation, publication proceeds as follows:

1. Read the root bytes and opaque provider revision from the **same GET**.
2. Verify the complete committed pair. A legacy manifest first tries the
   current pair, then only its exact original dated snapshot pair. Both files
   must satisfy header, decoded SHA-256, row count and byte counts. Authorization,
   service and invalid response failures are not missing data. There is no
   newest-snapshot search or partial pair fallback.
3. Before modifying a legacy current copy, seal the verified old pair as one
   generation, preserving its original archive day, encoded bytes and timestamp.
   Keep the exact old manifest in a create-only, hash-named origin record.
   Upgrade the root with `If-Match` and retain only the actual acknowledged
   provider revision. A missing or lost acknowledgement stops the invocation.
4. Seal the new complete generation; conditionally update affected compatibility
   CSV copies; commit the root once with the captured revision. A conflict stops
   without rebinding to the competitor's newer root or rolling anything back.
5. Publish successful source-check receipts only after confirmed publication.

A cold runner follows the committed generation, even if compatibility CSVs
were partly changed when a previous writer stopped. Restore validates both
remote files before replacing local files. It returns a typed baseline proof
only after both replacements succeed and the local pair is checked again.

## Normal derivation versus explicit full export

General, scheduled, granular-enabled, historical and targeted normal refreshes
carry that same proof through their existing stages. The proof binds storage
client/bucket, invocation, local directory, exact root body/revision and both
input hashes. Direct normal summary/activity entry points verify their local
pair before source fetching. Missing local files can restore the whole pair;
existing divergent local files stop and are not silently overwritten or adopted.
After a failed direct publication, re-establish the committed local baseline
before another normal source fetch. Shared orchestration already restores it.

`summary_export.run`/CLI is a separate explicit full-pair operation. It captures
a verified existing target before mutation; a missing normal proof never grants
full-export intent. No ordinary producer uses that replacement entry point.

Initial publication requires known missing root/current keys and a bounded
summary-prefix inspection, not merely a root 404. Unknown, corrupt, orphaned,
truncated or inaccessible storage stops. The existing **direct summary
submodule** bootstrap has a separate typed verified-empty proof captured before
source fetch. The shared refresh orchestrator still requires a manifest; this
change does not implement its empty-bucket bootstrap.

With no prior commit: create the complete archive, conditionally create the root,
then conditionally create or verify equal compatibility copies. A root-create
loser never replaces current CSVs. Explicit full-export retry may complete a
bounded matching initial archive stage (at most two three-object generations,
one listing capped at seven entries). Paths, real archive day, generation ID and
all existing bytes must match the incoming pair. A cross-day retry creates a new
dated generation and retains old staging objects. A committed initial root with
incomplete copies is recovered through the normal verified existing-target path.
No orphan garbage collection or historical inventory is introduced.

## Boundaries and rollback

The root is a single conditional commit, not a multi-object transaction.
Existing Worker and Python consumers that read `summary/*.csv` directly may
observe an intermediate pair. They are not changed here. Deployment must quiesce
old and queued writers and preserve the existing `garmin-sync` coordination;
older unconditional writers must not overlap the new publisher. Out-of-band
object mutation can still make data unverified. Already lost legacy bytes are
not reconstructed or guessed.

The existing 250-write / 512-MiB refresh-stage and 100000-object / 5-GiB bucket
guards remain unchanged. Conditional attempts, including rejections and unknown
results, consume those allowances. Immutable generations and incomplete stages
increase stored objects/bytes. Full budget inventory remains the existing
guarded operation; initial recovery adds only a bounded prefix check. A guard
stop leaves the last committed generation readable. No schedule, retry, source
window, rate, Garmin write policy or Coach Input budget is increased.

Rollback must retain a reader that understands committed generations and their
verification rules. Reverting to the old unconditional publisher can reintroduce
history loss and needs a separate reviewed plan. Do not restore/delete R2 data,
alter security configuration or remove retained origin/generation objects.

## Offline verification and resource evidence

Permanent tests exercise actual export/R2Store/restore and normal orchestration
with synthetic external S3/Garmin responses. They cover before/after every PUT
for initial, marked and legacy publications, cold recovery, retries, cross-day
origin preservation, proof binding, source-stage races, root/create collisions,
unknown acknowledgements, invalid storage and every existing guard category.
The targeted tests retain partial moving-time and immutable coach-history checks.

`scripts/benchmark_summary_recovery.py` runs the same synthetic workloads against
a supplied source tree. Compare separate processes with `--source-repo` pointing
at the reviewed baseline and candidate. It measures SDK GET/HEAD/LIST/PUT, body
download/upload, stored objects/bytes, source calls and local elapsed time for
1/1, 1000/365 and 10000/3650 activity/health rows.

For the largest sample, unchanged explicit export changes from 1 GET + 5 HEAD to
7 GET + 0 HEAD; both perform 0 PUT / 0 LIST. A changed activity's metadata changes
from 3 to 5 PUT, sealing the paired archive before commit. A new archive day
changes from 3 to 4 PUT. Cold restore remains 3 GET. Normal cold reconciliation
uses the same 72 synthetic source calls, with 9→10 PUT; the recent cold repeat
uses the same 25 source calls and 2 PUT. Both still use one guarded full inventory.
Initial explicit publication adds one bounded namespace LIST and one archive
metadata PUT. Increased body reads and retained storage are the cost of verified
paired recovery; this is not a total performance or free-tier saving claim.

These are local synthetic observations. Internal provider retries, network,
queue/scheduler delay, real history size and billing are not measured. Production
conditional behavior, writer quiescence and canary recovery require separately
authorized release validation; no negative faults should be provoked live.
