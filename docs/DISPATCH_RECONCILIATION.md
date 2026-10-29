# Dispatch reconciliation

This contract applies to new MCP general and targeted dispatches. The
[startup receipt format](DISPATCH_RECEIPTS.md) binds their original operation UUID
through the workflow and Python producer. It changes control metadata while
preserving Garmin source data, canonical analyses, OAuth policy, bindings and
data schedules.

## Persisted ownership and evidence

The existing SQLite coordinator adds `dispatch_operations_v1` and its indexes.
Each operation has an immutable operation ID, owner key and owner token, a
revision, a fixed request descriptor and a durable read claim. Targeted jobs keep
their original `fresh_jobs` request ID, scope, expiry, daily debit and poll count.
There is no external migration, new Durable Object class or binding.

The direct POST helper returns one controlled result before any follow-up GET:
a positive ACK candidate; a known rejection from an actual POST HTTP response
(400/401/403/404/422/429); or unconfirmed. Empty 204, malformed ACK, transport
exceptions and other HTTP responses remain unconfirmed. Provider exception text
does not establish rejection. GET 422 cannot become POST 422.

A same-owner direct POST result merges idempotently against the current revision.
It never replaces a different candidate or verified binding. Contradictory
evidence holds the owner for inspection. A read claim remains occupied while its
I/O is pending, including eviction/restart. Stale GET results cannot rebase onto
a newer revision: they can close only their own claim, without refunding a ticket,
releasing another owner or publishing their observation.

Matched startup metadata is stored before any run GET on the same claim and
revision. Unmarked older owners are retained without upgrading them to this
contract. Verification requires the actual run response ID, configured repository and head
repository, workflow, workflow_dispatch event, ref and source SHA, together with
the existing scoped receipt and per-run report (or general diagnostics/report)
that establish mode and requested coverage. A full startup receipt plus matching
actual run verifies queued/running job status; it establishes no source check or
package readiness. Terminal still requires relevant per-run metadata.
Missing/malformed/mismatched evidence cannot release the duplicate guard.

Only a newly committed observation is fresh evidence for this response. Cached
terminal facts retain job status; targeted cached results do not reassert current
package readiness or re-read per-run control objects. Stored source checks remain
separate from current-job evidence. Legacy general `data_ready` means job success;
it does not establish a particular complete, fresh activity/night package.

## Budgets and aliases

- Targeted `fresh_jobs.polls` is the sole three-ticket authority for automatic
  windows. Normal admitted
  claims allow at most two run GETs; after the original two-hour expiry only an
  explicit status request can spend a remaining ticket, with at most one run GET.
  No automatic polling window starts after expiry. A new explicit sync request
  has the separate, single-use overlap lane described below.
- After all three tickets are spent, a later expressly requested
  `refresh_status(request_id=..., terminal_inspection=true)` can admit at most
  one additional inspection for that same modern targeted operation. Omitted
  or false leaves ordinary admission unchanged. Never enable it automatically.
  The original request ID and full modern metadata are required; a run alias
  alone, general jobs and unmarked legacy cannot use it. An optional run ID must
  match. The same owner/scope/job/run/revision and receipt/effect checks apply.
  Optional `terminal_inspection_used: true` is persisted in the existing operation
  payload with its read claim in the same transaction/CAS, before external I/O.
  Unknown, failed and interrupted observations spend it; no refund/reset/TTL
  release or legacy adoption. `polls` remains three and the daily debit remains.
  Existing per-call bounds still apply, including one run GET after expiry.
- A new explicit `sync_latest_activity` or `sync_latest_night` request can
  reconcile at most one old blocker before ordinary admission, including an old
  active job for the same scope. This is not a background poll or an automatic
  `terminal_inspection`. The old job must be modern, targeted, still active,
  past its original two-hour expiry and at exactly three spent polls. Its exact
  startup receipt, already verified active run/source/descriptor and owner must
  all still match. The persisted job scope/owner must match the original request.
  For an activity repair, the immutable receipt-bound descriptor separately names
  the resolved activity/date; it may fill omitted original fields, but cannot
  contradict an explicit original date or activity ID. Its resolved scope and
  repair origin must match that transformation. General, candidate, unmarked
  legacy, conflicted and open-claim records cannot enter this lane.
  New requests capture one admission instant for requestedAt and the2hour expiry.
  Older modern producers used two clock reads. A differing requestedAt is accepted
  only when it is no later than admission and the unchanged original
  sync-latest-kind[-repair] lease token is this operation ID, with lease expiry
  minus its original300s (normal) or60s (repair) equal to job expiry minus2hours.
  Missing/replaced/mismatched lease evidence stays held; no skew tolerance, lease
  reconstruction, migration, data rewrite or age-only release is permitted.
  `overlap_reconcile_used: true` and an `overlap_reconcile` read claim are saved
  together under the coordinator's transaction/revision CAS before I/O. The lane
  reserves the operation and actor/operation/run25second gates, allows one
  bounded startup-receipt GET and one exact GitHub run GET, and permits no report,
  diagnostics, run-list or dispatch calls. Each control object remains bounded
  at64KiB. `polls`, `terminal_inspection_used` and the old daily debit stay intact.
  An active run, missing/failed/mismatched read or interruption spends the one
  opportunity and keeps the guard; changing date, actor or alias cannot refill
  it. An old timestamp only admits this check and never proves job termination.
- Only in that already-verified overlap lane, matching authoritative GitHub
  terminal metadata settles job ownership even if the final data report is
  missing, or the run failed/cancelled. Startup, repository, workflow, event,
  ref, source SHA, exact run, owner and revision checks remain mandatory. This
  does not attest to source checks, package completeness or freshness: those
  keep their separate unknown/stale/partial status. Ordinary polling and explicit
  terminal-inspection report-coverage rules are unchanged. After settlement,
  `beginFresh` atomically checks active jobs and blockers again; any new dispatch
  still needs its ordinary cooldown and daily allowance. A second blocker is
  returned without another reconciliation read in the same request.
- Each claim can reserve one fixed request receipt and one fixed report and
  diagnostics object, each under the existing 64 KiB control-object limit.
  Metadata reads do not create an additional run-GET allowance.
- General admission keeps the original two-minute start lease, five total initial
  run GETs, successful-run cooldown and missing-workout cooldown. A later explicit
  status window keeps a25second actor/operation gate across aliases and honors
  the existing actor/run gate, at most five run GETs and the existing
  30-per-minute rate limit. It may reserve one fixed receipt before a run ID exists.
- The adapter currently uses at most one run GET per admitted observation.
  Conservative early return does not increase or refill any maximum.
- Operation/request ID, candidate and verified run ID resolve ownership before
  general fallback. Changing alias, actor, day or request cannot refill targeted
  tickets. Unknown overlapping mode/coverage blocks new admission before another
  dispatch or daily debit; unverified operations are not shared as verified work.

Optional `dispatch_status` exposes controlled state and a maintainer next action,
without tokens, owner keys or descriptors. Unknown status projects
`job_state: unknown`, nullable start confirmation, stopped polling and no automatic
next action. Follow its `inspect_dispatch` guidance; do not probe by starting
another sync.

## Legacy availability and recovery

Active targeted legacy rows are adopted only on exact lookup, preserving the
original correlation, expiry, run candidate and spent polls. They have no invented
immutable descriptor, so remain unverified and held. Terminal legacy rows are
never reopened. Existing unowned/manual run reads retain their prior contract;
this change does not complete the separate source-binding/freshness work.

A general204 can use the same request-receipt mechanism as targeted jobs when its
new correlated producer startup exists. Missing or old metadata remains held;
unrelated latest runs and time matching are not evidence. Orphaned read claims
and contradictory evidence also stay held. There is no automatic POST retry, ticket reset/refund, forced release
or expiry-based recovery. Maintainer inspection is guidance, not an added reset
tool. These deliberate availability limits need acceptance before deployment.

State rows add durable storage and SQLite work; no retention or price guarantee
is claimed. Reconciliation reads only fixed per-operation control keys, existing
canonical package reads and active coordinator state; it adds no R2 history scan.
Synthetic operation counts and wall time do not attest live cost, billed CPU or
production exploitation.

A rollout must check compatibility with older in-flight consumers. New code
reads older operations without the optional inspection or overlap marker. Older
strict operation readers cannot decode a record with either new marker or claim intent;
rollback can therefore make those jobs fail-closed unavailable even though their
history and spent opportunity remain. Rolling code back does not undo the
persisted inspection/reconciliation or make it available again. Code rollback
that ignores the new owners/claims is unsafe while they exist. Preserve the
coordinator state and use reviewed forward correction or an explicitly controlled
quiescent transition. Do not delete rows, reset tickets, repoint R2 or restore data
to work around held operations. No deployment or live recovery action follows
from this document.
