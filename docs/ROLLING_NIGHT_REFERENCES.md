# Bounded stored-only rolling references

The existing `pipeline.history_backfill` job can plan historical 28/30/90-date
HR/HRV products and publish them through its existing queue and owner protocol.
The explicit planning commands below support selected stored-only work. Routine
daily continuation needs no operator CLI: the existing daily worker also drains
its persistent source cohort through finite reference-only successors. Neither
path searches the bucket for all historical endpoints.

From the repository root, use an existing private job and its verified plan SHA:

```console
python -B -X utf8 -m pipeline.history_backfill --state PRIVATE_JOB rolling-plan --plan-sha JOB_SHA --confirm-r2-read --endpoint 2026-10-25 --today 2026-10-25 --timezone Europe/Oslo
python -B -X utf8 -m pipeline.history_backfill --state PRIVATE_JOB rolling-refresh --plan-sha JOB_SHA --reference-plan-sha REFERENCE_SHA --confirm-r2-write
```

Planning accepts one to seven explicitly named endpoints spanning at most seven
dates. It reads only bounded canonical/context months, with no provider calls,
R2 writes, bucket inventory or product mutation. Usable, fresh Sleep evidence
establishes eligibility. Each endpoint has six independent channel/window
targets. Missing/ambiguous source coverage is reported honestly. A plan is not a
guarantee that all targets fit the publication budget.

The private immutable plan binds the job, target, calendar, selected endpoints,
source proofs, journal tip and metered read totals. Its bounded hash-addressed
cache holds at most 512 objects/16 MiB. Preserve the job, plans and cache for
restart; do not copy them to a public repository. Plan reads consume the same
publication GET ledger, not a new allowance.

Each `rolling-refresh` invocation opens one ordinary publication phase with a
fresh client. It verifies the plan and target, freshly verifies Sleep proofs,
and uses the existing owner, source fences, CAS, archive/history and checkpoint
engine. The exact selected endpoint set limits the drain; unrelated calendar or
rolling work remains queued. Repeating the invocation resumes known checkpoints
and cumulative budgets. Unknown owner/write outcomes remain HOLD.

An invocation can return `bounded_pending` (native 2) with exact remaining
window/channel ranges. Fourteen phases remain the per-round limit; a larger
backlog is not falsely declared complete or silently retried. When the completed
physical phases exhaust that round, an explicit reference-only round admission
is available:

```console
python -B -X utf8 -m pipeline.history_backfill --state PRIVATE_JOB rolling-refresh --plan-sha JOB_SHA --reference-plan-sha REFERENCE_SHA --confirm-r2-write --admit-reference-round
python -B -X utf8 -m pipeline.history_backfill --state PRIVATE_JOB rolling-refresh --plan-sha JOB_SHA --reference-plan-sha REFERENCE_SHA --confirm-r2-write --reference-round-sha ROUND_SHA
```

Admission requires the exact known terminal reference-only receipt, clean owner,
unchanged target and validated cumulative usage. The hash-bound admission records
the previous receipt and a new round baseline. Replaying it cannot refund usage.
Each round retains GET400/LIST100/PUT100/2 MiB per phase, fourteen phases, at most
6000 publication GET plus 256 independent verification GET, 1000 LIST+PUT and
10 MiB attempted writes. Cumulative counters and earlier evidence are retained;
the explicit plan command never silently rolls over and grants no source allowance.

The ordinary daily job first publishes its raw chunks, then drains relevant
calendar and rolling work in the same run. Thirty new source dates can generate
more references than one round can drain. It retains the active source cohort
and reports `baselines_pending`, with discovery candidates separated from known
channel jobs. Its next ordinary invocation validates the exact terminal receipt,
clean owner, same source cohort, metered cumulative usage and prior progress,
then admits a finite reference-only successor when the previous reference budget
is exhausted. Each invocation retains its fourteen-phase ceiling and all original
round limits. Usage is carried forward with a receipt-bound baseline; replay
cannot refund it. A successor does not reload sources or admit a new thirty-date
cohort. Only finished references close the original source round; the normal
source ledger and actual rolling expiry then govern the next thirty dates.
Unknown operations, unverified receipts, missing progress and genuine capacity
stops retain precise pending/HOLD evidence. There is no time-based takeover or
unbounded scheduler/retry. Routine continuation bypasses neither source quotas
nor its existing journal and does not use the explicit planner's 32-plan cache.
Ordinary hosted consumers can also drain queued references. Do not infer full freshness
from successful canonical writes, and do not increase source quotas to chase a
reference backlog.

The Worker remains a stored-product reader: use `28`, `90`, or a 30-date `custom`
range. No enum, method, units, quantile definition, key format or read schema
changes. Week/month products, old history and unknown-outcome safeguards remain
compatible. A missing product stays missing until explicitly published.

Rollback is a coordinated code rollback only. Keep immutable source and product
history, existing configuration, queued checkpoints and private job evidence.
Older code must not consume a queue containing the additive rolling protocol;
pause consumers before any rollback and review that compatibility boundary.
