# Correlated workflow startup receipts

New MCP general, activity, night and R2-only repair dispatches carry the original
immutable operation UUID through the existing `sync_request_id` workflow input.
The Python orchestrator uses it as its diagnostic correlation ID as well.
The workflow, permissions, concurrency, schedules, input types and budgets stay
unchanged. Invalid boolean text is rejected instead of becoming a known false flag.

## Format and source

The existing private key `refresh/requests/<request_id>.json` contains one
create-only JSON object, bounded by the existing 64 KiB control limit:

| Field | Contract |
| --- | --- |
| schema_version / kind | Integer1 / refresh-dispatch-receipt |
| request_id | Canonical RFC UUID with supported version, exactly the operation ID |
| run_id | Positive safe integer matching the actual workflow run |
| source.repository | GITHUB_REPOSITORY |
| source.workflow_path | Workflow file parsed from GITHUB_WORKFLOW_REF for the same repository and ref |
| source.event | Verified workflow_dispatch from GITHUB_EVENT_NAME |
| source.ref | Original GITHUB_REF branch ref |
| source.source_sha | Original GITHUB_SHA, matching the later actual run response |
| request | Strict fields mode, scope, date, activity_id, new_expected, repair_only, include_granular, components |

The producer checks run ID against GITHUB_RUN_ID and rejects missing, mismatched
or unsupported context before storage/login. No new provider fields are inferred.
Normal correlated receipts support bounded ASCII branch refs and workflow file names,
not tag refs or arbitrary ref syntax. Unsupported context is rejected; the existing
fixed refresh/main path is supported.
Local calls without workflow context do not manufacture this proof. Scheduled
and historical jobs preserve existing behavior and emit no bounded startup proof,
even if a correlation ID is supplied. Targeted modes retain their otherwise
ignored include_granular flag literally in metadata; it adds no components or
extra work. Old minimal receipts are distinguishable by format.

Request coverage expresses requested work, not completed data:

- General: null scope/date/activity_id; summary, health and body, plus
  activity_files and coach_input only when include_granular is true.
- Night: exact night/wake-date scope; sleep and hrv. Activity ID is null and
  new_expected is deliberately false because activity expectation is inapplicable.
- Activity: exact existing activity/date/ID/new-or-known scope; activity_files,
  coach_input and context. Repair-only is a separate explicit boolean; scope alone
  cannot prove repair. Missing dates/IDs remain null where that mode permits them.

The Worker compares every relevant request field with its persisted descriptor.
It accepts no string/number boolean coercion, missing coverage, inferred mode or
unsupported format. Repository, workflow, event, ref and SHA must also agree with
the actual GitHub run response before job status is verified. Startup metadata
alone proves neither terminal status, source_checked nor data_ready.

## Publication and immutable history

The first existing storage stage publishes startup before restore or provider
login. It uses that stage's R2Store and ordinary shared bucket inventory.
Conditional create reserves writes/bytes before transmission. An already present
object is read once under the64KiB bound and accepted only for identical bytes;
a mismatch, old minimal receipt or uncertain write stops without overwriting it.
There is no automatic retry or metadata upgrade.

After that successful publication, orchestrated targeted children do not publish
a second minimal receipt. Standalone legacy receipt writers also use create-only,
identical-byte matching. Existing reports, checks and canonical storage retain
their prior contracts. This change performs no historical regeneration, owner
repair, key migration or receipt adoption.

A matching replay is metadata idempotence; it is not an additional distributed
execution lock. Existing GitHub concurrency and Worker admission/duplicate guards
remain the execution controls. SDK-internal retries are distinct from counted
application operations and cannot refund reserved capacity.

## Reads and admission

Each admitted claim may take one fixed receipt before any run GET. The coordinator
persists the full matched startup evidence, candidate and current revision in the
same immutable owner/claim transaction. All external I/O remains outside it.
A stale/rejected metadata response cannot rebase to the returned current revision.
Changed evidence or another operation's run is held; first evidence is preserved.

General no-ID/204 can now use this fixed UUID correlation instead of latest/time
matching. Initial reads still require the original two-minute start lease and
share its five-run-GET total. Later explicit status uses a shared25second
actor/operation gate across request/run aliases and also honors any existing
actor/run gate. Learning the run ID cannot bypass an occupied run gate.
Failed dual admission does not consume a fresh operation window.

Targeted still has exactly three persisted tickets, at most two run GETs per
normal ticket and one after original expiry on explicit status. General adds one
bounded metadata GET relative to the previous no-receipt path; report/diagnostics
remain at most one each, under the same object limits. There is no new polling
round, lifetime quota, automatic POST, refund or reset.

The adapter performs one run observation per admitted invocation. A verified
queued/running general result remains data-unready, stops automatic polling and
states the existing25second status interval for a later explicit check.
Terminal projection still needs the actual run plus relevant existing
report/diagnostics and exact mode/scope coverage. Stored checks and package
completeness remain separate from current-job source evidence.

## Compatibility and rollout

Old minimal or missing receipts stay unverified. An unmarked C8 active owner or
descriptor-free legacy owner is not upgraded into this new startup contract,
rewritten or given fresh tickets. Existing cached verified terminal facts remain
readable. Missing/invalid receipts, unsupported state, orphan claims and exhausted
tickets do not authorize discovery, owner release, TTL recovery or a new dispatch.

Receipt arrival can recover a normal204 through an already permitted invocation;
it does not promise that workflow setup finishes before the initial MCP response.
There is no automatic later follow-up. Retention, orphan/exhausted recovery and
mixed old/new consumer rollout still need an explicit release decision.

Metadata and durable payloads add storage and operations. A synthetic general
flow whose other stages write nothing now also establishes the ordinary inventory
and enables existing private diagnostic publication; count both startup and that
diagnostic PUT. This is not a new diagnostic feature or a cost reduction.
Other writing stages reuse the same inventory and unchanged limits.

Rollback must preserve immutable receipt publication and current owner/claim
semantics. Old producer code can overwrite rich receipts and old consumers cannot
interpret new state safely. Use reviewed forward correction or an explicitly
controlled quiescent transition, never row deletion, ticket reset, R2 restore or
silent adoption. Synthetic tests do not establish production availability.
