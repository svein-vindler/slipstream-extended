# Explicit full SDK verification with connected-reader canaries

`ALLSDK_EXACT_CONNECTED_READER_CANARY_V1` is an explicit verification policy for
historical, source-free bulk continuation and fresh daily-after-source bulk.
It changes no product bytes, permissions, account allowance, rate limit or
deadline. Full SDK **and** full connected-reader verification remain the default
when neither mode explicitly activates its policy: `bulkVerificationPolicy`
for rolling continuation, or `bulkAfterSource.verification_policy` for daily.
An explicit null or invalid policy cannot silently select the default or a
smaller sample. Existing rolling activation and all resource bounds stay unchanged.

For the opt-in policy, every produced product in the sealed pending set receives the existing
independent SDK byte, native revision, header and source/context postimage
checks. Only the number of connected-reader functional checks is reduced.
An unchecked connected-reader target must never be reported as reader-verified.

## Deterministic selection

Selection uses actual pending target identities and the pinned scope timezone;
it reads no source objects. For the legacy three-window scope, there are six
groups: `heart_rate` and `hrv`, each with 28-, 30- and 90-day windows.

For each nonempty group, sort available endpoint dates and select:

1. First, lower-middle (`(count - 1) // 2`) and last target.
2. The earliest actually available endpoint on day 1 of a month.
3. The earliest actually available January 1 endpoint.

Then, for each channel, select the earliest available endpoint with a forward
DST transition and the earliest with a backward transition. A transition is a
change in `dst()` from the preceding day's local noon to the endpoint's local
noon in the declared scope timezone. If several windows contain that same
earliest date, prefer 90, then 30, then 28 days.

Deduplicate exact target keys. At most `6 * 5 + 2 * 2 = 34` targets are selected.
Large ordinary groups provide at least the first/middle/last coverage; tiny
groups naturally deduplicate to fewer actual targets. All six groups are
reported, including absent groups. Missing month/year/DST endpoints are null,
never replaced by manufactured targets or claimed coverage. The scope timezone
describes calendar selection; it does not assert a traveller's actual night
timezone. Exact date, UTC, exclusion and context arguments remain unchanged.

## Local generator and explicit activation

Before the new owner verifies or acknowledges products, create a policy using
`pipeline.reference_verification_policy.create_canary_policy(job, plan_sha,
scope_ref, workset_path, new_invocation=..., output=...)`.

This create-only local generator uses the existing sealed workset, original
scope/deadline, source/build seals and fully verified checkpoint prefix. It
does not fetch, collect, rebuild, publish or create a new account. The policy
binds the exact pending identity digest, prior verified keys and independent
GET count, deterministic selection and new invocation. The complete versioned
schema is the module's strict `FIELDS` set; caller-supplied counts or a
self-selected target subset are not authority.

Root must explicitly put the resulting path/bytes/SHA256 pin in the new
operation binding's `bulkVerificationPolicy`. The ordinary rolling entry
permits this only with the existing historical `bulkContinuation` transition.

For daily-after-source, explicitly set `verification_policy` to
`ALLSDK_EXACT_CONNECTED_READER_CANARY_V1` in the pinned `bulkAfterSource` request.
After genuine source delivery and physical source termination with no active
native children, the operator creates a `DAILY_AFTER_SOURCE_CANARY_POLICY_V1`
phase wrapper for a fresh sealed workset before publication, with no prior
publication checkpoint. It pins the actual daily binding, after-source context
and deterministic selection policy; the operation remains `daily`. The shared
validation binds the source/build seals, invocation, plan, scope, target,
windows, timezone and original deadline. Native ACK independently validates
this same genuine context and phase wrapper. Every produced sealed product
still requires full SDK verification; only connected-reader coverage is reduced
to the existing deterministic selection, at most 34 targets. Declared-window
scopes use their existing selector; the three-window example above is unchanged.

No environment flag, scheduler, global daily default or mere file existence
activates either policy. Daily without explicit activation retains all readers.
Neither mode adds permission, source calls, retries or resource allowance.

## Evidence and accounting

The existing single-intent connected-reader protocol remains serial. Every
selected target requires a fresh observer/permission/capture, one actual MCP
response and the existing complete semantic JSON/SSE parser match. SDK or HTTP
reads are not substituted for that production connected-reader call.

The scope and proof pin the policy and selection before verification. Proof
fields explicitly separate:

- `verification_policy_ref`: the exact policy pin (binding policy for rolling,
  phase-wrapper pin for daily);
- `verification_coverage`: policy mode, SDK target count, connected-reader
  target/GET count, targets not checked through the reader, selection digest;
- `verification_gets`: actual SDK, connected-reader and combined independent
  GETs, plus reader observation count.

For `N` new pending products and `K` canaries, successful verification consumes
`N` SDK GETs and `K` reader GETs. The old independent count remains charged;
the final account count must equal prior count plus `N + K`. The original
finite resource plan and all technical bounds stay unchanged. Lower actual
usage does not reset or refill an allowance.

ACK independently verifies every SDK target and the exact deterministic
canary set, rejects missing/duplicate/mismatched observations or false counts,
and preserves compatibility with older full-reader proofs. The proof's
`verified_products` advancement means compliance with its explicitly approved
policy; it does not mean all products were checked through the connected reader.
Original historical fully verified targets are not probed or republished.

Any mismatch, unknown result, expired original deadline or invalid policy
stops. This policy adds no retry, unknown-write adoption, general recovery,
deadline extension, parallel RPC or automatic permission.
