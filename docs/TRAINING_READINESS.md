# Stored Training Readiness and Recovery Time

This additive component is **disabled by default**. Local implementation and
synthetic tests are not activation, release or production verification. Training
Effect remains in existing activity details; Status/Load Focus, factor scores,
backfill, new schedules and coach/analyzer changes are outside v1.

## Collection and authentication

Only literal `TRAINING_READINESS_ENABLED=true` enables the component. Unset or
`false` performs no component session/login/provider/R2 operations; invalid values
never enable it. The refresh workflow explicitly keeps `false`. Activation needs
separate approval and a reviewed configuration change. Ordinary existing shared
login is unchanged. The component receives that authenticated client; it never
initiates login or calls a provider-login fallback.

Collection occurs after critical work in general refresh only, without backfill
or repair-only. Targeted night/activity and R2-only repair never fetch training
data. `HEALTH_TIMEZONE` must be explicitly available to Python as a valid IANA
zone; the workflow wires its existing private secret without supplying a value.
There is no UTC/runner-date fallback. Windows must have a usable IANA database
(or an explicitly supplied existing `PYTHONTZPATH`); missing timezone support
skips the component with `configuration_required`.

Optional private `TRAINING_READINESS_DEVICE_ID` is a canonical positive decimal
ID of at most 20 digits. Unset/empty leaves selection automatic only for exactly
one device. Multiple devices stay ambiguous. Only a complete validated response
(`available`/valid empty `no_observation`) can establish `selected_device_unavailable`.
Failure/partial preserves a known device's retained observations (`available`)
or leaves selection `unknown` without earlier observations. Last collection
outcome is separate and takes precedence in the day status. There is never
another-device fallback. MCP input cannot change configuration or transport.

The pinned Garmin Connect 0.3.17 SDK supplies existing DI or JWT_WEB headers via
its reviewed `get_api_headers()`. There is **at most one fitness HTTP attempt and
zero component auth HTTP**, with no auth adapter, auth POST, token parser,
mutation, token file, proactive renewal, `_refresh_session()` or 401 replay.
401 closes unread and returns `auth_required`; a later ordinary approved refresh
may try again. Shared login/auth traffic elsewhere in the job is separate.

The internal sender builds exactly a GET to the already validated
`connectapi.garmin.com` or `.cn` origin and
`/metrics-service/metrics/trainingreadiness/YYYY-MM-DD`. It traverses both guards.
Generic SDK/native/decorated paths retain their route/stream restrictions; no
arbitrary URL, method, parameters or stream option is added. The capability for
the private streaming path is closed inside the policy installer. SDK/session/
adapter/header drift fails closed. TLS verification is required; redirects and
fitness writes remain forbidden. No network/429/5xx retry is added.
The shared policy exception lives in an import-independent module and remains
available through the existing read-only import. Bounded transport re-raises
`GarminReadOnlyViolation` before handling genuine network `OSError`/timeouts.
An opened response still closes on policy failure; no source-error day is PUT.

Responses request identity encoding. Non-200/error/401/3xx bodies have a zero
application-read budget and close before handling. A 200 JSON response is read
as raw identity bytes, at most 256 KiB plus one overflow-control byte, then closed
before strict UTF-8/JSON parsing. Metadata is an early check, not the sole limit.
Compressed/invalid/truncated responses fail safely. At most 64 observations and
four devices are normalized; arbitrary DTO/text/feedback is discarded. The
helper returns fixed statuses/counts plus the bounded internal DTO; raw payload
is never logged or exposed by MCP.

Connect timeout is 3s, read-idle 5s, with a cooperative 30s budget checked before
send and between bounded `read1` calls. DNS/multiple addresses and blocking I/O
prevent a hard 30s wall-time claim. No new send begins after the budget expires.
All response paths close; existing shared sessions remain owned by the pipeline.
The fitness counter increments immediately before the guarded request, including
failed sends, independently of the existing `connectapi` counter.

## Canonical objects and preservation

The sole root is `health/training-context/v1/YYYY/MM/YYYY-MM-DD.json`: one plain
UTF-8 JSON day, at most 16 KiB, with schema/normalizer revision 1. No index,
manifest, compressed alias, cache, LIST or index repair is introduced. Python
uses a counted bounded GET with ContentLength/encoding checks, at most 16 KiB
plus one control byte, closing the StreamingBody before parsing.

Normal collection adds 1 R2 GET, 1 normal HEAD and at most 1 day PUT. It reuses
the final critical stage's write allowance and the existing shared bucket budget;
it does not reset quotas. There is no separate inventory. If previous writers
established none, its first PUT initializes the existing shared inventory guard:
this can add a full bucket LIST scan versus an otherwise read-only baseline.
If inventory already exists, only possible extra pages from bucket growth remain.
Stage timings/counters are deltas when the store is reused.
Values and checks publish in the same single PUT. A failed/uncertain PUT is not
retried and is not reported as definitely published. The next ordinary run reads
actual canonical storage. Unknown previous schema/read failure prevents overwrite.
If an existing day's configured timezone differs, collection stops before the
provider with `configuration_required`, preserving its original clocks/zone.
There is no automatic migration or relabeling of retained time provenance.

Partial, empty, auth and genuine provider/schema failures preserve previous good
fields and their own observation/check times, setting `retained_previous`.
Only validated incoming fields advance their checks. A valid empty response can
advance the day source check while preserving older fields; a check does not
change their observation time. Policy/budget failures propagate. Existing
sleep/HRV/activity receipts, freshness/cooldown/polling and five-minute contracts
are unchanged. No new receipt or measurement PUT exists.

## Semantics exposed to AI

- Readiness and Recovery are validated independently. Either available field is
  exposed even when the other is missing or invalid; the day's `partial` status
  does not hide valid fields. Missing is null, never zero or proof that hardware
  lacks support. Recovery 0 remains an available measurement even without Readiness.
- Readiness is a **Garmin estimate**, integer 1–100 with unit `score_1_100`. 0,
  booleans, coerced strings and nonfinite numbers are invalid, not known sentinels.
- Recovery is **reported minutes at observation time**, integer 0 through
  2,147,483,647; 0 is valid. Hours are a client display conversion `/60`. There is
  no server countdown. Positive minutes with the verified `REACHED_ZERO` marker
  are `conflicting`, with unknown effective remaining minutes. Unknown/missing
  phrase is null, never raw text or a claim that recovery has not reached zero.
- Garmin `calendarDate` and local timestamps take priority. Explicit offset/Z
  can establish UTC. Offsetless `timestamp` is not assumed UTC. IANA fallback
  from local clock rejects DST holes/repeated-clock ambiguity. Travel/timezone
  conflicts and unknown clocks give null UTC/age, no adjacent-date calls.
- Latest is selected within one device with verified ordering; tied/conflicting
  or unordered multiple observations stay ambiguous. Morning requires exactly
  one deduplicated `AFTER_WAKEUP_RESET` observation. No first-row/device fallback.
  Sleep wake-date/night_of is separate from this observation date.
- Each field exposes value/status/unit, observation/local time, time provenance,
  check time/age and retention. The `training-observation-8h-v1` display policy
  labels historical/within_8h/older_than_8h/unknown; it does not promise current
  Garmin state or change existing five-minute freshness. Unknown support remains
  unknown. Source revisions hash normalized values/time provenance independently
  of attempt/check timestamps. Raw device IDs remain private; MCP uses only
  response-local candidate references.

## Watch display versus Garmin Connect data

Garmin's [Recovery Time support documentation](https://support.garmin.com/in-ID/?faq=8ImmxVkZMh4EYYq5Zp2bR8)
states that the watch updates Recovery Time continuously, while Garmin Connect
updates it after device sync. It also states that Connect exposes Recovery Time
only for devices supporting Training Readiness; other devices display it on the
device. A recovery value visible on a watch therefore does not establish that
the value is available through this component's fixed Connect endpoint.

This documentation describes Garmin's product behavior, not a guaranteed contract
for the unofficial endpoint or all devices. The implementation uses validated
returned fields, with no model-specific capability registry, rejection list or
fallback endpoint. Empty/partial responses and auth/schema failures cannot establish
unsupported hardware. MCP keeps `support: unknown`, including when values are
available. Multiple devices remain separate candidates; selecting one never
fills its missing field from another device. Previous good fields retain their
own measurement/check times and provenance after an incomplete or failed check.

Synthetic coverage establishes these data contracts without identifying a watch
model. Positive live coverage of actual Readiness/Recovery values remains
unverified; support on all watches is not promised. Further live verification
and activation require separate approval; collection remains default-off.

## Direct history tool

`training_history(start_date,end_date)` is authenticated private R2 reading in
every existing registration mode. Inputs are strict, real ascending dates,
1–31 inclusive. The tool derives one key per date and makes exactly one GET per
date, including missing dates. There are at most four active GET/body readers;
results are date ordered. Metadata and BYOB reads enforce 16 KiB plus at most
one control byte before parsing, without decompression. Every stream is released.
Successful object content is at most 496 KiB for 31 dates.

There is no positive/negative/date-TTL cache, HEAD, LIST, PUT, provider call, sync,
automatic split or repair. The next call sees a new/updated historical object
after its PUT completes; a multi-day result is not an atomic cross-object snapshot.
Absence means `not_collected`/`canonical_object_absent`, not proof of no Garmin
data. Stored valid empty, stored auth failure, malformed/oversize and R2 failure
are distinct. No raw keys, DTOs, device/profile IDs or exceptions are returned.

Output limits are 64 KiB structured content and 128 KiB the serialized whole MCP
result including escaped text copy/metadata, excluding HTTP/JSON-RPC framing.
Oversize returns `output_limit`, the specific limit and a shorter-period message,
with no silently truncated day rows. The standard compact projection returns
all 31 ordinary one-device rows, including distinct morning/latest observations.
Complex legal multi-device/retained-field cases can still exceed these same limits.

`training-history-compact-v1` shares source, SDK/normalizer version and metric_units
at result level. `as_of` is the clock used for ages. Each day has these local references:

| Reference | Resolve within the same day | Meaning |
| --- | --- | --- |
| candidate latest_observation / morning_observation | observations.observation_ref | Slots and their independent availability/ambiguity remain explicit; identical slots share one observation within that device. |
| observation / metric time_ref | clocks.time_ref | Local/UTC observation clock, time source, observation age and display state. A retained field can reference an older clock than its surrounding observation. |
| metric check_ref | checks.check_ref | That field's own check timestamp/age; null means unknown, never the latest attempt. |

Metrics keep value, status, retained_previous and Recovery conflict/remaining-time
semantics. Day provenance keeps source_revision, source_checked_at, timezone and
last_attempt, independently of selection_status/selected_candidate. Units resolve
by metric name in metric_units. These references are a response-local projection,
with no persisted index, cache or extra read. There is no full-detail mode.
Earlier unactivated v1 objects with a false absence label following failure/partial
are projected conservatively as known retained selection or unknown; source outcome
stays visible. Canonical storage root/limits and the other tools are unchanged.

```json
{"tool":"training_history","arguments":{"start_date":"2026-10-01","end_date":"2026-10-07"}}
```

Use existing sleep/HRV and user-supplied RPE/context alongside these stored
estimates, with explicit missing data and timing. No medical causal claims,
automatic replacement of thresholds or historical coach revisions.

## Validation and resource measurements

Run existing `ruff check .`, `pytest`, and from `worker` `npm run typecheck` and
`npm test` with unchanged locks. The runtime fixture generator produces synthetic
Python day bytes, then local Workers R2 and authenticated MCP exercise history.
Network is mocked/blocked; no Garmin account or production bucket is used.
Transport tests cover real SDK DI/JWT_WEB header paths, 401/non-200/redirect bodies,
byte limits, compression, close, timeout, policy/drift and no token mutation.
Policy tests include the identical exception from send/read and real lower-guard
drift after the upper check; a policy failure cannot publish a source-error day.
History tests cover overwrite/missing-to-created, full-reader concurrency,
false metadata/body cap, both output limits, privacy and schema errors, all 31
ordinary rows through authenticated MCP, retained field references, and configured
device failures/partial versus validated absence with/without earlier values.
The same Python fixture generator also round-trips independently available fields
through the canonical codec into authenticated MCP: both measurements, either
measurement alone, Recovery 0 without Readiness, invalid independent fields,
empty responses, retained fields after empty/partial/error, and complementary
measurements from two devices with automatic or explicit selection. Existing
configured auth/timeout/schema/byte-limit failure tests are reused rather than
duplicated. All fixtures are invented and contain no model-specific configuration.
The independent-field follow-up passed 879 Python, 135 Worker unit and 167 Worker
runtime/contract tests (14 added), Ruff, typecheck, catalog and tracked-tree privacy
checks. Production code, dependency locks, workflow and all limits are unchanged.

Reproduce component deltas with `python scripts/benchmark_training_day.py`; after
fixture generation, run `node scripts/benchmark-training-history.mjs` in `worker`.
Each case uses 100 samples on the same invented data. Feature-off remains the
zero-operation component baseline. Review comparisons also use the same input
objects before and after the compact projection; a previous output_limit is not
the same output as 31 returned rows. No speedup is promised. Full core-pipeline
cost is outside this component benchmark.

Local synthetic Windows run on 2026-10-09, Python 3.14 / Node 24.19.0:

| Component case | New fitness/auth | GET/HEAD/PUT/LIST | Consumed provider /day read/write bytes | Mean wall ms |
| --- | --- | --- | --- | --- |
| Disabled baseline | 0/0 | 0/0/0/0 | 0 /0/0 | 0.0032 |
| Enabled, absent day, HTTP 200, shared inventory ready | 1/0 | 1/1/1/0 | 230 /0/2,066 | 2.1778 |
| Enabled, stored day, HTTP 200, shared inventory ready | 1/0 | 1/1/1/0 | 230 /2,066/2,066 | 2.2291 |
| Enabled, stored day, HTTP 401, shared inventory ready | 1/0 | 1/1/1/0 | 0 /2,066/2,066 | 2.0089 |
| Enabled, stored day, HTTP 200, shared inventory absent (one synthetic page) | 1/0 | 1/1/1/1 | 230 /2,066/2,066 | 1.8467 |

| Added history call | GET | Consumed object bytes | Peak readers | Structured/full-result bytes | Mean wall ms |
| --- | --- | --- | --- | --- | --- |
| 1 day, one device | 1 | 2,066 | 1 | 1,819/3,920, 1 row | 0.5518 |
| 31 days, one device, shared morning/latest | 31 | 64,046 | 4 | 41,338/87,758, 31 rows | 6.0564 |
| 31 days, four devices | 31 | 206,677 | 4 | 620/1,370, output_limit | 9.3318 |
| 31 missing days | 31 | 0 | 0 | 3,631/7,880, 31 rows | 0.1612 |
| 31 days, one device, distinct morning/latest | 31 | 63,488 | 4 | 58,779/124,872, 31 rows | 4.5093 |
| 31 days, four devices, distinct morning/latest | 31 | 204,445 | 4 | 620/1,370, output_limit | 7.4801 |

Before this review, the first four identical-input history cases had respectively
2,734/5,830 bytes (1 row), output_limit 395/890 (0 rows), output_limit 396/892
(0 rows), and 3,407/7,402 (31 missing rows). GET/body/reader counts for those inputs
are unchanged. The monthly one-device case now returns 31 analyzable rows at the
same 31 GETs and body-byte cost. The extra distinct-slot cases are new coverage.

History has 0 HEAD/LIST/PUT/fitness/auth. The pre-feature baseline had no tool and
zero component operations; this measures added functionality, not equal outputs
from an older implementation. Times include bounded read/parse/normalization or
projection/serialization in warm Python/Node, with synthetic in-memory transports.
Startup, shared login, network latency/wire prefetch/headers/TLS, SDK retries,
queueing and cold edge CPU are excluded. They are not live measurements,
Cloudflare-billed CPU, GitHub-billed minutes or a hard transport-time guarantee.
Operation counts are application SDK/binding calls; underlying retries are separate.
Inventory/HEAD metadata bytes are not measured; the byte columns cover consumed
provider and day-object bodies. Fixtures use correct local/UTC dates across the
October DST transition. Final local validation passed 879 Python tests, 135
Worker unit tests and 153 Worker runtime/contract tests, plus Ruff and typecheck.
Installed dependencies are consistent and locks are unchanged; no dependency was
added or upgraded. Runtime `pip-audit` and `npm audit` found no known vulnerabilities
on this date; release still requires its normal checks against the release base.
The additional fitness/auth/byte counters are returned in memory to synthetic
callers, without a new production log or measurement PUT. Existing aggregate
`garmin_connectapi_calls` does not see this direct bounded transport: zero in
that older counter must not be interpreted as zero new fitness attempts.

After separately approved activation, 120 syncs and 30 maximum history calls add
at most 120 fitness attempts, 0 component auth, 120 PUT and 1,170 GET/HEAD, with
480 KiB/30 retained day objects at the caps. Consumed success fitness body is at
most 30 MiB total plus overflow bytes, no component-auth body. Existing inventory,
Worker 06 detail reads, core jobs and account usage must be added. No quota rises,
paid-service guarantee, certain free operation or latency improvement is claimed.

Linear R2 Standard resource pricing for this example is about $0.00097, or
$0.00151 with an assumed 120 extra existing-inventory pages. If training is the
first writer in all runs, add up to `120 × S` pages, with `S` the actual full
inventory page count (unknown), about `$0.00054 × S` linearly. That scenario is
additional to the simple page-growth example and can increase job time. This is not a bill
forecast; account allowances and rounding matter. An assumed 2–10s of real
additional normal job work could add 0–120 rounded Actions minutes ($0.72 without
allowance at $0.006/minute). Synthetic millisecond timings cannot establish that
network/job estimate. Edge CPU, actual source/device/date/identity compatibility
and production cost remain unmeasured. Release, activation/live canary and public
promotion need separate approvals; stop at Master-review.

Sources: [pinned SDK fields](https://github.com/cyberjunky/python-garminconnect/blob/218e72ca5459e014435fc2d94fd18bd601fa0c14/garminconnect/typed.py#L323),
[SDK headers](https://github.com/cyberjunky/python-garminconnect/blob/218e72ca5459e014435fc2d94fd18bd601fa0c14/garminconnect/client.py#L450),
[Requests streaming](https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow),
[timeouts](https://requests.readthedocs.io/en/latest/user/advanced/#timeouts),
[R2 binding API](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/),
[R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/),
[R2 prices](https://developers.cloudflare.com/r2/pricing/),
[Actions prices](https://docs.github.com/en/billing/concepts/product-billing/github-actions).
