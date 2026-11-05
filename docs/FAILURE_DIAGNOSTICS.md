# Failure diagnostics

Application failures keep their failed status, native exit and existing stop/HOLD
policy. Hosted Python retains its `Pipeline failed: <category>.` line and adds a
`Pipeline diagnostic: ` JSON record. For example, synthetic output may contain:

```json
{"category":"r2_write_attempt_limit","component":"r2","operation":"put","reason":"write_attempt_limit","site":{"module":"pipeline.r2_store","line":250},"stage":"cli"}
```

Use category/reason to distinguish resource admission, source rejection and
unknown outcome; use the source module and line from the installed version to
locate the failed boundary. Site fields contain only a fixed application module
and bounded line, never a traceback, function name, filesystem path or identifier.
`http_status` appears only when a recognized typed boundary actually retained a
valid numeric HTTP status. Absence means unknown, not success. An empty curve,
an unknown error, or403 is not proof of an expired login or exhausted quota.

R2 write-attempt, write-byte, bucket-object and bucket-byte limits are separate
reasons. Authentication/access/rate/timeout/missing/service/read-boundary failures
are separate fixed categories. Garmin policy, authentication, rate, connection,
account and unverified response categories use typed errors. Unknown classes and
provider messages cannot select a category. Wrapped explicit and implicit causes
are inspected at most8 links, with cycles stopped and explicit suppression
respected. Builtin missing-field/value/type/JSON/encoding/local-I/O failures retain
a safe category plus site. Private callers keep original exception values/args.

## Coverage

- Existing hosted pipeline CLI decorators and SafeArgumentParser share this
  projection, including operational ValueError/KeyError routed into parser.error.
  Genuine bad arguments remain native2 without echoing their values.
- Additional local pipeline CLI entrypoints use the same outer boundary. Local
  history retains native2/3 and owner/HOLD semantics. Local backup retains known
  failure returns and emits only fixed backup reasons. Interactive login and
  installation-check errors use the same projection; credential persistence and
  source selection are unchanged.
- Existing handled activity, granular, detail/HRV, recent-body/night, optional
  provider/training and refresh-stage failure sinks emit contextual diagnostics
  only within a hosted CLI invocation. Private reports, stored errors and return
  schemas retain their original values. A diagnostic does not make a partial
  result complete or authorize another request.
  Activity backfill records the original import/pagination diagnosis before its
  existing checkpointed stop; Personal records typed transport failures before
  its fixed source-error translation. Their manifests, latches and retries are
  unchanged.
- The pinned SDK logger projection remains scoped and restores existing filters;
  attached typed exceptions gain the same safe details without copying text.
- Worker-owned console sinks use fixed component/operation/site and typed GitHub
  HTTP/read reasons. R2 summary head/read/missing/schema and payload limits remain
  distinguishable. Generic platform R2 errors remain unknown. MCP/HTTP schemas,
  Registered application tool callbacks emit the shared diagnosis before the SDK
  converts an exception into an error result. Public responses, status codes and
  typed application guidance are unchanged. Existing fixed-only
  context claim/completion and invalid-history messages remain fixed safe sites.

Sanitized MCP tool failures retain the SDK's recognized `resultType: "complete"`
wire discriminator when present, so protocol `2026-07-28` clients can decode
the same safe guidance and generic failures. Other supplied result fields remain
projected away; legacy protocol responses retain their existing shape. The
runtime privacy regression uses the installed v2 client against the actual
handler with synthetic auth and storage boundaries.

Activity refresh also emits a bounded `by_status` aggregate in its hosted report.
An unsupported activity or provider capability is explicit; checked activity
counts alone do not prove that files or Coach inputs were delivered. The legacy
single-activity endpoint nests native local/UTC start times in `summaryDTO`;
refresh uses those source fields when the corresponding top-level time is
missing, preserving native local dates and existing top-level precedence.
Explicit activity-ID mismatches stop before importing another activity's files.
These checks and outcomes can diagnose new failures. They do not establish the
cause of an older missing activity, and a capped blocked-activity list cannot
prove that an omitted activity was unblocked.

Bounded R2 snapshots consume fragmented SDK bodies through actual EOF, retain
object encoding metadata and preserve the primary read rejection when closing
the body also fails. A cleanup failure after a successful read remains a failure.
This does not relax per-object encoding, body, resource or conditional-write
checks.

Normal authorized fitness responses and intentional local product/report output
remain private data; this is not a scrubber for those payloads. Test/build tools,
import-time/dependency/interpreter failures and third-party/platform-generated
telemetry are outside the application error boundary. Historical logs are not
rewritten. In particular, the older generic Coach failure cannot be diagnosed
retroactively from this change.

No logger backend, diagnostic R2 write, dependency, scheduler, quota, retry,
provider fallback, login, lock clearing or data operation is added. Do not retry a
failed/unknown operation because its diagnostic is more specific. Follow the
existing exact source/lease/native/conditional-write/readback procedure.
