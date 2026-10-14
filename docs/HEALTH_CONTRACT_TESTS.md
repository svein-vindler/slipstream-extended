# Python-to-MCP health contract tests

Slipstream imports Garmin sleep and HRV into private R2, builds compact monthly
indexes, and serves those objects through an authenticated MCP Worker. The
contract suite tests this complete path with invented data, without Garmin,
GitHub or a production R2 bucket.

## Run locally

Use Python 3.12 or newer with the repository's locked dependencies installed,
and install the Worker's npm dependencies:

```bash
pip install -r requirements-dev.txt
pytest tests/test_health_contract.py
cd worker
npm ci
npm run test:runtime
```

`npm test` includes the suite. Before runtime tests, npm invokes
`scripts/build_health_contract.py` using `python` on PATH. If the virtual
environment is not activated, set `SLIPSTREAM_TEST_PYTHON` to its interpreter:

```powershell
$env:SLIPSTREAM_TEST_PYTHON = (Resolve-Path ../.venv/Scripts/python.exe).Path
npm run test:runtime
```

The Worker CI job also installs Python and the locked runtime dependencies.
Generated snapshots live in `worker/test-runtime/generated/`, are ignored by
Git, and are regenerated for every run. Do not replace them with saved personal
data or hand-written Worker-only versions of the pipeline output.

## What the suite verifies

`tests/fixtures/health-contract.json` contains shared synthetic Garmin responses
and independent expected metrics and local times. Python runs the actual recent
health import, normalizers, guarded R2 store, monthly index builder, health CSV
writer and summary exporter against an in-memory S3 adapter. The exporter saves
the exact gzip bytes, content metadata and object revisions for each state.

The Worker tests put those bytes into the Cloudflare test runtime's real local
R2 binding. Requests enter the exported Worker handler, authenticate with a
signed synthetic Access assertion, initialize MCP and call the registered
tools. Only the synthetic Access certificate endpoint is mocked. Unexpected
network requests fail the test. Responses are checked against their registered
output schemas and must use `no-store` and omit provider-only fields.

Coverage includes:

- ordinary nights, Garmin local time during travel, DST fallback and a night
  crossing New Year;
- unchanged refreshes that recheck the source while avoiding canonical/index
  downloads and writes; Worker summary reads use verified index revisions;
- warm MCP requests after changed source data and interrupted index writes,
  including an unchanged successful source-check receipt until recovery;
- missing or corrupt indexes, canonical read-through, and subsequent pipeline
  repair without contacting Garmin;
- wrong-date and incomplete responses preserving good stored data and receipts;
- the daily summary cache reloading when the Python-exported R2 ETag changes.

Ordinary history reads remain read-only. They report stale indexes and return
canonical data where needed; the pipeline performs persisted index repair.
Changing a derived index alone does not claim a new successful Garmin check.

These tests verify storage and reader compatibility in the local Workers
runtime. They complement private installation validation; they do not exercise
Garmin's live API, Cloudflare's edge OAuth flow or a production deployment.

`worker/test-runtime/d1-output-contract.test.ts` verifies structural projection
for `hrv_curve`, `sleep_detail`, `strength_session` and `endurance_session` through
authenticated modern JSON and legacy SSE. Known fields are projected before
both text and structured content are built. Unsupported stored object, row or
known scalar types return the existing fixed tool error rather than fabricated
zeroes, missing-data results or partial success. Optional legacy fields remain
optional; this checks supported shapes, not source completeness. Existing
storage/decode limits, sampling and request budgets are unchanged.

These detail contracts preserve numeric strings without coercion, nulls, zero,
units, source clocks, calendar provenance, dynamic sleep-score names and all
strength metric names. Opaque nested `performance_metrics` extensions remain
open for compatibility; already-stringified producer qualifiers remain strings.
Separate tests explicitly preserve both deferred cases. This is not complete
hardening of arbitrary extension leaves, history/index calculations, coach
payloads or upstream producer normalization. It adds no partial-status protocol.

The existing training-context fixture builder also emits strength dimension rows
from the actual Python normalizer. The native tests load those bytes as raw JSON
to preserve literal data keys such as `__proto__`, check both dimension maps and
totals, and require typed leaf projection without changing object prototypes.

The suite checks both output channels, concurrent valid/invalid HRV requests,
summary-cache isolation, unchanged fixture bytes, no request writes and no
non-JWKS external requests. Its JSON-report metadata records synthetic R2 call
counts, fixture body bytes, response bytes and request time for comparable
base/candidate resource cases. These are local measurements, not edge CPU,
heap, provider latency, billed usage or a guarantee of free operation.

`worker/test-runtime/tool-catalog.test.ts` fingerprints the complete authenticated
tool catalog with coach writes enabled and disabled. Its pre-refactor snapshots
cover tool order, descriptions, input/output schemas and safety annotations,
so moving domain registrations cannot silently change client contracts.

The same generation command also runs `scripts/build_weight_contract.py`.
`worker/test-runtime/weight-index.test.ts` loads exact Python canonical/index
bytes into real local R2 and verifies all weighings, alternative morning windows,
travel, DST, New Year, changed/deleted sources, repeated reads and missing/corrupt
indexes. These ordinary reads are checked to perform no writes.

`worker/test-runtime/r2-storage.test.ts` also exercises the shared storage
module directly against local R2. It counts HEAD/GET operations across separate
request readers, checks changed activity summaries and explicit cache clearing,
and verifies deletion, malformed replacements and metadata failures do not
return old cached data. Additional cases cover ordered object aliases,
missing objects, exact stored/decoded size boundaries, gzip decoding and invalid
JSON/compressed objects. These checks preserve the reader's existing errors and
limits when moving storage logic between modules.

## Verified HRV absence and retry checkpoints

HRV imports accept the existing readings aliases and recognized summary fields.
A confirmed empty response requires an explicit empty readings list and a
matching calendar declaration. Unknown envelopes, malformed/partial readings or
conflicting dates are rejected before canonical writes. Summary-only values
remain readable; `available` continues to describe detailed readings, while
the index can report available summary data without a curve.

Historical backfill records invalid responses as date failures under the
existing three-attempt limit and explicit `--retry-failures` policy, rather
than creating an unavailable object and marking the date complete. Auth,
transport, policy and storage-budget handling are unchanged. Recent/targeted
imports preserve good canonical bytes and successful checks; invalid responses
are not confirmed successful Garmin checks.

Existing historical objects and unavailable markers are not reclassified or
automatically fetched again. Earlier unknown-schema responses may therefore
still have legacy unavailable markers. Any historical inspection/revalidation
requires separate authorization; this fix performs no migration or backfill.
Synthetic caller/checkpoint regressions are in `tests/test_hrv_contracts.py`.
