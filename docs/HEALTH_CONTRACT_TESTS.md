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

`worker/test-runtime/tool-catalog.test.ts` fingerprints the complete authenticated
tool catalog with coach writes enabled and disabled. Its pre-refactor snapshots
cover tool order, descriptions, input/output schemas and safety annotations,
so moving domain registrations cannot silently change client contracts.

`worker/test-runtime/r2-storage.test.ts` also exercises the shared storage
module directly against local R2. It counts HEAD/GET operations across separate
request readers, checks changed activity summaries and explicit cache clearing,
and verifies deletion, malformed replacements and metadata failures do not
return old cached data. Additional cases cover ordered object aliases,
missing objects, exact stored/decoded size boundaries, gzip decoding and invalid
JSON/compressed objects. These checks preserve the reader's existing errors and
limits when moving storage logic between modules.
