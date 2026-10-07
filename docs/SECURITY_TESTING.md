# Security verification checklist

Run this checklist after authentication, Worker or MCP-tool changes. Use only a
test installation with synthetic data for adversarial cases.

## Automated gates

CI must pass all of the following:

- Python lint and tests
- Python dependency audit against the hash-locked runtime requirements
- npm production dependency audit
- TypeScript typecheck against the current Wrangler configuration
- Node unit tests
- Workers-runtime tests for Durable Object persistence and concurrent quotas
- public release tree check for private artifacts, legacy authentication and
  installation-specific Worker configuration

## Authentication and transport

1. `/mcp` without a valid Access assertion returns 401.
2. A request to any other path returns 404.
3. A valid assertion for another Access audience is rejected.
4. A request whose hostname differs from `MCP_HOSTNAME` returns 421.
5. Removing any required authentication setting fails closed with 503.
6. Repeated authenticated requests reach the per-identity rate limit; another
   authenticated identity retains its own allowance.

## Tool exposure and authorization

1. With `MCP_WRITES_ENABLED` absent, the tool list contains no coach-profile or
   activity-context write tools.
2. With the secret set to the exact value `true`, only immutable coach profiles
   and append-only activity context become writable.
3. Without the optional GitHub secrets, refresh tools are absent.
4. `refresh_status` rejects a GitHub run ID from any workflow other than the
   configured refresh workflow.
5. Concurrent writes cannot exceed the exact UTC-day budget, and cooldowns are
   shared across sessions for the same Access identity.

## Input and prompt-injection cases

Store synthetic activity names and notes containing text such as “ignore prior
instructions”, fake tool calls, URLs, shell fragments and requests for secrets.
Confirm they are returned only as fitness data and never alter tool selection or
server behavior. Also confirm:

- arbitrary R2 keys cannot be supplied to any tool
- activity IDs, dates, limits, notes, zones and repository names reject values
  outside their explicit schemas
- no tool returns raw FIT/TCX files, GPS coordinates, credentials or Access
  assertions
- oversized request bodies and oversized/decompression-bomb R2 objects fail
  before parsing
- error responses contain no tokens, email addresses or object contents

## Garmin read-only boundary

Run `pytest -q tests/test_garmin_readonly.py` without real credentials. The suite
uses the pinned SDK with a fake HTTP session and verifies that:

- every pipeline read still works through the shared guarded login;
- upload, edit, delete and unknown SDK operations are not exposed;
- non-GET requests, unknown endpoints, bodies, method overrides, traversal and
  redirects are rejected before an authenticated data request is sent;
- diagnostics decorators cannot remove the transport policy;
- new SDK transport layouts fail closed rather than dropping the guard.

Do not attempt destructive tests against a real Garmin account. A policy change
must be reviewed before merge; no MCP input or environment flag may disable it.

The Workers-runtime hardening tests additionally pass SQL-looking lease keys
and job context through the coordinator. Values must remain bound parameters,
never change query structure, delete tables or affect another stored job.

## Refresh consistency

After a successful on-demand refresh, verify the newest date with
`daily_health`, `hrv_curve` and `sleep_detail`, and verify `coach_input` when the
newest activity is a run with an applicable profile and usable telemetry. The
workflow must not report `data_ready: true` until summary upload plus recent HRV,
sleep, activity-artifact and coach processing have finished. Re-reading from a
warm Worker isolate must observe the new R2 ETag.

## Operational review

The Worker pins MCP SDK 1.31.0 and the test client 2.2.0 for
[GHSA-6qxp-vccf-f47h](https://github.com/modelcontextprotocol/typescript-sdk/security/advisories/GHSA-6qxp-vccf-f47h).
`agents@0.24.0` still declares exact older MCP peer versions. A scoped npm
override points those two peers to the root's patched pins; it does not upgrade
Agents or the MCP server. Validate with a clean `npm ci`, `npm audit`, typecheck,
unit tests, Workers-runtime tests and the private authenticated canary. Remove
the override when a tested Agents release supports the patched peers directly.
Slipstream serves MCP and does not implement an outbound OAuth client or store
SDK OAuth-client credentials. Adding such a client requires the advisory's
issuer-binding/provider migration checks as well as patched packages.

`worker/test/mcp-oauth-security.test.ts` exercises the installed SDK v1 and
client v2 with synthetic issuer-bound credentials and a fake fetch function.
Both must reject a changed issuer before preparing or sending a token request,
while a request to the original issuer still succeeds. This does not validate
external MCP clients or migrate credentials stored by other applications.

- Review Access logs, Workers errors/traces, R2 operation counts and GitHub
  Actions runs.
- Confirm Workers preview URLs are covered by the Access application or disabled.
- Confirm the R2 API token is restricted to the Slipstream bucket.
- Confirm the GitHub token is fine-grained, single-repository and Actions-only.
- Repeat the checklist before a public release and after major dependency bumps.
