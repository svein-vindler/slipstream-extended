# Troubleshooting

Start with the failing layer: GitHub Actions fetches data, R2 stores it,
Cloudflare Access authenticates, and the Worker serves MCP.

## Local setup

**`python` or `pytest` is missing.** Activate `.venv` or use its Python directly.
On Windows: `.venv\Scripts\python.exe`; on macOS/Linux: `.venv/bin/python`.

**Wrangler cannot spawn or write locally.** Run from a normal terminal with the
repository writable, then retry `npm run typecheck`. Corporate endpoint
protection can also block child processes.

## Garmin and GitHub Actions

**Garmin requests MFA.** This is expected during session creation. Enter the
current code; only the resulting session is stored.

**Garmin returns 429.** Stop retrying. Wait for the throttle to clear, then run
one job. All workflows share `garmin-sync` concurrency to prevent overlap.

**A workflow cannot find R2.** Verify all four GitHub secrets exist:
`GARMINTOKENS`, `CLOUDFLARE_ACCOUNT_ID`, `R2_ACCESS_KEY_ID` and
`R2_SECRET_ACCESS_KEY`. Confirm the R2 token is scoped to `slipstream-data`.

**The summary refresh succeeds but detail is missing.** Summary and detailed
exports use separate jobs. Check the relevant scheduled activity, HRV or health
detail workflow and its progress output.

**A backfill says complete and does no work.** That is expected. It has
quiesced. HRV/sleep/body plans reactivate when the summary contains a new
eligible date.

**A corrected Garmin activity still looks old.** Force exactly one activity:

```bash
gh workflow run activity-refresh.yml -f activity_id=24431147581 -f force=true
```

The `garmin-` prefix is optional. Force mode intentionally requires one ID.

## R2 and cost controls

**R2 creation returns code 10042.** Enable R2 in the Cloudflare dashboard first.
The free allowance may still require adding a payment method.

**A job stops with an R2 budget message.** This is a safety stop, not data
corruption. Inspect R2 Usage and the bucket inventory before increasing any
`R2_MAX_*` value.

**Both `.json` and `.json.gz` exist.** Current writers/readers prefer canonical
objects defined by the schema. Old duplicates can be deleted only after
confirming no manifest references them. Never bulk-delete by prefix without a
verified inventory.

## Cloudflare Access and OAuth

**The MCP endpoint returns `503 OAuth is not configured`.** Set
`ACCESS_TEAM_DOMAIN`, `ACCESS_AUD` and `MCP_HOSTNAME` as Worker secrets and
redeploy. `MCP_HOSTNAME` contains only the hostname, without scheme or path.

**The MCP endpoint returns `421 Misdirected request`.** The requested hostname
does not exactly match `MCP_HOSTNAME`. Update the secret if the production
Worker hostname changed; do not permit arbitrary preview hostnames.

**Cloudflare login succeeds but MCP returns 401.** The team domain or AUD does
not match the Access application, or the assertion expired. Copy the values
again and redeploy.

**Access denies the login.** The signing-in email must exactly match an Include
rule in the Allow policy. Confirm the policy is attached to the application.

**OAuth registration rejects the callback.** Add the exact client callback to
Managed OAuth. ChatGPT uses:

```text
https://chatgpt.com/connector_platform_oauth_redirect
https://chatgpt.com/connector/oauth/*
```

**The client tries localhost or 127.0.0.1.** Leave those Managed OAuth toggles
off for hosted ChatGPT. Enable them only for a trusted local MCP client that
requires a loopback callback.

**The Worker base URL is blocked too.** Correct: the recommended Access
application protects the whole Worker so rejected traffic does not reach code.

## MCP and data

**The connector is added but ignored.** Enable Slipstream for that conversation
and ask explicitly to use it. Test `data_status` first.

**`data_status` or `health_status` is disconnected.** Confirm
`summary/manifest.json` exists in `slipstream-data`, then check the latest
`refresh.yml` run and the Worker's `SLIPSTREAM_DATA` binding.

**Data is a few minutes stale after refresh.** Summaries use a short per-isolate
cache. Successful chat-triggered refresh clears the current isolate's cache;
otherwise allow up to roughly five minutes.

**`refresh_today` says it is not configured.** Add both optional Worker secrets:
`GITHUB_REPOSITORY` (`owner/repository`) and `GITHUB_ACTIONS_TOKEN` (fine-grained,
one repository, Actions read/write), then deploy.

**Refresh starts but returns before data is ready.** The client must honor
`should_continue_polling` and call `refresh_status`. Current Slipstream schemas
make this explicit; reconnect the client if it cached an older tool definition.

**An endurance session has no TCX analysis.** Confirm the activity is an eligible
endurance type and the detailed activity workflow stored `activity.tcx` plus the
normalized endurance object. Raw TCX is never returned directly over MCP.

## Diagnostics

Run local checks:

```bash
python -m pytest -q
cd worker
npm run typecheck
npm test
```

Inspect a GitHub run with `gh run view <id> --log`. For short-lived Worker
diagnosis, use `npx wrangler tail`, reproduce once, then stop tailing. Redact
tokens, email addresses, account IDs, Access assertions and fitness data before
sharing logs.
