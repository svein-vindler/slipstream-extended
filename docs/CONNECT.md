# Connect an OAuth-capable MCP client

Slipstream exposes one canonical endpoint:

```text
https://<worker>.<workers-subdomain>.workers.dev/mcp
```

Cloudflare Access Managed OAuth must already be configured as described in
[INSTALL.md](INSTALL.md). There is no secret-path or unauthenticated connector.

## ChatGPT

The exact menu labels may change. On the ChatGPT web app:

1. Open **Settings → Apps & Connectors** and enable Developer mode if required.
2. Create a new app/connector and name it `Slipstream`.
3. Paste the canonical `/mcp` URL as the MCP server URL.
4. Choose **OAuth** authentication.
5. Create/connect the app. Cloudflare opens an authorization page.
6. Sign in with the exact email allowed by the Access policy.
7. Enable Slipstream in the conversation where you want to use it.

If OAuth registration is rejected, confirm these redirect URIs exist in the
Access application's Managed OAuth settings:

```text
https://chatgpt.com/connector_platform_oauth_redirect
https://chatgpt.com/connector/oauth/*
```

## Other clients

The client must support remote MCP over Streamable HTTP, OAuth discovery,
dynamic client registration and PKCE. Add only the redirect URI shown by that
client to Managed OAuth. Do not enable localhost or loopback redirects unless a
trusted local client genuinely requires them.

## Verify the connection

Ask the assistant to call `data_status`, `health_status`, `hrv_curve` for a
recent date, and `strength_session` or `endurance_session` for a known activity.
Successful status responses report `connected: true` and `storage: "r2"`.
After creating a coach profile, also verify `coach_profile` and `coach_input`.
The write tools require explicit user intent and cannot address arbitrary R2
keys.

For longer health analysis, ask the assistant to use `hrv_history` or
`sleep_history`. Ranges longer than 31 days automatically return weekly
summaries, so six months fits in one tool call. Ask for a specific week or one
night afterward when you need full HRV readings or the sleep-stage timeline.
For Friday/Saturday night comparisons, use `sleep_history`'s
`by_night_of_weekday`, not the existing wake-date `date`. Configure
`HEALTH_TIMEZONE` as a fallback for older records. Newly imported Garmin
nights use their recorded local times, including during travel.

For weight trends, use `weight_history` in consecutive chunks of at most 31
days. It picks the first stored individual weighing in a local morning window
and reports missing/uncertain days and the time source. `daily_health.weight_kg`
is normally Garmin's latest daily value, not a daily mean or a standardized
morning measurement. See [WEIGHT_HISTORY.md](WEIGHT_HISTORY.md).

For a refresh test, ask it to refresh today's Garmin data and wait until the
run completes. This requires the optional GitHub Worker secrets described in
[INSTALL.md](INSTALL.md#11-optional-allow-chat-triggered-refresh).
For coaching right after training, ask: **"Sync today's latest Garmin workout, wait
until its detailed files and Coach Input are ready, then analyze that exact
activity."** The `sync_latest_activity` tool fetches a recent Garmin summary,
then imports only the selected activity's FIT/TCX and Coach Input. It checks
the same activity ID throughout and returns a specific incomplete status if
Garmin has not made the files available or Coach Input cannot be generated.
If you say a new workout is expected but Garmin still shows only an older,
already complete workout, it returns `no_new_activity` instead of calling the
older workout the new one. When the date is known, the client passes
`expected_date` and a different Garmin-local date returns
`expected_activity_missing` even if the older workout is complete.
It may be requested at most once every five minutes and twelve times per UTC
day. Use `refresh_status` with the returned run ID until the job finishes;
only then read `coach_input` and `endurance_session` for that ID. Simply asking
about data already stored in Slipstream should remain a read-only request.
If a recent workout is missing, say so explicitly: `refresh_today` can use a
five-minute minimum interval for that case instead of the usual 30-minute
cooldown. Its result distinguishes a completed summary refresh from newly
imported activity files and running coach input. If no new workout appears,
Garmin has not returned one during that refresh; the assistant should not claim
that the requested workout is ready.

## Reauthentication and removal

The recommended configuration uses a one-week grant and short-lived access
tokens. Renewal is normally automatic; Cloudflare asks you to authorize again
when the grant expires or the policy changes.

To remove access, delete the connector and remove the email from the Access
Allow policy (or delete the application). No Worker secret URL needs rotation.
