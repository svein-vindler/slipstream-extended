# AI workflows

Copy a prompt below into your MCP client. These are user instructions, not
server security controls. They require a client that exposes the registered
Slipstream tools; actual client acceptance must be checked separately. No native
MCP prompts or server instructions are required or added.

Use the [generated tool catalog](TOOL_CATALOG.md) for current names, schemas,
permissions and limits, and [prompt ideas](PROMPTS.md) for more questions.
JSON examples here are synthetic schema examples, not calls to execute blindly.
Replace dates, activity IDs and timezone with the user's confirmed local context.

## Shared rules

- Historical analysis reads stored data only. Missing data does not authorize
  Garmin sync, backfill, profile/context writes or a new schedule.
- An explicit request for fresh activity/night data uses the targeted R2-first
  flow. A ready R2 package with a recent successful source check needs no job;
  derived data can be repaired from existing R2 source. Compatible jobs share IDs.
- Pin the Garmin-local date and activity ID, or the local wake-date. Preserve the
  same ID through detail, user context and Coach Input. On travel, use Garmin
  local timestamps; do not assume the home timezone or shift dates manually.
- Read `sync_status.data_state`, source-check time, completeness, freshness,
  missing components and next action separately from job success. Claim a complete
  fresh package only at `ready`. Legacy `data_ready` alone is insufficient.
- Follow `refresh_status` with the same returned `request_id` while
  `should_continue_polling` is true; obey `poll_after_seconds`. When false, stop
  even if the job is running. Report `retry_after_seconds`/`Retry-After`, missing
  components, source-pending/blocked state or 429. Never dispatch another job to
  extend polling. A later user request may check the same ID. Do not promise later
  automatic follow-up without a separately agreed mechanism.
- A client may expose HTTP 429 without forwarding the `Retry-After` header to
  the model. Use a cooldown explicitly stated in the response body when the
  header is unavailable. If neither the body nor header states a cooldown,
  report it as unknown. Do not invent a delay or retry automatically.
- Report coverage and unavailable/invalid components. An empty Garmin result is
  source-pending only when a successful source check establishes that result;
  auth errors, missing indexes and unknown schemas are not proof of no data.
  Missing values are unknown, not zero. Device estimates and derived statistics
  keep their units, time and source; no medical causal claims.
- Read-only results may lack pipeline diagnostics. Old timings may be null or
  omitted: label them unknown, not failed or zero. Never sync to obtain timings.
  R2 inventory and LIST timings overlap and must not be added.
- Profile/context writes require a preview and the user's explicit confirmation
  to save to private R2. If write tools are absent, retain the draft in chat.
  Do not infer RPE, thresholds or zones. New versions preserve old analyses.

## 1. Coach setup

> Using Slipstream, help me set up a running coach profile. First use `coach_profile` to read the
> existing profile for the effective date I choose. Ask me for the effective
> date, ordered non-overlapping heart-rate zones, optional LT1/LT2 references and
> interval BPM thresholds. Leave unknown references unset; do not replace my
> thresholds with Garmin estimates. Show the entire proposed profile and explain
> that saving writes an immutable version to private R2. Wait for my explicit
> confirmation before saving with `add_coach_profile`. If writing is unavailable, give me the draft.
> Changed zones must not overwrite historical analyses.

Read with `coach_profile`. Save with `add_coach_profile` only after confirmation
and when it is registered. No sync is needed to prepare the profile.
These example zone values demonstrate schema validity only; they are not defaults.

```json
[
  {"tool":"coach_profile","arguments":{"date":"2026-09-20"}},
  {"tool":"add_coach_profile","arguments":{"name":"Synthetic example","effective_from":"2026-09-20","zones":[{"label":"Example zone","min_bpm":100,"max_bpm":140}],"references":{"interval_thresholds_bpm":[]}}}
]
```

## 2. Freshest workout, with optional context

> Using Slipstream, use `sync_latest_activity` to fetch my freshest workout for [Garmin-local date]. This
> authorizes the targeted fresh-data flow to check R2 first and, if necessary,
> sync the recent workout from Garmin to private R2. If several sessions match,
> ask which activity ID I mean. Keep that exact ID for details and Coach Input.
> Follow `refresh_status` with the same returned `request_id` only while
> `should_continue_polling` is true, respecting `poll_after_seconds`. When false,
> stop even if the job is running; report `retry_after_seconds` or 429/Retry-After.
> Never start another job to extend polling or promise automatic later follow-up.
> Missing/null diagnostics are unknown and do not justify another sync.
> Check actual sync_status.data_state,
> source_checked_at and missing_components before claiming it is ready.
> Report any missing profile, files or Coach Input instead of treating a successful
> job as a complete dataset. If needed, distinguish the available summary from
> the incomplete analysis. Optional context: RPE [value or omit], conditions
> [text or omit], note [text or omit]. Use only what I supplied; preview this
> context and wait for my confirmation before saving it with `add_activity_context` to R2. Explain whether
> a new Coach Input revision is still pending after saving.

Start with `sync_latest_activity`, with `expected_date` and an exact `activity_id`
when known. The returned coherent package is the first analysis source. If extra
reads are needed, use `endurance_session` and `coach_input` for that same running
ID; use `strength_session` for strength. An R2 context write does not immediately
regenerate Coach Input: do not present an older revision as including the new note.
An expressly authorized fresh flow can subsequently repair from R2; otherwise
report the pending revision without starting a job.

```json
[
  {"tool":"sync_latest_activity","arguments":{"expected_date":"2026-09-20","activity_id":"garmin-1001","new_activity_expected":true}},
  {"tool":"refresh_status","arguments":{"request_id":"00000000-0000-4000-8000-000000000001"}},
  {"tool":"endurance_session","arguments":{"activity_id":"garmin-1001"}},
  {"tool":"coach_input","arguments":{"activity_id":"garmin-1001"}},
  {"tool":"add_activity_context","arguments":{"activity_id":"garmin-1001","rpe":3,"conditions":"Synthetic dry conditions","note":"Synthetic example note"}}
]
```

For **an already stored workout**, use this reading variant:

> Using Slipstream, analyze my stored workout on [local date / activity ID].
> Read stored summaries to identify the correct activity, then its appropriate
> detail and Coach Input using the same ID. Show local-date provenance and missing
> components. Do not start Garmin sync or write context/profile data.

```json
{"tool":"list_activities","arguments":{"start_date":"2026-09-20","end_date":"2026-09-20","limit":20,"sort":"date_desc"}}
```

## 3. Last night

> Using Slipstream, use `sync_latest_night` to fetch sleep and associated HRV for my last night, waking on
> [Garmin-local wake-date]. I authorize the targeted R2-first fresh-night flow
> and any necessary bounded import to private R2. Use that wake-date; describe
> night_of and the returned local sleep times separately. Keep Garmin-local
> provenance and any IANA/DST fallback visible, including travel or missing
> context. Follow `refresh_status` with the same returned `request_id` only while
> `should_continue_polling` is true and respect `poll_after_seconds`. When false,
> stop even if the job is running; report `retry_after_seconds` or 429/Retry-After.
> Never start a replacement job or promise automatic later follow-up.
> Missing/null diagnostics are unknown and do not justify another sync.
> Report sync_status.data_state,
> source_checked_at and missing sleep/HRV components. A completed job is not
> evidence of complete sleep or detailed HRV. If Garmin is still finalizing the
> night, explain the partial data and retry guidance, then stop.

Use `sync_latest_night` and its package first. Only drill into `sleep_detail` or
`hrv_curve` for the same wake-date when needed. For a stored-night question,
read those tools directly and do not sync. A nightly HRV summary can exist while
the detailed curve is missing.

```json
[
  {"tool":"sync_latest_night","arguments":{"wake_date":"2026-09-20"}},
  {"tool":"sleep_detail","arguments":{"date":"2026-09-20"}},
  {"tool":"hrv_curve","arguments":{"date":"2026-09-20"}}
]
```

## 4. Weekly review

> Using Slipstream, review my stored training, sleep and HRV for [inclusive
> local dates] and compare with the preceding week. This is read-only historical
> analysis: no Garmin sync or R2 writes. Start with activity_stats for the two
> bounded periods and compact sleep_history/hrv_history summaries. Use daily
> rows only where a particular night needs explanation. Use list_activities and
> appropriate session detail only for selected sessions. Report coverage,
> available versus missing data, and measured versus estimated/derived values.
> Use wake_date for joining HRV/sleep and night_of for sleep weekday comparisons.
> Give three observations and one practical training focus; do not infer medical
> causes, missing RPE or user thresholds.

There is no `week` group_by in `activity_stats`: call it once for each week.
Use `granularity=weekly` for compact history even over a short period. Week
summaries group wake-dates; use the returned night-of weekday fields when comparing
Friday/Saturday nights. Coverage follows the stored indexes, not a fixed 30 days.

```json
[
  {"tool":"activity_stats","arguments":{"start_date":"2026-09-14","end_date":"2026-09-20","group_by":"sport"}},
  {"tool":"activity_stats","arguments":{"start_date":"2026-09-07","end_date":"2026-09-13","group_by":"sport"}},
  {"tool":"sleep_history","arguments":{"start_date":"2026-09-07","end_date":"2026-09-20","granularity":"weekly","detail_level":"summary"}},
  {"tool":"hrv_history","arguments":{"start_date":"2026-09-07","end_date":"2026-09-20","granularity":"weekly","detail_level":"summary"}}
]
```

## 5. Long-term sleep and HRV

> Using Slipstream, analyze stored sleep and nightly HRV over [inclusive dates,
> at most 366 days]. Do not sync or backfill. Start with weekly summary history
> for both streams, including coverage, per-day/index consistency and missing
> local context. Separate Garmin nightly metrics from Slipstream-derived HRV
> statistics. Inspect selected unusual weeks with daily summaries (at most
> 31 days per call), and request full readings/stages only for specific periods
> of at most seven days. For lag comparisons use daily rows joined on the
> returned wake_date/night_of; do not manually offset dates. Treat associations
> as observations, not medical causes. State limits before drawing conclusions.

Auto gives daily summaries through 31 days and weekly summaries for 32–366 days.
Use explicit weekly summaries for the first pass. Daily lag analysis can use
overlapping windows with deduplication; full detail must be daily/auto.
Older weekly index rows need not have verified raw detail; read `index_consistency`.

```json
[
  {"tool":"sleep_history","arguments":{"start_date":"2026-04-01","end_date":"2026-09-20","granularity":"weekly","detail_level":"summary"}},
  {"tool":"hrv_history","arguments":{"start_date":"2026-04-01","end_date":"2026-09-20","granularity":"weekly","detail_level":"summary"}},
  {"tool":"hrv_history","arguments":{"start_date":"2026-09-14","end_date":"2026-09-20","granularity":"daily","detail_level":"full"}}
]
```

## 6. Standardized morning weight and trend

> Using Slipstream, analyze my stored morning-weight trend over [inclusive
> local dates]. Do not sync or write data. Use weight_history in non-overlapping
> windows of at most 31 days. Use the first real weighing in the confirmed local
> morning window [04:00,12:00) unless I choose another window. Distinguish actual
> weighings, Garmin daily averages and selected morning measurements. Never
> replace a missing morning weighing with daily_health.weight_kg, a daily average
> or a later weighing. Report missing-day reasons, selected-day counts, local-time
> provenance and timezone fallback. Show 7-, 14- and 28-calendar-day mean trends
> only from observed selected mornings, with observed/requested counts for each
> window; do not interpolate missing days. Inspect body_composition for specific
> days only if necessary. Explain how sparse coverage limits the conclusion.

`weight_history` returns observations and selection provenance; rolling means are
an analysis over those selected values, not a Garmin measurement or a precomputed
server trend. Declare the chosen window and weighting. Garmin-local timestamps
take priority; explicit timezone affects UTC-only fallback, not recorded local time.

```json
[
  {"tool":"weight_history","arguments":{"start_date":"2026-09-01","end_date":"2026-09-20","morning_start":"04:00","morning_end":"12:00","timezone":"Europe/Oslo"}},
  {"tool":"body_composition","arguments":{"date":"2026-09-20"}}
]
```

## Verification and client acceptance

Existing CI checks real registrations, catalog determinism and these JSON examples.
Synthetic workflow scenarios check selected tool paths and bounded continuation
for ready R2, incomplete activity/night, source-pending, missing profile, 429 and
missing morning weight. Existing runtime tests exercise actual fresh-data/storage
behavior. These checks do not demonstrate what an AI client will select in chat.

On 2026-10-07, an authorized local client test ran these six prompt families in
Codex CLI 0.162.0-alpha.2 with `gpt-6.1-sol`, reasoning effort `xhigh`, through
12 synthetic scenarios. Each run was ephemeral and isolated from user MCP/plugin
configuration. The loopback-only mock's complete `tools/list` matched the actual
Worker contracts exactly in all four write/refresh modes (19/23/21/25 tools).
Successful mock responses were validated against the registered output schemas.
Only the synthetic server was pre-approved; user configuration was not changed.

Observed behavior covered profile/context previews without unconfirmed writes,
ready R2 without dispatch, activity-ID and wake-date continuity, stopping while a
job still runs, missing profile/HRV, source-pending after job success, historical
read-only bounds, missing morning measurements and refresh-unavailable fallback.
The morning-weight result matched independent 7/14/28-day calculations and
observed counts. For inconsistent synthetic weekly/daily/index data, the client
reported the discrepancies rather than claiming a reliable long-term trend.
Some optional historical detail reads deliberately had no fixture; this run did
not validate a complete, consistent long-term dataset or all stored session detail.

In the initial pass, eleven scenarios met their acceptance criteria. The HTTP-429 scenario met the
stop/no-retry criterion but had a client visibility limitation: the mock used the
actual Worker's plain `Too many requests` body and `Retry-After: 60` header, while
Codex forwarded only the status/body in its tool error. The model reported an
unknown cooldown. A prompt cannot recover a header the client does not expose.

An authorized local follow-up now includes the same 60-second cooldown in the
plain error body: `Too many requests. Retry after 60 seconds.` The 429 status,
`Retry-After: 60` header, security headers and rate limiter are preserved. Runtime
tests block selected sync/write tools before any storage or dispatch access.
The same Codex client and unchanged night prompt were rerun against the response
captured from the changed Worker in an isolated runtime. The client reported
60 seconds and stopped after one call, with no polling, write or replacement job.
Together with the initial eleven passing scenarios, this covers the twelve
bounded acceptance criteria; the other eleven were not rerun for this body-only fix.

These are observations from one synthetic pass per final scenario, not a guarantee
for other models or clients. Post-confirmation persistence, production OAuth,
live Garmin/R2, desktop UI and ChatGPT web were not tested. A live acceptance test
remains a separate authorized step. Native MCP prompts/server instructions also
remain a separate change after client support is verified.
