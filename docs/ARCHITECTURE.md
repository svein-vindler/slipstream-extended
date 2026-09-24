# Architecture

Slipstream has three moving parts, designed for free-tier personal use and owned
by the installer. There is no shared backend.

```
┌─────────────────┐   schedule + on-demand    ┌──────────────────────────┐
│  GitHub Actions │ ◄───────────────────────► │  private Cloudflare R2   │
│  (compute only) │   restore, merge, upload  │  summaries + raw exports │
└────────┬────────┘                            └────────────┬─────────────┘
         │ python-garminconnect                             │ R2 binding
         ▼                                                  ▼
┌─────────────────┐                            ┌──────────────────────────┐
│  Garmin Connect │                            │  Cloudflare Worker       │
└─────────────────┘                            │  Access OAuth + /mcp     │
                                               └────────────┬─────────────┘
                                                            │ MCP (Streamable HTTP)
                                              ┌─────────────┴─────────────┐
                                              ▼                           ▼
                                          Claude                       ChatGPT
```

## 1. Fetch (`pipeline/`)

A scheduled GitHub Action runs `python -m pipeline.fetch`:

- `sources/garmin.py` logs into Garmin Connect using a **saved session token**
  (`GARMINTOKENS`) - no password is stored - and pulls the last N days of
  activities.
- `schema.py` normalizes each activity (sport families, units) into a small
  `Activity` dataclass.
- `summary_restore.py` restores the current activity and health summaries from
  R2 and verifies their SHA-256 checksums before a refresh.
- The writers merge new rows into those local working files.
- `summary_export.py` uploads the merged summaries and a dated recovery snapshot
  to R2. Generated datasets are not committed to Git.

The current R2 summaries are the source of truth. Their CSV columns are stable and self-describing
(`Activity Date`, `Activity Type`, `Distance`, `Moving Time`, `Average Heart
Rate`, …), all in metric, times in seconds, dates in UTC.

R2 also stores detailed HRV, sleep, body-composition, and activity exports.
Daily summary snapshots provide recovery points without putting private fitness
data in Git history.

Every six-hour summary refresh also advances up to three newly eligible sleep
and body-composition dates, refreshes the latest three eligible detail dates so
late Garmin corrections replace early snapshots, and rewrites the latest three
compact HRV curves. An on-demand MCP refresh then performs the optional recent
activity artifact check and R2-only coach generation in the same workflow. This
makes successful workflow completion the readiness boundary for summaries,
recent health detail, activity artifacts and any applicable coach input, without
re-downloading FIT/TCX files on every scheduled run.

### Free-tier guardrails

All R2 uploads pass through the same fail-closed budget check. Before the first
write in a job, the pipeline inventories the bucket and refuses further writes
if the projected result would exceed any configured limit. The hosted workflows
use conservative defaults: 5 GiB total storage, 100,000 objects, 250 writes per
run, and 512 MiB uploaded per run. These values can be lowered with the
`R2_MAX_*` environment variables.

The scheduled refresh and granular export also share one GitHub Actions
concurrency group. This serializes Garmin access and prevents two jobs from
writing to R2 at the same time. Granular runs additionally limit their date
windows and activity counts before signing in to Garmin.

Detailed activity history is filled through the manual `activity-backfill.yml`
workflow. Each run accepts at most one year, exports no more than 50 missing
strength or endurance activities, and skips canonical FIT, decoded JSON,
compressed TCX, and normalized endurance-analysis objects that already exist. A stable progress object per date
range makes runs resumable. Persistent Garmin errors are paused after three
attempts unless an operator explicitly requests a retry.

An otherwise valid TCX export with no timed trackpoints is stored as an
explicit `available: false` endurance-analysis object with reason
`no_timed_trackpoints`. This makes very short, abandoned or empty recordings
complete without inventing telemetry or retrying them forever. A previously
blocked activity plan rechecks its range progress, so repaired artifacts can
move the plan from `complete_with_blocked` to `complete` without rebuilding the
history.

The endurance-analysis schema has its own versioned progress plan. When that
schema changes, a new plan can derive the compact objects from the already stored
TCX files without downloading the original activities again.

The scheduled activity backfill creates a fixed plan from the earliest and
latest dates in the R2 activity summary, then advances the newest actionable
year four times each day. Activities within each year are also processed
newest first. One run has a shared 50-activity budget; when a year finishes
with capacity left, the same authenticated job immediately continues into the
next year instead of wasting the remainder. Its cutoff never moves after plan creation; new
activities belong to the later incremental granular sync. Once all planned
ranges are complete (or only explicitly blocked failures remain), scheduled
runs return without contacting Garmin or writing another R2 object.

Detailed overnight HRV history is filled independently by
`scheduled-hrv-backfill.yml`. It derives its target list from dates that already
have an HRV nightly average in the R2 daily-health summary, refreshes that list
on every run, processes up to 100 missing curves newest first four times per day, and
skips objects already stored under `health/hrv/`. A valid Garmin response with
no detailed readings is stored once as an explicit unavailable marker instead
of consuming three later retries. New nights therefore arrive
while older history continues filling in. Targeting only HRV dates avoids
spending Garmin calls and R2 objects on calendar dates without HRV. Failures pause after three attempts, while
authentication, rate-limit, and service errors stop the whole batch without
marking the date as bad. The job uses the same `garmin-sync` lock and R2
write/storage limits as every other Garmin job. After completion it only reads
the health summary; it contacts Garmin and resumes writes when a new eligible
HRV date appears.

Detailed sleep and body-composition history is filled by
`scheduled-health-detail-backfill.yml`. Separate resumable plans target only
dates that already contain sleep or weight in the daily-health summary. Sleep is
stored under `health/sleep/v1/` with its window, stages, score components, SpO2,
respiration and sleep stress. Body measurements are stored under
`health/body-composition/v1/`, preserving multiple measurements per day and the
available weight, BMI, fat, water, muscle, bone and metabolic fields. Each
stream processes at most 100 dates per run, pauses a persistent date after three
failures, and shares the global Garmin/R2 limits.

For efficient longitudinal queries, `pipeline.health_history_index` converts
the canonical per-night HRV and sleep objects into versioned, gzip-compressed
monthly indexes under `health/indexes/{hrv|sleep}/v1/YYYY/MM.json`. The builder
is revision-aware, resumable, never contacts Garmin and writes only changed
months. HRV and sleep ingestion invokes it for affected months; a local command
or the manual `health-history-index.yml` workflow performs the initial build.

The Worker reads at most 13 monthly indexes for a 366-day summary request.
`auto` granularity returns daily rows through 31 days and ISO-week summaries
after that. It verifies every day in a daily request and the newest seven days
in a weekly request against the canonical object, replacing a stale index row
in memory and reporting the consistency state. Full HRV readings or sleep-stage
timelines remain limited to seven days. The refresh workflow finishes with a
revision-aware reconciliation of every history month. Together these safeguards
make canonical R2 data self-healing while bounding R2 operations, Worker CPU and
MCP response size.

This is the R2 consistency contract for every data type: canonical objects are
authoritative; derived manifests and indexes are updated by their writer and
must have a reconciliation path; bounded readers prefer canonical state when a
derived view disagrees. Tools without a derived index (single-night HRV/sleep,
body composition, activity artifacts and coach inputs) already read their
canonical keys directly. Summary CSV tools validate their R2 ETag before reusing
an in-isolate parsed cache.

All historical backfills are self-quiescing. A completed activity plan returns
before Garmin login and performs no R2 writes. HRV, sleep and body-composition
plans do the same unless the latest summary contains a newly eligible date; in
that case only the new dates reactivate the plan. The GitHub schedules remain in
place so new data can be noticed, but completed runs are cheap read-only checks
rather than continued backfills.

An optional local bootstrap orchestrator can run these same bounded modules in
consecutive cycles for initial history imports. It does not define another data
path or schema: progress and canonical objects remain in R2, and the hosted jobs
can resume from the exact same point. Each phase keeps its existing write and
byte guardrails. The local runner adds a process lock, a wall-clock limit, a
three-minute default pause, per-phase duration metrics, and fail-closed handling
of Garmin service/rate-limit and R2-budget errors. Hosted schedules must be
paused during local operation because GitHub's concurrency group cannot
coordinate with a personal computer.

Coach generation is a separate R2-only consumer. Activity-producing workflows
trigger it after successful completion, and its independent schedule provides a
fallback. A persisted per-activity source signature contains the activity
summary, selected profile, newest user-context key, analyzer version and any
activity refresh manifest revision. Only new or changed signatures cause FIT,
TCX and endurance JSON reads; unchanged activities are skipped from the compact
R2 inventory. Activities whose endurance object explicitly reports unavailable
telemetry are recorded as processed but do not receive an empty coach analysis.
The canonical versioned coach objects remain immutable outputs.

The local path uses Garmin date-range calls where the API supports them:
activity discovery is fetched by year and missing body-composition dates are
grouped into bounded windows. HRV and sleep remain daily Garmin calls, but one
authenticated process owns the requested history and streams normalized,
compressed days to R2. A separate local import adapter can seed canonical HRV
and sleep objects from existing raw JSON exports. It uses the same normalizers,
R2 limits and process lock; source files and paths never enter Git or R2.

`activity-refresh.yml` is an intentionally ongoing rolling check, not a
historical backfill. It fingerprints recent Garmin metadata and strength exercise
sets, then replaces the canonical FIT, normalized JSON, TCX and endurance object
when a change is detected. Its first pass writes a small source manifest for an
already complete activity without downloading the files again. A manual run can
force one explicit activity ID when Garmin changes a binary export without
changing any observable metadata.

The schedules are deliberately staggered around the six-hour summary refresh:
refresh runs at 00:00, 06:00, 12:00, and 18:00 UTC; health details at 01:15,
07:15, 13:15, and 19:15; activity backfill at 02:30, 08:30, 14:30, and 20:30;
HRV backfill at 04:30, 10:30, 16:30, and 22:30; and recent activity change
detection once daily at 23:15. All jobs share the `garmin-sync` concurrency
group, so GitHub queues rather than overlaps delayed runs.

## 2. Serve (`worker/`)

A Cloudflare Worker exposes the data over MCP (the open protocol both Claude and
ChatGPT speak). It's built on the `agents` MCP runtime and the official MCP SDK.

- Each request gets a fresh, stateless MCP SDK v2 server. On a tool call it
  reads the required summary or detailed object from R2 through a private
  binding and answers in-memory. Parsed summaries are cached per isolate, but
  each reuse checks the R2 ETag first so a completed refresh cannot leave a
  different warm isolate serving a five-minute-old view.
- It exposes read-only activity, health, HRV, sleep, body-composition, strength,
  and endurance tools. Every MCP tool includes an explicit output schema.
- Its canonical Streamable HTTP endpoint is `/mcp`, protected at the edge by
  Cloudflare Access Managed OAuth. Access handles discovery, dynamic client
  registration, PKCE, login, and opaque client tokens. The Worker independently
  validates the signed Access assertion's signature, issuer, audience, expiry,
  and human identity before invoking MCP. All other routes return 404; there is
  no unauthenticated or secret-path fallback.
- It exposes summaries plus normalized HRV curves, sleep stages,
  body-composition measurements, strength sets, and a compact endurance dataset
  derived from TCX. The endurance view contains laps,
  kilometre splits, heart-rate seconds per BPM, distance-half drift, and a
  10-second curve. Raw FIT/TCX, arbitrary R2 objects, and GPS coordinates are
  never returned by MCP.
- OAuth traffic is rate-limited per Access subject. Requests and R2 payloads
  are bounded, and responses are marked `no-store`.
- The transport accepts only the installation's exact `MCP_HOSTNAME` and
  applies the MCP runtime's default browser Origin checks.
- A small SQLite-backed Durable Object lease serializes on-demand refresh
  dispatches. This closes the race where two simultaneous calls could both pass
  the GitHub cooldown check.
- Write tools are absent by default. When explicitly enabled, the same Durable
  Object enforces a global per-identity cooldown and an exact 60-write UTC-day
  budget atomically, even when requests arrive concurrently.
- On-demand refresh uses short, bounded GitHub status polls. The tool outputs
  explicit `terminal`, `data_ready`, and `should_continue_polling` fields so an
  MCP client can keep checking within the same conversation turn rather than
  requiring another user prompt. Successful completion clears the per-isolate
  summary cache before reporting that R2 data is ready.
- Cloudflare invocation logs are disabled by default to reduce noise and log
  volume. Sanitized application error logs remain enabled.

`ACCESS_TEAM_DOMAIN`, `ACCESS_AUD` and `MCP_HOSTNAME` configure assertion and
transport validation. They are stored as Worker secrets to keep each self-hosted installation's identifiers
out of a public fork. R2 is attached as the `SLIPSTREAM_DATA` binding. Optional
on-demand refresh adds `GITHUB_REPOSITORY` and `GITHUB_ACTIONS_TOKEN` as Worker
secrets. Nothing sensitive or installation-specific is stored in source code.

## 3. Connect

OAuth-capable MCP clients connect to the canonical `/mcp` URL and authenticate
through Cloudflare Access Managed OAuth.

## Security model

| Concern | Mitigation |
|---|---|
| Garmin password | Never stored; a refreshable session token is used instead. |
| Who can read the data | Cloudflare Access policy plus a Worker-validated, app-specific signed assertion. |
| Worker's data access | A private R2 binding. Optional on-demand refresh uses a single-repository GitHub token limited to Actions read/write; the code invokes only `refresh.yml`, while the underlying GitHub permission remains repository-wide. |
| Abuse resistance | Access rejects unauthenticated requests at the edge; the Worker adds per-user authenticated rate limiting, bounded payloads, and an atomic refresh lease. |
| Location privacy | GPS/track data is never exposed by the connector. |
| Where data lives | Your own private Cloudflare R2 bucket. |

## Extending to more sources

The `pipeline/` layer is source-oriented (`sources/garmin.py`). Adding Wahoo,
Strava, or others is a matter of dropping in a new adapter that yields the same
`Activity` objects, plus a de-duplication pass (the same physical workout can
appear in several places). That multi-source design is the natural next step.

# Coach-input analysis

Coach input is a deterministic R2-derived layer, not AI-generated prose.  The
Python job reads the existing FIT, TCX and normalized endurance objects, applies
the user-selected versioned heart-rate profile, and writes immutable compressed
JSON under `activities/<year>/<id>/coach-input/v1/canonical/`. It never contacts
Garmin. User RPE, conditions, notes and workout corrections are append-only
objects under the activity's `context/v1/` prefix.

Profiles live under `coach/profiles/v1/` and have an `effective_from` date.
Changing zones therefore affects later activities without silently rewriting
historical analyses. The analysis identifier includes the source hashes,
profile, context revision and analyzer version, so explicit reanalysis produces
a new object rather than overwriting the previous one.

The scheduled job processes 50 activities by default and at maximum, with 60
R2 writes and 64 MiB per run. A completed plan remains passive until the activity
summary, a source manifest, a profile, or activity context changes. Heavy TCX
calculation runs in GitHub Actions; the Worker only performs bounded validation
and small R2 reads/writes. The existing per-user MCP rate limit still applies.
When MCP writes are enabled, Durable Object leases additionally serialize writes
per identity, limit profile writes to one per minute, context writes to one per
ten seconds, and cap all accepted writes at 60 per identity and UTC day.
