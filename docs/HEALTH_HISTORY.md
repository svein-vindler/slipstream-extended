# HRV and sleep history

Slipstream exposes two bounded, read-only MCP tools for efficient longitudinal
analysis: `hrv_history` and `sleep_history`. The existing `hrv_curve(date)` and
`sleep_detail(date)` tools remain unchanged for inspecting one night.

## Query behavior

Both history tools accept `start_date`, `end_date`, `granularity` and
`detail_level`.

| Request | Result | Hard limit |
| --- | --- | --- |
| `granularity=auto`, 1-31 days | one normalized row per day | 31 days |
| `granularity=auto`, 32-366 days | compact ISO-week summaries | 366 days |
| `granularity=daily` | one normalized row per day | 31 days |
| `granularity=weekly` | compact ISO-week summaries | 366 days |
| `detail_level=full` | daily HRV readings or sleep stages | 7 days |

A six-month request is one MCP call returning roughly 26 weekly rows. The
assistant can then drill into an unusual week with a daily request or inspect
one night with the existing single-date tools.

## Which calendar day belongs to a night?

Garmin's existing `date` remains the **morning/wake-date** for both sleep and
nightly HRV. It is unchanged for compatibility. A sleep row dated `2026-09-24`
can therefore describe sleep starting on the evening of `2026-09-23`. Do not
classify Friday or Saturday nights using `date`.

New Garmin imports preserve both UTC instants and Garmin's per-night local
wall-clock timestamps. The Worker prefers that recorded local time, including
on trips. Set the optional `HEALTH_TIMEZONE` to the user's usual IANA zone
(for example `Europe/Oslo`) as a fallback for older records without Garmin
local timestamps:

- `wake_date` is the explicit alias of the existing `date`.
- `night_of` and `sleep_start_date_local` are the local date on which sleep
  started. Use `night_of` for weekday and weekend grouping. A post-midnight
  sleep start is classified on that new calendar date.
- `sleep_start_local`, `sleep_end_local` and `sleep_midpoint_local` are ISO local
  timestamps with their actual UTC offsets. The offsets are computed separately
  at each instant, so daylight-saving transitions are handled correctly.
- `sleep_end_date_local`, `sleep_start_weekday_local` and
  `sleep_end_weekday_local` make the calendar meaning explicit. Weekday names
  are English Monday–Sunday.
- `local_time_source` is `garmin_local`, `configured_timezone` or
  `unavailable`. Garmin gives the actual local offset for each sleep-window
  endpoint, but not necessarily an IANA timezone such as `Asia/Tokyo`.
  Per-night `timezone` is therefore `null` if Garmin's offset differs from
  the configured reference zone. The ISO local timestamps still carry the
  correct offset. The top-level history `timezone` is only the configured
  fallback zone, not a claim about every night in the range.

The same fields appear in `sleep_detail`, `hrv_curve`, and daily rows of
`sleep_history` and `hrv_history`. Nightly HRV uses the same wake-date join key;
its `night_of` is populated only when its own sleep-start timestamp exists.
`daily_health` keeps `date` as its general health-calendar date and adds
`sleep_night` and `hrv_night` objects where those metrics exist. For its HRV
context, the matching sleep window is preferred when the HRV record lacks
Garmin local times, even if its UTC window is present.
This tool bounds its extra lookups to 13 index months and read-through of three
recent dates per stream; `night_context_limited` signals when a sparse query
spans more months. `night_context_unavailable` reports a context lookup error
without hiding the ordinary daily summary. Use the dedicated history tools for
longer night analyses.

If Garmin local times are absent, the Worker uses the configured IANA zone
with DST rules. If both are absent, derived local fields are `null`; it never
guesses a local date. If a stored night lacks a usable start timestamp,
`night_of` remains `null` even when the zone is configured. `wake_date`
remains available. If Garmin's start and end offsets differ and no matching
IANA zone is known, `sleep_midpoint_local` is `null` rather than guessing the
transition instant. Previously stored nights contain only UTC fields, so
travel nights need a targeted refetch from Garmin (or a retained raw export)
to gain correct local context. The ordinary backfill does not overwrite them.

`sleep_history` includes `by_night_of_weekday` even for compact weekly
requests. Each of the seven rows gives night counts, sleep-duration/score
statistics, and a circular mean of local mid-sleep clock time. The optional
`weekend_midpoint_shift_minutes` is the signed difference between Friday/
Saturday night and Sunday–Thursday night midpoints; it is a **calendar-based
proxy**, not a diagnosis or a work-schedule-aware social-jetlag measure. Check
sample counts and `nights_without_local_start` before interpreting it. Existing
weekly `period_start`/`period_end` still group **wake-dates** for compatibility.
For 1–2-night lag analysis with HRV, request daily HRV and sleep rows in
overlapping chunks of at most 31 days, then join on `wake_date` or `night_of`.

Every response distinguishes `available`, `no_data`, `not_stored` and
`invalid_schema`. One missing night does not fail the whole period. HRV fields
under `garmin` come from Garmin's nightly summary; fields under `derived` are
computed by Slipstream and must not be interpreted as Garmin metrics.
An HRV day remains `available` when Garmin provides its nightly summary but no
detailed readings; `detailed_readings_available` makes that distinction explicit.

The canonical daily R2 object is the source of truth. Daily history requests
check every requested day directly (at most 31 objects), while longer weekly
requests check the newest seven days directly. This bounded read-through makes
new or corrected recent data visible even if its monthly index has not been
rebuilt yet, without turning a year-long query into hundreds of R2 reads.
For summary requests, the Worker compares R2 ETags recorded in the index and
downloads a canonical object only when it is new or changed. Full-detail
requests still read the requested canonical objects to return their timelines.

`index_consistency` explains what was checked. `stale_dates` were served from a
canonical object that was missing from or differed from the index;
`orphaned_index_dates` existed only in the index; and
`confirmed_missing_dates` were absent from both. Daily rows additionally expose
`index_state` (`verified`, `read_through`, `confirmed_missing`,
`orphaned_index`, `indexed` or `index_only`). A malformed monthly object is
reported in `invalid_index_objects`; it does not hide canonical data in the
bounded read-through window.

## Monthly R2 indexes

Canonical per-night objects stay unchanged. Small gzip-compressed indexes are
stored at:

```text
health/indexes/hrv/v1/YYYY/MM.json
health/indexes/sleep/v1/YYYY/MM.json
```

The indexes contain normalized summaries and derived statistics, never raw
Garmin payloads, activity tracks or GPS. Full HRV readings and sleep-stage
timelines are read from canonical daily objects only for requests of at most
seven days.

Source object revisions are recorded inside each private index. Re-running the
builder skips unchanged months at the current builder revision, making it safe
and resumable while still allowing corrected summaries to be rebuilt. Normal HRV and
sleep ingestion updates only affected months automatically. The six-hour and
on-demand refresh workflow also performs a final full reconciliation. It reads
revision metadata first and rewrites only changed months, so a process that was
interrupted between writing a daily object and its index repairs itself on the
next refresh.

## Initial index build

The builder reads existing R2 objects and never contacts Garmin. Run it locally
from the repository root after configuring `.env.local-bootstrap`:

```powershell
.\.venv\Scripts\python.exe -m pipeline.health_history_index `
  --env-file .env.local-bootstrap
```

It is safe to stop and rerun. Unchanged months are skipped. To build only recent
months, add `--recent-months 3`. Users who prefer GitHub Actions can run
**Build health history indexes** once with `stream=all` and `recent_months=0`.

## Free-tier design

The R2 safeguards remain 5 GiB and 100,000 objects, below Cloudflare's 10
GB-month Standard-storage free allowance. A full first build for roughly 3,300
sleep nights and 1,400 HRV nights needs about 5,000 Class B reads and fewer than
200 Class A writes. A six-month MCP query reads six or seven monthly objects
rather than about 180 daily objects.

Read-through adds at most two bounded monthly prefix scans. A summary request
normally performs no extra object read when index ETags match; the worst case is
31 canonical reads for a daily query or seven for a weekly query. A normal
refresh's reconciliation lists revisions and skips unchanged months. These
bounds keep the self-healing behavior comfortably below the existing Worker
subrequest and project R2 budgets.

Cloudflare currently includes 1 million Class A operations, 10 million Class B
operations and 10 GB-month of Standard storage each month. The free allowance
does not apply to Infrequent Access storage, so Slipstream uses Standard R2.
Limits can change; verify the current Cloudflare R2 and Workers documentation
before a large deployment.
