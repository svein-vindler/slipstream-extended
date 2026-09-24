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

Every response distinguishes `available`, `no_data`, `not_stored` and
`invalid_schema`. One missing night does not fail the whole period. HRV fields
under `garmin` come from Garmin's nightly summary; fields under `derived` are
computed by Slipstream and must not be interpreted as Garmin metrics.
An HRV day remains `available` when Garmin provides its nightly summary but no
detailed readings; `detailed_readings_available` makes that distinction explicit.

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
sleep ingestion updates only affected months automatically.

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

Cloudflare currently includes 1 million Class A operations, 10 million Class B
operations and 10 GB-month of Standard storage each month. The free allowance
does not apply to Infrequent Access storage, so Slipstream uses Standard R2.
Limits can change; verify the current Cloudflare R2 and Workers documentation
before a large deployment.
