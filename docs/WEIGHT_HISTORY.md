# Weight history and standardized morning measurements

`weight_history` reads existing private R2 body-composition objects for a
calendar range of **at most 31 days**. It does not contact Garmin, write R2,
start a backfill or require a storage migration. Optional compact monthly indexes
reduce repeated canonical downloads while retaining every individual weighing
and timestamp. A June–September analysis
therefore needs several consecutive, non-overlapping requests rather than one
MCP call for every date. `body_composition(date)` remains available when all
stored measurements for one particular day are needed.

The health-detail pipeline fetches Garmin's **daily weigh-in view** for each
date, which contains individual measurements. Garmin's body-composition
date-range summary can contain only the latest measurement for each day; using
that summary alone would make an earlier morning weigh-in disappear when a
later weigh-in was recorded. The daily view is fetched sequentially with a
pause between requests. The existing bounded batch and Garmin error handling
still apply. No additional R2 objects are created per measurement: a date's
measurements remain together in its existing body-composition object.

Inputs are `start_date`, `end_date`, optional `morning_start` and `morning_end`
(`HH:MM`, default `04:00`–`12:00`, start inclusive and end exclusive), and
optional IANA `timezone`. Dates must be real calendar dates. The timezone
argument overrides the configured `HEALTH_TIMEZONE` **for that request only**;
it is useful for travel dates whose stored measurements have UTC timestamps
but no Garmin local timestamp.

The tool selects the earliest stored **individual** measurement with a valid
weight and a local timestamp within the requested morning window on that
calendar date. Garmin daily-average records are never substituted. The row
also reports the number of stored measurements, individual weights, daily
averages, timed measurements and morning candidates, plus the minimum, maximum
and range of individual weights for the day. This makes multiple weighings
visible without returning every body-composition field across the period.

For each selected measurement, `local_time_source` is `garmin_local` or
`configured_timezone`. Garmin's own local wall time takes priority, including
when its offset differs from the configured home timezone. A UTC-only
measurement needs a valid configured or explicitly supplied IANA timezone;
daylight-saving rules are then applied to its UTC instant. The top-level
`timezone` is only the fallback/override and is **not** a claim about every
measurement. A Garmin-local selection has `selected.timezone: null`, but its
`timestamp_local` includes the recorded UTC offset. When neither source yields
a reliable local time, the row is not selected.

Possible per-day statuses are:

| Status | Meaning |
| --- | --- |
| `selected` | An individual morning measurement was chosen. |
| `not_stored` | No body-composition object exists in R2 for that date. |
| `invalid_schema` | The stored object cannot be used as normalized measurements. |
| `no_actual_weight` | Only averages, missing weights or invalid weights are stored. |
| `no_usable_local_time` | An individual weight exists but cannot be timed locally. |
| `no_morning_measurement` | Timed individual weights exist, but none match the date and morning window. |

`outside_requested_date_count` reveals when a timestamp resolves to a
different local calendar date than the stored R2 date. This can indicate a
wrong timezone fallback during travel; retry just those days with the correct
IANA timezone if Garmin local time is absent. The tool never silently moves a
measurement to another day or invents a morning value.

For a rolling 7-, 14- or 28-calendar-day trend, combine the daily selected
weights across consecutive chunks, use **at most one selected value per day**,
and report the number of observed days in each window. Do not treat an average
of available days as if every calendar day were observed. Do not confuse this
with `daily_health.weight_kg`: the latter is one value from Garmin's daily
summary, normally `latestWeight`, with summary-field fallback. Slipstream does
not compute an average of all weighings for that field, and it is not a
standardized morning weight. `health_trends.weight_kg` summarizes those same
daily-summary values.

Canonical R2 objects remain the source of truth. Each request lists only the
relevant monthly body-composition prefixes and verifies source ETags. Matching
index entries can answer without downloading each day; missing, corrupt or stale
entries read canonical objects directly. Deleted sources never survive as old
index measurements. Both list pages and object sizes are bounded, and the
existing per-identity MCP rate limit still applies. If Garmin supplied only a
daily summary or a latest weight for an older date, this tool cannot recover
individual morning measurements from that object; a targeted Garmin re-fetch
is needed after inspecting coverage. To repair existing data, dispatch
`scheduled-health-detail-backfill.yml` with `repair_body_start` and
`repair_body_end` (both `YYYY-MM-DD`). One run accepts at most 31 calendar
days and re-fetches only dates with a weight in the health summary. It
replaces those dates' canonical body-composition objects with Garmin's full
daily view. Start with a short canary range and check `body_composition(date)`
and `weight_history` before repairing older history in further 31-day chunks.
The normal scheduled run does not re-fetch all historical dates after its
backfill is complete; regular refreshes continue to re-fetch recent dates.

Recent body ingestion and active body backfill update affected monthly indexes
under `health/indexes/body-composition/v1/`. The index keeps every actual weight,
local/UTC clock, average flag, measurement identity and source type. Selection is
computed at query time, so custom morning windows, travel and timezone fallback
work exactly as with canonical data. Other body metrics remain in canonical
objects and `body_composition`. Reads never repair indexes or write R2.

For existing history, index construction is optional and R2-only. With the
normal private R2 credentials loaded, run bounded chunks of at most 366 days:

```bash
python -m pipeline.weight_index --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

Building a month initially reads its canonical objects; repeats use revision
metadata and the current compact index, with no unchanged canonical downloads or
PUTs. Source edits reread affected objects only. Existing aliases remain readable.
The per-month index is limited to 512 KiB; an oversized month stays on canonical
reads. Missing indexes add a small lookup before the existing read-through path.
`source_objects_read` counts index and canonical GET attempts; list-operation
counts retain their existing meaning. No extra Garmin calls or per-measurement
objects are introduced. Index writes use the existing storage and write budgets.
