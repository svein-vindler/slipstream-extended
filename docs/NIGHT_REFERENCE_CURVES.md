# Personal night reference curves

`night_baseline` reads a precomputed reference curve made from exported native
night signals. The offline Python command builds the product; it does not fetch
Garmin data, scan R2, upload the product or fill historical gaps. The Worker reads
one fixed product object, or two for a comparison. Requesting 365 days does not
make it read 365 source-night objects.

The current method is `night-baseline-v1`. Its quantiles describe the supplied
observations, not a medical score or evidence of a training-related cause.

## Generate an offline product

Run from the repository root with the project's Python environment. Input files
are uncompressed UTF-8 JSON containing a single native `night_signals` result or
a `night_signals_history` result with a `nights` array. A tool-result object whose
own `structuredContent` contains that data is also accepted. Extract the
structured data from a complete JSON-RPC response before saving the input.
Compact bins and raw Garmin responses are not native curve inputs.

```sh
python -m pipeline.night_products baseline --input exports/nights-part-1.json --input exports/nights-part-2.json --end-date 2026-10-25 --period 28 > products/reference-28.json
```

Create the relative output directory beforehand. Repeat `--input` for bounded
export chunks; overlapping identical nights do not gain extra votes. No command
here performs an upload.

| Option | Accepted values and default |
| --- | --- |
| `--input` | Required; repeat for 1–64 files |
| `--end-date` | Required real date, `YYYY-MM-DD` |
| `--period` | `7`, `28`, `90`, `365`, or `custom`; default `28` |
| `--start-date` | Required for `custom`; a supplied preset start must agree with its derived start |
| `--alignment` | `sleep_start`, `wake`, or `clock`; default `sleep_start` |
| `--bin-minutes` | `15` or `30`; default `30` |
| `--channel` | `heart_rate`, `hrv`, `body_battery`, or `stress`; default `hrv` |
| `--exclude-date` | Repeat for explicit wake-date exclusions; default none |
| `--context-filter` | JSON object of exact structured user-context matches; default `{}` |

Dates are inclusive. For example, a seven-day period ending on 2026-10-25 starts
on 2026-10-19. Custom periods permit 1–366 days:

```sh
python -m pipeline.night_products baseline --input exports/native-nights.json --start-date 2026-10-01 --end-date 2026-10-25 --period custom --alignment wake --bin-minutes 15 --channel heart_rate > products/reference-wake.json
```

The command writes one JSON result to standard output with `storage_key` and
`product`. The latter is the product object for later, separately authorized
publication at the returned fixed key. Do not store the entire CLI wrapper as a
product. Generating the file alone does not make it available through MCP.

The fixed key has this layout:

```text
health/night-products/v1/baseline/YYYY/MM/DD/<normalized-argument-sha256>.json
```

It is derived from the end date and normalized start/end dates, alignment, bin
width, channel, sorted unique exclusions and context filter. Canonical JSON uses
sorted object keys, compact UTF-8 serialization and SHA256. An equivalent preset
and custom window therefore share a key when all other options agree. The
period label itself is not part of the normalized argument identity.

## Read or compare published products

Tool arguments use underscores instead of CLI hyphens. `end_date` is required;
other defaults match the CLI. There is no argument accepting an arbitrary R2 key.
The generating command and read request must describe the same normalized
window, channel, alignment, bin width, exclusions and filter.

```json
{
  "tool": "night_baseline",
  "arguments": {
    "end_date": "2026-10-25",
    "period": "28"
  }
}
```

For a seven-day comparison with the preceding seven days, generate both products
using the same options and input exports covering both windows:

```sh
python -m pipeline.night_products baseline --input exports/native-nights.json --end-date 2026-10-25 --period 7 > products/current-7.json
python -m pipeline.night_products baseline --input exports/native-nights.json --end-date 2026-10-18 --period 7 > products/previous-7.json
```

After separate publication of those products:

```json
{
  "tool": "night_baseline",
  "arguments": {
    "end_date": "2026-10-25",
    "period": "7",
    "compare": "previous"
  }
}
```

`compare` can instead be an object containing explicit `start_date` and
`end_date`, for example 2026-09-01 and 2026-09-28. That reference window is also
limited to 366 days. Both sides use the same method, channel, bins, exclusions
and filters. The response declares each window under `current` and `reference`.
A failed or absent reference remains visibly unavailable; it does not turn into
zero measurements or replace an available current product.

## Bins, quantiles and data quality

Only finite, documented UTC observations inside the half-open sleep interval
[start, end) contribute. The sleep interval must be positive and at most 36
hours. HR and HRV values must be positive; Body Battery and stress values permit
recorded zero through 100. Negative missing-value codes are not zero scores.
There is no interpolation.

Identical timestamp/value observations collapse. Conflicting values at the same
instant are rejected. Each eligible night contributes one median per bin before
the cross-night distribution is calculated. A dense or long night therefore
gets the same single vote as another contributing night in that bin.

For nightly bin medians of 40, 60, 80 and 100, the results are P10=46, P25=55,
P50=70, P75=85 and P90=94. These use linear interpolation at rank `(n-1)p`, even
if the nights contained 1, 3, 5 and 7 raw observations respectively.

| Alignment | Meaning |
| --- | --- |
| `sleep_start` | UTC elapsed time from the documented sleep start; first bin index is 0 |
| `wake` | UTC elapsed time relative to sleep end; bins before waking have negative indices |
| `clock` | Local minute-of-day grid using a documented actual IANA night zone |

Sleep-relative alignment works without a known actual location. Clock alignment
requires an actual zone in the export or matching user-context declaration; the
configured home projection is not evidence of the night's location. Nights with
unknown actual zones are explicitly excluded. During a DST fold, two distinct
UTC instants can occupy the same local clock bin. They are combined within that
night before its single cross-night vote, with `dst_fold_combined` reported.

Each bin reports its index, offset in minutes, quantiles, contributing `nights`
and `valid_points`. Empty bins have null quantiles and zero counts. The product
also reports requested, supplied and contributing night counts, exclusions and
quality notes. Valid points from an exported partial night may contribute, with
a `partial_source` note. Availability of a product does not mean every requested
night or every interval had complete measurements.

## Explicit exclusions and context filters

Filters match only structured user-supplied context for the same wake-date,
with the expected schema and a context identifier. Supported fields are:

- Boolean `alcohol`, `late_bedtime`, `travel` and `illness`.
- `actual_night_timezone` as an IANA zone string.
- Integer `sleep_quality` and `morning_condition` from 1 through 5.

```sh
python -m pipeline.night_products baseline --input exports/native-nights.json --end-date 2026-10-25 --period 28 --exclude-date 2026-10-20 --context-filter '{"illness":false,"travel":false}' > products/reference-filtered.json
```

The matching read request is:

```json
{
  "tool": "night_baseline",
  "arguments": {
    "end_date": "2026-10-25",
    "period": "28",
    "exclude_dates": ["2026-10-20"],
    "context_filter": {"illness": false, "travel": false}
  }
}
```

Missing, null, incorrectly typed or unverified context does not mean `false`.
Excluded dates and mismatch/unknown-field reasons are returned. Note-text filters
and unknown filter fields are rejected; illness or other events are not inferred.
The command also records absent exports, unsupported methods, non-native input,
unavailable channels, invalid sleep intervals, missing valid points and unknown
actual clock zones as explicit exclusions.

Identical exported snapshots for a wake-date collapse even if their volatile
resource/view metadata differs. Conflicting snapshots for that wake-date are
excluded instead of selecting whichever chunk was read last.

## Source identity and privacy

The product contains normalized arguments, their hash and a separate
`source_identity`. A sorted source manifest binds per-night snapshot hashes,
source revision references and context identifiers. Snapshot hashing excludes
volatile resource/view fields. Conflicting duplicates retain a stable combined
identity while remaining excluded from the calculation.

The Worker validates the kind, method, argument identity, source-manifest hash,
dates, units, counts and bin invariants before exposing quantiles. It returns the
source identity and projected summaries, not full source snapshots, the source
manifest or raw notes. The hashes bind the exported input; they are not an
independent verification of live Garmin data or current source freshness.
Revised exports or user context require rebuilding the relevant products before
a read can reflect those changes.

## Fixed limits and result states

| Boundary | Limit |
| --- | --- |
| Offline requested window | 366 inclusive dates; presets remain 7/28/90/365 |
| Offline export files | 64; each supplied night list at most 366 entries |
| Total input | 16 MiB of JSON file bytes and 200,000 points across all supplied channels/chunks |
| Individual product | 1 MiB of canonical product JSON |
| Worker reads | One fixed GET, or two with comparison; no source-night scan |
| Each product read | 1 MiB stored and 1 MiB decoded |
| Structured response | 256 KiB |
| Guarded serialized MCP envelope | 512 KiB, including a 1,024-byte framing reserve |

The ten-second initiation/result deadline rejects late success. R2 GET cannot be
cancelled through this boundary, so this is not a hard HTTP completion-time
promise. `decoded_bytes` counts successfully decoded text; it does not claim
exact byte consumption for failed reads. `envelope_budget_bytes` is a serialized
JSON-RPC estimate plus framing reserve, not observed wire traffic.

| State | Meaning |
| --- | --- |
| `available` | A valid requested product was found; inspect its counts, exclusions and quality notes |
| `not_stored` | The fixed requested product object is absent |
| `read_error` | Product reading/decoding failed, including a read-size failure |
| `invalid_schema` | Stored shape, method, hashes or method invariants are unsupported/inconsistent |
| `invalid_request` | Normalized date/window or other reference arguments are invalid |
| `partial` | A comparison has an unavailable current or reference side; inspect each side's state |
| `output_limit` | Result data was removed to respect the response limit |
| `budget_exhausted` | The result exceeded its initiation/result deadline |

Unknown tool fields and unsupported filter types can fail MCP input validation
before any product read. CLI input/argument failures exit unsuccessfully with a
controlled message and no product. Empty quantiles and absent products are
separate outcomes; neither is replaced with ordinary zero measurements.

Synthetic CLI and authenticated JSON/SSE tests cover the methods, fixed-key
parity and bounded product reads. They do not establish real historical
coverage, live provider availability, a published product, production deployment
or native platform behavior.
