# Training and following-night associations

`night_training_history` reads a bounded offline product made from stored native
night exports and the existing activity CSV. It describes temporal associations;
it provides no causal, medical, recovery or training-load score. Reading the tool
never contacts Garmin, refreshes data, scans source objects or writes storage.

## Build from stored exports

Use native `night_signals` / `night_signals_history` exports with
`method_version=night-signals-v1`. Multiple `--input` arguments can combine exports:

```sh
python -m pipeline.night_training build \
  --input native-nights.json \
  --activities activities.csv \
  --start-date 2026-10-25 --end-date 2026-10-26
```

The command prints `{storage_key, product}`. It does not fetch, upload, backfill,
choose a publication policy or change previous sources. The product is bound to
`night-training-v1`, its requested dates, source manifest and product identity.
A maintainer publishes the reviewed product through the ordinary delivery process.
Its fixed key is:

```text
health/night-products/v1/training/YYYY/MM/DD/<args_sha256>.json
```

The directory uses `end_date`. The argument hash contains only `start_date` and
`end_date`; changing page size or page start does not create another source product.

Omit `--activities` to explicitly record `not_stored`. With no activities file,
`--activity-state read_error` records a known failed source read instead. Supplying
an unreadable or malformed file fails the command; it is not silently treated as
an empty dataset. A valid header-only activity CSV documents zero **recorded**
rows in that export. It does not prove complete provider coverage or no exercise.

## UTC and linkage rules

The accepted CSV time contract is `slipstream-activities-csv-utc-v1`:
`pipeline/writer.py` writes `Activity Date` as UTC `YYYY-MM-DD HH:MM:SS`.
This exact producer format is interpreted as UTC. Explicit ISO timestamps with
`T` and `Z` or an offset are also accepted. Other naive strings and bare epoch
numbers are unknown, rather than assigned a guessed timezone or timestamp unit.
`Activity Local Date` and the configured home zone do not determine linkage.

An activity end is established only by its known start and recorded `Elapsed Time`
(nonnegative whole seconds, at most seven days). `Moving Time` cannot replace a
missing elapsed duration: pauses make that inference unsafe. A recorded zero is
retained. Missing or invalid starts and ends remain uncertain.

Each verified activity links once to the earliest documented sleep start after
its end, including equality at the start boundary, with a maximum 48-hour gap.
Activity intervals overlapping any documented sleep are excluded and flagged as
uncertain. Overlapping night windows are themselves ambiguous; the builder does
not choose an arbitrary wake-date or skip that uncertainty to a later night.
Duplicate activity identities are counted once when identical; conflicting
versions remain uncertain. `wake_date` remains the existing night identity.

These UTC rules work across midnight, DST folds and travel. For example:

- Oslo UTC sleep `2026-10-24T21:58Z` to `2026-10-25T06:48Z` spans 530 minutes.
  An activity at `23:30Z` occurs inside it and is not preceding training.
- Tokyo local activity start `2026-10-25T23:00+09:00` is `14:00Z`. With a recorded
  15-minute duration, its end at `14:15Z` precedes sleep starting at `14:30Z`.
- An unknown elapsed duration is not converted to zero or to moving time.

## Meaning of the returned data

The response `status` describes the stored product, not physiological completeness.
Each night has separate source, training, channel and context states.

`training.confirmed_activity_count` counts only confirmed links in the provided
export. `uncertain_activity_count` and `status=partial` expose unresolved records.
`recorded_duration_seconds` is null when the sum is incomplete;
`known_recorded_duration_seconds` is the sum of confirmed recorded intervals.
Missing/error datasets or unusable night windows retain null counts and sums.
Zero means zero recorded links in an explicitly supplied dataset, not verified
absence of real-world training. Coverage is always labelled
`provided_export_only_provider_completeness_unknown`.

The CSV provides activity type, recorded duration and optional average HR.
It does not provide a verified relative-intensity measure or training-load field;
`intensity` and `training_load` remain null. Activity names, filenames, GPS and raw
notes are not projected.

Channel summaries use only valid, explicitly UTC native observations clipped to
the half-open sleep interval `[start,end)`. Identical timestamp/value pairs are
deduplicated; conflicting values at the same instant are excluded. Local-only,
invalid and out-of-window points cannot become normal measurements. The median,
valid/invalid counts, source revisions, observed cadence and largest gap remain
visible. Coverage uses occupied observed-cadence slots and is unknown when cadence
cannot be established. There is no interpolation or assumed sensor sample rate.

`actual_night_timezone` comes only from a valid explicit exported IANA zone.
Invalid zones become null with quality flags; independently valid UTC data remains
usable. Home-zone projection never establishes the actual travel location.

Context is explicitly an `export_snapshot`, with revision and
`available`, `not_stored`, `not_provided` or `unavailable` status. Structured user
covariates can include alcohol, late bedtime, travel, illness, sleep quality,
morning condition and actual timezone. Null and `declared_fields` retain the
meaning of explicit clearing. Raw notes are excluded. This offline view does not
claim to include a later context edit; rebuild from a fresh stored export when a
new snapshot is needed. No automatic recomputation or provider request is started.

## Pagination and bounds

```json
{
  "start_date": "2026-08-01",
  "end_date": "2026-09-09",
  "page_start_date": "2026-08-01",
  "page_size": 7
}
```

Use `next_page_start_date` for the next page. The range identifies one product;
`page_size` defaults to seven and accepts 1–31. Dense results can return
`output_limit`; request a smaller page against the same product.

| Boundary | Limit |
| --- | --- |
| Offline requested date range | 1–366 days |
| Native export files | 64 |
| Combined export and CSV bytes | 16 MiB |
| Input native points | 200,000 |
| Activity CSV rows | 5,000 |
| Confirmed activities per night | 100 |
| Stored / decoded product | 1 MiB / 1 MiB |
| Product reads per valid request | One fixed-key GET; no LIST or HEAD |
| Returned nights | At most 31 |
| Structured / MCP envelope budget | 256 KiB / 512 KiB |
| Result acceptance deadline | 10 seconds |

The deadline is checked around awaited reads and before returning. R2 does not
supply an abort parameter here, so this is not a hard settlement deadline for a
pending platform operation. A late result cannot become successful output.
Missing, malformed, failed, oversized and late reads remain distinct controlled
states. No retries or alternate object keys are attempted.

## Source and product identities

`source_manifest` retains exact SHA256 and byte counts for each input export file
and the activity CSV, alongside per-night semantic snapshot and context revisions.
It stores no local paths. `source_identity` binds that manifest.

Typed product identities use canonical object-key ordering and finite IEEE754
binary64 material (`canonical-f64-v1`). Numeric signed zero is normalized to
positive zero; other finite numeric bits are retained. This allows ordinary JSON
reserialization without `-0` portability failures. The identity is semantic, not
a hash of the serialized product bytes. Exact raw-source file hashes separately
preserve original-byte provenance. Unknown fields and inconsistent counts,
intervals, units or identities are rejected before projection.

The local synthetic CLI and authenticated JSON/SSE checks exercise these rules.
Node app tests with explicit platform adapters do not replace ordinary native
Workers, Durable Object or generated-binding checks. No live provider coverage,
production correctness, causal effect, free operation or performance improvement
is established by those synthetic tests.
## Exported quality and surrounding windows

Each present channel can include `source_quality`, separate from the summary's own
`invalid_points` and `quality_flags`. It preserves the export's channel status,
controlled flags, nullable exclusion counts, source-observation flags and known
missing count. Missing counts remain unknown; they are not invented zeros. Bad
or unrecognized quality metadata is marked invalid without echoing arbitrary
source text. A channel with excluded ambiguous measurements remains
`ambiguous_time` or `partial`, even if its exported UTC points array is empty.
`no_data` requires an explicitly documented empty source without contradictory
quality evidence. A valid median can coexist with partial upstream quality.
`input_points` counts the exported point-array entries processed by this command;
exclusions performed by the earlier exporter remain separate source counts.

Association uses the documented windows already supplied in the exports before
filtering the returned wake-dates. Narrowing `start_date`/`end_date` cannot move an
activity past its earlier true night to a later returned night. Such an activity
is counted in `outside_window` for the product's result period. Recorded overlap
with a supplied outside-period sleep still prevents confirmed linkage. A known
window with unavailable measurements or conflicting source variants also blocks
confirmed reassignment; unavailable measurements do not erase its UTC window.

The source manifest retains bounded `association_windows` with source snapshot
identity. The product reader validates earliest-night and overlap rules against
this complete supplied context, including windows outside its returned dates.
The builder accepts at most 366 association dates/windows and fails explicitly
rather than dropping excess context. Existing 16MiB input, 200000-point, 5000-row,
1MiB product, one-GET reader and response/deadline limits are unchanged. No extra
source fetching, scanning or publication is performed. Reader pagination still
uses one fixed product and does not recompute associations.

Older products without these optional metadata fields retain their legacy read
contract; their missing upstream-quality/window evidence is not reconstructed.
Rebuilding from the original stored exports supplies the corrected provenance.
