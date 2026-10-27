# Stored night signals

The normal summary collector now preserves heart-rate and Body Battery curves
already returned by its reviewed Garmin reads. It stores additive, dated objects
under `health/signals/v1/YYYY/MM/YYYY-MM-DD.json` (gzip is supported). This adds
no HR or Body Battery provider calls. Detailed HRV retains the original
source timestamp, selected field, unit and time basis. Existing objects and
analyses are retained; this feature does not initiate historical collection.

## Time and night identity

`wake_date` remains the storage identity. Ordinary sleep/HRV detail and daily
history responses retain the existing travel-aware local view and original
source clocks in `source_time`. `configured_projection` separately projects
both UTC endpoints and midpoint through the configured IANA home timezone.
For example, the Oslo rollback night from `2026-10-24T21:58:00Z` to
`2026-10-25T06:48:00Z` projects to `23:58+02:00` and `07:48+01:00`.
A conflicting source wall clock is visible in `time_quality_flags`.

A configured home zone does not identify a travel location. Explicit
`actual_night_timezone`, when stored, controls the actual local view; otherwise
it remains unknown. Invalid, reversed or longer-than-36-hour sleep intervals
are flagged and have no invented midpoint. Local-only/ambiguous HRV readings
remain stored but are excluded from documented UTC signal curves.

## Reading curves

Call `night_signals` with a morning `date`, optional `resolution` (`native`,
`15m`, `30m`) and `alignment` (`sleep_start`, `utc_clock`).
`night_signals_history` accepts inclusive `start_date`/`end_date` with the same
resolution/alignment choices. Native requests cover at most seven nights;
compact requests at most31. Existing `hrv_history` behavior is retained.

Readings are merged from the bounded relevant source dates and clipped to the
half-open documented sleep interval `[start_utc,end_utc)`. They preserve source
clock/field/unit; duplicate timestamps do not increase coverage. Conflicting
values, invalid points, ambiguous times, outside-window samples and gaps are
reported. There is no interpolation and no one-minute cadence claim.

Cadence is the median observed positive spacing. Coverage is explicitly a
nominal occupied-cadence-slot estimate, not a device-certified measurement
window or proof of uninterrupted recording. Unknown cadence yields unknown
coverage. Compact bins are half-open, use the declared origin, report their
within-bin median/count and retain empty intervals as null/count zero.

The heart-rate minimum is `lowest-10-minute-median-v1`: scan full ten-minute
windows on the observed cadence grid from sleep start, require at least80%
occupied expected slots, four distinct valid points, cadence no greater than
150seconds and no gap greater than twice that cadence. Return the selected
window/count/coverage or null. This is distinct from a raw minimum or Garmin's
daily minimum. Five two-minute samples `[80,48,46,44,42]` in a ten-minute night
have median46. In a longer night, a shifted full window containing
`[48,46,44,42]` at80% coverage can instead yield45.

## Availability and limits

Channels distinguish `not_stored`, dated `no_data`, `invalid_schema` and
`read_error`. Missing historical curves cannot be reconstructed from daily
min/max/mean. A valid channel does not prove every source date or other channel
is complete. Failed source ingestion preserves previously good stored channels.

Requests enforce cumulative caps:256 attempted GETs including aliases,
8MiB stored bytes,16MiB decoded bytes,100000 input points,4096 returned native
points,256KiB structured output and512KiB serialized MCP envelope. Individual
new signal objects are capped at256KiB stored/1MiB decoded; legacy sleep/HRV
reads at512KiB/2MiB for this reader. Oversized output/budget exhaustion is
explicit, never presented as complete or as no data. The ten-second deadline
is checked before/after each await: R2 GET has no abort parameter, so this is a
result/initiation deadline rather than a hard response-settlement guarantee.
All reads remain authenticated and stored-only, with no provider calls or writes.

## Collection and verification scope

Signal publication uses the existing guarded R2Store budgets. It adds at most
one prior-signal GET and one changed-object PUT per collected source day;
byte-identical objects skip PUT. A synthetic14-day ordinary refresh measured
14GET/14PUT and3742 compressed signal bytes with the existing61 SDK /66
connectapi calls unchanged. These tiny invented curves do not estimate real
health payload sizes, runtime cost or live provider availability.

Tests include public collection/storage commands, exact Python-produced gzip
objects through authenticated JSON/SSE readers, independent DST/bin/minimum
examples, malformed/missing source semantics and fixed request bounds.
App HTTP tests can run in Node with explicit synthetic platform adapters;
those results do not establish native Cloudflare R2, Durable Object SQLite or
edge behavior. The normal locked Linux Workers-runtime and full generated-type
checks remain required before release. No schedule, service, quota or dependency
installation is changed by this feature.
Detailed HRV readers also retain optional `time_basis`, `timestamp_field` and
`timestamp_unit` fields from new canonical readings. HRV's time-dependent slope
uses only documented absolute instants; local-only/ambiguous numeric observations
still contribute to untimed source-value summaries. Index builder revision3
records the corrected calculation. Matching revision2 indexes remain readable,
but their slope is null with `legacy_index_hrv_time_basis_unverified` until a
bounded ordinary index refresh or canonical read-through supplies verified data.
A valid revision2 reconciliation receipt remains eligible for the existing
recent-month path. This does not trigger an automatic full-history migration;
the existing adjacent-month policy can cover up to three months per stream.

Failed decodes are charged against a conservative cumulative decode budget in
`decoded_budget_bytes`; `decoded_bytes` counts only successfully decoded text.
`envelope_bytes` measures the serialized JSON-RPC envelope and
`envelope_budget_bytes` includes reserved transport framing space. These are
not a measurement of network headers or a complete Cloudflare billing estimate.
A synthetic31-night, eight-hour,30-minute-bin example used93GETs,930186 stored
source bytes and10416 input points;992 returned bins fit the512KiB response
cap (515738 bytes JSON and515644 bytes SSE in the Node HTTP adapter). This is
one synthetic shape. Native or denser15-minute requests may need shorter
chunks because the same fixed caps still apply.

## Retained observations, provenance and source quality

A later dated response can be partial even when it contains some valid values.
Ordinary collection merges valid observations by absolute timestamp: a missing,
null or HR-zero sentinel does not erase a previously good observation. A valid
new value replaces the prior value at that instant; Body Battery zero remains a
valid value. Conflicting duplicates do not select an arbitrary value. Units,
original timestamps and their source fields remain explicit.

`preserve-good-observations-v1` records incoming, retained, new and replaced
counts, bounded source revisions and unknown source completeness. Individual
observations identify their source revision and whether they are incoming or
retained. Unknown prior provenance remains unknown. Revision summaries contain
at most16 hashes, with an explicit true revision count/truncation marker; they
do not grow without limit. Identical repeated source payloads reuse the same
composition and skip byte-identical publication. Existing object/write budgets
and point/byte bounds still apply before publication.

The reader projects only allow-listed `source_quality_flags`, a bounded
`source_missing_value_count` and dated `source_compositions`. Missing counts are
for the source calendar days; the provider may not locate each missing value
inside the sleep interval. Retained/missing/unknown-completeness source flags
make the combined result partial. Retention can improve observed cadence-slot
coverage without proving a fresh complete provider recording. A readable legacy
channel without composition metadata is not certified complete. Stored source
failures are not zero measurements and are not absence.

Clock provenance is bounded to small clock-shaped primitives. Bounded original
calendar strings remain visible for diagnosis even when the calendar is invalid;
`invalid_source_clock` distinguishes them from a valid instant. Oversized or
unrelated strings are not echoed. Stored source bytes remain unchanged. Output
limits are rechecked after reducing curves and after clearing excessive source
metadata; a fixed small control response is the final safeguard. Measured HTTP
response bytes remain distinct from the estimated JSON-RPC envelope and reserved
framing allowance.

HRV night display chooses explicit HRV night zone, then an explicit matching
sleep zone, then HRV/source sleep wall-clock evidence, then configured home
projection. A source wall clock cannot displace an explicit actual night zone.
When matching sleep supplies the display window, `source_time` retains the
HRV's original clocks and `night_context_source_time` records the selected sleep
clocks; `night_context_stream` identifies that window source. Discovery may read
matching sleep for wall-clock-only HRV, within the existing31-day daily limit,
but an explicit own HRV zone requires no such fallback read. No source objects
or previous analyses are rewritten by these read-side corrections.

## Body Battery and stress channels

`night_signals` and `night_signals_history` accept an optional `channels` array with distinct values from `hr`, `hrv`, `body_battery` and `stress`. The default remains HR and HRV. Only requested channels are returned, under the same shared point, byte, operation and deadline limits.

Body Battery and stress use Garmin's recorded 0–100 unit points. Zero is a valid measurement; Garmin stress sentinels -1/-2 are missing readings. The reader does not reconstruct a curve from daily summaries, interpolate gaps or interpret these channels as medical findings. Provenance, source-day missing counts and quality flags remain visible.

Body Battery uses the already fetched ordinary summary source. Stress collection uses one strictly bounded GET for each of at most two latest eligible source dates in an ordinary refresh. The exact fitness endpoint and parameters are allowlisted; retries, redirects and generic SDK routes remain blocked. No new schedule or historical backfill is introduced. An unavailable response preserves valid previously stored observations.

A synthetic ordinary refresh measured 63 SDK calls, 66 connectapi calls and two separately guarded fitness HTTP GETs, with no auth call. The two tiny stress replies totalled 215 bytes. Within that recorded synthetic fixture, signal storage grew from 4146 to 4385 compressed bytes (239 bytes); ordinary refresh storage counts stayed at 26 GET / 0 HEAD / 3 LIST / 24 PUT. The fixture builder also stores synthetic sleep/HRV/index objects, for 30 GET / 2 HEAD / 5 LIST / 30 PUT overall, with 10928 versus 11167 uploaded bytes. These invented payloads do not establish live availability or cost. Repeating identical input avoids signal PUTs.

A populated synthetic 31-night compact read measured 93 GETs, 31 context LISTs, 10416 processed points and 992 returned bins. Its structured result was 181170 bytes; the component's largest observed wire result was 384245 bytes. Output stays compact under the original limits; denser requests can still require shorter chunks.

Current explicit night context is described in [NIGHT_USER_CONTEXT.md](NIGHT_USER_CONTEXT.md). Bounded offline personal reference products are described in [NIGHT_REFERENCE_CURVES.md](NIGHT_REFERENCE_CURVES.md).
