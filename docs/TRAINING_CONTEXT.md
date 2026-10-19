# Training Effect, running measurements and stored Coach context

Daily Readiness and Recovery Time are a separate default-off additive source;
see [stored training days](TRAINING_READINESS.md). Training Effect below continues
to use the existing activity/FIT path without new fetching or reinterpretation.
The [Coach snapshot](#stored-pre-activity-context-in-coach-input) can use these
training days only when they are already stored; it does not enable collection.

## Source inventory

This Training Effect inventory was established with Garmin Connect 0.3.17 /
FIT SDK 21.217.0; current dependency pins are in `requirements.txt`.
It does not establish personal historical
coverage: no Garmin, R2, personal exports or raw historical files were inspected.

| Raw field / location | Existing fetch path | Actually retained today | Meaning / scale | Association and limitation |
| --- | --- | --- | --- | --- |
| Activity-list Training Effect DTO candidates; detail `summaryDTO` variants | `sources/garmin.py::fetch` calls the guarded `get_activities_by_date`; targeted import already calls guarded `get_activity` | Summary `Activity` / CSV has no Training Effect fields. `decode_fit` selects only activity identity/type/clocks from the DTO; no DTO Training Effect is persisted | SDK returns DTOs without a typed, versioned field definition. Candidate names alone are insufficient evidence | Activity ID exists, but no retained TE source. No DTO fallback or new endpoint is introduced |
| `messages.session_mesgs[].total_training_effect` (session 18, field 24) | Existing ORIGINAL download → `granular.decode_fit` → `granular_export.export_activity` | Entire decoded `messages` in gzip `activity.v1.json` | FIT uint8, scale 10, offset 0, no physical unit; Garmin Training Effect scale 0–5. Projected as aerobic TE, retaining the exact FIT field name | Only one session with a verified activity ID and matching UTC start. Device/firmware coverage unknown |
| `messages.session_mesgs[].total_anaerobic_training_effect` (session 18, field 137) | Same existing ORIGINAL path | Same existing decoded object | FIT uint8, scale 10, offset 0; anaerobic TE, 0–5 | Same checks; absence never proves device non-support |
| `messages.session_mesgs[].training_load_peak` (session 18, field 168) | Same existing ORIGINAL path | Retained when present in decoded messages | FIT sint32, scale 65536, offset 0; profile provides no unit or sufficiently precise activity-load definition | Not projected. Not relabelled as activity load, CTL, ATL or TSB |
| Activity DTO `activityTrainingLoad` candidate | Existing activity DTO fetches | Not retained in summary or decoded activity metadata | SDK references a removable field but does not define its meaning/unit | Not projected; unknown load does not block TE |
| Normalized TCX `activity.endurance.v1.json` | Existing TCX download → `normalize_endurance_session` | Explicit summary/laps/splits/HR/time series | No Training Effect source in this normalizer | TCX metrics cannot reconstruct Garmin TE |
| Coach Input | Existing stored FIT + endurance + TCX read by Python coach preparation | Engine 1.4.0 adds neutral `training_effect` from the same single verified FIT session | Engine 1.2.0 adds recorded laps/distance segments; engine 1.3.0 adds a [stored health-context snapshot](#stored-pre-activity-context-in-coach-input). New 1.4.0 output retains context when eligible | Immutable historical analyses retain their original bytes and engine; no engine-only history queue |

Garmin's [Training Effect manual](https://www8.garmin.com/manuals/webhelp/forerunner935/EN-US/GUID-7275629E-743A-4658-A284-C84F42A66AE5.html)
documents the aerobic/anaerobic interpretation and 0–5 scale. The conventional
aerobic mapping uses the FIT session's legacy `total_training_effect` alongside
the explicitly named anaerobic field; it is not a sum of the two values. Exact
field representation comes from the pinned installed `profile.py`, `fit.py` and
`decoder.py`, with synthetic encoding/decoding tests. The
[official FIT Python SDK](https://github.com/garmin/fit-python-sdk#read-method)
documents the decoder's default scale/offset application; the
[FIT overview](https://developer.garmin.com/fit/overview/) identifies the profile
as the format reference. No third-party source code is copied: the already
installed FIT SDK is used under its existing FIT Protocol License.

## Read contract and cost

`strength_session` and `endurance_session` add one optional top-level
`training_context` object. It projects only the two explicit session fields,
finite 0–5 values, fixed provenance and validated clocks. Raw messages, GPS,
DTOs and source text do not enter this object. Existing names, inputs, session
objects, availability and payload limits are retained.

Strength reads reuse the FIT object they already loaded. Endurance reads resolve
the activity clock and read canonical FIT JSON before the TCX-derived object.
Both optional FIT projections share that bounded read: normally one GET, at
most two GET attempts for the `.json` / `.json.gz` aliases when absent. No new HEAD,
LIST, PUT, Garmin calls or stored bytes. No list-row reads, index, dependency,
migration, scheduled job or importer change. Optional FIT read/parse/size errors
are reported as `source_unreadable`; they do not fail the TCX dataset or log
private exceptions. Existing FIT read limits still apply.

Only schema 1, `source=garmin-fit`, zero decode errors and exactly one FIT
session are accepted. The canonical activity ID and GMT start must match the
requested summary and the session start. Multi-session files remain explicitly
ambiguous even if one session shares the activity start: selecting the first,
summing or averaging would misrepresent multisport data. Local date/time comes
only from the validated canonical Garmin-local clock, never the CSV's UTC date.
The session timestamp is the estimate's measurement/end time when valid;
unknown times remain null.

Each metric distinguishes `available` (including 0), `missing`, `invalid` and
`unavailable` due to source/association failure. Null and absence are missing
with unknown support. The SDK omits raw uint8 invalid sentinel 255 before
scaling, so a persisted missing value cannot distinguish an original sentinel
from an absent field. Persisted numeric 255, 25.5, out-of-range, non-finite,
string, boolean and non-tenth values are invalid, never rescaled or coerced.

There is one accepted source, the verified FIT session. Conflicting DTO fields,
developer fields, lap values or pre-existing arbitrary context objects are
ignored; they are not verified alternative sources and are not exposed as
conflicts. No currently retained second TE source can be compared. A future
DTO source requires independent format evidence, compact conflict reporting
and an explicit priority review before it can be used.

Old objects containing these session fields gain context by pure reading. Old
objects without them return missing/unknown support; unknown schema, malformed
objects and ambiguous associations are distinct failures. No re-decoding,
backfill or regeneration is required or performed. New ordinary imports use
the unchanged storage format. The separate recorded running measurements below
use versioned Coach identities; they do not add Training Effect to old analyses.

The method is `fit_recorded_estimate`: these values are estimates recorded in
the FIT session, not directly measured physiology or a Slipstream calculation.
The source identifies the existing Garmin download/decoder path. That path
alone cannot prove the estimator's manufacturer for third-party FIT uploads
later imported into Garmin Connect; no such device provenance is invented.
Training Effect is for training discussion, not a diagnosis or a recovery
guarantee. It does not modify user thresholds, profiles, RPE,
source freshness, completeness, polling, TTLs, deduplication or budgets.

## Recorded running measurements

The session-only block below remains the historical schema-1 contract. New
engine `1.2.0` analyses and newly exported stored details can additionally expose
recorded laps and source-covered distance segments; see the [schema-2 policy](COACH_INPUT_VERSIONING.md#recorded-laps-and-distance-segments-engine-120).

`endurance_session` exposes an optional `recorded_running_metrics` sibling of
the existing TCX-derived session and Training Effect context. The initial Coach
addition used the same block with analyzer version `1.1.0`; outer schema and
v1 storage paths remain unchanged. The normalized TCX dataset and its stored
bytes also remain unchanged.

| Field | Recorded FIT session field | Unit |
| --- | --- | --- |
| `average_power` | `avg_power` | W |
| `average_step_length` | `avg_step_length` | mm |
| `average_ground_contact_time` | `avg_stance_time` | ms |
| `average_vertical_oscillation` | `avg_vertical_oscillation` | mm |
| `average_vertical_ratio` | `avg_vertical_ratio` | % |

The decoder has already applied FIT scaling. These are recorded means, with no
second scaling, cadence-to-stride conversion, inferred values or new thresholds.
Each field includes `status`, `value`, `unit` and `source_field`: a finite number
is `available`, including zero; absent/null is `missing`; strings, booleans and
other malformed/nonfinite values are `invalid`; failed source association makes
it `unavailable`. Every nonavailable value is null. Missing values never become
zero.

The block binds the numeric Garmin activity ID, source FIT hash, exact session
UTC start and available validated Garmin-local clock/date. Measurement time is
the recorded session timestamp when valid, never object upload time or now.
One running activity/session must match the identity and clocks; conflicting
sports, decode failures and ambiguous sessions stay explicit. A missing local
clock stays null rather than being inferred from UTC. The TCX first trackpoint
need not equal FIT session start. Failure of these optional fields does not
invalidate the existing TCX session or Coach analysis. Any declared TCX activity
ID must also agree. The block includes no raw messages or GPS.

Supported values already present in stored FIT can appear in endurance reads
without a new download or historical regeneration. Absence does not prove that
the device cannot supply them. Existing `1.0.0`/`1.0.1` Coach analyses retain
their original bytes, identities and engine labels and may omit this block.
Reading them never injects fields; upgrading the engine alone does not queue
historical recalculation. See [Coach versioning](COACH_INPUT_VERSIONING.md).

Both endurance projections reuse the same bounded canonical FIT JSON read.
Extraction adds no separate provider/storage operation, while response and new
analysis payloads contain additional bytes. No total cost or runtime guarantee
is implied. New canonical/pointer publication retains its normal write and byte
cost under existing budgets. Reads do not start a sync or write stored analyses.

## Stored pre-activity context in Coach Input

Engine `1.3.0` adds `stored_context_snapshot` when generation can bind a running
activity to its verified FIT session start and Garmin-local activity date. It
reads existing normalized sleep, HRV and optional training-day objects for that
exact date. It does not fetch Garmin, search neighbouring nights, enable
Readiness/Recovery or choose a historical training window. Without a verified
activity clock, generation retains the engine `1.2.0` path without this block.

The snapshot is neutral input for an external coach, not a Slipstream analysis
or recommendation. Recorded sleep duration, score, respiration, SpO2 and stress
and the known HRV summary fields retain explicit units and per-field status.
There are no raw sleep stages, HRV readings, provider DTOs or device IDs in this
block. Garmin estimates do not replace the user's confirmed zones or thresholds;
the separately stored `user_context` remains the user's own RPE, notes and context.
New engine `1.4.0` also includes aerobic/anaerobic `training_effect` from already
stored decoded FIT sessions. Its source is the neutral `fit_session`, method
`fit_recorded_estimate`, and scale `0_to_5`; it does not claim a third-party
FIT file's estimator manufacturer. Exact activity/session/local-day association,
source digest and known observation clocks accompany the values. Available
values are already-decoded tenths on the inclusive 0–5 scale; actual zero is
valid. Missing/invalid fields and unverified associations stay explicit, without
activity-list DTO fallback, lap summing, coaching labels or provider calls.

New 1.4.0 analyses have this block with or without eligible stored health
context. Earlier 1.0–1.3 analyses retain their own fields, IDs and stored bytes.
Unchanged or forced same-input generation reuses the canonical identity; changed
recorded inputs can append a new one. Ordinary bulk runs do not regenerate old
history merely to add Training Effect or new context.

### Which clocks mean what

| Clock | Meaning | Not evidence of |
| --- | --- | --- |
| Observation or completed-night clocks | When the stored value was observed, or the local night began/ended | When Slipstream checked Garmin |
| `source_checked_at` | A verified check for the selected stored source; retained fields keep their own check | A new observation, current device state or analysis creation time |
| `snapshot_at` | Explicit, stable provenance clock for the selected snapshot, or null when unknown | Data being known before the workout, execution-now or a live source check |

`semantics: retrospective_pre_activity` describes attribution, not historical
foreknowledge. Sleep/HRV must have the workout's local wake-date and a completed
night ending no later than verified activity start. Paired Garmin-local clocks
take precedence; explicit configured timezone is a fallback only when both local
clocks are absent. Wrong dates, missing/unknown clocks and nights ending later
remain distinct unavailable states instead of being backdated or filled with zero.
A source checked after the workout can still describe the preceding night;
the later check time stays visible.

For an already-stored training day, only its selected device is considered.
The latest eligible stored `latest` or `morning` observation before the activity
is used and its slot is labelled. This does not reconstruct every intraday value.
Multiple-device ambiguity never merges complementary fields from different
devices. Readiness and Recovery retain their own observation, check, retention
flag and local-time provenance. A retained value with unknown or post-activity
time is suppressed, not assigned the newer field's clock. Recovery zero remains
valid; recovery minutes are not a server-side countdown.

Missing, invalid, unavailable and not-collected values remain separate. None is
proof that a watch lacks a feature: no watch-model capability registry is used.
Sparse devices remain readable alongside richer devices, without inventing values.
Readiness activation remains a separate installer decision. VO2max, Garmin
thresholds, Training Status/Load Focus and a training-history context window are
not delivered by this snapshot.

### Generation, reading and bounded work

Legitimate new/pending calculations and an explicitly requested regeneration
of one activity can select these stored sources into a new immutable version.
Ordinary completed historical bulk work does not regenerate analyses merely
because a later sleep/HRV receipt or context source appears. Old analyses are
not enriched during a `coach_input` read; JSON/SSE returns the selected stored
version. See [identity and history preservation](COACH_INPUT_VERSIONING.md#stored-context-snapshots-engine-130).

Generation reads at most five exact-day keys: the training day, sleep, HRV and
the two sleep/HRV check receipts. This is bounded per-generation read work, not
a new provider call or full-bucket scan. A bulk invocation can share that result
between new activities on the same local date; the cache does not survive into
another invocation. Existing read/byte/write budgets still apply, and unknown
storage, authorization or budget failures stop publication instead of becoming
absence. Additional source reads and snapshot bytes are not free, and these
bounds do not guarantee total backfill cost, live latency or device coverage.

## Reproduce local validation

The historical Training Effect-only measurements below predate the recorded
running block. They do not measure engines `1.1.0`–`1.3.0`, their larger payloads or the
combined feature set.

With the unchanged locked dependencies installed, run `pytest`, `ruff check .`,
then from `worker` run `npm run typecheck` and `npm test`. Runtime tests generate
invented FIT with the pinned SDK, export it through the actual pipeline, load
the exact gzip bytes into local R2 and call the authenticated Worker. They fail
on any network request except the mocked synthetic Access certificate endpoint.
No test uses a Garmin account or a production bucket.

From `worker`, after `npm test`, run:

```text
node scripts/benchmark-training-context.mjs <verified-full-baseline-sha>
```

The benchmark reads the actual baseline detail-handler source from Git and
compares it with the new handler over identical generated bytes (1,800 synthetic
record messages, one session). Unchanged storage/security/summary helpers are
verified before reuse. It asserts identical pre-existing response fields.
Per-tool measurements distinguish cold versus warm summary cache. Each pair
uses 100 alternating-order samples after warmup; projection alone uses 10,000.
Operation/byte counts are deterministic. Wall timing includes bounded reads,
gzip decompression, JSON parse and serialization in warm Node, using in-memory
R2. Process CPU includes runtime/background threads and Windows timer noise;
it is not Cloudflare billed CPU. Network, retries, queueing and process startup
are excluded. Raw FIT is never decoded at read time. Results are synthetic
cost evidence, not production speed or free-tier claims.

Local synthetic run on 2026-10-08, Windows, Node 24.19.0:

| Tool / summary cache | GET before → after | MCP result bytes before → after | Mean wall ms before → after | Mean process CPU ms before → after |
| --- | --- | --- | --- | --- |
| Endurance / cold | 2 → 3 | 7,539 → 8,971 | 0.4761 → 1.8142 | 0.77 → 3.14 |
| Endurance / warm | 1 → 2 | 7,539 → 8,971 | 0.4388 → 1.4858 | 0.62 → 2.97 |
| Strength / cold | 2 → 2 | 1,127 → 2,559 | 0.8899 → 1.1221 | 1.40 → 1.10 |
| Strength / warm | 1 → 1 | 1,127 → 2,559 | 1.0979 → 1.2719 | 1.26 → 2.34 |
| List / cold | 1 → 1 | 612 → 612 | 0.0233 → 0.0249 | 0 → 0.15 |
| List / warm | 0 → 0 | 612 → 612 | 0.0091 → 0.0076 | 0 → 0 |

Every row has HEAD 1 → 1 and Garmin/LIST/PUT 0 → 0. Endurance downloads
10,252 additional stored bytes (FIT JSON expands to 140,713 bytes); the other
paths download no additional bytes. Detail result size increases by 1,432
bytes, counting both structured content and the existing formatted text copy,
excluding HTTP/JSON-RPC framing. Storage and raw-FIT decode changes are zero.
Projection alone averages 0.0105 ms wall and 0.0109 ms process CPU. Small
negative timings in unchanged paths are measurement noise, not speed gains.
Real file sizes, cold runtime startup, edge CPU and R2 latency are unmeasured.
