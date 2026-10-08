# Training Effect from existing activity data

## Source inventory

This inventory is based on the current code and the installed, pinned Garmin
Connect 0.3.17 / FIT SDK 21.217.0. It does not establish personal historical
coverage: no Garmin, R2, personal exports or raw historical files were inspected.

| Raw field / location | Existing fetch path | Actually retained today | Meaning / scale | Association and limitation |
| --- | --- | --- | --- | --- |
| Activity-list Training Effect DTO candidates; detail `summaryDTO` variants | `sources/garmin.py::fetch` calls the guarded `get_activities_by_date`; targeted import already calls guarded `get_activity` | Summary `Activity` / CSV has no Training Effect fields. `decode_fit` selects only activity identity/type/clocks from the DTO; no DTO Training Effect is persisted | SDK returns DTOs without a typed, versioned field definition. Candidate names alone are insufficient evidence | Activity ID exists, but no retained TE source. No DTO fallback or new endpoint is introduced |
| `messages.session_mesgs[].total_training_effect` (session 18, field 24) | Existing ORIGINAL download → `granular.decode_fit` → `granular_export.export_activity` | Entire decoded `messages` in gzip `activity.v1.json` | FIT uint8, scale 10, offset 0, no physical unit; Garmin Training Effect scale 0–5. Projected as aerobic TE, retaining the exact FIT field name | Only one session with a verified activity ID and matching UTC start. Device/firmware coverage unknown |
| `messages.session_mesgs[].total_anaerobic_training_effect` (session 18, field 137) | Same existing ORIGINAL path | Same existing decoded object | FIT uint8, scale 10, offset 0; anaerobic TE, 0–5 | Same checks; absence never proves device non-support |
| `messages.session_mesgs[].training_load_peak` (session 18, field 168) | Same existing ORIGINAL path | Retained when present in decoded messages | FIT sint32, scale 65536, offset 0; profile provides no unit or sufficiently precise activity-load definition | Not projected. Not relabelled as activity load, CTL, ATL or TSB |
| Activity DTO `activityTrainingLoad` candidate | Existing activity DTO fetches | Not retained in summary or decoded activity metadata | SDK references a removable field but does not define its meaning/unit | Not projected; unknown load does not block TE |
| Normalized TCX `activity.endurance.v1.json` | Existing TCX download → `normalize_endurance_session` | Explicit summary/laps/splits/HR/time series | No Training Effect source in this normalizer | TCX metrics cannot reconstruct Garmin TE |
| Existing Coach Input | Existing stored FIT + endurance + TCX read by Python coach preparation | Explicit allowlisted analysis; no TE | Analyzer/version/fingerprint unchanged by this delivery | Immutable historical analyses are neither rewritten nor augmented |

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

Strength reads reuse the FIT object they already loaded. Endurance reads use
the existing bounded JSON reader for the activity's canonical FIT JSON after
the existing TCX-derived object is found: normally one extra GET, at most two
GET attempts for the `.json` / `.json.gz` aliases when absent. No new HEAD,
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
the unchanged storage format. A future Coach Input addition must use a reviewed
analyzer/input version and existing immutable fingerprint mechanism; adding
that now would create avoidable historical revision pressure.

The method is `fit_recorded_estimate`: these values are estimates recorded in
the FIT session, not directly measured physiology or a Slipstream calculation.
The source identifies the existing Garmin download/decoder path. That path
alone cannot prove the estimator's manufacturer for third-party FIT uploads
later imported into Garmin Connect; no such device provenance is invented.
Training Effect is for training discussion, not a diagnosis or a recovery
guarantee. It does not modify user thresholds, profiles, RPE,
source freshness, completeness, polling, TTLs, deduplication or budgets.

## Reproduce local validation

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
