# User context for a night

Use the morning `wake_date` to identify a night. `night_of` is the local date on which sleep began. Context is supplied by the user, never inferred from Garmin estimates.

`add_night_context` accepts at least one explicit field: `alcohol`, `late_bedtime`, `travel`, `actual_night_timezone`, `illness`, `sleep_quality`, `morning_condition` or `note`. Boolean fields accept true/false/null. Quality and morning condition use integers 1–5 or null. The optional IANA timezone applies to this night. A note is at most 1000 characters.

For example, after the user asks to record it:

```json
{"name":"add_night_context","arguments":{"wake_date":"2026-10-26","travel":true,"actual_night_timezone":"Asia/Tokyo","morning_condition":3}}
```

Each accepted write creates an immutable event. Omitted fields preserve the previous effective value; explicit null clears a field. `declared_fields` records fields explicitly set across the effective history, while `changed_fields` belongs to this event. Context values and event revisions remain separate from source-data freshness and analysis versions. An explicitly cleared timezone is unknown, rather than silently restored from an earlier event.

`night_context_history` reads one night, with `limit` 1–20 (default 10) and optional `before_context_id` pagination. Newest events come first. No events means `not_stored`; unreadable or unverified history means `unavailable`, which does not prove absence.

Detailed sleep/HRV and daily history reads return current effective `user_context` and separate `view` revision/status fields. Daily health resolves at most the most recent 31 distinct returned wake-dates and marks rows outside that scope. Weekly summaries explicitly do not resolve each night's context. Signal reads account for context operations inside their existing cumulative budgets. A context read failure remains visible independently of a successfully read curve.

An explicit user timezone can determine the night display. Without that declaration, the reader uses documented actual-zone evidence before Garmin wall-clock fallback and configured projection. Original HRV source clocks and the selected matching-sleep fallback clocks are retained separately; a display correction does not alter stored source measurements.

Writes require the existing authenticated write mode, verified association with stored sleep/HRV, an actor budget and a durable per-night reservation. Events are capped at 100 per night; each stored event is at most 16KiB, decoded 32KiB. Concurrent writes, stale state and uncertain storage results return controlled errors; a successful store followed by a failed readback is still reported as saved. These limits do not imply unlimited retention or provider completeness.

Context reads and writes do not contact Garmin. They do not rewrite health source objects or frozen analysis history. The local application tests exercise production reservation SQL through a synthetic Node SQLite adapter; native Cloudflare lifecycle behavior and ordinary Linux CI remain separate release checks.
