# Authoritative MCP tool catalog

Generated from the authenticated Worker `tools/list` response, including domain modules. Do not edit tool entries by hand.

| Profile writes | Refresh configured | Registered tools |
| --- | --- | --- |
| off | no | 19 |
| off | yes | 23 |
| on | no | 21 |
| on | yes | 25 |

Profile writes and refresh availability are independent. Disabling profile writes does not disable sync. Every mode still requires authentication and existing server limits. Annotations describe intent; server validation, transport controls and budgets enforce safety. No tool allows Garmin fitness-account writes.

## Data limits and runtime rules

- Summary coverage follows stored indexes and retained data; there is no fixed 30-day activity-history limit. Status tools describe summary coverage, not detailed-package readiness. Null/missing values are unknown, never zero. Summary dates alone do not establish an activity's Garmin-local day.
- Detail tools return normalized, GPS-free datasets. There is no tool for arbitrary URLs/R2 keys or raw FIT/TCX download. Summary availability does not guarantee detailed streams or Coach Input. Planned workout steps differ from executed laps.
- Sleep/HRV history: inclusive range at most 366 days; auto is daily through 31 days and weekly thereafter. Explicit daily summary is at most 31 days; full readings/stages at most 7 days and require daily/auto, never weekly. Dates must be real calendar dates in ascending order. Weekly history verifies recent seven days directly; older index-only rows are not proof of complete raw detail. Report coverage, per-day status and index_consistency.
- Night dates are Garmin-local wake-dates; night_of is the local sleep-start date. Use returned local timestamps/provenance, matching sleep context for HRV and IANA/DST fallback. Missing context stays unknown; do not manually shift dates or apply a fixed offset. Garmin HRV summaries and Slipstream-derived statistics have different provenance.
- Weight history: inclusive range at most 31 days; same-day local window with morning_start < morning_end; explicit timezone must be IANA-valid. Default window is [04:00,12:00). Select the earliest actual local weighing in the window. Daily averages, daily_health.weight_kg and later weighings never replace a missing morning value. Report selected_days, per-day status and time provenance.
- Targeted fresh activity/night requests accept only the server's recent-date window (last seven days or today's Garmin-local day, with a UTC boundary allowance). Supply expected_date/activity_id or wake_date to pin the request, especially during travel. R2 is checked first; existing source can support derived-data repair, and compatible jobs share IDs. Use sync_status.data_state=ready, matching ID/date, source_checked_at and missing_components. A successful job or legacy data_ready alone is insufficient.
- Follow refresh_status with the same targeted request_id only while should_continue_polling is true, respecting poll_after_seconds and retry_after_seconds. Targeted jobs allow three short polling windows. Stop on exhaustion, cooldown, source-pending, blocked state or 429; honor Retry-After and never start a replacement job to extend polling.
- Profile zones must be ordered and non-overlapping (30–250 BPM); context needs at least one non-null user field. Values/RPE/thresholds are user supplied. Versions are immutable; changed zones do not rewrite historical analyses. Existing payload, concurrency and write budgets also apply.
- Diagnostics may be absent for a read-only result, and legacy timings may be null/omitted. This is not a failed sync and does not authorize another job. Inventory timing includes/overlaps LIST timing: do not sum them. No later automatic follow-up exists unless a mechanism is separately agreed.

## Updating and detecting drift

From worker, run `npm run test:runtime -- test-runtime/tool-catalog.test.ts`. Existing CI's `npm test` includes this check; no new workflow, schedule, permissions or live service access is needed. After an intentional contract change, regenerate with `npm run test:runtime -- test-runtime/tool-catalog.test.ts -u`, review this file and the existing contract digest snapshots, then rerun without -u. Never update snapshots merely to hide unexplained drift.

The test obtains real registrations in all four availability modes, checks incomplete refresh configuration, compares this deterministic file, and validates workflow JSON examples against the registered input schemas. Input-schema checks do not replace handler refinements; existing history/weight/runtime tests cover those bounds. Unknown/obsolete names fail. Workflow scenario tests are synthetic contracts, not evidence of AI-client acceptance.

See [AI workflows](AI_WORKFLOWS.md), [prompt ideas](PROMPTS.md), [fresh data](FRESH_DATA.md) and [history semantics](HEALTH_HISTORY.md).

### `activity_stats`

Totals (distance, time, elevation, calories, avg HR). Optionally group_by sport/month/year.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "group_by": {
      "enum": [
        "sport",
        "month",
        "year"
      ],
      "type": "string"
    },
    "sport_type": {
      "description": "Garmin sport type, e.g. \"Run\", \"Ride\", \"Swim\", \"Yoga\"; \"Running\"/\"Løping\" and \"Cycling\"/\"Sykling\" are also accepted",
      "maxLength": 64,
      "minLength": 1,
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `add_activity_context`

Append user-supplied RPE, conditions, note or workout correction to one activity. Call only when the user explicitly asks to record this information. The values are never inferred and existing context is never overwritten.

**Effect:** Explicit user confirmation required; append-only profile/context write to private R2; no Garmin account write.

**Availability:** MCP_WRITES_ENABLED must be literal true.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "activity_id": {
      "pattern": "^(garmin-)?\\d{1,20}$",
      "type": "string"
    },
    "conditions": {
      "anyOf": [
        {
          "maxLength": 240,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "note": {
      "anyOf": [
        {
          "maxLength": 2000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    },
    "rpe": {
      "anyOf": [
        {
          "maximum": 10,
          "minimum": 0,
          "type": "number"
        },
        {
          "type": "null"
        }
      ]
    },
    "workout_correction": {
      "anyOf": [
        {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "activity_id"
  ],
  "type": "object"
}
```

### `add_coach_profile`

Save a new immutable, versioned running analysis profile after the user explicitly supplies or confirms their heart-rate zones and threshold references. Never infer these values. effective_from prevents new zones from rewriting historical analyses.

**Effect:** Explicit user confirmation required; append-only profile/context write to private R2; no Garmin account write.

**Availability:** MCP_WRITES_ENABLED must be literal true.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "effective_from": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "name": {
      "default": "Running heart-rate profile",
      "maxLength": 80,
      "minLength": 1,
      "type": "string"
    },
    "references": {
      "properties": {
        "interval_thresholds_bpm": {
          "default": [],
          "items": {
            "maximum": 250,
            "minimum": 30,
            "type": "integer"
          },
          "maxItems": 20,
          "type": "array"
        },
        "lt1_max_bpm": {
          "anyOf": [
            {
              "maximum": 250,
              "minimum": 30,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "lt1_min_bpm": {
          "anyOf": [
            {
              "maximum": 250,
              "minimum": 30,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "lt2_max_bpm": {
          "anyOf": [
            {
              "maximum": 250,
              "minimum": 30,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "lt2_min_bpm": {
          "anyOf": [
            {
              "maximum": 250,
              "minimum": 30,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "type": "object"
    },
    "zones": {
      "items": {
        "properties": {
          "label": {
            "maxLength": 24,
            "minLength": 1,
            "type": "string"
          },
          "max_bpm": {
            "maximum": 250,
            "minimum": 30,
            "type": "integer"
          },
          "min_bpm": {
            "maximum": 250,
            "minimum": 30,
            "type": "integer"
          }
        },
        "required": [
          "label",
          "min_bpm",
          "max_bpm"
        ],
        "type": "object"
      },
      "maxItems": 10,
      "minItems": 1,
      "type": "array"
    }
  },
  "required": [
    "effective_from",
    "zones",
    "references"
  ],
  "type": "object"
}
```

### `body_composition`

Read all Garmin body-composition measurements stored for one date, including weight, BMI, body fat, water, muscle, bone mass and related scale metrics.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "required": [
    "date"
  ],
  "type": "object"
}
```

### `coach_input`

Read the newest deterministic coach-input analysis for one running activity. It includes the profile version, HR zones, drift, kilometre splits, Garmin workout plan and actual executed laps, plus explicitly supplied user context.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "activity_id": {
      "pattern": "^(garmin-)?\\d{1,20}$",
      "type": "string"
    }
  },
  "required": [
    "activity_id"
  ],
  "type": "object"
}
```

### `coach_profile`

Read the versioned heart-rate zones and threshold references used for coach-input analyses. A date selects the newest profile effective on that date; historical analyses keep their original profile.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `daily_health`

List daily health summaries. weight_kg is one Garmin daily value (normally latestWeight), not a computed daily mean or standardized morning measurement; use weight_history for the latter. For sleep/overnight HRV, date is the morning wake-date; sleep_night and hrv_night prefer Garmin's per-night local timestamps, with HEALTH_TIMEZONE as fallback.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "limit": {
      "default": 30,
      "maximum": 366,
      "minimum": 1,
      "type": "integer"
    },
    "sort": {
      "default": "date_desc",
      "enum": [
        "date_desc",
        "date_asc"
      ],
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `data_status`

Check the fitness data is connected; returns count, date range, sources.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {},
  "type": "object"
}
```

### `endurance_session`

Read a GPS-free analysis dataset derived from the Garmin TCX file for one endurance activity. Returns summary metrics, Garmin laps, kilometre splits, distance-half heart-rate drift, seconds per heart-rate BPM, and a compact 10-second trackpoint series. Includes optional recorded aerobic/anaerobic Training Effect estimates with one bounded stored FIT JSON read; missing context does not change dataset availability.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "activity_id": {
      "description": "Activity ID from list_activities, for example \"garmin-24444691902\"",
      "pattern": "^(garmin-)?\\d{1,20}$",
      "type": "string"
    }
  },
  "required": [
    "activity_id"
  ],
  "type": "object"
}
```

### `health_status`

Check daily Garmin health summaries: populated days and date range.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {},
  "type": "object"
}
```

### `health_trends`

Summarize health metrics over a date range, optionally grouped by month or year. Weight statistics use Garmin's one daily value (normally latestWeight), not standardized morning measurements; use weight_history for those.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "group_by": {
      "enum": [
        "month",
        "year"
      ],
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `hrv_curve`

Read detailed overnight Garmin HRV for one wake-date. Local night_of prefers Garmin's own timestamps and otherwise uses configured HEALTH_TIMEZONE, without GPS or raw device payloads.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "required": [
    "date"
  ],
  "type": "object"
}
```

### `hrv_history`

Analyze overnight HRV by morning wake-date. Daily rows use matching sleep's Garmin local night when HRV lacks its own timestamps, with night_context_stream showing provenance. Auto returns daily summaries for up to 31 days and compact wake-date weekly summaries for longer ranges (up to 366 days); use daily chunks for night-lag analysis. Full readings are limited to 7 days.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "detail_level": {
      "default": "summary",
      "enum": [
        "summary",
        "full"
      ],
      "type": "string"
    },
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "granularity": {
      "default": "auto",
      "enum": [
        "auto",
        "daily",
        "weekly"
      ],
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "required": [
    "start_date",
    "end_date"
  ],
  "type": "object"
}
```

### `list_activities`

List activities, newest first. Filter by sport/date/name; sort and limit.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "limit": {
      "default": 20,
      "maximum": 200,
      "minimum": 1,
      "type": "integer"
    },
    "name_contains": {
      "maxLength": 128,
      "minLength": 1,
      "type": "string"
    },
    "sort": {
      "default": "date_desc",
      "enum": [
        "date_desc",
        "date_asc",
        "distance_desc",
        "distance_asc"
      ],
      "type": "string"
    },
    "sport_type": {
      "description": "Garmin sport type, e.g. \"Run\", \"Ride\", \"Swim\", \"Yoga\"; \"Running\"/\"Løping\" and \"Cycling\"/\"Sykling\" are also accepted",
      "maxLength": 64,
      "minLength": 1,
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `list_sport_types`

Distinct activity types with counts.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {},
  "type": "object"
}
```

### `personal_bests`

Activity-level bests: longest distance, longest time, most elevation, fastest pace.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "sport_type": {
      "description": "Garmin sport type, e.g. \"Run\", \"Ride\", \"Swim\", \"Yoga\"; \"Running\"/\"Løping\" and \"Cycling\"/\"Sykling\" are also accepted",
      "maxLength": 64,
      "minLength": 1,
      "type": "string"
    }
  },
  "type": "object"
}
```

### `refresh_status`

Wait briefly for a general refresh, activity import or night sync without starting a new job. Run-only manual night checks validate the night report and canonical sleep/HRV. Read sync_status for job state, source-check time, completeness, freshness, missing components and required user action; use data_state=ready for readiness. Legacy run-only data_ready is workflow success. Null latency is unknown. Pass request_id from a targeted activity/night request (preferred), or the returned run_id. Targeted jobs permit three short polling windows in total. Stop polling when should_continue_polling is false, even if terminal is false; report the state and retry guidance. Ordinary status checks start no Garmin job.

**Effect:** Status read; may contact GitHub and update coordination/poll bookkeeping; starts no Garmin job.

**Availability:** GITHUB_ACTIONS_TOKEN and GITHUB_REPOSITORY must both be configured.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "request_id": {
      "format": "uuid",
      "pattern": "^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$",
      "type": "string"
    },
    "run_id": {
      "description": "GitHub Actions run ID returned by refresh_today or sync_latest_activity; omit only for a general latest-status check",
      "exclusiveMinimum": 0,
      "maximum": 9007199254740991,
      "type": "integer"
    }
  },
  "type": "object"
}
```

### `refresh_today`

Request an incremental Garmin refresh. Set new_activity_expected=true when the user says a recent workout is missing; this uses a 5-minute minimum interval instead of the normal 30-minute cooldown. Read sync_status for job state, successful Garmin check and actual package readiness. Legacy data_ready records workflow success; claim a complete fresh package only when sync_status.data_state is ready. Use targeted tools to verify a specific activity or night. This changes stored data and must only be called when the user explicitly asks to update or refresh their data. It cannot start a historical backfill. IMPORTANT: while should_continue_polling is true, call refresh_status with the returned run ID in the same conversation turn.

**Effect:** Explicit refresh authorization required; may dispatch a bounded job, contact Garmin and write private R2.

**Availability:** GITHUB_ACTIONS_TOKEN and GITHUB_REPOSITORY must both be configured.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "new_activity_expected": {
      "description": "True only when the user expects a recent workout that is not yet in Slipstream",
      "type": "boolean"
    }
  },
  "type": "object"
}
```

### `search_activities`

Free-text search over activity name, type, and source.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "limit": {
      "default": 20,
      "maximum": 100,
      "minimum": 1,
      "type": "integer"
    },
    "query": {
      "maxLength": 128,
      "minLength": 1,
      "type": "string"
    }
  },
  "required": [
    "query"
  ],
  "type": "object"
}
```

### `sleep_detail`

Read detailed Garmin sleep for one morning wake-date. Local night_of is the sleep-start date, using Garmin's local timestamps when available and HEALTH_TIMEZONE only as fallback. Includes window, stages and score without raw Garmin payload.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "required": [
    "date"
  ],
  "type": "object"
}
```

### `sleep_history`

Analyze sleep by morning wake-date. Daily rows expose local night_of and weekly output includes by_night_of_weekday for Friday/Saturday comparisons and a midpoint-shift proxy. Auto uses daily summaries up to 31 days and compact wake-date weekly summaries up to 366 days; full stages are limited to 7 days.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "detail_level": {
      "default": "summary",
      "enum": [
        "summary",
        "full"
      ],
      "type": "string"
    },
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "granularity": {
      "default": "auto",
      "enum": [
        "auto",
        "daily",
        "weekly"
      ],
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "required": [
    "start_date",
    "end_date"
  ],
  "type": "object"
}
```

### `strength_session`

Read normalized sets for one Garmin strength activity: exercise, reps, weight, active time and following rest. Includes optional recorded aerobic/anaerobic Training Effect estimates from verified stored FIT sessions. Raw FIT messages and GPS are not returned.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "activity_id": {
      "description": "Activity ID from list_activities, for example \"garmin-24431147581\"",
      "pattern": "^(garmin-)?\\d{1,20}$",
      "type": "string"
    }
  },
  "required": [
    "activity_id"
  ],
  "type": "object"
}
```

### `sync_latest_activity`

Use only for an explicitly authorized fresh-data or Garmin sync request. Reads canonical private R2 first and checks completeness and the last successful source check before starting at most one bounded recent activity job. This can contact Garmin and update private R2. Pass expected_date for the Garmin-local workout day and activity_id to disambiguate multiple sessions. Without a date, a newly expected workout uses today's configured HEALTH_TIMEZONE day; during travel supply the explicit local date. Returns a coherent package for the same activity ID, including details, Coach Input and explicit user context. Older workouts and ambiguous dates are never presented as the requested new session. Compatible requests share a correlation/run ID. Use sync_status.data_state for readiness, report source_checked_at and missing_components, and treat null latency as unknown. Follow refresh_status with request_id only while should_continue_polling; stop when false and report missing components and retry guidance.

**Effect:** Explicit refresh authorization required; may dispatch a bounded job, contact Garmin and write private R2.

**Availability:** GITHUB_ACTIONS_TOKEN and GITHUB_REPOSITORY must both be configured.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "activity_id": {
      "pattern": "^(garmin-)?\\d{1,20}$",
      "type": "string"
    },
    "expected_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "new_activity_expected": {
      "default": true,
      "type": "boolean"
    }
  },
  "type": "object"
}
```

### `sync_latest_night`

Use only when the user explicitly authorizes fetching fresh sleep/night data or Garmin sync. Reads canonical private R2 sleep and associated HRV first, separately checks completeness and the last successful source check, and if needed imports exactly one recent wake-date. Can contact Garmin and update private storage. wake_date is the Garmin-local date on waking, not the date the night began. Omit it only when today's configured HEALTH_TIMEZONE wake-date is appropriate; supply an explicit wake-date during travel. Garmin local timestamps take priority with existing IANA/DST fallback. Compatible requests share a job. Follow refresh_status with request_id while should_continue_polling and stop when false. Use sync_status to distinguish the job, Garmin check and canonical sleep/HRV readiness. A successful workflow alone does not prove that Garmin has finalized sleep and HRV; null latency is unknown.

**Effect:** Explicit refresh authorization required; may dispatch a bounded job, contact Garmin and write private R2.

**Availability:** GITHUB_ACTIONS_TOKEN and GITHUB_REPOSITORY must both be configured.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "wake_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    }
  },
  "type": "object"
}
```

### `weight_history`

Read up to 31 days of stored body-composition data in one call. Select the first actual weighing in a local morning window (default 04:00-12:00), never a Garmin daily average. Garmin local timestamps take priority; UTC-only records use the configured HEALTH_TIMEZONE or an explicit IANA timezone. Each day reports selection provenance, coverage, missing data and intraday weight range. Use body_composition(date) for all individual measurements on a selected day.

**Effect:** Stored-data read; starts no Garmin job and writes no fitness data.

**Availability:** All authenticated registration modes.

**Registered input schema** (bounds/defaults; runtime rules above also apply):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "end_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "morning_end": {
      "default": "12:00",
      "pattern": "^([01]\\d|2[0-3]):[0-5]\\d$",
      "type": "string"
    },
    "morning_start": {
      "default": "04:00",
      "pattern": "^([01]\\d|2[0-3]):[0-5]\\d$",
      "type": "string"
    },
    "start_date": {
      "description": "YYYY-MM-DD",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
      "type": "string"
    },
    "timezone": {
      "maxLength": 64,
      "minLength": 1,
      "type": "string"
    }
  },
  "required": [
    "start_date",
    "end_date"
  ],
  "type": "object"
}
```
