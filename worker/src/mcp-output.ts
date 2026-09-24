import { z } from "zod";

const nullableNumber = z.number().finite().nullable();
const nullableString = z.string().nullable();
const nullableBoolean = z.boolean().nullable();
const nullableTimestamp = z.union([z.string(), z.number().finite()]).nullable();
const openObject = z.record(z.string(), z.unknown());

const activitySummary = z.object({
  id: z.string(),
  date: nullableString,
  name: z.string(),
  type: z.string(),
  distance_km: nullableNumber,
  moving_time: nullableString,
  pace: nullableString,
  avg_hr: nullableNumber,
  elevation_gain_m: nullableNumber,
});

const activityStats = z.object({
  activities: z.number().int().nonnegative(),
  total_distance_km: z.number().finite(),
  total_moving_time: z.string(),
  total_elevation_gain_m: z.number().finite(),
  total_calories: nullableNumber,
  avg_hr: nullableNumber,
});

const healthDay = z.object({
  date: z.string(),
  sleep_hours: nullableNumber,
  deep_sleep_hours: nullableNumber,
  light_sleep_hours: nullableNumber,
  rem_sleep_hours: nullableNumber,
  awake_hours: nullableNumber,
  sleep_score: nullableNumber,
  hrv_last_night_avg_ms: nullableNumber,
  hrv_weekly_avg_ms: nullableNumber,
  hrv_status: nullableString,
  resting_hr: nullableNumber,
  min_hr: nullableNumber,
  max_hr: nullableNumber,
  avg_hr: nullableNumber,
  body_battery_high: nullableNumber,
  body_battery_low: nullableNumber,
  body_battery_charged: nullableNumber,
  body_battery_drained: nullableNumber,
  avg_stress: nullableNumber,
  max_stress: nullableNumber,
  stress_hours: nullableNumber,
  steps: nullableNumber,
  avg_respiration_brpm: nullableNumber,
  low_respiration_brpm: nullableNumber,
  high_respiration_brpm: nullableNumber,
  weight_kg: nullableNumber,
});

const healthMetric = z.object({
  days: z.number().int().nonnegative(),
  average: z.number().finite(),
  minimum: z.number().finite(),
  maximum: z.number().finite(),
}).nullable();

const healthStats = z.object({
  days: z.number().int().nonnegative(),
  sleep_hours: healthMetric,
  sleep_score: healthMetric,
  hrv_last_night_avg_ms: healthMetric,
  resting_hr: healthMetric,
  avg_hr: healthMetric,
  body_battery_high: healthMetric,
  body_battery_low: healthMetric,
  avg_stress: healthMetric,
  steps: healthMetric,
  avg_respiration_brpm: healthMetric,
  weight_kg: healthMetric,
});

const refreshRun = z.object({
  id: z.number().int().nonnegative(),
  status: z.string(),
  conclusion: nullableString,
  event: nullableString,
  created_at: z.string(),
  updated_at: nullableString,
  html_url: z.string(),
});

const sleepSummary = z.object({
  sleep_seconds: nullableNumber,
  deep_sleep_seconds: nullableNumber,
  light_sleep_seconds: nullableNumber,
  rem_sleep_seconds: nullableNumber,
  awake_sleep_seconds: nullableNumber,
  unmeasurable_sleep_seconds: nullableNumber,
  nap_seconds: nullableNumber,
  sleep_score: nullableNumber,
  average_spo2_percent: nullableNumber,
  lowest_spo2_percent: nullableNumber,
  average_respiration_brpm: nullableNumber,
  lowest_respiration_brpm: nullableNumber,
  highest_respiration_brpm: nullableNumber,
  average_sleep_stress: nullableNumber,
});

const sleepScore = z.object({
  value: nullableNumber,
  qualifier: nullableString,
});

const sleepStage = z.object({
  start_gmt: nullableTimestamp,
  end_gmt: nullableTimestamp,
  stage: z.union([z.string(), z.number().finite()]).nullable(),
});

const historyStatus = z.enum(["available", "no_data", "not_stored", "invalid_schema"]);
const historyIndexState = z.enum([
  "indexed", "verified", "read_through", "confirmed_missing", "orphaned_index", "index_only",
]);
const historyMetric = z.object({
  days: z.number().int().nonnegative(),
  average: z.number().finite(),
  minimum: z.number().finite(),
  maximum: z.number().finite(),
  median: z.number().finite(),
}).nullable();
const historyStatusCounts = z.object({
  available: z.number().int().nonnegative(),
  no_data: z.number().int().nonnegative(),
  not_stored: z.number().int().nonnegative(),
  invalid_schema: z.number().int().nonnegative(),
});
const hrvHistoryDay = z.object({
  date: z.string(),
  status: historyStatus,
  index_state: historyIndexState.optional(),
  detailed_readings_available: z.boolean().optional(),
  sleep_start_gmt: z.unknown().optional(),
  sleep_end_gmt: z.unknown().optional(),
  garmin: z.object({
    last_night_avg_ms: nullableNumber,
    last_night_5_min_high_ms: nullableNumber,
    weekly_avg_ms: nullableNumber,
    status: nullableString,
    baseline_low_ms: nullableNumber,
    baseline_high_ms: nullableNumber,
  }).optional(),
  derived: z.object({
    valid_reading_count: z.number().int().nonnegative(),
    minimum_ms: nullableNumber,
    maximum_ms: nullableNumber,
    mean_ms: nullableNumber,
    median_ms: nullableNumber,
    p10_ms: nullableNumber,
    p90_ms: nullableNumber,
    first_half_mean_ms: nullableNumber,
    second_half_mean_ms: nullableNumber,
    second_minus_first_ms: nullableNumber,
    slope_ms_per_hour: nullableNumber,
  }).optional(),
  readings: z.array(z.object({
    timestamp: z.unknown(),
    hrv_ms: z.unknown(),
  })).optional(),
});
const hrvHistoryWeek = z.object({
  period_start: z.string(),
  period_end: z.string(),
  days_requested: z.number().int().positive(),
  status_counts: historyStatusCounts,
  garmin_statuses: z.record(z.string(), z.number().int().nonnegative()),
  last_night_avg_ms: historyMetric,
  last_night_5_min_high_ms: historyMetric,
  weekly_avg_ms: historyMetric,
  reading_mean_ms: historyMetric,
  second_minus_first_ms: historyMetric,
  slope_ms_per_hour: historyMetric,
});
const sleepHistoryDay = z.object({
  date: z.string(),
  status: historyStatus,
  index_state: historyIndexState.optional(),
  sleep_start_gmt: nullableTimestamp.optional(),
  sleep_end_gmt: nullableTimestamp.optional(),
  confirmed: nullableBoolean.optional(),
  summary: sleepSummary.optional(),
  score_breakdown: z.record(z.string(), sleepScore).optional(),
  stage_count: z.number().int().nonnegative().optional(),
  stages: z.array(sleepStage).optional(),
});
const sleepHistoryWeek = z.object({
  period_start: z.string(),
  period_end: z.string(),
  days_requested: z.number().int().positive(),
  status_counts: historyStatusCounts,
  sleep_seconds: historyMetric,
  sleep_score: historyMetric,
  average_spo2_percent: historyMetric,
  lowest_spo2_percent: historyMetric,
  average_respiration_brpm: historyMetric,
  average_sleep_stress: historyMetric,
  stage_totals_seconds: z.object({
    deep: z.number().finite(),
    light: z.number().finite(),
    rem: z.number().finite(),
    awake: z.number().finite(),
  }),
  stage_percent_of_sleep: z.object({
    deep: nullableNumber,
    light: nullableNumber,
    rem: nullableNumber,
  }),
});

const historyBase = {
  start_date: z.string(),
  end_date: z.string(),
  granularity: z.enum(["daily", "weekly"]),
  detail_level: z.enum(["summary", "full"]),
  days_requested: z.number().int().positive(),
  available_days: z.number().int().nonnegative(),
  no_data_dates: z.array(z.string()),
  not_stored_dates: z.array(z.string()),
  invalid_dates: z.array(z.string()),
  source_objects_read: z.number().int().nonnegative(),
  index_consistency: z.object({
    mode: z.enum(["all_requested_days", "recent_7_days"]),
    checked_dates: z.number().int().nonnegative(),
    index_only_dates: z.number().int().nonnegative(),
    stale_dates: z.array(z.string()),
    orphaned_index_dates: z.array(z.string()),
    confirmed_missing_dates: z.array(z.string()),
    source_prefixes_scanned: z.number().int().nonnegative(),
    invalid_index_objects: z.array(z.string()),
  }),
};

const bodyMeasurement = z.object({
  timestamp_gmt: nullableTimestamp,
  timestamp_local: nullableTimestamp,
  weight_kg: nullableNumber,
  bmi: nullableNumber,
  body_fat_percent: nullableNumber,
  body_water_percent: nullableNumber,
  muscle_mass_kg: nullableNumber,
  bone_mass_kg: nullableNumber,
  visceral_fat_rating: nullableNumber,
  metabolic_age: nullableNumber,
  physique_rating: nullableNumber,
  basal_metabolic_rate_kcal: nullableNumber,
  source_type: nullableString,
  measurement_id: z.union([z.string(), z.number().finite()]).nullable(),
  is_daily_average: z.boolean(),
});

export const outputSchemas = {
  data_status: z.object({
    connected: z.boolean(),
    count: z.number().int().nonnegative(),
    earliest: nullableString,
    latest: nullableString,
    by_source: z.record(z.string(), z.number().int().nonnegative()),
    storage: z.enum(["r2", "none"]),
  }),
  list_activities: z.object({
    matched: z.number().int().nonnegative(),
    showing: z.number().int().nonnegative(),
    activities: z.array(activitySummary),
  }),
  activity_stats: z.object({
    overall: activityStats,
  }).catchall(z.record(z.string(), activityStats)),
  personal_bests: z.object({
    longest_distance: activitySummary.nullable(),
    longest_moving_time: activitySummary.nullable(),
    most_elevation_gain: activitySummary.nullable(),
    fastest_avg_pace: activitySummary.nullable(),
  }),
  list_sport_types: z.object({}).catchall(z.number().int().nonnegative()),
  search_activities: z.object({
    matched: z.number().int().nonnegative(),
    activities: z.array(activitySummary),
  }),
  health_status: z.object({
    connected: z.boolean(),
    days: z.number().int().nonnegative(),
    earliest: nullableString,
    latest: nullableString,
    storage: z.enum(["r2", "none"]),
    note: z.string(),
  }),
  daily_health: z.object({
    matched: z.number().int().nonnegative(),
    showing: z.number().int().nonnegative(),
    days: z.array(healthDay),
  }),
  health_trends: z.object({
    overall: healthStats,
  }).catchall(z.record(z.string(), healthStats)),
  hrv_curve: z.object({
    available: z.boolean(),
    date: z.unknown(),
    message: z.string().optional(),
    sleep_start_gmt: z.unknown().optional(),
    sleep_end_gmt: z.unknown().optional(),
    summary: z.unknown().optional(),
    reading_count: z.number().int().nonnegative().optional(),
    readings: z.array(z.object({
      timestamp: z.unknown(),
      hrv_ms: z.unknown(),
    })).optional(),
  }),
  hrv_history: z.object({
    ...historyBase,
    days: z.array(hrvHistoryDay),
    weeks: z.array(hrvHistoryWeek),
  }),
  sleep_detail: z.object({
    available: z.boolean(),
    date: z.string(),
    message: z.string().optional(),
    sleep_start_gmt: nullableTimestamp.optional(),
    sleep_end_gmt: nullableTimestamp.optional(),
    confirmed: nullableBoolean.optional(),
    summary: sleepSummary.optional(),
    score_breakdown: z.record(z.string(), sleepScore).optional(),
    stage_count: z.number().int().nonnegative().optional(),
    stages: z.array(sleepStage).optional(),
  }),
  sleep_history: z.object({
    ...historyBase,
    days: z.array(sleepHistoryDay),
    weeks: z.array(sleepHistoryWeek),
  }),
  body_composition: z.object({
    available: z.boolean(),
    date: z.string(),
    message: z.string().optional(),
    measurement_count: z.number().int().nonnegative().optional(),
    measurements: z.array(bodyMeasurement).optional(),
  }),
  strength_session: z.object({
    available: z.boolean(),
    activity_id: z.string().optional(),
    activity: activitySummary.optional(),
    message: z.string().optional(),
    session: openObject.optional(),
  }),
  endurance_session: z.object({
    available: z.boolean(),
    activity_id: z.string().optional(),
    activity: activitySummary.optional(),
    message: z.string().optional(),
    session: openObject.optional(),
  }),
  coach_profile: z.object({
    available: z.boolean(),
    selected_for_date: nullableString,
    profile: openObject.nullable(),
    profiles: z.array(openObject),
    message: z.string(),
  }),
  add_coach_profile: z.object({
    saved: z.boolean(),
    profile_id: z.string(),
    key: z.string(),
    profile: openObject,
    message: z.string(),
  }),
  add_activity_context: z.object({
    saved: z.boolean(),
    activity_id: z.string(),
    context_id: z.string(),
    key: z.string(),
    context: openObject,
    message: z.string(),
  }),
  coach_input: z.object({
    available: z.boolean(),
    activity_id: z.string().optional(),
    activity: activitySummary.optional(),
    analysis: openObject.optional(),
    message: z.string(),
  }),
  refresh_today: z.object({
    accepted: z.boolean(),
    reason: z.enum(["request_in_progress", "already_running", "recent_success"]).optional(),
    message: z.string(),
    retry_after_seconds: z.number().int().positive().optional(),
    run: refreshRun.nullable().optional(),
    terminal: z.boolean(),
    data_ready: z.boolean(),
    should_continue_polling: z.boolean(),
    poll_after_seconds: z.number().int().positive().optional(),
  }),
  refresh_status: z.object({
    available: z.boolean(),
    message: z.string(),
    run: refreshRun.nullable().optional(),
    terminal: z.boolean(),
    data_ready: z.boolean(),
    should_continue_polling: z.boolean(),
    poll_after_seconds: z.number().int().positive().optional(),
  }),
} as const;

export function structuredToolResult<T extends Record<string, unknown>>(value: T) {
  return {
    content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }],
    structuredContent: value,
  };
}
