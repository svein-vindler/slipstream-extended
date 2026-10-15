import { z } from "zod";

// Preserve stored numeric strings; do not coerce booleans or missing values to 0.
const numericString = z.string().refine(value => value.trim() !== "" && Number.isFinite(Number(value)));
export const detailNumber = z.union([z.number().finite(), numericString]).nullable();
export const detailClock = z.union([z.string(), z.number().finite()]).nullable();
const text = z.string().nullable();

export const hrvSummarySchema = z.object({
  calendarDate: text.optional(),
  lastNightAvg: detailNumber.optional(),
  lastNightAverage: detailNumber.optional(),
  lastNightAvgMs: detailNumber.optional(),
  lastNight5MinHigh: detailNumber.optional(),
  lastNightFiveMinHigh: detailNumber.optional(),
  lastNight5MinHighMs: detailNumber.optional(),
  weeklyAvg: detailNumber.optional(),
  weeklyAverage: detailNumber.optional(),
  weeklyAvgMs: detailNumber.optional(),
  status: text.optional(),
  statusKey: text.optional(),
  hrvStatus: text.optional(),
  baselineBalancedLower: detailNumber.optional(),
  baselineLow: detailNumber.optional(),
  baselineLower: detailNumber.optional(),
  baselineBalancedUpper: detailNumber.optional(),
  baselineHigh: detailNumber.optional(),
  baselineUpper: detailNumber.optional(),
}).nullable();

export const hrvReadingSchema = z.object({
  timestamp: detailClock,
  hrv_ms: detailNumber,
});

/** Detail tools project stored fields before either MCP serialization channel. */
export const hrvDetailSourceSchema = z.object({
  date: text.optional(),
  available: z.boolean().optional(),
  sleep_start_gmt: detailClock.optional(),
  sleep_end_gmt: detailClock.optional(),
  sleep_start_garmin_local: detailClock.optional(),
  sleep_end_garmin_local: detailClock.optional(),
  summary: hrvSummarySchema.optional(),
  readings: z.array(hrvReadingSchema.partial()).optional(),
});

export const sleepDetailSummarySchema = z.object({
  sleep_seconds: detailNumber,
  deep_sleep_seconds: detailNumber,
  light_sleep_seconds: detailNumber,
  rem_sleep_seconds: detailNumber,
  awake_sleep_seconds: detailNumber,
  unmeasurable_sleep_seconds: detailNumber,
  nap_seconds: detailNumber,
  sleep_score: detailNumber,
  average_spo2_percent: detailNumber,
  lowest_spo2_percent: detailNumber,
  average_respiration_brpm: detailNumber,
  lowest_respiration_brpm: detailNumber,
  highest_respiration_brpm: detailNumber,
  average_sleep_stress: detailNumber,
});
export const sleepDetailScoreSchema = z.object({ value: detailNumber, qualifier: text });
export const sleepDetailStageSchema = z.object({
  start_gmt: detailClock, end_gmt: detailClock, stage: detailClock,
});
export const sleepDetailSourceSchema = z.object({
  date: text.optional(),
  sleep_start_gmt: detailClock.optional(),
  sleep_end_gmt: detailClock.optional(),
  sleep_start_garmin_local: detailClock.optional(),
  sleep_end_garmin_local: detailClock.optional(),
  confirmed: z.boolean().nullish(),
  summary: sleepDetailSummarySchema,
  // Names remain dynamic. Already-stringified producer qualifiers remain strings;
  // their original upstream type cannot be recovered here.
  score_breakdown: z.record(z.string(), sleepDetailScoreSchema).nullish(),
  stages: z.array(sleepDetailStageSchema).nullish(),
});

const strengthTotals = z.object({
  sets: detailNumber.optional(), repetitions: detailNumber.optional(),
  external_volume_kg: detailNumber.optional(),
});
type StrengthDimension = z.infer<typeof strengthTotals>;
const { $schema: _strengthMapDialect, ...strengthDimensionsJsonSchema } =
  z.toJSONSchema(z.record(z.string(), strengthTotals));
// Export the same typed record JSON Schema without a second record parsing pass,
// which would silently remove literal labels. Every value is projected below.
const strengthDimensions = z.preprocess((value, context): Record<string, StrengthDimension> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    context.addIssue({ code: "custom", message: "Unsupported strength dimension shape." });
    return z.NEVER;
  }
  const entries: Array<[string, StrengthDimension]> = [];
  for (const [label, row] of Object.entries(value)) {
    const parsed = strengthTotals.safeParse(row);
    if (!parsed.success) {
      context.addIssue({ code: "custom", message: "Unsupported strength dimension shape." });
      return z.NEVER;
    }
    entries.push([label, parsed.data]);
  }
  // Creates own data properties even for __proto__, without invoking its setter.
  return Object.fromEntries(entries);
}, z.unknown()).meta(strengthDimensionsJsonSchema);
const strengthMetricScalar = z.union([z.number().finite(), z.string(), z.boolean(), z.null()]);
type StrengthMetricScalar = z.infer<typeof strengthMetricScalar>;
const { $schema: _strengthMetricDialect, ...strengthMetricsJsonSchema } =
  z.toJSONSchema(z.record(z.string(), strengthMetricScalar));
// Validate every leaf without record parsing, preserving all literal data keys.
const strengthMetrics = z.preprocess((value, context): Record<string, StrengthMetricScalar> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    context.addIssue({ code: "custom", message: "Unsupported strength metric shape." });
    return z.NEVER;
  }
  const entries: Array<[string, StrengthMetricScalar]> = [];
  for (const [name, leaf] of Object.entries(value)) {
    const parsed = strengthMetricScalar.safeParse(leaf);
    if (!parsed.success) {
      context.addIssue({ code: "custom", message: "Unsupported strength metric shape." });
      return z.NEVER;
    }
    entries.push([name, parsed.data]);
  }
  return Object.fromEntries(entries);
}, z.unknown()).meta(strengthMetricsJsonSchema);
export const strengthSessionSchema = z.object({
  schema_version: detailNumber.optional(), activity_id: text.optional(),
  sets: z.array(z.object({
    set_number: detailNumber.optional(), message_index: detailNumber.optional(),
    exercise: z.object({
      category: detailClock.optional(), name: text.optional(), probability: detailNumber.optional(),
    }).optional(),
    repetitions: detailNumber.optional(), weight_kg: detailNumber.optional(),
    active_duration_seconds: detailNumber.optional(), rest_after_seconds: detailNumber.optional(),
    start_time: detailClock.optional(), workout_step_index: detailNumber.optional(),
    performance_metrics: strengthMetrics.optional(),
  })).optional(),
  summary: z.object({
    active_sets: detailNumber.optional(), total_repetitions: detailNumber.optional(),
    external_volume_kg: detailNumber.optional(), active_duration_seconds: detailNumber.optional(),
    rest_duration_seconds: detailNumber.optional(),
    by_exercise: strengthDimensions.optional(),
    by_category: strengthDimensions.optional(),
  }).optional(),
});

const rangeStats = z.object({
  distance_m: detailNumber.optional(), duration_seconds: detailNumber.optional(),
  pace_seconds_per_km: detailNumber.optional(), average_speed_mps: detailNumber.optional(),
  average_heart_rate_bpm: detailNumber.optional(), maximum_heart_rate_bpm: detailNumber.optional(),
  elevation_change_m: detailNumber.optional(),
});
export const enduranceSessionSchema = z.object({
  schema_version: detailNumber.optional(), source: text.optional(), privacy: text.optional(),
  available: z.boolean().optional(), reason: text.optional(),
  activity: z.object({
    id: text.optional(), name: text.optional(), type: text.optional(), sport: text.optional(),
    start_time: detailClock.optional(),
  }).optional(),
  summary: z.object({
    duration_seconds: detailNumber.optional(), distance_m: detailNumber.optional(),
    average_speed_mps: detailNumber.optional(), average_pace_seconds_per_km: detailNumber.optional(),
    average_heart_rate_bpm: detailNumber.optional(), maximum_heart_rate_bpm: detailNumber.optional(),
    minimum_altitude_m: detailNumber.optional(), maximum_altitude_m: detailNumber.optional(),
    aerobic_decoupling_percent: detailNumber.optional(), trackpoint_count: detailNumber.optional(),
    sampled_trackpoint_count: detailNumber.optional(), observed_heart_rate_seconds: detailNumber.optional(),
  }).optional(),
  laps: z.array(z.object({
    lap: detailNumber.optional(), start_time: detailClock.optional(),
    duration_seconds: detailNumber.optional(), distance_m: detailNumber.optional(),
    maximum_speed_mps: detailNumber.optional(), calories: detailNumber.optional(),
    average_heart_rate_bpm: detailNumber.optional(), maximum_heart_rate_bpm: detailNumber.optional(),
    intensity: text.optional(), trigger_method: text.optional(),
  })).optional(),
  kilometer_splits: z.array(rangeStats.extend({
    split: detailNumber.optional(), partial: z.boolean().optional(),
  })).optional(),
  distance_halves: z.object({ first: rangeStats, second: rangeStats }).nullable().optional(),
  heart_rate_seconds_by_bpm: z.record(z.string(), detailNumber).optional(),
  sample_interval_seconds: detailNumber.optional(),
  sampled_trackpoints: z.array(z.object({
    timestamp: detailClock.optional(), elapsed_seconds: detailNumber.optional(),
    distance_m: detailNumber.optional(), altitude_m: detailNumber.optional(),
    heart_rate_bpm: detailNumber.optional(), cadence_rpm: detailNumber.optional(), speed_mps: detailNumber.optional(),
  })).optional(),
});
