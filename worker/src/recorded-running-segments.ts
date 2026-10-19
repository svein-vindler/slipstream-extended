/** Read a producer-computed GPS-free block; never aggregate FIT records in a request. */
import { z } from "zod";
import { recordedRunningMetricsSchema } from "./recorded-running-metrics";

const status = z.enum(["available", "missing", "invalid", "unavailable"]);
const utcClock = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine(value => Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value,
    "Recorded lap clocks must be normalized UTC.");
const coverage = z.strictObject({
  valid_seconds: z.number().finite().nonnegative(),
  eligible_seconds: z.number().finite().nonnegative(),
  excluded_gap_seconds: z.number().finite().nonnegative(),
  contributing_intervals: z.number().int().nonnegative(),
  status: z.enum(["none", "complete", "partial"]),
}).refine(value => {
  // The producer rounds allocated durations to three decimals. A contributing
  // interval can therefore round to zero seconds without becoming missing.
  if (value.valid_seconds > value.eligible_seconds + 0.001) return false;
  if (value.status === "none") return value.valid_seconds === 0 && value.contributing_intervals === 0;
  if (value.contributing_intervals === 0) return false;
  return value.status !== "complete" || (value.excluded_gap_seconds === 0
    && Math.abs(value.valid_seconds - value.eligible_seconds) <= 0.001);
}, "Recorded coverage status, intervals, eligible time and gaps must agree.");
function metric<U extends string, F extends string, Extra extends Record<string, z.ZodType>>(
  unit: U, field: F, extra: Extra,
) {
  return z.discriminatedUnion("status", [
    z.strictObject({ ...extra, status: z.literal("available"), value: z.number().finite(),
      unit: z.literal(unit), source_field: z.literal(field) }),
    z.strictObject({ ...extra, status: z.enum(["missing", "invalid", "unavailable"]), value: z.null(),
      unit: z.literal(unit), source_field: z.literal(field) }),
  ]);
}
const unknownSide = { side_semantics: z.literal("unknown") };
const sessionMeasurements = {
  average_power: recordedRunningMetricsSchema.shape.average_power,
  average_step_length: recordedRunningMetricsSchema.shape.average_step_length,
  average_ground_contact_time: recordedRunningMetricsSchema.shape.average_ground_contact_time,
  average_vertical_oscillation: recordedRunningMetricsSchema.shape.average_vertical_oscillation,
  average_vertical_ratio: recordedRunningMetricsSchema.shape.average_vertical_ratio,
  average_ground_contact_balance: metric("%", "avg_stance_time_balance", unknownSide),
};
function distanceMetric<U extends string, F extends string>(unit: U, field: F) {
  return metric(unit, field, { coverage }).refine(value =>
    (value.status === "available") === (value.coverage.status !== "none"),
  "Recorded metric availability must agree with coverage.");
}
const distanceMeasurements = {
  average_power: distanceMetric("W", "power"),
  average_step_length: distanceMetric("mm", "step_length"),
  average_ground_contact_time: distanceMetric("ms", "stance_time"),
  average_vertical_oscillation: distanceMetric("mm", "vertical_oscillation"),
  average_vertical_ratio: distanceMetric("%", "vertical_ratio"),
  // The FIT percentage's side convention is not verified. No mean or L/R pair is inferred.
  average_ground_contact_balance: z.strictObject({ status: z.literal("unavailable"), value: z.null(),
    unit: z.literal("%"), source_field: z.literal("stance_time_balance"), ...unknownSide, coverage })
    .refine(value => value.coverage.status === "none", "Uncomputed balance cannot claim coverage."),
};
const distanceBounds = {
  start_distance_m: z.number().finite().nonnegative(),
  end_distance_m: z.number().finite().nonnegative(),
  distance_m: z.number().finite().positive(),
};
const group = {
  status, reason: z.string().nullable(),
  method: z.literal("fit_record_time_weighted_mean"),
  interval_policy: z.literal("left_sample_hold_distance_progressing_max_30s"),
  distance_allocation: z.literal("linear_between_record_distances"),
  balance_aggregation: z.literal("not_computed_without_verified_side_semantics"),
};

export const recordedRunningSegmentsSchema = z.strictObject({
  ...recordedRunningMetricsSchema.shape,
  schema_version: z.literal(2),
  ...sessionMeasurements,
  laps: z.strictObject({ status, reason: z.string().nullable(), method: z.literal("fit_recorded_measurement"),
    items: z.array(z.strictObject({ lap: z.number().int().positive(), start_time_utc: utcClock,
      end_time_utc: utcClock, distance_m: z.number().finite().nonnegative().nullable(), ...sessionMeasurements })) }),
  kilometer_splits: z.strictObject({ ...group,
    items: z.array(z.strictObject({ split: z.number().int().positive(), ...distanceBounds, ...distanceMeasurements })) }),
  distance_halves: z.strictObject({ ...group,
    items: z.array(z.strictObject({ segment: z.enum(["first", "second"]), ...distanceBounds, ...distanceMeasurements })) }),
}).refine(value => {
  if (!value.laps.items.length) return true;
  if (!value.session_start_time_utc || !value.measured_at_utc) return false;
  let previousEnd = value.session_start_time_utc;
  for (const [index, lap] of value.laps.items.entries()) {
    if (lap.lap !== index + 1 || lap.start_time_utc < previousEnd || lap.end_time_utc < lap.start_time_utc
      || lap.end_time_utc > value.measured_at_utc) return false;
    previousEnd = lap.end_time_utc;
  }
  return true;
}, "Recorded laps must be ordered within the verified FIT session.").refine(value => {
  for (const group of [value.kilometer_splits, value.distance_halves]) {
    let previousEnd = 0;
    for (const segment of group.items) {
      if (Math.abs(segment.start_distance_m - previousEnd) > 1e-6
        || segment.end_distance_m <= segment.start_distance_m
        || Math.abs(segment.end_distance_m - segment.start_distance_m - segment.distance_m) > 1e-6) return false;
      previousEnd = segment.end_distance_m;
    }
  }
  if (value.kilometer_splits.items.some((segment, index) => segment.split !== index + 1)) return false;
  const halves = value.distance_halves.items;
  return !halves.length || (halves.length === 2 && halves[0].segment === "first" && halves[1].segment === "second"
    && halves.every(segment => Math.abs(segment.distance_m - halves[1].end_distance_m / 2) <= 0.000500001));
}, "Recorded distance partitions must be ordered, contiguous and internally consistent.").refine(value =>
  [value.laps, value.kilometer_splits, value.distance_halves].every(group => group.status === "available"
    ? group.reason === null && group.items.length > 0
    : typeof group.reason === "string" && group.reason.length > 0 && group.items.length === 0),
"Recorded group status, reason and items must agree.");
export const recordedRunningMeasurementsSchema = z.union([recordedRunningMetricsSchema, recordedRunningSegmentsSchema]);
type RecordedRunningMetrics = z.infer<typeof recordedRunningMetricsSchema>;

/** Old/malformed/unbound optional data never hides an otherwise readable TCX dataset. */
export function storedRunningMeasurements(verified: RecordedRunningMetrics, endurance: unknown):
  z.infer<typeof recordedRunningMeasurementsSchema> {
  if (verified.association !== "matched" || !verified.source_fit_sha256 || !verified.session_start_time_utc
    || !endurance || typeof endurance !== "object" || Array.isArray(endurance)) return verified;
  const stored = (endurance as Record<string, unknown>).recorded_running_metrics_v2;
  const parsed = recordedRunningSegmentsSchema.safeParse(stored);
  if (!parsed.success) return verified;
  // Revalidate identity, clocks, digest and the five unchanged measurements with
  // the already-loaded FIT envelope. Stored optional data cannot select a source.
  for (const name of Object.keys(recordedRunningMetricsSchema.shape) as Array<keyof RecordedRunningMetrics>) {
    if (name !== "schema_version" && JSON.stringify(parsed.data[name]) !== JSON.stringify(verified[name])) return verified;
  }
  return parsed.data;
}
