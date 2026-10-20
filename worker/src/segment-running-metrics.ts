/** Validate stored Coach 1.6 statistics; never decode FIT or aggregate samples here. */
import { z } from "zod";
import { validCalendarDay } from "./lib";
import { recordedRunningMetricsSchema } from "./recorded-running-metrics";
import type { recordedRunningSegmentsSchema } from "./recorded-running-segments";

type Json = Record<string, unknown>;
type Recorded = z.infer<typeof recordedRunningSegmentsSchema>;
const hash = z.string().regex(/^[a-f0-9]{64}$/).nullable();
const clock = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine(value => Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value);
const coverage = z.strictObject({
  valid_seconds: z.number().finite().nonnegative(), eligible_seconds: z.number().finite().nonnegative(),
  excluded_gap_seconds: z.number().finite().nonnegative(), contributing_intervals: z.number().int().nonnegative(),
  status: z.enum(["none", "partial", "complete"]),
}).refine(value => value.valid_seconds <= value.eligible_seconds + 0.001
  && (value.status === "none") === (value.contributing_intervals === 0)
  && (value.contributing_intervals > 0 || value.valid_seconds === 0)
  && (value.status !== "complete" || (value.excluded_gap_seconds === 0
    && Math.abs(value.valid_seconds - value.eligible_seconds) <= 0.001)));

function metric<U extends string, F extends string>(
  unit: U, source_field: F, extra: Record<never, never> | { side_semantics: z.ZodLiteral<"unknown"> },
) {
  return z.discriminatedUnion("status", [
    z.strictObject({ ...extra, status: z.literal("available"), value: z.number().finite(),
      unit: z.literal(unit), source_field: z.literal(source_field), coverage }),
    z.strictObject({ ...extra, status: z.enum(["missing", "invalid", "unavailable"]), value: z.null(),
      unit: z.literal(unit), source_field: z.literal(source_field), coverage }),
  ]).refine(value => (value.status === "available") === (value.coverage.contributing_intervals > 0));
}
const measurements = {
  average_power: metric("W", "power", {}), average_step_length: metric("mm", "step_length", {}),
  average_ground_contact_time: metric("ms", "stance_time", {}),
  average_vertical_oscillation: metric("mm", "vertical_oscillation", {}),
  average_vertical_ratio: metric("%", "vertical_ratio", {}),
  // A recorded scalar may exceed 100. It has no verified left/right interpretation.
  average_ground_contact_balance: metric("%", "stance_time_balance", { side_semantics: z.literal("unknown") }),
};
const half = z.strictObject({ segment: z.enum(["first", "second"]),
  start_distance_m: z.number().finite().nonnegative(), end_distance_m: z.number().finite().nonnegative(),
  distance_m: z.number().finite().positive(), ...measurements });
const interval = z.strictObject({ interval: z.number().int().positive(),
  source_laps: z.array(z.number().int().positive()).min(1), workout_step_index: z.number().int().nonnegative(),
  distance_m: z.number().finite().positive(), start_time_utc: clock, end_time_utc: clock, ...measurements });
function group<B extends z.ZodType, R extends z.ZodType>(distance_basis: B, row: R) {
  return z.strictObject({ status: z.enum(["available", "unavailable"]), reason: z.string().nullable(), distance_basis,
    method: z.literal("fit_record_time_weighted_mean"), clock_basis: z.literal("fit_start_time_plus_total_elapsed_time"),
    interval_policy: z.literal("left_sample_hold_distance_progressing_max_30s"),
    distance_allocation: z.literal("linear_between_record_distances"),
    balance_aggregation: z.literal("raw_scalar_time_weighted_mean_side_unknown"), items: z.array(row),
  }).refine(value => value.status === "available" ? value.reason === null && value.items.length > 0
    : typeof value.reason === "string" && value.reason.length > 0 && value.items.length === 0);
}
const schema = z.strictObject({ schema_version: z.literal(1), activity_id: z.string(),
  source_fit_sha256: hash, source_tcx_sha256: hash, association: recordedRunningMetricsSchema.shape.association,
  session_start_time_utc: clock.nullable(), session_end_time_utc: clock.nullable(),
  session_summary_timestamp_utc: clock.nullable(),
  executed_intervals: group(z.literal("fit_executed_active_distance"), interval),
  whole_distance_halves: group(z.literal("tcx_endurance_distance"), half),
  main_distance_halves: group(z.enum(["fit_executed_main_distance", "tcx_endurance_distance_continuous_main"]), half),
});
type Measurements = Pick<z.infer<typeof half>, keyof typeof measurements>;
const metricNames = Object.keys(measurements) as Array<keyof typeof measurements>;
function object(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function finite(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value); }
function covered(row: Measurements, span: number): boolean {
  const common = row.average_power.coverage;
  return metricNames.every(name => {
    const value = row[name].coverage;
    return value.eligible_seconds + value.excluded_gap_seconds <= span + 0.001
      && value.eligible_seconds === common.eligible_seconds && value.excluded_gap_seconds === common.excluded_gap_seconds;
  });
}
function validHalves(rows: z.infer<typeof half>[], total: unknown, span: number): boolean {
  return rows.length === 2 && finite(total) && total > 0 && rows.every((row, index) =>
    row.segment === (index === 0 ? "first" : "second")
    && Math.abs(row.start_distance_m - (index === 0 ? 0 : rows[0].end_distance_m)) <= 1e-6
    && row.end_distance_m > row.start_distance_m
    && Math.abs(row.end_distance_m - row.start_distance_m - row.distance_m) <= 1e-6
    && Math.abs(row.distance_m - rows[1].end_distance_m / 2) <= 0.000500001 && covered(row, span))
    && Math.abs(rows[1].end_distance_m - total) <= 0.001
    && rows.reduce((seconds, row) => seconds + row.average_power.coverage.eligible_seconds
      + row.average_power.coverage.excluded_gap_seconds, 0) <= span + 0.002;
}
function lapClock(value: unknown): number | null {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,3})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(value)
    || !validCalendarDay(value.slice(0, 10))) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function validSegmentRunningMetrics(analysis: Json, recorded: Recorded): boolean {
  const parsed = schema.safeParse(analysis.segment_running_metrics);
  if (!parsed.success) return false;
  const block = parsed.data, source = object(analysis.source), structure = object(analysis.workout_structure);
  if (block.activity_id !== String(analysis.activity_id) || block.source_tcx_sha256 !== source.tcx_sha256
    || block.source_fit_sha256 !== recorded.source_fit_sha256
    || (block.source_fit_sha256 !== null && block.source_fit_sha256 !== source.fit_sha256)
    || block.association !== recorded.association || block.session_start_time_utc !== recorded.session_start_time_utc
    || (recorded.measured_at_utc !== null && block.session_summary_timestamp_utc !== recorded.measured_at_utc)
    || typeof structure.has_structured_workout !== "boolean") return false;
  const start = block.session_start_time_utc === null ? null : Date.parse(block.session_start_time_utc);
  const end = block.session_end_time_utc === null ? null : Date.parse(block.session_end_time_utc);
  if (end !== null && (start === null || end < start)) return false;
  // Summary timestamp is provenance only: it is never used as the new elapsed end bound.
  const span = start !== null && end !== null ? (end - start) / 1000 : null;
  const groups = [block.executed_intervals, block.whole_distance_halves, block.main_distance_halves];
  if (groups.some(value => value.status === "available")
    && (block.association !== "matched" || block.source_fit_sha256 === null || span === null)) return false;
  const mainBasis = structure.has_structured_workout ? "fit_executed_main_distance" : "tcx_endurance_distance_continuous_main";
  if (block.main_distance_halves.distance_basis !== mainBasis) return false;
  if (block.whole_distance_halves.status === "available"
    && !validHalves(block.whole_distance_halves.items, object(analysis.summary).distance_m, span!)) return false;
  if (block.main_distance_halves.status === "available") {
    const main = Array.isArray(analysis.sections) ? analysis.sections.map(object).filter(value => value.section === "main") : [];
    const total = structure.has_structured_workout ? main.length === 1 ? main[0].distance_m : null
      : object(analysis.summary).distance_m;
    const mainSpan = structure.has_structured_workout ? main.length === 1 ? main[0].elapsed_seconds : null : span;
    if (!finite(mainSpan) || mainSpan < 0 || !validHalves(block.main_distance_halves.items, total, mainSpan)) return false;
  }
  if (block.executed_intervals.status !== "available") return true;
  if (!structure.has_structured_workout || !Array.isArray(structure.executed_laps)) return false;
  const laps = new Map<number, Json>();
  for (const value of structure.executed_laps) {
    const lap = object(value);
    if (!finite(lap.lap) || !Number.isInteger(lap.lap) || lap.lap <= 0 || laps.has(lap.lap)) return false;
    laps.set(lap.lap, lap);
  }
  let previousEnd = start!, previousLap = 0;
  for (const [index, row] of block.executed_intervals.items.entries()) {
    const first = Date.parse(row.start_time_utc), last = Date.parse(row.end_time_utc);
    if (row.interval !== index + 1 || first < previousEnd || last <= first || last > end!
      || row.source_laps[0] <= previousLap || !covered(row, (last - first) / 1000)) return false;
    let sourceEnd = first, distance = 0;
    for (const [position, ordinal] of row.source_laps.entries()) {
      if (position > 0 && ordinal !== row.source_laps[position - 1] + 1) return false;
      const lap = laps.get(ordinal);
      if (!lap || lap.workout_step_index !== row.workout_step_index
        || typeof lap.intensity !== "string" || lap.intensity.toLowerCase() !== "active"
        || !finite(lap.elapsed_seconds) || lap.elapsed_seconds < 0 || !finite(lap.distance_m) || lap.distance_m < 0) return false;
      const lapStart = lapClock(lap.start_time);
      if (lapStart === null || lapStart !== sourceEnd) return false;
      sourceEnd = lapStart + lap.elapsed_seconds * 1000;
      distance += lap.distance_m;
    }
    if (sourceEnd !== last || Math.abs(distance - row.distance_m) > 0.001) return false;
    previousEnd = last; previousLap = row.source_laps[row.source_laps.length - 1];
  }
  return true;
}
