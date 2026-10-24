/** Strict read-only contract for the version 1.4 producer's recorded estimates. */
import { z } from "zod";

const localClock = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?$/)
  .refine(value => Number.isFinite(Date.parse(`${value}Z`))
    && new Date(`${value}Z`).toISOString().slice(0, 19) === value.slice(0, 19));
const utcClock = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine(value => Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value);
function effect<F extends string>(field: F) {
  return z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("available"), source_field: z.literal(field),
      value: z.number().finite().min(0).max(5).refine(value => Math.abs(value * 10 - Math.round(value * 10)) <= 1e-9) }),
    z.strictObject({ status: z.enum(["missing", "invalid", "unavailable"]), source_field: z.literal(field), value: z.null() }),
  ]);
}
const trainingEffect = z.strictObject({
  schema_version: z.literal(1), source: z.literal("fit_session"), method: z.literal("fit_recorded_estimate"),
  scale: z.literal("0_to_5"), activity_id: z.string().regex(/^[0-9]{1,20}$/),
  association: z.enum(["matched", "source_missing", "source_unreadable", "invalid_schema", "decode_errors",
    "id_mismatch", "time_mismatch", "sessions_missing", "sessions_ambiguous"]),
  local_start_time: localClock.nullable(), local_date: z.string().nullable(),
  session_start_time_utc: utcClock.nullable(), measured_at_utc: utcClock.nullable(),
  source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  aerobic: effect("total_training_effect"), anaerobic: effect("total_anaerobic_training_effect"),
});
const number = z.number().finite().nonnegative().nullable();
const totalCoverage = z.strictObject({ status: z.enum(["complete", "partial", "unknown"]),
  observed_laps: z.number().int().nonnegative(), total_laps: z.number().int().positive() });
const heartCoverage = z.strictObject({ basis: z.literal("lap_elapsed"), covered_seconds: z.number().finite().nonnegative(),
  contributing_laps: z.number().int().nonnegative(), status: z.enum(["complete", "partial", "none", "unknown"]) });
const sectionFields = { section: z.string(), source: z.string(), elapsed_seconds: number, distance_m: number,
  average_heart_rate_bpm: number };

type Json = Record<string, unknown>;
function object(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function nonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
// Round the actual binary64 value to decimal places, with Python's ties-to-even.
// A scaled Math.round/toFixed operation cannot establish this historical proof.
function pythonRound(value: number, digits: number): number | null {
  if (!nonnegative(value)) return null;
  const bytes = new DataView(new ArrayBuffer(8));
  bytes.setFloat64(0, value);
  const bits = bytes.getBigUint64(0);
  const exponentBits = Number((bits >> 52n) & 0x7ffn);
  const mantissa = (bits & ((1n << 52n) - 1n)) | (exponentBits ? 1n << 52n : 0n);
  const exponent = exponentBits ? exponentBits - 1023 - 52 : -1074;
  let numerator = mantissa * 10n ** BigInt(digits);
  let denominator = 1n;
  if (exponent >= 0) numerator <<= BigInt(exponent);
  else denominator <<= BigInt(-exponent);
  let rounded = numerator / denominator;
  const remainder = numerator % denominator;
  if (remainder * 2n > denominator || (remainder * 2n === denominator && rounded % 2n === 1n)) rounded++;
  // Real FIT elapsed/distance/HR projections fit this exact integer range.
  if (rounded > BigInt(Number.MAX_SAFE_INTEGER)) return null;
  return Number(rounded) / 10 ** digits;
}
function sequentialSum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}
function compensatedSum(values: number[]): number {
  let total = 0, correction = 0;
  for (const value of values) {
    const next = total + value;
    correction += Math.abs(total) >= Math.abs(value) ? (total - next) + value : (value - next) + total;
    total = next;
  }
  return total + correction;
}
function legacySectionHrRounding(analysis: Json, section: Json): boolean {
  if (analysis.analyzer_version !== "1.9.0" || analysis.time_basis !== "elapsed") return false;
  const structure = object(analysis.workout_structure), name = section.section;
  if (structure.source !== "garmin_workout" || structure.has_structured_workout !== true
    || !Array.isArray(structure.planned_steps) || !structure.planned_steps.length
    || !["warmup", "main", "cooldown"].includes(String(name))
    || section.source !== "garmin_executed_laps" || !Array.isArray(structure.executed_laps)
    || !Array.isArray(analysis.sections)
    || analysis.sections.filter(value => object(value).section === name).length !== 1) return false;
  const rows = structure.executed_laps.map(object);
  if (rows.some((row, index) => row.lap !== index + 1)) return false;
  const selected = rows.filter(row => row.section === name);
  const durations = selected.map(row => row.elapsed_seconds);
  if (!selected.length || section.laps !== selected.length || !durations.every(nonnegative)) return false;
  const positive = selected.filter(row => (row.elapsed_seconds as number) > 0);
  if (!positive.length || positive.some(row => !nonnegative(row.average_heart_rate_bpm))) return false;
  const heart = object(section.heart_rate_coverage), totals = object(section.measurement_coverage);
  const elapsedCoverage = object(totals.elapsed_seconds), distanceCoverage = object(totals.distance_m);
  if (heart.basis !== "lap_elapsed" || heart.status !== "partial"
    || heart.contributing_laps !== positive.length || elapsedCoverage.status !== "complete"
    || elapsedCoverage.observed_laps !== selected.length || elapsedCoverage.total_laps !== selected.length) return false;
  const distances = selected.map(row => row.distance_m).filter(nonnegative);
  const distanceStatus = distances.length === selected.length ? "complete" : distances.length ? "partial" : "unknown";
  if (distanceCoverage.status !== distanceStatus || distanceCoverage.observed_laps !== distances.length
    || distanceCoverage.total_laps !== selected.length) return false;
  for (const sum of [compensatedSum, sequentialSum]) {
    const elapsed = sum(durations), covered = sum(positive.map(row => row.elapsed_seconds as number));
    const weighted = sum(positive.map(row => (row.average_heart_rate_bpm as number) * (row.elapsed_seconds as number)));
    const published = pythonRound(elapsed, 3);
    const distance = distanceStatus === "complete" ? pythonRound(sum(distances), 3) : null;
    const average = pythonRound(weighted / covered, 2);
    if (published !== null && average !== null && nonnegative(weighted)
      && nonnegative(covered) && covered > 0 && covered === elapsed && covered !== published
      && section.elapsed_seconds === published && heart.covered_seconds === published
      && (distanceStatus !== "complete" || distance !== null) && section.distance_m === distance
      && section.average_heart_rate_bpm === average) return true;
  }
  return false;
}

const sections = z.array(z.union([
  z.strictObject({ ...sectionFields, note: z.string() }),
  z.strictObject({ ...sectionFields, laps: z.number().int().positive(), heart_rate_coverage: heartCoverage,
    measurement_coverage: z.strictObject({ elapsed_seconds: totalCoverage, distance_m: totalCoverage }) })
    .refine(value => {
      const heart = value.heart_rate_coverage;
      if (heart.contributing_laps > value.laps
        || (heart.covered_seconds === 0) !== (heart.contributing_laps === 0)
        || (heart.status === "none" && heart.covered_seconds !== 0)) return false;
      if ((heart.status === "complete" || heart.status === "partial")
        && (!heart.contributing_laps || value.elapsed_seconds === null
          || (heart.status === "complete" && Math.abs(heart.covered_seconds - value.elapsed_seconds) > 0.001)
          || (heart.status === "partial" && !(heart.covered_seconds > 0 && heart.covered_seconds <= value.elapsed_seconds)))) return false;
      for (const name of ["elapsed_seconds", "distance_m"] as const) {
        const total = value.measurement_coverage[name];
        const status = total.observed_laps === total.total_laps ? "complete" : total.observed_laps ? "partial" : "unknown";
        if (total.total_laps !== value.laps || total.observed_laps > total.total_laps || total.status !== status
          || (status === "complete" ? value[name] === null : value[name] !== null)) return false;
      }
      return true;
    }),
]));

export function validCoachTrainingEffect(analysis: Record<string, unknown>, activityId: string): boolean {
  const parsed = trainingEffect.safeParse(analysis.training_effect);
  const checkedSections = sections.safeParse(analysis.sections);
  if (!parsed.success || !checkedSections.success) return false;
  if (checkedSections.data.some(section => "laps" in section && section.heart_rate_coverage.status === "partial"
    && section.elapsed_seconds !== null && section.heart_rate_coverage.covered_seconds >= section.elapsed_seconds
    && !legacySectionHrRounding(analysis, section))) return false;
  const block = parsed.data;
  const source = analysis.source as Record<string, unknown> | null;
  const activity = analysis.activity as Record<string, unknown> | null;
  const running = analysis.recorded_running_metrics as Record<string, unknown>;
  if (block.activity_id !== activityId || !source || !activity
    || (block.source_fit_sha256 !== null && block.source_fit_sha256 !== source.fit_sha256)
    || block.local_date !== (block.local_start_time?.slice(0, 10) ?? null)) return false;
  if (["session_start_time_utc", "measured_at_utc", "local_start_time", "local_date"].some(name => {
    const value = block[name as keyof typeof block];
    return value !== null && running[name] !== null && value !== running[name];
  })) return false;
  const matched = block.association === "matched";
  if (matched) {
    if (block.session_start_time_utc === null || (block.local_date !== null && block.local_date !== activity.date)
      || [block.aerobic, block.anaerobic].some(value => value.status === "unavailable")) return false;
  } else if (block.session_start_time_utc !== null || block.measured_at_utc !== null
    || [block.aerobic, block.anaerobic].some(value => value.status !== "unavailable")) return false;
  return block.measured_at_utc === null || (block.session_start_time_utc !== null
    && block.measured_at_utc >= block.session_start_time_utc);
}
