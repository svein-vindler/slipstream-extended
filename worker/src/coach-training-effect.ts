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
          || (heart.status === "partial" && !(heart.covered_seconds > 0 && heart.covered_seconds < value.elapsed_seconds)))) return false;
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
  if (!parsed.success || !sections.safeParse(analysis.sections).success) return false;
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
