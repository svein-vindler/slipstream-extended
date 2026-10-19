/** Read only the producer's bounded, GPS/device-free retrospective snapshot. */
import { z } from "zod";
import { validCalendarDay } from "./lib";

const day = z.string().refine(validCalendarDay, "Invalid context calendar day.");
const utc = z.string().max(40).regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$/)
  .refine(value => Number.isFinite(Date.parse(value))
    && new Date(value).toISOString().slice(0, 19) === value.slice(0, 19), "Invalid UTC context clock.");
const local = z.string().max(40).regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$/)
  .refine(value => Number.isFinite(Date.parse(`${value}Z`))
    && new Date(`${value}Z`).toISOString().slice(0, 19) === value.slice(0, 19), "Invalid local context clock.");
const offsetLocal = z.string().max(40)
  .regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?[+-]\d{2}:\d{2}$/)
  .refine(value => {
    const wall = value.slice(0, -6);
    const offset = value.slice(-5);
    return validCalendarDay(value.slice(0, 10)) && Number.isFinite(Date.parse(value))
      && new Date(`${wall}Z`).toISOString().slice(0, 19) === wall.slice(0, 19)
      && Number(offset.slice(0, 2)) * 60 + Number(offset.slice(3)) <= 14 * 60;
  }, "Invalid offset context clock.");
const revision = z.string().regex(/^[a-f0-9]{64}$/).nullable();
const emptyStatus = z.enum(["not_collected", "source_error", "oversize", "invalid_schema"]);

function healthMetric<U extends string>(unit: U) {
  const value = unit === "%" || unit === "score_0_100"
    ? z.number().finite().min(0).max(100) : z.number().finite().nonnegative();
  return z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("available"), value, unit: z.literal(unit) }),
    z.strictObject({ status: z.enum(["missing", "invalid", "unavailable"]), value: z.null(), unit: z.literal(unit) }),
  ]);
}
const sleepMetrics = {
  sleep_seconds: healthMetric("s"), deep_sleep_seconds: healthMetric("s"),
  light_sleep_seconds: healthMetric("s"), rem_sleep_seconds: healthMetric("s"),
  awake_sleep_seconds: healthMetric("s"), unmeasurable_sleep_seconds: healthMetric("s"), nap_seconds: healthMetric("s"),
  sleep_score: healthMetric("score_0_100"), average_spo2_percent: healthMetric("%"), lowest_spo2_percent: healthMetric("%"),
  average_respiration_brpm: healthMetric("breaths_per_minute"), lowest_respiration_brpm: healthMetric("breaths_per_minute"),
  highest_respiration_brpm: healthMetric("breaths_per_minute"), average_sleep_stress: healthMetric("score_0_100"),
};
const hrvMetrics = {
  last_night_avg_ms: healthMetric("ms"), weekly_avg_ms: healthMetric("ms"),
  last_night_5_min_high_ms: healthMetric("ms"), baseline_low_ms: healthMetric("ms"), baseline_high_ms: healthMetric("ms"),
};
function healthContext<Shape extends Record<string, z.ZodType>>(shape: Shape) {
  const metrics = z.strictObject(shape);
  return z.union([
    z.strictObject({ status: emptyStatus, source: z.literal("garmin"), source_revision: z.null(),
      source_checked_at: z.null(), wake_date: day.nullable(), night_of: z.null(),
      local_time_source: z.literal("unavailable"), observation_start_utc: z.null(), observation_end_utc: z.null(), metrics }),
    z.strictObject({ status: z.enum(["available", "time_unknown", "wrong_date", "local_time_unknown", "after_activity"]),
      source: z.literal("garmin"), source_revision: revision, source_checked_at: utc.nullable(),
      wake_date: day, night_of: day.nullable(), sleep_start_local: offsetLocal.nullable(), sleep_end_local: offsetLocal.nullable(),
      local_time_source: z.enum(["garmin_local", "configured_timezone", "unavailable"]),
      observation_start_utc: utc.nullable(), observation_end_utc: utc.nullable(), metrics }),
  ]);
}
function trainingMetric<U extends "score_1_100" | "minutes">(unit: U) {
  const provenance = { unit: z.literal(unit), observation_at: utc.nullable(),
    source_checked_at: utc.nullable(), retained_previous: z.boolean() };
  return z.discriminatedUnion("status", [
    z.strictObject({ ...provenance, status: z.literal("available"), value: z.number().int().min(unit === "minutes" ? 0 : 1)
      .max(unit === "minutes" ? 2 ** 31 - 1 : 100) }),
    ...(unit === "minutes" ? [z.strictObject({ ...provenance,
      status: z.literal("conflicting"), value: z.number().int().min(1).max(2 ** 31 - 1) })] : []),
    z.strictObject({ ...provenance, status: z.enum(["no_observation", "invalid", "unavailable"]), value: z.null() }),
  ]);
}
const trainingMetrics = { readiness: trainingMetric("score_1_100"), recovery: trainingMetric("minutes") };
const timeProvenance = z.strictObject({ observation_local: local.nullable(),
  time_source: z.enum(["garmin_offset", "configured_timezone", "unavailable", "timezone_conflict"]) });
const training = z.union([
  z.strictObject({ status: emptyStatus, selection_status: z.literal("unknown"), selected_slot: z.null(),
    temporal_status: z.literal("selection_unavailable"), ...trainingMetrics }),
  z.strictObject({ status: z.literal("stored"), source: z.literal("garmin-connect"), sdk_version: z.literal("0.3.17"),
    normalizer_revision: z.literal(1), source_revision: z.string().regex(/^[a-f0-9]{64}$/), date: day,
    timezone: z.string().min(1).max(128),
    selection_status: z.enum(["available", "ambiguous", "no_observation", "selected_device_unavailable", "unknown"]),
    temporal_status: z.enum(["selection_unavailable", "time_unknown", "before_activity", "after_activity"]),
    selected_slot: z.enum(["latest", "morning"]).nullable(), input_context: z.literal("AFTER_WAKEUP_RESET").nullable(),
    last_attempt: z.strictObject({ status: z.enum(["available", "partial", "no_observation", "auth_required", "source_error",
      "timeout", "oversize", "schema_unverified", "wrong_date"]), attempted_at: utc }),
    source_checked_at: utc.nullable(), time_provenance: z.strictObject({ readiness: timeProvenance, recovery: timeProvenance }),
    ...trainingMetrics }),
]);

export const coachStoredContextSchema = z.strictObject({
  schema_version: z.literal(1), snapshot_at: utc.nullable(), reference_time_utc: utc.nullable(),
  semantics: z.literal("retrospective_pre_activity"), sleep: healthContext(sleepMetrics), hrv: healthContext(hrvMetrics),
  training_day: training,
});
type StoredContext = z.infer<typeof coachStoredContextSchema>;
type HealthContext = StoredContext["sleep"] | StoredContext["hrv"];

function validHealth(health: HealthContext, reference: number | null, localDate: string | null, checked: number[]): boolean {
  if (health.source_checked_at !== null) checked.push(Date.parse(health.source_checked_at));
  if (Object.values(health.metrics).some(metric => health.status === "available"
    ? metric.status === "unavailable" : metric.status !== "unavailable")) return false;
  if (!("sleep_start_local" in health)) return health.wake_date === localDate;
  const start = health.observation_start_utc === null ? null : Date.parse(health.observation_start_utc);
  const end = health.observation_end_utc === null ? null : Date.parse(health.observation_end_utc);
  const locals = [[health.sleep_start_local, start], [health.sleep_end_local, end]] as const;
  for (const [localClock, utcClock] of locals) {
    // The pure Garmin-local producer rounds the inferred UTC offset to a minute,
    // allowing a half-minute residual. Configured-timezone conversion is exact.
    // An incomplete Garmin pair is labelled unavailable but its known endpoint
    // still uses that same producer tolerance.
    const tolerance = health.local_time_source === "configured_timezone" ? 0 : 30_000;
    if (localClock !== null && (utcClock === null || Math.abs(Date.parse(localClock) - utcClock) > tolerance)) return false;
  }
  const bothLocal = health.sleep_start_local !== null && health.sleep_end_local !== null;
  if ((health.local_time_source !== "unavailable") !== bothLocal
    || health.night_of !== (health.sleep_start_local?.slice(0, 10) ?? null)) return false;
  const expected = localDate === null ? "time_unknown"
    : health.wake_date !== localDate ? "wrong_date"
    : reference === null || start === null || end === null || end <= start ? "time_unknown"
    : !bothLocal ? "local_time_unknown"
    : health.sleep_end_local!.slice(0, 10) !== localDate ? "wrong_date"
    : end > reference ? "after_activity" : "available";
  return health.status === expected;
}

function validTraining(context: StoredContext["training_day"], reference: number | null, checked: number[]): boolean {
  if (context.status === "stored") {
    if ((context.selected_slot === null) !== (context.temporal_status === "selection_unavailable")
      || (context.selected_slot !== null && context.selection_status !== "available")) return false;
    if (context.source_checked_at !== null) checked.push(Date.parse(context.source_checked_at));
    // Validate an existing producer-owned IANA name; never infer an activity zone.
    try { new Intl.DateTimeFormat("en", { timeZone: context.timezone }); } catch { return false; }
  }
  for (const name of ["readiness", "recovery"] as const) {
    const metric = context[name];
    const observed = metric.observation_at === null ? null : Date.parse(metric.observation_at);
    const sourceChecked = metric.source_checked_at === null ? null : Date.parse(metric.source_checked_at);
    if (sourceChecked !== null) checked.push(sourceChecked);
    const available = metric.status === "available" || metric.status === "conflicting";
    if ((available && (context.temporal_status !== "before_activity" || reference === null
      || observed === null || observed > reference || sourceChecked === null))
      || (["invalid", "no_observation"].includes(metric.status) && sourceChecked !== null)) return false;
    if (context.status !== "stored") {
      if (metric.status !== "unavailable" || observed !== null || sourceChecked !== null || metric.retained_previous) return false;
    } else {
      const provenance = context.time_provenance[name];
      if (provenance.observation_local !== null && provenance.observation_local.slice(0, 10) !== context.date) return false;
    }
  }
  return true;
}

/** Selection already proves the canonical key/activity/date; never re-hash Python's C3 number serialization here. */
export function validStoredCoachContext(value: unknown, reference: string | null, localDate: string | null): boolean {
  const parsed = coachStoredContextSchema.safeParse(value);
  if (!parsed.success || parsed.data.reference_time_utc !== reference) return false;
  const context = parsed.data;
  if (localDate !== null && ((context.training_day.status === "stored" && context.training_day.date !== localDate)
    || [context.sleep, context.hrv].some(health => health.status === "available" && health.wake_date !== localDate))) return false;
  const checked: number[] = [];
  const referenceTime = reference === null ? null : Date.parse(reference);
  if (!validHealth(context.sleep, referenceTime, localDate, checked)
    || !validHealth(context.hrv, referenceTime, localDate, checked)
    || !validTraining(context.training_day, referenceTime, checked)) return false;
  // A caller may supply a later explicit snapshot, or an unknown/null clock.
  // Neither is evidence that this retrospective context was known pre-workout.
  return context.snapshot_at === null || checked.every(clock => clock <= Date.parse(context.snapshot_at!));
}
