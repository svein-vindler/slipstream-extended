/** Typed stored plan facts only: unknown target codes are never expanded. */
import { z } from "zod";

type Json = Record<string, unknown>;
const code = z.union([z.number().int().min(0).max(4294967294), z.string().max(80), z.null()]);
const finite = z.number().finite().nonnegative();
const duration = z.strictObject({ status: z.enum(["available", "unavailable"]), type: z.string().nullable(),
  value: finite.nullable(), unit: z.enum(["s", "m"]).nullable(),
  source_field: z.enum(["duration_time", "duration_distance"]).nullable(),
  reason: z.enum(["active_duration_missing_or_invalid", "duration_unit_unverified"]).nullable(),
}).refine(row => {
  const time = row.type === "time" || row.type === "repetition_time", distance = row.type === "distance";
  if (row.unit !== (time ? "s" : distance ? "m" : null)
    || row.source_field !== (time ? "duration_time" : distance ? "duration_distance" : null)) return false;
  return row.status === "available" ? (time || distance) && row.value !== null && row.reason === null
    : row.value === null && row.reason === (time || distance ? "active_duration_missing_or_invalid" : "duration_unit_unverified");
});
const absentRange = (reason: "open_target" | "target_unit_unverified") => z.strictObject({
  status: z.literal("unavailable"), low: z.null(), high: z.null(), unit: z.null(),
  source_fields: z.tuple([]), reason: z.literal(reason),
});
const speed = z.strictObject({ type: z.literal("speed"), mode: z.enum(["custom", "zone", "unverified"]), raw_value: code,
  range: z.strictObject({ status: z.enum(["available", "unavailable"]), low: finite.nullable(), high: finite.nullable(),
    unit: z.literal("m/s"), source_fields: z.tuple([z.literal("custom_target_speed_low"), z.literal("custom_target_speed_high")]),
    reason: z.enum(["custom_range_missing_or_invalid", "zone_not_expanded", "active_target_unverified"]).nullable() }),
}).refine(row => {
  if (row.mode === "custom" && row.raw_value !== 0) return false;
  if (row.mode === "zone" && (typeof row.raw_value !== "number" || row.raw_value <= 0 || row.raw_value >= 65535)) return false;
  return row.range.status === "available" ? row.mode === "custom" && row.range.low !== null && row.range.high !== null
    && row.range.low <= row.range.high && row.range.reason === null
    : row.range.low === null && row.range.high === null && row.range.reason === (row.mode === "custom"
      ? "custom_range_missing_or_invalid" : row.mode === "zone" ? "zone_not_expanded" : "active_target_unverified");
});
const open = z.strictObject({ type: z.literal("open"), mode: z.literal("open"), raw_value: code, range: absentRange("open_target") });
const unknown = z.strictObject({ type: z.string().nullable(), mode: z.literal("unverified"), raw_value: code,
  range: absentRange("target_unit_unverified"), unverified_source_values: z.partialRecord(z.enum([
    "custom_target_value_low", "custom_target_value_high", "custom_target_heart_rate_low", "custom_target_heart_rate_high",
    "custom_target_power_low", "custom_target_power_high", "custom_target_cadence_low", "custom_target_cadence_high",
  ]), code),
}).refine(row => row.type !== "speed" && row.type !== "open");
const schema = z.strictObject({ schema_version: z.literal(1), method: z.literal("fit_active_typed_workout_fields"),
  activity_id: z.string().regex(/^\d{1,20}$/), source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  status: z.enum(["available", "unavailable"]), reason: z.enum(["source_unavailable", "no_planned_steps", "step_identity_unverified"]).nullable(),
  items: z.array(z.strictObject({ step_index: z.number().int().min(0).max(65534), duration, target: z.union([speed, open, unknown]) })),
});
function object(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
export function validWorkoutTargets(analysis: Json): boolean {
  const parsed = schema.safeParse(analysis.workout_targets);
  if (!parsed.success) return false;
  const block = parsed.data, source = object(analysis.source);
  if (block.activity_id !== analysis.activity_id) return false;
  if (block.status === "unavailable") return block.reason !== null && block.items.length === 0
    && (block.reason === "source_unavailable" ? block.source_fit_sha256 === null : block.source_fit_sha256 === source.fit_sha256);
  const planned = object(analysis.workout_structure).planned_steps;
  if (block.reason !== null || !block.items.length || !Array.isArray(planned) || planned.length !== block.items.length
    || object(analysis.recorded_running_metrics).association !== "matched"
    || block.source_fit_sha256 === null || block.source_fit_sha256 !== source.fit_sha256) return false;
  const seen = new Set<number>();
  return block.items.every((row, index) => {
    const original = object(planned[index]);
    if (seen.has(row.step_index) || row.step_index !== original.step_index) return false;
    seen.add(row.step_index);
    return row.duration.type === (typeof original.duration_type === "string" ? original.duration_type : null)
      && row.target.type === (typeof original.target_type === "string" ? original.target_type : null)
      && (row.target.raw_value === null || row.target.raw_value === original.target_value);
  });
}
