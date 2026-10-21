/** Validate references to existing native laps; never read or aggregate FIT records. */
import { z } from "zod";
import { recordedRunningSegmentsSchema } from "./recorded-running-segments";

type Json = Record<string, unknown>;
type Recorded = z.infer<typeof recordedRunningSegmentsSchema>;
const index = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const binding = z.strictObject({
  schema_version: z.literal(1), method: z.literal("fit_lap_wkt_step_index"),
  measurement_source: z.literal("recorded_running_metrics.laps"),
  activity_id: z.string().regex(/^\d{1,20}$/), source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  status: z.enum(["available", "partial", "unavailable"]),
  reason: z.enum(["some_laps_unbound", "no_verified_lap_workout_links", "source_unavailable", "recorded_laps_unavailable"]).nullable(),
  items: z.array(z.strictObject({ lap: index.refine(value => value > 0), source_message_index: index.nullable(),
    status: z.enum(["bound", "unbound"]),
    reason: z.enum(["lap_identity_unverified", "workout_step_unverified", "executed_lap_mismatch"]).nullable(),
    workout_step_index: index.nullable() })),
});
function object(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function indexed(value: unknown, field: string): Map<number, Json[]> {
  const result = new Map<number, Json[]>();
  for (const item of Array.isArray(value) ? value : []) {
    const row = object(item), key = row[field];
    if (typeof key === "number" && Number.isSafeInteger(key) && key >= 0) {
      const matches = result.get(key) ?? []; matches.push(row); result.set(key, matches);
    }
  }
  return result;
}
function utc(value: unknown): string | undefined {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|\+00:00)$/.test(value)) return;
  const normalized = value.replace(" ", "T").replace(/\+00:00$/, "Z")
    .replace(/(?:\.(\d{1,3}))?Z$/, (_match, fraction: string | undefined) => `.${(fraction ?? "").padEnd(3, "0")}Z`);
  const time = Date.parse(normalized);
  return Number.isFinite(time) && new Date(time).toISOString() === normalized ? normalized : undefined;
}

export function validNativeLapWorkoutBinding(analysis: Json, recorded: Recorded): boolean {
  const parsed = binding.safeParse(analysis.native_lap_workout_binding);
  if (!parsed.success) return false;
  const block = parsed.data;
  if (block.activity_id !== analysis.activity_id || block.activity_id !== recorded.activity_id) return false;
  const digest = recorded.source_fit_sha256;
  const sourceReady = recorded.association === "matched" && typeof digest === "string" && /^[a-f0-9]{64}$/.test(digest);
  if (block.source_fit_sha256 !== (sourceReady ? digest : null)) return false;
  if (!sourceReady || recorded.laps.status !== "available") return block.status === "unavailable"
    && block.reason === (!sourceReady ? "source_unavailable" : "recorded_laps_unavailable") && block.items.length === 0;
  const laps = recorded.laps.items;
  if (!laps.length || block.items.length !== laps.length) return false;
  const structure = object(analysis.workout_structure);
  const planned = indexed(structure.planned_steps, "step_index"), executed = indexed(structure.executed_laps, "lap");
  const seen = new Set<number>();
  let bound = 0;
  for (const [ordinal, item] of block.items.entries()) {
    const lap = laps[ordinal];
    if (item.lap !== lap.lap) return false;
    if (item.source_message_index !== null) {
      if (seen.has(item.source_message_index)) return false;
      seen.add(item.source_message_index);
    }
    if (item.status === "unbound") {
      if (item.workout_step_index !== null || item.reason === null
        || (item.reason === "lap_identity_unverified") !== (item.source_message_index === null)) return false;
      continue;
    }
    const step = item.workout_step_index, matches = executed.get(lap.lap) ?? [];
    if (item.reason !== null || item.source_message_index === null || step === null
      || planned.get(step)?.length !== 1 || matches.length !== 1) return false;
    const execution = matches[0], distance = execution.distance_m;
    if (execution.workout_step_index !== step || utc(execution.start_time) !== lap.start_time_utc
      || (distance !== null && (typeof distance !== "number" || !Number.isFinite(distance) || distance < 0))
      || distance !== lap.distance_m) return false;
    bound++;
  }
  return block.status === (bound === block.items.length ? "available" : bound ? "partial" : "unavailable")
    && block.reason === (bound === block.items.length ? null : bound ? "some_laps_unbound" : "no_verified_lap_workout_links");
}
