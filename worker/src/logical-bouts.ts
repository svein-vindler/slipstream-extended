/** Strict stored references only; never decode FIT or compute request-time means. */
import { z } from "zod";
import type { recordedRunningSegmentsSchema } from "./recorded-running-segments";

type Json = Record<string, unknown>;
type Recorded = z.infer<typeof recordedRunningSegmentsSchema>;
const index = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const positive = index.refine(value => value > 0);
const clock = z.string().regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
  .refine(value => Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value);
const group = z.strictObject({
  bout: positive, role: z.enum(["work", "recovery", "warmup", "cooldown"]),
  source_intensity: z.enum(["active", "rest", "recovery", "warmup", "cooldown"]),
  source_laps: z.array(positive).min(1), source_message_indices: z.array(index).min(1),
  workout_step_index: index, observed_step_occurrence: positive, planned_repeat_iteration: z.null(),
  start_time_utc: clock, end_time_utc: clock,
  record_aggregate: z.strictObject({ status: z.enum(["available", "unavailable"]),
    interval: positive.nullable(), reason: z.string().min(1).nullable() }),
});
const schema = z.strictObject({
  schema_version: z.literal(1), method: z.literal("verified_native_lap_contiguity"),
  clock_basis: z.literal("fit_start_time_plus_total_elapsed_time"),
  activity_id: z.string().regex(/^\d{1,20}$/), source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  status: z.enum(["available", "partial", "unavailable"]),
  reason: z.enum(["source_unavailable", "recorded_laps_unavailable", "no_verified_bouts", "some_laps_unassigned"]).nullable(),
  occurrence_basis: z.literal("derived_verified_bout_order_per_step"), items: z.array(group),
  unassigned_laps: z.array(z.strictObject({ lap: positive,
    reason: z.enum(["native_binding_unverified", "role_unverified", "clock_basis_unverified"]) })),
});
const sourceOrderGroup = group.omit({ start_time_utc: true, end_time_utc: true }).extend({
  timing: z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("available"), basis: z.literal("fit_start_time_plus_total_elapsed_time"),
      reason: z.null(), start_time_utc: clock, end_time_utc: clock }),
    z.strictObject({ status: z.literal("unavailable"), basis: z.literal("fit_start_time_plus_total_elapsed_time"),
      reason: z.literal("clock_basis_unverified"), start_time_utc: z.null(), end_time_utc: z.null() }),
  ]),
});
const sourceOrderSchema = schema.omit({ clock_basis: true }).extend({
  schema_version: z.literal(2), method: z.literal("verified_native_source_order"),
  grouping_basis: z.literal("verified_native_source_order"),
  occurrence_basis: z.literal("derived_verified_source_order_per_step"), items: z.array(sourceOrderGroup),
  reason: z.enum(["source_unavailable", "recorded_laps_unavailable", "source_coverage_unverified",
    "no_verified_bouts", "some_laps_unassigned"]).nullable(),
  unassigned_laps: z.array(z.strictObject({ lap: positive,
    reason: z.enum(["native_binding_unverified", "role_unverified", "source_coverage_unverified"]) })),
  source_coverage: z.union([
    z.strictObject({ status: z.literal("unavailable"),
      reason: z.enum(["source_unavailable", "recorded_laps_unavailable"]) }),
    z.strictObject({ status: z.enum(["complete", "unavailable"]),
      reason: z.literal("source_coverage_unverified").nullable(), raw_lap_count: index.nullable(),
      native_lap_count: index.nullable(), binding_count: index.nullable(), execution_count: index.nullable() }),
  ]),
});
function object(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function indexed(value: unknown, key: string): Map<number, Json | null> {
  const result = new Map<number, Json | null>();
  for (const valueRow of Array.isArray(value) ? value : []) {
    const row = object(valueRow), id = row[key];
    if (typeof id === "number" && Number.isSafeInteger(id) && id >= 0) result.set(id, result.has(id) ? null : row);
  }
  return result;
}

/** The producer accepts exact whole milliseconds, including a zero-length frame. */
export function exactElapsedMilliseconds(elapsed: unknown): number | undefined {
  if (typeof elapsed !== "number" || !Number.isFinite(elapsed) || elapsed < 0) return;
  // Match the producer's exact millisecond precision without binary rounding
  // (for example1.001*1000 is not reliably an integer in JavaScript).
  const parts = /^(\d+)(?:\.(\d{1,3}))?$/.exec(String(elapsed));
  if (!parts) return;
  const milliseconds = Number(parts[1]) * 1000 + Number((parts[2] ?? "").padEnd(3, "0"));
  return Number.isSafeInteger(milliseconds) ? milliseconds : undefined;
}

/** FIT elapsed seconds, not the native summarytimestamp's separate clock. */
function elapsedEnd(row: Json): string | undefined {
  if (typeof row.elapsed_seconds !== "number" || row.elapsed_seconds <= 0 || typeof row.start_time !== "string") return;
  const milliseconds = exactElapsedMilliseconds(row.elapsed_seconds);
  if (milliseconds === undefined) return;
  const end = new Date(Date.parse(row.start_time) + milliseconds);
  return Number.isFinite(end.getTime()) ? end.toISOString() : undefined;
}

export function validLogicalBouts(analysis: Json, recorded: Recorded): boolean {
  if (object(analysis.logical_bouts).schema_version === 2) return validSourceOrderBouts(analysis, recorded);
  const parsed = schema.safeParse(analysis.logical_bouts);
  if (!parsed.success) return false;
  const block = parsed.data;
  if (block.activity_id !== analysis.activity_id || block.activity_id !== recorded.activity_id) return false;
  const ready = recorded.association === "matched" && recorded.source_fit_sha256 !== null;
  if (block.source_fit_sha256 !== (ready ? recorded.source_fit_sha256 : null)) return false;
  if (!ready || recorded.laps.status !== "available") return block.status === "unavailable"
    && block.reason === (!ready ? "source_unavailable" : "recorded_laps_unavailable")
    && block.items.length === 0 && block.unassigned_laps.length === 0;
  const native = new Map(recorded.laps.items.map(row => [row.lap, row])),
    bindings = indexed(object(analysis.native_lap_workout_binding).items, "lap"),
    structure = object(analysis.workout_structure), planned = indexed(structure.planned_steps, "step_index"),
    executions = indexed(structure.executed_laps, "lap"),
    segments = object(object(analysis.segment_running_metrics).executed_intervals),
    intervals = indexed(segments.items, "interval"), occurrences = new Map<number, number>(), covered = new Set<number>();
  const roles = { active: "work", rest: "recovery", recovery: "recovery", warmup: "warmup", cooldown: "cooldown" };
  let previous: z.infer<typeof group> | undefined;
  for (const [ordinal, item] of block.items.entries()) {
    const laps = item.source_laps, step = item.workout_step_index, occurrence = (occurrences.get(step) ?? 0) + 1;
    occurrences.set(step, occurrence);
    if (item.bout !== ordinal + 1 || item.observed_step_occurrence !== occurrence
      || item.source_message_indices.length !== laps.length || roles[item.source_intensity] !== item.role
      || planned.get(step)?.intensity !== item.source_intensity || item.end_time_utc <= item.start_time_utc
      || item.start_time_utc !== native.get(laps[0])?.start_time_utc) return false;
    let lastElapsedEnd: string | undefined;
    for (const [position, lap] of laps.entries()) {
      const binding = bindings.get(lap), execution = executions.get(lap);
      if (!native.has(lap) || covered.has(lap) || lap !== laps[0] + position
        || binding?.status !== "bound" || binding.source_message_index !== item.source_message_indices[position]
        || binding.workout_step_index !== step || execution?.workout_step_index !== step
        || execution.intensity !== item.source_intensity) return false;
      const end = elapsedEnd(execution);
      if (!end || (lastElapsedEnd !== undefined
        && Date.parse(String(execution.start_time)) !== Date.parse(lastElapsedEnd))) return false;
      lastElapsedEnd = end;
      covered.add(lap);
    }
    if (item.end_time_utc !== lastElapsedEnd) return false;
    if (previous && (laps[0] <= previous.source_laps.at(-1)! || item.start_time_utc < previous.end_time_utc
      || (previous.source_laps.at(-1)! + 1 === laps[0] && previous.workout_step_index === step
        && previous.role === item.role && previous.end_time_utc === item.start_time_utc))) return false;
    const reference = item.record_aggregate;
    if (reference.status === "available") {
      const row = reference.interval === null ? null : intervals.get(reference.interval);
      if (!row || item.role !== "work" || segments.status !== "available" || reference.reason !== null
        || JSON.stringify(row.source_laps) !== JSON.stringify(laps) || row.workout_step_index !== step
        || row.start_time_utc !== item.start_time_utc || row.end_time_utc !== item.end_time_utc) return false;
    } else if (reference.interval !== null || reference.reason === null) return false;
    previous = item;
  }
  for (const row of block.unassigned_laps) {
    if (!native.has(row.lap) || covered.has(row.lap)) return false;
    covered.add(row.lap);
  }
  if (block.items.length) return covered.size === native.size
    && block.status === (block.unassigned_laps.length ? "partial" : "available")
    && block.reason === (block.unassigned_laps.length ? "some_laps_unassigned" : null);
  return block.status === "unavailable" && block.reason === "no_verified_bouts" && covered.size === native.size;
}

/** Source order proves membership; elapsed clocks and record references need separate evidence. */
function validSourceOrderBouts(analysis: Json, recorded: Recorded): boolean {
  const parsed = sourceOrderSchema.safeParse(analysis.logical_bouts);
  if (!parsed.success) return false;
  const block = parsed.data, coverage = block.source_coverage;
  if (block.activity_id !== analysis.activity_id || block.activity_id !== recorded.activity_id) return false;
  const ready = recorded.association === "matched" && recorded.source_fit_sha256 !== null;
  if (block.source_fit_sha256 !== (ready ? recorded.source_fit_sha256 : null)) return false;
  if (!ready || recorded.laps.status !== "available") {
    const reason = !ready ? "source_unavailable" : "recorded_laps_unavailable";
    return block.status === "unavailable" && block.reason === reason && block.items.length === 0
      && block.unassigned_laps.length === 0 && coverage.status === "unavailable"
      && coverage.reason === reason && !("raw_lap_count" in coverage);
  }
  const nativeRows = recorded.laps.items, native = new Map(nativeRows.map(row => [row.lap, row])),
    binding = object(analysis.native_lap_workout_binding), structure = object(analysis.workout_structure),
    bindingRows = Array.isArray(binding.items) ? binding.items.map(object) : [],
    executionRows = Array.isArray(structure.executed_laps) ? structure.executed_laps.map(object) : [];
  if (!("raw_lap_count" in coverage) || coverage.native_lap_count !== nativeRows.length
    || coverage.binding_count !== bindingRows.length || coverage.execution_count !== executionRows.length) return false;
  if (coverage.status === "unavailable") return coverage.reason === "source_coverage_unverified"
    && block.status === "unavailable" && block.reason === "source_coverage_unverified" && block.items.length === 0
    && JSON.stringify(block.unassigned_laps) === JSON.stringify(nativeRows.map(row =>
      ({ lap: row.lap, reason: "source_coverage_unverified" })));
  if (coverage.reason !== null || !nativeRows.length || coverage.raw_lap_count !== nativeRows.length
    || bindingRows.length !== nativeRows.length || executionRows.length !== nativeRows.length
    || native.size !== nativeRows.length) return false;
  for (const [ordinal, lap] of nativeRows.entries()) {
    const execution = executionRows[ordinal];
    if (lap.lap !== ordinal + 1 || bindingRows[ordinal].lap !== lap.lap || execution.lap !== lap.lap
      || typeof execution.start_time !== "string" || Date.parse(execution.start_time) !== Date.parse(lap.start_time_utc)
      || execution.distance_m !== lap.distance_m) return false;
  }
  const bindings = indexed(bindingRows, "lap"), executions = indexed(executionRows, "lap"),
    planned = indexed(structure.planned_steps, "step_index"),
    segmentBlock = object(analysis.segment_running_metrics), segments = object(segmentBlock.executed_intervals),
    intervals = indexed(segments.items, "interval"), occurrences = new Map<number, number>(), covered = new Set<number>();
  // A native summarytimestamp is never a substitute for the elapsed session end.
  if (block.items.some(item => item.timing.status === "available")) {
    const start = segmentBlock.session_start_time_utc, end = segmentBlock.session_end_time_utc;
    if (segmentBlock.association !== "matched" || segmentBlock.activity_id !== block.activity_id
      || segmentBlock.source_fit_sha256 !== block.source_fit_sha256 || start !== recorded.session_start_time_utc
      || typeof start !== "string" || typeof end !== "string" || !clock.safeParse(end).success || end <= start) return false;
    let previousEnd = Date.parse(start);
    for (const execution of executionRows) {
      const last = elapsedEnd(execution), first = Date.parse(String(execution.start_time));
      if (!last || first < previousEnd || last > end) return false;
      previousEnd = Date.parse(last);
    }
  }
  const roles = { active: "work", rest: "recovery", recovery: "recovery", warmup: "warmup", cooldown: "cooldown" };
  let previous: z.infer<typeof sourceOrderGroup> | undefined;
  for (const [ordinal, item] of block.items.entries()) {
    const laps = item.source_laps, step = item.workout_step_index, timing = item.timing,
      occurrence = (occurrences.get(step) ?? 0) + 1;
    occurrences.set(step, occurrence);
    if (item.bout !== ordinal + 1 || item.observed_step_occurrence !== occurrence
      || item.source_message_indices.length !== laps.length || roles[item.source_intensity] !== item.role
      || planned.get(step)?.intensity !== item.source_intensity) return false;
    let lastElapsedEnd: string | undefined;
    for (const [position, lap] of laps.entries()) {
      const reference = bindings.get(lap), execution = executions.get(lap);
      if (!native.has(lap) || covered.has(lap) || lap !== laps[0] + position
        || reference?.status !== "bound" || reference.source_message_index !== item.source_message_indices[position]
        || reference.workout_step_index !== step || execution?.workout_step_index !== step
        || execution.intensity !== item.source_intensity) return false;
      if (timing.status === "available") {
        const end = elapsedEnd(execution);
        if (!end || (lastElapsedEnd !== undefined
          && Date.parse(String(execution.start_time)) !== Date.parse(lastElapsedEnd))) return false;
        lastElapsedEnd = end;
      }
      covered.add(lap);
    }
    if (timing.status === "available" && (timing.start_time_utc !== native.get(laps[0])?.start_time_utc
      || timing.end_time_utc !== lastElapsedEnd || timing.end_time_utc <= timing.start_time_utc)) return false;
    if (previous && (laps[0] <= previous.source_laps.at(-1)!
      || (previous.source_laps.at(-1)! + 1 === laps[0] && previous.workout_step_index === step
        && previous.role === item.role))) return false;
    const reference = item.record_aggregate;
    if (reference.status === "available") {
      const row = reference.interval === null ? null : intervals.get(reference.interval);
      if (!row || timing.status !== "available" || item.role !== "work" || segments.status !== "available"
        || reference.reason !== null || JSON.stringify(row.source_laps) !== JSON.stringify(laps)
        || row.workout_step_index !== step || row.start_time_utc !== timing.start_time_utc
        || row.end_time_utc !== timing.end_time_utc) return false;
    } else if (reference.interval !== null || reference.reason === null
      || (timing.status === "unavailable" && reference.reason !== timing.reason)) return false;
    previous = item;
  }
  for (const row of block.unassigned_laps) {
    if (!native.has(row.lap) || covered.has(row.lap) || row.reason === "source_coverage_unverified") return false;
    covered.add(row.lap);
  }
  const status = block.items.length ? block.unassigned_laps.length ? "partial" : "available" : "unavailable";
  return covered.size === native.size && block.status === status
    && block.reason === (status === "available" ? null : status === "partial" ? "some_laps_unassigned" : "no_verified_bouts");
}
