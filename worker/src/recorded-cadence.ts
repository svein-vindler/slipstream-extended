/** Strict stored native cadence, never decode or aggregate FIT in a request. */
import { z } from "zod";
import { exactElapsedMilliseconds } from "./logical-bouts";
import type { recordedRunningSegmentsSchema } from "./recorded-running-segments";

type Json = Record<string, unknown>;
type Recorded = z.infer<typeof recordedRunningSegmentsSchema>;
const sourceReasons = ["source_unmatched", "source_identity_unverified", "session_ambiguous",
  "session_identity_unverified", "running_sport_unverified"] as const;
function metric(prefix: "avg" | "max") {
  const fields = [`${prefix}_cadence`, `${prefix}_running_cadence`, `${prefix}_fractional_cadence`] as const;
  return z.strictObject({ status: z.enum(["available", "unavailable"]), value: z.number().finite().nullable(),
    unit: z.literal("steps_per_minute"), source_fields: z.array(z.enum(fields)).max(2),
    precision: z.enum(["unverified", "base_unavailable", "base_invalid", "conflicting_base", "fraction_unavailable",
      "fraction_invalid", "recorded_base_and_fraction"]),
    reason: z.enum([...sourceReasons, "base_missing_or_invalid", "base_invalid", "base_alias_conflict",
      "fraction_missing_or_invalid", "fraction_invalid"]).nullable(),
  }).refine(row => {
    if (new Set(row.source_fields).size !== row.source_fields.length) return false;
    if (row.status !== "available") return row.value === null && row.reason !== null
      && row.precision !== "recorded_base_and_fraction";
    // Native FIT uint8 base/fraction encoding, not a physiological cap.
    return row.value !== null && row.value >= 0 && row.value <= 511.96875
      && Number.isInteger(row.value * 64) && row.reason === null && row.precision === "recorded_base_and_fraction"
      && row.source_fields.length === 2 && row.source_fields[1] === fields[2]
      && (row.source_fields[0] === fields[0] || row.source_fields[0] === fields[1]);
  });
}
const values = { average: metric("avg"), maximum: metric("max") };
const schema = z.strictObject({ schema_version: z.literal(1),
  method: z.literal("fit_running_native_cadence_conversion"), activity_id: z.string().regex(/^\d{1,20}$/),
  source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  association: z.enum(["matched", ...sourceReasons]), session: z.strictObject(values),
  laps: z.strictObject({ status: z.enum(["available", "unavailable"]), reason: z.string().min(1).nullable(),
    items: z.array(z.strictObject({ lap: z.number().int().positive().max(Number.MAX_SAFE_INTEGER), ...values })) }),
});

function validNativeCadence(analysis: Json, recorded: Recorded): boolean {
  const parsed = schema.safeParse(analysis.recorded_cadence);
  if (!parsed.success) return false;
  const block = parsed.data;
  if (block.activity_id !== analysis.activity_id || block.activity_id !== recorded.activity_id) return false;
  if (block.association !== "matched") return block.source_fit_sha256 === null
    && block.laps.status === "unavailable" && block.laps.reason === block.association && !block.laps.items.length
    && Object.values(block.session).every(row => row.status === "unavailable" && row.reason === block.association);
  if (recorded.association !== "matched" || block.source_fit_sha256 === null
    || block.source_fit_sha256 !== recorded.source_fit_sha256) return false;
  if (block.laps.status === "unavailable") return !block.laps.items.length
    && ["native_laps_unavailable", "lap_identity_unverified"].includes(block.laps.reason ?? "");
  return block.laps.reason === null && recorded.laps.status === "available"
    && block.laps.items.length === recorded.laps.items.length
    && block.laps.items.every((row, ordinal) => row.lap === recorded.laps.items[ordinal].lap);
}

function object(value: unknown): Json | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : null;
}
function exact(value: Json | null, fields: string[]): value is Json {
  return value !== null && Object.keys(value).length === fields.length && fields.every(name => name in value);
}
function nonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
const clockFields = ["session_start_time_utc", "session_end_time_utc", "session_summary_timestamp_utc"];
const windowFields = ["disposition", "original_record_count", "boundary_excluded_record_count", "boundary_excluded_pair_count"];
const groupLiterals = { method: "fit_record_time_weighted_mean", distance_basis: "tcx_endurance_distance",
  clock_basis: "fit_start_time_plus_total_elapsed_time", interval_policy: "left_sample_hold_distance_progressing_max_30s",
  distance_allocation: "linear_between_record_distances", input_unit: "rpm", unit: "steps_per_minute",
  conversion: "2 * (cadence + fractional_cadence)", valid_record_precision: "recorded_base_and_fraction" };
function cadenceFields(value: unknown): boolean {
  return Array.isArray(value) && value.length === 2 && value[0] === "cadence" && value[1] === "fractional_cadence";
}
function sameReference(actual: unknown, expected: unknown): boolean {
  return Array.isArray(actual) ? Array.isArray(expected) && actual.length === expected.length
    && actual.every((item, index) => item === expected[index]) : actual === expected;
}
function validMainReferences(analysis: Json, group: Json, recorded: Recorded): boolean {
  const structure = object(analysis.workout_structure);
  if (structure?.has_structured_workout !== true || !Array.isArray(structure.executed_laps)
    || recorded.laps.status !== "available") return false;
  const selected = structure.executed_laps.map(object).filter((row): row is Json => row?.section === "main");
  const native = new Map(recorded.laps.items.map(row => [row.lap, row]));
  if (!selected.length || !sameReference(group.source_laps, selected.map(row => row.lap))
    || !Array.isArray(group.source_windows) || group.source_windows.length !== selected.length) return false;
  return group.source_windows.every((rawWindow, index) => {
    const window = object(rawWindow), source = selected[index];
    const lap = typeof source.lap === "number" ? native.get(source.lap) : undefined;
    const milliseconds = exactElapsedMilliseconds(source.elapsed_seconds);
    if (!exact(window, ["lap", "workout_step_index", "start_time_utc", "end_time_utc", "distance_m"])
      || !lap || lap.start_time_utc === null || milliseconds === undefined || window.lap !== source.lap
      || window.workout_step_index !== source.workout_step_index || window.distance_m !== source.distance_m) return false;
    const start = Date.parse(lap.start_time_utc);
    return typeof window.start_time_utc === "string" && typeof window.end_time_utc === "string"
      && /(?:Z|[+-]\d{2}:\d{2})$/.test(window.start_time_utc) && /(?:Z|[+-]\d{2}:\d{2})$/.test(window.end_time_utc)
      && Date.parse(window.start_time_utc) === start
      && Date.parse(window.end_time_utc) === start + milliseconds;
  });
}
/** The caller validates logical2 source identities and elapsed clocks first. */
export function recoveryCadenceReferences(analysis: Json): Json | null {
  const logical = object(analysis.logical_bouts), structure = object(analysis.workout_structure);
  if (logical?.schema_version !== 2 || logical.status !== "available"
    || object(logical.source_coverage)?.status !== "complete"
    || !Array.isArray(logical.unassigned_laps) || logical.unassigned_laps.length
    || !Array.isArray(logical.items) || structure?.has_structured_workout !== true
    || !Array.isArray(structure.executed_laps)) return null;
  const executed = new Map(structure.executed_laps.map(raw => {
    const row = object(raw);
    return [row?.lap, row] as const;
  }));
  const items: Json[] = [];
  for (const raw of logical.items) {
    const row = object(raw);
    if (row?.role !== "recovery") continue;
    const timing = object(row.timing);
    if (!Array.isArray(row.source_laps) || !row.source_laps.length || timing?.status !== "available"
      || typeof timing.start_time_utc !== "string" || typeof timing.end_time_utc !== "string") return null;
    const start = Date.parse(timing.start_time_utc), end = Date.parse(timing.end_time_utc);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return null;
    let distance = 0;
    for (const lap of row.source_laps) {
      const source = executed.get(lap);
      if (!source || source.workout_step_index !== row.workout_step_index
        || !["recovery", "rest"].includes(String(source.intensity)) || !nonnegative(source.distance_m)) return null;
      distance += source.distance_m;
    }
    if (!Number.isFinite(distance) || distance <= 0) return null;
    items.push({ bout: row.bout, source_laps: row.source_laps, workout_step_index: row.workout_step_index,
      distance_m: distance, start_time_utc: timing.start_time_utc, end_time_utc: timing.end_time_utc,
      elapsed_seconds: (end - start) / 1000 });
  }
  return { status: "available", distance_basis: "fit_verified_recovery_bout_distance", items };
}
function validCadenceGroup(analysis: Json, block: Json, recorded: Recorded, name: string, rawGroup: unknown): boolean {
  const group = object(rawGroup), segment = object(analysis.segment_running_metrics), source = object(analysis.source);
  const work = name === "executed_intervals", main = name === "main_distance_halves", recovery = name === "recovery_bouts";
  const kilometers = name === "kilometer_splits";
  const basis = kilometers ? "tcx_endurance_kilometer_splits" : main ? group?.distance_basis : recovery ? "fit_verified_recovery_bout_distance"
    : work ? "fit_executed_active_distance" : "tcx_endurance_distance";
  if (main && basis !== "fit_executed_main_distance" && basis !== "tcx_endurance_distance_continuous_main") return false;
  const references = main && basis === "fit_executed_main_distance" && group?.status === "available";
  const literals = { ...groupLiterals, distance_basis: basis };
  if (!exact(group, [...Object.keys(literals), "status", "reason", "items", "source_tcx_sha256", "source_fields",
    ...clockFields, "source_window", ...(references ? ["source_laps", "source_windows"] : [])])
    || !segment || !source || !Array.isArray(group.items)
    || Object.entries(literals).some(([name, value]) => group[name] !== value)
    || group.source_tcx_sha256 !== source.tcx_sha256 || !cadenceFields(group.source_fields)
    || clockFields.some(name => group[name] !== null && (typeof group[name] !== "string" || group[name] !== segment[name]))) return false;
  const window = object(group.source_window);
  if (!exact(window, windowFields)) return false;
  const count = window.original_record_count, excluded = window.boundary_excluded_record_count;
  if ((excluded !== 0 && excluded !== 1) || window.boundary_excluded_pair_count !== excluded
    || (count !== null && (!Number.isSafeInteger(count) || typeof count !== "number" || count < 2))
    || window.disposition !== (count === null ? "unverified" : excluded ? "verified_terminal_exclusion" : "verified_in_frame")
    || (count === null && excluded !== 0)) return false;
  if (group.status !== "available") return group.status === "unavailable" && typeof group.reason === "string"
    && group.reason.length > 0 && group.items.length === 0;
  const original = kilometers ? object(recorded.kilometer_splits)
    : recovery ? recoveryCadenceReferences(analysis) : object(segment[name]), originalWindow = object(segment.source_window);
  if (block.association !== "matched" || group.reason !== null || original?.status !== "available"
    || !kilometers && original.distance_basis !== basis || !Array.isArray(original.items)
    || (work || recovery || kilometers ? !group.items.length || group.items.length !== original.items.length : group.items.length !== 2 || original.items.length !== 2)
    || !originalWindow || windowFields.some(name => window[name] !== originalWindow[name])
    || clockFields.slice(0, 2).some(name => group[name] === null || group[name] !== segment[name])) return false;
  if (references && !validMainReferences(analysis, group, recorded)) return false;
  if (main && !references && object(analysis.workout_structure)?.has_structured_workout !== false) return false;
  const originalItems = original.items;
  return group.items.every((rawRow, index) => {
    const row = object(rawRow), old = object(originalItems[index]);
    const geometry = work || recovery ? [recovery ? "bout" : "interval", "source_laps", "workout_step_index", "distance_m", "start_time_utc", "end_time_utc"]
      : [kilometers ? "split" : "segment", "start_distance_m", "end_distance_m", "distance_m"];
    if (!exact(row, [...geometry, "average_cadence"]) || !old || geometry.some(name => !sameReference(row[name], old[name]))) return false;
    const metric = object(row.average_cadence), coverage = object(metric?.coverage), oldPower = object(old.average_power);
    const reference = object(oldPower?.coverage);
    if (!exact(metric, ["status", "value", "unit", "source_fields", "coverage"]) || !recovery && !reference
      || metric.unit !== "steps_per_minute" || !cadenceFields(metric.source_fields)
      || !exact(coverage, ["valid_seconds", "eligible_seconds", "excluded_gap_seconds", "contributing_intervals", "status", "boundary_excluded_seconds"])) return false;
    const valid = coverage.valid_seconds, eligible = coverage.eligible_seconds, gaps = coverage.excluded_gap_seconds,
      boundary = coverage.boundary_excluded_seconds, contributions = coverage.contributing_intervals;
    if (!nonnegative(valid) || !nonnegative(eligible) || !nonnegative(gaps) || !nonnegative(boundary)
      || typeof contributions !== "number" || !Number.isSafeInteger(contributions) || contributions < 0
      || typeof count !== "number" || contributions > count - 1 - excluded || valid > eligible + 0.001
      || !recovery && !kilometers && ["eligible_seconds", "excluded_gap_seconds", "boundary_excluded_seconds"].some(name => coverage[name] !== reference?.[name])
      || recovery && (!nonnegative(old.elapsed_seconds) || eligible + gaps + boundary > old.elapsed_seconds + 0.001
        || excluded === 0 && boundary !== 0)
      || !["none", "partial", "complete"].includes(String(coverage.status))
      || (coverage.status === "none") !== (contributions === 0) || (contributions === 0 && valid !== 0)
      || (coverage.status === "complete" && (gaps !== 0 || boundary !== 0 || Math.abs(valid - eligible) > 0.001))) return false;
    // Legacy kilometre coverage includes the full terminal pair. Strict elapsed
    // allocation can remove time from it, never add or borrow time.
    if (kilometers && (!reference || !nonnegative(reference.eligible_seconds) || !nonnegative(reference.excluded_gap_seconds)
      || excluded === 0 && (boundary !== 0 || eligible !== reference.eligible_seconds || gaps !== reference.excluded_gap_seconds)
      || excluded === 1 && eligible + gaps + boundary > reference.eligible_seconds + reference.excluded_gap_seconds + 0.001)) return false;
    return metric.status === "available" ? nonnegative(metric.value) && metric.value <= 511.9688 && contributions > 0
      : (metric.status === "missing" || metric.status === "invalid") && metric.value === null && contributions === 0;
  });
}

export function validRecordedCadence(analysis: Json, recorded: Recorded): boolean {
  const block = object(analysis.recorded_cadence);
  if (block?.schema_version === 1) return validNativeCadence(analysis, recorded);
  if (!exact(block, ["schema_version", "method", "activity_id", "source_fit_sha256", "association", "session", "laps", "record_aggregates"])
    || block.schema_version !== 2) return false;
  const native: Json = {};
  for (const [name, value] of Object.entries(block)) if (name !== "record_aggregates") native[name] = value;
  native.schema_version = 1;
  const groups = object(block.record_aggregates);
  return validNativeCadence({ ...analysis, recorded_cadence: native }, recorded)
    && groups !== null && "whole_distance_halves" in groups
    && Object.keys(groups).every(name => ["whole_distance_halves", "main_distance_halves", "executed_intervals", "recovery_bouts", "kilometer_splits"].includes(name)
      && validCadenceGroup(analysis, block, recorded, name, groups[name]));
}
