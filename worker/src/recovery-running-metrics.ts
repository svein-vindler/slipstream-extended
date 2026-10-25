/** Strict current stored recovery means; the caller validates logical/segment/cadence first. */
import { recoveryCadenceReferences } from "./recorded-cadence";
type Json = Record<string, unknown>;
function object(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function exact(value: Json, keys: string[]): boolean {
  return Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
function finite(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value); }
const metrics = [
  ["average_power", "power", "W"], ["average_step_length", "step_length", "mm"],
  ["average_ground_contact_time", "stance_time", "ms"], ["average_vertical_oscillation", "vertical_oscillation", "mm"],
  ["average_vertical_ratio", "vertical_ratio", "%"], ["average_ground_contact_balance", "stance_time_balance", "%"],
] as const;
const geometry = ["bout", "source_laps", "workout_step_index", "distance_m", "start_time_utc", "end_time_utc"];
const windowKeys = ["disposition", "original_record_count", "boundary_excluded_record_count", "boundary_excluded_pair_count"];
const literals = { schema_version: 1, method: "fit_record_time_weighted_mean",
  distance_basis: "fit_verified_recovery_bout_distance", clock_basis: "fit_start_time_plus_total_elapsed_time",
  interval_policy: "left_sample_hold_distance_progressing_max_30s", distance_allocation: "linear_between_record_distances",
  balance_aggregation: "raw_scalar_time_weighted_mean_side_unknown" };

export function validRecoveryRunningMetrics(analysis: Json): boolean {
  const block = object(analysis.recovery_running_metrics), segment = object(analysis.segment_running_metrics),
    recorded = object(analysis.recorded_running_metrics), source = object(analysis.source);
  if (!exact(block, [...Object.keys(literals), "activity_id", "source_fit_sha256", "source_tcx_sha256",
    "session_start_time_utc", "session_end_time_utc", "source_window", "status", "reason", "items"])
    || Object.entries(literals).some(([key, value]) => block[key] !== value)
    || block.activity_id !== analysis.activity_id || block.source_fit_sha256 !== recorded.source_fit_sha256
    || block.source_tcx_sha256 !== source.tcx_sha256
    || ["session_start_time_utc", "session_end_time_utc"].some(key => block[key] !== segment[key])
    || !Array.isArray(block.items)) return false;
  const window = object(block.source_window), originalWindow = object(segment.source_window);
  if (!exact(window, windowKeys) || windowKeys.some(key => window[key] !== originalWindow[key])) return false;
  if (block.status !== "available") return block.status === "unavailable" && typeof block.reason === "string"
    && block.reason.length > 0 && block.items.length === 0;
  const references = recoveryCadenceReferences(analysis), count = window.original_record_count;
  if (block.reason !== null || recorded.association !== "matched" || !recorded.source_fit_sha256
    || window.disposition === "unverified" || !finite(count) || !Number.isSafeInteger(count) || count < 2
    || !references || !Array.isArray(references.items) || !block.items.length
    || block.items.length !== references.items.length) return false;
  return block.items.every((rawRow, index) => {
    const row = object(rawRow), reference = object((references.items as unknown[])[index]);
    if (!exact(row, [...geometry, ...metrics.map(([name]) => name)])
      || geometry.some(key => JSON.stringify(row[key]) !== JSON.stringify(reference[key]))
      || !finite(row.distance_m) || row.distance_m <= 0 || !finite(reference.elapsed_seconds)) return false;
    let common: string | undefined;
    return metrics.every(([name, field, unit]) => {
      const metric = object(row[name]), balance = field === "stance_time_balance", coverage = object(metric.coverage);
      if (!exact(metric, ["status", "value", "unit", "source_field", "coverage", ...(balance ? ["side_semantics"] : [])])
        || metric.unit !== unit || metric.source_field !== field
        || !["available", "missing", "invalid", "unavailable"].includes(String(metric.status))
        || balance && metric.side_semantics !== "unknown"
        || !exact(coverage, ["valid_seconds", "eligible_seconds", "excluded_gap_seconds", "boundary_excluded_seconds",
          "contributing_intervals", "status"])) return false;
      const valid = coverage.valid_seconds, eligible = coverage.eligible_seconds, gap = coverage.excluded_gap_seconds,
        boundary = coverage.boundary_excluded_seconds, intervals = coverage.contributing_intervals;
      if (!finite(valid) || !finite(eligible) || !finite(gap) || !finite(boundary)
        || [valid, eligible, gap, boundary].some(value => value < 0)
        || !finite(intervals) || !Number.isSafeInteger(intervals) || intervals < 0
        || intervals > count - 1 - Number(window.boundary_excluded_pair_count)
        || valid > eligible + 0.001 || eligible + gap + boundary > Number(reference.elapsed_seconds) + 0.001
        || !["none", "partial", "complete"].includes(String(coverage.status))
        || (metric.status === "available") !== (intervals > 0) || (coverage.status === "none") !== (intervals === 0)
        || (intervals ? !finite(metric.value) : metric.value !== null || valid !== 0)
        || coverage.status === "complete" && (gap !== 0 || boundary !== 0 || Math.abs(valid - eligible) > 0.001)
        || boundary !== 0 && window.disposition !== "verified_terminal_exclusion") return false;
      const shape = JSON.stringify([eligible, gap, boundary]);
      if (common !== undefined && common !== shape) return false;
      common = shape;
      return true;
    });
  });
}
