/** Activity/FIT-scoped user declaration; never a manufacturer-wide convention. */
import { z } from "zod";
import { recordedRunningSegmentsSchema } from "./recorded-running-segments";

export const gctSideInput = z.strictObject({
  source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/), raw_side: z.enum(["left", "right"]),
});
const declaration = gctSideInput.extend({ source: z.literal("user_activity_declaration") });
const contextFields = {
  activity_id: z.string().regex(/^\d{1,20}$/), context_id: z.string().min(1),
  rpe: z.number().finite().min(0).max(10).nullable(), conditions: z.string().min(1).max(240).nullable(),
  note: z.string().min(1).max(2000).nullable(), workout_correction: z.string().min(1).max(1000).nullable(),
  source: z.literal("user"), created_at: z.iso.datetime(),
};
const calendar = { activity_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  date_source: z.enum(["garmin_local", "utc_fallback"]) };
export const activityContextSchema = z.union([
  z.strictObject({ ...contextFields, schema_version: z.literal(1),
    activity_date: calendar.activity_date.optional(), date_source: calendar.date_source.optional() }),
  z.strictObject({ ...contextFields, ...calendar, schema_version: z.literal(2), gct_balance_side: declaration }),
]);
type Metrics = z.infer<typeof recordedRunningSegmentsSchema>;
type Metric = Metrics["average_ground_contact_balance"];
type Json = Record<string, unknown>;
function object(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}

/** Reconstruct the small expected side block, using already validated raw measurements. */
export function expectedGctSides(metrics: Metrics, rawContext: unknown, rawSegments?: unknown) {
  const context = rawContext && typeof rawContext === "object" && !Array.isArray(rawContext)
    ? rawContext as Record<string, unknown> : {};
  const parsed = declaration.safeParse(context.gct_balance_side);
  const ready = metrics.association === "matched" && !!metrics.source_fit_sha256;
  const valid = context.schema_version === 2 && context.activity_id === metrics.activity_id
    && context.source === "user" && (metrics.local_date === null || context.activity_date === metrics.local_date)
    && typeof context.context_id === "string" && !!context.context_id && parsed.success;
  const matched = ready && valid && parsed.data!.source_fit_sha256 === metrics.source_fit_sha256;
  const reason = !ready ? "source_unavailable" : context.gct_balance_side == null ? "declaration_missing"
    : !valid ? "invalid_declaration" : !matched ? "source_mismatch" : null;
  function project(metric: Pick<Metric, "status" | "value">) {
    const value = metric.value;
    const status = !matched ? ready ? "unverified" : "unavailable" : metric.status !== "available"
      ? metric.status : value !== null && value >= 0 && value <= 100 ? "available" : "invalid";
    if (status !== "available" || value === null) return { status, left_percent: null, right_percent: null,
      recorded_side: null, calculated_side: null, method: "not_computed" };
    const side = parsed.data!.raw_side;
    const other = side === "left" ? "right" : "left";
    return { status, left_percent: side === "left" ? value : 100 - value,
      right_percent: side === "right" ? value : 100 - value,
      recorded_side: side, calculated_side: other, method: "recorded_and_complement" };
  }
  const result = { schema_version: 1, activity_id: metrics.activity_id, source_fit_sha256: metrics.source_fit_sha256,
    status: matched ? "available" : ready ? "unverified" : "unavailable", reason,
    declaration: matched ? { ...parsed.data!, context_id: context.context_id } : null,
    session: project(metrics.average_ground_contact_balance),
    laps: { status: metrics.laps.status, reason: metrics.laps.reason, items: metrics.laps.items.map(lap => ({
      lap: lap.lap, start_time_utc: lap.start_time_utc, end_time_utc: lap.end_time_utc,
      ...project(lap.average_ground_contact_balance),
    })) } };
  if (rawSegments === undefined) return result;
  const segments = object(rawSegments), aggregates: Record<string, unknown> = {};
  const associated = segments.association === "matched" && segments.activity_id === metrics.activity_id
    && segments.source_fit_sha256 === metrics.source_fit_sha256
    && segments.session_start_time_utc === metrics.session_start_time_utc;
  for (const name of ["executed_intervals", "whole_distance_halves", "main_distance_halves"]) {
    // The caller validates this existing segment contract before side projection.
    const source = associated ? object(segments[name]) : {}, sourceReady = source.status === "available";
    const items: Json[] = [];
    if (sourceReady && Array.isArray(source.items)) for (const value of source.items) {
      const row = object(value), metric = object(row.average_ground_contact_balance);
      const projected = project({
        status: ["available", "missing", "invalid", "unavailable"].includes(String(metric.status))
          ? metric.status as Metric["status"] : "unavailable",
        value: typeof metric.value === "number" && Number.isFinite(metric.value) ? metric.value : null,
      });
      const identity = Object.fromEntries(["interval", "segment", "source_laps"]
        .filter(key => Object.hasOwn(row, key)).map(key => [key, row[key]]));
      items.push({ ...identity, status: projected.status, left_percent: projected.left_percent,
        right_percent: projected.right_percent, raw_side: projected.recorded_side,
        calculated_side: projected.calculated_side, both_sides_derived: true,
        method: projected.status === "available" ? "user_declared_record_mean_and_complement" : "not_computed",
        coverage: { ...object(metric.coverage) } });
    }
    aggregates[name] = { status: matched && sourceReady ? "available" : ready ? "unverified" : "unavailable",
      reason: reason || source.reason || (sourceReady ? null : "source_unavailable"),
      source_metric: `segment_running_metrics.${name}.average_ground_contact_balance`, items };
  }
  return { ...result, schema_version: 2, record_aggregates: aggregates };
}

function exactJson(actual: unknown, expected: unknown): boolean {
  if (actual === expected) return true;
  if (!actual || !expected || typeof actual !== "object" || typeof expected !== "object"
    || Array.isArray(actual) !== Array.isArray(expected)) return false;
  const left = actual as Record<string, unknown>, right = expected as Record<string, unknown>;
  return Object.keys(left).length === Object.keys(right).length
    && Object.keys(right).every(key => Object.hasOwn(left, key) && exactJson(left[key], right[key]));
}

export function validGroundContactSides(analysis: Record<string, unknown>, metrics: Metrics): boolean {
  const derived = object(analysis.ground_contact_sides).schema_version === 2;
  if (derived && !["1.9.0", "1.9.1"].includes(String(analysis.analyzer_version))) return false;
  return exactJson(analysis.ground_contact_sides, expectedGctSides(metrics, analysis.user_context,
    derived ? analysis.segment_running_metrics : undefined));
}
