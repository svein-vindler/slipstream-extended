/** Shared current-selection contract; immutable object timestamps are legacy only. */
import { recordedRunningSegmentsSchema } from "./recorded-running-segments";
import { validStoredCoachContext } from "./coach-stored-context";
import { validCoachTrainingEffect } from "./coach-training-effect";
import { validGroundContactSides } from "./gct-sides";
import { validSegmentRunningMetrics } from "./segment-running-metrics";
import { validNativeLapWorkoutBinding } from "./native-lap-workout-binding";
import { validLogicalBouts } from "./logical-bouts";
import { validRecordedCadence } from "./recorded-cadence";
import { validWorkoutTargets } from "./workout-targets";

type Json = Record<string, unknown>;
export type CoachSelection =
  | { status: "available"; analysis: Json }
  | { status: "absent" | "unverified" };
type ReadJson = (keys: string[], limits?: { stored: number; decoded: number }) =>
  Promise<{ key: string; data: unknown; uploaded?: Date } | null>;
const POINTER_LIMITS = { stored: 64 * 1024, decoded: 64 * 1024 };
function record(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
function scopedKey(key: string, prefix: string): boolean {
  return key.startsWith(prefix) && /^[a-zA-Z0-9_-]{1,128}\.json(?:\.gz)?$/.test(key.slice(prefix.length));
}

function uploadTime(value: Date | undefined): number | null {
  return value instanceof Date && Number.isFinite(value.getTime()) ? value.getTime() : null;
}
function latestLegacy(versions: R2Object[], prefix: string):
  { status: "selected"; object: R2Object } | { status: "absent" | "unverified" } {
  if (!versions.length) return { status: "absent" };
  const scoped = versions.filter(object => scopedKey(object.key, prefix));
  // Missing clocks and object-key tie-breakers cannot prove a current selection.
  if (!scoped.length || scoped.some(object => uploadTime(object.uploaded) === null)) return { status: "unverified" };
  scoped.sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime());
  if (scoped.some(object => object.key !== scoped[0].key
    && object.uploaded.getTime() === scoped[0].uploaded.getTime())) return { status: "unverified" };
  return { status: "selected", object: scoped[0] };
}

export async function readCurrentCoachInput(options: {
  canonicalPrefix: string; activityId: string; date?: string;
  read: ReadJson; versions: () => Promise<R2Object[]>;
  allowLegacy?: boolean; pointerUploaded?: Date; acceptPointer?: (pointer: Json) => boolean;
}): Promise<CoachSelection> {
  const { canonicalPrefix: prefix, activityId, read } = options;
  if (!prefix) return { status: "unverified" };
  const pointerKey = prefix.slice(0, -"canonical/".length) + "latest-ready.json";
  const storedPointer = await read([pointerKey], POINTER_LIMITS);
  let key: string | undefined;
  let strictIdentity = false;
  if (storedPointer) {
    const pointer = record(storedPointer.data);
    key = typeof pointer.analysis_key === "string" ? pointer.analysis_key : undefined;
    if (pointer.schema_version !== 1 || !key || !scopedKey(key, prefix)
      || (pointer.selection_version !== undefined && pointer.selection_version !== 1)
      || (options.acceptPointer && !options.acceptPointer(pointer))) return { status: "unverified" };
    strictIdentity = pointer.selection_version === 1;
    if (strictIdentity && !/^[0-9a-f]{24}\.json$/.test(key.slice(prefix.length))) return { status: "unverified" };
    if (!strictIdentity) {
      // Old bulk rewrote canonical objects; old targeted writers advanced the
      // pointer. Respect whichever legacy commit was newer. Marked selections
      // never use these upload clocks, including a return to an older A.
      const versions = await options.versions();
      // A broken original reference cannot be replaced by other legacy history.
      if (!versions.some(object => object.key === key)) return { status: "unverified" };
      const legacy = latestLegacy(versions, prefix);
      if (legacy.status === "unverified") return legacy;
      if (legacy.status === "selected" && legacy.object.key !== key) {
        const pointerTime = uploadTime(storedPointer.uploaded ?? options.pointerUploaded);
        const newestTime = legacy.object.uploaded.getTime();
        if (pointerTime === null || newestTime === pointerTime) return { status: "unverified" };
        if (newestTime > pointerTime) {
          if (options.allowLegacy === false) return { status: "unverified" };
          key = legacy.object.key;
        }
      }
    }
  } else {
    // Only a missing pointer permits legacy discovery. Corrupt/provider errors
    // propagate, and an invalid/dangling pointer never silently selects history.
    if (options.allowLegacy === false) return { status: "unverified" };
    const legacy = latestLegacy(await options.versions(), prefix);
    if (legacy.status !== "selected") return legacy;
    key = legacy.object.key;
  }
  if (!key) return { status: "unverified" };
  const stored = await read([key]);
  const analysis = record(stored?.data);
  if (analysis.schema_version !== 1 || String(analysis.activity_id) !== activityId
    || typeof analysis.analysis_id !== "string"
    || (options.date && record(analysis.activity).date !== options.date)
    || (strictIdentity && analysis.analysis_id !== key.slice(prefix.length, -5))) return { status: "unverified" };
  if (!["1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "logical_bouts" in analysis) return { status: "unverified" };
  if (!["1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "recorded_cadence" in analysis) return { status: "unverified" };
  if (!["1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "workout_targets" in analysis) return { status: "unverified" };
  if (!["1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "native_lap_workout_binding" in analysis) return { status: "unverified" };
  if (!["1.5.0", "1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "ground_contact_sides" in analysis) return { status: "unverified" };
  if (!["1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "segment_running_metrics" in analysis) return { status: "unverified" };
  if (["1.2.0", "1.3.0", "1.4.0", "1.5.0", "1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version))) {
    const measurements = recordedRunningSegmentsSchema.safeParse(analysis.recorded_running_metrics);
    if (!measurements.success || measurements.data.activity_id !== activityId) return { status: "unverified" };
    const block = measurements.data;
    if ((analysis.analyzer_version === "1.3.0" || (["1.4.0", "1.5.0", "1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && "stored_context_snapshot" in analysis))
      && !validStoredCoachContext(analysis.stored_context_snapshot,
      block.session_start_time_utc, block.local_date)) return { status: "unverified" };
    if (["1.4.0", "1.5.0", "1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && !validCoachTrainingEffect(analysis, activityId)) return { status: "unverified" };
    if (["1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && !validSegmentRunningMetrics(analysis, block)) return { status: "unverified" };
    if (["1.5.0", "1.6.0", "1.7.0", "1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && !validGroundContactSides(analysis, block)) return { status: "unverified" };
    if (["1.8.0", "1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && !validNativeLapWorkoutBinding(analysis, block)) return { status: "unverified" };
    if (["1.9.0", "1.9.1"].includes(String(analysis.analyzer_version)) && !validLogicalBouts(analysis, block)) return { status: "unverified" };
    if ("recorded_cadence" in analysis && !validRecordedCadence(analysis, block)) return { status: "unverified" };
    if ("workout_targets" in analysis && !validWorkoutTargets(analysis)) return { status: "unverified" };
    const sourceDigest = record(analysis.source).fit_sha256;
    if (block.association === "matched") {
      if (block.source_fit_sha256 !== sourceDigest
        || (block.local_date !== null && block.local_date !== record(analysis.activity).date)) return { status: "unverified" };
    } else {
      // A real producer failure can precede digest/date association. Keep the
      // ordinary summary, but only with the producer's wholly unavailable block.
      if (block.source_fit_sha256 !== null && block.source_fit_sha256 !== sourceDigest) return { status: "unverified" };
      const values = [block.average_power, block.average_step_length, block.average_ground_contact_time,
        block.average_vertical_oscillation, block.average_vertical_ratio, block.average_ground_contact_balance];
      const groups = [block.laps, block.kilometer_splits, block.distance_halves];
      if (values.some(value => value.status !== "unavailable" || value.value !== null)
        || groups.some(group => group.status !== "unavailable" || group.reason !== block.association || group.items.length !== 0)) {
        return { status: "unverified" };
      }
    }
  }
  return { status: "available", analysis };
}
