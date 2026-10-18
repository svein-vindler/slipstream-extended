/** Five already-scaled FIT measurements; no fetching, estimates or raw messages. */
import { z } from "zod";
import { type Activity, rawGarminActivityId, validCalendarDay } from "./lib";

function metricSchema<U extends string, F extends string>(unit: U, field: F) {
  return z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("available"), value: z.number().finite(), unit: z.literal(unit), source_field: z.literal(field) }),
    z.strictObject({ status: z.enum(["missing", "invalid", "unavailable"]), value: z.null(), unit: z.literal(unit), source_field: z.literal(field) }),
  ]);
}
export const recordedRunningMetricsSchema = z.strictObject({
  schema_version: z.literal(1), source: z.literal("garmin_fit_session"), method: z.literal("fit_recorded_measurement"),
  activity_id: z.string(),
  association: z.enum(["matched", "source_missing", "source_unreadable", "invalid_schema", "decode_errors",
    "id_mismatch", "time_mismatch", "sessions_missing", "sessions_ambiguous", "unsupported_sport"]),
  local_start_time: z.string().nullable(), local_date: z.string().nullable(),
  session_start_time_utc: z.string().nullable(), measured_at_utc: z.string().nullable(),
  source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  average_power: metricSchema("W", "avg_power"),
  average_step_length: metricSchema("mm", "avg_step_length"),
  average_ground_contact_time: metricSchema("ms", "avg_stance_time"),
  average_vertical_oscillation: metricSchema("mm", "avg_vertical_oscillation"),
  average_vertical_ratio: metricSchema("%", "avg_vertical_ratio"),
});
type RecordedRunningMetrics = z.infer<typeof recordedRunningMetricsSchema>;
type Metric<U extends string, F extends string> =
  | { status: "available"; value: number; unit: U; source_field: F }
  | { status: "missing" | "invalid" | "unavailable"; value: null; unit: U; source_field: F };

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}
function clock(value: unknown): string | null {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?$/.test(value)
    || !validCalendarDay(value.slice(0, 10))) return null;
  const normalized = value.replace(" ", "T");
  const time = Date.parse(`${normalized}Z`);
  if (!Number.isFinite(time)) return null;
  const iso = new Date(time).toISOString();
  if (iso.slice(0, 19) !== normalized.slice(0, 19)) return null;
  return iso.slice(20, 23) === "000" ? iso.slice(0, 19) : iso.slice(0, -1);
}
function utc(value: unknown, allowNaive = false): string | null {
  if (typeof value !== "string") return null;
  const explicitUtc = /(?:Z|\+00:00)$/.test(value);
  if (!explicitUtc && !allowNaive) return null;
  const local = clock(explicitUtc ? value.replace(/(?:Z|\+00:00)$/, "") : value);
  return local ? new Date(`${local}Z`).toISOString() : null;
}
function measurement<U extends string, F extends string>(session: Record<string, unknown>, unit: U, field: F): Metric<U, F> {
  const value = session[field];
  if (value === undefined || value === null) return { status: "missing", value: null, unit, source_field: field };
  // The pinned FIT decoder has already applied each field's scale and sentinel handling.
  return typeof value === "number" && Number.isFinite(value)
    ? { status: "available", value, unit, source_field: field }
    : { status: "invalid", value: null, unit, source_field: field };
}
function declared(value: unknown): boolean { return value !== undefined && value !== null && !(typeof value === "string" && !value.trim()); }
function running(value: unknown): boolean { return typeof value === "string" && value.toLowerCase().includes("run"); }

export function recordedRunningMetrics(activity: Activity, decoded: unknown, endurance: unknown,
  readFailure: "source_missing" | "source_unreadable" = "source_missing"): RecordedRunningMetrics {
  const result: RecordedRunningMetrics = {
    schema_version: 1, source: "garmin_fit_session", method: "fit_recorded_measurement",
    activity_id: rawGarminActivityId(activity.id), association: readFailure,
    local_start_time: null, local_date: null, session_start_time_utc: null, measured_at_utc: null, source_fit_sha256: null,
    average_power: { status: "unavailable", value: null, unit: "W", source_field: "avg_power" },
    average_step_length: { status: "unavailable", value: null, unit: "mm", source_field: "avg_step_length" },
    average_ground_contact_time: { status: "unavailable", value: null, unit: "ms", source_field: "avg_stance_time" },
    average_vertical_oscillation: { status: "unavailable", value: null, unit: "mm", source_field: "avg_vertical_oscillation" },
    average_vertical_ratio: { status: "unavailable", value: null, unit: "%", source_field: "avg_vertical_ratio" },
  };
  const fail = (association: RecordedRunningMetrics["association"]) => { result.association = association; return result; };
  if (decoded === undefined || decoded === null) return result;
  const payload = record(decoded);
  if (!payload || payload.schema_version !== 1 || payload.source !== "garmin-fit" || !Array.isArray(payload.decode_errors)) return fail("invalid_schema");
  if (payload.decode_errors.length) return fail("decode_errors");
  const identity = record(payload.activity);
  const enduranceIdentity = record(record(endurance)?.activity);
  const id = result.activity_id;
  if (!identity || activity.source !== "garmin" || !/^\d{1,20}$/.test(id) || identity.id !== id
    || (enduranceIdentity?.id !== undefined && enduranceIdentity.id !== null && rawGarminActivityId(String(enduranceIdentity.id)) !== id)) return fail("id_mismatch");
  const sourceStart = utc(identity.start_time_gmt, true);
  if (!sourceStart || !activity.date || Date.parse(sourceStart) !== activity.date.getTime()) return fail("time_mismatch");
  if (identity.start_time_local !== undefined && identity.start_time_local !== null && identity.start_time_local !== "") {
    result.local_start_time = clock(identity.start_time_local);
    if (!result.local_start_time) return fail("time_mismatch");
    result.local_date = result.local_start_time.slice(0, 10);
    if (activity.localDate && result.local_date !== activity.localDate) return fail("time_mismatch");
  }
  if (typeof payload.source_fit_sha256 === "string" && /^[a-f0-9]{64}$/.test(payload.source_fit_sha256)) result.source_fit_sha256 = payload.source_fit_sha256;
  const messages = record(payload.messages);
  if (payload.messages !== undefined && payload.messages !== null && !messages) return fail("invalid_schema");
  const sessions = messages?.session_mesgs;
  if (sessions === undefined || sessions === null || (Array.isArray(sessions) && !sessions.length)) return fail("sessions_missing");
  if (!Array.isArray(sessions)) return fail("invalid_schema");
  if (sessions.length !== 1) return fail("sessions_ambiguous");
  const session = record(sessions[0]);
  if (!session) return fail("invalid_schema");
  const start = utc(session.start_time);
  if (!start || start !== sourceStart) return fail("time_mismatch");
  const activitySports = [activity.type, identity.type];
  if (!activitySports.some(running) || [...activitySports, session.sport].some(value => declared(value) && !running(value))) return fail("unsupported_sport");
  result.association = "matched";
  result.session_start_time_utc = start;
  const measured = utc(session.timestamp);
  result.measured_at_utc = measured && measured >= start ? measured : null;
  result.average_power = measurement(session, "W", "avg_power");
  result.average_step_length = measurement(session, "mm", "avg_step_length");
  result.average_ground_contact_time = measurement(session, "ms", "avg_stance_time");
  result.average_vertical_oscillation = measurement(session, "mm", "avg_vertical_oscillation");
  result.average_vertical_ratio = measurement(session, "%", "avg_vertical_ratio");
  return result;
}
