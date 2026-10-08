/** Read-only projection from already decoded FIT JSON, never raw FIT or DTOs. */
import { z } from "zod";
import { type Activity, rawGarminActivityId } from "./lib";

function effectSchema(field: "total_training_effect" | "total_anaerobic_training_effect") {
  return z.discriminatedUnion("status", [
    z.strictObject({ status: z.literal("available"), value: z.number().finite().min(0).max(5), source_field: z.literal(field) }),
    z.strictObject({ status: z.enum(["missing", "invalid", "unavailable"]), value: z.null(), source_field: z.literal(field) }),
  ]);
}
export const trainingContextSchema = z.strictObject({
  schema_version: z.literal(1),
  source: z.literal("garmin_fit_session"),
  method: z.literal("fit_recorded_estimate"),
  scale: z.literal("0_to_5"),
  activity_id: z.string(),
  association: z.enum(["matched", "source_missing", "source_unreadable", "invalid_schema",
    "decode_errors", "id_mismatch", "time_mismatch", "sessions_missing", "sessions_ambiguous"]),
  local_start_time: z.string().nullable(),
  local_date: z.string().nullable(),
  session_start_time_utc: z.string().nullable(),
  measured_at_utc: z.string().nullable(),
  source_fit_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  aerobic: effectSchema("total_training_effect"),
  anaerobic: effectSchema("total_anaerobic_training_effect"),
});
type TrainingContext = z.infer<typeof trainingContextSchema>;
type Association = TrainingContext["association"];
type Effect = TrainingContext["aerobic"];

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}

/** Validate the calendar/clock without inferring a timezone for Garmin-local time. */
function clock(value: unknown): string | null {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?$/.test(value)) return null;
  const normalized = value.replace(" ", "T");
  const time = Date.parse(`${normalized}Z`);
  return Number.isFinite(time) && new Date(time).toISOString().slice(0, 19) === normalized.slice(0, 19)
    ? normalized : null;
}
function utcTime(value: unknown, allowNaive = false): string | null {
  if (typeof value !== "string") return null;
  const hasUtc = /(?:Z|\+00:00)$/.test(value);
  if (!hasUtc && !allowNaive) return null;
  const parsed = clock(hasUtc ? value.replace(/(?:Z|\+00:00)$/, "") : value);
  return parsed ? new Date(`${parsed}Z`).toISOString() : null;
}

function effect(row: Record<string, unknown>, field: Effect["source_field"]): Effect {
  const value = row[field];
  if (value === undefined || value === null) return { status: "missing", value: null, source_field: field };
  // Decoder.read() already applies scale 10. Never divide or coerce again.
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 5
    || Math.abs(value * 10 - Math.round(value * 10)) > 1e-9) {
    return { status: "invalid", value: null, source_field: field };
  }
  return { status: "available", value, source_field: field };
}

export function trainingContext(activity: Activity, decoded: unknown,
  readFailure: "source_missing" | "source_unreadable" = "source_missing"): TrainingContext {
  const result: TrainingContext = {
    schema_version: 1, source: "garmin_fit_session", method: "fit_recorded_estimate", scale: "0_to_5",
    activity_id: activity.id, association: readFailure,
    local_start_time: null, local_date: null, session_start_time_utc: null,
    measured_at_utc: null, source_fit_sha256: null,
    aerobic: { status: "unavailable", value: null, source_field: "total_training_effect" },
    anaerobic: { status: "unavailable", value: null, source_field: "total_anaerobic_training_effect" },
  };
  const fail = (association: Association) => { result.association = association; return result; };
  if (decoded === undefined || (decoded === null && readFailure === "source_unreadable")) return result;
  const payload = record(decoded);
  if (!payload || payload.schema_version !== 1 || payload.source !== "garmin-fit"
    || !Array.isArray(payload.decode_errors)) return fail("invalid_schema");
  if (payload.decode_errors.length) return fail("decode_errors");
  const identity = record(payload.activity);
  const id = rawGarminActivityId(activity.id);
  if (!identity || activity.source !== "garmin" || !/^\d{1,20}$/.test(id)
    || identity.id !== id) return fail("id_mismatch");
  const sourceStart = utcTime(identity.start_time_gmt, true);
  if (!sourceStart || !activity.date || Date.parse(sourceStart) !== activity.date.getTime()) return fail("time_mismatch");
  result.local_start_time = clock(identity.start_time_local);
  result.local_date = result.local_start_time?.slice(0, 10) ?? null;
  if (typeof payload.source_fit_sha256 === "string" && /^[a-f0-9]{64}$/.test(payload.source_fit_sha256)) {
    result.source_fit_sha256 = payload.source_fit_sha256;
  }
  const messages = record(payload.messages);
  if (payload.messages !== undefined && payload.messages !== null && !messages) return fail("invalid_schema");
  const sessions = messages?.session_mesgs;
  if (sessions === undefined || sessions === null || (Array.isArray(sessions) && !sessions.length)) return fail("sessions_missing");
  if (!Array.isArray(sessions)) return fail("invalid_schema");
  // A session sharing the activity start can still be just one leg of multisport.
  if (sessions.length !== 1) return fail("sessions_ambiguous");
  const session = record(sessions[0]);
  if (!session) return fail("invalid_schema");
  const start = utcTime(session.start_time);
  if (!start || start !== sourceStart) return fail("time_mismatch");
  result.association = "matched";
  result.session_start_time_utc = start;
  const measuredAt = utcTime(session.timestamp);
  result.measured_at_utc = measuredAt && measuredAt >= start ? measuredAt : null;
  result.aerobic = effect(session, "total_training_effect");
  result.anaerobic = effect(session, "total_anaerobic_training_effect");
  return result;
}
