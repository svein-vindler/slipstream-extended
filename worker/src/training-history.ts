import { z } from "zod";

export const TRAINING_DAY_BYTES = 16 * 1024;
export const TRAINING_STRUCTURED_BYTES = 64 * 1024;
export const TRAINING_RESULT_BYTES = 128 * 1024;
const dayString = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const deviceId = z.string().regex(/^[1-9][0-9]{0,19}$/);
function exactDay(day: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(day) && !day.startsWith("0000-") && Number.isFinite(Date.parse(`${day}T00:00:00Z`))
    && new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) === day;
}
function exactClock(value: string) {
  return exactDay(value.slice(0, 10)) && Number(value.slice(11, 13)) < 24
    && Number(value.slice(14, 16)) < 60 && Number(value.slice(17, 19)) < 60;
}
const utcTime = z.string().max(40).regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$/)
  .refine(exactClock);
const localTime = z.string().max(32).regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$/).refine(exactClock);
const timeSource = z.enum(["garmin_offset", "configured_timezone", "unavailable", "timezone_conflict"]);
const attemptStatus = z.enum(["available", "partial", "no_observation", "auth_required", "source_error", "timeout", "oversize", "schema_unverified", "wrong_date"]);
const metricStatus = z.enum(["available", "conflicting", "invalid", "no_observation"]);
const metric = z.object({ value: z.number().int().nonnegative().max(2 ** 31 - 1).nullable(), status: metricStatus,
  unit: z.enum(["score_1_100", "minutes"]), source_checked_at: utcTime.nullable(),
  observation_local: localTime.nullable(), observation_at: utcTime.nullable(),
  time_source: timeSource, retained_previous: z.boolean(), reached_zero: z.boolean().nullable().optional() });
const observation = z.object({ calendar_date: dayString, observation_local: localTime.nullable(),
  observation_at: utcTime.nullable(), time_source: timeSource,
  input_context: z.literal("AFTER_WAKEUP_RESET").nullable(), readiness: metric, recovery: metric });
const slotStatus = z.enum(["available", "ambiguous", "no_observation"]);
const selectionStatus = z.enum(["available", "ambiguous", "no_observation", "selected_device_unavailable", "unknown"]);
export const trainingSdkVersion = z.enum(["0.3.17", "0.3.20", "mixed"]);

export const trainingDaySchema = z.object({ schema_version: z.literal(1), kind: z.literal("training-day"),
  normalizer_revision: z.literal(1), source: z.literal("garmin-connect"), sdk_version: trainingSdkVersion,
  requested_date: dayString, calendar_date: dayString, source_checked_at: utcTime.nullable(),
  source_revision: z.string().regex(/^[a-f0-9]{64}$/),
  date_resolution: z.object({ timezone: z.string().min(1).max(128), basis: z.literal("configured_timezone") }),
  last_attempt: z.object({ status: attemptStatus, attempted_at: utcTime }),
  selection: z.object({ status: selectionStatus, device_id: deviceId.nullable() }),
  devices: z.array(z.object({ device_id: deviceId, latest: observation.nullable(), morning: observation.nullable(),
    latest_status: slotStatus, morning_status: slotStatus })).max(4),
}).superRefine((value, ctx) => {
  const invalid = () => ctx.addIssue({ code: "custom", message: "Unverified canonical training data" });
  if (value.requested_date !== value.calendar_date || new Set(value.devices.map(d => d.device_id)).size !== value.devices.length) invalid();
  try { new Intl.DateTimeFormat("en", { timeZone: value.date_resolution.timezone }); } catch { invalid(); }
  if (value.selection.status === "available" && !value.devices.some(d => d.device_id === value.selection.device_id)) invalid();
  for (const device of value.devices) for (const obs of [device.latest, device.morning]) {
    if (!obs) continue;
    if (obs.calendar_date !== value.calendar_date) invalid();
    if (obs.observation_local !== null && obs.observation_local.slice(0, 10) !== value.calendar_date) invalid();
    for (const [field, unit, low, high] of [[obs.readiness, "score_1_100", 1, 100], [obs.recovery, "minutes", 0, 2 ** 31 - 1]] as const) {
      const good = field.status === "available" || field.status === "conflicting";
      if (field.unit !== unit || (good && (field.value === null || field.value < low || field.value > high || !field.source_checked_at))
          || (!good && (field.value !== null || field.source_checked_at !== null))) invalid();
      if (field.observation_local !== null && field.observation_local.slice(0, 10) !== value.calendar_date) invalid();
      if (field.status === "conflicting" && (unit !== "minutes" || !field.reached_zero || !field.value)) invalid();
      if (unit === "minutes" && field.reached_zero !== null && typeof field.reached_zero !== "boolean") invalid();
      if (unit === "minutes" && field.status === "available" && field.reached_zero === true && field.value !== null && field.value > 0) invalid();
    }
  }
});

const clockRef = z.string().regex(/^clock_[1-9][0-9]?$/);
const checkRef = z.string().regex(/^check_[1-9][0-9]?$/);
const observationRef = z.string().regex(/^observation_[1-8]$/);
const outputClock = z.object({ time_ref: clockRef, observation_local: localTime.nullable(), observation_at: utcTime.nullable(),
  time_source: timeSource, observation_age_seconds: z.number().nonnegative().nullable(),
  display_state: z.enum(["historical", "within_8h", "older_than_8h", "unknown"]) });
const outputMetric = z.object({ value: z.number().int().nonnegative().max(2 ** 31 - 1).nullable(), status: metricStatus,
  time_ref: clockRef, check_ref: checkRef.nullable(),
  retained_previous: z.boolean(), reached_zero: z.boolean().nullable().optional(),
  effective_remaining_minutes: z.number().nonnegative().nullable().optional() });
const outputObservation = z.object({ observation_ref: observationRef, time_ref: clockRef,
  input_context: z.literal("AFTER_WAKEUP_RESET").nullable(), readiness: outputMetric, recovery: outputMetric });
const outputCheck = z.object({ check_ref: checkRef, source_checked_at: utcTime,
  source_check_age_seconds: z.number().nonnegative().nullable() });
const outputDay = z.object({ date: dayString,
  status: z.enum(["available", "partial", "no_observation", "not_collected", "schema_unverified", "source_error", "oversize", "auth_required", "timeout", "wrong_date", "ambiguous", "selected_device_unavailable"]),
  basis: z.enum(["canonical_object", "canonical_object_absent", "canonical_read_failed"]), support: z.literal("unknown"),
  provenance: z.object({ source_revision: z.string(), sdk_version: trainingSdkVersion,
    source_checked_at: utcTime.nullable(), last_attempt: z.object({ status: attemptStatus, attempted_at: utcTime }),
    timezone: z.string() }).optional(),
  selection_status: selectionStatus.optional(), selected_candidate: z.string().nullable().optional(),
  clocks: z.array(outputClock).max(24).optional(), checks: z.array(outputCheck).max(16).optional(), observations: z.array(outputObservation).max(8).optional(),
  candidates: z.array(z.object({ candidate_ref: z.string(), latest_observation: observationRef.nullable(), morning_observation: observationRef.nullable(),
    latest_status: slotStatus, morning_status: slotStatus })).max(4).optional(),
});
export const trainingHistorySchema = z.object({ start_date: dayString, end_date: dayString,
  projection: z.literal("training-history-compact-v1"), as_of: utcTime,
  source: z.object({ name: z.literal("garmin-connect"), sdk_version: trainingSdkVersion.nullable(), normalizer_revision: z.literal(1) }),
  metric_units: z.object({ readiness: z.literal("score_1_100"), recovery: z.literal("minutes") }),
  days_requested: z.number().int().min(1).max(31), source_objects_read: z.number().int().min(0).max(31),
  object_bytes_read: z.number().int().nonnegative(),
  display_policy: z.object({ id: z.literal("training-observation-8h-v1"), max_age_seconds: z.literal(28800),
    semantics: z.literal("stored_observation_not_live_freshness") }),
  days: z.array(outputDay).max(31), status: z.enum(["complete", "output_limit"]),
  limit: z.enum(["structured_content", "mcp_result"]).optional(), message: z.string().optional() });

export function trainingDates(start: string, end: string): string[] {
  if (!exactDay(start) || !exactDay(end) || start > end) throw new Error("Choose real dates in ascending order.");
  const count = (Date.parse(`${end}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`)) / 86400000 + 1;
  if (count > 31) throw new Error("Training history permits at most 31 dates.");
  return Array.from({ length: count }, (_, i) => new Date(Date.parse(`${start}T00:00:00Z`) + i * 86400000).toISOString().slice(0, 10));
}
export function trainingDayKey(day: string) {
  trainingDates(day, day);
  return `health/training-context/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
}

function age(value: string | null, now: number) {
  if (value === null) return null;
  const seconds = (now - Date.parse(value)) / 1000;
  return Number.isFinite(seconds) && seconds >= 0 ? seconds : null;
}
function project(day: z.infer<typeof trainingDaySchema>, now: number): z.infer<typeof outputDay> {
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: day.date_resolution.timezone,
    year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(now));
  const clocks: z.infer<typeof outputClock>[] = [];
  const clockRefs = new Map<string, string>();
  const checks: z.infer<typeof outputCheck>[] = [];
  const checkRefs = new Map<string, string>();
  const observations: z.infer<typeof outputObservation>[] = [];
  const clock = (value: Pick<z.infer<typeof metric>, "observation_local" | "observation_at" | "time_source">) => {
    const observationAge = age(value.observation_at, now);
    const data = { observation_local: value.observation_local, observation_at: value.observation_at, time_source: value.time_source,
      observation_age_seconds: observationAge,
      display_state: day.calendar_date < today ? "historical" as const : observationAge === null ? "unknown" as const
        : observationAge <= 28800 ? "within_8h" as const : "older_than_8h" as const };
    const key = JSON.stringify(data);
    let ref = clockRefs.get(key);
    if (!ref) { ref = `clock_${clocks.length + 1}`; clockRefs.set(key, ref); clocks.push({ time_ref: ref, ...data }); }
    return ref;
  };
  const check = (checked: string | null) => {
    if (checked === null) return null;
    let ref = checkRefs.get(checked);
    if (!ref) { ref = `check_${checks.length + 1}`; checkRefs.set(checked, ref); checks.push({ check_ref: ref,
      source_checked_at: checked, source_check_age_seconds: age(checked, now) }); }
    return ref;
  };
  const field = (value: z.infer<typeof metric>) => ({ value: value.value, status: value.status, time_ref: clock(value),
    check_ref: check(value.source_checked_at),
    retained_previous: value.retained_previous });
  const candidates = day.devices.map((device, index) => {
    // Only identical morning/latest within this device share an observation.
    // References and clocks live in this response's day row, never in a cache.
    const refs = new Map<string, string>();
    const obs = (value: z.infer<typeof observation> | null) => {
      if (value === null) return null;
      const data = { time_ref: clock(value), input_context: value.input_context, readiness: field(value.readiness),
        recovery: { ...field(value.recovery), reached_zero: value.recovery.reached_zero,
          effective_remaining_minutes: value.recovery.status === "available" ? value.recovery.value : null } };
      const key = JSON.stringify(data);
      let ref = refs.get(key);
      if (!ref) { ref = `observation_${observations.length + 1}`; refs.set(key, ref); observations.push({ observation_ref: ref, ...data }); }
      return ref;
    };
    return { candidate_ref: `candidate_${index + 1}`, latest_observation: obs(device.latest), morning_observation: obs(device.morning),
      latest_status: device.latest_status, morning_status: device.morning_status };
  });
  const selectedDevice = day.devices.find(d => d.device_id === day.selection.device_id);
  const checkedComplete = day.last_attempt.status === "available" || day.last_attempt.status === "no_observation";
  // Also read older v1 objects safely: a failed/partial check cannot establish absence.
  const selection = day.selection.status === "selected_device_unavailable" && !checkedComplete
    ? selectedDevice ? "available" : "unknown" : day.selection.status;
  const status = !checkedComplete ? day.last_attempt.status : selection === "ambiguous" || selection === "selected_device_unavailable"
    ? selection : selectedDevice?.latest_status === "ambiguous" ? "ambiguous"
      : day.devices.length === 0 ? day.last_attempt.status
      : day.last_attempt.status === "available" ? "available" : "partial";
  return { date: day.calendar_date, status, basis: "canonical_object", support: "unknown",
    provenance: { source_revision: day.source_revision, sdk_version: day.sdk_version, source_checked_at: day.source_checked_at,
      last_attempt: day.last_attempt, timezone: day.date_resolution.timezone }, selection_status: selection,
    selected_candidate: day.selection.device_id === null ? null : (() => {
      const index = day.devices.findIndex(d => d.device_id === day.selection.device_id);
      return index < 0 ? null : `candidate_${index + 1}`;
    })(),
    clocks, checks, observations, candidates };
}

export async function readTrainingHistory(bucket: Pick<R2Bucket, "get">, start: string, end: string, now = Date.now()) {
  const dates = trainingDates(start, end); // Validate all inputs before storage.
  const days: z.infer<typeof outputDay>[] = new Array(dates.length);
  let next = 0, bytesRead = 0;
  async function reader() {
    while (next < dates.length) {
      const index = next++, date = dates[index];
      const fail = (status: "source_error" | "oversize" | "schema_unverified") => ({ date, status,
        basis: "canonical_read_failed" as const, support: "unknown" as const });
      let object: R2ObjectBody | null = null;
      let stream: ReadableStreamBYOBReader | null = null;
      try {
        object = await bucket.get(trainingDayKey(date));
        if (object === null) {
          days[index] = { date, status: "not_collected", basis: "canonical_object_absent", support: "unknown" };
          continue;
        }
        if (object.size > TRAINING_DAY_BYTES) { days[index] = fail("oversize"); continue; }
        if (!Number.isInteger(object.size) || object.size < 0) { days[index] = fail("schema_unverified"); continue; }
        if (object.httpMetadata?.contentEncoding && object.httpMetadata.contentEncoding !== "identity") {
          days[index] = fail("schema_unverified"); continue;
        }
        stream = object.body.getReader({ mode: "byob" });
        const buffer = new Uint8Array(TRAINING_DAY_BYTES);
        let size = 0, oversize = false;
        while (true) {
          const part = await stream.read(new Uint8Array(Math.min(4096, TRAINING_DAY_BYTES + 1 - size)));
          if (part.done) break;
          // BYOB limits consumption, including the one overflow-control byte.
          bytesRead += part.value.byteLength;
          if (size + part.value.byteLength > TRAINING_DAY_BYTES) { oversize = true; break; }
          buffer.set(part.value, size); size += part.value.byteLength;
        }
        if (oversize) { days[index] = fail("oversize"); continue; }
        if (size !== object.size) { days[index] = fail("schema_unverified"); continue; }
        try {
          const parsed = trainingDaySchema.safeParse(JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(buffer.subarray(0, size))));
          days[index] = parsed.success && parsed.data.calendar_date === date ? project(parsed.data, now) : fail("schema_unverified");
        } catch { days[index] = fail("schema_unverified"); }
      } catch { days[index] = fail("source_error"); }
      finally {
        try {
          if (stream) { try { await stream.cancel(); } finally { stream.releaseLock(); } }
          else if (object) await object.body.cancel();
        } catch { days[index] = fail("source_error"); }
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(4, dates.length) }, () => reader()));
  const versions = new Set(days.flatMap(day => day.provenance ? [day.provenance.sdk_version] : []));
  const sdkVersion = versions.size === 1 ? [...versions][0] : versions.size === 0 ? null : "mixed" as const;
  return { start_date: start, end_date: end, days_requested: dates.length, source_objects_read: dates.length,
    projection: "training-history-compact-v1" as const, as_of: new Date(now).toISOString(),
    source: { name: "garmin-connect" as const, sdk_version: sdkVersion, normalizer_revision: 1 as const },
    metric_units: { readiness: "score_1_100" as const, recovery: "minutes" as const },
    object_bytes_read: bytesRead, display_policy: { id: "training-observation-8h-v1" as const,
      max_age_seconds: 28800 as const, semantics: "stored_observation_not_live_freshness" as const }, days, status: "complete" as const };
}

export function boundedTrainingResult(value: z.infer<typeof trainingHistorySchema>) {
  const result = (payload: z.infer<typeof trainingHistorySchema>) => ({
    content: [{ type: "text" as const, text: JSON.stringify(payload) }], structuredContent: payload });
  const bytes = (payload: unknown) => new TextEncoder().encode(JSON.stringify(payload)).byteLength;
  const full = result(value);
  const limit = bytes(value) > TRAINING_STRUCTURED_BYTES ? "structured_content" : bytes(full) > TRAINING_RESULT_BYTES ? "mcp_result" : null;
  return limit === null ? full : result({ ...value, days: [], status: "output_limit", limit,
    message: "Training history exceeds the output limit. Request a shorter period." });
}
