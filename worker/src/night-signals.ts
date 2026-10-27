import { readNightUserContext, nightUserContextSchema, nightUserContextViewSchema } from "./night-user-context";
import { applyNightUserContext } from "./night-view";
/** Bounded stored-only night signals. Readers and counters belong to one request. */
import type { McpServer, RequestId } from "@modelcontextprotocol/server";
import { z } from "zod";
import { R2Storage, type R2ReadLimits } from "./r2-storage";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { exactDate, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";

const LIMITS = {
  gets: 256, lists: 64, stored: 8 * 1024 * 1024, decoded: 16 * 1024 * 1024,
  input: 100000, native: 4096, structured: 256 * 1024,
  envelope: 512 * 1024, deadline: 10000,
} as const;
const encoder = new TextEncoder();
const integer = z.number().int().nonnegative();
const clock = z.string().nullable();
const sourceClock = z.union([z.string(), z.number().finite()]).nullable();
const statusSchema = z.enum(["available", "no_data", "not_stored", "invalid_schema", "read_error"]);
const resolutionSchema = z.enum(["native", "15m", "30m"]);
const alignmentSchema = z.enum(["sleep_start", "utc_clock"]);
const channelNameSchema = z.enum(["heart_rate", "hrv", "body_battery", "stress"]);
const requestedChannelsSchema = z.array(channelNameSchema).min(1).max(4)
  .refine(names => new Set(names).size === names.length, { message: "Select each channel once." })
  .default(["heart_rate", "hrv"]);
const CHANNEL_UNITS = { heart_rate: "bpm", hrv: "ms", body_battery: "points", stress: "points" } as const;
const nightProjectionSchema = z.object({
  wake_date: z.string(), night_of: clock,
  sleep_start_local: clock, sleep_end_local: clock, sleep_midpoint_local: clock,
  sleep_start_date_local: clock, sleep_end_date_local: clock,
  sleep_start_weekday_local: clock, sleep_end_weekday_local: clock,
  timezone: clock,
  local_time_source: z.enum(["garmin_local", "configured_timezone", "actual_night_timezone", "unavailable"]),
  actual_night_timezone: clock, configured_timezone: clock,
  configured_projection: z.object({
    timezone: z.string(), night_of: clock,
    sleep_start_local: clock, sleep_end_local: clock, sleep_midpoint_local: clock,
  }).nullable(),
  source_time: z.object({
    sleep_start_gmt: sourceClock, sleep_end_gmt: sourceClock,
    sleep_start_garmin_local: sourceClock, sleep_end_garmin_local: sourceClock,
  }),
  time_quality_flags: z.array(z.string()),
});
const sourceRevisionSchema = z.string().regex(/^[a-f0-9]{64}$/).nullable();
const observationOriginSchema = z.enum(["incoming", "retained"]).nullable();
const pointSchema = z.object({
  timestamp_utc: z.string(), value: z.number().finite(),
  source_timestamp: z.union([z.string(), z.number()]), time_basis: z.literal("utc"),
  timestamp_field: z.string(), timestamp_unit: z.string(),
  source_revision: sourceRevisionSchema.optional(),
  observation_origin: observationOriginSchema.optional(),
});
const qualitySchema = z.object({
  invalid: integer, ambiguous_time: integer, outside_interval: integer,
  duplicates: integer, conflicting_timestamps: integer, gaps: integer,
});
const minimumSchema = z.object({
  value: z.number(), method: z.literal("lowest-10-minute-median-v1"),
  window_start_utc: z.string(), window_end_utc: z.string(),
  coverage: z.number(), valid_points: integer,
}).nullable();
const binSchema = z.object({
  start_utc: z.string(), end_utc: z.string(), median: z.number().nullable(), count: integer,
});
const sourceQualityFlagSchema = z.enum([
  "missing_values", "partial_source", "retained_previous_observations", "source_completeness_unknown", "retained_revision_unknown",
  "invalid_source_shape", "missing_or_conflicting_source_date", "invalid_or_oversize_curve", "invalid_point",
  "invalid_timestamp", "invalid_timestamp_unit_or_range", "invalid_value",
  "conflicting_source_observations", "duplicate_source_observations", "unverified_source_observations",
]);
const sourceCompositionSchema = z.object({
  method_version: z.literal("preserve-good-observations-v1"), completeness: z.literal("unknown"),
  incoming_valid_points: integer.max(10000), retained_valid_points: integer.max(10000),
  new_valid_points: integer.max(10000), replaced_valid_points: integer.max(10000),
  conflicting_source_timestamps: integer.max(10000).optional(), duplicate_source_points: integer.max(10000).optional(),
  source_revisions: z.array(z.string().regex(/^[a-f0-9]{64}$/)).max(16),
  source_revision_count: integer.max(100000), source_revisions_truncated: z.boolean(),
}).strict();
const channelSchema = z.object({
  status: statusSchema, unit: z.enum(["bpm", "ms", "points"]), valid_points: integer,
  measurement_kind: z.literal("garmin_derived").optional(),
  observed_cadence_seconds: z.number().positive().nullable(),
  measurement_window_seconds: z.null(), measurement_window_basis: z.literal("not_documented_in_stored_source"),
  coverage: z.number().min(0).max(1).nullable(),
  coverage_basis: z.literal("occupied-observed-cadence-slots-v1"),
  largest_gap_seconds: z.number().nonnegative().nullable(),
  quality: qualitySchema, quality_flags: z.array(z.string()), source_dates: z.array(z.string()),
  source_revisions: z.array(z.object({ date: z.string(), revision: z.string() })),
  points: z.array(pointSchema), bins: z.array(binSchema), minimum: minimumSchema,
  source_quality_flags: z.array(sourceQualityFlagSchema).max(16).optional(),
  source_missing_value_count: integer.max(100000).describe("Known missing values across source calendar days; their individual times may be unknown.").optional(),
  source_compositions: z.array(sourceCompositionSchema.extend({date: z.string()})).max(8).optional(),
});
const channelsSchema = z.strictObject({
  heart_rate: channelSchema.optional(), hrv: channelSchema.optional(),
  body_battery: channelSchema.optional(), stress: channelSchema.optional(),
});
const resourcesSchema = z.object({
  r2_get_attempts: integer, r2_list_attempts: integer, stored_bytes: integer, decoded_bytes: integer,
  decoded_budget_bytes: integer, input_points: integer, returned_points: integer,
  elapsed_ms: z.number().nonnegative(), structured_bytes: integer, envelope_bytes: integer, envelope_budget_bytes: integer,
});
export const nightSignalsSchema = z.object({
  date: z.string(), wake_date: z.string(), night_of: clock,
  sleep_start_utc: clock, sleep_end_utc: clock, timezone: clock,
  configured_timezone: clock, actual_night_timezone: clock,
  night_context: nightProjectionSchema.nullable(),
  user_context: nightUserContextSchema.nullable().optional(), view: nightUserContextViewSchema.optional(),
  status: z.enum(["available", "partial", "not_stored", "invalid_schema", "read_error", "budget_exhausted", "output_limit"]),
  resolution: resolutionSchema, alignment: alignmentSchema, bin_origin_utc: clock,
  method_version: z.literal("night-signals-v1"),
  channels: channelsSchema,
  quality_flags: z.array(z.string()), resources: resourcesSchema,
});
export const nightSignalsHistorySchema = z.object({
  start_date: z.string(), end_date: z.string(), requested_nights: integer,
  status: z.enum(["available", "partial", "invalid_request", "budget_exhausted", "output_limit"]),
  resolution: resolutionSchema, alignment: alignmentSchema,
  method_version: z.literal("night-signals-v1"),
  nights: z.array(nightSignalsSchema.omit({ resources: true })).max(31),
  quality_flags: z.array(z.string()), resources: resourcesSchema,
});

type Row = Record<string, unknown>;
type Point = z.infer<typeof pointSchema>;
type Channel = z.infer<typeof channelSchema>;
type ChannelName = z.infer<typeof channelNameSchema>;
type Channels = z.infer<typeof channelsSchema>;
type ReadStatus = z.infer<typeof statusSchema>;
type Resolution = z.infer<typeof resolutionSchema>;
type Alignment = z.infer<typeof alignmentSchema>;
type NightResult = z.infer<typeof nightSignalsSchema>;
type HistoryResult = z.infer<typeof nightSignalsHistorySchema>;
type Stream = "sleep" | "hrv" | "signals";
type Stored = { status: "available"; data: Row; revision: string }
  | { status: Exclude<ReadStatus, "available" | "no_data"> };

class RequestLimit extends Error {}
class InvalidSource extends Error {}

function row(value: unknown): Row | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : null;
}
function iso(value: number) {
  return new Date(value).toISOString().replace(".000Z", "Z");
}
function bytes(value: unknown) {
  return encoder.encode(JSON.stringify(value)).byteLength;
}
function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}
function realDay(day: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(day) && !day.startsWith("0000-")
    && Number.isFinite(Date.parse(`${day}T00:00:00Z`))
    && new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) === day;
}
function objectKeys(stream: Stream, day: string) {
  const root = stream === "sleep" ? "health/sleep/v1"
    : stream === "signals" ? "health/signals/v1" : "health/hrv";
  const key = `${root}/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
  return [key, `${key}.gz`];
}
/** A naive timestamp is UTC only when its source explicitly documents GMT. */
function instant(value: unknown, explicitUtc: boolean, unit?: unknown): number | null {
  let parsed: number;
  if (typeof value === "number" || (typeof value === "string" && /^\d{9,13}$/.test(value.trim()))) {
    const numeric = Number(value);
    parsed = unit === "epoch_seconds" ? numeric * 1000
      : unit === "epoch_milliseconds" ? numeric
      : numeric < 10000000000 ? numeric * 1000 : numeric;
  } else if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
    const source = value.trim();
    if (!realDay(source.slice(0, 10)) || Number(source.slice(11, 13)) >= 24
      || Number(source.slice(14, 16)) >= 60 || Number(source.slice(17, 19)) >= 60) return null;
    const hasOffset = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(source);
    if (!hasOffset && !explicitUtc) return null;
    parsed = Date.parse(hasOffset ? source : `${source}Z`);
  } else return null;
  return Number.isFinite(parsed) && parsed >= Date.UTC(1970, 0, 1)
    && parsed < Date.UTC(2200, 0, 1) ? parsed : null;
}
function numeric(value: unknown, derived = false) {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !value.trim()) return null;
  const number = Number(value);
  return Number.isFinite(number) && (derived ? number >= 0 && number <= 100 : number > 0) ? number : null;
}
function emptyChannel(unit: "bpm" | "ms" | "points", status: ReadStatus): Channel {
  return {
    status, unit, valid_points: 0, observed_cadence_seconds: null, coverage: null,
    ...(unit === "points" ? { measurement_kind: "garmin_derived" as const } : {}),
    measurement_window_seconds: null, measurement_window_basis: "not_documented_in_stored_source",
    coverage_basis: "occupied-observed-cadence-slots-v1", largest_gap_seconds: null,
    quality: { invalid: 0, ambiguous_time: 0, outside_interval: 0, duplicates: 0, conflicting_timestamps: 0, gaps: 0 },
    quality_flags: [], source_dates: [], source_revisions: [], points: [], bins: [], minimum: null,
  };
}

function emptyChannels(names: ChannelName[], status: ReadStatus): Channels {
  const channels: Channels = {};
  for (const name of names) channels[name] = emptyChannel(CHANNEL_UNITS[name], status);
  return channels;
}
function channelValues(channels: Channels): Channel[] {
  return Object.values(channels).filter((channel): channel is Channel => channel !== undefined);
}

/** R2 GET has no AbortSignal. Every owned await is checked before and after.
 * An over-deadline operation cannot become success; settlement is not a hard10s guarantee. */
class SignalRequest {
  readonly started = performance.now();
  readonly resources = {
    r2_get_attempts: 0, r2_list_attempts: 0, stored_bytes: 0, decoded_bytes: 0, decoded_budget_bytes: 0,
    input_points: 0, returned_points: 0, elapsed_ms: 0, structured_bytes: 0, envelope_bytes: 0, envelope_budget_bytes: 0,
  };
  readonly storage: R2Storage;
  private readonly cache = new Map<string, Stored>();
  private objectCount = 0;

  constructor(env: Env) {
    const state = this;
    const bucket = new Proxy(env.SLIPSTREAM_DATA, {
      get(target, property) {
        const value = Reflect.get(target, property);
        if (property !== "get") return typeof value === "function" ? value.bind(target) : value;
        return async (...args: unknown[]) => {
          state.check();
          if (state.resources.r2_get_attempts >= LIMITS.gets) throw new RequestLimit("get_limit");
          state.resources.r2_get_attempts++;
          const object = await Reflect.apply(value, target, args) as R2ObjectBody | null;
          if (object) state.objectCount++;
          try {
            state.check();
            if (object && object.size > LIMITS.stored - state.resources.stored_bytes)
              throw new RequestLimit("stored_byte_limit");
          } catch (error) {
            if (object) try { await object.body.cancel(); } catch { /* primary failure wins */ }
            throw error;
          }
          return object ? state.trackObject(object) : null;
        };
      },
    });
    this.storage = new R2Storage(bucket);
  }

  private trackObject(object: R2ObjectBody): R2ObjectBody {
    const reader = object.body.getReader();
    const state = this;
    const tracked = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          state.check();
          const next = await reader.read();
          state.check();
          if (next.done) {
            controller.close();
            reader.releaseLock();
            return;
          }
          state.resources.stored_bytes += next.value.byteLength;
          if (state.resources.stored_bytes > LIMITS.stored) throw new RequestLimit("stored_byte_limit");
          controller.enqueue(next.value);
        } catch (error) {
          try { await reader.cancel(); } catch { /* preserve the primary failure */ }
          try { reader.releaseLock(); } catch { /* cancellation owns cleanup */ }
          controller.error(error);
        }
      },
      async cancel(reason) {
        try { await reader.cancel(reason); } finally { reader.releaseLock(); }
      },
    });
    return new Proxy(object, {
      get(target, property) {
        return property === "body" ? tracked : Reflect.get(target, property, target);
      },
    });
  }

  check() {
    if (performance.now() - this.started > LIMITS.deadline) throw new RequestLimit("deadline_exceeded");
  }
  points(count: number) {
    this.resources.input_points += count;
    if (this.resources.input_points > LIMITS.input) throw new RequestLimit("input_point_limit");
  }
  private async contextJson(keys: string[], limits: R2ReadLimits) {
    this.check();
    const allowance = Math.min(limits.decoded, LIMITS.decoded - this.resources.decoded_budget_bytes);
    if (allowance <= 0) throw new RequestLimit("decoded_byte_limit");
    const objectBefore = this.objectCount;
    let accounted = false;
    this.resources.decoded_budget_bytes += allowance;
    try {
      const stored = await this.storage.getR2Text(keys, { stored: limits.stored, decoded: allowance });
      this.check();
      if (!stored) {
        this.resources.decoded_budget_bytes -= allowance;
        accounted = true;
        return null;
      }
      const decoded = encoder.encode(stored.text).byteLength;
      this.resources.decoded_bytes += decoded;
      this.resources.decoded_budget_bytes += decoded - allowance;
      accounted = true;
      return { key: stored.key, data: JSON.parse(stored.text) as unknown };
    } catch (error) {
      if (!accounted) {
        if (this.objectCount > objectBefore) this.resources.decoded_budget_bytes++;
        else this.resources.decoded_budget_bytes -= allowance;
      }
      if (error instanceof RequestLimit) throw error;
      if (this.resources.decoded_budget_bytes > LIMITS.decoded) throw new RequestLimit("decoded_byte_limit");
      throw error;
    }
  }

  async context(env: Env, day: string) {
    const current = await readNightUserContext(env, day, {
      beforeList: () => {
        this.check();
        if (this.resources.r2_list_attempts >= LIMITS.lists) throw new RequestLimit("list_limit");
        this.resources.r2_list_attempts++;
      },
      readJson: (keys, limits) => this.contextJson(keys, limits),
      onError: error => { if (error instanceof RequestLimit) throw error; },
    });
    this.check();
    return current;
  }
  async read(stream: Stream, day: string): Promise<Stored> {
    const cacheKey = `${stream}/${day}`;
    const cached = this.cache.get(cacheKey);
    if (cached) return cached;
    this.check();
    const objectBefore = this.objectCount;
    let decodedAccounted = false;
    const perObject = stream === "signals"
      ? { stored: 256 * 1024, decoded: 1024 * 1024 }
      : { stored: 512 * 1024, decoded: 2 * 1024 * 1024 };
    const decodedAllowance = Math.min(perObject.decoded, LIMITS.decoded - this.resources.decoded_budget_bytes);
    if (decodedAllowance <= 0) throw new RequestLimit("decoded_byte_limit");
    let result: Stored;
    try {
      const source = await this.storage.getR2Text(objectKeys(stream, day), {
        stored: perObject.stored, decoded: decodedAllowance,
      });
      this.check();
      if (!source) result = { status: "not_stored" };
      else {
        const decodedBytes = encoder.encode(source.text).byteLength;
        this.resources.decoded_bytes += decodedBytes;
        this.resources.decoded_budget_bytes += decodedBytes;
        decodedAccounted = true;
        if (this.resources.decoded_bytes > LIMITS.decoded) throw new RequestLimit("decoded_byte_limit");
        let data: Row | null;
        try { data = row(JSON.parse(source.text)); } catch { throw new InvalidSource(); }
        if (!data || data.date !== day || data.schema_version !== 1 || data.source !== "garmin")
          throw new InvalidSource();
        result = { status: "available", data, revision: source.etag };
      }
    } catch (error) {
      if (error instanceof RequestLimit) throw error;
      // Exact decoded text bytes are available only on success. A failed object's
      // whole allowed decode budget is charged conservatively, never as zero.
      if (!decodedAccounted && this.objectCount > objectBefore) {
        this.resources.decoded_budget_bytes += decodedAllowance + 1;
        if (this.resources.decoded_budget_bytes > LIMITS.decoded) throw new RequestLimit("decoded_byte_limit");
      }
      result = { status: error instanceof InvalidSource ? "invalid_schema" : "read_error" };
    }
    this.cache.set(cacheKey, result);
    return result;
  }
}

/** Project only reviewed metadata. Unknown text/counts never become diagnostics. */
function sourceQuality(channel: Channel, source: Row, date: string) {
  let invalid = false;
  if (source.quality_flags !== undefined) {
    if (!Array.isArray(source.quality_flags) || source.quality_flags.length > 16) invalid = true;
    else {
      const flags = [...(channel.source_quality_flags ?? [])];
      for (const flag of source.quality_flags) {
        const parsed = sourceQualityFlagSchema.safeParse(flag);
        if (parsed.success) flags.push(parsed.data);
        else invalid = true;
      }
      channel.source_quality_flags = [...new Set(flags)].sort();
    }
  }
  if (source.missing_value_count !== undefined) {
    const parsed = integer.max(10000).safeParse(source.missing_value_count);
    if (parsed.success) channel.source_missing_value_count = (channel.source_missing_value_count ?? 0) + parsed.data;
    else invalid = true;
  }
  if (source.source_composition !== undefined) {
    const parsed = sourceCompositionSchema.safeParse(source.source_composition);
    const composition = parsed.success ? parsed.data : null;
    if (!composition || composition.incoming_valid_points !== composition.new_valid_points + composition.replaced_valid_points
      || composition.source_revision_count < composition.source_revisions.length
      || new Set(composition.source_revisions).size !== composition.source_revisions.length
      || (!composition.source_revisions_truncated && composition.source_revision_count !== composition.source_revisions.length)) invalid = true;
    else (channel.source_compositions ??= []).push({date, ...composition});
  }
  if (invalid && !channel.quality_flags.includes("source_quality_metadata_invalid"))
    channel.quality_flags.push("source_quality_metadata_invalid");
}
function addReadings(channel: Channel, readings: unknown, hrv: boolean, start: number, end: number, state: SignalRequest) {
  if (!Array.isArray(readings)) {
    channel.quality.invalid++;
    return;
  }
  state.points(readings.length);
  for (const raw of readings) {
    const reading = row(raw);
    if (!reading) {
      channel.quality.invalid++;
      continue;
    }
    const value = numeric(hrv ? reading.hrv_ms : reading.value, channel.unit === "points");
    if (value === null) {
      channel.quality.invalid++;
      continue;
    }
    let basis = reading.time_basis;
    const source = reading.timestamp;
    let field = reading.timestamp_field;
    const unit = reading.timestamp_unit;
    // Older normalizers retained the source row. An appended Z alone does not
    // prove that a previously local-only reading was UTC; original GMT does.
    const original = row(reading.raw);
    if (basis === undefined && original?.readingTimeGMT !== undefined) {
      const originalTime = instant(original.readingTimeGMT, true);
      if (originalTime !== null && instant(source, true, unit) === originalTime) {
        basis = "utc";
        field = "readingTimeGMT";
      }
    }
    if (basis !== "utc") {
      channel.quality.ambiguous_time++;
      continue;
    }
    const timestamp = instant(source, true, unit);
    if (timestamp === null || (typeof source !== "string" && typeof source !== "number")) {
      channel.quality.invalid++;
      continue;
    }
    if (timestamp < start || timestamp >= end) {
      channel.quality.outside_interval++;
      continue;
    }
    const point: Point = {
      timestamp_utc: iso(timestamp), value,
      source_timestamp: typeof source === "string" ? source.slice(0, 80) : source,
      time_basis: "utc", timestamp_field: typeof field === "string" ? field.slice(0, 128) : "legacy_documented_gmt",
      timestamp_unit: typeof unit === "string" ? unit.slice(0, 32)
        : typeof source === "number" ? (source < 10000000000 ? "epoch_seconds" : "epoch_milliseconds") : "iso8601",
    };
    if (reading.source_revision !== undefined) {
      const revision = sourceRevisionSchema.safeParse(reading.source_revision);
      point.source_revision = revision.success ? revision.data : null;
      if (!revision.success && !channel.quality_flags.includes("source_quality_metadata_invalid"))
        channel.quality_flags.push("source_quality_metadata_invalid");
    }
    if (reading.observation_origin !== undefined) {
      const origin = observationOriginSchema.safeParse(reading.observation_origin);
      point.observation_origin = origin.success ? origin.data : null;
      if (!origin.success && !channel.quality_flags.includes("source_quality_metadata_invalid"))
        channel.quality_flags.push("source_quality_metadata_invalid");
      if (point.observation_origin === "retained" && !point.source_revision && !channel.quality_flags.includes("source_retained_revision_unknown"))
        channel.quality_flags.push("source_retained_revision_unknown");
    }
    channel.points.push(point);
  }
}

function lowestWindow(channel: Channel, times: number[], cadence: number, start: number, end: number, state: SignalRequest) {
  if (cadence > 150000 || Math.ceil(600000 / cadence) * .8 > channel.points.length) return;
  let lower = 0, upper = 0;
  // No eligible window begins more than two cadence slots before the first
  // point. Advancing pointers avoids rescanning the full curve per window.
  const firstWindow = start + Math.max(0, Math.ceil((times[0] - start - 2 * cadence) / cadence)) * cadence;
  for (let windowStart = firstWindow; windowStart + 600000 <= end && windowStart <= times.at(-1)!; windowStart += cadence) {
    state.check();
    const windowEnd = windowStart + 600000;
    while (lower < times.length && times[lower] < windowStart) lower++;
    if (upper < lower) upper = lower;
    while (upper < times.length && times[upper] < windowEnd) upper++;
    const points = channel.points.slice(lower, upper);
    const windowTimes = times.slice(lower, upper);
    const slots = new Set(windowTimes.map(time => Math.floor((time - windowStart) / cadence))).size;
    const coverage = slots / Math.ceil(600000 / cadence);
    const gaps = [windowTimes[0] - windowStart,
      ...windowTimes.slice(1).map((time, i) => time - windowTimes[i]), windowEnd - windowTimes.at(-1)!];
    if (points.length < 4 || coverage < .8 || gaps.some(gap => gap > cadence * 2)) continue;
    const value = median(points.map(point => point.value))!;
    if (channel.minimum === null || value < channel.minimum.value) {
      channel.minimum = {
        value, method: "lowest-10-minute-median-v1", window_start_utc: iso(windowStart),
        window_end_utc: iso(windowEnd), coverage, valid_points: points.length,
      };
    }
  }
}

function finish(channel: Channel, start: number, end: number, hr: boolean, state: SignalRequest, nativeRemaining: number | null) {
  const byInstant = new Map<string, Point[]>();
  for (const point of channel.points) {
    const group = byInstant.get(point.timestamp_utc) ?? [];
    group.push(point);
    byInstant.set(point.timestamp_utc, group);
  }
  channel.points = [];
  for (const group of byInstant.values()) {
    if (new Set(group.map(point => point.value)).size > 1) {
      channel.quality.conflicting_timestamps++;
      continue;
    }
    channel.quality.duplicates += group.length - 1;
    channel.points.push(group[0]);
  }
  channel.points.sort((a, b) => Date.parse(a.timestamp_utc) - Date.parse(b.timestamp_utc));
  channel.valid_points = channel.points.length;
  if (nativeRemaining !== null && channel.valid_points > nativeRemaining) throw new RequestLimit("native_point_limit");
  const times = channel.points.map(point => Date.parse(point.timestamp_utc));
  const diffs = times.slice(1).map((time, i) => time - times[i]);
  const cadence = median(diffs);
  channel.observed_cadence_seconds = cadence === null ? null : cadence / 1000;
  channel.largest_gap_seconds = diffs.length ? diffs.reduce((largest, gap) => Math.max(largest, gap), 0) / 1000 : null;
  if (cadence !== null && cadence > 0) {
    channel.quality.gaps = diffs.filter(diff => diff > cadence * 2).length;
    const occupied = new Set(times.map(time => Math.floor((time - start) / cadence))).size;
    channel.coverage = Math.min(1, occupied / Math.ceil((end - start) / cadence));
    if (hr) lowestWindow(channel, times, cadence, start, end, state);
  }
  if (channel.valid_points > 0) channel.status = "available";
  if (channel.quality.invalid) channel.quality_flags.push("invalid_observations");
  if (channel.quality.ambiguous_time) channel.quality_flags.push("ambiguous_time_excluded");
  if (channel.quality.conflicting_timestamps) channel.quality_flags.push("conflicting_instants_excluded");
  if (channel.quality.gaps) channel.quality_flags.push("observed_gaps");
  if (channel.coverage === null) channel.quality_flags.push("coverage_unknown");
  if (hr && channel.minimum === null) channel.quality_flags.push("minimum_window_not_covered");
}

function compact(channel: Channel, start: number, end: number, width: number, origin: number) {
  for (let first = origin; first < end; first += width) {
    const last = Math.min(first + width, end);
    const points = channel.points.filter(point => {
      const time = Date.parse(point.timestamp_utc);
      return time >= first && time < last;
    });
    channel.bins.push({
      start_utc: iso(Math.max(start, first)), end_utc: iso(last),
      median: median(points.map(point => point.value)), count: points.length,
    });
  }
  channel.points = [];
}
function complete(channel: Channel) {
  return ["available", "no_data"].includes(channel.status)
    && !channel.quality_flags.some(flag => flag.startsWith("source_"))
    && channel.quality.invalid === 0 && channel.quality.ambiguous_time === 0
    && channel.quality.conflicting_timestamps === 0
    && (channel.source_missing_value_count ?? 0) === 0
    && (channel.source_quality_flags ?? []).length === 0
    && !(channel.source_compositions ?? []).some(source => source.completeness === "unknown");
}

export class NightSignalsTools {
  constructor(private readonly env: Env) {}

  private async night(day: string, state: SignalRequest, resolution: Resolution, alignment: Alignment, selected: ChannelName[]): Promise<NightResult> {
    const zone = validatedHealthTimezone(this.env.HEALTH_TIMEZONE);
    const channels = emptyChannels(selected, "not_stored");
    const output: NightResult = {
      date: day, wake_date: day, night_of: null, sleep_start_utc: null, sleep_end_utc: null,
      timezone: null, configured_timezone: zone, actual_night_timezone: null, night_context: null,
      status: "not_stored", resolution, alignment, bin_origin_utc: null,
      method_version: "night-signals-v1", channels,
      quality_flags: [], resources: state.resources,
    };
    const current = await state.context(this.env, day);
    Object.assign(output, current);
    if (current.view.context_status === "unavailable") output.quality_flags.push("user_context_unavailable");
    const sleep = await state.read("sleep", day);
    if (sleep.status !== "available") {
      output.status = sleep.status;
      for (const channel of channelValues(channels)) channel.status = sleep.status;
      return output;
    }
    const start = instant(sleep.data.sleep_start_gmt, true), end = instant(sleep.data.sleep_end_gmt, true);
    if (start === null || end === null || end <= start || end - start > 36 * 3600000) {
      output.status = "invalid_schema";
      for (const channel of channelValues(channels)) channel.status = "invalid_schema";
      output.quality_flags.push("invalid_sleep_interval");
      return output;
    }
    const sourceContext = nightContext(day, sleep.data.sleep_start_gmt, sleep.data.sleep_end_gmt, zone,
      sleep.data.sleep_start_garmin_local, sleep.data.sleep_end_garmin_local,
      typeof sleep.data.actual_night_timezone === "string" ? sleep.data.actual_night_timezone : null);
    output.night_context = sourceContext;
    Object.assign(output, applyNightUserContext(this.env, day, output, current));
    const context = nightProjectionSchema.parse(output.night_context);
    output.night_of = context.night_of;
    output.timezone = context.timezone;
    output.actual_night_timezone = context.actual_night_timezone;
    output.night_context = context;
    output.sleep_start_utc = iso(start);
    output.sleep_end_utc = iso(end);
    output.bin_origin_utc = iso(start);

    // Include documented source-calendar endpoints as well as every UTC date.
    const dates = new Set<string>([day]);
    for (let date = Date.parse(`${iso(start).slice(0, 10)}T00:00:00Z`); date < end; date += 86400000)
      dates.add(iso(date).slice(0, 10));
    for (const local of [context.sleep_start_date_local, context.sleep_end_date_local])
      if (local && realDay(local)) dates.add(local);
    for (const [raw, utc] of [[sleep.data.sleep_start_garmin_local, start], [sleep.data.sleep_end_garmin_local, end]] as const) {
      // Garmin wall fields encode the calendar independently of an optional
      // suffix. Keep their source day even when actual-zone presentation wins.
      const wallSource = typeof raw === "string" ? raw.replace(/(?:Z|[+-]\d{2}:?\d{2})$/i, "") : raw;
      const wall = instant(wallSource, true);
      if (wall !== null && Math.abs(wall - utc) <= 14 * 3600000) dates.add(iso(wall).slice(0, 10));
    }
    const dailySelected = selected.filter(name => name !== "hrv");
    for (const date of dailySelected.length ? [...dates].sort() : []) {
      // One bounded body serves every selected daily channel; never refetch per channel.
      const source = await state.read("signals", date);
      for (const name of dailySelected) {
        const channel = channels[name]!;
        channel.source_dates.push(date);
        if (source.status !== "available") {
          if (channel.status !== "read_error" && channel.status !== "invalid_schema") channel.status = source.status;
          channel.quality_flags.push(`source_${source.status}`);
          continue;
        }
        channel.source_revisions.push({ date, revision: source.revision });
        const sourceChannel = row(row(source.data.channels)?.[name]);
        if (!sourceChannel) {
          channel.status = name === "heart_rate" ? "invalid_schema" : "not_stored";
          channel.quality_flags.push(name === "heart_rate" ? "source_invalid_schema" : "source_not_stored");
          continue;
        }
        sourceQuality(channel, sourceChannel, date);
        if (!["available", "no_data", "invalid_response"].includes(String(sourceChannel.status))
          || sourceChannel.unit !== CHANNEL_UNITS[name] || !Array.isArray(sourceChannel.readings)) {
          channel.status = "invalid_schema";
          channel.quality_flags.push("source_invalid_schema");
          continue;
        }
        if (sourceChannel.status === "invalid_response") {
          channel.status = "invalid_schema";
          channel.quality_flags.push("source_invalid_response");
          continue;
        }
        if (sourceChannel.status === "no_data" && sourceChannel.readings.length !== 0) {
          channel.status = "invalid_schema";
          channel.quality_flags.push("source_invalid_schema");
          continue;
        }
        if (channel.status === "not_stored") channel.status = sourceChannel.status === "no_data" ? "no_data" : "available";
        addReadings(channel, sourceChannel.readings, false, start, end, state);
      }
    }
    const hrv = channels.hrv;
    if (hrv) {
      const source = await state.read("hrv", day);
      hrv.source_dates.push(day);
      if (source.status !== "available") hrv.status = source.status;
      else {
        hrv.source_revisions.push({ date: day, revision: source.revision });
        if (!Array.isArray(source.data.readings)) {
          hrv.status = "invalid_schema";
          hrv.quality_flags.push("source_invalid_schema");
        } else {
          hrv.status = source.data.readings.length ? "available" : "no_data";
          addReadings(hrv, source.data.readings, true, start, end, state);
        }
      }
    }
    let nativePoints = 0;
    for (const name of selected) {
      const channel = channels[name]!;
      finish(channel, start, end, name === "heart_rate", state,
        resolution === "native" ? LIMITS.native - state.resources.returned_points - nativePoints : null);
      nativePoints += channel.valid_points;
      channel.quality_flags = [...new Set(channel.quality_flags)];
    }
    if (resolution !== "native") {
      const width = resolution === "15m" ? 900000 : 1800000;
      const origin = alignment === "utc_clock" ? Math.floor(start / width) * width : start;
      output.bin_origin_utc = iso(origin);
      for (const channel of channelValues(channels)) compact(channel, start, end, width, origin);
    }
    state.resources.returned_points += channelValues(channels).reduce((count, channel) =>
      count + channel.points.length + channel.bins.length, 0);
    if (resolution === "native" && state.resources.returned_points > LIMITS.native) throw new RequestLimit("native_point_limit");
    output.quality_flags = [...new Set([...output.quality_flags, ...context.time_quality_flags, "coverage_is_nominal_observed_cadence"])];
    output.status = channelValues(channels).every(complete) ? "available" : "partial";
    return output;
  }

  private result(value: NightResult | HistoryResult, state: SignalRequest, id: RequestId) {
    state.resources.elapsed_ms = Math.max(0, performance.now() - state.started);
    const result = () => ({
      content: [{ type: "text" as const, text: JSON.stringify(value) }], structuredContent: value,
    });
    const measure = () => {
      // Counters are themselves serialized; converge their digit lengths.
      for (let i = 0; i < 5; i++) {
        state.resources.structured_bytes = bytes(value);
        state.resources.envelope_bytes = bytes({ jsonrpc: "2.0", id, result: result() });
        state.resources.envelope_budget_bytes = state.resources.envelope_bytes + 1024;
      }
    };
    measure();
    if (state.resources.structured_bytes > LIMITS.structured || state.resources.envelope_budget_bytes > LIMITS.envelope) {
      value.status = "output_limit";
      value.quality_flags.push("output_limit");
      if ("channels" in value) {
        for (const channel of channelValues(value.channels)) {
          channel.points = [];
          channel.bins = [];
        }
      } else value.nights = [];
      state.resources.returned_points = 0;
      measure();
    }
    if (state.resources.structured_bytes > LIMITS.structured || state.resources.envelope_budget_bytes > LIMITS.envelope) {
      // Curve removal is insufficient when metadata alone is oversized. Replace
      // every source-derived field with a fixed small projection, not a slice.
      value.status = "output_limit";
      value.quality_flags = ["output_limit", "metadata_suppressed"];
      if ("channels" in value) {
        value.night_context = null;
        value.user_context = null;
        delete value.view;
        value.night_of = value.timezone = value.configured_timezone = value.actual_night_timezone = null;
        value.sleep_start_utc = value.sleep_end_utc = value.bin_origin_utc = null;
        value.channels = emptyChannels(Object.keys(value.channels) as ChannelName[], "read_error");
      } else value.nights = [];
      state.resources.returned_points = 0;
      measure();
      if (state.resources.structured_bytes > LIMITS.structured || state.resources.envelope_budget_bytes > LIMITS.envelope) {
        return { content: [{ type: "text" as const, text: "The requested night output exceeds the response limit." }], isError: true };
      }
    }
    const response = result();
    try {
      // JSON construction above is owned CPU work too. A source read that
      // finished in time does not authorize publishing a result built late.
      state.check();
    } catch (error) {
      if (!(error instanceof RequestLimit)) throw error;
      value.status = "budget_exhausted";
      if (!value.quality_flags.includes(error.message)) value.quality_flags.push(error.message);
      if ("channels" in value) {
        value.channels = emptyChannels(Object.keys(value.channels) as ChannelName[], "read_error");
      } else value.nights = [];
      state.resources.returned_points = 0;
      state.resources.elapsed_ms = Math.max(0, performance.now() - state.started);
      measure();
      return result();
    }
    return response;
  }

  registerTools(server: McpServer) {
    server.registerTool("night_signals_history", {
      description: "Read up to 7 native or 31 compact stored nights. UTC points or 15/30-minute medians with sleep-start/UTC-clock origins, explicit empty bins and bounded reads/output. No provider calls, writes or interpolation.",
      inputSchema: z.strictObject({
        start_date: exactDate, end_date: exactDate, resolution: resolutionSchema.default("native"),
        alignment: alignmentSchema.default("sleep_start"), channels: requestedChannelsSchema,
      }),
      outputSchema: nightSignalsHistorySchema, annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args, context) => {
      const state = new SignalRequest(this.env);
      const count = realDay(args.start_date) && realDay(args.end_date)
        ? Math.floor((Date.parse(`${args.end_date}T00:00:00Z`) - Date.parse(`${args.start_date}T00:00:00Z`)) / 86400000) + 1 : 0;
      const output: HistoryResult = {
        start_date: args.start_date, end_date: args.end_date, requested_nights: Math.max(0, count),
        status: "partial", resolution: args.resolution, alignment: args.alignment,
        method_version: "night-signals-v1", nights: [], quality_flags: [], resources: state.resources,
      };
      const limit = args.resolution === "native" ? 7 : 31;
      if (count < 1 || count > limit) {
        output.status = "invalid_request";
        output.quality_flags.push(count < 1 ? "invalid_date_range"
          : args.resolution === "native" ? "native_night_limit_7" : "compact_night_limit_31");
        return this.result(output, state, context.mcpReq.id);
      }
      try {
        for (let i = 0; i < count; i++) {
          const day = iso(Date.parse(`${args.start_date}T00:00:00Z`) + i * 86400000).slice(0, 10);
          const { resources: _resources, ...night } = await this.night(day, state, args.resolution, args.alignment, args.channels);
          output.nights.push(night);
        }
        output.status = output.nights.every(night => night.status === "available") ? "available" : "partial";
      } catch (error) {
        if (!(error instanceof RequestLimit)) throw error;
        output.status = "budget_exhausted";
        output.nights = [];
        state.resources.returned_points = 0;
        output.quality_flags.push(error.message);
      }
      return this.result(output, state, context.mcpReq.id);
    });
    server.registerTool("night_signals", {
      description: "Read selected stored overnight HR, HRV, Body Battery or stress for one wake-date as native UTC points or 15/30-minute medians. No interpolation, provider calls or writes. Coverage uses observed cadence; clocks/provenance and a covered ten-minute HR median are explicit.",
      inputSchema: z.strictObject({ date: exactDate, resolution: resolutionSchema.default("native"), alignment: alignmentSchema.default("sleep_start"), channels: requestedChannelsSchema }),
      outputSchema: nightSignalsSchema, annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args, context) => {
      const state = new SignalRequest(this.env);
      try {
        return this.result(await this.night(args.date, state, args.resolution, args.alignment, args.channels), state, context.mcpReq.id);
      } catch (error) {
        if (!(error instanceof RequestLimit)) throw error;
        state.resources.returned_points = 0;
        return this.result({
          date: args.date, wake_date: args.date, night_of: null, sleep_start_utc: null, sleep_end_utc: null,
          timezone: null, configured_timezone: validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
          actual_night_timezone: null, night_context: null, status: "budget_exhausted",
          resolution: args.resolution, alignment: args.alignment, bin_origin_utc: null, method_version: "night-signals-v1",
          channels: emptyChannels(args.channels, "read_error"),
          quality_flags: [error.message], resources: state.resources,
        }, state, context.mcpReq.id);
      }
    });
  }
}
