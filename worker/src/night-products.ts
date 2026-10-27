/** Reads fixed, validated offline reference products. Never scans source nights. */
import type { McpServer, RequestId } from "@modelcontextprotocol/server";
import { z } from "zod";
import { R2Storage } from "./r2-storage";
import { exactDate, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";

const METHOD = "night-baseline-v1";
const encoder = new TextEncoder();
const integer = z.number().int().nonnegative();
const hash = z.string().regex(/^[a-f0-9]{64}$/);
const filtersSchema = z.object({
  alcohol: z.boolean().optional(), late_bedtime: z.boolean().optional(), travel: z.boolean().optional(), illness: z.boolean().optional(),
  actual_night_timezone: z.string().min(1).max(128).optional(),
  sleep_quality: z.number().int().min(1).max(5).optional(), morning_condition: z.number().int().min(1).max(5).optional(),
}).strict();
const argsSchema = z.object({
  start_date: exactDate, end_date: exactDate, alignment: z.enum(["sleep_start", "wake", "clock"]),
  bin_minutes: z.union([z.literal(15), z.literal(30)]), channel: z.enum(["heart_rate", "hrv", "body_battery", "stress"]),
  exclude_dates: z.array(exactDate).max(366), context_filter: filtersSchema,
}).strict();
const binSchema = z.object({
  bin_index: z.number().int().min(-144).max(143), offset_minutes: z.number().int().min(-2160).max(2160),
  p10: z.number().finite().nullable(), p25: z.number().finite().nullable(), p50: z.number().finite().nullable(),
  p75: z.number().finite().nullable(), p90: z.number().finite().nullable(), nights: integer.max(366), valid_points: integer.max(200000),
}).strict();
const exclusionSchema = z.object({
  wake_date: exactDate, reason: z.enum(["not_in_export", "explicit_date_exclusion", "non_native_input", "invalid_sleep_interval",
    "channel_unavailable", "no_valid_points", "context_source_unverified", "context_field_unknown", "context_filter_mismatch",
    "unknown_actual_timezone", "duplicate_conflict", "unsupported_source_method"]), field: z.string().max(32).optional(),
}).strict();
const methodSchema = z.object({
  within_night: z.literal("median"), quantiles: z.literal("linear-(n-1)p"), one_contribution_per_night_bin: z.literal(true),
}).strict();
const qualityNoteSchema = z.object({wake_date: exactDate, flags: z.array(z.enum(["dst_fold_combined", "partial_source"])).max(2)}).strict();
const sourceSchema = z.object({
  wake_date: exactDate, snapshot_sha256: hash,
  source_revisions: z.array(z.object({ date: exactDate, revision: z.string().max(256) }).strict()).max(16),
  context_id: z.string().max(128).nullable(),
}).strict();
const productSchema = z.object({
  schema_version: z.literal(1), kind: z.literal("night-baseline"), method_version: z.literal(METHOD), args: argsSchema,
  args_sha256: hash, source_identity: hash, source_manifest: z.array(sourceSchema).max(366),
  nights_requested: integer.min(1).max(366), source_nights: integer.max(366), contributing_nights: integer.max(366),
  unit: z.enum(["bpm", "ms", "points"]), excluded: z.array(exclusionSchema).max(366), method: methodSchema,
  bins: z.array(binSchema).max(144), quality_notes: z.array(qualityNoteSchema).max(366).default([]), resources: z.object({ input_bytes: integer.max(16 * 1024 * 1024), input_points: integer.max(200000), invalid_points: integer.max(200000) }).strict(),
}).strict();
const projectionSchema = z.object({
  status: z.enum(["available", "not_stored", "read_error", "invalid_schema"]),
  window: z.object({ start_date: exactDate, end_date: exactDate }), args: argsSchema,
  source_identity: hash.nullable(), nights_requested: integer, source_nights: integer, contributing_nights: integer,
  unit: z.string().nullable(), bins: z.array(binSchema), excluded: z.array(exclusionSchema), quality_notes: z.array(qualityNoteSchema), method: methodSchema.nullable(),
});
const resourcesSchema = z.object({
  r2_get_attempts: integer, decoded_bytes: integer, elapsed_ms: z.number().nonnegative(), structured_bytes: integer, envelope_budget_bytes: integer,
});
export const nightBaselineSchema = z.object({
  status: z.enum(["available", "partial", "not_stored", "read_error", "invalid_schema", "invalid_request", "output_limit", "budget_exhausted"]),
  method_version: z.literal(METHOD), current: projectionSchema.nullable(), reference: projectionSchema.nullable(),
  quality_flags: z.array(z.string()), resources: resourcesSchema,
});
type Args = z.infer<typeof argsSchema>;
type Projection = z.infer<typeof projectionSchema>;
type Product = z.infer<typeof productSchema>;
type Result = z.infer<typeof nightBaselineSchema>;
class ProductDeadline extends Error {}
function checkDeadline(started: number) {
  if (performance.now() - started > 10000) throw new ProductDeadline();
}

function canonical(value: unknown): string {
  const sort = (item: unknown): unknown => {
    if (Array.isArray(item)) return item.map(sort);
    if (item !== null && typeof item === "object") return Object.fromEntries(Object.entries(item)
      .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, child]) => [key, sort(child)]));
    return item;
  };
  return JSON.stringify(sort(value));
}
async function digest(value: unknown) {
  const bytes = await crypto.subtle.digest("SHA-256", encoder.encode(canonical(value)));
  return [...new Uint8Array(bytes)].map(part => part.toString(16).padStart(2, "0")).join("");
}
function dayNumber(day: string) {
  const time = Date.parse(`${day}T00:00:00Z`);
  if (!Number.isFinite(time) || day < "1970-01-01" || new Date(time).toISOString().slice(0, 10) !== day) throw new Error("invalid_date");
  return time / 86400000;
}
function normalized(options: {end_date: string; period: string; start_date?: string; alignment: Args["alignment"]; bin_minutes: 15 | 30;
  channel: Args["channel"]; exclude_dates: string[]; context_filter: z.infer<typeof filtersSchema>}): Args {
  const end = dayNumber(options.end_date);
  const start = options.period === "custom" ? dayNumber(options.start_date ?? "") : end - Number(options.period) + 1;
  if (!Number.isInteger(start) || end - start + 1 < 1 || end - start + 1 > 366) throw new Error("invalid_period");
  const startDate = new Date(start * 86400000).toISOString().slice(0, 10);
  if (options.period !== "custom" && options.start_date && options.start_date !== startDate) throw new Error("conflicting_start_date");
  const excluded = [...new Set(options.exclude_dates)].sort();
  excluded.forEach(dayNumber);
  if (options.context_filter.actual_night_timezone) new Intl.DateTimeFormat("en", {timeZone: options.context_filter.actual_night_timezone});
  return { alignment: options.alignment, bin_minutes: options.bin_minutes, channel: options.channel,
    context_filter: options.context_filter, end_date: options.end_date, exclude_dates: excluded, start_date: startDate };
}
function empty(args: Args, status: Projection["status"]): Projection {
  return { status, window: {start_date: args.start_date, end_date: args.end_date}, args, source_identity: null,
    nights_requested: dayNumber(args.end_date) - dayNumber(args.start_date) + 1, source_nights: 0, contributing_nights: 0,
    unit: null, bins: [], excluded: [], quality_notes: [], method: null };
}
function validCounts(product: Product) {
  if (product.nights_requested !== dayNumber(product.args.end_date) - dayNumber(product.args.start_date) + 1
    || product.source_nights !== product.source_manifest.length || product.contributing_nights > product.source_nights
    || product.source_nights > product.nights_requested || product.resources.invalid_points > product.resources.input_points
    || product.unit !== ({heart_rate:"bpm",hrv:"ms",body_battery:"points",stress:"points"} as const)[product.args.channel]) return false;
  const dates = product.source_manifest.map(source => source.wake_date);
  if (new Set(dates).size !== dates.length || dates.some((day, index) => day < product.args.start_date || day > product.args.end_date || (index > 0 && day <= dates[index-1]))) return false;
  const excludedDates = product.excluded.map(entry => entry.wake_date);
  if (new Set(excludedDates).size !== excludedDates.length || excludedDates.some(day => day < product.args.start_date || day > product.args.end_date)
    || product.contributing_nights + product.excluded.length !== product.nights_requested) return false;
  const indices = product.bins.map(bin => bin.bin_index);
    if (new Set(indices).size !== indices.length || indices.some((index, i) => i > 0 && index !== indices[i-1] + 1)) return false;
  if (indices.length) {
    if (product.args.alignment === "sleep_start" && indices[0] !== 0) return false;
    if (product.args.alignment === "wake" && indices.at(-1) !== -1) return false;
    if (product.args.alignment === "clock" && (indices[0] !== 0 || indices.length !== 1440 / product.args.bin_minutes)) return false;
  }
  if (product.bins.reduce((sum, bin) => sum + bin.valid_points, 0) > product.resources.input_points) return false;
  for (const bin of product.bins) {
    if ((product.args.alignment === "sleep_start" && bin.bin_index < 0) || (product.args.alignment === "wake" && bin.bin_index >= 0)
      || (product.args.alignment === "clock" && (bin.bin_index < 0 || bin.bin_index >= 1440 / product.args.bin_minutes))) return false;
    if (bin.offset_minutes !== bin.bin_index * product.args.bin_minutes || bin.nights > product.contributing_nights || bin.valid_points < bin.nights) return false;
    const values = [bin.p10, bin.p25, bin.p50, bin.p75, bin.p90];
    if (bin.nights === 0 ? values.some(value => value !== null) : values.some(value => value === null)) return false;
    if (values.some(value => value !== null && (["heart_rate", "hrv"].includes(product.args.channel) ? value <= 0 : value < 0 || value > 100))) return false;
    if (values.some((value, index) => index > 0 && value !== null && values[index-1] !== null && value < values[index-1]!)) return false;
  }
  return true;
}

export class NightProductsTools {
  constructor(private readonly env: Env) {}
  private async read(args: Args, state: Result["resources"], started: number): Promise<Projection> {
    checkDeadline(started);
    const argsHash = await digest(args);
    const end = args.end_date;
    checkDeadline(started);
    const key = `health/night-products/v1/baseline/${end.slice(0,4)}/${end.slice(5,7)}/${end.slice(8,10)}/${argsHash}.json`;
    let source: {text: string} | null;
    try {
      state.r2_get_attempts++;
      source = await new R2Storage(this.env.SLIPSTREAM_DATA).getR2Text([key], {stored: 1024 * 1024, decoded: 1024 * 1024});
    } catch {
      return empty(args, "read_error");
    }
    checkDeadline(started);
    if (!source) return empty(args, "not_stored");
    state.decoded_bytes += encoder.encode(source.text).byteLength;
    try {
      const product = productSchema.parse(JSON.parse(source.text));
      if (canonical(product.args) !== canonical(args) || product.args_sha256 !== argsHash || !validCounts(product)
        || product.source_identity !== await digest(product.source_manifest)) return empty(args, "invalid_schema");
      checkDeadline(started);
      return { status: "available", window: {start_date: args.start_date, end_date: args.end_date}, args,
        source_identity: product.source_identity, nights_requested: product.nights_requested, source_nights: product.source_nights,
        contributing_nights: product.contributing_nights, unit: product.unit, bins: product.bins, excluded: product.excluded, quality_notes: product.quality_notes, method: product.method };
    } catch (error) {
      if (error instanceof ProductDeadline) throw error;
      return empty(args, "invalid_schema");
    }
  }
  private result(value: Result, id: RequestId, started: number) {
    const response = () => ({content: [{type: "text" as const, text: JSON.stringify(value)}], structuredContent: value});
    const measure = () => {
      for (let i = 0; i < 5; i++) {
        value.resources.structured_bytes = encoder.encode(JSON.stringify(value)).byteLength;
        value.resources.envelope_budget_bytes = encoder.encode(JSON.stringify({jsonrpc: "2.0", id, result: response()})).byteLength + 1024;
      }
    };
    value.resources.elapsed_ms = Math.max(0, performance.now() - started);
    measure();
    if (value.resources.structured_bytes > 256 * 1024 || value.resources.envelope_budget_bytes > 512 * 1024) {
      value.status = "output_limit";
      value.current = value.reference = null;
      value.quality_flags.push("output_limit");
      measure();
    }
    const output = response();
    if (performance.now() - started > 10000) {
      value.status = "budget_exhausted";
      value.current = value.reference = null;
      value.quality_flags.push("deadline_exceeded");
      measure();
      return response();
    }
    return output;
  }
  registerTools(server: McpServer) {
    server.registerTool("night_baseline", {
      description: "Read one bounded precomputed personal night reference product. Each night contributes one within-night median per bin before linear P10/P25/P50/P75/P90. Explicit exclusions, context filters and coverage remain visible. No source-night scan, provider call or write.",
      inputSchema: z.object({end_date: exactDate, period: z.enum(["7", "28", "90", "365", "custom"]).default("28"), start_date: exactDate.optional(),
        alignment: z.enum(["sleep_start", "wake", "clock"]).default("sleep_start"), bin_minutes: z.union([z.literal(15),z.literal(30)]).default(30),
        channel: z.enum(["heart_rate", "hrv", "body_battery", "stress"]).default("hrv"), exclude_dates: z.array(exactDate).max(366).default([]), context_filter: filtersSchema.default({}),
        compare: z.union([z.literal("previous"), z.object({start_date: exactDate, end_date: exactDate}).strict()]).optional()}).strict(),
      outputSchema: nightBaselineSchema, annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (options, context) => {
      const started = performance.now();
      const value: Result = {status: "invalid_request", method_version: METHOD, current: null, reference: null, quality_flags: [],
        resources: {r2_get_attempts: 0, decoded_bytes: 0, elapsed_ms: 0, structured_bytes: 0, envelope_budget_bytes: 0}};
      let args: Args, referenceArgs: Args | null = null;
      try {
        args = normalized(options);
        if (options.compare) {
          const count = dayNumber(args.end_date) - dayNumber(args.start_date) + 1;
          const previousEnd = dayNumber(args.start_date) - 1;
          const period = options.compare === "previous"
            ? {start_date: new Date((previousEnd - count + 1) * 86400000).toISOString().slice(0,10), end_date: new Date(previousEnd * 86400000).toISOString().slice(0,10)}
            : options.compare;
          referenceArgs = normalized({...options, period: "custom", ...period});
        }
      } catch { value.quality_flags.push("invalid_reference_arguments"); return this.result(value, context.mcpReq.id, started); }
      try {
        value.current = await this.read(args, value.resources, started);
        value.status = value.current.status;
        if (referenceArgs) {
          value.reference = await this.read(referenceArgs, value.resources, started);
          value.status = value.current.status === "available" && value.reference.status === "available" ? "available" : "partial";
          if (value.current.status !== "available") value.quality_flags.push("current_unavailable");
          if (value.reference.status !== "available") value.quality_flags.push("reference_unavailable");
        }
      }
      catch (error) {
        value.status = error instanceof ProductDeadline ? "budget_exhausted" : "read_error";
        value.current = value.reference = null;
        value.quality_flags.push(error instanceof ProductDeadline ? "deadline_exceeded" : "product_unavailable");
      }
      return this.result(value, context.mcpReq.id, started);
    });
  }
}
