/** Explicit user context for one wake-date. Provider sources and analyses stay immutable. */
import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { ApplicationToolError, applicationToolResult } from "./application-errors";
import { exactDate, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";
import { hrvObjectKeys, sleepObjectKeys, validCalendarDay } from "./lib";
import { R2Storage, type R2ReadLimits } from "./r2-storage";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { structuredToolResult } from "./mcp-result";

const contextDate = exactDate.refine(value => validCalendarDay(value) !== undefined, { message: "Use a valid calendar wake-date." });
const EVENT_LIMIT = 100;
const EVENT_LIMITS = { stored: 16 * 1024, decoded: 32 * 1024 };
const NIGHT_LIMITS = { stored: 512 * 1024, decoded: 2 * 1024 * 1024 };
const WRITE_COOLDOWN_MS = 10_000;
const CONTEXT_FIELDS = ["alcohol", "late_bedtime", "travel", "actual_night_timezone", "illness",
  "sleep_quality", "morning_condition", "note"] as const;
const contextIdSchema = z.string().regex(/^nctx-[a-f0-9]{24}$/);
const timezone = z.string().trim().min(1).max(100)
  .refine(value => validatedHealthTimezone(value) !== null, { message: "Use a valid IANA timezone." });
const values = {
  alcohol: z.boolean().nullable(), late_bedtime: z.boolean().nullable(), travel: z.boolean().nullable(),
  actual_night_timezone: timezone.nullable(), illness: z.boolean().nullable(),
  sleep_quality: z.number().int().min(1).max(5).nullable(),
  morning_condition: z.number().int().min(1).max(5).nullable(),
  note: z.string().trim().min(1).max(1000).nullable(),
};
export const nightUserContextSchema = z.strictObject({
  schema_version: z.literal(1), wake_date: contextDate, context_id: contextIdSchema,
  previous_context_id: contextIdSchema.nullable(), created_at: z.iso.datetime(), source: z.literal("user"),
  ...values,
  declared_fields: z.array(z.enum(CONTEXT_FIELDS)).min(1).max(CONTEXT_FIELDS.length)
    .refine(fields => new Set(fields).size === fields.length && fields.every((field, index) => index === 0 || fields[index - 1] < field)),
  changed_fields: z.array(z.enum(CONTEXT_FIELDS)).min(1).max(CONTEXT_FIELDS.length)
    .refine(fields => new Set(fields).size === fields.length),
});
export type NightUserContext = z.infer<typeof nightUserContextSchema>;
export const nightUserContextViewSchema = z.object({
  schema_version: z.literal(1), context_status: z.enum(["available", "not_stored", "unavailable"]),
  context_revision: contextIdSchema.nullable(), context_updated_at: z.string().nullable(), resolved_at: z.string(),
});
export type NightUserContextRead = {
  user_context: NightUserContext | null;
  view: z.infer<typeof nightUserContextViewSchema>;
};
export const addNightContextOutputSchema = z.object({
  saved: z.literal(true), wake_date: contextDate, context_id: contextIdSchema,
  context: nightUserContextSchema, user_context: nightUserContextSchema.nullable(),
  view: nightUserContextViewSchema, message: z.string(),
});
export const nightContextHistoryOutputSchema = z.object({
  wake_date: contextDate, status: z.enum(["available", "not_stored", "unavailable"]),
  events: z.array(nightUserContextSchema).max(20),
  next_before_context_id: contextIdSchema.nullable(), more: z.boolean(),
});
const inputSchema = z.strictObject({ wake_date: contextDate,
  alcohol: values.alcohol.optional(), late_bedtime: values.late_bedtime.optional(), travel: values.travel.optional(),
  actual_night_timezone: values.actual_night_timezone.optional(), illness: values.illness.optional(),
  sleep_quality: values.sleep_quality.optional(), morning_condition: values.morning_condition.optional(),
  note: values.note.optional(),
}).refine(value => CONTEXT_FIELDS.some(field => Object.hasOwn(value, field)),
  { message: "Supply at least one explicit context field." });

function prefix(day: string): string {
  contextDate.parse(day);
  return `health/night-context/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}/`;
}
function stamp(createdAt: string): string { return createdAt.replace(/[-:.Z]/g, ""); }
async function contentId(core: Record<string, unknown>): Promise<string> {
  const sorted = Object.fromEntries(Object.entries(core).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0));
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(sorted)));
  return "nctx-" + [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("").slice(0, 24);
}
function boundedResult<T extends Record<string, unknown>>(value: T) {
  if (new TextEncoder().encode(JSON.stringify(value)).byteLength > 128 * 1024) {
    throw new Error("Night context output exceeded its bounded limit.");
  }
  return structuredToolResult(value);
}
export interface NightContextReadHooks {
  onError?: (error: unknown) => void;
  beforeList?: () => void;
  readJson?: (keys: string[], limits: R2ReadLimits) => Promise<{ key: string; data: unknown } | null>;
}
async function eventKeys(env: Env, day: string, hooks: NightContextReadHooks = {}): Promise<string[]> {
  const root = prefix(day);
  hooks.beforeList?.();
  const listed = await env.SLIPSTREAM_DATA.list({ prefix: root, limit: EVENT_LIMIT + 1 });
  if (listed.truncated || listed.objects.length > EVENT_LIMIT) throw new Error("Night context history exceeds its bounded limit.");
  const keys = listed.objects.map(object => object.key);
  if (keys.some(key => !key.startsWith(root)
    || !/^\d{8}T\d{9}-nctx-[a-f0-9]{24}\.json$/.test(key.slice(root.length)))) {
    throw new Error("Stored night context keys are unverified.");
  }
  return keys.sort().reverse();
}
async function eventAt(env: Env, day: string, key: string, hooks: NightContextReadHooks = {}): Promise<NightUserContext> {
  const stored = hooks.readJson ? await hooks.readJson([key], EVENT_LIMITS)
    : await new R2Storage(env.SLIPSTREAM_DATA).getR2Json([key], EVENT_LIMITS);
  const parsed = nightUserContextSchema.safeParse(stored?.data);
  if (!parsed.success || parsed.data.wake_date !== day) throw new Error("Stored night context is unverified.");
  const { context_id, ...core } = parsed.data;
  if (context_id !== await contentId(core)
    || key !== `${prefix(day)}${stamp(parsed.data.created_at)}-${context_id}.json`) {
    throw new Error("Stored night context revision is unverified.");
  }
  return parsed.data;
}
async function latest(env: Env, day: string, hooks: NightContextReadHooks = {}) {
  const keys = await eventKeys(env, day, hooks);
  return { count: keys.length, context: keys.length ? await eventAt(env, day, keys[0], hooks) : null };
}

/** Failures stay unavailable; an empty, successful bounded listing alone proves not_stored. */
export async function readNightUserContext(env: Env, wakeDate: string, hooks: NightContextReadHooks = {}): Promise<NightUserContextRead> {
  let userContext: NightUserContext | null = null;
  let status: NightUserContextRead["view"]["context_status"] = "unavailable";
  try {
    userContext = (await latest(env, wakeDate, hooks)).context;
    status = userContext ? "available" : "not_stored";
  } catch (error) {
    hooks.onError?.(error);
    // Ordinary readers preserve independent night data; budget owners may rethrow their own typed limit.
  }
  return { user_context: userContext, view: { schema_version: 1, context_status: status,
    context_revision: userContext?.context_id ?? null, context_updated_at: userContext?.created_at ?? null,
    resolved_at: new Date().toISOString() } };
}

async function documentedNight(env: Env, day: string): Promise<void> {
  const storage = new R2Storage(env.SLIPSTREAM_DATA);
  for (const keys of [sleepObjectKeys(day), hrvObjectKeys(day)]) {
    const stored = await storage.getR2Json(keys, NIGHT_LIMITS);
    if (!stored) continue;
    const value = stored.data;
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("The stored night is unverified.");
    const night = value as Record<string, unknown>;
    if (night.date !== day || nightContext(day, night.sleep_start_gmt, night.sleep_end_gmt, null)
      .time_quality_flags.includes("invalid_sleep_interval")) throw new Error("The stored night is unverified.");
    return;
  }
  throw new Error("No documented night is stored for this wake-date.");
}

export class NightContextTools {
  constructor(private readonly env: Env, private readonly actorKey: string) {}

  private async reserveActorWrite(): Promise<void> {
    const configured = Number.parseInt(this.env.MCP_WRITE_DAILY_LIMIT, 10);
    const dailyLimit = Number.isSafeInteger(configured) && configured > 0 ? Math.min(configured, 1000) : 60;
    const key = `mcp-write:${this.actorKey}`;
    const lease = await this.env.REFRESH_COORDINATOR.getByName("global")
      .reserveBudgeted(key, key, Date.now(), WRITE_COOLDOWN_MS, dailyLimit);
    if (!lease.acquired) {
      const retrySeconds = Math.max(1, Math.ceil((lease.retryAfterMs ?? WRITE_COOLDOWN_MS) / 1000));
      throw new ApplicationToolError({ kind: lease.reason === "daily_limit" ? "write_daily_limit" : "write_cooldown", retrySeconds });
    }
  }

  registerTools(server: McpServer): void {
    server.registerTool("night_context_history", {
      description: "Read bounded append-only explicit user context for one wake-date. Current effective context is independent of analysis age. No provider request or storage write is made.",
      inputSchema: z.strictObject({ wake_date: contextDate, limit: z.number().int().min(1).max(20).default(10),
        before_context_id: contextIdSchema.optional() }),
      outputSchema: nightContextHistoryOutputSchema,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async args => {
      try {
        const keys = await eventKeys(this.env, args.wake_date);
        let offset = 0;
        if (args.before_context_id) {
          const before = keys.findIndex(key => key.endsWith(`-${args.before_context_id}.json`));
          if (before < 0) throw new Error("Night context cursor is unverified.");
          offset = before + 1;
        }
        const selected = keys.slice(offset, offset + args.limit);
        const events: NightUserContext[] = [];
        for (const key of selected) events.push(await eventAt(this.env, args.wake_date, key));
        const more = offset + selected.length < keys.length;
        return boundedResult({ wake_date: args.wake_date, status: keys.length ? "available" : "not_stored",
          events, next_before_context_id: more ? events.at(-1)!.context_id : null, more });
      } catch {
        return boundedResult({ wake_date: args.wake_date, status: "unavailable", events: [], next_before_context_id: null, more: false });
      }
    });

    if (this.env.MCP_WRITES_ENABLED !== "true") return;
    server.registerTool("add_night_context", {
      description: "Append explicit user context to one documented wake-date. Call only on explicit user request. Omitted fields are retained; explicit null clears only that field. Source is user, never an inferred diagnosis. If saved is true, retry only an unavailable read, never this saved append.",
      inputSchema, outputSchema: addNightContextOutputSchema,
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    }, async (args, mcpContext) => applicationToolResult(server, mcpContext, async () => {
      const day = args.wake_date;
      const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
      // Claim before either source/identity or effective-snapshot reads. An unknown PUT may still arrive later.
      const claim = await coordinator.reserveNightContextWrite(day);
      if (!claim.acquired) throw new ApplicationToolError({ kind: "night_context_in_progress" });
      const token = claim.token;
      if (!token) throw new Error("Night context reservation was not confirmed.");
      let attempted = false;
      let context!: NightUserContext;
      try {
        await documentedNight(this.env, day);
        const previous = await latest(this.env, day);
        if (previous.count >= EVENT_LIMIT) throw new Error("Night context event capacity has been reached.");
        const createdAt = new Date(Math.max(Date.now(), previous.context ? Date.parse(previous.context.created_at) + 1 : 0)).toISOString();
        const fields = Object.fromEntries(CONTEXT_FIELDS.map(field => [field,
          Object.hasOwn(args, field) ? args[field] : previous.context?.[field] ?? null]));
        const core = { schema_version: 1 as const, wake_date: day,
          previous_context_id: previous.context?.context_id ?? null, created_at: createdAt, source: "user" as const,
          ...fields,
          declared_fields: [...new Set([...(previous.context?.declared_fields ?? []),
            ...CONTEXT_FIELDS.filter(field => Object.hasOwn(args, field))])].sort(),
          changed_fields: CONTEXT_FIELDS.filter(field => Object.hasOwn(args, field)) };
        const contextId = await contentId(core);
        context = nightUserContextSchema.parse({ ...core, context_id: contextId });
        const body = JSON.stringify(context);
        if (new TextEncoder().encode(body).byteLength > EVENT_LIMITS.stored) throw new Error("Night context event exceeds its bounded size.");
        await this.reserveActorWrite();
        attempted = true;
        const saved = await this.env.SLIPSTREAM_DATA.put(`${prefix(day)}${stamp(createdAt)}-${contextId}.json`, body,
          { httpMetadata: { contentType: "application/json" }, onlyIf: { etagDoesNotMatch: "*" } });
        if (!saved) throw new Error("Night context append was not confirmed.");
      } catch (error) {
        if (!attempted) {
          try { await coordinator.finishNightContextWrite(day, token); }
          catch { console.error(JSON.stringify({ message: "Night context pre-write claim release was not confirmed" })); }
        }
        throw error;
      }
      try {
        const completed = await coordinator.finishNightContextWrite(day, token);
        if (!completed.released) throw new Error("Night context completion was not confirmed.");
      } catch { console.error(JSON.stringify({ message: "Saved night context completion was not confirmed" })); }
      const current = await readNightUserContext(this.env, day);
      return boundedResult({ saved: true as const, wake_date: day, context_id: context.context_id, context,
        ...current, message: current.view.context_status === "unavailable"
          ? "The context is saved, but its current view is unavailable. Retry only the read, not this saved append."
          : "The append-only effective night context was saved; existing source and analysis history were preserved." });
    }));
  }
}