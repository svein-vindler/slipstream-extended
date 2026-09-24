/**
 * Slipstream - remote MCP connector (Cloudflare Worker).
 *
 * Serves your Garmin training data from private R2 to Claude and ChatGPT as a
 * custom connector - so you can ask about it from the phone apps, web, and
 * agent surfaces.
 *
 * The /mcp route is protected by Cloudflare Access Managed OAuth.
 * The MCP tools expose summaries plus normalized HRV and strength data, never
 * GPS tracks.
 *
 * Config:
 *   ACCESS_TEAM_DOMAIN  Cloudflare Access team origin
 *   ACCESS_AUD          Access application audience tag
 *   MCP_HOSTNAME        exact public hostname accepted by the MCP transport
 *   MCP_WRITES_ENABLED  optional literal true for bounded append-only tools
 *   SLIPSTREAM_DATA     private R2 bucket binding
 *   GITHUB_ACTIONS_TOKEN  optional fine-grained token for on-demand refresh
 *   GITHUB_REPOSITORY     optional owner/repository for on-demand refresh
 *
 * Pure data helpers live in ./lib (unit-tested); this file is the MCP wiring.
 */
import { createMcpHandler } from "agents/mcp/server";
import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import {
  Activity, HealthDay, parseCsv, parseHealthCsv, filterActs, filterHealth,
  summarize, summarizeHealth, toSummary, toHealthSummary, bucketKey, healthBucketKey,
  rawGarminActivityId, hrvObjectKeys, activityJsonObjectKeys,
  activityEnduranceObjectKeys, sleepObjectKeys, bodyCompositionObjectKeys,
  coachInputPrefix, activityContextPrefix,
} from "./lib";
import {
  buildHrvHistory, buildSleepHistory, historyMonthKeys, historyReadThroughDates,
  historyRowsEquivalent, indexedHistoryRows, indexedHistorySourceRevisions,
  resolveHistoryRequest,
  summarizeHrvPayload, summarizeSleepPayload,
} from "./health-history";
import {
  RefreshConfig, RefreshRun, dispatchRefresh, latestRefreshRun, pollRefreshRun,
  refreshDecision, refreshProgress,
} from "./github";
import { RefreshCoordinator } from "./refresh-coordinator";
import {
  PayloadTooLargeError, decodePossiblyGzippedText, limitRequestBody,
  secureResponse,
} from "./security";
import { outputSchemas, structuredToolResult } from "./mcp-output";
import { authenticateMcpRequest } from "./mcp-auth";

export { RefreshCoordinator } from "./refresh-coordinator";

const R2_CSV_PATH = "summary/activities.csv";
const R2_HEALTH_CSV_PATH = "summary/health_daily.csv";
const COACH_PROFILE_PREFIX = "coach/profiles/v1/";
const COACH_PROFILE_INDEX_KEY = "coach/indexes/profiles-v1.json";
const REQUEST_BODY_LIMIT_BYTES = 256 * 1024;
const SUMMARY_R2_LIMITS = { stored: 2 * 1024 * 1024, decoded: 16 * 1024 * 1024 };
const GRANULAR_R2_LIMITS = { stored: 8 * 1024 * 1024, decoded: 32 * 1024 * 1024 };
const HISTORY_INDEX_R2_LIMITS = { stored: 512 * 1024, decoded: 2 * 1024 * 1024 };
const COACH_CONFIG_R2_LIMITS = { stored: 256 * 1024, decoded: 512 * 1024 };
const REFRESH_LEASE_TTL_MS = 2 * 60 * 1000;
const REFRESH_POLL_ATTEMPTS = 5;
const REFRESH_POLL_INTERVAL_MS = 4_000;
const REFRESH_STATUS_LEASE_TTL_MS = REFRESH_POLL_ATTEMPTS * REFRESH_POLL_INTERVAL_MS + 5_000;
const COACH_PROFILE_WRITE_TTL_MS = 60_000;
const ACTIVITY_CONTEXT_WRITE_TTL_MS = 10_000;
const DEFAULT_MCP_WRITE_DAILY_LIMIT = 60;
const PRIVATE_READ_TOOL_ANNOTATIONS = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: false,
} as const;
type SummaryStorage = "r2" | "none";

async function contentId(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 24);
}

// Per-isolate caches retain parsed summaries, while an inexpensive R2 HEAD
// check prevents a different warm isolate from serving a stale post-refresh
// view for several minutes.
let activityCache: { etag: string; data: Activity[] } | undefined;
let healthCache: { etag: string; data: HealthDay[] } | undefined;

function refreshControl(run: RefreshRun | null) {
  const progress = refreshProgress(run);
  return {
    terminal: progress.terminal,
    data_ready: progress.dataReady,
    should_continue_polling: progress.shouldContinuePolling,
    ...(progress.shouldContinuePolling ? { poll_after_seconds: 4 } : {}),
  };
}

function clearSummaryCachesWhenReady(run: RefreshRun | null): void {
  if (!refreshProgress(run).dataReady) return;
  activityCache = undefined;
  healthCache = undefined;
}

class FitnessService {
  private activityStorage: SummaryStorage = "none";
  private healthStorage: SummaryStorage = "none";

  constructor(
    private readonly env: Env,
    private readonly actorKey: string,
  ) {}

  private refreshConfig(): RefreshConfig {
    const cooldownMinutes = Number.parseInt(this.env.REFRESH_COOLDOWN_MINUTES, 10);
    if (!this.env.GITHUB_ACTIONS_TOKEN || !this.env.GITHUB_REPOSITORY) {
      throw new Error("On-demand refresh is not configured on this Worker.");
    }
    return {
      repository: this.env.GITHUB_REPOSITORY,
      workflow: this.env.GITHUB_REFRESH_WORKFLOW,
      ref: this.env.GITHUB_REFRESH_REF,
      token: this.env.GITHUB_ACTIONS_TOKEN,
      cooldownMinutes: Number.isFinite(cooldownMinutes) ? cooldownMinutes : 30,
    };
  }

  async getActivities(): Promise<Activity[]> {
    try {
      const metadata = await this.env.SLIPSTREAM_DATA.head(R2_CSV_PATH);
      if (activityCache && metadata?.etag === activityCache.etag) {
        this.activityStorage = "r2";
        return activityCache.data;
      }
      const stored = await this.getR2Text([R2_CSV_PATH], SUMMARY_R2_LIMITS);
      if (!stored) throw new Error(`R2 object ${R2_CSV_PATH} was not found.`);
      const data = parseCsv(stored.text);
      if (!data.length && !stored.text.startsWith("Activity ID,")) {
        throw new Error("R2 activity summary has an invalid CSV header.");
      }
      this.activityStorage = "r2";
      activityCache = { etag: stored.etag, data };
      return data;
    } catch (error) {
      console.error(JSON.stringify({
        message: "R2 activity summary unavailable",
        error: error instanceof Error ? error.message : String(error),
      }));
      throw new Error("Activity data is unavailable in private R2.");
    }
  }

  async getHealth(): Promise<HealthDay[]> {
    try {
      const metadata = await this.env.SLIPSTREAM_DATA.head(R2_HEALTH_CSV_PATH);
      if (healthCache && metadata?.etag === healthCache.etag) {
        this.healthStorage = "r2";
        return healthCache.data;
      }
      const stored = await this.getR2Text([R2_HEALTH_CSV_PATH], SUMMARY_R2_LIMITS);
      if (!stored) throw new Error(`R2 object ${R2_HEALTH_CSV_PATH} was not found.`);
      const data = parseHealthCsv(stored.text);
      if (!data.length && !stored.text.startsWith("Date,")) {
        throw new Error("R2 health summary has an invalid CSV header.");
      }
      this.healthStorage = "r2";
      healthCache = { etag: stored.etag, data };
      return data;
    } catch (error) {
      console.error(JSON.stringify({
        message: "R2 health summary unavailable",
        error: error instanceof Error ? error.message : String(error),
      }));
      throw new Error("Health data is unavailable in private R2.");
    }
  }

  async getR2Text(
    keys: string[],
    limits = GRANULAR_R2_LIMITS,
  ): Promise<{ key: string; text: string; etag: string } | null> {
    for (const key of keys) {
      const object = await this.env.SLIPSTREAM_DATA.get(key);
      if (!object) continue;
      if (object.size > limits.stored) {
        throw new Error(`R2 object ${key} exceeds the stored-size limit.`);
      }
      const buffer = await object.arrayBuffer();
      const text = await decodePossiblyGzippedText(buffer, limits.decoded);
      return { key, text, etag: object.etag };
    }
    return null;
  }

  async getR2Json(
    keys: string[],
    limits = GRANULAR_R2_LIMITS,
  ): Promise<{ key: string; data: unknown } | null> {
    const stored = await this.getR2Text(keys, limits);
    return stored ? { key: stored.key, data: JSON.parse(stored.text) as unknown } : null;
  }

  async readCanonicalHistoryRow(
    stream: "hrv" | "sleep",
    day: string,
    indexedRow: Record<string, unknown> | undefined,
    includeDetail: boolean,
    source: { key: string; etag: string } | undefined,
    indexedRevision: string | undefined,
  ): Promise<{
    row: Record<string, unknown>;
    stale: boolean;
    orphaned: boolean;
    confirmedMissing: boolean;
    sourceRead: boolean;
  }> {
    if (!source) {
      const orphaned = indexedRow !== undefined;
      return {
        row: {
          date: day,
          status: "not_stored",
          index_state: orphaned ? "orphaned_index" : "confirmed_missing",
        },
        stale: false,
        orphaned,
        confirmedMissing: !orphaned,
        sourceRead: false,
      };
    }
    if (!includeDetail && indexedRow && indexedRevision === source.etag) {
      return {
        row: { ...indexedRow, index_state: "verified" },
        stale: false,
        orphaned: false,
        confirmedMissing: false,
        sourceRead: false,
      };
    }
    let stored: { key: string; text: string; etag: string } | null = null;
    let invalid = false;
    try {
      stored = await this.getR2Text([source.key]);
    } catch {
      // The object was found but could not be decoded within the bounded limits.
      invalid = true;
    }
    // R2 listing is strongly consistent. A key disappearing between LIST and
    // GET is treated as a stale index/source race rather than serving old data.
    if (!stored && !invalid) {
      const orphaned = indexedRow !== undefined;
      return {
        row: {
          date: day,
          status: "not_stored",
          index_state: orphaned ? "orphaned_index" : "confirmed_missing",
        },
        stale: false,
        orphaned,
        confirmedMissing: !orphaned,
        sourceRead: false,
      };
    }

    let canonical: Record<string, unknown>;
    try {
      const payload = invalid ? null : JSON.parse(stored!.text) as unknown;
      canonical = stream === "hrv"
        ? summarizeHrvPayload(day, payload)
        : summarizeSleepPayload(day, payload);
    } catch {
      canonical = { date: day, status: "invalid_schema" };
    }
    const stale = !indexedRow || !historyRowsEquivalent(indexedRow, canonical);
    const row: Record<string, unknown> = {
      ...canonical,
      index_state: stale ? "read_through" : "verified",
    };
    if (!includeDetail) {
      delete row.readings;
      delete row.stages;
    }
    return {
      row, stale, orphaned: false, confirmedMissing: false, sourceRead: true,
    };
  }

  async discoverCanonicalHistoryKeys(
    stream: "hrv" | "sleep",
    dates: string[],
  ): Promise<{
    keys: Map<string, { key: string; etag: string }>;
    prefixesScanned: number;
  }> {
    const allowed = new Set(dates);
    const months = [...new Set(dates.map((day) => day.slice(0, 7)))];
    const keys = new Map<string, { key: string; etag: string }>();
    for (const month of months) {
      const root = stream === "hrv" ? "health/hrv" : "health/sleep/v1";
      const prefix = `${root}/${month.slice(0, 4)}/${month.slice(5, 7)}/`;
      const listed = await this.env.SLIPSTREAM_DATA.list({ prefix, limit: 100 });
      if (listed.truncated) {
        throw new Error(`R2 history source listing exceeded the bounded monthly limit: ${prefix}`);
      }
      for (const object of listed.objects) {
        const filename = object.key.slice(prefix.length);
        const match = /^(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?$/.exec(filename);
        if (!match || !allowed.has(match[1])) continue;
        const current = keys.get(match[1]);
        if (!current || (current.key.endsWith(".json.gz") && object.key.endsWith(".json"))) {
          keys.set(match[1], { key: object.key, etag: object.etag });
        }
      }
    }
    return { keys, prefixesScanned: months.length };
  }

  async putSmallJson(key: string, value: Record<string, unknown>): Promise<void> {
    const body = JSON.stringify(value);
    if (new TextEncoder().encode(body).byteLength > COACH_CONFIG_R2_LIMITS.stored) {
      throw new Error("Coach configuration exceeds the 256 KiB safety limit.");
    }
    await this.env.SLIPSTREAM_DATA.put(key, body, {
      httpMetadata: { contentType: "application/json" },
    });
  }

  private writesEnabled(): boolean {
    return this.env.MCP_WRITES_ENABLED === "true";
  }

  async getCoachProfiles(): Promise<{
    keys: string[];
    profiles: Record<string, unknown>[];
  }> {
    const listed = await this.env.SLIPSTREAM_DATA.list({
      prefix: COACH_PROFILE_PREFIX,
      limit: 101,
    });
    if (listed.truncated || listed.objects.length > 100) {
      throw new Error("Coach profile count exceeds the supported safety limit of 100.");
    }
    const keys = listed.objects.map((object) => object.key).sort();
    const indexed = await this.getR2Json(
      [COACH_PROFILE_INDEX_KEY],
      COACH_CONFIG_R2_LIMITS,
    );
    if (indexed?.data && typeof indexed.data === "object" && !Array.isArray(indexed.data)) {
      const value = indexed.data as Record<string, unknown>;
      const indexedKeys = Array.isArray(value.profile_keys)
        ? value.profile_keys.filter((key): key is string => typeof key === "string").sort()
        : [];
      const profiles = Array.isArray(value.profiles)
        ? value.profiles.filter((profile): profile is Record<string, unknown> =>
          !!profile && typeof profile === "object" && !Array.isArray(profile))
        : [];
      if (indexedKeys.length === keys.length
        && indexedKeys.every((key, index) => key === keys[index])
        && profiles.length === keys.length) {
        return { keys, profiles };
      }
    }

    const profiles: Record<string, unknown>[] = [];
    for (const key of keys) {
      const stored = await this.getR2Json([key], COACH_CONFIG_R2_LIMITS);
      if (stored?.data && typeof stored.data === "object" && !Array.isArray(stored.data)) {
        profiles.push(stored.data as Record<string, unknown>);
      }
    }
    return { keys, profiles };
  }

  async updateCoachProfileIndex(
    keys: string[],
    profiles: Record<string, unknown>[],
  ): Promise<void> {
    await this.putSmallJson(COACH_PROFILE_INDEX_KEY, {
      schema_version: 1,
      updated_at: new Date().toISOString(),
      profile_keys: [...keys].sort(),
      profiles: [...profiles].sort((a, b) =>
        String(a.effective_from ?? "").localeCompare(String(b.effective_from ?? ""))),
    });
  }

  async reserveCoachWrite(ttlMs: number): Promise<void> {
    const configured = Number.parseInt(this.env.MCP_WRITE_DAILY_LIMIT, 10);
    const dailyLimit = Number.isSafeInteger(configured) && configured > 0
      ? Math.min(configured, 1000)
      : DEFAULT_MCP_WRITE_DAILY_LIMIT;
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const key = `mcp-write:${this.actorKey}`;
    const lease = await coordinator.reserveBudgeted(
      key,
      key,
      Date.now(),
      ttlMs,
      dailyLimit,
    );
    if (!lease.acquired) {
      const retrySeconds = Math.max(
        1, Math.ceil((lease.retryAfterMs ?? ttlMs) / 1000),
      );
      if (lease.reason === "daily_limit") {
        throw new Error(`The daily MCP write safety limit has been reached. Retry in ${retrySeconds} seconds.`);
      }
      throw new Error(`A similar write was accepted recently. Retry in ${retrySeconds} seconds.`);
    }
  }

  private text<T extends Record<string, unknown>>(obj: T) {
    return structuredToolResult(obj);
  }

  registerTools(server: McpServer) {
    const exactDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).describe("YYYY-MM-DD");
    const dateRange = exactDate.optional();
    const sport = z.string().trim().min(1).max(64)
      .describe('e.g. "Run", "Ride", "Swim", "Yoga"').optional();

    server.registerTool("data_status", {
      description: "Check the fitness data is connected; returns count, date range, sources.",
      inputSchema: z.object({}),
      outputSchema: outputSchemas.data_status,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async () => {
      const a = await this.getActivities();
      const dates = a.map((x) => x.date).filter((d): d is Date => !!d).map((d) => d.getTime());
      const srcs: Record<string, number> = {};
      a.forEach((x) => { srcs[x.source] = (srcs[x.source] ?? 0) + 1; });
      return this.text({
        connected: true, count: a.length,
        earliest: dates.length ? new Date(Math.min(...dates)).toISOString().slice(0, 10) : null,
        latest: dates.length ? new Date(Math.max(...dates)).toISOString().slice(0, 10) : null,
        by_source: srcs, storage: this.activityStorage,
      });
    });

    server.registerTool("list_activities", {
      description: "List activities, newest first. Filter by sport/date/name; sort and limit.",
      inputSchema: z.object({
        sport_type: sport, start_date: dateRange, end_date: dateRange,
        name_contains: z.string().trim().min(1).max(128).optional(),
        limit: z.number().int().min(1).max(200).default(20),
        sort: z.enum(["date_desc", "date_asc", "distance_desc", "distance_asc"]).default("date_desc"),
      }),
      outputSchema: outputSchemas.list_activities,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      let rows = filterActs(await this.getActivities(), args);
      const cmp: Record<string, (a: Activity, b: Activity) => number> = {
        date_desc: (a, b) => (b.date?.getTime() ?? 0) - (a.date?.getTime() ?? 0),
        date_asc: (a, b) => (a.date?.getTime() ?? 0) - (b.date?.getTime() ?? 0),
        distance_desc: (a, b) => (b.distanceKm ?? 0) - (a.distanceKm ?? 0),
        distance_asc: (a, b) => (a.distanceKm ?? 0) - (b.distanceKm ?? 0),
      };
      rows = [...rows].sort(cmp[args.sort]);
      return this.text({ matched: rows.length, showing: Math.min(args.limit, rows.length),
        activities: rows.slice(0, args.limit).map(toSummary) });
    });

    server.registerTool("activity_stats", {
      description: "Totals (distance, time, elevation, calories, avg HR). Optionally group_by sport/month/year.",
      inputSchema: z.object({
        sport_type: sport, start_date: dateRange, end_date: dateRange,
        group_by: z.enum(["sport", "month", "year"]).optional(),
      }),
      outputSchema: outputSchemas.activity_stats,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const rows = filterActs(await this.getActivities(), args);
      const result: Record<string, unknown> = { overall: summarize(rows) };
      if (args.group_by) {
        const buckets: Record<string, Activity[]> = {};
        for (const a of rows) { const k = bucketKey(a, args.group_by); if (k) (buckets[k] ??= []).push(a); }
        const grouped: Record<string, unknown> = {};
        Object.keys(buckets).sort().forEach((k) => { grouped[k] = summarize(buckets[k]); });
        result["by_" + args.group_by] = grouped;
      }
      return this.text(result);
    });

    server.registerTool("personal_bests", {
      description: "Activity-level bests: longest distance, longest time, most elevation, fastest pace.",
      inputSchema: z.object({ sport_type: sport }),
      outputSchema: outputSchemas.personal_bests,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const rows = filterActs(await this.getActivities(), args);
      const best = (f: (a: Activity) => number | undefined, desc = true) => {
        const c = rows.filter((a) => f(a) != null);
        if (!c.length) return null;
        c.sort((a, b) => desc ? (f(b)! - f(a)!) : (f(a)! - f(b)!));
        return toSummary(c[0]);
      };
      return this.text({
        longest_distance: best((a) => a.distanceKm),
        longest_moving_time: best((a) => a.movingS),
        most_elevation_gain: best((a) => a.elevGain),
        fastest_avg_pace: best((a) => (a.distanceKm && a.movingS ? a.movingS / a.distanceKm : undefined), false),
      });
    });

    server.registerTool("list_sport_types", {
      description: "Distinct activity types with counts.", inputSchema: z.object({}),
      outputSchema: outputSchemas.list_sport_types,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async () => {
      const counts: Record<string, number> = {};
      for (const a of await this.getActivities()) counts[a.type || "Unknown"] = (counts[a.type || "Unknown"] ?? 0) + 1;
      return this.text(Object.fromEntries(Object.entries(counts).sort((a, b) => b[1] - a[1])));
    });

    server.registerTool("search_activities", {
      description: "Free-text search over activity name, type, and source.",
      inputSchema: z.object({
        query: z.string().trim().min(1).max(128),
        limit: z.number().int().min(1).max(100).default(20),
      }),
      outputSchema: outputSchemas.search_activities,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const q = args.query.toLowerCase();
      const hits = (await this.getActivities()).filter((a) =>
        a.name.toLowerCase().includes(q) || a.type.toLowerCase().includes(q) || a.source.toLowerCase().includes(q));
      hits.sort((a, b) => (b.date?.getTime() ?? 0) - (a.date?.getTime() ?? 0));
      return this.text({ matched: hits.length, activities: hits.slice(0, args.limit).map(toSummary) });
    });

    server.registerTool("health_status", {
      description: "Check daily Garmin health summaries: populated days and date range.",
      inputSchema: z.object({}),
      outputSchema: outputSchemas.health_status,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async () => {
      const rows = await this.getHealth();
      const dates = rows.map((row) => row.date).sort();
      return this.text({
        connected: true, days: rows.length,
        earliest: dates[0] ?? null, latest: dates[dates.length - 1] ?? null,
        storage: this.healthStorage,
        note: rows.length
          ? "This status covers daily summaries; detailed streams may be available through dedicated tools."
          : "No health backfill has completed yet.",
      });
    });

    server.registerTool("daily_health", {
      description: "List daily sleep, HRV, pulse, Body Battery, stress, steps, respiration and weight summaries.",
      inputSchema: z.object({
        start_date: dateRange, end_date: dateRange,
        limit: z.number().int().min(1).max(366).default(30),
        sort: z.enum(["date_desc", "date_asc"]).default("date_desc"),
      }),
      outputSchema: outputSchemas.daily_health,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      let rows = filterHealth(await this.getHealth(), args);
      rows = [...rows].sort((a, b) => args.sort === "date_asc"
        ? a.date.localeCompare(b.date) : b.date.localeCompare(a.date));
      return this.text({ matched: rows.length, showing: Math.min(args.limit, rows.length),
        days: rows.slice(0, args.limit).map(toHealthSummary) });
    });

    server.registerTool("health_trends", {
      description: "Summarize health metrics over a date range, optionally grouped by month or year.",
      inputSchema: z.object({
        start_date: dateRange, end_date: dateRange,
        group_by: z.enum(["month", "year"]).optional(),
      }),
      outputSchema: outputSchemas.health_trends,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const rows = filterHealth(await this.getHealth(), args);
      const result: Record<string, unknown> = { overall: summarizeHealth(rows) };
      if (args.group_by) {
        const buckets: Record<string, HealthDay[]> = {};
        for (const row of rows) (buckets[healthBucketKey(row, args.group_by)] ??= []).push(row);
        result["by_" + args.group_by] = Object.fromEntries(
          Object.keys(buckets).sort().map((key) => [key, summarizeHealth(buckets[key])]),
        );
      }
      return this.text(result);
    });

    server.registerTool("hrv_curve", {
      description: "Read the detailed overnight Garmin HRV curve for one date. Returns timestamps and HRV values, without GPS or raw device payloads.",
      inputSchema: z.object({ date: exactDate }),
      outputSchema: outputSchemas.hrv_curve,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const stored = await this.getR2Json(hrvObjectKeys(args.date));
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({
          available: false,
          date: args.date,
          message: "No detailed HRV curve is stored for this date yet.",
        });
      }
      const payload = stored.data as Record<string, unknown>;
      if (payload.available === false) {
        return this.text({
          available: false,
          date: payload.date ?? args.date,
          message: "Garmin has no detailed HRV readings for this date.",
        });
      }
      const readings = Array.isArray(payload.readings) ? payload.readings.flatMap((value) => {
        if (!value || typeof value !== "object" || Array.isArray(value)) return [];
        const row = value as Record<string, unknown>;
        return [{ timestamp: row.timestamp ?? null, hrv_ms: row.hrv_ms ?? null }];
      }) : [];
      return this.text({
        available: true,
        date: payload.date ?? args.date,
        sleep_start_gmt: payload.sleep_start_gmt ?? null,
        sleep_end_gmt: payload.sleep_end_gmt ?? null,
        summary: payload.summary ?? null,
        reading_count: readings.length,
        readings,
      });
    });

    server.registerTool("hrv_history", {
      description: "Analyze detailed overnight HRV across a date range. Auto returns daily summaries for up to 31 days and compact weekly summaries for longer ranges (up to 366 days); full readings are limited to 7 days.",
      inputSchema: z.object({
        start_date: exactDate,
        end_date: exactDate,
        granularity: z.enum(["auto", "daily", "weekly"]).default("auto"),
        detail_level: z.enum(["summary", "full"]).default("summary"),
      }),
      outputSchema: outputSchemas.hrv_history,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const request = resolveHistoryRequest(
        args.start_date, args.end_date, args.granularity, args.detail_level,
      );
      const keys = historyMonthKeys("hrv", request.dates);
      const indexes: unknown[] = [];
      const invalidIndexObjects: string[] = [];
      for (const key of keys) {
        try {
          const stored = await this.getR2Json([key], HISTORY_INDEX_R2_LIMITS);
          if (stored) indexes.push(stored.data);
        } catch {
          invalidIndexObjects.push(key);
        }
      }
      const indexedRows = indexedHistoryRows("hrv", indexes, request.dates);
      const indexedRevisions = indexedHistorySourceRevisions("hrv", indexes);
      const probeDates = historyReadThroughDates(request.dates, request.granularity);
      const discovered = await this.discoverCanonicalHistoryKeys("hrv", probeDates);
      const overrides = new Map<string, Record<string, unknown>>();
      const staleDates: string[] = [];
      const orphanedIndexDates: string[] = [];
      const confirmedMissingDates: string[] = [];
      let canonicalObjectsRead = 0;
      for (const day of probeDates) {
        const source = discovered.keys.get(day);
        const result = await this.readCanonicalHistoryRow(
          "hrv", day, indexedRows.get(day), args.detail_level === "full",
          source, source ? indexedRevisions.get(source.key) : undefined,
        );
        overrides.set(day, result.row);
        if (result.stale) staleDates.push(day);
        if (result.orphaned) orphanedIndexDates.push(day);
        if (result.confirmedMissing) confirmedMissingDates.push(day);
        if (result.sourceRead) canonicalObjectsRead += 1;
      }
      const history = buildHrvHistory(
        indexes, request.dates, request.granularity, overrides,
      );
      return this.text({
        start_date: args.start_date,
        end_date: args.end_date,
        granularity: request.granularity,
        detail_level: args.detail_level,
        ...history,
        source_objects_read: keys.length + canonicalObjectsRead,
        index_consistency: {
          mode: request.granularity === "daily" ? "all_requested_days" : "recent_7_days",
          checked_dates: probeDates.length,
          index_only_dates: request.dates.length - probeDates.length,
          stale_dates: staleDates,
          orphaned_index_dates: orphanedIndexDates,
          confirmed_missing_dates: confirmedMissingDates,
          source_prefixes_scanned: discovered.prefixesScanned,
          invalid_index_objects: invalidIndexObjects,
        },
      });
    });

    server.registerTool("sleep_detail", {
      description: "Read detailed Garmin sleep for one date: sleep window, stages, score components, oxygen, respiration and sleep stress. Returns normalized data without the raw Garmin payload.",
      inputSchema: z.object({ date: exactDate }),
      outputSchema: outputSchemas.sleep_detail,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    }, async (args) => {
      const stored = await this.getR2Json(sleepObjectKeys(args.date));
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({
          available: false,
          date: args.date,
          message: "No detailed sleep record is stored for this date yet.",
        });
      }
      const payload = stored.data as Record<string, unknown>;
      const summary = payload.summary;
      const scoreBreakdown = payload.score_breakdown;
      const stages = Array.isArray(payload.stages) ? payload.stages : [];
      if (!summary || typeof summary !== "object" || Array.isArray(summary)) {
        return this.text({
          available: false,
          date: args.date,
          message: "The stored sleep record has an unsupported schema.",
        });
      }
      return this.text({
        available: true,
        date: typeof payload.date === "string" ? payload.date : args.date,
        sleep_start_gmt: payload.sleep_start_gmt ?? null,
        sleep_end_gmt: payload.sleep_end_gmt ?? null,
        confirmed: typeof payload.confirmed === "boolean" ? payload.confirmed : null,
        summary,
        score_breakdown: scoreBreakdown && typeof scoreBreakdown === "object"
          && !Array.isArray(scoreBreakdown) ? scoreBreakdown : {},
        stage_count: stages.length,
        stages,
      });
    });

    server.registerTool("sleep_history", {
      description: "Analyze detailed sleep across a date range. Auto returns daily summaries for up to 31 days and compact weekly summaries for longer ranges (up to 366 days); full stage timelines are limited to 7 days.",
      inputSchema: z.object({
        start_date: exactDate,
        end_date: exactDate,
        granularity: z.enum(["auto", "daily", "weekly"]).default("auto"),
        detail_level: z.enum(["summary", "full"]).default("summary"),
      }),
      outputSchema: outputSchemas.sleep_history,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const request = resolveHistoryRequest(
        args.start_date, args.end_date, args.granularity, args.detail_level,
      );
      const keys = historyMonthKeys("sleep", request.dates);
      const indexes: unknown[] = [];
      const invalidIndexObjects: string[] = [];
      for (const key of keys) {
        try {
          const stored = await this.getR2Json([key], HISTORY_INDEX_R2_LIMITS);
          if (stored) indexes.push(stored.data);
        } catch {
          invalidIndexObjects.push(key);
        }
      }
      const indexedRows = indexedHistoryRows("sleep", indexes, request.dates);
      const indexedRevisions = indexedHistorySourceRevisions("sleep", indexes);
      const probeDates = historyReadThroughDates(request.dates, request.granularity);
      const discovered = await this.discoverCanonicalHistoryKeys("sleep", probeDates);
      const overrides = new Map<string, Record<string, unknown>>();
      const staleDates: string[] = [];
      const orphanedIndexDates: string[] = [];
      const confirmedMissingDates: string[] = [];
      let canonicalObjectsRead = 0;
      for (const day of probeDates) {
        const source = discovered.keys.get(day);
        const result = await this.readCanonicalHistoryRow(
          "sleep", day, indexedRows.get(day), args.detail_level === "full",
          source, source ? indexedRevisions.get(source.key) : undefined,
        );
        overrides.set(day, result.row);
        if (result.stale) staleDates.push(day);
        if (result.orphaned) orphanedIndexDates.push(day);
        if (result.confirmedMissing) confirmedMissingDates.push(day);
        if (result.sourceRead) canonicalObjectsRead += 1;
      }
      const history = buildSleepHistory(
        indexes, request.dates, request.granularity, overrides,
      );
      return this.text({
        start_date: args.start_date,
        end_date: args.end_date,
        granularity: request.granularity,
        detail_level: args.detail_level,
        ...history,
        source_objects_read: keys.length + canonicalObjectsRead,
        index_consistency: {
          mode: request.granularity === "daily" ? "all_requested_days" : "recent_7_days",
          checked_dates: probeDates.length,
          index_only_dates: request.dates.length - probeDates.length,
          stale_dates: staleDates,
          orphaned_index_dates: orphanedIndexDates,
          confirmed_missing_dates: confirmedMissingDates,
          source_prefixes_scanned: discovered.prefixesScanned,
          invalid_index_objects: invalidIndexObjects,
        },
      });
    });

    server.registerTool("body_composition", {
      description: "Read all Garmin body-composition measurements stored for one date, including weight, BMI, body fat, water, muscle, bone mass and related scale metrics.",
      inputSchema: z.object({ date: exactDate }),
      outputSchema: outputSchemas.body_composition,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    }, async (args) => {
      const stored = await this.getR2Json(bodyCompositionObjectKeys(args.date));
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({
          available: false,
          date: args.date,
          message: "No body-composition measurements are stored for this date yet.",
        });
      }
      const payload = stored.data as Record<string, unknown>;
      const measurements = Array.isArray(payload.measurements) ? payload.measurements : [];
      return this.text({
        available: true,
        date: typeof payload.date === "string" ? payload.date : args.date,
        measurement_count: measurements.length,
        measurements,
      });
    });

    server.registerTool("strength_session", {
      description: "Read normalized sets for one Garmin strength activity: exercise, reps, weight, active time and following rest. Raw FIT messages and GPS are not returned.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/)
          .describe('Activity ID from list_activities, for example "garmin-24431147581"'),
      }),
      outputSchema: outputSchemas.strength_session,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const activity = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!activity) {
        return this.text({
          available: false,
          activity_id: args.activity_id,
          message: "The activity ID was not found in the Slipstream activity index.",
        });
      }
      const stored = await this.getR2Json(activityJsonObjectKeys(activity));
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "No granular export is stored for this activity yet.",
        });
      }
      const payload = stored.data as Record<string, unknown>;
      const session = payload.normalized_strength_session;
      if (!session || typeof session !== "object" || Array.isArray(session)) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "The stored activity predates the normalized strength schema; rerun the granular export.",
        });
      }
      return this.text({ available: true, activity: toSummary(activity), session });
    });

    server.registerTool("endurance_session", {
      description: "Read a GPS-free analysis dataset derived from the Garmin TCX file for one endurance activity. Returns summary metrics, Garmin laps, kilometre splits, distance-half heart-rate drift, seconds per heart-rate BPM, and a compact 10-second trackpoint series.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/)
          .describe('Activity ID from list_activities, for example "garmin-24444691902"'),
      }),
      outputSchema: outputSchemas.endurance_session,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const activity = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!activity) {
        return this.text({
          available: false,
          activity_id: args.activity_id,
          message: "The activity ID was not found in the Slipstream activity index.",
        });
      }
      const stored = await this.getR2Json(activityEnduranceObjectKeys(activity));
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "No normalized TCX analysis is stored for this activity yet. Run the activity backfill or recent granular export.",
        });
      }
      return this.text({
        available: true,
        activity: toSummary(activity),
        session: stored.data,
      });
    });

    const coachZone = z.object({
      label: z.string().trim().min(1).max(24),
      min_bpm: z.number().int().min(30).max(250),
      max_bpm: z.number().int().min(30).max(250),
    });
    const coachReferences = z.object({
      lt1_min_bpm: z.number().int().min(30).max(250).nullable().default(null),
      lt1_max_bpm: z.number().int().min(30).max(250).nullable().default(null),
      lt2_min_bpm: z.number().int().min(30).max(250).nullable().default(null),
      lt2_max_bpm: z.number().int().min(30).max(250).nullable().default(null),
      interval_thresholds_bpm: z.array(z.number().int().min(30).max(250))
        .max(20).default([]),
    });

    server.registerTool("coach_profile", {
      title: "Read coach analysis profile",
      description: "Read the versioned heart-rate zones and threshold references used for coach-input analyses. A date selects the newest profile effective on that date; historical analyses keep their original profile.",
      inputSchema: z.object({ date: exactDate.optional() }),
      outputSchema: outputSchemas.coach_profile,
      annotations: { readOnlyHint: true, destructiveHint: false,
        idempotentHint: true, openWorldHint: false },
    }, async (args) => {
      const storedProfiles = await this.getCoachProfiles();
      const profiles = storedProfiles.profiles.sort((a, b) =>
        String(a.effective_from ?? "").localeCompare(String(b.effective_from ?? "")));
      const selected = args.date
        ? [...profiles].reverse().find((profile) =>
          typeof profile.effective_from === "string" && profile.effective_from <= args.date!) ?? null
        : profiles.at(-1) ?? null;
      return this.text({
        available: selected !== null,
        selected_for_date: args.date ?? null,
        profile: selected,
        profiles,
        message: selected
          ? "Coach analysis profile is available."
          : "No coach profile is stored yet. Ask the user for heart-rate zones and threshold references.",
      });
    });

    if (this.writesEnabled()) {
      server.registerTool("add_coach_profile", {
      title: "Save coach analysis profile",
      description: "Save a new immutable, versioned running analysis profile after the user explicitly supplies or confirms their heart-rate zones and threshold references. Never infer these values. effective_from prevents new zones from rewriting historical analyses.",
      inputSchema: z.object({
        name: z.string().trim().min(1).max(80).default("Running heart-rate profile"),
        effective_from: exactDate,
        zones: z.array(coachZone).min(1).max(10),
        references: coachReferences,
      }),
      outputSchema: outputSchemas.add_coach_profile,
      annotations: { readOnlyHint: false, destructiveHint: false,
        idempotentHint: true, openWorldHint: false },
    }, async (args) => {
      for (let index = 0; index < args.zones.length; index++) {
        const zone = args.zones[index];
        if (zone.min_bpm > zone.max_bpm || (index > 0
          && zone.min_bpm <= args.zones[index - 1].max_bpm)) {
          throw new Error("Heart-rate zones must be ordered and non-overlapping.");
        }
      }
      const core = {
        schema_version: 1,
        name: args.name,
        sport: "running",
        effective_from: args.effective_from,
        default_time_basis: "elapsed",
        zones: args.zones,
        references: args.references,
        source: "user",
      };
      const profileId = `hr-${await contentId(core)}`;
      const profile = { ...core, profile_id: profileId, created_at: new Date().toISOString() };
      const key = `coach/profiles/v1/${args.effective_from}/${profileId}.json`;
      const existing = await this.getR2Json([key]);
      if (!existing) {
        const current = await this.getCoachProfiles();
        await this.reserveCoachWrite(COACH_PROFILE_WRITE_TTL_MS);
        await this.putSmallJson(key, profile);
        try {
          await this.updateCoachProfileIndex(
            [...current.keys, key],
            [...current.profiles, profile],
          );
        } catch (error) {
          console.error(JSON.stringify({
            message: "Coach profile was saved but its read index could not be updated",
            error: error instanceof Error ? error.message : String(error),
          }));
        }
      }
      const savedProfile = existing?.data && typeof existing.data === "object"
        && !Array.isArray(existing.data) ? existing.data as Record<string, unknown> : profile;
      return this.text({ saved: true, profile_id: profileId, key, profile: savedProfile,
        message: existing
          ? "This identical immutable profile was already stored."
          : "The immutable profile was saved. The R2-only coach backfill will use it for activities on or after its effective date." });
    });

      server.registerTool("add_activity_context", {
      title: "Add user context to an activity",
      description: "Append user-supplied RPE, conditions, note or workout correction to one activity. Call only when the user explicitly asks to record this information. The values are never inferred and existing context is never overwritten.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/),
        rpe: z.number().min(0).max(10).nullable().optional(),
        conditions: z.string().trim().min(1).max(240).nullable().optional(),
        note: z.string().trim().min(1).max(2000).nullable().optional(),
        workout_correction: z.string().trim().min(1).max(1000).nullable().optional(),
      }).refine((value) => value.rpe != null || value.conditions != null
        || value.note != null || value.workout_correction != null,
      { message: "At least one context field is required." }),
      outputSchema: outputSchemas.add_activity_context,
      annotations: { readOnlyHint: false, destructiveHint: false,
        idempotentHint: false, openWorldHint: false },
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const activity = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!activity || !activity.date) throw new Error("The activity ID was not found.");
      const createdAt = new Date().toISOString();
      const core = {
        schema_version: 1,
        activity_id: requested,
        rpe: args.rpe ?? null,
        conditions: args.conditions ?? null,
        note: args.note ?? null,
        workout_correction: args.workout_correction ?? null,
        source: "user",
        created_at: createdAt,
      };
      const contextId = `ctx-${await contentId(core)}`;
      const context = { ...core, context_id: contextId };
      const stamp = createdAt.replace(/[-:.Z]/g, "");
      const key = `${activityContextPrefix(activity)}${stamp}-${contextId}.json`;
      await this.reserveCoachWrite(ACTIVITY_CONTEXT_WRITE_TTL_MS);
      await this.putSmallJson(key, context);
      return this.text({ saved: true, activity_id: requested, context_id: contextId,
        key, context, message: "The append-only context was saved. A later coach-input run will create a new analysis revision without overwriting the old one." });
    });
    }

    server.registerTool("coach_input", {
      title: "Read coach input",
      description: "Read the newest deterministic coach-input analysis for one running activity. It includes the profile version, HR zones, drift, kilometre splits, Garmin workout plan and actual executed laps, plus explicitly supplied user context.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/),
      }),
      outputSchema: outputSchemas.coach_input,
      annotations: { readOnlyHint: true, destructiveHint: false,
        idempotentHint: true, openWorldHint: false },
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const activity = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!activity) return this.text({ available: false, activity_id: args.activity_id,
        message: "The activity ID was not found in the Slipstream activity index." });
      const prefix = coachInputPrefix(activity);
      const listed = prefix ? await this.env.SLIPSTREAM_DATA.list({ prefix, limit: 1000 }) : null;
      const latest = listed?.objects.sort((a, b) =>
        b.uploaded.getTime() - a.uploaded.getTime())[0];
      const stored = latest ? await this.getR2Json([latest.key]) : null;
      if (!stored || !stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        return this.text({ available: false, activity: toSummary(activity),
          message: "No coach input is stored yet. Add a coach profile and let the R2-only coach backfill run." });
      }
      return this.text({ available: true, activity: toSummary(activity),
        analysis: stored.data as Record<string, unknown>, message: "Coach input is available." });
    });

    if (this.env.GITHUB_ACTIONS_TOKEN && this.env.GITHUB_REPOSITORY) {
      server.registerTool("refresh_today", {
      title: "Refresh today's Garmin data",
      description: "Request one safe incremental Garmin refresh, including recent detailed health and activity artifacts plus coach input for refreshed running activities. This changes stored data and must only be called when the user explicitly asks to update or refresh their data. It cannot start a historical backfill. IMPORTANT: do not give the user a final response while should_continue_polling is true. Call refresh_status with the returned run ID in the same conversation turn until terminal is true, so the user does not need to ask again.",
      inputSchema: z.object({}),
      outputSchema: outputSchemas.refresh_today,
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
    }, async () => {
      const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
      const lease = await coordinator.reserve("refresh-today", Date.now(), REFRESH_LEASE_TTL_MS);
      if (!lease.acquired || !lease.token) {
        return this.text({
          accepted: false,
          reason: "request_in_progress",
          message: "Another refresh request is already being checked or dispatched.",
          retry_after_seconds: Math.max(1, Math.ceil((lease.retryAfterMs ?? 1000) / 1000)),
          ...refreshControl(null),
        });
      }
      try {
        const config = this.refreshConfig();
        const latest = await latestRefreshRun(config);
        const decision = refreshDecision(latest, Date.now(), config.cooldownMinutes);
        if (!decision.dispatch) {
          await coordinator.release("refresh-today", lease.token);
          const run = decision.reason === "already_running"
            ? await pollRefreshRun(config, {
              runId: decision.run.id,
              maxPolls: REFRESH_POLL_ATTEMPTS,
              intervalMs: REFRESH_POLL_INTERVAL_MS,
            })
            : decision.run;
          clearSummaryCachesWhenReady(run);
          const control = refreshControl(run);
          const message = control.should_continue_polling
            ? "A Garmin refresh is still queued or running. Call refresh_status now in this same turn; do not ask the user to send another prompt."
            : control.data_ready
              ? decision.reason === "recent_success"
                ? `A successful refresh was already completed within the last ${config.cooldownMinutes} minutes; the R2 data and applicable coach input are ready.`
                : "The existing Garmin refresh completed successfully and the updated R2 data and applicable coach input are ready."
              : `The existing Garmin refresh completed with conclusion ${run?.conclusion ?? "unknown"}; updated data is not ready.`;
          return this.text({
            accepted: false,
            reason: decision.reason,
            message,
            run,
            ...control,
          });
        }
        const dispatched = await dispatchRefresh(config);
        const run = await pollRefreshRun(config, {
          runId: dispatched?.id,
          excludeRunId: dispatched ? undefined : latest?.id,
          maxPolls: REFRESH_POLL_ATTEMPTS,
          intervalMs: REFRESH_POLL_INTERVAL_MS,
        });
        clearSummaryCachesWhenReady(run);
        const control = refreshControl(run);
        // Keep the short lease until expiry so GitHub has time to expose the new run.
        return this.text({
          accepted: true,
          message: control.data_ready
            ? "The Garmin refresh completed successfully and the updated R2 data and applicable coach input are ready."
            : "The Garmin refresh was accepted and is still running. Call refresh_status now in this same turn; do not ask the user to send another prompt.",
          run,
          ...control,
        });
      } catch (error) {
        await coordinator.release("refresh-today", lease.token);
        console.error(JSON.stringify({
          message: "Could not request Garmin refresh",
          error: error instanceof Error ? error.message : String(error),
        }));
        return {
          ...this.text({
            accepted: false,
            message: error instanceof Error ? error.message : "Could not request the Garmin refresh.",
            terminal: true,
            data_ready: false,
            should_continue_polling: false,
          }),
          isError: true,
        };
      }
    });

      server.registerTool("refresh_status", {
      title: "Check Garmin refresh status",
      description: "Wait briefly for a Garmin refresh and report its status without starting a new job. Pass the run ID returned by refresh_today. If should_continue_polling remains true, call this tool again in the same conversation turn instead of answering the user or asking them for another prompt. Stop when terminal is true.",
      inputSchema: z.object({
        run_id: z.number().int().positive().optional()
          .describe("GitHub Actions run ID returned by refresh_today; omit only for a general latest-status check"),
      }),
      outputSchema: outputSchemas.refresh_status,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
    }, async (args) => {
      const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
      const statusKey = `refresh-status:${this.actorKey}:${args.run_id ?? "latest"}`;
      const lease = await coordinator.reserve(
        statusKey,
        Date.now(),
        REFRESH_STATUS_LEASE_TTL_MS,
      );
      if (!lease.acquired) {
        return this.text({
          available: false,
          message: "Another status check for this refresh is already in progress.",
          terminal: false,
          data_ready: false,
          should_continue_polling: true,
          poll_after_seconds: Math.max(
            1,
            Math.ceil((lease.retryAfterMs ?? REFRESH_POLL_INTERVAL_MS) / 1000),
          ),
        });
      }
      try {
        const run = await pollRefreshRun(this.refreshConfig(), {
          runId: args.run_id,
          maxPolls: REFRESH_POLL_ATTEMPTS,
          intervalMs: REFRESH_POLL_INTERVAL_MS,
        });
        clearSummaryCachesWhenReady(run);
        const control = refreshControl(run);
        return this.text({
          available: run !== null,
          message: !run
            ? "No Garmin refresh run was found yet. Call refresh_status again in this same turn."
            : control.data_ready
              ? "The Garmin refresh completed successfully and the updated R2 data and applicable coach input are ready."
              : control.terminal
                ? `The Garmin refresh completed with conclusion ${run.conclusion ?? "unknown"}; updated data is not ready.`
                : "The Garmin refresh is still queued or running. Call refresh_status again in this same turn; do not ask the user to send another prompt.",
          run,
          ...control,
        });
      } catch (error) {
        console.error(JSON.stringify({
          message: "Could not read Garmin refresh status",
          error: error instanceof Error ? error.message : String(error),
        }));
        return {
          ...this.text({
            available: false,
            message: error instanceof Error ? error.message : "Could not read Garmin refresh status.",
            terminal: true,
            data_ready: false,
            should_continue_polling: false,
          }),
          isError: true,
        };
      }
    });
    }
  }
}

function createServer(env: Env, actorKey: string): McpServer {
  const server = new McpServer({ name: "slipstream-fitness", version: "1.0.0" });
  new FitnessService(env, actorKey).registerTools(server);
  return server;
}

// Cloudflare Access performs the OAuth flow at the edge. The Worker still
// validates the signed Access assertion, issuer and application audience.
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const authentication = await authenticateMcpRequest(request, env);
    if (authentication instanceof Response) return authentication;

    const rateLimit = await env.MCP_RATE_LIMITER.limit({ key: authentication.rateLimitKey });
    if (!rateLimit.success) {
      return secureResponse(new Response("Too many requests", {
        status: 429,
        headers: { "retry-after": "60" },
      }));
    }

    let boundedRequest: Request;
    try {
      boundedRequest = await limitRequestBody(request, REQUEST_BODY_LIMIT_BYTES);
    } catch (error) {
      if (error instanceof PayloadTooLargeError) {
        return secureResponse(new Response("Request body too large", { status: 413 }));
      }
      throw error;
    }

    const actorKey = await contentId(authentication.rateLimitKey);
    const handler = createMcpHandler(() => createServer(env, actorKey), {
      route: url.pathname,
      legacy: "stateless",
      allowedHostnames: [authentication.hostname],
      onerror(error) {
        console.error(JSON.stringify({
          message: "MCP transport error",
          error: error.message,
        }));
      },
    });
    const response = await handler(boundedRequest, env, ctx);
    return secureResponse(response);
  },
} satisfies ExportedHandler<Env>;
