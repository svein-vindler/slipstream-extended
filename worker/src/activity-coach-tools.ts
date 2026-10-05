import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import {
  Activity, filterActs, activityFilterWarning, summarize, toSummary, bucketKey,
  rawGarminActivityId, activityJsonObjectKeys, activityEnduranceObjectKeys,
  coachInputPrefix, activityContextPrefix,
} from "./lib";
import { structuredToolResult, outputSchemas } from "./mcp-output";
import { R2Storage, COACH_CONFIG_R2_LIMITS } from "./r2-storage";
import { exactDate, dateRange, sport, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";

const COACH_PROFILE_PREFIX = "coach/profiles/v1/";
const COACH_PROFILE_INDEX_KEY = "coach/indexes/profiles-v1.json";
const COACH_PROFILE_WRITE_TTL_MS = 60_000;
const ACTIVITY_CONTEXT_WRITE_TTL_MS = 10_000;
const DEFAULT_MCP_WRITE_DAILY_LIMIT = 60;

async function contentId(value: unknown): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)));
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("").slice(0, 24);
}

/** Activity reads and bounded, opt-in append-only coach writes. */
export class ActivityCoachTools {
  constructor(private readonly env: Env, private readonly actorKey: string,
    private readonly storage: R2Storage) {}
  private getActivities() { return this.storage.getActivities(); }
  private getR2Json(...args: Parameters<R2Storage["getR2Json"]>) {
    return this.storage.getR2Json(...args);
  }
  private text<T extends Record<string, unknown>>(obj: T) { return structuredToolResult(obj); }

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

  registerSummaryTools(server: McpServer) {
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
        by_source: srcs, storage: this.storage.activityStorage,
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
      const warning = activityFilterWarning(rows.length, args);
      return this.text({ matched: rows.length, showing: Math.min(args.limit, rows.length),
        ...(warning ? { filter_warning: warning } : {}),
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
      const warning = activityFilterWarning(rows.length, args);
      const result: Record<string, unknown> = {
        overall: { ...summarize(rows), ...(warning ? { filter_warning: warning } : {}) },
      };
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

  }

  registerDetailTools(server: McpServer) {
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

  }
}
