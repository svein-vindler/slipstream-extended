import { COACH_PROFILE_CAPACITY } from "./coach-profile-capacity";
import { readCurrentCoachInput } from "./coach-selection";
import { coachPresentation } from "./coach-presentation";
import { decouplingInterpretation } from "./coach-tables";
import { activityCalendarYears, resolveActivityClock } from "./activity-clock";
import { activityContextSchema, expectedGctSides, gctSideInput } from "./gct-sides";
import { ApplicationToolError, applicationToolResult } from "./application-errors";
import { McpServer } from "@modelcontextprotocol/server";
import { errorCategory } from "./output-privacy";

import { z } from "zod";
import {
  Activity, filterActs, activityFilterWarning, summarize, toSummary, bucketKey,
  rawGarminActivityId, activityJsonObjectKeys, activityEnduranceObjectKeys,
  coachInputPrefix, activityContextPrefix, activityCalendarDay, validCalendarDay,
} from "./lib";
import { structuredToolResult, outputSchemas } from "./mcp-output";
import { R2Storage, COACH_CONFIG_R2_LIMITS } from "./r2-storage";
import { exactDate, dateRange, sport, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";
import { trainingContext } from "./training-context";
import { recordedRunningMetrics } from "./recorded-running-metrics";
import { recordedRunningSegmentsSchema, storedRunningMeasurements } from "./recorded-running-segments";
import { strengthSessionSchema, enduranceSessionSchema } from "./detail-projection";

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

  private async previousContext(id: string, day: string) {
    const objects: R2Object[] = [];
    for (const year of activityCalendarYears(day)) {
      const prefix = `activities/${year}/${id}/context/v1/`;
      // Match the existing FreshData activity-context lookup's bounded limit.
      const listed = await this.env.SLIPSTREAM_DATA.list({ prefix, limit: 100 });
      if (listed.truncated) throw new Error("Activity context exceeds its bounded limit.");
      objects.push(...listed.objects.filter(item => item.key.startsWith(prefix)
        && !item.key.slice(prefix.length).includes("/") && item.key.endsWith(".json")));
    }
    const timestamp = (key: string) => key.slice(key.lastIndexOf("/") + 1).split("-")[0];
    objects.sort((a, b) => timestamp(a.key) < timestamp(b.key) ? 1 : timestamp(a.key) > timestamp(b.key) ? -1 : 0);
    if (objects.length > 1 && timestamp(objects[0].key) === timestamp(objects[1].key)) {
      throw new Error("Activity context selection is ambiguous.");
    }
    if (!objects.length) return null;
    const stored = await this.getR2Json([objects[0].key], COACH_CONFIG_R2_LIMITS);
    const parsed = activityContextSchema.safeParse(stored?.data);
    if (!parsed.success || parsed.data.activity_id !== id) throw new Error("Stored activity context is unverified.");
    return parsed.data;
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

  /** One bounded read path shared by normal reads and successful context receipts. */
  private async readCoachView(activity: Activity, requested: string, format?: "coach" | "full") {
    const prefix = coachInputPrefix(activity);
    const selection = await readCurrentCoachInput({
      canonicalPrefix: prefix, activityId: requested,
      ...(activity.localDate ? { date: activity.localDate } : {}),
      read: (keys, limits) => this.storage.getR2JsonWithMetadata(keys, limits),
      versions: async () => {
        const listed = await this.env.SLIPSTREAM_DATA.list({ prefix, limit: 1000 });
        if (listed.truncated) throw new Error("Coach analysis versions exceed the bounded legacy limit.");
        return listed.objects;
      },
    });
    if (selection.status !== "available") {
      return { available: false, activity: toSummary(activity),
        message: selection.status === "absent"
          ? "No coach input is stored yet. Add a coach profile and let the R2-only coach backfill run."
          : "The current coach input could not be verified. Retry this read later; if it persists, ask the maintainer to check stored analysis compatibility." };
    }
    // User context has a different lifetime from immutable analysis/profile
    // snapshots. A failed current read must never be reported as not_stored.
    let context: Awaited<ReturnType<ActivityCoachTools["previousContext"]>> = null;
    let contextStatus: "available" | "not_stored" | "unavailable" = "unavailable";
    const day = validCalendarDay(activityCalendarDay(activity));
    if (day) {
      try {
        context = await this.previousContext(requested, day);
        contextStatus = context ? "available" : "not_stored";
      } catch { /* Keep the selected analysis readable, with explicit uncertainty. */ }
    }
    const view = { schema_version: 1 as const, analysis_id: String(selection.analysis.analysis_id),
      analyzer_version: String(selection.analysis.analyzer_version), context_status: contextStatus,
      context_revision: context?.context_id ?? null, context_updated_at: context?.created_at ?? null,
      resolved_at: new Date().toISOString(), interpretation: decouplingInterpretation(selection.analysis) };
    const metrics = recordedRunningSegmentsSchema.safeParse(selection.analysis.recorded_running_metrics);
    const groundContactSides = metrics.success ? expectedGctSides(metrics.data, context,
      selection.analysis.segment_running_metrics, { currentView: true, contextUnavailable: contextStatus === "unavailable",
        recovery: selection.analysis.recovery_running_metrics }) : null;
    return { available: true, activity: toSummary(activity), view, user_context: context,
      ground_contact_sides: groundContactSides,
      ...(format === "coach" ? { presentation: coachPresentation(selection.analysis,
        { status: contextStatus, value: context, groundContactSides }) } : { analysis: selection.analysis }),
      message: activity.localDate ? "Coach input is available."
        : "Coach input is available. The indexed date uses a UTC fallback; its historical local-calendar association is unconfirmed." };
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
      limit: COACH_PROFILE_CAPACITY + 1,
    });
    if (listed.truncated || listed.objects.length > COACH_PROFILE_CAPACITY) {
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

  async reserveCoachWrite(ttlMs: number, profile?: { key: string; keys: string[] }): Promise<void> {
    const configured = Number.parseInt(this.env.MCP_WRITE_DAILY_LIMIT, 10);
    const dailyLimit = Number.isSafeInteger(configured) && configured > 0
      ? Math.min(configured, 1000)
      : DEFAULT_MCP_WRITE_DAILY_LIMIT;
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const key = `mcp-write:${this.actorKey}`;
    const lease = profile
      ? await coordinator.reserveCoachProfileWrite(profile.key, profile.keys, this.actorKey, Date.now(), dailyLimit)
      : await coordinator.reserveBudgeted(key, key, Date.now(), ttlMs, dailyLimit);
    if (!lease.acquired) {
      if (lease.reason === "profile_capacity" || lease.reason === "profile_in_progress") {
        throw new ApplicationToolError({ kind: lease.reason });
      }
      const retrySeconds = Math.max(
        1, Math.ceil((lease.retryAfterMs ?? ttlMs) / 1000),
      );
      if (lease.reason === "daily_limit") {
        throw new ApplicationToolError({ kind: "write_daily_limit", retrySeconds });
      }
      throw new ApplicationToolError({ kind: "write_cooldown", retrySeconds });
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
      const dates = a.map(activityCalendarDay).filter((day): day is string => day != null).sort();
      const srcs: Record<string, number> = {};
      a.forEach((x) => { srcs[x.source] = (srcs[x.source] ?? 0) + 1; });
      return this.text({
        connected: true, count: a.length,
        earliest: dates[0] ?? null,
        latest: dates.at(-1) ?? null,
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
      description: "Totals (distance, time, elevation, calories) require a valid recorded value for every matched activity; a missing or invalid field makes that total null. Recorded zero stays zero. Empty results have activities=0 and zero totals. Average HR uses known values. Optionally group_by sport/month/year.",
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
      description: "Read normalized sets for one Garmin strength activity: exercise, reps, weight, active time and following rest. Includes optional recorded aerobic/anaerobic Training Effect estimates from verified stored FIT sessions. Raw FIT messages and GPS are not returned.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/)
          .describe('Activity ID from list_activities, for example "garmin-24431147581"'),
      }),
      outputSchema: outputSchemas.strength_session,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const indexed = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!indexed) {
        return this.text({
          available: false,
          activity_id: args.activity_id,
          message: "The activity ID was not found in the Slipstream activity index.",
        });
      }
      const resolved = await resolveActivityClock(indexed, keys => this.getR2Json(keys));
      const activity = resolved.activity;
      const stored = indexed.localDate ? await this.getR2Json(activityJsonObjectKeys(activity)) : resolved.stored;
      if (!stored) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "No granular export is stored for this activity yet.",
          training_context: trainingContext(activity, null),
        });
      }
      if (!stored.data || typeof stored.data !== "object" || Array.isArray(stored.data)) {
        throw new Error("Unsupported stored activity shape.");
      }
      const payload = stored.data as Record<string, unknown>;
      const session = payload.normalized_strength_session;
      if (session === undefined || session === null) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "The stored activity predates the normalized strength schema; rerun the granular export.",
          training_context: trainingContext(activity, payload),
        });
      }
      return this.text({ available: true, activity: toSummary(activity), session: strengthSessionSchema.parse(session),
        training_context: trainingContext(activity, payload) });
    });

    server.registerTool("endurance_session", {
      description: "Read a GPS-free analysis dataset derived from the Garmin TCX file for one endurance activity. Returns summary metrics, Garmin laps, kilometre splits, distance-half heart-rate drift, seconds per heart-rate BPM, and a compact 10-second trackpoint series. Includes optional recorded aerobic/anaerobic Training Effect estimates and recorded running measurements. New stored datasets also include raw ground-contact balance with unknown side semantics, recorded lap measurements, and producer-computed distance-split/half measurements with coverage. Existing datasets retain their five-field fallback. No new FIT fetch or segment computation is performed here, and missing or unverified optional fields do not change dataset availability.",
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
      const indexed = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!indexed) {
        return this.text({
          available: false,
          activity_id: args.activity_id,
          message: "The activity ID was not found in the Slipstream activity index.",
        });
      }
      let activity = indexed;
      let context;
      let decodedFit: unknown;
      let fitReadFailure: "source_missing" | "source_unreadable" = "source_missing";
      try {
        const resolved = await resolveActivityClock(indexed, keys => this.getR2Json(keys));
        activity = resolved.activity;
        const fit = indexed.localDate ? await this.getR2Json(activityJsonObjectKeys(activity)) : resolved.stored;
        decodedFit = fit?.data;
        context = trainingContext(activity, fit?.data);
      } catch {
        fitReadFailure = "source_unreadable";
        context = trainingContext(activity, null, "source_unreadable");
      }
      const stored = await this.getR2Json(activityEnduranceObjectKeys(activity));
      if (!stored) {
        return this.text({
          available: false,
          activity: toSummary(activity),
          message: "No normalized TCX analysis is stored for this activity yet. Run the activity backfill or recent granular export.",
        });
      }
      return this.text({
        available: true,
        activity: toSummary(activity),
        session: enduranceSessionSchema.parse(stored.data),
        training_context: context,
        recorded_running_metrics: storedRunningMeasurements(
          recordedRunningMetrics(activity, decodedFit, stored.data, fitReadFailure), stored.data),
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
      const profiles = storedProfiles.profiles.sort((a, b) => {
        // Match Python's max(effective_from, created_at). Reverse the ID tie-break
        // so the first canonical key wins when selection takes the last entry.
        for (const [left, right] of [
          [String(a.effective_from ?? ""), String(b.effective_from ?? "")],
          [String(a.created_at || ""), String(b.created_at || "")],
          [String(b.profile_id ?? ""), String(a.profile_id ?? "")],
        ]) {
          if (left !== right) return left < right ? -1 : 1;
        }
        return 0;
      });
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
    }, async (args, mcpContext) => applicationToolResult(server, mcpContext, async () => {
      if (!validCalendarDay(args.effective_from)) {
        throw new ApplicationToolError({ kind: "calendar_date" });
      }
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
        if (current.keys.length >= COACH_PROFILE_CAPACITY) {
          throw new ApplicationToolError({ kind: "profile_capacity" });
        }
        await this.reserveCoachWrite(COACH_PROFILE_WRITE_TTL_MS, { key, keys: current.keys });
        await this.putSmallJson(key, profile);
        try {
          await this.updateCoachProfileIndex(
            [...current.keys, key],
            [...current.profiles, profile],
          );
        } catch (error) {
          console.error(JSON.stringify({
            message: "Coach profile was saved but its read index could not be updated",
            error: errorCategory(error),
          }));
        }
      }
      const savedProfile = existing?.data && typeof existing.data === "object"
        && !Array.isArray(existing.data) ? existing.data as Record<string, unknown> : profile;
      return this.text({ saved: true, profile_id: profileId, key, profile: savedProfile,
        message: existing
          ? "This identical immutable profile was already stored."
          : "The immutable profile was saved. The R2-only coach backfill will use it for activities on or after its effective date." });
    }));

      server.registerTool("add_activity_context", {
      title: "Add user context to an activity",
      description: "Append explicit user context to one activity. gct_balance_side declares the raw session/recorded-lap percentage's side for this exact stored FIT digest, not a device-wide convention. Call only on explicit user request. When the user provides context while asking about a workout, save it with this tool first; the receipt includes the updated Coach Input. Omitted fields are retained; explicit null clears an ordinary context field. Historical contexts and analyses are never overwritten or regenerated by this tool. If saved is true but the included view is unavailable, retry only coach_input, not the saved write.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/),
        rpe: z.number().min(0).max(10).nullable().optional(),
        conditions: z.string().trim().min(1).max(240).nullable().optional(),
        note: z.string().trim().min(1).max(2000).nullable().optional(),
        workout_correction: z.string().trim().min(1).max(1000).nullable().optional(),
        gct_balance_side: gctSideInput.optional(),
      }).refine((value) => value.rpe !== undefined || value.conditions !== undefined
        || value.note !== undefined || value.workout_correction !== undefined || value.gct_balance_side != null,
      { message: "At least one context field is required." }),
      outputSchema: outputSchemas.add_activity_context,
      annotations: { readOnlyHint: false, destructiveHint: false,
        idempotentHint: false, openWorldHint: false },
    }, async (args, mcpContext) => applicationToolResult(server, mcpContext, async () => {
      const requested = rawGarminActivityId(args.activity_id);
      const indexed = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!indexed || !indexed.date) throw new Error("The activity ID was not found.");
      const resolved = await resolveActivityClock(indexed, keys => this.getR2Json(keys));
      const { activity } = resolved;
      const activityDate = validCalendarDay(activityCalendarDay(activity));
      if (!activityDate) {
        throw new ApplicationToolError({ kind: "calendar_date" });
      }
      if (args.gct_balance_side) {
        const stored = resolved.stored ?? await this.getR2Json(activityJsonObjectKeys(activity));
        const source = recordedRunningMetrics(activity, stored?.data, {});
        if (source.association !== "matched" || source.source_fit_sha256 !== args.gct_balance_side.source_fit_sha256) {
          throw new ApplicationToolError({ kind: "gct_declaration" });
        }
      }
      // Claim the activity before reading its snapshot, independently of the
      // actor cooldown. Keep the claim forever if the PUT result is unknown.
      const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
      const claim = await coordinator.reserveActivityContextWrite(requested);
      if (!claim.acquired) throw new ApplicationToolError({ kind: "context_in_progress" });
      const contextToken = claim.token;
      if (!contextToken) throw new Error("Context reservation was not confirmed.");
      let attempted = false, key: string, context: Record<string, unknown>, contextId: string;
      try {
        const previous = await this.previousContext(requested, activityDate);
        const createdAt = new Date(Math.max(Date.now(), previous ? Date.parse(previous.created_at) + 1 : 0)).toISOString();
        const side = args.gct_balance_side
          ? { ...args.gct_balance_side, source: "user_activity_declaration" as const }
          : previous?.schema_version === 2 ? previous.gct_balance_side : undefined;
        const keep = <T>(supplied: T | undefined, prior: T | undefined): T | null =>
          supplied !== undefined ? supplied : prior ?? null;
        const core = {
          schema_version: side ? 2 : 1,
          activity_id: requested,
          activity_date: activityDate,
          date_source: activity.localDate ? "garmin_local" : "utc_fallback",
          rpe: keep(args.rpe, previous?.rpe),
          conditions: keep(args.conditions, previous?.conditions),
          note: keep(args.note, previous?.note),
          workout_correction: keep(args.workout_correction, previous?.workout_correction),
          ...(side ? { gct_balance_side: side } : {}),
          source: "user",
          created_at: createdAt,
        };
        contextId = `ctx-${await contentId(core)}`;
        context = { ...core, context_id: contextId };
        const stamp = createdAt.replace(/[-:.Z]/g, "");
        key = `${activityContextPrefix(activity)}${stamp}-${contextId}.json`;
        await this.reserveCoachWrite(ACTIVITY_CONTEXT_WRITE_TTL_MS);
        attempted = true;
        await this.putSmallJson(key, context);
      } catch (error) {
        // Before PUT no append can arrive late; release only this exact owned
        // claim. The accepted-attempt budget is never refunded.
        if (!attempted) await coordinator.finishActivityContextWrite(requested, contextToken);
        throw error;
      }
      try {
        const completed = await coordinator.finishActivityContextWrite(requested, contextToken);
        if (!completed.released) throw new Error("Context completion was not confirmed.");
      } catch {
        // The PUT already succeeded. Do not misreport it as an unsaved append
        // or retry acknowledgement; an unresolved durable claim stays closed.
        console.error(JSON.stringify({ message: "Saved context completion was not confirmed" }));
      }
      let coachInput: Record<string, unknown>;
      try { coachInput = await this.readCoachView(activity, requested, "coach"); }
      catch {
        // The append already succeeded. A read failure must not turn that into
        // a failed write receipt that encourages another append/reservation.
        coachInput = { available: false, activity_id: requested,
          message: "The context is saved, but its Coach Input view is unavailable. Retry only the read, not the saved context write." };
      }
      return this.text({ saved: true, activity_id: requested, context_id: contextId,
        key, context, coach_input: coachInput,
        message: "The append-only effective context was saved. The included Coach Input view uses current context without regenerating or overwriting the selected analysis." });
    }));
    }

    server.registerTool("coach_input", {
      title: "Read coach input",
      description: "Read the selected verified immutable analysis together with the current effective user context for one running activity. If the user supplies RPE, conditions, notes or corrections, call add_activity_context first and use its updated Coach Input receipt. format=coach returns neutral coherent kilometer and logical work/recovery tables, units, coverage and decoupling interpretation guards; format=full or omitted returns the original complete analysis plus separately versioned current-view metadata. Several verified Auto Laps can form one logical rep; planned completion is never guessed. Raw GCT balance remains available with unknown side semantics; a current exact activity/date/FIT declaration projects recorded and derived sides. Missing measurements are not zero, and unreadable context is unavailable rather than not_stored. Profiles, zones, frozen health snapshots and analysis history remain unchanged. This read performs no Garmin request, regeneration or storage write.",
      inputSchema: z.object({
        activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/),
        format: z.enum(["coach", "full"]).optional()
          .describe("Use coach for a neutral selected-fields presentation; full or omitted preserves the complete analysis."),
      }),
      outputSchema: outputSchemas.coach_input,
      annotations: { readOnlyHint: true, destructiveHint: false,
        idempotentHint: true, openWorldHint: false },
    }, async (args) => {
      const requested = rawGarminActivityId(args.activity_id);
      const indexed = (await this.getActivities()).find(
        (item) => rawGarminActivityId(item.id) === requested,
      );
      if (!indexed) return this.text({ available: false, activity_id: args.activity_id,
        message: "The activity ID was not found in the Slipstream activity index." });
      const { activity } = await resolveActivityClock(indexed, keys => this.getR2Json(keys));
      return this.text(await this.readCoachView(activity, requested, args.format));
    });

  }
}
