import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { hrvObjectKeys, sleepObjectKeys } from "./lib";
import {
  buildHrvHistory, buildSleepHistory, historyMonthKeys, historyReadThroughDates,
  historyRowsEquivalent, indexedHistoryRows, indexedHistorySourceRevisions,
  resolveHistoryRequest, summarizeHrvPayload, summarizeSleepPayload,
} from "./health-history";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { structuredToolResult, outputSchemas } from "./mcp-output";
import { R2Storage, HISTORY_INDEX_R2_LIMITS } from "./r2-storage";
import { exactDate, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";

/** Read-only night tools; each request owns its reader and environment. */
export class SleepHrvTools {
  constructor(private readonly env: Env, private readonly storage: R2Storage) {}
  private getR2Text(...args: Parameters<R2Storage["getR2Text"]>) {
    return this.storage.getR2Text(...args);
  }
  private getR2Json(...args: Parameters<R2Storage["getR2Json"]>) {
    return this.storage.getR2Json(...args);
  }
  private text<T extends Record<string, unknown>>(obj: T) { return structuredToolResult(obj); }

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

  /** Reuse indexed nights for daily health, with bounded read-through for three recent dates. */
  async nightRowsForHealth(
    stream: "hrv" | "sleep", dates: string[],
  ): Promise<Map<string, Record<string, unknown>>> {
    if (!dates.length) return new Map();
    // At most 13 index reads and three LIST/GET pairs per stream. This also
    // bounds requests when a sparse 366-row health query spans many years.
    const allowedMonths = new Set([...new Set(dates.map((day) => day.slice(0, 7)))].slice(0, 13));
    const selectedDates = dates.filter((day) => allowedMonths.has(day.slice(0, 7)));
    const indexes: unknown[] = [];
    for (const key of historyMonthKeys(stream, selectedDates)) {
      try {
        const stored = await this.getR2Json([key], HISTORY_INDEX_R2_LIMITS);
        if (stored) indexes.push(stored.data);
      } catch {
        // A bad index must not make the ordinary daily summary unavailable.
      }
    }
    const rows = indexedHistoryRows(stream, indexes, selectedDates);
    const revisions = indexedHistorySourceRevisions(stream, indexes);
    const recent = [...new Set(selectedDates)].sort().slice(-3);
    const discovered = await this.discoverCanonicalHistoryKeys(stream, recent);
    for (const day of recent) {
      const source = discovered.keys.get(day);
      const result = await this.readCanonicalHistoryRow(
        stream, day, rows.get(day), false, source,
        source ? revisions.get(source.key) : undefined,
      );
      rows.set(day, result.row);
    }
    return rows;
  }

  registerTools(server: McpServer) {
    server.registerTool("hrv_curve", {
      description: "Read detailed overnight Garmin HRV for one wake-date. Local night_of prefers Garmin's own timestamps and otherwise uses configured HEALTH_TIMEZONE, without GPS or raw device payloads.",
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
        ...nightContext(typeof payload.date === "string" ? payload.date : args.date, payload.sleep_start_gmt,
          payload.sleep_end_gmt, validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
          payload.sleep_start_garmin_local, payload.sleep_end_garmin_local),
        sleep_start_gmt: payload.sleep_start_gmt ?? null,
        sleep_end_gmt: payload.sleep_end_gmt ?? null,
        summary: payload.summary ?? null,
        reading_count: readings.length,
        readings,
      });
    });

    server.registerTool("hrv_history", {
      description: "Analyze overnight HRV by morning wake-date. Daily rows use matching sleep's Garmin local night when HRV lacks its own timestamps, with night_context_stream showing provenance. Auto returns daily summaries for up to 31 days and compact wake-date weekly summaries for longer ranges (up to 366 days); use daily chunks for night-lag analysis. Full readings are limited to 7 days.",
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
      const matchingSleep = new Map<string, Record<string, unknown>>();
      let sleepContextObjectsRead = 0;
      if (request.granularity === "daily") {
        for (const day of request.dates) {
          const row = overrides.get(day) ?? indexedRows.get(day);
          if (row?.status !== "available") continue;
          const own = nightContext(day, row.sleep_start_gmt, row.sleep_end_gmt,
            validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
            row.sleep_start_garmin_local, row.sleep_end_garmin_local);
          if (own.local_time_source === "garmin_local") continue;
          try {
            const stored = await this.getR2Json(sleepObjectKeys(day));
            if (!stored) continue;
            sleepContextObjectsRead += 1;
            matchingSleep.set(day, summarizeSleepPayload(day, stored.data));
          } catch {
            // An invalid matching sleep object must not hide available HRV.
          }
        }
      }
      const history = buildHrvHistory(
        indexes, request.dates, request.granularity, overrides,
        validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
        matchingSleep,
      );
      return this.text({
        start_date: args.start_date,
        end_date: args.end_date,
        timezone: validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
        granularity: request.granularity,
        detail_level: args.detail_level,
        ...history,
        source_objects_read: keys.length + canonicalObjectsRead + sleepContextObjectsRead,
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
      description: "Read detailed Garmin sleep for one morning wake-date. Local night_of is the sleep-start date, using Garmin's local timestamps when available and HEALTH_TIMEZONE only as fallback. Includes window, stages and score without raw Garmin payload.",
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
        ...nightContext(typeof payload.date === "string" ? payload.date : args.date,
          payload.sleep_start_gmt, payload.sleep_end_gmt,
          validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
          payload.sleep_start_garmin_local, payload.sleep_end_garmin_local),
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
      description: "Analyze sleep by morning wake-date. Daily rows expose local night_of and weekly output includes by_night_of_weekday for Friday/Saturday comparisons and a midpoint-shift proxy. Auto uses daily summaries up to 31 days and compact wake-date weekly summaries up to 366 days; full stages are limited to 7 days.",
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
        validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
      );
      return this.text({
        start_date: args.start_date,
        end_date: args.end_date,
        timezone: validatedHealthTimezone(this.env.HEALTH_TIMEZONE),
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

  }
}
