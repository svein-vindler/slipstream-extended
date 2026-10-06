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
 *   HEALTH_TIMEZONE       optional IANA zone for local sleep-night context
 *
 * Pure data helpers live in ./lib; bounded reads and summary reuse live in
 * ./r2-storage. This file wires those services to MCP tools.
 */
import { createMcpHandler } from "agents/mcp/server";
import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import {
  Activity, HealthDay, filterHealth, summarizeHealth, toHealthSummary,
  healthBucketKey, bodyCompositionObjectKeys,
} from "./lib";
import { SleepHrvTools } from "./sleep-hrv-tools";
import { ActivityCoachTools } from "./activity-coach-tools";
import { exactDate, dateRange, PRIVATE_READ_TOOL_ANNOTATIONS } from "./tool-contracts";
import {
  RefreshConfig, RefreshRun, dispatchRefresh,
  latestRefreshRun, pollRefreshRun,
  refreshDecision, refreshProgress,
} from "./github";
import { RefreshCoordinator } from "./refresh-coordinator";
import { FreshDataService, requestIdSchema } from "./fresh-data";
import {
  PayloadTooLargeError, limitRequestBody,
  secureResponse,
} from "./security";
import { outputSchemas, structuredToolResult } from "./mcp-output";
import { authenticateMcpRequest } from "./mcp-auth";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { activityReady, refreshReportMessage, refreshReportSchema } from "./refresh-report";
import { latency, syncStatus, latestNightReportSchema, pipelineDiagnosticsSchema, activityRepairReportSchema } from "./sync-status";
import {
  LatestActivityReport, latestActivityReportSchema,
} from "./latest-activity-report";
import {
  resolveWeightRequest,
} from "./weight-history";
import { WeightHistoryReader } from "./weight-history-reader";
import {
  R2Storage, R2ReadLimits, clearSummaryCaches, GRANULAR_R2_LIMITS,
} from "./r2-storage";

export { RefreshCoordinator } from "./refresh-coordinator";

const REQUEST_BODY_LIMIT_BYTES = 256 * 1024;
const REFRESH_LEASE_TTL_MS = 2 * 60 * 1000;
const REFRESH_POLL_ATTEMPTS = 5;
const REFRESH_POLL_INTERVAL_MS = 4_000;
const REFRESH_STATUS_LEASE_TTL_MS = REFRESH_POLL_ATTEMPTS * REFRESH_POLL_INTERVAL_MS + 5_000;
type RefreshDetails = Omit<Awaited<ReturnType<FreshDataService["completedDetails"]>>, "freshness" | "pipeline_diagnostics"> & {
  pipeline_diagnostics?: z.infer<typeof pipelineDiagnosticsSchema> | null;
  freshness?: Awaited<ReturnType<FreshDataService["snapshot"]>>;
  activity_refresh?: import("./refresh-report").RefreshReport;
  latest_activity?: LatestActivityReport;
  latest_night?: z.infer<typeof latestNightReportSchema>;
};

async function contentId(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 24);
}

function refreshControl(run: RefreshRun | null) {
  const progress = refreshProgress(run);
  return {
    terminal: progress.terminal,
    data_ready: progress.dataReady,
    should_continue_polling: progress.shouldContinuePolling,
    sync_status: syncStatus({ kind: "unknown", run, polling: progress.shouldContinuePolling }),
    latency: latency(run),
    ...(progress.shouldContinuePolling ? { poll_after_seconds: 4 } : {}),
  };
}

function clearSummaryCachesWhenReady(run: RefreshRun | null): void {
  if (!refreshProgress(run).dataReady) return;
  clearSummaryCaches();
}

class FitnessService {
  private readonly storage: R2Storage;
  private readonly nights: SleepHrvTools;
  private readonly activities: ActivityCoachTools;

  constructor(
    private readonly env: Env,
    private readonly actorKey: string,
  ) {
    this.storage = new R2Storage(env.SLIPSTREAM_DATA);
    this.nights = new SleepHrvTools(env, this.storage);
    this.activities = new ActivityCoachTools(env, actorKey, this.storage);
  }

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

  private freshData(): FreshDataService {
    return new FreshDataService(this.env, this, this.refreshConfig());
  }

  async getActivities(): Promise<Activity[]> {
    return this.storage.getActivities();
  }

  getCoachProfiles() { return this.activities.getCoachProfiles(); }

  async getHealth(): Promise<HealthDay[]> {
    return this.storage.getHealth();
  }

  async getR2Text(
    keys: string[],
    limits: R2ReadLimits = GRANULAR_R2_LIMITS,
  ): Promise<{ key: string; text: string; etag: string } | null> {
    return this.storage.getR2Text(keys, limits);
  }

  async getR2Json(
    keys: string[],
    limits: R2ReadLimits = GRANULAR_R2_LIMITS,
  ): Promise<{ key: string; data: unknown } | null> {
    return this.storage.getR2Json(keys, limits);
  }

  private async completedRefreshDetails(run: RefreshRun | null): Promise<RefreshDetails> {
    const empty = { message: "", activity_ready: null,
      sync_status: syncStatus({ kind: "unknown", run, polling: run?.status !== "completed" }), latency: latency(run) };
    if (!run || run.status !== "completed") return empty;
    try {
      const stored = await this.getR2Json(
        [`refresh/reports/${run.id}.json`],
        { stored: 64 * 1024, decoded: 64 * 1024 },
      );
      const measured = pipelineDiagnosticsSchema.safeParse((await this.getR2Json(
        [`refresh/diagnostics/v1/${run.id}.json`], { stored: 64 * 1024, decoded: 64 * 1024 },
      ))?.data);
      const pipeline = measured.success ? measured.data : null;
      const night = latestNightReportSchema.safeParse(stored?.data);
      if (night.success) {
        return { ...await this.freshData().completedDetails(run,
          { kind: "night", date: night.data.wake_date }, pipeline, night.data.source_checked),
          latest_night: night.data };
      }
      const parsed = refreshReportSchema.safeParse(stored?.data);
      if (parsed.success) {
        const status = syncStatus({ kind: "general", run, pipeline });
        status.missing_components = [...new Set(parsed.data.activities.flatMap(item => [
          ...(!item.files_ready ? ["activity_files"] : []),
          ...(!["ready", "not_applicable"].includes(item.coach_status) ? ["coach_input"] : []),
        ]))];
        if (status.missing_components.length && status.data_state === "unknown") status.data_state = "partial";
        status.user_action_required = parsed.data.activities.some(item =>
          ["no_effective_profile", "endurance_data_unavailable"].includes(item.coach_status));
        if (status.user_action_required) { status.data_state = "blocked"; status.next_action = "fix_configuration"; }
        return {
          message: `${refreshReportMessage(parsed.data)} General refresh does not verify a specific complete, fresh activity or night; use the matching targeted tool for that package.`,
          activity_ready: activityReady(parsed.data),
          activity_refresh: parsed.data,
          sync_status: status, latency: latency(run, pipeline),
          pipeline_diagnostics: pipeline,
        };
      }
      const latest = latestActivityReportSchema.safeParse(stored?.data);
      if (latest.success) {
        const parts = latest.data.scope?.split("/");
        return {
          ...await this.freshData().completedDetails(run, { kind: "activity",
            date: parts?.[1] === "latest" ? undefined : parts?.[1] ?? latest.data.expected_date ?? undefined,
            activityId: parts?.[2] === "latest" ? undefined : parts?.[2] ?? latest.data.activity_id?.replace("garmin-", ""),
            newExpected: parts ? parts[3] === "new" : false }, pipeline, latest.data.source_checked),
          latest_activity: latest.data,
        };
      }
      // R2-only repair has its own additive report, without a new Garmin check.
      const repair = activityRepairReportSchema.safeParse(stored?.data);
      if (repair.success) return this.freshData().completedDetails(run, { kind: "activity",
        date: repair.data.activity_date, activityId: repair.data.activity_id.replace("garmin-", ""),
        newExpected: false }, pipeline);
      return { ...empty, message: "The job ended. Requested data completeness and freshness could not be verified from this run's report.",
        sync_status: syncStatus({ kind: pipeline?.mode ?? "unknown", run, pipeline }), latency: latency(run, pipeline),
        pipeline_diagnostics: pipeline };
    } catch {
      console.error(JSON.stringify({
        message: "Could not verify bounded refresh report",
        run_id: run.id,
      }));
    }
    return {
      ...empty,
      message: "The job ended, but the refresh report could not be read or validated. Requested data readiness is unknown.",
    };
  }

  private text<T extends Record<string, unknown>>(obj: T) {
    return structuredToolResult(obj);
  }

  registerTools(server: McpServer) {

    this.activities.registerSummaryTools(server);

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
        storage: this.storage.healthStorage,
        note: rows.length
          ? "This status covers daily summaries; detailed streams may be available through dedicated tools."
          : "No health backfill has completed yet.",
      });
    });

    server.registerTool("daily_health", {
      description: "List daily health summaries. weight_kg is one Garmin daily value (normally latestWeight), not a computed daily mean or standardized morning measurement; use weight_history for the latter. For sleep/overnight HRV, date is the morning wake-date; sleep_night and hrv_night prefer Garmin's per-night local timestamps, with HEALTH_TIMEZONE as fallback.",
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
      const displayed = rows.slice(0, args.limit);
      const timezone = validatedHealthTimezone(this.env.HEALTH_TIMEZONE);
      const contextMonths = new Set(displayed.map((row) => row.date.slice(0, 7)));
      const contexts = await Promise.allSettled([
        this.nights.nightRowsForHealth("sleep", displayed.filter((row) => row.sleepSeconds != null)
          .map((row) => row.date)),
        this.nights.nightRowsForHealth("hrv", displayed.filter((row) => row.hrvLastNightAvg != null)
          .map((row) => row.date)),
      ]);
      const emptyContext = () => new Map<string, Record<string, unknown>>();
      const sleepRows = contexts?.[0].status === "fulfilled" ? contexts[0].value : emptyContext();
      const hrvRows = contexts?.[1].status === "fulfilled" ? contexts[1].value : emptyContext();
      return this.text({ matched: rows.length, showing: Math.min(args.limit, rows.length),
        timezone,
        weight_kg_semantics: "garmin_daily_latest_or_summary_fallback",
        night_context_limited: contextMonths.size > 13,
        night_context_unavailable: contexts?.some((result) => result.status === "rejected") ?? false,
        days: displayed.map((row) => {
          const sleep = sleepRows.get(row.date);
          const hrv = hrvRows.get(row.date);
          const hrvSource = hrv?.sleep_start_garmin_local != null ? hrv
            : sleep?.sleep_start_garmin_local != null ? sleep
              : hrv?.sleep_start_gmt != null ? hrv : sleep;
          return {
            ...toHealthSummary(row),
            sleep_night: row.sleepSeconds != null
              ? nightContext(row.date, sleep?.sleep_start_gmt, sleep?.sleep_end_gmt, timezone,
                sleep?.sleep_start_garmin_local, sleep?.sleep_end_garmin_local)
              : null,
            hrv_night: row.hrvLastNightAvg != null
              ? nightContext(row.date, hrvSource?.sleep_start_gmt, hrvSource?.sleep_end_gmt,
                timezone, hrvSource?.sleep_start_garmin_local,
                hrvSource?.sleep_end_garmin_local)
              : null,
          };
        }) });
    });

    server.registerTool("health_trends", {
      description: "Summarize health metrics over a date range, optionally grouped by month or year. Weight statistics use Garmin's one daily value (normally latestWeight), not standardized morning measurements; use weight_history for those.",
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

    this.nights.registerTools(server);

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

    server.registerTool("weight_history", {
      description: "Read up to 31 days of stored body-composition data in one call. Select the first actual weighing in a local morning window (default 04:00-12:00), never a Garmin daily average. Garmin local timestamps take priority; UTC-only records use the configured HEALTH_TIMEZONE or an explicit IANA timezone. Each day reports selection provenance, coverage, missing data and intraday weight range. Use body_composition(date) for all individual measurements on a selected day.",
      inputSchema: z.object({
        start_date: exactDate,
        end_date: exactDate,
        morning_start: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/).default("04:00"),
        morning_end: z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/).default("12:00"),
        timezone: z.string().min(1).max(64).optional(),
      }),
      outputSchema: outputSchemas.weight_history,
      annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
    }, async (args) => {
      const request = resolveWeightRequest(args.start_date, args.end_date,
        args.morning_start, args.morning_end, args.timezone, this.env.HEALTH_TIMEZONE);
      const { days, sourceObjectsRead, listOperations } = await new WeightHistoryReader(
        this.env.SLIPSTREAM_DATA, this.storage,
      ).read(request);
      return this.text({
        start_date: args.start_date, end_date: args.end_date,
        timezone: request.timezone,
        morning_start: args.morning_start, morning_end: args.morning_end,
        requested_days: days.length,
        selected_days: days.filter((day) => day.status === "selected").length,
        source_objects_read: sourceObjectsRead,
        source_list_operations: listOperations,
        days,
      });
    });

    this.activities.registerDetailTools(server);

    if (this.env.GITHUB_ACTIONS_TOKEN && this.env.GITHUB_REPOSITORY) {
      server.registerTool("sync_latest_activity", {
        title: "Fetch fresh data for the latest Garmin workout",
        description: "Use only for an explicitly authorized fresh-data or Garmin sync request. Reads canonical private R2 first and checks completeness and the last successful source check before starting at most one bounded recent activity job. This can contact Garmin and update private R2. Pass expected_date for the Garmin-local workout day and activity_id to disambiguate multiple sessions. Without a date, a newly expected workout uses today's configured HEALTH_TIMEZONE day; during travel supply the explicit local date. Returns a coherent package for the same activity ID, including details, Coach Input and explicit user context. Older workouts and ambiguous dates are never presented as the requested new session. Compatible requests share a correlation/run ID. Use sync_status.data_state for readiness, report source_checked_at and missing_components, and treat null latency as unknown. Follow refresh_status with request_id only while should_continue_polling; stop when false and report missing components and retry guidance.",
        inputSchema: z.object({
          activity_id: z.string().regex(/^(garmin-)?\d{1,20}$/).optional(),
          new_activity_expected: z.boolean().default(true),
          expected_date: exactDate.optional(),
        }),
        outputSchema: outputSchemas.sync_latest_activity,
        annotations: { readOnlyHint: false, destructiveHint: false,
          idempotentHint: true, openWorldHint: true },
      }, async (args) => {
        const fresh = this.freshData();
        return this.text(await fresh.request(fresh.activityRequest(args)));
      });

      server.registerTool("sync_latest_night", {
        title: "Fetch fresh sleep and HRV for the last night",
        description: "Use only when the user explicitly authorizes fetching fresh sleep/night data or Garmin sync. Reads canonical private R2 sleep and associated HRV first, separately checks completeness and the last successful source check, and if needed imports exactly one recent wake-date. Can contact Garmin and update private storage. wake_date is the Garmin-local date on waking, not the date the night began. Omit it only when today's configured HEALTH_TIMEZONE wake-date is appropriate; supply an explicit wake-date during travel. Garmin local timestamps take priority with existing IANA/DST fallback. Compatible requests share a job. Follow refresh_status with request_id while should_continue_polling and stop when false. Use sync_status to distinguish the job, Garmin check and canonical sleep/HRV readiness. A successful workflow alone does not prove that Garmin has finalized sleep and HRV; null latency is unknown.",
        inputSchema: z.object({ wake_date: exactDate.optional() }),
        outputSchema: outputSchemas.sync_latest_night,
        annotations: { readOnlyHint: false, destructiveHint: false,
          idempotentHint: true, openWorldHint: true },
      }, async (args) => {
        const fresh = this.freshData();
        return this.text(await fresh.request(fresh.nightRequest(args.wake_date)));
      });

      server.registerTool("refresh_today", {
      title: "Refresh today's Garmin data",
      description: "Request an incremental Garmin refresh. Set new_activity_expected=true when the user says a recent workout is missing; this uses a 5-minute minimum interval instead of the normal 30-minute cooldown. Read sync_status for job state, successful Garmin check and actual package readiness. Legacy data_ready records workflow success; claim a complete fresh package only when sync_status.data_state is ready. Use targeted tools to verify a specific activity or night. This changes stored data and must only be called when the user explicitly asks to update or refresh their data. It cannot start a historical backfill. IMPORTANT: while should_continue_polling is true, call refresh_status with the returned run ID in the same conversation turn.",
      inputSchema: z.object({
        new_activity_expected: z.boolean().optional()
          .describe("True only when the user expects a recent workout that is not yet in Slipstream"),
      }),
      outputSchema: outputSchemas.refresh_today,
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: true,
      },
    }, async (args) => {
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
        const cooldownMinutes = args.new_activity_expected
          ? Math.min(5, config.cooldownMinutes)
          : config.cooldownMinutes;
        const latestDetails = await this.completedRefreshDetails(latest);
        const decision = latestDetails.latest_activity || latestDetails.latest_night || latestDetails.sync_status.kind === "night"
          ? { dispatch: true as const }
          : refreshDecision(latest, Date.now(), cooldownMinutes);
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
          const details = await this.completedRefreshDetails(run);
          const message = control.should_continue_polling
            ? "A Garmin refresh is still queued or running. Call refresh_status now in this same turn; do not ask the user to send another prompt."
            : control.data_ready
              ? decision.reason === "recent_success"
                ? `No new refresh was started because a successful run began within the last ${cooldownMinutes} minutes. ${details.message}`
                : details.message
              : `The existing Garmin refresh completed with conclusion ${run?.conclusion ?? "unknown"}; updated data is not ready.`;
          return this.text({
            accepted: false,
            reason: decision.reason,
            run,
            ...control,
            ...details,
            message,
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
        const details = await this.completedRefreshDetails(run);
        // Keep the short lease until expiry so GitHub has time to expose the new run.
        return this.text({
          accepted: true,
          run,
          ...control,
          ...details,
          message: control.terminal ? details.message || `The job ended with ${run?.conclusion ?? "unknown"}.`
            : "The Garmin refresh was accepted and is queued or running. Follow refresh_status while should_continue_polling is true.",
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
            sync_status: syncStatus({ kind: "general", reason: "configuration_error" }), latency: latency(null),
          }),
          isError: true,
        };
      }
    });

      server.registerTool("refresh_status", {
      title: "Check Garmin refresh status",
      description: "Wait briefly for a general refresh, activity import or night sync without starting a new job. Run-only manual night checks validate the night report and canonical sleep/HRV. Read sync_status for job state, source-check time, completeness, freshness, missing components and required user action; use data_state=ready for readiness. Legacy run-only data_ready is workflow success. Null latency is unknown. Pass request_id from a targeted activity/night request (preferred), or the returned run_id. Targeted jobs permit three short polling windows in total. Stop polling when should_continue_polling is false, even if terminal is false; report the state and retry guidance. Ordinary status checks start no Garmin job.",
      inputSchema: z.object({
        request_id: requestIdSchema.optional(),
        run_id: z.number().int().positive().optional()
          .describe("GitHub Actions run ID returned by refresh_today or sync_latest_activity; omit only for a general latest-status check"),
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
      const freshJob = args.request_id ? await coordinator.freshJob(args.request_id)
        : args.run_id ? await coordinator.freshJobForRun(args.run_id) : null;
      if (args.request_id && !freshJob) {
        return this.text({ available: false, message: "The targeted correlation ID was not found.",
          terminal: true, data_ready: false, should_continue_polling: false,
          sync_status: syncStatus({ kind: "unknown" }), latency: latency(null) });
      }
      if (freshJob) return this.text(await this.freshData().status(freshJob));
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
          sync_status: syncStatus({ kind: "unknown", accepted: true, polling: true }), latency: latency(null),
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
        const details = await this.completedRefreshDetails(run);
        return this.text({
          available: run !== null,
          run,
          ...control,
          ...details,
          message: control.terminal ? details.message || `The job ended with ${run?.conclusion ?? "unknown"}. Requested data readiness is unverified.`
            : "The job is queued, running or not yet confirmed. Follow refresh_status while should_continue_polling is true.",
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
            sync_status: syncStatus({ kind: "unknown", reason: "status_error" }), latency: latency(null),
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
