import { DispatchReader } from "./dispatch-reader";
import { dispatchStatus, dispatchStatusSchema } from "./dispatch-status";
import type { DispatchClaim, DispatchDescriptor, DispatchOperation } from "./dispatch-operations";
import { readCurrentCoachInput } from "./coach-selection";
import { activityCalendarYears, localCalendarDay, sourceUtcClock, supportedActivityCalendarContract } from "./activity-clock";
/** R2-first, explicitly authorized recent activity/night retrieval. */
import { z } from "zod";
import { Activity, hrvObjectKeys, rawGarminActivityId, sleepObjectKeys } from "./lib";
import { summarizeHrvPayload, summarizeSleepPayload, timestampSeconds } from "./health-history";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { dispatchLatestActivity, dispatchLatestNight, getRefreshRun,
  GitHubRequestError, pollRefreshRun, RefreshConfig, RefreshRun } from "./github";
import type { FreshJob } from "./refresh-coordinator";
import { latency, latencySchema, pipelineDiagnosticsSchema, syncStatus, syncStatusSchema,
  latestNightReportSchema, activityRepairReportSchema, type PipelineDiagnostics, type SyncStatus } from "./sync-status";
import { latestActivityReportSchema } from "./latest-activity-report";

const CHECK_TTL_MS = 5 * 60_000;
const NEGATIVE_TTL_MS = 2 * 60_000;
const SMALL = { stored: 64 * 1024, decoded: 64 * 1024 };
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
export const requestIdSchema = z.string().uuid();
export const freshnessSchema = z.object({
  scope: z.string(), status: z.string(), complete: z.boolean(),
  source_checked_at: z.string().nullable(), source_age_seconds: z.number().nullable(),
  source_fresh: z.boolean(), missing_components: z.array(z.string()),
  source_freshness_reason: z.enum(["within_ttl", "ttl_expired", "no_confirmed_check",
    "invalid_check_time", "current_check_failed"]).optional(),
  source_outcome: z.enum(["checked", "not_ready", "unknown"]).optional(),
  package: z.record(z.string(), z.unknown()),
});
export const freshResultSchema = z.object({
  dispatch_status: dispatchStatusSchema.optional(),
  accepted: z.boolean(), available: z.boolean().optional(), message: z.string(),
  reason: z.string().optional(), request_id: requestIdSchema.optional(),
  run: z.object({ id: z.number(), status: z.string(), conclusion: z.string().nullable(),
    event: z.string().nullable(), created_at: z.string(), updated_at: z.string().nullable(),
    html_url: z.string(), run_started_at: z.string().nullable().optional() }).nullable().optional(),
  sync_status: syncStatusSchema.optional(), latency: latencySchema.optional(),
  pipeline_diagnostics: pipelineDiagnosticsSchema.nullable().optional(),
  terminal: z.boolean(), data_ready: z.boolean(), should_continue_polling: z.boolean(),
  poll_after_seconds: z.number().int().positive().optional(),
  retry_after_seconds: z.number().int().positive().optional(),
  activity_ready: z.boolean().nullable().optional(),
  freshness: freshnessSchema.optional(),
  diagnostics: z.record(z.string(), z.number()).optional(),
});

type Json = Record<string, unknown>;
export function record(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Json : {};
}
export interface FreshReader {
  getActivities(): Promise<Activity[]>;
  getR2Json(keys: string[], limits?: { stored: number; decoded: number }):
    Promise<{ key: string; data: unknown } | null>;
  getCoachProfiles(): Promise<{ keys: string[]; profiles: Json[] }>;
}
export interface FreshRequest {
  kind: "activity" | "night";
  date?: string;
  activityId?: string;
  newExpected?: boolean;
  requestedAt?: number;
}
export function freshScope(request: FreshRequest): string {
  return request.kind === "night" ? `night/${request.date}`
    : `activity/${request.date ?? "latest"}/${request.activityId ?? "latest"}/${request.newExpected ? "new" : "known"}`;
}
export function localToday(timezone: string | null, now: number): string {
  if (!timezone) throw new Error("Supply an explicit Garmin-local date; HEALTH_TIMEZONE is unavailable.");
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(new Date(now));
  const part = (name: string) => parts.find((item) => item.type === name)?.value;
  return `${part("year")}-${part("month")}-${part("day")}`;
}
export function validateRecentDate(day: string, now: number): void {
  const parsed = Date.parse(`${day}T00:00:00Z`);
  const utcDay = Math.floor(now / 86_400_000) * 86_400_000;
  if (!datePattern.test(day) || !Number.isFinite(parsed)
    || new Date(parsed).toISOString().slice(0, 10) !== day
    || parsed < utcDay - 7 * 86_400_000 || parsed > utcDay + 86_400_000) {
    throw new Error("Supply a valid recent Garmin-local date (last seven days or today's local day).");
  }
}

function usableProfile(profile: Json): boolean {
  const day = profile.effective_from;
  if (typeof day !== "string" || !datePattern.test(day)
    || !Number.isFinite(Date.parse(`${day}T00:00:00Z`))
    || new Date(`${day}T00:00:00Z`).toISOString().slice(0, 10) !== day
    || typeof profile.profile_id !== "string" || !profile.profile_id.trim()
    || !Array.isArray(profile.zones) || !profile.zones.length || profile.zones.length > 10) return false;
  let previousMax = -1;
  for (const value of profile.zones) {
    const zone = record(value);
    const min = Number(zone.min_bpm), max = Number(zone.max_bpm);
    if (typeof zone.label !== "string" || !zone.label.trim()
      || zone.min_bpm == null || zone.max_bpm == null
      || typeof zone.min_bpm === "boolean" || typeof zone.max_bpm === "boolean"
      || !Number.isFinite(min) || !Number.isFinite(max) || min > max || min <= previousMax) return false;
    previousMax = max;
  }
  return true;
}

type Snapshot = z.infer<typeof freshnessSchema>;
type Fetcher = typeof fetch;

function packageStatusMessage(kind: "activity" | "night", snapshot: Snapshot, status: SyncStatus): string {
  const name = kind === "night" ? "sleep and HRV" : "activity";
  const source = status.current_job_source_checked === true
    ? `This job confirmed a successful Garmin check${status.current_job_source_checked_at ? ` at ${status.current_job_source_checked_at}` : ""}.`
    : status.current_job_source_checked === false ? "This job did not perform or confirm a successful Garmin check."
      : "A successful Garmin check for this job is unconfirmed.";
  const stored = snapshot.source_checked_at
    ? `Last confirmed stored source check: ${snapshot.source_checked_at}.`
    : "There is no confirmed stored source check for this scope.";
  const freshness = snapshot.source_freshness_reason === "ttl_expired"
    ? "The five-minute freshness window has expired."
    : snapshot.source_freshness_reason === "current_check_failed"
      ? "This job's source retrieval is unconfirmed; stored history is preserved."
      : snapshot.source_freshness_reason === "invalid_check_time"
        ? "The stored check time cannot establish current freshness."
        : snapshot.source_fresh ? "The stored source check is within the five-minute freshness window."
          : "Source freshness is unconfirmed.";
  const data = snapshot.complete ? `The stored ${name} package is complete.`
    : `The requested ${name} package is ${status.data_state}. Missing: ${snapshot.missing_components.join(", ") || "canonical package verification"}.`;
  return `${data} ${source} ${stored} ${freshness}`
    + (snapshot.source_outcome === "not_ready" ? " The stored successful Garmin check found the requested data unavailable." : "");
}

export class FreshDataService {
  private startedAt = performance.now();
  private diagnostics = { canonical_json_gets: 0, canonical_lists: 0,
    summary_reads: 0, profile_reads: 0, github_requests: 0, r2_read_ms: 0, github_ms: 0 };
  constructor(private env: Env, private reader: FreshReader, private config: RefreshConfig,
    private now = () => Date.now(), private fetcher: Fetcher = fetch,
    private sleeper = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
    private actorKey = "internal-targeted") {
    const originalFetch = this.fetcher;
    this.fetcher = async (input, init) => {
      this.diagnostics.github_requests++;
      const started = performance.now();
      try { return await originalFetch(input, init); }
      finally { this.diagnostics.github_ms += performance.now() - started; }
    };
  }

  activityRequest(args: { activity_id?: string; expected_date?: string; new_activity_expected: boolean }): FreshRequest {
    const date = args.expected_date ?? (args.new_activity_expected && !args.activity_id
      ? localToday(validatedHealthTimezone(this.env.HEALTH_TIMEZONE), this.now()) : undefined);
    if (date) validateRecentDate(date, this.now());
    return { kind: "activity", date, activityId: args.activity_id
      ? rawGarminActivityId(args.activity_id) : undefined, newExpected: args.new_activity_expected };
  }

  nightRequest(wakeDate?: string): FreshRequest {
    const date = wakeDate ?? localToday(validatedHealthTimezone(this.env.HEALTH_TIMEZONE), this.now());
    validateRecentDate(date, this.now());
    return { kind: "night", date };
  }

  private async json(keys: string[], small = false): Promise<Json> {
    return await this.jsonNullable(keys, small) ?? {};
  }

  private async jsonNullable(keys: string[], small = false): Promise<Json | null> {
    const began = performance.now();
    let stored;
    try { stored = await this.reader.getR2Json(keys, small ? SMALL : undefined); }
    finally { this.diagnostics.r2_read_ms += performance.now() - began; }
    this.diagnostics.canonical_json_gets += stored ? keys.indexOf(stored.key) + 1 : keys.length;
    return stored ? record(stored.data) : null;
  }

  private async list(prefix: string, limit = 100): Promise<R2Object[]> {
    this.diagnostics.canonical_lists++;
    const began = performance.now();
    let listed;
    try { listed = await this.env.SLIPSTREAM_DATA.list({ prefix, limit }); }
    finally { this.diagnostics.r2_read_ms += performance.now() - began; }
    if (listed.truncated) throw new Error("Canonical R2 listing exceeded its bounded limit; select an exact activity ID.");
    return listed.objects;
  }

  private async latestActivityContext(id: string, day: string): Promise<Json> {
    const objects = [];
    for (const year of activityCalendarYears(day)) {
      objects.push(...await this.list(`activities/${year}/${id}/context/v1/`));
    }
    // Context keys are append-only timestamp keys, as used by the Python builder.
    objects.sort((a, b) => b.key.split("/").at(-1)!.localeCompare(a.key.split("/").at(-1)!));
    if (objects.length > 1 && objects[0].key.split("/").at(-1) === objects[1].key.split("/").at(-1)) {
      throw new Error("Activity context timestamp is ambiguous across calendar years.");
    }
    return objects.length ? this.json([objects[0].key]) : {};
  }

  async snapshot(request: FreshRequest): Promise<Snapshot> {
    const scope = freshScope(request);
    const check = await this.json([`refresh/checks/v1/${scope}.json`], true);
    const checkedAt = typeof check.checked_at === "string" && z.iso.datetime({ offset: true }).safeParse(check.checked_at).success && check.scope === scope
      && check.source_checked === true ? check.checked_at : null;
    const age = checkedAt ? this.now() - Date.parse(checkedAt) : NaN;
    const sourceFresh = Number.isFinite(age) && age >= 0 && age <= CHECK_TTL_MS;
    const result: Snapshot = { scope, status: "missing", complete: false,
      source_checked_at: checkedAt, source_age_seconds: Number.isFinite(age) ? Math.floor(age / 1000) : null,
      source_fresh: sourceFresh, source_freshness_reason: sourceFresh ? "within_ttl" : !checkedAt
        ? "no_confirmed_check" : age < 0 ? "invalid_check_time" : "ttl_expired",
      missing_components: [], package: {} };
    result.source_outcome = checkedAt ? check.status === "pending" ||
      ["no_recent_activity", "no_new_activity", "expected_activity_missing", "files_unavailable"].includes(String(check.status))
      ? "not_ready" : "checked" : "unknown";
    if (request.kind === "night") await this.readNight(request, result);
    else await this.readActivity(request, check, result);
    if (result.complete) result.status = sourceFresh ? "ready" : "source_stale";
    // A recent empty source response is distinct from a failed request.
    if (sourceFresh && ["no_recent_activity", "no_new_activity", "expected_activity_missing"].includes(String(check.status))) {
      result.status = String(check.status);
      result.complete = false;
    }
    if (sourceFresh && check.status === "ambiguous_activity") {
      result.status = "ambiguous_activity"; result.complete = false;
      result.package.candidate_ids = check.candidate_ids;
    }
    if (sourceFresh && check.status === "activity_date_unknown") {
      result.status = "activity_date_unknown"; result.complete = false;
    }
    if (request.kind === "night" && sourceFresh && check.status === "pending") {
      for (const stream of ["sleep", "hrv"]) {
        if (check[`${stream}_status`] !== "stored") result.missing_components.push(`${stream}_latest_source`);
      }
      result.complete = false; result.status = "night_pending";
    }
    return result;
  }

  private async readNight(request: FreshRequest, result: Snapshot): Promise<void> {
    const day = request.date!;
    const sleep = await this.json(sleepObjectKeys(day));
    const hrv = await this.json(hrvObjectKeys(day));
    const zone = validatedHealthTimezone(this.env.HEALTH_TIMEZONE);
    const context = nightContext(day, sleep.sleep_start_gmt, sleep.sleep_end_gmt, zone,
      sleep.sleep_start_garmin_local, sleep.sleep_end_garmin_local);
    const sleepRow = summarizeSleepPayload(day, sleep);
    const hrvRow = summarizeHrvPayload(day, hrv);
    const sleepSummary = record(sleepRow.summary);
    const sleepStart = timestampSeconds(sleep.sleep_start_gmt);
    const sleepEnd = timestampSeconds(sleep.sleep_end_gmt);
    const validStage = Array.isArray(sleepRow.stages) && sleepRow.stages.some((value) => {
      const stage = record(value);
      const start = timestampSeconds(stage.start_gmt), end = timestampSeconds(stage.end_gmt);
      return stage.stage != null && start != null && end != null && end > start
        && sleepStart != null && sleepEnd != null && start >= sleepStart && end <= sleepEnd;
    });
    if (sleep.date !== day || sleepRow.status !== "available") result.missing_components.push("sleep");
    if (!(Number(sleepSummary.sleep_seconds) > 0) || sleep.confirmed === false
      || sleepStart == null || sleepEnd == null || sleepEnd <= sleepStart
      || !Number(sleepRow.stage_count) || !validStage) {
      result.missing_components.push("complete_sleep");
    }
    if (context.sleep_end_date_local !== day) result.missing_components.push("local_wake_date");
    const positiveReadings = Array.isArray(hrv.readings) && hrv.readings.some((value) => {
      const reading = record(value);
      return reading.hrv_ms != null && typeof reading.hrv_ms !== "boolean"
        && Number.isFinite(Number(reading.hrv_ms)) && Number(reading.hrv_ms) > 0
        && (timestampSeconds(reading.timestamp) ?? 0) > 0;
    });
    if (hrv.date !== day || hrvRow.status !== "available" || !hrvRow.detailed_readings_available || !positiveReadings) {
      result.missing_components.push("hrv_readings");
    }
    const hrvContext = nightContext(day, hrv.sleep_start_gmt, hrv.sleep_end_gmt, zone,
      hrv.sleep_start_garmin_local, hrv.sleep_end_garmin_local);
    if (hrv.sleep_end_gmt && hrvContext.sleep_end_date_local !== day) {
      result.missing_components.push("associated_hrv_night");
    }
    result.package = { wake_date: day, sleep: { ...sleepRow, ...context },
      hrv: { ...hrvRow, ...(hrv.sleep_start_gmt ? hrvContext : context) }, index_state: "canonical" };
    result.complete = result.missing_components.length === 0;
    result.status = result.complete ? "ready" : "night_pending";
  }

  private async readActivity(request: FreshRequest, check: Json, result: Snapshot): Promise<void> {
    let summaries: Activity[] = [];
    this.diagnostics.summary_reads++;
    try { summaries = await this.reader.getActivities(); } catch { /* Canonical data may predate the index. */ }
    let ids = request.activityId ? [request.activityId]
      : result.source_fresh && typeof check.activity_id === "string" && check.activity_id.startsWith("garmin-")
        ? [rawGarminActivityId(check.activity_id)]
        : summaries.filter((a) => !request.date || a.date && Math.abs(a.date.getTime()
          - Date.parse(`${request.date}T12:00:00Z`)) <= 2 * 86_400_000)
          .sort((a, b) => (b.date?.getTime() ?? 0) - (a.date?.getTime() ?? 0))
          .slice(0, 20).map((a) => rawGarminActivityId(a.id));
    const years = new Set([...(request.date ? [request.date.slice(0, 4)] :
      [this.now(), this.now() - 7 * 86_400_000, this.now() + 86_400_000]
        .map((date) => String(new Date(date).getUTCFullYear())))]);
    if (!ids.length) {
      const recent = (await Promise.all([...years].map((year) => this.list(`activities/${year}/`, 1000)))).flat()
        .filter((a) => /\/activity\.v1\.json(?:\.gz)?$/.test(a.key))
        .sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime()).slice(0, 20);
      ids = recent.map((a) => a.key.split("/")[2]);
    }
    const candidates: Array<{ id: string; local: string; utc: number | null; prefix: string; decoded: Json }> = [];
    for (const id of [...new Set(ids)]) {
      if (!/^\d{1,20}$/.test(id)) continue;
      for (const year of years) {
        const prefix = `activities/${year}/${id}`;
        const decoded = await this.json([`${prefix}/activity.v1.json`, `${prefix}/activity.v1.json.gz`]);
        const activity = record(decoded.activity);
        const local = typeof activity.start_time_local === "string" ? activity.start_time_local : "";
        if (String(activity.id) !== id) continue;
        if (!supportedActivityCalendarContract(decoded)) {
          result.status = "activity_date_unknown"; continue;
        }
        const localDay = localCalendarDay(local);
        if (!localDay || localDay.slice(0, 4) !== year) {
          result.status = "activity_date_unknown"; continue;
        }
        if (request.date && local.slice(0, 10) !== request.date) continue;
        if (!request.activityId) {
          // An older last-known session is not an error in a latest-data request.
          // Leave it out so the bounded Garmin check can discover a newer upload.
          try { validateRecentDate(local.slice(0, 10), this.now()); } catch { continue; }
        }
        if (/run|cycl|bik|walk|hik|swim|cardio|row|ski|elliptical|stair|snow|paddle|kayak|canoe|triathlon|multisport|strength/.test(String(activity.type))) {
          const clock = typeof activity.start_time_gmt === "string" && /[T ]\d{2}:\d{2}/.test(activity.start_time_gmt)
            ? sourceUtcClock(activity.start_time_gmt) : null;
          const utc = clock && Math.abs(Date.parse(`${localDay}T00:00:00Z`)
            - Date.parse(`${clock.toISOString().slice(0, 10)}T00:00:00Z`)) <= 86_400_000 ? clock.getTime() : null;
          candidates.push({ id, local, utc, prefix, decoded });
        } else result.status = "unsupported_sport";
      }
    }
    if (!request.activityId && request.date && candidates.length > 1) {
      result.status = "ambiguous_activity";
      result.package = { candidate_ids: candidates.map((a) => `garmin-${a.id}`) };
      result.missing_components = ["unambiguous_activity_id"]; return;
    }
    const unanchored = !request.activityId && !request.date && !(result.source_fresh
      && typeof check.activity_id === "string" && check.activity_id.startsWith("garmin-"));
    if (unanchored && candidates.some(candidate => candidate.utc === null)) {
      result.status = "activity_date_unknown";
      result.missing_components = ["verified_activity_utc"]; return;
    }
    candidates.sort((a, b) => (b.utc ?? -Infinity) - (a.utc ?? -Infinity));
    if (unanchored && candidates.length > 1 && candidates[0].utc === candidates[1].utc) {
      result.status = "ambiguous_activity";
      result.package = { candidate_ids: candidates.filter(candidate => candidate.utc === candidates[0].utc)
        .map(candidate => `garmin-${candidate.id}`) };
      result.missing_components = ["unambiguous_activity_id"]; return;
    }
    const selected = candidates[0];
    if (!selected) { result.missing_components = ["matching_local_activity"]; return; }
    validateRecentDate(selected.local.slice(0, 10), this.now());
    const { id, prefix, decoded } = selected;
    const activity = record(decoded.activity);
    const running = /run/.test(String(activity.type));
    const strength = /strength/.test(String(activity.type));
    const endurance = strength ? {} : await this.json([`${prefix}/activity.endurance.v1.json`, `${prefix}/activity.endurance.v1.json.gz`]);
    const objects = await this.list(`${prefix}/`, 1000);
    const keys = new Set(objects.map((a) => a.key));
    for (const name of strength ? ["activity.fit"] : ["activity.fit", "activity.tcx"]) {
      if (!keys.has(`${prefix}/${name}`)) result.missing_components.push(name);
    }
    if (Array.isArray(decoded.decode_errors) && decoded.decode_errors.length) result.missing_components.push("decoded_fit");
    if (!strength && (record(endurance.activity).id !== id || endurance.available === false
      || !Object.keys(record(endurance.summary)).length)) result.missing_components.push("endurance_analysis");
    if (strength && !Object.keys(record(decoded.normalized_strength_session)).length) {
      result.missing_components.push("strength_analysis");
    }
    const context = await this.latestActivityContext(id, selected.local.slice(0, 10));
    if (running) this.diagnostics.profile_reads++;
    const profile = running ? (await this.reader.getCoachProfiles()).profiles
      .filter((p) => typeof p.effective_from === "string" && p.effective_from <= selected.local.slice(0, 10)
        && usableProfile(p))
      .sort((a, b) => String(b.effective_from).localeCompare(String(a.effective_from))
        || String(b.created_at ?? "").localeCompare(String(a.created_at ?? "")))[0] : null;
    let coach: Json = {};
    if (running) {
      const selection = await readCurrentCoachInput({
        canonicalPrefix: `${prefix}/coach-input/v1/canonical/`, activityId: id,
        date: selected.local.slice(0, 10), allowLegacy: false,
        pointerUploaded: objects.find(item => item.key === `${prefix}/coach-input/v1/latest-ready.json`)?.uploaded,
        read: async (keys, limits) => {
          const data = await this.json(keys, Boolean(limits));
          return Object.keys(data).length ? { key: keys[0], data } : null;
        },
        versions: async () => objects,
        acceptPointer: (pointer) => {
          const revisions = record(pointer.source_revisions);
          return ["activity.v1.json", "activity.endurance.v1.json", "activity.tcx"].every((name) => {
            const key = `${prefix}/${name}`;
            return revisions[key] && objects.find(item => item.key === key)?.etag === revisions[key];
          });
        },
      });
      const candidate = selection.status === "available" ? selection.analysis : null;
      // Explicit user context is composed separately below. A context-only
      // append must not turn a verified immutable analysis into a repair job.
      if (candidate && typeof decoded.source_fit_sha256 === "string"
        && record(candidate.source).fit_sha256 === decoded.source_fit_sha256
        && profile && record(candidate.profile).profile_id === profile.profile_id) coach = candidate;
    }
    if (running && !Object.keys(coach).length) {
      result.missing_components.push("coach_input");
    }
    result.package = { activity_id: `garmin-${id}`, activity_date: selected.local.slice(0, 10),
      activity: { ...activity }, summary: { ...record(endurance.summary),
        id: `garmin-${id}`, date: selected.local, name: activity.name, type: activity.type,
        index_state: summaries.find((a) => rawGarminActivityId(a.id) === id)
          ? "stored_summary_with_canonical_details" : "canonical_read_through" },
      analysis: strength ? decoded.normalized_strength_session : endurance,
      coach_input: running ? coach : null, user_context: Object.keys(context).length ? context : null,
      coach_status: running ? endurance.available === false ? "endurance_data_unavailable"
        : !profile ? "no_effective_profile"
        : result.missing_components.includes("coach_input") ? "pending" : "ready" : "not_applicable" };
    result.complete = result.missing_components.length === 0;
    result.status = result.complete ? "ready" : "activity_pending";
  }

  private response(snapshot: Snapshot, message: string, extra: Json = {}): Json {
    const ready = snapshot.complete && snapshot.source_fresh && snapshot.status === "ready";
    const diagnostics = { ...this.diagnostics, elapsed_ms: Math.round(performance.now() - this.startedAt) };
    console.log(JSON.stringify({ event: "fresh_data_read", kind: snapshot.scope.split("/")[0],
      status: snapshot.status, request_id: extra.request_id ?? null, ...diagnostics }));
    return { accepted: false, message, terminal: true, data_ready: ready,
      should_continue_polling: false, activity_ready: snapshot.scope.startsWith("activity/") ? ready : null,
      freshness: snapshot, diagnostics,
      sync_status: syncStatus({ kind: snapshot.scope.startsWith("night/") ? "night" : "activity",
        snapshot, run: extra.run as RefreshRun | null | undefined, accepted: extra.accepted === true,
        polling: extra.should_continue_polling === true, reason: typeof extra.reason === "string" ? extra.reason : undefined }),
      latency: latency(null), ...extra };
  }

  async completedDetails(run: RefreshRun, request: FreshRequest,
    pipeline?: PipelineDiagnostics | null, sourceChecked?: boolean, sourceCheckedAt?: string) {
    const snapshot = await this.snapshot(request);
    // A failed current source check cannot attest to the latest version even
    // when an older successful receipt and good canonical history remain.
    if (sourceChecked === false) {
      snapshot.source_fresh = false;
      snapshot.source_freshness_reason = "current_check_failed";
      if (snapshot.complete) snapshot.status = "source_stale";
    }
    const status = syncStatus({ kind: request.kind, run, snapshot, pipeline, sourceChecked, sourceCheckedAt });
    const message = status.data_state === "ready" ? `The requested ${request.kind === "night" ? "sleep and HRV" : "activity"} package is complete and fresh.`
      : status.user_action_required ? `The requested package needs user action (${snapshot.package.coach_status ?? snapshot.status}).`
        : "The job ended.";
    const detail = packageStatusMessage(request.kind, snapshot, status);
    return { message: `${message} ${detail}`, activity_ready: request.kind === "activity" ? status.data_state === "ready" : null,
      freshness: snapshot, sync_status: status, pipeline_diagnostics: pipeline ?? null,
      latency: latency(run, pipeline, request.requestedAt, status.data_state === "ready", this.now()) };
  }

  private overlap(snapshot: Snapshot, operation: DispatchOperation): Json {
    const status = syncStatus({ kind: snapshot.scope.startsWith("night/") ? "night" : "activity", snapshot });
    status.job_state = "unknown"; status.job_start_confirmed = null;
    status.polling_state = "stopped"; status.next_action = "none";
    return this.response(snapshot, "An existing operation may overlap this request. Keep its correlation for maintainer inspection; no new job was started.",
      { accepted: false, terminal: false, data_ready: false, should_continue_polling: false, reason: "dispatch_overlap",
        sync_status: status, dispatch_status: dispatchStatus(operation) });
  }

  async request(request: FreshRequest): Promise<Json> {
    const snapshot = await this.snapshot(request);
    if (snapshot.status === "ambiguous_activity" || snapshot.status === "activity_date_unknown" && snapshot.source_fresh
      || snapshot.status === "unsupported_sport") {
      return this.response(snapshot, "Select an exact activity ID and a verified Garmin-local date before importing details.");
    }
    if (snapshot.complete && snapshot.source_fresh && snapshot.status === "ready") {
      return this.response(snapshot, "Complete canonical R2 data and a recent successful Garmin check are available. No job was started.");
    }
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const active = await coordinator.activeFreshJob(snapshot.scope, this.now());
    if (active) return this.status(active, false, "shared");
    const blocked = await coordinator.blockingDispatch(request.kind, "targeted:" + snapshot.scope);
    if (blocked) return this.overlap(snapshot, blocked);
    if (snapshot.source_fresh && ["no_effective_profile", "endurance_data_unavailable"].includes(String(snapshot.package.coach_status))) {
      return this.response(snapshot, `Activity data is stored, but Coach Input is blocked (${snapshot.package.coach_status}). Add an effective profile or use supported analysis before retrying; polling cannot resolve this.`,
        { reason: String(snapshot.package.coach_status) });
    }
    const repairOnly = snapshot.source_fresh && snapshot.missing_components.length === 1
      && snapshot.missing_components[0] === "coach_input";
    if (!repairOnly && snapshot.source_fresh && snapshot.source_age_seconds! * 1000 < NEGATIVE_TTL_MS) {
      return this.response(snapshot, "Garmin was checked recently; the requested package is still unavailable or incomplete. Retry after the short source-check cooldown.",
        { reason: "negative_cooldown", retry_after_seconds: Math.max(1, 120 - snapshot.source_age_seconds!) });
    }
    const sent = repairOnly ? { ...request,
      activityId: rawGarminActivityId(String(snapshot.package.activity_id)),
      date: String(snapshot.package.activity_date) } : request;
    const descriptor: DispatchDescriptor = { repository: this.config.repository, workflow: this.config.workflow,
      ref: this.config.ref, mode: request.kind, scope: freshScope(sent), date: sent.date ?? null,
      activity_id: sent.activityId ?? null, new_expected: sent.newExpected ?? false, repair_only: repairOnly,
      include_granular: false, components: request.kind === "night" ? ["sleep", "hrv"] : ["activity_files", "coach_input", "context"] };
    const reservation = await coordinator.beginFresh(snapshot.scope, request.kind,
      JSON.stringify({ ...request, requestedAt: this.now() }), this.now(), repairOnly, descriptor);
    if (reservation.operation) return this.overlap(snapshot, reservation.operation);
    if (!reservation.job) return this.response(snapshot, "The targeted sync cooldown or daily safety budget prevents another job.",
      { reason: reservation.reason, retry_after_seconds: Math.max(1, Math.ceil((reservation.retryAfterMs ?? 1000) / 1000)) });
    if (!reservation.acquired) return this.status(reservation.job, false, "shared");
    const job = reservation.job;
    const identity = { operation_id: job.request_id, owner_key: "targeted:" + job.scope, owner_token: job.request_id };
    const outcome = request.kind === "night"
      ? await dispatchLatestNight(this.config, request.date!, job.request_id, this.fetcher)
      : await dispatchLatestActivity(this.config, { activityId: sent.activityId, expectedDate: sent.date,
        newActivityExpected: sent.newExpected ?? false, requestId: job.request_id, repairOnly }, this.fetcher);
    const operation = await coordinator.mergeDispatchPost(identity, outcome, this.now());
    if (operation?.dispatch_state === "post_rejected") {
      return this.response(snapshot, "GitHub rejected this dispatch. Check workflow inputs and access before retrying after the cooldown.",
        { accepted: false, request_id: job.request_id, reason: "dispatch_rejected",
          terminal: true, should_continue_polling: false, retry_after_seconds: 300, dispatch_status: dispatchStatus(operation) });
    }
    return this.status(job, true, "initial");
  }

  async status(job: FreshJob, accepted = false, intent: DispatchClaim["intent"] = "status"): Promise<Json> {
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const request = JSON.parse(job.request) as FreshRequest;
    const operation = await coordinator.dispatchOperation(job.request_id) ?? await coordinator.adoptFreshDispatch(job.request_id);
    if (!operation) {
      return this.response(await this.snapshot(request), "This legacy operation has no verified dispatch record. Keep its correlation for maintainer inspection.",
        { accepted, request_id: job.request_id, terminal: false, should_continue_polling: false });
    }
    const dispatchReader = new DispatchReader(this.env, this.config, keys => this.jsonNullable(keys, true), this.now, this.fetcher);
    let observed = await dispatchReader.observe(operation, intent, this.actorKey);
    // Only an early admission can wait: never retry failed/unknown control I/O.
    // The coordinator atomically keeps the same three observation tickets.
    for (let waits = 0; waits < 2 && observed.retryAfterMs !== undefined; waits++) {
      if (!Number.isSafeInteger(observed.retryAfterMs) || observed.retryAfterMs <= 0 || observed.retryAfterMs > 25_000) break;
      await this.sleeper(observed.retryAfterMs);
      const current = await coordinator.dispatchOperation(job.request_id);
      if (!current || current.owner_key !== operation.owner_key || current.owner_token !== operation.owner_token) break;
      observed = await dispatchReader.observe(current, intent, this.actorKey);
    }
    const snapshot = await this.snapshot(request);
    if (observed.report.source_checked === false && observed.report.kind !== "activity-repair") {
      snapshot.source_fresh = false; snapshot.source_freshness_reason = "current_check_failed";
      if (snapshot.complete) snapshot.status = "source_stale";
    }
    if (observed.fresh && observed.run) {
      const polling = observed.run.status !== "completed" && await this.canContinueJob(job);
      const status = syncStatus({ kind: request.kind, snapshot, run: observed.run, pipeline: observed.pipeline,
        polling,
        sourceChecked: typeof observed.report.source_checked === "boolean" ? observed.report.source_checked : undefined,
        sourceCheckedAt: typeof observed.report.checked_at === "string" ? observed.report.checked_at : undefined });
      return this.response(snapshot, packageStatusMessage(request.kind, snapshot, status),
        { accepted, available: true, request_id: job.request_id, run: observed.run,
          terminal: observed.operation.owner_released_at !== null, should_continue_polling: polling,
          ...(polling ? { poll_after_seconds: 25 } : {}), sync_status: status,
          pipeline_diagnostics: observed.pipeline, dispatch_status: dispatchStatus(observed.operation, !polling),
          latency: latency(observed.run, observed.pipeline, request.requestedAt, status.data_state === "ready", this.now()),
          ...(observed.run.conclusion !== "success" ? { data_ready: false, activity_ready: request.kind === "activity" ? false : null } : {}) });
    }
    const status = syncStatus({ kind: request.kind, snapshot });
    if (observed.awaiting_receipt && observed.operation.dispatch_state === "candidate"
      && observed.operation.candidate_run_id !== null) {
      if (await this.canContinueJob(job)) {
        status.job_state = "unknown"; status.job_start_confirmed = null;
        status.current_job_source_checked = null; status.current_job_source_checked_at = null;
        status.polling_state = "continue"; status.next_action = "check_later";
        return this.response(snapshot, "The dispatch is awaiting its workflow startup receipt. Check the same request after the stated delay; no new sync is needed.",
          { accepted, available: false, request_id: job.request_id, terminal: false, data_ready: false,
            should_continue_polling: true, poll_after_seconds: 25, reason: "awaiting_startup_receipt",
            sync_status: status, dispatch_status: dispatchStatus(observed.operation, false) });
      }
    }
    if (observed.operation.dispatch_state === "verified_terminal") {
      const success = observed.operation.last_observation?.conclusion === "success";
      status.job_state = success ? "completed" : "failed";
      status.job_conclusion = observed.operation.last_observation?.conclusion ?? null;
      status.job_start_confirmed = true; status.polling_state = "complete"; status.next_action = "none";
      status.fresh = null;
      if (snapshot.complete) status.data_state = "unknown";
      return this.response(snapshot, "The verified job has ended. Current-job source evidence is not re-read, so current package readiness is unverified. Stored data remain available through read tools.",
        { accepted, available: true, request_id: job.request_id, terminal: true, should_continue_polling: false,
          data_ready: false, activity_ready: null,
          sync_status: status, dispatch_status: dispatchStatus(observed.operation) });
    }
    if (observed.operation.dispatch_state === "post_rejected") {
      status.job_start_confirmed = false; status.polling_state = "stopped";
      return this.response(snapshot, "This dispatch was rejected. Keep the existing cooldown before retrying.",
        { accepted: false, available: false, request_id: job.request_id, terminal: true, should_continue_polling: false,
          sync_status: status, dispatch_status: dispatchStatus(observed.operation) });
    }
    status.job_state = "unknown"; status.job_start_confirmed = null;
    status.current_job_source_checked = null; status.current_job_source_checked_at = null;
    status.polling_state = "stopped"; status.next_action = "none";
    return this.response(snapshot, "Dispatch verification is incomplete. Preserve the correlation for maintainer inspection; do not start another sync to test it.",
      { accepted, available: false, request_id: job.request_id, terminal: false, should_continue_polling: false,
        reason: "dispatch_unconfirmed", sync_status: status, dispatch_status: dispatchStatus(observed.operation) });
  }

  private async canContinueJob(job: FreshJob): Promise<boolean> {
    const current = await this.env.REFRESH_COORDINATOR.getByName("global").freshJob(job.request_id);
    return current?.state === "active" && current.polls < 3 && this.now() < current.expires_at
      && current.scope === job.scope && current.request === job.request;
  }
}
