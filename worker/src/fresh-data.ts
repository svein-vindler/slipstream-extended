/** R2-first, explicitly authorized recent activity/night retrieval. */
import { z } from "zod";
import { Activity, hrvObjectKeys, rawGarminActivityId, sleepObjectKeys } from "./lib";
import { summarizeHrvPayload, summarizeSleepPayload } from "./health-history";
import { nightContext, validatedHealthTimezone } from "./night-context";
import { dispatchLatestActivity, dispatchLatestNight, getRefreshRun,
  pollRefreshRun, RefreshConfig, RefreshRun } from "./github";
import type { FreshJob } from "./refresh-coordinator";

const CHECK_TTL_MS = 5 * 60_000;
const NEGATIVE_TTL_MS = 2 * 60_000;
const SMALL = { stored: 64 * 1024, decoded: 64 * 1024 };
const datePattern = /^\d{4}-\d{2}-\d{2}$/;
export const requestIdSchema = z.string().uuid();
export const freshnessSchema = z.object({
  scope: z.string(), status: z.string(), complete: z.boolean(),
  source_checked_at: z.string().nullable(), source_age_seconds: z.number().nullable(),
  source_fresh: z.boolean(), missing_components: z.array(z.string()),
  package: z.record(z.string(), z.unknown()),
});
export const freshResultSchema = z.object({
  accepted: z.boolean(), available: z.boolean().optional(), message: z.string(),
  reason: z.string().optional(), request_id: requestIdSchema.optional(),
  run: z.object({ id: z.number(), status: z.string(), conclusion: z.string().nullable(),
    event: z.string().nullable(), created_at: z.string(), updated_at: z.string().nullable(),
    html_url: z.string() }).nullable().optional(),
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
export class FreshDataService {
  private startedAt = performance.now();
  private diagnostics = { canonical_json_gets: 0, canonical_lists: 0,
    summary_reads: 0, profile_reads: 0, github_requests: 0 };
  constructor(private env: Env, private reader: FreshReader, private config: RefreshConfig,
    private now = () => Date.now(), private fetcher: Fetcher = fetch,
    private sleeper = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))) {
    const originalFetch = this.fetcher;
    this.fetcher = async (input, init) => {
      this.diagnostics.github_requests++;
      return originalFetch(input, init);
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
    const stored = await this.reader.getR2Json(keys, small ? SMALL : undefined);
    this.diagnostics.canonical_json_gets += stored ? keys.indexOf(stored.key) + 1 : keys.length;
    return record(stored?.data);
  }

  private async list(prefix: string, limit = 100): Promise<R2Object[]> {
    this.diagnostics.canonical_lists++;
    const listed = await this.env.SLIPSTREAM_DATA.list({ prefix, limit });
    if (listed.truncated) throw new Error("Canonical R2 listing exceeded its bounded limit; select an exact activity ID.");
    return listed.objects;
  }

  private async latestJson(prefix: string): Promise<Json> {
    const objects = await this.list(prefix);
    objects.sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime() || b.key.localeCompare(a.key));
    return objects.length ? this.json([objects[0].key]) : {};
  }

  async snapshot(request: FreshRequest): Promise<Snapshot> {
    const scope = freshScope(request);
    const check = await this.json([`refresh/checks/v1/${scope}.json`], true);
    const checkedAt = typeof check.checked_at === "string" && check.scope === scope
      && check.source_checked === true ? check.checked_at : null;
    const age = checkedAt ? this.now() - Date.parse(checkedAt) : NaN;
    const sourceFresh = Number.isFinite(age) && age >= 0 && age <= CHECK_TTL_MS;
    const result: Snapshot = { scope, status: "missing", complete: false,
      source_checked_at: checkedAt, source_age_seconds: Number.isFinite(age) ? Math.floor(age / 1000) : null,
      source_fresh: sourceFresh, missing_components: [], package: {} };
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
    if (sleep.date !== day || sleepRow.status !== "available") result.missing_components.push("sleep");
    if (!(Number(sleepSummary.sleep_seconds) > 0) || sleep.confirmed === false
      || !sleep.sleep_start_gmt || !sleep.sleep_end_gmt || !Number(sleepRow.stage_count)) {
      result.missing_components.push("complete_sleep");
    }
    if (context.sleep_end_date_local !== day) result.missing_components.push("local_wake_date");
    const positiveReadings = Array.isArray(hrv.readings) && hrv.readings.some((value) => {
      const reading = record(value);
      return reading.hrv_ms != null && typeof reading.hrv_ms !== "boolean"
        && Number.isFinite(Number(reading.hrv_ms)) && Number(reading.hrv_ms) > 0
        && reading.timestamp != null;
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
      : typeof check.activity_id === "string" && check.activity_id.startsWith("garmin-")
        ? [rawGarminActivityId(check.activity_id)]
        : summaries.filter((a) => !request.date || a.date && Math.abs(a.date.getTime()
          - Date.parse(`${request.date}T12:00:00Z`)) <= 2 * 86_400_000)
          .slice(0, 20).map((a) => rawGarminActivityId(a.id));
    const years = new Set([request.date?.slice(0, 4) ?? String(new Date(this.now()).getUTCFullYear()),
      ...summaries.filter((a) => ids.includes(rawGarminActivityId(a.id)))
        .map((a) => String(a.date?.getUTCFullYear())).filter((year) => /^\d{4}$/.test(year))]);
    if (!ids.length) {
      const recent = (await this.list(`activities/${[...years][0]}/`, 1000))
        .filter((a) => /\/activity\.v1\.json(?:\.gz)?$/.test(a.key))
        .sort((a, b) => b.uploaded.getTime() - a.uploaded.getTime()).slice(0, 20);
      ids = recent.map((a) => a.key.split("/")[2]);
    }
    const candidates: Array<{ id: string; local: string; prefix: string; decoded: Json }> = [];
    for (const id of [...new Set(ids)]) {
      if (!/^\d{1,20}$/.test(id)) continue;
      for (const year of years) {
        const prefix = `activities/${year}/${id}`;
        const decoded = await this.json([`${prefix}/activity.v1.json`, `${prefix}/activity.v1.json.gz`]);
        const activity = record(decoded.activity);
        const local = typeof activity.start_time_local === "string" ? activity.start_time_local : "";
        if (String(activity.id) !== id) continue;
        if (!local || !datePattern.test(local.slice(0, 10))) {
          result.status = "activity_date_unknown"; continue;
        }
        if (request.date && local.slice(0, 10) !== request.date) continue;
        if (/run|cycl|bik|walk|hik|swim|cardio|row|ski|elliptical|stair|snow|paddle|kayak|canoe|triathlon|multisport|strength/.test(String(activity.type))) {
          candidates.push({ id, local, prefix, decoded });
        } else result.status = "unsupported_sport";
      }
    }
    if (!request.activityId && request.date && candidates.length > 1) {
      result.status = "ambiguous_activity";
      result.package = { candidate_ids: candidates.map((a) => `garmin-${a.id}`) };
      result.missing_components = ["unambiguous_activity_id"]; return;
    }
    candidates.sort((a, b) => b.local.localeCompare(a.local));
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
    const context = await this.latestJson(`${prefix}/context/v1/`);
    if (running) this.diagnostics.profile_reads++;
    const profile = running ? (await this.reader.getCoachProfiles()).profiles
      .filter((p) => typeof p.effective_from === "string" && p.effective_from <= selected.local.slice(0, 10)
        && usableProfile(p))
      .sort((a, b) => String(b.effective_from).localeCompare(String(a.effective_from))
        || String(b.created_at ?? "").localeCompare(String(a.created_at ?? "")))[0] : null;
    let coach: Json = {};
    if (running) {
      const pointer = await this.json([`${prefix}/coach-input/v1/latest-ready.json`], true);
      const revisions = record(pointer.source_revisions);
      const sources = ["activity.v1.json", "activity.endurance.v1.json", "activity.tcx"];
      const pointerValid = pointer.schema_version === 1 && sources.every((name) => {
        const key = `${prefix}/${name}`;
        return revisions[key] && objects.find((item) => item.key === key)?.etag === revisions[key];
      });
      const analysisKey = typeof pointer.analysis_key === "string" ? pointer.analysis_key : "";
      const versions = pointerValid && analysisKey.startsWith(`${prefix}/coach-input/v1/canonical/`)
        ? objects.filter((object) => object.key === analysisKey) : [];
      for (const version of versions) {
        const candidate = await this.json([version.key]);
        if (candidate.schema_version === 1 && typeof candidate.analysis_id === "string"
          && String(candidate.activity_id) === id && record(candidate.activity).date === selected.local.slice(0, 10)
          && record(candidate.user_context).context_id === context.context_id
          && typeof decoded.source_fit_sha256 === "string"
          && record(candidate.source).fit_sha256 === decoded.source_fit_sha256
          && profile && record(candidate.profile).profile_id === profile.profile_id) {
          coach = candidate; break;
        }
      }
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
      freshness: snapshot, diagnostics, ...extra };
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
    if (active) return this.status(active);
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
    const reservation = await coordinator.beginFresh(snapshot.scope, request.kind, JSON.stringify(request), this.now(), repairOnly);
    if (!reservation.job) return this.response(snapshot, "The targeted sync cooldown or daily safety budget prevents another job.",
      { reason: reservation.reason, retry_after_seconds: Math.max(1, Math.ceil((reservation.retryAfterMs ?? 1000) / 1000)) });
    if (!reservation.acquired) return this.status(reservation.job);
    const job = reservation.job;
    try {
      const run = request.kind === "night"
        ? await dispatchLatestNight(this.config, request.date!, job.request_id, this.fetcher)
        : await dispatchLatestActivity(this.config, { activityId: repairOnly
          ? rawGarminActivityId(String(snapshot.package.activity_id)) : request.activityId,
          expectedDate: repairOnly ? String(snapshot.package.activity_date) : request.date,
          newActivityExpected: request.newExpected ?? false, requestId: job.request_id, repairOnly }, this.fetcher);
      if (run) { await coordinator.bindFreshRun(job.request_id, run.id); job.run_id = run.id; }
    } catch {
      return this.response(snapshot, "Dispatch could not be confirmed. The correlation ID and reservation are retained to prevent duplicate imports; check refresh_status with request_id.",
        { accepted: true, request_id: job.request_id, terminal: false, should_continue_polling: false,
          reason: "dispatch_unconfirmed", retry_after_seconds: 60 });
    }
    return this.status(job, true);
  }

  async status(job: FreshJob, accepted = false): Promise<Json> {
    const coordinator = this.env.REFRESH_COORDINATOR.getByName("global");
    const request = JSON.parse(job.request) as FreshRequest;
    if (!job.run_id) {
      const receipt = await this.json([`refresh/requests/${job.request_id}.json`], true);
      if (Number.isSafeInteger(receipt.run_id) && Number(receipt.run_id) > 0) {
        job.run_id = Number(receipt.run_id);
        await coordinator.bindFreshRun(job.request_id, job.run_id);
      }
    }
    const canPoll = await coordinator.takeFreshPoll(job.request_id);
    const run: RefreshRun | null = job.run_id ? canPoll
      ? await pollRefreshRun(this.config, { runId: job.run_id, maxPolls: 2, intervalMs: 4_000 }, this.fetcher, this.sleeper)
      : await getRefreshRun(this.config, job.run_id, this.fetcher) : null;
    const terminal = run?.status === "completed";
    if (terminal) await coordinator.finishFresh(job.request_id, run.conclusion === "success");
    const snapshot = await this.snapshot(request);
    const report = run && terminal ? await this.json([`refresh/reports/${run.id}.json`], true) : {};
    const stillPolling = !terminal && canPoll && (job.polls + 1 < 3) && job.expires_at > this.now();
    const blocked = String(report.coach_status ?? "");
    return this.response(snapshot, terminal
      ? run?.conclusion !== "success" ? `The targeted job ended with ${run?.conclusion}; requested data readiness is unconfirmed.`
        : snapshot.complete && snapshot.source_fresh && snapshot.status === "ready" ? "The requested canonical package is ready."
          : blocked === "no_effective_profile" || blocked === "endurance_data_unavailable"
            ? `Activity files are stored; Coach Input is blocked (${blocked}). Add an effective profile or use supported analysis; polling will not resolve this.`
            : `The source check completed; the requested package is ${snapshot.status}. Missing: ${snapshot.missing_components.join(", ") || "the expected Garmin upload"}. Garmin may not have received or finalized the data yet.`
      : "The targeted job is queued, running, or not yet confirmed. Polling is bounded; check this correlation ID later if the polling window has ended.",
    { accepted, available: run !== null, request_id: job.request_id, run,
      terminal: terminal || job.expires_at <= this.now(), should_continue_polling: stillPolling,
      ...(terminal && run?.conclusion !== "success" ? { data_ready: false, activity_ready: false } : {}),
      ...(stillPolling ? { poll_after_seconds: 4 } : !terminal ? { retry_after_seconds: 60 } : {}) });
  }
}
