import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { FreshDataService, freshResultSchema, freshScope, localToday, record,
  validateRecentDate, type FreshRequest } from "../src/fresh-data";
import { decodePossiblyGzippedText } from "../src/security";

const NOW = Date.parse("2026-10-05T12:00:00Z");
const DAY = "2026-10-05";
const REQUEST: FreshRequest = { kind: "activity", date: DAY, newExpected: true };
const NIGHT: FreshRequest = { kind: "night", date: DAY };
const CONFIG = { repository: "owner/slipstream", workflow: "refresh.yml", ref: "main",
  token: "synthetic-test-token", cooldownMinutes: 30 };
const RUN = { id: 44, status: "queued", conclusion: null, event: "workflow_dispatch",
  path: ".github/workflows/refresh.yml", created_at: "2026-10-05T12:00:00Z",
  updated_at: null, html_url: "https://github.com/owner/slipstream/actions/runs/44" };
const PROFILE = { profile_id: "test-profile", effective_from: "2026-01-01", created_at: "2026-01-01T12:00:00Z",
  zones: [{ label: "test-zone", min_bpm: 100, max_bpm: 150 }] };
let keys: string[];
let testEnv: Env;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let posts: Array<Record<string, unknown>>;
let runState: Record<string, unknown>;
let gets: number;

async function put(key: string, value: unknown) {
  keys.push(key);
  await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
}
async function json(keys: string[]) {
  for (const key of keys) {
    const object = await env.SLIPSTREAM_DATA.get(key);
    if (object) return { key, data: JSON.parse(await decodePossiblyGzippedText(await object.arrayBuffer(), 32 * 1024 * 1024)) };
  }
  return null;
}
const fetcher: typeof fetch = async (_url, init) => {
  if (init?.method === "POST") {
    posts.push(record(JSON.parse(String(init.body))));
    return Response.json({ workflow_run_id: 44 });
  }
  gets++;
  return Response.json(runState);
};
function service(customFetch = fetcher, getActivities = async () => [] as import("../src/lib").Activity[], now = NOW) {
  return new FreshDataService(testEnv, { getActivities, getR2Json: json,
    getCoachProfiles: async () => ({ keys: ["test-profile"], profiles: [PROFILE] }) },
  CONFIG, () => now, customFetch, async () => {});
}
async function check(request = REQUEST, age = 30_000, extra = {}) {
  await put(`refresh/checks/v1/${freshScope(request)}.json`, {
    scope: freshScope(request), source_checked: true, checked_at: new Date(NOW - age).toISOString(),
    status: request.kind === "night" ? "stored" : "ready", activity_id: "garmin-1", ...extra,
  });
}
async function activity(id = "1", localDate = DAY) {
  const prefix = `activities/2026/${id}`;
  await put(`${prefix}/activity.v1.json`, { schema_version: 1, source_fit_sha256: "synthetic-fit-hash",
    activity: { id, type: "running", name: "Synthetic run", start_time_local: `${localDate} 10:00:00`,
      start_time_gmt: `${localDate} 08:00:00` }, decode_errors: [] });
  await put(`${prefix}/activity.fit`, "synthetic marker");
  await put(`${prefix}/activity.tcx`, "synthetic marker");
  await put(`${prefix}/activity.endurance.v1.json`, { schema_version: 1, available: true,
    activity: { id }, summary: { distance_m: 5000 }, trackpoints: [] });
  await put(`${prefix}/coach-input/v1/canonical/test.json`, { schema_version: 1,
    activity_id: id, activity: { date: localDate }, analysis_id: `test-analysis-${id}`,
    profile: { profile_id: PROFILE.profile_id }, source: { fit_sha256: "synthetic-fit-hash" }, user_context: null });
  const objects = (await env.SLIPSTREAM_DATA.list({ prefix: `${prefix}/` })).objects;
  await put(`${prefix}/coach-input/v1/latest-ready.json`, { schema_version: 1,
    analysis_key: `${prefix}/coach-input/v1/canonical/test.json`,
    source_revisions: Object.fromEntries(objects.filter((o) =>
      ["activity.v1.json", "activity.endurance.v1.json", "activity.tcx"].some((name) => o.key === `${prefix}/${name}`))
      .map((o) => [o.key, o.etag])), profile_id: PROFILE.profile_id, context_id: null });
}
async function night() {
  await put(`health/sleep/v1/2026/10/${DAY}.json`, { schema_version: 1, date: DAY, confirmed: true,
    sleep_start_gmt: "2026-10-04T21:00:00Z", sleep_end_gmt: "2026-10-05T04:00:00Z",
    sleep_start_garmin_local: "2026-10-04T23:00:00", sleep_end_garmin_local: "2026-10-05T06:00:00",
    summary: { sleep_seconds: 25200 }, stage_count: 1, stages: [{ start_gmt: "2026-10-04T21:00:00Z",
      end_gmt: "2026-10-05T04:00:00Z", stage: 1 }] });
  await put(`health/hrv/2026/10/${DAY}.json`, { schema_version: 1, date: DAY,
    summary: { lastNightAvg: 51 }, readings: [{ timestamp: "2026-10-05T01:00:00Z", hrv_ms: 51 }] });
}

beforeEach(() => {
  keys = []; posts = []; gets = 0; runState = { ...RUN };
  const namespace = env.REFRESH_COORDINATOR;
  coordinator = namespace.getByName(crypto.randomUUID());
  testEnv = { ...env, HEALTH_TIMEZONE: "Europe/Oslo",
    REFRESH_COORDINATOR: { getByName: () => coordinator } } as Env;
});
afterEach(async () => { if (keys.length) await env.SLIPSTREAM_DATA.delete([...new Set(keys)]); });

describe("R2-first targeted freshness", () => {
  it("checks Garmin rather than failing on an older last-known workout", async () => {
    await activity("1", "2026-09-01");
    const result = await service().request({ kind: "activity", newExpected: false });
    expect(result).toMatchObject({ accepted: true, data_ready: false });
    expect(posts).toHaveLength(1);
  });

  it("sorts the summary candidates before applying the discovery limit", async () => {
    await activity("21");
    const summaries = Array.from({ length: 21 }, (_, i) => ({ id: `garmin-${i + 1}`,
      date: new Date(i === 20 ? `${DAY}T08:00:00Z` : "2026-09-01T08:00:00Z"), name: "Synthetic", type: "running", source: "garmin" }));
    const result = await service(fetcher, async () => summaries)
      .snapshot({ kind: "activity", newExpected: false });
    expect(result.package.activity_id).toBe("garmin-21");
  });

  it("does not let an expired checkpoint pin an older activity", async () => {
    await activity("1", "2026-09-01"); await activity("2");
    const request: FreshRequest = { kind: "activity", newExpected: false };
    await check(request, 10 * 60_000);
    expect((await service().snapshot(request)).package.activity_id).toBe("garmin-2");
  });

  it("serves canonical activity ahead of the summary index with no dispatch", async () => {
    await activity(); await check();
    const result = await service().request(REQUEST);
    expect(freshResultSchema.safeParse(result).success).toBe(true);
    expect(result).toMatchObject({ accepted: false, data_ready: true, activity_ready: true,
      freshness: { source_fresh: true, complete: true, missing_components: [],
        package: { activity_id: "garmin-1", summary: { index_state: "canonical_read_through" } } } });
    expect(posts).toHaveLength(0); expect(gets).toBe(0);
  });

  it("starts exactly the bounded activity mode for stale data", async () => {
    await activity(); await check(REQUEST, 10 * 60_000);
    const result = await service().request(REQUEST);
    expect(result).toMatchObject({ accepted: true, data_ready: false, terminal: false });
    expect(posts).toHaveLength(1);
    expect(posts[0].inputs).toMatchObject({ latest_activity_only: true,
      latest_expected_date: DAY, latest_new_activity_expected: true, sync_request_id: result.request_id });
    expect(posts[0].inputs).not.toHaveProperty("include_granular");
  });

  it("shares one persistent correlation and run ID across concurrent requests", async () => {
    const results = await Promise.all([service().request(REQUEST), service().request(REQUEST), service().request(REQUEST)]);
    expect(posts).toHaveLength(1);
    expect(new Set(results.map((r) => r.request_id)).size).toBe(1);
    const persisted = await coordinator.freshJob(String(results[0].request_id));
    expect(persisted?.run_id).toBe(44);
    expect(gets).toBeLessThanOrEqual(7);
    expect(results.filter((r) => r.should_continue_polling).length).toBeLessThanOrEqual(2);
  });

  it("finds a recent last-year activity without a summary at New Year", async () => {
    await activity("1", "2026-12-31");
    const snapshot = await service(fetcher, async () => [], Date.parse("2027-01-01T12:00:00Z"))
      .snapshot({ kind: "activity", newExpected: false });
    expect(snapshot.package.activity_id).toBe("garmin-1");
    expect(snapshot.package.activity_date).toBe("2026-12-31");
  });

  it("uses timestamp-key context order consistently with the coach builder", async () => {
    await activity(); await check();
    await put("activities/2026/1/context/v1/20261005-new.json", { context_id: "new-context" });
    await put("activities/2026/1/context/v1/20261004-old.json", { context_id: "old-context" });
    expect(record((await service().snapshot(REQUEST)).package.user_context).context_id).toBe("new-context");
  });

  it("stops after three short polling windows and still checks a later completion", async () => {
    const result = await service().request(REQUEST);
    const id = String(result.request_id);
    for (let i = 0; i < 4; i++) await service().status((await coordinator.freshJob(id))!);
    expect(await coordinator.freshJob(id)).toMatchObject({ polls: 3 });
    expect(await service().status((await coordinator.freshJob(id))!))
      .toMatchObject({ should_continue_polling: false, retry_after_seconds: 60 });
    await activity(); await check(); runState = { ...RUN, status: "completed", conclusion: "success" };
    expect(await service().status((await coordinator.freshJob(id))!)).toMatchObject({ terminal: true, data_ready: true });
    expect(posts).toHaveLength(1);
  });

  it("keeps an expired polling window nonterminal while the actual job is running", async () => {
    const result = await service().request(NIGHT);
    const job = (await coordinator.freshJob(String(result.request_id)))!;
    const status = await service(fetcher, async () => [], NOW + 3 * 60 * 60_000).status(job);
    expect(status).toMatchObject({ terminal: false, should_continue_polling: false,
      sync_status: { job_state: "queued", polling_state: "stopped", next_action: "check_later" } });
    expect(posts).toHaveLength(1);
  });

  it("reports delayed files and coach input after a completed successful job", async () => {
    await activity();
    await env.SLIPSTREAM_DATA.delete("activities/2026/1/activity.tcx");
    const first = await service().request(REQUEST);
    await check();
    runState = { ...RUN, status: "completed", conclusion: "success" };
    const result = await service().status((await coordinator.freshJob(String(first.request_id)))!);
    expect(result).toMatchObject({ terminal: true, data_ready: false,
      sync_status: { kind: "activity", job_state: "completed", data_state: "partial",
        missing_components: ["activity.tcx", "coach_input"] },
      latency: { pipeline_ms: null, request_to_ready_observed_ms: null } });
    expect(posts).toHaveLength(1);
  });

  it("uses the original shared job request time and can observe readiness after polling stops", async () => {
    const first = await service().request(NIGHT);
    const job = (await coordinator.freshJob(String(first.request_id)))!;
    expect(JSON.parse(job.request).requestedAt).toBe(NOW);
    for (let i = 0; i < 3; i++) await service().status((await coordinator.freshJob(job.request_id))!);
    await night(); await check(NIGHT); runState = { ...RUN, status: "completed", conclusion: "success" };
    const later = await service(fetcher, async () => [], NOW + 60_000).status((await coordinator.freshJob(job.request_id))!);
    expect(later).toMatchObject({ data_ready: true, latency: { request_elapsed_ms: 60000,
      request_to_ready_observed_ms: 60000 }, sync_status: { data_state: "ready", job_state: "completed" } });
    expect(posts).toHaveLength(1);
  });

  it("returns explicit status-access failure without dispatching another job", async () => {
    const first = await service().request(NIGHT);
    const denied: typeof fetch = async () => new Response(null, { status: 403 });
    expect(await service(denied).status((await coordinator.freshJob(String(first.request_id)))!))
      .toMatchObject({ reason: "status_error", terminal: false, should_continue_polling: false,
        sync_status: { job_state: "unknown", user_action_required: true, next_action: "fix_configuration" } });
    expect(posts).toHaveLength(1);
  });

  it("does not mistake another local day's canonical workout for today's upload", async () => {
    await activity("1", "2026-10-04"); await check(REQUEST, 30_000, { status: "expected_activity_missing" });
    const result = await service().request(REQUEST);
    expect(result).toMatchObject({ data_ready: false, reason: "negative_cooldown",
      freshness: { status: "expected_activity_missing", missing_components: ["matching_local_activity"] } });
    expect(posts).toHaveLength(0);
  });

  it("reports ambiguity without downloading either candidate", async () => {
    await activity("1"); await activity("2");
    const result = await service().request(REQUEST);
    expect(result).toMatchObject({ data_ready: false, freshness: { status: "ambiguous_activity" } });
    expect(record(record(result.freshness).package).candidate_ids).toEqual(expect.arrayContaining(["garmin-1", "garmin-2"]));
    expect(posts).toHaveLength(0);
  });

  it("detects a missing artifact and targets only the recent activity", async () => {
    await activity(); await env.SLIPSTREAM_DATA.delete("activities/2026/1/activity.tcx");
    const result = await service().request(REQUEST);
    expect(result).toMatchObject({ data_ready: false, freshness: { complete: false,
      missing_components: ["activity.tcx", "coach_input"] } });
    expect(posts).toHaveLength(1);
  });

  it("repairs coach input from R2 when a newer explicit context arrives", async () => {
    await activity(); await check();
    await put("activities/2026/1/context/v1/20261005-test.json", { context_id: "test-context", note: "Synthetic note" });
    const result = await service().request(REQUEST);
    expect(posts).toHaveLength(1);
    expect(posts[0].inputs).toMatchObject({ latest_r2_only: true, latest_activity_id: "1" });
    expect(result).toMatchObject({ data_ready: false, freshness: { missing_components: ["coach_input"] } });
  });

  it("a workflow success with no data never means requested data are ready", async () => {
    runState = { ...RUN, status: "completed", conclusion: "success" };
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ terminal: true, data_ready: false, should_continue_polling: false });
    expect(String(result.message)).toContain("Missing:");
    expect(posts[0].inputs).toMatchObject({ latest_night_only: true, latest_wake_date: DAY });
  });

  it("serves canonical sleep and associated HRV even when the monthly indexes are absent", async () => {
    await night(); await check(NIGHT);
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ data_ready: true, freshness: { complete: true,
      package: { wake_date: DAY, sleep: { night_of: "2026-10-04", local_time_source: "garmin_local" } } } });
    expect(result.diagnostics).toMatchObject({ canonical_json_gets: 3, canonical_lists: 0, github_requests: 0 });
    expect(posts).toHaveLength(0);
  });

  it.each([300_000, 300_001])("retains the five-minute boundary at %s ms", async age => {
    await night(); await check(NIGHT, age);
    const snapshot = await service().snapshot(NIGHT);
    expect(snapshot).toMatchObject({ complete: true, source_fresh: age === 300_000,
      source_freshness_reason: age === 300_000 ? "within_ttl" : "ttl_expired" });
    expect(posts).toHaveLength(0); expect(gets).toBe(0);
  });

  it("does not treat a future-dated stored check as expired or fresh", async () => {
    await night(); await check(NIGHT, -60_000);
    expect(await service().snapshot(NIGHT)).toMatchObject({ complete: true,
      source_fresh: false, source_freshness_reason: "invalid_check_time" });
  });

  it("keeps partial sleep and missing detailed HRV explicit", async () => {
    await night();
    await put(`health/hrv/2026/10/${DAY}.json`, { date: DAY, summary: { lastNightAvg: 51 }, readings: [] });
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ data_ready: false, freshness: { missing_components: ["hrv_readings"] } });
    expect(posts).toHaveLength(1);
  });

  it.each(["reversed_window", "missing_stages"])("rejects incomplete sleep: %s", async (defect) => {
    await night(); await check(NIGHT);
    const key = `health/sleep/v1/2026/10/${DAY}.json`;
    const stored = record((await json([key]))?.data);
    await put(key, { ...stored, ...(defect === "missing_stages" ? { stages: [] }
      : { sleep_start_gmt: "2026-10-05T05:00:00Z", sleep_start_garmin_local: "2026-10-05T07:00:00" }) });
    expect(await service().request(NIGHT)).toMatchObject({ data_ready: false,
      freshness: { missing_components: expect.arrayContaining(["complete_sleep"]) } });
    expect(posts).toHaveLength(0);
  });

  it("rejects HRV readings with invalid timestamps", async () => {
    await night(); await check(NIGHT);
    await put(`health/hrv/2026/10/${DAY}.json`, { date: DAY,
      summary: { lastNightAvg: 51 }, readings: [{ timestamp: "not-a-time", hrv_ms: 51 }] });
    expect(await service().request(NIGHT)).toMatchObject({ data_ready: false,
      freshness: { missing_components: ["hrv_readings"] } });
  });

  it("does not advance readiness from a current negative check over older canonical data", async () => {
    await night(); await check(NIGHT, 30_000, { status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" });
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ data_ready: false, reason: "negative_cooldown" });
    expect(posts).toHaveLength(0);
  });

  it("reports failed/cancelled jobs and keeps the cooldown", async () => {
    runState = { ...RUN, status: "completed", conclusion: "cancelled" };
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ data_ready: false, terminal: true });
    expect(await service().request(NIGHT)).toMatchObject({ accepted: false, reason: "cooldown" });
    expect(posts).toHaveLength(1);
  });

  it("retains uncertain dispatches and cannot attach the unrelated latest workflow", async () => {
    const result = await service(async () => { throw new Error("synthetic connection lost"); }).request(NIGHT);
    expect(result).toMatchObject({ reason: "dispatch_unconfirmed", should_continue_polling: false });
    expect(await coordinator.activeFreshJob(freshScope(NIGHT), NOW)).toMatchObject({ request_id: result.request_id, run_id: null });
    expect(await service().request(NIGHT)).toMatchObject({ request_id: result.request_id, run: null });
    expect(posts).toHaveLength(0); expect(gets).toBe(0);
  });

  it.each([NIGHT, REQUEST])("recovers an uncertain $kind dispatch and reports an expired confirmed check", async request => {
    let dispatches = 0;
    const uncertain: typeof fetch = async () => { dispatches++; throw new Error("synthetic response lost"); };
    const first = await service(uncertain).request(request);
    expect(first).toMatchObject({ reason: "dispatch_unconfirmed", terminal: false,
      sync_status: { job_start_confirmed: false, current_job_source_checked: null } });
    const id = String(first.request_id);
    expect(await service().request(request)).toMatchObject({ request_id: id, run: null,
      reason: "dispatch_unconfirmed", should_continue_polling: false });
    for (let i = 0; i < 2; i++) await service().request(request);
    // The receipt arrives later, after the same reservation has been reused.
    await put(`refresh/requests/${id}.json`, { scope: freshScope(request), run_id: RUN.id });
    if (request.kind === "night") await night(); else await activity();
    await check(request, -60_000);
    const checkedAt = new Date(NOW + 60_000).toISOString();
    await put(`refresh/reports/${RUN.id}.json`, request.kind === "night"
      ? { schema_version: 1, kind: "latest-night", wake_date: DAY, scope: freshScope(request),
        checked_at: checkedAt, source_checked: true, status: "stored", sleep_status: "stored", hrv_status: "stored" }
      : { schema_version: 1, kind: "latest-activity", scope: freshScope(request), checked_at: checkedAt,
        source_checked: true, status: "ready", activity_id: "garmin-1", activity_date: DAY,
        activity_started_at_utc: `${DAY}T08:00:00Z`, activity_started_at_garmin_local: `${DAY}T10:00:00`,
        expected_date: DAY, already_in_slipstream: true, activity_name: "Synthetic run",
        summary_updated: false, files_ready: true, file_status: "existing", coach_status: "ready" });
    runState = { ...RUN, status: "completed", conclusion: "success" };
    const recovered = await service(fetcher, async () => [], NOW + 10 * 60_000)
      .status((await coordinator.freshJob(id))!);
    expect(freshResultSchema.safeParse(recovered).success).toBe(true);
    expect(recovered).toMatchObject({ request_id: id, run: { id: RUN.id }, terminal: true,
      data_ready: false, should_continue_polling: false,
      freshness: { complete: true, source_fresh: false, source_checked_at: checkedAt,
        source_age_seconds: 540, source_freshness_reason: "ttl_expired", missing_components: [] },
      sync_status: { job_state: "completed", job_start_confirmed: true, source_checked: true,
        current_job_source_checked: true, current_job_source_checked_at: checkedAt,
        stored_source_checked_at: checkedAt, complete: true, fresh: false, data_state: "stale" } });
    expect(String(recovered.message)).toContain(checkedAt);
    expect(String(recovered.message)).toContain("five-minute freshness window has expired");
    expect(String(recovered.message)).not.toContain("job success alone");
    expect(await coordinator.freshJob(id)).toMatchObject({ run_id: RUN.id, state: "completed", polls: 3 });
    expect(dispatches).toBe(1); expect(posts).toHaveLength(0); expect(gets).toBe(1);
  });

  it.each([undefined, false])("keeps a stored check separate from current-job evidence %s", async sourceChecked => {
    const first = await service().request(NIGHT);
    await night(); await check(NIGHT);
    const older = new Date(NOW - 30_000).toISOString();
    if (sourceChecked !== undefined) await put(`refresh/reports/${RUN.id}.json`, {
      schema_version: 1, kind: "latest-night", wake_date: DAY, scope: freshScope(NIGHT),
      checked_at: new Date(NOW).toISOString(), source_checked: false, status: "pending",
      sleep_status: "stored", hrv_status: "import_error" });
    runState = { ...RUN, status: "completed", conclusion: "success" };
    const result = await service().status((await coordinator.freshJob(String(first.request_id)))!);
    expect(result).toMatchObject({ sync_status: { current_job_source_checked: sourceChecked ?? null,
      current_job_source_checked_at: null, stored_source_checked_at: older },
      freshness: { source_checked_at: older, source_fresh: sourceChecked !== false,
        source_freshness_reason: sourceChecked === false ? "current_check_failed" : "within_ttl" } });
    expect(String(result.message)).toContain(older);
    expect(posts).toHaveLength(1);
  });

  it("ends a rejected dispatch without permanently blocking later attempts", async () => {
    const rejected: typeof fetch = async () => new Response(null, { status: 422 });
    const result = await service(rejected).request(NIGHT);
    expect(result).toMatchObject({ accepted: false, terminal: true, reason: "dispatch_rejected" });
    expect(await coordinator.activeFreshJob(freshScope(NIGHT), NOW)).toBeNull();
    expect(await service().status((await coordinator.freshJob(String(result.request_id)))!))
      .toMatchObject({ terminal: true, data_ready: false });
    expect(await service().request(NIGHT)).toMatchObject({ reason: "cooldown" });
    expect(await service(fetcher, async () => [], NOW + 5 * 60_000).request(NIGHT))
      .toMatchObject({ accepted: true });
    expect(posts).toHaveLength(1);
  });

  it("retains an accepted dispatch when the follow-up run lookup is rejected", async () => {
    const unavailableDetails: typeof fetch = async (_url, init) => init?.method === "POST"
      ? Response.json({ workflow_run_id: 44 }) : new Response(null, { status: 403 });
    const result = await service(unavailableDetails).request(NIGHT);
    expect(result).toMatchObject({ accepted: true, terminal: false, reason: "dispatch_unconfirmed" });
    expect(await coordinator.activeFreshJob(freshScope(NIGHT), NOW))
      .toMatchObject({ state: "active", request_id: result.request_id });
    expect(await service().request(NIGHT)).toMatchObject({ request_id: result.request_id,
      reason: "dispatch_unconfirmed", sync_status: { job_start_confirmed: false } });
    await put(`refresh/requests/${result.request_id}.json`, { run_id: RUN.id, scope: freshScope(NIGHT) });
    expect(await service().status((await coordinator.freshJob(String(result.request_id)))!))
      .toMatchObject({ run: { id: RUN.id }, sync_status: { job_start_confirmed: true } });
    expect(posts).toHaveLength(0);
  });

  it("resolves a 204 dispatch with its own persisted receipt only", async () => {
    const receiptFetch: typeof fetch = async (_url, init) => {
      if (init?.method === "POST") {
        const id = record(JSON.parse(String(init.body)).inputs).sync_request_id;
        await put(`refresh/requests/${id}.json`, { run_id: 44, scope: freshScope(NIGHT) });
        return new Response(null, { status: 204 });
      }
      return Response.json(RUN);
    };
    expect(await service(receiptFetch).request(NIGHT)).toMatchObject({ run: { id: 44 } });
  });

  it("cannot attach a receipt for another requested scope", async () => {
    const mismatched: typeof fetch = async (_url, init) => {
      if (init?.method === "POST") {
        const id = record(JSON.parse(String(init.body)).inputs).sync_request_id;
        await put(`refresh/requests/${id}.json`, { run_id: 44, scope: freshScope(REQUEST) });
        return new Response(null, { status: 204 });
      }
      throw new Error("Unexpected lookup of another scope's run");
    };
    expect(await service(mismatched).request(NIGHT)).toMatchObject({ run: null, terminal: false });
  });

  it("cannot attach a receipt for another UUID even when its scope matches", async () => {
    const first = await service(async () => new Response(null, { status: 204 })).request(NIGHT);
    await put(`refresh/requests/${crypto.randomUUID()}.json`, { run_id: RUN.id, scope: freshScope(NIGHT) });
    expect(await service().status((await coordinator.freshJob(String(first.request_id)))!))
      .toMatchObject({ run: null, reason: "dispatch_unconfirmed" });
    expect(gets).toBe(0); expect(posts).toHaveLength(0);
  });

  it("rejects receipt lookup from a different workflow without another dispatch", async () => {
    const first = await service(async () => new Response(null, { status: 204 })).request(NIGHT);
    await put(`refresh/requests/${first.request_id}.json`, { run_id: RUN.id, scope: freshScope(NIGHT) });
    runState = { ...RUN, path: ".github/workflows/unrelated.yml" };
    const result = await service().status((await coordinator.freshJob(String(first.request_id)))!);
    expect(result).not.toHaveProperty("run");
    expect(result).toMatchObject({ reason: "status_error",
        sync_status: { job_start_confirmed: null, current_job_source_checked: null } });
    expect(gets).toBe(1); expect(posts).toHaveLength(0);
  });

  it("uses IANA local dates and refuses invalid dates or implicit UTC", () => {
    expect(localToday("Pacific/Auckland", Date.parse("2026-10-04T13:00:00Z"))).toBe(DAY);
    expect(() => localToday(null, NOW)).toThrow("explicit Garmin-local date");
    for (const day of ["2026-02-30", "2026-09-01", "2026-10-07"]) expect(() => validateRecentDate(day, NOW)).toThrow();
  });

  it("enforces the shared daily budget before dispatch", async () => {
    for (let i = 0; i < 12; i++) {
      await coordinator.reserveBudgeted(`seed-${i}`, "sync-latest-night", NOW, 1, 12);
    }
    expect(await service().request(NIGHT)).toMatchObject({ accepted: false, reason: "daily_limit", data_ready: false });
    expect(posts).toHaveLength(0);
  });

  it("keeps incompatible activity IDs separate without bypassing the cooldown", async () => {
    await service().request({ ...REQUEST, activityId: "1" });
    const result = await service().request({ ...REQUEST, activityId: "2" });
    expect(result).toMatchObject({ accepted: false, reason: "cooldown" });
    expect(result.request_id).toBeUndefined();
    expect(posts).toHaveLength(1);
  });

  it("requires coach revision repair after a canonical endurance replacement", async () => {
    await activity(); await check();
    await put("activities/2026/1/activity.endurance.v1.json", { activity: { id: "1" },
      available: true, summary: { distance_m: 5100 } });
    const result = await service().request(REQUEST);
    expect(result).toMatchObject({ data_ready: false, freshness: { missing_components: ["coach_input"] } });
    expect(posts[0].inputs).toMatchObject({ latest_r2_only: true });
  });

  it("does not label a wrong local wake-date as complete even if the key matches", async () => {
    await night(); await check(NIGHT);
    const stored = record((await json([`health/sleep/v1/2026/10/${DAY}.json`]))?.data);
    await put(`health/sleep/v1/2026/10/${DAY}.json`, { ...stored, sleep_end_garmin_local: "2026-10-04T23:00:00" });
    expect(await service().request(NIGHT)).toMatchObject({ data_ready: false,
      freshness: { missing_components: ["local_wake_date"] } });
  });
});
