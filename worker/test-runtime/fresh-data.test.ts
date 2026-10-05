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
function service(customFetch = fetcher) {
  return new FreshDataService(testEnv, { getActivities: async () => [], getR2Json: json,
    getCoachProfiles: async () => ({ keys: ["test-profile"], profiles: [PROFILE] }) },
  CONFIG, () => NOW, customFetch, async () => {});
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

  it("keeps partial sleep and missing detailed HRV explicit", async () => {
    await night();
    await put(`health/hrv/2026/10/${DAY}.json`, { date: DAY, summary: { lastNightAvg: 51 }, readings: [] });
    const result = await service().request(NIGHT);
    expect(result).toMatchObject({ data_ready: false, freshness: { missing_components: ["hrv_readings"] } });
    expect(posts).toHaveLength(1);
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
