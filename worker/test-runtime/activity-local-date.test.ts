import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { R2Storage } from "../src/r2-storage";
import { ActivityCoachTools } from "../src/activity-coach-tools";
import { FreshDataService } from "../src/fresh-data";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { outputSchemas } from "../src/mcp-output";

const issuer = "https://activity-calendar-synthetic.cloudflareaccess.com";
let privateKey: CryptoKey;
const reads: string[] = [], writes: string[] = [];
let allowContextLookup = false;
const contextLists: { prefix: string; limit: number }[] = [];
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, field) {
    const method = Reflect.get(target, field);
    if (typeof method !== "function") return method;
    return (...args: unknown[]) => {
      if (field === "get") reads.push(String(args[0]));
      if (field === "put") writes.push(String(args[0]));
      if (field === "list") {
        const options = args[0] as Record<string, unknown> | undefined;
        const prefixes = ["activities/2027/73/context/v1/", "activities/2026/73/context/v1/"];
        if (!allowContextLookup || !options || !prefixes.includes(String(options.prefix))
          || options.limit !== 100 || Object.keys(options).some(key => key !== "prefix" && key !== "limit")) {
          throw new Error("Calendar resolution must not scan R2");
        }
        contextLists.push({ prefix: String(options.prefix), limit: options.limit });
      }
      return Reflect.apply(method, target, args);
    };
  },
});
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "calendar", alg: "RS256", use: "sig" };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("Calendar test external request forbidden");
    return Response.json({ keys: [jwk] });
  });
});
afterAll(() => vi.restoreAllMocks());
beforeEach(async () => {
  allowContextLookup = false;
  contextLists.length = 0;
  for (const year of ["2026", "2027"]) {
    const contexts = await env.SLIPSTREAM_DATA.list({ prefix: `activities/${year}/73/context/v1/` });
    if (contexts.objects.length) await env.SLIPSTREAM_DATA.delete(contexts.objects.map(item => item.key));
  }
  await env.SLIPSTREAM_DATA.put("summary/activities.csv", "Activity ID,Activity Date,Activity Name,Activity Type,Source\n"
    + "garmin-73,2026-12-31 23:30:00,Synthetic run,Run,garmin\n");
  await env.SLIPSTREAM_DATA.put("activities/2027/73/activity.v1.json", JSON.stringify({
    schema_version: 1, source: "garmin-fit", decode_errors: [], activity: {
      id: "73", type: "running", start_time_gmt: "2026-12-31 23:30:00", start_time_local: "2027-01-01 00:30:00",
    }, normalized_strength_session: { sets: [] },
  }));
  await env.SLIPSTREAM_DATA.put("activities/2027/73/activity.endurance.v1.json", JSON.stringify({ available: true }));
  clearSummaryCaches(); reads.length = 0; writes.length = 0;
});
async function call(name: string, args: Record<string, unknown> = { activity_id: "73" }, protocol = "2025-03-26") {
  const modern = protocol === "2026-07-28";
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "calendar" }).setIssuer(issuer).setAudience("calendar")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const context = createExecutionContext();
  const response = await worker.fetch(new Request("https://calendar.example/mcp", {
    method: "POST", headers: { host: "calendar.example", "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}),
    } }),
  }), { ...env, SLIPSTREAM_DATA: bucket, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "calendar",
    MCP_HOSTNAME: "calendar.example", MCP_WRITES_ENABLED: "true", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } }, context);
  const body = await response.text();
  await waitOnExecutionContext(context);
  expect(response.status, body).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = body.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  expect(message.error, body).toBeUndefined();
  expect(message.result.isError, body).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}
it("reads the local-year endurance object and reuses the resolved FIT", async () => {
  const result = await call("endurance_session");
  expect(result.available).toBe(true);
  expect(result.activity).toMatchObject({ date: "2027-01-01", date_source: "garmin_local" });
  expect(reads.filter(key => key === "activities/2027/73/activity.v1.json")).toHaveLength(1);
  expect(writes).toEqual([]);
});
it.each(["2025-03-26", "2026-07-28"])("%s: saves a legacy activity's context in the producer's local-year namespace", async protocol => {
  allowContextLookup = true;
  const result = await call("add_activity_context", { activity_id: "73", rpe: 7 }, protocol);
  allowContextLookup = false;
  expect(result.saved).toBe(true);
  expect(result.key).toMatch(/^activities\/2027\/73\/context\/v1\//);
  expect(result.context).toMatchObject({ schema_version: 1, activity_date: "2027-01-01",
    date_source: "garmin_local", rpe: 7 });
  expect(contextLists).toEqual([
    { prefix: "activities/2027/73/context/v1/", limit: 100 },
    { prefix: "activities/2026/73/context/v1/", limit: 100 },
    // The successful receipt resolves current context independently of analysis.
    { prefix: "activities/2027/73/context/v1/", limit: 100 },
    { prefix: "activities/2026/73/context/v1/", limit: 100 },
  ]);
  expect(writes).toEqual([result.key]);
  const list = await call("list_activities", {});
  expect(list.activities[0]).toMatchObject({ date: "2026-12-31", date_source: "utc_fallback" });
});

it("keeps old UTC-year context visible to the local-date freshness reader", async () => {
  const old = { activity_id: "73", context_id: "ctx-old", rpe: 7 };
  await env.SLIPSTREAM_DATA.put("activities/2026/73/context/v1/20270102T120000-ctx-old.json", JSON.stringify(old));
  const storage = new R2Storage(env.SLIPSTREAM_DATA);
  const tools = new ActivityCoachTools(env, "synthetic", storage);
  const fresh = new FreshDataService(env, {
    getActivities: () => storage.getActivities(),
    getR2Json: (keys, limits) => storage.getR2Json(keys, limits),
    getCoachProfiles: () => tools.getCoachProfiles(),
  }, { repository: "", workflow: "refresh.yml", ref: "main", token: "", cooldownMinutes: 30 },
  () => Date.parse("2027-01-02T12:00:00Z"));
  const snapshot = await fresh.snapshot({ kind: "activity", activityId: "73", date: "2027-01-01" });
  expect(snapshot.package.activity_date).toBe("2027-01-01");
  expect(snapshot.package.user_context).toEqual(old);
  expect(writes).toEqual([]);
});

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`${protocol}: activity_stats keeps nonempty all-missing totals unknown`, async () => {
    const csv = "Activity ID,Activity Date,Activity Name,Activity Type,Source,Distance,Moving Time,Elevation Gain,Calories\n"
      + "garmin-73,2026-12-31 23:30:00,Synthetic missing run,Run,garmin,,,,\n";
    await env.SLIPSTREAM_DATA.put("summary/activities.csv", csv);
    clearSummaryCaches(); reads.length = 0; writes.length = 0;
    const result = await call("activity_stats", {}, protocol);
    expect(outputSchemas.activity_stats.parse(result)).toEqual(result);
    expect(result.overall).toEqual({ activities: 1, total_distance_km: null, total_moving_time: null,
      total_elevation_gain_m: null, total_calories: null, avg_hr: null });
    expect(writes).toEqual([]);
    expect(await (await env.SLIPSTREAM_DATA.get("summary/activities.csv"))?.text()).toBe(csv);
  });
  const statsHeader = "Activity ID,Activity Date,Activity Name,Activity Type,Source,Distance,Moving Time,Elevation Gain,Calories,Average Heart Rate\n";
  const recorded = "garmin-81,2026-09-01 06:00:00,Synthetic recorded,Run,garmin,1.234,61,1.26,2.4,120";
  const complete = { activities: 2, total_distance_km: 3.58, total_moving_time: "2:03",
    total_elevation_gain_m: 3.5, total_calories: 6, avg_hr: 120 };
  it.each([
    { label: "partial distance", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic partial,Ride,garmin,,62,2.26,3.4,"],
      args: {}, expected: { overall: { ...complete, total_distance_km: null } } },
    { label: "partial moving time", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic partial,Ride,garmin,2.345,,2.26,3.4,"],
      args: {}, expected: { overall: { ...complete, total_moving_time: null } } },
    { label: "partial elevation", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic partial,Ride,garmin,2.345,62,,3.4,"],
      args: {}, expected: { overall: { ...complete, total_elevation_gain_m: null } } },
    { label: "partial calories", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic partial,Ride,garmin,2.345,62,2.26,,"],
      args: {}, expected: { overall: { ...complete, total_calories: null } } },
    { label: "recorded zero", rows: [
      "garmin-83,2026-09-01 06:00:00,Synthetic zero,Run,garmin,0,0,0,0,0"],
      args: {}, expected: { overall: { activities: 1, total_distance_km: 0, total_moving_time: "0:00",
        total_elevation_gain_m: 0, total_calories: 0, avg_hr: 0 } } },
    { label: "empty selection", rows: [],
      args: {}, expected: { overall: { activities: 0, total_distance_km: 0, total_moving_time: "0:00",
        total_elevation_gain_m: 0, total_calories: 0, avg_hr: null } } },
    { label: "complete rounded totals and known HR average", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic complete,Ride,garmin,2.345,62,2.26,3.4,"],
      args: {}, expected: { overall: complete } },
    { label: "nonfinite totals remain unknown", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic invalid,Ride,garmin,Infinity,Infinity,Infinity,Infinity,"],
      args: {}, expected: { overall: { activities: 2, total_distance_km: null, total_moving_time: null,
        total_elevation_gain_m: null, total_calories: null, avg_hr: 120 } } },
    { label: "sport groups keep their own completeness", rows: [recorded,
      "garmin-82,2027-10-02 07:00:00,Synthetic missing,Ride,garmin,,,,,"],
      args: { group_by: "sport" }, expected: {
        overall: { activities: 2, total_distance_km: null, total_moving_time: null,
          total_elevation_gain_m: null, total_calories: null, avg_hr: 120 },
        by_sport: {
          Ride: { activities: 1, total_distance_km: null, total_moving_time: null,
            total_elevation_gain_m: null, total_calories: null, avg_hr: null },
          Run: { activities: 1, total_distance_km: 1.23, total_moving_time: "1:01",
            total_elevation_gain_m: 1.3, total_calories: 2, avg_hr: 120 },
        },
      } },
  ])(`${protocol}: activity_stats full totals contract: $label`, async ({ rows, args, expected }) => {
    const csv = statsHeader + rows.join("\n") + (rows.length ? "\n" : "");
    await env.SLIPSTREAM_DATA.put("summary/activities.csv", csv);
    clearSummaryCaches(); reads.length = 0; writes.length = 0;
    const result = await call("activity_stats", args, protocol);
    expect(outputSchemas.activity_stats.parse(result)).toEqual(result);
    expect(result).toEqual(expected);
    expect(writes).toEqual([]);
    expect(await (await env.SLIPSTREAM_DATA.get("summary/activities.csv"))?.text()).toBe(csv);
  });
  it.each([[2, "garmin-fit"], [true, "garmin-fit"], [1, "other-source"], [1, null], [null, "garmin-fit"]])(
    `does not claim a local day from schema=%s source=%s through ${protocol}`, async (version, source) => {
      await env.SLIPSTREAM_DATA.put("activities/2027/73/activity.v1.json", JSON.stringify({
        schema_version: version, source, activity: { id: "73", start_time_gmt: "2026-12-31 23:30:00",
          start_time_local: "2027-01-01 00:30:00" }, normalized_strength_session: { sets: [] },
      }));
      const result = await call("strength_session", { activity_id: "73" }, protocol);
      expect(result.available).toBe(false);
      expect(result.activity).toMatchObject({ date: "2026-12-31", date_source: "utc_fallback" });
      expect(writes).toEqual([]);
    });
  it.each(["2027-01-01", "2026-12-31"])(`verifies the resolved local day against marked coach date %s through ${protocol}`, async day => {
    const id = "b".repeat(24), key = `activities/2027/73/coach-input/v1/canonical/${id}.json`;
    await env.SLIPSTREAM_DATA.put(key, JSON.stringify({ schema_version: 1, analysis_id: id,
      activity_id: "73", activity: { date: day }, profile: { profile_id: "new-year-profile" } }));
    await env.SLIPSTREAM_DATA.put("activities/2027/73/coach-input/v1/latest-ready.json", JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: key,
    }));
    const result = await call("coach_input", { activity_id: "73" }, protocol);
    expect(result.available).toBe(day === "2027-01-01");
    expect(result.activity).toMatchObject({ date: "2027-01-01", date_source: "garmin_local" });
    if (result.available) expect(result.analysis.profile.profile_id).toBe("new-year-profile");
    else expect(result.analysis).toBeUndefined();
    expect(writes).toEqual([]);
  });
}

it.each([[2, "garmin-fit"], [true, "garmin-fit"], [1, "other-source"], [1, null], [null, "garmin-fit"]])(
  "freshness reader does not select a local calendar from schema=%s source=%s", async (version, source) => {
    await env.SLIPSTREAM_DATA.put("activities/2027/73/activity.v1.json", JSON.stringify({
      schema_version: version, source, activity: { id: "73", type: "running",
        start_time_gmt: "2026-12-31 23:30:00", start_time_local: "2027-01-01 00:30:00" },
    }));
    const storage = new R2Storage(env.SLIPSTREAM_DATA);
    const profileRead = vi.fn(async () => ({ profiles: [] }));
    const fresh = new FreshDataService(env, { getActivities: () => storage.getActivities(),
      getR2Json: (keys, limits) => storage.getR2Json(keys, limits), getCoachProfiles: profileRead,
    }, { repository: "", workflow: "refresh.yml", ref: "main", token: "", cooldownMinutes: 30 },
    () => Date.parse("2027-01-02T12:00:00Z"));
    const snapshot = await fresh.snapshot({ kind: "activity", activityId: "73", date: "2027-01-01" });
    expect(snapshot.package.activity_date).toBeUndefined();
    expect(snapshot.complete).toBe(false);
    expect(profileRead).not.toHaveBeenCalled();
    expect(writes).toEqual([]);
  });
