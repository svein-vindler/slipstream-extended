import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";

const issuer = "https://synthetic-write-calendar.cloudflareaccess.com";
const hostname = "write-calendar.example";
const profile = { name: "Synthetic calendar profile", effective_from: "2026-01-01",
  zones: [{ label: "Synthetic zone", min_bpm: 90, max_bpm: 120 }], references: {} };
let privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env, actor: string;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let counts: { get: number; list: number; put: number; get_body_bytes: number; put_body_bytes: number };
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "calendar", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  for (const prefix of ["coach/", "activities/"]) {
    const listed = await env.SLIPSTREAM_DATA.list({ prefix, limit: 1000 });
    if (listed.objects.length) await env.SLIPSTREAM_DATA.delete(listed.objects.map(item => item.key));
  }
  clearSummaryCaches(); actor = crypto.randomUUID();
  counts = { get: 0, list: 0, put: 0, get_body_bytes: 0, put_body_bytes: 0 };
  coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, property) {
    if (property === "getByName") return () => coordinator;
    return Reflect.get(target, property);
  } });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const method = Reflect.get(target, property);
    if (typeof method !== "function") return method;
    return async (...params: unknown[]) => {
      if (property === "get") counts.get++;
      if (property === "list") counts.list++;
      if (property === "put") { counts.put++; counts.put_body_bytes += new TextEncoder().encode(String(params[1])).length; }
      const result = await Reflect.apply(method, target, params);
      if (property === "get" && result) counts.get_body_bytes += result.size;
      return result;
    };
  } });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "calendar", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "true", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    SLIPSTREAM_DATA: bucket, REFRESH_COORDINATOR: namespace };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === issuer + "/cdn-cgi/access/certs") return Response.json({ keys: [jwk] });
    throw new Error("External network forbidden in synthetic write calendar tests");
  });
});
afterEach(() => vi.restoreAllMocks());

async function call(name: string, values: unknown, protocol = "2025-03-26") {
  const modern = protocol === "2026-07-28";
  const authNow = Math.floor(new Date().getTime() / 1000);
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "calendar" }).setIssuer(issuer).setAudience("calendar")
    .setSubject(actor).setIssuedAt(authNow).setExpirationTime(authNow + 300).sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp", {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: values,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const body = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = body.split("\n").find(item => item.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  expect(message.error).toBeUndefined();
  if (message.result.structuredContent)
    expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result;
}
async function debits() {
  return runInDurableObject(coordinator, (_instance, state) => state.storage.sql.exec<{ used: number }>(
    "SELECT used FROM daily_budgets").toArray().reduce((sum, item) => sum + item.used, 0));
}

it("rejects an impossible profile calendar day before storage or accepted budget", async () => {
  const result = await call("add_coach_profile", { ...profile, effective_from: "2026-02-30" });
  console.log("C12_WRITE_METRIC " + JSON.stringify({ case: "invalid_profile", ...counts,
    accepted_write_debits: await debits(), saved: result.structuredContent?.saved === true, actual_provider_calls: 0 }));
  expect(result.isError).toBe(true);
  expect(JSON.stringify(result)).toContain("Invalid calendar date");
  expect(counts.put).toBe(0); expect(await debits()).toBe(0);
});

it("rejects an unsupported derived context calendar day before storage or accepted budget", async () => {
  await env.SLIPSTREAM_DATA.put("summary/activities.csv",
    "Activity ID,Activity Date,Activity Name,Activity Type,Source\n73,0000-01-01 12:00:00,Synthetic legacy activity,Run,manual\n");
  const result = await call("add_activity_context", { activity_id: "73", note: "Synthetic context" });
  console.log("C12_WRITE_METRIC " + JSON.stringify({ case: "invalid_context", ...counts,
    accepted_write_debits: await debits(), saved: result.structuredContent?.saved === true, actual_provider_calls: 0 }));
  expect(result.isError).toBe(true);
  expect(JSON.stringify(result)).toContain("Invalid calendar date");
  expect(counts.put).toBe(0); expect(await debits()).toBe(0);
});

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["2026-2-01", "2026-01-01T12:00:00", "2026-01-01\n", "10000-01-01"])(
    `retains profile format rejection for %s through ${protocol}`, async day => {
      const result = await call("add_coach_profile", { ...profile, effective_from: day }, protocol);
      expect(result.isError).toBe(true);
      expect(counts.put).toBe(0); expect(await debits()).toBe(0);
    });

  it.each(["sync_latest_activity", "sync_latest_night"])(
    `keeps existing real-date rejection before fresh writes in %s through ${protocol}`, async name => {
      testEnv.GITHUB_ACTIONS_TOKEN = "synthetic-offline-token";
      testEnv.GITHUB_REPOSITORY = "synthetic/offline";
      // September 31 normalizes to October 1, inside the existing recent window.
      // Invalidity must be detected independently of the seven-day range check.
      vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-10-01T12:00:00Z"));
      const result = await call(name, name === "sync_latest_activity"
        ? { expected_date: "2026-09-31" } : { wake_date: "2026-09-31" }, protocol);
      expect(result.isError).toBe(true);
      expect(counts).toEqual({ get: 0, list: 0, put: 0, get_body_bytes: 0, put_body_bytes: 0 });
      expect(await debits()).toBe(0);
    });

  it.each(["2026-02-30", "2026-02-29", "1900-02-29", "2100-02-29", "2026-04-31",
    "2026-00-01", "2026-13-01", "2026-01-00", "2026-01-32", "0000-01-01"])(
    `rejects invalid profile date %s with useful guidance and no writes through ${protocol}`, async day => {
      const result = await call("add_coach_profile", { ...profile, effective_from: day }, protocol);
      expect(result.isError).toBe(true);
      expect(JSON.stringify(result)).toContain("Invalid calendar date. Use YYYY-MM-DD with a valid calendar day.");
      expect(counts).toEqual({ get: 0, list: 0, put: 0, get_body_bytes: 0, put_body_bytes: 0 });
      expect(await debits()).toBe(0);
      const accepted = await call("add_coach_profile", profile, protocol);
      expect(accepted.structuredContent.saved).toBe(true); expect(await debits()).toBe(1);
    });

  it.each(["0001-01-01", "0099-12-31", "0100-02-28", "1900-02-28", "2000-02-29", "2400-02-29", "9999-12-31"])(
    `preserves legitimate profile date %s and immutable reuse through ${protocol}`, async day => {
      const values = { ...profile, effective_from: day };
      const accepted = await call("add_coach_profile", values, protocol);
      expect(accepted.isError).not.toBe(true); expect(accepted.structuredContent.saved).toBe(true);
      expect(accepted.structuredContent.profile.effective_from).toBe(day);
      const original = await (await env.SLIPSTREAM_DATA.get(accepted.structuredContent.key))!.text();
      const repeated = await call("add_coach_profile", values, protocol);
      expect(repeated.structuredContent.profile).toEqual(accepted.structuredContent.profile);
      expect(counts.put).toBe(2); expect(await debits()).toBe(1);
      expect(await (await env.SLIPSTREAM_DATA.get(accepted.structuredContent.key))!.text()).toBe(original);
      const read = await call("coach_profile", { date: day }, protocol);
      expect(read.structuredContent.profile.profile_id).toBe(accepted.structuredContent.profile_id);
    });

  it(`rejects invalid derived context date and retains the next valid write through ${protocol}`, async () => {
    await seedActivity("0000-01-01", "", "manual");
    const rejected = await call("add_activity_context", { activity_id: "73", note: "Synthetic context" }, protocol);
    expect(rejected.isError).toBe(true); expect(JSON.stringify(rejected)).toContain("Invalid calendar date");
    expect(counts.put).toBe(0); expect(await debits()).toBe(0);
    await seedActivity("2000-02-29", "", "manual");
    const accepted = await call("add_activity_context", { activity_id: "73", note: "Synthetic context" }, protocol);
    expect(accepted.structuredContent.saved).toBe(true);
    expect(accepted.structuredContent.context.activity_date).toBe("2000-02-29");
    expect(counts.put).toBe(1); expect(await debits()).toBe(1);
  });

  it.each([
    ["2026-12-31", "2027-01-01", "garmin_local"],
    ["2000-02-29", "", "utc_fallback"],
    ["2026-03-08", "2026-03-08", "garmin_local"],
  ])(`preserves context date selection %s / %s through ${protocol}`, async (utc, local, provenance) => {
    await seedActivity(utc, local);
    const result = await call("add_activity_context", { activity_id: "73", rpe: 7 }, protocol);
    expect(result.structuredContent.saved).toBe(true);
    expect(result.structuredContent.context).toMatchObject({ activity_date: local || utc, date_source: provenance, rpe: 7 });
    expect(result.structuredContent.key).toMatch(new RegExp(`^activities/${(local || utc).slice(0, 4)}/73/context/v1/`));
    const stored = JSON.parse(await (await env.SLIPSTREAM_DATA.get(result.structuredContent.key))!.text());
    expect(stored).toEqual(result.structuredContent.context);
    expect(counts.put).toBe(1); expect(await debits()).toBe(1);
  });

  it(`does not revalidate or mutate an existing legacy profile through ${protocol}`, async () => {
    const key = "coach/profiles/v1/2026-02-30/legacy.json";
    const legacy = { ...profile, effective_from: "2026-02-30", schema_version: 1,
      profile_id: "legacy-invalid-date", sport: "running", source: "user", created_at: "2026-01-01T00:00:00Z" };
    const body = JSON.stringify(legacy);
    await env.SLIPSTREAM_DATA.put(key, body);
    const result = await call("coach_profile", {}, protocol);
    expect(result.structuredContent.profiles).toEqual([legacy]);
    const rejected = await call("add_coach_profile", { ...profile, effective_from: "2026-02-30" }, protocol);
    expect(rejected.isError).toBe(true);
    expect(await (await env.SLIPSTREAM_DATA.get(key))!.text()).toBe(body);
    expect(counts.put).toBe(0); expect(await debits()).toBe(0);
  });
}

async function seedActivity(utc: string, local: string, source = "garmin") {
  await env.SLIPSTREAM_DATA.put("summary/activities.csv",
    "Activity ID,Activity Date,Activity Name,Activity Type,Source,Activity Local Date\n"
    + `73,${utc} 12:00:00,Synthetic activity,Run,${source},${local}\n`);
  clearSummaryCaches();
}
