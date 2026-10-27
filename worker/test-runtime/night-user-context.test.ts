import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";

const issuer = "https://night-context-synthetic.cloudflareaccess.com";
const hostname = "night-context.example";
let key: CryptoKey;
let jwk: Record<string, unknown>;
let token: string;
let testEnv: Env;
let day: string;
let sequence = 0;
let lastHttpBodyBytes = 0;
const touchedDates = new Set<string>();
const sleepKey = (date: string) => `health/sleep/v1/${date.slice(0,4)}/${date.slice(5,7)}/${date}.json`;
const contextPrefix = (date: string) => `health/night-context/v1/${date.slice(0,4)}/${date.slice(5,7)}/${date}/`;
const summary = { sleep_seconds: 25200, deep_sleep_seconds: null, light_sleep_seconds: null,
  rem_sleep_seconds: null, awake_sleep_seconds: null, unmeasurable_sleep_seconds: null,
  nap_seconds: null, sleep_score: 80, average_spo2_percent: null, lowest_spo2_percent: null,
  average_respiration_brpm: null, lowest_respiration_brpm: null, highest_respiration_brpm: null,
  average_sleep_stress: null };

async function signedToken(subject = crypto.randomUUID()) {
  return new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "night-context" }).setIssuer(issuer).setAudience("night-context")
    .setSubject(subject).setIssuedAt().setExpirationTime("5m").sign(key);
}
async function seedNight(date: string) {
  touchedDates.add(date);
  await env.SLIPSTREAM_DATA.put(sleepKey(date), JSON.stringify({ schema_version: 1, source: "garmin", date,
    sleep_start_gmt: `${date}T00:00:00Z`, sleep_end_gmt: `${date}T07:00:00Z`,
    sleep_start_garmin_local: null, sleep_end_garmin_local: null, summary, stages: [] }));
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "night-context", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  touchedDates.clear();
  day = new Date(Date.UTC(2026, 10, ++sequence)).toISOString().slice(0,10);
  token = await signedToken();
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden");
    return Response.json({ keys: [jwk] });
  });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "night-context", MCP_HOSTNAME: hostname,
    HEALTH_TIMEZONE: "Europe/Oslo", MCP_WRITES_ENABLED: "true", MCP_WRITE_DAILY_LIMIT: "60",
    GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "" };
  await seedNight(day);
});
afterEach(async () => {
  vi.restoreAllMocks();
  for (const date of touchedDates) {
    await env.SLIPSTREAM_DATA.delete(sleepKey(date));
    const context = await env.SLIPSTREAM_DATA.list({ prefix: contextPrefix(date), limit: 101 });
    for (const object of context.objects) await env.SLIPSTREAM_DATA.delete(object.key);
  }
});

async function rpc(name: string, args: Record<string, unknown>, protocol: string, assertion = token) {
  const modern = protocol === "2026-07-28";
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": assertion, "mcp-protocol-version": protocol,
      ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); lastHttpBodyBytes = new TextEncoder().encode(text).byteLength; await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  return message.error ? { isError: true, content: [{ type: "text", text: message.error.message }] } : message.result;
}
async function success(name: string, args: Record<string, unknown>, protocol: string) {
  const result = await rpc(name,args,protocol);
  expect(result.isError, JSON.stringify(result)).not.toBe(true);
  expect(JSON.parse(result.content[0].text)).toEqual(result.structuredContent);
  return result.structuredContent;
}

describe("append-only explicit night context through authenticated HTTP", () => {
  for (const protocol of ["2025-03-26", "2026-07-28"]) {
    it(`saves explicit night context and immediately exposes it on the ordinary reader via ${protocol}`, async () => {
      const saved = await success("add_night_context", { wake_date: day, alcohol: true, morning_condition: 3 }, protocol);
      expect(saved).toMatchObject({ saved: true, wake_date: day,
        context: { wake_date: day, source: "user", alcohol: true, morning_condition: 3 } });
      const night = await success("sleep_detail", { date: day }, protocol);
      expect(night.user_context).toMatchObject({ wake_date: day, alcohol: true, morning_condition: 3, source: "user" });
      expect(night.view).toMatchObject({ context_status: "available", context_revision: saved.context_id });
    });
  }
});
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`distinguishes omitted actual zone from explicit clearing across later patches via ${protocol}`, async () => {
    await env.SLIPSTREAM_DATA.put(sleepKey(day), JSON.stringify({ date: day, actual_night_timezone: "Asia/Tokyo",
      sleep_start_gmt: `${day}T00:00:00Z`, sleep_end_gmt: `${day}T07:00:00Z`, summary, stages: [] }));
    const first = await success("add_night_context", { wake_date: day, alcohol: true }, protocol);
    expect(first.context.declared_fields).toEqual(["alcohol"]);
    const sourceZone = await success("sleep_detail", { date: day }, protocol);
    expect(sourceZone.actual_night_timezone).toBe("Asia/Tokyo");
    token = await signedToken();
    const cleared = await success("add_night_context", { wake_date: day, actual_night_timezone: null }, protocol);
    expect(cleared.context).toMatchObject({ alcohol: true, actual_night_timezone: null,
      declared_fields: ["actual_night_timezone", "alcohol"], changed_fields: ["actual_night_timezone"] });
    token = await signedToken();
    const third = await success("add_night_context", { wake_date: day, illness: false }, protocol);
    expect(third.context).toMatchObject({ alcohol: true, illness: false, actual_night_timezone: null,
      declared_fields: ["actual_night_timezone", "alcohol", "illness"], changed_fields: ["illness"] });
    const after = await success("sleep_detail", { date: day }, protocol);
    expect(after.actual_night_timezone).toBeNull();
    expect(after.time_quality_flags).toContain("actual_timezone_unknown");
    expect(after.view.context_revision).toBe(third.context_id);
    const history = await success("night_context_history", { wake_date: day, limit: 20 }, protocol);
    expect(history.status).toBe("available");
    expect(history.events).toHaveLength(3);
    expect(history.events.map((event: {context_id:string}) => event.context_id))
      .toEqual([third.context_id, cleared.context_id, first.context_id]);
    expect(history.events[2]).toMatchObject({ alcohol: true, illness: null });
    const page1 = await success("night_context_history", { wake_date: day, limit: 1 }, protocol);
    expect(page1).toMatchObject({ more: true, next_before_context_id: third.context_id });
    expect(page1.events.map((event: {context_id:string}) => event.context_id)).toEqual([third.context_id]);
    const page2 = await success("night_context_history", { wake_date: day, limit: 1, before_context_id: page1.next_before_context_id }, protocol);
    expect(page2).toMatchObject({ more: true, next_before_context_id: cleared.context_id });
    expect(page2.events.map((event: {context_id:string}) => event.context_id)).toEqual([cleared.context_id]);
    const page3 = await success("night_context_history", { wake_date: day, limit: 1, before_context_id: page2.next_before_context_id }, protocol);
    expect(page3).toMatchObject({ more: false, next_before_context_id: null });
    expect(page3.events.map((event: {context_id:string}) => event.context_id)).toEqual([first.context_id]);
  });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["2026-02-30", "0000-01-01", "2026-13-01"])(
    `rejects invalid calendar wake-date %s instead of reporting no context via ${protocol}`, async invalid => {
      const result = await rpc("night_context_history", { wake_date: invalid }, protocol);
      expect(result.isError, JSON.stringify(result)).toBe(true);
    });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`enforces literal write opt-in and rejects unauthenticated context writes via ${protocol}`, async () => {
    testEnv.MCP_WRITES_ENABLED = "TRUE";
    const disabled = await rpc("add_night_context", { wake_date: day, note: "blocked" }, protocol);
    expect(disabled.isError).toBe(true);
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history).toMatchObject({ status: "not_stored", events: [] });
    testEnv.MCP_WRITES_ENABLED = "true";
    const ctx = createExecutionContext();
    const denied = await worker.fetch(new Request(`https://${hostname}/mcp`, { method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call",
        params: { name: "add_night_context", arguments: { wake_date: day, note: "unauthenticated" } } }) }), testEnv, ctx);
    await waitOnExecutionContext(ctx);
    expect(denied.status).toBe(401);
    expect((await success("night_context_history", { wake_date: day }, protocol)).events).toEqual([]);
  });

  it(`uses the real actor cooldown without appending a duplicate via ${protocol}`, async () => {
    const first = await success("add_night_context", { wake_date: day, note: "first" }, protocol);
    const blocked = await rpc("add_night_context", { wake_date: day, illness: true }, protocol);
    expect(blocked.isError).toBe(true);
    expect(blocked.content[0].text).toContain("Retry in");
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history.events).toHaveLength(1);
    expect(history.events[0].context_id).toBe(first.context_id);
  });

  it(`shares the actor daily budget with existing activity context writes via ${protocol}`, async () => {
    testEnv.MCP_WRITE_DAILY_LIMIT = "1";
    await success("add_night_context", { wake_date: day, note: "uses shared budget" }, protocol);
    const now = Date.now(); vi.spyOn(Date, "now").mockReturnValue(now + 11000);
    const previous = await env.SLIPSTREAM_DATA.get("summary/activities.csv");
    const priorBody = previous ? await previous.text() : null;
    try {
      await env.SLIPSTREAM_DATA.put("summary/activities.csv", "Activity ID,Activity Date,Activity Name,Activity Type,Source,Activity Local Date\n"
        + `garmin-991001,${day} 12:00:00,synthetic,run,garmin,${day}\n`);
      const denied = await rpc("add_activity_context", { activity_id: "garmin-991001", note: "must fail shared budget" }, protocol);
      expect(denied.isError).toBe(true);
      expect(denied.content[0].text).toContain("daily MCP write safety limit");
      const history = await success("night_context_history", { wake_date: day }, protocol);
      expect(history.events).toHaveLength(1);
    } finally {
      if (priorBody === null) await env.SLIPSTREAM_DATA.delete("summary/activities.csv");
      else await env.SLIPSTREAM_DATA.put("summary/activities.csv", priorBody);
    }
  });

  it(`keeps current night data readable while context failure stays unavailable via ${protocol}`, async () => {
    await env.SLIPSTREAM_DATA.put(sleepKey(day), JSON.stringify({ date: day, actual_night_timezone: "Asia/Tokyo",
      sleep_start_gmt: `${day}T00:00:00Z`, sleep_end_gmt: `${day}T07:00:00Z`, summary, stages: [] }));
    await success("add_night_context", { wake_date: day, note: "saved" }, protocol);
    const originalList = env.SLIPSTREAM_DATA.list.bind(env.SLIPSTREAM_DATA);
    vi.spyOn(env.SLIPSTREAM_DATA, "list").mockImplementation(async options => {
      if (options?.prefix === contextPrefix(day)) throw new Error("synthetic context read failure");
      return originalList(options);
    });
    const night = await success("sleep_detail", { date: day }, protocol);
    expect(night.available).toBe(true);
    expect(night.user_context).toBeNull();
    expect(night.view.context_status).toBe("unavailable");
    expect(night.actual_night_timezone).toBeNull();
    expect(night.time_quality_flags).toContain("user_context_unavailable");
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history).toMatchObject({ status: "unavailable", events: [] });
  });

  it(`reports saved true when only the post-PUT effective reread fails via ${protocol}`, async () => {
    const originalList = env.SLIPSTREAM_DATA.list.bind(env.SLIPSTREAM_DATA);
    let lists = 0;
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "list").mockImplementation(async options => {
      if (options?.prefix === contextPrefix(day) && ++lists === 2) throw new Error("synthetic post-save read failure");
      return originalList(options);
    });
    const saved = await success("add_night_context", { wake_date: day, note: "known stored" }, protocol);
    expect(saved).toMatchObject({ saved: true, context: { note: "known stored" }, user_context: null,
      view: { context_status: "unavailable" } });
    spy.mockRestore();
    const current = await success("sleep_detail", { date: day }, protocol);
    expect(current.view).toMatchObject({ context_status: "available", context_revision: saved.context_id });
  });

  it(`retains a cross-actor pending claim after unknown PUT but isolates the next night via ${protocol}`, async () => {
    const originalPut = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (name, value, options) => {
      const written = await originalPut(name, value, options);
      if (name.startsWith(contextPrefix(day))) throw new Error("synthetic lost PUT acknowledgement");
      return written;
    });
    const uncertain = await rpc("add_night_context", { wake_date: day, note: "first may be stored" }, protocol);
    expect(uncertain.isError).toBe(true);
    spy.mockRestore();
    token = await signedToken();
    const blocked = await rpc("add_night_context", { wake_date: day, note: "must never replace" }, protocol);
    expect(blocked.isError).toBe(true);
    expect(blocked.content[0].text).toContain("pending");
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history.events).toHaveLength(1);
    expect(history.events[0].note).toBe("first may be stored");
    const next = new Date(Date.parse(`${day}T00:00:00Z`) + 86400000).toISOString().slice(0,10);
    await seedNight(next);
    token = await signedToken();
    const separate = await success("add_night_context", { wake_date: next, illness: false }, protocol);
    expect(separate.context).toMatchObject({ wake_date: next, note: null, illness: false });
  });

  it(`claims the date before reading its prior effective snapshot via ${protocol}`, async () => {
    const originalList = env.SLIPSTREAM_DATA.list.bind(env.SLIPSTREAM_DATA);
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    let paused = false;
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "list").mockImplementation(async options => {
      if (!paused && options?.prefix === contextPrefix(day)) { paused = true; entered(); await gate; }
      return originalList(options);
    });
    const first = rpc("add_night_context", { wake_date: day, note: "serialized first" }, protocol);
    await started;
    let blocked;
    try {
      blocked = await rpc("add_night_context", { wake_date: day, illness: true }, protocol, await signedToken());
    } finally { release(); }
    const result = await first;
    spy.mockRestore();
    expect(blocked.isError).toBe(true);
    expect(blocked.content[0].text).toContain("pending");
    expect(result.isError).not.toBe(true);
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history.events).toHaveLength(1);
    expect(history.events[0]).toMatchObject({ note: "serialized first", illness: null });
  });

  it(`releases a pre-PUT failed claim without consuming an accepted write budget via ${protocol}`, async () => {
    const originalGet = env.SLIPSTREAM_DATA.get.bind(env.SLIPSTREAM_DATA);
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "get").mockImplementation(async (name, options) => {
      if (name === sleepKey(day)) throw new Error("synthetic source read failure");
      return originalGet(name, options);
    });
    const failed = await rpc("add_night_context", { wake_date: day, note: "not written" }, protocol);
    expect(failed.isError).toBe(true);
    spy.mockRestore();
    testEnv.MCP_WRITE_DAILY_LIMIT = "1";
    const saved = await success("add_night_context", { wake_date: day, note: "first accepted attempt" }, protocol);
    expect(saved.context.note).toBe("first accepted attempt");
  });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each([
    { label: "out-of-range morning condition", patch: { morning_condition: 0 } },
    { label: "out-of-range sleep quality", patch: { sleep_quality: 6 } },
    { label: "invalid IANA zone", patch: { actual_night_timezone: "Invalid/Zone" } },
    { label: "oversized note", patch: { note: "x".repeat(1001) } },
    { label: "forged source", patch: { source: "garmin" } },
    { label: "empty patch", patch: {} },
  ])(`rejects $label without saving a context event via ${protocol}`, async fixture => {
    const result = await rpc("add_night_context", { wake_date: day, ...fixture.patch }, protocol);
    expect(result.isError).toBe(true);
    expect(await success("night_context_history", { wake_date: day }, protocol))
      .toMatchObject({ status: "not_stored", events: [] });
  });
  it(`fails closed for truncated and over100-event metadata instead of pretending no context via ${protocol}`, async () => {
    const originalList = env.SLIPSTREAM_DATA.list.bind(env.SLIPSTREAM_DATA);
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "list").mockImplementation(async options => {
      const listed = await originalList(options);
      return options?.prefix === contextPrefix(day) ? { ...listed, truncated: true } : listed;
    });
    const history = await success("night_context_history", { wake_date: day }, protocol);
    expect(history).toMatchObject({ status: "unavailable", events: [] });
    const blocked = await rpc("add_night_context", { wake_date: day, note: "must fail closed" }, protocol);
    expect(blocked.isError).toBe(true);
    spy.mockRestore();
    const saved = await success("add_night_context", { wake_date: day, note: "known source after recovery" }, protocol);
    expect(saved.saved).toBe(true);
    const object = (await originalList({ prefix: contextPrefix(day), limit: 101 })).objects[0];
    const oversized = vi.spyOn(env.SLIPSTREAM_DATA, "list").mockImplementation(async options => {
      const listed = await originalList(options);
      return options?.prefix === contextPrefix(day) ? { ...listed, truncated: false,
        objects: Array.from({ length: 101 }, (_, index) => ({ ...object, key: contextPrefix(day) + index + ".json" })) } : listed;
    });
    expect(await success("night_context_history", { wake_date: day }, protocol))
      .toMatchObject({ status: "unavailable", events: [] });
    oversized.mockRestore();
    expect((await success("night_context_history", { wake_date: day }, protocol)).events).toHaveLength(1);
  });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`resolves current context across the ordinary night reader family via ${protocol}`, async () => {
    const hrvKey = `health/hrv/${day.slice(0,4)}/${day.slice(5,7)}/${day}.json`;
    const prior = await env.SLIPSTREAM_DATA.get("summary/health_daily.csv");
    const priorBody = prior ? await prior.text() : null;
    try {
      await env.SLIPSTREAM_DATA.put(hrvKey, JSON.stringify({ schema_version: 1, source: "garmin", date: day, available: true,
        sleep_start_gmt: `${day}T00:00:00Z`, sleep_end_gmt: `${day}T07:00:00Z`,
        summary: { lastNightAvg: 50 }, readings: [{ timestamp: `${day}T01:00:00Z`,
          hrv_ms: 50, time_basis: "utc", timestamp_field: "readingTimeGMT", timestamp_unit: "iso8601" }] }));
      await env.SLIPSTREAM_DATA.put("summary/health_daily.csv", `Date,Sleep Seconds,HRV Last Night Average\n${day},25200,50\n`);
      const saved = await success("add_night_context", { wake_date: day, actual_night_timezone: "Asia/Tokyo", note: "explicit current context" }, protocol);
      const range = { start_date: day, end_date: day };
      const curve = await success("hrv_curve", { date: day }, protocol);
      const sleeps = await success("sleep_history", { ...range, granularity: "daily", detail_level: "full" }, protocol);
      const hrv = await success("hrv_history", { ...range, granularity: "daily", detail_level: "full" }, protocol);
      const daily = await success("daily_health", range, protocol);
      const signals = await success("night_signals", { date: day, resolution: "15m" }, protocol);
      const bulk = await success("night_signals_history", { ...range, resolution: "15m" }, protocol);
      const family = [
        ["hrv_curve", curve], ["sleep_history", sleeps.days[0]], ["hrv_history", hrv.days[0]],
        ["daily_health.sleep_night", daily.days[0].sleep_night], ["daily_health.hrv_night", daily.days[0].hrv_night],
        ["night_signals", signals], ["night_signals_history", bulk.nights[0]],
      ] as const;
      const observed = family.map(([name, value]) => ({ name, context_revision: value?.view?.context_revision,
        context_status: value?.view?.context_status, note: value?.user_context?.note,
        actual_night_timezone: value?.actual_night_timezone }));
      expect(observed).toEqual(family.map(([name]) => ({ name, context_revision: saved.context_id,
        context_status: "available", note: "explicit current context", actual_night_timezone: "Asia/Tokyo" })));
    } finally {
      await env.SLIPSTREAM_DATA.delete(hrvKey);
      if (priorBody === null) await env.SLIPSTREAM_DATA.delete("summary/health_daily.csv");
      else await env.SLIPSTREAM_DATA.put("summary/health_daily.csv", priorBody);
    }
  });
}
it("bounds a full20-event maximum-note history through the public JSON transport", async () => {
  const counts = { get: 0, list: 0, put: 0, written_bytes: 0 };
  testEnv.SLIPSTREAM_DATA = new Proxy(env.SLIPSTREAM_DATA, {
    get(target, property) {
      const value = Reflect.get(target, property);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        if (property === "get") counts.get++;
        if (property === "list") counts.list++;
        if (property === "put") {
          counts.put++;
          if (typeof args[1] === "string") counts.written_bytes += new TextEncoder().encode(args[1]).byteLength;
        }
        return Reflect.apply(value, target, args);
      };
    },
  });
  const started = performance.now();
  for (let index = 0; index < 20; index++) {
    token = await signedToken();
    await success("add_night_context", { wake_date: day, note: "界".repeat(1000), morning_condition: index % 5 + 1 }, "2026-07-28");
  }
  const history = await success("night_context_history", { wake_date: day, limit: 20 }, "2026-07-28");
  const structuredBytes = new TextEncoder().encode(JSON.stringify(history)).byteLength;
  expect(history.events).toHaveLength(20);
  expect(history.more).toBe(false);
  expect(new Set(history.events.map((event: {context_id:string}) => event.context_id)).size).toBe(20);
  expect(structuredBytes).toBeLessThanOrEqual(128 * 1024);
  expect(lastHttpBodyBytes).toBeLessThanOrEqual(512 * 1024);
  console.info(JSON.stringify({ synthetic_night_context_resources: { ...counts, user_appends: 20,
    history_events: 20, structured_bytes: structuredBytes, http_body_bytes: lastHttpBodyBytes,
    elapsed_ms: Math.round(performance.now() - started), platform: "explicit Node app/SQLite adapters, not native Workers" } }));
});
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`requires immutable conditional PUT and never claims a refused append was saved via ${protocol}`, async () => {
    const originalPut = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
    const spy = vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (name, value, options) => {
      if (name.startsWith(contextPrefix(day))) {
        // This assertion is at the external R2 write boundary: existing event bytes must not be overwritten.
        expect(options?.onlyIf).toEqual({ etagDoesNotMatch: "*" });
        return null;
      }
      return originalPut(name, value, options);
    });
    const refused = await rpc("add_night_context", { wake_date: day, note: "conditional refusal" }, protocol);
    expect(refused.isError).toBe(true);
    spy.mockRestore();
    expect(await success("night_context_history", { wake_date: day }, protocol))
      .toMatchObject({ status: "not_stored", events: [] });
  });
}