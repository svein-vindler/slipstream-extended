/** Actual authenticated MCP producer: bounded R2 transactions are the only fake boundary. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";

const QUEUE = "health/night-products/v1/refresh-queue.json";
const ISSUER = "https://rolling-context.cloudflareaccess.com", HOST = "rolling-context.example";
let token: string, privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env;
let sequence = 6, day: string;
const calls: Array<{ operation: string; key: string }> = [];
const contextPrefix = () => `health/night-context/v1/${day.slice(0,4)}/${day.slice(5,7)}/${day}/`;
const sleepKey = () => `health/sleep/v1/${day.slice(0,4)}/${day.slice(5,7)}/${day}.json`;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({ operation: String(property), key: String(args[0]) });
    return Reflect.apply(value, target, args);
  };
}});
function cleanQueue() {
  return { schema_version: 1, kind: "night-baseline-refresh-queue", pending: {},
    rolling: { eligible: [["2026-11-01", "2026-12-31"]], dirty: Object.fromEntries(
      [28,30,90].map(n => [String(n), { heart_rate: [], hrv: [] }])) } };
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "rolling-context", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  day = `2026-11-${String(++sequence).padStart(2,"0")}`;
  calls.length = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${ISSUER}/cdn-cgi/access/certs`) return Response.json({ keys: [jwk] });
    throw new Error("External systems forbidden");
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "rolling-context" }).setIssuer(ISSUER).setAudience("rolling-context")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: ISSUER, ACCESS_AUD: "rolling-context", MCP_HOSTNAME: HOST,
    MCP_WRITES_ENABLED: "true", MCP_WRITE_DAILY_LIMIT: "60", HEALTH_TIMEZONE: "Europe/Oslo",
    SLIPSTREAM_DATA: bucket, GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
  await env.SLIPSTREAM_DATA.put(sleepKey(), JSON.stringify({ date: day,
    sleep_start_gmt: `${day}T00:00:00Z`, sleep_end_gmt: `${day}T07:00:00Z` }));
  await env.SLIPSTREAM_DATA.put(QUEUE, JSON.stringify(cleanQueue()), { httpMetadata: { contentType: "application/json" } });
});
afterEach(async () => {
  vi.restoreAllMocks();
  const events = await env.SLIPSTREAM_DATA.list({ prefix: contextPrefix(), limit: 101 });
  await env.SLIPSTREAM_DATA.delete([QUEUE, sleepKey(), ...events.objects.map(item => item.key)]);
});
async function call(protocol: string, args = { wake_date: day, illness: true }) {
  const modern = protocol === "2026-07-28", context = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${HOST}/mcp`, { method: "POST", headers: {
    host: HOST, "content-type": "application/json", accept: "application/json, text/event-stream",
    "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
    ...(modern ? { "mcp-method": "tools/call", "mcp-name": "add_night_context" } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "add_night_context", arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }) }), testEnv, context);
  const text = await response.text(); await waitOnExecutionContext(context);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  return message.error ? { isError: true, content: [{ text: message.error.message }] } : message.result;
}
it("records exact context intent before append and invalidates all eligible forward rolling endpoints", async () => {
  const original = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
  vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (key, value, options) => {
    if (key.startsWith(contextPrefix())) {
      expect(typeof value).toBe("string");
      const event = JSON.parse(value as string);
      const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value as string));
      const sha = [...new Uint8Array(digest)].map(part => part.toString(16).padStart(2,"0")).join("");
      const prepared = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
      expect(prepared.rolling_context_intents[event.context_id]).toEqual({ wake_date: day, key, sha256: sha, state: "intent" });
      expect(options?.onlyIf).toEqual({ etagDoesNotMatch: "*" });
    }
    return original(key, value, options);
  });
  const result = await call("2026-07-28");
  expect(result.isError).not.toBe(true);
  expect(result.structuredContent.saved).toBe(true);
  const queue = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(queue.rolling.dirty).toEqual({
    "28": { heart_rate: [["2026-11-07","2026-12-04"]], hrv: [["2026-11-07","2026-12-04"]] },
    "30": { heart_rate: [["2026-11-07","2026-12-06"]], hrv: [["2026-11-07","2026-12-06"]] },
    "90": { heart_rate: [["2026-11-07","2026-12-31"]], hrv: [["2026-11-07","2026-12-31"]] },
  });
  expect(queue.rolling_context_intents).toEqual({});
  expect(calls.filter(item => item.operation === "put").map(item => item.key))
    .toEqual([QUEUE, expect.stringContaining(contextPrefix()), QUEUE]);
  expect(calls.filter(item => item.operation === "get")).toHaveLength(7);
  expect(calls.filter(item => item.operation === "list")).toHaveLength(2);
  expect(calls.filter(item => item.operation === "head")).toHaveLength(0);
});

it("keeps an unknown intent write held without appending or replaying context", async () => {
  const original = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
  vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (key, value, options) => {
    const ack = await original(key, value, options);
    if (key === QUEUE) throw new Error("PRIVATE_QUEUE_ACK_FAILURE");
    return ack;
  });
  const result = await call("2025-03-26");
  expect(result.isError).toBe(true);
  const queue = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(Object.values(queue.rolling_context_intents)).toHaveLength(1);
  expect(Object.values(queue.rolling_context_intents)[0]).toMatchObject({ wake_date: day, state: "intent" });
  expect(calls.filter(item => item.operation === "put")).toEqual([{ operation: "put", key: QUEUE }]);
  expect((await call("2025-03-26")).isError).toBe(true);
  expect(calls.filter(item => item.operation === "put")).toHaveLength(1);
  expect(JSON.stringify(result)).not.toContain("PRIVATE_QUEUE_ACK_FAILURE");
});

it.each(["2026-07-28", "2025-03-26"])("%s keeps saved:true after a post-append queue failure and forbids reappend", async protocol => {
  const original = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
  let queuePuts = 0;
  vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (key, value, options) => {
    if (key === QUEUE && ++queuePuts === 2) throw new Error("PRIVATE_QUEUE_FINALIZE_FAILURE");
    return original(key, value, options);
  });
  const result = await call(protocol);
  expect(result.isError).not.toBe(true);
  expect(result.structuredContent).toMatchObject({ saved: true, wake_date: day });
  expect(result.structuredContent.message).toContain("Retry only the read");
  expect(JSON.stringify(result)).not.toContain("PRIVATE_QUEUE_FINALIZE_FAILURE");
  const eventPuts = () => calls.filter(item => item.operation === "put" && item.key.startsWith(contextPrefix()));
  expect(eventPuts()).toHaveLength(1);
  expect((await call(protocol)).isError).toBe(true);
  expect(eventPuts()).toHaveLength(1);
  const queue = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(Object.keys(queue.rolling_context_intents)).toHaveLength(1);
});

it.each(["2026-07-28", "2025-03-26"])("%s preserves the busy publisher but releases a confirmed context dayclaim", async protocol => {
  const queue: any = cleanQueue();
  const name = `rolling/28/${day}/hrv`;
  const first = new Date(Date.parse(day + "T00:00:00Z") - 27 * 86400000).toISOString().slice(0,10);
  const row = { rolling_window: 28, channel: "hrv", start_date: first, end_date: day,
    phase: "publishing", sources: {}, completed: {} };
  queue.pending[name] = row;
  await env.SLIPSTREAM_DATA.put(QUEUE, JSON.stringify(queue), { httpMetadata: { contentType: "application/json" } });
  const result = await call(protocol);
  expect(result.structuredContent.saved).toBe(true);
  expect(result.structuredContent.message).toContain("durably queued");
  const after = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(after.pending[name]).toEqual(row);
  const saved = after.rolling_context_intents[result.structuredContent.context_id];
  expect(saved).toMatchObject({ wake_date: day, state: "saved" });
  expect(saved.saved_revision).toMatch(/^".+"$/);
  expect(after.rolling.dirty["28"].hrv[0][0]).toBe(day);
  const cooldown = await call(protocol, { wake_date: day, illness: false });
  expect(cooldown.isError).toBe(true);
  expect(JSON.stringify(cooldown)).toContain("Retry in");
  const now = Date.now(); vi.spyOn(Date, "now").mockReturnValue(now + 11000);
  const next = await call(protocol, { wake_date: day, illness: false });
  expect(next.isError).not.toBe(true);
  expect(next.structuredContent).toMatchObject({ saved: true, context: {
    previous_context_id: result.structuredContent.context_id, illness: false } });
  expect(calls.filter(item => item.operation === "put" && item.key.startsWith(contextPrefix()))).toHaveLength(2);
  const final = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(final.pending[name]).toEqual(row);
  expect(Object.values(final.rolling_context_intents).map((value: any) => value.state)).toEqual(["saved", "saved"]);
});

it("rebuilds a safe materialized row without losing its bounded retry ownership", async () => {
  const queue: any = cleanQueue();
  const name = `rolling/30/${day}/heart_rate`;
  const first = new Date(Date.parse(day + "T00:00:00Z") - 29 * 86400000).toISOString().slice(0,10);
  queue.pending[name] = { rolling_window: 30, channel: "heart_rate", start_date: first, end_date: day,
    phase: "dirty", sources: {}, completed: { product: "old-proof" }, blocked: "old-context", validated_progress: { old: true } };
  await env.SLIPSTREAM_DATA.put(QUEUE, JSON.stringify(queue), { httpMetadata: { contentType: "application/json" } });
  const result = await call("2025-03-26");
  expect(result.structuredContent.saved).toBe(true);
  const after = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(after.pending[name]).toEqual({ rolling_window: 30, channel: "heart_rate", start_date: first,
    end_date: day, phase: "dirty", sources: {}, completed: {} });
  expect(after.rolling.dirty["30"].heart_rate[0][0]).not.toBe(day);
  expect(after.rolling_context_intents).toEqual({});
});

it("does not append after a known queue CAS rejection and releases only that pre-append claim", async () => {
  const original = env.SLIPSTREAM_DATA.put.bind(env.SLIPSTREAM_DATA);
  let rejected = false;
  const spy = vi.spyOn(env.SLIPSTREAM_DATA, "put").mockImplementation(async (key, value, options) => {
    if (key === QUEUE && !rejected) { rejected = true; return null; }
    return original(key, value, options);
  });
  expect((await call("2026-07-28")).isError).toBe(true);
  expect(calls.filter(item => item.operation === "put")).toEqual([{ operation: "put", key: QUEUE }]);
  spy.mockRestore();
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "rolling-context" }).setIssuer(ISSUER).setAudience("rolling-context")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  expect((await call("2026-07-28")).structuredContent.saved).toBe(true);
  expect(calls.filter(item => item.operation === "put" && item.key.startsWith(contextPrefix()))).toHaveLength(1);
});

it("keeps saved:true and the intent after the exact event readback fails", async () => {
  const original = env.SLIPSTREAM_DATA.get.bind(env.SLIPSTREAM_DATA);
  let failed = false;
  vi.spyOn(env.SLIPSTREAM_DATA, "get").mockImplementation(async (key, options) => {
    if (key.startsWith(contextPrefix()) && !failed) { failed = true; throw new Error("PRIVATE_EVENT_READBACK"); }
    return original(key, options);
  });
  const result = await call("2025-03-26");
  expect(result.structuredContent.saved).toBe(true);
  expect(result.structuredContent.message).toContain("Retry only the read");
  expect(JSON.stringify(result)).not.toContain("PRIVATE_EVENT_READBACK");
  expect(calls.filter(item => item.operation === "put" && item.key === QUEUE)).toHaveLength(1);
  const queue = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(Object.keys(queue.rolling_context_intents)).toHaveLength(1);
});

it("persists discovery for an unregistered historical context without inventing eligible endpoints", async () => {
  // This date is intentionally independent of the registered November fixture.
  day = "2026-01-01";
  await env.SLIPSTREAM_DATA.put(sleepKey(), JSON.stringify({ date: day,
    sleep_start_gmt: `${day}T00:00:00Z`, sleep_end_gmt: `${day}T07:00:00Z` }));
  await env.SLIPSTREAM_DATA.put(QUEUE, JSON.stringify({ schema_version: 1,
    kind: "night-baseline-refresh-queue", pending: {}, rolling_discovery: [["2025-12-30","2025-12-31"]] }),
    { httpMetadata: { contentType: "application/json" } });
  const result = await call("2026-07-28");
  expect(result.structuredContent.saved).toBe(true);
  const queue = await (await env.SLIPSTREAM_DATA.get(QUEUE))!.json<Record<string, any>>();
  expect(queue.rolling_discovery).toEqual([["2025-12-30","2026-03-31"]]);
  expect(queue.rolling).toBeUndefined();
  expect(queue.rolling_context_intents).toEqual({});
});
