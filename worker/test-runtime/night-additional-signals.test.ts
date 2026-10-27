import collected from "./generated/night-additional-signals-contract.json";
/** Actual synthetic Python collection/storage bytes -> authenticated app HTTP readers. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { beforeAll, beforeEach, afterEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";

type Row = Record<string, unknown>;
const DAY = "2026-10-25", ISSUER = "https://night-signals.cloudflareaccess.com", HOST = "night-signals.example";
const START = "2026-10-24T23:56:00Z", END = "2026-10-25T00:06:00Z";
const sleepKey = (day = DAY) => `health/sleep/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const signalKey = (day: string) => `health/signals/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const hrvKey = (day = DAY) => `health/hrv/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const stored = new Set<string>(), calls: Array<{operation: string; key: string}> = [];
const responseSamples: Row[] = [];
let lastResponseBytes = 0;
let token: string, privateKey: CryptoKey, jwk: Row, testEnv: Env, rpcId = 0;
let lateContextGet = false, oversizeContextGet = false, clockOffset = 0;
let oversizedRevision: string | undefined;
let oversizedRevisionReads = 0;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({operation: String(property), key: String(args[0])});
    const result = await Reflect.apply(value, target, args);
    if (property === "get" && String(args[0]).startsWith("health/night-context/")) {
      if (lateContextGet) clockOffset = 10001;
      if (oversizeContextGet && result) return new Proxy(result, {
        get(object, key) { return key === "size" ? 8 * 1024 * 1024 + 1 : Reflect.get(object, key); },
      });
    }
    if (property === "get" && String(args[0]) === signalKey(DAY) && result && oversizedRevision) {
      // Keep the native R2 body and metadata accessors while replacing only ETag.
      const revision = oversizedRevision;
      return new Proxy(result, {get(object, key) {
        if (key === "etag") { oversizedRevisionReads++; return revision; }
        const member = Reflect.get(object, key, object);
        return typeof member === "function" ? member.bind(object) : member;
      }});
    }
    return result;
  };
}});
const reading = (timestamp: string, value: number) => ({timestamp, value, time_basis: "utc", timestamp_field: "heartRateValues.timestamp", timestamp_unit: "iso8601"});
async function put(key: string, value: unknown) {
  stored.add(key); await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", {extractable: true}); privateKey = pair.privateKey;
  jwk = {...await exportJWK(pair.publicKey), kid: "night-signals", alg: "RS256", use: "sig"};
});
beforeEach(async () => {
  calls.length = 0; responseSamples.length = 0; lastResponseBytes = 0; rpcId = 0; lateContextGet = false; oversizeContextGet = false; clockOffset = 0; oversizedRevision = undefined; oversizedRevisionReads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${ISSUER}/cdn-cgi/access/certs`) return Response.json({keys: [jwk]});
    throw new Error("Unexpected external request in synthetic night signals test");
  });
  token = await new SignJWT({type: "app", email: "synthetic@example.invalid"})
    .setProtectedHeader({alg: "RS256", kid: "night-signals"}).setIssuer(ISSUER).setAudience("night-signals")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv = {...env, ACCESS_TEAM_DOMAIN: ISSUER, ACCESS_AUD: "night-signals", MCP_HOSTNAME: HOST,
    HEALTH_TIMEZONE: "Europe/Oslo", MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    SLIPSTREAM_DATA: bucket, MCP_RATE_LIMITER: {limit: async () => ({success: true})}};
  await put(sleepKey(), {schema_version: 1, source: "garmin", date: DAY, sleep_start_gmt: START, sleep_end_gmt: END,
    sleep_start_garmin_local: "2026-10-25T01:56:00", sleep_end_garmin_local: "2026-10-25T02:06:00"});
  await put(signalKey("2026-10-24"), {schema_version: 1, source: "garmin", date: "2026-10-24", channels: {
    heart_rate: {status: "available", unit: "bpm", readings: [reading(START,80), reading("2026-10-24T23:58:00Z",48)], quality_flags: []}}});
  await put(signalKey(DAY), {schema_version: 1, source: "garmin", date: DAY, channels: {
    heart_rate: {status: "available", unit: "bpm", readings: [reading("2026-10-25T00:00:00Z",46), reading("2026-10-25T00:02:00Z",44),
      reading("2026-10-25T00:04:00Z",42), reading(END,20)], quality_flags: []}}});
  await put(hrvKey(), {schema_version: 1, source: "garmin", date: DAY,
    readings: [{timestamp: "2026-10-25T00:00:00Z", hrv_ms: 55, time_basis: "utc", timestamp_field: "readingTimeGMT", timestamp_unit: "iso8601"}]});
});
afterEach(async context => {
  context.task.meta.additionalSignalResponses = [...responseSamples];
  try {expect(calls.filter(call => ["put","delete"].includes(call.operation))).toEqual([]);}
  finally {await env.SLIPSTREAM_DATA.delete([...stored]); stored.clear(); vi.restoreAllMocks();}
});
async function rpc(protocol: string, method: string, params: Row) {
  const modern = protocol === "2026-07-28", id = ++rpcId, context = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${HOST}/mcp`, {method: "POST", headers: {
    host: HOST, "content-type": "application/json", accept: "application/json, text/event-stream",
    "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
    ...(modern ? {"mcp-method": method, ...(method === "tools/call" ? {"mcp-name": String(params.name)} : {})} : {})},
    body: JSON.stringify({jsonrpc: "2.0", id, method, params: {...params, ...(modern ? {_meta: {
      [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {}}} : {})}})}), testEnv, context);
  const raw = await response.text(); await waitOnExecutionContext(context);
  lastResponseBytes = new TextEncoder().encode(raw).byteLength;
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  expect(response.headers.get("cache-control")).toContain("no-store");
  const line = raw.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : raw); expect(message.id).toBe(id);
  if (method === "tools/call") responseSamples.push({protocol, tool: String(params.name), wire_bytes: lastResponseBytes,
    status: message.result?.structuredContent?.status, resources: message.result?.structuredContent?.resources});
  return message;
}
async function call(protocol: string, name: string, args: Row) {
  if (protocol !== "2026-07-28") await rpc(protocol, "initialize", {protocolVersion: protocol, capabilities: {}, clientInfo: {name:"night-signals-test",version:"1"}});
  return rpc(protocol, "tools/call", {name, arguments: args});
}
function success(message: Row) {
  expect(message.error).toBeUndefined();
  const result = message.result as {isError?: boolean; content: Array<{text: string}>; structuredContent: Row};
  expect(result.isError).not.toBe(true);
  expect(JSON.parse(result.content[0].text)).toEqual(result.structuredContent);
  return result.structuredContent;
}
async function putCollected() {
  await env.SLIPSTREAM_DATA.delete([...stored]);
  stored.clear();
  for (const [key, record] of Object.entries(collected.objects)) {
    const bytes = Uint8Array.from(atob(record.base64), character => character.charCodeAt(0));
    stored.add(key);
    await env.SLIPSTREAM_DATA.put(key, bytes, { httpMetadata: {
      contentType: record.content_type,
      ...(record.content_encoding ? { contentEncoding: record.content_encoding } : {}),
    } });
  }
  calls.length = 0;
}

it.each(["2026-07-28", "2025-03-26"])("%s reads Python-produced Body Battery and bounded stress alongside unchanged HR and HRV", async protocol => {
  await putCollected();
  const value = success(await call(protocol, "night_signals", {date: collected.wake_date,
    channels: ["heart_rate", "hrv", "body_battery", "stress"]}));
  const channels = value.channels as Record<string, {points: Array<{value: number}>; source_missing_value_count: number}>;
  expect(Object.keys(channels).sort()).toEqual(["body_battery", "heart_rate", "hrv", "stress"]);
  expect(channels.heart_rate.points.map(point => point.value)).toEqual(collected.expected.heart_rate_values);
  expect(channels.hrv.points.map(point => point.value)).toEqual(collected.expected.hrv_values);
  expect(channels.body_battery.points.map(point => point.value)).toEqual([55, 65]);
  expect(channels.stress.points.map(point => point.value)).toEqual([0, 35, 45, 50]);
  expect(channels.stress).toMatchObject({unit: "points", measurement_kind: "garmin_derived",
    source_missing_value_count: 2, measurement_window_seconds: null,
    source_quality_flags: expect.arrayContaining(["missing_values"])});
  expect(channels.body_battery).toMatchObject({unit: "points", measurement_kind: "garmin_derived"});
  expect(collected.collection.stress).toMatchObject({fitness_http_attempts: 2, component_auth_http_attempts: 0});
  expect(collected.collection.stress_responses_closed).toBe(true);
});
it.each(["2026-07-28", "2025-03-26"])("%s returns only selected channels and leaves default A channels unchanged", async protocol => {
  await putCollected();
  const defaults = success(await call(protocol, "night_signals", {date: DAY}));
  expect(Object.keys(defaults.channels as Row)).toEqual(["heart_rate", "hrv"]);
  calls.length = 0;
  const onlyHrv = success(await call(protocol, "night_signals", {date: DAY, channels: ["hrv"]}));
  expect(Object.keys(onlyHrv.channels as Row)).toEqual(["hrv"]);
  expect(calls.some(entry => entry.operation === "get" && entry.key.startsWith("health/signals/"))).toBe(false);
  calls.length = 0;
  const derived = success(await call(protocol, "night_signals", {date: DAY, channels: ["body_battery", "stress"]}));
  expect(derived.status).toBe("partial"); // Documented missing stress source observations remain partial.
  expect(Object.keys(derived.channels as Row)).toEqual(["body_battery", "stress"]);
  expect(calls.filter(entry => entry.operation === "get" && entry.key.startsWith("health/signals/"))).toHaveLength(2);
  expect(calls.some(entry => entry.operation === "get" && entry.key.startsWith("health/hrv/"))).toBe(false);
  expect(derived.resources).toMatchObject({input_points: 6, returned_points: 6});
});

it.each([[], ["stress", "stress"], ["unknown"], ["heart_rate", "hrv", "body_battery", "stress", "hrv"]].map(channels => ({channels})))(
  "rejects invalid channel selection %j before storage", async ({channels}) => {
    await putCollected();
    const message = await call("2026-07-28", "night_signals", {date: DAY, channels});
    expect(message.error !== undefined || (message.result as {isError?: boolean})?.isError === true).toBe(true);
    expect(calls).toEqual([]);
  },
);

it("preserves derived zero while rejecting negative and over-range stored observations", async () => {
  await putCollected();
  await put(signalKey("2026-10-24"), {schema_version: 1, source: "garmin", date: "2026-10-24", channels: {
    stress: {status: "no_data", unit: "points", readings: [], quality_flags: []},
  }});
  await put(signalKey(DAY), {schema_version: 1, source: "garmin", date: DAY, channels: {
    stress: {status: "available", unit: "points", quality_flags: [], readings: [-1, 0, 101, 50].map((value, index) => ({
      timestamp: 1792886400000 + index * 120000, value, time_basis: "utc", timestamp_unit: "epoch_milliseconds",
    }))},
  }});
  calls.length = 0;
  const value = success(await call("2026-07-28", "night_signals", {date: DAY, channels: ["stress"]}));
  expect(value).toMatchObject({status: "partial", channels: {stress: {valid_points: 2,
    quality: {invalid: 2}, measurement_window_seconds: null}}});
  expect(((value.channels as Row).stress as {points: Array<{value: number}>}).points.map(point => point.value)).toEqual([0, 50]);
});

it("counts the native cap cumulatively across selected derived channels", async () => {
  await putCollected();
  const point = (index: number, value: number) => ({timestamp: 1792886400 + index, value,
    time_basis: "utc", timestamp_unit: "epoch_seconds"});
  const payload = {schema_version: 1, source: "garmin", date: DAY, channels: {
    body_battery: {status: "available", unit: "points", readings: Array.from({length: 2500}, (_, i) => point(i, 50))},
    stress: {status: "available", unit: "points", readings: Array.from({length: 2000}, (_, i) => point(i, 35))},
  }};
  const compressed = await new Response(new Blob([JSON.stringify(payload)]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer();
  stored.add(signalKey(DAY));
  await env.SLIPSTREAM_DATA.put(signalKey(DAY), compressed);
  calls.length = 0;
  const value = success(await call("2026-07-28", "night_signals", {date: DAY, channels: ["body_battery", "stress"]}));
  expect(value).toMatchObject({status: "budget_exhausted", quality_flags: ["native_point_limit"],
    resources: {returned_points: 0}, channels: {body_battery: {points: []}, stress: {points: []}}});
  expect(Object.keys(value.channels as Row)).toEqual(["body_battery", "stress"]);
});

it("counts compact bins cumulatively for selected derived channels", async () => {
  await putCollected();
  const value = success(await call("2026-07-28", "night_signals_history", {start_date: DAY, end_date: DAY,
    channels: ["body_battery", "stress"], resolution: "30m"}));
  const night = (value.nights as Array<{channels: Record<string, {bins: unknown[]; points: unknown[]}>}>)[0];
  expect(Object.keys(night.channels)).toEqual(["body_battery", "stress"]);
  expect(night.channels.body_battery.points).toEqual([]);
  expect(night.channels.stress.points).toEqual([]);
  expect(night.channels.body_battery.bins).toHaveLength(18);
  expect(night.channels.stress.bins).toHaveLength(18);
  expect(value.resources).toMatchObject({returned_points: 36});
});

it.each(["2026-07-28", "2025-03-26"])("%s cannot swallow a genuine context read deadline or cumulative stored-byte budget", async protocol => {
  await putCollected();
  const key = `health/night-context/v1/2026/10/${DAY}/20261025T070000000-nctx-aaaaaaaaaaaaaaaaaaaaaaaa.json`;
  stored.add(key);
  await env.SLIPSTREAM_DATA.put(key, "{}");
  const realNow = performance.now.bind(performance);
  vi.spyOn(performance, "now").mockImplementation(() => realNow() + clockOffset);
  lateContextGet = true;
  calls.length = 0;
  const late = success(await call(protocol, "night_signals", {date: DAY, channels: ["stress"]}));
  expect(late).toMatchObject({status: "budget_exhausted", quality_flags: ["deadline_exceeded"],
    resources: {r2_get_attempts: 1, returned_points: 0}, channels: {stress: {points: []}}});
  lateContextGet = false;
  clockOffset = 0;
  oversizeContextGet = true;
  const oversize = success(await call(protocol, "night_signals", {date: DAY, channels: ["stress"]}));
  expect(oversize).toMatchObject({status: "budget_exhausted", quality_flags: ["stored_byte_limit"],
    resources: {r2_get_attempts: 1, returned_points: 0}, channels: {stress: {points: []}}});
  expect(Object.keys(oversize.channels as Row)).toEqual(["stress"]);
});

it.each(["2026-07-28", "2025-03-26"])("%s bounds oversized source clocks while preserving selected derived observations", async protocol => {
  await putCollected();
  await put(sleepKey(), {schema_version: 1, source: "garmin", date: DAY,
    sleep_start_gmt: collected.expected.sleep_start_utc, sleep_end_gmt: collected.expected.sleep_end_utc,
    sleep_start_garmin_local: "x".repeat(300000)});
  calls.length = 0;
  const value = success(await call(protocol, "night_signals", {date: DAY, channels: ["stress"]}));
  expect(lastResponseBytes).toBeLessThanOrEqual(512 * 1024);
  expect((value.resources as Row).structured_bytes).toBeLessThanOrEqual(256 * 1024);
  expect(Object.keys(value.channels as Row)).toEqual(["stress"]);
  expect(((value.channels as Row).stress as {points: Row[]}).points.map(point => point.value)).toEqual([0,35,45,50]);
  expect((value.night_context as Row).source_time).toMatchObject({sleep_start_garmin_local: null});
  expect((value.night_context as Row).time_quality_flags).toContain("invalid_source_clock");
});
it.each(["2026-07-28", "2025-03-26"])("%s bounds metadata-only overflow for the selected derived channel", async protocol => {
  await putCollected();
  oversizedRevision = "SYNTHETIC_REVISION_" + "R".repeat(300000);
  const value = success(await call(protocol, "night_signals", {date: DAY, channels: ["stress"]}));
  expect(oversizedRevisionReads).toBeGreaterThan(0);
  expect(value).toMatchObject({status: "output_limit", night_context: null, channels: {stress: {points: [], source_revisions: []}}});
  expect(Object.keys(value.channels as Row)).toEqual(["stress"]);
  expect(lastResponseBytes).toBeLessThan(8192);
  expect((value.resources as Row).structured_bytes).toBeLessThanOrEqual(256 * 1024);
});
