import collected from "./generated/night-signals-contract.json";
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
let token: string, privateKey: CryptoKey, jwk: Row, testEnv: Env, rpcId = 0;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({operation: String(property), key: String(args[0])});
    return Reflect.apply(value, target, args);
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
  calls.length = 0; rpcId = 0;
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
afterEach(async () => {
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
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  expect(response.headers.get("cache-control")).toContain("no-store");
  const line = raw.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : raw); expect(message.id).toBe(id);
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
// The generated input is produced by scripts/build_night_signals_contract.py
// through ordinary collection and the real guarded canonical stores.
it.each(["2026-07-28", "2025-03-26"])("%s reads exact Python-produced canonical signal/HRV/sleep bytes", async protocol => {
  await env.SLIPSTREAM_DATA.delete([...stored]); stored.clear();
  for (const [key, record] of Object.entries(collected.objects)) {
    const bytes=Uint8Array.from(atob(record.base64),character=>character.charCodeAt(0));
    stored.add(key);
    await env.SLIPSTREAM_DATA.put(key,bytes,{httpMetadata:{contentType:record.content_type,...(record.content_encoding?{contentEncoding:record.content_encoding}:{})}});
  }
  calls.length=0;
  const value=success(await call(protocol,"night_signals",{date:collected.wake_date}));
  const channels=value.channels as Record<string, {points:Array<{value:number}>;quality:{ambiguous_time:number};minimum:{value:number}}>;
  expect(channels.heart_rate.points.map(point=>point.value)).toEqual([80,48,46,44,42,60]);
  expect(channels.hrv.points.map(point=>point.value)).toEqual([42,50]);
  expect(channels.hrv.quality.ambiguous_time).toBe(1);
  // A later full ten-minute window has four covered2-minute slots (80%).
  // Its values[48,46,44,42] have median45; the first five-slot window has46.
  expect(channels.heart_rate.minimum).toMatchObject({value:45,window_start_utc:"2026-10-24T22:00:00Z",window_end_utc:"2026-10-24T22:10:00Z",coverage:0.8,valid_points:4});
  const ordinaryHrv=success(await call(protocol,"hrv_curve",{date:collected.wake_date}));
  expect(ordinaryHrv.readings).toEqual([
    {timestamp:"2026-10-24T22:00:00",hrv_ms:42,time_basis:"utc",timestamp_field:"readingTimeGMT",timestamp_unit:"iso8601"},
    {timestamp:"2026-10-25T00:00:00Z",hrv_ms:50,time_basis:"utc",timestamp_field:"readingTimeGMT",timestamp_unit:"iso8601"},
    {timestamp:"2026-10-25T01:00:00",hrv_ms:999,time_basis:"local_only",timestamp_field:"readingTimeLocal",timestamp_unit:"iso8601"},
  ]);
  for (const detail_level of ["summary","full"]) {
    const history=success(await call(protocol,"hrv_history",{start_date:collected.wake_date,end_date:collected.wake_date,detail_level}));
    // Only GMT42 at22:00Z and50 at00:00Z establish a timed slope:8ms /2h=4.
    // The local-only999 reading remains numeric source data, never a UTC instant.
    expect(history.days[0].derived.slope_ms_per_hour).toBe(4);
  }
  // A matching legacy index remains usable without a broad canonical scan,
  // but its old timestamp-derived slope cannot establish a UTC trend.
  const indexKey="health/indexes/hrv/v1/2026/10.json";
  const indexObject=await env.SLIPSTREAM_DATA.get(indexKey);
  const priorIndex=JSON.parse(await new Response(indexObject!.body.pipeThrough(new DecompressionStream("gzip"))).text());
  priorIndex.builder_revision=2;
  priorIndex.days[0].derived.slope_ms_per_hour=9999;
  priorIndex.source_revisions[hrvKey()]=(await env.SLIPSTREAM_DATA.head(hrvKey()))!.etag;
  await put(indexKey,priorIndex);
  calls.length=0;
  const legacy=success(await call(protocol,"hrv_history",{start_date:collected.wake_date,end_date:collected.wake_date,detail_level:"summary"}));
  expect(legacy.days[0].derived.slope_ms_per_hour).toBeNull();
  expect(legacy.days[0].time_quality_flags).toContain("legacy_index_hrv_time_basis_unverified");
  expect(calls.filter(entry=>entry.operation==="get"&&entry.key===hrvKey())).toEqual([]);
  const sleep=success(await call(protocol,"sleep_detail",{date:collected.wake_date}));
  expect(sleep.configured_projection).toMatchObject({sleep_start_local:"2026-10-24T23:58:00+02:00",sleep_end_local:"2026-10-25T07:48:00+01:00"});
  expect(collected.collection.health_sdk_calls).toBe(61);
  expect(collected.collection.provider_api_calls).toBe(66);
});
