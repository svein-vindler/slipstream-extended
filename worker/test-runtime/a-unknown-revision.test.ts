/** Synthetic stored source -> authenticated JSON/SSE night signal contract. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { beforeAll, beforeEach, afterEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import unknownFixture from "./generated/night-signals-unknown-revision.json";

type Row = Record<string, unknown>;
const DAY = "2026-10-25", ISSUER = "https://night-signals.cloudflareaccess.com", HOST = "night-signals.example";
const START = "2026-10-24T23:56:00Z", END = "2026-10-25T00:06:00Z";
const sleepKey = (day = DAY) => `health/sleep/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const signalKey = (day: string) => `health/signals/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const hrvKey = (day = DAY) => `health/hrv/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
const stored = new Set<string>(), calls: Array<{operation: string; key: string}> = [];
const responseSamples: Row[]=[];
let oversizedRevision:string|undefined;
let token: string, privateKey: CryptoKey, jwk: Row, testEnv: Env, rpcId = 0, failedGet: string | undefined, expiredGet: string | undefined, clockOffset=0;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({operation: String(property), key: String(args[0])});
    if(property==="get"&&String(args[0])===failedGet)throw new Error("SYNTHETIC_READ_FAILURE_PRIVATE");
    let result=await Reflect.apply(value, target, args);
    if(property==="get"&&String(args[0])===signalKey(DAY)&&result&&oversizedRevision)result={...result,etag:oversizedRevision};
    if(property==="get"&&String(args[0])===expiredGet)clockOffset=10001;
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
  calls.length = 0; responseSamples.length=0; rpcId = 0; failedGet=undefined; expiredGet=undefined; clockOffset=0; oversizedRevision=undefined;
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
  context.task.meta.nightSignalResources={responses:[...responseSamples],operations:Object.fromEntries(["get","head","list","put","delete"].map(operation=>[operation,calls.filter(call=>call.operation===operation).length]))};
  try {expect(calls.filter(call => ["put","delete"].includes(call.operation))).toEqual([]);}
  finally {await env.SLIPSTREAM_DATA.delete([...stored]); stored.clear(); vi.restoreAllMocks();}
});
async function rpc(protocol: string, method: string, params: Row) {
  const modern = protocol === "2026-07-28", id = ++rpcId, context = createExecutionContext(), started=performance.now();
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
  if(method==="tools/call"){
    const rawBytes=new TextEncoder().encode(raw).byteLength;
    expect(rawBytes).toBeLessThanOrEqual(512*1024);
    responseSamples.push({tool:String(params.name),protocol,raw_response_bytes:rawBytes,elapsed_ms:performance.now()-started,resources:message.result?.structuredContent?.resources,status:message.result?.structuredContent?.status});
  }
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
it.each(["2026-07-28", "2025-03-26"])("%s keeps explicit unknown retained provenance through two distinct public partial collections", async protocol => {
  for (const [key, object] of Object.entries(unknownFixture.objects)) {
    stored.add(key);
    await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(object.base64), char => char.charCodeAt(0)), {httpMetadata: {contentType: object.content_type, contentEncoding: object.content_encoding}});
  }
  const value = success(await call(protocol, "night_signals", {date: DAY}));
  const heart = (value.channels as Row).heart_rate as Row;
  expect((heart.points as Row[]).map(point => point.value)).toEqual([82,48]);
  expect(heart.points).toMatchObject([{value:82,observation_origin:"incoming",source_revision:unknownFixture.expected.incoming_source_revision}, {value:48,observation_origin:"retained",source_revision:null}]);
  const retained = (heart.points as Row[])[1];
  expect(Object.hasOwn(retained, "source_revision")).toBe(true);
  expect(unknownFixture.expected.incoming_source_revision).not.toBe(unknownFixture.expected.first_partial_source_revision);
  expect(heart.source_quality_flags).toEqual(expect.arrayContaining(["retained_revision_unknown", "retained_previous_observations", "source_completeness_unknown"]));
  expect(value.status).toBe("partial");
});
