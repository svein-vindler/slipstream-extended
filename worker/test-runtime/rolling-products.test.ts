/** Real offline products -> actual authenticated MCP JSON and SSE, no read-time compute. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import fixture from "./generated/rolling-products-contract.json";

const ISSUER = "https://rolling-products.cloudflareaccess.com", HOST = "rolling-products.example";
let token: string, privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env;
let failGet = false;
const calls: Array<{ operation: string; key: string }> = [];
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return async (...args: unknown[]) => {
    calls.push({ operation: String(property), key: String(args[0]) });
    if (property !== "get" || failGet) throw new Error("PRIVATE_ROLLING_READ_BOUNDARY");
    return Reflect.apply(value, target, args);
  };
}});
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "rolling-products", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  calls.length = 0; failGet = false;
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${ISSUER}/cdn-cgi/access/certs`) return Response.json({ keys: [jwk] });
    throw new Error("External systems forbidden");
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "rolling-products" }).setIssuer(ISSUER).setAudience("rolling-products")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: ISSUER, ACCESS_AUD: "rolling-products", MCP_HOSTNAME: HOST,
    MCP_WRITES_ENABLED: "false", HEALTH_TIMEZONE: "Europe/Oslo", SLIPSTREAM_DATA: bucket,
    GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "", MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
  for (const item of fixture.products) await env.SLIPSTREAM_DATA.put(item.storage_key, JSON.stringify(item.product));
});
afterEach(async () => {
  vi.restoreAllMocks();
  await env.SLIPSTREAM_DATA.delete(fixture.products.map(item => item.storage_key));
});
async function call(protocol: string, window: number, channel: string) {
  const modern = protocol === "2026-07-28", context = createExecutionContext();
  const args = { end_date: "2026-11-07", period: window === 30 ? "custom" : String(window), channel,
    ...(window === 30 ? { start_date: "2026-10-09" } : {}) };
  const response = await worker.fetch(new Request(`https://${HOST}/mcp`, { method: "POST", headers: {
    host: HOST, "content-type": "application/json", accept: "application/json, text/event-stream",
    "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
    ...(modern ? { "mcp-method": "tools/call", "mcp-name": "night_baseline" } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "night_baseline", arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }) }), testEnv, context);
  const text = await response.text(); await waitOnExecutionContext(context);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error).toBeUndefined(); expect(message.result.isError).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}
it("covers every supported rolling period for both channels", () => {
  expect(fixture.products.map(item => `${item.window}/${item.channel}`).sort()).toEqual([
    "7/heart_rate", "7/hrv", "28/heart_rate", "28/hrv", "30/heart_rate", "30/hrv",
    "90/heart_rate", "90/hrv", "365/heart_rate", "365/hrv",
  ].sort());
});
for (const protocol of ["2026-07-28", "2025-03-26"]) {
  it.each(fixture.products)(`${protocol} reads stored $window/$channel with exactly one GET`, async item => {
    const result = await call(protocol, item.window, item.channel);
    expect(result).toMatchObject({ status: "available", current: { nights_requested: item.window,
      contributing_nights: item.window, unit: item.channel === "hrv" ? "ms" : "bpm" } });
    expect(result.current.bins[0]).toMatchObject({ p50: item.channel === "hrv" ? 40 : 60, nights: item.window });
    expect(calls).toEqual([{ operation: "get", key: item.storage_key }]);
    expect(JSON.stringify(result)).not.toContain("snapshot_sha256");
  });
  it(`${protocol} keeps missing and failed products unavailable without computing`, async () => {
    const item = fixture.products[0];
    await env.SLIPSTREAM_DATA.delete(item.storage_key);
    expect(await call(protocol, item.window, item.channel)).toMatchObject({ status: "not_stored", current: { bins: [] } });
    failGet = true;
    const failed = await call(protocol, item.window, item.channel);
    expect(failed).toMatchObject({ status: "read_error", current: { bins: [] } });
    expect(JSON.stringify(failed)).not.toContain("PRIVATE_ROLLING_READ_BOUNDARY");
    expect(calls).toEqual([{ operation: "get", key: item.storage_key }, { operation: "get", key: item.storage_key }]);
  });
}
