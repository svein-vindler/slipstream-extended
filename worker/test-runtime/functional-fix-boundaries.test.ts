import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import produced from "./generated/functional-fix-contract.json";

const issuer = "https://functional-synthetic.cloudflareaccess.com", hostname = "functional.example";
let key: CryptoKey, jwk: Record<string, unknown>, token: string, testEnv: Env;
const objects = new Map<string, Uint8Array>();

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "functional", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); objects.clear();
  objects.set("summary/activities.csv", new TextEncoder().encode(produced.csv));
  for (const session of Object.values(produced.strength)) {
    objects.set(`activities/2026/${session.activity_id}/activity.v1.json`, new TextEncoder().encode(JSON.stringify({
      schema_version: 1, source: "garmin-fit", activity: { id: session.activity_id, type: "strength_training",
        start_time_local: "2026-10-23 15:00:00", start_time_gmt: "2026-10-23 13:00:00" },
      normalized_strength_session: session,
    })));
  }
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "functional" }).setIssuer(issuer).setAudience("functional")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const bucket = {
    head: async (name: string) => objects.has(name) ? { etag: "synthetic-functional" } : null,
    get: async (name: string) => { const body = objects.get(name);
      return body ? { size: body.byteLength, etag: "synthetic-functional", body: new Response(body).body } : null; },
    list: async () => { throw new Error("Unexpected discovery"); },
    put: async () => { throw new Error("Unexpected write"); },
    delete: async () => { throw new Error("Unexpected delete"); },
  } as unknown as R2Bucket;
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "functional", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });

async function call(name: string, args: unknown, protocol: string) {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args,
      ...(protocol === "2026-07-28" ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol,
        [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  const line = text.split("\n").find(line => line.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined(); expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`real CSV producer names survive the authenticated Worker response via ${protocol}`, async () => {
    const result = await call("list_activities", { sport_type: "run", limit: 20, sort: "date_asc" }, protocol);
    expect(result.matched).toBe(5); expect(result.showing).toBe(5);
    expect(result.activities.map((row: { name: string }) => row.name)).toEqual(produced.names);
    expect(result.activities.every((row: { distance_km: number; date: string }) =>
      row.distance_km === 10 && row.date === "2026-10-23")).toBe(true);
  });
  for (const name of ["mixed", "zero", "empty"] as const) {
    it(`strength ${name} totals retain null/zero in the authenticated Worker response via ${protocol}`, async () => {
      const session = produced.strength[name];
      const result = await call("strength_session", { activity_id: `garmin-${session.activity_id}` }, protocol);
      expect(result.available).toBe(true);
      const summary = result.session.summary;
      expect(summary.external_volume_kg).toBe(name === "mixed" ? null : 0);
      expect(summary.active_duration_seconds).toBe(name === "mixed" ? null : 0);
      expect(summary.total_repetitions).toBe(name === "mixed" ? 20 : 0);
      if (name === "mixed") expect(summary.by_exercise.UNKNOWN.external_volume_kg).toBeNull();
    });
  }
}
