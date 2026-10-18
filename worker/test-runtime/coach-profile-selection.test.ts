import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";

const issuer = "https://coach-profile-selection-synthetic.cloudflareaccess.com";
const hostname = "coach-profile-selection.example";
let privateKey: CryptoKey;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "coach-profile-selection", alg: "RS256", use: "sig" };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden in profile selection contract");
    return Response.json({ keys: [jwk] });
  });
});

afterAll(() => vi.restoreAllMocks());

it("selects newer created_at from a valid index when its canonical key sorts first", async () => {
  // Literal valid profiles, in canonical key order. Python select_profile must
  // choose the first because its created_at is newer, despite the equal date.
  const newer = {
    schema_version: 1, profile_id: "111111111111111111111111", name: "Synthetic newer", sport: "running",
    effective_from: "2026-01-01", created_at: "2026-01-03T12:00:00Z", default_time_basis: "elapsed",
    zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
  };
  const older = {
    schema_version: 1, profile_id: "eeeeeeeeeeeeeeeeeeeeeeee", name: "Synthetic older", sport: "running",
    effective_from: "2026-01-01", created_at: "2026-01-02T12:00:00Z", default_time_basis: "elapsed",
    zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
  };
  const newerKey = "coach/profiles/v1/2026-01-01/111111111111111111111111.json";
  const olderKey = "coach/profiles/v1/2026-01-01/eeeeeeeeeeeeeeeeeeeeeeee.json";
  const stored = new Map<string, string>([
    [newerKey, JSON.stringify(newer)],
    [olderKey, JSON.stringify(older)],
    ["coach/indexes/profiles-v1.json", JSON.stringify({
      profile_keys: [newerKey, olderKey], profiles: [newer, older],
    })],
  ]);
  // Outer storage fake, as in output-privacy.test.ts; R2Storage stays real.
  const bucket = {
    async list(options: { prefix?: string; limit?: number } = {}) {
      const keys = [...stored.keys()].filter(key => key.startsWith(options.prefix ?? "")).sort();
      const limit = options.limit ?? 1000;
      return { objects: keys.slice(0, limit).map(key => ({ key })), truncated: keys.length > limit };
    },
    async get(key: string) {
      const body = stored.get(key);
      if (body === undefined) return null;
      return { size: new TextEncoder().encode(body).byteLength, etag: "synthetic-profile",
        body: new Response(body).body };
    },
  } as unknown as R2Bucket;

  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "coach-profile-selection" }).setIssuer(issuer)
    .setAudience("coach-profile-selection").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const protocol = "2026-07-28";
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: {
      host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      "mcp-method": "tools/call", "mcp-name": "coach_profile",
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "coach_profile", arguments: { date: "2026-01-10" },
      _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} },
    } }),
  }), {
    ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "coach-profile-selection", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "", SLIPSTREAM_DATA: bucket,
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) },
  }, ctx);
  const body = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, body).toBe(200);
  expect(response.headers.get("content-type")).toContain("application/json");
  const message = JSON.parse(body);
  expect(message.error, body).toBeUndefined();
  expect(message.result.isError, body).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  expect(message.result.structuredContent).toMatchObject({
    available: true, selected_for_date: "2026-01-10", profile: { profile_id: "111111111111111111111111" },
  });
});
