import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";

const issuer = "https://coach-profile-matrix-synthetic.cloudflareaccess.com";
const hostname = "coach-profile-matrix.example";
const indexKey = "coach/indexes/profiles-v1.json";
const indexModes = ["valid", "missing", "stale-keyset"] as const;
const protocols = ["2026-07-28", "2025-03-26"] as const;
let privateKey: CryptoKey;

const newerCreated = {
  schema_version: 1, profile_id: "111111111111111111111111", name: "Synthetic newer", sport: "running",
  effective_from: "2026-01-01", created_at: "2026-01-03T12:00:00Z", default_time_basis: "elapsed",
  zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
};
const olderCreated = {
  schema_version: 1, profile_id: "eeeeeeeeeeeeeeeeeeeeeeee", name: "Synthetic older", sport: "running",
  effective_from: "2026-01-01", created_at: "2026-01-02T12:00:00Z", default_time_basis: "elapsed",
  zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
};
const laterEffective = {
  schema_version: 1, profile_id: "777777777777777777777777", name: "Synthetic later", sport: "running",
  effective_from: "2026-02-01", created_at: "2026-01-01T12:00:00Z", default_time_basis: "elapsed",
  zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
};
const tiedHigherId = { ...olderCreated, created_at: "2026-01-03T12:00:00Z" };
const staleOnly = {
  schema_version: 1, profile_id: "999999999999999999999999", name: "Synthetic stale index", sport: "running",
  effective_from: "2026-01-01", created_at: "2026-01-09T12:00:00Z", default_time_basis: "elapsed",
  zones: [{ label: "Easy", min_bpm: 100, max_bpm: 140 }], references: {}, source: "user",
};
type Profile = typeof newerCreated;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "coach-profile-matrix", alg: "RS256", use: "sig" };
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden in profile matrix");
    return Response.json({ keys: [jwk] });
  });
});
afterAll(() => vi.restoreAllMocks());

function profileKey(profile: Profile): string {
  return `coach/profiles/v1/${profile.effective_from}/${profile.profile_id}.json`;
}

async function call(profiles: Profile[], mode: typeof indexModes[number], protocol: string, date?: string) {
  const keys = profiles.map(profileKey);
  const stored = new Map(profiles.map(profile => [profileKey(profile), JSON.stringify(profile)]));
  if (mode !== "missing") {
    stored.set(indexKey, JSON.stringify(mode === "valid"
      ? { profile_keys: keys, profiles }
      : {
        // Same key/profile counts as the canonical store, but one wrong key.
        // The tempting newer profile must never leak from this stale snapshot.
        profile_keys: [profileKey(staleOnly), ...keys.slice(1)], profiles: [staleOnly, ...profiles.slice(1)],
      }));
  }
  const bucket = {
    async list(options: { prefix?: string; limit?: number } = {}) {
      const listed = [...stored.keys()].filter(key => key.startsWith(options.prefix ?? "")).sort();
      const limit = options.limit ?? 1000;
      return { objects: listed.slice(0, limit).map(key => ({ key })), truncated: listed.length > limit };
    },
    async get(key: string) {
      const body = stored.get(key);
      if (body === undefined) return null;
      return { size: new TextEncoder().encode(body).byteLength, etag: "synthetic-profile-matrix",
        body: new Response(body).body };
    },
  } as unknown as R2Bucket;
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "coach-profile-matrix" }).setIssuer(issuer)
    .setAudience("coach-profile-matrix").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const modern = protocol === "2026-07-28";
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: {
      host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(modern ? { "mcp-method": "tools/call", "mcp-name": "coach_profile" } : {}),
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
      name: "coach_profile", arguments: date === undefined ? {} : { date },
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}),
    } }),
  }), {
    ...env, SLIPSTREAM_DATA: bucket, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "coach-profile-matrix",
    MCP_HOSTNAME: hostname, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) },
  }, ctx);
  const body = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, body).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = body.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  expect(message.error, body).toBeUndefined();
  expect(message.result.isError, body).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  const result = message.result.structuredContent;
  expect(result.selected_for_date).toBe(date ?? null);
  expect(result.profiles).toHaveLength(profiles.length);
  expect(result.profiles.map((profile: Profile) => profile.profile_id)).not.toContain("999999999999999999999999");
  return result;
}

for (const mode of indexModes) for (const protocol of protocols) {
  for (const query of [
    { label: "dated", date: "2026-01-10", expectedId: "111111111111111111111111" },
    { label: "undated", date: undefined, expectedId: "777777777777777777777777" },
  ]) {
    it(`profile matrix newer/index=${mode}/date=${query.label}/protocol=${protocol}`, async () => {
      const result = await call([newerCreated, olderCreated, laterEffective], mode, protocol, query.date);
      expect(result.available).toBe(true);
      expect(result.profile.profile_id).toBe(query.expectedId);
    });
  }
  it(`profile matrix undated-created/index=${mode}/protocol=${protocol}`, async () => {
    const result = await call([newerCreated, olderCreated], mode, protocol);
    expect(result.available).toBe(true);
    expect(result.profile.profile_id).toBe("111111111111111111111111");
  });
  for (const order of ["ascending", "descending"] as const) for (const date of ["2026-01-10", undefined]) {
    it(`profile matrix full-tie/index=${mode}/order=${order}/date=${date ?? "undated"}/protocol=${protocol}`, async () => {
      const profiles = order === "ascending" ? [newerCreated, tiedHigherId] : [tiedHigherId, newerCreated];
      const result = await call(profiles, mode, protocol, date);
      expect(result.available).toBe(true);
      expect(result.profile.profile_id).toBe("111111111111111111111111");
    });
  }
  it(`profile matrix before-effective/index=${mode}/protocol=${protocol}`, async () => {
    const result = await call([newerCreated, olderCreated, laterEffective], mode, protocol, "2025-12-31");
    expect(result.available).toBe(false);
    expect(result.profile).toBeNull();
  });
}
