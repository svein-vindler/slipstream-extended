import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";
import contract from "./generated/health-contract.json";

type Stream = "sleep" | "hrv";
type Snapshot = typeof contract.states.initial;
const issuer = "https://synthetic-health-contract.cloudflareaccess.com";
const hostname = "health-contract.example";
const reads: string[] = [];
const writes: string[] = [];
const trackedBucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return (...args: unknown[]) => {
      if (property === "get") reads.push(String(args[0]));
      if (property === "put" || property === "delete") writes.push(String(args[0]));
      return Reflect.apply(value, target, args);
    };
  },
});
const testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-contract",
  MCP_HOSTNAME: hostname, HEALTH_TIMEZONE: contract.fixture.timezone, SLIPSTREAM_DATA: trackedBucket };
const ordinary = contract.fixture.cases[0];
let privateKey: CryptoKey;
let requestId = 0;
let fetchSpy: ReturnType<typeof vi.spyOn>;

async function load(state: Snapshot) {
  for (const key of Object.keys(contract.states.initial)) {
    if (!(key in state)) await env.SLIPSTREAM_DATA.delete(key);
  }
  for (const [key, value] of Object.entries(state)) {
    const stored = await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(value.body), c => c.charCodeAt(0)), {
      httpMetadata: { contentType: value.contentType, contentEncoding: value.contentEncoding ?? undefined },
    });
    // Real local R2 metadata must agree with the Python S3 client's revision.
    expect(stored.etag).toBe(value.etag);
  }
}

async function rpc(method: string, params: Record<string, unknown>, subject: string) {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "contract-key" }).setIssuer(issuer)
    .setAudience(testEnv.ACCESS_AUD).setSubject(subject).setIssuedAt().setExpirationTime("5m")
    .sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++requestId, method, params }),
  }), testEnv, ctx);
  const text = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("cache-control")).toContain("no-store");
  const message = response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(text.split("\n").find(line => line.startsWith("data: "))!.slice(6))
    : JSON.parse(text);
  expect(message.error, text).toBeUndefined();
  return message.result;
}

async function session() {
  const subject = crypto.randomUUID();
  const result = await rpc("initialize", { protocolVersion: "2025-03-26", capabilities: {},
    clientInfo: { name: "synthetic-contract-test", version: "1" } }, subject);
  expect(result.serverInfo.name).toBe("slipstream-fitness");
  return subject;
}

async function history(stream: Stream, day: string, subject: string, full = false) {
  const name = stream === "sleep" ? "sleep_history" : "hrv_history";
  const result = await rpc("tools/call", { name, arguments: {
    start_date: day, end_date: day, granularity: "daily", detail_level: full ? "full" : "summary",
  } }, subject);
  expect(result.isError).not.toBe(true);
  const output = outputSchemas[name].parse(result.structuredContent);
  expect(JSON.parse(result.content[0].text)).toEqual(output);
  expect(JSON.stringify(output)).not.toContain("synthetic-provider-field");
  return output;
}

async function indexBodies() {
  return Promise.all(Object.keys(contract.states.initial).filter(key => key.startsWith("health/indexes/"))
    .map(async key => [key, await (await env.SLIPSTREAM_DATA.get(key))?.text()]));
}

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "contract-key", alg: "RS256", use: "sig" };
  fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error(`Unexpected network request: ${url}`);
    return Response.json({ keys: [jwk] });
  });
});
afterEach(async () => {
  expect(writes).toEqual([]); // Ordinary MCP reads never write data, indexes or receipts.
  for (const key of Object.keys(contract.states.initial)) await env.SLIPSTREAM_DATA.delete(key);
});
afterAll(() => {
  expect(fetchSpy).toHaveBeenCalledTimes(1); // Authentication only; no Garmin/GitHub network.
  vi.restoreAllMocks();
});

describe("Python import bytes through the authenticated MCP Worker", () => {
  it.each(contract.fixture.cases)("preserves metrics and local night context: $id", async fixture => {
    await load(contract.states.initial);
    const subject = await session();
    reads.length = 0;
    const sleep = await history("sleep", fixture.date, subject);
    const hrv = await history("hrv", fixture.date, subject);
    const expected = fixture.expected;
    for (const row of [sleep.days[0], hrv.days[0]]) {
      expect(row).toMatchObject({ date: fixture.date, status: "available", index_state: "verified",
        night_of: expected.night_of, sleep_start_local: expected.sleep_start_local,
        sleep_end_local: expected.sleep_end_local, local_time_source: expected.local_time_source });
    }
    expect(sleep.days[0].summary).toMatchObject({ sleep_seconds: expected.sleep_seconds, sleep_score: expected.sleep_score });
    expect(hrv.days[0].derived).toMatchObject({ mean_ms: expected.hrv_mean_ms });
    expect(hrv.days[0].night_context_stream).toBe(fixture.id === "travel" ? "sleep" : "hrv");
    expect(sleep.source_objects_read).toBe(1); // Verified summary reads only its index.
    expect(hrv.source_objects_read).toBe(2); // Index plus matching sleep context, not HRV detail.
    expect(reads).toEqual([
      `health/indexes/sleep/v1/${fixture.date.slice(0, 4)}/${fixture.date.slice(5, 7)}.json`,
      `health/indexes/hrv/v1/${fixture.date.slice(0, 4)}/${fixture.date.slice(5, 7)}.json`,
      `health/sleep/v1/${fixture.date.slice(0, 4)}/${fixture.date.slice(5, 7)}/${fixture.date}.json`,
    ]);
    const full = await history("hrv", fixture.date, subject, true);
    expect(full.days[0].readings).toHaveLength(2);
    expect(full.days[0].derived).toEqual(hrv.days[0].derived);
    const fullSleep = await history("sleep", fixture.date, subject, true);
    expect(fullSleep.days[0].stages).toHaveLength(1);
    expect(fullSleep.days[0].summary).toEqual(sleep.days[0].summary);
  });

  it.each(["sleep", "hrv"] as const)("sees changed %s in a warm session through an interrupted write and recovery", async stream => {
    await load(contract.states.repeated);
    const subject = await session();
    const before = await history(stream, ordinary.date, subject);
    expect(before.days[0].index_state).toBe("verified");
    const interrupted = contract.states[`interrupted_${stream}`];
    await load(interrupted);
    const indexes = await indexBodies();
    const updated = await history(stream, ordinary.date, subject);
    expect(updated.days[0].index_state).toBe("read_through");
    expect(updated.index_consistency.stale_dates).toEqual([ordinary.date]);
    if (stream === "sleep") expect(updated.days[0].summary?.sleep_seconds).toBe(ordinary.changed.sleep_seconds);
    else expect(updated.days[0].derived?.mean_ms).toBe(ordinary.changed.hrv_mean_ms);
    expect(await indexBodies()).toEqual(indexes); // Reads do not claim to repair storage.
    await load(contract.states[`repaired_${stream}`]);
    const recovered = await history(stream, ordinary.date, subject);
    expect(recovered.days[0].index_state).toBe("verified");
    expect(recovered.index_consistency.stale_dates).toEqual([]);
    const repeated = await history(stream, ordinary.date, subject);
    expect(repeated).toEqual(recovered);
  });

  it.each(["missing", "corrupt"] as const)("reads canonical data with %s indexes and observes pipeline repair", async kind => {
    await load(contract.states.completed);
    const subject = await session();
    const expected = await history("sleep", ordinary.date, subject);
    await load(contract.states[kind === "missing" ? "missing_indexes" : "corrupt_indexes"]);
    const indexes = await indexBodies();
    for (const stream of ["sleep", "hrv"] as const) {
      const result = await history(stream, ordinary.date, subject);
      expect(result.days[0].index_state).toBe("read_through");
      expect(result.days[0].status).toBe("available");
      expect(result.index_consistency.invalid_index_objects).toHaveLength(kind === "corrupt" ? 1 : 0);
      if (stream === "sleep") expect(result.days[0].summary).toEqual(expected.days[0].summary);
      else expect(result.days[0].derived?.mean_ms).toBe(ordinary.changed.hrv_mean_ms);
    }
    expect(await indexBodies()).toEqual(indexes);
    await load(contract.states[`recovered_${kind}`]);
    for (const stream of ["sleep", "hrv"] as const) {
      const result = await history(stream, ordinary.date, subject);
      expect(result.days[0].index_state).toBe("verified");
    }
  });

  it("keeps good answers after wrong-date or incomplete provider responses", async () => {
    await load(contract.states.completed);
    const subject = await session();
    const before = await history("sleep", ordinary.date, subject);
    for (const state of [contract.states.wrong_date, contract.states.incomplete]) {
      await load(state);
      expect(await history("sleep", ordinary.date, subject)).toEqual(before);
      expect((await history("hrv", ordinary.date, subject)).days[0].derived?.mean_ms).toBe(ordinary.changed.hrv_mean_ms);
    }
  });

  it("reloads the Python-exported summary after its ETag changes in a warm session", async () => {
    await load(contract.states.initial);
    const subject = await session();
    async function daily() {
      const result = await rpc("tools/call", { name: "daily_health", arguments: {
        start_date: ordinary.date, end_date: ordinary.date,
      } }, subject);
      expect(result.isError).not.toBe(true);
      return outputSchemas.daily_health.parse(result.structuredContent);
    }
    const before = await daily();
    expect(before.days[0]).toMatchObject({ date: ordinary.date, sleep_hours: 7, hrv_last_night_avg_ms: 50 });
    await load(contract.states.repeated);
    expect(await daily()).toEqual(before);
    await load(contract.states.completed);
    const updated = await daily();
    expect(updated.days[0]).toMatchObject({ date: ordinary.date, sleep_hours: 6.83, hrv_last_night_avg_ms: 70 });
    expect(await daily()).toEqual(updated);
  });
});
