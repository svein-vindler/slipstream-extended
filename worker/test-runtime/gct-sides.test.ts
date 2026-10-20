import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import produced from "./generated/gct-sides.json";
import legacy140 from "../../tests/fixtures/gct_engine_140.json";

const issuer = "https://gct-sides.cloudflareaccess.com", hostname = "gct-sides.example";
const prefix = "activities/2026/42/";
let privateKey: CryptoKey, jwk: Record<string, unknown>, token: string, testEnv: Env;
let operations: string[], reservations: number;

type Fixture = { analysis_id: string; objects_base64: Record<string, string> };
async function load(fixture: Fixture) {
  clearSummaryCaches();
  for (const [name, encoded] of Object.entries(fixture.objects_base64)) {
    await env.SLIPSTREAM_DATA.put(name, Uint8Array.from(atob(encoded), char => char.charCodeAt(0)));
  }
  operations.length = 0;
}
async function analysis(fixture: Fixture) {
  const stored = await env.SLIPSTREAM_DATA.get(`${prefix}coach-input/v1/canonical/${fixture.analysis_id}.json`);
  if (!stored) throw new Error("Missing genuine GCT fixture");
  return await new Response(stored.body.pipeThrough(new DecompressionStream("gzip"))).json() as Record<string, unknown>;
}

async function competingContexts(sameTime: boolean) {
  const source = await analysis(produced.left);
  const template = source.user_context as Record<string, unknown>;
  const keys = Object.keys(produced.left.objects_base64);
  for (const [id, hour, rpe, note] of [
    ["ctx-a", "13", 4, "SYNTHETIC_GCT_NOTE_A"],
    ["ctx-b", sameTime ? "13" : "14", 8, "SYNTHETIC_GCT_NOTE_B"],
  ] as const) {
    const name = `${prefix}context/v1/20261008T${hour}0000000-${id}.json`;
    await env.SLIPSTREAM_DATA.put(name, JSON.stringify({ ...template, context_id: id, rpe, note,
      created_at: `2026-10-08T${hour}:00:00.000Z` }));
    keys.push(name);
  }
  const before = new Map<string, ArrayBuffer>();
  for (const name of keys) {
    const stored = await env.SLIPSTREAM_DATA.get(name);
    if (!stored) throw new Error("Missing synthetic context-preservation input");
    before.set(name, await stored.arrayBuffer());
  }
  operations.length = 0;
  return { source, before };
}

async function preserved(before: Map<string, ArrayBuffer>) {
  for (const [name, bytes] of before) {
    const stored = await env.SLIPSTREAM_DATA.get(name);
    expect(stored).not.toBeNull();
    expect(new Uint8Array(await stored!.arrayBuffer())).toEqual(new Uint8Array(bytes));
  }
}

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "gct", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  operations = []; reservations = 0;
  // A previous case's real context PUT can outrank the fixed-date fixtures.
  // Reset only this synthetic activity's context namespace before reseeding.
  const contexts = await env.SLIPSTREAM_DATA.list({ prefix: `${prefix}context/v1/`, limit: 100 });
  if (contexts.truncated) throw new Error("Synthetic GCT context isolation exceeded its limit");
  if (contexts.objects.length) await env.SLIPSTREAM_DATA.delete(contexts.objects.map(item => item.key));
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "gct" }).setIssuer(issuer).setAudience("gct")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    if ((input instanceof Request ? input.url : String(input)) === issuer + "/cdn-cgi/access/certs") {
      return Response.json({ keys: [jwk] });
    }
    throw new Error("Unexpected network in synthetic GCT test");
  });
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, property) {
    if (property === "getByName") return () => new Proxy(coordinator, { get(stub, method) {
      if (method === "reserveBudgeted") return (...args: Parameters<typeof coordinator.reserveBudgeted>) => {
        reservations++; return coordinator.reserveBudgeted(...args);
      };
      return Reflect.get(stub, method);
    } });
    return Reflect.get(target, property);
  } });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const method = Reflect.get(target, property);
    if (typeof method !== "function") return method;
    return (...args: unknown[]) => { operations.push(String(property)); return Reflect.apply(method, target, args); };
  } });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "gct", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, REFRESH_COORDINATOR: namespace, MCP_WRITES_ENABLED: "true",
    GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
  await load(produced.left);
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });

async function call(protocol: string, name: string, args: Record<string, unknown>) {
  const modern = protocol === "2026-07-28", ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined();
  if (message.result.structuredContent) {
    expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  }
  return message.result;
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`GCT review return: same newest timestamp rejects before reservation ${protocol}`, async () => {
    const { source, before } = await competingContexts(true);
    const result = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: (source.source as Record<string, unknown>).fit_sha256, raw_side: "right" } });
    expect.soft(result.isError).toBe(true);
    expect.soft(result.structuredContent?.saved).not.toBe(true);
    expect.soft(reservations).toBe(0);
    expect.soft(operations.filter(op => op === "put" || op === "delete")).toEqual([]);
    const text = JSON.stringify(result);
    expect.soft(text).not.toContain("SYNTHETIC_GCT_NOTE_A");
    expect.soft(text).not.toContain("SYNTHETIC_GCT_NOTE_B");
    expect.soft(String(result.content[0].text).length).toBeGreaterThan(0);
    await preserved(before);
  });

  it(`GCT review return: unique newest timestamp retains exact context ${protocol}`, async () => {
    const { source, before } = await competingContexts(false);
    const result = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: (source.source as Record<string, unknown>).fit_sha256, raw_side: "right" } });
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent.context).toMatchObject({ schema_version: 2, rpe: 8,
      conditions: "Synthetic dry", note: "SYNTHETIC_GCT_NOTE_B", gct_balance_side: { raw_side: "right" } });
    expect(reservations).toBe(1);
    expect(operations.filter(op => op === "put")).toHaveLength(1);
    expect(operations.filter(op => op === "delete")).toEqual([]);
    await preserved(before);
  });

  it(`GCT sides: genuine complete/unknown/stale and historical reads ${protocol}`, async () => {
    for (const mode of ["left", "right", "unknown", "stale"] as const) {
      const fixture = produced[mode]; await load(fixture);
      const result = await call(protocol, "coach_input", { activity_id: "42" });
      expect(result.isError).not.toBe(true);
      const output = result.structuredContent;
      expect(output.available).toBe(true);
      const block = output.analysis.ground_contact_sides;
      expect(output.analysis.analyzer_version).toBe("1.6.0");
      expect(block.session.left_percent).toBe(mode === "left" ? 49.25 : mode === "right" ? 50.75 : null);
      expect(block.session.right_percent).toBe(mode === "left" ? 50.75 : mode === "right" ? 49.25 : null);
      expect(block.laps.items[1].left_percent).toBe(mode === "left" ? 51.25 : mode === "right" ? 48.75 : null);
      expect(output.analysis.recorded_running_metrics.average_ground_contact_balance.value).toBe(49.25);
      expect(operations.filter(op => op === "put" || op === "delete")).toEqual([]);
      expect(reservations).toBe(0);
    }
    await load(legacy140);
    const old = await call(protocol, "coach_input", { activity_id: "42" });
    expect(old.structuredContent.analysis.analyzer_version).toBe("1.4.0");
    expect(old.structuredContent.analysis).not.toHaveProperty("ground_contact_sides");
  });

  it(`GCT sides: rejects forged complement, provenance, clock and extras ${protocol}`, async () => {
    const original = await analysis(produced.left);
    const key = `${prefix}coach-input/v1/canonical/${produced.left.analysis_id}.json`;
    for (const [field, value] of [["right_percent", 99], ["left_percent", false],
      ["calculated_side", "left"], ["private_extra", "synthetic-extra"]] as const) {
      const changed = structuredClone(original);
      const block = changed.ground_contact_sides as { session: Record<string, unknown> };
      block.session[field] = value;
      await env.SLIPSTREAM_DATA.put(key, JSON.stringify(changed));
      const result = await call(protocol, "coach_input", { activity_id: "42" });
      expect(result.isError).not.toBe(true); expect(result.structuredContent.available).toBe(false);
      expect(result.structuredContent.message).toContain("could not be verified");
    }
    for (const mutate of [
      (block: Record<string, unknown>) => { block.source_fit_sha256 = "f".repeat(64); },
      (block: Record<string, unknown>) => { (block.declaration as Record<string, unknown>).source = "manufacturer"; },
      (block: Record<string, unknown>) => { ((block.laps as { items: Record<string, unknown>[] }).items[0]).start_time_utc = "2026-10-08T00:00:00.000Z"; },
    ]) {
      const changed = structuredClone(original); mutate(changed.ground_contact_sides as Record<string, unknown>);
      await env.SLIPSTREAM_DATA.put(key, JSON.stringify(changed));
      expect((await call(protocol, "coach_input", { activity_id: "42" })).structuredContent.available).toBe(false);
    }
    expect(operations.filter(op => op === "put" || op === "delete")).toEqual([]);
  });

  it(`GCT sides: side-only append retains RPE/note and cooldown ${protocol}`, async () => {
    const source = await analysis(produced.left);
    const oldKey = prefix + "context/v1/20261008T120000000-ctx-sides.json";
    const original = await (await env.SLIPSTREAM_DATA.get(oldKey))!.text();
    const result = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: (source.source as Record<string, unknown>).fit_sha256, raw_side: "right" } });
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent.context).toMatchObject({ schema_version: 2, rpe: 7,
      conditions: "Synthetic dry", note: "Synthetic note", gct_balance_side: { raw_side: "right" } });
    const old = await env.SLIPSTREAM_DATA.get(oldKey);
    expect(await old!.text()).toBe(original);
    const retry = await call(protocol, "add_activity_context", { activity_id: "42", rpe: 8 });
    expect(retry.isError).toBe(true); expect(retry.content[0].text).toContain("Retry in");
    expect(operations.filter(op => op === "put")).toHaveLength(1);
  });

  it(`GCT sides: stale declaration rejects before reservation; valid retry succeeds ${protocol}`, async () => {
    const rejected = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: "f".repeat(64), raw_side: "left" } });
    expect(rejected.isError).toBe(true); expect(rejected.content[0].text).toContain("exact stored FIT hash");
    expect(reservations).toBe(0); expect(operations.filter(op => op === "put")).toEqual([]);
    const source = await analysis(produced.left);
    const accepted = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: (source.source as Record<string, unknown>).fit_sha256, raw_side: "left" } });
    expect(accepted.structuredContent.saved).toBe(true);
    expect(reservations).toBe(1);
  });
}
