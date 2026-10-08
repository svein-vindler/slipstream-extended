import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";
import { clearSummaryCaches } from "../src/r2-storage";
import { decodePossiblyGzippedText } from "../src/security";
import contract from "./generated/training-context.json";

const issuer = "https://synthetic-training-context.cloudflareaccess.com";
const hostname = "training-context.example";
const fitKey = "activities/2026/42/activity.v1.json";
const reads: string[] = [];
const operations = { get: 0, head: 0, list: 0, put: 0, delete: 0 };
let privateKey: CryptoKey;
let fetchSpy: ReturnType<typeof vi.spyOn>;
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return (...args: unknown[]) => {
      if (property in operations) operations[property as keyof typeof operations]++;
      if (property === "get") reads.push(String(args[0]));
      return Reflect.apply(value, target, args);
    };
  },
});

async function call(name: string, args: Record<string, unknown> = { activity_id: "garmin-42" }) {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "training-key" }).setIssuer(issuer)
    .setAudience("synthetic-training").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } }),
  }), { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-training", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    SLIPSTREAM_DATA: bucket, MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } }, ctx);
  const text = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("cache-control")).toContain("no-store");
  const message = response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(text.split("\n").find(line => line.startsWith("data: "))!.slice(6)) : JSON.parse(text);
  expect(message.error, text).toBeUndefined();
  expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

async function decodedFixture() {
  const object = await env.SLIPSTREAM_DATA.get(fitKey);
  return JSON.parse(await decodePossiblyGzippedText(await object!.arrayBuffer(), 32 * 1024 * 1024));
}
function resetCounts() {
  Object.keys(operations).forEach(key => { operations[key as keyof typeof operations] = 0; });
  reads.length = 0;
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "training-key", alg: "RS256", use: "sig" };
  fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected training-context network request");
    return Response.json({ keys: [jwk] });
  });
});
beforeEach(async () => {
  await env.SLIPSTREAM_DATA.delete(`${fitKey}.gz`);
  for (const [key, value] of Object.entries(contract)) {
    await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(value.body), c => c.charCodeAt(0)), {
      httpMetadata: { contentType: value.content_type, contentEncoding: value.encoding ?? undefined },
    });
  }
  clearSummaryCaches();
  resetCounts();
});
afterAll(() => {
  expect(fetchSpy).toHaveBeenCalledTimes(1); // Synthetic auth only; no Garmin or GitHub.
  vi.restoreAllMocks();
});

it("serves Python-exported Training Effect through actual authenticated MCP with bounded reads", async () => {
  const output = await call("endurance_session");
  expect(outputSchemas.endurance_session.parse(output)).toEqual(output);
  expect(output).toMatchObject({ available: true, activity: { id: "garmin-42" },
    training_context: { activity_id: "garmin-42", local_date: "2026-10-08", association: "matched",
      aerobic: { status: "available", value: 3.4 }, anaerobic: { status: "available", value: 0 } } });
  expect(output.session.activity.id).toBe("42");
  expect(operations).toEqual({ get: 3, head: 1, list: 0, put: 0, delete: 0 });
  expect(reads).toEqual(["summary/activities.csv", "activities/2026/42/activity.endurance.v1.json", fitKey]);
  expect(JSON.stringify(output.training_context)).not.toMatch(/messages|training_load_peak|trainingEffect|position/);
});

it("reuses the strength tool's loaded FIT with no added GET", async () => {
  const output = await call("strength_session", { activity_id: "42" });
  expect(outputSchemas.strength_session.parse(output)).toEqual(output);
  expect(output.training_context.aerobic.value).toBe(3.4);
  expect(operations).toEqual({ get: 2, head: 1, list: 0, put: 0, delete: 0 });
});

it("leaves summary lists unchanged and never reads FIT per row", async () => {
  const output = await call("list_activities", {});
  expect(outputSchemas.list_activities.parse(output)).toEqual(output);
  expect(output.activities[0]).not.toHaveProperty("training_context");
  expect(operations).toEqual({ get: 1, head: 1, list: 0, put: 0, delete: 0 });
});

it("preserves legacy TCX availability when the optional source is absent or corrupt", async () => {
  await env.SLIPSTREAM_DATA.delete(fitKey);
  const missing = await call("endurance_session");
  expect(missing.available).toBe(true);
  expect(missing.training_context.association).toBe("source_missing");
  expect(operations.get).toBe(4); // Summary, TCX and two bounded alias misses.
  resetCounts();
  await env.SLIPSTREAM_DATA.put(fitKey, "{invalid-json");
  const corrupt = await call("endurance_session");
  expect(corrupt.available).toBe(true);
  expect(corrupt.session).toEqual(missing.session);
  expect(corrupt.training_context.association).toBe("source_unreadable");
  expect(operations).toEqual({ get: 2, head: 1, list: 0, put: 0, delete: 0 });
});

it("reads legacy, wrong-ID and multi-session objects without changing dataset availability", async () => {
  const decoded = await decodedFixture();
  const session = decoded.messages.session_mesgs[0];
  for (const [payload, association] of [
    [null, "invalid_schema"],
    [{ ...decoded, messages: {} }, "sessions_missing"],
    [{ ...decoded, schema_version: 2 }, "invalid_schema"],
    [{ ...decoded, activity: { ...decoded.activity, id: "43" } }, "id_mismatch"],
    [{ ...decoded, messages: { session_mesgs: [session, session] } }, "sessions_ambiguous"],
  ] as const) {
    await env.SLIPSTREAM_DATA.put(fitKey, JSON.stringify(payload));
    const output = await call("endurance_session");
    expect(outputSchemas.endurance_session.parse(output)).toEqual(output);
    expect(output.available).toBe(true);
    expect(output.training_context.association).toBe(association);
    expect(output.training_context.aerobic.value).toBeNull();
  }
  expect(operations.put + operations.list + operations.delete).toBe(0);
});

it("supports the gzip alias and exposes missing fields without regenerating old objects", async () => {
  const decoded = await decodedFixture();
  delete decoded.messages.session_mesgs[0].total_training_effect;
  decoded.messages.session_mesgs[0].total_anaerobic_training_effect = null;
  await env.SLIPSTREAM_DATA.delete(fitKey);
  await env.SLIPSTREAM_DATA.put(`${fitKey}.gz`, JSON.stringify(decoded));
  const output = await call("endurance_session");
  expect(output.training_context).toMatchObject({ association: "matched",
    aerobic: { status: "missing", value: null }, anaerobic: { status: "missing", value: null } });
  expect(operations).toEqual({ get: 4, head: 1, list: 0, put: 0, delete: 0 });
});

it("leaves saved profiles and immutable historical Coach Input bytes/IDs unchanged", async () => {
  const analysisKey = "activities/2026/42/coach-input/v1/canonical/synthetic-old.json";
  const profileKey = "coach/profiles/v1/2026-01-01/synthetic-old.json";
  const profile = { profile_id: "synthetic-old", effective_from: "2026-01-01",
    zones: [{ label: "user-zone", min_bpm: 100, max_bpm: 150 }] };
  const analysis = { schema_version: 1, analyzer_version: "1.0.0", analysis_id: "synthetic-old-analysis",
    activity_id: "42", activity: { date: "2026-10-08" }, profile, user_context: { rpe: 7 } };
  const oldAnalysisBytes = JSON.stringify(analysis);
  const oldProfileBytes = JSON.stringify(profile);
  await env.SLIPSTREAM_DATA.put(analysisKey, oldAnalysisBytes);
  await env.SLIPSTREAM_DATA.put(profileKey, oldProfileBytes);
  const detail = await call("endurance_session");
  expect(detail.training_context.aerobic.value).toBe(3.4);
  const saved = await call("coach_input");
  expect(outputSchemas.coach_input.parse(saved)).toEqual(saved);
  expect(saved.analysis).toEqual(analysis);
  expect(saved.analysis).not.toHaveProperty("training_context");
  expect(await (await env.SLIPSTREAM_DATA.get(analysisKey))!.text()).toBe(oldAnalysisBytes);
  expect(await (await env.SLIPSTREAM_DATA.get(profileKey))!.text()).toBe(oldProfileBytes);
  expect(operations.put + operations.delete).toBe(0);
});
