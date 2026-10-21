import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import produced from "./generated/coach-training-effect.json";
import matrix from "./generated/coach-training-effect-matrix.json";
import legacy130 from "../../tests/fixtures/coach_engine_130_structured_transition.json";

const issuer = "https://training-effect-synthetic.cloudflareaccess.com";
const hostname = "training-effect.example";
const rows = new Map<string, Uint8Array>();
const operations: string[] = [];
let key: CryptoKey, jwk: Record<string, unknown>, token: string, testEnv: Env;
let currentAnalysisId = produced.analysis_id;

function load(fixture: { analysis_id: string; objects_base64: Record<string, string> }) {
  clearSummaryCaches(); rows.clear(); operations.length = 0; currentAnalysisId = fixture.analysis_id;
  for (const [name, encoded] of Object.entries(fixture.objects_base64)) {
    rows.set(name, Uint8Array.from(atob(encoded), character => character.charCodeAt(0)));
  }
}

async function editCoach(change: (value: Record<string, unknown>) => void) {
  const name = `activities/2026/42/coach-input/v1/canonical/${currentAnalysisId}.json`;
  const bytes = rows.get(name);
  if (!bytes) throw new Error("Missing genuine Training Effect canonical fixture");
  const stream = new Response(bytes).body;
  if (!stream) throw new Error("Missing genuine canonical stream");
  const plain = bytes[0] === 0x1f && bytes[1] === 0x8b ? stream.pipeThrough(new DecompressionStream("gzip")) : stream;
  const body = await new Response(plain).json() as Record<string, unknown>;
  change(body);
  rows.set(name, new TextEncoder().encode(JSON.stringify(body)));
}

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "training-effect", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  load(produced);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected Training Effect fixture network request");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "training-effect" }).setIssuer(issuer).setAudience("training-effect")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const bucket = {
    head: async (name: string) => { operations.push(`HEAD ${name}`); return rows.has(name) ? { etag: "synthetic-effect" } : null; },
    get: async (name: string) => {
      operations.push(`GET ${name}`); const body = rows.get(name);
      return body === undefined ? null : { size: body.byteLength, etag: "synthetic-effect", body: new Response(body).body };
    },
    list: async () => { operations.push("LIST"); throw new Error("Unexpected Training Effect discovery"); },
    put: async () => { operations.push("PUT"); throw new Error("Unexpected Training Effect write"); },
    delete: async () => { operations.push("DELETE"); throw new Error("Unexpected Training Effect delete"); },
  } as unknown as R2Bucket;
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "training-effect", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });

async function call(protocol: string) {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": "coach_input" } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "coach_input",
      arguments: { activity_id: "garmin-42" }, ...(protocol === "2026-07-28" ? { _meta: {
        [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {},
      } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined(); expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`Training Effect stored-source integrity: preserves real estimates and rejects nested extras via ${protocol}`, async () => {
    const original = await call(protocol);
    expect(original.available).toBe(true);
    expect(original.analysis).toMatchObject({ analyzer_version: "1.7.0", summary: { distance_m: 2000 },
      training_effect: { source: "fit_session", method: "fit_recorded_estimate", association: "matched",
        aerobic: { value: 3.4, source_field: "total_training_effect" },
        anaerobic: { value: 0, source_field: "total_anaerobic_training_effect" } } });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
    operations.length = 0;
    await editCoach(analysis => {
      (analysis.training_effect as Record<string, unknown>).device_id = "synthetic-private-extra";
    });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const rejected = await call(protocol);
    expect(rejected.available).toBe(false);
    expect(JSON.stringify(rejected)).not.toContain("synthetic-private-extra");
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
  });
  for (const mode of ["zero", "missing", "invalid_estimate", "unverified_source", "no_clock",
    "partial_totals", "unknown_totals", "stored_context"] as const) {
    it(`Training Effect compatibility matrix: real ${mode} source stays explicit via ${protocol}`, async () => {
      const fixture = matrix.cases[mode]; load(fixture);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call(protocol);
      expect(output.available).toBe(true);
      expect(output.analysis).toMatchObject({ analysis_id: fixture.analysis_id, analyzer_version: "1.7.0",
        summary: { distance_m: 2000 }, training_effect: { source: "fit_session", method: "fit_recorded_estimate" } });
      const effect = output.analysis.training_effect;
      if (mode === "zero") expect(effect.aerobic).toMatchObject({ status: "available", value: 0 });
      if (mode === "missing") {
        expect(effect.aerobic).toMatchObject({ status: "missing", value: null });
        expect(effect.anaerobic).toMatchObject({ status: "missing", value: null });
      }
      if (mode === "invalid_estimate") expect(effect.aerobic).toMatchObject({ status: "invalid", value: null });
      if (mode === "unverified_source" || mode === "no_clock") {
        expect(effect.association).toBe(mode === "no_clock" ? "time_mismatch" : "invalid_schema");
        expect(effect.aerobic).toMatchObject({ status: "unavailable", value: null });
        expect(effect.anaerobic).toMatchObject({ status: "unavailable", value: null });
      }
      if (mode === "no_clock") expect(output.analysis).not.toHaveProperty("stored_context_snapshot");
      if (mode === "partial_totals" || mode === "unknown_totals") {
        expect(output.analysis.sections[0]).toMatchObject({ elapsed_seconds: null, distance_m: null,
          measurement_coverage: { elapsed_seconds: { status: mode === "partial_totals" ? "partial" : "unknown" },
            distance_m: { status: mode === "partial_totals" ? "partial" : "unknown" } } });
      }
      if (mode === "stored_context") {
        expect(output.analysis.stored_context_snapshot.sleep.metrics.sleep_seconds.value).toBe(25200);
        expect(output.analysis.stored_context_snapshot.training_day.recovery.value).toBe(0);
      }
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`]);
    });
  }
  const corruptions: Array<[string, (analysis: Record<string, any>) => void]> = [
    ["out of scale", analysis => { analysis.training_effect.aerobic.value = 5.1; }],
    ["fractional scale", analysis => { analysis.training_effect.aerobic.value = 3.14; }],
    ["boolean value", analysis => { analysis.training_effect.aerobic.value = false; }],
    ["missing claimed available", analysis => { analysis.training_effect.aerobic.value = null; }],
    ["wrong activity", analysis => { analysis.training_effect.activity_id = "43"; }],
    ["wrong FIT", analysis => { analysis.training_effect.source_fit_sha256 = "f".repeat(64); }],
    ["wrong local date", analysis => { analysis.training_effect.local_date = "2026-10-09"; }],
    ["unbound clock", analysis => { analysis.training_effect.session_start_time_utc = "2026-10-07T22:30:00.000Z"; }],
    ["unavailable with value", analysis => { analysis.training_effect.association = "source_missing"; }],
    ["nested source extra", analysis => { analysis.training_effect.aerobic.device_id = "synthetic-private-extra"; }],
    ["coverage counts", analysis => { analysis.sections[0].measurement_coverage.elapsed_seconds.observed_laps = 0; }],
    ["coverage extra", analysis => { analysis.sections[0].heart_rate_coverage.device_id = "synthetic-private-extra"; }],
    ["unknown becomes zero", analysis => { analysis.sections[0].elapsed_seconds = 0; }],
    ["raw context", analysis => { analysis.stored_context_snapshot.sleep.metrics.device_id = "synthetic-private-extra"; }],
  ];
  for (const [label, change] of corruptions) {
    it(`Training Effect compatibility matrix: rejects ${label} without repair or payload leaks via ${protocol}`, async () => {
      if (label === "unknown becomes zero") load(matrix.cases.unknown_totals);
      await editCoach(change);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call(protocol);
      expect(output.available).toBe(false);
      expect(JSON.stringify(output)).not.toContain("synthetic-private-extra");
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations).toHaveLength(4);
      expect(operations.some(value => /^(LIST|PUT|DELETE)/.test(value))).toBe(false);
    });
  }
  it(`Training Effect compatibility matrix: genuine structured1.3 stays unchanged via ${protocol}`, async () => {
    load(legacy130);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call(protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toMatchObject({ analysis_id: legacy130.analysis_id, analyzer_version: "1.3.0",
      sections: [{ heart_rate_coverage: { covered_seconds: 40 } }] });
    expect(output.analysis).not.toHaveProperty("training_effect");
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toHaveLength(4);
  });
  it(`Training Effect HR coverage refinement: complete with zero contributors is unverified via ${protocol}`, async () => {
    await editCoach(analysis => {
      const heart = (analysis.sections as Array<{ heart_rate_coverage: Record<string, unknown> }>)[0].heart_rate_coverage;
      heart.covered_seconds = 0; heart.contributing_laps = 0;
    });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call(protocol);
    expect(output.available).toBe(false);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
  });
}
