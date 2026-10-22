import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import produced from "./generated/coach-stored-context.json";
import legacy100 from "../../tests/fixtures/coach_legacy_transition.json";
import legacy101 from "../../tests/fixtures/coach_engine_101_transition.json";
import legacy110 from "../../tests/fixtures/coach_engine_110_transition.json";
import legacy120 from "../../tests/fixtures/coach_engine_120_transition.json";

const issuer = "https://stored-context-synthetic.cloudflareaccess.com";
const hostname = "stored-context.example";
const rows = new Map<string, Uint8Array>();
const operations: string[] = [];
let key: CryptoKey, jwk: Record<string, unknown>, token: string;
let testEnv: Env;
function load(objects: Record<string, string>) {
  rows.clear();
  for (const [name, body] of Object.entries(objects)) rows.set(name,
    Uint8Array.from(atob(body), character => character.charCodeAt(0)));
}
async function editCoach(change: (analysis: Record<string, unknown>) => void) {
  const name = `activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`;
  const bytes = rows.get(name);
  if (!bytes) throw new Error("Missing genuine stored-context canonical fixture");
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
  jwk = { ...await exportJWK(pair.publicKey), kid: "stored-context", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); operations.length = 0; load(produced.objects_base64);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected context fixture network request");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "stored-context" }).setIssuer(issuer).setAudience("stored-context")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const bucket = {
    head: async (name: string) => { operations.push(`HEAD ${name}`); return rows.has(name) ? { etag: "synthetic-context" } : null; },
    get: async (name: string) => {
      operations.push(`GET ${name}`); const body = rows.get(name);
      return body === undefined ? null : { size: body.byteLength, etag: "synthetic-context", body: new Response(body).body };
    },
    list: async () => { operations.push("LIST"); throw new Error("Unexpected context discovery"); },
    put: async () => { operations.push("PUT"); throw new Error("Unexpected context write"); },
    delete: async () => { operations.push("DELETE"); throw new Error("Unexpected context deletion"); },
  } as unknown as R2Bucket;
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "stored-context", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });
async function call(protocol: string, activityId = "garmin-42", extraArguments: Record<string, unknown> = {}) {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": "coach_input" } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "coach_input",
      arguments: { activity_id: activityId, ...extraArguments }, ...(protocol === "2026-07-28" ? { _meta: {
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
function expectBoundedReads(identifier = produced.analysis_id, activityId = "42") {
  expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
    `GET activities/2026/${activityId}/coach-input/v1/latest-ready.json`,
    `GET activities/2026/${activityId}/coach-input/v1/canonical/${identifier}.json`]);
}
const contextCases = ["sparse", "missing_hrv", "after", "time_unknown", "ambiguous", "morning", "retained", "retained_unknown",
  "source_error", "garmin_residual", "partial_local", "zero", "invalid_measurement"] as const;
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const mode of ["zero", "retained", "after"] as const) {
    it(`Coach presentation frozen context: ${mode} keeps temporal status and observed values via ${protocol}`, async () => {
      const fixture = produced.cases[mode];
      load(fixture.objects_base64);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call(protocol, "garmin-42", { format: "coach" });
      expect(output.available).toBe(true);
      expect(output).not.toHaveProperty("analysis");
      const text = output.presentation.text;
      const line = text.split("\n").find((value: string) => value.startsWith("Stored context: "));
      expect(line).toBeDefined();
      const snapshot = JSON.parse(line!.slice("Stored context: ".length));
      expect(snapshot).toMatchObject({ reference_time_utc: "2026-10-19T07:00:00.000Z", semantics: "retrospective_pre_activity" });
      if (mode === "zero") {
        expect(snapshot.sleep.metrics.sleep_score).toMatchObject({ value: 0, status: "available" });
        expect(snapshot.hrv.metrics.last_night_avg_ms).toMatchObject({ value: 0, status: "available" });
        expect(snapshot.training_day.recovery).toMatchObject({ value: 0, status: "available" });
      } else if (mode === "retained") {
        expect(snapshot.training_day).toMatchObject({ readiness: { retained_previous: true, source_checked_at: "2026-10-19T08:00:00Z" },
          last_attempt: { status: "auth_required", attempted_at: "2026-10-19T09:00:00Z" } });
      } else {
        expect(snapshot.sleep.status).toBe("after_activity");
        expect(snapshot.training_day).toMatchObject({ temporal_status: "after_activity", readiness: { value: null, status: "unavailable" } });
      }
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expectBoundedReads(fixture.analysis_id);
    });
  }
  for (const mode of contextCases) {
    it(`stored context producer matrix: ${mode} preserves the genuine workout and source semantics via ${protocol}`, async () => {
      const fixture = produced.cases[mode];
      load(fixture.objects_base64);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call(protocol);
      expect(output.available).toBe(true);
      expect(output.analysis).toMatchObject({ analysis_id: fixture.analysis_id, analyzer_version: "1.8.0",
        summary: { distance_m: 2000 }, recorded_running_metrics: { schema_version: 2, association: "matched" } });
      const context = output.analysis.stored_context_snapshot;
      expect(context).toMatchObject({ reference_time_utc: "2026-10-19T07:00:00.000Z", semantics: "retrospective_pre_activity" });
      const training = context.training_day;
      if (mode === "sparse") {
        expect(context.snapshot_at).toBeNull();
        for (const kind of ["sleep", "hrv", "training_day"]) expect(context[kind].status).toBe("not_collected");
        expect(training.readiness).toMatchObject({ status: "unavailable", value: null, retained_previous: false });
        expect(JSON.stringify(context)).not.toContain("unsupported");
      } else if (mode === "missing_hrv") {
        expect(context.hrv.status).toBe("not_collected");
        expect(context.sleep.metrics.sleep_seconds.value).toBe(25200);
        expect(training.readiness.value).toBe(80);
      } else if (mode === "after") {
        expect(training).toMatchObject({ temporal_status: "after_activity", readiness: { status: "unavailable", value: null },
          recovery: { status: "unavailable", value: null } });
        expect(context.sleep).toMatchObject({ status: "after_activity", metrics: { sleep_seconds: { status: "unavailable", value: null } } });
        expect(context.hrv.status).toBe("after_activity");
      } else if (mode === "time_unknown" || mode === "retained_unknown") {
        expect(training).toMatchObject({ temporal_status: "time_unknown", readiness: { status: "unavailable", value: null, observation_at: null } });
        if (mode === "retained_unknown") expect(training.readiness.retained_previous).toBe(true);
        else for (const kind of ["sleep", "hrv"]) expect(context[kind].status).toBe("time_unknown");
      } else if (mode === "ambiguous") {
        expect(training).toMatchObject({ selection_status: "ambiguous", selected_slot: null,
          temporal_status: "selection_unavailable", readiness: { status: "unavailable", value: null },
          recovery: { status: "unavailable", value: null } });
      } else if (mode === "morning") {
        expect(training).toMatchObject({ selected_slot: "morning", temporal_status: "before_activity", readiness: { value: 80 }, recovery: { value: 0 } });
      } else if (mode === "retained") {
        expect(training).toMatchObject({ last_attempt: { status: "auth_required", attempted_at: "2026-10-19T09:00:00Z" },
          readiness: { value: 80, retained_previous: true, source_checked_at: "2026-10-19T08:00:00Z" } });
        // A failed later attempt is not a newer source check or a countdown.
        expect(context.snapshot_at).toBe("2026-10-19T08:00:00.000Z");
        expect(training.recovery.value).toBe(0);
      } else if (mode === "source_error") {
        expect(training).toMatchObject({ last_attempt: { status: "source_error" }, temporal_status: "selection_unavailable",
          readiness: { status: "unavailable", value: null } });
        for (const kind of ["sleep", "hrv"]) expect(context[kind].status).toBe("invalid_schema");
      } else if (mode === "garmin_residual") {
        for (const kind of ["sleep", "hrv"]) expect(context[kind]).toMatchObject({ status: "available",
          local_time_source: "garmin_local", observation_start_utc: "2026-10-18T22:00:20.000Z",
          sleep_start_local: "2026-10-19T00:00:00+02:00" });
      } else if (mode === "partial_local") {
        for (const kind of ["sleep", "hrv"]) expect(context[kind]).toMatchObject({ status: "local_time_unknown",
          local_time_source: "unavailable", sleep_start_local: "2026-10-19T00:00:00+02:00", sleep_end_local: null });
      } else if (mode === "zero") {
        expect(training).toMatchObject({ readiness: { value: 1, status: "available" }, recovery: { value: 0, status: "available" } });
        expect(context.sleep.metrics.sleep_score).toMatchObject({ value: 0, status: "available" });
        expect(context.hrv.metrics.last_night_avg_ms).toMatchObject({ value: 0, status: "available" });
      } else if (mode === "invalid_measurement") {
        expect(training.readiness).toMatchObject({ value: null, status: "invalid" });
        expect(training.recovery).toMatchObject({ value: 0, status: "available" });
        expect(context.sleep.metrics.sleep_score).toMatchObject({ value: null, status: "invalid" });
        expect(context.sleep.metrics.sleep_seconds.value).toBe(25200);
      }
      expect(JSON.stringify(context)).not.toMatch(/9000004[23]|synthetic-private-raw|providerOnly|hrvReadings|sleepLevels|deviceId|position_lat/);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expectBoundedReads(fixture.analysis_id);
    });
  }
  const corruptions: Array<[string, (analysis: Record<string, unknown>) => void]> = [
    ["snapshot older than checks", analysis => { (analysis.stored_context_snapshot as Record<string, unknown>).snapshot_at = "2026-10-19T07:00:00Z"; }],
    ["available health after workout", analysis => {
      const health = (analysis.stored_context_snapshot as { sleep: Record<string, unknown> }).sleep;
      health.observation_end_utc = "2026-10-19T08:00:00.000Z"; health.sleep_end_local = "2026-10-19T10:00:00+02:00";
    }],
    ["sleep score outside range", analysis => {
      const metric = (analysis.stored_context_snapshot as { sleep: { metrics: { sleep_score: Record<string, unknown> } } }).sleep.metrics.sleep_score;
      metric.value = 101;
    }],
    ["negative recovery", analysis => { (analysis.stored_context_snapshot as { training_day: { recovery: Record<string, unknown> } }).training_day.recovery.value = -1; }],
    ["nonUTC snapshot clock", analysis => { (analysis.stored_context_snapshot as Record<string, unknown>).snapshot_at = "2026-10-19T08:00:00+02:00"; }],
    ["nested raw GPS", analysis => { (analysis.stored_context_snapshot as { hrv: { metrics: Record<string, unknown> } }).hrv.metrics.position_lat = 12; }],
    ["unbound FIT digest", analysis => { (analysis.source as Record<string, unknown>).fit_sha256 = "f".repeat(64); }],
    ["unbound context reference", analysis => { (analysis.stored_context_snapshot as Record<string, unknown>).reference_time_utc = "2026-10-19T07:00:01.000Z"; }],
  ];
  for (const [label, change] of corruptions) {
    it(`stored context corrupt matrix: ${label} is unverified rather than partially exposed via ${protocol}`, async () => {
      await editCoach(change);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      expect((await call(protocol)).available).toBe(false);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expectBoundedReads();
    });
  }
  for (const [version, fixture, activityId, identifier] of [
    ["1.0.0", legacy100, "1", "759db33b577538bdad02468e"],
    ["1.0.1", legacy101, "1", "fb275eaa68dd80b8be93990f"],
    ["1.1.0", legacy110, "42", "deb436b814332360e4773d6d"],
    ["1.2.0", legacy120, "42", "711438a421bc2d2f3d889d96"],
  ] as const) {
    it(`stored context historical reader: genuine ${version} bytes and canonical selection remain unchanged via ${protocol}`, async () => {
      load(fixture.objects_base64);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call(protocol, `garmin-${activityId}`);
      expect(output.available).toBe(true);
      expect(output.analysis).toMatchObject({ analysis_id: identifier, analyzer_version: version });
      expect(output.analysis).not.toHaveProperty("stored_context_snapshot");
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expectBoundedReads(identifier, activityId);
    });
  }
  it(`stored context future-observation integrity: available training values cannot follow the workout via ${protocol}`, async () => {
    await editCoach(analysis => {
      const snapshot = analysis.stored_context_snapshot as { training_day: { readiness: Record<string, unknown> } };
      snapshot.training_day.readiness.observation_at = "2026-10-19T08:00:00+00:00";
    });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const rejected = await call(protocol);
    expect(rejected.available).toBe(false);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expectBoundedReads();
  });
  it(`stored context raw-field integrity: genuine snapshot remains readable but nested raw device data fails closed via ${protocol}`, async () => {
    const original = await call(protocol);
    expect(original.available).toBe(true);
    expect(original.analysis.analyzer_version).toBe("1.8.0");
    expect(original.analysis.stored_context_snapshot).toMatchObject({ schema_version: 1,
      reference_time_utc: "2026-10-19T07:00:00.000Z", semantics: "retrospective_pre_activity",
      sleep: { status: "available", metrics: { sleep_seconds: { value: 25200 } } },
      hrv: { status: "available", metrics: { last_night_avg_ms: { value: 50 } } },
      training_day: { temporal_status: "before_activity", readiness: { value: 80 }, recovery: { value: 0 } } });
    expect(original.analysis.summary.distance_m).toBe(2000);
    expectBoundedReads();
    operations.length = 0;
    await editCoach(analysis => {
      const snapshot = analysis.stored_context_snapshot as { training_day: { last_attempt: Record<string, unknown> } };
      snapshot.training_day.last_attempt.deviceId = "synthetic-private-raw";
    });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const rejected = await call(protocol);
    expect(rejected.available).toBe(false);
    expect(JSON.stringify(rejected)).not.toContain("synthetic-private-raw");
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
  });
}
