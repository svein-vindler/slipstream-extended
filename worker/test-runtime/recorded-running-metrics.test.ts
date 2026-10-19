import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { outputSchemas } from "../src/mcp-output";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import produced from "./generated/recorded-running-metrics.json";
import old101 from "../../tests/fixtures/coach_engine_101_transition.json";

const issuer = "https://recorded-metrics-synthetic.cloudflareaccess.com";
const hostname = "recorded-metrics.example";
const fitKey = "activities/2026/42/activity.v1.json";
const enduranceKey = "activities/2026/42/activity.endurance.v1.json";
const rows = new Map<string, string | Uint8Array>();
const operations: string[] = [];
let key: CryptoKey, jwk: Record<string, unknown>, token: string;
let testEnv: Env;
function decoded() {
  return { schema_version: 1, source: "garmin-fit", decode_errors: [], source_fit_sha256: "a".repeat(64),
    activity: { id: "42", type: "running", start_time_gmt: "2026-10-08 12:00:00", start_time_local: "2026-10-08 14:00:00" },
    messages: { session_mesgs: [{ start_time: "2026-10-08T12:00:00Z", timestamp: "2026-10-08T12:10:00Z", sport: "running",
      avg_power: 250, avg_step_length: 1050.5, avg_stance_time: 240.5, avg_vertical_oscillation: 80.5, avg_vertical_ratio: 7.25,
      total_training_effect: 3.4, total_anaerobic_training_effect: 0 }] } };
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "recorded-metrics", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); rows.clear(); operations.length = 0;
  rows.set("summary/activities.csv", "Activity ID,Activity Date,Activity Local Date,Activity Name,Activity Type,Source\ngarmin-42,2026-10-08 12:00:00,2026-10-08,Synthetic run,Run,garmin");
  rows.set(fitKey, JSON.stringify(decoded()));
  // The first TCX point is later than the FIT start, and is not a session identity clock.
  rows.set(enduranceKey, JSON.stringify({ schema_version: 1, source: "garmin-tcx", activity: { id: "42", start_time: "2026-10-08T12:00:02Z" }, summary: { distance_m: 2000 } }));
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected metrics fixture network request");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "recorded-metrics" }).setIssuer(issuer).setAudience("recorded-metrics")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const bucket = {
    head: async (name: string) => { operations.push(`HEAD ${name}`); return rows.has(name) ? { etag: "synthetic-recorded" } : null; },
    get: async (name: string) => {
      operations.push(`GET ${name}`);
      const body = rows.get(name);
      return body === undefined ? null : { size: typeof body === "string" ? new TextEncoder().encode(body).byteLength : body.byteLength,
        etag: "synthetic-recorded", body: new Response(body).body };
    },
    list: async () => { operations.push("LIST"); throw new Error("Unexpected legacy discovery"); },
    put: async () => { operations.push("PUT"); throw new Error("Unexpected fixture write"); },
    delete: async () => { operations.push("DELETE"); throw new Error("Unexpected fixture delete"); },
  } as unknown as R2Bucket;
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "recorded-metrics", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });

async function call(name = "endurance_session", protocol = "2025-03-26", activityId = "garmin-42") {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: { activity_id: activityId },
      ...(protocol === "2026-07-28" ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  const media = response.headers.get("content-type");
  expect(media).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined(); expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

it("serves five recorded running measurements without rescaling or another FIT read", async () => {
  const output = await call();
  expect(output.available).toBe(true);
  expect(output.recorded_running_metrics).toEqual({ schema_version: 1, source: "garmin_fit_session", method: "fit_recorded_measurement",
    activity_id: "42", association: "matched", local_start_time: "2026-10-08T14:00:00", local_date: "2026-10-08",
    session_start_time_utc: "2026-10-08T12:00:00.000Z", measured_at_utc: "2026-10-08T12:10:00.000Z", source_fit_sha256: "a".repeat(64),
    average_power: { status: "available", value: 250, unit: "W", source_field: "avg_power" },
    average_step_length: { status: "available", value: 1050.5, unit: "mm", source_field: "avg_step_length" },
    average_ground_contact_time: { status: "available", value: 240.5, unit: "ms", source_field: "avg_stance_time" },
    average_vertical_oscillation: { status: "available", value: 80.5, unit: "mm", source_field: "avg_vertical_oscillation" },
    average_vertical_ratio: { status: "available", value: 7.25, unit: "%", source_field: "avg_vertical_ratio" } });
  expect(outputSchemas.endurance_session.parse(output)).toEqual(output);
  expect(output.training_context.aerobic.value).toBe(3.4);
  expect(output.session.activity.start_time).toBe("2026-10-08T12:00:02Z");
  expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv", `GET ${fitKey}`, `GET ${enduranceKey}`]);
  expect(JSON.stringify(output.recorded_running_metrics)).not.toMatch(/messages|position|GPS/);
});

const protocols = ["2025-03-26", "2026-07-28"];
const fieldNames = ["average_power", "average_step_length", "average_ground_contact_time", "average_vertical_oscillation", "average_vertical_ratio"];
const sourceFields = ["avg_power", "avg_step_length", "avg_stance_time", "avg_vertical_oscillation", "avg_vertical_ratio"];
for (const protocol of protocols) {
  it(`recorded metrics expanded: distinguishes zero, missing and invalid without coercion via ${protocol}`, async () => {
    const payload = decoded();
    Object.assign(payload.messages.session_mesgs[0], { avg_power: 0, avg_step_length: null, avg_stance_time: "240", avg_vertical_oscillation: false, avg_vertical_ratio: 0 });
    rows.set(fitKey, JSON.stringify(payload));
    const output = await call("endurance_session", protocol);
    expect(outputSchemas.endurance_session.parse(output)).toEqual(output);
    expect(output.recorded_running_metrics).toMatchObject({ association: "matched",
      average_power: { status: "available", value: 0, unit: "W" },
      average_step_length: { status: "missing", value: null, unit: "mm" },
      average_ground_contact_time: { status: "invalid", value: null, unit: "ms" },
      average_vertical_oscillation: { status: "invalid", value: null, unit: "mm" },
      average_vertical_ratio: { status: "available", value: 0, unit: "%" } });
    expect(output.available).toBe(true);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv", `GET ${fitKey}`, `GET ${enduranceKey}`]);
  });
  it(`recorded metrics expanded: absent measurements remain missing with unknown sensor support via ${protocol}`, async () => {
    const payload = decoded();
    for (const field of sourceFields) delete (payload.messages.session_mesgs[0] as Record<string, unknown>)[field];
    rows.set(fitKey, JSON.stringify(payload));
    const output = await call("endurance_session", protocol);
    expect(output.recorded_running_metrics.association).toBe("matched");
    for (const field of fieldNames) expect(output.recorded_running_metrics[field]).toMatchObject({ status: "missing", value: null });
    expect(output.available).toBe(true);
    expect(output.training_context.aerobic.value).toBe(3.4);
    expect(JSON.stringify(output.recorded_running_metrics)).not.toContain("support");
  });
  for (const failure of ["source_missing", "source_unreadable", "nonfinite_json"] as const) {
    it(`recorded metrics expanded: ${failure} preserves existing TCX dataset via ${protocol}`, async () => {
      if (failure === "source_missing") rows.delete(fitKey);
      else rows.set(fitKey, failure === "nonfinite_json" ? JSON.stringify(decoded()).replace('"avg_power":250', '"avg_power":NaN') : "{invalid-json");
      const output = await call("endurance_session", protocol);
      expect(output.available).toBe(true);
      expect(output.session.summary.distance_m).toBe(2000);
      expect(output.recorded_running_metrics.association).toBe(failure === "source_missing" ? failure : "source_unreadable");
      for (const field of fieldNames) expect(output.recorded_running_metrics[field]).toMatchObject({ status: "unavailable", value: null });
      expect(operations.filter(operation => operation.startsWith("GET activities/"))).toHaveLength(failure === "source_missing" ? 3 : 2);
      expect(operations).not.toContain("PUT"); expect(operations).not.toContain("LIST");
    });
  }
  const failures: Array<[string, string, (payload: ReturnType<typeof decoded>) => void]> = [
    ["new_schema", "invalid_schema", payload => { payload.schema_version = 2; }],
    ["boolean_schema", "invalid_schema", payload => { Object.assign(payload, { schema_version: true }); }],
    ["wrong_source", "invalid_schema", payload => { payload.source = "other"; }],
    ["decode_errors", "decode_errors", payload => { Object.assign(payload, { decode_errors: ["synthetic"] }); }],
    ["wrong_decoded_id", "id_mismatch", payload => { payload.activity.id = "43"; }],
    ["wrong_indexed_utc", "time_mismatch", payload => { payload.activity.start_time_gmt = "2026-10-08 12:00:01"; }],
    ["wrong_session_utc", "time_mismatch", payload => { payload.messages.session_mesgs[0].start_time = "2026-10-08T12:00:01Z"; }],
    ["naive_session_utc", "time_mismatch", payload => { payload.messages.session_mesgs[0].start_time = "2026-10-08 12:00:00"; }],
    ["wrong_local_date", "time_mismatch", payload => { payload.activity.start_time_local = "2026-10-09 14:00:00"; }],
    ["malformed_local_clock", "time_mismatch", payload => { payload.activity.start_time_local = "2026-10-08 25:00:00"; }],
    ["sessions_missing", "sessions_missing", payload => { payload.messages.session_mesgs = []; }],
    ["sessions_ambiguous", "sessions_ambiguous", payload => { payload.messages.session_mesgs.push({ ...payload.messages.session_mesgs[0] }); }],
    ["malformed_sessions", "invalid_schema", payload => { Object.assign(payload.messages, { session_mesgs: {} }); }],
    ["decoded_cycling", "unsupported_sport", payload => { payload.activity.type = "cycling"; }],
    ["session_cycling", "unsupported_sport", payload => { payload.messages.session_mesgs[0].sport = "cycling"; }],
    ["invalid_session_sport", "unsupported_sport", payload => { Object.assign(payload.messages.session_mesgs[0], { sport: false }); }],
  ];
  for (const [label, association, change] of failures) {
    it(`recorded metrics expanded: ${label} leaves only the optional measurements unavailable via ${protocol}`, async () => {
      const payload = decoded(); change(payload); rows.set(fitKey, JSON.stringify(payload));
      const output = await call("endurance_session", protocol);
      expect(output.available).toBe(true); expect(output.session.summary.distance_m).toBe(2000);
      expect(output.recorded_running_metrics.association).toBe(association);
      for (const field of fieldNames) expect(output.recorded_running_metrics[field]).toMatchObject({ status: "unavailable", value: null });
      expect(outputSchemas.endurance_session.parse(output)).toEqual(output);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv", `GET ${fitKey}`, `GET ${enduranceKey}`]);
    });
  }
  it(`recorded metrics expanded: contradictory TCX identity does not misattribute FIT measurements via ${protocol}`, async () => {
    rows.set(enduranceKey, JSON.stringify({ schema_version: 1, activity: { id: "garmin-43" }, summary: { distance_m: 2000 } }));
    const output = await call("endurance_session", protocol);
    expect(output.available).toBe(true); expect(output.session.activity.id).toBe("garmin-43");
    expect(output.recorded_running_metrics.association).toBe("id_mismatch");
    expect(output.recorded_running_metrics.average_power.value).toBeNull();
  });
  for (const sport of ["missing_caller", "missing_session", "missing_everywhere", "contradictory_caller"] as const) {
    it(`recorded metrics expanded: ${sport} requires actual running evidence via ${protocol}`, async () => {
      const payload = decoded();
      if (sport === "missing_session") Object.assign(payload.messages.session_mesgs[0], { sport: "   " });
      else {
        rows.set("summary/activities.csv", "Activity ID,Activity Date,Activity Local Date,Activity Name,Activity Type,Source\ngarmin-42,2026-10-08 12:00:00,2026-10-08,Synthetic run," + (sport === "contradictory_caller" ? "Cycling" : "") + ",garmin");
        if (sport === "missing_everywhere") payload.activity.type = "";
      }
      rows.set(fitKey, JSON.stringify(payload));
      const output = await call("endurance_session", protocol);
      const association = sport === "missing_everywhere" || sport === "contradictory_caller" ? "unsupported_sport" : "matched";
      expect(output.recorded_running_metrics.association).toBe(association);
      expect(output.recorded_running_metrics.average_power.value).toBe(association === "matched" ? 250 : null);
    });
  }
  it(`recorded metrics expanded: missing local and end clocks stay unknown without inventing UTC calendar via ${protocol}`, async () => {
    const payload = decoded();
    Object.assign(payload.activity, { start_time_local: null });
    payload.messages.session_mesgs[0].timestamp = "2026-10-08T11:59:59Z";
    payload.source_fit_sha256 = "not-a-digest";
    rows.set(fitKey, JSON.stringify(payload));
    const output = await call("endurance_session", protocol);
    expect(output.recorded_running_metrics).toMatchObject({ association: "matched", local_start_time: null, local_date: null,
      session_start_time_utc: "2026-10-08T12:00:00.000Z", measured_at_utc: null, source_fit_sha256: null });
    expect(output.recorded_running_metrics.average_power.value).toBe(250);
  });
  it(`recorded metrics expanded: equivalent explicit UTC clocks retain normalized millisecond provenance via ${protocol}`, async () => {
    const payload = decoded();
    payload.activity.start_time_gmt = "2026-10-08T12:00:00.100+00:00";
    payload.activity.start_time_local = "2026-10-08 14:00:00.1";
    payload.messages.session_mesgs[0].start_time = "2026-10-08T12:00:00.1Z";
    rows.set(fitKey, JSON.stringify(payload));
    rows.set("summary/activities.csv", "Activity ID,Activity Date,Activity Local Date,Activity Name,Activity Type,Source\ngarmin-42,2026-10-08 12:00:00.100,2026-10-08,Synthetic run,Run,garmin");
    const output = await call("endurance_session", protocol);
    expect(output.recorded_running_metrics).toMatchObject({ association: "matched", local_start_time: "2026-10-08T14:00:00.100",
      session_start_time_utc: "2026-10-08T12:00:00.100Z", average_power: { value: 250 } });
  });
  it(`recorded metrics expanded: retained genuine 1.1.0 FIT and immutable Coach analysis agree via ${protocol}`, async () => {
    rows.clear();
    for (const [name, body] of Object.entries(produced.objects_base64)) rows.set(name, Uint8Array.from(atob(body), character => character.charCodeAt(0)));
    const before = [...rows].map(([name, body]) => [name, body instanceof Uint8Array ? Array.from(body) : body]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true); expect(coach.available).toBe(true);
    expect(coach.analysis.analyzer_version).toBe("1.1.0");
    // FIT SDK 21.218 writes its profile version into this newly generated FIT source.
    expect(coach.analysis.analysis_id).toBe("deb436b814332360e4773d6d");
    expect(detail.recorded_running_metrics).toEqual(coach.analysis.recorded_running_metrics);
    expect(detail.recorded_running_metrics).toMatchObject({ activity_id: "42", association: "matched", local_date: "2026-10-08",
      session_start_time_utc: "2026-10-07T23:30:00.000Z", measured_at_utc: "2026-10-08T00:00:00.000Z",
      average_power: { status: "available", value: 250, unit: "W" },
      average_step_length: { status: "available", value: 1234.5, unit: "mm" },
      average_ground_contact_time: { status: "available", value: 241.2, unit: "ms" },
      average_vertical_oscillation: { status: "available", value: 86.4, unit: "mm" },
      average_vertical_ratio: { status: "available", value: 6.78, unit: "%" } });
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, body instanceof Uint8Array ? Array.from(body) : body])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv", `GET ${fitKey}`, `GET ${enduranceKey}`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/deb436b814332360e4773d6d.json"]);
  });
  it(`recorded metrics expanded: genuine 1.0.1 Coach selection is read unchanged without adding a block via ${protocol}`, async () => {
    rows.clear();
    for (const [name, body] of Object.entries(old101.objects_base64)) rows.set(name, Uint8Array.from(atob(body), character => character.charCodeAt(0)));
    // The original immutable analysis/pointer bytes remain genuine. A literal caller index supplies the Worker's existing CSV clock format.
    rows.set("summary/activities.csv", "Activity ID,Activity Date,Activity Local Date,Activity Name,Activity Type,Source\ngarmin-1,2026-01-05 10:00:00,2026-01-05,Synthetic legacy run,Run,garmin");
    const before = [...rows].map(([name, body]) => [name, body instanceof Uint8Array ? Array.from(body) : body]);
    const output = await call("coach_input", protocol, "garmin-1");
    expect(output.available).toBe(true);
    expect(output.analysis).toMatchObject({ activity_id: "1", analyzer_version: "1.0.1", analysis_id: "fb275eaa68dd80b8be93990f" });
    expect(output.analysis).not.toHaveProperty("recorded_running_metrics");
    expect([...rows].map(([name, body]) => [name, body instanceof Uint8Array ? Array.from(body) : body])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/1/coach-input/v1/latest-ready.json", "GET activities/2026/1/coach-input/v1/canonical/fb275eaa68dd80b8be93990f.json"]);
  });
  it(`recorded metrics expanded: invalid current pointer never falls back to another analysis via ${protocol}`, async () => {
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", JSON.stringify({ schema_version: 1, selection_version: 1,
      analysis_key: "activities/2026/43/coach-input/v1/canonical/aaaaaaaaaaaaaaaaaaaaaaaa.json" }));
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(false); expect(output).not.toHaveProperty("analysis");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json"]);
  });
}
