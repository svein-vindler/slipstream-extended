/** Authenticated D1/X0 projection and D4 scalar-leaf contracts. */
import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { beforeAll, beforeEach, afterEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";
import { clearSummaryCaches } from "../src/r2-storage";
import strengthLabelJson from "./generated/strength-label-contract.json?raw";
import scalarLeafJson from "./generated/scalar-leaf-contract.json?raw";
const strengthLabels = JSON.parse(strengthLabelJson);
const scalarLeaves = JSON.parse(scalarLeafJson);

const DAY = "2026-09-30", OTHER_DAY = "2026-10-01";
const HRV = "health/hrv/2026/09/2026-09-30.json";
const OTHER_HRV = "health/hrv/2026/10/2026-10-01.json";
const SLEEP = "health/sleep/v1/2026/09/2026-09-30.json";
const FIT = "activities/2026/123/activity.v1.json";
const ENDURANCE = "activities/2026/123/activity.endurance.v1.json";
const ACTIVITIES = "summary/activities.csv", HEALTH = "summary/health_daily.csv";
const MARKER = "D1_SYNTHETIC_PRIVATE_MARKER";
const FAILURE = "The request could not be completed. Check arguments, access, configuration and service availability.";
const ISSUER = "https://d1-synthetic.cloudflareaccess.com", HOST = "d1-contract.example";
type Row = Record<string, unknown>;

function hrv(date: unknown = DAY) {
  return { schema_version: 1, source: "garmin", date,
    sleep_start_gmt: "2026-09-29T21:30:00Z" as unknown,
    sleep_end_gmt: "2026-09-30T04:30:00Z" as unknown,
    sleep_start_garmin_local: null as unknown, sleep_end_garmin_local: null as unknown,
    summary: { calendarDate: DAY, lastNightAvg: 0, weeklyAvg: "50", status: "BALANCED" } as Row,
    readings: [{ timestamp: "2026-09-30T00:00:00Z", hrv_ms: 0 },
      { timestamp: null, hrv_ms: null }, { timestamp: 1790726400000, hrv_ms: "50" },
      { timestamp: "1790726400", hrv_ms: 50 }] as Array<{ timestamp: unknown; hrv_ms: unknown }> };
}
function sleep() {
  return { schema_version: 1, source: "garmin", date: DAY,
    sleep_start_gmt: "2026-09-29T21:30:00Z", sleep_end_gmt: "2026-09-30T04:30:00Z", confirmed: true,
    summary: { sleep_seconds: 25200, deep_sleep_seconds: 4200, light_sleep_seconds: 15000,
      rem_sleep_seconds: 6000, awake_sleep_seconds: 0, unmeasurable_sleep_seconds: null,
      nap_seconds: null, sleep_score: 82, average_spo2_percent: null, lowest_spo2_percent: null,
      average_respiration_brpm: null, lowest_respiration_brpm: null, highest_respiration_brpm: null,
      average_sleep_stress: null },
    score_breakdown: { overall: { value: 82, qualifier: "GOOD" },
      duration: { value: 90, qualifier: "EXCELLENT" },
      AD_SYNTHETIC_DYNAMIC_SCORE: { value: 0, qualifier: null } } as Record<string, Row>,
    stages: [{ start_gmt: "2026-09-29T21:30:00Z", end_gmt: "2026-09-30T04:30:00Z", stage: "light" }] };
}
function fit() {
  return { schema_version: 1, source: "garmin-fit", decode_errors: [], source_fit_sha256: "a".repeat(64),
    activity: { id: "123", start_time_gmt: "2026-09-30T10:00:00Z", start_time_local: "2026-09-30T12:00:00" },
    messages: { session_mesgs: [{ start_time: "2026-09-30T10:00:00Z", timestamp: "2026-09-30T10:30:00Z",
      total_training_effect: 1.5, total_anaerobic_training_effect: 0 }] },
    normalized_strength_session: { schema_version: 1, activity_id: "123",
      sets: [{ set_number: 1, message_index: 0, exercise: { category: "SQUAT", name: "Synthetic squat", probability: 95 } as Row,
        repetitions: 12, weight_kg: 60, active_duration_seconds: 66.317, rest_after_seconds: 69.024,
        start_time: "2026-09-30T12:00:00", workout_step_index: null,
        performance_metrics: { avgConcentricMeanVelocity: 0.42, tal_grit: null,
          AD_SYNTHETIC_NUMBER: 0, AD_SYNTHETIC_TEXT: "scalar", AD_SYNTHETIC_BOOLEAN: false,
          AD_SYNTHETIC_NULL: null } } as Row],
      summary: { active_sets: 1, total_repetitions: 12, external_volume_kg: 720,
        active_duration_seconds: 66.317, rest_duration_seconds: 69.024,
        by_exercise: { "Synthetic squat": { sets: 1, repetitions: 12, external_volume_kg: 720 } },
        by_category: { SQUAT: { sets: 1, repetitions: 12, external_volume_kg: 720 } } } } };
}
function endurance() {
  const range = { distance_m: 500, duration_seconds: 150, pace_seconds_per_km: 300,
    average_speed_mps: "3.3333", average_heart_rate_bpm: 150,
    maximum_heart_rate_bpm: null, elevation_change_m: 0 };
  return { schema_version: 1, source: "garmin-tcx", privacy: "GPS coordinates and route geometry removed",
    available: true, activity: { id: "123", name: "Synthetic run", type: "running", sport: "Running",
      start_time: "2026-09-30T10:00:00Z" } as Row,
    summary: { duration_seconds: 300, distance_m: 1000, average_speed_mps: "3.3333",
      average_pace_seconds_per_km: 300, average_heart_rate_bpm: 150, maximum_heart_rate_bpm: null,
      minimum_altitude_m: 0, maximum_altitude_m: 0, aerobic_decoupling_percent: null,
      trackpoint_count: 2, sampled_trackpoint_count: 2, observed_heart_rate_seconds: 10 } as Row,
    laps: [{ lap: 1, start_time: "2026-09-30T10:00:00Z", duration_seconds: 300, distance_m: 1000,
      maximum_speed_mps: null, calories: 0, average_heart_rate_bpm: 150, maximum_heart_rate_bpm: null,
      intensity: "Active", trigger_method: "Manual" } as Row],
    kilometer_splits: [{ ...range, distance_m: 1000, duration_seconds: 300, split: 1, partial: false } as Row],
    distance_halves: { first: { ...range } as Row, second: { ...range } as Row } as Row,
    heart_rate_seconds_by_bpm: { "150": 10, "151": 0 } as Row, sample_interval_seconds: 10,
    sampled_trackpoints: [{ timestamp: "2026-09-30T10:00:00Z", elapsed_seconds: 0, distance_m: 0,
      altitude_m: 0, heart_rate_bpm: 150, cadence_rpm: null, speed_mps: "3.3333" } as Row,
    { timestamp: "2026-09-30T10:05:00Z", elapsed_seconds: 300, distance_m: 1000,
      altitude_m: null, heart_rate_bpm: null, cadence_rpm: 0, speed_mps: 0 } as Row] };
}
const activityCsv = "Activity ID,Activity Date,Activity Name,Activity Type,Source,Distance,Moving Time,Average Heart Rate\n"
  + "garmin-123,2026-09-30 10:00:00,Synthetic strength,strength_training,garmin,0,1800,150\n";
const healthCsv = "Date,Source,Sleep Seconds\n2026-09-30,garmin,25200\n";
let privateKey: CryptoKey, jwk: Row, token: string, testEnv: Env, rpcId = 0;
let failedGet: string | undefined;
let gate: { keys: Set<string>; arrived: Set<string>; wait: Promise<void>; release: () => void } | undefined;
const calls: Array<{ operation: string; key: string }> = [], logs: unknown[][] = [], external: string[] = [];
const responses: Array<{ tool: string; bytes: number; elapsed_ms: number }> = [];
const stored = new Map<string, string>();
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return async (...args: unknown[]) => {
      const operation = String(property), key = String(args[0]); calls.push({ operation, key });
      if (property === "get" && failedGet === key) throw new Error(MARKER);
      if (property === "get" && gate?.keys.has(key)) {
        const active = gate; active.arrived.add(key);
        if (active.arrived.size === active.keys.size) active.release();
        await active.wait;
      }
      return Reflect.apply(value, target, args);
    };
  },
});
async function put(key: string, value: unknown) {
  const body = typeof value === "string" ? value : JSON.stringify(value);
  stored.set(key, body); await env.SLIPSTREAM_DATA.put(key, body);
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "d1-contract", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); stored.clear(); calls.length = 0; logs.length = 0; external.length = 0; responses.length = 0;
  failedGet = undefined; gate = undefined; rpcId = 0;
  for (const method of ["error", "warn", "log", "info"] as const)
    vi.spyOn(console, method).mockImplementation((...args) => { logs.push(args); });
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${ISSUER}/cdn-cgi/access/certs`) return Response.json({ keys: [jwk] });
    external.push(url); throw new Error("Unexpected external request in D1 contract");
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "d1-contract" }).setIssuer(ISSUER).setAudience("d1")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(privateKey);
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: ISSUER, ACCESS_AUD: "d1", MCP_HOSTNAME: HOST,
    HEALTH_TIMEZONE: "Europe/Oslo", MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    SLIPSTREAM_DATA: bucket, MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
  await put(HRV, hrv()); await put(OTHER_HRV, hrv(OTHER_DAY)); await put(SLEEP, sleep());
  await put(FIT, fit()); await put(ENDURANCE, endurance());
  await put(ACTIVITIES, activityCsv); await put(HEALTH, healthCsv);
});
afterEach(async context => {
  context.task.meta.d1Resources = {
    operations: Object.fromEntries(["get", "head", "list", "put", "delete"].map(operation =>
      [operation, calls.filter(call => call.operation === operation).length])),
    // Exact UTF-8 fixture body bytes for these identity-encoded GETs; excludes headers/JWKS.
    fixture_read_bytes: calls.filter(call => call.operation === "get")
      .reduce((sum, call) => sum + new TextEncoder().encode(stored.get(call.key) ?? "").byteLength, 0),
    responses: [...responses], non_jwks_external_calls: external.length,
  };
  try {
    expect(calls.filter(call => ["put", "delete"].includes(call.operation))).toEqual([]);
    expect(external).toEqual([]);
    expect(JSON.stringify(logs)).not.toContain(MARKER);
    for (const [key, body] of stored) expect(await (await env.SLIPSTREAM_DATA.get(key))?.text()).toBe(body);
  } finally {
    await env.SLIPSTREAM_DATA.delete([...stored.keys()]);
    clearSummaryCaches(); vi.restoreAllMocks();
  }
});

async function rpc(protocol: string, method: string, params: Row) {
  const modern = protocol === "2026-07-28", id = ++rpcId;
  const started = performance.now();
  const context = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${HOST}/mcp`, {
    method: "POST", headers: { host: HOST, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(modern ? { "mcp-method": method, ...(method === "tools/call" ? { "mcp-name": String(params.name) } : {}) } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id, method, params: { ...params,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, context);
  const raw = await response.text(); await waitOnExecutionContext(context);
  if (method === "tools/call") responses.push({ tool: String(params.name),
    bytes: new TextEncoder().encode(raw).byteLength, elapsed_ms: performance.now() - started });
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  expect(response.headers.get("cache-control")).toContain("no-store");
  expect(response.headers.get("x-content-type-options")).toBe("nosniff");
  expect(response.headers.get("referrer-policy")).toBe("no-referrer");
  const line = raw.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : raw); expect(message.id).toBe(id); expect(message.error).toBeUndefined();
  return { result: message.result, raw, id };
}
async function initialize(protocol: string) {
  // The installed 2026 transport is stateless and has no initialize handshake.
  if (protocol === "2026-07-28") {
    const response = await rpc(protocol, "tools/list", {});
    expect(response.result.tools.map((tool: { name: string }) => tool.name)).toContain("hrv_curve");
    return;
  }
  const response = await rpc(protocol, "initialize", { protocolVersion: protocol, capabilities: {},
    clientInfo: { name: "D1 synthetic contract", version: "1" } });
  expect(response.result.serverInfo.name).toBe("slipstream-fitness");
}
const call = (protocol: string, name: string, args: Row) => rpc(protocol, "tools/call", { name, arguments: args });
function success(reply: Awaited<ReturnType<typeof rpc>>, schema: { parse(value: unknown): unknown }) {
  expect(reply.result.isError).not.toBe(true);
  expect(JSON.parse(reply.result.content[0].text)).toEqual(reply.result.structuredContent);
  expect(schema.parse(reply.result.structuredContent)).toEqual(reply.result.structuredContent);
  return reply.result.structuredContent;
}
function controlledError(reply: Awaited<ReturnType<typeof rpc>>) {
  expect(reply.raw).not.toContain(MARKER);
  expect(reply.result).toEqual({ content: [{ type: "text", text: FAILURE }], isError: true });
}
function contractTest(name: string, body: () => Promise<void>) {
  it(name, body);
}

for (const protocol of ["2026-07-28", "2025-03-26"]) {
  contractTest(`${protocol}: preserves HRV zero/null/numeric strings and clock scalars`, async () => {
    await initialize(protocol); const output = success(await call(protocol, "hrv_curve", { date: DAY }), outputSchemas.hrv_curve);
    expect(output.readings).toEqual([{ timestamp: "2026-09-30T00:00:00Z", hrv_ms: 0 }, { timestamp: null, hrv_ms: null },
      { timestamp: 1790726400000, hrv_ms: "50" }, { timestamp: "1790726400", hrv_ms: 50 }]);
    expect(output.summary).toEqual({ calendarDate: DAY, lastNightAvg: 0, weeklyAvg: "50", status: "BALANCED" });
    expect(output).toMatchObject({ available: true, wake_date: DAY, night_of: "2026-09-29",
      sleep_start_local: "2026-09-29T23:30:00+02:00", local_time_source: "configured_timezone", reading_count: 4 });
  });
  for (const clock of ["seconds", "milliseconds", "decimal-string"] as const) {
    contractTest(`${protocol}: preserves ${clock} source clocks and local provenance`, async () => {
      const source = hrv();
      const start = Date.parse("2026-09-29T21:30:00Z"), end = Date.parse("2026-09-30T04:30:00Z");
      source.sleep_start_gmt = clock === "seconds" ? start / 1000 : clock === "milliseconds" ? start : String(start);
      source.sleep_end_gmt = clock === "seconds" ? end / 1000 : clock === "milliseconds" ? end : String(end);
      await put(HRV, source); await initialize(protocol);
      const output = success(await call(protocol, "hrv_curve", { date: DAY }), outputSchemas.hrv_curve);
      expect(output.sleep_start_gmt).toBe(source.sleep_start_gmt); expect(output.sleep_end_gmt).toBe(source.sleep_end_gmt);
      expect(output.sleep_start_local).toBe("2026-09-29T23:30:00+02:00");
    });
  }
  contractTest(`${protocol}: preserves Garmin-local travel clocks before configured timezone`, async () => {
    const source = hrv(); source.sleep_start_gmt = "2026-09-29T15:30:00Z"; source.sleep_end_gmt = "2026-09-29T22:30:00Z";
    source.sleep_start_garmin_local = "2026-09-30T00:30:00"; source.sleep_end_garmin_local = "2026-09-30T07:30:00";
    await put(HRV, source); await initialize(protocol);
    const output = success(await call(protocol, "hrv_curve", { date: DAY }), outputSchemas.hrv_curve);
    expect(output).toMatchObject({ night_of: DAY, sleep_start_local: "2026-09-30T00:30:00+09:00",
      sleep_end_local: "2026-09-30T07:30:00+09:00", local_time_source: "garmin_local", timezone: null });
  });
  contractTest(`${protocol}: preserves dynamic score names, zero/null and sleep units`, async () => {
    await initialize(protocol); const output = success(await call(protocol, "sleep_detail", { date: DAY }), outputSchemas.sleep_detail);
    expect(output.score_breakdown).toEqual({ overall: { value: 82, qualifier: "GOOD" }, duration: { value: 90, qualifier: "EXCELLENT" },
      AD_SYNTHETIC_DYNAMIC_SCORE: { value: 0, qualifier: null } });
    expect(output.summary).toMatchObject({ sleep_seconds: 25200, awake_sleep_seconds: 0, average_spo2_percent: null });
    expect(output).toMatchObject({ confirmed: true, stage_count: 1, wake_date: DAY, night_of: "2026-09-29" });
  });
  contractTest(`${protocol}: preserves dynamic metric scalars and recorded TE provenance`, async () => {
    await initialize(protocol); const output = success(await call(protocol, "strength_session", { activity_id: "garmin-123" }), outputSchemas.strength_session);
    expect(output.session.sets[0].performance_metrics).toEqual({ avgConcentricMeanVelocity: 0.42, tal_grit: null,
      AD_SYNTHETIC_NUMBER: 0, AD_SYNTHETIC_TEXT: "scalar", AD_SYNTHETIC_BOOLEAN: false, AD_SYNTHETIC_NULL: null });
    expect(output.session.sets[0]).toMatchObject({ weight_kg: 60, rest_after_seconds: 69.024 });
    expect(output.session.summary.external_volume_kg).toBe(720);
    expect(output.training_context).toMatchObject({ source: "garmin_fit_session", method: "fit_recorded_estimate", scale: "0_to_5",
      activity_id: "garmin-123", association: "matched", local_date: DAY, source_fit_sha256: "a".repeat(64),
      aerobic: { status: "available", value: 1.5, source_field: "total_training_effect" },
      anaerobic: { status: "available", value: 0, source_field: "total_anaerobic_training_effect" } });
  });
  contractTest(`${protocol}: preserves actual Python producer literal and normal dimension rows`, async () => {
    const source = fit(); source.normalized_strength_session = strengthLabels;
    await put(FIT, source); await initialize(protocol);
    const reply = await call(protocol, "strength_session", { activity_id: "123" });
    const output = success(reply, outputSchemas.strength_session);
    const expected = Object.fromEntries([
      ["__proto__", { sets: 2, repetitions: 5, external_volume_kg: 8 }],
      ["Synthetic normal", { sets: 1, repetitions: 0, external_volume_kg: 0 }],
    ]);
    expect(strengthLabels.summary.by_exercise).toEqual(expected);
    expect(strengthLabels.summary.by_category).toEqual(expected);
    for (const channel of [output, JSON.parse(reply.result.content[0].text)]) {
      expect(channel.session).toEqual(strengthLabels);
      expect(channel.session.summary).toMatchObject({ active_sets: 3, total_repetitions: 5, external_volume_kg: 8 });
      for (const group of [channel.session.summary.by_exercise, channel.session.summary.by_category]) {
        expect(Object.hasOwn(group, "__proto__")).toBe(true);
        expect(group).toEqual(expected);
        expect(Object.getPrototypeOf(group)).toBe(Object.prototype);
      }
    }
    expect(Object.hasOwn(Object.prototype, "sets")).toBe(false);
    expect(Object.hasOwn(Object.prototype, "repetitions")).toBe(false);
  });
  contractTest(`${protocol}: projects producer dimension extras and preserves nullable numeric strings`, async () => {
    const session = JSON.parse(strengthLabelJson);
    for (const map of [session.summary.by_exercise, session.summary.by_category]) {
      map.__proto__.sets = "2";
      map.__proto__.repetitions = 0;
      map.__proto__.external_volume_kg = null;
      for (const row of Object.values(map)) (row as Row).AD_PRIVATE_EXTRA = { raw: MARKER };
    }
    const source = fit(); source.normalized_strength_session = session;
    await put(FIT, source); await initialize(protocol);
    const reply = await call(protocol, "strength_session", { activity_id: "123" });
    expect(reply.raw).not.toContain(MARKER);
    const output = success(reply, outputSchemas.strength_session);
    for (const map of [output.session.summary.by_exercise, output.session.summary.by_category]) {
      expect(Object.hasOwn(map, "__proto__")).toBe(true);
      expect(map.__proto__).toEqual({ sets: "2", repetitions: 0, external_volume_kg: null });
      expect(map["Synthetic normal"]).toEqual({ sets: 1, repetitions: 0, external_volume_kg: 0 });
    }
    expect(Object.hasOwn(Object.prototype, "AD_PRIVATE_EXTRA")).toBe(false);
  });
  for (const dimension of ["by_exercise", "by_category"] as const)
    for (const label of ["__proto__", "Synthetic normal"])
      for (const [field, invalid] of [["sets", false], ["repetitions", [MARKER]],
        ["external_volume_kg", { raw: MARKER }]] as const)
        contractTest(`${protocol}: rejects producer ${dimension}/${label}/${field} invalid leaf`, async () => {
          const session = JSON.parse(strengthLabelJson); session.summary[dimension][label][field] = invalid;
          const source = fit(); source.normalized_strength_session = session;
          await put(FIT, source); await initialize(protocol);
          controlledError(await call(protocol, "strength_session", { activity_id: "123" }));
          expect(Object.hasOwn(Object.prototype, field)).toBe(false);
        });
  for (const dimension of ["by_exercise", "by_category"] as const)
    for (const shape of ["map array", "row boolean"])
      contractTest(`${protocol}: rejects producer ${dimension} ${shape}`, async () => {
        const session = JSON.parse(strengthLabelJson);
        if (shape === "map array") session.summary[dimension] = [MARKER];
        else session.summary[dimension].__proto__ = false;
        const source = fit(); source.normalized_strength_session = session;
        await put(FIT, source); await initialize(protocol);
        controlledError(await call(protocol, "strength_session", { activity_id: "123" }));
      });
  contractTest(`${protocol}: distinguishes missing source from controlled storage error`, async () => {
    await env.SLIPSTREAM_DATA.delete(HRV); stored.delete(HRV); await initialize(protocol);
    const missing = success(await call(protocol, "hrv_curve", { date: DAY }), outputSchemas.hrv_curve);
    expect(missing.available).toBe(false);
    await put(HRV, hrv()); failedGet = HRV;
    controlledError(await call(protocol, "hrv_curve", { date: DAY }));
  });
  const wrongShapes: Array<[string, (source: ReturnType<typeof hrv>) => void]> = [
    ["measurement object", source => { source.readings[0].hrv_ms = { private: MARKER }; }],
    ["timestamp array", source => { source.readings[0].timestamp = [MARKER]; }],
    ["date object", source => { source.date = { private: MARKER }; }],
    ["summary scalar object", source => { source.summary.lastNightAvg = { private: MARKER }; }],
    ["boolean numeric measurement", source => { source.readings[0].hrv_ms = false; }],
  ];
  for (const [label, mutate] of wrongShapes) contractTest(`${protocol}: rejects HRV ${label} without no_data or marker`, async () => {
    const source = hrv(); mutate(source); await put(HRV, source); await initialize(protocol);
    controlledError(await call(protocol, "hrv_curve", { date: DAY }));
  });
  contractTest(`${protocol}: projects extra HRV summary keys before both output channels`, async () => {
    const source = hrv(); source.summary.AD_PRIVATE_EXTRA = { raw: MARKER }; await put(HRV, source); await initialize(protocol);
    const reply = await call(protocol, "hrv_curve", { date: DAY }); const output = success(reply, outputSchemas.hrv_curve);
    expect(reply.raw).not.toContain(MARKER);
    expect(output.summary).toEqual({ calendarDate: DAY, lastNightAvg: 0, weeklyAvg: "50", status: "BALANCED" });
  });
  contractTest(`${protocol}: projects stored score extra keys before both output channels`, async () => {
    const source = sleep(); source.score_breakdown.overall.AD_PRIVATE_EXTRA = MARKER; await put(SLEEP, source); await initialize(protocol);
    const reply = await call(protocol, "sleep_detail", { date: DAY });
    expect(reply.raw).not.toContain(MARKER);
    const output = success(reply, outputSchemas.sleep_detail); expect(output.score_breakdown.overall).toEqual({ value: 82, qualifier: "GOOD" });
  });
  for (const field of ["value", "qualifier"] as const) contractTest(`${protocol}: rejects necessary sleep score ${field} object`, async () => {
    const source = sleep(); source.score_breakdown.overall[field] = { private: MARKER }; await put(SLEEP, source); await initialize(protocol);
    controlledError(await call(protocol, "sleep_detail", { date: DAY }));
  });
  contractTest(`${protocol}: projects strength structural extra keys without losing scalar metrics`, async () => {
    const source = fit(); source.normalized_strength_session.sets[0].AD_PRIVATE_EXTRA = { raw: MARKER }; await put(FIT, source); await initialize(protocol);
    const reply = await call(protocol, "strength_session", { activity_id: "123" }); const output = success(reply, outputSchemas.strength_session);
    expect(reply.raw).not.toContain(MARKER);
    expect(output.session.sets[0]).not.toHaveProperty("AD_PRIVATE_EXTRA");
    expect(output.session.sets[0].performance_metrics.AD_SYNTHETIC_NUMBER).toBe(0);
  });
  contractTest(`${protocol}: rejects object in a required strength structural scalar`, async () => {
    const source = fit(); (source.normalized_strength_session.sets[0].exercise as Row).probability = { private: MARKER };
    await put(FIT, source); await initialize(protocol); controlledError(await call(protocol, "strength_session", { activity_id: "123" }));
  });
  contractTest(`${protocol}: preserves endurance units, nulls, zero, numeric strings and training provenance`, async () => {
    await initialize(protocol);
    const output = success(await call(protocol, "endurance_session", { activity_id: "123" }), outputSchemas.endurance_session);
    expect(output.session).toEqual(endurance());
    expect(output.training_context).toMatchObject({ source: "garmin_fit_session", local_date: DAY,
      source_fit_sha256: "a".repeat(64), aerobic: { value: 1.5 }, anaerobic: { value: 0 } });
  });
  contractTest(`${protocol}: projects endurance structural extras in every row before both channels`, async () => {
    const source = endurance() as unknown as Row;
    const typed = source as ReturnType<typeof endurance>;
    for (const row of [source, typed.activity, typed.summary, ...typed.laps, ...typed.kilometer_splits,
      typed.distance_halves, typed.distance_halves.first as Row, typed.distance_halves.second as Row,
      ...typed.sampled_trackpoints]) row.AD_PRIVATE_EXTRA = { raw: MARKER };
    await put(ENDURANCE, source); await initialize(protocol);
    const reply = await call(protocol, "endurance_session", { activity_id: "123" });
    expect(reply.raw).not.toContain(MARKER);
    expect(success(reply, outputSchemas.endurance_session).session).toEqual(endurance());
  });
  const badEndurance: Array<[string, (source: ReturnType<typeof endurance>) => void]> = [
    ["summary measurement object", source => { source.summary.distance_m = { private: MARKER }; }],
    ["lap scalar array", source => { source.laps[0].calories = [MARKER]; }],
    ["split boolean measurement", source => { source.kilometer_splits[0].distance_m = false; }],
    ["split partial object", source => { source.kilometer_splits[0].partial = { private: MARKER }; }],
    ["half row array", source => { source.distance_halves.first = [MARKER]; }],
    ["BPM duration object", source => { source.heart_rate_seconds_by_bpm["150"] = { private: MARKER }; }],
    ["trackpoint scalar object", source => { source.sampled_trackpoints[0].heart_rate_bpm = { private: MARKER }; }],
    ["trackpoint timestamp array", source => { source.sampled_trackpoints[0].timestamp = [MARKER]; }],
  ];
  for (const [label, mutate] of badEndurance) contractTest(`${protocol}: rejects endurance ${label}`, async () => {
    const source = endurance(); mutate(source); await put(ENDURANCE, source); await initialize(protocol);
    controlledError(await call(protocol, "endurance_session", { activity_id: "123" }));
  });
  contractTest(`${protocol}: preserves the producer no_timed_trackpoints result`, async () => {
    const source = { ...endurance(), available: false, reason: "no_timed_trackpoints",
      distance_halves: null, sampled_trackpoints: [], kilometer_splits: [] };
    await put(ENDURANCE, source); await initialize(protocol);
    const output = success(await call(protocol, "endurance_session", { activity_id: "123" }), outputSchemas.endurance_session);
    expect(output.available).toBe(true); expect(output.session).toEqual(source);
  });
  contractTest(`${protocol}: preserves all HRV summary aliases and dynamic score names`, async () => {
    const source = hrv();
    source.summary = { calendarDate: DAY, lastNightAvg: 0, lastNightAverage: "50", lastNightAvgMs: null,
      lastNight5MinHigh: 60, lastNightFiveMinHigh: " 0x3d ", lastNight5MinHighMs: null,
      weeklyAvg: 50, weeklyAverage: "51", weeklyAvgMs: null, status: "BALANCED", statusKey: null, hrvStatus: "LOW",
      baselineBalancedLower: 0, baselineLow: "40", baselineLower: null,
      baselineBalancedUpper: 80, baselineHigh: "81", baselineUpper: null };
    const night = sleep(); night.score_breakdown = Object.fromEntries(Array.from({ length: 17 }, (_, i) =>
      [`future_score_${i}`, { value: i === 0 ? 0 : null, qualifier: i % 2 ? "GOOD" : null }]));
    await put(HRV, source); await put(SLEEP, night); await initialize(protocol);
    expect(success(await call(protocol, "hrv_curve", { date: DAY }), outputSchemas.hrv_curve).summary).toEqual(source.summary);
    expect(success(await call(protocol, "sleep_detail", { date: DAY }), outputSchemas.sleep_detail).score_breakdown)
      .toEqual(night.score_breakdown);
  });
  contractTest(`${protocol}: projects sleep summary and stage extras without changing numeric strings`, async () => {
    const source = sleep(); const summary = source.summary as Row;
    summary.sleep_seconds = "25200"; summary.AD_PRIVATE_EXTRA = { raw: MARKER };
    (source.stages[0] as Row).AD_PRIVATE_EXTRA = MARKER;
    await put(SLEEP, source); await initialize(protocol);
    const reply = await call(protocol, "sleep_detail", { date: DAY }); expect(reply.raw).not.toContain(MARKER);
    const output = success(reply, outputSchemas.sleep_detail);
    expect(output.summary).toEqual({ ...sleep().summary, sleep_seconds: "25200" });
    expect(output.stages).toEqual(sleep().stages);
  });
  contractTest(`${protocol}: projects strength root, exercise, summary and group extras`, async () => {
    const source = fit(); const session = source.normalized_strength_session;
    for (const row of [session, session.sets[0].exercise as Row, session.summary,
      session.summary.by_exercise["Synthetic squat"], session.summary.by_category.SQUAT]) (row as Row).AD_PRIVATE_EXTRA = MARKER;
    session.sets[0].repetitions = "12";
    await put(FIT, source); await initialize(protocol);
    const reply = await call(protocol, "strength_session", { activity_id: "123" }); expect(reply.raw).not.toContain(MARKER);
    const expected = fit().normalized_strength_session; expected.sets[0].repetitions = "12";
    expect(success(reply, outputSchemas.strength_session).session).toEqual(expected);
  });
  contractTest(`${protocol}: D4 rejects opaque metric extensions`, async () => {
    const source = fit(); source.normalized_strength_session.sets[0].performance_metrics = {
      future_scalar_zero: 0, future_scalar_false: false, future_scalar_text: "scalar", future_scalar_null: null,
      future_opaque: { original: [MARKER, { value: 1 }] },
    };
    await put(FIT, source); await initialize(protocol);
    controlledError(await call(protocol, "strength_session", { activity_id: "123" }));
  });
  contractTest(`${protocol}: D4 preserves already-stringified qualifiers`, async () => {
    const source = sleep(); source.score_breakdown.overall.qualifier = JSON.stringify({ original: ["synthetic extension"] });
    await put(SLEEP, source); await initialize(protocol);
    expect(success(await call(protocol, "sleep_detail", { date: DAY }), outputSchemas.sleep_detail).score_breakdown)
      .toEqual(source.score_breakdown);
  });
  contractTest(`${protocol}: D4 preserves literal scalar metric names without record rewriting`, async () => {
    const source = fit();
    source.normalized_strength_session.sets[0].performance_metrics = JSON.parse(
      '{"__proto__":false,"constructor":0,"future_scalar":null,"numeric_text":"0","empty_text":""}');
    await put(FIT, source); await initialize(protocol);
    const output = success(await call(protocol, "strength_session", { activity_id: "123" }), outputSchemas.strength_session);
    expect(output.session.sets[0].performance_metrics).toEqual(source.normalized_strength_session.sets[0].performance_metrics);
    expect(Object.getPrototypeOf({})).not.toHaveProperty("original");
  });
  for (const name of ["__proto__", "constructor", "future_metric"]) {
    for (const value of [{ private: MARKER }, [MARKER], {}, []]) {
      contractTest(`${protocol}: D4 rejects compound ${name} metric ${JSON.stringify(value)}`, async () => {
        const source = fit();
        source.normalized_strength_session.sets[0].performance_metrics = Object.fromEntries([[name, value]]);
        await put(FIT, source); await initialize(protocol);
        controlledError(await call(protocol, "strength_session", { activity_id: "123" }));
      });
    }
  }
  contractTest(`${protocol}: D4 preserves actual Python producer strength scalars and literal dimensions`, async () => {
    const source = fit(); source.normalized_strength_session = scalarLeaves.strength;
    await put(FIT, source); await initialize(protocol);
    const reply = await call(protocol, "strength_session", { activity_id: "123" });
    const output = success(reply, outputSchemas.strength_session);
    expect(output.session).toEqual(scalarLeaves.strength);
    for (const channel of [output, JSON.parse(reply.result.content[0].text)]) {
      expect(Object.hasOwn(channel.session.sets[0].performance_metrics, "__proto__")).toBe(true);
      expect(channel.session.sets[0].performance_metrics).toEqual(JSON.parse(
        '{"avgConcentricMeanVelocity":0.42,"tal_grit":null,"__proto__":0,"constructor":false,"future_numeric_text":"0","future_text":"scalar","future_true":true}'));
    }
  });
  contractTest(`${protocol}: D4 preserves actual Python producer sleep qualifier scalars and history`, async () => {
    await put(SLEEP, scalarLeaves.sleep); await initialize(protocol);
    const output = success(await call(protocol, "sleep_detail", { date: DAY }), outputSchemas.sleep_detail);
    expect(output.score_breakdown).toEqual(scalarLeaves.sleep.score_breakdown);
    expect(output.score_breakdown.future_numeric_qualifier).toEqual({ value: 0, qualifier: "0" });
    expect(output.score_breakdown.future_boolean).toEqual({ value: 0, qualifier: "False" });
    expect(output.score_breakdown.future_history.qualifier).toBe("{'historical': ['synthetic text']}");
    expect(output).toMatchObject({ wake_date: DAY, night_of: "2026-09-29",
      sleep_start_local: "2026-09-29T23:30:00+02:00", local_time_source: "garmin_local" });
  });
  const badStructures: Array<[string, string, string, unknown]> = [
    ["HRV root", HRV, "hrv_curve", [MARKER]],
    ["HRV rows", HRV, "hrv_curve", { ...hrv(), readings: { private: MARKER } }],
    ["HRV row", HRV, "hrv_curve", { ...hrv(), readings: [false] }],
    ["HRV local clock", HRV, "hrv_curve", { ...hrv(), sleep_start_garmin_local: { private: MARKER } }],
    ["sleep root", SLEEP, "sleep_detail", false],
    ["sleep summary", SLEEP, "sleep_detail", { ...sleep(), summary: [MARKER] }],
    ["sleep rows", SLEEP, "sleep_detail", { ...sleep(), stages: { private: MARKER } }],
    ["sleep row", SLEEP, "sleep_detail", { ...sleep(), stages: [false] }],
    ["sleep confirmation", SLEEP, "sleep_detail", { ...sleep(), confirmed: { private: MARKER } }],
    ["strength root", FIT, "strength_session", false],
    ["strength session", FIT, "strength_session", { ...fit(), normalized_strength_session: [MARKER] }],
    ["strength rows", FIT, "strength_session", { ...fit(), normalized_strength_session: { sets: { private: MARKER } } }],
    ["strength row", FIT, "strength_session", { ...fit(), normalized_strength_session: { sets: [false] } }],
    ["strength summary", FIT, "strength_session", { ...fit(), normalized_strength_session: { sets: [], summary: [MARKER] } }],
    ["endurance root", ENDURANCE, "endurance_session", [MARKER]],
    ["endurance rows", ENDURANCE, "endurance_session", { ...endurance(), sampled_trackpoints: { private: MARKER } }],
    ["endurance row", ENDURANCE, "endurance_session", { ...endurance(), sampled_trackpoints: [false] }],
  ];
  for (const [label, key, tool, source] of badStructures) contractTest(`${protocol}: rejects unsupported ${label} instead of false no_data`, async () => {
    await put(key, source); await initialize(protocol);
    controlledError(await call(protocol, tool, key === HRV || key === SLEEP ? { date: DAY } : { activity_id: "123" }));
  });
  for (const [tool, reads] of [["hrv_curve", 1], ["sleep_detail", 1], ["strength_session", 2], ["endurance_session", 3]] as const)
    contractTest(`${protocol}: resource comparison ${tool}`, async () => {
      await initialize(protocol);
      success(await call(protocol, tool, tool === "hrv_curve" || tool === "sleep_detail" ? { date: DAY } : { activity_id: "123" }),
        outputSchemas[tool]);
      expect(calls.filter(call => call.operation === "get")).toHaveLength(reads);
      expect(calls.filter(call => call.operation === "head")).toHaveLength(reads > 1 ? 1 : 0);
      expect(calls.some(call => ["list", "put", "delete"].includes(call.operation))).toBe(false);
    });
  contractTest(`${protocol}: isolates concurrent positive A and invalid marker B without cache mutation`, async () => {
    const bad = hrv(OTHER_DAY); bad.readings[0].hrv_ms = { private: MARKER }; await put(OTHER_HRV, bad); await initialize(protocol);
    const beforeActivities = success(await call(protocol, "list_activities", {}), outputSchemas.list_activities);
    const beforeHealth = success(await call(protocol, "health_status", {}), outputSchemas.health_status);
    let release!: () => void; const wait = new Promise<void>(resolve => { release = resolve; });
    gate = { keys: new Set([HRV, OTHER_HRV]), arrived: new Set(), wait, release };
    const [good, rejected] = await Promise.all([call(protocol, "hrv_curve", { date: DAY }), call(protocol, "hrv_curve", { date: OTHER_DAY })]);
    expect(gate.arrived.size).toBe(2); gate = undefined;
    const afterActivities = success(await call(protocol, "list_activities", {}), outputSchemas.list_activities);
    const afterHealth = success(await call(protocol, "health_status", {}), outputSchemas.health_status);
    expect(afterActivities).toEqual(beforeActivities); expect(afterHealth).toEqual(beforeHealth);
    expect(calls.filter(call => call.operation === "get" && call.key === ACTIVITIES)).toHaveLength(1);
    expect(calls.filter(call => call.operation === "get" && call.key === HEALTH)).toHaveLength(1);
    const again = await call(protocol, "hrv_curve", { date: DAY });
    expect(success(again, outputSchemas.hrv_curve)).toEqual(success(good, outputSchemas.hrv_curve));
    expect(good.id).not.toBe(rejected.id); expect(good.raw).not.toContain(MARKER);
    controlledError(rejected);
  });
}
