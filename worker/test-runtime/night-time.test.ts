import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";

const issuer = "https://night-time-synthetic.cloudflareaccess.com";
const hostname = "night-time.example";
const day = "2026-10-25";
const sleepKey = "health/sleep/v1/2026/10/2026-10-25.json";
const hrvKey = "health/hrv/2026/10/2026-10-25.json";
// Independent worked DST example: 21:58Z -> 06:48Z spans 530 minutes.
const sourceWindow = {
  sleep_start_gmt: "2026-10-24T21:58:00Z",
  sleep_end_gmt: "2026-10-25T06:48:00Z",
  sleep_start_garmin_local: "2026-10-24T22:58:00",
  sleep_end_garmin_local: "2026-10-25T07:48:00",
};
const summary = {
  sleep_seconds: 31800, deep_sleep_seconds: null, light_sleep_seconds: null,
  rem_sleep_seconds: null, awake_sleep_seconds: null, unmeasurable_sleep_seconds: null,
  nap_seconds: null, sleep_score: 80, average_spo2_percent: null, lowest_spo2_percent: null,
  average_respiration_brpm: null, lowest_respiration_brpm: null, highest_respiration_brpm: null,
  average_sleep_stress: null,
};
let key: CryptoKey;
let jwk: Record<string, unknown>;
let token: string;
let testEnv: Env;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "night-time", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== issuer + "/cdn-cgi/access/certs") throw new Error("External request forbidden");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "night-time" }).setIssuer(issuer).setAudience("night-time")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "night-time", MCP_HOSTNAME: hostname,
    HEALTH_TIMEZONE: "Europe/Oslo", MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "" };
  await env.SLIPSTREAM_DATA.put(sleepKey, JSON.stringify({ date: day, ...sourceWindow, summary, stages: [] }));
  await env.SLIPSTREAM_DATA.put(hrvKey, JSON.stringify({ date: day, available: true, ...sourceWindow,
    summary: { lastNightAvg: 50 }, readings: [{ timestamp: "2026-10-25T00:30:00Z", hrv_ms: 50 }] }));
});
afterEach(async () => {
  await env.SLIPSTREAM_DATA.delete(sleepKey);
  await env.SLIPSTREAM_DATA.delete(hrvKey);
  vi.restoreAllMocks();
});

async function call(name: string, args: Record<string, unknown>, protocol: string) {
  const modern = protocol === "2026-07-28";
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined();
  expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  return message.result.structuredContent;
}

describe("documented night time through authenticated JSON and SSE readers", () => {
  for (const protocol of ["2025-03-26", "2026-07-28"]) {
    it.each(["sleep_detail", "hrv_curve", "sleep_history", "hrv_history"])(
      `preserves source clocks and separately projects both DST endpoints via %s / ${protocol}`, async name => {
        const history = name.endsWith("history");
        const output = await call(name, history
          ? { start_date: day, end_date: day, granularity: "daily", detail_level: "full" }
          : { date: day }, protocol);
        const night = history ? output.days[0] : output;
        expect(night).toMatchObject({
          wake_date: day, actual_night_timezone: null, configured_timezone: "Europe/Oslo",
          // Preserve the legacy Garmin view; a configured home zone cannot prove a trip location.
          local_time_source: "garmin_local", sleep_start_local: "2026-10-24T22:58:00+01:00",
          source_time: sourceWindow,
          configured_projection: {
            timezone: "Europe/Oslo", night_of: "2026-10-24",
            sleep_start_local: "2026-10-24T23:58:00+02:00",
            sleep_end_local: "2026-10-25T07:48:00+01:00",
            sleep_midpoint_local: "2026-10-25T03:23:00+01:00",
          },
        });
        expect(night.time_quality_flags).toEqual(expect.arrayContaining([
          "actual_timezone_unknown", "source_local_conflicts_configured_timezone",
        ]));
      });
  }
});
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["sleep_detail", "hrv_curve", "sleep_history", "hrv_history"])(
    `uses explicit actual travel zone for night identity while retaining the home projection via %s / ${protocol}`, async name => {
      const window = {
        sleep_start_gmt: "2026-10-24T15:30:00Z", sleep_end_gmt: "2026-10-24T22:30:00Z",
        sleep_start_garmin_local: "2026-10-24T17:30:00", sleep_end_garmin_local: "2026-10-25T00:30:00",
      };
      // Tokyo midnight is the new date, while the configured Oslo view starts on October24.
      await env.SLIPSTREAM_DATA.put(sleepKey, JSON.stringify({ date: day, ...window,
        actual_night_timezone: "Asia/Tokyo", summary: { ...summary, sleep_seconds: 25200 }, stages: [] }));
      await env.SLIPSTREAM_DATA.put(hrvKey, JSON.stringify({ date: day, available: true, ...window,
        actual_night_timezone: "Asia/Tokyo", summary: { lastNightAvg: 50 },
        readings: [{ timestamp: "2026-10-24T16:00:00Z", hrv_ms: 50 }] }));
      const history = name.endsWith("history");
      const output = await call(name, history
        ? { start_date: day, end_date: day, granularity: "daily", detail_level: "full" }
        : { date: day }, protocol);
      const night = history ? output.days[0] : output;
      expect(night).toMatchObject({
        wake_date: day, night_of: "2026-10-25", actual_night_timezone: "Asia/Tokyo", timezone: "Asia/Tokyo",
        local_time_source: "actual_night_timezone",
        sleep_start_local: "2026-10-25T00:30:00+09:00", sleep_end_local: "2026-10-25T07:30:00+09:00",
        sleep_midpoint_local: "2026-10-25T04:00:00+09:00", source_time: window,
        configured_projection: { timezone: "Europe/Oslo", night_of: "2026-10-24",
          sleep_start_local: "2026-10-24T17:30:00+02:00", sleep_end_local: "2026-10-25T00:30:00+02:00" },
      });
      expect(night.time_quality_flags).toContain("source_local_conflicts_actual_timezone");
      expect(night.time_quality_flags).not.toContain("actual_timezone_unknown");
    });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each([
    { label: "reversed", start: "2026-10-25T06:48:00Z", end: "2026-10-24T21:58:00Z",
      projectedStart: "2026-10-25T07:48:00+01:00" },
    { label: "over36hours", start: "2026-10-22T21:58:00Z", end: "2026-10-25T06:48:00Z",
      projectedStart: "2026-10-22T23:58:00+02:00" },
    { label: "invalidcalendar", start: "2026-02-30T21:58:00Z", end: "2026-10-25T06:48:00Z",
      projectedStart: null },
  ])(`marks invalid source sleep window $label without inventing a midpoint via ${protocol}`, async fixture => {
    await env.SLIPSTREAM_DATA.put(sleepKey, JSON.stringify({ date: day,
      sleep_start_gmt: fixture.start, sleep_end_gmt: fixture.end,
      sleep_start_garmin_local: null, sleep_end_garmin_local: null, summary, stages: [] }));
    const night = await call("sleep_detail", { date: day }, protocol);
    expect(night.source_time.sleep_start_gmt).toBe(fixture.start);
    expect(night.configured_projection.sleep_start_local).toBe(fixture.projectedStart);
    expect(night.configured_projection.sleep_midpoint_local).toBeNull();
    expect(night.sleep_midpoint_local).toBeNull();
    expect(night.time_quality_flags).toContain("invalid_sleep_interval");
    expect(JSON.stringify(night)).not.toContain("NaN");
  });
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`keeps actual zone unknown for source-only offsets without assuming UTC via ${protocol}`, async () => {
    testEnv.HEALTH_TIMEZONE = "";
    const night = await call("sleep_detail", { date: day }, protocol);
    expect(night).toMatchObject({
      actual_night_timezone: null, configured_timezone: null, configured_projection: null, timezone: null,
      night_of: "2026-10-24", local_time_source: "garmin_local",
      sleep_start_local: "2026-10-24T22:58:00+01:00", source_time: sourceWindow,
    });
    expect(night.time_quality_flags).toContain("actual_timezone_unknown");
    expect(night.time_quality_flags).not.toContain("source_local_conflicts_configured_timezone");
    expect(JSON.stringify(night)).not.toContain("NaN");
  });
  it(`rejects an invalid actual timezone while preserving source clocks and configured projection via ${protocol}`, async () => {
    await env.SLIPSTREAM_DATA.put(sleepKey, JSON.stringify({ date: day, ...sourceWindow,
      actual_night_timezone: "Invalid/Zone", summary, stages: [] }));
    const night = await call("sleep_detail", { date: day }, protocol);
    expect(night).toMatchObject({ actual_night_timezone: null, source_time: sourceWindow,
      local_time_source: "garmin_local", sleep_start_local: "2026-10-24T22:58:00+01:00",
      configured_projection: { sleep_start_local: "2026-10-24T23:58:00+02:00" } });
    expect(night.time_quality_flags).toEqual(expect.arrayContaining([
      "actual_timezone_unknown", "invalid_actual_night_timezone",
    ]));
    expect(JSON.stringify(night)).not.toContain("NaN");
  });
}