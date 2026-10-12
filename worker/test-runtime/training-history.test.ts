import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { boundedTrainingResult, readTrainingHistory, trainingDates, trainingDayKey, trainingHistorySchema,
  TRAINING_STRUCTURED_BYTES, TRAINING_RESULT_BYTES } from "../src/training-history";
import contract from "./generated/training-day.json";

const start = "2026-10-01", end = "2026-10-31";
const issuer = "https://synthetic-training-history.cloudflareaccess.com";
const hostname = "training-history.example";
const counts = { get: 0, head: 0, list: 0, put: 0, delete: 0 };
let key: CryptoKey;
let fetchSpy: ReturnType<typeof vi.spyOn>;
const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
  const value = Reflect.get(target, property);
  if (typeof value !== "function") return value;
  return (...args: unknown[]) => {
    if (property in counts) counts[property as keyof typeof counts]++;
    return Reflect.apply(value, target, args);
  };
} });

function fixture(day = "2026-10-08", multiple = false, distinct = false) {
  return structuredClone(contract.days[day as keyof typeof contract.days][distinct ? multiple ? "multiple_dual" : "dual" : multiple ? "multiple" : "single"]);
}
type HistoryDay = Awaited<ReturnType<typeof readTrainingHistory>>["days"][number];
function latest(day: HistoryDay, candidate = 0) {
  return day.observations!.find(obs => obs.observation_ref === day.candidates![candidate].latest_observation)!;
}
function clock(day: HistoryDay, ref: string) {
  return day.clocks!.find(value => value.time_ref === ref)!;
}
function check(day: HistoryDay, ref: string | null) {
  return day.checks!.find(value => value.check_ref === ref)!;
}
async function call(args: Record<string, unknown>) {
  const jwt = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "day-key" }).setIssuer(issuer).setAudience("synthetic-day")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": jwt, "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "training_history", arguments: args } }),
  }), { ...env, SLIPSTREAM_DATA: bucket, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-day", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } }, ctx);
  const text = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.headers.get("cache-control")).toContain("no-store");
  const message = response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(text.split("\n").find(line => line.startsWith("data: "))!.slice(6)) : JSON.parse(text);
  return message;
}
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); key = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "day-key", alg: "RS256", use: "sig" };
  fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("External network forbidden");
    return Response.json({ keys: [jwk] });
  });
});
beforeEach(async () => {
  for (const date of trainingDates(start, end)) await env.SLIPSTREAM_DATA.delete(trainingDayKey(date));
  for (const property of Object.keys(counts)) counts[property as keyof typeof counts] = 0;
});
afterAll(() => { expect(fetchSpy).toHaveBeenCalledTimes(1); vi.restoreAllMocks(); });

it("reads Python canonical bytes through authenticated read-only MCP with no private IDs", async () => {
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(contract.single));
  const message = await call({ start_date: "2026-10-08", end_date: "2026-10-08" });
  expect(message.error).toBeUndefined(); expect(message.result.isError).not.toBe(true);
  const output = message.result.structuredContent;
  expect(trainingHistorySchema.parse(output)).toEqual(output);
  expect(JSON.parse(message.result.content[0].text)).toEqual(output);
  expect(latest(output.days[0]).readiness.value).toBe(80);
  expect(latest(output.days[0]).recovery).toMatchObject({ value: 60, status: "conflicting", effective_remaining_minutes: null });
  expect(output.metric_units).toEqual({ readiness: "score_1_100", recovery: "minutes" });
  expect(output.source).toEqual({ name: "garmin-connect", sdk_version: "0.3.17", normalizer_revision: 1 });
  expect(output.days[0].observations).toHaveLength(1); expect(output.days[0].clocks).toHaveLength(1);
  expect(output.days[0].candidates[0].latest_observation).toBe(output.days[0].candidates[0].morning_observation);
  expect(JSON.stringify(output)).not.toMatch(/device_id|900000|123456|userProfile|feedbackLong|private|\.json/);
  expect(counts).toEqual({ get: 1, head: 0, list: 0, put: 0, delete: 0 });
});

it.each([
  ["both", 80, "available", 60, "available", "available"],
  ["readiness_only", 80, "available", null, "no_observation", "partial"],
  ["recovery_only", null, "no_observation", 60, "available", "partial"],
  ["recovery_zero", null, "no_observation", 0, "available", "partial"],
  ["invalid_readiness", null, "invalid", 60, "available", "partial"],
  ["invalid_recovery", 80, "available", null, "invalid", "partial"],
  ["empty_retained", 80, "available", 60, "available", "no_observation"],
  ["readiness_retained", 80, "available", 30, "available", "partial"],
  ["recovery_retained", 90, "available", 60, "available", "partial"],
  ["source_error_retained", 80, "available", 60, "available", "source_error"],
] as const)("exposes independent Python-normalized fields through authenticated MCP: %s",
  async (name, readiness, readinessStatus, recovery, recoveryStatus, attempt) => {
    const value = contract.field_cases[name];
    await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
    const result = (await call({ start_date: "2026-10-08", end_date: "2026-10-08" })).result;
    const output = result.structuredContent, day = output.days[0], obs = latest(day);
    expect(result.isError).not.toBe(true);
    expect(trainingHistorySchema.parse(output)).toEqual(output);
    expect(output.metric_units).toEqual({ readiness: "score_1_100", recovery: "minutes" });
    expect(output.source).toEqual({ name: "garmin-connect", sdk_version: "0.3.17", normalizer_revision: 1 });
    expect(day.support).toBe("unknown"); expect(day.selection_status).toBe("available");
    expect(day.provenance.last_attempt.status).toBe(attempt);
    expect(day.status).toBe(attempt === "no_observation" ? "partial" : attempt);
    expect(day.provenance.source_revision).toBe(value.source_revision);
    expect(day.provenance.source_checked_at).toBe(value.source_checked_at);
    if (name === "empty_retained" || name === "source_error_retained") {
      expect(day.provenance.source_revision).toBe(contract.field_cases.both.source_revision);
    }
    expect(obs.readiness).toMatchObject({ value: readiness, status: readinessStatus });
    expect(obs.recovery).toMatchObject({ value: recovery, status: recoveryStatus, effective_remaining_minutes: recovery });
    for (const field of ["readiness", "recovery"] as const) {
      const retained = name === "empty_retained" || name === "source_error_retained" || name === `${field}_retained`;
      const updated = name.endsWith("_retained") && !retained;
      expect(obs[field].retained_previous).toBe(retained);
      expect(clock(day, obs[field].time_ref)).toMatchObject({
        observation_at: `2026-10-08T0${updated ? 6 : 5}:30:00+00:00`,
        observation_local: `2026-10-08T0${updated ? 8 : 7}:30:00`, time_source: "garmin_offset",
      });
      if (obs[field].value === null) expect(obs[field].check_ref).toBeNull();
      else expect(check(day, obs[field].check_ref).source_checked_at).toBe(`2026-10-08T0${updated ? 8 : 7}:00:00+00:00`);
    }
    expect(JSON.stringify(output)).not.toMatch(/device_id|900000|userProfile|feedbackLong/);
    expect(counts).toEqual({ get: 1, head: 0, list: 0, put: 0, delete: 0 });
  });

it("keeps a Python-normalized empty response distinct from unsupported hardware", async () => {
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(contract.field_cases.empty));
  const day = (await call({ start_date: "2026-10-08", end_date: "2026-10-08" })).result.structuredContent.days[0];
  expect(day).toMatchObject({ status: "no_observation", support: "unknown", selected_candidate: null });
  expect(day.observations).toEqual([]); expect(day.candidates).toEqual([]);
  expect(counts).toEqual({ get: 1, head: 0, list: 0, put: 0, delete: 0 });
});

it.each(["multiple_complementary", "selected_readiness_only", "selected_recovery_only"] as const)(
  "keeps complementary measurements on separate devices without combining them: %s", async name => {
    await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(contract.field_cases[name]));
    const day = (await call({ start_date: "2026-10-08", end_date: "2026-10-08" })).result.structuredContent.days[0];
    expect(day.support).toBe("unknown"); expect(day.candidates).toHaveLength(2);
    expect([0, 1].map(index => [latest(day, index).readiness.value, latest(day, index).recovery.value]))
      .toEqual([[80, null], [null, 60]]);
    expect(day.selection_status).toBe(name === "multiple_complementary" ? "ambiguous" : "available");
    expect(day.selected_candidate).toBe(name === "multiple_complementary" ? null
      : name === "selected_readiness_only" ? "candidate_1" : "candidate_2");
    expect(JSON.stringify(day)).not.toMatch(/900000|900001|device_id/);
    expect(counts).toEqual({ get: 1, head: 0, list: 0, put: 0, delete: 0 });
  });

it("sees historical overwrite and missing-then-created day on next call without a cache/index/sync", async () => {
  const day = "2026-10-01";
  const args = { start_date: day, end_date: day };
  expect((await call(args)).result.structuredContent.days[0].status).toBe("not_collected");
  const value = fixture(day);
  await env.SLIPSTREAM_DATA.put(trainingDayKey(day), JSON.stringify(value));
  expect(latest((await call(args)).result.structuredContent.days[0]).readiness.value).toBe(80);
  value.devices[0].latest!.readiness.value = 95;
  value.source_revision = "b".repeat(64);
  await env.SLIPSTREAM_DATA.put(trainingDayKey(day), JSON.stringify(value));
  const output = (await call(args)).result.structuredContent;
  expect(latest(output.days[0]).readiness.value).toBe(95);
  expect(output.days[0].provenance.source_revision).toBe("b".repeat(64));
  expect(counts).toEqual({ get: 3, head: 0, list: 0, put: 0, delete: 0 });
});

it.each([
  { start_date: "2026-10-01", end_date: "2026-11-01" },
  { start_date: "2026-02-30", end_date: "2026-03-01" },
  { start_date: "0000-01-01", end_date: "0000-01-01" },
  { start_date: "2026-10-08", end_date: "2026-10-01" },
  { start_date: "2026-10-08", end_date: "2026-10-08", url: "https://example.invalid" },
])("rejects invalid range/extra input before any R2 access", async args => {
  const response = await call(args);
  expect(response.error || response.result.isError).toBeTruthy();
  expect(counts.get).toBe(0);
});

it("rejects large/compressed/unknown-schema days separately from absence and releases bodies", async () => {
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-01"), " ".repeat(16385));
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-02"), JSON.stringify(fixture("2026-10-02")), { httpMetadata: { contentEncoding: "gzip" } });
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-03"), '{"schema_version":99}');
  const output = await readTrainingHistory(bucket, "2026-10-01", "2026-10-04");
  expect(output.days.map(day => day.status)).toEqual(["oversize", "schema_unverified", "schema_unverified", "not_collected"]);
  expect(counts.get).toBe(4);
});

it.each(["2026-10-07T07:30:00", "2026-10-08T24:30:00", "2026-10-08T07:60:00"])(
  "rejects invalid or different-day canonical local clocks", async clock => {
    const value = fixture();
    value.devices[0].latest!.readiness.observation_local = clock;
    await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
    expect((await readTrainingHistory(bucket, "2026-10-08", "2026-10-08")).days[0].status).toBe("schema_unverified");
    expect(counts.get).toBe(1);
  },
);

it("caps active GET/readers at four, missing GET counts, and reads 31 dates directly", async () => {
  let active = 0, peak = 0;
  const controlled: Pick<R2Bucket, "get"> = { get: async (...args: Parameters<R2Bucket["get"]>) => {
    active++; peak = Math.max(peak, active);
    await new Promise(resolve => setTimeout(resolve, 1));
    const object = await bucket.get(...args); active--;
    return object;
  } };
  const output = await readTrainingHistory(controlled, start, end);
  expect(output.days).toHaveLength(31); expect(counts.get).toBe(31); expect(peak).toBe(4);
  expect(output.days.map(day => day.date)).toEqual(trainingDates(start, end));
});

it("limits falsely sized stream consumption to 16KiB plus one byte and cancels", async () => {
  const day = "2026-10-08";
  await env.SLIPSTREAM_DATA.put(trainingDayKey(day), "{}");
  const original = (await env.SLIPSTREAM_DATA.get(trainingDayKey(day)))!;
  await original.body.cancel();
  let consumed = 0, cancelled = 0;
  const body = new ReadableStream<Uint8Array>({ type: "bytes", pull(controller) {
    const request = controller.byobRequest!;
    const length = request.view!.byteLength;
    new Uint8Array(request.view!.buffer, request.view!.byteOffset, length).fill(32);
    consumed += length; request.respond(length);
  }, cancel() { cancelled++; } });
  const oversized = new Proxy(original, { get(target, property) {
    if (property === "body") return body;
    if (property === "size") return 2;
    return Reflect.get(target, property);
  } });
  const controlled: Pick<R2Bucket, "get"> = { get: async () => oversized };
  const output = await readTrainingHistory(controlled, day, day);
  expect(output.days[0].status).toBe("oversize");
  expect(consumed).toBe(16385); expect(cancelled).toBe(1); expect(body.locked).toBe(false);
});

it("holds the four-slot concurrency bound through delayed body reads, not only GET", async () => {
  const day = "2026-10-08";
  await env.SLIPSTREAM_DATA.put(trainingDayKey(day), "{}");
  const original = (await env.SLIPSTREAM_DATA.get(trainingDayKey(day)))!;
  await original.body.cancel();
  let active = 0, peak = 0, calls = 0;
  const controlled: Pick<R2Bucket, "get"> = { get: async () => {
    calls++; active++; peak = Math.max(peak, active);
    let read = false;
    const body = new ReadableStream<Uint8Array>({ type: "bytes", async pull(controller) {
      if (read) { controller.close(); controller.byobRequest?.respond(0); return; }
      await new Promise(resolve => setTimeout(resolve, 1));
      read = true;
      const request = controller.byobRequest!;
      new Uint8Array(request.view!.buffer, request.view!.byteOffset, 2).set([123, 125]);
      request.respond(2); controller.close(); active--;
    } });
    return new Proxy(original, { get(target, property) { return property === "body" ? body : Reflect.get(target, property); } });
  } };
  await readTrainingHistory(controlled, start, end);
  expect(peak).toBe(4); expect(active).toBe(0); expect(calls).toBe(31);
});

it("returns R2 failures and stored auth failure explicitly, without claiming absent Garmin data", async () => {
  const controlled: Pick<R2Bucket, "get"> = { get: async () => { throw new Error("private raw exception"); } };
  expect((await readTrainingHistory(controlled, "2026-10-08", "2026-10-08")).days[0])
    .toMatchObject({ status: "source_error", basis: "canonical_read_failed", support: "unknown" });
  const value = fixture();
  value.devices = []; value.source_checked_at = null;
  value.selection = { status: "no_observation", device_id: null };
  value.last_attempt.status = "auth_required";
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
  expect((await readTrainingHistory(bucket, "2026-10-08", "2026-10-08")).days[0].status).toBe("auth_required");
});

it("enforces structured and whole-result output limits without truncation", async () => {
  for (const day of trainingDates(start, end)) await env.SLIPSTREAM_DATA.put(trainingDayKey(day), JSON.stringify(fixture(day, true, true)));
  const output = await readTrainingHistory(bucket, start, end);
  const result = boundedTrainingResult(output);
  expect(result.structuredContent).toMatchObject({ status: "output_limit", limit: "structured_content", days: [] });
  expect(new TextEncoder().encode(JSON.stringify(result.structuredContent)).length).toBeLessThanOrEqual(TRAINING_STRUCTURED_BYTES);
  expect(new TextEncoder().encode(JSON.stringify(result)).length).toBeLessThanOrEqual(TRAINING_RESULT_BYTES);
  expect(counts).toEqual({ get: 31, head: 0, list: 0, put: 0, delete: 0 });
});

it("checks the whole MCP result including its escaped text copy separately", async () => {
  const output = await readTrainingHistory(bucket, start, end);
  const result = boundedTrainingResult({ ...output, message: "\"".repeat(22000) });
  expect(result.structuredContent.limit).toBe("mcp_result");
  expect(new TextEncoder().encode(JSON.stringify(result)).length).toBeLessThanOrEqual(TRAINING_RESULT_BYTES);
});

it("separates field age/check and eight-hour display from historical freshness", async () => {
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(contract.single));
  const output = await readTrainingHistory(bucket, "2026-10-08", "2026-10-08", Date.parse("2026-10-08T15:00:00Z"));
  const field = latest(output.days[0]).readiness;
  expect(check(output.days[0], field.check_ref)).toMatchObject({ source_check_age_seconds: 28800 });
  expect(clock(output.days[0], field.time_ref)).toMatchObject({ observation_age_seconds: 34200, display_state: "older_than_8h" });
  expect(output.display_policy.semantics).toBe("stored_observation_not_live_freshness");
});

it.each([false, true])("returns all 31 ordinary one-device rows, distinct morning/latest=%s, within both unchanged limits", async distinct => {
  for (const day of trainingDates(start, end)) await env.SLIPSTREAM_DATA.put(trainingDayKey(day), JSON.stringify(fixture(day, false, distinct)));
  const response = await call({ start_date: start, end_date: end });
  const result = response.result, output = result.structuredContent;
  expect(output.status).toBe("complete"); expect(output.days).toHaveLength(31);
  expect(trainingHistorySchema.parse(output)).toEqual(output);
  expect(output.days.map((day: HistoryDay) => day.date)).toEqual(trainingDates(start, end));
  for (const day of output.days) {
    expect(day.observations).toHaveLength(distinct ? 2 : 1);
    expect(latest(day).readiness.value).toBe(distinct ? 85 : 80);
    expect(clock(day, latest(day).readiness.time_ref).observation_local!.slice(0, 10)).toBe(day.date);
    expect(day.provenance.source_revision).toMatch(/^[a-f0-9]{64}$/);
  }
  expect(new TextEncoder().encode(JSON.stringify(output)).length).toBeLessThanOrEqual(TRAINING_STRUCTURED_BYTES);
  expect(new TextEncoder().encode(JSON.stringify(result)).length).toBeLessThanOrEqual(TRAINING_RESULT_BYTES);
  expect(counts).toEqual({ get: 31, head: 0, list: 0, put: 0, delete: 0 });
});

it("preserves a retained field's own clock/check instead of inheriting the new observation clock", async () => {
  const value = fixture("2026-10-08", false, true);
  value.devices[0].latest!.readiness = { ...contract.single.devices[0].latest!.readiness, retained_previous: true };
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
  const output = await readTrainingHistory(bucket, "2026-10-08", "2026-10-08", Date.parse("2026-10-08T22:00:00Z"));
  const day = output.days[0], observation = latest(day), field = observation.readiness;
  expect(field.time_ref).not.toBe(observation.time_ref);
  expect(field).toMatchObject({ value: 80, retained_previous: true });
  expect(check(day, field.check_ref)).toMatchObject({ source_checked_at: "2026-10-08T07:00:00+00:00", source_check_age_seconds: 54000 });
  expect(clock(day, field.time_ref)).toMatchObject({ observation_at: "2026-10-08T05:30:00+00:00", observation_age_seconds: 59400, time_source: "garmin_offset" });
  expect(clock(day, observation.time_ref).observation_age_seconds).toBe(19800);
});

it.each(Object.entries(contract.configured))("keeps configured device collection outcome separate from absence: %s", async (name, value) => {
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
  const response = await call({ start_date: "2026-10-08", end_date: "2026-10-08" });
  const day = response.result.structuredContent.days[0];
  const complete = name.startsWith("available-") || name.startsWith("no_observation-");
  const retained = name.endsWith("-retained");
  expect(day.status).toBe(complete ? "selected_device_unavailable" : value.last_attempt.status);
  expect(day.selection_status).toBe(complete ? "selected_device_unavailable" : retained ? "available" : "unknown");
  expect(day.provenance.last_attempt).toEqual(value.last_attempt);
  if (retained) {
    expect(day.selected_candidate).toBe("candidate_1");
    const field = latest(day).readiness;
    expect(field).toMatchObject({ value: 80, retained_previous: true });
    expect(check(day, field.check_ref).source_checked_at).toBe("2026-10-08T07:00:00+00:00");
    expect(clock(day, field.time_ref)).toMatchObject({ observation_at: "2026-10-08T05:30:00+00:00", time_source: "garmin_offset" });
  } else expect(day.selected_candidate).toBeNull();
  expect(JSON.stringify(day)).not.toMatch(/900000|900001|device_id|private/);
  expect(counts).toEqual({ get: 1, head: 0, list: 0, put: 0, delete: 0 });
});

it("does not repeat an old v1 failure's false selected-device-absence label", async () => {
  const value = structuredClone(contract.configured["auth_required-retained"]);
  value.selection.status = "selected_device_unavailable";
  await env.SLIPSTREAM_DATA.put(trainingDayKey("2026-10-08"), JSON.stringify(value));
  const day = (await readTrainingHistory(bucket, "2026-10-08", "2026-10-08")).days[0];
  expect(day.status).toBe("auth_required"); expect(day.selection_status).toBe("available");
  expect(latest(day).readiness.retained_previous).toBe(true);
});
