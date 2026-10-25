import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import history from "../../tests/fixtures/gct_engine_140.json";
import current1100 from "../../tests/fixtures/coach_engine_1100_raw_balance.json";
import recovery1100 from "../../tests/fixtures/coach_engine_1100_recovery.json";

const issuer = "https://current-context.cloudflareaccess.com", hostname = "current-context.example";
const prefix = "activities/2026/42/";
let key: CryptoKey, jwk: Record<string, unknown>, token: string, testEnv: Env;
let failCanonicalRead: boolean, failContextRead: boolean, writes: number, failedReadAttempts: number;
let holdNextContextPut: boolean, failAfterNextContextPut: boolean, contextPutStarted: () => void, releaseContextPut: () => void;
let heldContextPut: Promise<void>, startedContextPut: Promise<void>;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "current", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  failCanonicalRead = false; failContextRead = false; writes = 0; failedReadAttempts = 0;
  holdNextContextPut = false; failAfterNextContextPut = false;
  heldContextPut = new Promise(resolve => { releaseContextPut = resolve; });
  startedContextPut = new Promise(resolve => { contextPutStarted = resolve; });
  clearSummaryCaches();
  const contexts = await env.SLIPSTREAM_DATA.list({ prefix: prefix + "context/v1/", limit: 100 });
  if (contexts.truncated) throw new Error("Synthetic context limit");
  if (contexts.objects.length) await env.SLIPSTREAM_DATA.delete(contexts.objects.map(item => item.key));
  for (const [name, encoded] of Object.entries(history.objects_base64)) {
    await env.SLIPSTREAM_DATA.put(name, Uint8Array.from(atob(encoded), char => char.charCodeAt(0)));
  }
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "current" }).setIssuer(issuer).setAudience("current")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    if ((input instanceof Request ? input.url : String(input)) === issuer + "/cdn-cgi/access/certs") {
      return Response.json({ keys: [jwk] });
    }
    throw new Error("Unexpected network in synthetic current-context test");
  });
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, property) {
    return property === "getByName" ? () => coordinator : Reflect.get(target, property);
  } });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const method = Reflect.get(target, property);
    if (typeof method !== "function") return method;
    return (...args: unknown[]) => {
      if (property === "put") writes++;
      if (property === "put" && failAfterNextContextPut && String(args[0]).includes("/context/v1/")) {
        failAfterNextContextPut = false;
        return Reflect.apply(method, target, args).then(() => { throw new Error("SYNTHETIC_PRIVATE_UNKNOWN_PUT"); });
      }
      if (property === "put" && holdNextContextPut && String(args[0]).includes("/context/v1/")) {
        holdNextContextPut = false; contextPutStarted();
        return heldContextPut.then(() => Reflect.apply(method, target, args));
      }
      if (property === "get" && failContextRead && String(args[0]).includes("/context/v1/")) {
        throw new Error("SYNTHETIC_PRIVATE_CONTEXT_ERROR");
      }
      if (property === "get" && failCanonicalRead && String(args[0]).includes("/canonical/")) {
        failedReadAttempts++; throw new Error("SYNTHETIC_PRIVATE_PROVIDER_DETAIL");
      }
      return Reflect.apply(method, target, args);
    };
  } });
  testEnv = { ...env, SLIPSTREAM_DATA: bucket, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "current", MCP_HOSTNAME: hostname,
    REFRESH_COORDINATOR: namespace, MCP_WRITES_ENABLED: "true", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
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

async function loadCurrentPublication() {
  for (const [name, encoded] of Object.entries(current1100)) {
    await env.SLIPSTREAM_DATA.put(name, Uint8Array.from(atob(encoded), char => char.charCodeAt(0)));
  }
  // run_one's source fixture contains no summary index. Seed the explicit outer
  // index with its actual decoded FIT UTC/local clocks, not the unrelated1.4 index.
  await env.SLIPSTREAM_DATA.put("summary/activities.csv",
    "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
    + "garmin-42,2026-10-08 07:00:00,Synthetic cadence run,running,60,2026-10-08\n");
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`an unconfirmed context PUT stays held rather than expiring into another append ${protocol}`, async () => {
    const now = Date.now();
    failAfterNextContextPut = true;
    const first = await call(protocol, "add_activity_context", { activity_id: "42", rpe: 7 });
    expect(first.isError).toBe(true);
    expect(JSON.stringify(first)).not.toContain("SYNTHETIC_PRIVATE_UNKNOWN_PUT");
    vi.spyOn(Date, "now").mockReturnValue(now + 60_000);
    const second = await call(protocol, "add_activity_context", { activity_id: "42", note: "SYNTHETIC_NOT_ACCEPTED" });
    expect(second.isError).toBe(true);
    expect(JSON.stringify(second)).toContain("This attempt was not saved");
    expect(writes).toBe(1);
    const current = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(current.user_context).toMatchObject({ rpe: 7, note: null });
  });

  it(`a delayed context append cannot be overtaken after the actor cooldown ${protocol}`, async () => {
    const began = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(began);
    holdNextContextPut = true;
    const first = call(protocol, "add_activity_context", { activity_id: "42", rpe: 7 });
    await startedContextPut;
    clock.mockReturnValue(began + 11_000);
    let overlapping;
    try { overlapping = await call(protocol, "add_activity_context", { activity_id: "42", note: "SYNTHETIC_LATER_NOTE" }); }
    finally { releaseContextPut(); }
    expect((await first).structuredContent.saved).toBe(true);
    // This attempt must be explicitly unaccepted, not another successful stale
    // snapshot. Only a confirmed non-write may subsequently be tried again.
    expect(overlapping.isError).toBe(true);
    expect(overlapping.structuredContent?.saved).not.toBe(true);
    expect(writes).toBe(1);
    clock.mockReturnValue(began + 22_000);
    const later = await call(protocol, "add_activity_context", { activity_id: "42", note: "SYNTHETIC_LATER_NOTE" });
    expect(later.structuredContent.saved).toBe(true);
    const current = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(current.user_context).toMatchObject({ rpe: 7, note: "SYNTHETIC_LATER_NOTE" });
    expect(writes).toBe(2);
  });

  it(`an unreadable current context is unavailable, never a claim that no declaration was stored ${protocol}`, async () => {
    const saved = await call(protocol, "add_activity_context", { activity_id: "42", note: "SYNTHETIC_CURRENT_NOTE" });
    expect(saved.structuredContent.saved).toBe(true);
    failContextRead = true;
    const result = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(result.available).toBe(true);
    expect(result.view.context_status).toBe("unavailable");
    expect(result.user_context).toBeNull();
    expect(result.ground_contact_sides.reason).toBe("current_context_unavailable");
    expect(JSON.stringify(result)).not.toContain("SYNTHETIC_PRIVATE_CONTEXT_ERROR");
    expect(writes).toBe(1);
  });

  it(`current recovery measurements are source-bound and reject a forged unit ${protocol}`, async () => {
    for (const [name, encoded] of Object.entries(recovery1100.objects_base64)) {
      await env.SLIPSTREAM_DATA.put(name, Uint8Array.from(atob(encoded), char => char.charCodeAt(0)));
    }
    const before = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(before.available).toBe(true);
    expect(before.analysis.recovery_running_metrics.items[0]).toMatchObject({ bout: 3, source_laps: [4],
      average_ground_contact_time: { value: 240, unit: "ms" },
      average_ground_contact_balance: { value: 48.24, unit: "%", side_semantics: "unknown" } });
    expect(before.ground_contact_sides.record_aggregates.recovery_bouts.items[0]).toMatchObject({
      bout: 3, source_laps: [4], raw_percent: 48.24, raw_side: "right", left_percent: 51.76, right_percent: 48.24 });
    const coach = (await call(protocol, "coach_input", { activity_id: "42", format: "coach" })).structuredContent;
    expect(coach.presentation.text).toContain('{"value":48.24,"side_semantics":"right","left_percent":51.76,"right_percent":48.24,"calculated_side":"left"}');
    const broken = structuredClone(before.analysis);
    broken.recovery_running_metrics.items[0].average_ground_contact_time.unit = "s";
    await env.SLIPSTREAM_DATA.put(`${prefix}coach-input/v1/canonical/${recovery1100.analysis_id}.json`, JSON.stringify(broken));
    const invalid = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
  });

  it(`a current declaration labels measured kilometers and FIT halves without changing raw values ${protocol}`, async () => {
    await loadCurrentPublication();
    const before = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(before.available).toBe(true);
    const saved = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: before.analysis.source.fit_sha256, raw_side: "right" } });
    expect(saved.isError, JSON.stringify(saved)).not.toBe(true);
    expect(saved.structuredContent.saved).toBe(true);
    const after = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(after.ground_contact_sides.distance_aggregates.kilometer_splits.items[0]).toMatchObject({
      split: 1, status: "available", raw_percent: 48, recorded_side: "right",
      calculated_side: "left", right_percent: 48, left_percent: 52,
    });
    expect(after.ground_contact_sides.distance_aggregates.distance_halves.items).toHaveLength(2);
    expect(after.ground_contact_sides.distance_aggregates.distance_halves.items[1]).toMatchObject({
      segment: "second", right_percent: 48, left_percent: 52,
    });
    expect(after.analysis).toEqual(before.analysis);
    expect(saved.structuredContent.coach_input.presentation.text).toContain('"left_percent":52,"right_percent":48');
  });

  it(`a genuine current publication retains measured kilometer balance with unknown side ${protocol}`, async () => {
    await loadCurrentPublication();
    const result = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(result.available).toBe(true);
    expect(result.analysis.analyzer_version).toBe("1.10.0");
    const row = result.analysis.recorded_running_metrics.kilometer_splits.items[0];
    // Every original synthetic FIT record has 48%; no native-lap substitution.
    expect(row.average_ground_contact_balance).toMatchObject({ status: "available", value: 48,
      unit: "%", side_semantics: "unknown", coverage: { status: "complete", valid_seconds: 60 } });
    expect(row.average_step_length).toMatchObject({ status: "available", value: 1000, unit: "mm" });
    expect(result.ground_contact_sides.status).toBe("unverified");
    expect(result.view.interpretation.semantics).toBe("display_guard_only_not_physiological_validation");
    const coached = (await call(protocol, "coach_input", { activity_id: "42", format: "coach" })).structuredContent;
    expect(coached.presentation.text).toContain("| Km | Pace (s/km) | HR avg/max (bpm) |");
    expect(coached.presentation.text).toContain('{"value":48,"side_semantics":"unknown"}');
  });

  it(`a current FIT-bound declaration updates displayed session and native sides without rewriting analysis ${protocol}`, async () => {
    const before = (await call(protocol, "coach_input", { activity_id: "42" })).structuredContent;
    expect(before.available).toBe(true);
    const saved = await call(protocol, "add_activity_context", { activity_id: "42",
      gct_balance_side: { source_fit_sha256: before.analysis.source.fit_sha256, raw_side: "right" } });
    expect(saved.structuredContent.saved).toBe(true);
    const after = (await call(protocol, "coach_input", { activity_id: "42" })).structuredContent;
    expect(after.ground_contact_sides).toMatchObject({ status: "available",
      declaration: { raw_side: "right", context_id: saved.structuredContent.context_id },
      session: { recorded_side: "right", calculated_side: "left", right_percent: 49.25, left_percent: 50.75 } });
    expect(after.ground_contact_sides.laps.items[1]).toMatchObject({ recorded_side: "right",
      calculated_side: "left", right_percent: 51.25, left_percent: 48.75 });
    expect(after.analysis).toEqual(before.analysis);
  });

  it(`a failed receipt view preserves the saved context without retrying its write ${protocol}`, async () => {
    failCanonicalRead = true;
    const saved = await call(protocol, "add_activity_context", { activity_id: "42", note: "SYNTHETIC_SAVED_NOTE" });
    expect(saved.isError).not.toBe(true);
    expect(saved.structuredContent).toMatchObject({ saved: true, context: { note: "SYNTHETIC_SAVED_NOTE" },
      coach_input: { available: false } });
    expect(JSON.stringify(saved)).not.toContain("SYNTHETIC_PRIVATE_PROVIDER_DETAIL");
    expect(writes).toBe(1); expect(failedReadAttempts).toBe(1);
    failCanonicalRead = false;
    const current = await call(protocol, "coach_input", { activity_id: "42" });
    expect(current.structuredContent.user_context.note).toBe("SYNTHETIC_SAVED_NOTE");
    expect(writes).toBe(1);
  });

  it(`saving explicit context returns its updated Coach Input without another user step ${protocol}`, async () => {
    const saved = await call(protocol, "add_activity_context", {
      activity_id: "42", rpe: 7, note: "SYNTHETIC_AUTOMATIC_VIEW",
    });
    expect(saved.structuredContent.saved).toBe(true);
    expect(saved.structuredContent.coach_input).toMatchObject({ available: true,
      user_context: { rpe: 7, note: "SYNTHETIC_AUTOMATIC_VIEW" },
      view: { analysis_id: history.analysis_id, context_revision: saved.structuredContent.context_id } });
    expect(saved.structuredContent.coach_input.presentation.text).toContain("SYNTHETIC_AUTOMATIC_VIEW");
  });

  it(`an explicit null clears only that current context field ${protocol}`, async () => {
    const first = await call(protocol, "add_activity_context", {
      activity_id: "42", rpe: 6, conditions: "Synthetic dry", note: "SYNTHETIC_KEEP_NOTE",
    });
    expect(first.structuredContent.saved).toBe(true);
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 11_000);
    const cleared = await call(protocol, "add_activity_context", { activity_id: "42", rpe: null });
    expect(cleared.isError).not.toBe(true);
    expect(cleared.structuredContent.saved).toBe(true);
    const current = (await call(protocol, "coach_input", { activity_id: "42" })).structuredContent;
    expect(current.user_context).toMatchObject({ rpe: null, conditions: "Synthetic dry", note: "SYNTHETIC_KEEP_NOTE" });
    expect(current.view.context_revision).toBe(cleared.structuredContent.context_id);
  });

  it(`partial context updates retain fields the user did not change ${protocol}`, async () => {
    const first = await call(protocol, "add_activity_context", {
      activity_id: "42", rpe: 6, conditions: "Synthetic dry", note: "FIRST_SYNTHETIC_NOTE",
      workout_correction: "Synthetic correction",
    });
    expect(first.structuredContent.saved).toBe(true);
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 11_000);
    const second = await call(protocol, "add_activity_context", { activity_id: "42", note: "SECOND_SYNTHETIC_NOTE" });
    expect(second.structuredContent.saved).toBe(true);
    const current = (await call(protocol, "coach_input", { activity_id: "42" })).structuredContent;
    expect(current.user_context).toMatchObject({ rpe: 6, conditions: "Synthetic dry",
      note: "SECOND_SYNTHETIC_NOTE", workout_correction: "Synthetic correction" });
    expect(current.view.context_revision).toBe(second.structuredContent.context_id);
  });

  it(`new user context is immediately visible with an unchanged historical analysis ${protocol}`, async () => {
    const before = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(before.available).toBe(true);
    const saved = await call(protocol, "add_activity_context", {
      activity_id: "42", rpe: 7, conditions: "Synthetic wet", note: "SYNTHETIC_CURRENT_NOTE",
    });
    expect(saved.isError).not.toBe(true);
    expect(saved.structuredContent.saved).toBe(true);
    const after = (await call(protocol, "coach_input", { activity_id: "42", format: "full" })).structuredContent;
    expect(after.user_context).toMatchObject({ rpe: 7, conditions: "Synthetic wet", note: "SYNTHETIC_CURRENT_NOTE" });
    expect(after.view).toMatchObject({ schema_version: 1, context_status: "available",
      context_revision: saved.structuredContent.context_id, analysis_id: history.analysis_id, analyzer_version: "1.4.0" });
    expect(after.analysis).toEqual(before.analysis);
    const coach = (await call(protocol, "coach_input", { activity_id: "42", format: "coach" })).structuredContent;
    expect(coach.presentation.text).toContain("SYNTHETIC_CURRENT_NOTE");
    expect(coach.presentation.source_analysis_id).toBe(history.analysis_id);
  });
}
