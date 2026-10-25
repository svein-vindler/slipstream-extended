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
let truncateContextList: boolean;
let failActivityDataRead: boolean;
const contextReads: Array<{ operation: "get" | "list"; key: string; limit?: number }> = [];
let holdNextContextPut: boolean, failAfterNextContextPut: boolean, contextPutStarted: () => void, releaseContextPut: () => void;
let heldContextPut: Promise<void>, startedContextPut: Promise<void>;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "current", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  failCanonicalRead = false; failContextRead = false; writes = 0; failedReadAttempts = 0;
  truncateContextList = false; contextReads.length = 0;
  failActivityDataRead = false;
  holdNextContextPut = false; failAfterNextContextPut = false;
  heldContextPut = new Promise(resolve => { releaseContextPut = resolve; });
  startedContextPut = new Promise(resolve => { contextPutStarted = resolve; });
  clearSummaryCaches();
  for (const id of ["42", "43"]) {
    const contexts = await env.SLIPSTREAM_DATA.list({ prefix: `activities/2026/${id}/context/v1/`, limit: 100 });
    if (contexts.truncated) throw new Error("Synthetic context limit");
    if (contexts.objects.length) await env.SLIPSTREAM_DATA.delete(contexts.objects.map(item => item.key));
  }
  await env.SLIPSTREAM_DATA.delete([
    "activities/2026/43/activity.v1.json", "activities/2026/43/activity.v1.json.gz",
    "activities/2026/43/activity.endurance.v1.json", "activities/2026/43/activity.endurance.v1.json.gz",
    "activities/2026/43/coach-input/v1/latest-ready.json",
  ]);
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
      if (property === "get" && failActivityDataRead && /^activities\/2026\/43\/(?:activity\.|coach-input\/)/.test(String(args[0]))) {
        throw new Error("SYNTHETIC_PRIVATE_ACTIVITY_READ_FAILURE");
      }
      if (property === "get" && String(args[0]).includes("/context/v1/")) {
        contextReads.push({ operation: "get", key: String(args[0]) });
      }
      if (property === "list" && String((args[0] as R2ListOptions)?.prefix).includes("/context/v1/")) {
        const options = args[0] as R2ListOptions;
        contextReads.push({ operation: "list", key: String(options.prefix), limit: options.limit });
        if (truncateContextList) return Reflect.apply(method, target, args).then((listed: R2Objects) => ({ ...listed, truncated: true }));
      }
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
  it(`saved whole-session context accompanies a strength session without a running analysis ${protocol}`, async () => {
    const session = { schema_version: 1, activity_id: "43", sets: [], summary: { active_sets: 0, total_repetitions: 0 } };
    await env.SLIPSTREAM_DATA.put("summary/activities.csv",
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-43,2026-10-08 07:00:00,Synthetic strength,strength_training,60,2026-10-08\n");
    await env.SLIPSTREAM_DATA.put("activities/2026/43/activity.v1.json", JSON.stringify({
      schema_version: 1, source: "garmin-fit", activity: { id: "43", type: "strength_training",
        start_time_gmt: "2026-10-08T07:00:00Z", start_time_local: "2026-10-08T09:00:00" },
      normalized_strength_session: session,
    }));
    const saved = (await call(protocol, "add_activity_context", {
      activity_id: "43", rpe: 7, conditions: "Synthetic warm room", note: "SYNTHETIC_WHOLE_SESSION_NOTE",
    })).structuredContent;
    expect(saved.saved).toBe(true);
    const read = (await call(protocol, "strength_session", { activity_id: "garmin-43" })).structuredContent;
    expect(read.available).toBe(true);
    expect(read.session).toEqual(session);
    expect(read.user_context).toMatchObject({ activity_id: "43", rpe: 7,
      conditions: "Synthetic warm room", note: "SYNTHETIC_WHOLE_SESSION_NOTE" });
    expect(read.view).toMatchObject({ context_status: "available", context_revision: saved.context_id });
    failContextRead = true;
    const unreadable = (await call(protocol, "strength_session", { activity_id: "43" })).structuredContent;
    expect(unreadable.available).toBe(true);
    expect(unreadable.session).toEqual(session);
    expect(unreadable.user_context).toBeNull();
    expect(unreadable.view.context_status).toBe("unavailable");
    expect(writes).toBe(1);
  });

  for (const type of ["strength_training", "cycling", "hiking"]) {
    it(`current context remains readable without a running analysis for ${type} ${protocol}`, async () => {
      await env.SLIPSTREAM_DATA.put("summary/activities.csv",
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + `garmin-43,2026-10-08 07:00:00,Synthetic activity,${type},60,2026-10-08\n`);
      const saved = (await call(protocol, "add_activity_context", {
        activity_id: "43", rpe: 4, note: "SYNTHETIC_CONTEXT_WITHOUT_ANALYSIS",
      })).structuredContent;
      expect(saved.saved).toBe(true);
      for (const view of [saved.coach_input,
        (await call(protocol, "coach_input", { activity_id: "garmin-43", format: "full" })).structuredContent,
        (await call(protocol, "coach_input", { activity_id: "43", format: "coach" })).structuredContent]) {
        expect(view.available).toBe(false);
        expect(view.user_context).toMatchObject({ activity_id: "43", rpe: 4, note: "SYNTHETIC_CONTEXT_WITHOUT_ANALYSIS" });
        expect(view.view).toMatchObject({ context_status: "available", context_revision: saved.context_id });
        expect(view.view).not.toHaveProperty("analysis_id");
        expect(view.view).not.toHaveProperty("analyzer_version");
        expect(view).not.toHaveProperty("analysis");
        expect(view).not.toHaveProperty("presentation");
        expect(view).not.toHaveProperty("ground_contact_sides");
      }
      expect(writes).toBe(1);
    });
  }

  for (const type of ["running", "cycling", "hiking"]) {
    it(`an endurance read composes the latest context without changing its stored ${type} dataset ${protocol}`, async () => {
      await env.SLIPSTREAM_DATA.put("summary/activities.csv",
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + `garmin-43,2026-10-08 07:00:00,Synthetic activity,${type},60,2026-10-08\n`);
      const session = { schema_version: 1, source: "garmin-tcx", available: true,
        activity: { id: "43", type }, summary: { duration_seconds: 60, distance_m: 1000 }, laps: [] };
      const sessionBytes = JSON.stringify(session);
      await env.SLIPSTREAM_DATA.put("activities/2026/43/activity.endurance.v1.json", sessionBytes);
      const saved = (await call(protocol, "add_activity_context", {
        activity_id: "43", rpe: 6, conditions: "Synthetic dry", note: "SYNTHETIC_KEEP_THIS_NOTE",
      })).structuredContent;
      expect(saved.saved).toBe(true);
      const initial = (await call(protocol, "endurance_session", { activity_id: "43" })).structuredContent;
      expect(initial.available).toBe(true);
      expect(initial.user_context).toMatchObject({ rpe: 6, note: "SYNTHETIC_KEEP_THIS_NOTE" });
      vi.spyOn(Date, "now").mockReturnValue(Date.now() + 11_000);
      const updated = (await call(protocol, "add_activity_context", { activity_id: "43", rpe: null,
        workout_correction: "Synthetic correction" })).structuredContent;
      expect(updated.saved).toBe(true);
      const read = (await call(protocol, "endurance_session", { activity_id: "garmin-43" })).structuredContent;
      expect(read.available).toBe(true);
      expect(read.session).toEqual(session);
      expect(read.user_context).toMatchObject({ activity_id: "43", rpe: null, conditions: "Synthetic dry",
        note: "SYNTHETIC_KEEP_THIS_NOTE", workout_correction: "Synthetic correction" });
      expect(read.view).toMatchObject({ context_status: "available", context_revision: updated.context_id });
      expect(await (await env.SLIPSTREAM_DATA.get("activities/2026/43/activity.endurance.v1.json"))!.text()).toBe(sessionBytes);
      expect(await (await env.SLIPSTREAM_DATA.get(saved.key))!.json()).toMatchObject({ rpe: 6, workout_correction: null });
      failContextRead = true;
      const unreadable = (await call(protocol, "endurance_session", { activity_id: "43" })).structuredContent;
      expect(unreadable.available).toBe(true);
      expect(unreadable.session).toEqual(session);
      expect(unreadable.user_context).toBeNull();
      expect(unreadable.view.context_status).toBe("unavailable");
      expect(writes).toBe(2);
    });
  }

  for (const failure of ["read", "truncated", "foreign", "ambiguous"] as const) {
    it(`activity context ${failure} stays unavailable, private and bounded across individual tools ${protocol}`, async () => {
      await env.SLIPSTREAM_DATA.put("summary/activities.csv",
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + "garmin-43,2026-10-08 07:00:00,Synthetic other,hiking,60,2026-10-08\n");
      for (const tool of ["strength_session", "endurance_session", "coach_input"]) {
        const absent = (await call(protocol, tool, { activity_id: "43" })).structuredContent;
        expect(absent.available).toBe(false);
        expect(absent.user_context).toBeNull();
        expect(absent.view).toMatchObject({ context_status: "not_stored", context_revision: null, context_updated_at: null });
      }
      const saved = (await call(protocol, "add_activity_context", { activity_id: "43", rpe: 5,
        note: "SYNTHETIC_VALID_CONTEXT" })).structuredContent;
      expect(saved.saved).toBe(true);
      if (failure === "read") failContextRead = true;
      if (failure === "truncated") truncateContextList = true;
      if (failure === "foreign") await env.SLIPSTREAM_DATA.put("activities/2026/43/context/v1/20990101T000000000-foreign.json",
        JSON.stringify({ ...saved.context, activity_id: "44", note: "SYNTHETIC_PRIVATE_FOREIGN_CONTEXT" }));
      if (failure === "ambiguous") for (const suffix of ["a", "b"]) {
        await env.SLIPSTREAM_DATA.put(`activities/2026/43/context/v1/20990101T000000000-${suffix}.json`,
          JSON.stringify({ ...saved.context, context_id: suffix }));
      }
      for (const tool of ["strength_session", "endurance_session", "coach_input"]) {
        contextReads.length = 0;
        const result = (await call(protocol, tool, { activity_id: "43" })).structuredContent;
        expect(result.available).toBe(false);
        expect(result.user_context).toBeNull();
        expect(result.view).toMatchObject({ context_status: "unavailable", context_revision: null, context_updated_at: null });
        expect(contextReads.filter(read => read.operation === "list")).toEqual([
          { operation: "list", key: "activities/2026/43/context/v1/", limit: 100 },
        ]);
        expect(contextReads.filter(read => read.operation === "get")).toHaveLength(["read", "foreign"].includes(failure) ? 1 : 0);
        expect(JSON.stringify(result)).not.toMatch(/SYNTHETIC_PRIVATE|SYNTHETIC_VALID_CONTEXT/);
      }
      expect(writes).toBe(1);
      expect(await (await env.SLIPSTREAM_DATA.get(saved.key))!.json()).toEqual(saved.context);
    });
  }

  it(`one activity's context never leaks to another or adds per-row reads to lists ${protocol}`, async () => {
    await env.SLIPSTREAM_DATA.put("summary/activities.csv",
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 07:00:00,Synthetic running,running,60,2026-10-08\n"
      + "garmin-43,2026-10-08 08:00:00,Synthetic other,hiking,60,2026-10-08\n");
    const saved = (await call(protocol, "add_activity_context", { activity_id: "43", note: "SYNTHETIC_ONLY_ACTIVITY_43" })).structuredContent;
    expect(saved.saved).toBe(true);
    for (const tool of ["strength_session", "endurance_session", "coach_input"]) {
      const other = await call(protocol, tool, { activity_id: "42" });
      expect(other.isError).not.toBe(true);
      expect(JSON.stringify(other)).not.toContain("SYNTHETIC_ONLY_ACTIVITY_43");
      expect(other.structuredContent.view.context_revision).not.toBe(saved.context_id);
    }
    contextReads.length = 0;
    const missing = (await call(protocol, "coach_input", { activity_id: "999" })).structuredContent;
    expect(missing.available).toBe(false);
    expect(missing).not.toHaveProperty("user_context");
    const list = (await call(protocol, "list_activities", {})).structuredContent;
    expect(list.activities).toHaveLength(2);
    expect(list.activities.every((activity: Record<string, unknown>) => !Object.hasOwn(activity, "user_context"))).toBe(true);
    expect(contextReads).toEqual([]);
    expect(writes).toBe(1);
  });

  for (const tool of ["strength_session", "endurance_session", "coach_input"]) for (const failure of ["read", "malformed"]) {
    it(`verified current context survives an independent ${tool} ${failure} failure ${protocol}`, async () => {
      await env.SLIPSTREAM_DATA.put("summary/activities.csv",
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + "garmin-43,2026-10-08 07:00:00,Synthetic other,hiking,60,2026-10-08\n");
      const saved = (await call(protocol, "add_activity_context", { activity_id: "43", rpe: 5,
        note: "SYNTHETIC_KEEP_READABLE_CONTEXT" })).structuredContent;
      expect(saved.saved).toBe(true);
      if (failure === "read") failActivityDataRead = true;
      else {
        const key = tool === "coach_input" ? "coach-input/v1/latest-ready.json"
          : tool === "strength_session" ? "activity.v1.json" : "activity.endurance.v1.json";
        await env.SLIPSTREAM_DATA.put("activities/2026/43/" + key, "{SYNTHETIC_PRIVATE_MALFORMED_ACTIVITY");
      }
      const reply = await call(protocol, tool, { activity_id: "43" });
      expect(reply.isError).not.toBe(true);
      const result = reply.structuredContent;
      expect(result.available).toBe(false);
      expect(result.user_context).toMatchObject({ activity_id: "43", rpe: 5, note: "SYNTHETIC_KEEP_READABLE_CONTEXT" });
      expect(result.view).toMatchObject({ context_status: "available", context_revision: saved.context_id });
      expect(result).not.toHaveProperty("analysis");
      expect(result).not.toHaveProperty("session");
      expect(result).not.toHaveProperty("presentation");
      expect(result.message).not.toMatch(/not stored|No granular|No normalized|No running/);
      expect(JSON.stringify(reply)).not.toContain("SYNTHETIC_PRIVATE");
      expect(writes).toBe(1);
    });
  }

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
      coach_input: { available: false, user_context: { note: "SYNTHETIC_SAVED_NOTE" },
        view: { context_status: "available", context_revision: saved.structuredContent.context_id } } });
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
