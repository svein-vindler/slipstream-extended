import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it as test, vi } from "vitest";
import worker from "../src/index";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import { clearSummaryCaches } from "../src/r2-storage";

const marker = "SYNTHETIC_PRIVATE token email@example.invalid GPS=1,2 health=77 2041-02-03 key/private\n\u001b[31m" + "x".repeat(1000);
const issuer = "https://privacy-synthetic.cloudflareaccess.com";
const hostname = "privacy.example";
let jwk: Record<string, unknown>, token: string, testEnv: Env;
let pair: Awaited<ReturnType<typeof generateKeyPair>>;
let githubCalls: string[], r2Calls: string[];
let mode = "error";
let providerError: unknown;
let responseBytes = 0, elapsedMs = 0;
const logs: unknown[][] = [];
beforeAll(async () => {
  pair = await generateKeyPair("RS256", { extractable: true });
  jwk = { ...await exportJWK(pair.publicKey), kid: "privacy", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); logs.length = 0; responseBytes = 0; elapsedMs = 0; githubCalls = []; r2Calls = []; mode = "error"; providerError = new Error(marker);
  for (const method of ["error", "warn", "log", "info"] as const)
    vi.spyOn(console, method).mockImplementation((...args) => { logs.push(args); });
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = input instanceof Request ? input.url : String(input);
    if (mode === "auth" && url.endsWith("/cdn-cgi/access/certs")) throw new Error(marker);
    if (url === issuer + "/cdn-cgi/access/certs") {
      if (mode === "auth") throw new Error(marker);
      return Response.json({ keys: [jwk] });
    }
    if (!url.startsWith("https://api.github.com/repos/owner/example/")) throw new Error("Unexpected external request");
    githubCalls.push(init?.method ?? "GET");
    if (mode.startsWith("targeted") && init?.method === "POST") {
      if (mode === "targeted_rejected") return new Response(marker, { status: 403, headers: { "x-github-request-id": "SYNTHETIC_PRIVATE" } });
      throw new Error(marker);
    }
    if (mode === "github") throw new Error(marker);
    return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
      created_at: "2026-10-05T12:00:00Z", updated_at: "2026-10-05T12:02:00Z",
      html_url: "https://github.com/owner/example/actions/runs/55", path: ".github/workflows/refresh.yml" });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "privacy" }).setIssuer(issuer).setAudience("privacy")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(pair.privateKey);
  const bucket = {
    list: async () => { r2Calls.push("list"); if (mode === "provider") throw providerError; if (mode === "list") throw new Error(marker); return { objects: [], truncated: false }; },
    put: async (key: string) => { r2Calls.push("put"); if (key.startsWith("coach/indexes/")) throw new Error(marker); },
    head: async () => { r2Calls.push("head"); if (mode === "context") return { etag: "synthetic-context" }; throw new Error(marker); },
    get: async (key: string) => {
      r2Calls.push("get");
      if (mode === "provider") throw providerError;
      if (mode === "context" && key === "summary/activities.csv") {
        const body = "Activity ID,Activity Date,Activity Name,Activity Type,Source\ngarmin-123,2041-02-03,Synthetic context workout,Run,garmin";
        return { size: body.length, etag: "synthetic-context", arrayBuffer: async () => new TextEncoder().encode(body).buffer };
      }
      if (mode === "context") return null; // Synthetic index has no canonical clock yet.
      if (mode === "profile" || mode === "list" || mode.startsWith("targeted")) return null;
      if (mode === "report") {
        if (!key.startsWith("refresh/reports/")) return null;
        const body = JSON.stringify({ schema_version: 1, updated_at: "2041-02-03T12:00:00Z",
          new_activity_count: 1, checked_activity_count: 1, remaining_candidate_count: 0,
          activities: [{ activity_id: "garmin-123", name: "Authorized synthetic workout", date: "2041-02-03",
            is_new: true, files_ready: false, file_status: "error", coach_status: "error", error: marker.slice(0, 300) }] });
        return { size: body.length, arrayBuffer: async () => new TextEncoder().encode(body).buffer };
      }
      if (mode === "oversize") return { size: 100 * 1024 * 1024 };
      if (mode === "json") {
        const body = '{"private":"' + marker + '"';
        return { size: body.length, arrayBuffer: async () => new TextEncoder().encode(body).buffer };
      }
      if (mode === "body") return { size: 1, arrayBuffer: async () => { throw new Error(marker); } };
      throw new Error(marker);
    },
  } as unknown as R2Bucket;
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "privacy", MCP_HOSTNAME: hostname,
    HEALTH_TIMEZONE: "Europe/Oslo", SLIPSTREAM_DATA: bucket,
    GITHUB_ACTIONS_TOKEN: "synthetic", GITHUB_REPOSITORY: "owner/example",
    REFRESH_COORDINATOR: { getByName: () => coordinator } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });
function it(name: string, operation: () => Promise<void>) {
  test(name, async ({ annotate }) => {
    const fixtureStarted = performance.now();
    try { await operation(); }
    finally {
      elapsedMs = performance.now() - fixtureStarted;
      await annotate(JSON.stringify({ case: expect.getState().currentTestName, events: logs.length,
        log_bytes: new TextEncoder().encode(JSON.stringify(logs)).length, response_bytes: responseBytes,
        elapsed_ms: elapsedMs, github: githubCalls.length, r2: r2Calls.length, garmin: 0 }), "privacy-metric");
    }
  });
}

async function call(name: string, args: unknown, protocol = "2025-03-26") {
  const started = performance.now();
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp", {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args, ...(protocol === "2026-07-28" ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const body = await response.text(); responseBytes += new TextEncoder().encode(body).length; await waitOnExecutionContext(ctx);
  elapsedMs += performance.now() - started;
  const line = body.split("\n").find(value => value.startsWith("data: "));
  return { body, media: response.headers.get("content-type"), status: response.status, message: JSON.parse(line ? line.slice(6) : body) };
}
for (const protocol of ["2025-03-26", "2025-11-25", "2026-07-28"]) {
  for (const failure of ["error", "body", "json", "oversize"]) {
    it("sanitizes actual MCP R2 " + failure + " errors with protocol " + protocol, async () => {
      mode = failure;
      const result = await call("hrv_curve", { date: "2041-02-03" }, protocol);
      expect(result.status).toBe(200);
      expect(result.media).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
      expect(result.message.result.isError).toBe(true);
      expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
      expect(result.body).not.toContain("health/hrv/2041");
      expect(r2Calls).toEqual(["get"]); expect(githubCalls).toEqual([]);
    });
  }
  it("sanitizes SDK validation errors before JSON/SSE", async () => {
    const result = await call("hrv_curve", { date: marker }, protocol);
    expect(result.message.result.isError).toBe(true);
    expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(r2Calls).toEqual([]);
  });
}
for (const name of ["refresh_today", "refresh_status"]) {
  it("preserves general " + name + " error/status shape", async () => {
    mode = "github";
    const result = await call(name, {});
    expect(result.message.result.isError).toBe(true);
    expect(result.message.result.structuredContent).toMatchObject({
      terminal: true, data_ready: false, should_continue_polling: false,
    });
    expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(githubCalls).toEqual(["GET"]);
  });
}
it("projects only private error fields in successful authorized activity reports", async () => {
  mode = "report";
  const result = await call("refresh_status", { run_id: 55 });
  expect(result.message.result.isError).not.toBe(true);
  const activity = result.message.result.structuredContent.activity_refresh.activities[0];
  expect(activity).toMatchObject({ name: "Authorized synthetic workout", date: "2041-02-03", activity_id: "garmin-123" });
  expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
  expect(githubCalls).toEqual(["GET"]); expect(r2Calls).toEqual(["get", "get"]);
});

for (const name of ["list_activities", "health_status"]) {
  it("sanitizes the " + name + " summary log and MCP failure", async () => {
    const result = await call(name, {});
    expect(result.message.result.isError).toBe(true);
    expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(logs).toHaveLength(1); expect(r2Calls).toEqual(["head"]);
  });
}
it("sanitizes a profile-index failure while retaining the saved profile result", async () => {
  mode = "profile"; testEnv.MCP_WRITES_ENABLED = "true";
  const result = await call("add_coach_profile", { name: "Authorized synthetic profile",
    effective_from: "2041-02-03", zones: [{ label: "Easy", min_bpm: 90, max_bpm: 120 }],
    references: {} });
  expect(result.message.result.isError).not.toBe(true);
  expect(result.message.result.structuredContent).toMatchObject({ saved: true, profile: { name: "Authorized synthetic profile" } });
  expect(JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
  expect(logs).toHaveLength(1); expect(r2Calls.filter(x => x === "put")).toHaveLength(2);
});
it("sanitizes transitive R2 list failure", async () => {
  mode = "list";
  const result = await call("coach_profile", {});
  expect(result.message.result.isError).toBe(true);
  expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
});
it("keeps HTTP refusal status and removes raw Host values before connect", async () => {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp", {
    method: "POST", headers: { host: "SYNTHETIC_PRIVATE.example", "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token },
    body: '{"jsonrpc":"2.0","id":1,"method":"tools/list"}',
  }), testEnv, ctx);
  const body = await response.text(); responseBytes += new TextEncoder().encode(body).length; await waitOnExecutionContext(ctx);
  expect(response.status).toBe(403);
  expect(JSON.parse(body)).toMatchObject({ jsonrpc: "2.0", error: { code: -32000 }, id: null });
  expect(body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
});
it("keeps the rate limit HTTP body and Retry-After without reading private data", async () => {
  testEnv.MCP_RATE_LIMITER = { limit: async () => ({ success: false }) } as RateLimit;
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp",
    { headers: { "cf-access-jwt-assertion": token } }), testEnv, ctx);
  expect(response.status).toBe(429); expect(response.headers.get("retry-after")).toBe("60");
  const body = await response.text(); responseBytes += new TextEncoder().encode(body).length;
  expect(body).toBe("Too many requests. Retry after 60 seconds.");
  expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
});
it("sanitizes unexpected request-body IO errors without turning failure into success", async () => {
  const request = new Request("https://" + hostname + "/mcp", { method: "POST",
    headers: { "cf-access-jwt-assertion": token }, body: new ReadableStream({
      start(controller) { controller.error(new Error(marker)); },
    }) });
  await expect(worker.fetch(request, testEnv, createExecutionContext()))
    .rejects.toThrow("MCP request failed.");
  expect(JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
  expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]);
});


for (const outcome of ["targeted_unconfirmed", "targeted_rejected"]) {
  it("preserves targeted dispatch reservations for " + outcome, async () => {
    mode = outcome;
    const first = await call("sync_latest_night", {});
    const value = first.message.result.structuredContent;
    expect(first.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(value).toMatchObject(outcome === "targeted_unconfirmed"
      ? { accepted: true, reason: "dispatch_unconfirmed", terminal: false, should_continue_polling: false }
      : { reason: "dispatch_rejected", retry_after_seconds: 300 });
    if (outcome === "targeted_unconfirmed") {
      const again = await call("refresh_status", { request_id: value.request_id });
      expect(again.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
      expect(again.message.result.structuredContent).toMatchObject({
        request_id: value.request_id, reason: "dispatch_unconfirmed", terminal: false,
      });
    }
    expect(githubCalls).toEqual(["POST"]);
  });
}


const historyGuidance = [
  [{ start_date: "2041-02-30", end_date: "2041-03-01" }, "Invalid calendar date. Use YYYY-MM-DD with a valid calendar day."],
  [{ start_date: "2041-02-02", end_date: "2041-02-01" }, "start_date cannot be after end_date."],
  [{ start_date: "2041-01-01", end_date: "2042-01-02" }, "Health history is limited to 366 days per request."],
  [{ start_date: "2041-02-01", end_date: "2041-02-01", granularity: "weekly", detail_level: "full" }, "Full detail requires daily granularity."],
  [{ start_date: "2041-01-01", end_date: "2041-02-01", granularity: "daily" }, "Daily summaries are limited to 31 days; use weekly or auto granularity."],
] as const;
const weightGuidance = [
  [{ start_date: "2041-01-01", end_date: "2041-02-01" }, "Weight history is limited to 31 calendar days per request."],
  [{ morning_start: "12:00", morning_end: "04:00" }, "morning_start must be earlier than morning_end on the same day."],
  [{ morning_start: "04:00", morning_end: "04:00" }, "morning_start must be earlier than morning_end on the same day."],
  [{ timezone: "SYNTHETIC_PRIVATE/Invalid" }, "timezone must be a valid IANA timezone, such as Europe/Oslo."],
] as const;
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const name of ["sleep_history", "hrv_history"]) {
    for (const [args, guidance] of historyGuidance) {
      it("preserves " + name + " guidance: " + guidance + " in " + protocol, async () => {
        const result = await call(name, args, protocol);
        expect(result.status).toBe(200);
        expect(result.message.result).toEqual({ content: [{ type: "text", text: guidance }], isError: true });
        expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
      });
    }
  }
  for (const [args, guidance] of weightGuidance) {
    it("preserves weight guidance: " + JSON.stringify(args) + " in " + protocol, async () => {
      const result = await call("weight_history", { start_date: "2041-02-01", end_date: "2041-02-01", ...args }, protocol);
      expect(result.status).toBe(200);
      expect(result.message.result).toEqual({ content: [{ type: "text", text: guidance }], isError: true });
      expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
      expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
    });
  }
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it("preserves eight-day full sleep guidance in " + protocol, async () => {
    const result = await call("sleep_history", {
      start_date: "2041-02-01", end_date: "2041-02-08", detail_level: "full",
    }, protocol);
    expect(result.status).toBe(200);
    expect(result.media).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
    expect(result.message.result).toEqual({
      content: [{ type: "text", text: "Full HRV readings or sleep stages are limited to 7 days per request." }],
      isError: true,
    });
    expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
  });
}


const profileArgs = { name: "Synthetic guidance profile", effective_from: "2041-02-03",
  zones: [{ label: "Easy", min_bpm: 90, max_bpm: 120 }], references: {} };
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const limit of ["cooldown", "daily"]) {
    it("preserves coach " + limit + " retry guidance in " + protocol, async () => {
      mode = "profile"; testEnv.MCP_WRITES_ENABLED = "true";
      let now = Math.floor(Date.now() / 86_400_000) * 86_400_000 + 43_200_000;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      if (limit === "daily") testEnv.MCP_WRITE_DAILY_LIMIT = "1";
      const first = await call("add_coach_profile", profileArgs, protocol);
      expect(first.message.result.isError).not.toBe(true);
      expect(first.message.result.structuredContent).toMatchObject({ saved: true, profile: { name: profileArgs.name } });
      if (limit === "daily") now += 61_000; // Cooldown expires; same UTC day's budget remains exhausted.
      const second = await call("add_coach_profile", { ...profileArgs, name: "Another synthetic profile" }, protocol);
      expect(second.status).toBe(200);
      expect(second.message.result).toEqual({
        content: [{ type: "text", text: limit === "daily"
          ? "The daily MCP write safety limit has been reached. Retry in 43139 seconds."
          : "A similar write was accepted recently. Retry in 60 seconds." }], isError: true,
      });
      expect(r2Calls.filter(value => value === "put")).toHaveLength(2);
      expect(githubCalls).toEqual([]);
      expect(second.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    });
  }
}


const spoofedGuidance = "Full HRV readings or sleep stages are limited to 7 days per request.";
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const [label, error] of [
    ["matching text", new Error(spoofedGuidance)],
    ["forged class and fields", Object.assign(new Error("A similar write was accepted recently. Retry in 60 seconds."), {
      name: "ApplicationToolError", kind: "write_cooldown", retrySeconds: 60, private: marker,
    })],
    ["plain provider object", { name: "ApplicationToolError", kind: "timezone",
      message: "timezone must be a valid IANA timezone, such as Europe/Oslo.", stack: marker }],
    ["provider string and control characters", spoofedGuidance + marker],
  ] as const) {
    for (const [name, args] of [
      ["sleep_history", { start_date: "2041-02-01", end_date: "2041-02-01" }],
      ["weight_history", { start_date: "2041-02-01", end_date: "2041-02-01" }],
      ["add_coach_profile", profileArgs],
    ] as const) {
      it("rejects " + label + " from " + name + " provider in " + protocol, async () => {
        mode = "provider"; providerError = error; testEnv.MCP_WRITES_ENABLED = "true";
        const result = await call(name, args, protocol);
        expect(result.status).toBe(200);
        expect(result.media).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
        expect(result.message.result).toEqual({ content: [{ type: "text",
          text: "The request could not be completed. Check arguments, access, configuration and service availability." }], isError: true });
        expect(result.body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
        expect(r2Calls).not.toContain("put"); expect(githubCalls).toEqual([]);
      });
    }
  }
  for (const name of ["sleep_history", "hrv_history"]) {
    for (const [args, granularity, requestedDays] of [
      [{ start_date: "2041-02-01", end_date: "2041-02-07", detail_level: "full" }, "daily", 7],
      [{ start_date: "2041-01-01", end_date: "2041-01-31" }, "daily", 31],
      [{ start_date: "2041-01-01", end_date: "2042-01-01" }, "weekly", 366],
    ] as const) {
      it("keeps valid " + requestedDays + "-day " + name + " boundary in " + protocol, async () => {
        mode = "profile";
        const result = await call(name, args, protocol);
        expect(result.status).toBe(200);
        expect(result.message.result.isError).not.toBe(true);
        expect(result.message.result.structuredContent).toMatchObject({
          granularity, days_requested: requestedDays,
        });
        expect(r2Calls).not.toContain("put"); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
      });
    }
  }
  it("keeps the valid 31-day weight/morning/timezone boundary in " + protocol, async () => {
    mode = "profile";
    const result = await call("weight_history", {
      start_date: "2041-01-01", end_date: "2041-01-31",
      morning_start: "04:00", morning_end: "12:00", timezone: "Europe/Oslo",
    }, protocol);
    expect(result.message.result.isError).not.toBe(true);
    expect(result.message.result.structuredContent).toMatchObject({
      requested_days: 31, selected_days: 0, morning_start: "04:00", morning_end: "12:00", timezone: "Europe/Oslo",
    });
    expect(r2Calls).not.toContain("put"); expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
  });
  it("does not reuse safe guidance for a provider error with the same MCP ID in " + protocol, async () => {
    const rejected = await call("sleep_history", {
      start_date: "2041-02-01", end_date: "2041-02-08", detail_level: "full",
    }, protocol);
    mode = "provider"; providerError = new Error(spoofedGuidance);
    const failed = await call("sleep_history", { start_date: "2041-02-01", end_date: "2041-02-01" }, protocol);
    expect(rejected.message.result.content[0].text).toBe(spoofedGuidance);
    expect(failed.message.result.content[0].text).toBe(
      "The request could not be completed. Check arguments, access, configuration and service availability.");
    expect(failed.message.result.isError).toBe(true);
  });
}


for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const limit of ["cooldown", "daily"]) {
    it("preserves activity context " + limit + " retry guidance in " + protocol, async () => {
      mode = "context"; testEnv.MCP_WRITES_ENABLED = "true";
      let now = Math.floor(Date.now() / 86_400_000) * 86_400_000 + 43_200_000;
      vi.spyOn(Date, "now").mockImplementation(() => now);
      if (limit === "daily") testEnv.MCP_WRITE_DAILY_LIMIT = "1";
      const first = await call("add_activity_context", { activity_id: "garmin-123", rpe: 5 }, protocol);
      expect(first.message.result.isError).not.toBe(true);
      expect(first.message.result.structuredContent).toMatchObject({ saved: true, activity_id: "123" });
      if (limit === "daily") now += 11_000;
      const second = await call("add_activity_context", { activity_id: "garmin-123", rpe: 6 }, protocol);
      expect(second.message.result).toEqual({ content: [{ type: "text", text: limit === "daily"
        ? "The daily MCP write safety limit has been reached. Retry in 43189 seconds."
        : "A similar write was accepted recently. Retry in 10 seconds." }], isError: true });
      expect(r2Calls.filter(value => value === "put")).toHaveLength(1);
      expect(githubCalls).toEqual([]); expect(logs).toEqual([]);
    });
  }
}

it("sanitizes actual JWKS retrieval failure and retains HTTP 401", async () => {
  mode = "auth";
  const brokenIssuer = issuer.replace("privacy-synthetic", "privacy-jwks-failure");
  testEnv.ACCESS_TEAM_DOMAIN = brokenIssuer;
  token = await new SignJWT({ type: "app" }).setProtectedHeader({ alg: "RS256", kid: "privacy" })
    .setIssuer(brokenIssuer).setAudience("privacy").setSubject("synthetic-identity")
    .setIssuedAt().setExpirationTime("5m").sign(pair.privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp",
    { headers: { "cf-access-jwt-assertion": token } }), testEnv, ctx);
  expect(response.status).toBe(401);
  const body = await response.text(); responseBytes += new TextEncoder().encode(body).length;
  expect(body).toBe("Unauthorized"); expect(body + JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
  expect(logs).toHaveLength(1); expect(r2Calls).toEqual([]); expect(githubCalls).toEqual([]);
});
