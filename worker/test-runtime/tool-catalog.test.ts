import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import worker from "../src/index";
import { CfWorkerJsonSchemaValidator } from "@modelcontextprotocol/server/validators/cf-worker";
import workflows from "../../docs/AI_WORKFLOWS.md?raw";
import prompts from "../../docs/PROMPTS.md?raw";
import { canonical, checkDocumentationNames, documentationExamples, renderCatalog, requireTool,
  type CatalogMode, type ToolContract } from "../test-support/tool-catalog";
import { confirmedWrite, initialWorkflowCalls, nextFreshStep, WORKFLOW_HEADINGS,
  type Workflow } from "../test-support/ai-workflow-contracts";
import { syncStatus } from "../src/sync-status";
import { buildWeightDay } from "../src/weight-history";

const issuer = "https://synthetic-tool-catalog.cloudflareaccess.com";
const hostname = "tool-catalog.example";
let privateKey: CryptoKey;
let fetchSpy: ReturnType<typeof vi.spyOn>;
const modes: CatalogMode[] = [];
const validator = new CfWorkerJsonSchemaValidator();

async function rpc(method: string, params: unknown, writes: boolean, tokenConfigured = true, repoConfigured = true, limited = false) {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "catalog-key" }).setIssuer(issuer)
    .setAudience("synthetic-catalog").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const ctx = createExecutionContext();
  // Listing/invalid-input checks must never touch fitness storage or dispatch.
  const noStorage = new Proxy(env.SLIPSTREAM_DATA, { get() { throw new Error("Catalog touched R2"); } });
  const noCoordinator = new Proxy(env.REFRESH_COORDINATOR, { get() { throw new Error("Catalog touched coordinator"); } });
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  }), { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-catalog", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: String(writes), GITHUB_REPOSITORY: repoConfigured ? "synthetic/example" : "",
    GITHUB_ACTIONS_TOKEN: tokenConfigured ? "synthetic-catalog-only" : "",
    SLIPSTREAM_DATA: noStorage, REFRESH_COORDINATOR: noCoordinator,
    MCP_RATE_LIMITER: { limit: async () => ({ success: !limited }) } }, ctx);
  const body = await response.text();
  await waitOnExecutionContext(ctx);
  if (limited) {
    expect(response.status).toBe(429);
    return { httpStatus: response.status, retryAfter: response.headers.get("retry-after"), body,
      contentType: response.headers.get("content-type"), cacheControl: response.headers.get("cache-control"),
      noSniff: response.headers.get("x-content-type-options"), referrerPolicy: response.headers.get("referrer-policy") };
  }
  expect(response.status, body).toBe(200);
  return response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(body.split("\n").find(line => line.startsWith("data: "))!.slice(6)) : JSON.parse(body);
}

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "catalog-key", alg: "RS256", use: "sig" };
  fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected catalog network request");
    return Response.json({ keys: [jwk] });
  });
  for (const writes of [false, true]) for (const refresh of [false, true]) {
    const message = await rpc("tools/list", {}, writes, refresh, refresh);
    expect(message.error).toBeUndefined();
    modes.push({ writes, refresh, tools: message.result.tools });
  }
});
afterAll(() => {
  expect(fetchSpy).toHaveBeenCalledTimes(1);
  vi.restoreAllMocks();
});

it.each([false, true])("preserves the complete tool contracts with writes=%s", async writes => {
  const tools = modes.find(mode => mode.writes === writes && mode.refresh)!.tools;
  expect(tools.some((tool: { name: string }) => tool.name === "add_coach_profile")).toBe(writes);
  expect(tools.some((tool: { name: string }) => tool.name === "add_activity_context")).toBe(writes);
  // Includes order, descriptions, titles, input/output schemas and safety
  // annotations, including D4's scalar metric schema, with no fitness data.
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(tools)));
  const hash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
  expect({ writes, count: tools.length, hash }).toMatchSnapshot();
});

it("advertises the D4 scalar metric contract in every registration mode", () => {
  for (const mode of modes) {
    const schema = requireTool(mode.tools, "strength_session").outputSchema as {
      properties: { session: { properties: { sets: { items: {
        properties: { performance_metrics: Record<string, unknown> };
      } } } } };
    };
    expect(schema.properties.session.properties.sets.items.properties.performance_metrics).toEqual({
      type: "object", propertyNames: { type: "string" },
      additionalProperties: { type: ["number", "string", "boolean", "null"] },
    });
  }
});

// Historical guards normalize only this reviewed additive input/description.
// Full current snapshots and the explicit metadata test retain the new contract.
function beforeExplicitInspection(tool: ToolContract): ToolContract {
  if (tool.name !== "refresh_status") return tool;
  const previous = structuredClone(tool);
  delete (previous.inputSchema.properties as Record<string, unknown>).terminal_inspection;
  previous.description = "Wait briefly for a general refresh, activity import or night sync without starting a new job. Run-only manual night checks validate the night report and canonical sleep/HRV. Read sync_status for job state, source-check time, completeness, freshness, missing components and required user action; use data_state=ready for readiness. Legacy run-only data_ready is workflow success. Null latency is unknown. Pass request_id from a targeted activity/night request (preferred), or the returned run_id. Targeted jobs permit three short polling windows in total. Stop polling when should_continue_polling is false, even if terminal is false; report the state and retry guidance. Ordinary status checks start no Garmin job.";
  return previous;
}

it("advertises the explicit terminal inspection input only in refresh registration modes", () => {
  for (const mode of modes) {
    const tool = mode.tools.find(item => item.name === "refresh_status");
    if (!mode.refresh) { expect(tool).toBeUndefined(); continue; }
    const schema = tool!.inputSchema;
    expect((schema.properties as Record<string, unknown>).terminal_inspection).toEqual({ type: "boolean",
      description: "Only for an explicitly requested later inspection of the same request_id after its three automatic windows. One opportunity per job, spent even on unknown/error; never enable for automatic polling." });
    expect(schema.required ?? []).not.toContain("terminal_inspection");
    const validate = validator.getValidator(schema);
    expect(validate({}).valid).toBe(true);
    expect(validate({ terminal_inspection: false }).valid).toBe(true);
    expect(validate({ request_id: "11111111-1111-4111-8111-111111111111", terminal_inspection: true }).valid).toBe(true);
    expect(validate({ terminal_inspection: "true" }).valid).toBe(false);
    expect(tool!.description).toContain("at most one additional inspection after those three windows");
    expect(tool!.description).toContain("Never set it automatically");
    expect(tool!.description).toContain("Unknown or failed inspection consumes that opportunity");
    expect(tool!.description).toContain("does not prove data readiness");
  }
});

it.each([false, true])("preserves catalog bytes apart from D4 metric leaves with writes=%s", async writes => {
  const tools = structuredClone(modes.find(mode => mode.writes === writes && mode.refresh)!.tools).map(beforeExplicitInspection);
  const schema = requireTool(tools, "strength_session").outputSchema as {
    properties: { session: { properties: { sets: { items: { properties: Record<string, unknown> } } } } };
  };
  schema.properties.session.properties.sets.items.properties.performance_metrics = {};
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(tools)));
  const hash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
  expect(hash).toBe(writes ? "9f7e7b632e8e66689d53556face47c1462807b67cf22b79eda596e5b19a7c3ed"
    : "4b5b79ece31af172cecbae456f4b4b2916cd525686eddfb905912aece445085e");
});

it.each([false, true])("preserves previous contracts apart from reviewed optional and D1 detail outputs with writes=%s", async writes => {
  const additions = new Set(["job_start_confirmed", "current_job_source_checked",
    "current_job_source_checked_at", "stored_source_checked_at", "source_freshness_reason", "date_source", "dispatch_status"]);
  function previousSchema(value: unknown): unknown {
    if (Array.isArray(value)) return value.map(previousSchema);
    if (!value || typeof value !== "object") return value;
    const schema = value as Record<string, unknown>;
    return Object.fromEntries(Object.entries(schema).map(([key, child]) => {
      if (key !== "properties" || !child || typeof child !== "object") return [key, previousSchema(child)];
      const properties = Object.entries(child);
      const required = Array.isArray(schema.required) ? schema.required : [];
      for (const [name] of properties) if (additions.has(name)) expect(required).not.toContain(name);
      return [key, Object.fromEntries(properties.filter(([name]) => !additions.has(name))
        .map(([name, item]) => [name, previousSchema(item)]))];
    }));
  }
  const tools = modes.find(mode => mode.writes === writes && mode.refresh)!.tools.map(beforeExplicitInspection)
    .filter(tool => tool.name !== "training_history") // Worker 07's one additive read tool.
    .map(tool => {
      // Only these two reviewed detail outputs/descriptions change in Worker 06.
      const descriptions: Record<string, string> = {
        strength_session: "Read normalized sets for one Garmin strength activity: exercise, reps, weight, active time and following rest. Raw FIT messages and GPS are not returned.",
        endurance_session: "Read a GPS-free analysis dataset derived from the Garmin TCX file for one endurance activity. Returns summary metrics, Garmin laps, kilometre splits, distance-half heart-rate drift, seconds per heart-rate BPM, and a compact 10-second trackpoint series.",
      };
      const outputSchema = structuredClone(tool.outputSchema);
      // Reconstruct only D1's reviewed old output fields for the historical hash.
      // Full current hashes above and authenticated D1 tests cover the new shapes.
      const detail = outputSchema!.properties as Record<string, Record<string, unknown>>;
      if (tool.name === "hrv_curve") {
        for (const field of ["date", "sleep_start_gmt", "sleep_end_gmt", "summary"]) detail[field] = {};
        const reading = detail.readings.items as { properties: Record<string, unknown> };
        reading.properties.timestamp = {}; reading.properties.hrv_ms = {};
      }
      if (tool.name === "sleep_detail") {
        const summary = detail.summary.properties as Record<string, unknown>;
        for (const field of Object.keys(summary)) summary[field] = { type: ["number", "null"] };
        const score = detail.score_breakdown.additionalProperties as { properties: Record<string, unknown> };
        score.properties.value = { type: ["number", "null"] };
      }
      if (tool.name === "strength_session" || tool.name === "endurance_session") {
        detail.session = { type: "object", propertyNames: { type: "string" }, additionalProperties: {} };
      }
      if (tool.name in descriptions) {
        const properties = outputSchema!.properties as Record<string, unknown>;
        expect(properties.training_context).toBeDefined();
        expect(outputSchema!.required).not.toContain("training_context");
        delete properties.training_context;
        return { ...tool, description: descriptions[tool.name], outputSchema: previousSchema(outputSchema) };
      }
      return { ...tool, outputSchema: previousSchema(outputSchema) };
    });
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(tools)));
  const hash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
  expect(hash).toBe(writes ? "98af2252c0b4126f3118d8869b32186f2acad77b06e747075a4c51a5cc506508"
    : "af7b4408ac88a0eda1af5d9c7079a73766323331bdb3377d4f8100782855acbf");
});

it("matches the deterministic catalog from all actual registration modes", async () => {
  expect(modes.map(mode => mode.tools.length)).toEqual([20, 24, 22, 26]);
  const rendered = renderCatalog(modes);
  expect(renderCatalog([...modes].reverse().map(mode => ({ ...mode, tools: [...mode.tools].reverse() })))).toBe(rendered);
  await expect(rendered).toMatchFileSnapshot("../../docs/TOOL_CATALOG.md");
});

it.each([[true, false], [false, true]])("requires both refresh settings: token=%s repository=%s", async (token, repo) => {
  const message = await rpc("tools/list", {}, false, token, repo);
  expect(message.result.tools).toEqual(modes.find(mode => !mode.writes && !mode.refresh)!.tools);
});

it("validates all copyable workflow examples against registered input schemas", () => {
  const tools = modes.find(mode => mode.writes && mode.refresh)!.tools;
  checkDocumentationNames(workflows, tools);
  checkDocumentationNames(prompts, tools);
  expect(() => checkDocumentationNames(prompts.replace("`sync_latest_night`", "`sync_latest_sleep`"), tools))
    .toThrow("Unknown documented MCP name");
  const examples = documentationExamples(workflows);
  expect(examples.length).toBeGreaterThan(15);
  for (const example of examples) {
    const tool = requireTool(tools, example.tool);
    const result = validator.getValidator(tool.inputSchema)(example.arguments);
    expect(result.valid, `${example.tool}: ${result.errorMessage}`).toBe(true);
  }
  for (const name of ["sync_latest_sleep", "get_latest_activity", "weight_trend", "unknown_tool"]) {
    expect(() => requireTool(tools, name)).toThrow("Unregistered MCP tool");
  }
  const stale = documentationExamples(workflows.replace('"tool":"weight_history"', '"tool":"weight_trend"'));
  expect(() => stale.forEach(example => requireTool(tools, example.tool))).toThrow("weight_trend");
  expect(() => renderCatalog([{ writes: true, refresh: true, tools: [tools[0], tools[0]] }])).toThrow("Duplicate");
  const changed = { ...modes[0], tools: [{ ...modes[0].tools[0], description: "stale contract" }] };
  expect(() => renderCatalog([changed, ...modes.slice(1)])).toThrow("Contract differs");
  expect(canonical({ b: 1, a: 2 })).toEqual({ a: 2, b: 1 });
});

it.each([
  ["list_activities", { limit: 201 }],
  ["activity_stats", { group_by: "week" }],
  ["add_activity_context", { activity_id: "garmin-1001", rpe: 11 }],
  ["add_coach_profile", { effective_from: "2026-09-20", zones: [], references: {} }],
  ["refresh_status", { request_id: "not-a-uuid" }],
  ["hrv_history", { start_date: "2026-09-20", end_date: "2026-09-20", granularity: "hourly" }],
])("rejects invalid documented input for %s", (name, args) => {
  const tools = modes.find(mode => mode.writes && mode.refresh)!.tools;
  expect(validator.getValidator(requireTool(tools, name as string).inputSchema)(args).valid).toBe(false);
});

it("retains the server refinement requiring nonempty user context without R2 access", async () => {
  const message = await rpc("tools/call", { name: "add_activity_context", arguments: { activity_id: "garmin-1001" } }, true);
  expect(message.result?.isError ?? Boolean(message.error)).toBe(true);
});

it.each(["sync_latest_night", "add_activity_context"])("returns an actionable 429 before executing %s", async name => {
  const response = await rpc("tools/call", { name, arguments: {} }, true, true, true, true);
  expect(response).toEqual({ httpStatus: 429, retryAfter: "60",
    body: "Too many requests. Retry after 60 seconds.", contentType: "text/plain;charset=UTF-8",
    cacheControl: "no-store", noSniff: "nosniff", referrerPolicy: "no-referrer" });
});

it.each(["add_coach_profile", "add_activity_context"])("cannot call write-disabled tool %s", async name => {
  const message = await rpc("tools/call", { name, arguments: {} }, false);
  expect(message.error?.code).toBe(-32602);
});

it("checks all selectable workflow paths and availability without an AI service", () => {
  const expected: Record<Workflow, string[]> = {
    coach_setup: ["coach_profile"], latest_activity: ["list_activities"],
    last_night: ["sleep_detail", "hrv_curve"],
    weekly_review: ["activity_stats", "sleep_history", "hrv_history"],
    long_term_sleep_hrv: ["sleep_history", "hrv_history"], morning_weight: ["weight_history"],
  };
  for (const mode of modes) for (const workflow of Object.keys(expected) as Workflow[]) {
    expect(workflows).toContain(WORKFLOW_HEADINGS[workflow]);
    const calls = initialWorkflowCalls(workflow, mode.tools);
    expect(calls.map(call => call.tool)).toEqual(expected[workflow]);
    for (const call of calls) {
      const tool = requireTool(mode.tools, call.tool);
      expect(tool.annotations?.readOnlyHint).toBe(true);
      expect(validator.getValidator(tool.inputSchema)(call.arguments).valid).toBe(true);
    }
    if (workflow === "latest_activity" || workflow === "last_night") {
      if (!mode.refresh) expect(() => initialWorkflowCalls(workflow, mode.tools, true)).toThrow("Unregistered");
      else expect(initialWorkflowCalls(workflow, mode.tools, true)[0].tool)
        .toBe(workflow === "latest_activity" ? "sync_latest_activity" : "sync_latest_night");
    }
  }
  const writes = documentationExamples(workflows).filter(call => ["add_activity_context", "add_coach_profile"].includes(call.tool));
  for (const mode of modes) for (const call of writes) {
    expect(confirmedWrite(mode.tools, call, false)).toBeNull();
    if (mode.writes) expect(confirmedWrite(mode.tools, call, true)).toEqual(call);
    else expect(() => confirmedWrite(mode.tools, call, true)).toThrow("Unregistered");
  }
});

it("distinguishes ready R2, incomplete packages, empty source, missing profile and stopped polling", () => {
  const requestId = "00000000-0000-4000-8000-000000000001";
  const base = { status: "ready", complete: true, source_fresh: true,
    source_checked_at: "2026-09-20T08:00:00Z", missing_components: [], package: { activity_id: "garmin-1001" } };
  const successfulRun = { id: 100, status: "completed", conclusion: "success", event: "workflow_dispatch",
    created_at: "2026-09-20T08:00:00Z", updated_at: "2026-09-20T08:01:00Z",
    html_url: "https://example.invalid/synthetic-run/100" };
  const ready = syncStatus({ kind: "activity", snapshot: base });
  expect(ready).toMatchObject({ job_state: "not_started", data_state: "ready" });
  const next = (status: ReturnType<typeof syncStatus>, polling = false, targetMatches = true, httpStatus = 200) =>
    nextFreshStep({ status, shouldContinuePolling: polling, requestId, targetMatches, httpStatus });
  expect(next(ready)).toBe("analyze"); // Stored fresh package; no sync/status call.
  expect(next(ready, false, false)).toBe("select_target");
  expect(next(ready, true, true, 429)).toBe("stop_and_report");
  for (const [kind, missing] of [["activity", "coach_input"], ["night", "hrv_readings"]] as const) {
    const status = syncStatus({ kind, run: successfulRun,
      snapshot: { ...base, complete: false, missing_components: [missing] } });
    expect(status).toMatchObject({ job_state: "completed", job_conclusion: "success", data_state: "partial" });
    expect(next(status)).toBe("stop_and_report");
    expect(next(status, true)).toEqual({ tool: "refresh_status", arguments: { request_id: requestId } });
  }
  const empty = syncStatus({ kind: "activity", snapshot: { ...base, status: "no_new_activity", complete: false,
    source_outcome: "not_ready", missing_components: ["matching_local_activity"], package: {} } });
  expect(empty.data_state).toBe("source_pending");
  expect(next(empty)).toBe("stop_and_report");
  const blocked = syncStatus({ kind: "activity", snapshot: { ...base, complete: false,
    package: { coach_status: "no_effective_profile" }, missing_components: ["coach_input"] } });
  expect(blocked).toMatchObject({ data_state: "blocked", user_action_required: true });
  expect(next(blocked, true)).toBe("stop_and_report");
  const unknown = syncStatus({ kind: "night" });
  expect(unknown).toMatchObject({ data_state: "unknown", source_checked: null });
  expect(next(unknown)).toBe("stop_and_report");
});

it("does not substitute a daily mean or an evening weighing for a missing morning", () => {
  const day = buildWeightDay("2026-09-20", { date: "2026-09-20", measurements: [
    { weight_kg: 80, is_daily_average: true, timestamp_local: "2026-09-20T07:00:00" },
    { weight_kg: 81, is_daily_average: false, timestamp_local: "2026-09-20T18:00:00", timestamp_gmt: "2026-09-20T16:00:00Z" },
  ] }, "Europe/Oslo", 240, 720);
  expect(day).toMatchObject({ status: "no_morning_measurement", selected: null,
    actual_weight_count: 1, daily_average_count: 1 });
});
