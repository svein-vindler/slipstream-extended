import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject, evictDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { syntheticStartupReceipt } from "../test-support/dispatch-receipt";
import { clearSummaryCaches } from "../src/r2-storage";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";

const NOW = Date.parse("2026-10-14T12:00:00Z"), DAY = "2026-10-14";
const issuer = "https://synthetic-dispatch.cloudflareaccess.com", hostname = "dispatch.example";
let privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env, actor: string;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let provider: typeof fetch;
let startupOnAck: boolean;
let beforeControlGet: ((key: string) => Promise<void>) | undefined;
let counts: { github_post: number; github_get: number; r2_get: number; r2_control_get: number; r2_head: number; r2_list: number; r2_put: number; r2_body_bytes: number };
beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true }); privateKey = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "dispatch", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  for (const prefix of ["refresh/", "health/", "activities/", "coach/"]) {
    const listed = await env.SLIPSTREAM_DATA.list({ prefix, limit: 1000 });
    if (listed.objects.length) await env.SLIPSTREAM_DATA.delete(listed.objects.map(item => item.key));
  }
  clearSummaryCaches(); actor = crypto.randomUUID();
  beforeControlGet = undefined;
  startupOnAck = true;
  counts = { github_post: 0, github_get: 0, r2_get: 0, r2_control_get: 0, r2_head: 0, r2_list: 0, r2_put: 0, r2_body_bytes: 0 };
  coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const namespace = new Proxy(env.REFRESH_COORDINATOR, { get(target, property) {
    if (property === "getByName") return () => coordinator;
    return Reflect.get(target, property);
  } });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, { get(target, property) {
    const method = Reflect.get(target, property);
    if (typeof method !== "function") return method;
    return async (...params: unknown[]) => {
      if (property === "get") {
        counts.r2_get++;
        if (/^refresh\/(requests|reports|diagnostics)\//.test(String(params[0]))) counts.r2_control_get++;
        if (beforeControlGet) await beforeControlGet(String(params[0]));
      }
      if (property === "head") counts.r2_head++;
      if (property === "list") counts.r2_list++;
      if (property === "put") counts.r2_put++;
      const result = await Reflect.apply(method, target, params);
      if (property === "get" && result) counts.r2_body_bytes += result.size;
      return result;
    };
  } });
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "dispatch", MCP_HOSTNAME: hostname,
    GITHUB_ACTIONS_TOKEN: "synthetic-offline-token", GITHUB_REPOSITORY: "synthetic/slipstream",
    GITHUB_REFRESH_WORKFLOW: "refresh.yml", GITHUB_REFRESH_REF: "main", HEALTH_TIMEZONE: "Europe/Oslo",
    SLIPSTREAM_DATA: bucket, REFRESH_COORDINATOR: namespace,
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } };
  provider = async () => { throw new Error("Unconfigured synthetic dispatch response"); };
  vi.spyOn(Date, "now").mockReturnValue(NOW);
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === issuer + "/cdn-cgi/access/certs") return Response.json({ keys: [jwk] });
    if (!url.startsWith("https://api.github.com/repos/synthetic/slipstream/"))
      throw new Error("External network forbidden in synthetic dispatch tests");
    if (init?.method === "POST") counts.github_post++; else counts.github_get++;
    const response = await provider(input, init);
    if (startupOnAck && init?.method === "POST" && response.ok && response.status !== 204) {
      const ack = await response.clone().json().catch(() => null);
      const id = JSON.parse(String(init.body)).inputs.sync_request_id;
      if (id && Number.isSafeInteger(ack?.workflow_run_id)
        && !await env.SLIPSTREAM_DATA.get("refresh/requests/" + id + ".json"))
        await writeStartup(id, ack.workflow_run_id);
    }
    return response;
  });
});
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

async function writeStartup(id: string, runId: number) {
  const operation = (await coordinator.dispatchOperation(id))!;
  await env.SLIPSTREAM_DATA.put("refresh/requests/" + id + ".json", JSON.stringify(syntheticStartupReceipt(operation, runId)));
}

it.each(["2025-03-26", "2026-07-28"])("startup polling: waits for delayed real-format receipt within three windows through %s", async protocol => {
  // Synthetic startup receipt publication is delayed until 31 seconds.
  // Immediate follow-ups must preserve the three observation windows.
  // Only time and the outer GitHub/storage boundaries are controlled here.
  vi.mocked(Date.now).mockRestore();
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  vi.setSystemTime(NOW);
  const timers = vi.spyOn(globalThis, "setTimeout");
  startupOnAck = false;
  provider = async (_input, init) => init?.method === "POST"
    ? Response.json({ workflow_run_id: 55 })
    : Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
      path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW + 43_000).toISOString(),
      html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
  const first = await call("sync_latest_night", { wake_date: DAY }, protocol);
  expect(first).toMatchObject({ should_continue_polling: true, data_ready: false });

  let settled = false;
  const secondPending = call("refresh_status", { request_id: first.request_id }, protocol)
    .then(value => { settled = true; return value; });
  await vi.waitFor(() => expect(settled || timers.mock.calls.some(([, delay]) =>
    Number(delay) > 10_000 && Number(delay) <= 25_000)).toBe(true));
  if (!settled) await vi.advanceTimersByTimeAsync(25_000);
  const second = await secondPending;
  expect(second).toMatchObject({ should_continue_polling: true, data_ready: false,
    dispatch_status: { state: "candidate", duplicate_guard_held: true } });
  expect(Date.now()).toBeGreaterThanOrEqual(NOW + 25_000);
  expect(counts.github_get).toBe(0);
  expect(await coordinator.freshJob(first.request_id)).toMatchObject({ polls: 2 });

  timers.mockClear(); settled = false;
  const thirdPending = call("refresh_status", { request_id: first.request_id }, protocol)
    .then(value => { settled = true; return value; });
  await vi.waitFor(() => expect(settled || timers.mock.calls.some(([, delay]) =>
    Number(delay) > 10_000 && Number(delay) <= 25_000)).toBe(true));
  await vi.advanceTimersByTimeAsync(6_000);
  // Independent literal matches the actual Python receipt format, not a
  // receipt computed from the coordinator's private implementation.
  await env.SLIPSTREAM_DATA.put("refresh/requests/" + first.request_id + ".json", JSON.stringify({
    schema_version: 1, kind: "refresh-dispatch-receipt", request_id: first.request_id, run_id: 55,
    source: { repository: "synthetic/slipstream", workflow_path: ".github/workflows/refresh.yml",
      event: "workflow_dispatch", ref: "refs/heads/main", source_sha: "a".repeat(40) },
    request: { mode: "night", scope: `night/${DAY}`, date: DAY, activity_id: null,
      new_expected: false, repair_only: false, include_granular: false, components: ["sleep", "hrv"] },
  }));
  await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1,
    kind: "latest-night", wake_date: DAY, scope: `night/${DAY}`,
    checked_at: new Date(NOW + 43_000).toISOString(), source_checked: false,
    status: "pending", sleep_status: "not_checked", hrv_status: "not_checked" }));
  await vi.advanceTimersByTimeAsync(19_000);
  const third = await thirdPending;
  expect(third).toMatchObject({ terminal: true, should_continue_polling: false, data_ready: false,
    sync_status: { job_state: "completed", job_start_confirmed: true, current_job_source_checked: false },
    dispatch_status: { state: "verified_terminal", duplicate_guard_held: false } });
  expect(Date.now()).toBeGreaterThanOrEqual(NOW + 50_000);
  expect(await coordinator.freshJob(first.request_id)).toMatchObject({ polls: 3, state: "completed" });
  expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(1);
});

for (const kind of ["night", "activity"] as const) {
it.each(["2025-03-26", "2026-07-28"])(`keeps a newly acknowledged targeted job pollable while its ${kind} startup receipt is delayed through %s`, async protocol => {
  // GitHub accepts the dispatch before the independent workflow writes its receipt.
  startupOnAck = false;
  provider = async (_input, init) => {
    if (init?.method === "POST") return Response.json({ workflow_run_id: 55 });
    throw new Error("A run must not be trusted before its matching startup receipt");
  };
  const pending = await call(kind === "night" ? "sync_latest_night" : "sync_latest_activity", kind === "night"
    ? { wake_date: DAY } : { activity_id: "73", expected_date: DAY, new_activity_expected: false }, protocol);
  expect(pending).toMatchObject({ accepted: true, terminal: false, data_ready: false,
    should_continue_polling: true, dispatch_status: { state: "candidate", duplicate_guard_held: true },
    sync_status: { job_start_confirmed: null, current_job_source_checked: null } });
  expect(counts.github_post).toBe(1);
  expect(counts.github_get).toBe(0);
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ state: "active", polls: 1 });
  await writeStartup(pending.request_id, 55);
  let finished = false;
  provider = async () => Response.json({ id: 55, status: finished ? "completed" : "queued",
    conclusion: finished ? "success" : null, event: "workflow_dispatch",
    path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
    head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
    created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
    html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
  vi.mocked(Date.now).mockReturnValue(NOW + 25_000);
  const queued = await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(queued).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: true,
    dispatch_status: { state: "verified_active", duplicate_guard_held: true },
    sync_status: { job_state: "queued", job_start_confirmed: true, current_job_source_checked: null } });
  finished = true;
  await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify(kind === "night" ? { schema_version: 1, kind: "latest-night",
    wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: true,
    status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" }
    : { schema_version: 1, kind: "latest-activity", source_checked: true, scope: `activity/${DAY}/73/known`,
      checked_at: new Date(NOW).toISOString(), status: "no_recent_activity", activity_id: null, activity_date: null,
      activity_started_at_utc: null, activity_started_at_garmin_local: null, expected_date: DAY,
      already_in_slipstream: false, activity_name: null, summary_updated: false,
      files_ready: false, file_status: "not_checked", coach_status: "not_checked" }));
  vi.mocked(Date.now).mockReturnValue(NOW + 50_000);
  const terminal = await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(terminal).toMatchObject({ terminal: true, data_ready: false, should_continue_polling: false,
    dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
    sync_status: { job_state: "completed", job_conclusion: "success", current_job_source_checked: true } });
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ state: "completed", polls: 3 });
  expect(counts.github_post).toBe(1);
});
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
it("requires an explicit later terminal inspection after three ordinary checks without claiming ready data through " + protocol, async ({ task }) => {
  startupOnAck = false;
  provider = async (_input, init) => {
    if (init?.method === "POST") return Response.json({ workflow_run_id: 55 });
    return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
      path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
      html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
  };
  const pending = await call("sync_latest_night", { wake_date: DAY }, protocol);
  vi.mocked(Date.now).mockReturnValue(NOW + 25_000);
  await call("refresh_status", { request_id: pending.request_id }, protocol);
  vi.mocked(Date.now).mockReturnValue(NOW + 50_000);
  const exhausted = await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(exhausted).toMatchObject({ terminal: false, should_continue_polling: false, data_ready: false,
    dispatch_status: { duplicate_guard_held: true } });
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ state: "active", polls: 3 });
  await writeStartup(pending.request_id, 55);
  // A successful job may end without having confirmed the requested source/data.
  await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-night",
    wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: false,
    status: "pending", sleep_status: "not_checked", hrv_status: "not_checked" }));
  vi.mocked(Date.now).mockReturnValue(NOW + 3 * 60 * 60_000);
  const beforeDefault = { ...counts };
  const defaultStatus = await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(defaultStatus).toMatchObject({ terminal: false, should_continue_polling: false, data_ready: false,
    dispatch_status: { duplicate_guard_held: true } });
  expect(counts.github_get).toBe(beforeDefault.github_get);
  expect(counts.r2_control_get).toBe(beforeDefault.r2_control_get);
  const beforeExplicit = { ...counts };
  const terminal = await call("refresh_status", { request_id: pending.request_id, terminal_inspection: true }, protocol);
  task.meta.late_terminal = { before_default: beforeDefault, before_explicit: beforeExplicit,
    after_explicit: { ...counts }, result: terminal };
  expect(terminal).toMatchObject({ terminal: true, data_ready: false, should_continue_polling: false,
    dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
    sync_status: { job_state: "completed", job_conclusion: "success", current_job_source_checked: false,
      complete: false, fresh: false } });
  expect(terminal.sync_status.data_state).not.toBe("ready");
  expect(terminal.latency.request_to_ready_observed_ms).toBeNull();
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ state: "completed", polls: 3 });
  expect(counts.github_get - beforeExplicit.github_get).toBe(1);
  expect(counts.r2_control_get - beforeExplicit.r2_control_get).toBe(3);
  expect(counts.github_post).toBe(1);
  expect(counts.r2_put).toBe(0);
  const beforeRepeat = { ...counts };
  await call("refresh_status", { request_id: pending.request_id, terminal_inspection: true }, protocol);
  expect(counts.github_get).toBe(beforeRepeat.github_get);
  expect(counts.r2_control_get).toBe(beforeRepeat.r2_control_get);
});
}

it.each(["2025-03-26", "2026-07-28"])("does not refund or extend the three targeted receipt checks through %s", async protocol => {
  startupOnAck = false;
  provider = async () => Response.json({ workflow_run_id: 55 });
  const pending = await call("sync_latest_night", { wake_date: DAY }, protocol);
  vi.mocked(Date.now).mockReturnValue(NOW + 25_000);
  expect((await call("refresh_status", { request_id: pending.request_id }, protocol)).should_continue_polling).toBe(true);
  vi.mocked(Date.now).mockReturnValue(NOW + 50_000);
  const exhausted = await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(exhausted).toMatchObject({ terminal: false, should_continue_polling: false,
    dispatch_status: { duplicate_guard_held: true }, sync_status: { job_start_confirmed: null } });
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ polls: 3, state: "active" });
  const before = { ...counts };
  await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(counts.github_post).toBe(1);
  expect(counts.r2_control_get).toBe(before.r2_control_get);
  expect(counts.github_get).toBe(before.github_get);
});

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["invalid_json", "empty_object", "null", "legacy", "wrong_scope", "read_failure", "no_ack"])(
    "does not call a faulty or unacknowledged startup %s normal waiting through " + protocol, async fault => {
      startupOnAck = false;
      provider = async (_input, init) => {
        if (init?.method !== "POST") throw new Error("Unverified startup must not read a run");
        const id = JSON.parse(String(init.body)).inputs.sync_request_id;
        const receipt = syntheticStartupReceipt((await coordinator.dispatchOperation(id))!, 55);
        if (fault === "read_failure") beforeControlGet = async key => {
          if (key.startsWith("refresh/requests/")) throw new Error("Synthetic storage failure");
        };
        else if (fault !== "no_ack") await env.SLIPSTREAM_DATA.put("refresh/requests/" + id + ".json",
          fault === "invalid_json" ? "not JSON" : fault === "empty_object" ? "{}" : fault === "null" ? "null"
            : JSON.stringify(fault === "legacy" ? { run_id: 55, scope: `night/${DAY}` }
              : { ...receipt, request: { ...receipt.request, scope: "night/2026-10-13" } }));
        return fault === "no_ack" ? new Response(null, { status: 204 }) : Response.json({ workflow_run_id: 55 });
      };
      const held = await call("sync_latest_night", { wake_date: DAY }, protocol);
      expect(held).toMatchObject({ terminal: false, should_continue_polling: false, data_ready: false,
        dispatch_status: { duplicate_guard_held: true }, sync_status: { job_start_confirmed: null } });
      expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(0);
      expect(await coordinator.freshJob(held.request_id)).toMatchObject({ polls: 1, state: "active" });
    });
}

async function call(name: string, values: unknown, protocol: string, expectedError: boolean | "validation" = false) {
  const modern = protocol === "2026-07-28", authNow = Math.floor(new Date().getTime() / 1000);
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "dispatch" }).setIssuer(issuer).setAudience("dispatch")
    .setSubject(actor).setIssuedAt(authNow).setExpirationTime(authNow + 300).sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request("https://" + hostname + "/mcp", {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": protocol, ...(modern ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: values,
      ...(modern ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const body = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status).toBe(200);
  expect(response.headers.get("content-type")).toContain(modern ? "application/json" : "text/event-stream");
  const line = body.split("\n").find(item => item.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : body);
  expect(message.error).toBeUndefined();
  if (expectedError === "validation") {
    expect(message.result.isError).toBe(true);
    expect(message.result.structuredContent).toBeUndefined();
    expect(message.result.content).toEqual([{ type: "text",
      text: "The request could not be completed. Check arguments, access, configuration and service availability." }]);
    return message.result;
  }
  if (expectedError) expect(message.result.isError).toBe(true);
  else expect(message.result.isError).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  expect(body).not.toContain("SYNTHETIC_PROVIDER_TEXT");
  // Existing scenarios model a caller waiting between targeted responses.
  // The startup-polling regressions above use a separately driven fake clock
  // instead, so their immediate calls exercise the server's real wait path.
  if (typeof message.result.structuredContent?.request_id === "string" && vi.isMockFunction(Date.now))
    vi.mocked(Date.now).mockReturnValue(Date.now() + 25_000);
  return message.result.structuredContent;
}

async function spendNightWindows(protocol: string) {
  startupOnAck = false;
  provider = async (_input, init) => {
    if (init?.method === "POST") return Response.json({ workflow_run_id: 55 });
    throw new Error("Unexpected run read before the synthetic receipt");
  };
  const pending = await call("sync_latest_night", { wake_date: DAY }, protocol);
  vi.mocked(Date.now).mockReturnValue(NOW + 25_000);
  await call("refresh_status", { request_id: pending.request_id }, protocol);
  vi.mocked(Date.now).mockReturnValue(NOW + 50_000);
  await call("refresh_status", { request_id: pending.request_id }, protocol);
  expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ state: "active", polls: 3 });
  return pending.request_id as string;
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it("late-terminal negative: early explicit and later false keep automatic admission unchanged through " + protocol, async () => {
    startupOnAck = false;
    provider = async () => Response.json({ workflow_run_id: 55 });
    const pending = await call("sync_latest_night", { wake_date: DAY }, protocol);
    const beforeEarly = await coordinator.dispatchOperation(pending.request_id), earlyControl = counts.r2_control_get;
    const early = await call("refresh_status", { request_id: pending.request_id, terminal_inspection: true }, protocol);
    expect(early).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false });
    expect(await coordinator.dispatchOperation(pending.request_id)).toEqual(beforeEarly);
    expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ polls: 1, state: "active" });
    expect(counts.r2_control_get).toBe(earlyControl);
    for (const offset of [25_000, 50_000]) {
      vi.mocked(Date.now).mockReturnValue(NOW + offset);
      await call("refresh_status", { request_id: pending.request_id }, protocol);
    }
    const beforeFalse = await coordinator.dispatchOperation(pending.request_id), control = counts.r2_control_get;
    await call("refresh_status", { request_id: pending.request_id, terminal_inspection: false }, protocol);
    expect(await coordinator.dispatchOperation(pending.request_id)).toEqual(beforeFalse);
    expect(beforeFalse).not.toHaveProperty("terminal_inspection_used");
    expect(await coordinator.freshJob(pending.request_id)).toMatchObject({ polls: 3, state: "active" });
    expect(counts.r2_control_get).toBe(control);
    expect(counts.github_get).toBe(0); expect(counts.github_post).toBe(1); expect(counts.r2_put).toBe(0);
  });

  it.each(["true", 1, null])("late-terminal negative: rejects non-boolean explicit input %s through " + protocol, async value => {
    const before = { ...counts };
    await call("refresh_status", { request_id: crypto.randomUUID(), terminal_inspection: value }, protocol, "validation");
    expect(counts).toEqual(before);
  });

  it.each(["missing_request", "unknown_request", "run_alias_only", "wrong_run"])(
    "late-terminal negative: rejects explicit %s without discovery or spending the known job through " + protocol, async fault => {
      const id = await spendNightWindows(protocol);
      const before = await coordinator.dispatchOperation(id), jobBefore = await coordinator.freshJob(id), countsBefore = { ...counts };
      const params = fault === "missing_request" ? {} : fault === "unknown_request" ? { request_id: crypto.randomUUID() }
        : fault === "run_alias_only" ? { run_id: 55 } : { request_id: id, run_id: 66 };
      const rejected = await call("refresh_status", { ...params, terminal_inspection: true }, protocol);
      expect(rejected).toMatchObject({ available: false, terminal: false, data_ready: false, should_continue_polling: false });
      expect(counts).toEqual(countsBefore);
      expect(await coordinator.dispatchOperation(id)).toEqual(before);
      expect(await coordinator.freshJob(id)).toEqual(jobBefore);
    });

  it.each([false, true])("late-terminal negative: never upgrades or inspects legacy adopted=%s through " + protocol, async adopted => {
    const begun = await coordinator.beginFresh(`night/${DAY}`, "night", JSON.stringify({ kind: "night", date: DAY }), NOW);
    const id = begun.job!.request_id;
    await coordinator.bindFreshRun(id, 55);
    for (let i = 0; i < 3; i++) await coordinator.takeFreshPoll(id);
    if (adopted) await coordinator.adoptFreshDispatch(id); // Fixture for an already-adopted old record.
    const before = await coordinator.dispatchOperation(id), jobBefore = await coordinator.freshJob(id), countsBefore = { ...counts };
    const rejected = await call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
    expect(rejected).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false });
    expect(counts).toEqual(countsBefore);
    expect(await coordinator.dispatchOperation(id)).toEqual(before);
    expect(await coordinator.freshJob(id)).toEqual(jobBefore);
  });

  it("late-terminal negative: one concurrent unknown inspection survives eviction without actor or alias refund through " + protocol, async ({ task }) => {
    const id = await spendNightWindows(protocol);
    await writeStartup(id, 55);
    vi.mocked(Date.now).mockReturnValue(NOW + 3 * 60 * 60_000);
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; }), started = new Promise<void>(resolve => { entered = resolve; });
    beforeControlGet = async key => { if (key === `refresh/requests/${id}.json`) { entered(); await held; } };
    provider = async () => { throw new Error("SYNTHETIC_PROVIDER_TEXT"); };
    const before = { ...counts };
    const first = call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
    await started;
    try {
      await evictDurableObject(coordinator);
      const second = await call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
      expect(second).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false });
    } finally { release(); }
    const unknown = await first;
    beforeControlGet = undefined;
    expect(unknown).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false,
      dispatch_status: { duplicate_guard_held: true }, sync_status: { current_job_source_checked: null } });
    expect(await coordinator.dispatchOperation(id)).toMatchObject({ terminal_inspection_used: true, read_claim: null, owner_released_at: null });
    expect(await coordinator.freshJob(id)).toMatchObject({ state: "active", polls: 3 });
    await evictDurableObject(coordinator);
    actor = crypto.randomUUID(); vi.mocked(Date.now).mockReturnValue(NOW + 86_400_000);
    const spent = { ...counts };
    await call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
    await call("refresh_status", { run_id: 55, terminal_inspection: true }, protocol);
    expect(counts.github_get).toBe(spent.github_get);
    expect(counts.r2_control_get).toBe(spent.r2_control_get);
    expect(counts.github_get - before.github_get).toBe(1);
    expect(counts.r2_control_get - before.r2_control_get).toBe(1);
    expect(counts.github_post).toBe(1); expect(counts.r2_put).toBe(0);
    task.meta.late_unknown = { before, after: { ...counts } };
  });

  it.each(["invalid_json", "wrong_scope"])("late-terminal negative: corrupt %s receipt spends one inspection but never releases the guard through " + protocol, async fault => {
    const id = await spendNightWindows(protocol), operation = (await coordinator.dispatchOperation(id))!;
    const receipt = syntheticStartupReceipt(operation, 55);
    await env.SLIPSTREAM_DATA.put(`refresh/requests/${id}.json`, fault === "invalid_json" ? "not JSON"
      : JSON.stringify({ ...receipt, request: { ...receipt.request, scope: "night/2026-10-13" } }));
    const before = { ...counts };
    const held = await call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
    expect(held).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false,
      dispatch_status: { duplicate_guard_held: true }, sync_status: { current_job_source_checked: null } });
    expect(await coordinator.dispatchOperation(id)).toMatchObject({ terminal_inspection_used: true, owner_released_at: null, read_claim: null });
    await writeStartup(id, 55); // Later good metadata cannot refund the consumed attempt.
    const after = { ...counts };
    await call("refresh_status", { request_id: id, terminal_inspection: true }, protocol);
    expect(counts.r2_control_get).toBe(after.r2_control_get);
    expect(counts.github_get).toBe(before.github_get);
    expect(counts.github_post).toBe(1); expect(counts.r2_put).toBe(0);
    expect(await coordinator.freshJob(id)).toMatchObject({ state: "active", polls: 3 });
  });
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`preserves genuine discovery HTTP failure classification in ${protocol}`, async ({ task }) => {
    provider = async () => new Response("SYNTHETIC_PROVIDER_TEXT", { status: 403 });
    const result = await call("refresh_today", {}, protocol, true);
    expect(result.sync_status).toMatchObject({ job_state: "not_started", data_state: "blocked",
      user_action_required: true, next_action: "fix_configuration" });
    expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(0);
    task.meta.envelope = { counts: structuredClone(counts), result };
  });
  it(`preserves a valid active discovery without dispatch in ${protocol}`, async ({ task }) => {
    provider = async () => Response.json({ workflow_runs: [{ id: 55, status: "in_progress", conclusion: null,
      event: "workflow_dispatch", created_at: new Date(NOW).toISOString(), updated_at: null,
      html_url: "https://github.com/synthetic/slipstream/actions/runs/55" }] });
    const result = await call("refresh_today", {}, protocol);
    expect(result).toMatchObject({ accepted: false, reason: "already_running", data_ready: false });
    expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(0);
    task.meta.envelope = { counts: structuredClone(counts), result };
  });
  for (const name of ["refresh_today", "refresh_status"]) {
    for (const fault of ["size", "read", "json"]) {
      it(`keeps failed GET ${fault} unknown without dispatch in ${name} ${protocol}`, async ({ task }) => {
        provider = async () => fault === "size"
          ? new Response('{"workflow_runs":[]}'.padEnd(4_194_305), { headers: { "content-length": "1" } })
          : fault === "json" ? new Response("SYNTHETIC_PROVIDER_TEXT")
            : new Response(new ReadableStream({ start(controller) { controller.error(new Error("SYNTHETIC_PROVIDER_TEXT")); } }));
        const result = await call(name, {}, protocol, true);
        expect(result.sync_status).toMatchObject({ job_state: name === "refresh_today" ? "not_started" : "unknown",
          data_state: "unknown", user_action_required: false, next_action: "retry_later" });
        expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(0);
        expect(await coordinator.activeDispatch("general-refresh")).toBeNull();
        task.meta.d2 = { counts: structuredClone(counts), fault,
          delivered_body_bytes: fault === "size" ? 4_194_305 : fault === "json" ? 23 : 0, result };
      });
    }
    it(`keeps unverified discovery unknown without dispatch in ${name} ${protocol}`, async ({ task }) => {
      provider = async () => Response.json({ SYNTHETIC_PROVIDER_TEXT: "not a run-list envelope" });
      const result = await call(name, {}, protocol, true);
      const state = await runInDurableObject(coordinator, (_instance, storage) => ({
        operations: storage.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM dispatch_operations_v1").toArray()[0].n,
        budgets: storage.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM daily_budgets").toArray()[0].n,
        preflight: storage.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM refresh_leases WHERE lease_key = 'refresh-today'").toArray()[0].n,
      }));
      task.meta.envelope = { counts: structuredClone(counts), state, result };
      expect(result.sync_status).toMatchObject({
        job_state: name === "refresh_today" ? "not_started" : "unknown",
        job_conclusion: null, job_start_confirmed: null, data_state: "unknown",
        source_checked: null, user_action_required: false, next_action: "retry_later",
      });
      expect(result).toMatchObject({ data_ready: false, terminal: true, should_continue_polling: false });
      expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(0);
      expect(state).toEqual({ operations: 0, budgets: 0, preflight: 0 });
    });
  }
}

it.each(["2025-03-26", "2026-07-28"])("persists targeted ACK before a GET422 and returns honest held status in %s", async protocol => {
  provider = async (_input, init) => init?.method === "POST"
    ? Response.json({ workflow_run_id: 55 }) : new Response("SYNTHETIC_PROVIDER_TEXT", { status: 422 });
  const result = await call("sync_latest_night", { wake_date: DAY }, protocol);
  expect(result).toMatchObject({ accepted: true, terminal: false, should_continue_polling: false,
    dispatch_status: { state: "candidate", candidate_present: true, duplicate_guard_held: true },
    sync_status: { job_state: "unknown", job_start_confirmed: null } });
  expect(await coordinator.dispatchForRun(55)).toMatchObject({ operation_id: result.request_id, candidate_run_id: 55, owner_released_at: null });
  const budget = await runInDurableObject(coordinator, (_instance, state) => state.storage.sql.exec<{ used: number }>(
    "SELECT used FROM daily_budgets WHERE budget_key = 'sync-latest-night'").toArray());
  expect(budget).toEqual([{ used: 1 }]); expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(1);
});

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`does not rebase a stale receipt GET across a direct POST revision through ${protocol}`, async () => {
    let releasePost!: () => void, enteredPost!: () => void, releaseRead!: () => void, enteredRead!: () => void;
    const postHeld = new Promise<void>(resolve => { releasePost = resolve; });
    const postStarted = new Promise<void>(resolve => { enteredPost = resolve; });
    const readHeld = new Promise<void>(resolve => { releaseRead = resolve; });
    const readStarted = new Promise<void>(resolve => { enteredRead = resolve; });
    provider = async (_input, init) => {
      if (init?.method === "POST") {
        const inputs = JSON.parse(String(init.body)).inputs;
        await writeStartup(inputs.sync_request_id, 55);
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-night",
          wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: true,
          status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" }));
        enteredPost(); await postHeld; return Response.json({ workflow_run_id: 55 });
      }
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const a = call("sync_latest_night", { wake_date: DAY }, protocol); await postStarted;
    const job = (await coordinator.activeFreshJob(`night/${DAY}`, NOW))!;
    let held = false;
    beforeControlGet = async key => { if (!held && key.startsWith("refresh/requests/")) {
      held = true; enteredRead(); await readHeld;
    } };
    const b = call("refresh_status", { request_id: job.request_id }, protocol);
    try {
      expect(await Promise.race([readStarted.then(() => true), b.then(() => false)])).toBe(true);
      releasePost();
      expect((await a).dispatch_status.state).toBe("candidate");
    } finally { releasePost(); releaseRead(); }
    const late = await b;
    expect(late).toMatchObject({ terminal: false, dispatch_status: { state: "candidate", duplicate_guard_held: true },
      sync_status: { job_state: "unknown", job_start_confirmed: null } });
    expect(counts.github_get).toBe(0); expect(counts.github_post).toBe(1);
    expect(await coordinator.dispatchOperation(job.request_id)).toMatchObject({ candidate_run_id: 55,
      verified_binding: null, read_claim: null, owner_released_at: null });
  });

  it(`binds only the exact requested activity/date/new-expected contract through ${protocol}`, async () => {
    provider = async (_input, init) => {
      if (init?.method === "POST") {
        const inputs = JSON.parse(String(init.body)).inputs;
        expect(inputs).toMatchObject({ latest_activity_only: true, latest_activity_id: "73",
          latest_expected_date: DAY, latest_new_activity_expected: false });
        const scope = `activity/${DAY}/73/known`;
        await writeStartup(inputs.sync_request_id, 55);
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-activity",
          source_checked: true, scope, checked_at: new Date(NOW).toISOString(), status: "no_recent_activity",
          activity_id: null, activity_date: null, activity_started_at_utc: null, activity_started_at_garmin_local: null,
          expected_date: DAY, already_in_slipstream: false, activity_name: null, summary_updated: false,
          files_ready: false, file_status: "not_checked", coach_status: "not_checked" }));
        return Response.json({ workflow_run_id: 55 });
      }
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const result = await call("sync_latest_activity", { activity_id: "73", expected_date: DAY, new_activity_expected: false }, protocol);
    expect(result).toMatchObject({ terminal: true, data_ready: false,
      dispatch_status: { state: "verified_terminal" }, sync_status: { kind: "activity", current_job_source_checked: true } });
    expect((await coordinator.dispatchForRun(55))!.verified_binding!.coverage).toMatchObject({ mode: "activity",
      scope: `activity/${DAY}/73/known`, date: DAY, activity_id: "73", new_expected: false });
  });

  it(`does not forget a failed current source check and upgrade old canonical bytes on cached terminal status through ${protocol}`, async () => {
    provider = async (_input, init) => {
      if (init?.method === "POST") {
        const inputs = JSON.parse(String(init.body)).inputs;
        await writeStartup(inputs.sync_request_id, 55);
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-night",
          wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: false,
          status: "pending", sleep_status: "import_error", hrv_status: "not_checked" }));
        await env.SLIPSTREAM_DATA.put(`refresh/checks/v1/night/${DAY}.json`, JSON.stringify({ scope: `night/${DAY}`,
          source_checked: true, checked_at: new Date(NOW - 30_000).toISOString(), status: "stored" }));
        await env.SLIPSTREAM_DATA.put(`health/sleep/v1/2026/10/${DAY}.json`, JSON.stringify({ schema_version: 1, date: DAY, confirmed: true,
          sleep_start_gmt: "2026-10-13T21:00:00Z", sleep_end_gmt: "2026-10-14T04:00:00Z",
          sleep_start_garmin_local: "2026-10-13T23:00:00", sleep_end_garmin_local: "2026-10-14T06:00:00",
          summary: { sleep_seconds: 25200 }, stage_count: 1, stages: [{ start_gmt: "2026-10-13T21:00:00Z", end_gmt: "2026-10-14T04:00:00Z", stage: 1 }] }));
        await env.SLIPSTREAM_DATA.put(`health/hrv/2026/10/${DAY}.json`, JSON.stringify({ schema_version: 1, date: DAY,
          summary: { lastNightAvg: 51 }, readings: [{ timestamp: "2026-10-14T01:00:00Z", hrv_ms: 51 }] }));
        return Response.json({ workflow_run_id: 55 });
      }
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const first = await call("sync_latest_night", { wake_date: DAY }, protocol);
    expect(first).toMatchObject({ data_ready: false, sync_status: { complete: true, fresh: false, current_job_source_checked: false } });
    const control = counts.r2_control_get;
    const cached = await call("refresh_status", { request_id: first.request_id }, protocol);
    expect(cached).toMatchObject({ terminal: true, data_ready: false, sync_status: {
      job_state: "completed", complete: true, fresh: null, data_state: "unknown", current_job_source_checked: null } });
    expect(counts.r2_control_get).toBe(control);
  });

  it.each(["id", "repository", "head_repository", "workflow", "event", "ref", "sha", "missing_source", "scope", "kind", "receipt"])(
    `holds missing or contradictory %s evidence without claiming verified binding through ${protocol}`, async defect => {
      provider = async (_input, init) => {
        if (init?.method === "POST") {
          const inputs = JSON.parse(String(init.body)).inputs;
          await writeStartup(inputs.sync_request_id, defect === "receipt" ? 66 : 55);
          await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1,
            kind: defect === "kind" ? "latest-activity" : "latest-night", wake_date: DAY,
            scope: defect === "scope" ? "night/2026-10-13" : `night/${DAY}`,
            checked_at: new Date(NOW).toISOString(), source_checked: true,
            status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" }));
          return Response.json({ workflow_run_id: 55 });
        }
        return Response.json({ id: defect === "id" ? 66 : 55, status: "completed", conclusion: "success",
          event: defect === "event" ? "schedule" : "workflow_dispatch",
          path: defect === "workflow" ? ".github/workflows/other.yml" : ".github/workflows/refresh.yml",
          repository: { full_name: defect === "repository" ? "synthetic/other" : "synthetic/slipstream" },
          head_repository: { full_name: defect === "head_repository" ? "synthetic/fork" : "synthetic/slipstream" },
          head_branch: defect === "ref" ? "other" : "main", head_sha: defect === "missing_source" ? undefined : defect === "sha" ? "unverified" : "a".repeat(40),
          created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
          html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
      };
      const result = await call("sync_latest_night", { wake_date: DAY }, protocol);
      expect(result).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false,
        dispatch_status: { state: "candidate", duplicate_guard_held: true }, sync_status: { job_state: "unknown", job_start_confirmed: null } });
      expect(await coordinator.dispatchForRun(55)).toMatchObject({ verified_binding: null, owner_released_at: null });
      expect(counts.github_post).toBe(1); expect(counts.r2_put).toBe(0);
    });

  it.each(["ack", "post422"])(`keeps general %s while a parallel actual status call has no candidate through ${protocol}`, async outcome => {
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    provider = async (input, init) => {
      if (init?.method === "POST") {
        entered(); await held;
        return outcome === "ack" ? Response.json({ workflow_run_id: 55 }) : new Response("SYNTHETIC_PROVIDER_TEXT", { status: 422 });
      }
      return String(input).includes("/workflows/") ? Response.json({ workflow_runs: [] })
        : new Response("SYNTHETIC_PROVIDER_TEXT", { status: 422 });
    };
    const pending = call("refresh_today", {}, protocol); await started;
    const op = (await coordinator.activeDispatch("general-refresh"))!;
    try {
      const waiting = await call("refresh_status", { request_id: op.operation_id }, protocol);
      expect(waiting).toMatchObject({ terminal: false, should_continue_polling: false,
        dispatch_status: { state: "unconfirmed", candidate_present: false } });
      expect(counts.github_get).toBe(1); // Only the original admission precheck.
    } finally { release(); }
    const result = await pending;
    expect(result.dispatch_status.state).toBe(outcome === "ack" ? "candidate" : "post_rejected");
    expect(result.dispatch_status.duplicate_guard_held).toBe(outcome === "ack");
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(outcome === "ack" ? 2 : 1);
  });

  it(`holds an overlapping general request before another POST while a targeted operation is unverified through ${protocol}`, async () => {
    provider = async (input, init) => {
      if (init?.method === "POST") throw new Error("SYNTHETIC_PROVIDER_TEXT lost acknowledgement");
      if (String(input).includes("/workflows/")) return Response.json({ workflow_runs: [] });
      return new Response(null, { status: 404 });
    };
    const first = await call("sync_latest_night", { wake_date: DAY }, protocol);
    const before = counts.github_get;
    const general = await call("refresh_today", {}, protocol);
    expect(general).toMatchObject({ accepted: false, terminal: false, should_continue_polling: false,
      dispatch_status: { operation_id: first.request_id, duplicate_guard_held: true } });
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(before);
  });

  it(`holds targeted admission before POST or write debit while general204 owns its guard through ${protocol}`, async () => {
    provider = async (_input, init) => init?.method === "POST" ? new Response(null, { status: 204 })
      : Response.json({ workflow_runs: [] });
    const general = await call("refresh_today", {}, protocol);
    const before = counts.github_get;
    const targeted = await call("sync_latest_night", { wake_date: DAY }, protocol);
    expect(targeted).toMatchObject({ accepted: false, terminal: false, should_continue_polling: false,
      dispatch_status: { operation_id: general.dispatch_status.operation_id, duplicate_guard_held: true } });
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(before);
    expect(await coordinator.activeFreshJob(`night/${DAY}`, NOW)).toBeNull();
  });

  it(`binds completed general mode from existing diagnostics/report without claiming a complete fresh package through ${protocol}`, async () => {
    provider = async (input, init) => {
      if (init?.method === "POST") {
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1,
          updated_at: new Date(NOW).toISOString(), new_activity_count: 0, checked_activity_count: 0,
          remaining_candidate_count: 0, activities: [] }));
        await env.SLIPSTREAM_DATA.put("refresh/diagnostics/v1/55.json", JSON.stringify({ schema_version: 1,
          kind: "refresh-diagnostics", mode: "general", status: "complete", started_at: new Date(NOW).toISOString(),
          finished_at: new Date(NOW).toISOString(), elapsed_ms: 1, login_ms: null, source_checks: [],
          stages: ["refresh_summaries", "recent_health", "recent_body", "manual_activity"].map(stage => ({
            stage, status: "complete", elapsed_ms: 0, garmin_fetch_ms: null, r2_read_ms: null, r2_write_ms: null,
            activity_file_import_ms: null, coach_input_ms: null, garmin_connectapi_calls: null,
            garmin_connectapi_errors: null, r2_sdk_operations: {} })) }));
        return Response.json({ workflow_run_id: 55 });
      }
      if (String(input).includes("/workflows/")) return Response.json({ workflow_runs: [] });
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const result = await call("refresh_today", {}, protocol);
    expect(result).toMatchObject({ accepted: true, terminal: true, data_ready: true,
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
      sync_status: { kind: "general", job_state: "completed", data_state: "unknown", complete: null, fresh: null } });
    const control = counts.r2_control_get;
    const cached = await call("refresh_status", { run_id: 55 }, protocol);
    expect(cached).toMatchObject({ terminal: true, data_ready: true,
      dispatch_status: { operation_id: result.dispatch_status.operation_id, state: "verified_terminal" },
      sync_status: { job_state: "completed", current_job_source_checked: null, complete: null, fresh: null } });
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(2);
    expect(counts.r2_control_get).toBe(control);
  });

  it(`adopts one active legacy row without resetting polls or inventing descriptor evidence through ${protocol}`, async () => {
    const begun = await coordinator.beginFresh(`night/${DAY}`, "night", JSON.stringify({ kind: "night", date: DAY }), NOW - 3 * 60 * 60_000);
    const id = begun.job!.request_id;
    await coordinator.bindFreshRun(id, 55);
    await coordinator.takeFreshPoll(id); await coordinator.takeFreshPoll(id);
    await evictDurableObject(coordinator);
    provider = async () => new Response("SYNTHETIC_PROVIDER_TEXT", { status: 404 });
    const result = await call("refresh_status", { run_id: 55 }, protocol);
    expect(result).toMatchObject({ request_id: id, terminal: false, should_continue_polling: false,
      dispatch_status: { state: "unconfirmed", candidate_present: true, duplicate_guard_held: true },
      sync_status: { job_state: "unknown" } });
    expect(await coordinator.dispatchOperation(id)).toMatchObject({ descriptor: null, candidate_run_id: 55, owner_released_at: null });
    expect(await coordinator.freshJob(id)).toMatchObject({ polls: 2, state: "active", run_id: 55 });
    const beforeControl = counts.r2_control_get;
    await call("refresh_status", { request_id: id }, protocol);
    vi.spyOn(Date, "now").mockReturnValue(NOW + 86_400_000);
    await call("sync_latest_night", { wake_date: DAY }, protocol);
    expect(counts.r2_control_get).toBe(beforeControl);
    expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(0);
    expect(await coordinator.freshJob(id)).toMatchObject({ polls: 2, state: "active" });
  });

  it(`uses a late exact receipt within one expired status ticket and keeps terminal aliases read-only through ${protocol}`, async () => {
    provider = async (_input, init) => {
      if (init?.method === "POST") throw new Error("SYNTHETIC_PROVIDER_TEXT lost acknowledgement");
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const first = await call("sync_latest_night", { wake_date: DAY }, protocol);
    expect(first.dispatch_status.state).toBe("unconfirmed");
    await writeStartup(first.request_id, 55);
    await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-night",
      wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: true,
      status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" }));
    vi.spyOn(Date, "now").mockReturnValue(NOW + 3 * 60 * 60_000);
    const late = await call("refresh_status", { request_id: first.request_id }, protocol);
    expect(late).toMatchObject({ terminal: true, data_ready: false,
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false } });
    const cached = await call("refresh_status", { run_id: 55 }, protocol);
    expect(cached).toMatchObject({ terminal: true, should_continue_polling: false,
      dispatch_status: { operation_id: first.request_id, state: "verified_terminal" },
      sync_status: { job_state: "completed", current_job_source_checked: null } });
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(1);
    expect(await coordinator.freshJob(first.request_id)).toMatchObject({ state: "completed", polls: 2, run_id: 55 });
  });

  it(`keeps general204 without correlation held after the short lease and never attaches latest through ${protocol}`, async () => {
    let posted = false;
    provider = async (_input, init) => {
      if (init?.method === "POST") { posted = true; return new Response(null, { status: 204 }); }
      return Response.json({ workflow_runs: posted ? [{ id: 999, status: "completed", conclusion: "success",
        event: "workflow_dispatch", created_at: new Date(NOW).toISOString(), updated_at: null,
        html_url: "https://github.com/synthetic/slipstream/actions/runs/999" }] : [] });
    };
    const first = await call("refresh_today", {}, protocol);
    expect(first).toMatchObject({ accepted: true, terminal: false, should_continue_polling: false,
      dispatch_status: { state: "unconfirmed", candidate_present: false, duplicate_guard_held: true },
      sync_status: { job_state: "unknown", job_start_confirmed: null } });
    expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(1);
    vi.spyOn(Date, "now").mockReturnValue(NOW + 180_000);
    const late = await call("refresh_today", {}, protocol);
    expect(late.dispatch_status.operation_id).toBe(first.dispatch_status.operation_id);
    const inspected = await call("refresh_status", { request_id: first.dispatch_status.operation_id }, protocol);
    expect(inspected.dispatch_status.duplicate_guard_held).toBe(true);
    expect(counts.github_get).toBe(1); expect(counts.github_post).toBe(1);
    expect(counts.r2_put).toBe(0);
  });

  it.each(["ack", "post422"])(`keeps direct %s after parallel actual MCP status revision changes through ${protocol}`, async outcome => {
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    provider = async (_input, init) => {
      if (init?.method !== "POST") return new Response("SYNTHETIC_PROVIDER_TEXT", { status: 422 });
      entered(); await held;
      return outcome === "ack" ? Response.json({ workflow_run_id: 55 }) : new Response("SYNTHETIC_PROVIDER_TEXT", { status: 422 });
    };
    const pending = call("sync_latest_night", { wake_date: DAY }, protocol);
    await started;
    const job = (await coordinator.activeFreshJob(`night/${DAY}`, NOW))!;
    let during, postReplyAt = NOW;
    try {
      const status = await call("refresh_status", { request_id: job.request_id }, protocol);
      expect(status.sync_status.job_state).toBe("unknown");
      during = await coordinator.dispatchOperation(job.request_id);
      expect(during!.revision).toBeGreaterThan(0);
    } finally { postReplyAt = Date.now(); release(); }
    const completed = await pending;
    const saved = await coordinator.dispatchOperation(job.request_id);
    expect(saved!.revision).toBeGreaterThan(during!.revision);
    expect(saved).toMatchObject(outcome === "ack" ? { dispatch_state: "candidate", candidate_run_id: 55, owner_released_at: null }
      : { dispatch_state: "post_rejected", owner_released_at: postReplyAt });
    expect(completed.dispatch_status.state).toBe(outcome === "ack" ? "candidate" : "post_rejected");
    expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(outcome === "ack" ? 1 : 0);
  });

  it(`binds a completed night only with actual-format run metadata, exact receipt and scoped report through ${protocol}`, async () => {
    let clock = NOW;
    vi.spyOn(Date, "now").mockImplementation(() => clock++);
    provider = async (_input, init) => {
      if (init?.method === "POST") {
        const inputs = JSON.parse(String(init.body)).inputs;
        await writeStartup(inputs.sync_request_id, 55);
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1, kind: "latest-night",
          wake_date: DAY, scope: `night/${DAY}`, checked_at: new Date(NOW).toISOString(), source_checked: true,
          status: "pending", sleep_status: "garmin_not_ready", hrv_status: "garmin_not_ready" }));
        return Response.json({ workflow_run_id: 55 });
      }
      return Response.json({ id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: new Date(NOW).toISOString(),
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
    };
    const result = await call("sync_latest_night", { wake_date: DAY }, protocol);
    expect(result).toMatchObject({ terminal: true, data_ready: false, dispatch_status: {
      state: "verified_terminal", duplicate_guard_held: false }, sync_status: {
      job_state: "completed", current_job_source_checked: true, complete: false } });
    expect(await coordinator.dispatchForRun(55)).toMatchObject({ verified_binding: { source_sha: "a".repeat(40) }, owner_released_at: expect.any(Number) });
  });
}

// Drive actual authenticated MCP + coordinator through all three ordinary polls.
async function expiredActiveActivity(protocol: string, kind: "activity" | "night" = "activity") {
  const state = { terminal: false, outcome: "success", sha: "a".repeat(40), status: 200, posts: 0,
    beforeRun: undefined as (() => Promise<void>) | undefined };
  provider = async (input, init) => {
    if (init?.method === "POST") return Response.json({ workflow_run_id: ++state.posts === 1 ? 55 : 66 });
    const runId = Number(String(input).split("/").at(-1));
    expect([55, 66]).toContain(runId);
    if (runId === 55 && state.beforeRun) await state.beforeRun();
    if (state.status !== 200) return new Response("SYNTHETIC_PROVIDER_TEXT", { status: state.status });
    return Response.json({ id: runId, status: runId === 55 && state.terminal ? "completed" : "in_progress",
      conclusion: runId === 55 && state.terminal ? state.outcome : null, event: "workflow_dispatch",
      path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: state.sha,
      created_at: new Date(NOW).toISOString(), updated_at: new Date(Date.now()).toISOString(),
      html_url: `https://github.com/synthetic/slipstream/actions/runs/${runId}` });
  };
  const first = await call(kind === "night" ? "sync_latest_night" : "sync_latest_activity",
    kind === "night" ? { wake_date: DAY } : { expected_date: DAY, new_activity_expected: true }, protocol);
  await call("refresh_status", { request_id: first.request_id }, protocol);
  await call("refresh_status", { request_id: first.request_id }, protocol);
  expect(await coordinator.freshJob(first.request_id)).toMatchObject({ polls: 3, state: "active" });
  expect(await coordinator.dispatchOperation(first.request_id)).toMatchObject({ dispatch_state: "verified_active" });
  vi.mocked(Date.now).mockReturnValue(NOW + 3 * 86_400_000);
  return { state, id: first.request_id as string };
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each([
    ["2026-10-17", "success"], [DAY, "success"], [DAY, "failure"], [DAY, "cancelled"],
  ])(`reconciles an expired exact job before %s readmission (%s) through ${protocol}`, async (day, outcome) => {
    const { state, id } = await expiredActiveActivity(protocol);
    state.terminal = true; state.outcome = outcome;
    const before = { ...counts };
    // Missing final report: exact job termination does not attest to data readiness.
    const next = await call("sync_latest_activity", { expected_date: day, new_activity_expected: true }, protocol);
    expect(await coordinator.dispatchOperation(id)).toMatchObject({ dispatch_state: "verified_terminal",
      owner_released_at: expect.any(Number), overlap_reconcile_used: true });
    expect((await coordinator.dispatchOperation(id))!.terminal_inspection_used).toBeUndefined();
    // Normal beginFresh retention prunes completed fresh_jobs older than two days.
    expect(await coordinator.freshJob(id)).toBeNull();
    expect(next).toMatchObject({ accepted: true, data_ready: false, dispatch_status: { state: "verified_active" } });
    expect(next.request_id).not.toBe(id);
    expect(counts.github_get - before.github_get).toBe(2); // old exact run + new initial observation
    expect(counts.github_post - before.github_post).toBe(1);
    expect(counts.r2_control_get - before.r2_control_get).toBe(2); // receipts only
    expect(counts.r2_put).toBe(0);
  });

  it.each(["active", "403", "429", "wrong_sha", "missing_receipt", "changed_receipt"])(
    `retains the expired guard on %s and never refunds its overlap check through ${protocol}`, async variant => {
      const { state, id } = await expiredActiveActivity(protocol);
      state.terminal = variant !== "active";
      if (variant === "403" || variant === "429") state.status = Number(variant);
      if (variant === "wrong_sha") state.sha = "b".repeat(40);
      if (variant === "missing_receipt") await env.SLIPSTREAM_DATA.delete(`refresh/requests/${id}.json`);
      if (variant === "changed_receipt") {
        const receipt = syntheticStartupReceipt((await coordinator.dispatchOperation(id))!, 55);
        receipt.source.source_sha = "b".repeat(40);
        await env.SLIPSTREAM_DATA.put(`refresh/requests/${id}.json`, JSON.stringify(receipt));
      }
      const before = { ...counts };
      const blocked = await call("sync_latest_activity", { expected_date: "2026-10-17", new_activity_expected: true }, protocol);
      expect(blocked).toMatchObject({ reason: "dispatch_overlap", accepted: false, data_ready: false });
      expect(await coordinator.dispatchOperation(id)).toMatchObject({ owner_released_at: null, read_claim: null,
        overlap_reconcile_used: true });
      expect(counts.github_get - before.github_get).toBeLessThanOrEqual(1);
      expect(counts.r2_control_get - before.r2_control_get).toBe(1);
      expect(counts.github_post).toBe(1);
      state.status = 200; state.sha = "a".repeat(40); state.terminal = true;
      vi.mocked(Date.now).mockReturnValue(NOW + 4 * 86_400_000);
      const spent = { ...counts }; actor = crypto.randomUUID();
      await call("sync_latest_activity", { expected_date: "2026-10-18", new_activity_expected: true }, protocol);
      expect(counts.github_get).toBe(spent.github_get);
      expect(counts.github_post).toBe(spent.github_post);
      expect(counts.r2_control_get).toBe(spent.r2_control_get);
    });

  it(`admits one old exact GET across concurrent distinct requests through ${protocol}`, async () => {
    const { state, id } = await expiredActiveActivity(protocol); state.terminal = true;
    let entered!: () => void, release!: () => void;
    const started = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    state.beforeRun = async () => { entered(); await held; };
    const before = { ...counts };
    const pending = call("sync_latest_activity", { expected_date: "2026-10-17", new_activity_expected: true }, protocol);
    await started;
    try {
      const blocked = await call("sync_latest_activity", { expected_date: "2026-10-16", new_activity_expected: true }, protocol);
      expect(blocked.reason).toBe("dispatch_overlap"); expect(counts.github_post).toBe(1);
    } finally { release(); }
    const next = await pending;
    expect(next.accepted).toBe(true);
    expect(counts.github_get - before.github_get).toBe(2);
    expect(counts.github_post - before.github_post).toBe(1);
    expect((await coordinator.dispatchOperation(id))!.read_claim).toBeNull();
  });
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["stale", "fresh"])(`separates exact terminal ownership from a %s complete package through ${protocol}`, async freshness => {
    const { state, id } = await expiredActiveActivity(protocol);
    const clock = NOW + 2 * 60 * 60_000 + 1;
    vi.mocked(Date.now).mockReturnValue(clock); state.terminal = true;
    const prefix = "activities/2026/73";
    await env.SLIPSTREAM_DATA.put(`${prefix}/activity.v1.json`, JSON.stringify({ schema_version: 1, source: "garmin-fit",
      activity: { id: "73", type: "strength_training", name: "Synthetic", start_time_local: DAY + "T12:00:00",
        start_time_gmt: DAY + "T10:00:00Z" }, normalized_strength_session: { sets: [] } }));
    await env.SLIPSTREAM_DATA.put(`${prefix}/activity.fit`, "synthetic-not-a-real-fit");
    await env.SLIPSTREAM_DATA.put(`refresh/checks/v1/activity/${DAY}/latest/new.json`, JSON.stringify({
      scope: `activity/${DAY}/latest/new`, source_checked: true, status: "ready",
      checked_at: new Date(freshness === "fresh" ? clock : NOW).toISOString() }));
    const next = await call("sync_latest_activity", { expected_date: "2026-10-15", new_activity_expected: true }, protocol);
    expect(next.accepted).toBe(true);
    const before = { ...counts };
    const old = await call("refresh_status", { request_id: id }, protocol);
    expect(old).toMatchObject({ terminal: true, data_ready: false, freshness: { complete: true,
      source_fresh: freshness === "fresh" }, sync_status: { job_state: "completed", data_state: "unknown" },
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false } });
    expect(counts.github_get).toBe(before.github_get);
    expect(counts.r2_control_get).toBe(before.r2_control_get);
  });
}

it.each(["2025-03-26", "2026-07-28"])("uses the same single bounded reconciliation for night requests through %s", async protocol => {
  const { state, id } = await expiredActiveActivity(protocol, "night"); state.terminal = true;
  const before = { ...counts };
  const next = await call("sync_latest_night", { wake_date: "2026-10-17" }, protocol);
  expect(next).toMatchObject({ accepted: true, data_ready: false });
  expect(await coordinator.dispatchOperation(id)).toMatchObject({ dispatch_state: "verified_terminal",
    overlap_reconcile_used: true, owner_released_at: expect.any(Number) });
  expect(counts.github_get - before.github_get).toBe(2);
  expect(counts.github_post - before.github_post).toBe(1);
  expect(counts.r2_control_get - before.r2_control_get).toBe(2);
});

// Round02: actual repair producer and advancing admission clock, no row repair/migration.
function provider02() {
  const state = { terminal: false, posts: 0 };
  provider = async (input, init) => {
    if (init?.method === "POST") return Response.json({ workflow_run_id: ++state.posts === 1 ? 55 : 66 });
    const id = Number(String(input).split("/").at(-1)); expect([55, 66]).toContain(id);
    return Response.json({ id, status: id === 55 && state.terminal ? "completed" : "in_progress",
      conclusion: id === 55 && state.terminal ? "success" : null, event: "workflow_dispatch",
      path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: new Date(Date.now()).toISOString(),
      html_url: `https://github.com/synthetic/slipstream/actions/runs/${id}` });
  };
  return state;
}
async function repairSource02() {
  const prefix = "activities/2026/73";
  await env.SLIPSTREAM_DATA.put(`${prefix}/activity.v1.json`, JSON.stringify({ schema_version: 1, source: "garmin-fit",
    source_fit_sha256: "synthetic-fit-sha", activity: { id: "73", type: "running", name: "Synthetic",
      start_time_local: DAY + "T12:00:00", start_time_gmt: DAY + "T10:00:00Z" }, decode_errors: [] }));
  await env.SLIPSTREAM_DATA.put(`${prefix}/activity.endurance.v1.json`, JSON.stringify({ schema_version: 1,
    available: true, activity: { id: "73" }, summary: { distance_m: 5000 }, trackpoints: [] }));
  for (const name of ["activity.fit", "activity.tcx"]) await env.SLIPSTREAM_DATA.put(`${prefix}/${name}`, "synthetic");
  await env.SLIPSTREAM_DATA.put("coach/profiles/v1/2026-01-01/hr-" + "a".repeat(24) + ".json",
    JSON.stringify({ profile_id: "synthetic-profile", effective_from: "2026-01-01",
      zones: [{ label: "Z1", min_bpm: 100, max_bpm: 150 }] }));
  const scope = `activity/${DAY}/latest/new`;
  await env.SLIPSTREAM_DATA.put(`refresh/checks/v1/${scope}.json`, JSON.stringify({ scope, source_checked: true,
    checked_at: new Date(NOW).toISOString(), status: "ready", activity_id: "garmin-73" }));
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each([DAY, "2026-10-17"])(`reconciles actual resolved repair before target %s through ${protocol}`, async day => {
    const state = provider02(); await repairSource02();
    const first = await call("sync_latest_activity", { expected_date: DAY, new_activity_expected: true }, protocol);
    const op = (await coordinator.dispatchOperation(first.request_id))!;
    const job = (await coordinator.freshJob(first.request_id))!;
    expect(op).toMatchObject({ origin: "targeted_repair", descriptor: { activity_id: "73", date: DAY,
      scope: `activity/${DAY}/73/new`, repair_only: true } });
    expect(job.scope).toBe(`activity/${DAY}/latest/new`);
    expect(JSON.parse(job.request).activityId).toBeUndefined();
    await call("refresh_status", { request_id: first.request_id }, protocol);
    await call("refresh_status", { request_id: first.request_id }, protocol);
    expect(await coordinator.freshJob(first.request_id)).toMatchObject({ polls: 3, state: "active" });
    vi.mocked(Date.now).mockReturnValue(NOW + 3 * 86_400_000); state.terminal = true;
    const before = { ...counts };
    const next = await call("sync_latest_activity", { expected_date: day, new_activity_expected: true }, protocol);
    expect(next).toMatchObject({ accepted: true, data_ready: false });
    expect(next.request_id).not.toBe(first.request_id);
    expect(await coordinator.dispatchOperation(first.request_id)).toMatchObject({ dispatch_state: "verified_terminal",
      overlap_reconcile_used: true, owner_released_at: expect.any(Number) });
    expect(counts.github_get - before.github_get).toBe(2);
    expect(counts.github_post - before.github_post).toBe(1);
    expect(counts.r2_control_get - before.r2_control_get).toBe(2);
  });

  it(`captures one admission instant while the actual MCP writer clock advances through ${protocol}`, async () => {
    const state = provider02(); let ticking = NOW;
    vi.mocked(Date.now).mockImplementation(() => ticking++);
    const first = await call("sync_latest_activity", { expected_date: DAY, new_activity_expected: true }, protocol);
    const job = (await coordinator.freshJob(first.request_id))!;
    expect(job.expires_at).toBe(JSON.parse(job.request).requestedAt + 2 * 60 * 60_000);
    vi.mocked(Date.now).mockReturnValue(job.expires_at - 2 * 60 * 60_000 + 30_000);
    await call("refresh_status", { request_id: first.request_id }, protocol);
    await call("refresh_status", { request_id: first.request_id }, protocol);
    expect(await coordinator.freshJob(first.request_id)).toMatchObject({ polls: 3, state: "active" });
    vi.mocked(Date.now).mockReturnValue(NOW + 3 * 86_400_000); state.terminal = true;
    const before = { ...counts };
    const next = await call("sync_latest_activity", { expected_date: "2026-10-17", new_activity_expected: true }, protocol);
    expect(next.accepted).toBe(true);
    expect(await coordinator.dispatchOperation(first.request_id)).toMatchObject({ dispatch_state: "verified_terminal" });
    expect(counts.github_get - before.github_get).toBe(2);
    expect(counts.github_post - before.github_post).toBe(1);
  });
}
