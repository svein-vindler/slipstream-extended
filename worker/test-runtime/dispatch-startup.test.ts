import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject, evictDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import contract from "./generated/dispatch-contract.json";
import { clearSummaryCaches } from "../src/r2-storage";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";

const NOW = Date.parse("2026-10-14T12:00:00Z"), DAY = "2026-10-14";
const issuer = "https://synthetic-dispatch.cloudflareaccess.com", hostname = "dispatch.example";
let privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env, actor: string;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let provider: typeof fetch;
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
    return provider(input, init);
  });
});
afterEach(() => vi.restoreAllMocks());

async function call(name: string, values: unknown, protocol: string) {
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
  expect(message.error).toBeUndefined(); expect(message.result.isError).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  expect(body).not.toContain("SYNTHETIC_PROVIDER_TEXT");
  return message.result.structuredContent;
}


it.each(["2025-03-26", "2026-07-28"])("reconciles a general204 from actual startup producer bytes through %s", async protocol => {
  const id = contract.general.request_id;
  await runInDurableObject(coordinator, (_instance, state) => {
    // Native persisted start state equivalent to the one outgoing POST's owner.
    state.storage.sql.exec("INSERT INTO refresh_leases (lease_key,token,expires_at) VALUES ('refresh-today',?,?)", id, NOW + 120_000);
  });
  const operation = (await coordinator.beginGeneralDispatch(id, {
    repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main", mode: "general",
    scope: null, date: null, activity_id: null, new_expected: false, repair_only: false,
    include_granular: true, components: ["summary", "health", "body", "activity_files", "coach_input"],
  }, NOW))!;
  await coordinator.mergeDispatchPost(operation, { kind: "unconfirmed", reason: "empty_ack" }, NOW);
  await env.SLIPSTREAM_DATA.put("refresh/requests/" + id + ".json", JSON.stringify(contract.general.receipt));
  let phase = "queued";
  provider = async () => Response.json({ id: 55, status: phase, conclusion: phase === "completed" ? "success" : null, event: "workflow_dispatch",
    path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
    head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
    created_at: new Date(NOW).toISOString(), updated_at: null,
    html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
  const result = await call("refresh_status", { request_id: id }, protocol);
  expect(result).toMatchObject({ terminal: false, data_ready: false,
    dispatch_status: { state: "verified_active", candidate_present: true, duplicate_guard_held: true },
    sync_status: { job_state: "queued", job_start_confirmed: true, current_job_source_checked: null,
      complete: null, fresh: null } });
  expect(counts.github_post).toBe(0);
  expect(counts.github_get).toBe(1);
  expect(counts.r2_control_get).toBe(1);
  const before = { ...counts };
  // The other alias shares the same 25-second status window.
  expect((await call("refresh_status", { run_id: 55 }, protocol)).dispatch_status.duplicate_guard_held).toBe(true);
  expect(counts.github_get).toBe(before.github_get); expect(counts.r2_control_get).toBe(before.r2_control_get);
  phase = "in_progress"; vi.mocked(Date.now).mockReturnValue(NOW + 25_000);
  const active = await call("refresh_status", { run_id: 55 }, protocol);
  expect(active).toMatchObject({ terminal: false, data_ready: false,
    dispatch_status: { state: "verified_active" },
    sync_status: { job_state: "running", job_start_confirmed: true, current_job_source_checked: null, fresh: null } });
  await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1,
    updated_at: new Date(NOW).toISOString(), new_activity_count: 0, checked_activity_count: 0,
    remaining_candidate_count: 0, activities: [] }));
  await env.SLIPSTREAM_DATA.put("refresh/diagnostics/v1/55.json", JSON.stringify(contract.general.diagnostics));
  phase = "completed"; vi.mocked(Date.now).mockReturnValue(NOW + 50_000);
  const terminal = await call("refresh_status", { request_id: id }, protocol);
  expect(terminal).toMatchObject({ terminal: true, data_ready: true,
    dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
    sync_status: { job_state: "completed", complete: null, fresh: null, current_job_source_checked: null } });
  expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(3); expect(counts.r2_control_get).toBe(5);
  const noMore = { ...counts };
  expect((await call("refresh_status", { run_id: 55 }, protocol)).terminal).toBe(true);
  expect(counts.github_get).toBe(noMore.github_get); expect(counts.r2_control_get).toBe(noMore.r2_control_get);
});

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const kind of ["night", "activity", "repair"] as const) {
    it("uses actual " + kind + " startup/report producer bytes without fresh-data inflation through " + protocol, async () => {
      const fixture = contract[kind], night = kind === "night", repair = kind === "repair";
      const expected = { mode: night ? "night" as const : "activity" as const,
        scope: night ? "night/" + DAY : "activity/" + DAY + "/1/known",
        date: DAY, activity_id: night ? null : "1", new_expected: false, repair_only: repair,
        include_granular: false, components: night ? ["sleep", "hrv"] : ["activity_files", "coach_input", "context"] };
      expect(fixture.receipt.request).toEqual(expected);
      const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main", ...expected };
      const parameters = { kind: expected.mode, date: DAY, ...(night ? {} : { activityId: "1", newExpected: false }) };
      const begun = await runInDurableObject(coordinator, instance => {
        // Control only UUID generation during synchronous native admission.
        const uuid = vi.spyOn(crypto, "randomUUID").mockReturnValue(fixture.request_id);
        try { return instance.beginFresh(expected.scope, expected.mode, JSON.stringify(parameters), NOW, repair, descriptor); }
        finally { uuid.mockRestore(); }
      });
      expect(begun.job!.request_id).toBe(fixture.request_id);
      const operation = (await coordinator.dispatchOperation(fixture.request_id))!;
      await coordinator.mergeDispatchPost(operation, { kind: "unconfirmed", reason: "empty_ack" }, NOW);
      await env.SLIPSTREAM_DATA.put("refresh/requests/" + fixture.request_id + ".json", JSON.stringify(fixture.receipt));
      let complete = false;
      provider = async () => Response.json({ id: fixture.run_id, status: complete ? "completed" : "in_progress",
        conclusion: complete ? "success" : null, event: "workflow_dispatch", path: ".github/workflows/refresh.yml",
        repository: { full_name: "synthetic/slipstream" }, head_repository: { full_name: "synthetic/slipstream" },
        head_branch: "main", head_sha: "a".repeat(40), created_at: new Date(NOW).toISOString(), updated_at: null,
        html_url: "https://github.com/synthetic/slipstream/actions/runs/" + fixture.run_id });
      const active = await call("refresh_status", { request_id: fixture.request_id }, protocol);
      expect(active).toMatchObject({ terminal: false, data_ready: false,
        dispatch_status: { state: "verified_active", duplicate_guard_held: true },
        sync_status: { job_state: "running", job_start_confirmed: true, current_job_source_checked: null } });
      await env.SLIPSTREAM_DATA.put("refresh/reports/" + fixture.run_id + ".json", JSON.stringify(fixture.report));
      await env.SLIPSTREAM_DATA.put("refresh/diagnostics/v1/" + fixture.run_id + ".json", JSON.stringify(fixture.diagnostics));
      complete = true;
      const terminal = await call("refresh_status", { run_id: fixture.run_id }, protocol);
      expect(terminal).toMatchObject({ terminal: true, data_ready: false,
        dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
        sync_status: { job_state: "completed", current_job_source_checked: !repair } });
      expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(2); expect(counts.r2_control_get).toBe(4);
      expect(await coordinator.freshJob(fixture.request_id)).toMatchObject({ polls: 2, state: "completed" });
    });
  }
}

async function seedGeneral() {
  const id = contract.general.request_id;
  await runInDurableObject(coordinator, (_instance, state) =>
    state.storage.sql.exec("INSERT INTO refresh_leases (lease_key,token,expires_at) VALUES ('refresh-today',?,?)", id, NOW + 120_000));
  return (await coordinator.beginGeneralDispatch(id, {
    repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main", mode: "general",
    scope: null, date: null, activity_id: null, new_expected: false, repair_only: false,
    include_granular: true, components: ["summary", "health", "body", "activity_files", "coach_input"],
  }, NOW))!;
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it.each(["missing", "legacy", "unsupported", "request_id", "run_id_type", "other_run",
    "repository", "workflow", "event", "ref", "sha", "mode", "scope", "repair", "granular",
    "new_expected", "components", "bool_string", "provider_text", "invalid_json", "oversize"])(
    "holds unsupported or mismatching startup %s before false job proof through " + protocol, async defect => {
      const operation = await seedGeneral(), id = operation.operation_id;
      const receipt = structuredClone(contract.general.receipt) as Record<string, unknown> & {
        source: Record<string, unknown>; request: Record<string, unknown> };
      if (defect === "unsupported") receipt.schema_version = 2;
      if (defect === "request_id") receipt.request_id = "22345678-1234-4234-8234-123456789012";
      if (defect === "run_id_type") receipt.run_id = "55";
      if (defect === "other_run") receipt.run_id = 66;
      if (defect === "repository") receipt.source.repository = "synthetic/other";
      if (defect === "workflow") receipt.source.workflow_path = ".github/workflows/other.yml";
      if (defect === "event") receipt.source.event = "schedule";
      if (defect === "ref") receipt.source.ref = "refs/heads/other";
      if (defect === "sha") receipt.source.source_sha = "b".repeat(40);
      if (defect === "mode") receipt.request.mode = "night";
      if (defect === "scope") receipt.request.scope = "night/" + DAY;
      if (defect === "repair") receipt.request.repair_only = true;
      if (defect === "granular") receipt.request.include_granular = false;
      if (defect === "new_expected") receipt.request.new_expected = true;
      if (defect === "components") receipt.request.components = ["summary"];
      if (defect === "bool_string") receipt.request.repair_only = "false";
      if (defect === "provider_text") receipt.source.error = "SYNTHETIC_PROVIDER_TEXT";
      if (defect !== "missing") await env.SLIPSTREAM_DATA.put("refresh/requests/" + id + ".json",
        defect === "legacy" ? JSON.stringify({ run_id: 55, scope: null })
          : defect === "invalid_json" ? "SYNTHETIC_PROVIDER_TEXT"
          : defect === "oversize" ? "x".repeat(64 * 1024 + 1) : JSON.stringify(receipt));
      provider = async () => Response.json({ id: 55, status: "in_progress", conclusion: null, event: "workflow_dispatch",
        path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
        created_at: new Date(NOW).toISOString(), updated_at: null,
        html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
      const result = await call("refresh_status", { request_id: id }, protocol);
      expect(result).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false,
        dispatch_status: { duplicate_guard_held: true }, sync_status: { job_state: "unknown", job_start_confirmed: null,
          current_job_source_checked: null, complete: null, fresh: null } });
      expect(counts.github_get).toBe(["sha", "other_run"].includes(defect) ? 1 : 0);
      expect(counts.r2_control_get).toBe(1); expect(counts.github_post).toBe(0);
      expect(await coordinator.dispatchOperation(id)).toMatchObject({ owner_released_at: null, read_claim: null,
        verified_binding: null, last_observation: null });
      const before = { ...counts };
      await call("refresh_status", { request_id: id }, protocol);
      expect(counts.github_get).toBe(before.github_get); expect(counts.r2_control_get).toBe(before.r2_control_get);
    });
}

it.each(["2025-03-26", "2026-07-28"])("uses actual general dispatch input and initial204 receipt while preserving the occupied claim through %s", async protocol => {
  let entered!: () => void, release!: () => void, id = "";
  const started = new Promise<void>(resolve => { entered = resolve; });
  const held = new Promise<void>(resolve => { release = resolve; });
  provider = async (input, init) => {
    if (init?.method === "POST") {
      const inputs = JSON.parse(String(init.body)).inputs;
      id = inputs.sync_request_id;
      const operation = (await coordinator.dispatchOperation(id))!;
      expect(inputs).toEqual({ include_granular: true, sync_request_id: id, latest_new_activity_expected: false });
      expect(operation.operation_id).toBe(id);
      // Only the random operation UUID differs from the actual producer template.
      await env.SLIPSTREAM_DATA.put("refresh/requests/" + id + ".json",
        JSON.stringify({ ...contract.general.receipt, request_id: id }));
      return new Response(null, { status: 204 });
    }
    if (String(input).includes("/workflows/")) return Response.json({ workflow_runs: [] });
    const current = (await coordinator.dispatchOperation(id))!;
    expect(current).toMatchObject({ candidate_run_id: 55, startup_receipt: { request_id: id },
      read_claim: { receipt_used: true, run_gets_used: 1 }, general_initial_reads_used: 1 });
    entered(); await held;
    return Response.json({ id: 55, status: "in_progress", conclusion: null, event: "workflow_dispatch",
      path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: null, html_url: "https://github.com/synthetic/slipstream/actions/runs/55" });
  };
  const a = call("refresh_today", {}, protocol);
  await started;
  try {
    const before = { ...counts }, owner = await coordinator.dispatchOperation(id);
    const b = await call("refresh_status", { run_id: 55 }, protocol);
    expect(b).toMatchObject({ terminal: false, data_ready: false, dispatch_status: { operation_id: id, duplicate_guard_held: true } });
    expect(counts.github_get).toBe(before.github_get); expect(counts.r2_control_get).toBe(before.r2_control_get);
    expect(await coordinator.dispatchOperation(id)).toEqual(owner);
  } finally { release(); }
  expect(await a).toMatchObject({ accepted: true, terminal: false, data_ready: false,
    dispatch_status: { operation_id: id, state: "verified_active" }, sync_status: { job_state: "running", current_job_source_checked: null } });
  expect(counts.github_post).toBe(1); expect(counts.github_get).toBe(2); expect(counts.r2_control_get).toBe(1);
});

it.each(["2025-03-26", "2026-07-28"])("reads cached terminal C8 history without adopting the new receipt format through %s", async protocol => {
  const fixture = contract.night;
  const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
    mode: "night" as const, scope: "night/" + DAY, date: DAY, activity_id: null,
    new_expected: false, repair_only: false, include_granular: false, components: ["sleep", "hrv"] };
  await runInDurableObject(coordinator, instance => {
    const uuid = vi.spyOn(crypto, "randomUUID").mockReturnValue(fixture.request_id);
    try { instance.beginFresh(descriptor.scope, "night", JSON.stringify({ kind: "night", date: DAY }), NOW, false, descriptor); }
    finally { uuid.mockRestore(); }
  });
  for (const [key, value] of [["refresh/requests/" + fixture.request_id + ".json", fixture.receipt],
    ["refresh/reports/56.json", fixture.report], ["refresh/diagnostics/v1/56.json", fixture.diagnostics]] as const)
    await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
  provider = async () => Response.json({ id: 56, status: "completed", conclusion: "success", event: "workflow_dispatch",
    path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
    head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
    created_at: new Date(NOW).toISOString(), updated_at: null, html_url: "https://github.com/synthetic/slipstream/actions/runs/56" });
  expect((await call("refresh_status", { request_id: fixture.request_id }, protocol)).terminal).toBe(true);
  const raw = await runInDurableObject(coordinator, (_instance, state) => {
    const value = JSON.parse(state.storage.sql.exec<{ payload: string }>(
      "SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", fixture.request_id).toArray()[0].payload);
    delete value.startup_receipt; // Exact prior C8 serialized shape; no state transition is fabricated.
    const body = JSON.stringify(value);
    state.storage.sql.exec("UPDATE dispatch_operations_v1 SET payload = ? WHERE operation_id = ?", body, fixture.request_id);
    return body;
  });
  const before = { ...counts }, job = await coordinator.freshJob(fixture.request_id);
  const cached = await call("refresh_status", { run_id: 56 }, protocol);
  expect(cached).toMatchObject({ terminal: true, data_ready: false,
    dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
    sync_status: { kind: "night", job_state: "completed", job_start_confirmed: true, current_job_source_checked: null, fresh: null } });
  expect(counts.github_get).toBe(before.github_get); expect(counts.r2_control_get).toBe(before.r2_control_get);
  expect(await coordinator.freshJob(fixture.request_id)).toEqual(job);
  const after = await runInDurableObject(coordinator, (_instance, state) =>
    state.storage.sql.exec<{ payload: string }>("SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", fixture.request_id).toArray()[0].payload);
  expect(after).toBe(raw);
});
