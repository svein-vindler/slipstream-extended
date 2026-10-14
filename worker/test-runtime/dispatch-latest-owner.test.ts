import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { syntheticStartupReceipt } from "../test-support/dispatch-receipt";
import { outputSchemas } from "../src/mcp-output";
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


const scope = "night/" + DAY;
const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
  mode: "night" as const, scope, date: DAY, activity_id: null, new_expected: false,
  repair_only: false, include_granular: false, components: ["sleep", "hrv"] };
const RUN = { id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
  path: ".github/workflows/refresh.yml", created_at: new Date(NOW).toISOString(),
  updated_at: new Date(NOW).toISOString(), html_url: "https://github.com/synthetic/slipstream/actions/runs/55" };

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  for (const held of ["exhausted", "orphan"] as const) {
    it("keeps owned latest " + held + " behind its exact targeted claim through " + protocol, async () => {
      const begun = await coordinator.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: DAY }), NOW, false, descriptor);
      const id = begun.job!.request_id;
      const identity = { operation_id: id, owner_key: "targeted:" + scope, owner_token: id };
      await coordinator.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, NOW);
      for (let i = 0; i < (held === "exhausted" ? 3 : 1); i++) {
        const reading = await coordinator.claimDispatchRead(identity, "status", "synthetic-actor", NOW);
        expect(reading.acquired).toBe(true);
        if (held === "exhausted") await coordinator.finishDispatchRead(identity, reading.claim!, reading.operation!.revision, null, NOW);
      }
      await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify({ schema_version: 1,
        kind: "latest-night", wake_date: DAY, scope, checked_at: new Date(NOW).toISOString(),
        source_checked: false, status: "pending", sleep_status: "stored", hrv_status: "import_error" }));
      const before = await coordinator.dispatchOperation(id), beforeJob = await coordinator.freshJob(id);
      let discovered = 0, runGets = 0;
      provider = async (input, init) => {
        expect(init?.method).toBe("GET");
        if (String(input).includes("/workflows/")) { discovered++; return Response.json({ workflow_runs: [RUN] }); }
        runGets++; return Response.json(RUN);
      };
      const expected = { terminal: false, data_ready: false, should_continue_polling: false,
        dispatch_status: { operation_id: id, state: "candidate", candidate_present: true,
          polling_stopped: true, duplicate_guard_held: true },
        sync_status: { job_state: "unknown", job_start_confirmed: null, current_job_source_checked: null } };
      for (const args of [{ request_id: id }, { run_id: 55 }])
        expect(await call("refresh_status", args, protocol)).toMatchObject(expected);
      expect(counts.github_get).toBe(0); expect(counts.r2_control_get).toBe(0);
      const latest = await call("refresh_status", {}, protocol);
      console.log("C8_RETURN_RESOURCE " + JSON.stringify({ protocol, held, discovery_gets: discovered,
        owned_run_gets: runGets, control_r2_gets: counts.r2_control_get, post: counts.github_post,
        before_polls: beforeJob!.polls, after_polls: (await coordinator.freshJob(id))!.polls }));
      expect(latest).toMatchObject(expected);
      expect(discovered).toBe(1); expect(runGets).toBe(0); expect(counts.r2_control_get).toBe(0);
      actor = crypto.randomUUID(); vi.mocked(Date.now).mockReturnValue(NOW + 86_400_000);
      expect(await call("refresh_status", {}, protocol)).toMatchObject(expected);
      expect(discovered).toBe(2); expect(runGets).toBe(0); expect(counts.r2_control_get).toBe(0);
      expect(counts.github_post).toBe(0);
      expect(await coordinator.dispatchOperation(id)).toEqual(before);
      expect(await coordinator.freshJob(id)).toEqual(beforeJob);
    });
  }
  it("preserves genuinely unowned manual latest status through " + protocol, async () => {
    let discovered = 0;
    provider = async (input, init) => {
      expect(init?.method).toBe("GET"); expect(String(input)).toContain("/workflows/");
      discovered++; return Response.json({ workflow_runs: [RUN] });
    };
    const result = await call("refresh_status", {}, protocol);
    expect(result).toMatchObject({ terminal: true, data_ready: true, should_continue_polling: false,
      run: { id: 55, status: "completed" }, sync_status: { job_state: "completed" } });
    expect(result).not.toHaveProperty("dispatch_status");
    expect(discovered).toBe(1); expect(counts.r2_control_get).toBe(2); expect(counts.github_post).toBe(0);
    expect(await coordinator.dispatchForRun(55)).toBeNull();
  });
}

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it("keeps the existing unowned latest polling interval and completion through " + protocol, async () => {
    let discovered = 0;
    provider = async (input, init) => {
      expect(init?.method).toBe("GET"); expect(String(input)).toContain("/workflows/");
      discovered++;
      return Response.json({ workflow_runs: [{ ...RUN,
        ...(discovered === 1 ? { status: "queued", conclusion: null } : {}) }] });
    };
    const result = await call("refresh_status", {}, protocol);
    expect(result).toMatchObject({ terminal: true, data_ready: true, run: { id: 55, status: "completed" } });
    expect(result).not.toHaveProperty("dispatch_status");
    expect(discovered).toBe(2); expect(counts.r2_control_get).toBe(2); expect(counts.github_post).toBe(0);
  }, 10_000);

  it("routes an exactly owned general latest run to its occupied claim through " + protocol, async () => {
    const lease = await coordinator.reserve("refresh-today", NOW, 120_000);
    const operation = (await coordinator.beginGeneralDispatch(lease.token!, { ...descriptor, mode: "general",
      scope: null, date: null, include_granular: true,
      components: ["summary", "health", "body", "activity_files", "coach_input"] }, NOW))!;
    await coordinator.mergeDispatchPost(operation, { kind: "ack_candidate", run_id: 55 }, NOW);
    const claim = await coordinator.claimDispatchRead(operation, "initial", "synthetic-actor", NOW);
    expect(claim.acquired).toBe(true);
    const before = await coordinator.dispatchOperation(operation.operation_id);
    provider = async (input, init) => {
      expect(init?.method).toBe("GET"); expect(String(input)).toContain("/workflows/");
      return Response.json({ workflow_runs: [RUN] });
    };
    const result = await call("refresh_status", {}, protocol);
    expect(result).toMatchObject({ terminal: false, data_ready: false, should_continue_polling: false,
      dispatch_status: { operation_id: operation.operation_id, duplicate_guard_held: true },
      sync_status: { job_state: "unknown", job_start_confirmed: null } });
    expect(counts.github_get).toBe(1); expect(counts.r2_control_get).toBe(0); expect(counts.github_post).toBe(0);
    expect(await coordinator.dispatchOperation(operation.operation_id)).toEqual(before);
  });
}

for (const protocol of ["2025-03-26", "2026-07-28"]) for (const kind of ["night", "activity"] as const) for (const conclusion of ["success", "cancelled"] as const) {
  it("keeps targeted history and unknown readiness after normal job retention: " + kind + "/" + conclusion + " via " + protocol, async () => {
    const isNight = kind === "night";
    const requestedScope = isNight ? scope : "activity/" + DAY + "/1/known";
    const report = isNight ? { schema_version: 1, kind: "latest-night", wake_date: DAY, scope: requestedScope,
      checked_at: new Date(NOW).toISOString(), source_checked: false,
      status: "pending", sleep_status: "stored", hrv_status: "import_error" }
      : { schema_version: 1, kind: "latest-activity", scope: requestedScope,
        checked_at: new Date(NOW).toISOString(), source_checked: true, status: "expected_activity_missing",
        activity_id: null, activity_date: null, activity_started_at_utc: null, activity_started_at_garmin_local: null,
        expected_date: DAY, already_in_slipstream: false, activity_name: null, summary_updated: false,
        files_ready: false, file_status: "not_checked", coach_status: "not_checked" };
    provider = async (input, init) => {
      if (init?.method === "POST") {
        const inputs = JSON.parse(String(init.body)).inputs;
        await env.SLIPSTREAM_DATA.put("refresh/requests/" + inputs.sync_request_id + ".json",
          JSON.stringify(syntheticStartupReceipt((await coordinator.dispatchOperation(inputs.sync_request_id))!, 55)));
        await env.SLIPSTREAM_DATA.put("refresh/reports/55.json", JSON.stringify(report));
        return Response.json({ workflow_run_id: 55 });
      }
      const run = { ...RUN, conclusion, repository: { full_name: "synthetic/slipstream" },
        head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40) };
      return Response.json(String(input).includes("/workflows/") ? { workflow_runs: [run] } : run);
    };
    const started = await call(isNight ? "sync_latest_night" : "sync_latest_activity",
      isNight ? { wake_date: DAY } : { activity_id: "garmin-1", expected_date: DAY, new_activity_expected: false }, protocol);
    const id = started.request_id;
    expect(started).toMatchObject({ terminal: true, data_ready: false,
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false } });
    const operation = (await coordinator.dispatchOperation(id))!;
    expect(operation).toMatchObject({ dispatch_state: "verified_terminal", read_claim: null,
      owner_released_at: NOW, last_observation: { status: "completed", conclusion } });
    const beforeCounters = { ...counts };
    const before = await call("refresh_status", {}, protocol);
    expect(outputSchemas.refresh_status.safeParse(before).success).toBe(true);
    expect(before).toMatchObject({ terminal: true, data_ready: false,
      sync_status: { kind, job_state: conclusion === "success" ? "completed" : "failed",
        job_conclusion: conclusion, current_job_source_checked: null },
      dispatch_status: { duplicate_guard_held: false } });
    expect(counts.github_get - beforeCounters.github_get).toBe(1);
    expect(counts.r2_control_get - beforeCounters.r2_control_get).toBe(0);

    const late = NOW + 4 * 86_400_000, nextDay = "2026-10-18", nextScope = "night/" + nextDay;
    vi.mocked(Date.now).mockReturnValue(late);
    const retainedBy = await coordinator.beginFresh(nextScope, "night", JSON.stringify({ kind: "night", date: nextDay }),
      late, false, { ...descriptor, date: nextDay, scope: nextScope });
    expect(retainedBy.acquired).toBe(true);
    expect(await coordinator.freshJob(id)).toBeNull();
    expect(await coordinator.dispatchOperation(id)).toEqual(operation);
    const nextOperation = await coordinator.dispatchOperation(retainedBy.job!.request_id);
    const afterCounters = { ...counts };
    const after = await call("refresh_status", {}, protocol);
    console.log("C8_RETENTION_PAIR " + JSON.stringify({ protocol, kind, conclusion,
      before: { kind: before.sync_status.kind, data_ready: before.data_ready, terminal: before.terminal,
        job_state: before.sync_status.job_state, guard: before.dispatch_status.duplicate_guard_held },
      after: { kind: after.sync_status.kind, data_ready: after.data_ready, terminal: after.terminal,
        job_state: after.sync_status.job_state, guard: after.dispatch_status.duplicate_guard_held },
      discovery_gets: counts.github_get - afterCounters.github_get,
      control_gets: counts.r2_control_get - afterCounters.r2_control_get,
      canonical_gets: counts.r2_get - afterCounters.r2_get, post: counts.github_post - afterCounters.github_post }));
    const expected = { available: true, terminal: true, data_ready: false, should_continue_polling: false,
      sync_status: { kind, job_state: conclusion === "success" ? "completed" : "failed",
        job_conclusion: conclusion, job_start_confirmed: true, current_job_source_checked: null,
        complete: null, fresh: null },
      dispatch_status: { operation_id: id, state: "verified_terminal", duplicate_guard_held: false, action_required: false } };
    expect(after).toMatchObject(expected);
    expect(after.message).toContain("targeted");
    expect(outputSchemas.refresh_status.safeParse(after).success).toBe(true);
    expect(counts.github_get - afterCounters.github_get).toBe(1);
    expect(counts.r2_control_get - afterCounters.r2_control_get).toBe(0);
    expect(counts.r2_get - afterCounters.r2_get).toBe(0);
    for (const args of [{ request_id: id }, { run_id: 55 }]) {
      const idCounters = { ...counts };
      const result = await call("refresh_status", args, protocol);
      expect(result).toMatchObject(expected);
      expect(outputSchemas.refresh_status.safeParse(result).success).toBe(true);
      expect(counts.github_get).toBe(idCounters.github_get);
      expect(counts.r2_get).toBe(idCounters.r2_get);
    }
    expect(counts.github_post).toBe(1); // Only the original explicitly requested synthetic dispatch.
    expect(await coordinator.dispatchOperation(id)).toEqual(operation);
    expect(await coordinator.freshJob(id)).toBeNull();
    expect(await coordinator.dispatchOperation(retainedBy.job!.request_id)).toEqual(nextOperation);
    expect(await coordinator.freshJob(retainedBy.job!.request_id)).toMatchObject({ state: "active", polls: 0 });
  });
}
