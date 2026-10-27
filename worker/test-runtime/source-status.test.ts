import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";
import contractMetadata from "./generated/source-status-contract.meta.json";
// Await each complete fixture module separately; avoid a single large pool RPC.
const {case_count: caseCount, ...producerMetadata} = contractMetadata;
const contract = {...producerMetadata, cases: [
  (await import("./generated/source-status-contract.case-00.json")).default,
  (await import("./generated/source-status-contract.case-01.json")).default,
  (await import("./generated/source-status-contract.case-02.json")).default,
  (await import("./generated/source-status-contract.case-03.json")).default,
  (await import("./generated/source-status-contract.case-04.json")).default,
  (await import("./generated/source-status-contract.case-05.json")).default,
  (await import("./generated/source-status-contract.case-06.json")).default,
  (await import("./generated/source-status-contract.case-07.json")).default,
  (await import("./generated/source-status-contract.case-08.json")).default,
  (await import("./generated/source-status-contract.case-09.json")).default,
  (await import("./generated/source-status-contract.case-10.json")).default,
  (await import("./generated/source-status-contract.case-11.json")).default,
  (await import("./generated/source-status-contract.case-12.json")).default,
  (await import("./generated/source-status-contract.case-13.json")).default,
  (await import("./generated/source-status-contract.case-14.json")).default,
  (await import("./generated/source-status-contract.case-15.json")).default,
  (await import("./generated/source-status-contract.case-16.json")).default,
  (await import("./generated/source-status-contract.case-17.json")).default,
  (await import("./generated/source-status-contract.case-18.json")).default,
  (await import("./generated/source-status-contract.case-19.json")).default,
  (await import("./generated/source-status-contract.case-20.json")).default,
  (await import("./generated/source-status-contract.case-21.json")).default,
  (await import("./generated/source-status-contract.case-22.json")).default,
  (await import("./generated/source-status-contract.case-23.json")).default,
  (await import("./generated/source-status-contract.case-24.json")).default,
  (await import("./generated/source-status-contract.case-25.json")).default,
  (await import("./generated/source-status-contract.case-26.json")).default,
]};
if (contract.cases.length !== caseCount) throw new Error("Synthetic source-status fixture case count changed");
import { clearSummaryCaches } from "../src/r2-storage";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";

const NOW = Date.parse("2026-10-14T12:00:00Z"), DAY = "2026-10-14";
const issuer = "https://synthetic-dispatch.cloudflareaccess.com", hostname = "dispatch.example";
let privateKey: CryptoKey, jwk: Record<string, unknown>, testEnv: Env, actor: string;
let coordinator: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>;
let provider: typeof fetch;
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



for (const protocol of ["2025-03-26", "2026-07-28"]) for (const fixture of contract.cases) {
  it("keeps actual " + fixture.mode + "/" + fixture.outcome + " source provenance through " + protocol, async ({ task }) => {
    for (const [key, value] of Object.entries(fixture.objects))
      await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
    const receipt = fixture.objects["refresh/requests/" + fixture.request_id + ".json"];
    const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main", ...receipt.request };
    let operation;
    if (fixture.mode === "general") {
      await runInDurableObject(coordinator, (_instance, state) =>
        state.storage.sql.exec("INSERT INTO refresh_leases (lease_key,token,expires_at) VALUES ('refresh-today',?,?)",
          fixture.request_id, NOW + 120_000).toArray());
      operation = (await coordinator.beginGeneralDispatch(fixture.request_id, descriptor, NOW))!;
    } else {
      await runInDurableObject(coordinator, instance => {
        const uuid = vi.spyOn(crypto, "randomUUID").mockReturnValue(fixture.request_id);
        try { instance.beginFresh(receipt.request.scope!, fixture.mode as "activity" | "night",
            JSON.stringify({ kind: fixture.mode, date: DAY, ...(fixture.mode === "activity" ? { activityId: "1" } : {}) }), NOW, false, descriptor); }
        finally { uuid.mockRestore(); }
      });
      operation = (await coordinator.dispatchOperation(fixture.request_id))!;
    }
    await coordinator.mergeDispatchPost(operation, { kind: "ack_candidate", run_id: fixture.run_id }, NOW);
    provider = async () => Response.json({ id: fixture.run_id, status: "completed", conclusion: fixture.job_conclusion,
      event: "workflow_dispatch", path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: null,
      html_url: "https://github.com/synthetic/slipstream/actions/runs/" + fixture.run_id });
    const result = await call("refresh_status", { request_id: fixture.request_id }, protocol);
    task.meta.c10Resources = { outcome: fixture.outcome, mode: fixture.mode, protocol, counts: { ...counts },
      current_checked: result.sync_status.current_job_source_checked, data_state: result.sync_status.data_state };
    expect(outputSchemas.refresh_status.safeParse(result).success).toBe(true);
    if (fixture.fatal) {
      expect(result).toMatchObject({ terminal: false, should_continue_polling: false,
        dispatch_status: { state: "candidate", duplicate_guard_held: true },
        sync_status: { job_state: "unknown", current_job_source_checked: null, current_job_source_checked_at: null } });
      expect(result.data_ready).toBe(false);
      expect(result.sync_status.data_state).not.toBe("source_pending");
      expect(fixture.diagnostic.status).toBe("failed");
      expect(fixture.diagnostic.source_checks.some(check => check.checked === false && check.outcome === "failed")).toBe(true);
      expect(fixture.objects).not.toHaveProperty("refresh/reports/" + fixture.run_id + ".json");
      expect(fixture.canonical_bytes_preserved).toBe(true);
      expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(1);
      expect(counts.r2_control_get).toBe(3); expect(counts.r2_put).toBe(0);
      return;
    }
    const bad = ["wrong_date", "invalid_response", "source_error", "body_wrong_date", "body_invalid_response",
      "body_unknown", "body_missing_weight", "body_missing_time", "both_errors", "summary_schema"].includes(fixture.outcome);
    expect(result).toMatchObject({ terminal: true, data_ready: fixture.mode === "general" || fixture.outcome === "valid",
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
      sync_status: { kind: fixture.mode, job_state: "completed", source_checked: !bad, current_job_source_checked: !bad,
        current_job_source_checked_at: bad ? null : "2026-10-14T12:00:00+00:00",
        data_state: bad ? "error" : ["pending", "body_pending", "mixed_pending"].includes(fixture.outcome) ? "source_pending" : fixture.mode === "night" ? "ready" : "unknown" } });
    expect(fixture.canonical_bytes_preserved).toBe(true);
    if (fixture.outcome === "body_valid") {
      const day = fixture.objects["health/body-composition/v1/2026/10/" + DAY + ".json"];
      expect(day.measurements[0]).toMatchObject({ weight_kg: 80, body_fat_percent: 0, muscle_mass_kg: 60.1,
        timestamp_local: DAY + "T07:00:00", source_type: "MANUAL" });
      expect(day.measurements[1].body_fat_percent).toBeNull();
    }
    if (fixture.outcome === "full_valid") {
      expect(fixture.diagnostic.source_checks.map(check => check.scope)).toEqual(
        ["activities_summary", "health_summary", "sleep", "hrv", "body"]);
      expect(fixture.diagnostic.source_checks.every(check => check.checked)).toBe(true);
    }
    if (fixture.outcome === "both_errors") {
      expect(fixture.diagnostic.source_checks.filter(check => check.outcome === "failed").map(check => check.scope))
        .toEqual(["sleep", "body"]);
      expect(fixture.checkpoint_bytes_preserved["refresh/checks/v1/night/" + DAY + ".json"]).toBe(true);
    }
    if (bad) expect(fixture.checkpoint_bytes_preserved[fixture.outcome === "summary_schema"
      ? "refresh/checks/v1/summary/health/reconciliation.json"
      : (fixture.outcome.startsWith("body_") || fixture.outcome === "both_errors")
      ? "refresh/checks/v1/health/body_composition/" + DAY + ".json"
      : "refresh/checks/v1/night/" + DAY + ".json"]).toBe(true);
    if (fixture.mode === "night" && bad)
      expect(result.sync_status.stored_source_checked_at).toBe("2026-10-14T11:50:00+00:00");
    expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(1); expect(counts.r2_control_get).toBe(3);
    expect(counts.r2_put).toBe(0); expect(counts.r2_head).toBe(0); expect(counts.r2_list).toBe(0);
    console.log("C10_MCP_RESOURCE " + JSON.stringify({ mode: fixture.mode, outcome: fixture.outcome, protocol, counts,
      current_checked: result.sync_status.current_job_source_checked, data_state: result.sync_status.data_state }));
  });
}
