import { env, createExecutionContext, waitOnExecutionContext, runInDurableObject } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";
import contract from "./generated/source-status-contract.json";
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
  it("keeps actual " + fixture.mode + "/" + fixture.outcome + " source provenance through " + protocol, async () => {
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
        try { instance.beginFresh("night/" + DAY, "night", JSON.stringify({ kind: "night", date: DAY }), NOW, false, descriptor); }
        finally { uuid.mockRestore(); }
      });
      operation = (await coordinator.dispatchOperation(fixture.request_id))!;
    }
    await coordinator.mergeDispatchPost(operation, { kind: "ack_candidate", run_id: fixture.run_id }, NOW);
    provider = async () => Response.json({ id: fixture.run_id, status: "completed", conclusion: "success",
      event: "workflow_dispatch", path: ".github/workflows/refresh.yml", repository: { full_name: "synthetic/slipstream" },
      head_repository: { full_name: "synthetic/slipstream" }, head_branch: "main", head_sha: "a".repeat(40),
      created_at: new Date(NOW).toISOString(), updated_at: null,
      html_url: "https://github.com/synthetic/slipstream/actions/runs/" + fixture.run_id });
    const result = await call("refresh_status", { request_id: fixture.request_id }, protocol);
    expect(outputSchemas.refresh_status.safeParse(result).success).toBe(true);
    const bad = ["wrong_date", "invalid_response", "source_error"].includes(fixture.outcome);
    expect(result).toMatchObject({ terminal: true, data_ready: fixture.mode === "general" || fixture.outcome === "valid",
      dispatch_status: { state: "verified_terminal", duplicate_guard_held: false },
      sync_status: { kind: fixture.mode, job_state: "completed", source_checked: !bad, current_job_source_checked: !bad,
        current_job_source_checked_at: bad ? null : "2026-10-14T12:00:00+00:00",
        data_state: bad ? "error" : fixture.outcome === "pending" ? "source_pending" : fixture.mode === "night" ? "ready" : "unknown" } });
    expect(fixture.canonical_bytes_preserved).toBe(true);
    if (bad) expect(fixture.checkpoint_bytes_preserved["refresh/checks/v1/night/" + DAY + ".json"]).toBe(true);
    if (fixture.mode === "night" && bad)
      expect(result.sync_status.stored_source_checked_at).toBe("2026-10-14T11:50:00+00:00");
    expect(counts.github_post).toBe(0); expect(counts.github_get).toBe(1); expect(counts.r2_control_get).toBe(3);
    expect(counts.r2_put).toBe(0); expect(counts.r2_head).toBe(0); expect(counts.r2_list).toBe(0);
    console.log("C10_MCP_RESOURCE " + JSON.stringify({ mode: fixture.mode, outcome: fixture.outcome, protocol, counts,
      current_checked: result.sync_status.current_job_source_checked, data_state: result.sync_status.data_state }));
  });
}
