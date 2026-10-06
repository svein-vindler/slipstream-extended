import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { outputSchemas } from "../src/mcp-output";

const DAY = "2026-10-05";
const hostname = "refresh-status.example";
const issuer = "https://synthetic-refresh-status.cloudflareaccess.com";
const RUN = { id: 55, status: "completed", conclusion: "success", event: "workflow_dispatch",
  created_at: "2026-10-05T12:00:00Z", updated_at: "2026-10-05T12:02:00Z",
  html_url: "https://github.com/owner/example/actions/runs/55", path: ".github/workflows/refresh.yml" };
let keys: string[];
let testEnv: Env;
let token: string;
let githubCalls: number;
let pair: Awaited<ReturnType<typeof generateKeyPair>>;
let jwk: Record<string, unknown>;

async function put(key: string, value: unknown) {
  keys.push(key); await env.SLIPSTREAM_DATA.put(key, JSON.stringify(value));
}
async function report(sourceChecked = true) {
  await put("refresh/reports/55.json", { schema_version: 1, kind: "latest-night", wake_date: DAY,
    scope: `night/${DAY}`, checked_at: new Date().toISOString(), source_checked: sourceChecked,
    status: "stored", sleep_status: "stored", hrv_status: "stored" });
  await put(`refresh/checks/v1/night/${DAY}.json`, { scope: `night/${DAY}`, source_checked: true,
    checked_at: new Date().toISOString(), status: "stored" });
}
async function night(includeHrv = true) {
  await put(`health/sleep/v1/2026/10/${DAY}.json`, { schema_version: 1, date: DAY, confirmed: true,
    sleep_start_gmt: "2026-10-04T21:00:00Z", sleep_end_gmt: "2026-10-05T04:00:00Z",
    sleep_start_garmin_local: "2026-10-04T23:00:00", sleep_end_garmin_local: "2026-10-05T06:00:00",
    summary: { sleep_seconds: 25200 }, stage_count: 1, stages: [{ start_gmt: "2026-10-04T21:00:00Z",
      end_gmt: "2026-10-05T04:00:00Z", stage: 1 }] });
  if (includeHrv) await put(`health/hrv/2026/10/${DAY}.json`, { schema_version: 1, date: DAY,
    summary: { lastNightAvg: 51 }, readings: [{ timestamp: "2026-10-05T01:00:00Z", hrv_ms: 51 }] });
}
async function status() {
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, { method: "POST",
    headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call",
      params: { name: "refresh_status", arguments: { run_id: 55 } } }),
  }), testEnv, ctx);
  const body = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, body).toBe(200);
  const message = response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(body.split("\n").find(line => line.startsWith("data: "))!.slice(6)) : JSON.parse(body);
  expect(message.error, body).toBeUndefined();
  const result = message.result.structuredContent;
  expect(outputSchemas.refresh_status.safeParse(result).success, JSON.stringify(result)).toBe(true);
  return result;
}

beforeAll(async () => {
  pair = await generateKeyPair("RS256", { extractable: true });
  jwk = { ...await exportJWK(pair.publicKey), kid: "refresh-key", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  keys = []; githubCalls = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = input instanceof Request ? input.url : String(input);
    if (url === `${issuer}/cdn-cgi/access/certs`) return Response.json({ keys: [jwk] });
    expect(init?.method).toBe("GET");
    expect(url).toBe("https://api.github.com/repos/owner/example/actions/runs/55");
    githubCalls++; return Response.json(RUN);
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" }).setProtectedHeader({ alg: "RS256", kid: "refresh-key" })
    .setIssuer(issuer).setAudience("synthetic-refresh").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(pair.privateKey);
  const coordinator = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-refresh", MCP_HOSTNAME: hostname,
    HEALTH_TIMEZONE: "Europe/Oslo", GITHUB_REPOSITORY: "owner/example", GITHUB_ACTIONS_TOKEN: "synthetic",
    REFRESH_COORDINATOR: { getByName: () => coordinator } } as Env;
});
afterEach(async () => { vi.restoreAllMocks(); if (keys.length) await env.SLIPSTREAM_DATA.delete([...new Set(keys)]); });

it("verifies a manual night run without a registered request ID using sleep and HRV", async () => {
  await report(); await night();
  const result = await status();
  expect(result).toMatchObject({ data_ready: true, activity_ready: null, terminal: true,
    sync_status: { kind: "night", job_state: "completed", data_state: "ready", complete: true, fresh: true } });
  expect(result.message).toContain("sleep and HRV");
  expect(result.message).not.toContain("activity-file");
  expect(result.latency.pipeline_ms).toBeNull(); expect(githubCalls).toBe(1);
});
it("retains legacy job-success semantics while explicitly marking an incomplete night", async () => {
  await report(); await night(false);
  expect(await status()).toMatchObject({ data_ready: true, activity_ready: null,
    sync_status: { kind: "night", data_state: "partial", complete: false, missing_components: ["hrv_readings"] } });
});
it("does not trust a failed current source check over older preserved canonical data", async () => {
  await report(false); await night();
  expect(await status()).toMatchObject({ sync_status: { kind: "night", source_checked: false,
    data_state: "stale", fresh: false, complete: true } });
});
it("reports missing or invalid per-run reports as unknown without activity-specific advice", async () => {
  await put("refresh/reports/55.json", { kind: "latest-night", wake_date: "../../bad" });
  const result = await status();
  expect(result).toMatchObject({ sync_status: { kind: "unknown", data_state: "unknown", source_checked: null } });
  expect(result.message).not.toContain("activity-file");
});
