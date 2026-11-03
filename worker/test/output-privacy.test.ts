import { beforeEach, afterEach, expect, it, vi } from "vitest";
const { verifyAccessRequest } = vi.hoisted(() => ({ verifyAccessRequest: vi.fn() }));
vi.mock("../src/access", async (original) => ({ ...await original<typeof import("../src/access")>(), verifyAccessRequest }));
import { AccessConfigurationError } from "../src/access";
import { authenticateMcpRequest } from "../src/mcp-auth";
import { errorCategory, failureDiagnostic, PrivacyMcpServer, safeMcpMessage, TOOL_FAILURE } from "../src/output-privacy";
import { GitHubRequestError, GitHubReadError } from "../src/github";
import { PayloadTooLargeError } from "../src/security";
import { clearSummaryCaches, R2Storage } from "../src/r2-storage";
import { ActivityCoachTools } from "../src/activity-coach-tools";
import { Client, InMemoryTransport } from "@modelcontextprotocol/client";

const marker = "SYNTHETIC_PRIVATE token email@example.invalid activity date key GPS health\n" + "x".repeat(1000);
const logs: unknown[][] = [];
for (const field of ["accepted", "available"]) {
  it(`projects unverified discovery without trusting supplied status or private extras: ${field}`, () => {
    const safe = safeMcpMessage({ jsonrpc: "2.0", id: 1, result: { isError: true,
      content: [{ type: "text", text: marker }], structuredContent: {
        [field]: false, terminal: true, data_ready: false, should_continue_polling: false,
        message: "GitHub workflow-run discovery could not be verified.",
        sync_status: { job_state: "completed", data_state: "ready", private: marker },
        latency: { private: marker }, private: marker,
      } } });
    expect(JSON.stringify(safe)).not.toContain(marker);
    expect("result" in safe).toBe(true);
    if (!("result" in safe)) throw new Error("Expected a tool result");
    const value = safe.result.structuredContent as Record<string, unknown>;
    expect(value.sync_status).toMatchObject({ job_state: field === "accepted" ? "not_started" : "unknown",
      job_conclusion: null, data_state: "unknown", user_action_required: false, next_action: "retry_later" });
    expect(value).not.toHaveProperty("private");
    expect(JSON.parse((safe.result.content as Array<{ text: string }>)[0].text)).toEqual(value);
  });
}
beforeEach(() => {
  logs.length = 0; verifyAccessRequest.mockReset();
  vi.spyOn(console, "error").mockImplementation((...args) => { logs.push(args); });
  vi.spyOn(console, "warn").mockImplementation((...args) => { logs.push(args); });
});
afterEach(() => vi.restoreAllMocks());
it("diagnoses an actual registered coach_profile tool failure before SDK flattening", async () => {
  const bucket = { list: vi.fn().mockRejectedValue(new Error(marker)), get: vi.fn(), head: vi.fn() };
  const server = new PrivacyMcpServer({ name: "privacy-test", version: "1" });
  new ActivityCoachTools({ SLIPSTREAM_DATA: bucket } as Env, "synthetic", new R2Storage(bucket))
    .registerDetailTools(server);
  const client = new Client({ name: "privacy-test-client", version: "1" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  try {
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
    const result = await client.callTool({ name: "coach_profile", arguments: {} });
    expect(result).toMatchObject({ isError: true, content: [{ type: "text", text: TOOL_FAILURE }] });
    expect(bucket.list).toHaveBeenCalledTimes(1);
    expect(bucket.get).not.toHaveBeenCalled();
    expect(logs).toHaveLength(1);
    expect(JSON.parse(logs[0][0] as string)).toEqual({ message: "MCP tool callback failed",
      error: "operation_failed", component: "mcp", operation: "coach_profile", site: "mcp_tool_callback" });
    expect(JSON.stringify({ logs, result })).not.toContain("SYNTHETIC_PRIVATE");
  } finally { await client.close(); await server.close(); }
});
it("keeps typed guidance through the same registered application callback", async () => {
  const bucket = { list: vi.fn(), get: vi.fn(), head: vi.fn() };
  const server = new PrivacyMcpServer({ name: "privacy-guidance-test", version: "1" });
  new ActivityCoachTools({ SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "true" } as Env,
    "synthetic", new R2Storage(bucket)).registerDetailTools(server);
  const client = new Client({ name: "privacy-test-client", version: "1" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  try {
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
    const result = await client.callTool({ name: "add_coach_profile", arguments: {
      effective_from: "2041-02-30", zones: [{ label: "Easy", min_bpm: 90, max_bpm: 120 }], references: {},
    } });
    expect(result).toMatchObject({ isError: true,
      content: [{ type: "text", text: "Invalid calendar date. Use YYYY-MM-DD with a valid calendar day." }] });
    expect(bucket.get).not.toHaveBeenCalled();
    expect(bucket.list).not.toHaveBeenCalled();
    expect(logs).toHaveLength(0);
  } finally { await client.close(); await server.close(); }
});
it("keeps a successful registered coach_profile response without a failure log", async () => {
  const bucket = { list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    get: vi.fn().mockResolvedValue(null), head: vi.fn() };
  const server = new PrivacyMcpServer({ name: "privacy-success-test", version: "1" });
  new ActivityCoachTools({ SLIPSTREAM_DATA: bucket } as Env, "synthetic", new R2Storage(bucket))
    .registerDetailTools(server);
  const client = new Client({ name: "privacy-test-client", version: "1" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  try {
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
    const result = await client.callTool({ name: "coach_profile", arguments: {} });
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent).toMatchObject({ profiles: [] });
    expect(logs).toHaveLength(0);
    expect(bucket.list).toHaveBeenCalledTimes(1);
    expect(bucket.get).toHaveBeenCalledTimes(1);
  } finally { await client.close(); await server.close(); }
});
for (const error of [new Error(marker), new AccessConfigurationError(marker), marker]) {
  it("auth errors retain status and emit only controlled categories", async () => {
    verifyAccessRequest.mockRejectedValue(error);
    const result = await authenticateMcpRequest(new Request("https://privacy.example/mcp"), {
      MCP_HOSTNAME: "privacy.example", ACCESS_TEAM_DOMAIN: "https://privacy.cloudflareaccess.com", ACCESS_AUD: "synthetic",
    } as Env) as Response;
    expect(result.status).toBe(error instanceof AccessConfigurationError ? 503 : 401);
    expect(await result.text()).toBe(error instanceof AccessConfigurationError ? "OAuth is not configured" : "Unauthorized");
    expect(JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(logs).toHaveLength(1);
    expect(JSON.parse(logs[0][0] as string)).toMatchObject({
      component: "access", operation: "authenticate", site: "mcp_auth",
      error: error instanceof AccessConfigurationError ? "access_configuration" : "operation_failed",
    });
  });
}
it("projects only typed observed GitHub status and fixed read reasons", () => {
  expect(failureDiagnostic(new GitHubRequestError(429, marker, marker), "refresh_status"))
    .toEqual({ error: "github_request_failed", component: "refresh", operation: "status",
      site: "refresh_status", http_status: 429, reason: "rate_limited" });
  expect(failureDiagnostic(new GitHubReadError("timeout"), "refresh_status"))
    .toMatchObject({ error: "github_read_failed", reason: "timeout" });
  expect(failureDiagnostic({ name: "GitHubRequestError", status: 429, reason: marker }, "refresh_status"))
    .toEqual({ error: "operation_failed", component: "refresh", operation: "status", site: "refresh_status" });
  expect(failureDiagnostic(new GitHubRequestError(Number.NaN, marker, marker), "refresh_status"))
    .not.toHaveProperty("http_status");
});
for (const health of [false, true]) for (const kind of ["head", "read", "missing", "schema", "size"] as const) {
  it(`R2 summary failures identify a fixed stage without provider text: ${health ? "health" : "activities"}/${kind}`, async () => {
    clearSummaryCaches();
    const bucket = { head: vi.fn(), get: vi.fn() };
    bucket.head.mockResolvedValue(null);
    bucket.get.mockResolvedValue(null);
    if (kind === "head") bucket.head.mockRejectedValue(Object.assign(new Error(marker), { status: 403 }));
    if (kind === "read") bucket.get.mockRejectedValue(new Error(marker));
    if (kind === "schema" || kind === "size") bucket.get.mockResolvedValue({ size: kind === "size" ? 2 ** 30 : marker.length,
      etag: marker, body: new Response(marker.replace(/\n/g, "")).body });
    const storage = new R2Storage(bucket);
    await expect(health ? storage.getHealth() : storage.getActivities())
      .rejects.toThrow(health ? "Health data is unavailable in private R2." : "Activity data is unavailable in private R2.");
    expect(logs).toHaveLength(1);
    expect(JSON.parse(logs[0][0] as string)).toMatchObject({ component: "r2", operation: "read",
      site: health ? "r2_health" : "r2_activities", stage: kind === "size" ? "read" : kind,
      error: kind === "missing" ? "r2_missing" : kind === "schema" ? "r2_schema_invalid"
        : kind === "size" ? "payload_too_large" : "operation_failed" });
    expect(JSON.stringify(logs)).not.toContain("SYNTHETIC_PRIVATE");
    expect(JSON.parse(logs[0][0] as string)).not.toHaveProperty("http_status");
    if (kind === "head") expect(bucket.get).not.toHaveBeenCalled();
  });
}
it("only explicit known error classes select a category", () => {
  expect(errorCategory(new PayloadTooLargeError(marker))).toBe("payload_too_large");
  expect(errorCategory(new GitHubRequestError(403, "POST", marker))).toBe("github_request_failed");
  expect(errorCategory({ name: "PayloadTooLargeError", message: marker })).toBe("operation_failed");
});
it("unknown structured tool errors cannot smuggle other fields", () => {
  const result = safeMcpMessage({ jsonrpc: "2.0", id: 1, result: { isError: true,
    content: [{ type: "text", text: marker }], structuredContent: { message: marker, unknown: marker } } });
  expect(JSON.stringify(result)).not.toContain("SYNTHETIC_PRIVATE");
  expect(JSON.stringify(result)).toContain(TOOL_FAILURE);
});
it("protocol errors omit provider data and retain the code and request ID", () => {
  expect(safeMcpMessage({ jsonrpc: "2.0", id: 1, error: { code: -32602, message: marker, data: marker } }))
    .toEqual({ jsonrpc: "2.0", id: 1, error: { code: -32602, message: "The MCP request could not be completed." } });
});
it("normal authorized fitness responses and transport messages pass through by identity", () => {
  const message = { jsonrpc: "2.0" as const, id: 1, result: { content: [{ type: "text", text: marker }],
    structuredContent: { activity: { name: marker } } } };
  expect(safeMcpMessage(message)).toBe(message);
});
