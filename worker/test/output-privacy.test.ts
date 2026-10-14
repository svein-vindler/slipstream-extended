import { beforeEach, afterEach, expect, it, vi } from "vitest";
const { verifyAccessRequest } = vi.hoisted(() => ({ verifyAccessRequest: vi.fn() }));
vi.mock("../src/access", async (original) => ({ ...await original<typeof import("../src/access")>(), verifyAccessRequest }));
import { AccessConfigurationError } from "../src/access";
import { authenticateMcpRequest } from "../src/mcp-auth";
import { errorCategory, safeMcpMessage, TOOL_FAILURE } from "../src/output-privacy";
import { GitHubRequestError } from "../src/github";
import { PayloadTooLargeError } from "../src/security";

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
