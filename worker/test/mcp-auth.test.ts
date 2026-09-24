import { beforeEach, describe, expect, it, vi } from "vitest";

const { verifyAccessRequest } = vi.hoisted(() => ({
  verifyAccessRequest: vi.fn(),
}));

vi.mock("../src/access", async (importOriginal) => {
  const original = await importOriginal<typeof import("../src/access")>();
  return { ...original, verifyAccessRequest };
});

import { AccessConfigurationError } from "../src/access";
import { authenticateMcpRequest } from "../src/mcp-auth";

function env(overrides: Partial<Env> = {}): Env {
  return {
    ACCESS_TEAM_DOMAIN: "https://fitness.cloudflareaccess.com",
    ACCESS_AUD: "audience",
    MCP_HOSTNAME: "example.test",
    ...overrides,
  } as Env;
}

describe("MCP route authentication", () => {
  beforeEach(() => {
    verifyAccessRequest.mockReset();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
  });

  it("uses the verified Access subject as the OAuth rate-limit key", async () => {
    verifyAccessRequest.mockResolvedValue({ subject: "user-123", email: "owner@example.com" });
    await expect(authenticateMcpRequest(
      new Request("https://example.test/mcp"),
      env(),
    )).resolves.toEqual({ rateLimitKey: "oauth:user-123", hostname: "example.test" });
  });

  it("fails closed when the configured MCP hostname is missing", async () => {
    const result = await authenticateMcpRequest(
      new Request("https://example.test/mcp"),
      env({ MCP_HOSTNAME: undefined }),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(503);
    expect(verifyAccessRequest).not.toHaveBeenCalled();
  });

  it("rejects requests routed through a different hostname", async () => {
    const result = await authenticateMcpRequest(
      new Request("https://preview.example.test/mcp"),
      env(),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(421);
    expect(verifyAccessRequest).not.toHaveBeenCalled();
  });

  it("rejects a hostname secret containing a scheme or path", async () => {
    const result = await authenticateMcpRequest(
      new Request("https://example.test/mcp"),
      env({ MCP_HOSTNAME: "https://example.test/mcp" }),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(503);
    expect(verifyAccessRequest).not.toHaveBeenCalled();
  });

  it("fails closed when OAuth configuration is missing", async () => {
    verifyAccessRequest.mockRejectedValue(new AccessConfigurationError("missing config"));
    const result = await authenticateMcpRequest(
      new Request("https://example.test/mcp"),
      env({ ACCESS_AUD: undefined }),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(503);
  });

  it("returns 401 when Access assertion validation fails", async () => {
    verifyAccessRequest.mockRejectedValue(new Error("invalid assertion"));
    const result = await authenticateMcpRequest(
      new Request("https://example.test/mcp"),
      env(),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(401);
  });

  it("rejects every non-canonical path", async () => {
    const result = await authenticateMcpRequest(
      new Request("https://example.test/anything/mcp"),
      env(),
    );
    expect(result).toBeInstanceOf(Response);
    expect((result as Response).status).toBe(404);
  });
});
