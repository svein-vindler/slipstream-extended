import { beforeEach, describe, expect, it, vi } from "vitest";

const { jwtVerify, createRemoteJWKSet } = vi.hoisted(() => ({
  jwtVerify: vi.fn(),
  createRemoteJWKSet: vi.fn(() => "test-jwks"),
}));

vi.mock("jose", () => ({ jwtVerify, createRemoteJWKSet }));

import {
  AccessConfigurationError, normalizeAccessTeamDomain, verifyAccessRequest,
} from "../src/access";

describe("Cloudflare Access authentication", () => {
  beforeEach(() => {
    jwtVerify.mockReset();
    createRemoteJWKSet.mockClear();
  });

  it("normalizes a team hostname to an HTTPS origin", () => {
    expect(normalizeAccessTeamDomain("fitness.cloudflareaccess.com"))
      .toBe("https://fitness.cloudflareaccess.com");
    expect(normalizeAccessTeamDomain("https://fitness.cloudflareaccess.com/"))
      .toBe("https://fitness.cloudflareaccess.com");
  });

  it("rejects unsafe or path-bearing team domains", () => {
    expect(() => normalizeAccessTeamDomain("http://fitness.cloudflareaccess.com"))
      .toThrow(AccessConfigurationError);
    expect(() => normalizeAccessTeamDomain("https://fitness.cloudflareaccess.com/path"))
      .toThrow(AccessConfigurationError);
    expect(() => normalizeAccessTeamDomain("https://identity.example.com"))
      .toThrow(AccessConfigurationError);
  });

  it("fails closed when Access configuration is absent", async () => {
    await expect(verifyAccessRequest(new Request("https://example.test/mcp"), {}))
      .rejects.toBeInstanceOf(AccessConfigurationError);
  });

  it("requires the signed Access assertion header", async () => {
    await expect(verifyAccessRequest(new Request("https://example.test/mcp"), {
      teamDomain: "https://fitness.cloudflareaccess.com",
      audience: "audience",
    })).rejects.toThrow("Missing Cloudflare Access assertion");
  });

  it("verifies issuer, audience and RS256 before accepting a user", async () => {
    jwtVerify.mockResolvedValue({
      payload: { type: "app", sub: "user-123", email: "owner@example.com" },
    });
    const request = new Request("https://example.test/mcp", {
      headers: { "cf-access-jwt-assertion": "signed-token" },
    });

    await expect(verifyAccessRequest(request, {
      teamDomain: "fitness.cloudflareaccess.com",
      audience: "audience",
    })).resolves.toEqual({ subject: "user-123", email: "owner@example.com" });
    expect(createRemoteJWKSet).toHaveBeenCalledWith(
      new URL("https://fitness.cloudflareaccess.com/cdn-cgi/access/certs"),
    );
    expect(jwtVerify).toHaveBeenCalledWith("signed-token", "test-jwks", {
      issuer: "https://fitness.cloudflareaccess.com",
      audience: "audience",
      algorithms: ["RS256"],
    });
  });

  it("rejects service tokens and assertions without verified email identity", async () => {
    jwtVerify.mockResolvedValue({ payload: { type: "app", sub: "" } });
    const request = new Request("https://example.test/mcp", {
      headers: { "cf-access-jwt-assertion": "service-token" },
    });
    await expect(verifyAccessRequest(request, {
      teamDomain: "fitness.cloudflareaccess.com",
      audience: "audience",
    })).rejects.toThrow("does not contain a user identity");
  });
});
