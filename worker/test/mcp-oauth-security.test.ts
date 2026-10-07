import { describe, expect, it, vi } from "vitest";
import { fetchToken as fetchSdkToken } from "@modelcontextprotocol/sdk/client/auth.js";
import { fetchToken as fetchClientToken } from "@modelcontextprotocol/client";

const issuer = "https://trusted.example.invalid";
const otherIssuer = "https://other.example.invalid";
const clientSecret = "synthetic-client-secret";
const refreshToken = "synthetic-refresh-token";

function metadata(url: string) {
  return {
    issuer: url,
    authorization_endpoint: `${url}/authorize`,
    token_endpoint: `${url}/token`,
    response_types_supported: ["code"],
    token_endpoint_auth_methods_supported: ["client_secret_post"],
  };
}

function provider() {
  return {
    redirectUrl: undefined,
    clientMetadata: { redirect_uris: [] },
    clientInformation: () => ({
      client_id: "synthetic-client",
      client_secret: clientSecret,
      issuer,
    }),
    tokens: () => undefined,
    saveTokens: vi.fn(),
    redirectToAuthorization: vi.fn(),
    saveCodeVerifier: vi.fn(),
    codeVerifier: () => "unused-synthetic-verifier",
    prepareTokenRequest: vi.fn(() => new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
    })),
  };
}

describe.each([
  ["SDK v1", fetchSdkToken],
  ["client v2", fetchClientToken],
] as const)("MCP OAuth issuer isolation: %s", (_name, fetchToken) => {
  it("rejects a different issuer before preparing or sending credentials", async () => {
    const credentials = provider();
    const fetchFn = vi.fn(async () => Response.json({
      access_token: "synthetic-access-token", token_type: "Bearer",
    }));

    await expect(fetchToken(credentials, otherIssuer, {
      metadata: metadata(otherIssuer), fetchFn,
    })).rejects.toThrow(/authorization server/i);

    expect(credentials.prepareTokenRequest).not.toHaveBeenCalled();
    expect(fetchFn).not.toHaveBeenCalled();
  });

  it("still exchanges tokens with the issuer the credentials belong to", async () => {
    const fetchFn = vi.fn(async (_input: unknown, _init?: RequestInit) =>
      Response.json({ access_token: "synthetic-access-token", token_type: "Bearer" }));

    const tokens = await fetchToken(provider(), issuer, {
      metadata: metadata(issuer), fetchFn,
    });

    expect(tokens.access_token).toBe("synthetic-access-token");
    expect(fetchFn).toHaveBeenCalledTimes(1);
    const [url, init] = fetchFn.mock.calls[0];
    expect(String(url)).toBe(`${issuer}/token`);
    expect(init?.method).toBe("POST");
    const params = new URLSearchParams(String(init?.body));
    expect(params.get("client_secret")).toBe(clientSecret);
    expect(params.get("refresh_token")).toBe(refreshToken);
  });
});
