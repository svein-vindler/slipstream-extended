import { afterEach, beforeAll, expect, it, vi } from "vitest";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { verifyAccessRequest } from "../src/access";

// Only JWKS transport is synthetic. Signatures and all JOSE claim checks are real.
let key: CryptoKey, otherKey: CryptoKey, jwk: Record<string, unknown>;
beforeAll(async () => {
  const [primary, other] = await Promise.all([
    generateKeyPair("RS256", { extractable: true }), generateKeyPair("RS256", { extractable: true }),
  ]);
  key = primary.privateKey; otherKey = other.privateKey;
  jwk = { ...await exportJWK(primary.publicKey), kid: "synthetic-key", alg: "RS256", use: "sig" };
});
afterEach(() => { vi.unstubAllGlobals(); });

async function signedRequest(fault?: "audience" | "issuer" | "expired" | "signature") {
  // Unique issuers isolate the public JWKS cache without reaching into its internals.
  const issuer = `https://synthetic-${crypto.randomUUID()}.cloudflareaccess.com`;
  const jwks = vi.fn(async (input: string | URL | Request) => {
    const url = input instanceof Request ? input.url : String(input);
    expect(url).toBe(issuer + "/cdn-cgi/access/certs");
    return Response.json({ keys: [jwk] });
  });
  vi.stubGlobal("fetch", jwks);
  const now = Math.floor(Date.now() / 1000);
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "synthetic-key" })
    .setSubject("synthetic-user")
    .setIssuer(fault === "issuer" ? "https://other.cloudflareaccess.com" : issuer)
    .setAudience(fault === "audience" ? "other-app" : "synthetic-app")
    .setIssuedAt(now - 120).setExpirationTime(fault === "expired" ? now - 60 : now + 300)
    .sign(fault === "signature" ? otherKey : key);
  return { request: new Request("https://worker.example.invalid/mcp", {
    headers: { "cf-access-jwt-assertion": token },
  }), config: { teamDomain: issuer, audience: "synthetic-app" }, jwks };
}

it("accepts the verified identity of a real RS256 Access assertion", async () => {
  const { request, config, jwks } = await signedRequest();
  await expect(verifyAccessRequest(request, config)).resolves.toEqual({
    subject: "synthetic-user", email: "synthetic@example.invalid",
  });
  expect(jwks).toHaveBeenCalledTimes(1);
});
it.each(["audience", "issuer", "expired", "signature"] as const)(
  "rejects a real signed assertion with invalid %s", async fault => {
    const { request, config, jwks } = await signedRequest(fault);
    await expect(verifyAccessRequest(request, config)).rejects.toThrow();
    expect(jwks).toHaveBeenCalledTimes(1);
  });
