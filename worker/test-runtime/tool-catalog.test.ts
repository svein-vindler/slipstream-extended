import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import worker from "../src/index";

const issuer = "https://synthetic-tool-catalog.cloudflareaccess.com";
const hostname = "tool-catalog.example";
let privateKey: CryptoKey;
let fetchSpy: ReturnType<typeof vi.spyOn>;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...await exportJWK(pair.publicKey), kid: "catalog-key", alg: "RS256", use: "sig" };
  fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected catalog network request");
    return Response.json({ keys: [jwk] });
  });
});
afterAll(() => {
  expect(fetchSpy).toHaveBeenCalledTimes(1);
  vi.restoreAllMocks();
});

it.each([false, true])("preserves the complete tool contracts with writes=%s", async writes => {
  const token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "catalog-key" }).setIssuer(issuer)
    .setAudience("synthetic-catalog").setSubject(crypto.randomUUID())
    .setIssuedAt().setExpirationTime("5m").sign(privateKey);
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json",
      accept: "application/json, text/event-stream", "cf-access-jwt-assertion": token,
      "mcp-protocol-version": "2025-03-26" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
  }), { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "synthetic-catalog", MCP_HOSTNAME: hostname,
    MCP_WRITES_ENABLED: String(writes), GITHUB_REPOSITORY: "synthetic/example",
    GITHUB_ACTIONS_TOKEN: "synthetic-catalog-only" }, ctx);
  const body = await response.text();
  await waitOnExecutionContext(ctx);
  expect(response.status, body).toBe(200);
  const message = response.headers.get("content-type")?.includes("text/event-stream")
    ? JSON.parse(body.split("\n").find(line => line.startsWith("data: "))!.slice(6)) : JSON.parse(body);
  expect(message.error).toBeUndefined();
  const tools = message.result.tools;
  expect(tools.some((tool: { name: string }) => tool.name === "add_coach_profile")).toBe(writes);
  expect(tools.some((tool: { name: string }) => tool.name === "add_activity_context")).toBe(writes);
  // Captured against the pre-refactor Worker: includes order, descriptions,
  // titles, input/output schemas and safety annotations, with no fitness data.
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(tools)));
  const hash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
  expect({ writes, count: tools.length, hash }).toMatchSnapshot();
});
