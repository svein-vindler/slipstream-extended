import { AccessConfigurationError, verifyAccessRequest } from "./access";
import { secureResponse } from "./security";

export type McpAuthentication = { rateLimitKey: string; hostname: string };

export function normalizeMcpHostname(value: string | undefined): string {
  const candidate = value?.trim().toLowerCase();
  if (!candidate) throw new AccessConfigurationError("MCP_HOSTNAME is required.");
  if (candidate.includes("://") || candidate.includes("/") || candidate.includes("@")) {
    throw new AccessConfigurationError("MCP_HOSTNAME must contain only a hostname.");
  }
  const parsed = new URL(`https://${candidate}`);
  if (parsed.hostname !== candidate || parsed.port) {
    throw new AccessConfigurationError("MCP_HOSTNAME must contain only a hostname.");
  }
  return candidate;
}

export async function authenticateMcpRequest(
  request: Request,
  env: Env,
): Promise<McpAuthentication | Response> {
  const url = new URL(request.url);
  if (url.pathname === "/mcp") {
    try {
      const hostname = normalizeMcpHostname(env.MCP_HOSTNAME);
      if (url.hostname.toLowerCase() !== hostname) {
        return secureResponse(new Response("Misdirected request", { status: 421 }));
      }
      const identity = await verifyAccessRequest(request, {
        teamDomain: env.ACCESS_TEAM_DOMAIN,
        audience: env.ACCESS_AUD,
      });
      return { rateLimitKey: `oauth:${identity.subject}`, hostname };
    } catch (error) {
      if (error instanceof AccessConfigurationError) {
        console.error(JSON.stringify({ message: error.message }));
        return secureResponse(new Response("OAuth is not configured", { status: 503 }));
      }
      console.warn(JSON.stringify({
        message: "Cloudflare Access authentication failed",
        error: error instanceof Error ? error.message : String(error),
      }));
      return secureResponse(new Response("Unauthorized", { status: 401 }));
    }
  }

  return secureResponse(new Response("Not found", { status: 404 }));
}

