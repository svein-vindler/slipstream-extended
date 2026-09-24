import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";

export interface AccessConfig {
  teamDomain?: string;
  audience?: string;
}

export interface AccessIdentity {
  subject: string;
  email: string;
}

export class AccessConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AccessConfigurationError";
  }
}

const jwksByIssuer = new Map<string, ReturnType<typeof createRemoteJWKSet>>();

export function normalizeAccessTeamDomain(value: string): string {
  const candidate = value.includes("://") ? value : `https://${value}`;
  const url = new URL(candidate);
  if (url.protocol !== "https:" || url.username || url.password
    || url.search || url.hash || (url.pathname !== "/" && url.pathname !== "")
    || !url.hostname.endsWith(".cloudflareaccess.com")) {
    throw new AccessConfigurationError(
      "ACCESS_TEAM_DOMAIN must be a Cloudflare Access HTTPS origin.",
    );
  }
  return url.origin;
}

function getJwks(issuer: string): ReturnType<typeof createRemoteJWKSet> {
  let jwks = jwksByIssuer.get(issuer);
  if (!jwks) {
    jwks = createRemoteJWKSet(new URL(`${issuer}/cdn-cgi/access/certs`));
    jwksByIssuer.set(issuer, jwks);
  }
  return jwks;
}

function identityFromPayload(payload: JWTPayload): AccessIdentity {
  if (payload.type !== "app" || typeof payload.sub !== "string" || !payload.sub
    || typeof payload.email !== "string" || !payload.email) {
    throw new Error("Cloudflare Access token does not contain a user identity.");
  }
  return { subject: payload.sub, email: payload.email };
}

export async function verifyAccessRequest(
  request: Request,
  config: AccessConfig,
): Promise<AccessIdentity> {
  if (!config.teamDomain || !config.audience) {
    throw new AccessConfigurationError(
      "Cloudflare Access is not configured on this Worker.",
    );
  }

  const token = request.headers.get("cf-access-jwt-assertion");
  if (!token) throw new Error("Missing Cloudflare Access assertion.");

  const issuer = normalizeAccessTeamDomain(config.teamDomain);
  const { payload } = await jwtVerify(token, getJwks(issuer), {
    issuer,
    audience: config.audience,
    algorithms: ["RS256"],
  });
  return identityFromPayload(payload);
}
