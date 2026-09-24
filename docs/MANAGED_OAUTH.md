# Cloudflare Access Managed OAuth

Managed OAuth is Slipstream's only supported remote authentication model. For
the complete click-by-click setup, see
[INSTALL.md sections 7–10](INSTALL.md#7-enable-cloudflare-zero-trust-free).

Cloudflare Access performs OAuth discovery, dynamic client registration, PKCE,
login, token renewal and policy enforcement. The Worker separately verifies the
signed `Cf-Access-Jwt-Assertion` using Cloudflare's JWKS and requires:

- an HTTPS issuer under `cloudflareaccess.com`
- the configured application audience
- an RS256 signature
- a non-expired application token
- both a human subject and email identity
- the exact configured public hostname

The canonical endpoint is `/mcp`. Every other path returns 404 from the Worker.
The previous secret-path migration route has been removed.

Recommended personal settings:

- protect the entire Worker, including production and preview URLs
- Allow policy scoped to one exact email
- application/grant session: one week
- access token: default short lifetime (about 15 minutes)
- localhost and loopback clients: off
- only known client redirect URIs

Installation-specific values are stored as Worker secrets:

```bash
npx wrangler secret put ACCESS_TEAM_DOMAIN
npx wrangler secret put ACCESS_AUD
npx wrangler secret put MCP_HOSTNAME
```

Common errors:

- `503 OAuth is not configured`: one or both Worker secrets are missing.
- `421 Misdirected request`: the request hostname differs from `MCP_HOSTNAME`.
- `401 Unauthorized`: assertion missing, expired, signed by another team, or
  issued for another Access audience.
- Redirect URI rejected: copy the client's exact callback into Managed OAuth.
- Login succeeds but access is denied: the identity does not match the Access
  Allow policy.
