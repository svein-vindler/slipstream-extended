// Worker secrets merge into the binding types generated from wrangler.jsonc.
interface Env {
  /** Cloudflare Access team origin, for example https://team.cloudflareaccess.com. */
  ACCESS_TEAM_DOMAIN?: string;
  /** Audience tag of the Access application protecting this Worker. */
  ACCESS_AUD?: string;
  /** Exact public hostname accepted by the MCP endpoint. */
  MCP_HOSTNAME?: string;
  /** Set to the literal value true to expose append-only MCP write tools. */
  MCP_WRITES_ENABLED?: string;
  /** Exact accepted writes per authenticated identity and UTC day. */
  MCP_WRITE_DAILY_LIMIT: string;
  /** Fine-grained, single-repository token with Actions read/write only. */
  GITHUB_ACTIONS_TOKEN?: string;
  /** owner/repository used only when on-demand refresh is enabled. */
  GITHUB_REPOSITORY?: string;
}
