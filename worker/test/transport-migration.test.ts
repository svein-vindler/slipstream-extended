import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const source = readFileSync(
  fileURLToPath(new URL("../src/index.ts", import.meta.url)),
  "utf8",
);
const config = readFileSync(
  fileURLToPath(new URL("../wrangler.jsonc", import.meta.url)),
  "utf8",
);

describe("stateless MCP transport migration", () => {
  it("uses the MCP SDK v2 stateless handler only", () => {
    expect(source).toContain('from "agents/mcp/server"');
    expect(source).toContain('from "@modelcontextprotocol/server"');
    expect(source).toContain('legacy: "stateless"');
    expect(source).not.toContain("McpAgent");
    expect(source).not.toContain("serveSSE");
    expect(source).not.toContain("@modelcontextprotocol/sdk/");
  });

  it("removes the MCP Durable Object binding through an additive migration", () => {
    const parsed = JSON.parse(config) as {
      durable_objects: { bindings: Array<{ name: string }> };
      migrations: Array<{ deleted_classes?: string[] }>;
    };
    expect(parsed.durable_objects.bindings.map((binding) => binding.name))
      .not.toContain("MCP_OBJECT");
    expect(parsed.migrations.some((migration) =>
      migration.deleted_classes?.includes("FitnessMCP"))).toBe(true);
  });
});
