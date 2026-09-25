import { describe, expect, it } from "vitest";
import { z } from "zod";
import { Client, InMemoryTransport } from "@modelcontextprotocol/client";
import { McpServer } from "@modelcontextprotocol/server";
import { outputSchemas, structuredToolResult } from "../src/mcp-output";

const TOOL_NAMES = [
  "data_status",
  "list_activities",
  "activity_stats",
  "personal_bests",
  "list_sport_types",
  "search_activities",
  "health_status",
  "daily_health",
  "health_trends",
  "hrv_curve",
  "hrv_history",
  "sleep_detail",
  "sleep_history",
  "body_composition",
  "strength_session",
  "endurance_session",
  "coach_profile",
  "add_coach_profile",
  "add_activity_context",
  "coach_input",
  "refresh_today",
  "refresh_status",
] as const;

describe("MCP structured outputs", () => {
  it("defines a JSON object output schema for every tool", () => {
    expect(Object.keys(outputSchemas).sort()).toEqual([...TOOL_NAMES].sort());
    for (const name of TOOL_NAMES) {
      expect(z.toJSONSchema(outputSchemas[name])).toMatchObject({ type: "object" });
    }
  });

  it("advertises every output schema through the installed MCP SDK", async () => {
    const server = new McpServer({ name: "schema-test", version: "1" });
    for (const name of TOOL_NAMES) {
      server.registerTool(name, { outputSchema: outputSchemas[name] }, async () =>
        structuredToolResult({ ok: true }));
    }
    const client = new Client({ name: "schema-test-client", version: "1" });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
    const tools = await client.listTools();
    expect(tools.tools).toHaveLength(TOOL_NAMES.length);
    expect(tools.tools.filter((tool) => !tool.outputSchema).map((tool) => tool.name)).toEqual([]);
    await client.close();
    await server.close();
  });

  it("returns matching text and structured content", () => {
    const value = { connected: true, count: 42 };
    const result = structuredToolResult(value);
    expect(result.structuredContent).toEqual(value);
    expect(JSON.parse(result.content[0].text)).toEqual(value);
  });

  it("advertises local night semantics on every sleep/overnight HRV output", () => {
    for (const name of ["daily_health", "sleep_detail", "sleep_history",
      "hrv_curve", "hrv_history"] as const) {
      const schema = JSON.stringify(z.toJSONSchema(outputSchemas[name]));
      expect(schema).toContain('"wake_date"');
      expect(schema).toContain('"night_of"');
      expect(schema).toContain('"timezone"');
      expect(schema).toContain('"local_time_source"');
    }
    expect(JSON.stringify(z.toJSONSchema(outputSchemas.sleep_history)))
      .toContain('"by_night_of_weekday"');
  });
});
