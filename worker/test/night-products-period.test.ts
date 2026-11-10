/** Registered MCP boundary, synthetic stored products, no provider access. */
import { createHash } from "node:crypto";
import { Client, InMemoryTransport } from "@modelcontextprotocol/client";
import { McpServer } from "@modelcontextprotocol/server";
import { expect, it, vi } from "vitest";
import { NightProductsTools } from "../src/night-products";

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .sort(([a], [b]) => a.localeCompare(b)).map(([key, child]) => [key, canonical(child)]));
  return value;
}
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex");

function storedProduct(channel: "heart_rate" | "hrv", start: string, end: string) {
  const args = { start_date: start, end_date: end, alignment: "sleep_start", bin_minutes: 30,
    channel, exclude_dates: [], context_filter: {} };
  const manifest = [{ wake_date: end, snapshot_sha256: "a".repeat(64), source_revisions: [], context_id: null }];
  const excluded = Array.from({ length: 29 }, (_, index) => ({
    wake_date: new Date(Date.parse(`${start}T00:00:00Z`) + index * 86400000).toISOString().slice(0, 10),
    reason: "not_in_export",
  }));
  const product = { schema_version: 1, kind: "night-baseline", method_version: "night-baseline-v1", args,
    args_sha256: hash(args), source_identity: hash(manifest), source_manifest: manifest,
    nights_requested: 30, source_nights: 1, contributing_nights: 1, unit: channel === "hrv" ? "ms" : "bpm",
    excluded, method: { within_night: "median", quantiles: "linear-(n-1)p", one_contribution_per_night_bin: true },
    bins: [{ bin_index: 0, offset_minutes: 0, p10: 50, p25: 50, p50: 50, p75: 50, p90: 50, nights: 1, valid_points: 1 }],
    quality_notes: [{ wake_date: end, flags: ["partial_source"] }],
    resources: { input_bytes: 100, input_points: 1, invalid_points: 0 } };
  return { key: `health/night-products/v1/baseline/${end.replaceAll("-", "/")}/${hash(args)}.json`, product };
}

it.each(["heart_rate", "hrv"] as const)("registered %s 30-day alias matches custom across New Year and preserves previous coverage", async channel => {
  const current = storedProduct(channel, "2025-12-17", "2026-01-15");
  const previous = storedProduct(channel, "2025-11-17", "2025-12-16");
  const objects = new Map([current, previous].map(({ key, product }) => [key, JSON.stringify(product)]));
  const forbidden = vi.fn(() => { throw new Error("Unexpected non-read operation"); });
  const get = vi.fn(async (key: string) => {
    const text = objects.get(key);
    return text === undefined ? null : { size: Buffer.byteLength(text), etag: "synthetic", body: new Response(text).body };
  });
  const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(forbidden);
  const server = new McpServer({ name: "night-period", version: "1" });
  const client = new Client({ name: "night-period-client", version: "1" });
  new NightProductsTools({ SLIPSTREAM_DATA: { get, put: forbidden, delete: forbidden, list: forbidden, head: forbidden } } as unknown as Env)
    .registerTools(server);
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  try {
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)]);
    const tool = (await client.listTools()).tools.find(tool => tool.name === "night_baseline")!;
    const input = tool.inputSchema.properties as Record<string, { enum?: string[]; default?: unknown }>;
    expect(input.period.enum).toEqual(["7", "28", "30", "90", "365", "custom"]);
    expect(input.period.default).toBe("28");
    expect(input.channel.default).toBe("hrv");
    const read = async (period: "30" | "custom") => {
      const response = await client.callTool({ name: "night_baseline", arguments: {
        channel, end_date: "2026-01-15", period, compare: "previous",
        ...(period === "custom" ? { start_date: "2025-12-17" } : {}),
      } });
      expect(response.isError).not.toBe(true);
      const value = response.structuredContent!;
      expect(JSON.parse((response.content as Array<{ text: string }>)[0].text)).toEqual(value);
      return value;
    };
    const alias = await read("30");
    const custom = await read("custom");
    for (const key of ["status", "method_version", "current", "reference", "quality_flags"]) expect(alias[key]).toEqual(custom[key]);
    expect(alias).toMatchObject({ status: "available", method_version: "night-baseline-v1",
      current: { window: { start_date: "2025-12-17", end_date: "2026-01-15" }, nights_requested: 30,
        source_nights: 1, contributing_nights: 1, quality_notes: [{ wake_date: "2026-01-15", flags: ["partial_source"] }] },
      reference: { window: { start_date: "2025-11-17", end_date: "2025-12-16" }, nights_requested: 30 },
      resources: { r2_get_attempts: 2 } });
    expect((alias.current as { excluded: unknown[] }).excluded).toHaveLength(29);
    expect(get.mock.calls.map(([key]) => key)).toEqual([current.key, previous.key, current.key, previous.key]);
    objects.delete(previous.key);
    expect(await read("30")).toMatchObject({ status: "partial", current: { status: "available" },
      reference: { status: "not_stored", window: { start_date: "2025-11-17", end_date: "2025-12-16" } },
      quality_flags: ["reference_unavailable"], resources: { r2_get_attempts: 2 } });
    expect(forbidden).not.toHaveBeenCalled();
  } finally {
    await client.close();
    await server.close();
    fetch.mockRestore();
  }
});
