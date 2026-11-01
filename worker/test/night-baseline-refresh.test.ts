/** Mandatory Python/actual Worker domain parity in the existing Node unit job. */
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { NightSignalsTools } from "../src/night-signals";
import { NightProductsTools } from "../src/night-products";

const root = fileURLToPath(new URL("../../", import.meta.url));
const fixtures = JSON.parse(execFileSync(
  process.env.SLIPSTREAM_TEST_PYTHON || "python",
  ["scripts/build_night_baseline_contract.py"],
  { cwd: root, encoding: "utf8" },
));
for (const fixture of fixtures) {
  it(`automatic references preserve native Worker contract: ${fixture.name}`, async () => {
    let handler: any;
    const bucket = {
      get: async (key: string) => {
        let raw: Uint8Array, etag: string;
        const item = fixture.objects[key];
        if (item) {
          raw = Buffer.from(item.body, "base64");
          etag = item.revision.slice(1, -1);
        } else if (key === fixture.baseline.storage_key) {
          raw = new TextEncoder().encode(JSON.stringify(fixture.baseline.product));
          etag = "synthetic-product";
        } else return null;
        return { size: raw.length, etag, body: new Response(raw).body };
      },
      list: async ({ prefix }: any) => ({
        objects: Object.keys(fixture.objects).filter(key => key.startsWith(prefix)).map(key => ({ key })),
        truncated: false,
      }),
    };
    const env = { SLIPSTREAM_DATA: bucket, HEALTH_TIMEZONE: fixture.timezone } as unknown as Env;
    new NightSignalsTools(env).registerTools({
      registerTool: (name: string, _schema: any, callback: any) => {
        if (name === "night_signals") handler = callback;
      },
    } as any);
    const response = await handler({
      date: fixture.day, resolution: "native", alignment: "sleep_start", channels: ["heart_rate", "hrv"],
    }, { mcpReq: { id: 1 } });
    const { resources: _resources, view: _view, ...actual } = response.structuredContent;
    expect(actual).toEqual(fixture.night);
    new NightProductsTools(env).registerTools({
      registerTool: (_name: string, _schema: any, callback: any) => { handler = callback; },
    } as any);
    const product = await handler({ ...fixture.baseline.product.args, period: "7" }, { mcpReq: { id: 2 } });
    expect(product.structuredContent.status).toBe("available");
    expect(product.structuredContent.current.source_identity).toBe(fixture.baseline.product.source_identity);
    expect(product.structuredContent.current.bins).toEqual(fixture.baseline.product.bins);
  });
}
