import fs from "node:fs";
import { NightSignalsTools } from "../worker/src/night-signals";
import { NightProductsTools } from "../worker/src/night-products";

const input = JSON.parse(fs.readFileSync(0, "utf8"));
let handler: any;
const bucket = {
  get: async (key: string) => {
    const item = input.objects[key];
    if (item) {
      const body = Buffer.from(item.body, "base64");
      return { size: body.length, etag: item.revision.slice(1, -1), body: new Response(body).body };
    }
    if (key === input.baseline?.storage_key) {
      const body = new TextEncoder().encode(JSON.stringify(input.baseline.product));
      return { size: body.length, etag: "synthetic-product", body: new Response(body).body };
    }
    return null;
  },
  list: async ({ prefix }: any) => ({
    objects: Object.keys(input.objects).filter(key => key.startsWith(prefix)).map(key => ({ key })),
    truncated: false,
  }),
};
new NightSignalsTools({ SLIPSTREAM_DATA: bucket, HEALTH_TIMEZONE: input.timezone } as any).registerTools({
  registerTool: (name: string, _schema: any, callback: any) => {
    if (name === "night_signals") handler = callback;
  },
} as any);
handler({
  date: input.day, resolution: "native", alignment: "sleep_start", channels: ["heart_rate", "hrv"],
}, { mcpReq: { id: 1 } }).then(async (result: any) => {
  const value = result.structuredContent;
  delete value.resources;
  delete value.view;
  if (input.baseline) {
    new NightProductsTools({ SLIPSTREAM_DATA: bucket } as any).registerTools({
      registerTool: (_name: string, _schema: any, callback: any) => { handler = callback; },
    } as any);
    const product = await handler({ ...input.baseline.product.args, period: "7" }, { mcpReq: { id: 2 } });
    console.log(JSON.stringify({ night: value, product: product.structuredContent }));
  } else console.log(JSON.stringify(value));
}).catch((error: any) => {
  console.error(error.constructor.name);
  process.exitCode = 1;
});
