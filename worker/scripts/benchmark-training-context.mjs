/** Offline paired benchmark of the actual baseline/current detail handlers. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { build } from "esbuild";

const root = fileURLToPath(new URL("../../", import.meta.url));
const workerRoot = resolve(root, "worker");
const baseArg = process.argv[2];
if (!baseArg || !/^[a-f0-9]{40}$/.test(baseArg)) throw new Error("Supply the verified full baseline SHA.");
const git = args => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
const base = git(["rev-parse", "--verify", `${baseArg}^{commit}`]);
assert.equal(base, baseArg);
// Baseline handler imports may use current versions only of unchanged helpers.
assert.equal(git(["diff", base, "--", "worker/src/lib.ts", "worker/src/r2-storage.ts", "worker/src/security.ts"]), "");
const scratch = resolve(workerRoot, "test-runtime/generated/training-benchmark");
await mkdir(scratch, { recursive: true });
const previous = git(["show", `${base}:worker/src/activity-coach-tools.ts`])
  .replaceAll(/from "\.\/([^\"]+)"/g, 'from "../../../src/$1"');
await writeFile(resolve(scratch, "baseline.ts"), previous);
const entry = `
export { ActivityCoachTools as Current } from "../../../src/activity-coach-tools";
export { ActivityCoachTools as Baseline } from "./baseline";
export { R2Storage, clearSummaryCaches } from "../../../src/r2-storage";
export { trainingContext } from "../../../src/training-context";
`;
await writeFile(resolve(scratch, "entry.ts"), entry);
await build({ entryPoints: [resolve(scratch, "entry.ts")], outfile: resolve(scratch, "bundle.mjs"),
  bundle: true, platform: "node", format: "esm", target: "node22", logLevel: "silent" });
const { Current, Baseline, R2Storage, clearSummaryCaches, trainingContext } =
  await import(pathToFileURL(resolve(scratch, "bundle.mjs")).href);
const fixture = JSON.parse(await readFile(resolve(workerRoot, "test-runtime/generated/training-context.json"), "utf8"));
// Fail on unexpected network use; no retries, queue, cold startup or live latency.
globalThis.fetch = () => { throw new Error("Benchmark must remain offline"); };

function counter() {
  const counts = { Garmin: 0, GET: 0, HEAD: 0, LIST: 0, PUT: 0, download_bytes: 0 };
  const bucket = {
    head: async key => { counts.HEAD++; return fixture[key] ? { etag: key } : null; },
    get: async key => {
      counts.GET++;
      const object = fixture[key];
      if (!object) return null;
      const bytes = Buffer.from(object.body, "base64");
      counts.download_bytes += bytes.length;
      return { size: bytes.length, etag: key,
        arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) };
    },
    list: () => { counts.LIST++; throw new Error("Unexpected LIST"); },
    put: () => { counts.PUT++; throw new Error("Unexpected PUT"); },
  };
  return { bucket, counts };
}
async function sample(Class, tool, cold) {
  const { bucket, counts } = counter();
  const storage = new R2Storage(bucket);
  if (cold) clearSummaryCaches();
  else { await storage.getActivities(); Object.keys(counts).forEach(k => { counts[k] = 0; }); }
  const handlers = {};
  const server = { registerTool: (name, _options, handler) => { handlers[name] = handler; } };
  const service = new Class({ SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false" }, "synthetic", storage);
  service.registerSummaryTools(server);
  service.registerDetailTools(server);
  const startCpu = process.cpuUsage();
  const start = performance.now();
  const response = await handlers[tool](tool === "list_activities"
    ? { limit: 20, sort: "date_desc" } : { activity_id: "42" });
  const bytes = Buffer.byteLength(JSON.stringify(response));
  const ms = performance.now() - start;
  const cpu = process.cpuUsage(startCpu);
  return { response, counts, bytes, ms, cpu_ms: (cpu.user + cpu.system) / 1000 };
}
const result = { baseline: base, synthetic: true, iterations: 100,
  timing_scope: "Warm Node handler + bounded read/gzip/JSON + MCP result serialization; in-memory R2. No network/retries/queue/startup.",
  stored_bytes_delta: 0, raw_fit_decode_ms_delta: 0, pairs: {} };
for (const tool of ["endurance_session", "strength_session", "list_activities"]) {
  for (const cold of [true, false]) {
    for (let i = 0; i < 5; i++) { await sample(Baseline, tool, cold); await sample(Current, tool, cold); }
    const samples = { before: [], after: [] };
    for (let i = 0; i < result.iterations; i++) {
      // Alternate order to reduce warmup/order bias on the same exact bytes.
      for (const when of i % 2 ? ["after", "before"] : ["before", "after"]) {
        samples[when].push(await sample(when === "before" ? Baseline : Current, tool, cold));
      }
    }
    const old = samples.before[0].response.structuredContent;
    const current = structuredClone(samples.after[0].response.structuredContent);
    delete current.training_context;
    assert.deepEqual(current, old);
    const summarize = rows => ({ operations: rows[0].counts, response_bytes: rows[0].bytes,
      mean_wall_ms: rows.reduce((sum, r) => sum + r.ms, 0) / rows.length,
      mean_process_cpu_ms: rows.reduce((sum, r) => sum + r.cpu_ms, 0) / rows.length });
    result.pairs[tool + (cold ? "_cold_summary" : "_warm_summary")] = {
      before: summarize(samples.before), after: summarize(samples.after) };
  }
}
const { gunzipSync } = await import("node:zlib");
const storedFit = Buffer.from(fixture["activities/2026/42/activity.v1.json"].body, "base64");
const decoded = JSON.parse(gunzipSync(storedFit));
const activity = { id: "garmin-42", source: "garmin", date: new Date("2026-10-07T23:30:00Z") };
const startCpu = process.cpuUsage();
const startProjection = performance.now();
for (let i = 0; i < 10000; i++) trainingContext(activity, decoded);
const cpuProjection = process.cpuUsage(startCpu);
result.projection = { iterations: 10000, mean_wall_ms: (performance.now() - startProjection) / 10000,
  mean_process_cpu_ms: (cpuProjection.user + cpuProjection.system) / 1000 / 10000,
  fit_stored_bytes: storedFit.length, fit_decoded_bytes: gunzipSync(storedFit).length,
  record_count: decoded.messages.record_mesgs.length };
console.log(JSON.stringify(result, null, 2));
