// Offline warm-Node measurements; in-memory byte streams, no external services.
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { createServer } from "vite";

const vite = await createServer({ configFile: false, server: { middlewareMode: true, watch: null }, appType: "custom" });
try {
  const { readTrainingHistory, boundedTrainingResult, trainingDates } = await vite.ssrLoadModule("/src/training-history.ts");
  const fixtures = JSON.parse(readFileSync("test-runtime/generated/training-day.json", "utf8"));
  const cases = [];
  for (const [days, multiple, missing, distinct] of [[1, false, false, false], [31, false, false, false],
    [31, true, false, false], [31, false, true, false], [31, false, false, true], [31, true, false, true]]) {
    const dates = trainingDates("2026-10-01", `2026-10-${String(days).padStart(2, "0")}`);
    const data = dates.map(date => {
      const value = structuredClone(fixtures.days[date][distinct ? multiple ? "multiple_dual" : "dual" : multiple ? "multiple" : "single"]);
      return new TextEncoder().encode(JSON.stringify(value));
    });
    let gets = 0, bytes = 0, active = 0, peak = 0;
    const bucket = { get: async key => {
      gets++; if (missing) return null;
      active++; peak = Math.max(peak, active);
      const index = dates.findIndex(date => key.endsWith(`${date}.json`));
      const value = data[index]; let offset = 0, closed = false;
      const finish = () => { if (!closed) { active--; closed = true; } };
      const body = new ReadableStream({ type: "bytes", pull(controller) {
        const request = controller.byobRequest;
        const length = Math.min(request.view.byteLength, value.length - offset);
        if (length) { request.view.set(value.subarray(offset, offset + length)); offset += length; bytes += length; request.respond(length); }
        if (offset === value.length) { controller.close(); if (!length) request.respond(0); finish(); }
      }, cancel: finish });
      return { size: value.length, body, httpMetadata: {} };
    } };
    const times = []; let output;
    for (let sample = 0; sample < 100; sample++) {
      gets = bytes = active = peak = 0;
      const began = performance.now();
      output = boundedTrainingResult(await readTrainingHistory(bucket, dates[0], dates.at(-1), Date.parse("2026-11-01T10:00:00Z")));
      times.push(performance.now() - began);
    }
    const encoder = new TextEncoder();
    cases.push({ requested_dates: days, devices_per_day: multiple ? 4 : 1, missing, distinct_morning_latest: distinct,
      before: { tool_registered: false, get: 0, head: 0, list: 0, put: 0, fitness: 0, bytes: 0 },
      after: { get: gets, head: 0, list: 0, put: 0, fitness: 0, body_bytes: bytes, peak_active_readers: peak,
        structured_bytes: encoder.encode(JSON.stringify(output.structuredContent)).length,
        result_bytes: encoder.encode(JSON.stringify(output)).length, status: output.structuredContent.status,
        days_returned: output.structuredContent.days.length },
      wall_ms_mean: Number((times.reduce((a, b) => a + b, 0) / times.length).toFixed(4)),
      wall_ms_median: Number(times.toSorted((a, b) => a - b)[50].toFixed(4)) });
  }
  console.log(JSON.stringify({ synthetic: true, samples_per_case: 100, node: process.version,
    queue_startup_auth_network_retries_included: false, cpu_is_cloudflare_billed: false, cases }, null, 2));
} finally { await vite.close(); }
