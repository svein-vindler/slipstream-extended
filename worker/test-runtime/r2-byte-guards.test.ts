import { env } from "cloudflare:test";
import { afterEach, expect, it } from "vitest";
import {
  COACH_CONFIG_R2_LIMITS, GRANULAR_R2_LIMITS, HISTORY_INDEX_R2_LIMITS,
  SUMMARY_R2_LIMITS, R2Storage,
} from "../src/r2-storage";
import { PayloadTooLargeError } from "../src/security";

const key = "synthetic-byte-guards/object";
afterEach(async () => { await env.SLIPSTREAM_DATA.delete(key); });

it("rejects actual stored overflow despite smaller metadata and stops the source", async ({ task }) => {
  await env.SLIPSTREAM_DATA.put(key, "synthetic placeholder");
  let bytes = 0, canceled = 0, buffered = 0, gets = 0, chunk = 0;
  const lengths = [65536, 65536, 65536, 65536, 1, 65536];
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      // One-byte excess followed by another chunk: the tail must stay unread.
      if (chunk === lengths.length) { controller.close(); return; }
      const length = lengths[chunk++];
      bytes += length;
      controller.enqueue(new Uint8Array(length).fill(32));
    },
    cancel() { canceled += 1; },
  }, { highWaterMark: 0 });
  const response = new Response(stream);
  const bucket = new Proxy(env.SLIPSTREAM_DATA, {
    get(target, property) {
      if (property === "get") return async () => {
        gets += 1;
        const object = (await target.get(key))!;
        await object.body.cancel();
        return new Proxy(object, {
          get(value, field) {
            if (field === "size") return 1;
            if (field === "body") return stream;
            if (field === "arrayBuffer") return () => { buffered += 1; return response.arrayBuffer(); };
            return Reflect.get(value, field, value);
          },
        });
      };
      return Reflect.get(target, property, target);
    },
  });
  let failure: unknown;
  try { await new R2Storage(bucket).getR2Text([key], COACH_CONFIG_R2_LIMITS); }
  catch (error) { failure = error; }
  task.meta.byteGuard = { bytes, canceled, buffered, gets, locked: stream.locked };
  expect(failure).toBeInstanceOf(PayloadTooLargeError);
  expect({ bytes, canceled, buffered, gets, locked: stream.locked }).toEqual({
    bytes: 262145, canceled: 1, buffered: 0, gets: 1, locked: false,
  });
});

for (const [name, limits] of [
  ["summary", SUMMARY_R2_LIMITS], ["granular", GRANULAR_R2_LIMITS],
  ["history index", HISTORY_INDEX_R2_LIMITS], ["coach config", COACH_CONFIG_R2_LIMITS],
] as const) it(`preserves exact stored/decoded JSON and rejects decoded +1 for ${name}`, async ({ task }) => {
  const data = { synthetic: 0, missing: null };
  const json = JSON.stringify(data);
  let gets = 0;
  const bucket = new Proxy(env.SLIPSTREAM_DATA, {
    get(target, property) {
      if (property === "get") return async (requested: string) => { gets += 1; return target.get(requested); };
      return Reflect.get(target, property, target);
    },
  });
  const storage = new R2Storage(bucket);
  const measurements = [{ phase: "stored_exact", stored_bytes: limits.stored, decoded_input_bytes: limits.stored }];
  await env.SLIPSTREAM_DATA.put(key, json.padEnd(limits.stored));
  const exactStored = await storage.getR2JsonWithMetadata([key], limits);
  expect(exactStored).toMatchObject({ key, data });
  expect(exactStored!.uploaded).toBeInstanceOf(Date);
  for (const excess of [0, 1]) {
    const compressed = await new Response(new Response(json.padEnd(limits.decoded + excess)).body!
      .pipeThrough(new CompressionStream("gzip"))).arrayBuffer();
    expect(compressed.byteLength).toBeLessThan(limits.stored);
    await env.SLIPSTREAM_DATA.put(key, compressed, { httpMetadata: { contentEncoding: "gzip" } });
    if (excess) await expect(storage.getR2Json([key], limits)).rejects.toBeInstanceOf(PayloadTooLargeError);
    else expect(await storage.getR2Json([key], limits)).toEqual({ key, data });
    measurements.push({ phase: excess ? "decoded_overflow" : "decoded_exact",
      stored_bytes: compressed.byteLength, decoded_input_bytes: limits.decoded + excess });
  }
  task.meta.byteGuard = { name, gets, measurements };
});

it("keeps missing, corrupt JSON/gzip, and provider failure distinct without alias fallback", async ({ task }) => {
  const seen: string[] = [];
  let gets = 0;
  const bucket = new Proxy(env.SLIPSTREAM_DATA, {
    get(target, property) {
      if (property === "get") return async (requested: string) => {
        gets += 1;
        seen.push(requested);
        if (requested === "synthetic-byte-guards/provider") throw new Error("synthetic provider failure");
        return target.get(requested);
      };
      return Reflect.get(target, property, target);
    },
  });
  const storage = new R2Storage(bucket);
  expect(await storage.getR2Json([key])).toBeNull();
  for (const corrupt of [new TextEncoder().encode("invalid JSON"), new Uint8Array([31, 139, 0])]) {
    seen.length = 0;
    await env.SLIPSTREAM_DATA.put(key, corrupt);
    await expect(storage.getR2Json([key, "synthetic-byte-guards/alias"])).rejects.not.toBeInstanceOf(PayloadTooLargeError);
    expect(seen).toEqual([key]);
  }
  await expect(storage.getR2Json(["synthetic-byte-guards/provider"]))
    .rejects.toThrow("synthetic provider failure");
  task.meta.byteGuard = { gets };
});

for (const cancelFails of [false, true]) it(`cancels declared oversize before pulling, including cancel failure=${cancelFails}`, async ({ task }) => {
  await env.SLIPSTREAM_DATA.put(key, "synthetic placeholder");
  let pulls = 0, cancels = 0, gets = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull() { pulls += 1; },
    cancel() { cancels += 1; if (cancelFails) throw new Error("synthetic cancel failure"); },
  }, { highWaterMark: 0 });
  const bucket = new Proxy(env.SLIPSTREAM_DATA, {
    get(target, property) {
      if (property === "get") return async () => {
        gets += 1;
        const object = (await target.get(key))!;
        await object.body.cancel();
        return new Proxy(object, {
          get(value, field) {
            if (field === "size") return COACH_CONFIG_R2_LIMITS.stored + 1;
            if (field === "body") return stream;
            return Reflect.get(value, field, value);
          },
        });
      };
      return Reflect.get(target, property, target);
    },
  });
  await expect(new R2Storage(bucket).getR2Text([key], COACH_CONFIG_R2_LIMITS))
    .rejects.toBeInstanceOf(PayloadTooLargeError);
  task.meta.byteGuard = { pulls, cancels, gets, locked: stream.locked };
  expect({ pulls, cancels, locked: stream.locked }).toEqual({ pulls: 0, cancels: 1, locked: false });
});
