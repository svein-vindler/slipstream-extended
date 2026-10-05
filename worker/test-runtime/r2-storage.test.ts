import { env } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { R2Storage, clearSummaryCaches } from "../src/r2-storage";
import { PayloadTooLargeError } from "../src/security";
import contract from "./generated/health-contract.json";

const activityKey = "summary/activities.csv";
const healthKey = "summary/health_daily.csv";
const detailKey = "synthetic-storage/detail.json";
const missingKey = "synthetic-storage/missing.json";
const gets: string[] = [];
const heads: string[] = [];
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return (...args: unknown[]) => {
      if (property === "get") gets.push(String(args[0]));
      if (property === "head") heads.push(String(args[0]));
      return Reflect.apply(value, target, args);
    };
  },
});

async function summaries() {
  await env.SLIPSTREAM_DATA.put(activityKey, "Activity ID,Activity Name,Distance\nsynthetic-1,Invented run,5\n");
  const stored = contract.states.initial[healthKey];
  await env.SLIPSTREAM_DATA.put(healthKey, Uint8Array.from(atob(stored.body), c => c.charCodeAt(0)), {
    httpMetadata: { contentType: stored.contentType, contentEncoding: stored.contentEncoding ?? undefined },
  });
}

beforeEach(() => {
  clearSummaryCaches();
  gets.length = 0;
  heads.length = 0;
  vi.spyOn(console, "error").mockImplementation(() => {});
});
afterEach(async () => {
  clearSummaryCaches();
  vi.restoreAllMocks();
  await env.SLIPSTREAM_DATA.delete([activityKey, healthKey, detailKey]);
});

describe("bounded storage reads in the Workers runtime", () => {
  it("shares parsed summaries across requests while checking both ETags every time", async () => {
    await summaries();
    const first = new R2Storage(bucket);
    expect([first.activityStorage, first.healthStorage]).toEqual(["none", "none"]);
    const activities = await first.getActivities();
    const health = await first.getHealth();
    expect(activities[0]).toMatchObject({ id: "synthetic-1", distanceKm: 5 });
    expect(health.find(row => row.date === "2026-09-30")).toMatchObject({ sleepSeconds: 25200, hrvLastNightAvg: 50 });
    const nextRequest = new R2Storage(bucket);
    expect([nextRequest.activityStorage, nextRequest.healthStorage]).toEqual(["none", "none"]);
    expect(await nextRequest.getActivities()).toEqual(activities);
    expect(await nextRequest.getHealth()).toEqual(health);
    expect([nextRequest.activityStorage, nextRequest.healthStorage]).toEqual(["r2", "r2"]);
    expect(gets).toEqual([activityKey, healthKey]);
    expect(heads).toEqual([activityKey, healthKey, activityKey, healthKey]);
  });

  it("reloads changed activity summaries without downloading unchanged health", async () => {
    await summaries();
    const storage = new R2Storage(bucket);
    await storage.getActivities();
    await storage.getHealth();
    gets.length = 0;
    await env.SLIPSTREAM_DATA.put(activityKey, "Activity ID,Activity Name,Distance\nsynthetic-1,Invented run,6\n");
    const nextRequest = new R2Storage(bucket);
    expect((await nextRequest.getActivities())[0].distanceKm).toBe(6);
    expect((await nextRequest.getActivities())[0].distanceKm).toBe(6);
    await nextRequest.getHealth();
    expect(gets).toEqual([activityKey]);
  });

  it("invalidates both summary caches explicitly after a ready refresh", async () => {
    await summaries();
    const storage = new R2Storage(bucket);
    await storage.getActivities();
    await storage.getHealth();
    gets.length = 0;
    clearSummaryCaches();
    await new R2Storage(bucket).getActivities();
    await new R2Storage(bucket).getHealth();
    expect(gets).toEqual([activityKey, healthKey]);
  });

  it.each(["activity", "health"] as const)("does not serve cached %s data after deletion or a malformed replacement", async stream => {
    await summaries();
    const key = stream === "activity" ? activityKey : healthKey;
    const read = () => {
      const storage = new R2Storage(bucket);
      return stream === "activity" ? storage.getActivities() : storage.getHealth();
    };
    await read();
    await env.SLIPSTREAM_DATA.delete(key);
    await expect(read()).rejects.toThrow(stream === "activity"
      ? "Activity data is unavailable in private R2." : "Health data is unavailable in private R2.");
    await env.SLIPSTREAM_DATA.put(key, "invalid header\n");
    await expect(read()).rejects.toThrow("unavailable in private R2.");
    await env.SLIPSTREAM_DATA.put(key, stream === "activity" ? "Activity ID,Activity Name\n" : "Date,Source\n");
    expect(await read()).toEqual([]);
  });

  it("propagates HEAD failures instead of trusting a warm summary cache", async () => {
    await summaries();
    await new R2Storage(bucket).getHealth();
    gets.length = 0;
    const unavailable = new Proxy(bucket, {
      get(target, property) {
        if (property === "head") return async () => { throw new Error("Synthetic metadata failure"); };
        return Reflect.get(target, property);
      },
    });
    await expect(new R2Storage(unavailable).getHealth()).rejects.toThrow("Health data is unavailable in private R2.");
    expect(gets).toEqual([]);
  });

  it("tries missing aliases in order and returns null when none exist", async () => {
    await env.SLIPSTREAM_DATA.put(detailKey, '{"synthetic":true}');
    const storage = new R2Storage(bucket);
    expect(await storage.getR2Json([missingKey, detailKey])).toEqual({ key: detailKey, data: { synthetic: true } });
    expect(gets).toEqual([missingKey, detailKey]);
    expect(await storage.getR2Json([missingKey])).toBeNull();
  });

  it("rejects oversized stored objects and enforces exact plain-text bounds", async () => {
    await env.SLIPSTREAM_DATA.put(detailKey, "abcdef");
    const storage = new R2Storage(bucket);
    await expect(storage.getR2Text([detailKey], { stored: 5, decoded: 6 })).rejects.toThrow("stored-size limit");
    await expect(storage.getR2Text([detailKey], { stored: 6, decoded: 5 })).rejects.toBeInstanceOf(PayloadTooLargeError);
    expect(await storage.getR2Text([detailKey], { stored: 6, decoded: 6 })).toMatchObject({ text: "abcdef", key: detailKey });
  });

  it("enforces decoded gzip bounds and does not skip a corrupt existing alias", async () => {
    const compressed = await new Response(new Response("abcdef").body!
      .pipeThrough(new CompressionStream("gzip"))).arrayBuffer();
    await env.SLIPSTREAM_DATA.put(detailKey, compressed, { httpMetadata: { contentEncoding: "gzip" } });
    const storage = new R2Storage(bucket);
    await expect(storage.getR2Text([detailKey], { stored: compressed.byteLength, decoded: 5 })).rejects.toBeInstanceOf(PayloadTooLargeError);
    expect(await storage.getR2Text([detailKey], { stored: compressed.byteLength, decoded: 6 })).toMatchObject({ text: "abcdef" });
    await env.SLIPSTREAM_DATA.put(healthKey, "invalid JSON");
    await expect(storage.getR2Json([healthKey, detailKey])).rejects.toThrow();
    await env.SLIPSTREAM_DATA.put(detailKey, new Uint8Array([0x1f, 0x8b, 0x00]));
    await expect(storage.getR2Text([detailKey])).rejects.toThrow();
  });
});
