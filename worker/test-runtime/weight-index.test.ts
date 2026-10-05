import { env } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { R2Storage } from "../src/r2-storage";
import { WeightHistoryReader } from "../src/weight-history-reader";
import { resolveWeightRequest } from "../src/weight-history";
import contract from "./generated/weight-contract.json";

const reads: string[] = [];
const writes: string[] = [];
const bucket = new Proxy(env.SLIPSTREAM_DATA, {
  get(target, property) {
    const value = Reflect.get(target, property);
    if (typeof value !== "function") return value;
    return (...args: unknown[]) => {
      if (property === "get") reads.push(String(args[0]));
      if (property === "put" || property === "delete") writes.push(String(args[0]));
      return Reflect.apply(value, target, args);
    };
  },
});
async function load(state: typeof contract.states.initial) {
  for (const key of Object.keys(contract.states.initial)) await env.SLIPSTREAM_DATA.delete(key);
  for (const [key, value] of Object.entries(state)) {
    const result = await env.SLIPSTREAM_DATA.put(key, Uint8Array.from(atob(value.body), c => c.charCodeAt(0)));
    expect(result.etag).toBe(value.etag);
  }
  reads.length = writes.length = 0;
}
function reader(day: string, start = "04:00") {
  return new WeightHistoryReader(bucket, new R2Storage(bucket)).read(
    resolveWeightRequest(day, day, start, "12:00", undefined, "Europe/Oslo"));
}
afterEach(async () => {
  expect(writes).toEqual([]);
  for (const key of Object.keys(contract.states.initial)) await env.SLIPSTREAM_DATA.delete(key);
});

describe("Python compact weight indexes against actual Cloudflare R2", () => {
  it.each(contract.fixtures)("preserves every weighing and local selection: $date", async fixture => {
    await load(contract.states.initial);
    const result = await reader(fixture.date);
    expect(result.days[0]).toMatchObject({ status: "selected", measurement_count: 3,
      actual_weight_count: 2, daily_average_count: 1,
      minimum_kg: fixture.weight, maximum_kg: fixture.weight + 1,
      selected: { weight_kg: fixture.weight, measurement_id: "synthetic-first" } });
    expect(reads).toEqual([`health/indexes/body-composition/v1/${fixture.date.slice(0, 7)}.json`]);
    expect(result.sourceObjectsRead).toBe(1);
    reads.length = 0;
    expect((await reader(fixture.date, "08:00")).days[0].selected?.weight_kg).toBe(fixture.weight + 1);
    expect(reads).toHaveLength(1);
  });
  it("reads through edited canonical bytes until repaired, including repeated requests", async () => {
    const day = contract.fixtures[0].date;
    await load(contract.states.initial);
    expect((await reader(day)).days[0].selected?.weight_kg).toBe(80);
    await load(contract.states.changed);
    for (let repeat = 0; repeat < 2; repeat++) {
      reads.length = 0;
      expect((await reader(day)).days[0].selected?.weight_kg).toBe(75);
      expect(reads).toHaveLength(2);
    }
    await load(contract.states.repaired);
    expect((await reader(day)).days[0].selected?.weight_kg).toBe(75);
    expect(reads).toHaveLength(1);
  });
  it.each(["no_indexes", "corrupt_indexes", "bad_entry"] as const)("uses canonical data with %s", async state => {
    await load(contract.states[state]);
    expect((await reader(contract.fixtures[0].date)).days[0].selected?.weight_kg).toBe(80);
    expect(reads).toHaveLength(2);
  });
  it("never serves a deleted source from an orphaned index", async () => {
    await load(contract.states.deleted);
    expect((await reader(contract.fixtures[0].date)).days[0]).toMatchObject({ status: "not_stored", selected: null });
    expect(reads).toHaveLength(1);
  });
  it("retains fallback from an invalid plain alias to a valid gzip alias", async () => {
    await load(contract.states.initial);
    const day = contract.fixtures[0].date;
    const key = `health/body-composition/v1/${day.slice(0, 4)}/${day.slice(5, 7)}/${day}.json`;
    const alias = key + ".gz";
    await env.SLIPSTREAM_DATA.put(alias, Uint8Array.from(atob(contract.states.initial[key].body), c => c.charCodeAt(0)));
    await env.SLIPSTREAM_DATA.put(key, "invalid-json");
    reads.length = 0;
    try {
      expect((await reader(day)).days[0].selected?.weight_kg).toBe(80);
      expect(reads).toEqual([`health/indexes/body-composition/v1/${day.slice(0, 7)}.json`, key, alias]);
    } finally { await env.SLIPSTREAM_DATA.delete(alias); }
  });
});
