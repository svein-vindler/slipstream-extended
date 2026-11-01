import { expect, it } from "vitest";
import { FreshDataService } from "../src/fresh-data";

const NOW = Date.parse("2026-11-01T20:00:00Z");
function fixture(options: { truncated?: boolean; denied?: boolean } = {}) {
  const lists: Array<{ prefix: string; limit: number }> = [];
  let network = 0;
  const rows = new Map<string, unknown>();
  const reader = {
    getActivities: async () => [],
    getCoachProfiles: async () => ({ keys: [], profiles: [] }),
    getR2Json: async (keys: string[]) => {
      const key = keys.find(value => rows.has(value));
      return key ? { key, data: rows.get(key) } : null;
    },
  };
  const env = { HEALTH_TIMEZONE: "Europe/Oslo", SLIPSTREAM_DATA: {
    list: async (args: { prefix: string; limit: number }) => {
      lists.push(args);
      if (options.denied) throw new Error("Synthetic R2 denial");
      const truncated = options.truncated ?? true;
      return { truncated, ...(truncated ? { cursor: "synthetic-cursor" } : {}),
        objects: truncated ? Array.from({ length: 1000 }, (_, i) => ({
          key: `activities/2026/1/context/v1/${i}.json`, uploaded: new Date(NOW),
        })) : [] };
    },
  } } as unknown as Env;
  const value = new FreshDataService(env, reader, { repository: "synthetic/repo", workflow: "refresh.yml",
    ref: "main", token: "synthetic", cooldownMinutes: 30 }, () => NOW,
    async () => { network++; throw new Error("External network forbidden"); });
  return { value, lists, rows, network: () => network };
}
it("keeps a truncated year discovery incomplete for valid new-workout arguments", async () => {
  const f = fixture();
  const snapshot = await f.value.snapshot(f.value.activityRequest({
    expected_date: "2026-11-01", new_activity_expected: true,
  }));
  expect(snapshot).toMatchObject({ status: "activity_discovery_incomplete", complete: false,
    source_fresh: false, source_outcome: "unknown", missing_components: ["bounded_activity_discovery"],
    package: {} });
  expect(f.lists).toEqual([{ prefix: "activities/2026/", limit: 1000 }]);
  expect(f.network()).toBe(0);
});
it("keeps a complete empty discovery distinct from a truncation", async () => {
  const f = fixture({ truncated: false });
  const snapshot = await f.value.snapshot({ kind: "activity", date: "2026-11-01", newExpected: true });
  expect(snapshot.missing_components).toEqual(["matching_local_activity"]);
  expect(snapshot.source_outcome).toBe("unknown");
});
it("does not turn R2 denial into missing data or a Garmin request", async () => {
  const f = fixture({ denied: true });
  await expect(f.value.snapshot({ kind: "activity", date: "2026-11-01", newExpected: true }))
    .rejects.toThrow("Synthetic R2 denial");
  expect(f.network()).toBe(0);
});
it("keeps exact-activity artifact enumeration fail-closed at the same bound", async () => {
  const f = fixture();
  f.rows.set("activities/2026/73/activity.v1.json", { schema_version: 1, source: "garmin-fit",
    activity: { id: "73", type: "cycling", start_time_local: "2026-11-01T12:00:00",
      start_time_gmt: "2026-11-01T11:00:00Z" }, decode_errors: [] });
  await expect(f.value.snapshot({ kind: "activity", date: "2026-11-01", activityId: "73", newExpected: true }))
    .rejects.toThrow("Canonical R2 listing exceeded its bounded limit");
  expect(f.lists).toEqual([{ prefix: "activities/2026/73/", limit: 1000 }]);
  expect(f.network()).toBe(0);
});
