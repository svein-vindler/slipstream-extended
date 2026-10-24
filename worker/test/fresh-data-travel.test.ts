import { expect, it } from "vitest";
import { FreshDataService, type FreshRequest } from "../src/fresh-data";
import { parseCsv } from "../src/lib";

const NOW = Date.parse("2026-10-24T12:00:00Z");
const request: FreshRequest = { kind: "activity", newExpected: false };

function service() {
  const rows = new Map<string, unknown>();
  const sources = [
    { id: "1", local: "2026-10-24 00:30:00", utc: "2026-10-23 11:30:00", name: "New Zealand" },
    { id: "2", local: "2026-10-23 22:30:00", utc: "2026-10-24 05:30:00", name: "California" },
  ];
  for (const source of sources) {
    const prefix = `activities/2026/${source.id}`;
    rows.set(`${prefix}/activity.v1.json`, { schema_version: 1, source: "garmin-fit",
      activity: { id: source.id, type: "cycling", name: source.name,
        start_time_local: source.local, start_time_gmt: source.utc }, decode_errors: [] });
    rows.set(`${prefix}/activity.fit`, "synthetic"); rows.set(`${prefix}/activity.tcx`, "synthetic");
    rows.set(`${prefix}/activity.endurance.v1.json`, { available: true, activity: { id: source.id },
      summary: { distance_m: 1000 } });
  }
  const summary = parseCsv("Activity ID,Activity Date,Activity Name,Activity Type,Source,Activity Local Date\n"
    + sources.map(source => `garmin-${source.id},${source.utc},${source.name},Ride,garmin,${source.local.slice(0, 10)}\n`).join(""));
  const reads: string[] = [];
  const reader = {
    getActivities: async () => summary,
    getR2Json: async (keys: string[]) => {
      reads.push(...keys);
      const key = keys.find(key => rows.has(key));
      return key ? { key, data: rows.get(key) } : null;
    },
    getCoachProfiles: async () => ({ keys: [], profiles: [] }),
  };
  const env = { SLIPSTREAM_DATA: {
    list: async ({ prefix, limit }: { prefix: string; limit: number }) => {
      expect(limit).toBeLessThanOrEqual(1000);
      return { objects: [...rows.keys()].filter(key => key.startsWith(prefix))
        .map(key => ({ key, uploaded: new Date(NOW) })), truncated: false };
    },
    put: async () => { throw new Error("Unexpected synthetic write"); },
    delete: async () => { throw new Error("Unexpected synthetic delete"); },
  } } as unknown as Env;
  const value = new FreshDataService(env, reader, { repository: "synthetic/repo", workflow: "refresh.yml",
    ref: "main", token: "synthetic", cooldownMinutes: 30 }, () => NOW,
    async () => { throw new Error("Unexpected provider request"); });
  return { value, rows, reads };
}

it("selects the UTC-newer California activity despite its earlier Garmin-local date", async () => {
  const { value } = service();
  const result = await value.snapshot(request);
  expect(result.package.activity_id).toBe("garmin-2");
  expect(result.package.activity_date).toBe("2026-10-23");
  expect(result.source_fresh).toBe(false);
});

it.each([null, "", "2026-02-30 12:00:00", "2026-10-24", "invalid"])
  ("keeps an unanchored candidate with UTC %s unknown", async utc => {
    const { value, rows } = service();
    const decoded = rows.get("activities/2026/1/activity.v1.json") as { activity: Record<string, unknown> };
    decoded.activity.start_time_gmt = utc;
    const result = await value.snapshot(request);
    expect(result.complete).toBe(false);
    expect(result.status).toBe("activity_date_unknown");
    expect(result.package.activity_id).toBeUndefined();
    expect(result.missing_components).toContain("verified_activity_utc");
  });

it("retains Garmin-local date filtering for an explicitly dated request", async () => {
  const { value } = service();
  const result = await value.snapshot({ ...request, date: "2026-10-24" });
  expect(result.package.activity_id).toBe("garmin-1");
  expect(result.package.activity_date).toBe("2026-10-24");
});

it("retains a fresh ID-bound source receipt instead of selecting another summary row", async () => {
  const { value, rows } = service();
  rows.set("refresh/checks/v1/activity/latest/latest/known.json", {
    scope: "activity/latest/latest/known", source_checked: true,
    checked_at: new Date(NOW - 1000).toISOString(), status: "ready", activity_id: "garmin-1",
  });
  const result = await value.snapshot(request);
  expect(result.package.activity_id).toBe("garmin-1");
  expect(result.source_fresh).toBe(true);
});

it("does not use a stale ID-bound receipt to override verified UTC chronology", async () => {
  const { value, rows } = service();
  rows.set("refresh/checks/v1/activity/latest/latest/known.json", {
    scope: "activity/latest/latest/known", source_checked: true,
    checked_at: new Date(NOW - 600_000).toISOString(), status: "ready", activity_id: "garmin-1",
  });
  const result = await value.snapshot(request);
  expect(result.package.activity_id).toBe("garmin-2");
  expect(result.source_fresh).toBe(false);
});

it("keeps tied verified UTC candidates ambiguous instead of choosing by local date", async () => {
  const { value, rows } = service();
  const decoded = rows.get("activities/2026/1/activity.v1.json") as { activity: Record<string, unknown> };
  decoded.activity.start_time_gmt = "2026-10-24 05:30:00";
  const result = await value.snapshot(request);
  expect(result.status).toBe("ambiguous_activity");
  expect(result.package.candidate_ids).toEqual(["garmin-2", "garmin-1"]);
  expect(result.complete).toBe(false);
});
