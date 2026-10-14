import { expect, it, vi } from "vitest";
import { parseCsv, toSummary, filterActs, bucketKey, activityJsonObjectKeys,
  activityEnduranceObjectKeys, coachInputPrefix, activityContextPrefix } from "../src/lib";

it("uses the Garmin calendar across summary, filtering, grouping and stored namespaces", () => {
  const [activity] = parseCsv("Activity ID,Activity Date,Activity Name,Activity Type,Source,Activity Local Date\n"
    + "garmin-1,2026-12-31 23:30:00,Synthetic run,Run,garmin,2027-01-01\n");
  expect(activity.date?.toISOString()).toBe("2026-12-31T23:30:00.000Z");
  expect(toSummary(activity).date).toBe("2027-01-01");
  expect(filterActs([activity], { start_date: "2027-01-01", end_date: "2027-01-01" })).toEqual([activity]);
  expect(bucketKey(activity, "year")).toBe("2027");
  expect(bucketKey(activity, "month")).toBe("2027-01");
  expect(activityJsonObjectKeys(activity)[0]).toBe("activities/2027/1/activity.v1.json");
  expect(activityEnduranceObjectKeys(activity)[0]).toBe("activities/2027/1/activity.endurance.v1.json");
  expect(coachInputPrefix(activity)).toBe("activities/2027/1/coach-input/v1/canonical/");
  expect(activityContextPrefix(activity)).toBe("activities/2027/1/context/v1/");
});

it.each([
  ["2026-03-29 01:30:00", "2026-03-29"],
  ["2026-10-25 00:30:00", "2026-10-25"],
  ["2027-01-01 07:30:00", "2026-12-31"],
])("keeps source travel/DST instant %s on day %s without a home-zone conversion", (utc, day) => {
  const [activity] = parseCsv("Activity ID,Activity Date,Activity Local Date\n"
    + `garmin-1,${utc},${day}\n`);
  expect(toSummary(activity)).toMatchObject({ date: day, date_source: "garmin_local" });
});

it.each(["", "2027-02-29", "2026-02-30", "0000-01-01", "invalid"])("labels invalid or absent local day %s as UTC fallback", local => {
  const [activity] = parseCsv("Activity ID,Activity Date,Activity Local Date\n"
    + `garmin-1,2026-12-31 23:30:00,${local}\n`);
  expect(toSummary(activity)).toMatchObject({ date: "2026-12-31", date_source: "utc_fallback" });
});

it("resolves a legacy year boundary from a bounded, matching canonical source", async () => {
  const { resolveActivityClock } = await import("../src/activity-clock");
  const [legacy] = parseCsv("Activity ID,Activity Date,Activity Name,Activity Type\n"
    + "garmin-1,2026-12-31 23:30:00,Synthetic run,Run\n");
  const read = vi.fn(async (keys: string[]) => keys[0].includes("/2027/") ? {
    key: keys[0], data: { schema_version: 1, source: "garmin-fit", activity: {
      id: "1", start_time_gmt: "2026-12-31 23:30:00", start_time_local: "2027-01-01 00:30:00" } },
  } : null);
  const result = await resolveActivityClock(legacy, read);
  expect(result.activity.localDate).toBe("2027-01-01");
  expect(legacy.localDate).toBeUndefined();
  expect(read).toHaveBeenCalledTimes(2);
  expect(activityContextPrefix(result.activity)).toBe("activities/2027/1/context/v1/");
});

it("accepts an explicit UTC suffix in the canonical GMT clock", async () => {
  const { resolveActivityClock } = await import("../src/activity-clock");
  const [activity] = parseCsv("Activity ID,Activity Date,Source\ngarmin-1,2026-09-30 23:30:00,garmin\n");
  const result = await resolveActivityClock(activity, async keys => ({ key: keys[0], data: {
    schema_version: 1, source: "garmin-fit", activity: { id: "1",
      start_time_gmt: "2026-09-30T23:30:00Z", start_time_local: "2026-10-01 00:30:00" },
  } }));
  expect(result.activity.localDate).toBe("2026-10-01");
});

it.each(["2027-02-30 12:00:00", "2027-01-01 24:00:00", "2027-01-01 12:00:00+00:60"])("does not normalize malformed source clock %s", async clock => {
  const { localCalendarDay } = await import("../src/activity-clock");
  expect(localCalendarDay(clock)).toBeUndefined();
});

it("rejects conflicting verified local calendars across the two year candidates", async () => {
  const { resolveActivityClock } = await import("../src/activity-clock");
  const [activity] = parseCsv("Activity ID,Activity Date,Source\ngarmin-1,2026-12-31 23:30:00,garmin\n");
  await expect(resolveActivityClock(activity, async keys => ({ key: keys[0], data: {
    schema_version: 1, source: "garmin-fit", activity: { id: "1",
      start_time_gmt: "2026-12-31 23:30:00",
      start_time_local: keys[0].includes("/2027/") ? "2027-01-01 00:30:00" : "2026-12-31 23:30:00" },
  } }))).rejects.toThrow("ambiguous");
});
