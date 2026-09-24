import { describe, it, expect } from "vitest";
import {
  parseCsv, parseHealthCsv, summarize, summarizeHealth, filterActs, filterHealth,
  toSummary, toHealthSummary, bucketKey, healthBucketKey, fmtDuration,
  rawGarminActivityId, hrvObjectKeys, activityJsonObjectKeys,
  activityEnduranceObjectKeys, sleepObjectKeys, bodyCompositionObjectKeys,
  healthHistoryIndexKey,
} from "../src/lib";

const CSV = [
  "Activity ID,Activity Date,Activity Name,Activity Type,Raw Sport,Distance,Filename,Moving Time,Elapsed Time,Max Heart Rate,Average Heart Rate,Elevation Gain,Average Speed,Average Watts,Calories,Source",
  "garmin-1,2026-06-01 06:30:00,Morning Run,Run,running,10,,3000,3100,180,160,50,3.33,,600,garmin",
  'garmin-2,2026-06-02 07:00:00,"Hill, Repeats",Run,running,8,,2400,2500,185,168,200,3.33,,500,garmin',
  "garmin-3,2026-05-20 08:00:00,Long Ride,Ride,cycling,40,,7200,7300,160,140,500,5.56,200,1200,garmin",
].join("\n");

describe("parseCsv", () => {
  const acts = parseCsv(CSV);
  it("parses every row", () => expect(acts).toHaveLength(3));
  it("handles a quoted field containing a comma", () =>
    expect(acts[1].name).toBe("Hill, Repeats"));
  it("reads numeric and date fields", () => {
    expect(acts[0].distanceKm).toBe(10);
    expect(acts[0].avgHr).toBe(160);
    expect(acts[0].date?.toISOString()).toBe("2026-06-01T06:30:00.000Z");
  });
  it("returns nothing for header-only input", () =>
    expect(parseCsv("a,b,c")).toHaveLength(0));
});

describe("summarize", () => {
  it("totals distance and averages heart rate", () => {
    const s = summarize(parseCsv(CSV));
    expect(s.activities).toBe(3);
    expect(s.total_distance_km).toBe(58);
    expect(s.avg_hr).toBe(156); // (160 + 168 + 140) / 3
  });
});

describe("filterActs", () => {
  const acts = parseCsv(CSV);
  it("filters by sport (case-insensitive)", () =>
    expect(filterActs(acts, { sport_type: "run" })).toHaveLength(2));
  it("filters by start date inclusive", () =>
    expect(filterActs(acts, { start_date: "2026-06-01" })).toHaveLength(2));
  it("filters by name substring", () =>
    expect(filterActs(acts, { name_contains: "hill" })).toHaveLength(1));
});

describe("toSummary", () => {
  it("computes pace per km", () =>
    expect(toSummary(parseCsv(CSV)[0]).pace).toBe("5:00/km")); // 3000s / 10km
});

describe("bucketKey", () => {
  const a = parseCsv(CSV)[0];
  it("buckets by month/year/sport", () => {
    expect(bucketKey(a, "month")).toBe("2026-06");
    expect(bucketKey(a, "year")).toBe("2026");
    expect(bucketKey(a, "sport")).toBe("Run");
  });
});

describe("fmtDuration", () => {
  it("formats with and without hours", () => {
    expect(fmtDuration(3661)).toBe("1:01:01");
    expect(fmtDuration(125)).toBe("2:05");
    expect(fmtDuration(undefined)).toBeUndefined();
  });
});

const HEALTH_CSV = [
  "Date,Sleep Seconds,Deep Sleep Seconds,Light Sleep Seconds,REM Sleep Seconds,Awake Sleep Seconds,Sleep Score,HRV Weekly Average,HRV Last Night Average,HRV Status,Resting Heart Rate,Minimum Heart Rate,Maximum Heart Rate,Average Heart Rate,Body Battery Highest,Body Battery Lowest,Body Battery Charged,Body Battery Drained,Average Stress,Maximum Stress,Stress Duration Seconds,Steps,Average Respiration,Lowest Respiration,Highest Respiration,Weight KG,Source",
  "2026-06-01,25200,3600,15000,5400,1200,82,45,47,BALANCED,50,42,170,72,90,20,70,60,25,70,3600,10000,14,10,20,81.2,garmin",
  "2026-06-02,27000,4000,16000,6000,1000,88,46,49,BALANCED,49,41,165,70,95,25,70,55,20,65,3000,12000,13.5,9,19,81.1,garmin",
].join("\n");

describe("health helpers", () => {
  const rows = parseHealthCsv(HEALTH_CSV);
  it("parses and filters daily health", () => {
    expect(rows).toHaveLength(2);
    expect(rows[0].sleepScore).toBe(82);
    expect(filterHealth(rows, { start_date: "2026-06-02" })).toHaveLength(1);
  });
  it("formats daily units and summarizes trends", () => {
    expect(toHealthSummary(rows[0]).sleep_hours).toBe(7);
    expect(summarizeHealth(rows).steps?.average).toBe(11000);
    expect(healthBucketKey(rows[0], "month")).toBe("2026-06");
  });
});

describe("granular object keys", () => {
  it("maps public activity IDs to private R2 object keys", () => {
    const activity = parseCsv(CSV)[0];
    expect(rawGarminActivityId(activity.id)).toBe("1");
    expect(activityJsonObjectKeys(activity)).toEqual([
      "activities/2026/1/activity.v1.json",
      "activities/2026/1/activity.v1.json.gz",
    ]);
    expect(activityEnduranceObjectKeys(activity)).toEqual([
      "activities/2026/1/activity.endurance.v1.json",
      "activities/2026/1/activity.endurance.v1.json.gz",
    ]);
  });

  it("supports current and legacy HRV object names", () => {
    expect(hrvObjectKeys("2026-09-20")).toEqual([
      "health/hrv/2026/09/2026-09-20.json",
      "health/hrv/2026/09/2026-09-20.json.gz",
    ]);
  });

  it("maps detailed health dates to versioned R2 object keys", () => {
    expect(sleepObjectKeys("2026-09-20")).toEqual([
      "health/sleep/v1/2026/09/2026-09-20.json",
      "health/sleep/v1/2026/09/2026-09-20.json.gz",
    ]);
    expect(bodyCompositionObjectKeys("2026-09-20")).toEqual([
      "health/body-composition/v1/2026/09/2026-09-20.json",
      "health/body-composition/v1/2026/09/2026-09-20.json.gz",
    ]);
  });

  it("maps monthly health history indexes", () => {
    expect(healthHistoryIndexKey("hrv", "2026-09")).toBe(
      "health/indexes/hrv/v1/2026/09.json",
    );
    expect(healthHistoryIndexKey("sleep", "2026-09")).toBe(
      "health/indexes/sleep/v1/2026/09.json",
    );
  });
});
