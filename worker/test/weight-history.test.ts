import { describe, expect, it } from "vitest";
import {
  buildWeightDay, invalidWeightDay, missingWeightDay, resolveWeightRequest,
} from "../src/weight-history";

const measurement = (weight: number, gmt: string, local: string | null = null) => ({
  weight_kg: weight,
  timestamp_gmt: gmt,
  timestamp_local: local,
  is_daily_average: false,
  measurement_id: null,
  source_type: "INDEX_SCALE",
});

describe("weight history request", () => {
  it("bounds calendar days and checks local morning window and timezone", () => {
    const request = resolveWeightRequest("2026-09-01", "2026-09-30",
      "04:00", "12:00", undefined, "Europe/Oslo");
    expect(request.dates).toHaveLength(30);
    expect(request.timezone).toBe("Europe/Oslo");
    expect(() => resolveWeightRequest("2026-09-01", "2026-10-02",
      "04:00", "12:00", undefined, undefined)).toThrow(/31 calendar days/);
    expect(() => resolveWeightRequest("2026-02-30", "2026-03-01",
      "04:00", "12:00", undefined, undefined)).toThrow(/Invalid calendar date/);
    expect(() => resolveWeightRequest("2026-09-01", "2026-09-01",
      "12:00", "04:00", undefined, undefined)).toThrow(/earlier/);
    expect(() => resolveWeightRequest("2026-09-01", "2026-09-01",
      "04:00", "12:00", "not/a-timezone", "Europe/Oslo")).toThrow(/IANA/);
  });

  it("selects the earliest individual morning measurement, never an average", () => {
    const day = buildWeightDay("2026-09-21", {
      date: "2026-09-21",
      measurements: [
        { ...measurement(81, "2026-09-21T05:30:00Z"), is_daily_average: true },
        measurement(80.9, "2026-09-21T07:30:00Z"),
        measurement(80.4, "2026-09-21T05:00:00Z"),
        measurement(81.3, "2026-09-21T16:00:00Z"),
      ],
    }, "Europe/Oslo", 4 * 60, 12 * 60);
    expect(day.status).toBe("selected");
    expect(day.selected).toMatchObject({
      weight_kg: 80.4,
      timestamp_local: "2026-09-21T07:00:00+02:00",
      local_time_source: "configured_timezone",
    });
    expect(day).toMatchObject({
      measurement_count: 4, actual_weight_count: 3, daily_average_count: 1,
      timed_actual_count: 3, morning_candidate_count: 2,
      minimum_kg: 80.4, maximum_kg: 81.3, intraday_range_kg: 0.9,
    });
  });

  it("prefers Garmin local wall time to the home timezone while travelling", () => {
    const day = buildWeightDay("2026-07-11", {
      date: "2026-07-11",
      measurements: [measurement(80.2, "2026-07-10T23:00:00Z",
        "2026-07-11T08:00:00")],
    }, "Europe/Oslo", 4 * 60, 12 * 60);
    expect(day.selected).toMatchObject({
      weight_kg: 80.2,
      timestamp_local: "2026-07-11T08:00:00+09:00",
      local_time_source: "garmin_local",
      timezone: null,
    });
  });

  it("applies IANA daylight-saving rules to UTC-only measurements", () => {
    const before = buildWeightDay("2026-03-28", {
      date: "2026-03-28",
      measurements: [measurement(80, "2026-03-28T06:00:00Z")],
    }, "Europe/Oslo", 240, 720);
    const after = buildWeightDay("2026-03-30", {
      date: "2026-03-30",
      measurements: [measurement(80.1, "2026-03-30T05:00:00Z")],
    }, "Europe/Oslo", 240, 720);
    expect(before.selected?.timestamp_local).toBe("2026-03-28T07:00:00+01:00");
    expect(after.selected?.timestamp_local).toBe("2026-03-30T07:00:00+02:00");
    const noZone = buildWeightDay("2026-03-30", {
      date: "2026-03-30",
      measurements: [measurement(80.1, "2026-03-30T05:00:00Z")],
    }, null, 240, 720);
    expect(noZone.status).toBe("no_usable_local_time");
  });

  it("does not silently use a daily average, an untimed weight or a different local day", () => {
    const average = buildWeightDay("2026-09-21", {
      date: "2026-09-21", measurements: [{
        ...measurement(81, "2026-09-21T07:00:00Z"), is_daily_average: true,
      }],
    }, "Europe/Oslo", 240, 720);
    expect(average.status).toBe("no_actual_weight");
    expect(average.selected).toBeNull();
    const untimed = buildWeightDay("2026-09-21", {
      date: "2026-09-21", measurements: [{
        weight_kg: 80, timestamp_gmt: null, timestamp_local: null,
        is_daily_average: false,
      }],
    }, "Europe/Oslo", 240, 720);
    expect(untimed.status).toBe("no_usable_local_time");
    const wrongDay = buildWeightDay("2026-09-21", {
      date: "2026-09-21", measurements: [measurement(80,
        "2026-09-20T21:00:00Z")],
    }, "Europe/Oslo", 0, 720);
    expect(wrongDay.status).toBe("no_morning_measurement");
    expect(wrongDay.outside_requested_date_count).toBe(1);
  });

  it("distinguishes missing and malformed source objects", () => {
    expect(missingWeightDay("2026-09-21").status).toBe("not_stored");
    expect(invalidWeightDay("2026-09-21").status).toBe("invalid_schema");
    expect(buildWeightDay("2026-09-21", {
      date: "2026-09-20", measurements: [],
    }, null, 240, 720).status).toBe("invalid_schema");
  });
});
