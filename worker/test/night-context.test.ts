import { describe, expect, it } from "vitest";
import { nightContext, validatedHealthTimezone } from "../src/night-context";

describe("local sleep-night context", () => {
  it("uses the actual local start date, not the Garmin wake-date", () => {
    const result = nightContext("2026-09-24", "2026-09-23T21:30:00Z",
      "2026-09-24T05:30:00Z", "Europe/Oslo");
    expect(result).toMatchObject({
      wake_date: "2026-09-24",
      night_of: "2026-09-23",
      sleep_start_local: "2026-09-23T23:30:00+02:00",
      sleep_end_local: "2026-09-24T07:30:00+02:00",
      sleep_midpoint_local: "2026-09-24T03:30:00+02:00",
      sleep_start_weekday_local: "Wednesday",
      sleep_end_weekday_local: "Thursday",
      timezone: "Europe/Oslo",
    });
  });

  it("uses different UTC offsets across the spring and autumn DST transitions", () => {
    expect(nightContext("2026-03-29", "2026-03-28T22:30:00Z",
      "2026-03-29T05:30:00Z", "Europe/Oslo")).toMatchObject({
      night_of: "2026-03-28",
      sleep_start_local: "2026-03-28T23:30:00+01:00",
      sleep_end_local: "2026-03-29T07:30:00+02:00",
    });
    expect(nightContext("2026-10-25", "2026-10-24T21:30:00Z",
      "2026-10-25T06:30:00Z", "Europe/Oslo")).toMatchObject({
      night_of: "2026-10-24",
      sleep_start_local: "2026-10-24T23:30:00+02:00",
      sleep_end_local: "2026-10-25T07:30:00+01:00",
    });
    expect(nightContext("2026-10-25", "2026-10-25T00:30:00Z",
      "2026-10-25T01:30:00Z", "Europe/Oslo")).toMatchObject({
      sleep_start_local: "2026-10-25T02:30:00+02:00",
      sleep_end_local: "2026-10-25T02:30:00+01:00",
    });
  });

  it("accepts Garmin Unix milliseconds and timezone-less GMT strings", () => {
    const start = Date.parse("2026-09-23T21:30:00Z");
    expect(nightContext("2026-09-24", start, "2026-09-24 05:30:00",
      "Europe/Oslo")).toMatchObject({
      night_of: "2026-09-23",
      sleep_end_local: "2026-09-24T07:30:00+02:00",
    });
  });

  it("prefers Garmin's travel-night clock over the configured home timezone", () => {
    const startGmt = Date.parse("2026-06-11T15:30:00Z");
    const endGmt = Date.parse("2026-06-11T22:30:00Z");
    const startLocal = Date.parse("2026-06-12T00:30:00Z");
    const endLocal = Date.parse("2026-06-12T07:30:00Z");
    const result = nightContext("2026-06-12", startGmt, endGmt,
      "Europe/Oslo", startLocal, endLocal);
    expect(result).toMatchObject({
      wake_date: "2026-06-12",
      night_of: "2026-06-12",
      sleep_start_local: "2026-06-12T00:30:00+09:00",
      sleep_end_local: "2026-06-12T07:30:00+09:00",
      sleep_midpoint_local: "2026-06-12T04:00:00+09:00",
      sleep_start_weekday_local: "Friday",
      timezone: null,
      local_time_source: "garmin_local",
    });
    expect(nightContext("2026-06-12", startGmt, endGmt,
      null, startLocal, endLocal).night_of).toBe("2026-06-12");
  });

  it("does not claim an IANA zone from Garmin's matching local offset", () => {
    expect(nightContext("2026-09-24", "2026-09-23T21:30:00Z",
      "2026-09-24T05:30:00Z", "Europe/Oslo",
      "2026-09-23T23:30:00.0", "2026-09-24T07:30:00.0")).toMatchObject({
        night_of: "2026-09-23",
        timezone: null,
        local_time_source: "garmin_local",
      });
  });

  it("falls back on malformed Garmin local time and never mixes zones", () => {
    const start = "2026-06-11T15:30:00Z";
    const end = "2026-06-11T22:30:00Z";
    expect(nightContext("2026-06-12", start, end, "Europe/Oslo",
      "2026-06-12T15:30:00.0", null)).toMatchObject({
        night_of: "2026-06-11", local_time_source: "configured_timezone",
      });
    expect(nightContext("2026-06-12", start, end, "Europe/Oslo",
      "2026-06-12T00:30:00.0", null)).toMatchObject({
        night_of: "2026-06-12", sleep_end_local: null,
        sleep_midpoint_local: null, local_time_source: "garmin_local",
      });
  });

  it("does not invent a start date when the zone or timestamp is unavailable", () => {
    expect(validatedHealthTimezone("Europe/Oslo")).toBe("Europe/Oslo");
    expect(validatedHealthTimezone("not/a-zone")).toBeNull();
    expect(nightContext("2026-09-24", null, "2026-09-24T05:30:00Z",
      "Europe/Oslo")).toMatchObject({
      wake_date: "2026-09-24", night_of: null,
      sleep_start_local: null, sleep_end_date_local: "2026-09-24",
    });
    expect(nightContext("2026-09-24", "2026-09-23T21:30:00Z", null,
      null)).toMatchObject({ night_of: null, timezone: null });
  });
});
