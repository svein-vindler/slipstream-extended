import { describe, expect, it } from "vitest";
import { nightContext } from "../src/night-context";

describe("night context in the Cloudflare Workers runtime", () => {
  it("uses IANA daylight-saving rules for Europe/Oslo", () => {
    const night = nightContext("2026-03-29", "2026-03-28T22:30:00Z",
      "2026-03-29T05:30:00Z", "Europe/Oslo");
    expect(night).toMatchObject({
      night_of: "2026-03-28",
      sleep_start_local: "2026-03-28T23:30:00+01:00",
      sleep_end_local: "2026-03-29T07:30:00+02:00",
    });
  });

  it("uses Garmin's per-night local offset during travel", () => {
    const night = nightContext("2026-06-12",
      Date.parse("2026-06-11T15:30:00Z"),
      Date.parse("2026-06-11T22:30:00Z"), "Europe/Oslo",
      Date.parse("2026-06-12T00:30:00Z"),
      Date.parse("2026-06-12T07:30:00Z"));
    expect(night).toMatchObject({
      night_of: "2026-06-12", timezone: null,
      local_time_source: "garmin_local",
      sleep_start_local: "2026-06-12T00:30:00+09:00",
    });
  });
});
