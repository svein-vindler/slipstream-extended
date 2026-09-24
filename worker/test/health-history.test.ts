import { describe, expect, it } from "vitest";
import {
  buildHrvHistory,
  buildSleepHistory,
  historyMonthKeys,
  historyReadThroughDates,
  historyRowsEquivalent,
  indexedHistorySourceRevisions,
  resolveHistoryRequest,
  summarizeHrvPayload,
  summarizeSleepPayload,
} from "../src/health-history";

const hrvIndex = {
  schema_version: 1,
  kind: "slipstream-hrv-month-index",
  month: "2026-09",
  days: [
    {
      date: "2026-09-01",
      status: "available",
      garmin: { last_night_avg_ms: 40, weekly_avg_ms: 42, status: "BALANCED" },
      derived: { mean_ms: 41, second_minus_first_ms: 2, slope_ms_per_hour: 0.5 },
    },
    {
      date: "2026-09-02",
      status: "available",
      garmin: { last_night_avg_ms: 44, weekly_avg_ms: 43, status: "BALANCED" },
      derived: { mean_ms: 45, second_minus_first_ms: -1, slope_ms_per_hour: -0.25 },
    },
    { date: "2026-09-03", status: "no_data" },
  ],
};

const sleepIndex = {
  schema_version: 1,
  kind: "slipstream-sleep-month-index",
  month: "2026-09",
  days: [
    {
      date: "2026-09-01",
      status: "available",
      summary: {
        sleep_seconds: 28800,
        deep_sleep_seconds: 3600,
        light_sleep_seconds: 18000,
        rem_sleep_seconds: 7200,
        awake_sleep_seconds: 1200,
        sleep_score: 80,
        average_spo2_percent: 96,
        average_respiration_brpm: 13,
        average_sleep_stress: 18,
      },
    },
    {
      date: "2026-09-02",
      status: "available",
      summary: {
        sleep_seconds: 25200,
        deep_sleep_seconds: 4200,
        light_sleep_seconds: 15000,
        rem_sleep_seconds: 6000,
        awake_sleep_seconds: 900,
        sleep_score: 84,
        average_spo2_percent: 95,
        average_respiration_brpm: 14,
        average_sleep_stress: 20,
      },
    },
  ],
};

describe("health history request limits", () => {
  it("automatically uses weekly rows for a half year", () => {
    const request = resolveHistoryRequest("2026-01-01", "2026-06-30", "auto", "summary");
    expect(request.granularity).toBe("weekly");
    expect(request.dates).toHaveLength(181);
    expect(historyMonthKeys("hrv", request.dates)).toHaveLength(6);
  });

  it("keeps short summary ranges daily and bounds expensive detail", () => {
    expect(resolveHistoryRequest("2026-09-01", "2026-09-30", "auto", "summary").granularity)
      .toBe("daily");
    expect(() => resolveHistoryRequest("2026-09-01", "2026-09-08", "auto", "full"))
      .toThrow(/limited to 7 days/);
    expect(() => resolveHistoryRequest("2026-01-01", "2026-02-01", "daily", "summary"))
      .toThrow(/Daily summaries are limited/);
  });
});

describe("health history summaries", () => {
  it("keeps explicit per-date availability for daily HRV", () => {
    const result = buildHrvHistory(
      [hrvIndex],
      ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"],
      "daily",
    );
    expect(result.available_days).toBe(2);
    expect(result.no_data_dates).toEqual(["2026-09-03"]);
    expect(result.not_stored_dates).toEqual(["2026-09-04"]);
    expect(result.days[3]).toEqual({
      date: "2026-09-04", status: "not_stored", index_state: "index_only",
    });
  });

  it("aggregates HRV into compact weekly metrics", () => {
    const result = buildHrvHistory(
      [hrvIndex],
      ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"],
      "weekly",
    );
    expect(result.days).toEqual([]);
    expect(result.weeks).toHaveLength(1);
    expect(result.weeks[0].last_night_avg_ms).toMatchObject({ average: 42, days: 2 });
    expect(result.weeks[0].garmin_statuses).toEqual({ BALANCED: 2 });
    expect(result.weeks[0].status_counts).toEqual({
      available: 2, no_data: 1, not_stored: 1, invalid_schema: 0,
    });
  });

  it("uses weighted stage totals for weekly sleep percentages", () => {
    const result = buildSleepHistory(
      [sleepIndex],
      ["2026-09-01", "2026-09-02"],
      "weekly",
    );
    expect(result.weeks[0].sleep_score).toMatchObject({ average: 82, days: 2 });
    expect(result.weeks[0].stage_totals_seconds.deep).toBe(7800);
    expect(result.weeks[0].stage_percent_of_sleep.deep).toBe(14.44);
  });

  it("lets canonical rows override a stale or missing monthly index", () => {
    const overrides = new Map([
      ["2026-09-04", {
        date: "2026-09-04",
        status: "available",
        index_state: "read_through",
        garmin: { last_night_avg_ms: 48 },
      }],
    ]);
    const result = buildHrvHistory(
      [hrvIndex],
      ["2026-09-03", "2026-09-04"],
      "daily",
      overrides,
    );
    expect(result.available_days).toBe(1);
    expect(result.not_stored_dates).toEqual([]);
    expect(result.days[1]).toMatchObject({
      status: "available", index_state: "read_through",
    });
  });
});

describe("canonical history read-through", () => {
  it("checks every daily date but only the newest seven dates of long weekly ranges", () => {
    const dates = Array.from({ length: 40 }, (_, index) => `day-${index}`);
    expect(historyReadThroughDates(dates.slice(0, 31), "daily")).toHaveLength(31);
    expect(historyReadThroughDates(dates, "weekly")).toEqual(dates.slice(-7));
  });

  it("trusts source revisions only from the current index builder", () => {
    const current = {
      ...hrvIndex,
      builder_revision: 2,
      source_revisions: { "health/hrv/2026/09/2026-09-01.json": "etag-1" },
    };
    expect(indexedHistorySourceRevisions("hrv", [current])).toEqual(new Map([
      ["health/hrv/2026/09/2026-09-01.json", "etag-1"],
    ]));
    expect(indexedHistorySourceRevisions("hrv", [
      { ...current, builder_revision: 1 },
    ])).toEqual(new Map());
  });

  it("normalizes canonical HRV using the same fields as the monthly builder", () => {
    const row = summarizeHrvPayload("2026-09-24", {
      sleep_start_gmt: "2026-09-23T22:00:00Z",
      sleep_end_gmt: "2026-09-24T06:00:00Z",
      summary: { lastNightAvg: "45", weeklyAvg: 44, status: "BALANCED" },
      readings: [
        { timestamp: "2026-09-23T22:00:00Z", hrv_ms: 40 },
        { timestamp: "2026-09-24T00:00:00Z", hrv_ms: "50" },
      ],
    });
    expect(row).toMatchObject({
      status: "available",
      detailed_readings_available: true,
      garmin: { last_night_avg_ms: 45, weekly_avg_ms: 44, status: "BALANCED" },
      derived: { valid_reading_count: 2, mean_ms: 45, second_minus_first_ms: 10 },
    });
    expect(historyRowsEquivalent(
      { ...row, index_state: "indexed" },
      { ...row, index_state: "read_through", readings: [{ timestamp: null, hrv_ms: 1 }] },
    )).toBe(true);
  });

  it("normalizes canonical sleep and preserves full stages for bounded detail", () => {
    const row = summarizeSleepPayload("2026-09-24", {
      summary: { sleep_seconds: 28800, sleep_score: "82" },
      score_breakdown: { overall: { value: "82", qualifier: "GOOD" } },
      stage_count: 1,
      stages: [{ start_gmt: "start", end_gmt: "end", stage: "deep" }],
    });
    expect(row).toMatchObject({
      status: "available",
      summary: { sleep_seconds: 28800, sleep_score: 82 },
      score_breakdown: { overall: { value: 82, qualifier: "GOOD" } },
      stage_count: 1,
      stages: [{ start_gmt: "start", end_gmt: "end", stage: "deep" }],
    });
  });
});
