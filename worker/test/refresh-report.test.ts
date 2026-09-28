import { describe, expect, it } from "vitest";
import {
  activityReady, refreshReportMessage, refreshReportSchema,
} from "../src/refresh-report";

const BASE = {
  schema_version: 1 as const,
  updated_at: "2026-09-28T12:00:00Z",
  new_activity_count: 0,
  checked_activity_count: 0,
  remaining_candidate_count: 0,
  activities: [],
};

describe("on-demand activity refresh report", () => {
  it("does not mistake a completed refresh for a newly imported workout", () => {
    const report = refreshReportSchema.parse(BASE);
    expect(activityReady(report)).toBe(false);
    expect(refreshReportMessage(report)).toContain("no new activity");
  });

  it("reports a new running workout only when files and coach are ready", () => {
    const report = refreshReportSchema.parse({
      ...BASE,
      new_activity_count: 1,
      checked_activity_count: 1,
      activities: [{
        activity_id: "garmin-123",
        date: "2026-09-28",
        name: "Easy run",
        is_new: true,
        files_ready: true,
        file_status: "refreshed",
        coach_status: "ready",
      }],
    });
    expect(activityReady(report)).toBe(true);
    expect(refreshReportMessage(report)).toContain("Coach input is ready for 1 of 1");
    expect(activityReady({ ...report, activities: [{
      ...report.activities[0], coach_status: "missing_artifacts",
    }] })).toBe(false);
  });

  it("rejects malformed reports from storage", () => {
    expect(refreshReportSchema.safeParse({ ...BASE, new_activity_count: -1 }).success).toBe(false);
    expect(refreshReportSchema.safeParse({
      ...BASE, activities: [{ activity_id: "../../bad" }],
    }).success).toBe(false);
  });
});
