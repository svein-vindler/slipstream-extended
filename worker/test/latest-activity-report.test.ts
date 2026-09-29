import { describe, expect, it } from "vitest";
import {
  latestActivityMessage, latestActivityReportSchema,
} from "../src/latest-activity-report";

const BASE = {
  schema_version: 1 as const,
  kind: "latest-activity" as const,
  checked_at: "2026-09-29T12:00:00Z",
  status: "ready" as const,
  activity_id: "garmin-123",
  activity_date: "2026-09-29",
  activity_started_at_utc: "2026-09-29T12:00:00Z",
  activity_started_at_garmin_local: "2026-09-29 14:00:00",
  expected_date: "2026-09-29",
  already_in_slipstream: false,
  activity_name: "Evening run",
  summary_updated: true,
  files_ready: true,
  file_status: "refreshed",
  coach_status: "ready",
};

describe("latest activity report", () => {
  it("identifies a complete single-workout import", () => {
    const report = latestActivityReportSchema.parse(BASE);
    expect(latestActivityMessage(report)).toContain("garmin-123");
    expect(latestActivityMessage(report)).toContain("Coach Input");
  });

  it("never calls summary-only data ready", () => {
    const report = latestActivityReportSchema.parse({
      ...BASE, status: "files_unavailable", files_ready: false,
      file_status: "error", coach_status: "waiting_for_files",
    });
    expect(latestActivityMessage(report)).toContain("incomplete");
  });

  it("does not present an already stored older workout as the new one", () => {
    const report = latestActivityReportSchema.parse({
      ...BASE, status: "no_new_activity", summary_updated: false,
      file_status: "already_stored",
    });
    expect(latestActivityMessage(report)).toContain("no new workout");
    expect(latestActivityMessage(report)).toContain("garmin-123");
  });

  it("reports a wrong-date Garmin result as missing instead of ready", () => {
    const report = latestActivityReportSchema.parse({
      ...BASE, status: "expected_activity_missing", activity_date: "2026-09-28",
      files_ready: false, coach_status: "not_checked",
    });
    expect(latestActivityMessage(report)).toContain("has not returned the expected workout");
  });

  it("rejects unrelated or malformed R2 reports", () => {
    expect(latestActivityReportSchema.safeParse({
      ...BASE, kind: "other",
    }).success).toBe(false);
    expect(latestActivityReportSchema.safeParse({
      ...BASE, activity_id: "../../bad",
    }).success).toBe(false);
  });
});
