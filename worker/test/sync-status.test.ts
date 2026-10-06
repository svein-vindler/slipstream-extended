import { describe, expect, it } from "vitest";
import { latency, latestNightReportSchema, pipelineDiagnosticsSchema, syncStatus } from "../src/sync-status";

const RUN = { id: 1, status: "completed", conclusion: "success", event: "workflow_dispatch",
  created_at: "2026-10-05T12:00:00Z", run_started_at: "2026-10-05T12:00:20Z",
  updated_at: "2026-10-05T12:02:00Z", html_url: "https://example.invalid/run/1" };
const PIPELINE = pipelineDiagnosticsSchema.parse({ schema_version: 1, kind: "refresh-diagnostics",
  mode: "night", status: "partial", started_at: "2026-10-05T12:01:00Z", finished_at: "2026-10-05T12:02:00Z",
  elapsed_ms: 60000, login_ms: 1000, source_checks: [{ scope: "night", checked: true,
    checked_at: "2026-10-05T12:01:55Z", outcome: "not_ready" }], stages: [{ stage: "latest_night",
    status: "pending", elapsed_ms: 59000, garmin_fetch_ms: 10000, r2_read_ms: 150,
    r2_write_ms: 350, activity_file_import_ms: null, coach_input_ms: null,
    garmin_connectapi_calls: 2, garmin_connectapi_errors: 0, r2_sdk_operations: { get: 1, put: 2 } }] });

describe("separate workflow, source and data status", () => {
  it("reports a successful negative Garmin check independently of job success", () => {
    expect(syncStatus({ kind: "night", run: RUN, pipeline: PIPELINE })).toMatchObject({
      job_state: "completed", source_checked: true, source_checked_at: "2026-10-05T12:01:55Z",
      data_state: "source_pending", complete: null, fresh: null, user_action_required: false,
    });
  });
  it("does not derive complete/fresh data from workflow success or old unmeasured reports", () => {
    expect(syncStatus({ kind: "unknown", run: RUN })).toMatchObject({ data_state: "unknown",
      source_checked: null, source_checked_at: null, complete: null, fresh: null });
    expect(Object.values(latency(RUN))).toEqual([20000, null, null, null, null, null, null, null, null, null]);
  });
  it("keeps polling exhaustion separate from a running job", () => {
    expect(syncStatus({ kind: "activity", run: { ...RUN, status: "in_progress", conclusion: null }, polling: false }))
      .toMatchObject({ job_state: "running", polling_state: "stopped", next_action: "check_later" });
  });
  it("distinguishes queued, accepted and failed jobs", () => {
    expect(syncStatus({ kind: "night", accepted: true })).toMatchObject({ job_state: "accepted" });
    expect(syncStatus({ kind: "night", run: { ...RUN, status: "queued" } })).toMatchObject({ job_state: "queued" });
    expect(syncStatus({ kind: "night", run: { ...RUN, conclusion: "failure" } }))
      .toMatchObject({ job_state: "failed", data_state: "error" });
  });
  it("separates queue/startup/pipeline and observed request-to-ready delay", () => {
    expect(latency(RUN, PIPELINE, Date.parse(RUN.created_at), true, Date.parse(RUN.updated_at))).toEqual({
      github_queue_ms: 20000, github_startup_ms: 40000, pipeline_ms: 60000,
      request_elapsed_ms: 120000, request_to_ready_observed_ms: 120000,
      garmin_fetch_ms: 10000, r2_read_ms: 150, r2_write_ms: 350,
      activity_file_import_ms: null, coach_input_ms: null,
    });
    expect(latency(RUN, PIPELINE, undefined, false).request_to_ready_observed_ms).toBeNull();
    expect(latency({ ...RUN, run_started_at: "invalid" }).github_queue_ms).toBeNull();
  });
  it("keeps missing timing unknown while preserving measured zero", () => {
    expect(latency(RUN, { ...PIPELINE, stages: [{ ...PIPELINE.stages[0], garmin_fetch_ms: null,
      r2_write_ms: 0 }] })).toMatchObject({ garmin_fetch_ms: null, r2_write_ms: 0 });
  });
  it("rejects contradictory and mis-scoped night reports", () => {
    const report = { schema_version: 1, kind: "latest-night", wake_date: "2026-10-05", scope: "night/2026-10-05",
      checked_at: "2026-10-05T12:00:00Z", source_checked: true, status: "stored", sleep_status: "stored", hrv_status: "stored" };
    expect(latestNightReportSchema.safeParse(report).success).toBe(true);
    for (const change of [{ scope: "night/2026-10-04" }, { hrv_status: "import_error" },
      { status: "pending", hrv_status: "invalid_response" }, { checked_at: "invalid" }]) {
      expect(latestNightReportSchema.safeParse({ ...report, ...change }).success).toBe(false);
    }
  });
});
