/** Synthetic, offline prompt acceptance contract; never installed in the Worker. */
import { requireTool, type Example, type ToolContract } from "./tool-catalog";
import type { SyncStatus } from "../src/sync-status";

export type Workflow = "coach_setup" | "latest_activity" | "last_night"
  | "weekly_review" | "long_term_sleep_hrv" | "morning_weight";
export const WORKFLOW_HEADINGS: Record<Workflow, string> = {
  coach_setup: "## 1. Coach setup", latest_activity: "## 2. Freshest workout",
  last_night: "## 3. Last night", weekly_review: "## 4. Weekly review",
  long_term_sleep_hrv: "## 5. Long-term sleep and HRV",
  morning_weight: "## 6. Standardized morning weight",
};
const DAY = "2026-09-20";
const PERIOD = { start_date: "2026-09-14", end_date: DAY };
const HISTORY = { ...PERIOD, granularity: "weekly", detail_level: "summary" };
export function initialWorkflowCalls(workflow: Workflow, tools: ToolContract[], freshAuthorized = false): Example[] {
  const paths: Record<Workflow, Example[]> = {
    coach_setup: [{ tool: "coach_profile", arguments: { date: DAY } }],
    latest_activity: freshAuthorized
      ? [{ tool: "sync_latest_activity", arguments: { expected_date: DAY, new_activity_expected: true } }]
      : [{ tool: "list_activities", arguments: { ...PERIOD, limit: 20 } }],
    last_night: freshAuthorized
      ? [{ tool: "sync_latest_night", arguments: { wake_date: DAY } }]
      : [{ tool: "sleep_detail", arguments: { date: DAY } }, { tool: "hrv_curve", arguments: { date: DAY } }],
    weekly_review: [{ tool: "activity_stats", arguments: { ...PERIOD, group_by: "sport" } },
      { tool: "sleep_history", arguments: HISTORY }, { tool: "hrv_history", arguments: HISTORY }],
    long_term_sleep_hrv: ["sleep_history", "hrv_history"].map(tool => ({ tool,
      arguments: { ...HISTORY, start_date: "2026-04-01" } })),
    morning_weight: [{ tool: "weight_history", arguments: { ...PERIOD,
      morning_start: "04:00", morning_end: "12:00", timezone: "Europe/Oslo" } }],
  };
  const selected = paths[workflow];
  for (const call of selected) requireTool(tools, call.tool);
  return selected;
}

export function confirmedWrite(tools: ToolContract[], call: Example, confirmed: boolean): Example | null {
  if (!confirmed) return null;
  requireTool(tools, call.tool);
  return call;
}

export function nextFreshStep(input: {
  status: SyncStatus; shouldContinuePolling: boolean; requestId: string;
  targetMatches: boolean; httpStatus?: number;
}): "analyze" | "stop_and_report" | "select_target" | Example {
  if (input.httpStatus === 429) return "stop_and_report";
  if (!input.targetMatches) return "select_target";
  if (input.status.data_state === "ready") return "analyze";
  if (input.status.user_action_required || !input.shouldContinuePolling) return "stop_and_report";
  return { tool: "refresh_status", arguments: { request_id: input.requestId } };
}
