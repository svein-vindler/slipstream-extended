import { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { boundedTrainingResult, readTrainingHistory, trainingHistorySchema } from "./training-history";
import { PRIVATE_READ_TOOL_ANNOTATIONS, exactDate } from "./tool-contracts";

export function registerTrainingTools(server: McpServer, bucket: R2Bucket) {
  server.registerTool("training_history", {
    title: "Read stored daily Training Readiness and Recovery Time",
    description: "Read canonical private R2 training days directly, 1–31 inclusive real dates, at most four simultaneous reads. Compact-v1 shares source/metric_units at result level. Each day's candidate latest_observation/morning_observation references its observations; each metric time_ref/check_ref references that day's clocks/checks, preserving retained fields' own times and ages. Identical morning/latest share one observation; distinct ones remain separate. No cache, index, Garmin call, sync or write. Collection defaults off and needs separate approval. Missing objects mean not_collected, with Garmin support unknown. Readiness is a Garmin estimate on a 1–100 scale; Recovery Time is reported minutes at observation time, with no countdown. Positive minutes with REACHED_ZERO are conflicting. Device/wakeup ambiguity stays explicit with no first-row fallback; failure/partial is not proof of selected-device absence. Check last_attempt, selection_status and retained_previous separately. The eight-hour display policy is separate from existing fresh-data contracts; stored values do not claim current Garmin state. Sleep wake-date/night_of remain distinct. At most 16KiB per day, 64KiB structured content and 128KiB MCP result; ordinary 31-day one-device responses fit, with output_limit retained for genuinely oversized results.",
    inputSchema: z.object({ start_date: exactDate, end_date: exactDate }).strict(),
    outputSchema: trainingHistorySchema,
    annotations: PRIVATE_READ_TOOL_ANNOTATIONS,
  }, async ({ start_date, end_date }) => boundedTrainingResult(await readTrainingHistory(bucket, start_date, end_date)));
}
