import { z } from "zod";

export const latestActivityReportSchema = z.object({
  schema_version: z.literal(1),
  kind: z.literal("latest-activity"),
  checked_at: z.string(),
  status: z.enum([
    "ready", "no_recent_activity", "no_new_activity", "expected_activity_missing",
    "files_unavailable", "coach_pending", "ambiguous_activity", "activity_date_unknown",
  ]),
  candidate_ids: z.array(z.string()).optional(),
  activity_id: z.string().regex(/^garmin-\d{1,20}$/).nullable(),
  activity_date: z.string().nullable(),
  activity_started_at_utc: z.string().nullable(),
  activity_started_at_garmin_local: z.string().nullable(),
  expected_date: z.string().nullable(),
  already_in_slipstream: z.boolean(),
  activity_name: z.string().nullable(),
  summary_updated: z.boolean(),
  files_ready: z.boolean(),
  file_status: z.string().max(40),
  coach_status: z.string().max(80),
});

export type LatestActivityReport = z.infer<typeof latestActivityReportSchema>;

export function latestActivityMessage(report: LatestActivityReport): string {
  if (report.status === "ambiguous_activity") return "Multiple workouts match the requested local day; select an exact activity ID before importing details.";
  if (report.status === "activity_date_unknown") return "Garmin did not supply a reliable local workout date; readiness is unconfirmed.";
  if (report.status === "no_recent_activity") {
    return "Garmin returned no supported activity from the last seven days. No workout was imported.";
  }
  if (report.status === "no_new_activity") {
    return `Garmin returned no new workout. The newest supported activity is still ${report.activity_id}; it was already complete in Slipstream. Do not present it as the newly expected workout.`;
  }
  if (report.status === "expected_activity_missing") {
    return `Garmin has not returned the expected workout for ${report.expected_date}. Its latest supported activity is ${report.activity_id} from ${report.activity_date}. Do not analyze the older workout as the requested one.`;
  }
  if (report.status === "ready") {
    return `Activity ${report.activity_id} (${report.activity_started_at_garmin_local ?? report.activity_started_at_utc}) is ready with files${
      report.coach_status === "ready" ? " and Coach Input" : ""
    }. ${report.coach_status === "ready"
      ? "Read coach_input for this exact activity ID before coaching."
      : "Read the matching activity-detail tool for this activity ID."}`;
  }
  if (report.status === "coach_pending") {
    return `Activity ${report.activity_id} and its files are ready, but Coach Input is not yet ready (${report.coach_status}). Do not claim that the full analysis is available.`;
  }
  return report.file_status === "garmin_not_ready"
    ? `Activity ${report.activity_id} is visible, but Garmin has not supplied its detailed files yet. Do not analyze it from the summary alone.`
    : `Activity ${report.activity_id} is visible, but its detailed-file import is incomplete (${report.file_status}). Do not analyze it from the summary alone.`;
}
