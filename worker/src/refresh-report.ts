import { z } from "zod";

const activity = z.object({
  activity_id: z.string().regex(/^garmin-[0-9]{1,20}$/),
  date: z.string().max(10),
  name: z.string().max(300),
  is_new: z.boolean(),
  files_ready: z.boolean(),
  file_status: z.string().max(40),
  coach_status: z.string().max(40),
  error: z.string().max(300).optional(),
});

export const refreshReportSchema = z.object({
  schema_version: z.literal(1),
  updated_at: z.string(),
  new_activity_count: z.number().int().nonnegative(),
  checked_activity_count: z.number().int().nonnegative(),
  remaining_candidate_count: z.number().int().nonnegative(),
  activities: z.array(activity).max(10),
});

export type RefreshReport = z.infer<typeof refreshReportSchema>;

export function activityReady(report: RefreshReport): boolean {
  const newlyImported = report.activities.filter((item) => item.is_new);
  return report.new_activity_count > 0
    && report.remaining_candidate_count === 0
    && newlyImported.length === report.new_activity_count
    && newlyImported.every((item) =>
      item.files_ready && (item.coach_status === "ready" || item.coach_status === "not_applicable")
    );
}

export function refreshReportMessage(report: RefreshReport): string {
  if (report.new_activity_count === 0) {
    const checked = report.activities.filter((item) => item.files_ready).length;
    const coachReady = report.activities.filter((item) => item.coach_status === "ready").length;
    return checked
      ? `Refresh completed. Garmin returned no new activities; ${checked} recent existing activities were checked. Coach input is ready for ${coachReady} of those activities. A workout not yet visible in Garmin cannot be imported.`
      : "Refresh completed, but Garmin returned no new activity for the refreshed period. A workout that Garmin has not exposed yet cannot be imported or given coach input.";
  }
  const newlyImported = report.activities.filter((item) => item.is_new);
  const filesReady = newlyImported.filter((item) => item.files_ready).length;
  const running = newlyImported.filter((item) => item.coach_status !== "not_applicable");
  const coachReady = running.filter((item) => item.coach_status === "ready").length;
  const remainder = report.remaining_candidate_count;
  return `Refresh found ${report.new_activity_count} new activities; files are ready for ${filesReady}. `
    + `Coach input is ready for ${coachReady} of ${running.length} new running activities.`
    + (remainder ? ` ${remainder} activities remain outside this bounded batch.` : "")
    + (activityReady(report) ? " The newly imported activities are ready." : " Some activity details are not ready yet.");
}
