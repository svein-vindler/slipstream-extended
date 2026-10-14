import { type Activity, rawGarminActivityId, validCalendarDay } from "./lib";

type StoredActivity = { key: string; data: unknown } | null;

export async function resolveActivityClock(activity: Activity,
  read: (keys: string[]) => Promise<StoredActivity>): Promise<{ activity: Activity; stored: StoredActivity }> {
  if (validCalendarDay(activity.localDate) || !activity.date || activity.source !== "garmin") {
    return { activity, stored: null };
  }
  const id = rawGarminActivityId(activity.id);
  if (!/^[0-9]{1,20}$/.test(id)) return { activity, stored: null };
  // UTC ± one day covers every device timezone. Probe another year only when
  // that window crosses January 1; never LIST or scan historical objects.
  const years = activityCalendarYears(activity.date.toISOString().slice(0, 10));
  let stored: StoredActivity = null;
  let selected: { activity: Activity; stored: StoredActivity } | undefined;
  for (const year of years) {
    const prefix = `activities/${year}/${id}/activity.v1`;
    const candidate = await read([`${prefix}.json`, `${prefix}.json.gz`]);
    if (!candidate) continue;
    if (year === activity.date.toISOString().slice(0, 4)) stored ??= candidate;
    const payload = object(candidate.data);
    const metadata = object(payload?.activity);
    const local = localCalendarDay(metadata?.start_time_local);
    if (!local) continue;
    if (metadata?.id !== id) throw new Error("Stored activity calendar does not match its identity.");
    if (!supportedActivityCalendarContract(payload)
      || metadata.start_time_gmt == null || metadata.start_time_gmt === "") continue;
    const utc = sourceUtcClock(metadata.start_time_gmt);
    if (!utc || utc.getTime() !== activity.date.getTime()
      || local.slice(0, 4) !== year
      || Math.abs(Date.parse(`${local}T00:00:00Z`)
        - Date.parse(`${activity.date.toISOString().slice(0, 10)}T00:00:00Z`)) > 86_400_000) {
      throw new Error("Stored activity calendar does not match its identity, UTC clock or namespace.");
    }
    if (selected) throw new Error("Stored activity calendar is ambiguous across calendar years.");
    selected = { activity: { ...activity, localDate: local }, stored: candidate };
  }
  return selected ?? { activity, stored };
}

export function activityCalendarYears(day: string): string[] {
  if (!validCalendarDay(day)) throw new Error("Invalid activity calendar day.");
  return [...new Set([0, 1, -1].map(offset =>
    new Date(Date.parse(`${day}T00:00:00Z`) + offset * 86_400_000).getUTCFullYear())
    .filter(year => year >= 1 && year <= 9999).map(year => String(year).padStart(4, "0")))];
}

export function supportedActivityCalendarContract(value: unknown): boolean {
  const payload = object(value);
  return payload?.schema_version === 1 && payload.source === "garmin-fit";
}

function object(value: unknown): Record<string, unknown> | null {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}

function sourceUtcClock(value: unknown): Date | null {
  if (typeof value !== "string" || !localCalendarDay(value)) return null;
  const normalized = value.replace(" ", "T");
  const date = new Date(normalized + (/(?:Z|[+-][0-9]{2}:[0-9]{2})$/.test(normalized) ? "" : "Z"));
  return Number.isNaN(date.getTime()) ? null : date;
}

/** Accept a source clock without mapping travel or DST through a home timezone. */
export function localCalendarDay(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length > 64) return undefined;
  const match = /^([0-9]{4}-[0-9]{2}-[0-9]{2})(?:[ T]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.[0-9]{1,9})?)?(?:Z|([+-])([0-9]{2}):([0-9]{2}))?)?$/.exec(value);
  if (!match || (match[2] && (Number(match[2]) > 23 || Number(match[3]) > 59
    || Number(match[4] ?? 0) > 59)) || (match[6] && (Number(match[6]) > 23 || Number(match[7]) > 59))) return undefined;
  return validCalendarDay(match[1]);
}
