/** Calendar context for one Garmin night, keyed by the existing wake-date. */
export interface NightContext {
  wake_date: string;
  night_of: string | null;
  sleep_start_local: string | null;
  sleep_end_local: string | null;
  sleep_midpoint_local: string | null;
  sleep_start_date_local: string | null;
  sleep_end_date_local: string | null;
  sleep_start_weekday_local: string | null;
  sleep_end_weekday_local: string | null;
  timezone: string | null;
}

const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** Invalid or absent configuration must never silently become the Worker's UTC zone. */
export function validatedHealthTimezone(value: string | undefined): string | null {
  if (!value?.trim()) return null;
  try {
    return new Intl.DateTimeFormat("en-US", { timeZone: value.trim() })
      .resolvedOptions().timeZone;
  } catch (error) {
    if (error instanceof RangeError) return null;
    throw error;
  }
}

function utcInstant(value: unknown): Date | null {
  if (typeof value === "number" || (typeof value === "string" && /^\d{10,13}$/.test(value.trim()))) {
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    const parsed = new Date(number > 10_000_000_000 ? number : number * 1000);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  }
  if (typeof value !== "string" || !value.trim()) return null;
  const source = value.trim().replace(" ", "T");
  const timestamp = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(source) ? source : `${source}Z`;
  const parsed = new Date(timestamp);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

function localInstant(instant: Date, timezone: string) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
  }).formatToParts(instant);
  const part = (type: string) => parts.find((item) => item.type === type)?.value ?? "";
  const year = part("year");
  const month = part("month");
  const day = part("day");
  const hour = part("hour");
  const minute = part("minute");
  const second = part("second");
  const date = `${year}-${month}-${day}`;
  const localAsUtc = Date.UTC(Number(year), Number(month) - 1, Number(day),
    Number(hour), Number(minute), Number(second));
  const offsetMinutes = Math.round((localAsUtc - instant.getTime()) / 60_000);
  const sign = offsetMinutes < 0 ? "-" : "+";
  const absolute = Math.abs(offsetMinutes);
  const offset = `${sign}${String(Math.floor(absolute / 60)).padStart(2, "0")}`
    + `:${String(absolute % 60).padStart(2, "0")}`;
  const weekday = WEEKDAYS[new Date(`${date}T00:00:00Z`).getUTCDay()];
  return { date, weekday, timestamp: `${date}T${hour}:${minute}:${second}${offset}` };
}

export function nightContext(
  wakeDate: string,
  sleepStartGmt: unknown,
  sleepEndGmt: unknown,
  timezone: string | null,
): NightContext {
  const start = utcInstant(sleepStartGmt);
  const end = utcInstant(sleepEndGmt);
  const startLocal = timezone && start ? localInstant(start, timezone) : null;
  const endLocal = timezone && end ? localInstant(end, timezone) : null;
  const midpoint = timezone && start && end && end > start
    ? localInstant(new Date(start.getTime() + (end.getTime() - start.getTime()) / 2), timezone)
    : null;
  return {
    wake_date: wakeDate,
    night_of: startLocal?.date ?? null,
    sleep_start_local: startLocal?.timestamp ?? null,
    sleep_end_local: endLocal?.timestamp ?? null,
    sleep_midpoint_local: midpoint?.timestamp ?? null,
    sleep_start_date_local: startLocal?.date ?? null,
    sleep_end_date_local: endLocal?.date ?? null,
    sleep_start_weekday_local: startLocal?.weekday ?? null,
    sleep_end_weekday_local: endLocal?.weekday ?? null,
    timezone,
  };
}
