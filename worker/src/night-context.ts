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
  local_time_source: "garmin_local" | "configured_timezone" | "unavailable";
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

function offsetLabel(minutes: number): string {
  const sign = minutes < 0 ? "-" : "+";
  const absolute = Math.abs(minutes);
  return `${sign}${String(Math.floor(absolute / 60)).padStart(2, "0")}`
    + `:${String(absolute % 60).padStart(2, "0")}`;
}

function wallInstant(wallMilliseconds: number, offsetMinutes: number) {
  const wall = new Date(wallMilliseconds).toISOString().slice(0, 19);
  const date = wall.slice(0, 10);
  const weekday = WEEKDAYS[new Date(`${date}T00:00:00Z`).getUTCDay()];
  return { date, weekday, timestamp: `${wall}${offsetLabel(offsetMinutes)}`, offsetMinutes };
}

/** Garmin local timestamps encode wall-clock time, not an absolute instant. */
function garminLocalInstant(instant: Date, value: unknown) {
  let wallMilliseconds: number;
  if (typeof value === "number" || (typeof value === "string" && /^\d{10,13}$/.test(value.trim()))) {
    const parsed = utcInstant(value);
    if (!parsed) return null;
    wallMilliseconds = parsed.getTime();
  } else if (typeof value === "string") {
    const match = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/.exec(value.trim());
    if (!match) return null;
    wallMilliseconds = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]),
      Number(match[4]), Number(match[5]), Number(match[6]));
    if (new Date(wallMilliseconds).toISOString().slice(0, 19)
      !== `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}`) return null;
  } else return null;
  const rawOffset = (wallMilliseconds - instant.getTime()) / 60_000;
  const offsetMinutes = Math.round(rawOffset);
  if (Math.abs(offsetMinutes) > 14 * 60 || Math.abs(rawOffset - offsetMinutes) > 0.5) return null;
  return wallInstant(wallMilliseconds, offsetMinutes);
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
  const offset = offsetLabel(offsetMinutes);
  const weekday = WEEKDAYS[new Date(`${date}T00:00:00Z`).getUTCDay()];
  return { date, weekday, timestamp: `${date}T${hour}:${minute}:${second}${offset}`, offsetMinutes };
}

export function nightContext(
  wakeDate: string,
  sleepStartGmt: unknown,
  sleepEndGmt: unknown,
  timezone: string | null,
  sleepStartGarminLocal: unknown = null,
  sleepEndGarminLocal: unknown = null,
): NightContext {
  const start = utcInstant(sleepStartGmt);
  const end = utcInstant(sleepEndGmt);
  const garminStart = start ? garminLocalInstant(start, sleepStartGarminLocal) : null;
  const garminEnd = end ? garminLocalInstant(end, sleepEndGarminLocal) : null;
  const configuredStart = timezone && start ? localInstant(start, timezone) : null;
  const configuredEnd = timezone && end ? localInstant(end, timezone) : null;
  const useGarmin = garminStart !== null;
  const startLocal = garminStart ?? configuredStart;
  // Never combine a travel-night Garmin start with an unrelated configured-zone end.
  const endLocal = useGarmin ? garminEnd : configuredEnd;
  const zoneMatchesGarmin = useGarmin && garminEnd && configuredStart && configuredEnd
    && garminStart.timestamp === configuredStart.timestamp
    && garminEnd.timestamp === configuredEnd.timestamp;
  const midpointInstant = start && end && end > start
    ? new Date(start.getTime() + (end.getTime() - start.getTime()) / 2) : null;
  const midpoint = !midpointInstant ? null
    : useGarmin && garminEnd && garminStart.offsetMinutes === garminEnd.offsetMinutes
      ? wallInstant(midpointInstant.getTime() + garminStart.offsetMinutes * 60_000,
        garminStart.offsetMinutes)
      : useGarmin && zoneMatchesGarmin && timezone
        ? localInstant(midpointInstant, timezone)
        : !useGarmin && timezone ? localInstant(midpointInstant, timezone) : null;
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
    timezone: useGarmin ? zoneMatchesGarmin ? timezone : null : timezone,
    local_time_source: useGarmin ? "garmin_local"
      : configuredStart ? "configured_timezone" : "unavailable",
  };
}
