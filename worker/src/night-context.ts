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
  local_time_source: "garmin_local" | "configured_timezone" | "actual_night_timezone" | "unavailable";
  actual_night_timezone: string | null;
  configured_timezone: string | null;
  configured_projection: {
    timezone: string;
    night_of: string | null;
    sleep_start_local: string | null;
    sleep_end_local: string | null;
    sleep_midpoint_local: string | null;
  } | null;
  source_time: {
    sleep_start_gmt: string | number | null;
    sleep_end_gmt: string | number | null;
    sleep_start_garmin_local: string | number | null;
    sleep_end_garmin_local: string | number | null;
  };
  time_quality_flags: string[];
}

/** Formatting state is owned by one request; it contains zones, never source nights. */
export interface NightFormatting {
  zones: Map<string, Intl.DateTimeFormat>;
  instants: Map<string, Intl.DateTimeFormat>;
}

const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

/** Invalid or absent configuration must never silently become the Worker's UTC zone. */
export function validatedHealthTimezone(value: string | undefined, formatting?: NightFormatting): string | null {
  if (!value?.trim() || value.length > 128) return null;
  try {
    const zone = value.trim();
    let formatter = formatting?.zones.get(zone);
    if (!formatter) {
      formatter = new Intl.DateTimeFormat("en-US", { timeZone: zone });
      formatting?.zones.set(zone, formatter);
    }
    return formatter.resolvedOptions().timeZone;
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
  if (typeof value !== "string" || value.length > 64 || !value.trim()) return null;
  const source = value.trim().replace(" ", "T");
  const calendar = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})/.exec(source);
  if (calendar) {
    const date = new Date(`${calendar[1]}T00:00:00Z`);
    // Date parsing otherwise normalizes impossible dates such as February30.
    if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== calendar[1]
      || Number(calendar[2]) > 23 || Number(calendar[3]) > 59 || Number(calendar[4]) > 59) return null;
  }
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
  } else if (typeof value === "string" && value.length <= 64) {
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

function localInstant(instant: Date, timezone: string, formatters: Map<string, Intl.DateTimeFormat>) {
  let formatter = formatters.get(timezone);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-GB", {
      timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
    });
    formatters.set(timezone, formatter);
  }
  const parts = formatter.formatToParts(instant);
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

/** Bound source-clock provenance without rewriting a short malformed calendar. */
function sourceClock(value: unknown): string | number | null {
  if (typeof value === "number") return Number.isFinite(value) && utcInstant(value) !== null ? value : null;
  if (typeof value !== "string" || value.length > 64) return null;
  const clock = value.trim();
  const numeric = /^\d{10,13}$/.test(clock);
  const calendar = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,12})?(?:Z|[+-]\d{2}:?\d{2})?$/.test(clock);
  // Keep short calendar-shaped source text for forensic comparison even when
  // its date is impossible. It is never promoted to a valid parsed instant.
  return calendar || (numeric && utcInstant(value) !== null) ? value : null;
}

/** Existing six-argument callers cannot supply an actual night zone. */
export function nightContext(
  wakeDate: string,
  sleepStartGmt: unknown,
  sleepEndGmt: unknown,
  timezone: string | null,
  sleepStartGarminLocal?: unknown,
  sleepEndGarminLocal?: unknown,
): NightContext & { local_time_source: Exclude<NightContext["local_time_source"], "actual_night_timezone"> };
export function nightContext(
  wakeDate: string,
  sleepStartGmt: unknown,
  sleepEndGmt: unknown,
  timezone: string | null,
  sleepStartGarminLocal: unknown,
  sleepEndGarminLocal: unknown,
  actualNightTimezone: string | null | undefined,
  formatting?: NightFormatting,
): NightContext;
export function nightContext(
  wakeDate: string,
  sleepStartGmt: unknown,
  sleepEndGmt: unknown,
  timezone: string | null,
  sleepStartGarminLocal: unknown = null,
  sleepEndGarminLocal: unknown = null,
  actualNightTimezone: string | null = null,
  formatting?: NightFormatting,
): NightContext {
  const requestFormatting = formatting ?? { zones: new Map<string, Intl.DateTimeFormat>(), instants: new Map<string, Intl.DateTimeFormat>() };
  const formatters = requestFormatting.instants;
  const start = utcInstant(sleepStartGmt);
  const end = utcInstant(sleepEndGmt);
  const garminStart = start ? garminLocalInstant(start, sleepStartGarminLocal) : null;
  const garminEnd = end ? garminLocalInstant(end, sleepEndGarminLocal) : null;
  const configuredStart = timezone && start ? localInstant(start, timezone, formatters) : null;
  const configuredEnd = timezone && end ? localInstant(end, timezone, formatters) : null;
  const actualTimezone = validatedHealthTimezone(actualNightTimezone ?? undefined, requestFormatting);
  const actualStart = actualTimezone && start ? localInstant(start, actualTimezone, formatters) : null;
  const actualEnd = actualTimezone && end ? localInstant(end, actualTimezone, formatters) : null;
  const useGarmin = garminStart !== null;
  const startLocal = actualStart ?? garminStart ?? configuredStart;
  // Never combine a travel-night Garmin start with an unrelated configured-zone end.
  const endLocal = actualTimezone ? actualEnd : useGarmin ? garminEnd : configuredEnd;
  const zoneMatchesGarmin = useGarmin && garminEnd && configuredStart && configuredEnd
    && garminStart.timestamp === configuredStart.timestamp
    && garminEnd.timestamp === configuredEnd.timestamp;
  const validSleepInterval = start !== null && end !== null
    && end.getTime() > start.getTime() && end.getTime() - start.getTime() <= 36 * 60 * 60_000;
  const midpointInstant = validSleepInterval && start && end
    ? new Date(start.getTime() + (end.getTime() - start.getTime()) / 2) : null;
  const midpoint = !midpointInstant ? null
    : actualTimezone ? localInstant(midpointInstant, actualTimezone, formatters)
    : useGarmin && garminEnd && garminStart.offsetMinutes === garminEnd.offsetMinutes
      ? wallInstant(midpointInstant.getTime() + garminStart.offsetMinutes * 60_000,
        garminStart.offsetMinutes)
      : useGarmin && zoneMatchesGarmin && timezone
        ? localInstant(midpointInstant, timezone, formatters)
        : !useGarmin && timezone ? localInstant(midpointInstant, timezone, formatters) : null;
  const configuredMidpoint = midpointInstant && timezone ? localInstant(midpointInstant, timezone, formatters) : null;
  const timeQualityFlags: string[] = actualTimezone ? [] : ["actual_timezone_unknown"];
  if (!validSleepInterval) timeQualityFlags.push("invalid_sleep_interval");
  if ([sleepStartGmt, sleepEndGmt, sleepStartGarminLocal, sleepEndGarminLocal]
    .some(value => value !== undefined && value !== null && (sourceClock(value) === null || utcInstant(value) === null))) {
    timeQualityFlags.push("invalid_source_clock");
  }
  if (actualNightTimezone !== null && actualTimezone === null) {
    timeQualityFlags.push("invalid_actual_night_timezone");
  }
  if ((garminStart && actualStart && garminStart.timestamp !== actualStart.timestamp)
    || (garminEnd && actualEnd && garminEnd.timestamp !== actualEnd.timestamp)) {
    timeQualityFlags.push("source_local_conflicts_actual_timezone");
  }
  if ((garminStart && configuredStart && garminStart.timestamp !== configuredStart.timestamp)
    || (garminEnd && configuredEnd && garminEnd.timestamp !== configuredEnd.timestamp)) {
    // A mismatch does not establish which location or source clock is correct.
    timeQualityFlags.push("source_local_conflicts_configured_timezone");
  }
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
    // An offset matching the configured zone does not identify the travel location.
    timezone: actualTimezone ?? (useGarmin ? null : timezone),
    local_time_source: actualStart ? "actual_night_timezone" : useGarmin ? "garmin_local"
      : configuredStart ? "configured_timezone" : "unavailable",
    actual_night_timezone: actualTimezone,
    configured_timezone: timezone,
    configured_projection: timezone ? {
      timezone,
      night_of: configuredStart?.date ?? null,
      sleep_start_local: configuredStart?.timestamp ?? null,
      sleep_end_local: configuredEnd?.timestamp ?? null,
      sleep_midpoint_local: configuredMidpoint?.timestamp ?? null,
    } : null,
    source_time: {
      sleep_start_gmt: sourceClock(sleepStartGmt),
      sleep_end_gmt: sourceClock(sleepEndGmt),
      sleep_start_garmin_local: sourceClock(sleepStartGarminLocal),
      sleep_end_garmin_local: sourceClock(sleepEndGarminLocal),
    },
    time_quality_flags: timeQualityFlags,
  };
}
