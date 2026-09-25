import { healthHistoryIndexKey } from "./lib";
import { nightContext } from "./night-context";

export type HistoryStream = "hrv" | "sleep";
export type HistoryGranularity = "auto" | "daily" | "weekly";
export type ResolvedGranularity = "daily" | "weekly";
export type HistoryDetail = "summary" | "full";
export type HistoryIndexState =
  | "indexed"
  | "verified"
  | "read_through"
  | "confirmed_missing"
  | "orphaned_index"
  | "index_only";

const DAY_MS = 86_400_000;
export const HEALTH_HISTORY_BUILDER_REVISION = 2;

function parseDate(value: string): Date {
  const parsed = new Date(`${value}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) {
    throw new Error(`Invalid calendar date: ${value}`);
  }
  return parsed;
}

export function historyDates(startDate: string, endDate: string): string[] {
  const start = parseDate(startDate);
  const end = parseDate(endDate);
  if (start > end) throw new Error("start_date cannot be after end_date.");
  const days = Math.floor((end.getTime() - start.getTime()) / DAY_MS) + 1;
  if (days > 366) throw new Error("Health history is limited to 366 days per request.");
  return Array.from({ length: days }, (_, offset) =>
    new Date(start.getTime() + offset * DAY_MS).toISOString().slice(0, 10));
}

export function resolveHistoryRequest(
  startDate: string,
  endDate: string,
  granularity: HistoryGranularity,
  detail: HistoryDetail,
): { dates: string[]; granularity: ResolvedGranularity } {
  const dates = historyDates(startDate, endDate);
  if (detail === "full") {
    if (dates.length > 7) {
      throw new Error("Full HRV readings or sleep stages are limited to 7 days per request.");
    }
    if (granularity === "weekly") {
      throw new Error("Full detail requires daily granularity.");
    }
    return { dates, granularity: "daily" };
  }
  const resolved = granularity === "auto"
    ? (dates.length <= 31 ? "daily" : "weekly")
    : granularity;
  if (resolved === "daily" && dates.length > 31) {
    throw new Error("Daily summaries are limited to 31 days; use weekly or auto granularity.");
  }
  return { dates, granularity: resolved };
}

export function historyMonthKeys(stream: HistoryStream, dates: string[]): string[] {
  const months = [...new Set(dates.map((day) => day.slice(0, 7)))];
  return months.map((month) => healthHistoryIndexKey(stream, month));
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function normalizedNumber(value: unknown): number | null {
  if (value === null || value === undefined || typeof value === "boolean") return null;
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !value.trim()) return null;
  const number = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(number)) return null;
  return Math.round(number * 1000) / 1000;
}

function firstNumber(row: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = normalizedNumber(row[key]);
    if (value !== null) return value;
  }
  return null;
}

function firstString(row: Record<string, unknown>, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function average(values: number[]): number | null {
  return values.length
    ? Math.round((values.reduce((total, value) => total + value, 0) / values.length) * 1000) / 1000
    : null;
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  const middle = Math.floor(ordered.length / 2);
  const value = ordered.length % 2
    ? ordered[middle]
    : (ordered[middle - 1] + ordered[middle]) / 2;
  return Math.round(value * 1000) / 1000;
}

function percentile(values: number[], fraction: number): number | null {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  const position = (ordered.length - 1) * fraction;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const value = lower === upper
    ? ordered[lower]
    : ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower);
  return Math.round(value * 1000) / 1000;
}

function timestampSeconds(value: unknown): number | null {
  const number = normalizedNumber(value);
  if (number !== null) return number > 10_000_000_000 ? number / 1000 : number;
  if (typeof value !== "string" || !value.trim()) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed / 1000 : null;
}

function linearSlopePerHour(points: Array<[number, number]>): number | null {
  if (points.length < 2) return null;
  const origin = points[0][0];
  const xs = points.map(([timestamp]) => (timestamp - origin) / 3600);
  const ys = points.map(([, value]) => value);
  const xMean = xs.reduce((total, value) => total + value, 0) / xs.length;
  const yMean = ys.reduce((total, value) => total + value, 0) / ys.length;
  const denominator = xs.reduce((total, value) => total + (value - xMean) ** 2, 0);
  if (denominator <= 0) return null;
  const numerator = xs.reduce(
    (total, value, index) => total + (value - xMean) * (ys[index] - yMean),
    0,
  );
  return Math.round((numerator / denominator) * 1000) / 1000;
}

/** Build the same allow-listed HRV row as the Python monthly-index builder. */
export function summarizeHrvPayload(day: string, value: unknown): Record<string, unknown> {
  const payload = asRecord(value);
  if (!payload) return { date: day, status: "invalid_schema" };
  const rawReadings = Array.isArray(payload.readings) ? payload.readings : [];
  const readings: Array<{ timestamp: unknown; hrv_ms: unknown }> = [];
  const values: number[] = [];
  const timed: Array<[number, number]> = [];
  for (const raw of rawReadings) {
    const reading = asRecord(raw);
    if (!reading) continue;
    readings.push({ timestamp: reading.timestamp ?? null, hrv_ms: reading.hrv_ms ?? null });
    const hrv = normalizedNumber(reading.hrv_ms);
    if (hrv === null) continue;
    values.push(hrv);
    const timestamp = timestampSeconds(reading.timestamp);
    if (timestamp !== null) timed.push([timestamp, hrv]);
  }
  const summary = asRecord(payload.summary) ?? {};
  const garmin = {
    last_night_avg_ms: firstNumber(summary, "lastNightAvg", "lastNightAverage", "lastNightAvgMs"),
    last_night_5_min_high_ms: firstNumber(
      summary, "lastNight5MinHigh", "lastNightFiveMinHigh", "lastNight5MinHighMs",
    ),
    weekly_avg_ms: firstNumber(summary, "weeklyAvg", "weeklyAverage", "weeklyAvgMs"),
    status: firstString(summary, "status", "statusKey", "hrvStatus"),
    baseline_low_ms: firstNumber(summary, "baselineBalancedLower", "baselineLow", "baselineLower"),
    baseline_high_ms: firstNumber(summary, "baselineBalancedUpper", "baselineHigh", "baselineUpper"),
  };
  if (!values.length && !Object.values(garmin).some((item) => item !== null)) {
    return { date: day, status: "no_data" };
  }
  const midpoint = Math.floor(values.length / 2);
  const firstMean = average(values.slice(0, midpoint));
  const secondMean = average(values.slice(midpoint));
  return {
    date: day,
    status: "available",
    detailed_readings_available: values.length > 0,
    sleep_start_gmt: payload.sleep_start_gmt ?? null,
    sleep_end_gmt: payload.sleep_end_gmt ?? null,
    sleep_start_garmin_local: payload.sleep_start_garmin_local ?? null,
    sleep_end_garmin_local: payload.sleep_end_garmin_local ?? null,
    garmin,
    derived: {
      valid_reading_count: values.length,
      minimum_ms: values.length ? Math.min(...values) : null,
      maximum_ms: values.length ? Math.max(...values) : null,
      mean_ms: average(values),
      median_ms: median(values),
      p10_ms: percentile(values, 0.10),
      p90_ms: percentile(values, 0.90),
      first_half_mean_ms: firstMean,
      second_half_mean_ms: secondMean,
      second_minus_first_ms: firstMean !== null && secondMean !== null
        ? Math.round((secondMean - firstMean) * 1000) / 1000
        : null,
      slope_ms_per_hour: linearSlopePerHour(timed.sort((a, b) => a[0] - b[0])),
    },
    readings,
  };
}

/** Build the same allow-listed sleep row as the Python monthly-index builder. */
export function summarizeSleepPayload(day: string, value: unknown): Record<string, unknown> {
  const payload = asRecord(value);
  if (!payload) return { date: day, status: "invalid_schema" };
  const summary = asRecord(payload.summary);
  if (!summary) return { date: day, status: "invalid_schema" };
  const stages = Array.isArray(payload.stages) ? payload.stages.flatMap((raw) => {
    const stage = asRecord(raw);
    return stage ? [{
      start_gmt: stage.start_gmt ?? null,
      end_gmt: stage.end_gmt ?? null,
      stage: typeof stage.stage === "string"
        || (typeof stage.stage === "number" && Number.isFinite(stage.stage))
        ? stage.stage
        : null,
    }] : [];
  }) : [];
  if (summary.sleep_seconds === null || summary.sleep_seconds === undefined) {
    if (!stages.length) return { date: day, status: "no_data" };
  }
  const allowedSummary = Object.fromEntries([
    "sleep_seconds", "deep_sleep_seconds", "light_sleep_seconds", "rem_sleep_seconds",
    "awake_sleep_seconds", "unmeasurable_sleep_seconds", "nap_seconds", "sleep_score",
    "average_spo2_percent", "lowest_spo2_percent", "average_respiration_brpm",
    "lowest_respiration_brpm", "highest_respiration_brpm", "average_sleep_stress",
  ].map((key) => [key, normalizedNumber(summary[key])]));
  const scoreBreakdown: Record<string, { value: number | null; qualifier: string | null }> = {};
  const rawBreakdown = asRecord(payload.score_breakdown);
  if (rawBreakdown) {
    for (const [name, raw] of Object.entries(rawBreakdown)) {
      const item = asRecord(raw);
      if (!item) continue;
      scoreBreakdown[name] = {
        value: normalizedNumber(item.value),
        qualifier: typeof item.qualifier === "string" ? item.qualifier : null,
      };
    }
  }
  return {
    date: day,
    status: "available",
    sleep_start_gmt: payload.sleep_start_gmt ?? null,
    sleep_end_gmt: payload.sleep_end_gmt ?? null,
    sleep_start_garmin_local: payload.sleep_start_garmin_local ?? null,
    sleep_end_garmin_local: payload.sleep_end_garmin_local ?? null,
    confirmed: typeof payload.confirmed === "boolean" ? payload.confirmed : null,
    summary: allowedSummary,
    score_breakdown: scoreBreakdown,
    stage_count: Math.max(0, Math.trunc(normalizedNumber(payload.stage_count) ?? 0)),
    stages,
  };
}

function comparable(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(comparable);
  const row = asRecord(value);
  if (!row) return value;
  return Object.fromEntries(Object.keys(row).sort().flatMap((key) =>
    ["index_state", "readings", "stages"].includes(key) ? [] : [[key, comparable(row[key])]]));
}

export function historyRowsEquivalent(left: unknown, right: unknown): boolean {
  return JSON.stringify(comparable(left)) === JSON.stringify(comparable(right));
}

export function historyReadThroughDates(
  dates: string[],
  granularity: ResolvedGranularity,
): string[] {
  return granularity === "daily" ? dates : dates.slice(-7);
}

function numberAt(row: Record<string, unknown>, ...path: string[]): number | null {
  let value: unknown = row;
  for (const key of path) {
    const record = asRecord(value);
    if (!record) return null;
    value = record[key];
  }
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function metricStats(values: Array<number | null>) {
  const valid = values.filter((value): value is number => value !== null).sort((a, b) => a - b);
  if (!valid.length) return null;
  const middle = Math.floor(valid.length / 2);
  const med = valid.length % 2 ? valid[middle] : (valid[middle - 1] + valid[middle]) / 2;
  return {
    days: valid.length,
    average: Math.round((valid.reduce((total, value) => total + value, 0) / valid.length) * 1000) / 1000,
    minimum: valid[0],
    maximum: valid[valid.length - 1],
    median: Math.round(med * 1000) / 1000,
  };
}

function isoWeekStart(day: string): string {
  const parsed = parseDate(day);
  const weekday = parsed.getUTCDay() || 7;
  parsed.setUTCDate(parsed.getUTCDate() - weekday + 1);
  return parsed.toISOString().slice(0, 10);
}

function statusOf(row: Record<string, unknown> | undefined): string {
  const status = row?.status;
  return status === "available" || status === "no_data" || status === "invalid_schema"
    ? status
    : "not_stored";
}

function baseStatus(dates: string[], rows: Map<string, Record<string, unknown>>) {
  const groups: Record<string, string[]> = {
    no_data: [],
    not_stored: [],
    invalid_schema: [],
  };
  let available = 0;
  for (const day of dates) {
    const status = statusOf(rows.get(day));
    if (status === "available") available += 1;
    else groups[status].push(day);
  }
  return {
    days_requested: dates.length,
    available_days: available,
    no_data_dates: groups.no_data,
    not_stored_dates: groups.not_stored,
    invalid_dates: groups.invalid_schema,
  };
}

export function indexedHistoryRows(
  stream: HistoryStream,
  indexes: unknown[],
  dates: string[],
): Map<string, Record<string, unknown>> {
  const allowed = new Set(dates);
  const rows = new Map<string, Record<string, unknown>>();
  for (const value of indexes) {
    const index = asRecord(value);
    if (!index || index.schema_version !== 1
      || index.kind !== `slipstream-${stream}-month-index`
      || !Array.isArray(index.days)) continue;
    for (const raw of index.days) {
      const row = asRecord(raw);
      if (!row || typeof row.date !== "string" || !allowed.has(row.date)) continue;
      rows.set(row.date, { ...row, index_state: "indexed" });
    }
  }
  return rows;
}

export function indexedHistorySourceRevisions(
  stream: HistoryStream,
  indexes: unknown[],
): Map<string, string> {
  const revisions = new Map<string, string>();
  for (const value of indexes) {
    const index = asRecord(value);
    if (!index || index.schema_version !== 1
      || index.builder_revision !== HEALTH_HISTORY_BUILDER_REVISION
      || index.kind !== `slipstream-${stream}-month-index`) continue;
    const sources = asRecord(index.source_revisions);
    if (!sources) continue;
    for (const [key, revision] of Object.entries(sources)) {
      if (typeof revision === "string" && revision) revisions.set(key, revision);
    }
  }
  return revisions;
}

function weeklyGroups(dates: string[]): Array<{ start: string; end: string; dates: string[] }> {
  const groups = new Map<string, string[]>();
  for (const day of dates) {
    const key = isoWeekStart(day);
    const group = groups.get(key) ?? [];
    group.push(day);
    groups.set(key, group);
  }
  return [...groups.entries()].map(([start, group]) => ({
    start,
    end: group[group.length - 1],
    dates: group,
  }));
}

function statusCounts(dates: string[], rows: Map<string, Record<string, unknown>>) {
  const result = { available: 0, no_data: 0, not_stored: 0, invalid_schema: 0 };
  for (const day of dates) result[statusOf(rows.get(day)) as keyof typeof result] += 1;
  return result;
}

function withNightContext(row: Record<string, unknown>, timezone: string | null) {
  const day = typeof row.date === "string" ? row.date : "";
  const { sleep_start_garmin_local, sleep_end_garmin_local, ...publicRow } = row;
  return {
    ...publicRow,
    ...nightContext(day, row.sleep_start_gmt, row.sleep_end_gmt, timezone,
      sleep_start_garmin_local, sleep_end_garmin_local),
  };
}

const NIGHT_WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

function clockMinutes(timestamp: unknown): number | null {
  if (typeof timestamp !== "string") return null;
  const match = /T(\d{2}):(\d{2}):\d{2}[+-]\d{2}:\d{2}$/.exec(timestamp);
  return match ? Number(match[1]) * 60 + Number(match[2]) : null;
}

function circularMeanMinutes(values: number[]): number | null {
  if (!values.length) return null;
  const radians = values.map((value) => value * 2 * Math.PI / 1440);
  const sine = radians.reduce((total, value) => total + Math.sin(value), 0);
  const cosine = radians.reduce((total, value) => total + Math.cos(value), 0);
  if (Math.hypot(sine, cosine) < 0.000001) return null;
  const angle = Math.atan2(sine, cosine);
  return ((Math.round(angle * 1440 / (2 * Math.PI)) % 1440) + 1440) % 1440;
}

function clockLabel(minutes: number | null): string | null {
  return minutes === null ? null
    : `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
}

function sleepWeekdaySummary(
  dates: string[], rows: Map<string, Record<string, unknown>>, timezone: string | null,
) {
  const nights = dates.flatMap((day) => {
    const row = rows.get(day);
    return row && statusOf(row) === "available" ? [withNightContext(row, timezone)] : [];
  });
  const classified = nights.filter((row) => typeof row.sleep_start_weekday_local === "string");
  const byNightOfWeekday = NIGHT_WEEKDAYS.map((weekday) => {
    const selected = classified.filter((row) => row.sleep_start_weekday_local === weekday);
    const midpoints = selected.map((row) => clockMinutes(row.sleep_midpoint_local))
      .filter((value): value is number => value !== null);
    return {
      weekday,
      nights: selected.length,
      sleep_seconds: metricStats(selected.map((row) => numberAt(row, "summary", "sleep_seconds"))),
      sleep_score: metricStats(selected.map((row) => numberAt(row, "summary", "sleep_score"))),
      midpoint_nights: midpoints.length,
      mean_sleep_midpoint_clock_local: clockLabel(circularMeanMinutes(midpoints)),
    };
  });
  const midpointFor = (weekdays: string[]) => circularMeanMinutes(
    classified.filter((row) => weekdays.includes(row.sleep_start_weekday_local as string))
      .map((row) => clockMinutes(row.sleep_midpoint_local))
      .filter((value): value is number => value !== null),
  );
  const workMidpoint = midpointFor(["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]);
  const weekendMidpoint = midpointFor(["Friday", "Saturday"]);
  const shift = workMidpoint === null || weekendMidpoint === null ? null
    : ((weekendMidpoint - workMidpoint + 2160) % 1440) - 720;
  return {
    by_night_of_weekday: byNightOfWeekday,
    nights_without_local_start: nights.length - classified.length,
    weekend_midpoint_shift_minutes: shift,
  };
}

export function buildHrvHistory(
  indexes: unknown[],
  dates: string[],
  granularity: ResolvedGranularity,
  overrides: Map<string, Record<string, unknown>> = new Map(),
  timezone: string | null = null,
) {
  const rows = indexedHistoryRows("hrv", indexes, dates);
  for (const [day, row] of overrides) rows.set(day, row);
  const status = baseStatus(dates, rows);
  if (granularity === "daily") {
    return {
      ...status,
      days: dates.map((day) => withNightContext(rows.get(day) ?? {
        date: day, status: "not_stored", index_state: "index_only",
      }, timezone)),
      weeks: [],
    };
  }
  const weeks = weeklyGroups(dates).map((group) => {
    const available = group.dates.flatMap((day) => {
      const row = rows.get(day);
      return statusOf(row) === "available" && row ? [row] : [];
    });
    const statuses: Record<string, number> = {};
    for (const row of available) {
      const garmin = asRecord(row.garmin);
      const value = garmin?.status;
      if (typeof value === "string" && value) statuses[value] = (statuses[value] ?? 0) + 1;
    }
    return {
      period_start: group.dates[0],
      period_end: group.end,
      days_requested: group.dates.length,
      status_counts: statusCounts(group.dates, rows),
      garmin_statuses: statuses,
      last_night_avg_ms: metricStats(available.map((row) => numberAt(row, "garmin", "last_night_avg_ms"))),
      last_night_5_min_high_ms: metricStats(available.map((row) => numberAt(row, "garmin", "last_night_5_min_high_ms"))),
      weekly_avg_ms: metricStats(available.map((row) => numberAt(row, "garmin", "weekly_avg_ms"))),
      reading_mean_ms: metricStats(available.map((row) => numberAt(row, "derived", "mean_ms"))),
      second_minus_first_ms: metricStats(available.map((row) => numberAt(row, "derived", "second_minus_first_ms"))),
      slope_ms_per_hour: metricStats(available.map((row) => numberAt(row, "derived", "slope_ms_per_hour"))),
    };
  });
  return { ...status, days: [], weeks };
}

export function buildSleepHistory(
  indexes: unknown[],
  dates: string[],
  granularity: ResolvedGranularity,
  overrides: Map<string, Record<string, unknown>> = new Map(),
  timezone: string | null = null,
) {
  const rows = indexedHistoryRows("sleep", indexes, dates);
  for (const [day, row] of overrides) rows.set(day, row);
  const status = baseStatus(dates, rows);
  const weekdaySummary = sleepWeekdaySummary(dates, rows, timezone);
  if (granularity === "daily") {
    return {
      ...status,
      ...weekdaySummary,
      days: dates.map((day) => withNightContext(rows.get(day) ?? {
        date: day, status: "not_stored", index_state: "index_only",
      }, timezone)),
      weeks: [],
    };
  }
  const weeks = weeklyGroups(dates).map((group) => {
    const available = group.dates.flatMap((day) => {
      const row = rows.get(day);
      return statusOf(row) === "available" && row ? [row] : [];
    });
    const totalSleep = available.reduce((total, row) => total + (numberAt(row, "summary", "sleep_seconds") ?? 0), 0);
    const stageTotal = (name: string) => available.reduce(
      (total, row) => total + (numberAt(row, "summary", name) ?? 0), 0,
    );
    const stagePercent = (name: string) => totalSleep > 0
      ? Math.round((stageTotal(name) / totalSleep) * 10_000) / 100
      : null;
    return {
      period_start: group.dates[0],
      period_end: group.end,
      days_requested: group.dates.length,
      status_counts: statusCounts(group.dates, rows),
      sleep_seconds: metricStats(available.map((row) => numberAt(row, "summary", "sleep_seconds"))),
      sleep_score: metricStats(available.map((row) => numberAt(row, "summary", "sleep_score"))),
      average_spo2_percent: metricStats(available.map((row) => numberAt(row, "summary", "average_spo2_percent"))),
      lowest_spo2_percent: metricStats(available.map((row) => numberAt(row, "summary", "lowest_spo2_percent"))),
      average_respiration_brpm: metricStats(available.map((row) => numberAt(row, "summary", "average_respiration_brpm"))),
      average_sleep_stress: metricStats(available.map((row) => numberAt(row, "summary", "average_sleep_stress"))),
      stage_totals_seconds: {
        deep: stageTotal("deep_sleep_seconds"),
        light: stageTotal("light_sleep_seconds"),
        rem: stageTotal("rem_sleep_seconds"),
        awake: stageTotal("awake_sleep_seconds"),
      },
      stage_percent_of_sleep: {
        deep: stagePercent("deep_sleep_seconds"),
        light: stagePercent("light_sleep_seconds"),
        rem: stagePercent("rem_sleep_seconds"),
      },
    };
  });
  return { ...status, ...weekdaySummary, days: [], weeks };
}
