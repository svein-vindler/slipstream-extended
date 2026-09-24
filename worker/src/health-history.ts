import { healthHistoryIndexKey } from "./lib";

export type HistoryStream = "hrv" | "sleep";
export type HistoryGranularity = "auto" | "daily" | "weekly";
export type ResolvedGranularity = "daily" | "weekly";
export type HistoryDetail = "summary" | "full";

const DAY_MS = 86_400_000;

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
      rows.set(row.date, row);
    }
  }
  return rows;
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

export function buildHrvHistory(
  indexes: unknown[],
  dates: string[],
  granularity: ResolvedGranularity,
) {
  const rows = indexedHistoryRows("hrv", indexes, dates);
  const status = baseStatus(dates, rows);
  if (granularity === "daily") {
    return {
      ...status,
      days: dates.map((day) => rows.get(day) ?? { date: day, status: "not_stored" }),
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
) {
  const rows = indexedHistoryRows("sleep", indexes, dates);
  const status = baseStatus(dates, rows);
  if (granularity === "daily") {
    return {
      ...status,
      days: dates.map((day) => rows.get(day) ?? { date: day, status: "not_stored" }),
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
  return { ...status, days: [], weeks };
}
