import { historyDates } from "./health-history";
import { nightContext, validatedHealthTimezone } from "./night-context";

export type WeightDayStatus =
  | "selected"
  | "not_stored"
  | "invalid_schema"
  | "no_actual_weight"
  | "no_usable_local_time"
  | "no_morning_measurement";

type LocalTimeSource = "garmin_local" | "configured_timezone";

export interface SelectedWeight {
  weight_kg: number;
  timestamp_local: string;
  local_time_source: LocalTimeSource;
  timezone: string | null;
  measurement_id: string | number | null;
  source_type: string | null;
}

export interface WeightDay {
  date: string;
  status: WeightDayStatus;
  measurement_count: number;
  actual_weight_count: number;
  daily_average_count: number;
  timed_actual_count: number;
  outside_requested_date_count: number;
  morning_candidate_count: number;
  minimum_kg: number | null;
  maximum_kg: number | null;
  intraday_range_kg: number | null;
  selected: SelectedWeight | null;
}

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}

function clockMinutes(value: string): number {
  const match = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(value);
  if (!match) throw new Error("Morning window times must use HH:MM (24-hour local time).");
  return Number(match[1]) * 60 + Number(match[2]);
}

export function resolveWeightRequest(
  startDate: string,
  endDate: string,
  morningStart: string,
  morningEnd: string,
  requestedTimezone: string | undefined,
  configuredTimezone: string | undefined,
) {
  const dates = historyDates(startDate, endDate);
  if (dates.length > 31) {
    throw new Error("Weight history is limited to 31 calendar days per request.");
  }
  const startMinute = clockMinutes(morningStart);
  const endMinute = clockMinutes(morningEnd);
  if (startMinute >= endMinute) {
    throw new Error("morning_start must be earlier than morning_end on the same day.");
  }
  const timezone = validatedHealthTimezone(requestedTimezone ?? configuredTimezone);
  if (requestedTimezone !== undefined && timezone === null) {
    throw new Error("timezone must be a valid IANA timezone, such as Europe/Oslo.");
  }
  return { dates, startMinute, endMinute, timezone };
}

function emptyDay(date: string, status: WeightDayStatus): WeightDay {
  return {
    date, status, measurement_count: 0, actual_weight_count: 0,
    daily_average_count: 0, timed_actual_count: 0,
    outside_requested_date_count: 0, morning_candidate_count: 0,
    minimum_kg: null, maximum_kg: null, intraday_range_kg: null,
    selected: null,
  };
}

export function missingWeightDay(date: string): WeightDay {
  return emptyDay(date, "not_stored");
}

export function invalidWeightDay(date: string): WeightDay {
  return emptyDay(date, "invalid_schema");
}

/** Select from stored individual measurements; never substitute a Garmin daily average. */
export function buildWeightDay(
  date: string,
  payload: unknown,
  timezone: string | null,
  startMinute: number,
  endMinute: number,
): WeightDay {
  const source = record(payload);
  if (!source || source.date !== date || !Array.isArray(source.measurements)) {
    return invalidWeightDay(date);
  }
  const result = emptyDay(date, "no_actual_weight");
  result.measurement_count = source.measurements.length;
  const weights: number[] = [];
  const candidates: Array<{ instant: number; selected: SelectedWeight }> = [];
  for (const value of source.measurements) {
    const item = record(value);
    if (!item) continue;
    if (item.is_daily_average === true) {
      result.daily_average_count += 1;
      continue;
    }
    const weight = item.weight_kg;
    if (typeof weight !== "number" || !Number.isFinite(weight) || weight <= 0) continue;
    result.actual_weight_count += 1;
    weights.push(weight);
    const context = nightContext(date, item.timestamp_gmt, null, timezone,
      item.timestamp_local, null);
    if (!context.sleep_start_local || !context.sleep_start_date_local
      || context.local_time_source === "unavailable") continue;
    result.timed_actual_count += 1;
    if (context.sleep_start_date_local !== date) {
      result.outside_requested_date_count += 1;
      continue;
    }
    const localMinute = clockMinutes(context.sleep_start_local.slice(11, 16));
    if (localMinute < startMinute || localMinute >= endMinute) continue;
    result.morning_candidate_count += 1;
    candidates.push({
      instant: Date.parse(context.sleep_start_local),
      selected: {
        weight_kg: weight,
        timestamp_local: context.sleep_start_local,
        local_time_source: context.local_time_source,
        timezone: context.timezone,
        measurement_id: typeof item.measurement_id === "string"
          || typeof item.measurement_id === "number" ? item.measurement_id : null,
        source_type: typeof item.source_type === "string" ? item.source_type : null,
      },
    });
  }
  if (weights.length) {
    result.minimum_kg = Math.min(...weights);
    result.maximum_kg = Math.max(...weights);
    result.intraday_range_kg = Math.round((result.maximum_kg - result.minimum_kg) * 1000) / 1000;
  }
  candidates.sort((a, b) => a.instant - b.instant);
  if (candidates.length) {
    result.selected = candidates[0].selected;
    result.status = "selected";
  } else if (result.actual_weight_count && !result.timed_actual_count) {
    result.status = "no_usable_local_time";
  } else if (result.actual_weight_count) {
    result.status = "no_morning_measurement";
  }
  return result;
}
