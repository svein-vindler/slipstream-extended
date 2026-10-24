/**
 * Pure data helpers (CSV parsing, filtering, and stats). Kept separate from the
 * MCP wiring in index.ts so they can be unit-tested without a Worker runtime.
 */

export interface Activity {
  id: string;
  date: Date | null;
  localDate?: string;
  name: string;
  type: string;
  distanceKm?: number;
  movingS?: number;
  elapsedS?: number;
  maxHr?: number;
  avgHr?: number;
  elevGain?: number;
  calories?: number;
  source: string;
}

export interface HealthDay {
  date: string;
  sleepSeconds?: number;
  deepSleepSeconds?: number;
  lightSleepSeconds?: number;
  remSleepSeconds?: number;
  awakeSleepSeconds?: number;
  sleepScore?: number;
  hrvWeeklyAvg?: number;
  hrvLastNightAvg?: number;
  hrvStatus?: string;
  restingHr?: number;
  minHr?: number;
  maxHr?: number;
  avgHr?: number;
  bodyBatteryHigh?: number;
  bodyBatteryLow?: number;
  bodyBatteryCharged?: number;
  bodyBatteryDrained?: number;
  avgStress?: number;
  maxStress?: number;
  stressDurationS?: number;
  steps?: number;
  avgRespiration?: number;
  lowRespiration?: number;
  highRespiration?: number;
  weightKg?: number;
  source: string;
}

export function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inq = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inq) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else inq = false;
      } else cur += c;
    } else if (c === ",") { out.push(cur); cur = ""; }
    else if (c === '"') inq = true;
    else cur += c;
  }
  out.push(cur);
  return out;
}

export function parseDate(s: string): Date | null {
  if (!s) return null;
  const d = new Date(s.replace(" ", "T") + "Z"); // writer emits UTC "YYYY-MM-DD HH:MM:SS"
  return isNaN(d.getTime()) ? null : d;
}

/** Physical newlines inside a quoted field belong to the same CSV record. */
function csvRecords(text: string): string[] {
  const records: string[] = [];
  let quoted = false;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    if (text[i] === '"') {
      if (quoted && text[i + 1] === '"') i++;
      else quoted = !quoted;
    } else if (!quoted && text[i] === "\n") {
      const record = text.slice(start, text[i - 1] === "\r" ? i - 1 : i);
      if (record.length) records.push(record);
      start = i + 1;
    }
  }
  if (start < text.length) records.push(text.slice(start));
  return records;
}

/** The source calendar is independent of the UTC instant and home timezone. */
export function activityCalendarDay(a: Activity): string | null {
  return validCalendarDay(a.localDate) ?? (a.date ? a.date.toISOString().slice(0, 10) : null);
}

export function validCalendarDay(value: unknown): string | undefined {
  if (typeof value !== "string" || !/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(value)) return undefined;
  const parsed = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value
    && value.slice(0, 4) !== "0000" ? value : undefined;
}

export function parseCsv(text: string): Activity[] {
  const lines = csvRecords(text);
  if (lines.length < 2) return [];
  const header = parseCsvLine(lines[0]);
  const ix = (name: string) => header.indexOf(name);
  const iId = ix("Activity ID"), iDate = ix("Activity Date"), iName = ix("Activity Name"),
    iType = ix("Activity Type"), iDist = ix("Distance"), iMov = ix("Moving Time"),
    iEl = ix("Elapsed Time"), iMax = ix("Max Heart Rate"), iAvg = ix("Average Heart Rate"),
    iElev = ix("Elevation Gain"), iCal = ix("Calories"), iSrc = ix("Source"), iLocal = ix("Activity Local Date");
  const acts: Activity[] = [];
  for (let r = 1; r < lines.length; r++) {
    const c = parseCsvLine(lines[r]);
    const num = (i: number) => {
      const v = i >= 0 ? c[i] : "";
      if (!v) return undefined;
      const n = parseFloat(v);
      return isNaN(n) ? undefined : n;
    };
    acts.push({
      id: c[iId] ?? "", date: parseDate(c[iDate] ?? ""), name: c[iName] ?? "",
      type: c[iType] ?? "", distanceKm: num(iDist), movingS: num(iMov), elapsedS: num(iEl),
      maxHr: num(iMax), avgHr: num(iAvg), elevGain: num(iElev), calories: num(iCal),
      source: c[iSrc] ?? "garmin",
      localDate: validCalendarDay(c[iLocal]),
    });
  }
  return acts;
}

export function parseHealthCsv(text: string): HealthDay[] {
  const lines = csvRecords(text);
  if (lines.length < 2) return [];
  const header = parseCsvLine(lines[0]);
  const ix = (name: string) => header.indexOf(name);
  const textAt = (cells: string[], name: string) => {
    const index = ix(name);
    return index >= 0 ? cells[index] ?? "" : "";
  };
  const numAt = (cells: string[], name: string) => {
    const value = textAt(cells, name);
    if (!value) return undefined;
    const number = Number.parseFloat(value);
    return Number.isNaN(number) ? undefined : number;
  };
  return lines.slice(1).map((line) => {
    const c = parseCsvLine(line);
    return {
      date: textAt(c, "Date"),
      sleepSeconds: numAt(c, "Sleep Seconds"),
      deepSleepSeconds: numAt(c, "Deep Sleep Seconds"),
      lightSleepSeconds: numAt(c, "Light Sleep Seconds"),
      remSleepSeconds: numAt(c, "REM Sleep Seconds"),
      awakeSleepSeconds: numAt(c, "Awake Sleep Seconds"),
      sleepScore: numAt(c, "Sleep Score"),
      hrvWeeklyAvg: numAt(c, "HRV Weekly Average"),
      hrvLastNightAvg: numAt(c, "HRV Last Night Average"),
      hrvStatus: textAt(c, "HRV Status") || undefined,
      restingHr: numAt(c, "Resting Heart Rate"),
      minHr: numAt(c, "Minimum Heart Rate"),
      maxHr: numAt(c, "Maximum Heart Rate"),
      avgHr: numAt(c, "Average Heart Rate"),
      bodyBatteryHigh: numAt(c, "Body Battery Highest"),
      bodyBatteryLow: numAt(c, "Body Battery Lowest"),
      bodyBatteryCharged: numAt(c, "Body Battery Charged"),
      bodyBatteryDrained: numAt(c, "Body Battery Drained"),
      avgStress: numAt(c, "Average Stress"),
      maxStress: numAt(c, "Maximum Stress"),
      stressDurationS: numAt(c, "Stress Duration Seconds"),
      steps: numAt(c, "Steps"),
      avgRespiration: numAt(c, "Average Respiration"),
      lowRespiration: numAt(c, "Lowest Respiration"),
      highRespiration: numAt(c, "Highest Respiration"),
      weightKg: numAt(c, "Weight KG"),
      source: textAt(c, "Source") || "garmin",
    } satisfies HealthDay;
  }).filter((row) => /^\d{4}-\d{2}-\d{2}$/.test(row.date));
}

export function filterHealth(rows: HealthDay[], o: { start_date?: string; end_date?: string }) {
  return rows.filter((row) =>
    (!o.start_date || row.date >= o.start_date) && (!o.end_date || row.date <= o.end_date));
}

function rounded(value: number, digits = 1) {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

export function toHealthSummary(row: HealthDay) {
  const hours = (seconds?: number) => seconds == null ? null : rounded(seconds / 3600, 2);
  return {
    date: row.date,
    sleep_hours: hours(row.sleepSeconds), deep_sleep_hours: hours(row.deepSleepSeconds),
    light_sleep_hours: hours(row.lightSleepSeconds), rem_sleep_hours: hours(row.remSleepSeconds),
    awake_hours: hours(row.awakeSleepSeconds), sleep_score: row.sleepScore ?? null,
    hrv_last_night_avg_ms: row.hrvLastNightAvg ?? null,
    hrv_weekly_avg_ms: row.hrvWeeklyAvg ?? null, hrv_status: row.hrvStatus ?? null,
    resting_hr: row.restingHr ?? null, min_hr: row.minHr ?? null,
    max_hr: row.maxHr ?? null, avg_hr: row.avgHr ?? null,
    body_battery_high: row.bodyBatteryHigh ?? null, body_battery_low: row.bodyBatteryLow ?? null,
    body_battery_charged: row.bodyBatteryCharged ?? null,
    body_battery_drained: row.bodyBatteryDrained ?? null,
    avg_stress: row.avgStress ?? null, max_stress: row.maxStress ?? null,
    stress_hours: hours(row.stressDurationS), steps: row.steps ?? null,
    avg_respiration_brpm: row.avgRespiration ?? null,
    low_respiration_brpm: row.lowRespiration ?? null,
    high_respiration_brpm: row.highRespiration ?? null,
    weight_kg: row.weightKg ?? null,
  };
}

export function summarizeHealth(rows: HealthDay[]) {
  const metric = (pick: (row: HealthDay) => number | undefined) => {
    const values = rows.map(pick).filter((value): value is number => value != null);
    return values.length ? {
      days: values.length,
      average: rounded(values.reduce((total, value) => total + value, 0) / values.length),
      minimum: Math.min(...values), maximum: Math.max(...values),
    } : null;
  };
  return {
    days: rows.length,
    sleep_hours: metric((row) => row.sleepSeconds == null ? undefined : row.sleepSeconds / 3600),
    sleep_score: metric((row) => row.sleepScore),
    hrv_last_night_avg_ms: metric((row) => row.hrvLastNightAvg),
    resting_hr: metric((row) => row.restingHr),
    avg_hr: metric((row) => row.avgHr),
    body_battery_high: metric((row) => row.bodyBatteryHigh),
    body_battery_low: metric((row) => row.bodyBatteryLow),
    avg_stress: metric((row) => row.avgStress),
    steps: metric((row) => row.steps),
    avg_respiration_brpm: metric((row) => row.avgRespiration),
    weight_kg: metric((row) => row.weightKg),
  };
}

export function healthBucketKey(row: HealthDay, by: "month" | "year") {
  return by === "year" ? row.date.slice(0, 4) : row.date.slice(0, 7);
}

export function fmtDuration(sec?: number): string | undefined {
  if (sec == null) return undefined;
  const s = Math.round(sec);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), ss = s % 60;
  return h ? `${h}:${String(m).padStart(2, "0")}:${String(ss).padStart(2, "0")}`
    : `${m}:${String(ss).padStart(2, "0")}`;
}

export function filterActs(acts: Activity[], o: {
  sport_type?: string; start_date?: string; end_date?: string; name_contains?: string;
}): Activity[] {
  let out = acts;
  if (o.sport_type) {
    const requested = o.sport_type.trim().toLowerCase();
    const aliases: Record<string, string> = {
      running: "run", "løping": "run", "løp": "run", jogging: "run",
      cycling: "ride", "sykling": "ride",
    };
    const sport = aliases[requested] ?? requested;
    out = out.filter((a) => a.type.toLowerCase() === sport);
  }
  if (o.name_contains) { const q = o.name_contains.toLowerCase(); out = out.filter((a) => a.name.toLowerCase().includes(q)); }
  if (o.start_date) out = out.filter((a) => { const day = activityCalendarDay(a); return day != null && day >= o.start_date!; });
  if (o.end_date) out = out.filter((a) => { const day = activityCalendarDay(a); return day != null && day <= o.end_date!; });
  return out;
}

export function activityFilterWarning(
  matched: number,
  filters: { sport_type?: string; name_contains?: string },
): string | undefined {
  if (matched > 0 || (!filters.sport_type && !filters.name_contains)) return undefined;
  return "No activities matched all filters. This does not establish that the date has no stored activities; retry without sport_type or name_contains before concluding that data is missing.";
}

export function summarize(acts: Activity[]) {
  const sum = (f: (a: Activity) => number | undefined, scale = 1) => {
    let total = 0;
    for (const activity of acts) {
      const value = f(activity);
      if (value == null || !Number.isFinite(value)) return null;
      total += value;
    }
    const rounded = Math.round(total * scale) / scale;
    return Number.isFinite(rounded) ? rounded : null;
  };
  const movingTime = sum((a) => a.movingS);
  const hrs = acts.map((a) => a.avgHr).filter((x): x is number => x != null);
  return {
    activities: acts.length,
    total_distance_km: sum((a) => a.distanceKm, 100),
    total_moving_time: movingTime === null ? null : fmtDuration(movingTime) ?? null,
    total_elevation_gain_m: sum((a) => a.elevGain, 10),
    total_calories: sum((a) => a.calories),
    avg_hr: hrs.length ? Math.round((hrs.reduce((t, x) => t + x, 0) / hrs.length) * 10) / 10 : null,
  };
}

export function toSummary(a: Activity) {
  const pace = a.distanceKm && a.movingS && a.distanceKm > 0
    ? fmtDuration(a.movingS / a.distanceKm) + "/km" : undefined;
  return {
    id: a.id, date: activityCalendarDay(a), name: a.name,
    date_source: validCalendarDay(a.localDate) ? "garmin_local" : a.date ? "utc_fallback" : "unknown",
    type: a.type, distance_km: a.distanceKm ?? null, moving_time: fmtDuration(a.movingS) ?? null,
    pace: pace ?? null, avg_hr: a.avgHr ?? null, elevation_gain_m: a.elevGain ?? null,
  };
}

export function bucketKey(a: Activity, by: string): string | null {
  if (by === "sport") return a.type || "Unknown";
  const day = activityCalendarDay(a);
  if (!day) return null;
  if (by === "year") return day.slice(0, 4);
  if (by === "month") return day.slice(0, 7);
  return null;
}

export function rawGarminActivityId(id: string): string {
  return id.startsWith("garmin-") ? id.slice("garmin-".length) : id;
}

export function hrvObjectKeys(date: string): string[] {
  const prefix = `health/hrv/${date.slice(0, 4)}/${date.slice(5, 7)}/${date}`;
  return [`${prefix}.json`, `${prefix}.json.gz`];
}

export function sleepObjectKeys(date: string): string[] {
  const prefix = `health/sleep/v1/${date.slice(0, 4)}/${date.slice(5, 7)}/${date}`;
  return [`${prefix}.json`, `${prefix}.json.gz`];
}

export function healthHistoryIndexKey(stream: "hrv" | "sleep", month: string): string {
  return `health/indexes/${stream}/v1/${month.slice(0, 4)}/${month.slice(5, 7)}.json`;
}

export function bodyCompositionObjectKeys(date: string): string[] {
  const prefix = `health/body-composition/v1/${date.slice(0, 4)}/${date.slice(5, 7)}/${date}`;
  return [`${prefix}.json`, `${prefix}.json.gz`];
}

export function activityJsonObjectKeys(activity: Activity): string[] {
  const day = activityCalendarDay(activity);
  if (!day) return [];
  const id = rawGarminActivityId(activity.id);
  const prefix = `activities/${day.slice(0, 4)}/${id}/activity.v1`;
  return [`${prefix}.json`, `${prefix}.json.gz`];
}

export function activityEnduranceObjectKeys(activity: Activity): string[] {
  const day = activityCalendarDay(activity);
  if (!day) return [];
  const id = rawGarminActivityId(activity.id);
  const prefix = `activities/${day.slice(0, 4)}/${id}/activity.endurance.v1`;
  return [`${prefix}.json`, `${prefix}.json.gz`];
}

export function coachInputPrefix(activity: Activity): string {
  const day = activityCalendarDay(activity);
  if (!day) return "";
  const id = rawGarminActivityId(activity.id);
  return `activities/${day.slice(0, 4)}/${id}/coach-input/v1/canonical/`;
}

export function activityContextPrefix(activity: Activity): string {
  const day = activityCalendarDay(activity);
  if (!day) return "";
  const id = rawGarminActivityId(activity.id);
  return `activities/${day.slice(0, 4)}/${id}/context/v1/`;
}
