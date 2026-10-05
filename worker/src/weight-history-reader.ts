import { bodyCompositionObjectKeys } from "./lib";
import { buildWeightDay, invalidWeightDay, missingWeightDay, resolveWeightRequest } from "./weight-history";
import { R2Storage, HISTORY_INDEX_R2_LIMITS } from "./r2-storage";
import { PayloadTooLargeError } from "./security";

const fields = new Set(["timestamp_gmt", "timestamp_local", "weight_kg", "is_daily_average", "measurement_id", "source_type"]);
function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

/** Optional indexes only serve rows matching the current canonical LIST ETag. */
export class WeightHistoryReader {
  constructor(private readonly bucket: Pick<R2Bucket, "list">, private readonly storage: R2Storage) {}

  async read(request: ReturnType<typeof resolveWeightRequest>) {
    const prefixes = [...new Set(request.dates.map(date =>
      `health/body-composition/v1/${date.slice(0, 4)}/${date.slice(5, 7)}/`))];
    const requestedDates = new Set(request.dates);
    const allowedKeys = new Set(request.dates.flatMap(bodyCompositionObjectKeys));
    const keys = new Map<string, Array<{ key: string; etag: string }>>();
    let listOperations = 0;
    for (const prefix of prefixes) {
      let cursor: string | undefined;
      do {
        const listed = await this.bucket.list({ prefix, limit: 1000, ...(cursor ? { cursor } : {}) });
        listOperations += 1;
        for (const object of listed.objects) {
          const match = /(\d{4}-\d{2}-\d{2})\.json(?:\.gz)?$/.exec(object.key);
          if (!match || !requestedDates.has(match[1]) || !allowedKeys.has(object.key)) continue;
          const candidates = keys.get(match[1]) ?? [];
          candidates.push({ key: object.key, etag: object.etag });
          keys.set(match[1], candidates);
        }
        if (listed.truncated && (!listed.cursor || listOperations >= 6)) {
          throw new Error("Body-composition listing exceeded its bounded page limit.");
        }
        cursor = listed.truncated ? listed.cursor : undefined;
      } while (cursor);
    }
    const indexes = new Map<string, Record<string, unknown>>();
    let sourceObjectsRead = 0;
    for (const month of [...new Set(request.dates.map(day => day.slice(0, 7)))]) {
      try {
        sourceObjectsRead += 1;
        const stored = await this.storage.getR2Json([`health/indexes/body-composition/v1/${month}.json`], HISTORY_INDEX_R2_LIMITS);
        const value = record(stored?.data);
        const objects = record(value?.objects);
        if (value?.schema_version === 1 && value.kind === "weight-month-index" && value.month === month && objects) {
          indexes.set(month, objects);
        }
      } catch { /* Optional/corrupt indexes must not hide canonical measurements. */ }
    }
    const days = [];
    for (const date of request.dates) {
      const candidates = keys.get(date)?.sort((a, b) => a.key.length - b.key.length) ?? [];
      if (!candidates.length) { days.push(missingWeightDay(date)); continue; }
      let day = invalidWeightDay(date);
      for (const source of candidates) {
        const entry = record(indexes.get(date.slice(0, 7))?.[source.key]);
        if (entry?.etag === source.etag && entry.date === date && Array.isArray(entry.measurements)
          && entry.measurements.every(item => item === null || record(item)
            && Object.entries(item).every(([key, value]) => fields.has(key)
              && (value === null || ["number", "string", "boolean"].includes(typeof value))))) {
          day = buildWeightDay(date, { date, measurements: entry.measurements },
            request.timezone, request.startMinute, request.endMinute);
          if (day.status !== "invalid_schema") break;
        }
        try {
          sourceObjectsRead += 1;
          const stored = await this.storage.getR2Json([source.key], { stored: 256 * 1024, decoded: 512 * 1024 });
          if (!stored) continue;
          day = buildWeightDay(date, stored.data, request.timezone, request.startMinute, request.endMinute);
          if (day.status !== "invalid_schema") break;
        } catch (error) {
          if (!(error instanceof SyntaxError || error instanceof PayloadTooLargeError
            || error instanceof Error && error.message.includes("exceeds the stored-size limit"))) throw error;
          console.error(JSON.stringify({ message: "Invalid body-composition history object" }));
        }
      }
      days.push(day);
    }
    return { days, sourceObjectsRead, listOperations };
  }
}
