/** Bounded R2 reads and parsed summary reuse; no MCP, dispatch or write logic. */
import { errorCategory } from "./output-privacy";

import { Activity, HealthDay, parseCsv, parseHealthCsv } from "./lib";
import { PayloadTooLargeError, decodePossiblyGzippedText, readStreamLimited } from "./security";

export interface R2ReadLimits { stored: number; decoded: number }
export const SUMMARY_R2_LIMITS = { stored: 2 * 1024 * 1024, decoded: 16 * 1024 * 1024 };
export const GRANULAR_R2_LIMITS = { stored: 8 * 1024 * 1024, decoded: 32 * 1024 * 1024 };
export const HISTORY_INDEX_R2_LIMITS = { stored: 512 * 1024, decoded: 2 * 1024 * 1024 };
export const COACH_CONFIG_R2_LIMITS = { stored: 256 * 1024, decoded: 512 * 1024 };

const ACTIVITY_KEY = "summary/activities.csv";
const HEALTH_KEY = "summary/health_daily.csv";
type SummaryStorage = "r2" | "none";

// Shared per isolate, containing only parsed data and revisions, never request
// state, bindings, response bodies or in-flight I/O. HEAD still runs per read.
let activityCache: { etag: string; data: Activity[] } | undefined;
let healthCache: { etag: string; data: HealthDay[] } | undefined;

export function clearSummaryCaches(): void {
  activityCache = undefined;
  healthCache = undefined;
}

export class R2Storage {
  private activitySource: SummaryStorage = "none";
  private healthSource: SummaryStorage = "none";

  constructor(private readonly bucket: Pick<R2Bucket, "get" | "head">) {}

  get activityStorage(): SummaryStorage { return this.activitySource; }
  get healthStorage(): SummaryStorage { return this.healthSource; }

  async getActivities(): Promise<Activity[]> {
    try {
      const metadata = await this.bucket.head(ACTIVITY_KEY);
      if (activityCache && metadata?.etag === activityCache.etag) {
        this.activitySource = "r2";
        return activityCache.data;
      }
      const stored = await this.getR2Text([ACTIVITY_KEY], SUMMARY_R2_LIMITS);
      if (!stored) throw new Error(`R2 object ${ACTIVITY_KEY} was not found.`);
      const data = parseCsv(stored.text);
      if (!data.length && !stored.text.startsWith("Activity ID,")) {
        throw new Error("R2 activity summary has an invalid CSV header.");
      }
      this.activitySource = "r2";
      activityCache = { etag: stored.etag, data };
      return data;
    } catch (error) {
      console.error(JSON.stringify({
        message: "R2 activity summary unavailable",
        error: errorCategory(error),
      }));
      throw new Error("Activity data is unavailable in private R2.");
    }
  }

  async getHealth(): Promise<HealthDay[]> {
    try {
      const metadata = await this.bucket.head(HEALTH_KEY);
      if (healthCache && metadata?.etag === healthCache.etag) {
        this.healthSource = "r2";
        return healthCache.data;
      }
      const stored = await this.getR2Text([HEALTH_KEY], SUMMARY_R2_LIMITS);
      if (!stored) throw new Error(`R2 object ${HEALTH_KEY} was not found.`);
      const data = parseHealthCsv(stored.text);
      if (!data.length && !stored.text.startsWith("Date,")) {
        throw new Error("R2 health summary has an invalid CSV header.");
      }
      this.healthSource = "r2";
      healthCache = { etag: stored.etag, data };
      return data;
    } catch (error) {
      console.error(JSON.stringify({
        message: "R2 health summary unavailable",
        error: errorCategory(error),
      }));
      throw new Error("Health data is unavailable in private R2.");
    }
  }

  async getR2Text(keys: string[], limits: R2ReadLimits = GRANULAR_R2_LIMITS):
    Promise<{ key: string; text: string; etag: string } | null> {
    const stored = await this.readR2Text(keys, limits);
    return stored ? { key: stored.key, text: stored.text, etag: stored.etag } : null;
  }

  private async readR2Text(keys: string[], limits: R2ReadLimits):
    Promise<{ key: string; text: string; etag: string; uploaded?: Date } | null> {
    for (const key of keys) {
      const object = await this.bucket.get(key);
      if (!object) continue;
      if (object.size > limits.stored) {
        try { await object.body.cancel("size limit exceeded"); } catch { /* Preserve the size error. */ }
        throw new PayloadTooLargeError("R2 object exceeds the stored-size limit.");
      }
      const buffer = await readStreamLimited(object.body, limits.stored,
        "R2 object exceeds the stored-size limit.");
      const text = await decodePossiblyGzippedText(buffer, limits.decoded);
      return { key, text, etag: object.etag, ...(object.uploaded ? { uploaded: object.uploaded } : {}) };
    }
    return null;
  }

  async getR2Json(keys: string[], limits: R2ReadLimits = GRANULAR_R2_LIMITS):
    Promise<{ key: string; data: unknown } | null> {
    const stored = await this.getR2Text(keys, limits);
    return stored ? { key: stored.key, data: JSON.parse(stored.text) as unknown } : null;
  }

  async getR2JsonWithMetadata(keys: string[], limits: R2ReadLimits = GRANULAR_R2_LIMITS):
    Promise<{ key: string; data: unknown; uploaded?: Date } | null> {
    const stored = await this.readR2Text(keys, limits);
    return stored ? { key: stored.key, data: JSON.parse(stored.text) as unknown,
      ...(stored.uploaded ? { uploaded: stored.uploaded } : {}) } : null;
  }
}
