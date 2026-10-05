import { z } from "zod";

export const PRIVATE_READ_TOOL_ANNOTATIONS = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: false,
} as const;

export const exactDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).describe("YYYY-MM-DD");
export const dateRange = exactDate.optional();
export const sport = z.string().trim().min(1).max(64)
  .describe('Garmin sport type, e.g. "Run", "Ride", "Swim", "Yoga"; "Running"/"Løping" and "Cycling"/"Sykling" are also accepted').optional();
