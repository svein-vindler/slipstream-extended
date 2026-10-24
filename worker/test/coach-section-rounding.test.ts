import { expect, it } from "vitest";
import { validCoachTrainingEffect } from "../src/coach-training-effect";
import { readCurrentCoachInput } from "../src/coach-selection";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";

// Synthetic format-contract input, not a relabelled historical canonical fixture.
function legacy() {
  const utc = "2026-10-07T23:30:00.000Z", local = "2026-10-08T01:30:00";
  return { analyzer_version: "1.9.0", time_basis: "elapsed",
    activity: { date: "2026-10-08" }, source: { fit_sha256: "a".repeat(64) },
    recorded_running_metrics: { local_start_time: local, local_date: "2026-10-08",
      session_start_time_utc: utc, measured_at_utc: null },
    training_effect: { schema_version: 1, source: "fit_session", method: "fit_recorded_estimate",
      scale: "0_to_5", activity_id: "42", association: "matched", local_start_time: local,
      local_date: "2026-10-08", session_start_time_utc: utc, measured_at_utc: null,
      source_fit_sha256: "a".repeat(64),
      aerobic: { status: "available", source_field: "total_training_effect", value: 3.4 },
      anaerobic: { status: "available", source_field: "total_anaerobic_training_effect", value: 0 } },
    workout_structure: { source: "garmin_workout", has_structured_workout: true, planned_steps: [{ step_index: 0 }],
      executed_laps: [
        { lap: 1, section: "main", elapsed_seconds: 10.1, distance_m: 100, average_heart_rate_bpm: 100 },
        { lap: 2, section: "main", elapsed_seconds: 20.2, distance_m: 200, average_heart_rate_bpm: 200 },
      ] },
    sections: [{ section: "main", source: "garmin_executed_laps", laps: 2,
      elapsed_seconds: 30.3, distance_m: 300, average_heart_rate_bpm: 166.67,
      measurement_coverage: { elapsed_seconds: { status: "complete", observed_laps: 2, total_laps: 2 },
        distance_m: { status: "complete", observed_laps: 2, total_laps: 2 } },
      heart_rate_coverage: { basis: "lap_elapsed", covered_seconds: 30.3, contributing_laps: 2, status: "partial" } }],
  };
}

it("accepts only the exactly reconstructible 1.9.0 rounding variant without mutating it", () => {
  const value = legacy(), before = JSON.stringify(value);
  expect(validCoachTrainingEffect(value, "42")).toBe(true);
  expect(JSON.stringify(value)).toBe(before);
});

it("reads genuine old-producer canonical bytes through all current-selection validators", async () => {
  const fixture = JSON.parse(readFileSync(new URL("../../tests/fixtures/coach_engine_190_decimal_hr_rounding.json", import.meta.url), "utf8"));
  const bytes = new Map<string, Buffer>();
  for (const [key, body] of Object.entries(fixture.objects_base64)) bytes.set(key, Buffer.from(body as string, "base64"));
  const before = [...bytes].map(([key, body]) => [key, body.toString("base64")]);
  const result = await readCurrentCoachInput({
    canonicalPrefix: "activities/2026/42/coach-input/v1/canonical/", activityId: "42", date: "2026-10-08",
    read: async keys => {
      const key = keys.find(key => bytes.has(key));
      if (!key) return null;
      const body = bytes.get(key)!;
      return { key, data: JSON.parse((body[0] === 0x1f ? gunzipSync(body) : body).toString("utf8")) };
    },
    versions: async () => { throw new Error("Unexpected history discovery"); },
  });
  expect(result.status).toBe("available");
  if (result.status === "available") expect(result.analysis.analyzer_version).toBe("1.9.0");
  expect([...bytes].map(([key, body]) => [key, body.toString("base64")])).toEqual(before);
});

it.each(["version", "missing_pulse", "negative_time", "count", "mean", "distance", "duplicate", "unstructured", "extra"])
  ("rejects equal times with %s rather than widening the historical exception", damage => {
    const value = legacy();
    if (damage === "version") value.analyzer_version = "1.9.1";
    if (damage === "missing_pulse") (value.workout_structure.executed_laps[1] as Record<string, unknown>).average_heart_rate_bpm = null;
    if (damage === "negative_time") value.workout_structure.executed_laps[1].elapsed_seconds = -1;
    if (damage === "count") value.sections[0].heart_rate_coverage.contributing_laps = 1;
    if (damage === "mean") value.sections[0].average_heart_rate_bpm = 160;
    if (damage === "distance") value.sections[0].distance_m = 299;
    if (damage === "duplicate") value.sections.push(structuredClone(value.sections[0]));
    if (damage === "unstructured") value.workout_structure.has_structured_workout = false;
    if (damage === "extra") (value.sections[0].heart_rate_coverage as Record<string, unknown>).extra = 1;
    expect(validCoachTrainingEffect(value, "42")).toBe(false);
  });
