import { gunzipSync } from "node:zlib";
import { expect, it } from "vitest";
import { coachPresentation } from "../src/coach-presentation";
import { expectedGctSides } from "../src/gct-sides";
import { recordedRunningSegmentsSchema } from "../src/recorded-running-segments";
import current from "../../tests/fixtures/coach_engine_1100_raw_balance.json";
import logical from "../../tests/fixtures/coach_engine_190_derived_sides.json";
import legacy180 from "../../tests/fixtures/coach_engine_180_native_binding.json";
import recovery from "../../tests/fixtures/coach_engine_1100_recovery.json";
import { FreshDataService } from "../src/fresh-data";
import { parseCsv } from "../src/lib";

function currentAnalysis() {
  const pointer = JSON.parse(Buffer.from(current["activities/2026/42/coach-input/v1/latest-ready.json"], "base64").toString());
  const objects: Record<string, string> = current;
  return JSON.parse(gunzipSync(Buffer.from(objects[pointer.analysis_key], "base64")).toString());
}

it("the stored Coach package remains ready after a context-only append without replacing its analysis", async () => {
  const analysis = currentAnalysis(), pointerKey = "activities/2026/42/coach-input/v1/latest-ready.json";
  const pointer = JSON.parse(Buffer.from(current[pointerKey], "base64").toString());
  const values = new Map<string, Buffer>(Object.entries(current).map(([name, encoded]) => [name, Buffer.from(encoded, "base64")]));
  const currentContext = { schema_version: 1, activity_id: "42", activity_date: "2026-10-08", date_source: "garmin_local",
    context_id: "ctx-synthetic-current", rpe: 7, conditions: null, note: "Synthetic latest note", workout_correction: null,
    source: "user", created_at: "2026-10-08T12:00:00.000Z" };
  values.set("activities/2026/42/context/v1/20261008T120000000-ctx-synthetic-current.json", Buffer.from(JSON.stringify(currentContext)));
  const before = [...values].map(([name, bytes]) => [name, bytes.toString("base64")]);
  const read = async (keys: string[]) => {
    const name = keys.find(key => values.has(key));
    if (!name) return null;
    const bytes = values.get(name)!;
    return { key: name, data: JSON.parse((bytes[0] === 0x1f ? gunzipSync(bytes) : bytes).toString()) };
  };
  const service = new FreshDataService({ SLIPSTREAM_DATA: {
    list: async ({ prefix, limit }: { prefix: string; limit: number }) => {
      expect(limit).toBeLessThanOrEqual(1000);
      return { objects: [...values.keys()].filter(name => name.startsWith(prefix)).map(name => ({ key: name,
        uploaded: new Date("2026-10-08T12:00:00Z"), etag: pointer.source_revisions[name] ?? "synthetic" })), truncated: false };
    },
    put: async () => { throw new Error("Unexpected stored-view write"); },
    delete: async () => { throw new Error("Unexpected stored-view delete"); },
  } } as unknown as Env, {
    getActivities: async () => parseCsv("Activity ID,Activity Date,Activity Name,Activity Type,Activity Local Date\n"
      + "garmin-42,2026-10-08 07:00:00,Synthetic run,running,2026-10-08\n"),
    getR2Json: read, getCoachProfiles: async () => ({ keys: ["synthetic-profile"], profiles: [analysis.profile] }),
  }, { repository: "synthetic/repo", workflow: "refresh.yml", ref: "main", token: "synthetic", cooldownMinutes: 30 },
  () => Date.parse("2026-10-08T12:00:00Z"), async () => { throw new Error("Unexpected provider request"); });
  const result = await service.snapshot({ kind: "activity", activityId: "42", date: "2026-10-08", newExpected: false });
  expect(result.package.coach_status).toBe("ready");
  expect(result.missing_components).not.toContain("coach_input");
  expect(result.package.coach_input).toEqual(analysis);
  expect(result.package.user_context).toEqual(currentContext);
  expect([...values].map(([name, bytes]) => [name, bytes.toString("base64")])).toEqual(before);
});

it("coach output combines the verified kilometer measurements without dropping unknown-side balance", () => {
  const analysis = currentAnalysis(), frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  const header = "| Km | Pace (s/km) | HR avg/max (bpm) | Ascent/descent (m) | Cadence (steps/min) | Step length (mm) | GCT (ms) | VR (%) | GCT balance (%) |";
  expect(view.text.split(header)).toHaveLength(2);
  expect(view.text).toContain('| 1 | 300 | [null,null] | [null,null] | 161 | 1000 | 200 | 7 | {"value":48,"side_semantics":"unknown"} |');
  expect(view.text).toContain('"distance_m":200');
  expect(view.text).toContain('"partial":true');
  expect(view.text).toContain('"valid_seconds":60');
  expect(JSON.stringify(analysis)).toBe(frozen);
});

it("coach output presents a multi-lap logical rep once and does not guess planned completion", () => {
  const analysis = JSON.parse(gunzipSync(Buffer.from(
    (logical.objects as Record<string, string>)[logical.analysis_key], "base64")).toString());
  const frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  const header = "| Rep | Role | Duration (s) | Distance (m) / pace (s/km) | HR avg/max (bpm) | Cadence (steps/min) | Step length (mm) | GCT (ms) | VR (%) | GCT balance (%) |";
  expect(view.text.split(header)).toHaveLength(2);
  expect(view.text.split('\n').filter(line => line.startsWith('| 1 | "work" |'))).toHaveLength(1);
  expect(view.text).toContain('| 1 | "work" | 20 | [1000,20] | [null,null] | null | 1050 | 210 | 7 | {"value":48.5,"side_semantics":"unknown"} |');
  expect(view.text).toContain('"source_laps":[2,3]');
  expect(view.text).toContain('"observed_step_occurrence":2');
  expect(view.text).toContain('"planned_repeat_iteration":null');
  expect(view.text).toContain('"completion_status":"unknown"');
  expect(JSON.stringify(analysis)).toBe(frozen);
});

it("coach output keeps decoupling but warns against physiological interpretation of a structured workout", () => {
  const analysis = currentAnalysis();
  analysis.heart_rate.aerobic_decoupling_percent = 22.5;
  analysis.workout_structure.has_structured_workout = true;
  const frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  expect(view.text).toContain('"aerobic_decoupling_percent":22.5,"interpretability":"low"');
  expect(view.text).toContain('"structured_workout"');
  expect(JSON.stringify(analysis)).toBe(frozen);
});

it("coach output flags materially different half gradients without changing decoupling", () => {
  const analysis = currentAnalysis();
  analysis.heart_rate.aerobic_decoupling_percent = 0;
  analysis.workout_structure.has_structured_workout = false;
  analysis.summary.distance_m = 4000;
  analysis.distance_halves = { first: { distance_m: 2000, elevation_change_m: 100 },
    second: { distance_m: 2000, elevation_change_m: -100 } };
  const frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  expect(view.text).toContain('"aerobic_decoupling_percent":0,"interpretability":"low"');
  expect(view.text).toContain('"terrain_net_grade_imbalance"');
  expect(view.text).toContain('"net_grade_difference_percentage_points":10');
  expect(view.text).toContain('"display_guard_only_not_physiological_validation"');
  expect(JSON.stringify(analysis)).toBe(frozen);
});

it("current additions keep the genuine compact native-lap view within its existing size bound", () => {
  const view = coachPresentation(legacy180, { status: "not_stored", value: null });
  expect(view.text).toContain("| 1 | 7 | 3 | 400 | 4.5 | 111 | 1000 | 200 | 10 | 5 | 101 |");
  expect(view.text.length).toBeLessThanOrEqual(6000);
});

it("new-generation coach output is compact without repeating the native-lap and raw cadence tables", () => {
  const view = coachPresentation(currentAnalysis(), { status: "not_stored", value: null });
  expect(view.text).not.toContain("Native recorded laps:");
  expect(view.text).not.toContain("Record-derived cadence:");
  expect(view.text).toContain("| Km | Pace (s/km) |");
  expect(view.text).toContain("format=full");
  expect(view.text.length).toBeLessThanOrEqual(6000);
});

it("the recovery row uses its own measured GCT and raw balance, not the preceding work rep", () => {
  const analysis = JSON.parse(gunzipSync(Buffer.from(
    (recovery.objects_base64 as Record<string, string>)[`activities/2026/42/coach-input/v1/canonical/${recovery.analysis_id}.json`], "base64")).toString());
  const frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  expect(view.text).toContain('| 1 | "recovery" | 10 | [500,20] | [null,null] | null | null | 240 | 10 | {"value":48.24,"side_semantics":"unknown"} |');
  expect(view.text).not.toContain('"raw_side":"right"');
  expect(JSON.stringify(analysis)).toBe(frozen);
});

it("logical work and recovery rows retain their own recorded vertical ratios, including zero versus missing", () => {
  const analysis = JSON.parse(gunzipSync(Buffer.from(
    (recovery.objects_base64 as Record<string, string>)[`activities/2026/42/coach-input/v1/canonical/${recovery.analysis_id}.json`], "base64")).toString());
  const frozen = JSON.stringify(analysis);
  const view = coachPresentation(analysis, { status: "not_stored", value: null });
  expect(view.text).toContain("| Step length (mm) | GCT (ms) | VR (%) | GCT balance (%) |");
  const groupRows = view.text.split("\n").filter(row => /^\| \d+ \| "(work|recovery)"/.test(row));
  expect(groupRows.map(row => row.split(" | ")[8])).toEqual(["7", "10", "12"]);
  expect(JSON.stringify(analysis)).toBe(frozen);
  analysis.recovery_running_metrics.items[0].average_vertical_ratio.value = 0;
  expect(coachPresentation(analysis).text).toContain('| 240 | 0 | {"value":48.24');
  analysis.recovery_running_metrics.items[0].average_vertical_ratio = { status: "missing", value: null, unit: "%",
    source_field: "vertical_ratio", coverage: { status: "none", valid_seconds: 0 } };
  expect(coachPresentation(analysis).text).toContain('| 240 | null | {"value":48.24');
});

it("the current declared-side view propagates to the verified recovery row without relabeling its raw metric", () => {
  const analysis = JSON.parse(gunzipSync(Buffer.from(
    (recovery.objects_base64 as Record<string, string>)[`activities/2026/42/coach-input/v1/canonical/${recovery.analysis_id}.json`], "base64")).toString());
  const frozen = JSON.stringify(analysis);
  const sides = expectedGctSides(recordedRunningSegmentsSchema.parse(analysis.recorded_running_metrics),
    analysis.user_context, analysis.segment_running_metrics, { currentView: true, recovery: analysis.recovery_running_metrics });
  const groups = "record_aggregates" in sides ? sides.record_aggregates : {};
  expect((groups.recovery_bouts as { items: unknown[] }).items[0]).toMatchObject({
    bout: 3, source_laps: [4], raw_percent: 48.24, raw_side: "right", right_percent: 48.24, left_percent: 51.76,
    calculated_side: "left", method: "user_declared_record_mean_and_complement",
  });
  const view = coachPresentation(analysis, { status: "available", value: analysis.user_context, groundContactSides: sides });
  expect(view.text).toContain('{"value":48.24,"side_semantics":"right","left_percent":51.76,"right_percent":48.24,"calculated_side":"left"}');
  expect(JSON.stringify(analysis)).toBe(frozen);
});
