import { env, createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { afterEach, beforeAll, beforeEach, expect, it, vi } from "vitest";
import worker from "../src/index";
import { clearSummaryCaches } from "../src/r2-storage";
import { outputSchemas } from "../src/mcp-output";
import { PROTOCOL_VERSION_META_KEY, CLIENT_CAPABILITIES_META_KEY } from "@modelcontextprotocol/server";
import produced from "./generated/recorded-running-segments.json";
import produced16 from "./generated/coach-segment-metrics.json";
import legacy150 from "./fixtures/recorded-running-segments-engine-150.json";
import coach17 from "./fixtures/coach17-intermediate-contract.json";
import native18 from "./generated/native-lap-binding.json";
import native17 from "../../tests/fixtures/coach_engine_170_native_lap_transition.json";
import declaredSides from "./generated/gct-sides.json";
import legacy110 from "../../tests/fixtures/coach_engine_110_transition.json";
import genuine180 from "../../tests/fixtures/coach_engine_180_native_binding.json";
import genuine130 from "../../tests/fixtures/coach_engine_130_structured_transition.json";
import logicalBoutObjects from "./generated/logical-bouts.json";
import sourceOrderObjects from "./generated/logical-bouts-source-order.json";
import cadenceObjects from "../../tests/fixtures/coach_engine_190_cadence_schema1.json";
import derivedSideFixture from "../../tests/fixtures/coach_engine_190_derived_sides.json";
import targetsFixture from "../../tests/fixtures/coach_engine_190_targets.json";
import recordCadenceFixture from "../../tests/fixtures/coach_engine_190_record_cadence.json";
import typedZeroFixture from "../../tests/fixtures/coach_engine_190_typed_zero.json";
import workCadenceFixture from "../../tests/fixtures/coach_engine_190_record_cadence_work.json";
import recoveryCadenceFixture from "../../tests/fixtures/coach_engine_190_record_cadence_recovery.json";
import kmCadenceFixture from "../../tests/fixtures/coach_engine_190_record_cadence_km.json";
import decimalMainFixture from "../../tests/fixtures/coach_engine_190_main_decimal_elapsed.json";
import terminalCadenceFixture from "../../tests/fixtures/coach_engine_190_terminal_cadence.json";

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`AB23 SP02 terminal contribution: genuine partial cadence is readable but excluded pair cannot contribute via ${protocol}`, async () => {
    load(terminalCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: terminalCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const bytes = rows.get(terminalCadenceFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual terminal-cadence canonical");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing terminal-cadence canonical stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine.recorded_cadence.record_aggregates.whole_distance_halves).toMatchObject({
      status: "available", source_window: {
        original_record_count: 3, boundary_excluded_record_count: 1, boundary_excluded_pair_count: 1,
      },
    });
    expect(genuine.recorded_cadence.record_aggregates.whole_distance_halves.items[0].average_cadence).toMatchObject({
      status: "available", value: 160, unit: "steps_per_minute", coverage: {
        valid_seconds: 10, eligible_seconds: 10, contributing_intervals: 1,
        boundary_excluded_seconds: 10, excluded_gap_seconds: 0, status: "partial",
      },
    });
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    await editCoach(analysis => {
      const cadence = analysis.recorded_cadence as {
        record_aggregates: { whole_distance_halves: {
          items: Array<{ average_cadence: { coverage: { contributing_intervals: number } } }>;
        } };
      };
      const item = cadence.record_aggregates.whole_distance_halves.items[0].average_cadence.coverage;
      item.contributing_intervals = 2;
      const restored = structuredClone(analysis);
      const originalCadence = restored.recorded_cadence as typeof cadence;
      originalCadence.record_aggregates.whole_distance_halves.items[0].average_cadence.coverage.contributing_intervals = 1;
      expect(restored).toEqual(genuine);
    }, full.analysis.analysis_id);
    const mutated = [...rows].map(([name, body]) => [name, Array.from(body)]);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(mutated);
  });
  it(`AB23 SP01 submillisecond main cadence: false precision remains rejected via ${protocol}`, async () => {
    load(decimalMainFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: decimalMainFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const full = await call("coach_input", protocol);
    expect(full.available).toBe(true);
    expect(full.analysis.workout_structure.executed_laps[1].elapsed_seconds).toBe(1.001);
    expect(operations).toHaveLength(4);
    await editCoach(analysis => {
      const structure = analysis.workout_structure as { executed_laps: Array<{ elapsed_seconds: number }> };
      structure.executed_laps[1].elapsed_seconds = 1.0011;
    }, full.analysis.analysis_id);
    const mutated = [...rows].map(([name, body]) => [name, Array.from(body)]);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(mutated);
  });
  it(`AB23 SP01 decimal main cadence: genuine 1.001s window remains readable via ${protocol}`, async () => {
    load(decimalMainFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: decimalMainFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const bytes = rows.get(decimalMainFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual decimal-main canonical");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing decimal-main canonical stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine.workout_structure.executed_laps[1].elapsed_seconds).toBe(1.001);
    expect(genuine.recorded_cadence.record_aggregates.main_distance_halves).toMatchObject({
      status: "available", source_laps: [2, 3, 4, 5],
    });
    expect(genuine.recorded_cadence.record_aggregates.main_distance_halves.source_windows[0]).toMatchObject({
      lap: 2, distance_m: 50.05, end_time_utc: "2026-10-07T23:30:11.001Z",
    });
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Kilometer cadence compact first tracer: separate TCX split rows retain geometry and half scopes via ${protocol}`, async () => {
    load(kmCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: kmCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis.recorded_cadence.record_aggregates.kilometer_splits.items.map(
      (row: { average_cadence: { value: number } }) => row.average_cadence.value)).toEqual([161, 165, 169]);
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const text = coached.presentation.text;
    for (const row of [
      '| "kilometer_splits" | 1 | null | 161 |',
      '| "kilometer_splits" | 2 | null | 165 |',
      '| "kilometer_splits" | 3 | null | 169 |',
      '| "recovery_bouts" | 3 | [4] | 166 |',
      '| "whole_distance_halves" | "first" | null | 162 |',
      '| "main_distance_halves" | "first" | null | 163 |',
    ]) expect(text.split(row)).toHaveLength(2);
    expect(text).toContain("tcx_endurance_kilometer_splits");
    expect(text).toContain('"start_distance_m":0,"end_distance_m":1000,"distance_m":1000');
    expect(text).toContain('"valid_seconds":20');
    expect(outputSchemas.coach_input.parse(coached)).toEqual(coached);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  for (const mutation of ["native_lap_geometry", "wrong_split", "false_eligible_coverage"] as const) {
    it(`Kilometer cadence corruption: ${mutation} cannot turn laps into splits or invent coverage via ${protocol}`, async () => {
      load(kmCadenceFixture.objects);
      rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
        schema_version: 1, selection_version: 1, analysis_key: kmCadenceFixture.analysis_key, analyzer_version: "1.9.0",
      })));
      rows.set("summary/activities.csv", new TextEncoder().encode(
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
      const full = await call("coach_input", protocol, { format: "full" });
      expect(full.available).toBe(true);
      expect(full.analysis.recorded_cadence.record_aggregates.kilometer_splits.items[0].average_cadence.value).toBe(161);
      expect(operations).toHaveLength(4);
      await editCoach(analysis => {
        const block = analysis.recorded_cadence as { record_aggregates: { kilometer_splits: { items: Array<{
          split: number; end_distance_m: number; distance_m: number;
          average_cadence: { coverage: { valid_seconds: number; eligible_seconds: number } };
        }> } } };
        const row = block.record_aggregates.kilometer_splits.items[0];
        if (mutation === "native_lap_geometry") { row.end_distance_m = 500; row.distance_m = 500; }
        else if (mutation === "wrong_split") row.split = 2;
        else { row.average_cadence.coverage.valid_seconds = 21; row.average_cadence.coverage.eligible_seconds = 21; }
      }, full.analysis.analysis_id);
      const mutated = [...rows].map(([name, body]) => [name, Array.from(body)]);
      clearSummaryCaches(); operations.length = 0;
      const invalid = await call("coach_input", protocol);
      expect(invalid.available).toBe(false);
      expect(invalid).not.toHaveProperty("analysis");
      expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
      expect(operations).toHaveLength(4);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(mutated);
    });
  }
  it(`Verified kilometer cadence full reader first tracer: TCX partitions are not native laps via ${protocol}`, async () => {
    load(kmCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: kmCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const bytes = rows.get(kmCadenceFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual kilometer-cadence canonical");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing actual kilometer-cadence stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    const items = full.analysis.recorded_cadence.record_aggregates.kilometer_splits.items;
    expect(items.map((row: { split: number; start_distance_m: number; end_distance_m: number }) =>
      [row.split, row.start_distance_m, row.end_distance_m])).toEqual([[1, 0, 1000], [2, 1000, 2000], [3, 2000, 3000]]);
    expect(items.map((row: { average_cadence: { value: number } }) => row.average_cadence.value)).toEqual([161, 165, 169]);
    expect(full.analysis.recorded_running_metrics.laps.items).toHaveLength(6);
    expect(full.analysis.recorded_cadence.record_aggregates.recovery_bouts.items[0].average_cadence.value).toBe(166);
    expect(operations).toHaveLength(4);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Recovery cadence compact first tracer: recovery stays separate from work and half scopes via ${protocol}`, async () => {
    load(recoveryCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: recoveryCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis.recorded_cadence.record_aggregates.recovery_bouts.items[0].average_cadence.value).toBe(166);
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const text = coached.presentation.text;
    expect(text).toContain('| "recovery_bouts" | 3 | [4] | 166 |');
    expect(text).toContain('| "executed_intervals" | 1 | [2,3] | 163 |');
    expect(text).toContain('| "executed_intervals" | 2 | [5] | 168 |');
    expect(text).toContain('| "whole_distance_halves" | "first" | null | 162 |');
    expect(text).toContain('| "main_distance_halves" | "first" | null | 163 |');
    expect(text).toContain("fit_verified_recovery_bout_distance");
    expect(text).toContain('"valid_seconds":10');
    expect(outputSchemas.coach_input.parse(coached)).toEqual(coached);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  for (const mutation of ["wrong_source_lap", "wrong_elapsed_end", "false_elapsed_coverage"] as const) {
    it(`Recovery cadence corruption: ${mutation} cannot change the verified recovery scope via ${protocol}`, async () => {
      load(recoveryCadenceFixture.objects);
      rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
        schema_version: 1, selection_version: 1, analysis_key: recoveryCadenceFixture.analysis_key, analyzer_version: "1.9.0",
      })));
      rows.set("summary/activities.csv", new TextEncoder().encode(
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
      const full = await call("coach_input", protocol, { format: "full" });
      expect(full.available).toBe(true);
      expect(full.analysis.recorded_cadence.record_aggregates.recovery_bouts.items[0].average_cadence.value).toBe(166);
      expect(operations).toHaveLength(4);
      await editCoach(analysis => {
        const block = analysis.recorded_cadence as { record_aggregates: { recovery_bouts: { items: Array<{
          source_laps: number[]; end_time_utc: string;
          average_cadence: { coverage: { valid_seconds: number; eligible_seconds: number } };
        }> } } };
        const row = block.record_aggregates.recovery_bouts.items[0];
        if (mutation === "wrong_source_lap") row.source_laps = [5];
        else if (mutation === "wrong_elapsed_end") row.end_time_utc = "2026-10-07T23:30:50.000Z";
        else { row.average_cadence.coverage.valid_seconds = 11; row.average_cadence.coverage.eligible_seconds = 11; }
      }, full.analysis.analysis_id);
      const mutated = [...rows].map(([name, body]) => [name, Array.from(body)]);
      clearSummaryCaches(); operations.length = 0;
      const invalid = await call("coach_input", protocol);
      expect(invalid.available).toBe(false);
      expect(invalid).not.toHaveProperty("analysis");
      expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
      expect(operations).toHaveLength(4);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(mutated);
    });
  }
  it(`Verified recovery cadence full reader first tracer: recovery retains logical identity and distinct work values via ${protocol}`, async () => {
    load(recoveryCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: recoveryCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const bytes = rows.get(recoveryCadenceFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual recovery-cadence canonical");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing actual recovery-cadence stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine.recorded_cadence.record_aggregates.recovery_bouts.items[0]).toMatchObject({
      bout: 3, source_laps: [4], workout_step_index: 2,
      average_cadence: { value: 166, unit: "steps_per_minute", coverage: { valid_seconds: 10, eligible_seconds: 10 } },
    });
    const full = await call("coach_input", protocol, { format: "full" });
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    expect(full.analysis.logical_bouts.items.find((row: { bout: number }) => row.bout === 3))
      .toMatchObject({ role: "recovery", source_laps: [4], workout_step_index: 2, timing: { status: "available" } });
    expect(full.analysis.recorded_cadence.record_aggregates.executed_intervals.items.map(
      (row: { average_cadence: { value: number } }) => row.average_cadence.value)).toEqual([163, 168]);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Derived contact compact first tracer: native missing values and distinct declared record sides remain scoped via ${protocol}`, async () => {
    load(derivedSideFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: derivedSideFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    const bytes = rows.get(derivedSideFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual derived-side canonical fixture");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing actual derived-side canonical stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    // This frozen backup fixture contains the analysis only. Explicitly seed
    // its synthetic declaration as CURRENT context for the current-view check.
    const stamp = genuine.user_context.created_at.replace(/[-:.Z]/g, "");
    rows.set(`activities/2026/42/context/v1/${stamp}-${genuine.user_context.context_id}.json`,
      new TextEncoder().encode(JSON.stringify(genuine.user_context)));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    expect(full.analysis.ground_contact_sides.session).toMatchObject({
      status: "missing", left_percent: null, right_percent: null,
    });
    expect(full.analysis.segment_running_metrics.executed_intervals.items[0].average_ground_contact_balance)
      .toMatchObject({ value: 48.5, side_semantics: "unknown" });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(coached)).toEqual(coached);
    const text = coached.presentation.text;
    expect(text).toContain("Contact-balance side view:");
    expect(text.split("| Scope | Reference | Source laps (row) | Left (%) | Right (%) |")).toHaveLength(2);
    for (const row of [
      '| "native_session" | null | null | null | null |',
      '| "executed_intervals" | 1 | [2,3] | 48.5 | 51.5 |',
      '| "executed_intervals" | 2 | [5] | 52 | 48 |',
      '| "whole_distance_halves" | "first" | null | 49 | 51 |',
      '| "whole_distance_halves" | "second" | null | 50.6667 | 49.3333 |',
      '| "main_distance_halves" | "first" | null | 48.5 | 51.5 |',
      '| "main_distance_halves" | "second" | null | 51 | 49 |',
    ]) expect(text.split(row)).toHaveLength(2);
    expect(text).toContain('"source":"user_activity_declaration"');
    expect(text).toContain('"raw_side":"left"');
    expect(text).toContain('"both_sides_derived":true');
    expect(text).toContain("user_declared_record_mean_and_complement");
    expect(text).toContain("not two measured contact channels");
    expect(text).toContain('"status":"missing"');
    expect(text).toContain('"side_semantics":{"6":"unknown"}');
    expect(text).toContain('"valid_seconds":10');
    expect(text).toContain('"valid_seconds":20');
    expect(text).toContain('"valid_seconds":30');
    expect(text).not.toContain(JSON.stringify(full.analysis.ground_contact_sides));
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Record cadence compact first tracer: distinct whole main and work rows retain their own values and coverage via ${protocol}`, async () => {
    load(workCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: workCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis.recorded_cadence.record_aggregates.executed_intervals.items[1].average_cadence)
      .toMatchObject({ value: 168, coverage: { valid_seconds: 10, eligible_seconds: 10 } });
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const text = coached.presentation.text;
    expect(text).toContain("Record-derived cadence:");
    expect(text.split("| Scope | Reference | Source laps (row) | Average (steps/min) |")).toHaveLength(2);
    for (const row of [
      '| "whole_distance_halves" | "first" | null | 162 |',
      '| "whole_distance_halves" | "second" | null | 168 |',
      '| "main_distance_halves" | "first" | null | 163 |',
      '| "main_distance_halves" | "second" | null | 167 |',
      '| "executed_intervals" | 1 | [2,3] | 163 |',
      '| "executed_intervals" | 2 | [5] | 168 |',
    ]) expect(text.split(row)).toHaveLength(2);
    expect(text).toContain("fit_record_time_weighted_mean");
    expect(text).toContain('"valid_seconds":10');
    expect(text).toContain('"valid_seconds":20');
    expect(text).toContain("Record-derived whole_distance_halves:");
    expect(text).toContain("Record-derived main_distance_halves:");
    expect(text).not.toContain(JSON.stringify(full.analysis.recorded_cadence.record_aggregates));
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Verified work cadence full reader first tracer: work and main stay scoped to original intervals and halves via ${protocol}`, async () => {
    load(workCadenceFixture.objects);
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: workCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const bytes = rows.get(workCadenceFixture.analysis_key);
    if (!bytes) throw new Error("Missing actual work-cadence fixture");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing actual work-cadence stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine.recorded_cadence.record_aggregates.executed_intervals.items.map(
      (row: { average_cadence: { value: number } }) => row.average_cadence.value)).toEqual([163, 168]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(genuine);
    const groups = full.analysis.recorded_cadence.record_aggregates;
    expect(groups.main_distance_halves.items.map((row: { average_cadence: { value: number } }) => row.average_cadence.value))
      .toEqual([163, 167]);
    expect(groups.executed_intervals.items.map((row: { interval: number; source_laps: number[];
      workout_step_index: number; average_cadence: { value: number; coverage: { valid_seconds: number } } }) => [
      row.interval, row.source_laps, row.workout_step_index, row.average_cadence.value, row.average_cadence.coverage.valid_seconds,
    ])).toEqual([[1, [2, 3], 1, 163, 20], [2, [5], 1, 168, 10]]);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Typed target compact first tracer: planned units and custom zero are readable without repeating raw plan JSON via ${protocol}`, async () => {
    loadNativeForContract(targetsFixture);
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 09:00:00,Synthetic targets,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(targetsFixture);
    expect(full.analysis.workout_structure.planned_steps[0].duration_value).toBe(60000);
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const text = coached.presentation.text;
    expect(text).toContain("Verified planned targets:");
    expect(text).toContain("| Step | Name | Role | Planned duration | Target | Raw target code |");
    expect(text).toContain('| 0 | null | "active" | "60 s" | "speed custom 3..4 m/s" | 0 |');
    expect(text).toContain('| 1 | null | "recovery" | "400 m" | "open" | 0 |');
    expect(text).not.toContain("Workout plan: [");
    expect(text).not.toContain('"duration_value":60000');
    expect(text).toContain("fit_active_typed_workout_fields");
    expect(text).toContain("not a completion claim");
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Sparse typed target first tracer: unknown target fields remain readable and genuine duration zero survives via ${protocol}`, async () => {
    loadNativeForContract(typedZeroFixture);
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 01:30:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    expect(typedZeroFixture.analysis_id).toBe("6f0183db2a43d34f4d4d4095");
    expect(typedZeroFixture.workout_targets.items[1].target.unverified_source_values).toEqual({});
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(typedZeroFixture);
    expect(full.analysis.workout_targets.items[1].duration).toEqual({ status: "available", type: "time",
      value: 0, unit: "s", source_field: "duration_time", reason: null });
    expect(full.analysis.workout_targets.items[1].target).toEqual({ type: null, mode: "unverified", raw_value: null,
      range: { status: "unavailable", low: null, high: null, unit: null, source_fields: [], reason: "target_unit_unverified" },
      unverified_source_values: {} });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  for (const fault of ["unit", "coverage"] as const) {
    it(`Record cadence coverage integrity: ${fault} corruption is rejected without new source reads via ${protocol}`, async () => {
      load(recordCadenceFixture.objects);
      rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
        schema_version: 1, selection_version: 1, analysis_key: recordCadenceFixture.analysis_key, analyzer_version: "1.9.0",
      })));
      rows.set("summary/activities.csv", new TextEncoder().encode(
        "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
        + "garmin-42,2026-10-08 07:00:00,Synthetic record cadence,running,20,2026-10-08\n"));
      const genuine = await call("coach_input", protocol);
      expect(genuine.available).toBe(true);
      expect(genuine.analysis.recorded_cadence.record_aggregates.whole_distance_halves.items[0].average_cadence)
        .toMatchObject({ value: 161, unit: "steps_per_minute", coverage: { valid_seconds: 10, eligible_seconds: 10 } });
      expect(operations).toHaveLength(4);
      await editCoach(analysis => {
        const block = analysis.recorded_cadence as { record_aggregates: { whole_distance_halves: {
          items: Array<{ average_cadence: { unit: string; coverage: { valid_seconds: number } } }>;
        } } };
        const metric = block.record_aggregates.whole_distance_halves.items[0].average_cadence;
        if (fault === "unit") metric.unit = "rpm";
        else metric.coverage.valid_seconds = 11;
      }, genuine.analysis.analysis_id);
      clearSummaryCaches(); operations.length = 0;
      const invalid = await call("coach_input", protocol);
      expect(invalid.available).toBe(false);
      expect(invalid).not.toHaveProperty("analysis");
      expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
      expect(operations).toHaveLength(4);
    });
  }
  it(`Record cadence whole halves first tracer: genuine record means stay distinct from native cadence via ${protocol}`, async () => {
    load(recordCadenceFixture.objects);
    // Supply only the two missing outer contracts; canonical/profile/source bytes stay genuine.
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: recordCadenceFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 07:00:00,Synthetic record cadence,running,20,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const canonical = "activities/2026/42/coach-input/v1/canonical/a873199b6c3fe7eca53a0d4f.json";
    expect(recordCadenceFixture.analysis_key).toBe(canonical);
    const bytes = rows.get(canonical);
    if (!bytes) throw new Error("Missing genuine record-cadence canonical fixture");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing genuine record-cadence fixture stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine).toMatchObject({ analysis_id: "a873199b6c3fe7eca53a0d4f", analyzer_version: "1.9.0",
      activity_id: "42", recorded_cadence: { schema_version: 2, association: "matched",
        session: { average: { value: 180 }, maximum: { value: 250 } },
        laps: { items: [{ lap: 1, average: { value: 182 }, maximum: { value: 250 } }] } } });
    const omitted = await call("coach_input", protocol);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    // First causal assertion: the current native-only reader rejects cadence2.
    expect(omitted.available).toBe(true);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full.analysis).toEqual(genuine);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(full.analysis).toMatchObject({ analysis_id: "a873199b6c3fe7eca53a0d4f", analyzer_version: "1.9.0",
      summary: { elapsed_seconds: 20, distance_m: 20 },
      source: { fit_sha256: "2997d7270739416d4d0b6a50f51a2fb7e5972615cd3e8f963a62a8ffb7813262",
        tcx_sha256: "7aa12538bd79948587c4223dd848ddcfdfd6b04bf915d99be970fa0e383fcbd7" } });
    const cadence = full.analysis.recorded_cadence;
    expect(cadence).toMatchObject({ schema_version: 2, activity_id: "42", association: "matched",
      method: "fit_running_native_cadence_conversion",
      source_fit_sha256: "2997d7270739416d4d0b6a50f51a2fb7e5972615cd3e8f963a62a8ffb7813262",
      session: { average: { value: 180, unit: "steps_per_minute" }, maximum: { value: 250, unit: "steps_per_minute" } },
      laps: { status: "available", reason: null, items: [{ lap: 1,
        average: { value: 182, unit: "steps_per_minute" }, maximum: { value: 250, unit: "steps_per_minute" } }] } });
    expect(Object.keys(cadence.record_aggregates)).toEqual(["whole_distance_halves"]);
    const whole = cadence.record_aggregates.whole_distance_halves;
    expect(whole).toMatchObject({ status: "available", reason: null,
      method: "fit_record_time_weighted_mean", distance_basis: "tcx_endurance_distance",
      clock_basis: "fit_start_time_plus_total_elapsed_time", interval_policy: "left_sample_hold_distance_progressing_max_30s",
      distance_allocation: "linear_between_record_distances",
      source_tcx_sha256: "7aa12538bd79948587c4223dd848ddcfdfd6b04bf915d99be970fa0e383fcbd7",
      source_fields: ["cadence", "fractional_cadence"], input_unit: "rpm", unit: "steps_per_minute",
      conversion: "2 * (cadence + fractional_cadence)", valid_record_precision: "recorded_base_and_fraction",
      session_start_time_utc: "2026-10-08T07:00:00.000Z", session_end_time_utc: "2026-10-08T07:00:20.000Z",
      session_summary_timestamp_utc: "2026-10-08T07:00:20.000Z",
      source_window: { disposition: "verified_in_frame", original_record_count: 3,
        boundary_excluded_record_count: 0, boundary_excluded_pair_count: 0 } });
    expect(whole.items.map((row: { segment: string; start_distance_m: number; end_distance_m: number;
      distance_m: number; average_cadence: { value: number } }) => [row.segment, row.start_distance_m,
        row.end_distance_m, row.distance_m, row.average_cadence.value])).toEqual([
      ["first", 0, 10, 10, 161], ["second", 10, 20, 10, 164],
    ]);
    for (const row of whole.items) {
      expect(row.average_cadence).toMatchObject({ status: "available", unit: "steps_per_minute",
        source_fields: ["cadence", "fractional_cadence"] });
      expect(row.average_cadence.coverage).toEqual({ valid_seconds: 10, eligible_seconds: 10,
        excluded_gap_seconds: 0, contributing_intervals: 1, status: "complete", boundary_excluded_seconds: 0 });
      expect(row).not.toHaveProperty("maximum_cadence");
    }
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Typed workout target integrity: retains actual units and custom zero but rejects a false speed unit via ${protocol}`, async () => {
    loadNativeForContract(targetsFixture);
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 09:00:00,Synthetic targets,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis).toEqual(targetsFixture);
    expect(full.analysis.workout_targets.items[0]).toMatchObject({ step_index: 0,
      duration: { value: 60, unit: "s", source_field: "duration_time" },
      target: { type: "speed", mode: "custom", raw_value: 0,
        range: { low: 3, high: 4, unit: "m/s" } } });
    expect(full.analysis.workout_targets.items[1].duration).toMatchObject({ value: 400, unit: "m" });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    await editCoach(analysis => {
      const block = analysis.workout_targets as { items: Array<{ target: { range: { unit: string } } }> };
      block.items[0].target.range.unit = "rpm";
    }, targetsFixture.analysis_id);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toHaveLength(4);
  });
  it(`Derived contact side integrity: preserves scoped record means but rejects a false complement via ${protocol}`, async () => {
    load(derivedSideFixture.objects);
    // The authenticated-backup fixture preserves the actual canonical, not a
    // live current-selector. Supply only that outer storage contract here.
    rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
      schema_version: 1, selection_version: 1, analysis_key: derivedSideFixture.analysis_key, analyzer_version: "1.9.0",
    })));
    const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis.ground_contact_sides).toMatchObject({ schema_version: 2, status: "available",
      declaration: { raw_side: "left", context_id: "ctx-derived-sides", source: "user_activity_declaration" },
      record_aggregates: { executed_intervals: { items: [
        { interval: 1, left_percent: 48.5, right_percent: 51.5, both_sides_derived: true,
          method: "user_declared_record_mean_and_complement" },
        { interval: 2, left_percent: 52, right_percent: 48, both_sides_derived: true },
      ] } } });
    expect(full.analysis.user_context.note).toBe("Synthetic preserved note");
    expect(full.analysis.segment_running_metrics.executed_intervals.items[0].average_ground_contact_balance)
      .toMatchObject({ value: 48.5, side_semantics: "unknown" });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol), full);
    expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
    await editCoach(analysis => {
      const block = analysis.ground_contact_sides as { record_aggregates: { executed_intervals: { items: Array<{ right_percent: number }> } } };
      block.record_aggregates.executed_intervals.items[0].right_percent = 52;
    }, full.analysis.analysis_id);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toHaveLength(4);
  });
  it(`Native cadence compact first tracer: session and lap averages and maxima share recorded provenance via ${protocol}`, async () => {
    load(cadenceObjects);
    // Same explicit outer index as Main's corrected cadence integrity tracer.
    // run_one did not produce this index; all genuine producer bytes stay intact.
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 07:00:00,Synthetic run,running,60,2026-10-08\n"));
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const canonical = "activities/2026/42/coach-input/v1/canonical/6a27a75940c16475e45ccac3.json";
    const bytes = rows.get(canonical);
    if (!bytes) throw new Error("Missing genuine native cadence fixture");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing genuine native cadence fixture stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    const omitted = await call("coach_input", protocol);
    expect(omitted.available).toBe(true);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full.analysis).toEqual(genuine);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(full.analysis).toMatchObject({ analysis_id: "6a27a75940c16475e45ccac3", analyzer_version: "1.9.0",
      source: { fit_sha256: "21ab2bdb32f2f47cdcd748da793ed7f53b518b6994c5c718ea223ad28fdf2920" } });
    const cadence = full.analysis.recorded_cadence;
    expect(cadence).toMatchObject({ schema_version: 1, method: "fit_running_native_cadence_conversion",
      association: "matched", activity_id: "42",
      source_fit_sha256: "21ab2bdb32f2f47cdcd748da793ed7f53b518b6994c5c718ea223ad28fdf2920",
      session: { average: { value: 161 }, maximum: { value: 164 } },
      laps: { status: "available", reason: null, items: [{ lap: 1, average: { value: 161 }, maximum: { value: 164 } }] } });
    for (const row of [cadence.session, ...cadence.laps.items]) {
      for (const [name, value, sourceFields] of [
        ["average", 161, ["avg_cadence", "avg_fractional_cadence"]],
        ["maximum", 164, ["max_cadence", "max_fractional_cadence"]],
      ] as const) expect(row[name]).toEqual({ status: "available", value, unit: "steps_per_minute",
        source_fields: [...sourceFields], precision: "recorded_base_and_fraction", reason: null });
    }
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);

    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation).toMatchObject({ source_analysis_id: "6a27a75940c16475e45ccac3",
      source_analyzer_version: "1.9.0", scope: "selected_fields", full_data_hint: "format=full" });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    const text = coached.presentation.text;
    // First new display assertion, after actual reader/full/provenance/I/O guards.
    expect(text).toContain("Native recorded cadence:");
    expect(text.split("Native recorded cadence:")).toHaveLength(2);
    const start = text.indexOf("Native recorded cadence:");
    const end = text.indexOf("\nRecorded session measurements:", start);
    expect(end).toBeGreaterThan(start);
    const compact = text.slice(start, end);
    const header = "| Scope | Lap | Average (steps/min) | Maximum (steps/min) |";
    expect(text.split(header)).toHaveLength(2);
    for (const row of ['| "session" | null | 161 | 164 |', '| "lap" | 1 | 161 | 164 |']) {
      expect(compact.split(row)).toHaveLength(2);
    }
    expect(compact.match(/\b161\b/g)).toHaveLength(2);
    expect(compact.match(/\b164\b/g)).toHaveLength(2);
    for (const fact of ["fit_running_native_cadence_conversion", "steps_per_minute", "recorded_base_and_fraction",
      '"avg_cadence"', '"avg_fractional_cadence"', '"max_cadence"', '"max_fractional_cadence"',
      "21ab2bdb32f2f47cdcd748da793ed7f53b518b6994c5c718ea223ad28fdf2920"]) {
      expect(compact.split(fact)).toHaveLength(2);
    }
    expect(compact).toContain('"association":"matched"');
    expect(compact).toContain('"status":"available"');
    expect(compact).toContain('"reason":null');
    expect(compact).not.toContain(JSON.stringify(cadence));
    expect(compact).not.toContain('"session":');
    expect(compact).not.toContain('"items":');
    expect(compact).not.toMatch(/rpm|not_stored|Record-derived|average_power/);
    expect(text.slice(end)).toContain("| 300 | 1250 | 255 | 85 | 6.5 | 48.5 |");
  });
  it(`Recorded cadence integrity: genuine decoded source is readable but a false unit is unverified via ${protocol}`, async () => {
    load(cadenceObjects);
    // run_one's stored-source output does not include the outer activity index.
    // Supply only that external storage fixture; canonical bytes stay genuine.
    rows.set("summary/activities.csv", new TextEncoder().encode(
      "Activity ID,Activity Date,Activity Name,Activity Type,Moving Time,Activity Local Date\n"
      + "garmin-42,2026-10-08 07:00:00,Synthetic run,running,60,2026-10-08\n"));
    const baseline = await call("coach_input", protocol);
    expect(baseline.available).toBe(true);
    const analysis = baseline.analysis;
    expect(analysis.recorded_cadence).toMatchObject({ schema_version: 1, association: "matched",
      method: "fit_running_native_cadence_conversion", source_fit_sha256: analysis.source.fit_sha256,
      session: { average: { status: "available", value: 161, unit: "steps_per_minute" },
        maximum: { status: "available", value: 164, unit: "steps_per_minute" } },
      laps: { items: [{ lap: 1, average: { value: 161 }, maximum: { value: 164 } }] } });
    expect(operations).toHaveLength(4);
    await editCoach(value => {
      const block = value.recorded_cadence as { session: { average: { unit: string } } };
      block.session.average.unit = "rpm";
    }, analysis.analysis_id);
    const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
  });
  it(`Logical groups schema2 stored tracer: source groups remain visible without elapsed timing via ${protocol}`, async () => {
    // Exact real stored-source producer output, not a modified schema1 body.
    load(sourceOrderObjects);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const canonical = "activities/2026/42/coach-input/v1/canonical/de67c3b1c1dd511f29c6dbfe.json";
    const bytes = rows.get(canonical);
    if (!bytes) throw new Error("Missing genuine schema2 canonical fixture");
    const stream = new Response(bytes).body;
    if (!stream) throw new Error("Missing genuine schema2 fixture stream");
    const genuine = await new Response(stream.pipeThrough(new DecompressionStream("gzip"))).json();
    expect(genuine).toMatchObject({ analysis_id: "de67c3b1c1dd511f29c6dbfe", analyzer_version: "1.9.0",
      activity_id: "42", logical_bouts: { schema_version: 2, method: "verified_native_source_order",
        grouping_basis: "verified_native_source_order", status: "available", reason: null } });

    const omitted = await call("coach_input", protocol);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    // First causal assertion: the current schema1-only reader rejects this
    // genuine schema2 source-order analysis before compact formatting starts.
    expect(omitted.available).toBe(true);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full.analysis).toEqual(genuine);
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    const block = full.analysis.logical_bouts;
    expect(block).toMatchObject({ schema_version: 2, method: "verified_native_source_order",
      grouping_basis: "verified_native_source_order", status: "available", reason: null,
      occurrence_basis: "derived_verified_source_order_per_step", unassigned_laps: [],
      source_fit_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      source_coverage: { status: "complete", reason: null, raw_lap_count: 4, native_lap_count: 4,
        binding_count: 4, execution_count: 4 } });
    expect(block).not.toHaveProperty("clock_basis");
    expect(block.items.map((row: { bout: number; role: string; source_intensity: string;
      workout_step_index: number; observed_step_occurrence: number; planned_repeat_iteration: number | null;
      source_laps: number[]; source_message_indices: number[] }) => [row.bout, row.role, row.source_intensity,
        row.workout_step_index, row.observed_step_occurrence, row.planned_repeat_iteration,
        row.source_laps, row.source_message_indices])).toEqual([
      [1, "work", "active", 3, 1, null, [1, 2], [10, 14]],
      [2, "recovery", "recovery", 8, 1, null, [3], [21]],
      [3, "work", "active", 3, 2, null, [4], [30]],
    ]);
    for (const row of block.items) {
      expect(row.timing).toEqual({ status: "unavailable", basis: "fit_start_time_plus_total_elapsed_time",
        reason: "clock_basis_unverified", start_time_utc: null, end_time_utc: null });
      expect(row.record_aggregate).toEqual({ status: "unavailable", interval: null, reason: "clock_basis_unverified" });
      for (const name of ["start_time_utc", "end_time_utc", "duration_seconds", "distance_m"]) {
        expect(row).not.toHaveProperty(name);
      }
    }
    const native = full.analysis.recorded_running_metrics.laps.items;
    expect(native.map((row: { lap: number }) => row.lap)).toEqual([1, 2, 3, 4]);
    for (const [name, unit, values] of [
      ["average_power", "W", [111, 222, 333, 444]], ["average_step_length", "mm", [1000, 1100, 1200, 1300]],
      ["average_ground_contact_time", "ms", [200, 210, 220, 230]],
      ["average_vertical_oscillation", "mm", [10, 11, 12, 13]],
      ["average_vertical_ratio", "%", [5, 6, 7, 8]], ["average_ground_contact_balance", "%", [101, 103, 105, 107]],
    ] as const) {
      expect(native.map((row: Record<string, { value: number; status: string; unit: string }>) =>
        [row[name].value, row[name].status, row[name].unit])).toEqual(values.map(value => [value, "available", unit]));
    }
    expect(native.map((row: { average_ground_contact_balance: { side_semantics: string } }) =>
      row.average_ground_contact_balance.side_semantics)).toEqual(["unknown", "unknown", "unknown", "unknown"]);
    for (const name of ["executed_intervals", "whole_distance_halves", "main_distance_halves"]) {
      expect(full.analysis.segment_running_metrics[name]).toMatchObject({ status: "unavailable",
        reason: "clock_basis_unverified", items: [] });
    }
    expect(full.analysis.segment_running_metrics.session_end_time_utc).toBeNull();
    expect(full.analysis.segment_running_metrics.session_summary_timestamp_utc).toBe("2026-01-01T00:00:40.000Z");

    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation).toMatchObject({ source_analysis_id: "de67c3b1c1dd511f29c6dbfe",
      source_analyzer_version: "1.9.0", scope: "selected_fields", full_data_hint: "format=full" });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonical}`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    const text = coached.presentation.text;
    const start = text.indexOf("Logical groups:");
    expect(start).toBeGreaterThanOrEqual(0);
    const end = text.indexOf("\nNative recorded laps:", start);
    expect(end).toBeGreaterThan(start);
    const grouped = text.slice(start, end);
    expect(grouped).toContain('"schema_version":2');
    expect(grouped).toContain('"grouping_basis":"verified_native_source_order"');
    expect(grouped).toContain('"occurrence_basis":"derived_verified_source_order_per_step"');
    expect(grouped).toMatch(/timing[^\n]*"status":"unavailable"[^\n]*"basis":"fit_start_time_plus_total_elapsed_time"[^\n]*"reason":"clock_basis_unverified"/i);
    const header = "| Group | Role | Step | Observed occurrence (derived) | Planned repeat iteration | Source laps | Source messages | Start UTC | End UTC | Record interval |";
    expect(grouped.split(header)).toHaveLength(2);
    for (const row of [
      '| 1 | "work" | 3 | 1 | null | [1,2] | [10,14] | null | null | null |',
      '| 2 | "recovery" | 8 | 1 | null | [3] | [21] | null | null | null |',
      '| 3 | "work" | 3 | 2 | null | [4] | [30] | null | null | null |',
    ]) expect(grouped.split(row)).toHaveLength(2);
    expect(grouped).not.toContain("not_stored");
    expect(grouped).not.toContain("2026-01-01T");
    expect(grouped).not.toContain("verified_native_lap_contiguity");
    expect(grouped).not.toContain(JSON.stringify(block));
    expect(grouped).not.toMatch(/average_power|Power \(W\)|\b111\b|\b222\b|\b333\b|\b444\b/);
    const nativeText = text.slice(end, text.indexOf("Record-derived executed_intervals:", end));
    for (const value of [111, 222, 333, 444]) expect(nativeText).toContain(`| ${value} |`);
    expect(text).toContain('Record-derived executed_intervals: "unavailable"; reason: "clock_basis_unverified"');
    expect(text).not.toContain("executed_intervals interval 1:");
    expect(text).toContain('whole_distance_halves first: unavailable; reason: "clock_basis_unverified"');
    expect(text).toContain('main_distance_halves first: unavailable; reason: "clock_basis_unverified"');
  });
  it(`Logical group clock integrity: rejects an unsupported final elapsed end via ${protocol}`, async () => {
    load(logicalBoutObjects);
    const original = await call("coach_input", protocol);
    expect(original.available).toBe(true);
    expect(original.analysis.logical_bouts.items.at(-1)).toMatchObject({ role: "cooldown", source_laps: [6],
      end_time_utc: "2026-10-07T23:31:00.000Z", record_aggregate: { status: "unavailable" } });
    await editCoach(analysis => {
      const block = analysis.logical_bouts as { items: Array<{ end_time_utc: string }> };
      block.items.at(-1)!.end_time_utc = "2026-10-07T23:32:00.000Z";
    }, original.analysis.analysis_id);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    clearSummaryCaches(); operations.length = 0;
    const invalid = await call("coach_input", protocol);
    expect(invalid.available).toBe(false);
    expect(invalid).not.toHaveProperty("analysis");
    expect(invalid.message).toContain("current coach input could not be verified");
    expect(outputSchemas.coach_input.parse(invalid)).toEqual(invalid);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${original.analysis.analysis_id}.json`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Logical groups compact first tracer: genuine schema1 keeps source groups distinct via ${protocol}`, async () => {
    // The already bound real producer1.9 object map, with schema1 continuity
    // clocks. It is not the prospective schema2 source-order/timing contract.
    load(logicalBoutObjects);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const omitted = await call("coach_input", protocol);
    expect(omitted.available).toBe(true);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/9487effdc2d0e426b5321387.json"]);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(outputSchemas.coach_input.parse(full)).toEqual(full);
    expect(full.analysis).toMatchObject({ analysis_id: "9487effdc2d0e426b5321387", analyzer_version: "1.9.0",
      activity_id: "42", summary: { elapsed_seconds: 60, distance_m: 3000 },
      source: { fit_sha256: "3144e4ad6c8c6c291e91d47dd521867805e05c1bc9ceb0a1e5b91b827e10a91f" },
      logical_bouts: { schema_version: 1, status: "available", reason: null,
        method: "verified_native_lap_contiguity", clock_basis: "fit_start_time_plus_total_elapsed_time",
        occurrence_basis: "derived_verified_bout_order_per_step",
        source_fit_sha256: "3144e4ad6c8c6c291e91d47dd521867805e05c1bc9ceb0a1e5b91b827e10a91f" } });
    const groups = full.analysis.logical_bouts.items;
    expect(groups.map((row: { bout: number; role: string; source_laps: number[]; source_message_indices: number[];
      workout_step_index: number; observed_step_occurrence: number; planned_repeat_iteration: number | null }) =>
      [row.bout, row.role, row.workout_step_index, row.observed_step_occurrence,
        row.planned_repeat_iteration, row.source_laps, row.source_message_indices])).toEqual([
      [1, "warmup", 0, 1, null, [1], [0]], [2, "work", 1, 1, null, [2, 3], [1, 2]],
      [3, "recovery", 2, 1, null, [4], [3]], [4, "work", 1, 2, null, [5], [4]],
      [5, "cooldown", 3, 1, null, [6], [5]],
    ]);
    expect(groups.map((row: { record_aggregate: { status: string; interval: number | null; reason: string | null } }) =>
      [row.record_aggregate.status, row.record_aggregate.interval, row.record_aggregate.reason])).toEqual([
      ["unavailable", null, "no_verified_record_aggregate"], ["available", 1, null],
      ["unavailable", null, "no_verified_record_aggregate"], ["available", 2, null],
      ["unavailable", null, "no_verified_record_aggregate"],
    ]);
    expect(full.analysis.recorded_running_metrics.laps.items.map(
      (row: { average_power: { value: number } }) => row.average_power.value)).toEqual([777, 777, 777, 777, 777, 777]);
    for (const lap of full.analysis.recorded_running_metrics.laps.items) {
      expect(lap.average_power).toMatchObject({ value: 777, status: "available", unit: "W" });
    }
    expect(full.analysis.segment_running_metrics.executed_intervals.items.map(
      (row: { interval: number; source_laps: number[]; workout_step_index: number; average_power: { value: number; unit: string } }) =>
        [row.interval, row.source_laps, row.workout_step_index, row.average_power.value, row.average_power.unit])).toEqual([
      [1, [2, 3], 1, 150, "W"], [2, [5], 1, 400, "W"],
    ]);
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation).toMatchObject({ source_analysis_id: "9487effdc2d0e426b5321387",
      source_analyzer_version: "1.9.0", scope: "selected_fields", full_data_hint: "format=full" });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/9487effdc2d0e426b5321387.json"]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    const text = coached.presentation.text;
    expect(text).toContain('Analysis: "9487effdc2d0e426b5321387"');
    expect(text).toContain('Analyzer: "1.9.0"');

    // First new assertion: the current formatter omits the verified logical
    // structure. The source guards above must pass before this causal failure.
    expect(text).toContain("Logical groups:");
    expect(text).toContain("\nNative recorded laps:");
    const start = text.indexOf("Logical groups:");
    const end = text.indexOf("\nNative recorded laps:", start);
    expect(end).toBeGreaterThan(start);
    const grouped = text.slice(start, end);
    expect(grouped).toContain('"schema_version":1');
    expect(grouped).toContain('"method":"verified_native_lap_contiguity"');
    expect(grouped).toContain('"clock_basis":"fit_start_time_plus_total_elapsed_time"');
    expect(grouped).toContain('"occurrence_basis":"derived_verified_bout_order_per_step"');
    expect(grouped.split("3144e4ad6c8c6c291e91d47dd521867805e05c1bc9ceb0a1e5b91b827e10a91f")).toHaveLength(2);
    expect(text.split("9487effdc2d0e426b5321387")).toHaveLength(2);
    const header = "| Group | Role | Step | Observed occurrence (derived) | Planned repeat iteration | Source laps | Source messages | Start UTC | End UTC | Record interval |";
    expect(grouped.split(header)).toHaveLength(2);
    for (const row of [
      '| 1 | "warmup" | 0 | 1 | null | [1] | [0] | "2026-10-07T23:30:00.000Z" | "2026-10-07T23:30:10.000Z" | null |',
      '| 2 | "work" | 1 | 1 | null | [2,3] | [1,2] | "2026-10-07T23:30:10.000Z" | "2026-10-07T23:30:30.000Z" | 1 |',
      '| 3 | "recovery" | 2 | 1 | null | [4] | [3] | "2026-10-07T23:30:30.000Z" | "2026-10-07T23:30:40.000Z" | null |',
      '| 4 | "work" | 1 | 2 | null | [5] | [4] | "2026-10-07T23:30:40.000Z" | "2026-10-07T23:30:50.000Z" | 2 |',
      '| 5 | "cooldown" | 3 | 1 | null | [6] | [5] | "2026-10-07T23:30:50.000Z" | "2026-10-07T23:31:00.000Z" | null |',
    ]) expect(grouped.split(row)).toHaveLength(2);
    expect(grouped.split("no_verified_record_aggregate")).toHaveLength(2);
    expect(grouped).not.toContain("verified_native_source_order");
    expect(grouped).not.toContain(JSON.stringify(full.analysis.logical_bouts));
    // Group references carry no new lap mean or copy of the native measures.
    expect(grouped).not.toMatch(/\b777\b|average_power|Power \(W\)|150 W|400 W/);
    const native = text.slice(text.indexOf("Native recorded laps:"), text.indexOf("Record-derived executed_intervals:"));
    expect(native.match(/\| 777 \|/g)).toHaveLength(6);
    expect(text).toContain("executed_intervals interval 1:");
    expect(text).toContain("executed_intervals interval 2:");
    expect(text).toContain("150 W");
    expect(text).toContain("400 W");
  });
  it(`Logical bouts current reader: actual stored producer1.9 remains readable via ${protocol}`, async () => {
    // Exact output of the real stored-source producer, not a relabelled1.8 body.
    load(logicalBoutObjects);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toMatchObject({ analyzer_version: "1.9.0", activity_id: "42",
      summary: { elapsed_seconds: 60, distance_m: 3000 },
      recorded_running_metrics: { schema_version: 2, association: "matched" },
      logical_bouts: { status: "available", source_fit_sha256: output.analysis.source.fit_sha256 } });
    const groups = output.analysis.logical_bouts.items;
    expect(groups.map((row: { role: string; source_laps: number[]; observed_step_occurrence: number;
      planned_repeat_iteration: number | null }) => [row.role, row.source_laps,
      row.observed_step_occurrence, row.planned_repeat_iteration])).toEqual([
      ["warmup", [1], 1, null], ["work", [2, 3], 1, null], ["recovery", [4], 1, null],
      ["work", [5], 2, null], ["cooldown", [6], 1, null],
    ]);
    expect(output.analysis.recorded_running_metrics.laps.items.map(
      (row: { average_power: { value: number } }) => row.average_power.value)).toEqual([777, 777, 777, 777, 777, 777]);
    expect(output.analysis.segment_running_metrics.executed_intervals.items.map(
      (row: { average_power: { value: number } }) => row.average_power.value)).toEqual([150, 400]);
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${output.analysis.analysis_id}.json`]);
    clearSummaryCaches(); operations.length = 0;
    expectSameCoachRead(await call("coach_input", protocol, { format: "full" }), output);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Compact stored context first tracer: genuine1.3 absence stays explicit via ${protocol}`, async () => {
    // Frozen real1.3 canonical and selector envelope; never relabel as1.8 or
    // invent collected/retained/failed observations absent from this fixture.
    load(genuine130.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const omitted = await call("coach_input", protocol);
    expect(omitted.available).toBe(true);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/d6f3b079934fcf63af5a4988.json"]);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full.analysis).toMatchObject({ analysis_id: "d6f3b079934fcf63af5a4988", analyzer_version: "1.3.0",
      activity: { date: "2026-10-08" }, summary: { distance_m: 2000, average_heart_rate_bpm: 145 },
      stored_context_snapshot: { schema_version: 1, snapshot_at: null,
        reference_time_utc: "2026-10-07T23:30:00.000Z", semantics: "retrospective_pre_activity",
        sleep: { status: "not_collected", source: "garmin", source_checked_at: null,
          wake_date: "2026-10-08", night_of: null, local_time_source: "unavailable",
          observation_start_utc: null, observation_end_utc: null,
          metrics: { sleep_seconds: { value: null, status: "unavailable", unit: "s" },
            sleep_score: { value: null, status: "unavailable", unit: "score_0_100" } } },
        hrv: { status: "not_collected", source: "garmin", source_checked_at: null,
          wake_date: "2026-10-08", night_of: null, local_time_source: "unavailable",
          metrics: { last_night_avg_ms: { value: null, status: "unavailable", unit: "ms" },
            weekly_avg_ms: { value: null, status: "unavailable", unit: "ms" } } },
        training_day: { status: "not_collected", selection_status: "unknown", selected_slot: null,
          temporal_status: "selection_unavailable",
          readiness: { value: null, status: "unavailable", unit: "score_1_100",
            observation_at: null, source_checked_at: null, retained_previous: false },
          recovery: { value: null, status: "unavailable", unit: "minutes",
            observation_at: null, source_checked_at: null, retained_previous: false } } } });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation).toMatchObject({ source_analysis_id: "d6f3b079934fcf63af5a4988",
      source_analyzer_version: "1.3.0", scope: "selected_fields", full_data_hint: "format=full" });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/d6f3b079934fcf63af5a4988.json"]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    const text = coached.presentation.text;
    expect(text).toContain('Analysis: "d6f3b079934fcf63af5a4988"');
    expect(text).toContain('Analyzer: "1.3.0"');

    // First new assertion: complete JSON belongs in full, not in the compact
    // context section. Equality/known literals above protect the genuine data.
    expect(text).not.toContain(JSON.stringify(full.analysis.stored_context_snapshot));
    expect(text).toContain("Stored context:");
    expect(text).toContain("\nSections:");
    const context = text.slice(text.indexOf("Stored context:"), text.indexOf("\nSections:"));
    expect(context).toContain('"schema_version":1');
    expect(context).toContain('"snapshot_at":null');
    expect(context).toContain('"reference_time_utc":"2026-10-07T23:30:00.000Z"');
    expect(context).toContain('"semantics":"retrospective_pre_activity"');
    expect(context).toContain("  sleep:");
    expect(context).toContain("  hrv:");
    expect(context).toContain("  training_day:");
    const sleep = context.slice(context.indexOf("  sleep:"), context.indexOf("  hrv:"));
    const hrv = context.slice(context.indexOf("  hrv:"), context.indexOf("  training_day:"));
    const training = context.slice(context.indexOf("  training_day:"));
    for (const source of [sleep, hrv]) {
      expect(source).toContain('"status":"not_collected"');
      expect(source).toContain('"source":"garmin"');
      expect(source).toContain('"source_checked_at":null');
      expect(source).toContain('"wake_date":"2026-10-08"');
      expect(source).toContain('"night_of":null');
      expect(source).toContain('"local_time_source":"unavailable"');
    }
    expect(sleep).toContain('sleep_seconds: null; status: "unavailable"; unit: "s"');
    expect(sleep).toContain('sleep_score: null; status: "unavailable"; unit: "score_0_100"');
    expect(hrv).toContain('last_night_avg_ms: null; status: "unavailable"; unit: "ms"');
    expect(hrv).toContain('weekly_avg_ms: null; status: "unavailable"; unit: "ms"');
    expect(training).toContain('"status":"not_collected"');
    expect(training).toContain('"selection_status":"unknown"');
    expect(training).toContain('"selected_slot":null');
    expect(training).toContain('"temporal_status":"selection_unavailable"');
    const readiness = training.split("\n").find((line: string) => line.includes("readiness:"));
    const recovery = training.split("\n").find((line: string) => line.includes("recovery:"));
    expect(readiness).toContain('readiness: null; status: "unavailable"; unit: "score_1_100"');
    expect(recovery).toContain('recovery: null; status: "unavailable"; unit: "minutes"');
    for (const metric of [readiness!, recovery!]) {
      expect(metric).toContain("observation_at: null");
      expect(metric).toContain("source_checked_at: null");
      expect(metric).toContain("retained_previous: false");
    }
    // Only this frozen sparse context's budget; no general size/cost claim.
    expect(new TextEncoder().encode(context).byteLength).toBeLessThanOrEqual(1500);
  });
  it(`Compact Coach first tracer: one native lap table keeps genuine1.8 facts via ${protocol}`, async () => {
    // Genuine frozen producer output: no presentation/selector/internal fake,
    // no fabricated successful analysis and no relabelled historical fixture.
    loadNativeForContract(genuine180);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const omitted = await call("coach_input", protocol);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/98685be355b2de697f0166d8.json"]);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full.analysis).toEqual(genuine180);
    expect(full.analysis).toMatchObject({ analysis_id: "98685be355b2de697f0166d8", analyzer_version: "1.8.0",
      summary: { distance_m: 900, average_heart_rate_bpm: null },
      heart_rate: { zones_total: { observed_seconds: 0 } } });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation).toMatchObject({ version: "1.0.0", format: "coach_text", scope: "selected_fields",
      source_analysis_id: "98685be355b2de697f0166d8", source_analyzer_version: "1.8.0", full_data_hint: "format=full" });
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      "GET activities/2026/42/coach-input/v1/canonical/98685be355b2de697f0166d8.json"]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    const text = coached.presentation.text;
    expect(text).toContain('Analysis: "98685be355b2de697f0166d8"');
    expect(text).toContain('Analyzer: "1.8.0"');

    // First new requirement: one readable table, preserving source identities
    // and all six native measurements. Step3 is NOT proof of two repetitions.
    // These literal rows come from the frozen FIT/Coach fixture, not a renderer.
    const header = "| Lap | Source message | Step | Distance (m) | Executed elapsed (s) | Power (W) | Step length (mm) | GCT (ms) | VO (mm) | VR (%) | GCT balance (%) |";
    expect(text).toContain(header);
    expect(text.split(header)).toHaveLength(2);
    expect(text.split("| 1 | 7 | 3 | 400 | 4.5 | 111 | 1000 | 200 | 10 | 5 | 101 |")).toHaveLength(2);
    expect(text.split("| 2 | 9 | 3 | 500 | 4.5 | 222 | 1100 | 210 | 11 | 6 | 103 |")).toHaveLength(2);
    expect(text).toContain("Native recorded laps:");
    expect(text).toContain("Workout plan:");
    expect(text).toContain('"Synthetic repeated step"');
    expect(text).toContain('"Heart-rate profile"');
    expect(text).not.toContain("Stored executed laps (separate from native rows):");
    expect(text).not.toMatch(/^Lap [12]:$/m);
    expect(text).not.toContain(JSON.stringify(full.analysis.workout_structure.executed_laps));
    expect(text).not.toContain(JSON.stringify(full.analysis.recorded_running_metrics.distance_halves));

    expect(text.match(/^Summary:/gm)).toHaveLength(1);
    expect(text).toContain("distance_m: 900");
    expect(text).toContain("average_heart_rate_bpm: null");
    expect(text).toContain('"observed_seconds":0');
    expect(text).toContain("Recorded session measurements:");
    expect(text).toContain('average_power: null; status: "missing"');
    expect(text).toContain('average_step_length: null; status: "missing"');
    expect(text).toContain('average_ground_contact_time: null; status: "missing"');
    expect(text).toContain('average_vertical_oscillation: null; status: "missing"');
    expect(text).toContain('average_vertical_ratio: null; status: "missing"');
    expect(text).toContain('average_ground_contact_balance: null; status: "missing"');
    expect(text).toContain("User context: not_stored");
    expect(text).toContain("Stored context: not_stored");

    // The two whole-distance sources differ in clock/window and second-half
    // power; equal first-half values do not permit an alias or an invented main.
    expect(text).toContain("Recorded FIT distance halves:");
    expect(text).toContain("388.8889 W");
    expect(text).toContain("whole_distance_halves first:");
    expect(text).toContain("166.6667 W");
    expect(text).toContain("whole_distance_halves second:");
    expect(text).toContain("357.1429 W");
    const firstHalf = text.slice(text.indexOf("whole_distance_halves first:"), text.indexOf("whole_distance_halves second:"));
    const secondHalf = text.slice(text.indexOf("whole_distance_halves second:"), text.indexOf("Record-derived main_distance_halves:"));
    for (const value of ["166.6667 W", "1000 mm", "200 ms", "10 mm", "5 %", "101 %"]) expect(firstHalf).toContain(value);
    for (const value of ["357.1429 W", "1000 mm", "200 ms", "10 mm", "5 %", "101 %"]) expect(secondHalf).toContain(value);
    expect(text).toContain("main_distance_halves first: unavailable");
    expect(text).toContain("main_distance_halves second: unavailable");
    expect(text).toContain("clock_basis_unverified");
    expect(text).toContain("Coverage/warnings:");
    expect(text.split('"boundary_excluded_seconds":0.5')).toHaveLength(2);
    const warnings = text.slice(text.indexOf("Coverage/warnings:"));
    expect(warnings).toContain('"status":"partial"');
    expect(warnings).toContain('"eligible_seconds":3.5');
    expect(warnings).toContain('"valid_seconds":3.5');
    expect(warnings).toContain('"boundary_excluded_seconds":0.5');
    expect(text).toContain("unknown");
    expect(text).toContain("declaration_missing");
    // Fixture-specific readability ceiling, never a general savings claim.
    // The independent rows/halves/missing-state checks above forbid pruning
    // the native measurements simply to meet this ceiling.
    expect(new TextEncoder().encode(text).byteLength).toBeLessThanOrEqual(6000);
  });
  it(`Coach presentation quoting: legacy source strings stay data, not extra field lines via ${protocol}`, async () => {
    load(legacy110.objects_base64);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.available).toBe(true);
    expect(full.analysis.analyzer_version).toBe("1.1.0");
    const normal = await call("coach_input", protocol, { format: "coach" });
    expect(normal.presentation.text).toContain('Analysis: "deb436b814332360e4773d6d"');
    expect(normal.presentation.text).toContain('Analyzer: "1.1.0"');
    // An outer stored-input fault, not a newly generated producer version.
    // The existing legacy reader accepts these strings; the new view must
    // preserve them as data without inventing new application field lines.
    await editCoach(analysis => {
      analysis.analyzer_version = "legacy\nSummary: forged-version";
      (analysis.recorded_running_metrics as { average_power: { unit: string } }).average_power.unit = "W\nSummary: forged-unit";
      analysis.user_context = { note: "Synthetic\nSummary: forged-note", rpe: 0 };
      // Current user strings now come from their own append-only storage seam,
      // not the selected historical analysis. Keep the same hostile text test.
      rows.set("activities/2026/42/context/v1/20261024T120000000-ctx-escape.json", new TextEncoder().encode(JSON.stringify({
        schema_version: 1, activity_id: "42", context_id: "ctx-escape", source: "user",
        created_at: "2026-10-24T12:00:00.000Z", rpe: 0, conditions: null,
        note: "Synthetic\nSummary: forged-note", workout_correction: null,
      })));
    }, "deb436b814332360e4773d6d");
    clearSummaryCaches(); operations.length = 0;
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const result = await call("coach_input", protocol, { format: "coach" });
    expect(result.available).toBe(true);
    const text = result.presentation.text;
    expect(text).toContain('Analyzer: "legacy\\nSummary: forged-version"');
    expect(text).toContain('"W\\nSummary: forged-unit"');
    expect(text).toContain('"note":"Synthetic\\nSummary: forged-note"');
    expect(text).not.toMatch(/^Summary: forged-/m);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Coach presentation resource: deterministic excerpt keeps source and read budgets via ${protocol}`, async () => {
    loadNativeForContract(native18.complete);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    clearSummaryCaches(); operations.length = 0;
    const first = await call("coach_input", protocol, { format: "coach" });
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    const second = await call("coach_input", protocol, { format: "coach" });
    expectSameCoachRead(second, first);
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    console.info(JSON.stringify({ synthetic_presentation_bytes: new TextEncoder().encode(JSON.stringify(first)).length,
      synthetic_full_bytes: new TextEncoder().encode(JSON.stringify(full)).length, immutable_analysis_reads_per_view: 4,
      source_mutations: 0, protocol }));
  });
  it(`Coach presentation rich facts: original scopes and native references survive via ${protocol}`, async () => {
    loadNativeForContract(native18.complete);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.analysis).toEqual(native18.complete);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const text = coached.presentation.text;
    expect(text).toContain("Summary:");
    expect(text).toContain("distance_m: 900");
    expect(text).toContain("Profile/references:");
    expect(text).toContain('"profile_id":"synthetic-partial"');
    expect(text).toContain('"min_bpm":100');
    expect(text).toContain('"max_bpm":150');
    expect(text).toContain("User context: not_stored");
    expect(text).toContain("Training Effect:");
    expect(text).toContain('scale: "0_to_5"');
    expect(text).toContain('aerobic: null; status: "missing"');
    expect(text).toContain("Stored context: not_stored");
    expect(text).toContain("Sections:");
    expect(text).toContain('"section":"main"');
    // Current compact tables do not duplicate raw laps; full retains all facts.
    expect(text).not.toContain("Native recorded laps:");
    expect(text).toContain("Logical rep table");
    expect(full.analysis.recorded_running_metrics.laps.items).toMatchObject([
      { lap: 1, distance_m: 400, average_power: { value: 111 }, average_step_length: { value: 1000 },
        average_ground_contact_time: { value: 200 }, average_vertical_oscillation: { value: 10 },
        average_vertical_ratio: { value: 5 }, average_ground_contact_balance: { value: 101 } },
      { lap: 2, distance_m: 500, average_power: { value: 222 }, average_step_length: { value: 1100 },
        average_ground_contact_time: { value: 210 }, average_vertical_oscillation: { value: 11 },
        average_vertical_ratio: { value: 6 }, average_ground_contact_balance: { value: 103 } },
    ]);
    expect(full.analysis.native_lap_workout_binding.items).toMatchObject([
      { lap: 1, source_message_index: 7, workout_step_index: 3 },
      { lap: 2, source_message_index: 9, workout_step_index: 3 },
    ]);
    expect(text).toContain("whole_distance_halves first");
    expect(text).toContain("166.6667 W");
    expect(text).toContain("whole_distance_halves second");
    expect(text).toContain("357.1429 W");
    expect(text).toContain('"boundary_excluded_seconds":0.5');
    expect(text).toContain('"status":"partial"');
    expect(text).toContain("main_distance_halves first: unavailable");
    expect(text).toContain("main_distance_halves second: unavailable");
    expect(text).toContain("clock_basis_unverified");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${native18.complete.analysis_id}.json`]);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
}

const issuer = "https://running-segments-synthetic.cloudflareaccess.com";
const hostname = "running-segments.example";
const rows = new Map<string, Uint8Array>();
const operations: string[] = [];
// Keep the original immutable-analysis I/O assertions exact. Current-context
// reads are an additional, separately bounded operation measured at each call.
const contextOperations: string[] = [];
function expectSameCoachRead(actual: Record<string, unknown>, expected: Record<string, unknown>) {
  const view = actual.view as Record<string, unknown>;
  expect(view.resolved_at).toEqual(expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/));
  expect(actual).toEqual({ ...expected, view: { ...expected.view as Record<string, unknown>, resolved_at: view.resolved_at } });
}
const enduranceKey = "activities/2026/42/activity.endurance.v1.json";
let key: CryptoKey, jwk: Record<string, unknown>, token: string;
let testEnv: Env;
function load(objects: Record<string, string>) {
  rows.clear();
  for (const [name, body] of Object.entries(objects)) {
    rows.set(name, Uint8Array.from(atob(body), character => character.charCodeAt(0)));
  }
}
async function editStored(change: (block: Record<string, unknown>) => void) {
  const bytes = rows.get(enduranceKey);
  if (!bytes) throw new Error("Missing genuine endurance fixture");
  const stream = new Response(bytes).body;
  if (!stream) throw new Error("Missing genuine endurance fixture stream");
  // Real export objects may be gzip encoded. Fault injection is at the outer
  // storage boundary, after decoding, not inside the production projection.
  const plain = bytes[0] === 0x1f && bytes[1] === 0x8b ? stream.pipeThrough(new DecompressionStream("gzip")) : stream;
  const body = await new Response(plain).json() as { recorded_running_metrics_v2: Record<string, unknown> };
  change(body.recorded_running_metrics_v2);
  rows.set(enduranceKey, new TextEncoder().encode(JSON.stringify(body)));
}
async function editCoach(change: (analysis: Record<string, unknown>) => void, analysisId = produced.analysis_id) {
  const canonicalKey = `activities/2026/42/coach-input/v1/canonical/${analysisId}.json`;
  const bytes = rows.get(canonicalKey);
  if (!bytes) throw new Error("Missing genuine canonical Coach fixture");
  const stream = new Response(bytes).body;
  if (!stream) throw new Error("Missing genuine canonical Coach fixture stream");
  const plain = bytes[0] === 0x1f && bytes[1] === 0x8b ? stream.pipeThrough(new DecompressionStream("gzip")) : stream;
  const body = await new Response(plain).json() as Record<string, unknown>;
  change(body);
  rows.set(canonicalKey, new TextEncoder().encode(JSON.stringify(body)));
}

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  key = pair.privateKey;
  jwk = { ...await exportJWK(pair.publicKey), kid: "running-segments", alg: "RS256", use: "sig" };
});
beforeEach(async () => {
  clearSummaryCaches(); rows.clear(); operations.length = 0;
  // These objects are generated by the real Python FIT/TCX/Coach producers.
  load(produced.objects_base64);
  vi.spyOn(globalThis, "fetch").mockImplementation(async input => {
    const url = input instanceof Request ? input.url : String(input);
    if (url !== `${issuer}/cdn-cgi/access/certs`) throw new Error("Unexpected running segment fixture network request");
    return Response.json({ keys: [jwk] });
  });
  token = await new SignJWT({ type: "app", email: "synthetic@example.invalid" })
    .setProtectedHeader({ alg: "RS256", kid: "running-segments" }).setIssuer(issuer).setAudience("running-segments")
    .setSubject(crypto.randomUUID()).setIssuedAt().setExpirationTime("5m").sign(key);
  const bucket = {
    head: async (name: string) => { operations.push(`HEAD ${name}`); return rows.has(name) ? { etag: "synthetic-segments" } : null; },
    get: async (name: string) => {
      (name.includes("/context/v1/") ? contextOperations : operations).push(`GET ${name}`);
      const body = rows.get(name);
      return body === undefined ? null : { size: body.byteLength, etag: "synthetic-segments", body: new Response(body).body };
    },
    list: async (options: R2ListOptions) => {
      if (/^activities\/(2025|2026|2027)\/42\/context\/v1\/$/.test(options.prefix ?? "")) {
        expect(options.limit).toBe(100);
        contextOperations.push("LIST " + options.prefix);
        return { truncated: false, objects: [...rows.keys()].filter(name => name.startsWith(options.prefix!))
          .map(name => ({ key: name })) };
      }
      operations.push("LIST"); throw new Error("Unexpected legacy discovery");
    },
    put: async () => { operations.push("PUT"); throw new Error("Unexpected fixture write"); },
    delete: async () => { operations.push("DELETE"); throw new Error("Unexpected fixture delete"); },
  } as unknown as R2Bucket;
  testEnv = { ...env, ACCESS_TEAM_DOMAIN: issuer, ACCESS_AUD: "running-segments", MCP_HOSTNAME: hostname,
    SLIPSTREAM_DATA: bucket, MCP_WRITES_ENABLED: "false", GITHUB_ACTIONS_TOKEN: "", GITHUB_REPOSITORY: "",
    MCP_RATE_LIMITER: { limit: async () => ({ success: true }) } } as Env;
});
afterEach(() => { clearSummaryCaches(); vi.restoreAllMocks(); });

async function call(name: "endurance_session" | "coach_input", protocol: string, extraArguments: Record<string, unknown> = {}, expectError = false) {
  const contextStart = contextOperations.length;
  const ctx = createExecutionContext();
  const response = await worker.fetch(new Request(`https://${hostname}/mcp`, {
    method: "POST", headers: { host: hostname, "content-type": "application/json", accept: "application/json, text/event-stream",
      "cf-access-jwt-assertion": token, "mcp-protocol-version": protocol,
      ...(protocol === "2026-07-28" ? { "mcp-method": "tools/call", "mcp-name": name } : {}) },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: { activity_id: "garmin-42", ...extraArguments },
      ...(protocol === "2026-07-28" ? { _meta: { [PROTOCOL_VERSION_META_KEY]: protocol, [CLIENT_CAPABILITIES_META_KEY]: {} } } : {}) } }),
  }), testEnv, ctx);
  const text = await response.text(); await waitOnExecutionContext(ctx);
  expect(response.status, text).toBe(200);
  expect(response.headers.get("content-type")).toContain(protocol === "2026-07-28" ? "application/json" : "text/event-stream");
  const line = text.split("\n").find(value => value.startsWith("data: "));
  const message = JSON.parse(line ? line.slice(6) : text);
  expect(message.error, text).toBeUndefined();
  if (expectError) { expect(message.result.isError, text).toBe(true); return message.result; }
  expect(message.result.isError, text).not.toBe(true);
  expect(JSON.parse(message.result.content[0].text)).toEqual(message.result.structuredContent);
  const currentReads = contextOperations.slice(contextStart);
  const date = String(message.result.structuredContent.activity?.date ?? "").slice(5, 10);
  expect(currentReads.filter(item => item.startsWith("LIST"))).toHaveLength(
    message.result.structuredContent.activity && date.length === 5
      ? ["01-01", "12-31"].includes(date) ? 2 : 1 : 0);
  expect(currentReads.filter(item => item.startsWith("GET")).length).toBeLessThanOrEqual(1);
  return message.result.structuredContent;
}

function loadNativeForContract(analysis: Record<string, unknown>) {
  // Reuse the accepted outer storage envelope without changing the old tracers.
  load(coach17.partial.objects_base64);
  for (const name of rows.keys()) if (name.includes("/canonical/")) rows.delete(name);
  const canonical = `activities/2026/42/coach-input/v1/canonical/${analysis.analysis_id}.json`;
  rows.set(canonical, new TextEncoder().encode(JSON.stringify(analysis)));
  rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
    schema_version: 1, selection_version: 1, analysis_key: canonical, analyzer_version: analysis.analyzer_version,
  })));
}

type NativeJson = Record<string, unknown>;
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`Coach presentation sides: preserves declared sides, raw session and neutral recorded zero via ${protocol}`, async () => {
    load(declaredSides.left.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full.analysis.workout_structure.planned_steps).toEqual([]);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    const text = coached.presentation.text;
    expect(text).toContain('"rpe":7');
    expect(text).toContain('"note":"Synthetic note"');
    expect(text).toContain('anaerobic: 0; status: "available"');
    expect(text).toContain("Recorded session measurements:");
    expect(text).toContain("225 W");
    expect(text).toContain("Declared ground-contact sides:");
    expect(text).toContain("| Scope | Reference | Source laps (row) | Left (%) | Right (%) |");
    expect(text).toContain('| "native_session" | null | null | 49.25 | 50.75 |\n| "native_lap" | 1 | null | 49.25 | 50.75 |\n| "native_lap" | 2 | null | 51.25 | 48.75 |');
    for (const reference of ['"native_session"/null', '"native_lap"/1', '"native_lap"/2']) {
      const metadata = text.match(new RegExp(`^Contact side ${reference} metadata: (D\\d+)$`, "m"));
      expect(metadata).not.toBeNull();
      expect(text.split("\n")).toContain(`  ${metadata![1]}: {"status":"available","method":"recorded_and_complement","recorded_side":"left","calculated_side":"right"}`);
    }
    expect(text).toContain('"recorded_side":"left"');
    expect(text).toContain('"calculated_side":"right"');
    expect(text).toContain('"side_semantics":{"6":"unknown"}');
    expect(text).toContain("Record-derived whole_distance_halves:");
    expect(text).not.toContain("Verified planned targets:");
    expect(text).toContain("format=full");
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  it(`Coach presentation safety: rejects an unknown view before storage via ${protocol}`, async () => {
    await call("coach_input", protocol, { format: "summary" }, true);
    expect(operations).toEqual([]);
  });
  for (const state of ["unknown_activity", "missing_analysis", "unverified_analysis"] as const) {
    it(`Coach presentation safety: ${state} never fabricates a presentation via ${protocol}`, async () => {
      loadNativeForContract(native18.complete);
      if (state === "missing_analysis") {
        const canonical = `activities/2026/42/coach-input/v1/canonical/${native18.complete.analysis_id}.json`;
        rows.delete(canonical);
      } else if (state === "unverified_analysis") {
        await editCoach(analysis => { analysis.activity_id = "43"; }, native18.complete.analysis_id);
      }
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call("coach_input", protocol, { format: "coach", ...(state === "unknown_activity" ? { activity_id: "garmin-99" } : {}) });
      expect(output.available).toBe(false);
      expect(output).not.toHaveProperty("analysis");
      expect(output).not.toHaveProperty("presentation");
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations.every(op => /^(HEAD|GET) /.test(op))).toBe(true);
    });
  }
}
for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`Coach presentation history: genuine1.6 retains original halves and time basis via ${protocol}`, async () => {
    load(coach17.genuine16.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    expect(coached.presentation.source_analyzer_version).toBe("1.6.0");
    const text = coached.presentation.text;
    expect(text).toContain("TCX distance halves:");
    expect(text).toContain('TCX distance halves:\n  columns: ["distance_m","duration_seconds","pace_seconds_per_km","average_speed_mps","average_heart_rate_bpm","maximum_heart_rate_bpm","elevation_change_m"]\n  first: [1500,30,20,50,140,140,null]\n  second: [1500,30,20,50,140,140,null]');
    expect(text).toContain('Time basis: "elapsed"');
    expect(text).toContain("whole_distance_halves first");
    expect(text).toContain("main_distance_halves second");
    expect(text).toContain("Heart rate:");
    expect(text).toContain('"observed_seconds":60');
    expect(text).not.toContain("source_message_index:");
    expect(text).not.toContain("workout_step_index:");
    expect(operations).toHaveLength(4);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  for (const kind of ["sparse", "unbound"] as const) {
    it(`Coach presentation sparse: preserves ${kind} measurements without inferred links via ${protocol}`, async () => {
      loadNativeForContract(native18[kind]);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const full = await call("coach_input", protocol, { format: "full" });
      expect(full.analysis).toEqual(native18[kind]);
      clearSummaryCaches(); operations.length = 0;
      const coached = await call("coach_input", protocol, { format: "coach" });
      expect(coached.available).toBe(true);
      expect(coached).not.toHaveProperty("analysis");
      const text = coached.presentation.text;
      expect(text).toContain('"side_semantics":{"6":"unknown"}');
      expect(text).not.toContain("left_percent");
      expect(text).not.toContain("right_percent");
      if (kind === "sparse") {
        expect(text).toContain('Recorded session measurements: null W;');
        expect(full.analysis.recorded_running_metrics.average_power).toMatchObject({ status: "missing", value: null, unit: "W" });
        expect(text).not.toContain("Native recorded laps:");
        expect(full.analysis.recorded_running_metrics.laps.items).toMatchObject([
          { lap: 1, distance_m: null, average_power: { status: "missing", value: null },
            average_step_length: { status: "invalid", value: null }, average_ground_contact_time: { value: 200 },
            average_vertical_oscillation: { value: 10 }, average_vertical_ratio: { value: 5 },
            average_ground_contact_balance: { value: 101, side_semantics: "unknown" } },
          { lap: 2, distance_m: 500, average_power: { status: "available", value: 0 },
            average_step_length: { value: 1100 }, average_ground_contact_time: { value: 210 },
            average_vertical_oscillation: { value: 11 }, average_vertical_ratio: { value: 6 },
            average_ground_contact_balance: { value: 103, side_semantics: "unknown" } },
        ]);
        expect(text).toContain("main_distance_unverified");
      } else {
        expect(text).not.toContain("Native recorded laps:");
        expect(full.analysis.recorded_running_metrics.laps.items).toMatchObject([
          { lap: 1, distance_m: 400, average_power: { value: 111 }, average_ground_contact_balance: { value: 101 } },
          { lap: 2, distance_m: 500, average_power: { value: 222 }, average_ground_contact_balance: { value: 103 } },
        ]);
        expect(full.analysis.native_lap_workout_binding.reason).toBe("no_verified_lap_workout_links");
        expect(full.analysis.native_lap_workout_binding.items).toMatchObject([
          { lap: 1, source_message_index: 7, status: "unbound", reason: "workout_step_unverified", workout_step_index: null },
          { lap: 2, source_message_index: 9, status: "unbound", reason: "workout_step_unverified", workout_step_index: null },
        ]);
        expect(text).toContain("step_binding_unverified");
        expect(text).not.toContain("workout_step_index:");
      }
      expect(operations).toHaveLength(4);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    });
  }
}
const nativeBlock = (analysis: NativeJson) => analysis.native_lap_workout_binding as NativeJson;
const nativeItems = (analysis: NativeJson) => nativeBlock(analysis).items as NativeJson[];
const nativeStructure = (analysis: NativeJson) => analysis.workout_structure as {
  planned_steps: NativeJson[]; executed_laps: NativeJson[];
};
const nativeReferenceFaults: Array<{ name: string; unbound?: boolean; change: (analysis: NativeJson) => void }> = [
  { name: "missing required block", change: analysis => { delete analysis.native_lap_workout_binding; } },
  { name: "duplicated metric payload", change: analysis => { nativeItems(analysis)[0].average_power = 111; } },
  { name: "foreign activity reference", change: analysis => { nativeBlock(analysis).activity_id = "43"; } },
  { name: "foreign source digest", change: analysis => { nativeBlock(analysis).source_fit_sha256 = "b".repeat(64); } },
  { name: "null matched source digest", change: analysis => { nativeBlock(analysis).source_fit_sha256 = null; } },
  { name: "missing lap reference", change: analysis => { nativeItems(analysis).pop(); } },
  { name: "reversed lap order", change: analysis => { nativeItems(analysis).reverse(); } },
  { name: "duplicate lap ordinal", change: analysis => { nativeItems(analysis)[1].lap = 1; } },
  { name: "boolean raw index", change: analysis => { nativeItems(analysis)[0].source_message_index = true; } },
  { name: "unsafe raw index", change: analysis => { nativeItems(analysis)[0].source_message_index = 9007199254740992; } },
  { name: "duplicate raw index", change: analysis => { nativeItems(analysis)[1].source_message_index = 7; } },
  { name: "string workout index", change: analysis => { nativeItems(analysis)[0].workout_step_index = "3"; } },
  { name: "null bound raw index", change: analysis => { nativeItems(analysis)[0].source_message_index = null; } },
  { name: "null bound workout index", change: analysis => { nativeItems(analysis)[0].workout_step_index = null; } },
  { name: "missing planned step", change: analysis => { nativeStructure(analysis).planned_steps = []; } },
  { name: "duplicate planned step", change: analysis => {
    const planned = nativeStructure(analysis).planned_steps; planned.push({ ...planned[0] });
  } },
  { name: "missing executed lap", change: analysis => { nativeStructure(analysis).executed_laps.shift(); } },
  { name: "duplicate executed lap", change: analysis => {
    const executed = nativeStructure(analysis).executed_laps; executed.push({ ...executed[0] });
  } },
  { name: "different executed workout index", change: analysis => { nativeStructure(analysis).executed_laps[0].workout_step_index = 4; } },
  { name: "different executed start", change: analysis => { nativeStructure(analysis).executed_laps[0].start_time = "2026-01-01T00:00:01Z"; } },
  { name: "different executed distance", change: analysis => { nativeStructure(analysis).executed_laps[0].distance_m = 401; } },
  { name: "bound item with failure reason", change: analysis => { nativeItems(analysis)[0].reason = "executed_lap_mismatch"; } },
  { name: "all-bound partial status", change: analysis => {
    Object.assign(nativeBlock(analysis), { status: "partial", reason: "some_laps_unbound" });
  } },
  { name: "available with failure reason", change: analysis => { nativeBlock(analysis).reason = "some_laps_unbound"; } },
  { name: "unbound item with claimed workout index", unbound: true, change: analysis => { nativeItems(analysis)[0].workout_step_index = 3; } },
  { name: "unbound item without reason", unbound: true, change: analysis => { nativeItems(analysis)[0].reason = null; } },
  { name: "unverified identity with claimed raw index", unbound: true, change: analysis => { nativeItems(analysis)[0].reason = "lap_identity_unverified"; } },
  { name: "historical engine with native metadata", change: analysis => { analysis.analyzer_version = "1.7.0"; } },
];

for (const protocol of ["2025-03-26", "2026-07-28"]) {
  it(`Coach presentation tracer: coach is a versioned excerpt and full stays unchanged via ${protocol}`, async () => {
    loadNativeForContract(native18.complete);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const expectedReads = ["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${native18.complete.analysis_id}.json`];
    const omitted = await call("coach_input", protocol);
    expect(omitted.available).toBe(true);
    expect(omitted.analysis).toEqual(native18.complete);
    expect(omitted).not.toHaveProperty("presentation");
    expect(operations).toEqual(expectedReads);
    clearSummaryCaches(); operations.length = 0;
    const coached = await call("coach_input", protocol, { format: "coach" });
    expect(coached.available).toBe(true);
    expect(coached).not.toHaveProperty("analysis");
    const { analysis: _analysis, ...originalEnvelope } = omitted;
    const { presentation, ...coachEnvelope } = coached;
    expectSameCoachRead(coachEnvelope, originalEnvelope);
    expect(presentation).toMatchObject({
      version: "1.0.0", format: "coach_text", source_analysis_id: native18.complete.analysis_id,
      source_analyzer_version: "1.10.0", scope: "selected_fields", full_data_hint: "format=full",
    });
    expect(Object.keys(presentation).sort()).toEqual([
      "format", "full_data_hint", "scope", "source_analysis_id", "source_analyzer_version", "text", "version",
    ]);
    expect(typeof presentation.text).toBe("string");
    expect(presentation.text).toContain(`Analysis: ${JSON.stringify(native18.complete.analysis_id)}`);
    expect(presentation.text).not.toContain("Native recorded laps:");
    expect(presentation.text).toContain("Logical rep table");
    expect(omitted.analysis).toEqual(native18.complete);
    expect(presentation.text).toContain("clock_basis_unverified");
    expect(operations).toEqual(expectedReads);
    clearSummaryCaches(); operations.length = 0;
    const full = await call("coach_input", protocol, { format: "full" });
    expect(full).toEqual({ ...omitted, view: { ...omitted.view, resolved_at: full.view.resolved_at } });
    expect(full).not.toHaveProperty("presentation");
    expect(operations).toEqual(expectedReads);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
  });
  for (const fault of nativeReferenceFaults) {
    it(`Coach 1.8 strict native references: rejects ${fault.name} via ${protocol}`, async () => {
      // Start from an already accepted real producer output. Only the outer
      // stored canonical object is corrupted; no internal validator is mocked.
      const fixture = fault.unbound ? native18.unbound : native18.complete;
      const analysis = structuredClone(fixture) as NativeJson;
      fault.change(analysis);
      loadNativeForContract(analysis);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(false);
      expect(output).not.toHaveProperty("analysis");
      expect(output.message).toContain("current coach input could not be verified");
      expect(outputSchemas.coach_input.parse(output)).toEqual(output);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`]);
    });
  }
  for (const name of ["sparse", "unbound"] as const) {
    it(`Coach 1.8 native sparse/unbound: reads actual ${name} producer output via ${protocol}`, async () => {
      const fixture = native18[name];
      loadNativeForContract(fixture);
      const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(true);
      expect(output.analysis).toEqual(fixture);
      expect(output.analysis.analyzer_version).toBe("1.10.0");
      const block = output.analysis.native_lap_workout_binding;
      expect(block).toMatchObject({
        schema_version: 1, method: "fit_lap_wkt_step_index",
        measurement_source: "recorded_running_metrics.laps", activity_id: "42",
        source_fit_sha256: "a".repeat(64),
      });
      if (name === "sparse") {
        expect(block.status).toBe("available"); expect(block.reason).toBeNull();
        expect(block.items).toEqual([
          { lap: 1, source_message_index: 7, status: "bound", reason: null, workout_step_index: 3 },
          { lap: 2, source_message_index: 9, status: "bound", reason: null, workout_step_index: 3 },
        ]);
        const laps = output.analysis.recorded_running_metrics.laps.items;
        expect(laps[0].distance_m).toBeNull();
        expect(laps[0].average_power).toEqual({ status: "missing", value: null, unit: "W", source_field: "avg_power" });
        expect(laps[0].average_step_length).toEqual({ status: "invalid", value: null, unit: "mm", source_field: "avg_step_length" });
        expect(laps[1].average_power).toEqual({ status: "available", value: 0, unit: "W", source_field: "avg_power" });
        expect(laps.map((lap: { average_ground_contact_balance: { value: number } }) => lap.average_ground_contact_balance.value))
          .toEqual([101, 103]);
      } else {
        expect(block.status).toBe("unavailable"); expect(block.reason).toBe("no_verified_lap_workout_links");
        expect(block.items).toEqual([
          { lap: 1, source_message_index: 7, status: "unbound", reason: "workout_step_unverified", workout_step_index: null },
          { lap: 2, source_message_index: 9, status: "unbound", reason: "workout_step_unverified", workout_step_index: null },
        ]);
        expect(output.analysis.recorded_running_metrics.laps.items.map((lap: { average_power: { value: number } }) => lap.average_power.value))
          .toEqual([111, 222]);
      }
      for (const lap of output.analysis.recorded_running_metrics.laps.items) {
        expect(lap.average_ground_contact_balance.side_semantics).toBe("unknown");
        expect(lap.average_ground_contact_balance).not.toHaveProperty("left_percent");
        expect(lap.average_ground_contact_balance).not.toHaveProperty("right_percent");
      }
      const derived = output.analysis.segment_running_metrics;
      expect(derived).toEqual(fixture.segment_running_metrics);
      expect(derived.whole_distance_halves.status).toBe("available");
      expect(derived.main_distance_halves).toMatchObject({
        status: "unavailable", reason: name === "sparse" ? "main_distance_unverified" : "step_binding_unverified", items: [],
      });
      expect(derived.executed_intervals.reason).toBe(derived.main_distance_halves.reason);
      expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`]);
    });
  }
  it(`Coach 1.8 native binding tracer: retains genuine1.7 and its clock gate via ${protocol}`, async () => {
    function loadNative(analysis: Record<string, unknown>) {
      load(coach17.partial.objects_base64);
      for (const name of rows.keys()) if (name.includes("/canonical/")) rows.delete(name);
      const canonical = `activities/2026/42/coach-input/v1/canonical/${analysis.analysis_id}.json`;
      rows.set(canonical, new TextEncoder().encode(JSON.stringify(analysis)));
      rows.set("activities/2026/42/coach-input/v1/latest-ready.json", new TextEncoder().encode(JSON.stringify({
        schema_version: 1, selection_version: 1, analysis_key: canonical, analyzer_version: analysis.analyzer_version,
      })));
    }
    loadNative(native17);
    const old = await call("coach_input", protocol);
    expect(old.available).toBe(true);
    expect(old.analysis).toEqual(native17);
    expect(old.analysis).not.toHaveProperty("native_lap_workout_binding");
    expect(operations).toHaveLength(4);
    clearSummaryCaches(); operations.length = 0;
    loadNative(native18.complete);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toEqual(native18.complete);
    expect(output.analysis.native_lap_workout_binding.items).toEqual([
      { lap: 1, source_message_index: 7, status: "bound", reason: null, workout_step_index: 3 },
      { lap: 2, source_message_index: 9, status: "bound", reason: null, workout_step_index: 3 },
    ]);
    expect(output.analysis.segment_running_metrics).toEqual(native17.segment_running_metrics);
    expect(output.analysis.segment_running_metrics.executed_intervals.reason).toBe("clock_basis_unverified");
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${native18.complete.analysis_id}.json`]);
  });
  it(`Coach 1.7 partial coverage tracer: retains genuine1.6 and exposes actual schema2 boundary exclusion via ${protocol}`, async () => {
    // Exact before-edit1.6 canonical bytes; never generated by the new producer.
    load(coach17.genuine16.objects_base64);
    const old = await call("coach_input", protocol);
    expect(old.available).toBe(true);
    expect(old.analysis).toMatchObject({ analyzer_version: "1.6.0", segment_running_metrics: { schema_version: 1 } });
    expect(outputSchemas.coach_input.parse(old)).toEqual(old);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${coach17.genuine16.analysis_id}.json`]);

    clearSummaryCaches(); operations.length = 0;
    // Actual immutable v2 producer output. Only the outer synthetic storage
    // envelope is assembled at the already agreed R2 system boundary.
    load(coach17.partial.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toEqual(coach17.partial.analysis);
    const block = output.analysis.segment_running_metrics;
    expect(output.analysis.analyzer_version).toBe("1.7.0");
    expect(block).toMatchObject({ schema_version: 2, session_end_time_utc: "2026-01-01T00:00:08.500Z",
      source_window: { disposition: "verified_terminal_exclusion", original_record_count: 6,
        boundary_excluded_record_count: 1, boundary_excluded_pair_count: 1 },
      executed_intervals: { status: "unavailable", reason: "no_verified_structured_workout", items: [] },
      whole_distance_halves: { status: "available", items: [
        { average_power: { value: 166.6667, coverage: { valid_seconds: 4.5, eligible_seconds: 4.5,
          excluded_gap_seconds: 0, boundary_excluded_seconds: 0, contributing_intervals: 3, status: "complete" } } },
        { average_power: { value: 357.1429, coverage: { valid_seconds: 3.5, eligible_seconds: 3.5,
          excluded_gap_seconds: 0, boundary_excluded_seconds: 0.5, contributing_intervals: 2, status: "partial" } },
          average_ground_contact_balance: { value: 101, side_semantics: "unknown" } },
      ] },
    });
    expect(block.main_distance_halves.items).toEqual(block.whole_distance_halves.items);
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${coach17.partial.analysis_id}.json`]);
  });

  type PartialBlock = { source_window: Record<string, unknown>;
    whole_distance_halves: { items: Array<Record<string, { coverage: Record<string, unknown> }>> } };
  const partialBlock = (analysis: Record<string, unknown>) => analysis.segment_running_metrics as PartialBlock;
  const schema2Faults: Array<{ name: string; genuine16?: boolean; checkBaseline?: boolean;
    change: (analysis: Record<string, unknown>) => void }> = [
    { name: "missing source window", change: analysis => {
      delete (analysis.segment_running_metrics as Record<string, unknown>).source_window;
    } },
    { name: "missing boundary duration", change: analysis => {
      delete partialBlock(analysis).whole_distance_halves.items[1].average_power.coverage.boundary_excluded_seconds;
    } },
    { name: "per-field boundary disagreement within its budget", change: analysis => {
      partialBlock(analysis).whole_distance_halves.items[1].average_power.coverage.boundary_excluded_seconds = 0.4;
    } },
    { name: "in-frame disposition with excluded boundary time", change: analysis => {
      Object.assign(partialBlock(analysis).source_window,
        { disposition: "verified_in_frame", boundary_excluded_record_count: 0, boundary_excluded_pair_count: 0 });
    } },
    { name: "forged excluded pair count", change: analysis => {
      partialBlock(analysis).source_window.boundary_excluded_pair_count = 0;
    } },
    { name: "impossible complete boundary coverage", change: analysis => {
      partialBlock(analysis).whole_distance_halves.items[1].average_power.coverage.status = "complete";
    } },
    { name: "contributions exceeding original record count", change: analysis => {
      partialBlock(analysis).source_window.original_record_count = 2;
    } },
    { name: "excluded pair not subtracted from original count", checkBaseline: true, change: analysis => {
      // Four records give three original pairs; one boundary-excluded pair
      // leaves at most two usable pairs, yet the first half claims three.
      partialBlock(analysis).source_window.original_record_count = 4;
    } },
    { name: "joint halves over budget despite individually valid rows", change: analysis => {
      const row = partialBlock(analysis).whole_distance_halves.items[0];
      for (const field of ["average_power", "average_step_length", "average_ground_contact_time",
        "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance"]) {
        Object.assign(row[field].coverage, { boundary_excluded_seconds: 0.5, status: "partial" });
      }
    } },
    { name: "schema2 under engine1.6", change: analysis => { analysis.analyzer_version = "1.6.0"; } },
    { name: "genuine schema1 under engine1.7", genuine16: true,
      change: analysis => { analysis.analyzer_version = "1.7.0"; } },
  ];
  for (const fault of schema2Faults) {
    it(`Coach 1.7 strict partial coverage: rejects ${fault.name} via ${protocol}`, async () => {
      // Current terminal fixture is from the ordinary final Python publisher;
      // corruption is confined to the same outer stored-object seam as before.
      const fixture = fault.genuine16 ? coach17.genuine16 : produced16.terminal_partial;
      load(fixture.objects_base64);
      if (fault.checkBaseline) {
        const valid = await call("coach_input", protocol);
        expect(valid.available).toBe(true);
        expect(valid.analysis).toEqual(produced16.terminal_partial.analysis);
        expect(valid.analysis.segment_running_metrics).toMatchObject({
          source_window: { original_record_count: 6, boundary_excluded_pair_count: 1 },
          whole_distance_halves: { items: [{ average_power: { coverage: { contributing_intervals: 3 } } },
            { average_power: { coverage: { contributing_intervals: 2 } } }] },
        });
        expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
          "GET activities/2026/42/coach-input/v1/latest-ready.json",
          `GET activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`]);
        clearSummaryCaches(); operations.length = 0;
      }
      await editCoach(fault.change, fixture.analysis_id);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(false);
      expect(output).not.toHaveProperty("analysis");
      expect(output.message).toContain("current coach input could not be verified");
      expect(outputSchemas.coach_input.parse(output)).toEqual(output);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`]);
    });
  }

  it(`Coach 1.6 segment metrics tracer: reads genuine executed and whole/main halves via ${protocol}`, async () => {
    load(produced16.complete.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis.analyzer_version).toBe("1.10.0");
    expect(output.analysis).toEqual(produced16.complete.analysis);
    const block = output.analysis.segment_running_metrics;
    // Independent worked values from the accepted FIT tracer, not a second implementation.
    expect(block).toMatchObject({ schema_version: 2, activity_id: "42", association: "matched",
      session_start_time_utc: "2026-10-07T23:30:00.000Z", session_end_time_utc: "2026-10-07T23:31:00.000Z",
      executed_intervals: { status: "available", reason: null,
        clock_basis: "fit_start_time_plus_total_elapsed_time",
        balance_aggregation: "raw_scalar_time_weighted_mean_side_unknown",
        items: [
          { interval: 1, source_laps: [2, 3], workout_step_index: 1, distance_m: 1000,
            average_power: { value: 150, unit: "W", source_field: "power" } },
          { interval: 2, source_laps: [5], workout_step_index: 1, distance_m: 500,
            average_power: { value: 400 } },
        ] },
      whole_distance_halves: { status: "available", distance_basis: "tcx_endurance_distance",
        items: [{ segment: "first", distance_m: 1500, average_power: { value: 400 } },
          { segment: "second", distance_m: 1500, average_power: { value: 433.3333 } }] },
      main_distance_halves: { status: "available", distance_basis: "fit_executed_main_distance",
        items: [
          { segment: "first", start_distance_m: 0, end_distance_m: 1000, distance_m: 1000,
            average_power: { value: 150 }, average_step_length: { value: 1050, unit: "mm" },
            average_ground_contact_time: { value: 210, unit: "ms" },
            average_vertical_oscillation: { value: 70, unit: "mm" },
            average_vertical_ratio: { value: 7, unit: "%" },
            average_ground_contact_balance: { value: 102, side_semantics: "unknown" } },
          { segment: "second", start_distance_m: 1000, end_distance_m: 2000, distance_m: 1000,
            average_power: { value: 200 }, average_step_length: { value: 1300,
              coverage: { valid_seconds: 10, eligible_seconds: 20, status: "partial" } },
            average_ground_contact_time: { value: 250 }, average_vertical_oscillation: { value: 110 },
            average_vertical_ratio: { value: 11 },
            average_ground_contact_balance: { value: 106, side_semantics: "unknown" } },
        ] },
    });
    expect(JSON.stringify(block)).not.toMatch(/left_percent|right_percent|recorded_side|calculated_side/);
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced16.complete.analysis_id}.json`]);
  });

  it(`Coach 1.6 segment metrics source windows: retains official elapsed end despite summary timestamp via ${protocol}`, async () => {
    load(produced16.official_clock.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toEqual(produced16.official_clock.analysis);
    expect(output.analysis.segment_running_metrics).toMatchObject({
      session_start_time_utc: "2026-10-07T23:30:00.000Z",
      session_end_time_utc: "2026-10-07T23:31:00.000Z",
      session_summary_timestamp_utc: "2026-10-07T23:30:59.000Z",
      executed_intervals: { status: "available", items: [{ source_laps: [2, 3] }, { source_laps: [5] }] },
      whole_distance_halves: { status: "available",
        items: [{ average_power: { value: 400 } }, { average_power: { value: 433.3333 } }] },
      main_distance_halves: { status: "available",
        items: [{ average_power: { value: 150 } }, { average_power: { value: 200 } }] },
    });
    expect(output.analysis.recorded_running_metrics).toMatchObject({ measured_at_utc: "2026-10-07T23:30:59.000Z",
      distance_halves: { status: "unavailable", reason: "record_order_unverified", items: [] } });
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced16.official_clock.analysis_id}.json`]);
  });

  it(`Coach 1.6 segment metrics source windows: keeps main and intervals when whole distance is unaligned via ${protocol}`, async () => {
    load(produced16.unaligned_whole.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toEqual(produced16.unaligned_whole.analysis);
    expect(output.analysis.segment_running_metrics).toMatchObject({
      whole_distance_halves: { status: "unavailable", reason: "distance_alignment_unverified", items: [] },
      executed_intervals: { status: "available", items: [{ source_laps: [2, 3] }, { source_laps: [5] }] },
      main_distance_halves: { status: "available", distance_basis: "fit_executed_main_distance",
        items: [{ average_power: { value: 150 }, average_ground_contact_balance: { value: 102 } },
          { average_power: { value: 200 }, average_ground_contact_balance: { value: 106 } }] },
    });
    expect(output.analysis.recorded_running_metrics.distance_halves).toMatchObject({
      status: "unavailable", reason: "distance_alignment_unverified", items: [] });
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced16.unaligned_whole.analysis_id}.json`]);
  });

  for (const [name, fixture] of [
    ["complete", produced], ["sparse", produced.sparse],
    ["source_error", produced.source_error], ["invalid_halves", produced.invalid_halves],
  ] as const) {
    it(`Coach 1.6 final source variants: ${name} retains genuine optional data via ${protocol}`, async () => {
      load(fixture.objects_base64);
      const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
      const canonicalKey = `activities/2026/42/coach-input/v1/canonical/${fixture.analysis_id}.json`;
      const bytes = rows.get(canonicalKey);
      if (!bytes) throw new Error("Missing genuine final Coach fixture");
      const stream = new Response(bytes).body;
      if (!stream) throw new Error("Missing genuine final Coach fixture stream");
      const plain = bytes[0] === 0x1f && bytes[1] === 0x8b ? stream.pipeThrough(new DecompressionStream("gzip")) : stream;
      const expected = await new Response(plain).json();
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(true);
      expect(output.analysis).toEqual(expected);
      expect(output.analysis).toMatchObject({ analyzer_version: "1.10.0", activity_id: "42", analysis_id: fixture.analysis_id,
        workout_structure: { has_structured_workout: false } });
      const block = output.analysis.segment_running_metrics;
      expect(block.main_distance_halves.distance_basis).toBe("tcx_endurance_distance_continuous_main");
      expect(block.executed_intervals).toMatchObject({ status: "unavailable", items: [] });
      if (name === "complete" || name === "sparse") {
        expect(block.executed_intervals.reason).toBe("no_verified_structured_workout");
        for (const group of [block.whole_distance_halves, block.main_distance_halves]) {
          expect(group).toMatchObject({ status: "available", reason: null });
          expect(group.items).toHaveLength(2);
          if (name === "sparse") {
            for (const row of group.items) {
              expect(row.average_power).toMatchObject({ status: "available", value: 0 });
              for (const field of ["average_step_length", "average_ground_contact_time", "average_vertical_oscillation",
                "average_vertical_ratio", "average_ground_contact_balance"]) {
                expect(row[field]).toMatchObject({ status: "missing", value: null,
                  coverage: { status: "none", valid_seconds: 0, contributing_intervals: 0 } });
              }
            }
          }
        }
        expect(block.main_distance_halves.items).toEqual(block.whole_distance_halves.items);
      } else {
        for (const group of [block.whole_distance_halves, block.main_distance_halves]) {
          expect(group).toMatchObject({ status: "unavailable", items: [],
            reason: name === "source_error" ? "source_unverified" : "invalid_distance_partition" });
        }
        if (name === "source_error") {
          // The stored source failed decoding; this is not evidence that FIT is absent.
          expect(block).toMatchObject({ association: "decode_errors", source_fit_sha256: null,
            session_start_time_utc: null, session_end_time_utc: null, session_summary_timestamp_utc: null });
          expect(block.executed_intervals.reason).toBe("source_unverified");
        } else {
          expect(block.association).toBe("matched");
          expect(block.executed_intervals.reason).toBe("no_verified_structured_workout");
        }
      }
      expect(outputSchemas.coach_input.parse(output)).toEqual(output);
      expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json", `GET ${canonicalKey}`]);
    });
  }

  for (const [group, seconds] of [["whole_distance_halves", 31], ["main_distance_halves", 21]] as const) {
    it(`Coach 1.6 review SP01: rejects aggregate ${group} coverage beyond its span via ${protocol}`, async () => {
      load(produced16.complete.objects_base64);
      await editCoach(analysis => {
        const block = analysis.segment_running_metrics as typeof produced16.complete.analysis.segment_running_metrics;
        // Preserve each field's positive original pair count; only corrupt the duration totals.
        for (const row of block[group].items) {
          for (const metric of ["average_power", "average_step_length", "average_ground_contact_time",
            "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance"] as const) {
            Object.assign(row[metric].coverage, { valid_seconds: seconds, eligible_seconds: seconds,
              excluded_gap_seconds: 0, status: "complete" });
          }
        }
      }, produced16.complete.analysis_id);
      const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(false);
      expect(output).not.toHaveProperty("analysis");
      expect(output.message).toContain("current coach input could not be verified");
      expect(outputSchemas.coach_input.parse(output)).toEqual(output);
      expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${produced16.complete.analysis_id}.json`]);
    });
  }

  const malformed: Array<{ name: string; change: (block: typeof produced16.complete.analysis.segment_running_metrics) => void }> = [
    { name: "unknown block field", change: block => { Object.assign(block, { unexpected: true }); } },
    { name: "different activity", change: block => { block.activity_id = "43"; } },
    { name: "different FIT digest", change: block => { block.source_fit_sha256 = "0".repeat(64); } },
    { name: "coverage beyond eligible time", change: block => {
      const coverage = block.main_distance_halves.items[0].average_power.coverage;
      coverage.valid_seconds = coverage.eligible_seconds + 1;
    } },
    { name: "available measurement with null value", change: block => {
      Object.assign(block.main_distance_halves.items[0].average_power, { value: null });
    } },
    { name: "invented balance policy", change: block => {
      block.main_distance_halves.balance_aggregation = "recorded_and_complement";
    } },
    { name: "unavailable group retaining successful items", change: block => {
      Object.assign(block.whole_distance_halves, { status: "unavailable", reason: "synthetic_corruption" });
    } },
    { name: "noncontiguous source laps", change: block => { block.executed_intervals.items[0].source_laps = [2, 4]; } },
  ];
  for (const { name, change } of malformed) {
    it(`Coach 1.6 segment metrics boundary: rejects ${name} via ${protocol}`, async () => {
      load(produced16.complete.objects_base64);
      // Corrupt only the outer stored object; the real reader/auth/MCP path stays intact.
      await editCoach(analysis => { change(analysis.segment_running_metrics as typeof produced16.complete.analysis.segment_running_metrics); },
        produced16.complete.analysis_id);
      const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
      const output = await call("coach_input", protocol);
      expect(output.available).toBe(false);
      expect(output).not.toHaveProperty("analysis");
      expect(output.message).toContain("current coach input could not be verified");
      expect(outputSchemas.coach_input.parse(output)).toEqual(output);
      expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/coach-input/v1/latest-ready.json",
        `GET activities/2026/42/coach-input/v1/canonical/${produced16.complete.analysis_id}.json`]);
    });
  }

  it(`Coach 1.6 segment metrics boundary: keeps genuine 1.5 readable but rejects the new block via ${protocol}`, async () => {
    load(legacy150.objects_base64);
    const before = [...rows].map(([key, body]) => [key, Array.from(body)]);
    const legacy = await call("coach_input", protocol);
    expect(legacy.available).toBe(true);
    expect(legacy.analysis.analyzer_version).toBe("1.5.0");
    expect(legacy.analysis).not.toHaveProperty("segment_running_metrics");
    expect(outputSchemas.coach_input.parse(legacy)).toEqual(legacy);
    expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(before);
    await editCoach(analysis => {
      analysis.segment_running_metrics = structuredClone(produced16.complete.analysis.segment_running_metrics);
    }, legacy150.analysis_id);
    const corrupted = [...rows].map(([key, body]) => [key, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(false);
    expect(output).not.toHaveProperty("analysis");
    expect(output.message).toContain("current coach input could not be verified");
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([key, body]) => [key, Array.from(body)])).toEqual(corrupted);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${legacy150.analysis_id}.json`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${legacy150.analysis_id}.json`]);
  });

  it(`running segment tracer: exposes the same genuine stored FIT segments and Coach source data via ${protocol}`, async () => {
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true); expect(coach.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 2, activity_id: "42", association: "matched",
      average_ground_contact_balance: { unit: "%", source_field: "avg_stance_time_balance", side_semantics: "unknown" },
      laps: { status: "available", method: "fit_recorded_measurement" },
      kilometer_splits: { status: "available", method: "fit_record_time_weighted_mean",
        interval_policy: "left_sample_hold_distance_progressing_max_30s", distance_allocation: "linear_between_record_distances" },
      distance_halves: { status: "available", method: "fit_record_time_weighted_mean" } });
    expect(coach.analysis.analyzer_version).toBe("1.10.0");
    expect(detail.recorded_running_metrics).toEqual(coach.analysis.recorded_running_metrics);
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", "GET activities/2026/42/activity.endurance.v1.json",
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
    expect(JSON.stringify(detail.recorded_running_metrics)).not.toMatch(/position_lat|position_long|record_mesgs|lap_mesgs/);
  });
  it(`running segment sparse: preserves zero, missing measurements and dataset availability via ${protocol}`, async () => {
    load(produced.sparse.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true); expect(coach.available).toBe(true);
    expect(detail.recorded_running_metrics).toEqual(coach.analysis.recorded_running_metrics);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 2, association: "matched",
      average_power: { status: "available", value: 0 },
      average_ground_contact_balance: { status: "missing", value: null, side_semantics: "unknown" } });
    for (const group of [detail.recorded_running_metrics.laps, detail.recorded_running_metrics.kilometer_splits,
      detail.recorded_running_metrics.distance_halves]) {
      expect(group.status).toBe("available"); expect(group.items).toHaveLength(2);
      for (const segment of group.items) {
        expect(segment.average_power).toMatchObject({ status: "available", value: 0 });
        for (const field of ["average_step_length", "average_ground_contact_time", "average_vertical_oscillation", "average_vertical_ratio"]) {
          expect(segment[field]).toMatchObject({ status: "missing", value: null });
        }
      }
    }
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.sparse.analysis_id}.json`]);
  });
  const untrusted: Array<[string, (block: Record<string, unknown>) => void]> = [
    ["missing group", block => { delete block.laps; }],
    ["unbound activity", block => { block.activity_id = "43"; }],
    ["unbound FIT digest", block => { block.source_fit_sha256 = "f".repeat(64); }],
    ["unbound session clock", block => { block.session_start_time_utc = "2026-10-07T23:30:01.000Z"; }],
    ["unbound local date", block => { block.local_date = "2026-10-09"; }],
    ["changed session measurement", block => { (block.average_power as Record<string, unknown>).value = 999; }],
    ["unknown GPS field", block => { block.position_lat = 1; }],
    ["nested raw field", block => { (block.laps as Record<string, unknown>).lap_mesgs = [{ position_long: 1 }]; }],
    ["nonfinite coverage", block => {
      const group = block.kilometer_splits as { items: Array<{ average_power: { coverage: Record<string, unknown> } }> };
      group.items[0].average_power.coverage.valid_seconds = "1";
    }],
  ];
  for (const [label, change] of untrusted) {
    it(`running segment fallback: ${label} does not hide TCX or add a read via ${protocol}`, async () => {
      await editStored(change);
      const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
      const detail = await call("endurance_session", protocol);
      expect(detail.available).toBe(true);
      expect(detail.session.summary.distance_m).toBe(2000);
      expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
        activity_id: "42", average_power: { status: "available", value: 225 } });
      expect(detail.recorded_running_metrics).not.toHaveProperty("laps");
      expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
      expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
      expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
        "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`]);
      expect(JSON.stringify(detail)).not.toMatch(/position_lat|position_long|lap_mesgs/);
    });
  }
  it(`stored coverage invariant: rejects valid time greater than eligible time via ${protocol}`, async () => {
    await editStored(block => {
      const group = block.kilometer_splits as { items: Array<{ average_power: { coverage: { valid_seconds: number; eligible_seconds: number } } }> };
      const coverage = group.items[0].average_power.coverage;
      coverage.valid_seconds = coverage.eligible_seconds + 1;
    });
    const detail = await call("endurance_session", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(detail.recorded_running_metrics).not.toHaveProperty("kilometer_splits");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`]);
  });
  it(`stored lap invariant: rejects a lap outside the verified FIT session via ${protocol}`, async () => {
    await editStored(block => {
      const laps = block.laps as { items: Array<{ end_time_utc: string }> };
      laps.items[0].end_time_utc = "2030-01-01T00:00:00.000Z";
    });
    const detail = await call("endurance_session", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(detail.recorded_running_metrics).not.toHaveProperty("laps");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`]);
  });
  it(`stored distance invariant: rejects overlapping distance partitions via ${protocol}`, async () => {
    await editStored(block => {
      const splits = block.kilometer_splits as { items: Array<{ start_distance_m: number }> };
      splits.items[1].start_distance_m = 900;
    });
    const detail = await call("endurance_session", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(detail.recorded_running_metrics).not.toHaveProperty("kilometer_splits");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`]);
  });
  it(`Coach 1.2 integrity: rejects an unknown raw field in the new immutable measurements via ${protocol}`, async () => {
    await editCoach(analysis => {
      (analysis.recorded_running_metrics as Record<string, unknown>).position_lat = 1;
    });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(false);
    expect(output).not.toHaveProperty("analysis");
    expect(output.message).toContain("current coach input could not be verified");
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
    expect(JSON.stringify(output)).not.toContain("position_lat");
  });
  it(`stored complete coverage invariant: rejects a complete metric with an excluded gap via ${protocol}`, async () => {
    await editStored(block => {
      const splits = block.kilometer_splits as { items: Array<{ average_power: { coverage: { excluded_gap_seconds: number } } }> };
      splits.items[0].average_power.coverage.excluded_gap_seconds = 1;
    });
    const detail = await call("endurance_session", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(detail.recorded_running_metrics).not.toHaveProperty("kilometer_splits");
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`]);
  });
  it(`Coach producer unavailable: retains the genuine summary when optional FIT decoding failed via ${protocol}`, async () => {
    load(produced.source_error.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const output = await call("coach_input", protocol);
    expect(output.available).toBe(true);
    expect(output.analysis).toMatchObject({ analyzer_version: "1.10.0", activity_id: "42",
      analysis_id: produced.source_error.analysis_id, summary: { distance_m: 2000 },
      recorded_running_metrics: { schema_version: 2, association: "decode_errors", source_fit_sha256: null,
        average_power: { status: "unavailable", value: null },
        laps: { status: "unavailable", reason: "decode_errors", items: [] },
        kilometer_splits: { status: "unavailable", reason: "decode_errors", items: [] },
        distance_halves: { status: "unavailable", reason: "decode_errors", items: [] } } });
    expect(output.analysis.source.fit_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(outputSchemas.coach_input.parse(output)).toEqual(output);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.source_error.analysis_id}.json`]);
  });
  it(`running segment group-state integrity: rejects invalid groups that retain successful items via ${protocol}`, async () => {
    const invalidate = (block: Record<string, unknown>) => {
      const splits = block.kilometer_splits as Record<string, unknown>;
      splits.status = "invalid";
      splits.reason = "invalid_distance_partition";
    };
    await editStored(invalidate);
    await editCoach(analysis => { invalidate(analysis.recorded_running_metrics as Record<string, unknown>); });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(detail.recorded_running_metrics).not.toHaveProperty("kilometer_splits");
    expect(coach.available).toBe(false);
    expect(coach).not.toHaveProperty("analysis");
    expect(coach.message).toContain("current coach input could not be verified");
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
  });
  it(`AD-SEG-01 producer invalid halves: retains the genuine Coach summary and valid other groups via ${protocol}`, async () => {
    load(produced.invalid_halves.objects_base64);
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true); expect(coach.available).toBe(true);
    expect(coach.analysis.summary).toMatchObject({ distance_m: 2000, elapsed_seconds: 40 });
    expect(detail.recorded_running_metrics).toEqual(coach.analysis.recorded_running_metrics);
    expect(coach.analysis.recorded_running_metrics).toMatchObject({ schema_version: 2, association: "matched",
      average_power: { status: "available", value: 225 },
      laps: { status: "available" }, kilometer_splits: { status: "available" },
      distance_halves: { status: "invalid", reason: "invalid_distance_partition", items: [] } });
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.invalid_halves.analysis_id}.json`]);
  });
  it(`AD-SEG-01 stored unequal halves: rejects corrupt unequal partitions in both readers via ${protocol}`, async () => {
    const corrupt = (block: Record<string, unknown>) => {
      const halves = block.distance_halves as { items: Array<{ start_distance_m: number; end_distance_m: number; distance_m: number }> };
      Object.assign(halves.items[0], { start_distance_m: 0, end_distance_m: 500, distance_m: 500 });
      Object.assign(halves.items[1], { start_distance_m: 500, end_distance_m: 2000, distance_m: 1500 });
    };
    await editStored(corrupt);
    await editCoach(analysis => { corrupt(analysis.recorded_running_metrics as Record<string, unknown>); });
    const before = [...rows].map(([name, body]) => [name, Array.from(body)]);
    const detail = await call("endurance_session", protocol);
    const coach = await call("coach_input", protocol);
    expect(detail.available).toBe(true);
    expect(detail.recorded_running_metrics).toMatchObject({ schema_version: 1, association: "matched",
      average_power: { status: "available", value: 225 } });
    expect(coach.available).toBe(false);
    expect(coach).not.toHaveProperty("analysis");
    expect(coach.message).toContain("current coach input could not be verified");
    expect(outputSchemas.endurance_session.parse(detail)).toEqual(detail);
    expect(outputSchemas.coach_input.parse(coach)).toEqual(coach);
    expect([...rows].map(([name, body]) => [name, Array.from(body)])).toEqual(before);
    expect(operations).toEqual(["HEAD summary/activities.csv", "GET summary/activities.csv",
      "GET activities/2026/42/activity.v1.json", `GET ${enduranceKey}`,
      "HEAD summary/activities.csv", "GET activities/2026/42/coach-input/v1/latest-ready.json",
      `GET activities/2026/42/coach-input/v1/canonical/${produced.analysis_id}.json`]);
  });
}
