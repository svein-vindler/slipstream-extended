import { describe, expect, it } from "vitest";
import { trainingContext, trainingContextSchema } from "../src/training-context";
import { type Activity } from "../src/lib";

const activity: Activity = { id: "garmin-42", source: "garmin", name: "Synthetic",
  type: "running", date: new Date("2026-10-07T23:30:00Z") };
function fixture() {
  return { schema_version: 1, source: "garmin-fit", decode_errors: [] as unknown[],
    source_fit_sha256: "a".repeat(64),
    activity: { id: "42", start_time_gmt: "2026-10-07 23:30:00", start_time_local: "2026-10-08 01:30:00" },
    messages: { session_mesgs: [{ start_time: "2026-10-07T23:30:00+00:00",
      timestamp: "2026-10-08T00:00:00+00:00", total_training_effect: 3.4,
      total_anaerobic_training_effect: 0 }] },
  };
}

describe("allowlisted FIT Training Effect projection", () => {
  it("preserves already-scaled values, zero, ID and Garmin-local date", () => {
    const payload = fixture();
    const before = structuredClone(payload);
    const context = trainingContext(activity, payload);
    expect(payload).toEqual(before);
    expect(trainingContextSchema.parse(context)).toEqual(context);
    expect(context).toMatchObject({ activity_id: "garmin-42", local_date: "2026-10-08",
      method: "fit_recorded_estimate",
      local_start_time: "2026-10-08T01:30:00", association: "matched",
      session_start_time_utc: "2026-10-07T23:30:00.000Z", measured_at_utc: "2026-10-08T00:00:00.000Z",
      aerobic: { status: "available", value: 3.4 }, anaerobic: { status: "available", value: 0 } });
  });

  it.each([undefined, null])("keeps missing value %s distinct from zero", value => {
    const payload = fixture();
    Object.assign(payload.messages.session_mesgs[0], { total_training_effect: value });
    expect(trainingContext(activity, payload).aerobic).toMatchObject({ status: "missing", value: null });
  });
  it.each([-1, 5.1, 25.5, 255, NaN, Infinity, "3.4", false, {}, [], 1.23])("rejects invalid decoded value %s", value => {
    const payload = fixture();
    Object.assign(payload.messages.session_mesgs[0], { total_training_effect: value });
    const context = trainingContext(activity, payload);
    expect(context.aerobic).toMatchObject({ status: "invalid", value: null });
    expect(context.anaerobic).toMatchObject({ status: "available", value: 0 });
    expect(trainingContextSchema.safeParse(context).success).toBe(true);
  });

  it.each([0, 0.1, 3.4, 5])("accepts documented decoded value %s without scaling", value => {
    const payload = fixture();
    payload.messages.session_mesgs[0].total_training_effect = value;
    expect(trainingContext(activity, payload).aerobic.value).toBe(value);
  });

  it("reports old/missing/unknown schema distinctly without claiming non-support", () => {
    expect(trainingContext(activity, undefined).association).toBe("source_missing");
    expect(trainingContext(activity, null).association).toBe("invalid_schema");
    expect(trainingContext(activity, null, "source_unreadable").association).toBe("source_unreadable");
    expect(trainingContext(activity, { schema_version: 2 }).association).toBe("invalid_schema");
    const payload = fixture();
    Object.assign(payload, { messages: {} });
    expect(trainingContext(activity, payload).association).toBe("sessions_missing");
    Object.assign(payload, { messages: { session_mesgs: [] } });
    expect(trainingContext(activity, payload).association).toBe("sessions_missing");
    Object.assign(payload, { messages: { session_mesgs: {} } });
    expect(trainingContext(activity, payload).association).toBe("invalid_schema");
    Object.assign(payload, { messages: "unknown-shape" });
    expect(trainingContext(activity, payload).association).toBe("invalid_schema");
  });

  it("rejects multi-session files even when the first session matches", () => {
    const payload = fixture();
    payload.messages.session_mesgs.push({ ...payload.messages.session_mesgs[0],
      start_time: "2026-10-08T00:00:00+00:00", total_training_effect: 4.2 });
    const context = trainingContext(activity, payload);
    expect(context.association).toBe("sessions_ambiguous");
    expect(context.aerobic.value).toBeNull();
  });

  it("rejects wrong identity, time, source and partial decode", () => {
    const payload = fixture();
    payload.activity.id = "43";
    expect(trainingContext(activity, payload).association).toBe("id_mismatch");
    payload.activity.id = "42";
    payload.activity.start_time_gmt = "2026-10-07 22:30:00";
    expect(trainingContext(activity, payload).association).toBe("time_mismatch");
    payload.activity.start_time_gmt = "2026-10-07 23:30:00";
    payload.messages.session_mesgs[0].start_time = "2026-10-07T22:30:00+00:00";
    expect(trainingContext(activity, payload).association).toBe("time_mismatch");
    payload.source = "other";
    expect(trainingContext(activity, payload).association).toBe("invalid_schema");
    payload.source = "garmin-fit";
    payload.decode_errors.push("synthetic-partial-error");
    expect(trainingContext(activity, payload).association).toBe("decode_errors");
    expect(trainingContext({ ...activity, source: "other" }, fixture()).association).toBe("id_mismatch");
  });

  it("keeps unknown local/measurement times null without inventing a UTC day", () => {
    const payload = fixture();
    payload.activity.start_time_local = "2026-02-30 01:00:00";
    payload.messages.session_mesgs[0].timestamp = "2026-10-07T23:00:00+00:00";
    const context = trainingContext(activity, payload);
    expect(context.local_date).toBeNull();
    expect(context.measured_at_utc).toBeNull();
    expect(context.aerobic.value).toBe(3.4);
  });

  it("uses FIT only and ignores conflicting DTO/developer/lap fields and source text", () => {
    const payload = fixture();
    Object.assign(payload, { training_context: { aerobic: 5 }, summaryDTO: { trainingEffect: 5 },
      messages: { ...payload.messages, lap_mesgs: [{ total_training_effect: 5 }],
        record_mesgs: [{ position_lat: 123, instruction: "synthetic-source-text" }] } });
    Object.assign(payload.messages.session_mesgs[0], { trainingEffect: 5,
      developer_fields: { total_training_effect: 5 }, upload_url: "synthetic-source-text" });
    const context = trainingContext(activity, payload);
    expect(context.aerobic.value).toBe(3.4);
    expect(JSON.stringify(context)).not.toMatch(/synthetic-source-text|developer|position|upload|summaryDTO|lap_mesgs/);
    expect(trainingContextSchema.safeParse({ ...context, extra: "injected" }).success).toBe(false);
    expect(trainingContextSchema.safeParse({ ...context, aerobic: { ...context.aerobic, value: null } }).success).toBe(false);
  });
});
