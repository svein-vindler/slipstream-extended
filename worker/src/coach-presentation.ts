type Json = Record<string, unknown>;

function record(value: unknown): Json {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Json : {};
}

function storedText(value: unknown): string {
  return typeof value === "string" ? value : "not_stored";
}

const nativeMeasurements = [
  "average_power", "average_step_length", "average_ground_contact_time",
  "average_vertical_oscillation", "average_vertical_ratio", "average_ground_contact_balance",
] as const;

function data(value: unknown): string {
  return JSON.stringify(value) ?? "not_stored";
}

function fields(value: unknown, names: readonly string[]): Json {
  const source = record(value), selected: Json = {};
  for (const name of names) if (source[name] !== undefined) selected[name] = source[name];
  return selected;
}

function measurement(name: string, value: unknown): string {
  const metric = record(value);
  const numeric = typeof metric.value === "number" && Number.isFinite(metric.value);
  const unit = typeof metric.unit === "string" ? metric.unit : undefined;
  const displayUnit = unit === undefined ? "" : ` ${["W", "mm", "ms", "%"].includes(unit) ? unit : data(unit)}`;
  return `  ${name}: ${numeric ? `${metric.value}${displayUnit}` : "null"}`
    + `; status: ${data(storedText(metric.status))}`
    + (!numeric && unit !== undefined ? `; unit: ${data(unit)}` : "")
    + (metric.source_field === undefined ? "" : `; source_field: ${data(metric.source_field)}`)
    + (metric.side_semantics === undefined ? "" : `; side_semantics: ${data(metric.side_semantics)}`)
    + (metric.coverage === undefined ? "" : `; coverage: ${data(metric.coverage)}`);
}

/** Duplicate or absent identities never select a row. No positional matching. */
function uniqueRows(value: unknown, field: string): Map<number, Json | null> {
  const rows = new Map<number, Json | null>();
  for (const item of Array.isArray(value) ? value : []) {
    const row = record(item), key = row[field];
    if (typeof key === "number" && Number.isSafeInteger(key) && key >= 0) {
      rows.set(key, rows.has(key) ? null : row);
    }
  }
  return rows;
}

/** A neutral excerpt of an analysis already verified by the current selector. No I/O or mutation. */
export function coachPresentation(analysis: Json) {
  const sourceId = storedText(analysis.analysis_id);
  const sourceVersion = storedText(analysis.analyzer_version);
  const lines = [
    `Analysis: ${data(sourceId)}`,
    `Analyzer: ${data(sourceVersion)}`,
    "Scope: selected_fields. Full data: format=full.",
    `Activity: ${data({ activity_id: analysis.activity_id, ...record(analysis.activity) })}`,
    "Summary:",
  ];
  const summary = record(analysis.summary);
  for (const field of ["elapsed_seconds", "moving_seconds", "distance_m", "average_speed_mps",
    "average_heart_rate_bpm", "maximum_heart_rate_bpm", "trackpoint_count", "sampled_trackpoint_count"]) {
    lines.push(`  ${field}: ${data(summary[field])}`);
  }
  lines.push(`Profile/references: ${analysis.profile == null ? "not_stored" : data(fields(analysis.profile,
    ["profile_id", "name", "effective_from", "zones", "references"]))}`);
  lines.push(`User context: ${analysis.user_context == null ? "not_stored" : data(analysis.user_context)}`);
  const effect = record(analysis.training_effect);
  lines.push(`Training Effect: ${analysis.training_effect == null ? "not_stored" : data(fields(effect,
    ["source", "method", "association", "local_start_time", "local_date", "session_start_time_utc", "measured_at_utc"]))}`);
  if (analysis.training_effect != null) {
    lines.push(`  scale: ${data(effect.scale)}`, measurement("aerobic", effect.aerobic),
      measurement("anaerobic", effect.anaerobic));
  }
  lines.push(`Stored context: ${analysis.stored_context_snapshot == null ? "not_stored" : data(analysis.stored_context_snapshot)}`,
    `Sections: ${data(analysis.sections)}`,
    `Time basis: ${data(analysis.time_basis)}`,
    `Heart rate: ${data(analysis.heart_rate)}`,
    `TCX distance halves: ${data(analysis.distance_halves)}`);
  const laps = record(record(analysis.recorded_running_metrics).laps);
  const native = uniqueRows(laps.items, "lap");
  const binding = record(analysis.native_lap_workout_binding);
  const references = uniqueRows(binding.items, "lap");
  const messages = uniqueRows(binding.items, "source_message_index");
  const structure = record(analysis.workout_structure);
  const planned = uniqueRows(structure.planned_steps, "step_index");
  const executed = uniqueRows(structure.executed_laps, "lap");
  lines.push("Recorded session measurements:");
  for (const field of nativeMeasurements) {
    lines.push(measurement(field, record(analysis.recorded_running_metrics)[field]));
  }
  const sides = record(analysis.ground_contact_sides);
  lines.push(`Declared ground-contact sides: ${analysis.ground_contact_sides == null ? "not_stored"
    : data(sides.status === "available" ? sides : fields(sides, ["status", "reason"]))}`,
    `Recorded FIT distance halves: ${data(record(analysis.recorded_running_metrics).distance_halves)}`,
    `Workout plan: ${data(structure.planned_steps)}`,
    `Stored executed laps (separate from native rows): ${data(structure.executed_laps)}`);
  lines.push(`Native recorded laps: ${JSON.stringify(storedText(laps.status))}`);
  if (laps.reason !== null && laps.reason !== undefined) {
    lines.push(`Reason: ${JSON.stringify(laps.reason)}`);
  }
  for (const value of Array.isArray(laps.items) ? laps.items : []) {
    const lap = record(value);
    lines.push(`Lap ${JSON.stringify(lap.lap)}:`);
    lines.push(`  recorded: ${data(fields(lap, ["start_time_utc", "end_time_utc", "distance_m"]))}`);
    const reference = typeof lap.lap === "number" ? references.get(lap.lap) : undefined;
    const execution = typeof lap.lap === "number" ? executed.get(lap.lap) : undefined;
    const step = typeof reference?.workout_step_index === "number"
      ? planned.get(reference.workout_step_index) : undefined;
    const verified = binding.method === "fit_lap_wkt_step_index" && reference?.status === "bound"
      && typeof lap.lap === "number" && native.get(lap.lap) === lap
      && typeof reference.source_message_index === "number" && messages.get(reference.source_message_index) === reference
      && step && execution && execution.workout_step_index === reference.workout_step_index;
    lines.push(`  native binding: ${data(reference?.status ?? "not_stored")}`
      + (reference?.reason == null ? "" : `; reason: ${data(reference.reason)}`));
    if (reference?.source_message_index != null) {
      lines.push(`  source_message_index: ${data(reference.source_message_index)}`);
    }
    if (verified && reference) {
      lines.push(`  workout_step_index: ${data(reference.workout_step_index)}`,
        `  execution: ${data(fields(execution, ["start_time", "elapsed_seconds", "moving_seconds", "distance_m", "intensity", "section"]))}`);
    }
    for (const field of nativeMeasurements) {
      lines.push(measurement(field, lap[field]));
    }
  }
  const segments = record(analysis.segment_running_metrics);
  for (const field of ["executed_intervals", "whole_distance_halves", "main_distance_halves"] as const) {
    const group = record(segments[field]);
    lines.push(`Record-derived ${field}: ${JSON.stringify(storedText(group.status))}`
      + (group.reason === null || group.reason === undefined ? ""
        : `; reason: ${JSON.stringify(group.reason)}`));
    lines.push(`  basis: ${data(fields(group, ["method", "distance_basis", "clock_basis", "interval_policy", "distance_allocation", "balance_aggregation"]))}`);
    for (const value of Array.isArray(group.items) ? group.items : []) {
      const item = record(value);
      const label = field === "executed_intervals" ? `interval ${data(item.interval)}` : storedText(item.segment);
      lines.push(`${field} ${label}:`, `  scope: ${data(fields(item,
        ["source_laps", "workout_step_index", "start_time_utc", "end_time_utc", "start_distance_m", "end_distance_m", "distance_m"]))}`);
      for (const name of nativeMeasurements) lines.push(measurement(name, item[name]));
    }
    if (field !== "executed_intervals" && group.status !== "available") {
      for (const half of ["first", "second"]) {
        lines.push(`${field} ${half}: ${storedText(group.status)}`
          + (group.reason == null ? "" : `; reason: ${data(group.reason)}`));
      }
    }
  }
  return {
    version: "1.0.0" as const,
    format: "coach_text" as const,
    source_analysis_id: sourceId,
    source_analyzer_version: sourceVersion,
    scope: "selected_fields" as const,
    text: lines.join("\n"),
    full_data_hint: "format=full" as const,
  };
}
